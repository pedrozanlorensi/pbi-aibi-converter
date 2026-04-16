"""
Power BI to Databricks AI/BI Dashboard Converter — Streamlit entrypoint.

This module handles the UI layout, user interaction, and orchestration.
All business logic lives in sub-modules:
  - clients.py   — Databricks + LLM client factories and shared constants
  - converter.py  — PBI parsing, LLM conversion, explanation generation
  - validator.py  — .lvdash.json structural and SQL validation
"""

import os
import json
import traceback
from dataclasses import dataclass, field
from typing import Any

import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import Dashboard

from clients import MODEL, STATIC_DIR, VALID_WIDGET_VERSIONS
from export_pdf import build_export_pdf
from converter import (
    extract_upload,
    find_pbi_folders,
    collect_pbi_context,
    collect_pbi_context_chunked,
    extract_pbi_source_tables,
    detect_external_sources,
    extract_pbi_theme_colors,
    build_color_context,
    parse_pbi_layout,
    build_free_layout_blueprint_prompt,
    call_llm,
    call_llm_chunked,
    generate_explanation,
    extract_json_from_response,
    apply_pbi_colors,
    _ensure_fqn_tables,
    fix_dataset_columns,
    _estimate_tokens,
    MAX_PROMPT_TOKENS,
)
from validator import validate_dashboard, validate_layout_fidelity, validate_table_coverage

# ---------------------------------------------------------------------------
# Page config (must be the first Streamlit command)
# ---------------------------------------------------------------------------

st.set_page_config(page_title="PBI to AI/BI Converter", page_icon=":bar_chart:", layout="centered")
st.markdown(
    "<style>div[data-testid='stFileUploader'] small {display:none}</style>",
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# Session-state helpers
# ---------------------------------------------------------------------------

@dataclass
class ReportResult:
    """All output data for a single converted report."""
    name: str = ""
    status: str = "pending"  # pending | running | done | error
    error_msg: str = ""
    raw_traceback: str = ""
    dashboard_json: dict = field(default_factory=dict)
    dashboard_id: str = ""
    dash_url: str = ""
    workspace_path: str = ""
    n_datasets: int = 0
    n_widgets: int = 0
    n_canvas: int = 0
    n_pages: int = 0
    layout_fidelity: Any = None
    explanation: str = ""
    validation: Any = None
    data_sources: list = field(default_factory=list)
    external_sources: list = field(default_factory=list)
    pdf_bytes: bytes | None = None


if "results" not in st.session_state:
    st.session_state["results"] = []
if "batch_running" not in st.session_state:
    st.session_state["batch_running"] = False


# ---------------------------------------------------------------------------
# Core conversion function (single report)
# ---------------------------------------------------------------------------

def convert_single_report(uploaded_file, report_name: str, progress, custom_instructions: str = "") -> ReportResult:
    """Run the full conversion pipeline for one report. Returns a ReportResult."""
    result = ReportResult(name=report_name, status="running")

    try:
        # --- Phase 1: Extract & Parse ---
        progress.write("Extracting uploaded files...")
        tmpdir = extract_upload(uploaded_file)
        report_dir, semantic_dir = find_pbi_folders(tmpdir)

        if not report_dir or not semantic_dir:
            found = []
            for r, dirs, files in os.walk(tmpdir):
                for f in files:
                    found.append(os.path.relpath(os.path.join(r, f), tmpdir))
            result.status = "error"
            result.error_msg = (
                "Could not find `.Report` and `.SemanticModel` folders.\n\nFiles found:\n"
                + "\n".join(found[:30])
            )
            return result

        progress.write(f"Report: `{os.path.basename(report_dir)}`")
        progress.write(f"Model: `{os.path.basename(semantic_dir)}`")

        progress.write("Reading PBI report files...")
        pbi_context = collect_pbi_context(report_dir, semantic_dir)
        pbi_source_tables = extract_pbi_source_tables(semantic_dir)
        data_sources = detect_external_sources(semantic_dir)
        result.data_sources = data_sources

        external_sources = [s for s in data_sources if not s["is_databricks"] and s["source_type"] != "Calculated (PBI)"]
        result.external_sources = external_sources
        if external_sources:
            unique_types = sorted({s["source_type"] for s in external_sources})
            progress.write(f"Warning: {len(external_sources)} table(s) from external sources: {', '.join(unique_types)}")

        progress.write("Extracting PBI theme colors...")
        color_palette = extract_pbi_theme_colors(report_dir)
        if color_palette.data_colors:
            preview = ", ".join(color_palette.data_colors[:6])
            progress.write(f"Found {len(color_palette.data_colors)} theme colors: {preview}")

        progress.write("Parsing PBI layout structure...")
        pbi_layout = parse_pbi_layout(report_dir, color_palette=color_palette)
        progress.write(
            f"Found {pbi_layout.total_canvas_pages} page(s), "
            f"{pbi_layout.total_data_visuals} data visual(s), "
            f"{pbi_layout.total_page_slicers} page-level slicer(s), "
            f"{pbi_layout.total_global_slicers} global slicer(s)"
        )

        layout_blueprint = build_free_layout_blueprint_prompt(pbi_layout)
        color_context = build_color_context(color_palette, pbi_layout)

        # --- Phase 2: LLM Conversion ---
        if custom_instructions and custom_instructions.strip():
            progress.write(f"Custom instructions included ({len(custom_instructions)} chars)")

        est_tokens = _estimate_tokens(pbi_context + layout_blueprint + color_context)
        progress.write(f"Estimated context size: ~{est_tokens:,} tokens")

        use_chunked = est_tokens > MAX_PROMPT_TOKENS
        if use_chunked:
            progress.write(f"Context exceeds limit — using multi-turn chunked mode")
            semantic_ctx, page_chunks = collect_pbi_context_chunked(report_dir, semantic_dir)
            progress.write(f"Split into {len(page_chunks)} page chunk(s)")
            raw_response = call_llm_chunked(
                report_name, semantic_ctx, page_chunks, layout_blueprint,
                color_context=color_context,
                custom_instructions=custom_instructions,
                progress_callback=lambda msg: progress.write(f"  {msg}"),
            )
        else:
            progress.write(f"Sending to {MODEL} for conversion...")
            raw_response = call_llm(report_name, pbi_context, layout_blueprint, color_context=color_context, custom_instructions=custom_instructions)

        progress.write("Parsing dashboard JSON...")
        dashboard_json = extract_json_from_response(raw_response)

        if color_palette.data_colors:
            progress.write("Applying PBI color palette to chart widgets...")
            dashboard_json = apply_pbi_colors(dashboard_json, color_palette, pbi_layout)

        result.dashboard_json = dashboard_json
        result.n_datasets = len(dashboard_json.get("datasets", []))
        result.n_pages = len(dashboard_json.get("pages", []))
        result.n_widgets = sum(len(p.get("layout", [])) for p in dashboard_json.get("pages", []))
        progress.write(f"Generated {result.n_datasets} datasets, {result.n_pages} pages, {result.n_widgets} widgets")

        # --- Phase 3: SQL column check & fix ---
        sp_client = WorkspaceClient()

        warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
        if not warehouse_id:
            for wh in sp_client.warehouses.list():
                warehouse_id = wh.id
                break
        if not warehouse_id:
            result.status = "error"
            result.error_msg = "No SQL warehouse found. Please set DATABRICKS_WAREHOUSE_ID."
            return result

        progress.write("Ensuring fully-qualified table names...")
        dashboard_json = _ensure_fqn_tables(dashboard_json)

        progress.write("Checking dataset SQL against UC tables...")
        dashboard_json = fix_dataset_columns(dashboard_json, warehouse_id, sp_client)
        result.dashboard_json = dashboard_json

        # --- Phase 4: Validation ---
        progress.write("Validating dashboard...")
        validation = validate_dashboard(dashboard_json, warehouse_id, sp_client)

        progress.write("Validating layout fidelity against PBI source...")
        layout_fidelity = validate_layout_fidelity(dashboard_json, pbi_layout)
        validation.layout_fidelity = layout_fidelity
        result.layout_fidelity = layout_fidelity

        progress.write("Validating table coverage...")
        table_coverage = validate_table_coverage(dashboard_json, pbi_source_tables)
        validation.table_coverage = table_coverage
        result.validation = validation

        result.n_canvas = layout_fidelity.actual_pages

        # --- Phase 5: Deploy ---
        progress.write("Deploying to Databricks workspace...")
        parent_path = f"/Workspace/Shared/aibi_converter/{report_name}"
        sp_client.workspace.mkdirs(parent_path)

        serialized = json.dumps(dashboard_json, indent=2)
        dashboard_obj = Dashboard(
            display_name=report_name,
            parent_path=parent_path,
            serialized_dashboard=serialized,
            warehouse_id=warehouse_id,
        )

        try:
            api_result = sp_client.lakeview.create(dashboard=dashboard_obj)
        except Exception as create_err:
            if "already exists" in str(create_err):
                progress.write("Dashboard already exists, updating...")
                existing_id = None
                for d in sp_client.lakeview.list():
                    if d.display_name == report_name:
                        existing_id = d.dashboard_id
                        break
                if not existing_id:
                    ws_path = f"{parent_path}/{report_name}.lvdash.json"
                    try:
                        sp_client.workspace.delete(ws_path)
                    except Exception:
                        pass
                    api_result = sp_client.lakeview.create(dashboard=dashboard_obj)
                else:
                    api_result = sp_client.lakeview.update(dashboard_id=existing_id, dashboard=dashboard_obj)
            else:
                raise

        result.dashboard_id = api_result.dashboard_id
        host = sp_client.config.host.rstrip("/")
        result.dash_url = f"{host}/sql/dashboardsv3/{result.dashboard_id}"
        result.workspace_path = f"{parent_path}/{report_name}.lvdash.json"

        progress.write("Publishing dashboard...")
        sp_client.lakeview.publish(dashboard_id=result.dashboard_id, warehouse_id=warehouse_id)

        progress.write("Generating conversion report...")
        result.explanation = generate_explanation(report_name, pbi_context, dashboard_json)

        # --- PDF ---
        try:
            result.pdf_bytes = build_export_pdf(
                report_name=report_name, model=MODEL,
                workspace_path=result.workspace_path, dash_url=result.dash_url,
                n_datasets=result.n_datasets, n_widgets=result.n_widgets,
                n_canvas=result.n_canvas, n_pages=result.n_pages,
                layout_fidelity=layout_fidelity, explanation=result.explanation,
                validation=validation, data_sources=data_sources,
                external_sources=external_sources, dashboard_json=dashboard_json,
                valid_widget_versions=VALID_WIDGET_VERSIONS,
            )
        except Exception:
            pass

        result.status = "done"

    except json.JSONDecodeError as e:
        result.status = "error"
        result.error_msg = f"LLM returned invalid JSON: {e}"
        result.raw_traceback = traceback.format_exc()

    except Exception as e:
        result.status = "error"
        result.error_msg = f"Conversion failed: {e}"
        result.raw_traceback = traceback.format_exc()

    return result


# ---------------------------------------------------------------------------
# Results display function (single report tab)
# ---------------------------------------------------------------------------

def render_report_results(res: ReportResult):
    """Render the full results UI for one converted report inside a tab."""

    if res.status == "error":
        st.error(res.error_msg)
        if res.raw_traceback:
            with st.expander("Full traceback"):
                st.code(res.raw_traceback, language="text")
        return

    if res.status == "pending" or res.status == "running":
        st.info("This report is still being processed...")
        return

    st.success("Dashboard converted and published successfully!")

    n_filter_pages = res.n_pages - res.n_canvas
    lf = res.layout_fidelity
    page_match = "ok" if (lf and lf.page_count_match) else "mismatch"

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Datasets", res.n_datasets)
    col2.metric("Widgets", res.n_widgets)
    col3.metric("Canvas Pages", f"{res.n_canvas}", delta=f"+ {n_filter_pages} filter page(s)" if n_filter_pages else None)
    col4.metric("PBI Tabs Matched", f"{lf.expected_pages} -> {res.n_canvas}" if lf else "N/A")

    st.markdown(f"**Report:** {res.name}")
    st.markdown(f"**Model:** `{MODEL}`")
    st.markdown(f"**Workspace path:** `{res.workspace_path}`")
    st.markdown(f"**[Open Dashboard]({res.dash_url})**")

    # PDF download
    if res.pdf_bytes:
        st.download_button(
            ":material/download: Export Validation Report (PDF)",
            data=res.pdf_bytes,
            file_name=f"{res.name}_validation_report.pdf",
            mime="application/pdf",
            key=f"pdf_{res.name}",
        )

    # Conversion explanation
    with st.expander("Conversion Report", expanded=False):
        st.markdown(res.explanation)

    # Tables & Data Sources
    _render_tables_section(res)

    # Validation
    _render_validation_section(res)


def _render_tables_section(res: ReportResult):
    """Render the Tables & Data Sources expander."""
    validation = res.validation
    if not validation:
        return

    has_external = bool(res.external_sources)
    section_title = "Tables & Data Sources" if not has_external else "Tables, Data Sources & Migration"
    with st.expander(section_title, expanded=False):
        tc = getattr(validation, "table_coverage", None)
        ds_lookup = {s["pbi_table"]: s for s in res.data_sources} if res.data_sources else {}

        if tc:
            n_physical = len(tc.queried_tables) + len(tc.missing_tables)
            n_calc = len(tc.calculated_tables)
            n_internal = len(tc.internal_tables)

            summary_parts = []
            if tc.passed and n_physical > 0:
                summary_parts.append(f"All **{len(tc.queried_tables)} physical table(s)** queried in the dashboard")
            elif tc.missing_tables:
                summary_parts.append(f"{len(tc.queried_tables)} of {n_physical} physical table(s) queried")
            if n_calc:
                summary_parts.append(f"**{n_calc} calculated** (DAX -> SQL)")
            if n_internal:
                summary_parts.append(f"{n_internal} local PBI table(s)")

            if has_external:
                unique_types = sorted({s["source_type"] for s in res.external_sources})
                st.warning(
                    f"**{len(res.external_sources)} table(s)** come from external sources "
                    f"({', '.join(unique_types)}). These need to be accessible from your Databricks workspace."
                )
            elif summary_parts:
                st.success(". ".join(summary_parts) + ". No migration needed.")

            table_rows = []
            for tbl in tc.queried_tables:
                ds_info = ds_lookup.get(tbl["pbi_table"], {})
                table_rows.append({
                    "Status": "Queried", "PBI Table": tbl["pbi_table"],
                    "Source": ds_info.get("source_type", "Databricks"),
                    "Connection": ds_info.get("connector_detail") or tbl["source_fqn"],
                })
            for tbl in tc.missing_tables:
                ds_info = ds_lookup.get(tbl["pbi_table"], {})
                table_rows.append({
                    "Status": "Unused", "PBI Table": tbl["pbi_table"],
                    "Source": ds_info.get("source_type", "Databricks"),
                    "Connection": ds_info.get("connector_detail") or tbl["source_fqn"],
                })
            for tbl in tc.calculated_tables:
                table_rows.append({
                    "Status": "Calculated", "PBI Table": tbl["pbi_table"],
                    "Source": "DAX -> SQL (CTE)", "Connection": "-",
                })
            for tbl in tc.internal_tables:
                table_rows.append({
                    "Status": "Local", "PBI Table": tbl["pbi_table"],
                    "Source": "PBI auto-generated", "Connection": "-",
                })

            if table_rows:
                import pandas as pd
                df = pd.DataFrame(table_rows)
                st.dataframe(df, hide_index=True, use_container_width=True)

            if has_external:
                st.markdown("### How to bring external data into Databricks")
                st.markdown(
                    "- **Lakehouse Federation** — Query external databases in-place without moving data. "
                    "Create a *foreign catalog* in Unity Catalog.\n\n"
                    "- **Lakeflow Connect** — Ingest data into Delta tables with managed CDC pipelines.\n\n"
                    "- **Lakebridge** — Migrate entire data warehouses and their workloads to Databricks."
                )
        else:
            st.info("Table validation was not run.")


def _render_validation_section(res: ReportResult):
    """Render the Validation Results expander."""
    validation = res.validation
    if not validation:
        return

    dashboard_json = res.dashboard_json
    n_datasets = res.n_datasets
    n_pages = res.n_pages
    n_widgets = res.n_widgets
    lf = res.layout_fidelity

    with st.expander("Validation Results", expanded=False):
        if validation.passed and not validation.warnings:
            st.success("All checks passed — no errors or warnings.")
        elif validation.passed:
            st.info(f"No errors, but {len(validation.warnings)} warning(s) found.")
        else:
            st.warning(f"{len(validation.errors)} error(s) and {len(validation.warnings)} warning(s) found.")

        st.markdown("#### Dashboard Structure")
        st.markdown(f"- **Datasets:** {n_datasets}")
        st.markdown(f"- **Pages:** {n_pages}")
        st.markdown(f"- **Widgets:** {n_widgets}")

        if lf:
            st.markdown("#### Layout Fidelity (PBI -> AI/BI)")

            if lf.page_count_match:
                st.markdown(
                    f"- **Page count:** {lf.actual_pages} canvas page(s) — "
                    f"matches PBI source ({lf.expected_pages} tab(s))"
                )
            else:
                st.markdown(
                    f"- **Page count mismatch:** expected {lf.expected_pages} canvas page(s) "
                    f"from PBI, got {lf.actual_pages}"
                )
                if lf.missing_pages:
                    for mp in lf.missing_pages:
                        st.markdown(f"  - Missing page: \"{mp}\"")
                if lf.extra_pages:
                    for ep in lf.extra_pages:
                        st.markdown(f"  - Extra page: \"{ep}\"")

            for entry in lf.page_visual_counts:
                match = entry["actual"] >= entry["expected"]
                icon = "ok" if match else "warning"
                st.markdown(
                    f"- **Page \"{entry['name']}\":** {entry['actual']} data widget(s) "
                    f"(expected {entry['expected']} from PBI)"
                )

            if lf.missing_visuals:
                st.markdown(f"- **{len(lf.missing_visuals)} PBI visual(s) not found in dashboard:**")
                for mv in lf.missing_visuals:
                    st.markdown(f"  - `{mv['visual_type']}` — {mv['description']} (page: {mv['page']})")
            else:
                st.markdown("- All PBI visuals are represented in the dashboard")

            if lf.position_warnings:
                st.markdown(f"- **{len(lf.position_warnings)} widget(s) with position drift:**")
                for pw in lf.position_warnings:
                    st.markdown(
                        f"  - `{pw['visual_type']}` ({pw['description']}): "
                        f"expected x={pw['expected_x']}, w={pw['expected_w']} -> "
                        f"got x={pw['actual_x']}, w={pw['actual_w']}"
                    )
            else:
                st.markdown("- Widget positions approximate PBI layout")

        st.markdown("#### Widget Inventory")
        for page in dashboard_json.get("pages", []):
            p_name = page.get("displayName", page.get("name", ""))
            p_type = page.get("pageType", "unknown")
            widgets_on_page = page.get("layout", [])
            st.markdown(f"**{p_name}** ({p_type}) — {len(widgets_on_page)} widget(s)")
            for item in widgets_on_page:
                w = item.get("widget", {})
                pos = item.get("position", {})
                w_name = w.get("name", "")
                pos_str = f"x={pos.get('x')}, y={pos.get('y')}, w={pos.get('width')}, h={pos.get('height')}"
                if "multilineTextboxSpec" in w:
                    text_preview = (w["multilineTextboxSpec"].get("lines", [""])[0] or "")[:60]
                    st.markdown(f"- `{w_name}` — **text** — {pos_str} — *{text_preview}*")
                else:
                    spec = w.get("spec", {})
                    wt = spec.get("widgetType", "unknown")
                    ver = spec.get("version", "?")
                    expected = VALID_WIDGET_VERSIONS.get(wt)
                    ver_status = "ok" if expected is None or ver == expected else f"expected {expected}"
                    st.markdown(f"- `{w_name}` — **{wt}** v{ver} ({ver_status}) — {pos_str}")

        if validation.sql_results:
            st.markdown("#### SQL Query Validation")
            for ds_name, succeeded, error_msg, cols in validation.sql_results:
                if succeeded:
                    st.markdown(f"- `{ds_name}` — query OK, {len(cols)} columns returned: `{'`, `'.join(cols[:15])}`")
                else:
                    st.markdown(f"- `{ds_name}` — {error_msg}")

        field_issues = [e for e in validation.errors if "fieldName" in e or "query fields" in e]
        dataset_issues = [e for e in validation.errors if "references dataset" in e]
        other_errors = [e for e in validation.errors if e not in field_issues and e not in dataset_issues and "SQL" not in e]

        if field_issues or dataset_issues or other_errors:
            st.markdown("#### Structural Errors")
            for err in field_issues + dataset_issues + other_errors:
                st.markdown(f"- {err}")
        else:
            st.markdown("#### Structural Checks")
            st.markdown("- All widget versions are correct")
            st.markdown("- All encoding fieldNames match query field names")
            st.markdown("- All dataset references are valid")
            st.markdown("- All widget positions are within the 6-column grid")

        if validation.warnings:
            st.markdown("#### Warnings")
            for warn in validation.warnings:
                st.markdown(f"- {warn}")


# ---------------------------------------------------------------------------
# UI Layout
# ---------------------------------------------------------------------------

st.title("Power BI -> AI/BI Converter")
st.caption(
    f"Upload up to **10** Power BI projects (.pbip) as **zip files** and convert them to "
    f"Databricks AI/BI dashboards using **{MODEL}**."
)

with st.expander("How to prepare your upload", icon=":material/help:"):
    st.markdown(
        "**Step 1 — Export as .pbip from Power BI Desktop**\n\n"
        'In Power BI Desktop, go to **File -> Save As** and select '
        '**Power BI project files (*.pbip)** from the "Save as type" dropdown:'
    )
    pbip_img = STATIC_DIR / "power_bi_save_as_pbip.png"
    if pbip_img.is_file():
        st.image(str(pbip_img))
    st.markdown(
        "This creates three items in the same folder:\n"
        "- `YourReport.pbip` — project file\n"
        "- `YourReport.Report/` — report visuals & pages\n"
        "- `YourReport.SemanticModel/` — data model & table definitions\n\n"
        "**Step 2 — Zip the results**\n\n"
        "Select all three items, right-click -> **Compress** (macOS) or **Send to -> Compressed folder** (Windows). "
        "Upload the resulting `.zip` file below."
    )

st.divider()

uploaded_files = st.file_uploader(
    "Upload .pbip project(s) (zip files)",
    type=["zip"],
    accept_multiple_files=True,
    help="Zip(s) containing the .pbip file, .Report/ folder, and .SemanticModel/ folder. Max 10 files.",
)
st.caption("Dashboards will be named after the uploaded zip files.")

if uploaded_files and len(uploaded_files) > 10:
    st.error("Please upload at most 10 files at a time.")
    st.stop()

custom_instructions = st.text_area(
    "Custom Instructions (optional)",
    placeholder=(
        "e.g. rename the table from 'sales' to 'sales_data'"
    ),
    help=(
        "Add any extra context for the conversion. Common uses:\n"
        "- Map external sources to a federated catalog (catalog.schema.table)\n"
        "- Rename or remap specific tables\n"
        "- Specify default catalog/schema for unqualified table names\n"
        "- Request specific SQL transformations or filters"
    ),
    height=100,
)

convert_clicked = st.button("Convert & Publish", type="primary", use_container_width=True)

# ---------------------------------------------------------------------------
# Batch Conversion Orchestration
# ---------------------------------------------------------------------------

if convert_clicked:
    if not uploaded_files:
        st.error("Please upload at least one .pbip zip file.")
        st.stop()

    st.session_state["results"] = []
    st.session_state["batch_running"] = True
    n_files = len(uploaded_files)
    results: list[ReportResult] = []

    overall = st.container()
    overall.markdown(f"### Batch conversion: {n_files} report(s)")

    for idx, uf in enumerate(uploaded_files):
        report_name = os.path.splitext(uf.name)[0]

        overall.markdown(f"---\n**[{idx + 1}/{n_files}]** Converting **{report_name}**...")
        progress = overall.status(f"Converting {report_name}...", expanded=True)

        result = convert_single_report(uf, report_name, progress, custom_instructions=custom_instructions)
        results.append(result)

        if result.status == "done":
            progress.update(label=f"{report_name} — done!", state="complete")
            with overall.container(border=True):
                c1, c2, c3 = st.columns([2, 1, 1])
                c1.markdown(f"**{report_name}** — [Open Dashboard]({result.dash_url})")
                c2.metric("Widgets", result.n_widgets)
                c3.metric("Pages", result.n_canvas)
        else:
            progress.update(label=f"{report_name} — failed", state="error")
            overall.error(f"**{report_name}** failed: {result.error_msg}")

        st.session_state["results"] = list(results)

    st.session_state["results"] = results
    st.session_state["batch_running"] = False

    summary_ok = sum(1 for r in results if r.status == "done")
    summary_err = sum(1 for r in results if r.status == "error")
    overall.markdown(f"### Batch complete: {summary_ok} succeeded, {summary_err} failed")
    overall.info("Scroll down for full results, validation details, and PDF exports for each report.")

# ---------------------------------------------------------------------------
# Results Tabs (persistent across reruns via session_state)
# ---------------------------------------------------------------------------

results: list[ReportResult] = st.session_state.get("results", [])

if results:
    st.divider()
    st.subheader("Conversion Results")

    tab_labels = [f"{r.name}" for r in results]
    tabs = st.tabs(tab_labels)

    for tab, res in zip(tabs, results):
        with tab:
            render_report_results(res)
