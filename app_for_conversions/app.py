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

import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import Dashboard

from clients import MODEL, STATIC_DIR, VALID_WIDGET_VERSIONS
from converter import (
    extract_upload,
    find_pbi_folders,
    collect_pbi_context,
    collect_pbi_context_chunked,
    extract_pbi_source_tables,
    detect_external_sources,
    parse_pbi_layout,
    build_layout_blueprint_prompt,
    build_free_layout_blueprint_prompt,
    should_use_free_layout,
    call_llm,
    call_llm_chunked,
    generate_explanation,
    extract_json_from_response,
    apply_blueprint_positions,
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

# ---------------------------------------------------------------------------
# UI Layout
# ---------------------------------------------------------------------------

st.title("Power BI → AI/BI Converter")
st.caption(
    f"Upload a Power BI project (.pbip) as a **zip file** and convert it to a "
    f"Databricks AI/BI dashboard using **{MODEL}**."
)

with st.expander("How to prepare your upload", icon=":material/help:"):
    st.markdown(
        "**Step 1 — Export as .pbip from Power BI Desktop**\n\n"
        'In Power BI Desktop, go to **File → Save As** and select '
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
        "Select all three items, right-click → **Compress** (macOS) or **Send to → Compressed folder** (Windows). "
        "Upload the resulting `.zip` file below."
    )

st.divider()

report_name = st.text_input("Dashboard Name", placeholder="e.g. Sales Highlights")
uploaded_file = st.file_uploader(
    "Upload .pbip project (zip file)",
    type=["zip"],
    help="Zip containing the .pbip file, .Report/ folder, and .SemanticModel/ folder.",
)

layout_mode = st.radio(
    "Layout mode",
    options=["Auto", "Strict", "Free"],
    horizontal=True,
    captions=[
        "The converter will decide the best layout for the report (Strict for simple reports, Free for dense ones)",
        "The converter will keep the positions from the original PBI report (may not be optimal for grouped visuals and custom visuals, deterministic conversion)",
        "LLM decides the best layout for the report (better aesthetics, non-deterministic conversion)"
    ],
)

convert_clicked = st.button("Convert & Publish", type="primary", use_container_width=True)

# ---------------------------------------------------------------------------
# Conversion Orchestration
# ---------------------------------------------------------------------------

if convert_clicked:
    if not report_name or not report_name.strip():
        st.error("Please enter a dashboard name.")
        st.stop()
    if not uploaded_file:
        st.error("Please upload a .pbip zip file.")
        st.stop()

    report_name = report_name.strip()
    progress = st.status("Converting...", expanded=True)

    try:
        # --- Phase 1: Extract & Parse ---
        progress.write("📦 Extracting uploaded files...")
        tmpdir = extract_upload(uploaded_file)
        report_dir, semantic_dir = find_pbi_folders(tmpdir)

        if not report_dir or not semantic_dir:
            found = []
            for r, dirs, files in os.walk(tmpdir):
                for f in files:
                    found.append(os.path.relpath(os.path.join(r, f), tmpdir))
            progress.update(label="Error", state="error")
            st.error(
                "Could not find `.Report` and `.SemanticModel` folders.\n\nFiles found:\n"
                + "\n".join(found[:30])
            )
            st.stop()

        progress.write(f"📄 Report: `{os.path.basename(report_dir)}`")
        progress.write(f"📄 Model: `{os.path.basename(semantic_dir)}`")

        progress.write("🔍 Reading PBI report files...")
        pbi_context = collect_pbi_context(report_dir, semantic_dir)
        pbi_source_tables = extract_pbi_source_tables(semantic_dir)
        data_sources = detect_external_sources(semantic_dir)
        n_visuals = pbi_context.count("### Visual:")
        physical_tables = [t for t in pbi_source_tables if t.get("table_type") == "physical"]
        calc_tables = [t for t in pbi_source_tables if t.get("table_type") == "calculated"]
        internal_tables = [t for t in pbi_source_tables if t.get("table_type") == "internal"]
        table_list = ", ".join(f"`{t['source_fqn']}`" for t in physical_tables)
        summary_parts = [f"**{len(physical_tables)} physical table(s)**"]
        if calc_tables:
            summary_parts.append(f"**{len(calc_tables)} calculated**")
        if internal_tables:
            summary_parts.append(f"{len(internal_tables)} local")
        progress.write(f"Collected {', '.join(summary_parts)} and **{n_visuals} visuals**: {table_list}")

        external_sources = [s for s in data_sources if not s["is_databricks"] and s["source_type"] != "Calculated (PBI)"]
        if external_sources:
            unique_types = sorted({s["source_type"] for s in external_sources})
            progress.write(f"⚠️ **{len(external_sources)} table(s) from external sources:** {', '.join(unique_types)}")

        progress.write("📐 Parsing PBI layout structure...")
        pbi_layout = parse_pbi_layout(report_dir)
        progress.write(
            f"Found **{pbi_layout.total_canvas_pages} page(s)**, "
            f"**{pbi_layout.total_data_visuals} data visual(s)**, "
            f"**{pbi_layout.total_page_slicers} page-level slicer(s)**, "
            f"**{pbi_layout.total_global_slicers} global slicer(s)**"
        )
        for pg in pbi_layout.pages:
            vis_types = [v.visual_type for v in pg.data_visuals]
            slicer_info = f", {len(pg.page_slicers)} page filter(s)" if pg.page_slicers else ""
            progress.write(f"  Page \"{pg.display_name}\": {', '.join(vis_types) or '(empty)'}{slicer_info}")

        if layout_mode == "Auto":
            free_layout = should_use_free_layout(pbi_layout)
        else:
            free_layout = layout_mode == "Free"

        if free_layout:
            layout_blueprint = build_free_layout_blueprint_prompt(pbi_layout)
            reason = "selected by user" if layout_mode == "Free" else "dense report detected"
            progress.write(f"🎨 Using **free layout mode** ({reason} — LLM decides positioning)")
        else:
            layout_blueprint = build_layout_blueprint_prompt(pbi_layout)
            if layout_mode == "Strict":
                progress.write("📐 Using **strict layout mode** (selected by user — positions from PBI coordinates)")

        # --- Phase 2: LLM Conversion ---
        est_tokens = _estimate_tokens(pbi_context + layout_blueprint)
        progress.write(f"📏 Estimated context size: **~{est_tokens:,} tokens** (after compression)")

        use_chunked = est_tokens > MAX_PROMPT_TOKENS
        if use_chunked:
            progress.write(
                f"⚡ Context exceeds single-call limit (~{MAX_PROMPT_TOKENS:,} tokens). "
                f"Switching to **multi-turn chunked mode** — pages will be sent in batches."
            )
            semantic_ctx, page_chunks = collect_pbi_context_chunked(report_dir, semantic_dir)
            progress.write(f"Split into **{len(page_chunks)} page chunk(s)**")
            raw_response = call_llm_chunked(
                report_name,
                semantic_ctx,
                page_chunks,
                layout_blueprint,
                progress_callback=lambda msg: progress.write(f"  🔄 {msg}"),
            )
        else:
            mode_label = "free layout" if free_layout else "layout blueprint"
            progress.write(f"🤖 Sending to **{MODEL}** for conversion...")
            raw_response = call_llm(report_name, pbi_context, layout_blueprint)

        progress.write("Received LLM response")

        progress.write("🔧 Parsing dashboard JSON...")
        dashboard_json = extract_json_from_response(raw_response)

        if not free_layout:
            progress.write("📐 Enforcing blueprint positions...")
            dashboard_json = apply_blueprint_positions(dashboard_json, pbi_layout)

        n_datasets = len(dashboard_json.get("datasets", []))
        n_pages = len(dashboard_json.get("pages", []))
        n_widgets = sum(len(p.get("layout", [])) for p in dashboard_json.get("pages", []))
        progress.write(f"Generated **{n_datasets} datasets**, **{n_pages} pages**, **{n_widgets} widgets**")

        # --- Phase 3: SQL column check & fix ---
        sp_client = WorkspaceClient()

        warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
        if not warehouse_id:
            for wh in sp_client.warehouses.list():
                warehouse_id = wh.id
                break
        if not warehouse_id:
            progress.update(label="Error", state="error")
            st.error("No SQL warehouse found. Please set DATABRICKS_WAREHOUSE_ID.")
            st.stop()

        progress.write("🔗 Ensuring fully-qualified table names (catalog.schema.table)...")
        dashboard_json = _ensure_fqn_tables(dashboard_json)

        progress.write("🔍 Checking dataset SQL against UC tables...")
        dashboard_json = fix_dataset_columns(dashboard_json, warehouse_id, sp_client)

        # --- Phase 4: Validation ---
        progress.write("🔍 Validating dashboard...")
        validation = validate_dashboard(dashboard_json, warehouse_id, sp_client)

        if validation.passed:
            progress.write("✅ **Structural validation passed**")
        else:
            progress.write(f"⚠️ **Structural validation found {len(validation.errors)} mismatches(s)** — see details below")

        progress.write("📐 Validating layout fidelity against PBI source...")
        layout_fidelity = validate_layout_fidelity(dashboard_json, pbi_layout)
        validation.layout_fidelity = layout_fidelity

        if layout_fidelity.passed:
            progress.write("✅ **Layout fidelity passed** — page count and visual coverage match")
        else:
            issues = []
            if not layout_fidelity.page_count_match:
                issues.append(
                    f"page count mismatch ({layout_fidelity.actual_pages} vs {layout_fidelity.expected_pages} expected)"
                )
            if layout_fidelity.missing_visuals:
                issues.append(f"{len(layout_fidelity.missing_visuals)} missing visual(s)")
            progress.write(f"⚠️ **Layout fidelity issues:** {', '.join(issues)}")

        if layout_fidelity.position_warnings:
            progress.write(f"ℹ️ {len(layout_fidelity.position_warnings)} widget(s) with position drift from PBI source")

        progress.write("📊 Validating table coverage...")
        table_coverage = validate_table_coverage(dashboard_json, pbi_source_tables)
        validation.table_coverage = table_coverage

        n_physical = len(table_coverage.queried_tables) + len(table_coverage.missing_tables)
        n_calc = len(table_coverage.calculated_tables)
        n_internal = len(table_coverage.internal_tables)

        if table_coverage.passed:
            progress.write(f"✅ **All {len(table_coverage.queried_tables)} physical table(s)** are queried in the dashboard")
        else:
            missing_names = ", ".join(f"`{t['source_fqn']}`" for t in table_coverage.missing_tables)
            progress.write(
                f"ℹ️ **{len(table_coverage.missing_tables)} physical table(s)** present in the semantic model but "
                f"not used by any visual in the report: {missing_names}"
            )
        if n_calc:
            progress.write(
                f"🔧 **{n_calc} calculated table(s)** translated from DAX to SQL (CTEs/subqueries)"
            )
        if n_internal:
            progress.write(
                f"⏭️ **{n_internal} PBI internal table(s)** skipped (auto-generated date tables)"
            )

        # --- Phase 4: Deploy ---
        progress.write("🚀 Deploying to Databricks workspace...")

        parent_path = f"/Workspace/Shared/aibi_converter/{report_name}"

        progress.write(f"Creating folder `{parent_path}`...")
        sp_client.workspace.mkdirs(parent_path)

        serialized = json.dumps(dashboard_json, indent=2)

        dashboard_obj = Dashboard(
            display_name=report_name,
            parent_path=parent_path,
            serialized_dashboard=serialized,
            warehouse_id=warehouse_id,
        )

        try:
            result = sp_client.lakeview.create(dashboard=dashboard_obj)
        except Exception as create_err:
            if "already exists" in str(create_err):
                progress.write("Dashboard already exists, searching for it to update...")
                existing_id = None
                for d in sp_client.lakeview.list():
                    if d.display_name == report_name:
                        existing_id = d.dashboard_id
                        break
                if not existing_id:
                    ws_path = f"{parent_path}/{report_name}.lvdash.json"
                    progress.write(f"Removing existing file at `{ws_path}`...")
                    try:
                        sp_client.workspace.delete(ws_path)
                    except Exception:
                        pass
                    result = sp_client.lakeview.create(dashboard=dashboard_obj)
                else:
                    progress.write(f"Updating existing dashboard `{existing_id}`...")
                    result = sp_client.lakeview.update(dashboard_id=existing_id, dashboard=dashboard_obj)
            else:
                raise

        dashboard_id = result.dashboard_id
        host = sp_client.config.host.rstrip("/")
        dash_url = f"{host}/sql/dashboardsv3/{dashboard_id}"
        workspace_path = f"{parent_path}/{report_name}.lvdash.json"
        progress.write(f"Dashboard created: `{workspace_path}`")

        progress.write("📢 Publishing dashboard...")
        sp_client.lakeview.publish(dashboard_id=dashboard_id, warehouse_id=warehouse_id)

        # --- Phase 5: Conversion Explanation ---
        progress.write("📝 Generating conversion report...")
        explanation = generate_explanation(report_name, pbi_context, dashboard_json)

        progress.update(label="Conversion complete!", state="complete")

        # --- Results Display ---
        st.divider()
        st.success("Dashboard converted and published successfully!")

        n_canvas = layout_fidelity.actual_pages
        n_filter_pages = n_pages - n_canvas
        page_match = "✅" if layout_fidelity.page_count_match else "❌"

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Datasets", n_datasets)
        col2.metric("Widgets", n_widgets)
        col3.metric("Canvas Pages", f"{n_canvas} {page_match}", delta=f"+ {n_filter_pages} filter page(s)" if n_filter_pages else None)
        col4.metric("PBI Tabs Matched", f"{layout_fidelity.expected_pages} → {n_canvas}")

        st.markdown(f"**Report:** {report_name}")
        st.markdown(f"**Model:** `{MODEL}`")
        st.markdown(f"**Workspace path:** `{workspace_path}`")
        st.markdown(f"**[Open Dashboard]({dash_url})**")

        # Conversion explanation (shown first)
        with st.expander("Conversion Report", expanded=False):
            st.markdown(explanation)

        # Tables & Data Sources (unified)
        has_external = bool(external_sources)
        section_title = "Tables & Data Sources" if not has_external else "Tables, Data Sources & Migration"
        with st.expander(section_title, expanded=False):
            tc = validation.table_coverage
            ds_lookup = {s["pbi_table"]: s for s in data_sources} if data_sources else {}

            if tc:
                n_physical = len(tc.queried_tables) + len(tc.missing_tables)
                n_calc = len(tc.calculated_tables)
                n_internal = len(tc.internal_tables)

                summary_parts = []
                if tc.passed and n_physical > 0:
                    summary_parts.append(f"All **{len(tc.queried_tables)} physical table(s)** queried in the dashboard")
                elif tc.missing_tables:
                    summary_parts.append(
                        f"{len(tc.queried_tables)} of {n_physical} physical table(s) queried"
                    )
                if n_calc:
                    summary_parts.append(
                        f"**{n_calc} calculated** (DAX → SQL)"
                    )
                if n_internal:
                    summary_parts.append(f"{n_internal} local PBI table(s)")

                if has_external:
                    unique_types = sorted({s["source_type"] for s in external_sources})
                    st.warning(
                        f"**{len(external_sources)} table(s)** come from external sources "
                        f"({', '.join(unique_types)}). "
                        "These need to be accessible from your Databricks workspace."
                    )
                elif summary_parts:
                    st.success(". ".join(summary_parts) + ". No migration needed.")

                table_rows = []
                for tbl in tc.queried_tables:
                    ds_info = ds_lookup.get(tbl["pbi_table"], {})
                    table_rows.append({
                        "Status": "✅ Queried",
                        "PBI Table": tbl["pbi_table"],
                        "Source": ds_info.get("source_type", "Databricks"),
                        "Connection": ds_info.get("connector_detail") or tbl["source_fqn"],
                    })
                for tbl in tc.missing_tables:
                    ds_info = ds_lookup.get(tbl["pbi_table"], {})
                    table_rows.append({
                        "Status": "ℹ️ Unused",
                        "PBI Table": tbl["pbi_table"],
                        "Source": ds_info.get("source_type", "Databricks"),
                        "Connection": ds_info.get("connector_detail") or tbl["source_fqn"],
                    })
                for tbl in tc.calculated_tables:
                    table_rows.append({
                        "Status": "🔧 Calculated",
                        "PBI Table": tbl["pbi_table"],
                        "Source": "DAX → SQL (CTE)",
                        "Connection": "—",
                    })
                for tbl in tc.internal_tables:
                    table_rows.append({
                        "Status": "⏭️ Local",
                        "PBI Table": tbl["pbi_table"],
                        "Source": "PBI auto-generated",
                        "Connection": "—",
                    })

                if table_rows:
                    import pandas as pd
                    df = pd.DataFrame(table_rows)
                    st.dataframe(
                        df,
                        hide_index=True,
                        use_container_width=True,
                        column_config={
                            "Status": st.column_config.TextColumn(width="small"),
                            "PBI Table": st.column_config.TextColumn(width="medium"),
                            "Source": st.column_config.TextColumn(width="medium"),
                            "Connection": st.column_config.TextColumn(width="large"),
                        },
                    )

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

        # Validation summary
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

            # --- Layout Fidelity ---
            lf = validation.layout_fidelity
            if lf:
                st.markdown("#### Layout Fidelity (PBI → AI/BI)")

                if lf.page_count_match:
                    st.markdown(
                        f"- ✅ **Page count:** {lf.actual_pages} canvas page(s) — "
                        f"matches PBI source ({lf.expected_pages} tab(s))"
                    )
                else:
                    st.markdown(
                        f"- ❌ **Page count mismatch:** expected {lf.expected_pages} canvas page(s) "
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
                    icon = "✅" if match else "⚠️"
                    st.markdown(
                        f"- {icon} **Page \"{entry['name']}\":** {entry['actual']} data widget(s) "
                        f"(expected {entry['expected']} from PBI)"
                    )

                if lf.missing_visuals:
                    st.markdown(f"- ❌ **{len(lf.missing_visuals)} PBI visual(s) not found in dashboard:**")
                    for mv in lf.missing_visuals:
                        st.markdown(f"  - `{mv['visual_type']}` — {mv['description']} (page: {mv['page']})")
                else:
                    st.markdown("- ✅ **All PBI visuals** are represented in the dashboard")

                if lf.position_warnings:
                    st.markdown(f"- ℹ️ **{len(lf.position_warnings)} widget(s) with position drift:**")
                    for pw in lf.position_warnings:
                        st.markdown(
                            f"  - `{pw['visual_type']}` ({pw['description']}): "
                            f"expected x={pw['expected_x']}, w={pw['expected_w']} → "
                            f"got x={pw['actual_x']}, w={pw['actual_w']}"
                        )
                else:
                    st.markdown("- ✅ **Widget positions** approximate PBI layout")

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
                        ver_status = "✅" if expected is None or ver == expected else f"❌ (expected {expected})"
                        st.markdown(f"- `{w_name}` — **{wt}** v{ver} {ver_status} — {pos_str}")

            if validation.sql_results:
                st.markdown("#### SQL Query Validation")
                for ds_name, succeeded, error_msg, cols in validation.sql_results:
                    if succeeded:
                        st.markdown(f"- ✅ `{ds_name}` — query OK, {len(cols)} columns returned: `{'`, `'.join(cols[:15])}`")
                    else:
                        st.markdown(f"- ❌ `{ds_name}` — {error_msg}")

            field_issues = [e for e in validation.errors if "fieldName" in e or "query fields" in e]
            dataset_issues = [e for e in validation.errors if "references dataset" in e]
            other_errors = [e for e in validation.errors if e not in field_issues and e not in dataset_issues and "SQL" not in e]

            if field_issues or dataset_issues or other_errors:
                st.markdown("#### Structural Errors")
                for err in field_issues + dataset_issues + other_errors:
                    st.markdown(f"- ❌ {err}")
            else:
                st.markdown("#### Structural Checks")
                st.markdown("- ✅ All widget versions are correct")
                st.markdown("- ✅ All encoding fieldNames match query field names")
                st.markdown("- ✅ All dataset references are valid")
                st.markdown("- ✅ All widget positions are within the 6-column grid")

            if validation.warnings:
                st.markdown("#### Warnings")
                for warn in validation.warnings:
                    st.markdown(f"- ⚠️ {warn}")

    except json.JSONDecodeError as e:
        progress.update(label="Error", state="error")
        st.error(f"LLM returned invalid JSON: {e}")
        with st.expander("Raw LLM response"):
            st.code(raw_response[:3000], language="text")

    except Exception as e:
        progress.update(label="Error", state="error")
        st.error(f"Conversion failed: {e}")
        with st.expander("Full traceback"):
            st.code(traceback.format_exc(), language="text")
