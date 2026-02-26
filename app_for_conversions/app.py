"""
Power BI to Databricks AI/BI Dashboard Converter — Streamlit App

Deployed as a Databricks App. Accepts a zipped .pbip project, sends the parsed
report structure to an LLM endpoint, and publishes the resulting AI/BI
dashboard to the workspace via the Lakeview API.

Authentication flow:
  - User identity: forwarded OAuth token from Databricks Apps proxy
    (X-Forwarded-Access-Token header) — used only for identifying the user.
  - Dashboard operations: service principal credentials injected by the
    Databricks Apps runtime (DATABRICKS_CLIENT_ID / CLIENT_SECRET env vars)
    — used for workspace.mkdirs, lakeview.create, and lakeview.publish
    because the forwarded user token lacks the required 'dashboards' scope.
"""

import os
import json
import glob
import zipfile
import tempfile
import traceback
from pathlib import Path
from dataclasses import dataclass, field

import streamlit as st
from openai import OpenAI
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.service.dashboards import Dashboard

st.set_page_config(page_title="PBI to AI/BI Converter", page_icon=":bar_chart:", layout="centered")

MODEL = os.getenv("LLM_MODEL", "databricks-claude-opus-4-6")
KNOWLEDGE_DIR = Path(__file__).parent / "knowledge"

VALID_WIDGET_VERSIONS = {
    "counter": 2,
    "table": 2,
    "filter-multi-select": 2,
    "filter-single-select": 2,
    "filter-date-range-picker": 2,
    "bar": 3,
    "line": 3,
    "pie": 3,
    "area": 3,
    "pivot": 3,
    "scatter": 3,
}

GRID_COLUMNS = 6


def _load_knowledge_file(filename: str) -> str:
    """Read a knowledge document from the knowledge/ directory."""
    path = KNOWLEDGE_DIR / filename
    if path.is_file():
        return path.read_text(encoding="utf-8")
    return ""


@st.cache_data
def build_system_prompt() -> str:
    """Assemble the LLM system prompt from knowledge documents.

    Loads CONVERSION_GUIDE.md (PBI-to-AIBI mapping rules) and
    AIBI_DASHBOARD_SKILL.md (full .lvdash.json spec) so the LLM has
    comprehensive reference material for the conversion.
    """
    conversion_guide = _load_knowledge_file("CONVERSION_GUIDE.md")
    aibi_skill = _load_knowledge_file("AIBI_DASHBOARD_SKILL.md")

    return f"""You are an expert at converting Power BI reports to Databricks AI/BI dashboards.

You will receive the full contents of a Power BI project (.pbip): table definitions (.tmdl), relationships, and visual definitions (visual.json). Your job is to produce a valid .lvdash.json dashboard definition.

Below are two comprehensive reference documents you MUST follow exactly. They contain the conversion rules, widget specifications, layout guidelines, and common pitfalls.

---

# REFERENCE 1: CONVERSION GUIDE (Power BI → AI/BI)

{conversion_guide}

---

# REFERENCE 2: AI/BI DASHBOARD SPECIFICATION

{aibi_skill}

---

# ADDITIONAL CRITICAL REMINDERS

1. **Field name matching**: The `name` in query.fields MUST exactly match the `fieldName` in encodings. This is the #1 cause of broken widgets.
2. **Widget versions**: counter=2, table=2, filters=2, bar/line/pie=3. Wrong versions cause "Invalid widget definition".
3. **Text widgets**: Use `multilineTextboxSpec` directly on the widget — NO `spec` block. Use SEPARATE widgets for title and subtitle.
4. **Filter widgets**: Must use `filter-multi-select`, `filter-single-select`, or `filter-date-range-picker`. NEVER use `widgetType: "filter"`.
5. **Layout**: 6-column grid, every row must sum to width=6 with no gaps.
6. **SQL**: Use Spark SQL syntax. Use `date_sub()` not `DATEADD()`. Use fully-qualified table names: `catalog.schema.table`.
7. **Disaggregated flag**: Use `false` for aggregating widgets (counters with SUM/AVG, charts), `true` for pre-aggregated single-row datasets.

## OUTPUT FORMAT
Return ONLY a valid JSON object — the .lvdash.json content. No markdown fences, no explanation, just the JSON."""


SYSTEM_PROMPT = build_system_prompt()


# ---------------------------------------------------------------------------
# Client Factories
# ---------------------------------------------------------------------------


def get_workspace_client() -> WorkspaceClient:
    """Return a WorkspaceClient authenticated as the current user.

    In a Databricks App the proxy injects an X-Forwarded-Access-Token header
    with the user's OAuth token. We temporarily hide the SP env vars so the
    SDK doesn't raise "more than one authorization method configured".
    """
    headers = st.context.headers
    token = headers.get("X-Forwarded-Access-Token")
    if token:
        host = os.getenv("DATABRICKS_HOST", Config().host)
        saved = {}
        for key in ("DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET"):
            if key in os.environ:
                saved[key] = os.environ.pop(key)
        try:
            return WorkspaceClient(host=host, token=token)
        finally:
            os.environ.update(saved)
    return WorkspaceClient()


def get_llm_client() -> OpenAI:
    """Return an OpenAI-compatible client pointed at the Databricks Model Serving endpoint."""
    cfg = Config()
    host = cfg.host.rstrip("/")
    token = os.getenv("DATABRICKS_TOKEN") or cfg.authenticate().get("Authorization", "").replace("Bearer ", "")

    if not token:
        cfg_obj = cfg.authenticate()
        if isinstance(cfg_obj, dict):
            token = cfg_obj.get("Authorization", "").replace("Bearer ", "")
        else:
            token = cfg_obj

    return OpenAI(
        base_url=f"{host}/serving-endpoints",
        api_key=token,
    )


# ---------------------------------------------------------------------------
# File Extraction & Parsing
# ---------------------------------------------------------------------------


def extract_upload(uploaded_file) -> str:
    """Save the uploaded file to a temp directory and extract if it's a zip."""
    tmpdir = tempfile.mkdtemp(prefix="pbi_upload_")
    file_path = os.path.join(tmpdir, uploaded_file.name)
    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    if file_path.endswith(".zip"):
        with zipfile.ZipFile(file_path, "r") as zf:
            zf.extractall(tmpdir)
    return tmpdir


def find_report_root(base_dir: str) -> str:
    """Walk the extracted directory tree to find the folder containing the .pbip file."""
    for root, dirs, fnames in os.walk(base_dir):
        for fn in fnames:
            if fn.endswith(".pbip"):
                return root
        for d in dirs:
            if d.endswith(".Report"):
                return root
    return base_dir


def collect_pbi_context(report_dir: str, semantic_model_dir: str) -> str:
    """Collect all PBI artifacts into a single text block for the LLM.

    Reads .tmdl table definitions, relationships, model metadata, page configs,
    and visual.json files — everything the LLM needs to understand the report.
    """
    sections = []

    # Semantic model: table definitions (.tmdl)
    tables_dir = os.path.join(semantic_model_dir, "definition", "tables")
    if os.path.isdir(tables_dir):
        for tmdl_file in sorted(glob.glob(os.path.join(tables_dir, "*.tmdl"))):
            name = os.path.basename(tmdl_file)
            with open(tmdl_file, "r") as f:
                content = f.read()
            sections.append(f"### Table: {name}\n```\n{content}\n```")

    # Semantic model: relationships between tables
    rel_file = os.path.join(semantic_model_dir, "definition", "relationships.tmdl")
    if os.path.isfile(rel_file):
        with open(rel_file, "r") as f:
            content = f.read()
        sections.append(f"### Relationships\n```\n{content}\n```")

    # Semantic model: top-level model metadata
    model_file = os.path.join(semantic_model_dir, "definition", "model.tmdl")
    if os.path.isfile(model_file):
        with open(model_file, "r") as f:
            content = f.read()
        sections.append(f"### Model\n```\n{content}\n```")

    # Report: pages and visuals
    pages_dir = os.path.join(report_dir, "definition", "pages")
    if os.path.isdir(pages_dir):
        pages_json = os.path.join(pages_dir, "pages.json")
        if os.path.isfile(pages_json):
            with open(pages_json, "r") as f:
                sections.append(f"### Pages Metadata\n```json\n{f.read()}\n```")

        for page_dir in sorted(glob.glob(os.path.join(pages_dir, "*"))):
            if not os.path.isdir(page_dir):
                continue
            page_json = os.path.join(page_dir, "page.json")
            if os.path.isfile(page_json):
                with open(page_json, "r") as f:
                    sections.append(f"### Page: {os.path.basename(page_dir)}\n```json\n{f.read()}\n```")

            for vis_path in sorted(glob.glob(os.path.join(page_dir, "visuals", "*", "visual.json"))):
                vis_id = os.path.basename(os.path.dirname(vis_path))
                with open(vis_path, "r") as f:
                    vis_content = f.read()
                sections.append(f"### Visual: {vis_id}\n```json\n{vis_content}\n```")

    return "\n\n".join(sections)


# ---------------------------------------------------------------------------
# LLM Interaction
# ---------------------------------------------------------------------------


def call_llm(report_name: str, pbi_context: str) -> str:
    """Send the PBI context to the LLM and return the raw response text."""
    client = get_llm_client()

    user_message = f"""Convert this Power BI report named "{report_name}" to a Databricks AI/BI dashboard (.lvdash.json).

## Power BI Report Contents

{pbi_context}

## Instructions

1. Extract the data source catalog/schema/table from the .tmdl partition blocks
2. Build SQL dataset(s) that JOIN the needed tables using fully-qualified names
3. Convert every visual to the appropriate AI/BI widget type
4. Convert slicers to global filter widgets on a PAGE_TYPE_GLOBAL_FILTERS page
5. Skip decorative shapes
6. Use proper 6-column grid layout with no gaps
7. Ensure all field names in query.fields match fieldNames in encodings exactly

Return ONLY the JSON — no markdown fences, no explanation."""

    response = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ],
        max_tokens=16384,
        temperature=0,
    )

    return response.choices[0].message.content


def generate_explanation(report_name: str, pbi_context: str, dashboard_json: dict) -> str:
    """Ask the LLM to produce a human-readable conversion report.

    Takes the original PBI context and the generated dashboard JSON and
    returns a markdown explanation of what was identified and how each
    element was mapped.
    """
    client = get_llm_client()

    serialized = json.dumps(dashboard_json, indent=2)
    # Truncate the dashboard JSON to avoid exceeding token limits on the explanation call
    if len(serialized) > 12000:
        serialized = serialized[:12000] + "\n... (truncated)"

    user_message = f"""I just converted a Power BI report named "{report_name}" to a Databricks AI/BI dashboard. Below are the original PBI contents and the resulting dashboard JSON.

Write a concise conversion report in markdown. Include:

1. **Source Summary** — tables, relationships, and pages found in the PBI report
2. **Visual Mapping** — for each PBI visual, state the original type and what AI/BI widget it was converted to (use a table)
3. **Data Sources** — list the catalog.schema.table references used in the SQL datasets
4. **Filters** — which PBI slicers were converted to AI/BI filters and their type
5. **Decisions & Trade-offs** — anything that was skipped (e.g. decorative shapes), approximated (e.g. unsupported chart types), or changed (e.g. DAX → SQL translations)
6. **Potential Issues** — any areas where the conversion might need manual review

Keep it under 500 words. Use markdown headers and tables for clarity.

## Original PBI Report
{pbi_context[:8000]}

## Generated Dashboard JSON
{serialized}"""

    response = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": "You are a technical writer that produces clear, concise conversion reports."},
            {"role": "user", "content": user_message},
        ],
        max_tokens=4096,
        temperature=0,
    )

    return response.choices[0].message.content


def extract_json_from_response(text: str) -> dict:
    """Extract a JSON object from the LLM response, stripping markdown fences if present."""
    text = text.strip()

    if text.startswith("```"):
        lines = text.split("\n")
        start = 1
        end = len(lines) - 1 if lines[-1].strip() == "```" else len(lines)
        text = "\n".join(lines[start:end])

    brace_start = text.find("{")
    brace_end = text.rfind("}")
    if brace_start != -1 and brace_end != -1:
        text = text[brace_start:brace_end + 1]

    return json.loads(text)


def find_pbi_folders(tmpdir: str):
    """Locate the .Report and .SemanticModel folders in the extracted upload."""
    root = find_report_root(tmpdir)
    report_dir = None
    semantic_dir = None

    for item in os.listdir(root):
        full = os.path.join(root, item)
        if item.endswith(".Report") and os.path.isdir(full):
            report_dir = full
        elif item.endswith(".SemanticModel") and os.path.isdir(full):
            semantic_dir = full

    if not report_dir or not semantic_dir:
        for r, dirs, _ in os.walk(tmpdir):
            for d in dirs:
                if d.endswith(".Report"):
                    report_dir = report_dir or os.path.join(r, d)
                elif d.endswith(".SemanticModel"):
                    semantic_dir = semantic_dir or os.path.join(r, d)

    return report_dir, semantic_dir


# ---------------------------------------------------------------------------
# Dashboard Validation
# ---------------------------------------------------------------------------


@dataclass
class ValidationResult:
    """Collects validation errors and warnings for a dashboard JSON."""
    errors: list = field(default_factory=list)
    warnings: list = field(default_factory=list)
    sql_results: list = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return len(self.errors) == 0

    @property
    def total_issues(self) -> int:
        return len(self.errors) + len(self.warnings)


def _get_dataset_sql(ds: dict) -> str:
    """Extract the SQL string from a dataset, handling both queryLines (array) and query (string) formats."""
    if "queryLines" in ds:
        return " ".join(ds["queryLines"])
    return ds.get("query", "")


def validate_dashboard(dashboard_json: dict, warehouse_id: str, sp_client: WorkspaceClient) -> ValidationResult:
    """Validate the generated .lvdash.json for structural correctness and SQL validity.

    Structural checks:
      - Required top-level keys (datasets, pages)
      - Every dataset has a non-empty SQL query
      - Widget versions match the spec (counter=2, bar/line/pie=3, etc.)
      - Field names in widget.queries[].query.fields match spec.encodings fieldNames
      - Every widget's datasetName references an existing dataset
      - Layout grid: widget widths are within 1-6
      - Text widgets use multilineTextboxSpec without a spec block

    SQL checks:
      - Execute each dataset query with LIMIT 1 against the warehouse
    """
    result = ValidationResult()
    datasets = dashboard_json.get("datasets", [])
    pages = dashboard_json.get("pages", [])

    if not datasets:
        result.errors.append("Missing `datasets` — dashboard has no data sources.")
    if not pages:
        result.errors.append("Missing `pages` — dashboard has no pages.")

    # --- Dataset validation + SQL execution ---
    dataset_names = set()
    for ds in datasets:
        ds_name = ds.get("name", "<unnamed>")
        dataset_names.add(ds_name)

        if not ds.get("displayName"):
            result.warnings.append(f"Dataset `{ds_name}`: missing `displayName`.")

        query_str = _get_dataset_sql(ds)
        if not query_str.strip():
            result.errors.append(f"Dataset `{ds_name}`: empty SQL query.")
            result.sql_results.append((ds_name, False, "Empty query", []))
            continue

        try:
            from databricks.sdk.service.sql import StatementState
            stmt = sp_client.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=f"SELECT * FROM ({query_str}) AS _t LIMIT 1",
                wait_timeout="30s",
            )
            if stmt.status and stmt.status.state == StatementState.SUCCEEDED:
                cols = [c.name for c in (stmt.manifest.schema.columns or [])] if stmt.manifest and stmt.manifest.schema else []
                result.sql_results.append((ds_name, True, None, cols))
            else:
                error_msg = stmt.status.error.message if stmt.status and stmt.status.error else "Unknown error"
                result.errors.append(f"Dataset `{ds_name}`: SQL query failed — {error_msg}")
                result.sql_results.append((ds_name, False, error_msg, []))
        except Exception as e:
            result.errors.append(f"Dataset `{ds_name}`: SQL execution error — {e}")
            result.sql_results.append((ds_name, False, str(e), []))

    # --- Page & widget validation ---
    for page in pages:
        page_name = page.get("displayName", page.get("name", "<unnamed>"))
        page_type = page.get("pageType", "")
        layout = page.get("layout", [])

        if not page_type:
            result.warnings.append(f"Page `{page_name}`: missing `pageType`.")

        for item in layout:
            widget = item.get("widget", {})
            position = item.get("position", {})
            w_name = widget.get("name", "<unnamed>")

            # --- Text widgets ---
            if "multilineTextboxSpec" in widget:
                if "spec" in widget:
                    result.warnings.append(
                        f"Text widget `{w_name}` on `{page_name}`: has both `multilineTextboxSpec` and `spec` — remove `spec`."
                    )
                lines = widget["multilineTextboxSpec"].get("lines", [])
                if not lines:
                    result.warnings.append(f"Text widget `{w_name}` on `{page_name}`: empty `lines` array.")
                continue

            # --- Data widgets ---
            spec = widget.get("spec", {})
            widget_queries = widget.get("queries", [])

            if not spec and not widget_queries:
                result.warnings.append(f"Widget `{w_name}` on `{page_name}`: has no `spec` and no `queries`.")
                continue

            widget_type = spec.get("widgetType", "")
            version = spec.get("version", None)

            # Version check
            if widget_type in VALID_WIDGET_VERSIONS:
                expected = VALID_WIDGET_VERSIONS[widget_type]
                if version != expected:
                    result.errors.append(
                        f"Widget `{w_name}` on `{page_name}`: `{widget_type}` requires version {expected}, found {version}."
                    )
            elif widget_type and widget_type not in VALID_WIDGET_VERSIONS:
                result.warnings.append(
                    f"Widget `{w_name}` on `{page_name}`: unrecognized widgetType `{widget_type}`."
                )

            # Collect field names from widget.queries[].query.fields
            query_field_names = set()
            referenced_datasets = set()
            for wq in widget_queries:
                query_obj = wq.get("query", {})
                ds_ref = query_obj.get("datasetName", "")
                if ds_ref:
                    referenced_datasets.add(ds_ref)
                for f in query_obj.get("fields", []):
                    fname = f.get("name", "")
                    if fname:
                        query_field_names.add(fname)

            # Dataset reference check
            for ds_ref in referenced_datasets:
                if ds_ref not in dataset_names:
                    result.errors.append(
                        f"Widget `{w_name}` on `{page_name}`: references dataset `{ds_ref}` which doesn't exist."
                    )

            # Field name matching: encoding fieldNames must exist in query.fields
            encodings = spec.get("encodings", {})
            encoding_field_names = set()
            for enc_key, enc_val in encodings.items():
                if isinstance(enc_val, list):
                    for enc_item in enc_val:
                        if isinstance(enc_item, dict):
                            fn = enc_item.get("fieldName", "")
                            if fn:
                                encoding_field_names.add(fn)
                elif isinstance(enc_val, dict):
                    fn = enc_val.get("fieldName", "")
                    if fn:
                        encoding_field_names.add(fn)

            if query_field_names and encoding_field_names:
                unmatched = encoding_field_names - query_field_names
                if unmatched:
                    result.errors.append(
                        f"Widget `{w_name}` on `{page_name}`: encoding fieldName(s) {unmatched} not found in query fields {query_field_names}."
                    )

            # Grid layout check
            w = position.get("width", 0)
            if w < 1 or w > GRID_COLUMNS:
                result.warnings.append(
                    f"Widget `{w_name}` on `{page_name}`: width={w} is outside the 1–{GRID_COLUMNS} range."
                )

    return result


# ---------------------------------------------------------------------------
# Streamlit UI
# ---------------------------------------------------------------------------

STATIC_DIR = Path(__file__).parent / "static"

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

convert_clicked = st.button("Convert & Publish", type="primary", use_container_width=True)

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
        n_visuals = pbi_context.count("### Visual:")
        n_tables = pbi_context.count("### Table:")
        progress.write(f"Collected **{n_tables} tables** and **{n_visuals} visuals**")

        # --- Phase 2: LLM Conversion ---
        progress.write(f"🤖 Sending to **{MODEL}** for conversion...")
        raw_response = call_llm(report_name, pbi_context)
        progress.write("Received LLM response")

        progress.write("🔧 Parsing dashboard JSON...")
        dashboard_json = extract_json_from_response(raw_response)

        n_datasets = len(dashboard_json.get("datasets", []))
        n_pages = len(dashboard_json.get("pages", []))
        n_widgets = sum(len(p.get("layout", [])) for p in dashboard_json.get("pages", []))
        progress.write(f"Generated **{n_datasets} datasets**, **{n_pages} pages**, **{n_widgets} widgets**")

        # --- Phase 3: Validation ---
        progress.write("🔍 Validating dashboard...")
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

        validation = validate_dashboard(dashboard_json, warehouse_id, sp_client)

        if validation.passed:
            progress.write("✅ **Validation passed**")
        else:
            progress.write(f"⚠️ **Validation found {len(validation.errors)} error(s)** — see details below")

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
                    # Fallback: delete the workspace node and recreate
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

        col1, col2, col3 = st.columns(3)
        col1.metric("Datasets", n_datasets)
        col2.metric("Widgets", n_widgets)
        col3.metric("Pages", n_pages)

        st.markdown(f"**Report:** {report_name}")
        st.markdown(f"**Model:** `{MODEL}`")
        st.markdown(f"**Workspace path:** `{workspace_path}`")
        st.markdown(f"**[Open Dashboard]({dash_url})**")

        # Conversion explanation (shown first)
        with st.expander("Conversion Report", expanded=False):
            st.markdown(explanation)

        # Validation summary
        with st.expander("Validation Results", expanded=False):
            if validation.passed and not validation.warnings:
                st.success("All checks passed — no errors or warnings.")
            elif validation.passed:
                st.info(f"No errors, but {len(validation.warnings)} warning(s) found.")
            else:
                st.warning(f"{len(validation.errors)} error(s) and {len(validation.warnings)} warning(s) found.")

            # Dashboard structure
            st.markdown("#### Dashboard Structure")
            st.markdown(f"- **Datasets:** {n_datasets}")
            st.markdown(f"- **Pages:** {n_pages}")
            st.markdown(f"- **Widgets:** {n_widgets}")

            # Widget inventory
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

            # SQL query validation
            if validation.sql_results:
                st.markdown("#### SQL Query Validation")
                for ds_name, succeeded, error_msg, cols in validation.sql_results:
                    if succeeded:
                        st.markdown(f"- ✅ `{ds_name}` — query OK, {len(cols)} columns returned: `{'`, `'.join(cols[:15])}`")
                    else:
                        st.markdown(f"- ❌ `{ds_name}` — {error_msg}")

            # Field name consistency
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
