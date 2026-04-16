"""
PBI file parsing, LLM-driven conversion, and response extraction.

Handles the full pipeline from uploaded zip to dashboard JSON:
  1. Extract and locate .Report / .SemanticModel folders
  2. Collect all PBI artifacts into a text context for the LLM
  3. Parse the PBI layout into a structured blueprint (pages, visuals, grid positions)
  4. Call the LLM to produce a .lvdash.json with the blueprint as explicit guidance
  5. Generate a human-readable conversion explanation
"""

import os
import json
import glob
import zipfile
import tempfile
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Optional

from clients import MODEL, KNOWLEDGE_DIR, GRID_COLUMNS, get_llm_client


def _load_knowledge_file(filename: str) -> str:
    """Read a knowledge document from the knowledge/ directory."""
    path = KNOWLEDGE_DIR / filename
    if path.is_file():
        return path.read_text(encoding="utf-8")
    return ""


@lru_cache(maxsize=1)
def build_system_prompt() -> str:
    """Assemble the LLM system prompt from knowledge documents.

    Loads CONVERSION_GUIDE.md (PBI-to-AIBI mapping rules) and
    AIBI_DASHBOARD_SKILL.md (full .lvdash.json spec) so the LLM has
    comprehensive reference material for the conversion.
    """
    conversion_guide = _load_knowledge_file("CONVERSION_GUIDE.md")
    aibi_skill = _load_knowledge_file("AIBI_DASHBOARD_SKILL.md")
    dax_guide = _load_knowledge_file("DAX_TO_SQL_GUIDE.md")

    return f"""You are an expert at converting Power BI reports to Databricks AI/BI dashboards.

You will receive the full contents of a Power BI project (.pbip): table definitions (.tmdl), relationships, and visual definitions (visual.json). Your job is to produce a valid .lvdash.json dashboard definition.

Below are three comprehensive reference documents you MUST follow exactly. They contain the conversion rules, widget specifications, layout guidelines, DAX translation patterns, and common pitfalls.

---

# REFERENCE 1: CONVERSION GUIDE (Power BI → AI/BI)

{conversion_guide}

---

# REFERENCE 2: AI/BI DASHBOARD SPECIFICATION

{aibi_skill}

---

# REFERENCE 3: DAX TO SQL TRANSLATION GUIDE

{dax_guide}

---

# ADDITIONAL CRITICAL REMINDERS

1. **Field name matching**: The `name` in query.fields MUST exactly match the `fieldName` in encodings. This is the #1 cause of broken widgets.
2. **Widget versions**: counter=2, table=2, filters=2, bar/line/pie=3. Wrong versions cause "Invalid widget definition".
3. **Text widgets**: ONLY create text widgets if the PBI report contains explicit `textbox` visuals with real text. NEVER create blank, empty, or placeholder text widgets. NEVER invent headers, titles, or spacers that don't exist in the PBI source.
4. **Filter widgets**: Must use `filter-multi-select`, `filter-single-select`, or `filter-date-range-picker`. NEVER use `widgetType: "filter"`. Place them on the same canvas page as the visuals they filter (page-level), unless explicitly told to use a global filters page.
5. **Layout**: 6-column grid, every row must sum to width=6. Use column-skyline packing — each widget's y is determined by the columns it occupies, so short widgets stack tightly next to taller ones with zero blank space.
6. **SQL**: Use Spark SQL syntax. Use `date_sub()` not `DATEADD()`. Use fully-qualified table names: `catalog.schema.table`.
7. **Disaggregated flag**: Use `false` for aggregating widgets (counters with SUM/AVG, charts), `true` for pre-aggregated single-row datasets or raw-row table widgets.
8. **Simple aggregates in dataset SQL are OK**: Simple aggregations like `SUM(col)`, `COUNT(col)`, `AVG(col)` in dataset SQL will be automatically promoted to widget custom calculations by the post-processor. You may use GROUP BY with simple aggregates freely.
9. **Complex expressions stay in dataset SQL**: For complex DAX translations (CALCULATE → `CASE WHEN` inside aggregates, DIVIDE → `SUM(a) / NULLIF(SUM(b), 0)`, IF/SWITCH), keep them as derived columns in dataset SQL with GROUP BY. AIBI widget expressions only support simple `SUM(\`col\`)` / `COUNT(\`col\`)` / `AVG(\`col\`)` / `MIN(\`col\`)` / `MAX(\`col\`)` — NOT `CASE WHEN`, `NULLIF`, or arithmetic.
10. **Calculated tables**: PBI tables with `partition = calculated` have NO external data source — their data is defined by a DAX expression. Translate the DAX table expression to an equivalent SQL subquery or CTE (e.g. `DISTINCT(SELECTCOLUMNS(...))` → `SELECT DISTINCT ...`, `DATATABLE(...)` → `UNION ALL` values). Use these as CTEs in datasets that feed the widgets referencing those tables. See REFERENCE 3, section 16.
11. **Color preservation**: When a COLOR PALETTE section is provided, preserve the PBI report's colors in chart widgets. For charts with a categorical `color` encoding, include a `scale.range` array with hex colors from the PBI palette. Example:
    ```json
    "color": {{"fieldName": "product", "scale": {{"type": "categorical", "range": ["#118DFF", "#12239E", "#E66C37"]}}, "displayName": "Product"}}
    ```
    If specific category-to-color mappings are given, also include `scale.domain` with the category values in the same order as the `scale.range` colors.

## OUTPUT FORMAT
Return ONLY a valid JSON object — the .lvdash.json content. No markdown fences, no explanation, just the JSON."""


def _get_system_prompt() -> str:
    return build_system_prompt()


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


def _parse_tmdl_table_name(content: str, filename: str) -> str:
    """Extract the table name from the first line of a .tmdl file.

    Handles both unquoted names (``table foo``) and single-quoted names
    (``table 'My Table Name'``).
    """
    import re
    m = re.match(r"^table\s+'([^']+)'", content)
    if m:
        return m.group(1)
    m = re.match(r"^table\s+(\S+)", content)
    if m:
        return m.group(1)
    return os.path.splitext(os.path.basename(filename))[0]


# Table names that PBI auto-generates; these are not real data sources.
_PBI_INTERNAL_TABLE_PREFIXES = ("LocalDateTable_", "DateTableTemplate_")


def extract_pbi_source_tables(semantic_model_dir: str) -> list[dict]:
    """Parse .tmdl files and extract fully-qualified source table references.

    Each .tmdl partition block contains M-expression code like:
        Source{[Name="samples",Kind="Database"]}[Data],
        bakehouse_Schema = ...{[Name="bakehouse",Kind="Schema"]}[Data],
        sales_transactions_Table = ...{[Name="sales_transactions",Kind="Table"]}[Data]

    Returns a list of dicts with keys: pbi_table, source_fqn, table_type.
    table_type is one of: "physical", "calculated", "internal".
    """
    import re

    tables_dir = os.path.join(semantic_model_dir, "definition", "tables")
    results = []

    if not os.path.isdir(tables_dir):
        return results

    for tmdl_file in sorted(glob.glob(os.path.join(tables_dir, "*.tmdl"))):
        with open(tmdl_file, "r") as f:
            content = f.read()

        pbi_table = _parse_tmdl_table_name(content, tmdl_file)

        if any(pbi_table.startswith(p) for p in _PBI_INTERNAL_TABLE_PREFIXES):
            results.append({
                "pbi_table": pbi_table,
                "source_fqn": pbi_table,
                "table_type": "internal",
            })
            continue

        partition_block = ""
        for line in content.splitlines():
            if line.strip().startswith("partition "):
                partition_block += line + "\n"
                continue
            if partition_block:
                partition_block += line + "\n"

        if not partition_block or "= calculated" in partition_block:
            results.append({
                "pbi_table": pbi_table,
                "source_fqn": pbi_table,
                "table_type": "calculated",
            })
            continue

        catalog = schema = table = None
        for line in content.splitlines():
            db_match = re.search(r'\[Name="([^"]+)",\s*Kind="Database"\]', line)
            if db_match:
                catalog = db_match.group(1)
            sc_match = re.search(r'\[Name="([^"]+)",\s*Kind="Schema"\]', line)
            if sc_match:
                schema = sc_match.group(1)
            tb_match = re.search(r'\[Name="([^"]+)",\s*Kind="Table"\]', line)
            if tb_match:
                table = tb_match.group(1)

        if catalog and schema and table:
            fqn = f"{catalog}.{schema}.{table}"
        elif table:
            fqn = table
        else:
            fqn = pbi_table

        results.append({
            "pbi_table": pbi_table,
            "source_fqn": fqn,
            "table_type": "physical",
        })

    return results


# M-expression connector patterns → (source_type, is_databricks)
_CONNECTOR_PATTERNS: list[tuple[str, str, bool]] = [
    ("DatabricksMultiCloud", "Databricks", True),
    ("Databricks.Catalogs", "Databricks", True),
    ("Sql.Database", "SQL Server", False),
    ("Sql.Databases", "SQL Server", False),
    ("AzureSynapseAnalytics", "Azure Synapse Analytics", False),
    ("Oracle.Database", "Oracle", False),
    ("Snowflake.Databases", "Snowflake", False),
    ("PostgreSQL.Database", "PostgreSQL", False),
    ("MySQL.Database", "MySQL", False),
    ("GoogleBigQuery", "Google BigQuery", False),
    ("AmazonRedshift", "Amazon Redshift", False),
    ("Teradata.Database", "Teradata", False),
    ("SapHana.Database", "SAP HANA", False),
    ("Odbc.DataSource", "ODBC", False),
    ("OleDb.DataSource", "OLE DB", False),
    ("Csv.Document", "CSV File", False),
    ("Excel.Workbook", "Excel", False),
    ("SharePoint", "SharePoint", False),
    ("Web.Contents", "Web API", False),
    ("AzureStorage", "Azure Blob Storage", False),
    ("AzureDataLakeStorage", "Azure Data Lake Storage", False),
]



def detect_external_sources(semantic_model_dir: str) -> list[dict]:
    """Scan .tmdl partition blocks and classify each table's data source connector.

    Skips PBI auto-generated tables (LocalDateTable_, DateTableTemplate_) and
    correctly identifies calculated tables (no external connector).

    Returns a list of dicts:
        [{"pbi_table": "...", "source_type": "Databricks", "is_databricks": True,
          "connector_detail": "..."}, ...]
    """
    import re

    tables_dir = os.path.join(semantic_model_dir, "definition", "tables")
    results = []

    if not os.path.isdir(tables_dir):
        return results

    for tmdl_file in sorted(glob.glob(os.path.join(tables_dir, "*.tmdl"))):
        with open(tmdl_file, "r") as f:
            content = f.read()

        pbi_table = _parse_tmdl_table_name(content, tmdl_file)

        if any(pbi_table.startswith(p) for p in _PBI_INTERNAL_TABLE_PREFIXES):
            continue

        partition_block = ""
        in_partition = False
        for line in content.splitlines():
            if line.strip().startswith("partition "):
                in_partition = True
            if in_partition:
                partition_block += line + "\n"

        if not partition_block or "= calculated" in partition_block:
            results.append({
                "pbi_table": pbi_table,
                "source_type": "Calculated (PBI)",
                "is_databricks": False,
                "connector_detail": "",
            })
            continue

        source_type = "Unknown"
        is_databricks = False
        connector_detail = ""

        for pattern, stype, is_dbx in _CONNECTOR_PATTERNS:
            if pattern in partition_block:
                source_type = stype
                is_databricks = is_dbx
                detail_match = re.search(rf'{pattern}[^"]*"([^"]*)"', partition_block)
                if detail_match:
                    connector_detail = detail_match.group(1)
                break

        results.append({
            "pbi_table": pbi_table,
            "source_type": source_type,
            "is_databricks": is_databricks,
            "connector_detail": connector_detail,
        })

    return results


# ---------------------------------------------------------------------------
# PBI Color Extraction
# ---------------------------------------------------------------------------


@dataclass
class PbiColorPalette:
    """Holds extracted color information from a PBI report."""
    data_colors: list[str] = field(default_factory=list)
    semantic_colors: dict[str, str] = field(default_factory=dict)
    visual_colors: dict[str, list[dict]] = field(default_factory=dict)


def _hex_to_rgb(hex_color: str) -> tuple[int, int, int]:
    hex_color = hex_color.lstrip("#")
    return (
        int(hex_color[0:2], 16),
        int(hex_color[2:4], 16),
        int(hex_color[4:6], 16),
    )


def _rgb_to_hex(r: int, g: int, b: int) -> str:
    return f"#{r:02X}{g:02X}{b:02X}"


def _adjust_color_brightness(hex_color: str, percent: float) -> str:
    """Apply a PBI ThemeDataColor brightness adjustment.

    Negative percent = darken (shade), positive = lighten (tint).
    PBI uses roughly linear interpolation toward black or white.
    """
    r, g, b = _hex_to_rgb(hex_color)
    if percent > 0:
        r = int(r + (255 - r) * percent)
        g = int(g + (255 - g) * percent)
        b = int(b + (255 - b) * percent)
    elif percent < 0:
        factor = 1 + percent
        r = int(r * factor)
        g = int(g * factor)
        b = int(b * factor)
    return _rgb_to_hex(
        max(0, min(255, r)),
        max(0, min(255, g)),
        max(0, min(255, b)),
    )


def _resolve_pbi_color_expr(color_expr: dict, data_colors: list[str]) -> Optional[str]:
    """Resolve a PBI color expression to a hex string.

    Handles:
    - Literal hex values: {"Literal": {"Value": "'#FF0000'"}}
    - Theme data color refs: {"ThemeDataColor": {"ColorId": 0, "Percent": -0.1}}
    - Direct solid hex: plain string like "#FF0000"
    """
    if isinstance(color_expr, str):
        if color_expr.startswith("#"):
            return color_expr.upper()
        return None

    if "expr" in color_expr:
        return _resolve_pbi_color_expr(color_expr["expr"], data_colors)

    if "Literal" in color_expr:
        val = color_expr["Literal"].get("Value", "")
        val = val.strip("'\"")
        if val.startswith("#") and len(val) in (4, 7):
            return val.upper()
        return None

    if "ThemeDataColor" in color_expr:
        tdc = color_expr["ThemeDataColor"]
        color_id = tdc.get("ColorId", 0)
        percent = tdc.get("Percent", 0)
        if color_id < len(data_colors):
            base = data_colors[color_id]
            if percent == 0:
                return base.upper()
            return _adjust_color_brightness(base, percent).upper()
        return None

    return None


def extract_pbi_theme_colors(report_dir: str) -> PbiColorPalette:
    """Extract the color palette from the PBI report's base theme.

    Reads report.json to find the theme reference, then loads the
    BaseThemes/*.json file to extract the dataColors palette and
    semantic colors (good, bad, neutral, etc.).
    """
    palette = PbiColorPalette()

    report_json_path = os.path.join(report_dir, "definition", "report.json")
    if not os.path.isfile(report_json_path):
        return palette

    with open(report_json_path, "r") as f:
        report_data = json.load(f)

    theme_path = None
    for rp in report_data.get("resourcePackages", []):
        for item in rp.get("items", []):
            if item.get("type") == "BaseTheme":
                rel_path = item.get("path", "")
                if rel_path:
                    theme_path = os.path.join(
                        report_dir, "StaticResources",
                        rp.get("name", "SharedResources"),
                        rel_path,
                    )
                    break
        if theme_path:
            break

    if not theme_path:
        candidates = glob.glob(
            os.path.join(report_dir, "StaticResources", "**", "BaseThemes", "*.json"),
            recursive=True,
        )
        if candidates:
            theme_path = candidates[0]

    if not theme_path or not os.path.isfile(theme_path):
        return palette

    with open(theme_path, "r") as f:
        theme_data = json.load(f)

    palette.data_colors = [
        c.upper() if isinstance(c, str) else c
        for c in theme_data.get("dataColors", [])
    ]

    for key in ("good", "bad", "neutral", "tableAccent", "maximum", "center", "minimum"):
        val = theme_data.get(key)
        if isinstance(val, str) and val.startswith("#"):
            palette.semantic_colors[key] = val.upper()

    return palette


def extract_visual_colors(
    visual_json: dict,
    data_colors: list[str],
) -> list[dict]:
    """Extract per-data-point or per-series color assignments from a visual.

    Returns a list of dicts like:
        [{"hex": "#118DFF", "category": "Coffee"}, {"hex": "#12239E"}]

    Handles objects.dataPoint with fill colors (literal or ThemeDataColor).
    """
    vis = visual_json.get("visual", {})
    objects = vis.get("objects", {})
    result: list[dict] = []

    for dp in objects.get("dataPoint", []):
        props = dp.get("properties", {})
        fill = props.get("fill", {})
        solid = fill.get("solid", {})
        color_val = solid.get("color", {})
        hex_color = _resolve_pbi_color_expr(color_val, data_colors)
        if not hex_color:
            continue

        entry: dict = {"hex": hex_color}

        selector = dp.get("selector", {})
        scope_data = selector.get("data", [])
        if scope_data:
            for scope_item in scope_data:
                if isinstance(scope_item, dict):
                    scope_id = scope_item.get("scopeId", {})
                    if isinstance(scope_id, dict):
                        val = scope_id.get("Value", "")
                        if val:
                            entry["category"] = val.strip("'\"")
        result.append(entry)

    for fill_item in objects.get("fill", []):
        props = fill_item.get("properties", {})
        fill_color = props.get("fillColor", {})
        solid = fill_color.get("solid", {})
        color_val = solid.get("color", {})
        hex_color = _resolve_pbi_color_expr(color_val, data_colors)
        if hex_color:
            result.append({"hex": hex_color})

    return result


def _slim_tmdl(content: str) -> str:
    """Strip non-essential metadata from a .tmdl file.

    Keeps table/column names, data types, measures, DAX expressions, and
    partition source blocks.  Drops lineageTags, annotations, formatStrings,
    and summarizeBy — none of these affect the SQL conversion.
    """
    skip_prefixes = ("lineageTag:", "annotation ", "formatString:", "summarizeBy:")
    lines = []
    prev_blank = False
    for line in content.splitlines():
        stripped = line.strip()
        if any(stripped.startswith(p) for p in skip_prefixes):
            continue
        if stripped == "":
            if prev_blank:
                continue
            prev_blank = True
        else:
            prev_blank = False
        lines.append(line)
    return "\n".join(lines)


def _slim_visual_data(vis_data: dict) -> dict:
    """Keep only the fields the LLM needs from a visual.json structure.

    Retains: visual type, position, data bindings (prototypeQuery,
    dataTransforms, columnProperties), and title.
    Drops: objects, visualContainerObjects, drillFilterOtherVisuals,
    and all formatting/style/conditional-formatting sections.
    """
    slim: dict = {}
    if "name" in vis_data:
        slim["name"] = vis_data["name"]
    if "position" in vis_data:
        slim["position"] = vis_data["position"]

    visual = vis_data.get("visual", {})
    slim_vis: dict = {}
    keep_keys = (
        "visualType", "prototypeQuery", "dataTransforms",
        "columnProperties", "title", "singleVisual",
    )
    for key in keep_keys:
        if key in visual:
            val = visual[key]
            if key == "singleVisual" and isinstance(val, dict):
                sv = {}
                for sk in ("prototypeQuery", "projections", "sort"):
                    if sk in val:
                        sv[sk] = val[sk]
                if sv:
                    slim_vis[key] = sv
            else:
                slim_vis[key] = val

    if not slim_vis:
        slim_vis["visualType"] = visual.get("visualType", "unknown")

    slim["visual"] = slim_vis
    return slim


def _collect_semantic_model_context(semantic_model_dir: str) -> str:
    """Collect the semantic model (tables, relationships, model metadata)."""
    sections = []

    tables_dir = os.path.join(semantic_model_dir, "definition", "tables")
    if os.path.isdir(tables_dir):
        for tmdl_file in sorted(glob.glob(os.path.join(tables_dir, "*.tmdl"))):
            name = os.path.basename(tmdl_file)
            with open(tmdl_file, "r") as f:
                content = f.read()
            sections.append(f"### Table: {name}\n```\n{_slim_tmdl(content)}\n```")

    rel_file = os.path.join(semantic_model_dir, "definition", "relationships.tmdl")
    if os.path.isfile(rel_file):
        with open(rel_file, "r") as f:
            content = f.read()
        sections.append(f"### Relationships\n```\n{_slim_tmdl(content)}\n```")

    model_file = os.path.join(semantic_model_dir, "definition", "model.tmdl")
    if os.path.isfile(model_file):
        with open(model_file, "r") as f:
            content = f.read()
        sections.append(f"### Model\n```\n{_slim_tmdl(content)}\n```")

    return "\n\n".join(sections)


def _collect_page_context(page_dir: str) -> str:
    """Collect context for a single PBI page (page.json + visuals)."""
    sections = []

    page_json = os.path.join(page_dir, "page.json")
    if os.path.isfile(page_json):
        with open(page_json, "r") as f:
            page_data = json.load(f)
        page_data.pop("visualInteractions", None)
        page_data.pop("objects", None)
        sections.append(f"### Page: {os.path.basename(page_dir)}\n```json\n{json.dumps(page_data, indent=2)}\n```")

    for vis_path in sorted(glob.glob(os.path.join(page_dir, "visuals", "*", "visual.json"))):
        vis_id = os.path.basename(os.path.dirname(vis_path))
        with open(vis_path, "r") as f:
            vis_data = json.load(f)
        if "visualGroup" in vis_data:
            continue
        slim = _slim_visual_data(vis_data)
        sections.append(f"### Visual: {vis_id}\n```json\n{json.dumps(slim, indent=2)}\n```")

    return "\n\n".join(sections)


def collect_pbi_context(report_dir: str, semantic_model_dir: str) -> str:
    """Collect all PBI artifacts into a single text block for the LLM."""
    model_ctx = _collect_semantic_model_context(semantic_model_dir)

    page_sections = []
    pages_dir = os.path.join(report_dir, "definition", "pages")
    if os.path.isdir(pages_dir):
        pages_json = os.path.join(pages_dir, "pages.json")
        if os.path.isfile(pages_json):
            with open(pages_json, "r") as f:
                page_sections.append(f"### Pages Metadata\n```json\n{f.read()}\n```")

        for page_dir_path in sorted(glob.glob(os.path.join(pages_dir, "*"))):
            if os.path.isdir(page_dir_path):
                page_sections.append(_collect_page_context(page_dir_path))

    return model_ctx + "\n\n" + "\n\n".join(page_sections)


def collect_pbi_context_chunked(
    report_dir: str, semantic_model_dir: str
) -> tuple[str, list[tuple[str, str]]]:
    """Split PBI context into a shared base and per-page chunks.

    Returns (semantic_model_context, [(page_display_name, page_context), ...]).
    Used for multi-turn LLM calls when the full context exceeds token limits.
    """
    model_ctx = _collect_semantic_model_context(semantic_model_dir)

    page_chunks: list[tuple[str, str]] = []
    pages_dir = os.path.join(report_dir, "definition", "pages")
    if not os.path.isdir(pages_dir):
        return model_ctx, page_chunks

    pages_meta_section = ""
    pages_json = os.path.join(pages_dir, "pages.json")
    if os.path.isfile(pages_json):
        with open(pages_json, "r") as f:
            pages_meta_section = f"### Pages Metadata\n```json\n{f.read()}\n```\n\n"

    page_order = []
    if os.path.isfile(pages_json):
        with open(pages_json, "r") as f:
            page_order = json.load(f).get("pageOrder", [])
    if not page_order:
        page_order = sorted(
            d for d in os.listdir(pages_dir)
            if os.path.isdir(os.path.join(pages_dir, d))
        )

    for page_id in page_order:
        page_dir_path = os.path.join(pages_dir, page_id)
        if not os.path.isdir(page_dir_path):
            continue
        display_name = page_id
        pj = os.path.join(page_dir_path, "page.json")
        if os.path.isfile(pj):
            with open(pj, "r") as f:
                display_name = json.load(f).get("displayName", page_id)
        page_ctx = _collect_page_context(page_dir_path)
        page_chunks.append((display_name, pages_meta_section + page_ctx))

    return model_ctx, page_chunks


# ---------------------------------------------------------------------------
# Structured PBI Layout Parsing
# ---------------------------------------------------------------------------

DECORATIVE_TYPES = {"shape", "image", "actionButton"}
SLICER_TYPES = {"slicer"}

PBI_CANVAS_WIDTH = 1280
PBI_CANVAS_HEIGHT = 720

GRID_ROWS_PER_CANVAS = 12


@dataclass
class PbiVisual:
    visual_id: str
    visual_type: str
    pbi_x: float
    pbi_y: float
    pbi_width: float
    pbi_height: float
    display_name: Optional[str] = None
    is_slicer: bool = False
    is_decorative: bool = False
    is_global_slicer: bool = False
    slicer_field: Optional[str] = None
    grid_x: int = 0
    grid_y: int = 0
    grid_width: int = 1
    grid_height: int = 3
    colors: list = field(default_factory=list)


@dataclass
class PbiPage:
    page_id: str
    display_name: str
    width: int = PBI_CANVAS_WIDTH
    height: int = PBI_CANVAS_HEIGHT
    visuals: list = field(default_factory=list)

    @property
    def data_visuals(self) -> list:
        return [v for v in self.visuals if not v.is_slicer and not v.is_decorative]

    @property
    def slicers(self) -> list:
        return [v for v in self.visuals if v.is_slicer]

    @property
    def page_slicers(self) -> list:
        return [v for v in self.visuals if v.is_slicer and not v.is_global_slicer]

    @property
    def global_slicers(self) -> list:
        return [v for v in self.visuals if v.is_slicer and v.is_global_slicer]


@dataclass
class PbiLayout:
    pages: list = field(default_factory=list)

    @property
    def total_canvas_pages(self) -> int:
        return len(self.pages)

    @property
    def total_data_visuals(self) -> int:
        return sum(len(p.data_visuals) for p in self.pages)

    @property
    def total_slicers(self) -> int:
        return sum(len(p.slicers) for p in self.pages)

    @property
    def total_page_slicers(self) -> int:
        return sum(len(p.page_slicers) for p in self.pages)

    @property
    def total_global_slicers(self) -> int:
        return sum(len(p.global_slicers) for p in self.pages)

    @property
    def has_global_filters(self) -> bool:
        return self.total_global_slicers > 0

    def classify_slicers(self) -> None:
        """Determine which slicers are global vs page-level.

        A slicer is global only if the same field appears as a slicer on EVERY
        page of a multi-page report. In a single-page report, all slicers are
        page-level since there's nothing to "globalize" across.
        """
        if len(self.pages) <= 1:
            for page in self.pages:
                for v in page.slicers:
                    v.is_global_slicer = False
            return

        slicer_fields_per_page: list[set[str]] = []
        for page in self.pages:
            fields = set()
            for v in page.slicers:
                if v.slicer_field:
                    fields.add(v.slicer_field)
            slicer_fields_per_page.append(fields)

        if not slicer_fields_per_page:
            return

        global_fields = slicer_fields_per_page[0].copy()
        for page_fields in slicer_fields_per_page[1:]:
            global_fields &= page_fields

        for page in self.pages:
            for v in page.slicers:
                v.is_global_slicer = (v.slicer_field in global_fields) if v.slicer_field else False


def _extract_visual_display_name(visual_json: dict) -> Optional[str]:
    """Try to extract a human-readable name from a PBI visual definition."""
    vis = visual_json.get("visual", {})
    vco = vis.get("visualContainerObjects", {})
    title_list = vco.get("title", [])
    for t in title_list:
        text = t.get("properties", {}).get("text", {}).get("expr", {}).get("Literal", {}).get("Value", "")
        if text and text.startswith("'") and text.endswith("'"):
            return text[1:-1]
    query_state = vis.get("query", {}).get("queryState", {})
    for role_key in ("Y", "Values", "Category", "Rows"):
        projections = query_state.get(role_key, {}).get("projections", [])
        if projections:
            name = projections[0].get("displayName") or projections[0].get("nativeQueryRef", "")
            if name:
                return name
    return None


def _extract_slicer_field(visual_json: dict) -> Optional[str]:
    """Extract the entity.property key from a slicer's query definition."""
    vis = visual_json.get("visual", {})
    query_state = vis.get("query", {}).get("queryState", {})
    for role_key in ("Values", "Category", "Rows"):
        projections = query_state.get(role_key, {}).get("projections", [])
        for proj in projections:
            col = proj.get("field", {}).get("Column", {})
            entity = col.get("Expression", {}).get("SourceRef", {}).get("Entity", "")
            prop = col.get("Property", "")
            if entity and prop:
                return f"{entity}.{prop}"
    return None


def _compute_grid_height(
    vis_type: str,
    pbi_height: float,
    canvas_height: int = PBI_CANVAS_HEIGHT,
) -> int:
    """Derive grid height purely from PBI pixel proportions.

    Maps pixel height proportionally to ~12 grid rows per 720px canvas.
    Only applies a universal minimum of 2 (1 for textbox) — no per-type
    maximums, so the PBI designer's sizing choices are preserved.
    """
    raw = pbi_height / canvas_height * GRID_ROWS_PER_CANVAS
    h = max(1, round(raw))
    min_h = 1 if vis_type == "textbox" else 2
    return max(min_h, h)


def _pixel_to_grid_x(pbi_x: float, canvas_width: int = PBI_CANVAS_WIDTH) -> int:
    return max(0, min(GRID_COLUMNS - 1, round(pbi_x / canvas_width * GRID_COLUMNS)))


def _pixel_to_grid_width(pbi_width: float, grid_x: int, canvas_width: int = PBI_CANVAS_WIDTH) -> int:
    raw = pbi_width / canvas_width * GRID_COLUMNS
    w = max(1, round(raw))
    if grid_x + w > GRID_COLUMNS:
        w = GRID_COLUMNS - grid_x
    return max(1, w)


def parse_pbi_layout(
    report_dir: str,
    color_palette: Optional[PbiColorPalette] = None,
) -> PbiLayout:
    """Parse PBI report into a structured layout with computed grid positions.

    Reads pages.json for ordering, each page.json for dimensions, and each
    visual.json for type and pixel position. Converts pixel positions to AIBI
    6-column grid coordinates and classifies visuals as data/slicer/decorative.
    When a color_palette is provided, also extracts per-visual color assignments.
    """
    layout = PbiLayout()
    pages_dir = os.path.join(report_dir, "definition", "pages")
    if not os.path.isdir(pages_dir):
        return layout

    page_order = []
    pages_json_path = os.path.join(pages_dir, "pages.json")
    if os.path.isfile(pages_json_path):
        with open(pages_json_path, "r") as f:
            pages_meta = json.load(f)
        page_order = pages_meta.get("pageOrder", [])

    if not page_order:
        page_order = sorted(
            d for d in os.listdir(pages_dir)
            if os.path.isdir(os.path.join(pages_dir, d))
        )

    for page_id in page_order:
        page_path = os.path.join(pages_dir, page_id)
        if not os.path.isdir(page_path):
            continue

        page_json_path = os.path.join(page_path, "page.json")
        display_name = page_id
        canvas_w, canvas_h = PBI_CANVAS_WIDTH, PBI_CANVAS_HEIGHT

        if os.path.isfile(page_json_path):
            with open(page_json_path, "r") as f:
                page_data = json.load(f)
            display_name = page_data.get("displayName", page_id)
            canvas_w = page_data.get("width", PBI_CANVAS_WIDTH)
            canvas_h = page_data.get("height", PBI_CANVAS_HEIGHT)

        page = PbiPage(page_id=page_id, display_name=display_name, width=canvas_w, height=canvas_h)

        visual_paths = sorted(glob.glob(os.path.join(page_path, "visuals", "*", "visual.json")))

        # Pass 1: collect visual group containers so we can offset children
        group_positions: dict[str, tuple[float, float]] = {}
        vis_data_list: list[tuple[str, dict]] = []
        for vis_path in visual_paths:
            with open(vis_path, "r") as f:
                vis_data = json.load(f)
            vis_id = vis_data.get("name", os.path.basename(os.path.dirname(vis_path)))
            vis_data_list.append((vis_id, vis_data))
            if "visualGroup" in vis_data:
                pos = vis_data.get("position", {})
                group_positions[vis_id] = (pos.get("x", 0), pos.get("y", 0))

        # Pass 2: build visuals, offsetting children by their parent group
        raw_visuals = []
        for vis_id, vis_data in vis_data_list:
            if "visualGroup" in vis_data:
                continue

            vis_inner = vis_data.get("visual", {})
            vis_type = vis_inner.get("visualType", "unknown")
            pos = vis_data.get("position", {})

            pbi_x = pos.get("x", 0)
            pbi_y = pos.get("y", 0)
            pbi_w = pos.get("width", 100)
            pbi_h = pos.get("height", 100)

            parent_group = vis_data.get("parentGroupName")
            if parent_group and parent_group in group_positions:
                gx, gy = group_positions[parent_group]
                pbi_x += gx
                pbi_y += gy

            is_slicer = vis_type in SLICER_TYPES
            slicer_field = _extract_slicer_field(vis_data) if is_slicer else None

            vis_colors = []
            if color_palette and color_palette.data_colors:
                vis_colors = extract_visual_colors(vis_data, color_palette.data_colors)

            visual = PbiVisual(
                visual_id=vis_id,
                visual_type=vis_type,
                pbi_x=pbi_x,
                pbi_y=pbi_y,
                pbi_width=pbi_w,
                pbi_height=pbi_h,
                display_name=_extract_visual_display_name(vis_data),
                is_slicer=is_slicer,
                is_decorative=vis_type in DECORATIVE_TYPES,
                slicer_field=slicer_field,
                colors=vis_colors,
            )

            visual.grid_x = _pixel_to_grid_x(pbi_x, canvas_w)
            visual.grid_width = _pixel_to_grid_width(pbi_w, visual.grid_x, canvas_w)
            visual.grid_height = _compute_grid_height(vis_type, pbi_h, canvas_h)

            raw_visuals.append(visual)

        _assign_grid_y_positions(raw_visuals)
        page.visuals = raw_visuals
        layout.pages.append(page)

    layout.classify_slicers()
    return layout


def _assign_grid_y_positions(visuals: list) -> None:
    """Pack visuals using pure per-visual column-skyline packing.

    Each visual is placed at the lowest available y across the columns it
    occupies, regardless of other visuals at similar PBI y-positions.  This
    lets items in independent columns (e.g. a counter below a card in col 1)
    pack tightly even when an adjacent column (col 0) has more stacked items.

    Row grouping is used only for width normalization of wide rows (5+ cols).
    """
    if not visuals:
        return

    non_decorative = [v for v in visuals if not v.is_decorative]
    non_decorative.sort(key=lambda v: (v.pbi_y, v.pbi_x))

    # Group into rows by PBI y-proximity for width normalization only
    ROW_THRESHOLD = 40
    rows: list[list[PbiVisual]] = []
    current_row: list[PbiVisual] = []
    current_y_anchor = -999.0

    for v in non_decorative:
        if abs(v.pbi_y - current_y_anchor) > ROW_THRESHOLD:
            if current_row:
                rows.append(current_row)
            current_row = [v]
            current_y_anchor = v.pbi_y
        else:
            current_row.append(v)
    if current_row:
        rows.append(current_row)

    # Identify columns where multiple visuals stack (same grid_x).
    # Visuals in stacked columns keep their natural width during
    # normalization so that all items in a column stay aligned.
    from collections import Counter
    x_counts = Counter(v.grid_x for v in non_decorative)
    stacked_xs = {x for x, count in x_counts.items() if count > 1}

    for row in rows:
        row.sort(key=lambda v: v.pbi_x)
        _normalize_row_widths(row, stacked_xs)

    # Vertical column alignment: visuals with similar pbi_x should share
    # the same grid_x so they stack in the same AIBI column.  Normalization
    # may re-seat grid_x for wide rows but leave narrow rows untouched;
    # this propagates the normalized position to all vertically-aligned
    # visuals.  The topmost visual (lowest pbi_y) is the reference because
    # it is most likely in the first (normalized) row.
    PBI_X_ALIGN = 60
    align_groups: list[list] = []
    for v in sorted(non_decorative, key=lambda v: v.pbi_x):
        if v.pbi_width >= PBI_CANVAS_WIDTH * 0.5:
            continue
        placed = False
        for g in align_groups:
            if abs(v.pbi_x - g[0].pbi_x) <= PBI_X_ALIGN:
                g.append(v)
                placed = True
                break
        if not placed:
            align_groups.append([v])

    for g in align_groups:
        if len(g) < 2:
            continue
        ref = min(g, key=lambda v: v.pbi_y)
        for v in g:
            v.grid_x = ref.grid_x

    # Pure per-visual skyline: each visual finds its own lowest y
    col_bottoms = [0] * GRID_COLUMNS

    for v in non_decorative:
        cols = range(v.grid_x, min(v.grid_x + v.grid_width, GRID_COLUMNS))
        v.grid_y = max((col_bottoms[c] for c in cols), default=0)
        for c in cols:
            col_bottoms[c] = v.grid_y + v.grid_height

    for v in visuals:
        if v.is_decorative:
            v.grid_y = 0


def _normalize_row_widths(row: list, stacked_xs: set[int] | None = None) -> None:
    """Distribute 6 grid columns using visual area, respecting stacked columns.

    Visuals whose grid_x appears in ``stacked_xs`` (i.e. they share a column
    with visuals in other rows) are width-locked at their natural proportional
    size so the column stays aligned.  The remaining grid columns are
    distributed among "free" visuals using sqrt(w*h) area weighting.

    Only activates for rows whose visuals collectively span most of the PBI
    canvas (>= 60%).  Narrow rows keep their original sizes.
    """
    if not row:
        return

    import math

    total_pbi_w = sum(v.pbi_width for v in row)
    if total_pbi_w < PBI_CANVAS_WIDTH * 0.6:
        return

    if stacked_xs is None:
        stacked_xs = set()

    locked_indices = {i for i, v in enumerate(row) if v.grid_x in stacked_xs}
    free_indices = [i for i in range(len(row)) if i not in locked_indices]

    locked_total = sum(row[i].grid_width for i in locked_indices)
    remaining = GRID_COLUMNS - locked_total

    if not free_indices:
        # All visuals are stacked — just re-seat grid_x sequentially
        running_x = 0
        for v in row:
            v.grid_x = running_x
            running_x += v.grid_width
        return

    # Distribute remaining columns among free visuals by area
    free_weights = [math.sqrt(max(1, row[i].pbi_width) * max(1, row[i].pbi_height))
                    for i in free_indices]
    total_fw = sum(free_weights)

    raw_fracs = [w / total_fw * remaining for w in free_weights]
    free_widths = [max(1, round(f)) for f in raw_fracs]

    delta = remaining - sum(free_widths)
    if delta != 0:
        n_free = len(free_indices)
        errors = [(raw_fracs[j] - free_widths[j], j) for j in range(n_free)]
        errors.sort(reverse=(delta > 0))
        for j in range(abs(delta)):
            idx = errors[j % n_free][1]
            free_widths[idx] += 1 if delta > 0 else -1
            free_widths[idx] = max(1, free_widths[idx])

    # Assign widths: locked visuals keep theirs, free visuals get new ones
    widths = [0] * len(row)
    fi = 0
    for i in range(len(row)):
        if i in locked_indices:
            widths[i] = row[i].grid_width
        else:
            widths[i] = free_widths[fi]
            fi += 1

    running_x = 0
    for v, w in zip(row, widths):
        v.grid_x = running_x
        v.grid_width = w
        running_x += w


def build_color_context(
    color_palette: PbiColorPalette,
    layout: PbiLayout,
) -> str:
    """Build a text section describing the PBI color palette for the LLM prompt.

    Includes the theme's data colors (the chart palette used by all visuals)
    and any per-visual explicit color assignments found in the report.
    """
    if not color_palette.data_colors:
        return ""

    lines = [
        "## COLOR PALETTE — PRESERVE THESE COLORS",
        "",
        "The original Power BI report uses the following data color palette (in order):",
        ", ".join(color_palette.data_colors[:12]),
        "",
        "**Rules for color preservation:**",
        "- For any chart that uses a `color` encoding with `scale.type: \"categorical\"`, "
        "add a `scale.range` array with colors from the palette above.",
        "- Use the first N colors from the palette (in order) where N is the expected "
        "number of distinct categories in that dimension.",
        "- For pie charts: set `color.scale.range` to the palette colors.",
        "- For bar/line charts with a color grouping dimension: set `color.scale.range`.",
        "- For single-series charts (no color encoding): the dashboard theme handles colors.",
        "",
    ]

    visuals_with_colors = []
    for page in layout.pages:
        for v in page.data_visuals:
            if v.colors:
                color_entries = []
                for c in v.colors:
                    if "category" in c:
                        color_entries.append(f"{c['category']}={c['hex']}")
                    else:
                        color_entries.append(c["hex"])
                visuals_with_colors.append(
                    f"  - `{v.visual_type}` \"{v.display_name or v.visual_id[:12]}\": "
                    + ", ".join(color_entries)
                )

    if visuals_with_colors:
        lines.append("**Per-visual color assignments from the PBI report:**")
        lines.extend(visuals_with_colors)
        lines.append("")
        lines.append(
            "When you know the category-to-color mapping, use `scale.domain` (category values) "
            "and `scale.range` (hex colors) together on the color encoding."
        )
        lines.append("")

    return "\n".join(lines)


def build_layout_blueprint_prompt(layout: PbiLayout) -> str:
    """Build explicit LLM instructions from the parsed PBI layout.

    Produces a structured description that tells the LLM exactly how many pages
    to create, which visuals go on each page, and the target grid positions.
    Page-level slicers are placed on their respective canvas pages; only truly
    global slicers (present on every page of a multi-page report) go on a
    separate PAGE_TYPE_GLOBAL_FILTERS page.
    """
    if not layout.pages:
        return ""

    lines = [
        "## LAYOUT BLUEPRINT — YOU MUST FOLLOW THIS EXACTLY",
        "",
        f"The original Power BI report has **{layout.total_canvas_pages} page(s)**.",
        f"You MUST create exactly **{layout.total_canvas_pages} PAGE_TYPE_CANVAS page(s)** — one for each PBI tab.",
    ]

    if layout.has_global_filters:
        lines.append(
            f"Additionally, create exactly **1 PAGE_TYPE_GLOBAL_FILTERS** page "
            f"for the {layout.total_global_slicers} global slicer(s) "
            f"(fields that appear on every page)."
        )
    else:
        lines.append(
            "Do NOT create a PAGE_TYPE_GLOBAL_FILTERS page — there are no global filters in this report."
        )

    lines.append("")

    for page in layout.pages:
        lines.append(f"### Page: \"{page.display_name}\"")
        lines.append(f"- PBI canvas: {page.width}×{page.height}px")
        lines.append(f"- Data visuals: {len(page.data_visuals)}")
        lines.append(f"- Page-level slicers (→ filter widgets ON THIS canvas page): {len(page.page_slicers)}")
        if page.global_slicers:
            lines.append(f"- Global slicers (→ global filters page): {len(page.global_slicers)}")
        lines.append("")

        all_on_page = list(page.data_visuals) + list(page.page_slicers)
        all_on_page.sort(key=lambda vis: (vis.grid_y, vis.grid_x))

        if all_on_page:
            lines.append("#### Visuals & Filters to place on this canvas page:")
            lines.append("| # | PBI Type | Description | Target Grid (x, y, w, h) |")
            lines.append("|---|----------|-------------|--------------------------|")
            for i, v in enumerate(all_on_page, 1):
                desc = v.display_name or v.visual_id[:12]
                kind = " **(page filter)**" if v.is_slicer else ""
                lines.append(
                    f"| {i} | `{v.visual_type}`{kind} | {desc} | "
                    f"x={v.grid_x}, y={v.grid_y}, w={v.grid_width}, h={v.grid_height} |"
                )
            lines.append("")

        if page.global_slicers:
            lines.append("#### Global slicers (place on the PAGE_TYPE_GLOBAL_FILTERS page):")
            for v in page.global_slicers:
                desc = v.display_name or v.visual_id[:12]
                lines.append(f"- `{v.visual_type}` — {desc} (field: `{v.slicer_field}`)")
            lines.append("")

    lines.extend([
        "### CRITICAL LAYOUT RULES:",
        f"1. Create EXACTLY {layout.total_canvas_pages} canvas page(s) — no more, no fewer.",
        "2. Every non-decorative visual MUST appear as a widget. Do NOT skip any.",
        "3. Use the Target Grid positions from the table above. Adjust only if needed to fill the 6-column grid with no gaps.",
        "4. Decorative shapes (type `shape`, `image`, `actionButton`) should be skipped.",
        "5. **Page-level slicers** MUST be placed as filter widgets (filter-multi-select, filter-date-range-picker, etc.) "
        "directly on their respective PAGE_TYPE_CANVAS page — NOT on a global filters page.",
    ])

    if layout.has_global_filters:
        lines.append(
            "6. **Global slicers** (fields present on every page) go on a single PAGE_TYPE_GLOBAL_FILTERS page."
        )
    else:
        lines.append(
            "6. Do NOT create a PAGE_TYPE_GLOBAL_FILTERS page — all slicers are page-level."
        )

    lines.extend([
        "",
        "### COMPACT LAYOUT — ZERO BLANK SPACE (column-skyline packing):",
        "- Use the **exact x, y, width, and height values** from the Target Grid column for every widget.",
        "- The y values are computed with a column-skyline algorithm: short widgets (filters, cards) stack tightly "
        "in their columns even when adjacent to taller widgets (charts, tables). Visuals in different columns "
        "may have different y values — this is intentional and eliminates blank space.",
        "- Every logical row MUST fill the full 6-column width. Widths have been normalized to sum to 6.",
        "- **NEVER create text widgets** unless the PBI source has an explicit `textbox` visual with real content. "
        "Do NOT invent titles, subtitles, headers, section separators, or any text widget that isn't in the table above.",
        "- The ONLY widgets in the output should be the ones listed in the table above. Nothing more.",
        "",
    ])

    return "\n".join(lines)


FREE_LAYOUT_VISUAL_THRESHOLD = 20


def build_free_layout_blueprint_prompt(layout: PbiLayout) -> str:
    """Build a relaxed blueprint that lists visuals but lets the LLM decide layout.

    Used for dense reports where rigid position mapping produces poor results.
    The LLM is given full creative freedom over grid positions while still
    being told exactly which visuals to include and their types.
    """
    if not layout.pages:
        return ""

    lines = [
        "## LAYOUT BLUEPRINT — VISUAL INVENTORY (LLM decides layout)",
        "",
        f"The original Power BI report has **{layout.total_canvas_pages} page(s)**.",
        f"You MUST create exactly **{layout.total_canvas_pages} PAGE_TYPE_CANVAS page(s)**.",
        "",
        "This is a **dense report** with many visuals. You have **full creative freedom** "
        "over widget positioning. Design an aesthetically pleasing, well-organized AI/BI "
        "dashboard that groups related visuals logically. Below is the inventory of visuals "
        "you must include.",
    ]

    if layout.has_global_filters:
        lines.append(
            f"\nAdditionally, create exactly **1 PAGE_TYPE_GLOBAL_FILTERS** page "
            f"for the {layout.total_global_slicers} global slicer(s)."
        )
    else:
        lines.append(
            "\nDo NOT create a PAGE_TYPE_GLOBAL_FILTERS page — there are no global filters."
        )

    lines.append("")

    for page in layout.pages:
        lines.append(f"### Page: \"{page.display_name}\"")
        lines.append(f"- Data visuals: {len(page.data_visuals)}")
        lines.append(f"- Page-level filters: {len(page.page_slicers)}")
        lines.append("")

        if page.page_slicers:
            lines.append("#### Filters (place as filter widgets on this page):")
            for v in page.page_slicers:
                desc = v.display_name or v.slicer_field or v.visual_id[:12]
                lines.append(f"- `{v.visual_type}` — {desc}")
            lines.append("")

        if page.data_visuals:
            lines.append("#### Visuals to include:")
            lines.append("| # | PBI Type | Description |")
            lines.append("|---|----------|-------------|")
            for i, v in enumerate(page.data_visuals, 1):
                desc = v.display_name or v.visual_id[:12]
                lines.append(f"| {i} | `{v.visual_type}` | {desc} |")
            lines.append("")

        if page.global_slicers:
            lines.append("#### Global slicers (place on PAGE_TYPE_GLOBAL_FILTERS page):")
            for v in page.global_slicers:
                desc = v.display_name or v.visual_id[:12]
                lines.append(f"- `{v.visual_type}` — {desc} (field: `{v.slicer_field}`)")
            lines.append("")

    lines.extend([
        "### LAYOUT GUIDELINES (you decide positions):",
        f"1. Create EXACTLY {layout.total_canvas_pages} canvas page(s).",
        "2. Every visual listed above MUST appear as a widget. Do NOT skip any.",
        "3. Decorative shapes (type `shape`, `image`, `actionButton`) are already excluded.",
        "4. Use the 6-column grid. Aim for a clean, professional layout:",
        "   - Place filters together at the top of each page (1 row, filling the 6 columns).",
        "   - Place KPI counters/cards together in a row below filters.",
        "   - Give charts and tables more height (4–6 grid rows) and wider columns (2–3 cols).",
        "   - Use the full 6-column width in every row — no gaps.",
        "   - Group related visuals near each other.",
        "5. **NEVER create text widgets** unless the PBI source has an explicit `textbox` visual listed above.",
        "6. The ONLY widgets in the output should be the ones listed above. Nothing more.",
        "",
    ])

    return "\n".join(lines)


def should_use_free_layout(layout: PbiLayout) -> bool:
    """Decide whether to use free layout mode based on report complexity."""
    max_visuals = max(
        (len(p.data_visuals) + len(p.page_slicers) for p in layout.pages),
        default=0,
    )
    return max_visuals >= FREE_LAYOUT_VISUAL_THRESHOLD


# ---------------------------------------------------------------------------
# Post-processing: enforce blueprint positions on LLM output
# ---------------------------------------------------------------------------

PBI_TO_AIBI_TYPE_MAP = {
    "card": {"counter"},
    "multiRowCard": {"counter", "table"},
    "kpi": {"counter"},
    "textbox": {"text"},
    "lineChart": {"line"},
    "barChart": {"bar"},
    "clusteredBarChart": {"bar"},
    "stackedBarChart": {"bar"},
    "columnChart": {"bar"},
    "clusteredColumnChart": {"bar"},
    "stackedColumnChart": {"bar"},
    "donutChart": {"pie"},
    "pieChart": {"pie"},
    "pivotTable": {"table"},
    "table": {"table"},
    "areaChart": {"area", "line"},
    "stackedAreaChart": {"area", "line"},
    "scatterChart": {"scatter"},
    "treemap": {"bar", "pie"},
    "funnel": {"bar"},
    "gauge": {"counter"},
    "waterfallChart": {"bar"},
    "slicer": {"filter-multi-select", "filter-single-select", "filter-date-range-picker"},
}


def _aibi_widget_type(widget: dict) -> str:
    """Return the semantic widget type from an AIBI widget dict."""
    if "multilineTextboxSpec" in widget:
        return "text"
    return widget.get("spec", {}).get("widgetType", "unknown")


def apply_blueprint_positions(dashboard_json: dict, pbi_layout: PbiLayout) -> dict:
    """Override every widget's grid position with the blueprint values.

    For each canvas page in the dashboard, matches LLM-generated widgets to
    PBI blueprint visuals by type and proximity, overrides positions, and
    removes any unmatched phantom widgets the LLM may have invented.
    """
    pages = dashboard_json.get("pages", [])
    canvas_pages = [p for p in pages if p.get("pageType") != "PAGE_TYPE_GLOBAL_FILTERS"]

    for page_idx, pbi_page in enumerate(pbi_layout.pages):
        aibi_page = None
        pbi_name_lower = pbi_page.display_name.lower()
        for cp in canvas_pages:
            cp_name = (cp.get("displayName") or cp.get("name", "")).lower()
            if cp_name == pbi_name_lower:
                aibi_page = cp
                break
        if aibi_page is None and page_idx < len(canvas_pages):
            aibi_page = canvas_pages[page_idx]
        if aibi_page is None:
            continue

        layout_items = aibi_page.get("layout", [])
        expected_visuals = list(pbi_page.data_visuals) + list(pbi_page.page_slicers)

        matched_indices: set[int] = set()
        kept_items: list[dict] = []

        for pbi_vis in expected_visuals:
            target_types = PBI_TO_AIBI_TYPE_MAP.get(pbi_vis.visual_type, set())
            best_idx = None
            best_dist = float("inf")

            for idx, item in enumerate(layout_items):
                if idx in matched_indices:
                    continue
                wt = _aibi_widget_type(item.get("widget", {}))
                if wt not in target_types:
                    continue
                pos = item.get("position", {})
                dist = abs(pos.get("x", 0) - pbi_vis.grid_x) + abs(pos.get("y", 0) - pbi_vis.grid_y)
                if dist < best_dist:
                    best_dist = dist
                    best_idx = idx

            if best_idx is None:
                for idx, item in enumerate(layout_items):
                    if idx in matched_indices:
                        continue
                    wt = _aibi_widget_type(item.get("widget", {}))
                    if wt in target_types:
                        best_idx = idx
                        break

            if best_idx is not None:
                matched_indices.add(best_idx)
                item = layout_items[best_idx]
                item["position"] = {
                    "x": pbi_vis.grid_x,
                    "y": pbi_vis.grid_y,
                    "width": pbi_vis.grid_width,
                    "height": pbi_vis.grid_height,
                }
                kept_items.append(item)

        for idx, item in enumerate(layout_items):
            if idx not in matched_indices:
                wt = _aibi_widget_type(item.get("widget", {}))
                if wt == "text":
                    if pbi_page.data_visuals and any(
                        v.visual_type == "textbox" for v in pbi_page.data_visuals
                    ):
                        kept_items.append(item)

        aibi_page["layout"] = kept_items

    return dashboard_json


# ---------------------------------------------------------------------------
# Post-processing: apply PBI colors to generated dashboard
# ---------------------------------------------------------------------------

_CHART_WIDGET_TYPES = {"bar", "line", "pie", "area", "scatter"}


def apply_pbi_colors(
    dashboard_json: dict,
    color_palette: PbiColorPalette,
    pbi_layout: Optional[PbiLayout] = None,
) -> dict:
    """Inject PBI theme colors into the generated dashboard JSON.

    Two strategies:
    1. For charts with a categorical `color` encoding that lacks an explicit
       `scale.range`, inject the PBI theme palette as the range so chart
       series colors match the original report.
    2. For visuals that had per-data-point color assignments in PBI, inject
       `scale.domain` + `scale.range` mappings when the info is available.
    """
    if not color_palette.data_colors:
        return dashboard_json

    palette = color_palette.data_colors

    # Build lookup: visual description → colors from PBI layout
    per_visual_colors: dict[str, list[dict]] = {}
    if pbi_layout:
        for page in pbi_layout.pages:
            for v in page.data_visuals:
                if v.colors:
                    key = (v.display_name or "").lower()
                    per_visual_colors[key] = v.colors

    for page in dashboard_json.get("pages", []):
        for item in page.get("layout", []):
            widget = item.get("widget", {})
            spec = widget.get("spec", {})
            if not isinstance(spec, dict):
                continue

            widget_type = spec.get("widgetType", "")
            if widget_type not in _CHART_WIDGET_TYPES:
                continue

            encodings = spec.get("encodings", {})
            if not isinstance(encodings, dict):
                continue

            color_enc = encodings.get("color")
            if not isinstance(color_enc, dict):
                continue

            scale = color_enc.get("scale", {})
            if not isinstance(scale, dict):
                continue

            if scale.get("type") != "categorical":
                continue

            if "range" in scale:
                continue

            # Check if this visual has specific PBI color assignments
            widget_title = spec.get("frame", {}).get("title", "").lower()
            matched_vis_colors = per_visual_colors.get(widget_title)

            if matched_vis_colors:
                cats_with_colors = [
                    c for c in matched_vis_colors if "category" in c
                ]
                if cats_with_colors:
                    scale["domain"] = [c["category"] for c in cats_with_colors]
                    scale["range"] = [c["hex"] for c in cats_with_colors]
                else:
                    explicit_colors = [c["hex"] for c in matched_vis_colors]
                    scale["range"] = explicit_colors
            else:
                scale["range"] = palette[:12]

            color_enc["scale"] = scale

    return dashboard_json


# ---------------------------------------------------------------------------
# LLM Interaction
# ---------------------------------------------------------------------------


def _estimate_tokens(text: str) -> int:
    """Rough token estimate: ~4 characters per token for mixed code/prose."""
    return len(text) // 4


MAX_PROMPT_TOKENS = 190_000


def call_llm(
    report_name: str,
    pbi_context: str,
    layout_blueprint: str = "",
    color_context: str = "",
    custom_instructions: str = "",
) -> str:
    """Send the PBI context to the LLM and return the raw response text.

    If the assembled prompt exceeds the model's context window, raises a
    ValueError with an actionable message rather than hitting a 400 error.
    """
    client = get_llm_client()

    blueprint_section = ""
    if layout_blueprint:
        blueprint_section = f"\n\n{layout_blueprint}\n"

    color_section = ""
    if color_context:
        color_section = f"\n\n{color_context}\n"

    user_message = f"""Convert this Power BI report named "{report_name}" to a Databricks AI/BI dashboard (.lvdash.json).

## Power BI Report Contents

{pbi_context}
{blueprint_section}{color_section}
## Instructions

1. Extract the data source catalog/schema/table from the .tmdl partition blocks
2. Build SQL dataset(s) that JOIN the needed tables using fully-qualified names. Simple aggregations (SUM, COUNT, AVG on a column) will be automatically promoted to widget custom calculations — you may use them freely in dataset SQL. For complex DAX translations (CALCULATE → CASE WHEN, DIVIDE → arithmetic), keep those as derived columns in dataset SQL with GROUP BY.
3. Convert every visual to the appropriate AI/BI widget type
4. Convert page-level slicers to filter widgets placed directly on their respective PAGE_TYPE_CANVAS page. Only create a PAGE_TYPE_GLOBAL_FILTERS page if the LAYOUT BLUEPRINT above explicitly says to (for global slicers present on every page).
5. Skip decorative shapes
6. Use proper 6-column grid layout with no gaps
7. Ensure all field names in query.fields match fieldNames in encodings exactly
8. **CRITICAL: Follow the LAYOUT BLUEPRINT above exactly — same number of pages, same visuals, same approximate positions**
9. **Preserve the PBI color palette** — for charts with categorical color encodings, use `scale.range` with the PBI theme colors
{f"10. **USER CUSTOM INSTRUCTIONS (follow these with highest priority):**{chr(10)}{custom_instructions}" if custom_instructions and custom_instructions.strip() else ""}
Return ONLY the JSON — no markdown fences, no explanation."""

    system_prompt = _get_system_prompt()
    estimated = _estimate_tokens(system_prompt + user_message)
    if estimated > MAX_PROMPT_TOKENS:
        raise ValueError(
            f"Prompt is too large (~{estimated:,} estimated tokens, limit ~{MAX_PROMPT_TOKENS:,}). "
            f"The PBI report has too many visuals/tables for a single LLM call. "
            f"Try reducing the number of pages or visuals in the report."
        )

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_message},
    ]

    full_response = ""

    while True:
        response = client.chat.completions.create(
            model=MODEL,
            messages=messages,
            max_tokens=32768,
            temperature=0,
        )

        chunk = response.choices[0].message.content or ""
        full_response += chunk

        finish = getattr(response.choices[0], "finish_reason", None)
        if finish != "length":
            break

        messages.append({"role": "assistant", "content": chunk})
        messages.append({"role": "user", "content": "The JSON was truncated. Continue EXACTLY where you left off — output only the remaining JSON, no repetition, no explanation."})

    return full_response


CHUNK_TOKEN_BUDGET = 150_000


def call_llm_chunked(
    report_name: str,
    semantic_model_ctx: str,
    page_chunks: list[tuple[str, str]],
    layout_blueprint: str = "",
    color_context: str = "",
    custom_instructions: str = "",
    progress_callback=None,
) -> str:
    """Convert a large PBI report using multi-turn chat, sending pages in batches.

    The first message sends the semantic model + as many pages as fit in the
    token budget. Subsequent messages send additional pages and ask the LLM to
    extend the dashboard JSON. The final response contains the complete JSON.

    ``progress_callback(msg)`` is called with status updates if provided.
    """
    client = get_llm_client()
    system_prompt = _get_system_prompt()
    system_tokens = _estimate_tokens(system_prompt)

    blueprint_section = ""
    if layout_blueprint:
        blueprint_section = f"\n\n{layout_blueprint}\n"

    color_section = ""
    if color_context:
        color_section = f"\n\n{color_context}\n"

    base_tokens = system_tokens + _estimate_tokens(semantic_model_ctx) + _estimate_tokens(blueprint_section) + _estimate_tokens(color_section)

    # Group pages into batches that fit within the token budget
    batches: list[list[tuple[str, str]]] = []
    current_batch: list[tuple[str, str]] = []
    current_tokens = base_tokens

    for page_name, page_ctx in page_chunks:
        page_tokens = _estimate_tokens(page_ctx)
        if current_batch and current_tokens + page_tokens > CHUNK_TOKEN_BUDGET:
            batches.append(current_batch)
            current_batch = []
            current_tokens = base_tokens
        current_batch.append((page_name, page_ctx))
        current_tokens += page_tokens

    if current_batch:
        batches.append(current_batch)

    total_pages = len(page_chunks)
    page_names_all = [name for name, _ in page_chunks]

    messages = [{"role": "system", "content": system_prompt}]

    last_response = ""
    for batch_idx, batch in enumerate(batches):
        batch_page_names = [name for name, _ in batch]
        batch_pages_ctx = "\n\n".join(ctx for _, ctx in batch)

        if batch_idx == 0:
            user_msg = (
                f'Convert this Power BI report named "{report_name}" to a Databricks AI/BI dashboard (.lvdash.json).\n\n'
                f"The report has **{total_pages} pages** in total: {', '.join(page_names_all)}.\n"
                f"I will send them in {len(batches)} batch(es) due to size. "
                f"This is batch 1/{len(batches)} containing pages: {', '.join(batch_page_names)}.\n\n"
                f"## Semantic Model\n\n{semantic_model_ctx}\n\n"
                f"## Pages (batch 1/{len(batches)})\n\n{batch_pages_ctx}"
                f"{blueprint_section}{color_section}\n"
                f"## Instructions\n\n"
                f"1. Extract the data source catalog/schema/table from the .tmdl partition blocks\n"
                f"2. Build SQL dataset(s) with JOINs and fully-qualified names. Simple aggregations (SUM/COUNT/AVG on a column) are fine — they'll be auto-promoted to custom calculations. Complex expressions (CASE WHEN, NULLIF, arithmetic) stay in dataset SQL.\n"
                f"3. Convert every visual to the appropriate AI/BI widget type\n"
                f"4. Convert page-level slicers to filter widgets on their respective page\n"
                f"5. Skip decorative shapes\n"
                f"6. Use proper 6-column grid layout with no gaps\n"
                f"7. Ensure all field names in query.fields match fieldNames in encodings exactly\n"
                f"8. Preserve the PBI color palette in charts with categorical color encodings using scale.range\n"
            )
            if custom_instructions and custom_instructions.strip():
                user_msg += f"9. **USER CUSTOM INSTRUCTIONS (follow these with highest priority):**\n{custom_instructions}\n"
            if len(batches) > 1:
                user_msg += (
                    f"\nSince there are more batches coming, generate the dashboard JSON for these pages now. "
                    f"I will send the remaining pages next, and you will extend the JSON.\n\n"
                    f"Return ONLY the JSON — no markdown fences, no explanation."
                )
            else:
                user_msg += "\nReturn ONLY the JSON — no markdown fences, no explanation."
        else:
            user_msg = (
                f"Here are the next pages (batch {batch_idx + 1}/{len(batches)}): "
                f"{', '.join(batch_page_names)}.\n\n"
                f"## Pages (batch {batch_idx + 1}/{len(batches)})\n\n{batch_pages_ctx}\n\n"
                f"Add these pages and their widgets to the dashboard JSON you already generated. "
                f"Reuse existing datasets where possible, or add new ones if needed.\n\n"
                f"Return the COMPLETE updated dashboard JSON (including all previous pages) "
                f"— no markdown fences, no explanation."
            )

        messages.append({"role": "user", "content": user_msg})

        if progress_callback:
            progress_callback(
                f"Sending batch {batch_idx + 1}/{len(batches)} "
                f"({', '.join(batch_page_names)})..."
            )

        max_out_tokens = 32768 if len(batches) > 1 else 16384

        response = client.chat.completions.create(
            model=MODEL,
            messages=messages,
            max_tokens=max_out_tokens,
            temperature=0,
        )

        last_response = response.choices[0].message.content
        messages.append({"role": "assistant", "content": last_response})

        if progress_callback:
            progress_callback(f"Batch {batch_idx + 1}/{len(batches)} complete")

    return last_response


def generate_explanation(report_name: str, pbi_context: str, dashboard_json: dict) -> str:
    """Ask the LLM to produce a human-readable conversion report.

    Takes the original PBI context and the generated dashboard JSON and
    returns a markdown explanation of what was identified and how each
    element was mapped.
    """
    client = get_llm_client()

    serialized = json.dumps(dashboard_json, indent=2)
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


def _is_simple_aggregate(node) -> bool:
    """Return True if *node* is an AIBI-compatible simple aggregate.

    AIBI widget expressions only support: SUM/AVG/MIN/MAX/COUNT directly
    on a single column (or COUNT(*), COUNT(DISTINCT col)).  Anything more
    complex (CASE WHEN, arithmetic, nested expressions) is NOT supported.
    """
    from sqlglot import exp as E

    if isinstance(node, (E.Sum, E.Avg, E.Min, E.Max)):
        return isinstance(node.this, E.Column)

    if isinstance(node, E.Count):
        if isinstance(node.this, E.Star):
            return True
        if isinstance(node.this, E.Distinct):
            exprs = node.this.expressions
            return len(exprs) == 1 and isinstance(exprs[0], E.Column)
        return isinstance(node.this, E.Column)

    return False


def _agg_to_widget_expression(node) -> str:
    """Convert an aggregate SQL AST node to a widget custom calculation string.

    Strips table qualifiers so column names become bare backtick-quoted
    identifiers that reference the dataset output columns.
    """
    from sqlglot import exp as E

    cloned = node.copy()
    for col in cloned.find_all(E.Column):
        col.replace(E.Column(this=E.Identifier(this=col.name, quoted=True)))
    return cloned.sql(dialect="spark")


def _make_custom_calc_field_name(node, fallback_alias: str) -> str:
    """Generate a canonical AIBI field name like ``sum(col)`` for simple aggregates.

    Falls back to *fallback_alias* for complex expressions (CASE, arithmetic, etc.).
    """
    from sqlglot import exp as E

    simple_map = {E.Sum: "sum", E.Avg: "avg", E.Min: "min", E.Max: "max"}
    for cls, prefix in simple_map.items():
        if isinstance(node, cls) and isinstance(node.this, E.Column):
            return f"{prefix}({node.this.name})"

    if isinstance(node, E.Count):
        if isinstance(node.this, E.Star):
            return "count(*)"
        if isinstance(node.this, E.Distinct):
            exprs = node.this.expressions
            if exprs and isinstance(exprs[0], E.Column):
                return f"countdistinct({exprs[0].name})"
        if isinstance(node.this, E.Column):
            return f"count({node.this.name})"

    return fallback_alias


def _update_encoding_field_name(encodings: dict, old_name: str, new_name: str):
    """Rename *old_name* → *new_name* inside all encoding entries."""
    if not encodings or old_name == new_name:
        return
    for _key, value in encodings.items():
        if isinstance(value, dict):
            if value.get("fieldName") == old_name:
                value["fieldName"] = new_name
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict) and item.get("fieldName") == old_name:
                    item["fieldName"] = new_name


def _promote_aggregations_to_custom_calcs(dashboard_json: dict) -> dict:
    """Post-process: move SQL aggregations from datasets into widget custom calculations.

    For each dataset whose SQL contains GROUP BY + aggregate functions (SUM, COUNT, …):
      1. Extract the aggregate expressions and their aliases.
      2. Rewrite the dataset SQL to SELECT only raw/base columns (no aggregations, no GROUP BY).
      3. Add any raw columns referenced inside the aggregates that aren't already in SELECT.
      4. For every widget that uses the dataset, replace the backtick-quoted alias reference
         with the actual aggregate expression and update the encoding fieldName to match.
      5. Set ``disaggregated: false`` so the widget performs the aggregation at query time.

    Queries with window functions are left untouched (too complex to safely split).
    """
    try:
        import sqlglot
        from sqlglot import exp as E
    except ImportError:
        return dashboard_json

    datasets = dashboard_json.get("datasets", [])
    pages = dashboard_json.get("pages", [])

    # --- Phase 1: rewrite dataset SQL and collect aggregate mappings --------
    dataset_agg_map: dict[str, dict[str, dict]] = {}

    for ds in datasets:
        ds_name = ds.get("name", "")
        query = ds.get("query", "")
        if not query:
            continue

        try:
            tree = sqlglot.parse_one(query, dialect="spark")
        except Exception:
            continue

        if list(tree.find_all(E.Window)):
            continue
        if not tree.args.get("group"):
            continue

        agg_selects = []
        non_agg_selects = []

        for sel in tree.selects:
            if list(sel.find_all(E.AggFunc)):
                agg_selects.append(sel)
            else:
                non_agg_selects.append(sel)

        if not agg_selects:
            continue

        # Only promote if EVERY aggregate is a simple AIBI-compatible form.
        # Complex expressions (CASE WHEN, arithmetic, NULLIF, etc.) are not
        # valid widget expressions — leave the entire dataset untouched.
        all_simple = all(
            _is_simple_aggregate(
                sel.unalias() if isinstance(sel, E.Alias) else sel
            )
            for sel in agg_selects
        )
        if not all_simple:
            continue

        agg_aliases: dict[str, dict] = {}
        raw_cols_to_add: list[tuple[str, str]] = []

        for sel in agg_selects:
            alias = sel.alias_or_name
            inner = sel.unalias() if isinstance(sel, E.Alias) else sel
            agg_aliases[alias] = {
                "expression": _agg_to_widget_expression(inner),
                "name": _make_custom_calc_field_name(inner, alias),
            }
            for col in sel.find_all(E.Column):
                tbl_node = col.args.get("table")
                tbl = tbl_node.name if tbl_node else ""
                raw_cols_to_add.append((tbl, col.name))

        existing_col_names: set[str] = set()
        for sel in non_agg_selects:
            if isinstance(sel, E.Alias):
                existing_col_names.add(sel.alias)
            for col in sel.find_all(E.Column):
                existing_col_names.add(col.name)

        for tbl, col_name in raw_cols_to_add:
            if col_name in existing_col_names:
                continue
            ref = f"{tbl}.`{col_name}`" if tbl else f"`{col_name}`"
            try:
                non_agg_selects.append(sqlglot.parse_one(ref, dialect="spark"))
                existing_col_names.add(col_name)
            except Exception:
                pass

        tree.args["expressions"] = non_agg_selects
        tree.args.pop("group", None)
        tree.args.pop("having", None)

        order = tree.args.get("order")
        if order:
            agg_alias_set = set(agg_aliases)
            kept = [
                o for o in order.expressions
                if not any(c.name in agg_alias_set for c in o.find_all(E.Column))
            ]
            if kept:
                order.args["expressions"] = kept
            else:
                tree.args.pop("order")

        ds["query"] = tree.sql(dialect="spark", pretty=True)
        dataset_agg_map[ds_name] = agg_aliases

    if not dataset_agg_map:
        return dashboard_json

    # --- Phase 2: inject custom calculations into widgets -------------------
    for page in pages:
        for item in page.get("layout", []):
            widget = item.get("widget", {})
            if not widget:
                continue

            spec = widget.get("spec", {})
            encodings = spec.get("encodings", {}) if isinstance(spec, dict) else {}

            for wq in widget.get("queries", []):
                query_obj = wq.get("query", {})
                ds_ref = query_obj.get("datasetName", "")

                if ds_ref not in dataset_agg_map:
                    continue

                agg_map = dataset_agg_map[ds_ref]
                modified = False

                for field in query_obj.get("fields", []):
                    old_name = field.get("name", "")
                    expr = field.get("expression", "")
                    bare = expr.strip("`").strip()

                    matched = agg_map.get(bare) or agg_map.get(old_name)
                    if not matched:
                        continue

                    field["name"] = matched["name"]
                    field["expression"] = matched["expression"]
                    _update_encoding_field_name(encodings, old_name, matched["name"])
                    modified = True

                if modified:
                    query_obj["disaggregated"] = False

    return dashboard_json


def _sanitize_widget_columns(dashboard_json: dict) -> dict:
    """Ensure widget field expressions only reference columns that exist in their dataset.

    Parses each dataset SQL with sqlglot to extract output column names,
    then checks every widget field expression for invalid column references.
    Attempts case-insensitive correction when possible; removes unfixable
    fields and their corresponding encodings to prevent broken widgets.
    """
    try:
        import re
        import sqlglot
        from sqlglot import exp as E
    except ImportError:
        return dashboard_json

    datasets = dashboard_json.get("datasets", [])
    pages = dashboard_json.get("pages", [])

    ds_col_map: dict[str, set[str]] = {}
    for ds in datasets:
        ds_name = ds.get("name", "")
        query = ds.get("query", "")
        if not query:
            continue
        try:
            tree = sqlglot.parse_one(query, dialect="spark")
            ds_col_map[ds_name] = {sel.alias_or_name for sel in tree.selects}
        except Exception:
            continue

    if not ds_col_map:
        return dashboard_json

    for page in pages:
        for item in page.get("layout", []):
            widget = item.get("widget", {})
            if not widget:
                continue

            spec = widget.get("spec", {})
            encodings = spec.get("encodings", {}) if isinstance(spec, dict) else {}

            for wq in widget.get("queries", []):
                query_obj = wq.get("query", {})
                ds_ref = query_obj.get("datasetName", "")

                if ds_ref not in ds_col_map:
                    continue

                ds_cols = ds_col_map[ds_ref]
                ds_cols_lower = {c.lower(): c for c in ds_cols}

                fields_to_remove = []
                for field in query_obj.get("fields", []):
                    expr = field.get("expression", "")
                    refs = set(re.findall(r"`([^`]+)`", expr))

                    for ref_col in refs:
                        if ref_col in ds_cols:
                            continue

                        lower_match = ds_cols_lower.get(ref_col.lower())
                        if lower_match:
                            expr = expr.replace(f"`{ref_col}`", f"`{lower_match}`")
                            field["expression"] = expr
                        else:
                            fields_to_remove.append(field)
                            break

                for bad_field in fields_to_remove:
                    old_name = bad_field.get("name", "")
                    query_obj["fields"].remove(bad_field)
                    _update_encoding_field_name(encodings, old_name, "")
                    for enc_key in list(encodings.keys()):
                        enc_val = encodings[enc_key]
                        if isinstance(enc_val, dict) and enc_val.get("fieldName") == "":
                            del encodings[enc_key]
                        elif isinstance(enc_val, list):
                            encodings[enc_key] = [
                                item for item in enc_val
                                if not (isinstance(item, dict) and item.get("fieldName") == "")
                            ]

    return dashboard_json


def _ensure_fqn_tables(dashboard_json: dict) -> dict:
    """Ensure every table reference in dataset SQL uses the 3-level namespace (catalog.schema.table).

    Strategy:
      1. Parse all dataset queries to collect every table reference.
      2. From the 3-part references, infer the most common catalog and schema.
      3. For tables with <3 parts, prepend the missing catalog/schema.
    """
    try:
        import sqlglot
        from sqlglot import exp as E
    except ImportError:
        return dashboard_json

    from collections import Counter

    catalogs: Counter = Counter()
    schemas: Counter = Counter()

    for ds in dashboard_json.get("datasets", []):
        sql = ds.get("query", "")
        if not sql.strip():
            continue
        try:
            tree = sqlglot.parse_one(sql, dialect="spark")
            for tbl in tree.find_all(E.Table):
                if tbl.catalog and tbl.db and tbl.name:
                    catalogs[tbl.catalog] += 1
                    schemas[f"{tbl.catalog}.{tbl.db}"] += 1
        except Exception:
            continue

    if not catalogs:
        return dashboard_json

    default_catalog = catalogs.most_common(1)[0][0]
    default_schema_fqn = schemas.most_common(1)[0][0] if schemas else None
    default_schema = default_schema_fqn.split(".", 1)[1] if default_schema_fqn else None

    for ds in dashboard_json.get("datasets", []):
        sql = ds.get("query", "")
        if not sql.strip():
            continue
        try:
            tree = sqlglot.parse_one(sql, dialect="spark")
            modified = False

            for tbl in tree.find_all(E.Table):
                has_catalog = bool(tbl.catalog)
                has_schema = bool(tbl.db)
                has_table = bool(tbl.name)

                if not has_table:
                    continue

                if has_catalog and has_schema:
                    continue

                if has_schema and not has_catalog:
                    tbl.set("catalog", E.Identifier(this=default_catalog, quoted=True))
                    modified = True
                elif not has_schema and not has_catalog and default_schema:
                    tbl.set("db", E.Identifier(this=default_schema, quoted=True))
                    tbl.set("catalog", E.Identifier(this=default_catalog, quoted=True))
                    modified = True

            if modified:
                ds["query"] = tree.sql(dialect="spark")
        except Exception:
            continue

    return dashboard_json


def fix_dataset_columns(dashboard_json: dict, warehouse_id: str, sp_client) -> dict:
    """Execute each dataset query and auto-fix invalid column references.

    For each dataset SQL:
      1. Try executing with LIMIT 0.
      2. If it fails with a column-not-found error, extract the bad column name.
      3. DESCRIBE the referenced tables to get real columns.
      4. Find the closest match (case-insensitive, then Levenshtein-like).
      5. Replace the bad column in the SQL and retry.

    Retries up to 3 times per dataset to fix multiple bad columns.
    """
    import re

    try:
        from databricks.sdk.service.sql import StatementState
    except ImportError:
        return dashboard_json

    def _run_query(sql: str) -> tuple[bool, str]:
        try:
            stmt = sp_client.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=f"SELECT * FROM ({sql}) AS _t LIMIT 0",
                wait_timeout="30s",
            )
            if stmt.status and stmt.status.state == StatementState.SUCCEEDED:
                return True, ""
            error_msg = stmt.status.error.message if stmt.status and stmt.status.error else "Unknown"
            return False, error_msg
        except Exception as e:
            return False, str(e)

    def _extract_bad_column(error_msg: str) -> str | None:
        for pattern in [
            r"UNRESOLVED_COLUMN\.WITH_SUGGESTION.*?`([^`]+)`",
            r"cannot be resolved.*?`([^`]+)`",
            r"Column '([^']+)' does not exist",
            r"COLUMN_NOT_FOUND.*?`([^`]+)`",
            r"cannot resolve '([^']+)'",
        ]:
            m = re.search(pattern, error_msg, re.IGNORECASE)
            if m:
                return m.group(1)
        return None

    def _best_column_match(bad_col: str, available: list[str]) -> str | None:
        lower_map = {c.lower(): c for c in available}
        if bad_col.lower() in lower_map:
            return lower_map[bad_col.lower()]

        stripped = bad_col.replace("_", "").lower()
        for col in available:
            if col.replace("_", "").lower() == stripped:
                return col

        best, best_score = None, 0
        bad_lower = bad_col.lower()
        for col in available:
            col_lower = col.lower()
            common = sum(1 for a, b in zip(bad_lower, col_lower) if a == b)
            score = common / max(len(bad_lower), len(col_lower))
            if score > best_score and score > 0.6:
                best_score = score
                best = col
        return best

    def _get_table_columns(sql: str) -> dict[str, list[str]]:
        table_cols = {}
        try:
            import sqlglot
            from sqlglot import exp as E
            tree = sqlglot.parse_one(sql, dialect="spark")
            for table in tree.find_all(E.Table):
                parts = []
                if table.catalog:
                    parts.append(table.catalog)
                if table.db:
                    parts.append(table.db)
                parts.append(table.name)
                if len(parts) >= 2:
                    fqn = ".".join(parts)
                    if fqn in table_cols:
                        continue
                    try:
                        stmt = sp_client.statement_execution.execute_statement(
                            warehouse_id=warehouse_id,
                            statement=f"DESCRIBE TABLE {fqn}",
                            wait_timeout="15s",
                        )
                        if stmt.status and stmt.status.state == StatementState.SUCCEEDED:
                            table_cols[fqn] = [
                                row[0] for row in (stmt.result.data_array or [])
                                if row and row[0] and not row[0].startswith("#")
                            ]
                    except Exception:
                        pass
        except Exception:
            pass
        return table_cols

    # Track column renames per dataset: ds_name -> {old_col: new_col}
    dataset_renames: dict[str, dict[str, str]] = {}

    for ds in dashboard_json.get("datasets", []):
        ds_name = ds.get("name", "")
        query = ds.get("query", "")
        if not query.strip():
            continue

        renames: dict[str, str] = {}
        for _attempt in range(5):
            ok, error = _run_query(query)
            if ok:
                break

            bad_col = _extract_bad_column(error)
            if not bad_col:
                break

            all_table_cols = _get_table_columns(query)
            all_available = [c for cols in all_table_cols.values() for c in cols]

            replacement = _best_column_match(bad_col, all_available)
            if not replacement:
                break

            query = re.sub(
                rf"(?<![a-zA-Z0-9_]){re.escape(bad_col)}(?![a-zA-Z0-9_])",
                replacement,
                query,
            )
            ds["query"] = query
            renames[bad_col] = replacement

        if renames:
            dataset_renames[ds_name] = renames

    # Propagate column renames to widget expressions and encodings
    if dataset_renames:
        for page in dashboard_json.get("pages", []):
            for item in page.get("layout", []):
                widget = item.get("widget", {})
                if not widget:
                    continue

                spec = widget.get("spec", {})
                encodings = spec.get("encodings", {}) if isinstance(spec, dict) else {}

                for wq in widget.get("queries", []):
                    query_obj = wq.get("query", {})
                    ds_ref = query_obj.get("datasetName", "")

                    if ds_ref not in dataset_renames:
                        continue

                    col_renames = dataset_renames[ds_ref]

                    for field in query_obj.get("fields", []):
                        expr = field.get("expression", "")
                        name = field.get("name", "")

                        for old_col, new_col in col_renames.items():
                            if f"`{old_col}`" in expr:
                                expr = expr.replace(f"`{old_col}`", f"`{new_col}`")
                                field["expression"] = expr

                            if old_col in name:
                                new_name = name.replace(old_col, new_col)
                                _update_encoding_field_name(encodings, name, new_name)
                                field["name"] = new_name
                                name = new_name

    return dashboard_json


def _format_sql(sql: str) -> str:
    """Format a SQL string for readability using sqlglot."""
    try:
        import sqlglot
        formatted = sqlglot.transpile(sql, read="spark", write="spark", pretty=True)
        if formatted:
            return formatted[0]
    except Exception:
        pass
    return sql


def _format_dashboard_sql(dashboard_json: dict) -> dict:
    """Format all dataset SQL queries in the dashboard for readability."""
    for ds in dashboard_json.get("datasets", []):
        if "query" in ds and ds["query"].strip():
            ds["query"] = _format_sql(ds["query"])
        elif "queryLines" in ds:
            raw = " ".join(ds["queryLines"])
            if raw.strip():
                ds["query"] = _format_sql(raw)
                ds.pop("queryLines", None)
    return dashboard_json


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

    dashboard = json.loads(text)
    dashboard = _promote_aggregations_to_custom_calcs(dashboard)
    dashboard = _sanitize_widget_columns(dashboard)
    return _format_dashboard_sql(dashboard)
