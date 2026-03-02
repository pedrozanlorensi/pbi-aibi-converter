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
3. **Text widgets**: ONLY create text widgets if the PBI report contains explicit `textbox` visuals with real text. NEVER create blank, empty, or placeholder text widgets. NEVER invent headers, titles, or spacers that don't exist in the PBI source.
4. **Filter widgets**: Must use `filter-multi-select`, `filter-single-select`, or `filter-date-range-picker`. NEVER use `widgetType: "filter"`. Place them on the same canvas page as the visuals they filter (page-level), unless explicitly told to use a global filters page.
5. **Layout**: 6-column grid, every row must sum to width=6. Use column-skyline packing — each widget's y is determined by the columns it occupies, so short widgets stack tightly next to taller ones with zero blank space.
6. **SQL**: Use Spark SQL syntax. Use `date_sub()` not `DATEADD()`. Use fully-qualified table names: `catalog.schema.table`.
7. **Disaggregated flag**: Use `false` for aggregating widgets (counters with SUM/AVG, charts), `true` for pre-aggregated single-row datasets.

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


def parse_pbi_layout(report_dir: str) -> PbiLayout:
    """Parse PBI report into a structured layout with computed grid positions.

    Reads pages.json for ordering, each page.json for dimensions, and each
    visual.json for type and pixel position. Converts pixel positions to AIBI
    6-column grid coordinates and classifies visuals as data/slicer/decorative.
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
        raw_visuals = []
        for vis_path in visual_paths:
            with open(vis_path, "r") as f:
                vis_data = json.load(f)

            vis_id = vis_data.get("name", os.path.basename(os.path.dirname(vis_path)))
            vis_inner = vis_data.get("visual", {})
            vis_type = vis_inner.get("visualType", "unknown")
            pos = vis_data.get("position", {})

            pbi_x = pos.get("x", 0)
            pbi_y = pos.get("y", 0)
            pbi_w = pos.get("width", 100)
            pbi_h = pos.get("height", 100)

            is_slicer = vis_type in SLICER_TYPES
            slicer_field = _extract_slicer_field(vis_data) if is_slicer else None

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
# LLM Interaction
# ---------------------------------------------------------------------------


def call_llm(report_name: str, pbi_context: str, layout_blueprint: str = "") -> str:
    """Send the PBI context to the LLM and return the raw response text."""
    client = get_llm_client()

    blueprint_section = ""
    if layout_blueprint:
        blueprint_section = f"""

{layout_blueprint}
"""

    user_message = f"""Convert this Power BI report named "{report_name}" to a Databricks AI/BI dashboard (.lvdash.json).

## Power BI Report Contents

{pbi_context}
{blueprint_section}
## Instructions

1. Extract the data source catalog/schema/table from the .tmdl partition blocks
2. Build SQL dataset(s) that JOIN the needed tables using fully-qualified names
3. Convert every visual to the appropriate AI/BI widget type
4. Convert page-level slicers to filter widgets placed directly on their respective PAGE_TYPE_CANVAS page. Only create a PAGE_TYPE_GLOBAL_FILTERS page if the LAYOUT BLUEPRINT above explicitly says to (for global slicers present on every page).
5. Skip decorative shapes
6. Use proper 6-column grid layout with no gaps
7. Ensure all field names in query.fields match fieldNames in encodings exactly
8. **CRITICAL: Follow the LAYOUT BLUEPRINT above exactly — same number of pages, same visuals, same approximate positions**

Return ONLY the JSON — no markdown fences, no explanation."""

    response = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": _get_system_prompt()},
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
