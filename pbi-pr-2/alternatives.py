"""
LLM-driven alternative suggestions for PBI visuals that don't cleanly
map to a supported AI/BI widget.

Each alternative is a full widget dict (spec + queries) plus metadata
(rationale, explicit style attributes: font size, colors, frame title,
grid size). A "skip" sentinel is always appended.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any

from clients import MODEL, get_llm_client


@dataclass
class VisualAlternative:
    """A single candidate replacement for a PBI visual.

    kind: "widget" for a real AIBI widget, "skip" to omit the visual.
    """
    kind: str
    label: str
    rationale: str
    widget: dict = field(default_factory=dict)
    attributes: dict = field(default_factory=dict)


def make_skip_option() -> VisualAlternative:
    return VisualAlternative(
        kind="skip",
        label="Skip this visual",
        rationale="Omit this PBI visual entirely from the AI/BI dashboard.",
    )


ALTERNATIVES_SYSTEM_PROMPT = """\
You suggest Databricks AI/BI widget alternatives for a single Power BI
visual that has no direct mapping (e.g. maps, gauges, decomposition
trees, custom visuals, KPIs with sparklines, R/Python visuals).

Return JSON only, no prose. Use this exact schema:

{
  "alternatives": [
    {
      "label": "short human-readable title",
      "rationale": "one sentence explaining why this represents the PBI visual",
      "widget": { ... complete AI/BI widget JSON ... },
      "attributes": {
        "font_size_px": 14,
        "title_font_size_px": 18,
        "title": "string shown as the tile title",
        "primary_color": "#118DFF",
        "palette": ["#118DFF", "#12239E", "#E66C37"],
        "grid_width": 3,
        "grid_height": 4,
        "show_legend": true,
        "show_title": true
      }
    }
  ]
}

Hard rules:
1. Produce 3-4 alternatives. When possible each uses a DIFFERENT
   widgetType (e.g. table, bar, counter, pie, line, filter-multi-select,
   or a multilineTextboxSpec summary).
2. Valid widget versions: counter / table / filter-* = 2; bar / line /
   pie / area / scatter = 3. Text widgets use `multilineTextboxSpec`
   with no `spec` block.
3. `fields[].name` MUST exactly equal `encodings.fieldName` for every
   encoding reference in the widget.
4. Always include the `attributes` object with EXPLICIT numeric values
   (font_size_px as int, grid_width in 1..6, grid_height >= 2, colors
   as "#RRGGBB").
5. COLUMN NAMES — STRICT.

   Use ONLY column names that appear in the dataset's `columns` array
   (when provided). If `columns` is not provided for a dataset, pick
   columns that clearly exist in `query_preview`. NEVER invent column
   names like `total_sales`, `total_quantity_sold`, `revenue`, etc.
   if they are not in `columns`.

   Aliases work like this. Each entry in `fields` has a `name` (the
   alias used inside the widget) and an `expression` (the SQL evaluated
   against the dataset, which MUST wrap a real column). The encoding's
   `fieldName` then refers to the `name`, NOT to a made-up column.

   Concrete example, given dataset.columns = ["product", "quantity",
   "totalPrice", "country"]:

   GOOD (alias `total_quantity` is defined as SUM of the real `quantity`):
   {
     "queries": [{"name": "q", "query": {
       "datasetName": "<ds>",
       "fields": [
         {"name": "product",        "expression": "`product`"},
         {"name": "total_quantity", "expression": "SUM(`quantity`)"}
       ]
     }}],
     "spec": {"widgetType": "bar", "encodings": {
       "x": {"fieldName": "product"},
       "y": {"fieldName": "total_quantity"}
     }}
   }

   BAD (encoded `total_quantity` but never defined as a field — the
   warehouse will reject the query because there is no `total_quantity`
   column):
   {
     "queries": [{"name": "q", "query": {"datasetName": "<ds>"}}],
     "spec": {"widgetType": "bar", "encodings": {
       "x": {"fieldName": "product"},
       "y": {"fieldName": "total_quantity"}
     }}
   }
6. For map / geo visuals: include at least one `table` option grouped
   by location with counts/sums, and one `bar` option of location vs
   metric.
7. For KPI / gauge visuals: include at least one `counter` with
   explicit numeric formatting.
8. Wrap each SQL column reference in backticks inside `expression`,
   e.g. "SUM(`amount`)".
"""


def _strip_json_fences(raw: str) -> str:
    """Remove ```json ... ``` fences if present."""
    raw = raw.strip()
    fence = re.match(r"^```(?:json)?\s*(.*?)\s*```$", raw, re.DOTALL)
    if fence:
        return fence.group(1).strip()
    return raw


def _extract_first_json_object(raw: str) -> str:
    """Find the first balanced {...} block in a string."""
    depth = 0
    start = -1
    for i, ch in enumerate(raw):
        if ch == "{":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start >= 0:
                return raw[start : i + 1]
    return raw


def _build_user_prompt(pbi_visual, dataset_summaries: list[dict],
                       report_context: str = "") -> str:
    name = pbi_visual.display_name or pbi_visual.visual_id
    ds_block = json.dumps(dataset_summaries, indent=2)
    ctx = (report_context or "")[:2000]
    return (
        "PBI visual that needs an alternative:\n"
        f"- visual_id: {pbi_visual.visual_id}\n"
        f"- visual_type: {pbi_visual.visual_type}\n"
        f"- display_name: {name}\n"
        f"- grid size in PBI: x={pbi_visual.grid_x}, y={pbi_visual.grid_y}, "
        f"w={pbi_visual.grid_width}, h={pbi_visual.grid_height}\n\n"
        f"Available datasets (pick ONE per alternative, use its exact columns):\n"
        f"{ds_block}\n\n"
        f"Extra context from the report (optional):\n{ctx}\n\n"
        "Return JSON matching the schema in the system prompt."
    )


def suggest_alternatives(pbi_visual,
                          dataset_summaries: list[dict],
                          report_context: str = "",
                          temperature: float = 0.2,
                          ) -> list[VisualAlternative]:
    """Call the LLM for alternatives. Always appends a SKIP option."""
    client = get_llm_client()
    try:
        resp = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": ALTERNATIVES_SYSTEM_PROMPT},
                {"role": "user", "content":
                 _build_user_prompt(pbi_visual, dataset_summaries, report_context)},
            ],
            temperature=temperature,
        )
        raw = resp.choices[0].message.content or ""
    except Exception as e:
        return [
            VisualAlternative(
                kind="widget",
                label="Fallback text summary",
                rationale=f"LLM call failed ({e}); using a neutral text placeholder.",
                widget=_fallback_text_widget(pbi_visual),
                attributes={
                    "font_size_px": 14, "title_font_size_px": 16,
                    "title": pbi_visual.display_name or "Visual placeholder",
                    "grid_width": max(2, pbi_visual.grid_width),
                    "grid_height": max(2, pbi_visual.grid_height),
                },
            ),
            make_skip_option(),
        ]

    try:
        body = _extract_first_json_object(_strip_json_fences(raw))
        data = json.loads(body)
    except Exception:
        data = {"alternatives": []}

    alts: list[VisualAlternative] = []
    for item in (data.get("alternatives") or [])[:5]:
        if not isinstance(item, dict):
            continue
        widget = item.get("widget") or {}
        attrs = item.get("attributes") or {}
        alts.append(VisualAlternative(
            kind="widget",
            label=str(item.get("label", "Alternative")),
            rationale=str(item.get("rationale", "")),
            widget=widget,
            attributes=attrs,
        ))

    if not alts:
        alts.append(VisualAlternative(
            kind="widget",
            label="Fallback text summary",
            rationale="Model returned no usable alternatives; inserting a text placeholder.",
            widget=_fallback_text_widget(pbi_visual),
            attributes={
                "font_size_px": 14, "title_font_size_px": 16,
                "title": pbi_visual.display_name or "Visual placeholder",
                "grid_width": max(2, pbi_visual.grid_width),
                "grid_height": 2,
            },
        ))

    alts.append(make_skip_option())
    return alts


def _fallback_text_widget(pbi_visual) -> dict:
    """Deterministic text widget used when the LLM can't propose anything usable."""
    name = pbi_visual.display_name or f"visual_{pbi_visual.visual_id[:8]}"
    return {
        "name": f"fallback_{pbi_visual.visual_id[:8]}",
        "multilineTextboxSpec": {
            "lines": [
                f"### {name}",
                "",
                f"_Original PBI visual type `{pbi_visual.visual_type}` has no direct AI/BI equivalent._",
            ],
        },
    }


def apply_attributes_to_widget(widget: dict, attributes: dict) -> dict:
    """Stamp explicit styling attributes into the widget's spec.

    Covers: title text, title font size, body font size, palette (categorical
    color range), and whether the title is shown. Best-effort; keys that
    don't apply to a given widget type are ignored.
    """
    if not attributes or not widget:
        return widget

    if "multilineTextboxSpec" in widget:
        size = attributes.get("font_size_px")
        title = attributes.get("title")
        if title and "lines" in widget["multilineTextboxSpec"]:
            lines = widget["multilineTextboxSpec"]["lines"]
            if lines and not lines[0].lstrip().startswith("#"):
                widget["multilineTextboxSpec"]["lines"] = [f"### {title}"] + lines
        if size:
            widget["multilineTextboxSpec"].setdefault("fontSize", size)
        return widget

    spec = widget.setdefault("spec", {})
    frame = spec.setdefault("frame", {})

    if "title" in attributes and attributes["title"]:
        frame["title"] = attributes["title"]
    frame.setdefault("showTitle", bool(attributes.get("show_title", True)))

    title_size = attributes.get("title_font_size_px")
    if title_size:
        frame.setdefault("titleFont", {})["size"] = int(title_size)

    palette = attributes.get("palette")
    if palette and isinstance(palette, list):
        encodings = spec.get("encodings", {})
        color_enc = encodings.get("color")
        if isinstance(color_enc, dict):
            scale = color_enc.setdefault("scale", {"type": "categorical"})
            scale.setdefault("range", palette)

    # Apply primary brand color via the right channel for each widget type.
    # CRITICAL: do NOT stamp primary on measure encodings' scale.range
    # (e.g. y.scale.range, value.scale.range). AI/BI treats `scale.range`
    # on a quantitative measure as a malformed encoding and renders the
    # widget as a "Select fields to visualize" placeholder. The correct
    # channels are:
    #   * counter / bar / line / area / scatter (single-series): mark.color
    #   * pie / donut: mark.colors (a list)
    #   * categorical-color charts: encodings.color.scale.range
    primary = attributes.get("primary_color")
    if primary:
        wtype = spec.get("widgetType", "")
        encodings = spec.get("encodings", {})
        color_enc = encodings.get("color") if isinstance(encodings, dict) else None

        if isinstance(color_enc, dict):
            # Categorical color encoding owns the palette. Honour the
            # author's full palette if provided, otherwise fall back to a
            # single-color range.
            scale = color_enc.setdefault("scale", {"type": "categorical"})
            palette_attr = attributes.get("palette")
            if isinstance(palette_attr, list) and palette_attr:
                scale.setdefault("range", list(palette_attr))
            else:
                scale.setdefault("range", [primary])
        elif wtype in {"pie", "donut"}:
            mark = spec.setdefault("mark", {})
            palette_attr = attributes.get("palette")
            if isinstance(palette_attr, list) and palette_attr:
                mark.setdefault("colors", list(palette_attr))
            else:
                mark.setdefault("colors", [primary])
        else:
            # Counter / single-series bar / line / area / scatter — a
            # single mark.color is the right channel.
            mark = spec.setdefault("mark", {})
            mark.setdefault("color", primary)

    return widget
