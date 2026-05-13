"""Single source of truth for PBI -> AI/BI widget type mapping.

Previously this dict was duplicated in `converter.py` and `validator.py`,
which means a new PBI visualType added in one place but not the other
would silently flag every visual of that type as "unmapped" (because
`find_unmapped_visuals` checks one copy and `validate_layout_fidelity`
checks the other). Owning the mapping here forces both sides to drift
together.

Keys are Power BI's `visualType` strings as they appear in
`{Report}.Report/visuals/*/visual.json`. Values are sets of acceptable
AI/BI widget types — Lakeview's `widgetType` field — so the matching
logic in both modules can do `widget_type in PBI_TO_AIBI_TYPE_MAP[v]`.
"""

PBI_TO_AIBI_TYPE_MAP: dict[str, set[str]] = {
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
    "slicer": {
        "filter-multi-select",
        "filter-single-select",
        "filter-date-range-picker",
    },
}


# PBI visual types we deliberately route through the alternatives panel
# even when the auto-converter produced a substitute, because the
# substitute is always a downgrade (a gauge as a counter loses the dial,
# a treemap as a bar loses the area encoding, etc). Listing these
# centrally keeps the "what's lossy" definition out of the matching
# logic.
LOSSY_PBI_TYPES: set[str] = {"gauge", "treemap", "funnel", "waterfallChart"}
