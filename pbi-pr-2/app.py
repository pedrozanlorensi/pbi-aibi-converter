"""
Power BI to Databricks AI/BI Dashboard Converter — Streamlit entrypoint.

Two-phase flow:
  1. `generate_draft` runs parse + LLM + column-fix and produces a DRAFT
     dashboard_json plus a list of unmapped visuals. The draft is rendered
     as a live preview with per-widget shuffle / skip controls.
  2. `publish_draft` (triggered by a button) runs validation, creates the
     dashboard in Databricks, and publishes it.
"""

import copy as _copy
import hashlib
import json
import logging
import os
import traceback
from dataclasses import dataclass, field
from itertools import groupby
from typing import Any

import streamlit as st

logger = logging.getLogger("pbi_aibi_converter")
if not logger.handlers:
    logger.setLevel(logging.INFO)
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import Dashboard
from databricks.sdk.service.iam import (
    AccessControlRequest,
    PermissionLevel,
)

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
    apply_brand_colors,
    apply_widget_name_transforms,
    should_use_free_layout,
    _ensure_fqn_tables,
    fix_dataset_columns,
    _estimate_tokens,
    MAX_PROMPT_TOKENS,
    find_unmapped_visuals,
    summarize_datasets,
)
from validator import validate_dashboard, validate_layout_fidelity, validate_table_coverage
from alternatives import (
    VisualAlternative,
    suggest_alternatives,
    apply_attributes_to_widget,
)
from color_utils import normalize_render_colors

# ---------------------------------------------------------------------------
# Page config (must be the first Streamlit command)
# ---------------------------------------------------------------------------

st.set_page_config(page_title="PBI to AI/BI Converter", page_icon=":bar_chart:", layout="wide")
st.markdown(
    " div[data-testid='stFileUploader'] small {display:none} ",
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Session-state helpers
# ---------------------------------------------------------------------------

@dataclass
class ReportResult:
    """All output data for a single converted report (draft or published)."""
    name: str = ""
    status: str = "pending"  # pending | running | draft | done | error
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
    pbi_layout: Any = None
    pbi_context: str = ""
    pbi_source_tables: list = field(default_factory=list)
    warehouse_id: str = ""
    unmapped: list = field(default_factory=list)
    alt_cache: dict = field(default_factory=dict)
    instruction_verdict: Any = None
    custom_instructions: str = ""
    # Per-dataset column lists, populated lazily by _probe_dataset_columns().
    # Used to (a) feed the LLM real column names in the alternatives prompt
    # so it stops inventing columns like `total_quantity_sold`, and
    # (b) validate synthesized query.fields before they hit the warehouse.
    dataset_columns: dict = field(default_factory=dict)


if "results" not in st.session_state:
    st.session_state["results"] = []
if "batch_running" not in st.session_state:
    st.session_state["batch_running"] = False

# ---------------------------------------------------------------------------
# On-behalf-of-user (OBO) auth helpers
# ---------------------------------------------------------------------------

def _user_token() -> str | None:
    """Return the OBO user access token forwarded by Databricks Apps, if any."""
    try:
        return st.context.headers.get("x-forwarded-access-token")
    except Exception:
        return None


def _user_email() -> str | None:
    try:
        return st.context.headers.get("x-forwarded-email")
    except Exception:
        return None


def _user_principal() -> str | None:
    """The X-Forwarded-User header value (typically the user's UPN)."""
    try:
        return st.context.headers.get("x-forwarded-user")
    except Exception:
        return None


def _sp_client() -> WorkspaceClient:
    """The app's service principal client (auto from env).

    Built with `auth_type="oauth-m2m"` so the SDK does not also try to
    pick up a stray DATABRICKS_TOKEN / DATABRICKS_PASSWORD that may be
    visible in the process. Avoids the "more than one authorization
    method configured" surprise.
    """
    from databricks.sdk.core import Config
    cfg = Config(auth_type="oauth-m2m")
    return WorkspaceClient(config=cfg)


def _user_client() -> WorkspaceClient | None:
    """A WorkspaceClient that authenticates as the signed-in user (OBO).

    Returns None when OBO is not active for the app (e.g. running locally
    or workspace admin has not enabled the preview feature).

    PREVIOUSLY this function popped DATABRICKS_CLIENT_ID/SECRET from
    `os.environ` while constructing the client and restored them after.
    That was a footgun under concurrent Streamlit sessions / threads — a
    second request entering `_sp_client()` during the pop window would
    observe missing SP credentials and authenticate as something
    unintended, or fail noisily.

    The race-free version below configures the SDK directly with
    `Config(auth_type="pat", host=..., token=...)`, which short-circuits
    the SDK's auth-resolver chain entirely. No global state is mutated.
    """
    tok = _user_token()
    if not tok:
        return None
    from databricks.sdk.core import Config
    sp_cfg = _sp_client().config
    cfg = Config(host=sp_cfg.host, token=tok, auth_type="pat")
    return WorkspaceClient(config=cfg)


def _work_client() -> WorkspaceClient:
    """OBO client if available, otherwise the SP client."""
    return _user_client() or _sp_client()


def _obo_status() -> str:
    """Tell the UI which auth path will actually be used for the next API call.

    Returns:
        "active"  — OBO token present and `current_user.me()` succeeds.
        "expired" — OBO token present but the API rejected it with 401/403
                    (most common cause: the user's session sat idle long
                    enough for the forwarded token to age out).
        "unknown" — OBO token present but the probe call failed for a
                    non-auth reason (network blip, workspace 5xx). The
                    UI should NOT push the user to re-sign-in in this
                    case — that wouldn't fix anything.
        "absent"  — no OBO token at all (running locally, or OBO is
                    disabled at the org level).
    """
    tok = _user_token()
    if not tok:
        return "absent"
    client = _user_client()
    if client is None:
        return "absent"
    try:
        client.current_user.me()
        return "active"
    except Exception as e:
        msg = str(e).lower()
        if any(s in msg for s in ("401", "403", "unauthorized", "forbidden",
                                  "invalid token", "expired")):
            return "expired"
        logger.warning("OBO probe failed for non-auth reason: %s", e)
        return "unknown"


def _current_user_cache_key() -> str:
    """Stable per-user cache key for @st.cache_data on data-plane calls.

    Streamlit's @cache_data is process-wide; without a user-bound
    component in the cache key, two signed-in users hitting the same
    function with the same args would share results. For SQL execution
    that means user A could observe user B's preview rows.

    We hash the user's OBO token (preferred) or x-forwarded-email so the
    raw token never lives in cache-key space. Anonymous (local dev) gets
    a stable "anon" key, which is fine because there's only one caller.
    """
    tok = _user_token()
    if tok:
        return "tok:" + hashlib.sha256(tok.encode("utf-8")).hexdigest()[:16]
    email = _user_email()
    if email:
        return "u:" + hashlib.sha256(email.encode("utf-8")).hexdigest()[:16]
    return "anon"


# ---------------------------------------------------------------------------
# Phase 1-3: generate a DRAFT (no deploy yet)
# ---------------------------------------------------------------------------

def generate_draft(
    uploaded_file,
    report_name: str,
    progress,
    warehouse_id: str,
    custom_instructions: str = "",
    preserve_colors: bool = True,
) -> ReportResult:
    """Parse PBI -> LLM convert -> FQN + column fix. Stops before validation/deploy.

    `warehouse_id` is the user-selected SQL warehouse (passed in from the UI).
    All warehouse operations are performed on the user's behalf when OBO is
    active; otherwise the app SP is used.
    """
    result = ReportResult(name=report_name, status="running")

    try:
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

        external_sources = [
            s for s in data_sources
            if not s["is_databricks"] and s["source_type"] != "Calculated (PBI)"
        ]
        result.external_sources = external_sources
        if external_sources:
            unique_types = sorted({s["source_type"] for s in external_sources})
            progress.write(
                f"Warning: {len(external_sources)} table(s) from external sources: {', '.join(unique_types)}"
            )

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

        if custom_instructions and custom_instructions.strip():
            progress.write(f"Custom instructions included ({len(custom_instructions)} chars)")

        est_tokens = _estimate_tokens(pbi_context + layout_blueprint + color_context)
        progress.write(f"Estimated context size: ~{est_tokens:,} tokens")

        use_chunked = est_tokens > MAX_PROMPT_TOKENS
        if use_chunked:
            progress.write("Context exceeds limit — using multi-turn chunked mode")
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
            raw_response = call_llm(
                report_name, pbi_context, layout_blueprint,
                color_context=color_context, custom_instructions=custom_instructions,
            )

        progress.write("Parsing dashboard JSON...")
        dashboard_json = extract_json_from_response(raw_response)

        if not warehouse_id:
            result.status = "error"
            result.error_msg = "No SQL warehouse selected."
            return result

        work_client = _work_client()
        auth_label = "you (OBO)" if _user_client() is not None else "the app SP"
        progress.write(f"Warehouse operations will run as {auth_label}.")

        progress.write("Ensuring fully-qualified table names...")
        dashboard_json = _ensure_fqn_tables(dashboard_json)

        if custom_instructions and custom_instructions.strip():
            before = json.dumps(dashboard_json, sort_keys=True)
            dashboard_json = apply_widget_name_transforms(
                dashboard_json, custom_instructions
            )
            after = json.dumps(dashboard_json, sort_keys=True)
            if before != after:
                progress.write(
                    "Applied widget rename transform from custom instructions."
                )

        free_layout = should_use_free_layout(pbi_layout)
        colored_visuals = [
            v for p in pbi_layout.pages for v in p.visuals if v.colors
        ]
        if preserve_colors and colored_visuals:
            progress.write(
                f"Injecting brand colors from {len(colored_visuals)} PBI visual(s)..."
            )
            dashboard_json = apply_brand_colors(
                dashboard_json,
                pbi_layout,
                warehouse_id=warehouse_id,
                sp_client=work_client,
                free_layout=free_layout,
            )
        elif preserve_colors:
            progress.write(
                "No per-visual brand colors found in PBI report — "
                "using Databricks defaults."
            )
        else:
            progress.write(
                "Brand color preservation disabled — using Databricks defaults."
            )

        progress.write("Checking dataset SQL against UC tables...")
        dashboard_json = fix_dataset_columns(dashboard_json, warehouse_id, work_client)

        if preserve_colors:
            progress.write("Normalizing chart colors for AI/BI renderer...")
            dashboard_json = normalize_render_colors(
                dashboard_json, sp_client=work_client, warehouse_id=warehouse_id
            )

        progress.write("Detecting visuals that need manual review...")
        unmapped = find_unmapped_visuals(pbi_layout, dashboard_json)

        result.dashboard_json = dashboard_json
        result.pbi_layout = pbi_layout
        result.pbi_context = pbi_context
        result.pbi_source_tables = pbi_source_tables
        result.warehouse_id = warehouse_id
        result.unmapped = unmapped
        result.custom_instructions = custom_instructions or ""
        result.n_datasets = len(dashboard_json.get("datasets", []))
        result.n_pages = len(dashboard_json.get("pages", []))
        result.n_widgets = sum(len(p.get("layout", [])) for p in dashboard_json.get("pages", []))

        progress.write(
            f"Draft ready: {result.n_datasets} datasets, {result.n_pages} pages, "
            f"{result.n_widgets} widgets, {len(unmapped)} flagged for review."
        )
        result.status = "draft"

    except json.JSONDecodeError as e:
        result.status = "error"
        result.error_msg = f"LLM returned invalid JSON: {e}"
        result.raw_traceback = traceback.format_exc()
    except Exception as e:
        result.status = "error"
        result.error_msg = f"Draft generation failed: {e}"
        result.raw_traceback = traceback.format_exc()

    return result


# ---------------------------------------------------------------------------
# Phase 4-5: validate + deploy + publish
# ---------------------------------------------------------------------------

def _is_already_exists_error(err: Exception) -> bool:
    """Heuristic for 'already exists' on lakeview.create.

    The Databricks SDK raises `DatabricksError` with `error_code` set to
    `ALREADY_EXISTS` (or HTTP 409). We prefer the structured field over
    string matching the message, but fall back to the message form for
    older SDK builds whose `DatabricksError` doesn't expose `error_code`.
    """
    code = getattr(err, "error_code", None)
    if code in ("ALREADY_EXISTS", "RESOURCE_ALREADY_EXISTS"):
        return True
    status = getattr(err, "status_code", None) or getattr(err, "status", None)
    if status == 409:
        return True
    return "already exists" in str(err).lower()


def _try_create_dashboard(client: WorkspaceClient, dashboard_obj: Dashboard,
                           parent_path: str, report_name: str):
    """Create the Lakeview dashboard with `client`; on a true 'already
    exists' error, find the colliding dashboard *in the same parent_path*
    and update it. Raises on any other failure.

    Match strategy: scope `lakeview.list()` to the same parent_path AND
    display_name. Two reports can legitimately share a display_name in
    different folders; in v4 this code matched by display_name only and
    could have updated the wrong dashboard. We never blindly call
    `workspace.delete` on the .lvdash.json file — if the create failed
    AND we can't find a colliding dashboard in the expected path, we
    surface the original error so the operator can investigate.
    """
    try:
        return client.lakeview.create(dashboard=dashboard_obj)
    except Exception as create_err:
        if not _is_already_exists_error(create_err):
            raise
        existing_id = None
        try:
            for d in client.lakeview.list():
                if (d.display_name == report_name
                        and getattr(d, "parent_path", None) == parent_path):
                    existing_id = d.dashboard_id
                    break
        except Exception as list_err:
            logger.warning("lakeview.list() failed during collision recovery: %s",
                           list_err)
            raise create_err
        if not existing_id:
            # Don't blow away an unrelated workspace file. Re-raise so the
            # operator sees the original 'already exists' and can
            # manually investigate the collision.
            raise create_err
        return client.lakeview.update(
            dashboard_id=existing_id, dashboard=dashboard_obj,
        )


def _transfer_dashboard_ownership_to_user(
    client: WorkspaceClient,
    dashboard_id: str,
    user_email: str,
    progress,
) -> bool:
    """Make `user_email` the owner of the Lakeview dashboard.

    Uses `permissions.set` (NOT `update`) so the call is authoritative —
    any previous SP-owner entry is replaced. We ask for CAN_MANAGE for
    the user; Lakeview translates the top permission into IS_OWNER when
    the caller is the current owner (the SP, which just created it).
    """
    try:
        client.permissions.set(
            request_object_type="dashboards",
            request_object_id=dashboard_id,
            access_control_list=[
                AccessControlRequest(
                    user_name=user_email,
                    permission_level=PermissionLevel.CAN_MANAGE,
                )
            ],
        )
        progress.write(f"Transferred dashboard control to {user_email}.")
        return True
    except Exception as e:
        progress.write(f"Could not transfer dashboard ownership to {user_email}: {e}")
        return False


def publish_draft(result: ReportResult, progress) -> ReportResult:
    """Validate the draft, deploy, publish, and build the PDF export.

    Auth flow:
      * `lakeview.create` and `lakeview.publish` run as the app's
        service principal (SP). These two endpoints are gated on a
        `dashboards` API scope that is not exposed in the OBO scope
        catalog today, so OBO can't make these calls. The SP creates
        the dashboard, ownership transfers to the signed-in user
        immediately, and publish runs with `embed_credentials=False`
        so every viewer queries the warehouse with their own identity.
        The SP is on the dashboard-create path for ~one round trip and
        never touches the runtime query path.
      * Everything else (warehouse list, SQL previews, schema probing,
        Genie space create, color validation) runs as the user via
        OBO when available.
    """
    try:
        sp_client = _sp_client()
        user_client = _user_client()
        user_email = _user_email()

        dashboard_json = result.dashboard_json
        warehouse_id = result.warehouse_id
        report_name = result.name

        work_client = user_client or sp_client

        # Re-apply brand colors and renderer normalization so any
        # alternatives the user accepted *after* generate_draft (which
        # were inserted with raw spec.encodings.color but never went
        # through the AI/BI renderer fix-up) carry their colors all the
        # way to the published dashboard. Pies and single-series
        # bar/line charts especially need normalize_render_colors or
        # AI/BI silently falls back to default colors at render time.
        if result.pbi_layout is not None:
            try:
                free_layout = should_use_free_layout(result.pbi_layout)
                colored_visuals = [
                    v for p in result.pbi_layout.pages
                    for v in p.visuals if getattr(v, "colors", None)
                ]
                if colored_visuals:
                    progress.write("Re-applying brand colors to accepted alternatives...")
                    dashboard_json = apply_brand_colors(
                        dashboard_json,
                        result.pbi_layout,
                        warehouse_id=warehouse_id,
                        sp_client=work_client,
                        free_layout=free_layout,
                    )
                progress.write("Normalizing chart colors for AI/BI renderer...")
                dashboard_json = normalize_render_colors(
                    dashboard_json, sp_client=work_client, warehouse_id=warehouse_id
                )
                result.dashboard_json = dashboard_json
            except Exception as color_err:
                progress.write(
                    f"(non-fatal) Could not re-normalize colors before publish: {color_err}"
                )

        progress.write("Validating dashboard...")
        validation = validate_dashboard(dashboard_json, warehouse_id, work_client)

        progress.write("Validating layout fidelity against PBI source...")
        layout_fidelity = validate_layout_fidelity(dashboard_json, result.pbi_layout)
        validation.layout_fidelity = layout_fidelity
        result.layout_fidelity = layout_fidelity

        progress.write("Validating table coverage...")
        table_coverage = validate_table_coverage(dashboard_json, result.pbi_source_tables)
        validation.table_coverage = table_coverage
        result.validation = validation
        result.n_canvas = layout_fidelity.actual_pages

        progress.write("Deploying to Databricks workspace...")
        parent_root = (os.getenv("DASHBOARD_PARENT_PATH") or "/Workspace/Shared/aibi_converter").rstrip("/")
        parent_path = f"{parent_root}/{report_name}"
        try:
            work_client.workspace.mkdirs(parent_path)
        except Exception:
            sp_client.workspace.mkdirs(parent_path)

        serialized = json.dumps(dashboard_json, indent=2)
        dashboard_obj = Dashboard(
            display_name=report_name,
            parent_path=parent_path,
            serialized_dashboard=serialized,
            warehouse_id=warehouse_id,
        )

        # Lakeview create + publish are not reachable over OBO today: the
        # API gates them on a `dashboards` scope that is not exposed in
        # the OBO scope catalog (the CLI rejects it as invalid). So we
        # call them with the app's service principal, then transfer
        # ownership to the user immediately. The SP only sits on these
        # two API calls — every query the published dashboard runs goes
        # through the viewer's identity (embed_credentials=False below),
        # and every other Databricks call in this app (warehouse list,
        # SQL previews, schema probing, Genie space create) still uses
        # the user's OBO token.
        progress.write("Creating dashboard...")
        api_result = _try_create_dashboard(
            sp_client, dashboard_obj, parent_path, report_name,
        )

        result.dashboard_id = api_result.dashboard_id
        host = sp_client.config.host.rstrip("/")
        result.dash_url = f"{host}/sql/dashboardsv3/{result.dashboard_id}"
        result.workspace_path = f"{parent_path}/{report_name}.lvdash.json"

        # Ownership transfer is not optional. If it fails the dashboard
        # is owned by the SP, which is the wrong end-state — the user
        # would be unable to share/edit it without first being granted
        # by an admin. Treat the transfer failure as a publish failure
        # so the operator can investigate, rather than silently leaving
        # the user without ownership.
        if user_email:
            ok = _transfer_dashboard_ownership_to_user(
                sp_client, result.dashboard_id, user_email, progress,
            )
            if not ok:
                raise RuntimeError(
                    f"Dashboard {result.dashboard_id} was created by the "
                    f"app SP but ownership transfer to {user_email} "
                    "failed. The dashboard exists in the workspace but "
                    "you are not the owner. Either delete the dashboard "
                    "and retry, or have a workspace admin run "
                    f"`databricks permissions update dashboards "
                    f"{result.dashboard_id} --json '{{\"access_control_list\""
                    f":[{{\"user_name\":\"{user_email}\","
                    "\"permission_level\":\"CAN_MANAGE\"}}]}}'`."
                )

        # Publish with embed_credentials=False so every viewer (you)
        # queries the warehouse as themselves. The SP is not on the
        # runtime path and needs zero warehouse grant.
        progress.write("Publishing dashboard (run-as-viewer)...")
        try:
            sp_client.lakeview.publish(
                dashboard_id=result.dashboard_id,
                warehouse_id=warehouse_id,
                embed_credentials=False,
            )
        except TypeError:
            # Older SDK without the `embed_credentials` keyword — publish
            # without it and rely on the explicit ownership transfer
            # above to move the "run as" identity to the user.
            sp_client.lakeview.publish(
                dashboard_id=result.dashboard_id,
                warehouse_id=warehouse_id,
            )

        progress.write("Generating conversion report...")
        result.explanation = generate_explanation(report_name, result.pbi_context, dashboard_json)

        # PDF build is best-effort — if it fails we log but do not
        # abort the publish. The dashboard already exists in Databricks
        # at this point and the PDF is just a downloadable summary.
        try:
            result.pdf_bytes = build_export_pdf(
                report_name=report_name, model=MODEL,
                workspace_path=result.workspace_path, dash_url=result.dash_url,
                n_datasets=result.n_datasets, n_widgets=result.n_widgets,
                n_canvas=result.n_canvas, n_pages=result.n_pages,
                layout_fidelity=layout_fidelity, explanation=result.explanation,
                validation=validation, data_sources=result.data_sources,
                external_sources=result.external_sources, dashboard_json=dashboard_json,
                valid_widget_versions=VALID_WIDGET_VERSIONS,
            )
        except Exception as pdf_err:
            logger.warning("PDF report build failed: %s", pdf_err)
            progress.write(
                f"(non-fatal) PDF report build failed: {pdf_err}. "
                "The dashboard was still published."
            )

        result.status = "done"
    except Exception as e:
        result.status = "error"
        result.error_msg = f"Publish failed: {e}"
        result.raw_traceback = traceback.format_exc()

    return result


# ---------------------------------------------------------------------------
# Preview rendering
# ---------------------------------------------------------------------------

_AGG_PREFIXES = (
    "sum(", "avg(", "mean(", "count(", "count_distinct(", "countdistinct(",
    "min(", "max(", "stddev(", "variance(", "median(", "percentile(",
    "approx_count_distinct(", "approx_percentile(", "first(", "last(",
    "any_value(", "bool_and(", "bool_or(", "bit_and(", "bit_or(", "bit_xor(",
)


def _is_aggregate_expr(expr: str) -> bool:
    e = (expr or "").strip().lower()
    return any(e.startswith(p) for p in _AGG_PREFIXES)


def _build_widget_sql(widget: dict, dataset_sql: str) -> str:
    """Build the actual query AI/BI would run for this widget: apply the
    field expressions + GROUP BY on non-aggregate fields when appropriate."""
    queries = widget.get("queries") or []
    if not queries:
        return f"SELECT * FROM ({dataset_sql}) AS _t LIMIT 500"

    q = queries[0].get("query", {}) or {}
    fields = q.get("fields", []) or []
    disaggregated = bool(q.get("disaggregated", False))

    if not fields:
        return f"SELECT * FROM ({dataset_sql}) AS _t LIMIT 500"

    select_parts: list[str] = []
    groupby_parts: list[str] = []
    has_any_agg = False
    for f in fields:
        name = f.get("name") or ""
        expr = f.get("expression") or ""
        if not name or not expr:
            continue
        select_parts.append(f"{expr} AS `{name}`")
        if _is_aggregate_expr(expr):
            has_any_agg = True
        else:
            groupby_parts.append(expr)

    if not select_parts:
        return f"SELECT * FROM ({dataset_sql}) AS _t LIMIT 500"

    sql = f"SELECT {', '.join(select_parts)} FROM ({dataset_sql}) AS _t"
    if has_any_agg and groupby_parts and not disaggregated:
        sql += f" GROUP BY {', '.join(groupby_parts)}"
    sql += " LIMIT 500"
    return sql


@st.cache_data(show_spinner=False, ttl=600)
def _run_preview_sql_cached(sql: str, warehouse_id: str, _user_key: str):
    """Execute a SQL string (already LIMIT-bounded) and return a DataFrame.

    `_user_key` is REQUIRED in the cache key so two different signed-in
    users hitting the same warehouse with the same SQL never share each
    other's result rows. Streamlit's @cache_data is process-wide and
    keyed only on the function arguments, so without `_user_key` user A
    could see user B's preview data when their (sql, warehouse_id) keys
    collide. The argument is intentionally underscore-prefixed so it's
    not mistaken for a real query parameter.
    """
    try:
        from databricks.sdk.service.sql import StatementState
        import pandas as pd

        wc = _work_client()
        stmt = wc.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql,
            wait_timeout="30s",
        )
        if stmt.status and stmt.status.state == StatementState.SUCCEEDED:
            cols = [c.name for c in (stmt.manifest.schema.columns or [])] \
                   if stmt.manifest and stmt.manifest.schema else []
            rows = stmt.result.data_array or []
            return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        logger.warning("preview sql failed: %s", e)
        return None
    return None


def _run_preview_sql(sql: str, warehouse_id: str):
    """User-isolated wrapper around `_run_preview_sql_cached`."""
    return _run_preview_sql_cached(sql, warehouse_id, _current_user_cache_key())


_WIDGET_TYPE_LABELS = {
    "counter": "Counter",
    "bar": "Bar chart",
    "line": "Line chart",
    "pie": "Pie / Donut",
    "area": "Area chart",
    "scatter": "Scatter",
    "table": "Table",
    "pivot": "Pivot table",
    "filter-multi-select": "Filter (multi)",
    "filter-single-select": "Filter (single)",
    "filter-date-range-picker": "Filter (date range)",
}


def _widget_palette(spec: dict) -> tuple[list[str], dict[str, str], str | None]:
    """Extract brand colors already embedded in the widget spec.

    Returns (sequence, mapping, single) where:
      * sequence: ordered list of hex colors (from scale.range / scale.colors /
        mark.colors) to feed into Plotly's `color_discrete_sequence`.
      * mapping:  dict[value -> hex] built from scale.mappings (exact
        category -> color) for `color_discrete_map`.
      * single:   single hex color from mark.color, used when the chart has
        no `color` encoding (single-series bar/line/area/scatter).
    """
    spec = spec or {}
    enc_color = ((spec.get("encodings") or {}).get("color") or {})
    scale = enc_color.get("scale") or {}
    mark = spec.get("mark") or {}

    sequence: list[str] = []
    mapping: dict[str, str] = {}

    raw_mappings = scale.get("mappings") or []
    for m in raw_mappings:
        val = m.get("value")
        col = m.get("color")
        if val is not None and col:
            mapping[str(val)] = col

    for key in ("range", "colors"):
        vals = scale.get(key) or []
        if isinstance(vals, list):
            for c in vals:
                if isinstance(c, str) and c and c not in sequence:
                    sequence.append(c)

    mark_colors = mark.get("colors") or []
    if isinstance(mark_colors, list):
        for c in mark_colors:
            if isinstance(c, str) and c and c not in sequence:
                sequence.append(c)

    if mapping and not sequence:
        sequence = list(dict.fromkeys(mapping.values()))

    single = mark.get("color") if isinstance(mark.get("color"), str) else None
    if not single and sequence:
        single = sequence[0]

    return sequence, mapping, single


def _render_widget_tile(widget: dict, res: ReportResult, key_suffix: str = ""):
    """Render a single widget as a Streamlit tile (best-effort approximation).

    For chart/counter/filter widgets we build the widget's ACTUAL query
    (dataset SQL + aggregations + GROUP BY) so Plotly can render real
    chart shapes. Table widgets fall back to the raw dataset rows.
    Brand colors embedded in the widget spec (scale.range, scale.mappings,
    mark.colors, mark.color) are honoured so the preview matches the
    published dashboard.
    """
    with st.container(border=True):
        if "multilineTextboxSpec" in widget:
            st.caption("Text")
            lines = widget["multilineTextboxSpec"].get("lines", [])
            st.markdown("\n".join(lines) or "_(empty text widget)_")
            return

        spec = widget.get("spec", {}) or {}
        wt = spec.get("widgetType", "unknown")
        type_label = _WIDGET_TYPE_LABELS.get(wt, wt)
        frame = spec.get("frame", {}) or {}
        title = frame.get("title") or widget.get("name", "")

        header_l, header_r = st.columns([4, 1])
        with header_l:
            if frame.get("showTitle", True) and title:
                st.markdown(f"**{title}**")
        with header_r:
            st.caption(type_label)

        queries = widget.get("queries") or [{}]
        q = queries[0].get("query", {}) if queries else {}
        ds_name = q.get("datasetName")
        encs = spec.get("encodings", {}) or {}

        if not ds_name:
            st.caption(f"_({type_label} — no dataset linked)_")
            return

        ds = next(
            (d for d in res.dashboard_json.get("datasets", []) if d.get("name") == ds_name),
            None,
        )
        if ds is None:
            st.caption(f"_(dataset `{ds_name}` not found)_")
            return

        dataset_sql = ds.get("query") or " ".join(ds.get("queryLines", []))

        # Table widgets: show raw dataset rows. Everything else: run the widget's
        # aggregated query so chart shapes come out correctly.
        if wt == "table":
            df = _run_preview_sql(f"SELECT * FROM ({dataset_sql}) AS _t LIMIT 500",
                                  res.warehouse_id)
        elif wt.startswith("filter-"):
            fields = encs.get("fields") or []
            fld = (fields[0].get("fieldName") if fields else "") or ""
            df = _run_preview_sql(f"SELECT * FROM ({dataset_sql}) AS _t LIMIT 500",
                                  res.warehouse_id)
        else:
            widget_sql = _build_widget_sql(widget, dataset_sql)
            df = _run_preview_sql(widget_sql, res.warehouse_id)
            if df is None:
                # Synthesized aggregation SQL failed (most common cause:
                # the model invented a column name like `total_sales`
                # that isn't in the dataset). Fall back to the raw rows
                # so the user at least sees what the dataset contains
                # and can make an informed Accept/Skip decision.
                df = _run_preview_sql(
                    f"SELECT * FROM ({dataset_sql}) AS _t LIMIT 500",
                    res.warehouse_id,
                )
                if df is not None and not df.empty:
                    st.caption(
                        f"_({type_label}: chart query failed — showing raw "
                        "dataset rows. The encoded fields are not real "
                        "columns; pick a different option or Skip.)_"
                    )

        if df is None:
            st.caption(f"_({type_label}: preview unavailable — SQL did not execute)_")
            return
        if df.empty:
            st.caption(f"_({type_label}: query returned no rows)_")
            return

        try:
            if wt == "counter":
                fld = (encs.get("value") or {}).get("fieldName")
                val = df[fld].iloc[0] if fld and fld in df.columns else (
                    df.iloc[0, 0] if len(df.columns) else "—"
                )
                # Pandas scalars from a SQL warehouse come back as
                # numpy.int64 / numpy.float64 which are NOT instances of
                # the python `int`/`float` built-ins on every numpy
                # version. The stricter check would silently fall through
                # to `str(val)` and the counter would render an unformatted
                # "12345" instead of "12,345". Format anything that walks
                # like a number.
                try:
                    formatted = f"{val:,}"
                except (TypeError, ValueError):
                    formatted = str(val)
                st.metric(title or "value", formatted)
                return

            if wt == "table":
                st.dataframe(df, hide_index=True, use_container_width=True, height=220)
                return

            if wt.startswith("filter-"):
                fields = encs.get("fields") or []
                fld = (fields[0].get("fieldName") if fields else "") or ""
                options = sorted(df[fld].dropna().unique().tolist()) if fld in df.columns else []
                st.multiselect(title or fld or "filter", options[:50],
                               key=f"pv_{widget.get('name')}_{key_suffix}")
                return

            try:
                import plotly.express as px
            except Exception:
                st.dataframe(df.head(20), hide_index=True, use_container_width=True)
                return

            palette, color_map, single_color = _widget_palette(spec)

            def _apply_single_color(fig, kind: str):
                if not single_color:
                    return
                if kind == "bar":
                    fig.update_traces(marker_color=single_color)
                elif kind in ("line", "area"):
                    fig.update_traces(line_color=single_color,
                                      marker_color=single_color)
                elif kind == "scatter":
                    fig.update_traces(marker_color=single_color)

            def _render_missing_columns_fallback(
                kind_label: str, missing: list[str | None]
            ):
                """The chart wanted columns that aren't in the result.

                Re-run the dataset's underlying SQL (without the chart's
                aggregation) and show the FULL set of real columns. This
                makes the failure mode actionable: the user sees what
                columns the dataset actually exposes and can pick a
                different alternative whose encoding refers to one of
                them, instead of seeing a 1-column slice that looks
                like the dataset is broken.
                """
                wanted = ", ".join(f"`{m}`" for m in missing if m)
                raw_df = _run_preview_sql(
                    f"SELECT * FROM ({dataset_sql}) AS _t LIMIT 50",
                    res.warehouse_id,
                )
                if raw_df is not None and not raw_df.empty:
                    real_cols = ", ".join(f"`{c}`" for c in raw_df.columns)
                    st.caption(
                        f"_({kind_label}: encoded {wanted} not in dataset. "
                        f"Real columns: {real_cols}. Pick a different "
                        "option or Skip.)_"
                    )
                    st.dataframe(
                        raw_df.head(20), hide_index=True,
                        use_container_width=True,
                    )
                else:
                    st.caption(
                        f"_({kind_label}: encoded {wanted} not in result, "
                        "and the underlying dataset query also did not return "
                        "rows. Pick a different option or Skip.)_"
                    )

            if wt == "bar":
                x = (encs.get("x") or {}).get("fieldName")
                y = (encs.get("y") or {}).get("fieldName")
                color = (encs.get("color") or {}).get("fieldName")
                if x and y and x in df.columns and y in df.columns:
                    kwargs = {}
                    if color and color in df.columns:
                        kwargs["color"] = color
                        if palette:
                            kwargs["color_discrete_sequence"] = palette
                        if color_map:
                            kwargs["color_discrete_map"] = color_map
                    fig = px.bar(df, x=x, y=y, **kwargs)
                    if not kwargs.get("color"):
                        _apply_single_color(fig, "bar")
                    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), height=260)
                    st.plotly_chart(fig, use_container_width=True,
                                    key=f"pv_{widget.get('name')}_{key_suffix}")
                else:
                    _render_missing_columns_fallback("bar", [x, y])
                return

            if wt == "line":
                x = (encs.get("x") or {}).get("fieldName")
                y = (encs.get("y") or {}).get("fieldName")
                color = (encs.get("color") or {}).get("fieldName")
                if x and y and x in df.columns and y in df.columns:
                    kwargs = {}
                    if color and color in df.columns:
                        kwargs["color"] = color
                        if palette:
                            kwargs["color_discrete_sequence"] = palette
                        if color_map:
                            kwargs["color_discrete_map"] = color_map
                    fig = px.line(df.sort_values(x), x=x, y=y, **kwargs)
                    if not kwargs.get("color"):
                        _apply_single_color(fig, "line")
                    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), height=260)
                    st.plotly_chart(fig, use_container_width=True,
                                    key=f"pv_{widget.get('name')}_{key_suffix}")
                else:
                    _render_missing_columns_fallback("line", [x, y])
                return

            if wt in ("pie", "donut"):
                names = (encs.get("color") or {}).get("fieldName")
                values = (encs.get("angle") or {}).get("fieldName")
                if names and values and names in df.columns and values in df.columns:
                    kwargs = {"names": names, "values": values,
                              "hole": 0.4 if wt == "donut" else 0}
                    if palette:
                        kwargs["color_discrete_sequence"] = palette
                    if color_map:
                        kwargs["color"] = names
                        kwargs["color_discrete_map"] = color_map
                    fig = px.pie(df, **kwargs)
                    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), height=260)
                    st.plotly_chart(fig, use_container_width=True,
                                    key=f"pv_{widget.get('name')}_{key_suffix}")
                else:
                    _render_missing_columns_fallback(wt, [names, values])
                return

            if wt == "scatter":
                x = (encs.get("x") or {}).get("fieldName")
                y = (encs.get("y") or {}).get("fieldName")
                if x and y and x in df.columns and y in df.columns:
                    fig = px.scatter(df, x=x, y=y)
                    _apply_single_color(fig, "scatter")
                    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), height=260)
                    st.plotly_chart(fig, use_container_width=True,
                                    key=f"pv_{widget.get('name')}_{key_suffix}")
                    return

            if wt == "area":
                x = (encs.get("x") or {}).get("fieldName")
                y = (encs.get("y") or {}).get("fieldName")
                if x and y and x in df.columns and y in df.columns:
                    fig = px.area(df.sort_values(x), x=x, y=y)
                    _apply_single_color(fig, "area")
                    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), height=260)
                    st.plotly_chart(fig, use_container_width=True,
                                    key=f"pv_{widget.get('name')}_{key_suffix}")
                    return

            st.caption(f"_(preview for `{wt}` not implemented — showing raw data)_")
            st.dataframe(df.head(20), hide_index=True, use_container_width=True)
        except Exception as e:
            st.caption(f"_(preview error: {e})_")
            st.dataframe(df.head(10), hide_index=True, use_container_width=True)


def _render_dashboard_preview(res: ReportResult, dashboard_json: dict | None = None):
    dj = dashboard_json if dashboard_json is not None else res.dashboard_json
    for page in dj.get("pages", []):
        if page.get("pageType") == "PAGE_TYPE_GLOBAL_FILTERS":
            st.caption(f"Global filter page: `{page.get('displayName', page.get('name',''))}`")
            continue
        st.markdown(f"#### {page.get('displayName', page.get('name',''))}")

        items = sorted(
            page.get("layout", []),
            key=lambda it: (it.get("position", {}).get("y", 0),
                            it.get("position", {}).get("x", 0)),
        )

        for y_val, row_iter in groupby(items, key=lambda it: it.get("position", {}).get("y", 0)):
            row = list(row_iter)
            widths = [max(1, it.get("position", {}).get("width", 1)) for it in row]
            total = sum(widths)
            if total > 6:
                widths = [max(1, int(round(w * 6 / total))) for w in widths]
            cols = st.columns(widths)
            for col, it in zip(cols, row):
                with col:
                    _render_widget_tile(it.get("widget", {}), res,
                                         key_suffix=f"{page.get('name','')}_{y_val}")


# ---------------------------------------------------------------------------
# Alternatives / shuffle / skip
# ---------------------------------------------------------------------------

def _option_signature(alt: VisualAlternative) -> str:
    """Stable fingerprint used to detect duplicate alternatives."""
    if alt.kind == "skip":
        return "skip"
    w = alt.widget or {}
    if "multilineTextboxSpec" in w:
        lines = w["multilineTextboxSpec"].get("lines", []) or []
        return "text::" + "|".join(lines)[:200]
    spec = w.get("spec") or {}
    wt = spec.get("widgetType", "unknown")
    encs = spec.get("encodings") or {}
    enc_fields: list[str] = []
    for key in sorted(encs.keys()):
        val = encs[key]
        if isinstance(val, dict):
            enc_fields.append(f"{key}={val.get('fieldName','')}")
        elif isinstance(val, list):
            for item in val:
                if isinstance(item, dict):
                    enc_fields.append(f"{key}={item.get('fieldName','')}")
    q = (w.get("queries") or [{}])[0].get("query", {})
    field_names = sorted(f.get("name", "") for f in q.get("fields", []) or [])
    return f"{wt}::{','.join(enc_fields)}::{','.join(field_names)}"


def _is_viable(alt: VisualAlternative) -> bool:
    """Reject empty or malformed widgets so they don't become dead options."""
    if alt.kind == "skip":
        return True
    w = alt.widget or {}
    if "multilineTextboxSpec" in w:
        lines = w["multilineTextboxSpec"].get("lines", []) or []
        return any((l or "").strip() for l in lines)
    spec = w.get("spec") or {}
    wt = spec.get("widgetType")
    if not wt:
        return False
    queries = w.get("queries") or []
    if not queries:
        return False
    fields = queries[0].get("query", {}).get("fields", []) or []
    return bool(fields)


def _infer_alt_widget_type(widget: dict, label: str = "") -> str | None:
    """Best-effort guess at `spec.widgetType` when the LLM omitted it.

    Strategy: read structural hints in this order, falling back to the
    next when the previous is ambiguous. We never overwrite an existing
    valid widgetType.
      1. existing `spec.widgetType` if it's a known type
      2. legacy `spec.mark.type`
      3. presence/shape of encodings (angle -> pie, value-only -> counter,
         x+y -> bar, x+y where x is temporal -> line)
      4. textual cue from the human-readable label ("bar", "pie", ...)
    """
    spec = widget.get("spec") or {}
    valid = set(VALID_WIDGET_VERSIONS.keys())

    wt = spec.get("widgetType")
    if isinstance(wt, str) and wt in valid:
        return wt

    mark = spec.get("mark") or {}
    mark_type = mark.get("type") if isinstance(mark, dict) else None
    if isinstance(mark_type, str) and mark_type in valid:
        return mark_type

    encs = spec.get("encodings") or {}
    if isinstance(encs.get("angle"), dict):
        return "pie"
    has_x = isinstance(encs.get("x"), dict)
    has_y = isinstance(encs.get("y"), dict)
    has_value = isinstance(encs.get("value"), dict)
    if has_value and not has_x and not has_y:
        return "counter"
    if has_x and has_y:
        x_enc = encs["x"]
        x_field_type = (x_enc.get("scale") or {}).get("type") or x_enc.get("type")
        if x_field_type in {"temporal", "time"}:
            return "line"
        return "bar"

    lbl = (label or "").lower()
    for hint, wt_guess in (
        ("pie", "pie"),
        ("donut", "pie"),
        ("counter", "counter"),
        ("kpi", "counter"),
        ("scorecard", "counter"),
        ("scatter", "scatter"),
        ("area", "area"),
        ("line", "line"),
        ("trend", "line"),
        ("bar", "bar"),
        ("column", "bar"),
        ("histogram", "bar"),
        ("pivot", "pivot"),
        ("table", "table"),
        ("grid", "table"),
    ):
        if hint in lbl:
            return wt_guess
    return None


def _repair_alternative_widget_shape(
    alt,
    dataset_columns_by_name: dict[str, list[str]] | None = None,
) -> None:
    """Make sure `spec.widgetType` and `spec.version` are present on an
    alternative widget.

    Without this, charts with a missing `widgetType` render as raw
    dataframes in the preview ("preview for `unknown` not implemented")
    and AI/BI rejects them on publish because every chart-style widget
    needs both a `widgetType` and a matching `version`.
    """
    if getattr(alt, "kind", None) != "widget":
        return
    widget = alt.widget or {}
    if "multilineTextboxSpec" in widget or not widget:
        return

    spec = widget.setdefault("spec", {})
    inferred = _infer_alt_widget_type(widget, getattr(alt, "label", ""))
    if inferred and spec.get("widgetType") not in VALID_WIDGET_VERSIONS:
        spec["widgetType"] = inferred

    wt = spec.get("widgetType")
    if isinstance(wt, str) and wt in VALID_WIDGET_VERSIONS:
        spec.setdefault("version", VALID_WIDGET_VERSIONS[wt])

    # Look up the dataset this widget points at so we can validate the
    # encoded fieldNames against the dataset's actual columns before
    # synthesizing aggregation expressions for them.
    cols: list[str] | None = None
    if dataset_columns_by_name:
        ds_name = ""
        try:
            ds_name = (
                widget.get("queries") or [{}]
            )[0].get("query", {}).get("datasetName", "")
        except Exception:
            ds_name = ""
        if ds_name:
            cols = dataset_columns_by_name.get(ds_name)

    _synthesize_query_fields_from_encodings(widget, cols)
    _ensure_measure_encoding(widget)
    _scrub_misplaced_color_scale(widget)
    _normalize_query_name(widget)


# Encodings that represent quantitative measures (numeric value, axis-y,
# pie angle, scatter size, etc). AI/BI rejects a `scale.range` block on
# these — `range` is reserved for color encodings — and renders the whole
# widget as a "Select fields to visualize" placeholder. This list is used
# by `_scrub_misplaced_color_scale` to detect and clean those up.
_MEASURE_ENCODING_KEYS = {"y", "value", "angle", "size", "theta", "radius"}


# Chart widget types that MUST have a measure encoding to render. For each,
# the expected measure encoding channel.
_REQUIRED_MEASURE_BY_WIDGET = {
    "bar": "y",
    "line": "y",
    "area": "y",
    "scatter": "y",
    "pie": "angle",
    "donut": "angle",
    "counter": "value",
}


def _ensure_measure_encoding(widget: dict) -> None:
    """Auto-fill the measure encoding for chart-style alts that arrived
    with only a dimension encoding (e.g. a pie with `color` but no
    `angle`). Without a measure, AI/BI renders the widget as a
    "Select fields to visualize" placeholder even though every other
    part of the widget is valid.

    Strategy: when the required measure channel is missing, synthesize a
    `COUNT(*)` measure. COUNT(*) always works because it doesn't depend on
    any dataset column being present. Naming is `count_records` and the
    encoding's `displayName` is "Records" so it reads sanely in the UI.

    We do nothing for widget types that aren't in the required-measure
    map (tables, filters, text, etc — those don't need a measure).
    """
    if not isinstance(widget, dict) or "multilineTextboxSpec" in widget:
        return
    spec = widget.get("spec")
    if not isinstance(spec, dict):
        return
    wtype = spec.get("widgetType")
    measure_key = _REQUIRED_MEASURE_BY_WIDGET.get(wtype)
    if not measure_key:
        return

    encs = spec.setdefault("encodings", {})
    if not isinstance(encs, dict):
        return
    existing = encs.get(measure_key)
    if isinstance(existing, dict) and existing.get("fieldName"):
        return

    # Only auto-fill if there's at least one dimension encoding so we
    # don't paper over a totally empty widget (which usually means the
    # LLM emitted garbage — better to surface that).
    dim_keys = ("x", "color", "names", "row", "column", "facet")
    has_dim = any(
        isinstance(encs.get(k), dict) and encs[k].get("fieldName")
        for k in dim_keys
    )
    if not has_dim and wtype != "counter":
        return

    queries = widget.setdefault("queries", [])
    if not queries:
        queries.append({"name": "main_query", "query": {}})
    q = queries[0].setdefault("query", {})
    fields = q.setdefault("fields", [])
    if isinstance(fields, list) and not any(
        isinstance(f, dict) and f.get("name") == "count_records"
        for f in fields
    ):
        fields.append({"name": "count_records", "expression": "COUNT(*)"})

    scale_type = "quantitative"
    encs[measure_key] = {
        "fieldName": "count_records",
        "displayName": "Records",
        "scale": {"type": scale_type},
    }


def _normalize_query_name(widget: dict) -> None:
    """Force `queries[0].name = "main_query"` for chart widgets.

    AI/BI looks up the query bound to each encoding by name. When an
    encoding omits a `queryName` (the common case for our generated
    charts), AI/BI implicitly looks for a query named `main_query`. The
    LLM sometimes emits short ad-hoc names like `q` for alternative
    widgets — the published widget then has a valid query AND valid
    encodings AND a valid widgetType, but AI/BI still renders
    "Select fields to visualize" because the implicit `main_query`
    lookup fails. Renaming the first query to `main_query` (and
    rewriting any encoding `queryName` that pointed at the old name)
    fixes it.

    Filters skip this because their encodings are wired with an
    explicit `queryName` to a per-field query (e.g.
    `sales_overview_country`) — renaming would break them.
    """
    if not isinstance(widget, dict) or "multilineTextboxSpec" in widget:
        return
    spec = widget.get("spec")
    if not isinstance(spec, dict):
        return
    wtype = spec.get("widgetType", "")
    if isinstance(wtype, str) and wtype.startswith("filter-"):
        return
    queries = widget.get("queries")
    if not isinstance(queries, list) or not queries:
        return
    first = queries[0]
    if not isinstance(first, dict):
        return
    old_name = first.get("name")
    if old_name == "main_query":
        return
    first["name"] = "main_query"

    # Rewrite any encoding.queryName that referred to the old query name
    # so we don't accidentally orphan it. (In practice our alts don't
    # set queryName at all, so this is mostly defensive.)
    if not isinstance(old_name, str):
        return
    encs = spec.get("encodings")
    if not isinstance(encs, dict):
        return

    def _rewrite(node):
        if isinstance(node, dict):
            if node.get("queryName") == old_name:
                node["queryName"] = "main_query"
            for v in node.values():
                _rewrite(v)
        elif isinstance(node, list):
            for v in node:
                _rewrite(v)

    _rewrite(encs)


def _scrub_misplaced_color_scale(widget: dict) -> None:
    """Strip `scale.range = [<color>...]` from measure encodings.

    This kept biting alternatives: `apply_attributes_to_widget()` and the
    LLM both occasionally drop a single hex color into `value.scale.range`
    or `y.scale.range`, which AI/BI reads as a malformed encoding and
    refuses to render — the user sees "Select fields to visualize" even
    though the field/expression wiring is correct.

    We leave a `scale.range` alone if the measure encoding's scale type is
    `quantitative` AND the values look like numeric domain bounds (so we
    don't accidentally nuke a legitimate domain) — but in practice every
    case we've seen so far has been a list of hex strings, so the heuristic
    is "any non-numeric range on a measure encoding is a mistake".
    """
    if not isinstance(widget, dict):
        return
    spec = widget.get("spec")
    if not isinstance(spec, dict):
        return
    encs = spec.get("encodings")
    if not isinstance(encs, dict):
        return
    for key in _MEASURE_ENCODING_KEYS:
        enc = encs.get(key)
        if not isinstance(enc, dict):
            continue
        scale = enc.get("scale")
        if not isinstance(scale, dict):
            continue
        rng = scale.get("range")
        if not isinstance(rng, list) or not rng:
            continue
        non_numeric = [
            v for v in rng if not isinstance(v, (int, float))
        ]
        if non_numeric:
            scale.pop("range", None)


_DEFAULT_AGG_BY_ENCODING = {
    # Measure-side encodings: wrap the column in SUM() by default. SUM is the
    # common case for "amount/total/value/quantity" type fields. If the column
    # is non-numeric the SQL will fail and the existing "(pie: columns ... not
    # in result)" caption + raw-data fallback already handles the degradation
    # gracefully.
    "y": "SUM",
    "value": "SUM",
    "angle": "SUM",
    "size": "SUM",
    "theta": "SUM",
    "radius": "SUM",
    # Dimension-side encodings: bare column reference -> becomes the GROUP BY.
    "x": None,
    "color": None,
    "names": None,
    "shape": None,
    "row": None,
    "column": None,
    "facet": None,
}


def _synthesize_query_fields_from_encodings(
    widget: dict,
    dataset_columns: list[str] | None = None,
) -> None:
    """Synthesize `queries[0].query.fields` from `spec.encodings` if missing,
    but ONLY for encoding fieldNames that are real dataset columns.

    Background: the model often produces alternative widgets where the
    encodings reference field names (e.g. `color.fieldName=country`,
    `angle.fieldName=total_sales`) but forgets to put matching entries in
    the query's `fields` array. Without a `fields` block, `_build_widget_sql`
    falls back to `SELECT *` over the raw dataset.

    Synthesizing blindly was worse than leaving it alone — when the model
    invented a column name (e.g. `total_quantity_sold` when the dataset
    actually has `quantity`), `SUM(\`total_quantity_sold\`)` errored at the
    warehouse, giving the user a "preview unavailable" instead of at least
    showing raw rows.

    This version requires the encoded fieldName to be in `dataset_columns`
    before synthesizing a SUM/bare-column expression. If columns are unknown
    (probe failed) we synthesize anyway as a best-effort. If a fieldName
    isn't in the known column list, we skip it — `_render_widget_tile`'s
    SQL-failed branch then falls back to the raw dataset preview.
    """
    if not isinstance(widget, dict) or "multilineTextboxSpec" in widget:
        return
    spec = widget.get("spec") or {}
    encs = spec.get("encodings") or {}
    if not isinstance(encs, dict) or not encs:
        return

    queries = widget.setdefault("queries", [])
    if not queries:
        queries.append({"name": "main_query", "query": {}})
    q = queries[0].setdefault("query", {})

    existing = q.get("fields") or []
    existing_names = {
        f.get("name") for f in existing if isinstance(f, dict) and f.get("name")
    }
    new_fields = list(existing) if isinstance(existing, list) else []

    known_cols = set(dataset_columns or [])

    for enc_key, enc_val in encs.items():
        if not isinstance(enc_val, dict):
            continue
        fld = enc_val.get("fieldName")
        if not fld or fld in existing_names:
            continue
        # If we know the dataset's columns, only synthesize when the
        # encoded fieldName is actually one of them. This avoids the
        # `SUM(\`total_quantity_sold\`)` failure mode when the model
        # invents an alias-style fieldName.
        if known_cols and fld not in known_cols:
            continue
        agg = _DEFAULT_AGG_BY_ENCODING.get(enc_key)
        if agg:
            expr = f"{agg}(`{fld}`)"
        else:
            expr = f"`{fld}`"
        new_fields.append({"name": fld, "expression": expr})
        existing_names.add(fld)

    if new_fields and new_fields != existing:
        q["fields"] = new_fields


def _repair_alternative_dataset_ref(alt, dashboard_json: dict) -> None:
    """Make sure an alternative widget points at a dataset that actually
    exists in the published dashboard.

    The LLM that generates alternatives sometimes:
      * uses a dataset's `displayName` instead of its `name`
      * uses different casing (`Sales_Overview` vs `sales_overview`)
      * drops the `queries` block entirely
      * leaves `datasetName` empty

    Without this repair the preview shows "(unknown — no dataset linked)"
    and the published widget has the same broken reference. We resolve
    against the same heuristics the main dashboard uses
    (`_repair_dataset_references` in converter.py): exact case-insensitive
    name match, displayName match, normalized form match, and finally the
    first dataset as a single-dataset fallback.
    """
    if getattr(alt, "kind", None) != "widget":
        return
    widget = alt.widget or {}
    if "multilineTextboxSpec" in widget:
        return  # text widgets have no dataset

    datasets = dashboard_json.get("datasets") or []
    if not datasets:
        return

    by_name = {ds.get("name", ""): ds.get("name", "") for ds in datasets if ds.get("name")}
    by_lower = {k.lower(): k for k in by_name}
    by_display = {
        (ds.get("displayName") or "").lower(): ds.get("name", "")
        for ds in datasets if ds.get("displayName") and ds.get("name")
    }
    def _norm(s: str) -> str:
        return "".join(c for c in (s or "").lower() if c.isalnum())
    by_norm = {_norm(k): v for k, v in by_name.items()}
    fallback = datasets[0].get("name", "")

    queries = widget.setdefault("queries", [])
    if not queries:
        queries.append({"name": "main_query", "query": {"datasetName": fallback}})

    for wq in queries:
        query_obj = wq.setdefault("query", {})
        ds_ref = query_obj.get("datasetName") or ""
        if ds_ref in by_name:
            continue
        fix = (
            by_lower.get(ds_ref.lower())
            or by_display.get(ds_ref.lower())
            or by_norm.get(_norm(ds_ref))
            or fallback
        )
        if fix:
            query_obj["datasetName"] = fix


def _inject_brand_colors_into_alt(alt, pbi_colors: list[str]):
    """Force the PBI visual's brand colors onto an alternative option so the
    preview and the published widget use the same palette the author picked
    in Power BI (not whatever palette the LLM invented in `attributes`).
    """
    if not pbi_colors or getattr(alt, "kind", None) != "widget":
        return
    attrs = alt.attributes or {}
    attrs["palette"] = list(pbi_colors)
    attrs["primary_color"] = pbi_colors[0]
    alt.attributes = attrs

    widget = alt.widget or {}
    if "multilineTextboxSpec" in widget or not widget:
        return
    spec = widget.setdefault("spec", {})
    encodings = spec.setdefault("encodings", {})
    color_enc = encodings.get("color")
    if isinstance(color_enc, dict):
        scale = color_enc.setdefault("scale", {"type": "categorical"})
        if scale.get("type", "categorical") == "categorical":
            scale["range"] = list(pbi_colors)
        else:
            scale["range"] = [pbi_colors[0]]
    else:
        # No color encoding: stamp a single mark color (bar/line/area/
        # scatter) and a mark.colors palette (pie/donut).
        mark = spec.setdefault("mark", {})
        mark.setdefault("color", pbi_colors[0])
        mark.setdefault("colors", list(pbi_colors))


@st.cache_data(show_spinner=False, ttl=600)
def _probe_dataset_columns_cached_inner(
    sql: str, warehouse_id: str, _user_key: str,
) -> list[str]:
    """Run `SELECT * FROM (sql) WHERE 1=0` to fetch the dataset's columns.

    `WHERE 1=0` returns zero rows but still populates the result schema, so
    this is cheap regardless of how big the underlying tables are. Returns
    [] on any failure — the caller should treat empty as "unknown columns"
    and not over-restrict the LLM.

    `_user_key` is in the cache key for the same multi-tenant isolation
    reason as `_run_preview_sql_cached`.
    """
    try:
        from databricks.sdk.service.sql import StatementState
        wc = _work_client()
        stmt = wc.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=f"SELECT * FROM ({sql}) AS _probe WHERE 1=0",
            wait_timeout="30s",
        )
        if (stmt.status and stmt.status.state == StatementState.SUCCEEDED
                and stmt.manifest and stmt.manifest.schema):
            return [c.name for c in (stmt.manifest.schema.columns or [])]
    except Exception as e:
        logger.warning("dataset column probe failed: %s", e)
        return []
    return []


def _probe_dataset_columns_cached(sql: str, warehouse_id: str) -> list[str]:
    """User-isolated wrapper around `_probe_dataset_columns_cached_inner`."""
    return _probe_dataset_columns_cached_inner(
        sql, warehouse_id, _current_user_cache_key(),
    )


def _ensure_dataset_columns(res: ReportResult) -> dict[str, list[str]]:
    """Probe and cache the column list for every dataset in this report.

    Idempotent: subsequent calls return the cached dict without re-querying.
    """
    if res.dataset_columns:
        return res.dataset_columns
    if not res.warehouse_id:
        return {}
    out: dict[str, list[str]] = {}
    for ds in res.dashboard_json.get("datasets", []):
        name = ds.get("name") or ""
        if not name:
            continue
        sql = ds.get("query") or " ".join(ds.get("queryLines", []))
        if not sql:
            continue
        cols = _probe_dataset_columns_cached(sql, res.warehouse_id)
        if cols:
            out[name] = cols
    res.dataset_columns = out
    return out


def _get_or_create_alt_cache(res: ReportResult, visual_id: str, pbi_visual):
    cache = res.alt_cache.get(visual_id)
    if cache is None:
        ds_cols = _ensure_dataset_columns(res)
        with st.spinner("Asking the model for alternatives..."):
            options = suggest_alternatives(
                pbi_visual,
                dataset_summaries=summarize_datasets(res.dashboard_json, ds_cols),
                report_context=res.pbi_context,
            )
        # Keep every alternative the model produced — the previous `_is_viable`
        # filter was rejecting widgets that lacked `queries[0].query.fields`,
        # which was stripping most of the 3–4 options the model generates.
        # Viability is still enforced in the "More options" dedup flow to
        # avoid promoting obviously malformed widgets there.
        viable = list(options)

        # Repair the LLM's output before it ever reaches the preview:
        #   1. point any drifted `datasetName` (displayName, case, etc.)
        #      back at a real dataset; add a queries block if missing.
        #      Done first so step 2's column validation has the right
        #      dataset to look up.
        #   2. infer/fix `spec.widgetType` + `spec.version` so the
        #      preview can render the chart shape and AI/BI accepts the
        #      widget on publish; synthesize missing query.fields from
        #      encodings, but only for fieldNames that match real columns.
        for o in viable:
            _repair_alternative_dataset_ref(o, res.dashboard_json)
            _repair_alternative_widget_shape(o, ds_cols)

        # Stamp the PBI visual's actual brand colors onto every alternative
        # so the preview (and the published widget) reflect the author's
        # palette rather than the LLM's guess.
        preserve_colors = st.session_state.get("preserve_colors_toggle", True)
        if preserve_colors and getattr(pbi_visual, "colors", None):
            for o in viable:
                _inject_brand_colors_into_alt(o, pbi_visual.colors)
        if not any(o.kind == "widget" for o in viable):
            from alternatives import (
                VisualAlternative,
                _fallback_text_widget,
            )
            viable.insert(0, VisualAlternative(
                kind="widget",
                label="Text summary placeholder",
                rationale=(
                    "The model did not return any widget for this "
                    f"PBI `{pbi_visual.visual_type}`; falling back to a "
                    "deterministic text tile you can keep or replace."
                ),
                widget=_fallback_text_widget(pbi_visual),
                attributes={
                    "font_size_px": 14,
                    "title_font_size_px": 16,
                    "title": pbi_visual.display_name or "Visual placeholder",
                    "grid_width": max(2, pbi_visual.grid_width),
                    "grid_height": max(2, pbi_visual.grid_height),
                },
            ))
        if not any(o.kind == "skip" for o in viable):
            from alternatives import make_skip_option
            viable.append(make_skip_option())
        cache = {
            "options": viable,
            "signatures": {_option_signature(o) for o in viable},
            "idx": 0,
            "viewed": {0},
            "accepted": False,
            "skipped": False,
            "exhausted": False,
            "last_more_note": "",
        }
        res.alt_cache[visual_id] = cache
    return cache


def _build_preview_dashboard_json(res: ReportResult) -> dict:
    """Return a copy of the draft JSON with each pending (un-accepted)
    alternative overlaid at its PBI grid position. If the current selection
    for a visual is 'skip', that visual is NOT rendered."""
    import copy as _copy

    dj = _copy.deepcopy(res.dashboard_json)
    canvas_pages = [
        p for p in dj.get("pages", [])
        if p.get("pageType") != "PAGE_TYPE_GLOBAL_FILTERS"
    ]

    for entry in res.unmapped:
        v = entry["pbi_visual"]
        cache = res.alt_cache.get(v.visual_id)
        if not cache:
            continue
        options = cache["options"]
        if not options:
            continue
        current = options[cache["idx"] % len(options)]
        if current.kind != "widget":
            continue

        page_idx = entry["page_index"]
        if page_idx >= len(canvas_pages):
            continue
        page = canvas_pages[page_idx]

        widget = apply_attributes_to_widget(
            _copy.deepcopy(current.widget), current.attributes or {}
        )
        _scrub_misplaced_color_scale(widget)
        widget["name"] = _alt_widget_unique_name(v.visual_id)
        attrs = current.attributes or {}
        width = int(attrs.get("grid_width", v.grid_width or 2))
        height = int(attrs.get("grid_height", v.grid_height or 3))
        layout = page.setdefault("layout", [])
        pos_x, pos_y = _next_free_position(
            layout, int(v.grid_x), int(v.grid_y), width, height,
        )
        layout.append({
            "widget": widget,
            "position": {"x": pos_x, "y": pos_y, "width": width, "height": height},
        })
    return dj


def _alt_widget_unique_name(pbi_visual_id: str) -> str:
    """Deterministic unique name for any accepted-alternative widget tied to
    a specific PBI visual. Prevents (a) collisions with unrelated widgets
    already in the draft that happen to share the LLM-picked name, and
    (b) lets us cleanly swap one accepted alternative for another on the
    same visual without touching any other widget in the layout.
    """
    return f"alt_{pbi_visual_id[:12]}"


def _next_free_position(layout: list, x: int, y: int, w: int, h: int) -> tuple[int, int]:
    """If (x, y, w, h) collides with an existing widget in `layout`, slide
    down in `h`-row increments until there's no collision. Prevents the
    accepted alternative from being visually hidden by an existing widget
    that happens to occupy the same grid cells.
    """
    def collides(a_x, a_y, a_w, a_h, b):
        bp = b.get("position", {}) or {}
        bx, by, bw, bh = (bp.get("x", 0), bp.get("y", 0),
                          bp.get("width", 1), bp.get("height", 1))
        return not (a_x + a_w <= bx or bx + bw <= a_x
                    or a_y + a_h <= by or by + bh <= a_y)

    cur_y = max(0, int(y))
    cur_x = max(0, int(x))
    for _ in range(200):
        if not any(collides(cur_x, cur_y, w, h, item) for item in layout):
            return cur_x, cur_y
        cur_y += max(1, h)
    return cur_x, cur_y


def _apply_alternative(res: ReportResult, entry: dict, alt: VisualAlternative):
    """Insert the chosen widget into the draft dashboard on the correct page.

    Key invariants:
      * The inserted widget's `name` is forced to a deterministic value
        derived from the PBI visual id, so re-accepting a different option
        for the same visual swaps it cleanly and never touches other
        widgets.
      * If the inserted widget would collide with an existing widget at
        (grid_x, grid_y), we slide it down to the next free row rather
        than letting Databricks silently drop or stack it.
    """
    v = entry["pbi_visual"]
    page_idx = entry["page_index"]
    canvas_pages = [
        p for p in res.dashboard_json.get("pages", [])
        if p.get("pageType") != "PAGE_TYPE_GLOBAL_FILTERS"
    ]
    if page_idx >= len(canvas_pages):
        return
    page = canvas_pages[page_idx]

    # IMPORTANT: deepcopy. `apply_attributes_to_widget` mutates `spec`,
    # `frame`, and `encodings` in place. `dict(alt.widget)` is a SHALLOW
    # copy — those nested dicts are shared with `res.alt_cache`, so a
    # subsequent re-render or "More options" click would see polluted
    # state from a previously-accepted alternative.
    widget = apply_attributes_to_widget(_copy.deepcopy(alt.widget), alt.attributes or {})
    _scrub_misplaced_color_scale(widget)
    forced_name = _alt_widget_unique_name(v.visual_id)
    widget["name"] = forced_name

    attrs = alt.attributes or {}
    width = int(attrs.get("grid_width", v.grid_width or 2))
    height = int(attrs.get("grid_height", v.grid_height or 3))

    layout = page.setdefault("layout", [])

    # Drop any previously-accepted alternative for this exact PBI visual
    # before placing the new one — this is the only name that is safe to
    # replace, because we control it.
    layout[:] = [
        item for item in layout
        if (item.get("widget", {}) or {}).get("name") != forced_name
    ]

    pos_x, pos_y = _next_free_position(
        layout, int(v.grid_x), int(v.grid_y), width, height,
    )
    layout.append({
        "widget": widget,
        "position": {"x": pos_x, "y": pos_y, "width": width, "height": height},
    })

    res.unmapped = [u for u in res.unmapped if u["pbi_visual"].visual_id != v.visual_id]
    res.n_widgets = sum(len(p.get("layout", [])) for p in res.dashboard_json.get("pages", []))
    cache = res.alt_cache.get(v.visual_id)
    if cache:
        cache["accepted"] = True

    # Re-apply any user widget-rename directive so the newly-inserted
    # alternative inherits the same prefix/suffix as the rest of the
    # dashboard. The transform is idempotent.
    if res.custom_instructions:
        apply_widget_name_transforms(res.dashboard_json, res.custom_instructions)


def _apply_skip(res: ReportResult, entry: dict):
    """Skip a PBI visual: remove it from the review queue AND strip any
    previously-accepted alternative widget for it from the draft JSON.

    Without the strip step, a user who first clicked Accept on a widget
    alternative and then changed their mind and clicked Skip would still
    find the accepted widget in the published dashboard, because
    `_apply_alternative` had already written it into `res.dashboard_json`.
    """
    v = entry["pbi_visual"]
    forced_name = _alt_widget_unique_name(v.visual_id)
    for page in res.dashboard_json.get("pages", []):
        layout = page.get("layout")
        if not layout:
            continue
        page["layout"] = [
            item for item in layout
            if (item.get("widget", {}) or {}).get("name") != forced_name
        ]

    res.unmapped = [u for u in res.unmapped if u["pbi_visual"].visual_id != v.visual_id]
    res.n_widgets = sum(
        len(p.get("layout", [])) for p in res.dashboard_json.get("pages", [])
    )
    cache = res.alt_cache.get(v.visual_id)
    if cache:
        cache["skipped"] = True
        cache["accepted"] = False


def _render_alternatives_panel(res: ReportResult, entry: dict):
    v = entry["pbi_visual"]
    key_base = f"{res.name}__{v.visual_id}"
    header = (
        f"⚠ PENDING — Page **{entry['page_name']}** — `{v.visual_type}` "
        f"({v.display_name or v.visual_id[:8]})"
    )
    with st.expander(header, expanded=True):
        st.warning(
            "**No selection committed for this visual yet.** Cycling "
            "through options with *Show next option* does not commit a "
            "choice — you must click **Accept** (to include the currently "
            "shown option) or **Skip visual** (to omit it). Until then, "
            "the dashboard cannot be published."
        )
        st.caption(entry["reason"])

        cache = _get_or_create_alt_cache(res, v.visual_id, v)
        options: list[VisualAlternative] = cache["options"]
        idx = cache["idx"] % len(options)
        current = options[idx]

        st.markdown(
            f"**Option {idx + 1} of {len(options)}: {current.label}**  \n"
            f"{current.rationale}"
        )

        if current.kind == "widget":
            with st.container(border=True):
                c_preview, c_attrs = st.columns([2, 1])
                with c_preview:
                    preview_widget = apply_attributes_to_widget(
                        _copy.deepcopy(current.widget), current.attributes or {}
                    )
                    _scrub_misplaced_color_scale(preview_widget)
                    _render_widget_tile(
                        preview_widget,
                        res, key_suffix=f"alt_{key_base}_{idx}",
                    )
                with c_attrs:
                    st.caption("Explicit attributes")
                    st.json(current.attributes or {}, expanded=False)
        else:
            st.warning("This visual will be SKIPPED.")

        all_viewed = len(cache.get("viewed", set())) >= len(options)
        exhausted = bool(cache.get("exhausted"))
        more_enabled = all_viewed and not exhausted
        if exhausted:
            more_help = (
                "The model could not produce any additional unique, viable "
                "alternatives for this visual."
            )
        elif not all_viewed:
            more_help = (
                f"Enabled after you've seen all {len(options)} current options "
                f"(viewed {len(cache.get('viewed', set()))} so far)."
            )
        else:
            more_help = "Ask the model for additional realistic alternatives."

        last_note = cache.get("last_more_note")
        if last_note:
            st.info(last_note)

        b1, b2, b3, b4 = st.columns(4)
        if b1.button("Show next option", key=f"next_{key_base}",
                     use_container_width=True):
            cache["idx"] = (idx + 1) % len(options)
            cache.setdefault("viewed", set()).add(cache["idx"])
            cache["last_more_note"] = ""
            st.rerun()
        if b2.button("More options", key=f"more_{key_base}",
                     use_container_width=True,
                     disabled=not more_enabled,
                     help=more_help):
            with st.spinner("Generating more..."):
                fresh = suggest_alternatives(
                    v,
                    dataset_summaries=summarize_datasets(
                        res.dashboard_json, _ensure_dataset_columns(res)
                    ),
                    report_context=res.pbi_context,
                    temperature=0.7,
                )

            # Apply the same shape + dataset repair we ran on the
            # initial batch, otherwise "More options" results would
            # come back with widgetType="unknown" or a broken
            # datasetName and waste the user's click.
            preserve_colors = st.session_state.get("preserve_colors_toggle", True)
            ds_cols_more = _ensure_dataset_columns(res)
            for o in fresh:
                _repair_alternative_dataset_ref(o, res.dashboard_json)
                _repair_alternative_widget_shape(o, ds_cols_more)
                if preserve_colors and getattr(v, "colors", None):
                    _inject_brand_colors_into_alt(o, v.colors)

            known = cache.get("signatures", set())
            added: list[VisualAlternative] = []
            dup_count = 0
            invalid_count = 0
            for o in fresh:
                if o.kind == "skip":
                    continue
                if not _is_viable(o):
                    invalid_count += 1
                    continue
                sig = _option_signature(o)
                if sig in known:
                    dup_count += 1
                    continue
                known.add(sig)
                added.append(o)

            if added:
                non_skip = [o for o in options if o.kind != "skip"]
                skip_tail = [o for o in options if o.kind == "skip"][:1]
                cache["options"] = non_skip + added + skip_tail
                cache["signatures"] = known
                cache["idx"] = len(non_skip)
                cache["viewed"] = {cache["idx"]}
                cache["exhausted"] = False
                cache["last_more_note"] = (
                    f"Added {len(added)} new option(s). "
                    f"Skipped {dup_count} duplicate(s) "
                    f"and {invalid_count} unviable result(s)."
                    if (dup_count or invalid_count) else
                    f"Added {len(added)} new option(s)."
                )
            else:
                cache["exhausted"] = True
                cache["last_more_note"] = (
                    "No additional unique, viable alternatives were produced. "
                    f"(Model returned {dup_count} duplicate(s) and "
                    f"{invalid_count} unviable result(s).) "
                    "Pick from the existing options, or Skip."
                )
            st.rerun()
        if b3.button("Skip visual", key=f"skip_{key_base}",
                     use_container_width=True):
            _apply_skip(res, entry)
            st.rerun()
        accept_label = "Accept skip" if current.kind == "skip" else "Accept"
        if b4.button(accept_label, key=f"acc_{key_base}",
                     use_container_width=True, type="primary"):
            if current.kind == "skip":
                _apply_skip(res, entry)
            else:
                _apply_alternative(res, entry, current)
            st.rerun()


# ---------------------------------------------------------------------------
# Draft renderer (review UI shown before publish)
# ---------------------------------------------------------------------------

def render_draft(res: ReportResult):
    if res.status == "error":
        st.error(res.error_msg)
        if res.raw_traceback:
            with st.expander("Full traceback"):
                st.code(res.raw_traceback, language="text")
        return

    # Persistent banner that records what happened to the custom
    # instruction for THIS draft, so the user can always trace whether
    # their textarea was applied, partially applied, or rejected — even
    # after Streamlit reruns or accepting alternatives.
    iv = getattr(res, "instruction_verdict", None)
    if iv is not None and iv.verdict not in (None, "empty"):
        if iv.verdict == "accepted":
            st.success(
                "Custom instruction applied to this draft."
                + (f"  \n_Reason: {iv.reasoning}_" if iv.reasoning else "")
            )
        elif iv.verdict == "partial":
            with st.container(border=True):
                st.warning(
                    "Custom instruction **partially applied** — only the "
                    "in-scope clauses below were sent to the converter."
                )
                if iv.accepted_parts:
                    st.markdown("**Applied:**")
                    for p in iv.accepted_parts:
                        st.markdown(f"- {p}")
                if iv.rejected_parts:
                    st.markdown("**Ignored (out of scope):**")
                    for p in iv.rejected_parts:
                        st.markdown(f"- {p}")
        elif iv.verdict == "rejected":
            st.error(
                "Custom instruction was rejected — none of it was applied."
                + (f"  \n_Reason: {iv.reasoning}_" if iv.reasoning else "")
            )
        elif iv.verdict == "error":
            st.info(
                "Custom instruction was sent through without scope "
                "validation (classifier unavailable)."
            )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Datasets", res.n_datasets)
    c2.metric("Widgets", res.n_widgets)
    c3.metric("Pages", res.n_pages)
    c4.metric("Needs review", len(res.unmapped))


    if res.unmapped:
        st.subheader(f"{len(res.unmapped)} visual(s) need your attention")
        st.caption(
            "These PBI visuals either have no direct AI/BI equivalent or were not "
            "rendered by the model. Shuffle, skip, or accept an alternative. "
            "Visuals that already have a valid mapping are NOT changed."
        )

        # Debug visibility: show the user exactly which dataset columns were
        # probed and sent to the LLM in the alternatives prompt. If a probe
        # silently failed, the LLM is guessing column names off the truncated
        # SQL string and will hallucinate aliases like `total_quantity`.
        ds_cols_for_debug = _ensure_dataset_columns(res)
        all_ds = [
            ds.get("name") for ds in res.dashboard_json.get("datasets", [])
            if ds.get("name")
        ]
        probed_ok = [n for n in all_ds if ds_cols_for_debug.get(n)]
        probed_fail = [n for n in all_ds if not ds_cols_for_debug.get(n)]
        with st.expander(
            f"LLM grounding: {len(probed_ok)}/{len(all_ds)} datasets "
            f"with real columns sent to the model"
            + (" — column probe FAILED for some datasets, LLM is guessing"
               if probed_fail else " — full grounding"),
            expanded=bool(probed_fail),
        ):
            if probed_fail:
                st.warning(
                    "**Column probe failed** for: "
                    + ", ".join(f"`{n}`" for n in probed_fail)
                    + ". Without this, the alternatives prompt only has "
                    "the truncated dataset SQL and the model will invent "
                    "alias-style column names. Common cause: the warehouse "
                    "rejected `SELECT * FROM (...) WHERE 1=0` (e.g. dataset "
                    "SQL is itself broken) or the OBO token lacks `sql` "
                    "scope on this warehouse."
                )
            for ds_name in all_ds:
                cols = ds_cols_for_debug.get(ds_name) or []
                if cols:
                    st.markdown(
                        f"**`{ds_name}`** ({len(cols)} columns): "
                        + ", ".join(f"`{c}`" for c in cols)
                    )
                else:
                    st.markdown(f"**`{ds_name}`** — _probe failed_")

        for entry in list(res.unmapped):
            _render_alternatives_panel(res, entry)
    else:
        st.success(
            "**All PBI visuals mapped cleanly — nothing to shuffle.** "
            "The Shuffle / Skip / Accept controls only appear when a visual "
            "has no direct AI/BI equivalent (e.g. map, gauge, custom visual)."
        )

    st.divider()
    st.subheader("Dashboard preview")
    st.caption(
        "Best-effort rendering using Plotly + Streamlit. Exact AI/BI styling "
        "(fonts, axes, tooltips) may differ slightly after publish. "
        "Un-accepted alternatives for flagged visuals appear here tentatively; "
        "the 'Skip' option hides the visual entirely."
    )
    preview_json = _build_preview_dashboard_json(res) if res.unmapped else res.dashboard_json
    _render_dashboard_preview(res, preview_json)

    st.divider()

    # Pre-flight the OBO token so the user knows up front whether the
    # publish will run as them or fall back to the app's service
    # principal. We only nudge the user when something has changed
    # since the page loaded (token expired or never existed) — the
    # happy path stays silent.
    obo = _obo_status()
    if obo == "expired":
        st.warning(
            "Your On-Behalf-Of-User session has expired since this draft "
            "was generated. If you publish now, the dashboard will be "
            "created by the app's **service principal** and ownership "
            "transferred to you immediately afterward. To publish as "
            "yourself instead, refresh this page (you'll be re-prompted "
            "to sign in) and re-generate the draft."
        )
    elif obo == "absent":
        st.warning(
            "No On-Behalf-Of-User token is available for this session. "
            "Publishing will use the app's **service principal** to "
            "create the dashboard, then transfer ownership to you. If "
            "you expect OBO to be active, ask an account admin to "
            "enable the *Databricks Apps - User Token Passthrough* "
            "preview, then refresh this page."
        )

    pending = list(res.unmapped)
    if pending:
        pending_lines = []
        for entry in pending:
            v = entry["pbi_visual"]
            label = v.display_name or v.visual_id[:8]
            pending_lines.append(
                f"- **{entry['page_name']}** — `{v.visual_type}` ({label})"
            )
        st.error(
            f"**Cannot publish — {len(pending)} visual(s) have no selection yet.**  \n"
            "For each item below, scroll up to its alternatives panel and click "
            "either **Accept** (to include the currently shown option) or "
            "**Skip visual** (to omit it from the published dashboard). "
            "Cycling through options with *Show next option* does NOT count as "
            "a selection — the dashboard preview shows un-accepted options "
            "tentatively, but only Accepted/Skipped visuals are committed.\n\n"
            + "\n".join(pending_lines)
        )

    c_json, c_pub = st.columns([3, 1])
    with c_json:
        with st.expander("Raw dashboard JSON"):
            st.json(res.dashboard_json, expanded=False)
    with c_pub:
        publish_disabled = bool(pending)
        publish_help = (
            f"Disabled: {len(pending)} visual(s) still need Accept or Skip."
            if publish_disabled else None
        )
        if st.button("Publish to Databricks", type="primary",
                     key=f"publish_{res.name}", use_container_width=True,
                     disabled=publish_disabled, help=publish_help):
            prog = st.status(f"Publishing {res.name}...", expanded=True)
            publish_draft(res, prog)
            if res.status == "done":
                prog.update(label=f"{res.name} — published!", state="complete")
            else:
                prog.update(label=f"{res.name} — failed", state="error")
            st.rerun()


# ---------------------------------------------------------------------------
# Post-publish results display
# ---------------------------------------------------------------------------

def render_report_results(res: ReportResult):
    """Render the full results UI for one converted report inside a tab."""

    if res.status == "error":
        st.error(res.error_msg)
        if res.raw_traceback:
            with st.expander("Full traceback"):
                st.code(res.raw_traceback, language="text")
        return

    if res.status in ("pending", "running"):
        st.info("This report is still being processed...")
        return

    if res.status == "draft":
        render_draft(res)
        return

    st.success("Dashboard converted and published successfully!")

    n_filter_pages = res.n_pages - res.n_canvas
    lf = res.layout_fidelity

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Datasets", res.n_datasets)
    col2.metric("Widgets", res.n_widgets)
    col3.metric("Canvas Pages", f"{res.n_canvas}",
                delta=f"+ {n_filter_pages} filter page(s)" if n_filter_pages else None)
    col4.metric("PBI Tabs Matched", f"{lf.expected_pages} -> {res.n_canvas}" if lf else "N/A")

    st.markdown(f"**Report:** {res.name}")
    st.markdown(f"**Model:** `{MODEL}`")
    st.markdown(f"**Workspace path:** `{res.workspace_path}`")
    st.markdown(f"**[Open Dashboard]({res.dash_url})**")

    if res.pdf_bytes:
        st.download_button(
            ":material/download: Export Validation Report (PDF)",
            data=res.pdf_bytes,
            file_name=f"{res.name}_validation_report.pdf",
            mime="application/pdf",
            key=f"pdf_{res.name}",
        )

    with st.expander("Conversion Report", expanded=False):
        st.markdown(res.explanation)

    _render_tables_section(res)
    _render_validation_section(res)


def _render_tables_section(res: ReportResult):
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
            for entry in lf.page_visual_counts:
                st.markdown(
                    f"- **Page \"{entry['name']}\":** {entry['actual']} data widget(s) "
                    f"(expected {entry['expected']} from PBI)"
                )

        if validation.sql_results:
            st.markdown("#### SQL Query Validation")
            for ds_name, succeeded, error_msg, cols in validation.sql_results:
                if succeeded:
                    st.markdown(f"- `{ds_name}` — query OK, {len(cols)} columns returned")
                else:
                    st.markdown(f"- `{ds_name}` — {error_msg}")

        if validation.errors:
            st.markdown("#### Errors")
            for err in validation.errors:
                st.markdown(f"- {err}")

        if validation.warnings:
            st.markdown("#### Warnings")
            for warn in validation.warnings:
                st.markdown(f"- {warn}")

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


# ---------------------------------------------------------------------------
# UI Layout
# ---------------------------------------------------------------------------

st.title("Power BI -> AI/BI Converter")
st.caption(
    f"Upload up to **10** Power BI projects (.pbip) as **zip files** and convert them to "
    f"Databricks AI/BI dashboards using **{MODEL}**. "
    f"Review a preview and shuffle/skip any visuals before publishing."
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

# ---------------------------------------------------------------------------
# Identity + warehouse picker (per-session)
# ---------------------------------------------------------------------------

_active_user_email = _user_email()
_active_user_client = _user_client()

with st.container(border=True):
    st.markdown("**Identity & SQL warehouse**")
    _sp_client_id = (os.getenv("DATABRICKS_CLIENT_ID") or "").strip()
    if _active_user_client is not None:
        st.success(
            f"Signed in as **{_active_user_email or '(unknown)'}**. "
            "On-Behalf-Of-User (OBO) authorization is **active**. "
            "Warehouse listing, SQL previews, schema probing, and Genie "
            "space creation run as **you**. The two `lakeview` calls "
            "(create + publish) run as the app's service principal — "
            "this is a Databricks platform constraint, not a "
            "misconfiguration; the `dashboards` API scope is not yet "
            "exposed in the OBO scope catalog. Ownership of the "
            "published dashboard transfers to you immediately after "
            "create, and every query the dashboard runs at view-time "
            "executes as the viewer's identity (`embed_credentials="
            "False`). The SP never sits on the runtime query path."
        )
    else:
        st.warning(
            "On-Behalf-Of-User authorization is **NOT active** for this "
            "session. The app will fall back to its service principal "
            "for every Databricks API call (warehouse listing, build-"
            "time SQL queries, dashboard create / publish). For this to "
            "work, the SP **must** be granted access on every resource "
            "the conversion will touch.\n\n"
            f"**App service principal:** `{_sp_client_id or '(env var DATABRICKS_CLIENT_ID is not set)'}`\n\n"
            "**Required grants** (run as a workspace admin or the resource owner):\n"
            "```bash\n"
            "# 1. CAN_USE on every SQL warehouse you might pick in the dropdown\n"
            f"databricks permissions update warehouses <WAREHOUSE_ID> --json '{{\"access_control_list\":[{{\"service_principal_name\":\"{_sp_client_id or '<SP_UUID>'}\",\"permission_level\":\"CAN_USE\"}}]}}'\n\n"
            "# 2. SELECT on every Unity Catalog table the source PBI report references\n"
            f"GRANT SELECT ON TABLE <catalog>.<schema>.<table> TO `{_sp_client_id or '<SP_UUID>'}`;\n"
            "# (run that in a SQL editor for each table)\n\n"
            "# 3. CAN_EDIT on the destination workspace folder (typically /Workspace/Shared/aibi_converter)\n"
            f"databricks permissions update directories <DIRECTORY_ID> --json '{{\"access_control_list\":[{{\"service_principal_name\":\"{_sp_client_id or '<SP_UUID>'}\",\"permission_level\":\"CAN_EDIT\"}}]}}'\n"
            "```\n\n"
            "To switch to the OBO path (no SP grants required), an "
            "**account admin** must enable the *Databricks Apps - User "
            "Token Passthrough* preview in Account Console -> Previews. "
            "Once enabled, the app will pick up `user_api_scopes: [sql, "
            "dashboards.genie]` from `app.yaml` automatically and run as "
            "the signed-in user."
        )

    _warehouse_options: list[tuple[str, str]] = []
    _warehouse_lookup_err: str | None = None
    _wh_listing_client = _active_user_client or _sp_client()

    # The dashboard is owned by the signed-in user. The dropdown should
    # therefore reflect THAT user's permissions — not the app SP's. The
    # SP is only used to enumerate the workspace's warehouses (which is
    # all the app can do without OBO); the per-warehouse access check
    # against the user's ACL is what decides whether each one shows up.
    _caller_principals: set[str] = set()
    if _active_user_email:
        _caller_principals.add(_active_user_email.lower())
    _user_upn = _user_principal()
    if _user_upn:
        _caller_principals.add(_user_upn.lower())

    _USABLE_LEVELS = {"CAN_USE", "CAN_MANAGE", "IS_OWNER"}

    _acl_read_failures: list[str] = []

    def _user_can_use_warehouse(client, wid: str, name: str) -> bool:
        """Decide if the signed-in user can query this warehouse.

        Checks the warehouse's ACL for any of:
          * direct grant on the user's email or UPN,
          * `admins` group (workspace admins implicitly CAN_MANAGE),
          * `users` group (everyone in the workspace).

        If we can't identify the user at all, return True (anonymous /
        local dev path).

        If we *can* identify the user but the ACL read fails, RETURN
        FALSE and stash the warehouse name in `_acl_read_failures`. The
        previous behavior was to be permissive and surface every
        un-readable warehouse — that defeats the entire user-scoping
        story when the SP lacks the CAN_VIEW grant on the warehouses
        list (which is exactly the configuration this branch fires in).
        We surface the failures in a UI warning so the operator can fix
        the SP grants instead of silently shipping a misleading dropdown.
        """
        if not _caller_principals:
            return True
        try:
            perms = client.permissions.get(
                request_object_type="warehouses",
                request_object_id=wid,
            )
        except Exception as e:
            _acl_read_failures.append(f"{name or wid}: {e}")
            return False
        acls = (perms.access_control_list or []) if perms else []
        for acl in acls:
            principal_candidates = {
                (getattr(acl, "user_name", None) or "").lower(),
                (getattr(acl, "group_name", None) or "").lower(),
            }
            principal_candidates.discard("")
            is_user_match = bool(principal_candidates & _caller_principals)
            is_open_group = bool(principal_candidates & {"users", "admins"})
            if is_user_match or is_open_group:
                for lvl in (acl.all_permissions or []):
                    plvl = getattr(lvl.permission_level, "value", lvl.permission_level)
                    if plvl in _USABLE_LEVELS:
                        return True
        return False

    try:
        for _wh in _wh_listing_client.warehouses.list():
            if _user_can_use_warehouse(_wh_listing_client, _wh.id, _wh.name or _wh.id):
                _warehouse_options.append((_wh.id, _wh.name or _wh.id))
    except Exception as _e:
        _warehouse_lookup_err = str(_e)

    if _acl_read_failures:
        st.warning(
            "The app could not read the ACL on some SQL warehouses and "
            "has hidden them from the dropdown to avoid surfacing "
            "warehouses you can't actually use. To debug, run "
            "`databricks permissions get warehouses <id>` as the app's "
            "service principal. Hidden warehouses: "
            + ", ".join(f"`{f.split(':',1)[0]}`" for f in _acl_read_failures[:8])
            + ("." if len(_acl_read_failures) <= 8
               else f" (+{len(_acl_read_failures)-8} more).")
        )

    if _warehouse_lookup_err:
        st.error(
            f"Could not list SQL warehouses: {_warehouse_lookup_err}"
        )
    if not _warehouse_options:
        st.error(
            "No SQL warehouses you can query. Ask a workspace admin to grant "
            "you **CAN USE** on at least one SQL warehouse."
        )
        st.stop()

    _warehouse_options.sort(key=lambda t: t[1].lower())
    _label_by_id = {wid: f"{name}  ({wid})" for wid, name in _warehouse_options}
    _id_by_label = {v: k for k, v in _label_by_id.items()}
    _default_env_wid = os.getenv("DATABRICKS_WAREHOUSE_ID", "").strip()
    _default_idx = 0
    if _default_env_wid and _default_env_wid in _label_by_id:
        _default_idx = list(_label_by_id.values()).index(_label_by_id[_default_env_wid])

    _selected_label = st.selectbox(
        "SQL warehouse for the dashboard",
        options=list(_label_by_id.values()),
        index=_default_idx,
        help=(
            "This warehouse is used for previews, validation, and as the "
            "compute backing the published dashboard. The dropdown lists "
            "warehouses YOU can query (CAN_USE / CAN_MANAGE / IS_OWNER, "
            "or membership in `users`/`admins`). The app's service "
            "principal is not part of this filter."
        ),
        key="selected_warehouse_label",
    )
    _selected_warehouse_id = _id_by_label[_selected_label]

    _preserve_colors = st.toggle(
        "Preserve brand colors",
        value=st.session_state.get("preserve_colors_toggle", True),
        help=(
            "Extract hex color codes from each PBI visual and inject them "
            "into the generated AI/BI dashboard (mark.colors, "
            "scale.mappings, etc.). Turn off to use Databricks default "
            "colors instead."
        ),
        key="preserve_colors_toggle",
    )

st.divider()

uploaded_files = st.file_uploader(
    "Upload .pbip project(s) (zip files)",
    type=["zip"],
    accept_multiple_files=True,
    help="Zip(s) containing the .pbip file, .Report/ folder, and .SemanticModel/ folder. Max 10 files.",
)

if uploaded_files and len(uploaded_files) > 10:
    st.error("Please upload at most 10 files at a time.")
    st.stop()

custom_names: dict[str, str] = {}
if uploaded_files:
    with st.container(border=True):
        st.markdown("**Dashboard names**")
        st.caption(
            "Each dashboard defaults to the uploaded zip's filename. "
            "Override any name below."
        )
        for uf in uploaded_files:
            default = os.path.splitext(uf.name)[0]
            custom_names[uf.name] = st.text_input(
                label=f"Dashboard name for `{uf.name}`",
                value=default,
                key=f"dash_name__{uf.name}",
                max_chars=200,
            )

custom_instructions = st.text_area(
    "Custom Instructions (optional)",
    placeholder=(
        "e.g. prefix every widget name with 'q4_' / put all KPIs in a "
        "single top row / use AVG instead of SUM for unitPrice / rename "
        "the dataset table from sales_transactions to sales_data"
    ),
    help=(
        "These instructions only influence **this dashboard's** widgets, "
        "layout, colors, filters, aggregations, and dataset SQL. They are "
        "scope-checked before the conversion runs.\n\n"
        "**In scope:**\n"
        "- Widget names, titles, ordering, grouping, sizing, layout\n"
        "- Page composition (split / merge / rename pages)\n"
        "- Visual color overrides\n"
        "- Filter defaults, multi vs single select\n"
        "- Aggregation choices (SUM / AVG / COUNT / DISTINCTCOUNT)\n"
        "- Column / table renames in the generated dataset SQL\n"
        "- Skipping or including specific PBI visual types\n\n"
        "**Out of scope (will be rejected):**\n"
        "- Genie spaces, AI agents, model serving endpoints\n"
        "- SQL warehouse provisioning / sizing / permissions\n"
        "- User / group / SP permission changes\n"
        "- Catalog / schema / table CREATE or DROP\n"
        "- App deployment, secrets, workspace settings"
    ),
    height=110,
)

convert_clicked = st.button("Generate Draft", type="primary", use_container_width=True)

# ---------------------------------------------------------------------------
# Batch draft generation
# ---------------------------------------------------------------------------

if convert_clicked:
    if not uploaded_files:
        st.error("Please upload at least one .pbip zip file.")
        st.stop()

    # ------------------------------------------------------------------
    # Pre-flight: scope-validate the custom instruction before we spend
    # any time/tokens on conversion. Out-of-scope clauses are stripped;
    # fully off-topic instructions block the run entirely with a clear
    # explanation.
    # ------------------------------------------------------------------
    from instruction_guard import validate_custom_instructions

    instruction_verdict = validate_custom_instructions(custom_instructions)
    st.session_state["last_instruction_verdict"] = instruction_verdict
    effective_instructions = instruction_verdict.applied_text

    if instruction_verdict.verdict == "rejected":
        st.error(
            "**Custom instruction rejected — out of scope.**\n\n"
            f"{instruction_verdict.reasoning}\n\n"
            "Custom instructions can only influence **this dashboard's** "
            "widgets, layout, colors, filters, aggregations, and the "
            "generated SQL. They cannot configure Genie spaces, "
            "warehouses, permissions, or any system outside the dashboard."
        )
        if instruction_verdict.rejected_parts:
            st.markdown("**Rejected clauses:**")
            for part in instruction_verdict.rejected_parts:
                st.markdown(f"- {part}")
        st.info(
            "Edit or remove the **Custom Instructions** above and click "
            "**Generate Draft** again."
        )
        st.stop()

    if instruction_verdict.verdict == "partial":
        st.warning(
            "**Custom instruction partially accepted.** Some clauses were "
            "in scope and will be applied; the rest were out of scope and "
            "will be ignored."
        )
        if instruction_verdict.accepted_parts:
            st.markdown("**Accepted (will be applied):**")
            for part in instruction_verdict.accepted_parts:
                st.markdown(f"- {part}")
        if instruction_verdict.rejected_parts:
            st.markdown("**Ignored (out of scope):**")
            for part in instruction_verdict.rejected_parts:
                st.markdown(f"- {part}")
        st.caption(instruction_verdict.reasoning)
    elif instruction_verdict.verdict == "accepted":
        st.success(
            "**Custom instruction accepted.** Will be applied to every "
            "widget the converter generates."
        )
        if instruction_verdict.accepted_parts:
            with st.expander("What will be applied", expanded=False):
                for part in instruction_verdict.accepted_parts:
                    st.markdown(f"- {part}")
    elif instruction_verdict.verdict == "error":
        st.warning(
            "Could not validate the custom instruction (classifier "
            "unavailable). The instruction was **dropped for safety** "
            "and the conversion will proceed without it. Re-run later "
            "if you want it applied."
        )
    # verdict == "empty" → no UI noise

    st.session_state["results"] = []
    st.session_state["batch_running"] = True
    n_files = len(uploaded_files)
    results: list[ReportResult] = []

    overall = st.container()
    overall.markdown(f"### Batch draft: {n_files} report(s)")

    for idx, uf in enumerate(uploaded_files):
        raw_name = (custom_names.get(uf.name) or os.path.splitext(uf.name)[0]).strip()
        report_name = raw_name or os.path.splitext(uf.name)[0]
        overall.markdown(f"---\n**[{idx + 1}/{n_files}]** Drafting **{report_name}**...")
        progress = overall.status(f"Drafting {report_name}...", expanded=True)

        result = generate_draft(
            uf,
            report_name,
            progress,
            warehouse_id=_selected_warehouse_id,
            custom_instructions=effective_instructions,
            preserve_colors=_preserve_colors,
        )
        # Stamp the instruction verdict onto the result so we can
        # surface it next to the draft preview on every rerun.
        result.instruction_verdict = instruction_verdict
        results.append(result)

        if result.status == "draft":
            progress.update(label=f"{report_name} — draft ready", state="complete")
        else:
            progress.update(label=f"{report_name} — failed", state="error")
            overall.error(f"**{report_name}** failed: {result.error_msg}")

    st.session_state["results"] = results
    st.session_state["batch_running"] = False

    n_drafts = sum(1 for r in results if r.status == "draft")
    n_err = sum(1 for r in results if r.status == "error")
    overall.markdown(
        f"### Drafts ready: {n_drafts} ok, {n_err} failed. "
        "Scroll down to review each draft and publish when satisfied."
    )

# ---------------------------------------------------------------------------
# Results Tabs (drafts + published, persisted across reruns)
# ---------------------------------------------------------------------------

results: list[ReportResult] = st.session_state.get("results", [])

if results:
    st.divider()
    st.subheader("Review & Publish")

    def _tab_label(r: ReportResult) -> str:
        marker = {
            "draft": "●",
            "done": "✓",
            "error": "✗",
            "running": "…",
        }.get(r.status, "•")
        return f"{marker} {r.name}"

    tabs = st.tabs([_tab_label(r) for r in results])
    for tab, res in zip(tabs, results):
        with tab:
            render_report_results(res)
