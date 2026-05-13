# Power BI -> AI/BI Dashboard Converter

A Databricks App (Streamlit) that converts Power BI projects (`.pbip`) into Databricks AI/BI dashboards.

**Identity model (read this part):**

* **OBO (signed-in user)** is used for: SQL warehouse listing & permission filtering, schema probing, preview SQL, Genie space creation, model-serving (LLM) calls, and the dashboard ownership transfer.
* **App service principal (SP)** is used for exactly two calls: `lakeview.create` and `lakeview.publish`. The Lakeview API gates these on a `dashboards` OAuth scope that is not currently exposed in the OBO scope catalog (the Apps `user_api_scopes` field rejects it). As soon as `lakeview.create` returns, the app calls `permissions.set` to transfer ownership of the new dashboard to the signed-in user. Every query the *published* dashboard runs at view-time uses the viewer's identity (`embed_credentials=False`), so the SP never sits on the data path.
* **Net effect:** the user owns the dashboard, the user's identity runs every read at view-time, and the SP only briefly holds the create/publish handle. If OBO is disabled at the org level the app falls back to the SP for everything; the in-app identity panel surfaces this clearly.

---

## What you get

- Upload one or more `.pbip` zips, get back AI/BI dashboards published to your workspace
- LLM-driven mapping of PBI visuals → AI/BI widgets (`databricks-claude-opus-4-6` by default)
- Per-visual color preservation, dataset-reference repair, layout blueprint
- Built-in widget validator that re-runs each query against your warehouse before publishing
- PDF export of the conversion report
- Instruction-injection guard on user-supplied free-text prompts

---

## Architecture & credentials at a glance

```
┌──────────┐     OAuth (Entra/Okta)      ┌────────────────────┐
│ Browser  │────────────────────────────>│ Databricks Apps    │
└──────────┘                              │ proxy              │
       │                                  │ (mints OBO token)  │
       │                                  └─────────┬──────────┘
       │                                            │ X-Forwarded-Access-Token
       │                                            ▼
       │                                  ┌────────────────────┐
       │   user-owned dashboards          │ Streamlit container│
       │ <────────────────────────────────│ - app.py           │
       │                                  │ - converter.py     │
       │                                  │ - clients.py       │
       │                                  └─────────┬──────────┘
       │                                            │
       │   OBO token (as user) ─────────────────────┤
       │   SP m2m (only for warehouse list / LLM)───┘
       ▼
┌────────────────────────────────────────────────────────┐
│ Databricks workspace (SQL Warehouses, Dashboards,     │
│ Genie, Workspace files, Permissions, Model Serving)    │
└────────────────────────────────────────────────────────┘
```

| Identity | Where it comes from | Used for |
|---|---|---|
| **Signed-in user (OBO)** | `X-Forwarded-Access-Token` header injected by the Apps proxy | Listing warehouses (filtered to user's ACL), running SQL for previews/validation, creating Genie spaces. **Owns every published dashboard** (ownership transferred immediately after create). |
| **App service principal** | `DATABRICKS_HOST` + `DATABRICKS_CLIENT_ID` + `DATABRICKS_CLIENT_SECRET` env vars auto-injected by the runtime | Enumerating workspace warehouses to populate the picker (read-only); calling Databricks Model Serving (`/serving-endpoints/...`) for the LLM; calling `lakeview.create` and `lakeview.publish` (currently not exposed to OBO — see [Why no `dashboards` scope?](#missing-scopes-on-obo-token) below). Ownership of the resulting dashboard transfers to the user immediately after create. |

**Nothing is hardcoded.** No tokens, no client secrets, no workspace URL, no warehouse ID, no email. Each customer deploying this app gets their own SP, their own OBO scopes, and their own users — the code reads everything from runtime env / headers.

---

## Prerequisites

You need:

1. **A Databricks workspace** (Azure / AWS / GCP) with:
   - Databricks Apps enabled
   - **On-Behalf-Of-User** authorization enabled (one-time, account-level — see [Step 1](#step-1-enable-on-behalf-of-user-obo) below)
   - At least one **SQL warehouse** the end-users have `CAN_USE` on
   - **Databricks Foundation Model APIs** access (or a custom serving endpoint) for the LLM. Default endpoint is `databricks-claude-opus-4-6`; override via `LLM_MODEL` env var.
2. **Workspace user permissions** for whoever deploys the app:
   - `Workspace access` (any user)
   - Permission to create apps in this workspace
3. **Local tooling** (CLI deploy only):
   - [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install) v0.205+
   - Python 3.11 (only for local dev; not needed to deploy)

---

## Step 1 — Enable On-Behalf-Of-User (OBO)

OBO is what lets the app act *as the signed-in user* instead of as its service principal. You only need to do this **once per workspace**.

### Account-level toggle (account admin)

1. Sign in to the **Databricks account console** (`https://accounts.<cloud>.databricks.com`)
2. **Settings -> Previews**
3. Enable **"Databricks Apps - User Token Passthrough"** (also called *Apps OBO*)
4. (AWS/GCP only) Confirm your workspace's identity provider is configured for federated identity — Azure workspaces get this automatically via Entra.

### Verify

After deploying the app (later steps), the first page renders a green banner:

> Signed in as `<you>`. **On-Behalf-Of-User authorization is active** — every Databricks API call (warehouse listing, SQL queries, dashboard create / publish, permissions) runs as **you**.

If you instead see an orange "OBO is NOT active" banner, OBO is not enabled at the account level — go back to step 1.

---

## Step 2 — Permissions the deployer / users need

### The user who deploys the app

- `CAN MANAGE` on the new app (granted automatically to the creator)

### Each end-user who will USE the app

- `CAN USE` on the app itself (grant after creation)
- `CAN USE` on at least one **SQL warehouse** (the picker hides warehouses they can't query)
- `SELECT` on every UC table the source PBI report references (otherwise validation will fail at publish time)
- `CAN_EDIT` on the destination workspace folder (default `/Workspace/Shared/aibi_converter`, override with `DASHBOARD_PARENT_PATH`)

### The app's service principal

Because all data-plane calls run via OBO, the SP needs **almost nothing**:

- `Workspace access` (auto-granted on app creation)
- `CAN_VIEW` on the SQL warehouse list endpoint (auto, comes with workspace access)
- Token access to the model serving endpoint named in `LLM_MODEL` (default Foundation Model APIs are accessible to all SPs in the workspace; if you point to a custom endpoint, grant the app SP `CAN_QUERY` on it)

If you ever see `Provided OAuth token does not have required scopes: sql` — that's not a permissions issue, that's an `app.yaml` issue (see [Troubleshooting](#troubleshooting)).

---

## Step 3 — Deploy the app

Pick **one** of CLI or UI; both produce the same result.

### Option A — Deploy via Databricks CLI (recommended for repeatability)

```bash
git clone <your-fork-url> powerbi-to-aibi-converter
cd powerbi-to-aibi-converter

databricks auth login --host https://<your-workspace-host>
databricks auth profiles

WORKSPACE_PATH="/Workspace/Users/$(databricks current-user me -p DEFAULT --output json | jq -r .userName)/powerbi-to-aibi-converter"
databricks workspace import-dir . "$WORKSPACE_PATH" -p DEFAULT --overwrite

databricks apps create powerbi-to-aibi-converter -p DEFAULT \
  --description "Convert Power BI projects to Databricks AI/BI dashboards"

databricks apps update powerbi-to-aibi-converter -p DEFAULT \
  --json '{"user_api_scopes": ["sql", "dashboards.genie"]}'

databricks apps deploy powerbi-to-aibi-converter -p DEFAULT \
  --source-code-path "$WORKSPACE_PATH"

databricks apps get powerbi-to-aibi-converter -p DEFAULT --output json | jq -r .url
```

The `apps update --json '{"user_api_scopes": ...}'` step looks redundant (the same scopes are in `app.yaml`), but the App **resource** field is what the proxy actually consults when minting OBO tokens. Setting it once via the API is the only reliable way to guarantee the proxy sees the scopes — relying on `app.yaml` propagation alone has been known to leave the resource at `user_api_scopes: null`, which produces "OAuth token does not have required scopes" at runtime.

### Option B — Deploy via Databricks UI

1. **Sync the code into your workspace.** Either:
   - Use **Repos**: Workspace -> Create -> Git folder, point at your fork, default branch.
   - Or upload via **Workspace -> Users -> `<you>` -> Import** (zip the repo first, then import as a folder).
2. **Compute -> Apps -> Create app**:
   - **Name**: `powerbi-to-aibi-converter`
   - **App template**: *Custom*
   - **Source code path**: the workspace path you synced to in step 1
   - Click **Create** and wait for the SP to be provisioned (~30s)
3. **Configure scopes** (this is the step the UI hides — do not skip):
   - Open the app -> **Authorization** tab
   - Under **User API scopes**, add `sql` and `dashboards.genie`
   - Save
4. **Deploy**:
   - **Deploy** button -> select the same source path -> **Deploy**
   - Wait for status `RUNNING / ACTIVE`
5. Open the app URL shown on the app page. You should see the green OBO banner.

If your UI version doesn't expose the **User API scopes** field on the Authorization tab (some workspace versions don't), fall back to one CLI call after you create the app:

```bash
databricks apps update powerbi-to-aibi-converter -p DEFAULT \
  --json '{"user_api_scopes": ["sql", "dashboards.genie"]}'
```

---

## Step 4 — Grant end-users access

After the app is `RUNNING`, by default only the creator can open it. To let others in:

### CLI

```bash
databricks apps set-permissions powerbi-to-aibi-converter -p DEFAULT --json '{
  "access_control_list": [
    {"group_name": "users", "permission_level": "CAN_USE"}
  ]
}'
```

### UI

App page -> **Permissions** -> add a user / group with **CAN USE**.

Then make sure those users also have:

- `CAN USE` on at least one SQL warehouse (Compute -> SQL Warehouses -> *<your warehouse>* -> Permissions)
- `SELECT` on the UC tables your PBI reports query (Catalog Explorer -> *<table>* -> Permissions, or `GRANT SELECT ON TABLE ... TO ...`)

---

## Step 5 — Use the app

1. Open the app URL. Confirm the green OBO banner.
2. Pick a SQL warehouse from the dropdown.
3. Upload one or more `.pbip` zip files (instructions for creating them are in the in-app help).
4. Click **Generate AI/BI dashboard preview**.
5. Review per-visual mappings; toggle preserve-brand-colors if needed.
6. Click **Publish**. Each dashboard lands at `/Workspace/Shared/aibi_converter/<report_name>/<report_name>.lvdash.json` (or wherever `DASHBOARD_PARENT_PATH` points).
7. Optionally export the conversion report as PDF.

---

## Configuration reference

All config is via env vars set in `app.yaml` (or in the **Environment** tab of the app in the UI). Nothing is required.

| Var | Default | Purpose |
|---|---|---|
| `LLM_MODEL` | `databricks-claude-opus-4-6` | Name of the Databricks Model Serving endpoint to use for conversion. Must be an OpenAI-compatible chat endpoint. |
| `DATABRICKS_WAREHOUSE_ID` | *(unset)* | Optional. If set, this warehouse is preselected in the picker. The picker still lists all warehouses the user has `CAN_USE` on. |
| `DASHBOARD_PARENT_PATH` | `/Workspace/Shared/aibi_converter` | Workspace folder where published dashboards land. Folder is created on first publish. |

The runtime additionally injects (do not set these yourself):

- `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET` — the app SP m2m credentials
- `X-Forwarded-Access-Token`, `X-Forwarded-Email`, `X-Forwarded-User` — per-request user identity (HTTP headers)

---

## Local development

You can run the Streamlit app locally to iterate on UI / prompts. OBO is not available locally (no Apps proxy), so all calls fall back to your personal CLI auth.

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

databricks auth login --host https://<your-workspace-host> --profile DEFAULT
export DATABRICKS_CONFIG_PROFILE=DEFAULT

streamlit run app.py
```

In local mode the app behaves as if you are the signed-in user *and* the SP at the same time (your PAT/OAuth is used everywhere). The UI banner will show the orange "OBO not active" warning — that's expected locally.

---

## Updating an already-deployed app

```bash
databricks workspace import-dir . "$WORKSPACE_PATH" -p DEFAULT --overwrite
databricks apps deploy powerbi-to-aibi-converter -p DEFAULT \
  --source-code-path "$WORKSPACE_PATH"
```

Or in the UI: re-sync the source folder (push to your Git repo if using Git folders), then click **Deploy** on the app page.

---

## Troubleshooting

### `invalid_client: Client authentication failed` (in container logs / on LLM call)

The app's service principal has been deleted out from under the app. The container keeps running on cached creds for OBO requests, but any call that triggers an SP m2m token exchange (LLM, warehouse list before login) fails immediately.

**Fix:** delete and recreate the app. The SP cannot be restored.

```bash
databricks apps delete powerbi-to-aibi-converter -p DEFAULT
```

Then redeploy via Step 3.

### `Provided OAuth token does not have required scopes: <scope>`

The app resource is missing one of the OBO scopes the API call needs. Most common:

| Missing scope | What it breaks |
|---|---|
| `sql` | Listing warehouses, running preview/validation SQL |
| `dashboards.genie` | Creating a Genie space pre-loaded with the converted dashboard's datasets |

> **Why dashboard create/publish runs as the SP, not OBO.** The Databricks `lakeview.create` and `lakeview.publish` endpoints are gated on a `dashboards` API scope that is **not exposed in the OBO scope catalog** today (the CLI rejects `dashboards` as an invalid scope: `databricks apps update --json '{"user_api_scopes":["dashboards"]}'` -> `Error: The specified scope dashboards is not a valid scope`). So the app calls those two endpoints with its service principal, then `permissions.set(...)` immediately transfers `CAN_MANAGE` (= ownership) to the signed-in user. Publish runs with `embed_credentials=False`, so every viewer queries the warehouse with their own identity — the SP is on the dashboard-create path for one round trip and never touches the runtime query path. If/when Databricks exposes a `dashboards` scope for OBO, this app can switch to a single OBO create+publish call.

**Fix** (covers all three at once):

```bash
databricks apps update powerbi-to-aibi-converter -p DEFAULT \
  --json '{"user_api_scopes": ["sql", "dashboards.genie"]}'
```

Then **sign out of the app and back in** (the OBO token is cached in your session cookie for the lifetime of the cookie; new scopes only show up on a fresh sign-in).

### `more than one authorization method configured`

Something constructed a `WorkspaceClient` while both PAT (OBO token) and OAuth (SP env vars) were visible to the SDK. This shouldn't happen with the shipped code (`clients.py` and `app.py` both pop the SP env vars before constructing the OBO client) but if you've added a code path that uses `Config()` directly, do the same dance.

### Orange "OBO is NOT active" banner

OBO is not enabled at the account level. See [Step 1](#step-1-enable-on-behalf-of-user-obo). If it IS enabled, double-check that you set `user_api_scopes` on the app resource (not just in `app.yaml`).

### `No SQL warehouses you can query`

The signed-in user has no `CAN_USE` warehouses. Grant `CAN USE` to them on at least one warehouse and refresh.

### Dashboard publish fails with `permission denied on /Workspace/Shared/aibi_converter`

Either the user lacks `CAN_EDIT` on that folder, or you want to publish elsewhere. Grant the permission, or set `DASHBOARD_PARENT_PATH` to a folder the user can write to.

### LLM call fails / times out

`LLM_MODEL` points to an endpoint that doesn't exist or that the app's SP can't query. Check **Serving** in the workspace UI and either grant `CAN_QUERY` on that endpoint to the app's SP, or change `LLM_MODEL` to one that's available (`databricks-meta-llama-3-3-70b-instruct`, etc.).

---

## File layout

```
.
├── app.py                  # Streamlit UI + auth helpers + orchestration
├── app.yaml                # Apps runtime config (command, scopes, env)
├── clients.py              # WorkspaceClient + OpenAI client factories
├── converter.py            # PBI -> AI/BI mapping prompt + LLM glue
├── alternatives.py         # Visual-alternative suggestions
├── color_utils.py          # Per-widget color extraction & SQL probes
├── export_pdf.py           # PDF export of conversion report
├── instruction_guard.py    # Prompt-injection guard for free-text inputs
├── validator.py            # Runs each generated query against the warehouse
├── knowledge/              # Markdown context loaded into the LLM prompt
├── static/                 # Images served by the Streamlit UI
├── .streamlit/             # Streamlit theme/config
├── requirements.txt
├── .gitignore
└── README.md
```

---

## License

Internal / customer-specific. Add your license of choice before public distribution.
