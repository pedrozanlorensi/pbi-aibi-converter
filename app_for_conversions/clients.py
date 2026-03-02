"""
Shared configuration constants and client factories for the PBI-to-AIBI converter.

Provides authenticated clients for:
  - Databricks workspace operations (via service principal or forwarded user token)
  - LLM inference via Databricks Model Serving (OpenAI-compatible)
"""

import os
from pathlib import Path

import streamlit as st
from openai import OpenAI
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

MODEL = os.getenv("LLM_MODEL", "databricks-claude-opus-4-6")
KNOWLEDGE_DIR = Path(__file__).parent / "knowledge"
STATIC_DIR = Path(__file__).parent / "static"
GRID_COLUMNS = 6

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
