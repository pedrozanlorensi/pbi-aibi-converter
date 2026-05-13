"""Gatekeeper for the user-supplied "Custom Instructions" textarea.

The downstream Power BI -> AI/BI converter will obediently include any
free-text instruction the user types, and the LLM will sometimes act on
out-of-scope requests (or, worse, ignore everything because the
instruction was confusing). This module scopes what the textarea is
allowed to influence and tells the user explicitly whether their
instruction was accepted, partially accepted, or rejected.

Allowed scope (in-scope):
  * widget names, titles, ordering, grouping, sizing, layout
  * page composition (splitting / merging pages)
  * visual color overrides
  * filter defaults, multi vs single select
  * aggregation choices (SUM vs AVG, COUNT vs DISTINCTCOUNT, etc.)
  * dataset SQL column / table renames inside the generated query
  * skipping or including specific PBI visual types

Out of scope (auto-rejected):
  * Genie spaces, AI agents, model serving endpoints
  * SQL warehouse provisioning / sizing
  * User / group / SP permission changes
  * Catalog / schema / table CREATE or DROP
  * App deployment, secrets, workspace settings
  * Anything that touches systems outside this single dashboard's
    generated artefacts.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field

from clients import MODEL, get_llm_client


@dataclass
class InstructionVerdict:
    verdict: str  # "accepted" | "partial" | "rejected" | "empty" | "error"
    reasoning: str = ""
    accepted_parts: list[str] = field(default_factory=list)
    rejected_parts: list[str] = field(default_factory=list)
    applied_text: str = ""   # cleaned in-scope text safe to send onward


_GATEKEEPER_SYSTEM_PROMPT = """\
You are a strict gatekeeper for a Power BI to Databricks AI/BI dashboard
converter. The downstream converter only generates ONE specific
artefact: a `lvdash.json` describing widgets, pages, datasets, filters,
and styling. Custom instructions from the user can only influence that
artefact.

ALLOWED scope:
  * widget names, titles, ordering, grouping, sizing, layout
  * page composition (split / merge pages, page titles)
  * visual color overrides
  * filter defaults, multi vs single select
  * aggregation choices (SUM vs AVG, COUNT vs DISTINCTCOUNT, etc.)
  * dataset SQL column / table renames inside the generated query
  * skipping or including specific PBI visual types

OUT OF SCOPE (must be rejected):
  * Genie spaces, AI agents, model serving endpoints
  * SQL warehouse provisioning, sizing, permissions
  * User / group / service-principal permission changes
  * Catalog / schema / table CREATE or DROP, Unity Catalog admin
  * App deployment, secrets, workspace settings
  * Anything outside this single dashboard's generated artefacts

Classify the user's instruction. Split it into clauses and judge each
clause independently:
  * "accepted" — every clause is in scope
  * "partial"  — at least one clause is in scope and at least one is out
  * "rejected" — no clause is in scope
  * "empty"    — instruction is blank or noise

Return JSON ONLY, no prose, matching:
{
  "verdict": "accepted" | "partial" | "rejected" | "empty",
  "reasoning": "one or two sentences (plain English) explaining why",
  "accepted_parts": ["short paraphrase of each in-scope clause"],
  "rejected_parts": ["short paraphrase of each out-of-scope clause"],
  "applied_text":   "the cleaned, in-scope clauses concatenated and lightly normalized — empty string if rejected"
}
"""


def _strip_json_fences(raw: str) -> str:
    raw = (raw or "").strip()
    fence = re.match(r"^```(?:json)?\s*(.*?)\s*```$", raw, re.DOTALL)
    return fence.group(1).strip() if fence else raw


def _extract_first_json_object(raw: str) -> str:
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


def validate_custom_instructions(text: str) -> InstructionVerdict:
    """Classify the textarea contents using the LLM as gatekeeper.

    On classifier failure (LLM error, JSON parse error, unexpected
    schema) we return verdict="error" with `applied_text=""`. The
    PREVIOUS behavior was to forward `applied_text=cleaned` through to
    the main conversion LLM, which defeated the entire safety gate the
    moment the classifier had a bad day. The conservative thing to do
    when we don't know whether the user's instruction is in-scope is to
    drop it and surface a warning in the UI; the conversion still
    proceeds without custom instructions, which is a strictly safer
    failure mode than letting unvalidated text reach the main prompt.
    """
    cleaned = (text or "").strip()
    if not cleaned:
        return InstructionVerdict(verdict="empty")

    try:
        client = get_llm_client()
        resp = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": _GATEKEEPER_SYSTEM_PROMPT},
                {"role": "user", "content": cleaned},
            ],
            temperature=0,
        )
        raw = resp.choices[0].message.content or ""
    except Exception as e:
        return InstructionVerdict(
            verdict="error",
            reasoning=f"Could not validate (LLM call failed: {e}). "
                       "Custom instructions were dropped for safety; "
                       "the conversion proceeded without them.",
            applied_text="",
        )

    try:
        body = _extract_first_json_object(_strip_json_fences(raw))
        data = json.loads(body)
    except Exception:
        return InstructionVerdict(
            verdict="error",
            reasoning="Could not parse classifier response. "
                       "Custom instructions were dropped for safety; "
                       "the conversion proceeded without them.",
            applied_text="",
        )

    verdict = str(data.get("verdict") or "").lower().strip()
    if verdict not in {"accepted", "partial", "rejected", "empty"}:
        verdict = "error"

    accepted_parts = [str(x) for x in (data.get("accepted_parts") or [])]
    rejected_parts = [str(x) for x in (data.get("rejected_parts") or [])]
    applied_text = str(data.get("applied_text") or "").strip()
    reasoning = str(data.get("reasoning") or "").strip()

    if verdict == "rejected":
        applied_text = ""
    elif verdict in {"accepted", "partial"} and not applied_text:
        applied_text = "\n".join(accepted_parts) or cleaned

    return InstructionVerdict(
        verdict=verdict,
        reasoning=reasoning,
        accepted_parts=accepted_parts,
        rejected_parts=rejected_parts,
        applied_text=applied_text,
    )
