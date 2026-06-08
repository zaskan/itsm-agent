"""ITSM-app MCP: service requests, catalog templates, and related operations."""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Literal

import httpx

from bot.mcp import mcp_call_tool, mcp_headers_itsm

from bot.knowledge import is_incident_channel_message

log = logging.getLogger("itsm-agent-bot")

_ITSM_REF_FIELDS = frozenset({"itsm_change_ref", "itsm_service_request_ref"})
WorkflowKind = Literal["incident", "catalog", "generic"]


def itsm_mcp_url(itsm_base: str) -> str:
    return itsm_base.rstrip("/") + "/mcp/"


async def itsm_call(
    client: httpx.AsyncClient,
    mcp_url_str: str,
    mcp_token: str | None,
    tool: str,
    arguments: dict[str, Any],
) -> Any:
    return await mcp_call_tool(client, mcp_url_str, mcp_headers_itsm(mcp_token), tool, arguments)


def tool_error(data: Any) -> str | None:
    if not isinstance(data, dict):
        return None
    if err := data.get("error"):
        if isinstance(err, str):
            return err
        return str(data.get("detail") or err)
    if data.get("message") and "error" in str(data.get("message", "")).lower():
        return str(data["message"])
    return None


def _coerce_spec_value(raw: str) -> Any:
    s = raw.strip()
    if s.isdigit():
        return int(s)
    if re.fullmatch(r"-?\d+\.\d+", s):
        return float(s)
    return s


def template_field_keys(template: dict[str, Any]) -> list[str]:
    defs = template.get("field_definitions")
    if not isinstance(defs, list):
        return []
    keys: list[str] = []
    for row in defs:
        if isinstance(row, dict) and (key := str(row.get("field_key") or "").strip()):
            keys.append(key)
    return keys


def required_template_field_keys(template: dict[str, Any]) -> list[str]:
    defs = template.get("field_definitions")
    if not isinstance(defs, list):
        return []
    keys: list[str] = []
    for row in defs:
        if not isinstance(row, dict):
            continue
        if not row.get("required", False):
            continue
        if key := str(row.get("field_key") or "").strip():
            keys.append(key)
    return keys


def extract_catalog_from_kb(kb_rows: list[dict[str, Any]], *, primary_only: bool = True) -> str | None:
    rows = kb_rows[:1] if primary_only else kb_rows[:3]
    for row in rows:
        blob = "\n".join(
            chunk
            for key in ("title", "description")
            if (chunk := str(row.get(key, "") or "").strip())
        )
        for pat in (
            r"(?is)ITSM catalog \*\*([^*]+)\*\*",
            r"(?is)Open catalog \*\*([^*]+)\*\*",
            r"(?is)service catalog \*\*([^*]+)\*\*",
        ):
            if m := re.search(pat, blob):
                if name := m.group(1).strip():
                    return name
    return None


def is_incident_kb_row(row: dict[str, Any]) -> bool:
    blob = "\n".join(
        str(row.get(key, "") or "")
        for key in ("title", "description")
    )
    if re.search(r"(?i)\[incident\.created\]|itsm_incident_ref", blob):
        return True
    title = str(row.get("title") or "").lower()
    return "incident" in title and "deploy" not in title and "service request" not in title


def incident_workflow_applies(kb_rows: list[dict[str, Any]], user_query: str) -> bool:
    if is_incident_channel_message(user_query):
        return True
    return bool(kb_rows) and is_incident_kb_row(kb_rows[0])


def resolve_workflow_kind(kb_rows: list[dict[str, Any]], user_query: str) -> WorkflowKind:
    if incident_workflow_applies(kb_rows, user_query):
        return "incident"
    if extract_catalog_from_kb(kb_rows, primary_only=True):
        return "catalog"
    return "generic"


def catalog_workflow_applies(kb_rows: list[dict[str, Any]], user_query: str = "") -> bool:
    return resolve_workflow_kind(kb_rows, user_query) == "catalog"


def filter_missing_for_catalog(missing: list[str]) -> list[str]:
    return [f for f in missing if f not in _ITSM_REF_FIELDS]


def specs_from_collected(template: dict[str, Any], collected: dict[str, str]) -> dict[str, Any]:
    allowed = set(template_field_keys(template))
    out: dict[str, Any] = {}
    for key, val in collected.items():
        if key in allowed and str(val).strip():
            out[key] = _coerce_spec_value(str(val))
    return out


async def list_request_templates(
    client: httpx.AsyncClient,
    mcp_url_str: str,
    mcp_token: str | None,
) -> list[dict[str, Any]]:
    data = await itsm_call(client, mcp_url_str, mcp_token, "list_request_templates", {})
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    return []


async def find_request_template(
    client: httpx.AsyncClient,
    mcp_url_str: str,
    mcp_token: str | None,
    name: str,
) -> dict[str, Any] | None:
    target = name.strip().lower()
    if not target:
        return None
    for row in await list_request_templates(client, mcp_url_str, mcp_token):
        row_name = str(row.get("name") or "").strip().lower()
        if row_name == target or target in row_name or row_name in target:
            return row
    return None


def _change_ref_from_request(detail: dict[str, Any]) -> str | None:
    ritms = [r for r in (detail.get("ritms") or []) if isinstance(r, dict)]
    for ritm in ritms:
        if ritm.get("request_template_id") and (ref := ritm.get("change_public_id")):
            return str(ref)
    for ritm in ritms:
        if ref := ritm.get("change_public_id"):
            return str(ref)
    return None


async def open_service_request(
    client: httpx.AsyncClient,
    mcp_url_str: str,
    mcp_token: str | None,
    *,
    template: dict[str, Any],
    specifications: dict[str, Any],
    description: str = "",
) -> dict[str, Any]:
    template_id = int(template["id"])
    template_name = str(template.get("name") or "Service request")
    desc = (description or template_name).strip()

    created = await itsm_call(
        client,
        mcp_url_str,
        mcp_token,
        "create_request",
        {"name": template_name, "description": desc},
    )
    if err := tool_error(created):
        raise RuntimeError(err)
    if not isinstance(created, dict) or not (request_ref := created.get("public_id")):
        raise RuntimeError(f"Unexpected create_request response: {str(created)[:300]}")

    added = await itsm_call(
        client,
        mcp_url_str,
        mcp_token,
        "add_ritm",
        {
            "request_ref": str(request_ref),
            "request_template_id": template_id,
            "specifications_json": json.dumps(specifications),
        },
    )
    if err := tool_error(added):
        raise RuntimeError(err)

    submitted = await itsm_call(
        client,
        mcp_url_str,
        mcp_token,
        "submit_request",
        {"request_ref": str(request_ref)},
    )
    if err := tool_error(submitted):
        raise RuntimeError(err)
    if not isinstance(submitted, dict):
        raise RuntimeError(f"Unexpected submit_request response: {str(submitted)[:300]}")

    change_ref = _change_ref_from_request(submitted)
    return {
        "request_ref": str(request_ref),
        "change_ref": change_ref,
        "template_name": template_name,
        "detail": submitted,
    }


async def ensure_itsm_refs_for_launch(
    client: httpx.AsyncClient,
    mcp_url_str: str,
    mcp_token: str | None,
    *,
    kb_rows: list[dict[str, Any]],
    collected: dict[str, str],
    user_query: str,
) -> tuple[dict[str, str], str | None]:
    """Open catalog service request when KB workflow applies and refs are missing."""
    if collected.get("itsm_change_ref"):
        return collected, None
    catalog_name = extract_catalog_from_kb(kb_rows)
    if not catalog_name:
        return collected, None
    if incident_workflow_applies(kb_rows, user_query):
        return collected, None

    template = await find_request_template(client, mcp_url_str, mcp_token, catalog_name)
    if template is None:
        return collected, f"I could not find the ITSM catalog template {catalog_name!r}."

    specs = specs_from_collected(template, collected)
    missing = [k for k in required_template_field_keys(template) if k not in specs]
    if missing:
        return collected, None

    try:
        opened = await open_service_request(
            client,
            mcp_url_str,
            mcp_token,
            template=template,
            specifications=specs,
            description=user_query[:500],
        )
    except Exception as e:
        log.exception("ITSM open_service_request failed")
        return collected, f"Opening the ITSM service request failed: {e}"

    out = dict(collected)
    out["itsm_service_request_ref"] = opened["request_ref"]
    if opened.get("change_ref"):
        out["itsm_change_ref"] = str(opened["change_ref"])
    elif catalog_workflow_applies(kb_rows):
        return out, (
            f"Opened ITSM service request {opened['request_ref']}, but no change reference was returned. "
            "Cannot launch automation without itsm_change_ref."
        )

    msg = f"Opened ITSM service request {opened['request_ref']}."
    if opened.get("change_ref"):
        msg += f" Change {opened['change_ref']} was created."
    return out, msg
