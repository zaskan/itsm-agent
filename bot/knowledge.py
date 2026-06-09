"""RAG retrieval via itsm-app MCP."""

from __future__ import annotations

import json
import re
from typing import Any

import httpx

from bot.mcp import mcp_call_tool, mcp_headers_itsm


def rag_has_usable_results(data: dict[str, Any]) -> tuple[bool, list[dict[str, Any]]]:
    if data.get("error") in ("rag_not_configured", "embedding_failed"):
        return False, []
    if data.get("message") == "no_indexed_articles":
        return False, []
    if not isinstance(results := data.get("results"), list) or not results:
        return False, []
    return True, results


def kb_rows_from_tool_payload(data: Any) -> tuple[bool, list[dict[str, Any]]]:
    if isinstance(data, list):
        rows = [x for x in data if isinstance(x, dict)]
        return (len(rows) > 0, rows[:15])
    if isinstance(data, dict):
        return rag_has_usable_results(data)
    return False, []


_INCIDENT_CREATED_RE = re.compile(r"(?i)\[incident\.created\]|incident\.created")
_REMEDIATION_COMPLETE_RE = re.compile(r"(?i)\[remediation\.complete\]|remediation\.complete")
_INCIDENT_REF_RE = re.compile(r"\b(INC-[\w-]+)\b", re.I)


def is_remediation_complete_message(body: str) -> bool:
    return bool(_REMEDIATION_COMPLETE_RE.search(body.strip()))


def is_incident_channel_message(body: str) -> bool:
    s = body.strip()
    if not s:
        return False
    if is_remediation_complete_message(s):
        return False
    if _INCIDENT_CREATED_RE.search(s):
        return True
    if _INCIDENT_REF_RE.search(s) and re.search(
        r"(?i)\b(apache|application down|httpd|incident|remediat|probe failed)\b", s
    ):
        return True
    return False


def parse_incident_from_body(body: str) -> dict[str, str]:
    """Extract itsm_incident_ref and vm_name from incident-shaped chat lines."""
    out: dict[str, str] = {}
    s = body.strip()
    if not s:
        return out
    if m := _INCIDENT_REF_RE.search(s):
        out["itsm_incident_ref"] = m.group(1).upper()
    segments = [part.strip() for part in re.split(r"\s[—–-]\s", s) if part.strip()]
    if len(segments) >= 2:
        host_part = re.sub(r"\s*\([^)]+\)\s*$", "", segments[-1]).strip()
        if host_part and not _INCIDENT_REF_RE.fullmatch(host_part):
            out["vm_name"] = host_part
    for line in s.splitlines():
        if ":" not in line:
            continue
        key, val = line.split(":", 1)
        k, v = key.strip().lower(), val.strip()
        if k == "vm_name" and v:
            out["vm_name"] = v
        elif k in ("itsm_incident_ref", "incident_ref") and v:
            out["itsm_incident_ref"] = v.upper()
    return out


def query_from_channel_body(body: str) -> str:
    """Use full message text as the RAG query (incident-shaped bodies still parse cleanly)."""
    s = body.strip()
    if not s.startswith("{"):
        return s
    try:
        obj = json.loads(s)
    except json.JSONDecodeError:
        return s
    if not isinstance(obj, dict):
        return s
    inc = obj.get("incident")
    if isinstance(inc, dict):
        parts = [
            v.strip()
            for key in ("title", "description", "severity", "status", "incident_ref")
            if isinstance(v := inc.get(key), str) and v.strip()
        ]
        if parts:
            return "\n".join(parts)
    return s


async def mcp_rag_search_kb(
    client: httpx.AsyncClient,
    mcp_url: str,
    mcp_token: str | None,
    query: str,
    top_k: int,
) -> dict[str, Any]:
    out = await mcp_call_tool(
        client, mcp_url, mcp_headers_itsm(mcp_token), "rag_search_kb", {"query": query, "top_k": top_k}
    )
    return out if isinstance(out, dict) else {"error": "unexpected_rag_shape", "raw": out}


def _search_kb_try_terms(query: str) -> list[str]:
    lines = [ln.strip() for ln in query.splitlines() if ln.strip()]
    if not lines:
        return []
    out: list[str] = []
    seen: set[str] = set()

    def add(term: str) -> None:
        t = term.strip()
        if len(t) >= 2 and t.lower() not in seen:
            seen.add(t.lower())
            out.append(t)

    for raw in lines[:3]:
        add(raw[:120])
        spaced = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", re.sub(r"([a-z])([A-Z])", r"\1 \2", raw))
        for w in re.findall(r"[A-Za-z0-9]+", spaced):
            if len(w) >= 3:
                add(w)
    return out[:8]


async def mcp_rag_search(
    client: httpx.AsyncClient,
    mcp_url: str,
    mcp_token: str | None,
    query: str,
    top_k: int,
) -> tuple[bool, list[dict[str, Any]]]:
    data = await mcp_rag_search_kb(client, mcp_url, mcp_token, query, top_k)
    ok, rows = kb_rows_from_tool_payload(data)
    if ok:
        return ok, rows
    if data.get("error") != "rag_not_configured":
        return False, []
    for token in _search_kb_try_terms(query):
        sk = await mcp_call_tool(
            client, mcp_url, mcp_headers_itsm(mcp_token), "search_kb", {"query": token, "limit": top_k}
        )
        ok2, rows2 = kb_rows_from_tool_payload(sk)
        if ok2:
            return ok2, rows2
    return False, []
