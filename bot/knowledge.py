"""Incident query parsing and ITSM MCP RAG / search_kb retrieval."""

from __future__ import annotations

import json
import logging
import re
from typing import Any

import httpx

from bot.mcp import mcp_call_tool, mcp_headers_itsm

log = logging.getLogger("itsm-agent-bot")


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


def _extract_plaintext_incident_query(body: str) -> str | None:
    parts: list[str] = []
    for raw in body.splitlines():
        line = raw.strip()
        if not line:
            continue
        low = line.lower()
        for key in ("title:", "description:", "severity:", "status:"):
            if low.startswith(key):
                if val := line[len(key) :].strip():
                    parts.append(val)
                break
    return "\n".join(parts) if parts else None


def query_from_channel_body(body: str) -> str:
    s = body.strip()
    if plain := _extract_plaintext_incident_query(s):
        return plain
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


async def mcp_rag_then_search_kb(
    client: httpx.AsyncClient,
    mcp_url: str,
    mcp_token: str | None,
    query: str,
    top_k: int,
) -> tuple[bool, list[dict[str, Any]]]:
    ok, rows = kb_rows_from_tool_payload(await mcp_rag_search_kb(client, mcp_url, mcp_token, query, top_k))
    if ok:
        return ok, rows
    for token in _search_kb_try_terms(query):
        sk = await mcp_call_tool(
            client, mcp_url, mcp_headers_itsm(mcp_token), "search_kb", {"query": token, "limit": 25}
        )
        ok2, rows2 = kb_rows_from_tool_payload(sk)
        if ok2:
            log.info("search_kb fallback matched %s row(s) for term=%r", len(rows2), token[:60])
            return ok2, rows2
    return False, []
