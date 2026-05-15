"""MCP JSON-RPC over HTTP (itsm-app and aap-mcp-server)."""

from __future__ import annotations

import json
from typing import Any

import httpx

from bot.config import JSON_HEADERS, PROTOCOL_VERSION


def rpc(method: str, params: dict[str, Any] | None, req_id: int) -> dict[str, Any]:
    msg: dict[str, Any] = {"jsonrpc": "2.0", "method": method, "id": req_id}
    if params is not None:
        msg["params"] = params
    return msg


def mcp_http_response_jsonrpc_messages(resp: httpx.Response) -> list[dict[str, Any]]:
    raw = resp.text or ""
    stripped = raw.strip()
    if stripped.startswith("{") or stripped.startswith("["):
        try:
            obj = json.loads(stripped)
            if isinstance(obj, dict):
                return [obj]
            if isinstance(obj, list):
                return [x for x in obj if isinstance(x, dict)]
        except json.JSONDecodeError:
            pass
    return [
        o
        for line in raw.splitlines()
        if (load := line.strip()[5:].strip() if line.strip().startswith("data:") else "")
        and (o := _try_json_dict(load))
    ]


def _try_json_dict(s: str) -> dict[str, Any] | None:
    try:
        o = json.loads(s)
        return o if isinstance(o, dict) else None
    except json.JSONDecodeError:
        return None


def mcp_jsonrpc_for_request(messages: list[dict[str, Any]], req_id: int) -> dict[str, Any] | None:
    def same_id(msg: dict[str, Any], rid: int) -> bool:
        mid = msg.get("id")
        return mid == rid or str(mid) == str(rid)

    for m in messages:
        if same_id(m, req_id):
            return m
    for m in messages:
        r = m.get("result")
        if isinstance(r, dict) and (
            r.get("content")
            or r.get("structuredContent") is not None
            or isinstance(r.get("results"), list)
        ):
            return m
    for m in reversed(messages):
        if "result" in m or "error" in m:
            return m
    return None


def mcp_streamable_followup_headers(
    base: dict[str, str], init_response: httpx.Response, init_body: dict[str, Any]
) -> dict[str, str]:
    out = dict(base)
    if sid := init_response.headers.get("mcp-session-id"):
        out["mcp-session-id"] = sid
    res = init_body.get("result")
    if isinstance(res, dict) and isinstance(pv := res.get("protocolVersion"), str) and pv.strip():
        out["mcp-protocol-version"] = pv.strip()
    return out


def mcp_headers_itsm(token: str | None) -> dict[str, str]:
    h = dict(JSON_HEADERS)
    if token:
        h["X-ITSM-MCP-Token"] = token
        h["Authorization"] = f"Bearer {token}"
    return h


def mcp_headers_aap(token: str | None) -> dict[str, str]:
    h = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


async def mcp_call_tool(
    client: httpx.AsyncClient,
    mcp_url: str,
    headers: dict[str, str],
    tool_name: str,
    arguments: dict[str, Any],
) -> Any:
    r1 = await client.post(
        mcp_url,
        json=rpc(
            "initialize",
            {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "itsm-agent-bot", "version": "1"},
            },
            1,
        ),
        headers=headers,
        timeout=120.0,
    )
    r1.raise_for_status()
    body1 = mcp_jsonrpc_for_request(mcp_http_response_jsonrpc_messages(r1), 1)
    if body1 is None:
        return {
            "error": "mcp_initialize",
            "detail": {
                "message": "no parseable JSON-RPC in MCP response",
                "content_type": r1.headers.get("content-type"),
                "snippet": (r1.text or "")[:500],
            },
        }
    if "error" in body1:
        return {"error": "mcp_initialize", "detail": body1["error"]}

    follow = mcp_streamable_followup_headers(headers, r1, body1)
    r_mid = await client.post(
        mcp_url,
        json={"jsonrpc": "2.0", "method": "notifications/initialized"},
        headers=follow,
        timeout=30.0,
    )
    r_mid.raise_for_status()

    r2 = await client.post(
        mcp_url,
        json=rpc("tools/call", {"name": tool_name, "arguments": arguments}, 2),
        headers=follow,
        timeout=120.0,
    )
    r2.raise_for_status()
    body2 = mcp_jsonrpc_for_request(mcp_http_response_jsonrpc_messages(r2), 2)
    if body2 is None:
        return {
            "error": "mcp_tools_call",
            "detail": {
                "message": "no parseable JSON-RPC in MCP response",
                "content_type": r2.headers.get("content-type"),
                "snippet": (r2.text or "")[:500],
            },
        }
    if "error" in body2:
        return {"error": "mcp_tools_call", "detail": body2["error"]}
    result = body2.get("result") or {}
    sc = result.get("structuredContent")
    if isinstance(sc, dict) and isinstance(sc.get("results"), list):
        return sc
    if isinstance(result, dict) and isinstance(result.get("results"), list):
        return result
    content = result.get("content")
    if not isinstance(content, list) or not content:
        return {"error": "mcp_empty_content", "detail": result}
    text = content[0].get("text")
    if not isinstance(text, str):
        return {"error": "mcp_no_text", "detail": content[0]}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"error": "bad_tool_json", "message": text[:500]}
