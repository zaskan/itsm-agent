#!/usr/bin/env python3
"""
ITSM-aware demo-chat bot (v1).

Environment variables (required unless noted):
  CHAT_BASE_URL       — demo-chat origin, e.g. http://demo-chat.demo-chat.svc.cluster.local:8000
  CHAT_USERNAME       — bot user login
  CHAT_PASSWORD       — bot user password
  CHANNEL_NAME          — channel to join (exact name), OR
  CHANNEL_ID            — channel UUID (use one of CHANNEL_NAME / CHANNEL_ID)

Incident-style chat bodies: lines ``Title:`` / ``Description:`` (plain text, not only JSON) are
condensed into the RAG query. If semantic search returns nothing, the bot tries MCP ``search_kb``
on title words (including CamelCase splits like ``HPA`` from ``HPAReplicasAtMaxCapacity``).
  ITSM_BASE_URL       — itsm-app origin (no path), MCP is at {ITSM_BASE_URL}/mcp/
  ITSM_MCP_TOKEN      — value for X-ITSM-MCP-Token (or Bearer); empty if MCP has no token
  LLM_BASE_URL        — LiteLLM (or any OpenAI-compatible) API base. Either origin only
                        (e.g. https://litellm.example.com) or already including /v1
                        (e.g. https://litellm.example.com/v1). Chat is always POST …/v1/chat/completions.
  LLM_MODEL           — default: llama-scout-17b
  LLM_API_KEY         — Bearer token for LiteLLM (Authorization: Bearer …)

Optional AAP MCP (same layout as Cursor ``mcpServers`` HTTP entries — only URL + token in env):
  AAP_MCP_BASE_URL    — Origin of the **ansible/aap-mcp-server** HTTP service (a separate Route from the AAP
                        **browser UI**). If you use the controller/gateway SPA host (GET on ``/mcp/…`` returns HTML),
                        POST will return **405** — point this at the MCP server instead. Toolset URLs are built as
                        ``{AAP_MCP_BASE_URL}/mcp/{toolset}`` (upstream also supports ``/{toolset}/mcp``). Template
                        checks use **job_management** only.
  AAP_MCP_TOKEN       — Bearer AAP OAuth2 token for ``Authorization`` (empty only if your gateway injects auth).
  AAP_MCP_TOOL_JOB_LIST — optional; MCP tool name for job template list (default: ``job_templates_list``).
  AAP_MCP_TOOL_WFJT_LIST — optional; MCP tool name for workflow job template list (default: ``workflow_job_templates_list``).
  AAP_TLS_VERIFY      — optional; set ``false`` / ``0`` to disable TLS verification **for AAP MCP only** (e.g.
                        self-signed ingress). Uses a dedicated ``httpx`` client with ``verify=False`` (httpx 0.28+
                        has no per-request ``verify``). If unset, ``TLS_VERIFY`` is checked the same way for AAP only.
                        Default: verify. Unsafe on untrusted networks.
  AAP_MCP_URL         — (legacy) full single MCP URL if you cannot use ``AAP_MCP_BASE_URL`` yet.

Optional:
  RAG_TOP_K           — default 5
  HEALTH_PORT         — default 8080; GET /healthz always 200 (liveness); GET /readyz 503 until WS subscribed (readiness)

Retrieval uses itsm-app MCP rag_search_kb. After KB hits, the bot can call AAP MCP
``job_templates_list`` / ``workflow_job_templates_list`` (names from the controller OpenAPI in aap-mcp-server)
with a ``search`` query for each template name inferred from KB text, then append an **AAP** section to the reply.
LiteLLM is used only for the KB summary (unchanged).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import httpx
import websockets

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(levelname)s %(message)s")
log = logging.getLogger("itsm-agent-bot")

PROTOCOL_VERSION = "2024-11-05"
JSON_HEADERS = {"Content-Type": "application/json", "Accept": "application/json"}
NOTHING = "Nothing matches"
# httpx 0.28+ has no per-request ``verify=`` on ``post()``; use a dedicated client when AAP skips TLS verify.
_HTTP_CLIENT_LIMITS = httpx.Limits(max_keepalive_connections=5, max_connections=10)

# ansible/aap-mcp-server registers tools from OpenAPI operationIds with dots → underscores (no ``controller.`` prefix).
# Defaults match bundled controller schema (``job_templates_list``, ``workflow_job_templates_list``).
def _aap_tool_job_templates_list_name() -> str:
    v = (os.environ.get("AAP_MCP_TOOL_JOB_LIST") or "job_templates_list").strip()
    return v or "job_templates_list"


def _aap_tool_workflow_job_templates_list_name() -> str:
    v = (
        os.environ.get("AAP_MCP_TOOL_WORKFLOW_JOB_TEMPLATES_LIST")
        or os.environ.get("AAP_MCP_TOOL_WFJT_LIST")
        or "workflow_job_templates_list"
    ).strip()
    return v or "workflow_job_templates_list"


MAX_AAP_CANDIDATES = 8

# Path pattern matches ansible/aap-mcp-server README: /mcp/{toolset} (not /{toolset}/mcp/).
AAP_MCP_TOOLSETS = (
    "job_management",
    "inventory_management",
    "system_monitoring",
    "user_management",
    "security_compliance",
    "platform_configuration",
)

_ready = threading.Event()


def _env(name: str, default: str | None = None) -> str:
    v = os.environ.get(name, "").strip()
    if v:
        return v
    if default is not None:
        return default
    raise RuntimeError(f"Missing required environment variable: {name}")


def _optional_env(name: str) -> str | None:
    v = os.environ.get(name, "").strip()
    return v or None


def _ws_url(chat_base: str, token: str) -> str:
    b = chat_base.rstrip("/")
    q = urllib.parse.urlencode({"token": token})
    if b.startswith("https://"):
        return "wss://" + b[len("https://") :] + "/api/v1/ws?" + q
    if b.startswith("http://"):
        return "ws://" + b[len("http://") :] + "/api/v1/ws?" + q
    raise ValueError("CHAT_BASE_URL must start with http:// or https://")


def _mcp_url(itsm_base: str) -> str:
    return itsm_base.rstrip("/") + "/mcp/"


def _litellm_chat_completions_url(llm_base: str) -> str:
    """Build POST URL for OpenAI-compatible chat completions (LiteLLM)."""
    b = llm_base.rstrip("/")
    if b.endswith("/v1"):
        return b + "/chat/completions"
    return b + "/v1/chat/completions"


def _rpc(method: str, params: dict[str, Any] | None, req_id: int) -> dict[str, Any]:
    msg: dict[str, Any] = {"jsonrpc": "2.0", "method": method, "id": req_id}
    if params is not None:
        msg["params"] = params
    return msg


def _mcp_http_response_jsonrpc_messages(resp: httpx.Response) -> list[dict[str, Any]]:
    """Parse JSON-RPC object(s) from an MCP HTTP POST response.

    itsm-app returns a single JSON object. Streamable HTTP (aap-mcp-server) returns
    ``text/event-stream`` frames: ``data: {"jsonrpc":...}`` per event.
    """
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
    out: list[dict[str, Any]] = []
    for line in raw.splitlines():
        s = line.strip()
        if not s.startswith("data:"):
            continue
        load = s[5:].strip()
        if not load:
            continue
        try:
            o = json.loads(load)
        except json.JSONDecodeError:
            continue
        if isinstance(o, dict):
            out.append(o)
    return out


def _mcp_jsonrpc_for_request(messages: list[dict[str, Any]], req_id: int) -> dict[str, Any] | None:
    """Pick the JSON-RPC response for our request id (Streamable HTTP may emit several ``data:`` lines)."""

    def _same_id(msg: dict[str, Any], rid: int) -> bool:
        mid = msg.get("id")
        return mid == rid or str(mid) == str(rid)

    for m in messages:
        if _same_id(m, req_id):
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


def _mcp_streamable_followup_headers(
    base: dict[str, str], init_response: httpx.Response, init_body: dict[str, Any]
) -> dict[str, str]:
    """Headers for POSTs after initialize (Streamable HTTP: session + negotiated protocol)."""
    out = dict(base)
    sid = init_response.headers.get("mcp-session-id")
    if sid:
        out["mcp-session-id"] = sid
    res = init_body.get("result")
    if isinstance(res, dict):
        pv = res.get("protocolVersion")
        if isinstance(pv, str) and pv.strip():
            out["mcp-protocol-version"] = pv.strip()
    return out


def _mcp_headers_itsm(token: str | None) -> dict[str, str]:
    h = dict(JSON_HEADERS)
    if token:
        h["X-ITSM-MCP-Token"] = token
        h["Authorization"] = f"Bearer {token}"
    return h


def _mcp_headers_aap(token: str | None) -> dict[str, str]:
    """Bearer auth + Accept for Streamable HTTP MCP (aap-mcp-server requires JSON and SSE types)."""
    h = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


def _aap_tls_verify_enabled() -> bool:
    """TLS verification for AAP MCP HTTPS. Off when AAP_TLS_VERIFY or TLS_VERIFY is false/0/off."""
    for key in ("AAP_TLS_VERIFY", "TLS_VERIFY"):
        raw = os.environ.get(key)
        if raw is not None and str(raw).strip() != "":
            return str(raw).strip().lower() not in {"0", "false", "no", "off"}
    return True


def rag_has_usable_results(data: dict[str, Any]) -> tuple[bool, list[dict[str, Any]]]:
    if data.get("error") in ("rag_not_configured", "embedding_failed"):
        return False, []
    if data.get("message") == "no_indexed_articles":
        return False, []
    results = data.get("results")
    if not isinstance(results, list) or len(results) == 0:
        return False, []
    return True, results


def _extract_plaintext_incident_query(body: str) -> str | None:
    """Pull Title / Description / Severity / Status lines from human-formatted incident posts."""
    parts: list[str] = []
    for raw in body.splitlines():
        line = raw.strip()
        if not line:
            continue
        low = line.lower()
        for key in ("title:", "description:", "severity:", "status:"):
            if low.startswith(key):
                val = line[len(key) :].strip()
                if val:
                    parts.append(val)
                break
    if not parts:
        return None
    return "\n".join(parts)


def _query_from_channel_body(body: str) -> str:
    s = body.strip()
    plain = _extract_plaintext_incident_query(s)
    if plain:
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
        parts: list[str] = []
        for key in ("title", "description", "severity", "status", "incident_ref"):
            v = inc.get(key)
            if isinstance(v, str) and v.strip():
                parts.append(v.strip())
        if parts:
            return "\n".join(parts)
    return s


class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        if path == "/healthz":
            body = b'{"status":"ok"}'
            self.send_response(200)
        elif path == "/readyz":
            if _ready.is_set():
                body = b'{"status":"ready"}'
                self.send_response(200)
            else:
                body = b'{"status":"starting"}'
                self.send_response(503)
        else:
            self.send_response(404)
            self.end_headers()
            return
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args: object) -> None:  # noqa: D102
        return


def _run_health_server(port: int) -> None:
    srv = HTTPServer(("0.0.0.0", port), _HealthHandler)
    srv.serve_forever()


async def chat_login(client: httpx.AsyncClient, base: str, user: str, password: str) -> str:
    r = await client.post(
        f"{base.rstrip('/')}/api/v1/auth/login",
        json={"username": user, "password": password},
        headers={"Content-Type": "application/json"},
        timeout=60.0,
    )
    r.raise_for_status()
    data = r.json()
    tok = data.get("access_token")
    if not isinstance(tok, str) or not tok:
        raise RuntimeError("login response missing access_token")
    return tok


async def chat_me(client: httpx.AsyncClient, base: str, token: str) -> dict[str, Any]:
    r = await client.get(
        f"{base.rstrip('/')}/api/v1/users/me",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30.0,
    )
    r.raise_for_status()
    return r.json()


async def _mcp_call_tool(
    client: httpx.AsyncClient,
    mcp_url: str,
    headers: dict[str, str],
    tool_name: str,
    arguments: dict[str, Any],
) -> Any:
    """Run one MCP tools/call after initialize (stateless HTTP MCP). Returns parsed JSON value."""
    r1 = await client.post(
        mcp_url,
        json=_rpc(
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
    msgs1 = _mcp_http_response_jsonrpc_messages(r1)
    body1 = _mcp_jsonrpc_for_request(msgs1, 1)
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

    follow = _mcp_streamable_followup_headers(headers, r1, body1)

    r_mid = await client.post(
        mcp_url,
        json={"jsonrpc": "2.0", "method": "notifications/initialized"},
        headers=follow,
        timeout=30.0,
    )
    r_mid.raise_for_status()

    r2 = await client.post(
        mcp_url,
        json=_rpc("tools/call", {"name": tool_name, "arguments": arguments}, 2),
        headers=follow,
        timeout=120.0,
    )
    r2.raise_for_status()
    msgs2 = _mcp_http_response_jsonrpc_messages(r2)
    body2 = _mcp_jsonrpc_for_request(msgs2, 2)
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


async def mcp_rag_search_kb(
    client: httpx.AsyncClient,
    mcp_url: str,
    mcp_token: str | None,
    query: str,
    top_k: int,
) -> dict[str, Any]:
    out = await _mcp_call_tool(
        client,
        mcp_url,
        _mcp_headers_itsm(mcp_token),
        "rag_search_kb",
        {"query": query, "top_k": top_k},
    )
    return out if isinstance(out, dict) else {"error": "unexpected_rag_shape", "raw": out}


def kb_rows_from_tool_payload(data: Any) -> tuple[bool, list[dict[str, Any]]]:
    """Normalize rag_search_kb dict or search_kb list into (ok, rows)."""
    if isinstance(data, list):
        rows = [x for x in data if isinstance(x, dict)]
        return (len(rows) > 0, rows[:15])
    if isinstance(data, dict):
        return rag_has_usable_results(data)
    return False, []


def _search_kb_try_terms(query: str) -> list[str]:
    """Build substring terms for search_kb (helps CamelCase alert names vs spaced KB titles)."""
    lines = [ln.strip() for ln in query.splitlines() if ln.strip()]
    if not lines:
        return []
    out: list[str] = []
    seen: set[str] = set()

    def add(s: str) -> None:
        t = s.strip()
        if len(t) >= 2 and t.lower() not in seen:
            seen.add(t.lower())
            out.append(t)

    for raw in lines[:3]:
        add(raw[:120])
        spaced = re.sub(r"([a-z])([A-Z])", r"\1 \2", raw)
        spaced = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", spaced)
        for w in re.findall(r"[A-Za-z0-9]+", spaced):
            if len(w) >= 3:
                add(w)
    return out[:8]


async def mcp_rag_then_search_kb(
    client: httpx.AsyncClient,
    mcp_url: str,
    mcp_token: str | None,
    query: str,
    top_k: int,
) -> tuple[bool, list[dict[str, Any]]]:
    """Semantic RAG first; if no rows, substring search_kb on title-derived terms."""
    rag = await mcp_rag_search_kb(client, mcp_url, mcp_token, query, top_k)
    ok, rows = kb_rows_from_tool_payload(rag)
    if ok:
        return ok, rows
    for token in _search_kb_try_terms(query):
        sk = await _mcp_call_tool(
            client,
            mcp_url,
            _mcp_headers_itsm(mcp_token),
            "search_kb",
            {"query": token, "limit": 25},
        )
        ok2, rows2 = kb_rows_from_tool_payload(sk)
        if ok2:
            log.info("search_kb fallback matched %s row(s) for term=%r", len(rows2), token[:60])
            return ok2, rows2
    return False, []


async def llm_answer(
    client: httpx.AsyncClient,
    llm_base: str,
    model: str,
    api_key: str | None,
    user_question: str,
    kb_snippets: list[dict[str, Any]],
) -> str:
    lines: list[str] = []
    for i, row in enumerate(kb_snippets, start=1):
        title = row.get("title", "")
        desc = row.get("description", "")
        lines.append(f"KB excerpt {i} — title: {title}\n{desc}\n")
    context = "\n".join(lines)
    system = (
        "You are a concise IT support assistant. The user message is from an operations chat "
        "(often an incident notification). The knowledge base excerpts were retrieved for you — "
        "summarize how they apply (alert names, remediation, links to workflows). "
        "Use only information supported by the excerpts. If excerpts clearly do not apply, say so briefly "
        "in one sentence (do not invent KB content). "
        "Reply in plain text only: no markdown, no headings, no bold, no bullet lists."
    )
    user_msg = f"User message:\n{user_question}\n\nKnowledge base excerpts:\n{context}"
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user_msg},
        ],
        "temperature": 0.2,
        "max_tokens": 1024,
    }
    url = _litellm_chat_completions_url(llm_base)
    r = await client.post(url, json=payload, headers=headers, timeout=120.0)
    r.raise_for_status()
    data = r.json()
    choices = data.get("choices")
    if not isinstance(choices, list) or not choices:
        raise RuntimeError("LLM response missing choices")
    msg = choices[0].get("message") or {}
    content = msg.get("content")
    if not isinstance(content, str) or not content.strip():
        raise RuntimeError("LLM response missing assistant content")
    return content.strip()


def _reply_is_non_answer(text: str) -> bool:
    s = text.strip().lower()
    if len(s) < 4:
        return True
    return s in {
        "nothing matches",
        "nothing match",
        "no matches",
        "no match",
        "n/a",
        "none",
    }


def _kb_fallback_reply(rows: list[dict[str, Any]], *, max_chars: int = 4000) -> str:
    """Post top KB row when the LLM hedges; keeps value when RAG did find articles."""
    parts: list[str] = []
    for row in rows[:3]:
        title = str(row.get("title", "")).strip()
        desc = str(row.get("description", "")).strip()
        if title and desc:
            parts.append(f"{title}\n\n{desc}")
        elif title:
            parts.append(title)
        elif desc:
            parts.append(desc)
    out = "\n\n".join(parts).strip()
    if len(out) > max_chars:
        return out[: max_chars - 1] + "…"
    return out or NOTHING


def _aap_mcp_toolset_urls() -> dict[str, str] | None:
    """Map toolset name → MCP HTTP URL. None if AAP is not configured.

    Upstream aap-mcp-server registers POST on ``/mcp``, ``/mcp/{toolset}``, and ``/{toolset}/mcp``.
    """
    base = os.environ.get("AAP_MCP_BASE_URL", "").strip().rstrip("/")
    if base:
        return {ts: f"{base}/mcp/{ts}" for ts in AAP_MCP_TOOLSETS}
    legacy = os.environ.get("AAP_MCP_URL", "").strip()
    if not legacy:
        return None
    u = legacy.rstrip("/")
    if "/mcp" not in u:
        u = u + "/mcp"
    return {"legacy": u}


def _aap_job_management_mcp_url() -> str | None:
    urls = _aap_mcp_toolset_urls()
    if not urls:
        return None
    return urls.get("job_management") or urls.get("legacy")


def _aap_configured() -> bool:
    return _aap_job_management_mcp_url() is not None


_AAP_APPENDIX_405 = (
    "AAP: HTTP POST was rejected (405) on the configured MCP URL. "
    "AAP_MCP_BASE_URL is probably the AAP web console host, not ansible/aap-mcp-server. "
    "Use the MCP server Route URL (POST …/mcp/job_management returns JSON-RPC), or set AAP_MCP_URL to that full endpoint."
)


async def _aap_warn_if_mcp_url_looks_like_ui(mcp_url: str) -> None:
    """Log when GET on the job MCP URL looks like a static SPA, not an MCP HTTP server."""
    try:
        tls = _aap_tls_verify_enabled()
        async with httpx.AsyncClient(
            limits=_HTTP_CLIENT_LIMITS,
            follow_redirects=True,
            verify=tls,
        ) as c:
            r = await c.get(mcp_url, timeout=15.0, headers={"Accept": "*/*"})
    except Exception as e:
        log.debug("AAP MCP URL probe (GET) failed for %s: %s", mcp_url, e)
        return
    ct = (r.headers.get("content-type") or "").lower()
    if r.is_success and "text/html" in ct:
        log.warning(
            "AAP job MCP URL %s returned HTTP %s with Content-Type %r — likely the AAP **browser UI**, "
            "not **aap-mcp-server**. MCP appendix POSTs will get HTTP 405 until you point "
            "AAP_MCP_BASE_URL / AAP_MCP_URL at the MCP HTTP service.",
            mcp_url,
            r.status_code,
            (r.headers.get("content-type") or "")[:100],
        )


def _tower_results(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict) and isinstance(payload.get("results"), list):
        return [x for x in payload["results"] if isinstance(x, dict)]
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    return []


def _strip_trailing_kb_field_noise(s: str) -> str:
    """Remove trailing structured KB fields accidentally captured on the same line as a template name."""
    s = s.strip()
    s = re.sub(r"(?is)\s+Description\s*:.*$", "", s)
    s = re.sub(r"(?is)\s+Alert\s+name\s*:.*$", "", s)
    s = re.sub(r"(?is)\s+AAP\s+Remediation\s*:.*$", "", s)
    return s.rstrip(" \t.;:|\"'").strip()


def _aap_search_terms(candidate: str) -> list[str]:
    """Build Tower ``search`` query variants (brackets often break single-string search)."""
    c = candidate.strip()
    out: list[str] = []
    seen: set[str] = set()

    def add(s: str) -> None:
        t = s.strip()
        if len(t) < 2:
            return
        k = t.lower()
        if k in seen:
            return
        seen.add(k)
        out.append(t)

    add(c)
    stripped = re.sub(r"^\[[^\]]+\]\s*", "", c).strip()
    if stripped:
        add(stripped)
    no_brackets = re.sub(r"[\[\]]", " ", c)
    no_brackets = re.sub(r"\s+", " ", no_brackets).strip()
    if no_brackets.lower() != c.lower():
        add(no_brackets)
    return out[:5]


def _aap_norm_template_label(s: str) -> str:
    """Lowercase label with brackets removed (Tower names vs KB strings)."""
    s = str(s or "").lower()
    s = re.sub(r"[\[\]]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _extract_aap_candidates(kb_rows: list[dict[str, Any]]) -> list[str]:
    """Infer job / workflow job template names from KB (prefer AAP Remediation and quoted template lines)."""
    blob_parts: list[str] = []
    for row in kb_rows[:6]:
        for key in ("title", "description"):
            chunk = str(row.get(key, "") or "").strip()
            if chunk:
                blob_parts.append(chunk)
    blob = "\n".join(blob_parts)

    patterns: list[tuple[str, int]] = [
        (r'(?is)AAP\s*Remediation\s*:\s*Workflow\s+job\s+template\s*"([^"]+)"', 1),
        (r'(?is)AAP\s*Remediation\s*:\s*Job\s+template\s*"([^"]+)"', 1),
        (r'(?is)\bWorkflow\s+job\s+template\s*"([^"]+)"', 1),
        (r'(?is)\bJob\s+template\s*"([^"]+)"', 1),
        (r"(?is)\bWorkflow\s+job\s+template\s*:\s*([^\n]+)", 1),
        (r"(?is)\bJob\s+template\s*:\s*([^\n]+)", 1),
    ]

    found: list[str] = []
    seen: set[str] = set()

    def consider(raw: str) -> None:
        x = raw.strip().strip('`"\'')
        x = _strip_trailing_kb_field_noise(x)
        if "\n" in x:
            x = x.split("\n", 1)[0].strip()
        x = _strip_trailing_kb_field_noise(x)
        low = x.lower()
        if len(x) < 2 or len(x) > 200:
            return
        if low.startswith("workflow job template") or low.startswith("job template"):
            return
        if low in seen:
            return
        seen.add(low)
        found.append(x)

    for pat, grp in patterns:
        for m in re.finditer(pat, blob):
            consider(m.group(grp))

    for m in re.finditer(r'"(\[[^\]]+\][^"]{0,180})"', blob):
        consider(m.group(1))

    for m in re.finditer(r"`([^`\n]{2,120})`", blob):
        consider(m.group(1))

    return found[:MAX_AAP_CANDIDATES]


def _row_matches_aap_template_name(row: dict[str, Any], candidate: str) -> bool:
    name = str(row.get("name", "") or "")
    nl = name.lower()
    c = candidate.strip().lower()
    if not c or not nl:
        return False
    if c in nl:
        return True
    c_alt = re.sub(r"^\[[^\]]+\]\s*", "", c).strip()
    if len(c_alt) >= 3 and c_alt in nl:
        return True
    cn = _aap_norm_template_label(candidate)
    nn = _aap_norm_template_label(name)
    if len(cn) >= 3 and cn in nn:
        return True
    if len(nn) >= 3 and nn in cn:
        return True
    if cn and cn == nn:
        return True
    return False


def _rows_matching_candidate(rows: list[dict[str, Any]], candidate: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[Any] = set()
    for r in rows:
        if not _row_matches_aap_template_name(r, candidate):
            continue
        rid = r.get("id")
        if rid in seen:
            continue
        seen.add(rid)
        out.append(r)
    return out


def _summarize_template_row(kind: str, r: dict[str, Any]) -> str:
    tid = r.get("id", "?")
    name = str(r.get("name", "") or "?").strip()
    desc = str(r.get("description", "") or "").strip()
    if len(desc) > 280:
        desc = desc[:279] + "…"
    line = f"  - {kind}: {name} (id={tid})"
    if desc:
        line += f" — {desc}"
    return line


async def aap_build_appendix(
    client: httpx.AsyncClient,
    aap_url: str,
    aap_token: str | None,
    candidates: list[str],
) -> str:
    """Return extra plain-text paragraph for chat: AAP template presence for each candidate."""
    if not candidates:
        return ""
    tls_verify = _aap_tls_verify_enabled()
    if not tls_verify:
        log.warning(
            "AAP MCP TLS certificate verification is disabled (AAP_TLS_VERIFY / TLS_VERIFY); "
            "use only on trusted networks."
        )

    headers = _mcp_headers_aap(aap_token)
    tool_j = _aap_tool_job_templates_list_name()
    tool_w = _aap_tool_workflow_job_templates_list_name()
    lines: list[str] = ["AAP (job / workflow job templates):"]

    async def _run(c: httpx.AsyncClient) -> str:
        for cand in candidates:
            terms = _aap_search_terms(cand)
            merged_j: dict[Any, dict[str, Any]] = {}
            merged_w: dict[Any, dict[str, Any]] = {}
            j_rows: list[dict[str, Any]] = []
            w_rows: list[dict[str, Any]] = []
            for term in terms:
                try:
                    j_raw = await _mcp_call_tool(
                        c, aap_url, headers, tool_j, {"search": term, "page_size": 100}
                    )
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 405:
                        log.error(
                            "AAP MCP POST returned 405 for %s — host is not the streamable HTTP MCP server "
                            "(often the AAP UI Route).",
                            e.request.url,
                        )
                        return _AAP_APPENDIX_405
                    raise
                w_raw = await _mcp_call_tool(
                    c, aap_url, headers, tool_w, {"search": term, "page_size": 100}
                )
                if isinstance(j_raw, dict) and j_raw.get("error"):
                    log.warning(
                        "AAP %s search=%r candidate=%r: %s",
                        tool_j,
                        term,
                        cand,
                        j_raw.get("detail") if isinstance(j_raw.get("detail"), (dict, str)) else j_raw,
                    )
                if isinstance(w_raw, dict) and w_raw.get("error"):
                    log.warning(
                        "AAP %s search=%r candidate=%r: %s",
                        tool_w,
                        term,
                        cand,
                        w_raw.get("detail") if isinstance(w_raw.get("detail"), (dict, str)) else w_raw,
                    )
                for r in _tower_results(j_raw):
                    rid = r.get("id")
                    if rid is not None:
                        merged_j[rid] = r
                for r in _tower_results(w_raw):
                    rid = r.get("id")
                    if rid is not None:
                        merged_w[rid] = r
                j_rows = _rows_matching_candidate(list(merged_j.values()), cand)
                w_rows = _rows_matching_candidate(list(merged_w.values()), cand)
                if j_rows or w_rows:
                    break
            if j_rows or w_rows:
                lines.append(f"- {cand}: found in AAP")
                for r in j_rows[:5]:
                    lines.append(_summarize_template_row("Job template", r))
                for r in w_rows[:5]:
                    lines.append(_summarize_template_row("Workflow job template", r))
            else:
                log.info(
                    "AAP no template match for candidate=%r after search terms=%s (merged job rows=%s wfjt rows=%s)",
                    cand,
                    terms,
                    len(merged_j),
                    len(merged_w),
                )
                lines.append(
                    f"- {cand}: no matching job or workflow job template in AAP for this search "
                    f"(KB name may differ slightly from the controller object name)."
                )
        return "\n".join(lines)

    if tls_verify:
        return await _run(client)
    async with httpx.AsyncClient(
        limits=_HTTP_CLIENT_LIMITS, follow_redirects=True, verify=False
    ) as aap_client:
        return await _run(aap_client)


def _subscribe_payload() -> dict[str, Any]:
    name = _optional_env("CHANNEL_NAME")
    cid = _optional_env("CHANNEL_ID")
    if name:
        return {"type": "subscribe", "channel_name": name}
    if cid:
        return {"type": "subscribe", "channel_id": cid}
    raise RuntimeError("Set CHANNEL_NAME or CHANNEL_ID")


def _send_payload(body: str) -> dict[str, Any]:
    name = _optional_env("CHANNEL_NAME")
    cid = _optional_env("CHANNEL_ID")
    if name:
        return {"type": "send_message", "channel_name": name, "body": body}
    if cid:
        return {"type": "send_message", "channel_id": cid, "body": body}
    raise RuntimeError("Set CHANNEL_NAME or CHANNEL_ID")


async def run_bot() -> None:
    chat_base = _env("CHAT_BASE_URL")
    chat_user = _env("CHAT_USERNAME")
    chat_pass = _env("CHAT_PASSWORD")
    itsm_base = _env("ITSM_BASE_URL")
    mcp_token = _optional_env("ITSM_MCP_TOKEN")
    llm_base = _env("LLM_BASE_URL")
    llm_model = _env("LLM_MODEL", "llama-scout-17b")
    llm_key = _optional_env("LLM_API_KEY")
    top_k = int(os.environ.get("RAG_TOP_K", "5"))

    mcp_url = _mcp_url(itsm_base)
    aap_urls = _aap_mcp_toolset_urls()
    aap_job_mcp_url = _aap_job_management_mcp_url()

    async with httpx.AsyncClient(limits=_HTTP_CLIENT_LIMITS, follow_redirects=True) as http:
        token = await chat_login(http, chat_base, chat_user, chat_pass)
        me = await chat_me(http, chat_base, token)
        my_id = me.get("id")
        if my_id is None:
            raise RuntimeError("/users/me missing id")
        my_id_str = str(my_id)
        log.info("Logged in as %s id=%s", me.get("username"), my_id_str)

        ws_uri = _ws_url(chat_base, token)
        sub = _subscribe_payload()

        async with websockets.connect(ws_uri, max_size=10 * 1024 * 1024) as ws:
            await ws.send(json.dumps(sub))
            raw0 = await ws.recv()
            ev0 = json.loads(raw0) if isinstance(raw0, str) else json.loads(raw0.decode())
            if ev0.get("type") == "error":
                raise RuntimeError(f"subscribe failed: {ev0}")
            if ev0.get("type") != "subscribed":
                raise RuntimeError(f"expected subscribed, got: {ev0}")
            _ready.set()
            log.info("Subscribed to channel channel_id=%s", ev0.get("channel_id"))
            if aap_urls and _aap_configured():
                log.info(
                    "AAP MCP: %s toolset URL(s) configured (template checks → %s)",
                    len(aap_urls),
                    (aap_job_mcp_url or "").rstrip("/"),
                )
                if aap_job_mcp_url:
                    await _aap_warn_if_mcp_url_looks_like_ui(aap_job_mcp_url)

            async for message in ws:
                if isinstance(message, bytes):
                    message = message.decode()
                try:
                    ev = json.loads(message)
                except json.JSONDecodeError:
                    continue
                if ev.get("type") != "message_created":
                    continue
                payload = ev.get("payload") or {}
                author = str(payload.get("user_id", ""))
                if author == my_id_str:
                    continue
                body = payload.get("body")
                if not isinstance(body, str) or not body.strip():
                    continue
                query = _query_from_channel_body(body)
                log.info(
                    "Handling user_id=%s RAG query (len=%s): %s",
                    author,
                    len(query),
                    query[:200].replace("\n", " | "),
                )
                try:
                    ok, rows = await mcp_rag_then_search_kb(http, mcp_url, mcp_token, query, top_k)
                    if not ok:
                        log.warning("No KB rows from rag_search_kb or search_kb; sample keys=%s", query[:80])
                        reply = NOTHING
                    else:
                        log.info("KB rows=%s first_title=%r", len(rows), rows[0].get("title", "")[:60])
                        reply = await llm_answer(http, llm_base, llm_model, llm_key, query, rows)
                        if _reply_is_non_answer(reply):
                            log.info("LLM returned empty/negative; using KB excerpt fallback")
                            reply = _kb_fallback_reply(rows)
                        if _aap_configured() and aap_job_mcp_url and ok and rows:
                            cands = _extract_aap_candidates(rows)
                            if cands:
                                try:
                                    apx = await aap_build_appendix(
                                        http,
                                        aap_job_mcp_url,
                                        _optional_env("AAP_MCP_TOKEN"),
                                        cands,
                                    )
                                    if apx:
                                        reply = f"{reply.rstrip()}\n\n{apx}"
                                except Exception:
                                    log.exception("AAP lookup appendix failed")
                except Exception:
                    log.exception("RAG/LLM failed")
                    reply = NOTHING
                await ws.send(json.dumps(_send_payload(reply)))


def main() -> None:
    port = int(os.environ.get("HEALTH_PORT", "8080"))
    t = threading.Thread(target=_run_health_server, args=(port,), daemon=True)
    t.start()
    log.info("Health server on :%s /healthz", port)
    asyncio.run(run_bot())


if __name__ == "__main__":
    main()
