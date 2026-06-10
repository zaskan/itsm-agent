"""WebSocket event loop: RAG-grounded thread replies and AAP MCP execution."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import threading
import time
from collections import deque
from typing import Any

import httpx
import websockets

from bot.aap_mcp import aap_mcp_configured, extract_template_from_kb, run_template_and_wait, with_aap_client
from bot.chat import chat_login, chat_me, reply_ws, subscribe_payload, ws_url
from bot.config import HTTP_CLIENT_LIMITS, _env, _optional_env, mcp_url
from bot.health import run_health_server, set_ready
from bot.knowledge import (
    is_incident_channel_message,
    is_remediation_complete_message,
    mcp_rag_search,
    parse_catalog_field_from_thread,
    parse_incident_from_body,
    parse_vm_name_from_query,
    query_from_channel_body,
)
from bot.llm import LlmDecision, llm_assess
from bot.itsm_mcp import (
    ensure_itsm_refs_for_launch,
    extract_catalog_from_kb,
    filter_missing_for_catalog,
    find_request_template,
    required_template_field_keys,
    resolve_workflow_kind,
    specs_from_collected,
    template_field_keys,
)
from bot.sessions import (
    ThreadSession,
    WorkflowKind,
    append_reply,
    get,
    incident_root_for,
    put,
    remember_incident_root,
    remove,
)

log = logging.getLogger("itsm-agent-bot")

_GO_RE = re.compile(r"(?i)\b(yes|yeah|yep|sure|ok|okay|please|launch|go\s+ahead|go|do\s+it|remediate|fix\s+it)\b")

_seen_ws_message_ids: deque[tuple[str, float]] = deque(maxlen=256)
_recent_message_fingerprints: deque[tuple[str, str, str, float]] = deque(maxlen=64)
_DEDUP_TTL_SEC = 12.0


def _consume_if_duplicate_event_id(mid: Any) -> bool:
    if mid is None:
        return False
    sid = str(mid).strip()
    if not sid:
        return False
    now = time.monotonic()
    while _seen_ws_message_ids and now - _seen_ws_message_ids[0][1] > 60.0:
        _seen_ws_message_ids.popleft()
    for seen, _ in _seen_ws_message_ids:
        if seen == sid:
            log.info("Skipping duplicate message_created id=%s", sid[:32])
            return True
    _seen_ws_message_ids.append((sid, now))
    return False


def _consume_if_duplicate_human_message(channel_id: str, author: str, body: str) -> bool:
    now = time.monotonic()
    key = (channel_id, author, body.strip())
    while _recent_message_fingerprints and now - _recent_message_fingerprints[0][3] > _DEDUP_TTL_SEC:
        _recent_message_fingerprints.popleft()
    for cid, a, b, _ in _recent_message_fingerprints:
        if (cid, a, b) == key:
            log.info("Skipping duplicate chat message fingerprint chan=%s user_id=%s", channel_id, author[:12])
            return True
    _recent_message_fingerprints.append((*key, now))
    return False


def _seed_catalog_context(session: ThreadSession) -> None:
    if _session_workflow_kind(session) != "catalog":
        return
    if not session.collected.get("vm_name"):
        if vm := parse_vm_name_from_query(session.user_query):
            session.collected["vm_name"] = vm


def _merge_collected(session: ThreadSession, body: str) -> None:
    text = body.strip()
    if not text:
        return
    parsed_any = False
    for line in text.splitlines():
        if ":" in line:
            key, val = line.split(":", 1)
            k, v = key.strip(), val.strip()
            if k and v:
                session.collected[k] = v
                parsed_any = True
    if _session_workflow_kind(session) == "catalog":
        targets = session.missing_fields or ["vm_name", "cpus", "mem", "app_repo"]
        for field in targets:
            if str(session.collected.get(field, "")).strip():
                continue
            if val := parse_catalog_field_from_thread(text, field):
                session.collected[field] = val
                parsed_any = True
    _seed_catalog_context(session)
    _seed_incident_fields(session)
    if not parsed_any and len(text.splitlines()) == 1 and ":" not in text:
        session.collected["user_input"] = text


def _seed_incident_fields(session: ThreadSession) -> None:
    if session.workflow_kind != "incident":
        return
    for key, val in parse_incident_from_body(session.user_query).items():
        session.collected.setdefault(key, val)


def _incident_fields_ready(session: ThreadSession) -> bool:
    return bool(session.collected.get("itsm_incident_ref") and session.collected.get("vm_name"))


def _session_workflow_kind(session: ThreadSession) -> WorkflowKind:
    return session.workflow_kind or resolve_workflow_kind(session.kb_rows, session.user_query)


def _remember_incident_thread_root(session: ThreadSession) -> None:
    if session.workflow_kind != "incident":
        return
    _seed_incident_fields(session)
    if ref := session.collected.get("itsm_incident_ref"):
        remember_incident_root(ref, session.root_id)


def _apply_session_workflow(
    session: ThreadSession,
    *,
    user_query: str,
    kb_rows: list[dict[str, Any]],
    catalog_name: str | None,
) -> None:
    session.workflow_kind = resolve_workflow_kind(kb_rows, user_query)
    session.catalog_template_name = catalog_name if session.workflow_kind == "catalog" else None
    _seed_catalog_context(session)
    _remember_incident_thread_root(session)


async def _forward_remediation_to_thread(ws: Any, body: str) -> bool:
    parsed = parse_incident_from_body(body)
    inc = parsed.get("itsm_incident_ref")
    if not inc:
        log.warning("remediation.complete missing incident ref")
        return False
    thread_root = incident_root_for(inc)
    if not thread_root:
        log.warning("No thread root mapped for remediation %s", inc)
        return False
    await reply_ws(ws, body.strip(), parent_id=thread_root)
    log.info("Forwarded remediation.complete for %s to thread root=%s", inc, thread_root[:12])
    return True


def _incident_confirmation_reply(user_query: str, template_name: str | None) -> str:
    parsed = parse_incident_from_body(user_query)
    inc = parsed.get("itsm_incident_ref") or "the incident"
    host = parsed.get("vm_name") or "the affected host"
    tmpl = template_name or "remediation"
    return (
        f"Incident {inc} indicates the Apache application is down on {host}. "
        f"Reply yes or remediate when you want me to launch {tmpl}."
    )


def _launch_prompt(workflow_kind: WorkflowKind, template_name: str) -> str:
    if workflow_kind == "incident":
        return f"Reply yes or remediate when you want me to launch {template_name}."
    return f"Reply go when you want me to launch {template_name}."


def _effective_template(decision: LlmDecision, kb_rows: list[dict[str, Any]], fallback: str | None) -> str | None:
    return decision.template_name or fallback or extract_template_from_kb(kb_rows)


async def _catalog_specs_ready(
    http: httpx.AsyncClient,
    session: ThreadSession,
    *,
    mcp_url_str: str,
    mcp_token: str | None,
) -> bool:
    if not session.catalog_template_name:
        return False
    template = await find_request_template(http, mcp_url_str, mcp_token, session.catalog_template_name)
    if template is None:
        return False
    _seed_catalog_context(session)
    specs = specs_from_collected(template, session.collected)
    return not [k for k in required_template_field_keys(template) if k not in specs]


async def _refresh_catalog_missing_fields(
    http: httpx.AsyncClient,
    session: ThreadSession,
    *,
    mcp_url_str: str,
    mcp_token: str | None,
) -> None:
    if _session_workflow_kind(session) != "catalog" or not session.catalog_template_name:
        return
    template = await find_request_template(http, mcp_url_str, mcp_token, session.catalog_template_name)
    if template is None:
        return
    _seed_catalog_context(session)
    session.missing_fields = [
        k
        for k in required_template_field_keys(template)
        if not str(session.collected.get(k, "")).strip()
    ]


async def _launch_fields_ready(
    http: httpx.AsyncClient,
    session: ThreadSession,
    *,
    mcp_url_str: str,
    mcp_token: str | None,
) -> bool:
    kind = _session_workflow_kind(session)
    if kind == "catalog":
        return await _catalog_specs_ready(http, session, mcp_url_str=mcp_url_str, mcp_token=mcp_token)
    if kind == "incident":
        return _incident_fields_ready(session)
    return session.phase == "ready"


async def _try_launch_from_session(
    ws: Any,
    http: httpx.AsyncClient,
    session: ThreadSession,
    *,
    mcp_url_str: str,
    mcp_token: str | None,
) -> bool:
    template_name = session.template_candidate
    if not template_name:
        return False
    _seed_incident_fields(session)
    if not await _launch_fields_ready(http, session, mcp_url_str=mcp_url_str, mcp_token=mcp_token):
        return False
    return await _maybe_launch(
        ws, http, session, template_name, mcp_url_str=mcp_url_str, mcp_token=mcp_token
    )


async def _launch_in_background(
    ws: Any,
    http: httpx.AsyncClient,
    session: ThreadSession,
    template_name: str,
    *,
    mcp_url_str: str,
    mcp_token: str | None,
) -> None:
    session.phase = "running"
    try:
        collected, itsm_msg = await ensure_itsm_refs_for_launch(
            http,
            mcp_url_str,
            mcp_token,
            kb_rows=session.kb_rows,
            collected=session.collected,
            user_query=session.user_query,
        )
        session.collected = collected
        if itsm_msg and (
            itsm_msg.startswith("Opening the ITSM service request failed")
            or itsm_msg.startswith("I could not find the ITSM catalog template")
            or "Cannot launch automation without itsm_change_ref" in itsm_msg
        ):
            result = itsm_msg
        elif (
            _session_workflow_kind(session) == "catalog"
            and not session.collected.get("itsm_change_ref")
        ):
            result = (
                "Cannot launch automation: itsm_change_ref is missing after the ITSM service request step."
            )
        else:
            parts: list[str] = []
            if itsm_msg:
                parts.append(itsm_msg)
            aap_result = await with_aap_client(
                http, lambda c: run_template_and_wait(c, template_name, session.collected)
            )
            parts.append(aap_result)
            result = "\n\n".join(parts)
    except Exception:
        log.exception("Launch pipeline failed root=%s", session.root_id[:12])
        result = "Automation run failed due to an internal error."
    await reply_ws(ws, result, parent_id=session.root_id)
    remove(session.root_id)


async def _maybe_launch(
    ws: Any,
    http: httpx.AsyncClient,
    session: ThreadSession,
    template_name: str,
    *,
    mcp_url_str: str,
    mcp_token: str | None,
) -> bool:
    if not template_name:
        return False
    if not aap_mcp_configured():
        await reply_ws(
            ws,
            "Automation is not configured (missing AAP_MCP_BASE_URL or AAP_MCP_TOKEN).",
            parent_id=session.root_id,
        )
        return True
    status = (
        "Opening ITSM service request and launching automation"
        if _session_workflow_kind(session) == "catalog"
        else f"Launching {template_name}"
    )
    session.phase = "running"
    await reply_ws(ws, f"{status}…", parent_id=session.root_id)
    asyncio.create_task(
        _launch_in_background(
            ws, http, session, template_name, mcp_url_str=mcp_url_str, mcp_token=mcp_token
        )
    )
    return True


async def _apply_decision(
    ws: Any,
    http: httpx.AsyncClient,
    *,
    root_id: str,
    ch_id: str,
    user_query: str,
    kb_rows: list[dict[str, Any]],
    decision: LlmDecision,
    session: ThreadSession | None,
    template_hint: str | None,
    mcp_url_str: str,
    mcp_token: str | None,
) -> None:
    template_name = _effective_template(decision, kb_rows, template_hint)
    workflow_kind = resolve_workflow_kind(kb_rows, user_query)
    catalog_name = extract_catalog_from_kb(kb_rows) if workflow_kind == "catalog" else None
    missing_fields = filter_missing_for_catalog(decision.missing_fields) if catalog_name else decision.missing_fields

    if decision.action == "silent":
        log.info("LLM silent for root=%s", root_id[:12])
        return

    if decision.action == "launch_ready":
        sess = session or ThreadSession(
            root_id=root_id,
            channel_id=ch_id,
            user_query=user_query,
            kb_rows=kb_rows,
            template_candidate=template_name,
            missing_fields=[],
            catalog_template_name=catalog_name,
            workflow_kind=workflow_kind,
            phase="ready",
        )
        sess.template_candidate = template_name
        sess.phase = "ready"
        _apply_session_workflow(sess, user_query=user_query, kb_rows=kb_rows, catalog_name=catalog_name)
        put(sess)
        if template_name and await _maybe_launch(
            ws, http, sess, template_name, mcp_url_str=mcp_url_str, mcp_token=mcp_token
        ):
            return
        await reply_ws(ws, decision.reply or "Ready to proceed.", parent_id=root_id)
        return

    if decision.action == "need_info":
        reply = decision.reply
        if catalog_name:
            reply = f"{reply.rstrip()} I will open the ITSM catalog request {catalog_name} for you once the deployment details are provided."
        sess = session or ThreadSession(
            root_id=root_id,
            channel_id=ch_id,
            user_query=user_query,
            kb_rows=kb_rows,
            template_candidate=template_name,
            missing_fields=missing_fields,
            catalog_template_name=catalog_name,
            workflow_kind=workflow_kind,
            phase="collect",
        )
        sess.missing_fields = missing_fields
        sess.template_candidate = template_name
        sess.phase = "collect"
        _apply_session_workflow(sess, user_query=user_query, kb_rows=kb_rows, catalog_name=catalog_name)
        put(sess)
        await reply_ws(ws, reply, parent_id=root_id)
        return

    # action == answer
    reply = decision.reply
    if template_name and aap_mcp_configured():
        sess = session or ThreadSession(
            root_id=root_id,
            channel_id=ch_id,
            user_query=user_query,
            kb_rows=kb_rows,
            template_candidate=template_name,
            missing_fields=[],
            catalog_template_name=catalog_name,
            workflow_kind=workflow_kind,
            phase="ready",
        )
        sess.template_candidate = template_name
        _apply_session_workflow(sess, user_query=user_query, kb_rows=kb_rows, catalog_name=catalog_name)
        put(sess)
        prompt = _launch_prompt(workflow_kind, template_name)
        if reply:
            reply = f"{reply.rstrip()}\n\n{prompt}"
        else:
            reply = prompt
    if reply:
        await reply_ws(ws, reply, parent_id=root_id)


async def _handle_root_message(
    ws: Any,
    http: httpx.AsyncClient,
    body: str,
    ch_id: str,
    root_id: str,
    *,
    mcp_url_str: str,
    mcp_token: str | None,
    top_k: int,
    llm_base: str,
    llm_model: str,
    llm_key: str | None,
) -> None:
    query = query_from_channel_body(body)
    log.info("Root message root=%s query=%s", root_id[:12], query[:120].replace("\n", " | "))

    if is_incident_channel_message(query):
        parsed = parse_incident_from_body(query)
        if ref := parsed.get("itsm_incident_ref"):
            remember_incident_root(ref, root_id)

    ok, rows = await mcp_rag_search(http, mcp_url_str, mcp_token, query, top_k)
    if not ok:
        log.info("No RAG hits root=%s", root_id[:12])
        return

    template_hint = extract_template_from_kb(rows)
    decision = await llm_assess(http, llm_base, llm_model, llm_key, query, rows, template_hint=template_hint)
    if decision is None:
        log.warning("LLM assess returned nothing root=%s", root_id[:12])
        if is_incident_channel_message(query):
            decision = LlmDecision(
                action="answer",
                reply=_incident_confirmation_reply(query, template_hint),
                missing_fields=[],
                template_name=template_hint,
            )
        else:
            return
    elif decision.action == "silent" and is_incident_channel_message(query):
        log.info("LLM silent on incident root=%s; using deterministic reply", root_id[:12])
        decision = LlmDecision(
            action="answer",
            reply=_incident_confirmation_reply(query, template_hint or decision.template_name),
            missing_fields=[],
            template_name=decision.template_name or template_hint,
        )

    await _apply_decision(
        ws,
        http,
        root_id=root_id,
        ch_id=ch_id,
        user_query=query,
        kb_rows=rows,
        decision=decision,
        session=None,
        template_hint=template_hint,
        mcp_url_str=mcp_url_str,
        mcp_token=mcp_token,
    )


async def _handle_thread_followup(
    ws: Any,
    http: httpx.AsyncClient,
    body: str,
    session: ThreadSession,
    *,
    mcp_url_str: str,
    mcp_token: str | None,
    llm_base: str,
    llm_model: str,
    llm_key: str | None,
) -> None:
    if session.phase == "running":
        return

    append_reply(session, body)
    _merge_collected(session, body)
    await _refresh_catalog_missing_fields(http, session, mcp_url_str=mcp_url_str, mcp_token=mcp_token)

    template_name = session.template_candidate or extract_template_from_kb(session.kb_rows)
    if template_name and await _launch_fields_ready(http, session, mcp_url_str=mcp_url_str, mcp_token=mcp_token):
        session.template_candidate = template_name
        put(session)
        if await _maybe_launch(
            ws, http, session, template_name, mcp_url_str=mcp_url_str, mcp_token=mcp_token
        ):
            return

    if _GO_RE.search(body.strip()):
        if await _try_launch_from_session(
            ws, http, session, mcp_url_str=mcp_url_str, mcp_token=mcp_token
        ):
            return

    decision = await llm_assess(
        http,
        llm_base,
        llm_model,
        llm_key,
        session.user_query,
        session.kb_rows,
        thread_replies=session.thread_replies,
        collected=session.collected,
        template_hint=session.template_candidate,
    )
    if decision is None:
        log.warning("LLM assess failed on thread root=%s", session.root_id[:12])
        return

    template_name = session.template_candidate or extract_template_from_kb(session.kb_rows)
    if decision.action == "launch_ready" and template_name:
        session.template_candidate = template_name
        put(session)
        if await _maybe_launch(
            ws,
            http,
            session,
            template_name,
            mcp_url_str=mcp_url_str,
            mcp_token=mcp_token,
        ):
            return

    await _apply_decision(
        ws,
        http,
        root_id=session.root_id,
        ch_id=session.channel_id,
        user_query=session.user_query,
        kb_rows=session.kb_rows,
        decision=decision,
        session=session,
        template_hint=session.template_candidate,
        mcp_url_str=mcp_url_str,
        mcp_token=mcp_token,
    )


async def run_bot() -> None:
    chat_base = _env("CHAT_BASE_URL")
    itsm_base = _env("ITSM_BASE_URL")
    mcp_url_str = mcp_url(itsm_base)
    mcp_token = _optional_env("ITSM_MCP_TOKEN")
    llm_base = _env("LLM_BASE_URL")
    llm_model = _env("LLM_MODEL", "llama-scout-17b")
    llm_key = _optional_env("LLM_API_KEY")
    top_k = int(os.environ.get("RAG_TOP_K", "5"))

    async with httpx.AsyncClient(limits=HTTP_CLIENT_LIMITS, follow_redirects=True) as http:
        token = await chat_login(http, chat_base, _env("CHAT_USERNAME"), _env("CHAT_PASSWORD"))
        me = await chat_me(http, chat_base, token)
        if me.get("id") is None:
            raise RuntimeError("/users/me missing id")
        my_id_str = str(me["id"])
        log.info("Logged in as %s id=%s", me.get("username"), my_id_str)

        if aap_mcp_configured():
            log.info("AAP MCP → %s", _optional_env("AAP_MCP_BASE_URL"))
        log.info("ITSM MCP → %s", mcp_url_str)

        async with websockets.connect(ws_url(chat_base, token), max_size=10 * 1024 * 1024) as ws:
            await ws.send(json.dumps(subscribe_payload()))
            raw0 = await ws.recv()
            ev0 = json.loads(raw0 if isinstance(raw0, str) else raw0.decode())
            if ev0.get("type") == "error":
                raise RuntimeError(f"subscribe failed: {ev0}")
            if ev0.get("type") != "subscribed":
                raise RuntimeError(f"expected subscribed, got: {ev0}")
            set_ready()
            log.info("Subscribed to channel channel_id=%s", ev0.get("channel_id"))

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
                body = payload.get("body")
                if not isinstance(body, str) or not body.strip():
                    continue
                author = str(payload.get("user_id", ""))
                parent_id = payload.get("parent_id")
                if author == my_id_str:
                    if parent_id is not None:
                        continue
                    if is_remediation_complete_message(body):
                        await _forward_remediation_to_thread(ws, body)
                        continue
                    if not is_incident_channel_message(body):
                        continue
                    log.info("Processing incident notification posted as bot user")
                ch_id = str(
                    payload.get("channel_id") or ev0.get("channel_id") or _optional_env("CHANNEL_ID") or ""
                )
                mid = payload.get("id") if payload.get("id") is not None else payload.get("message_id")
                if _consume_if_duplicate_event_id(mid):
                    continue
                if _consume_if_duplicate_human_message(ch_id, author, body):
                    continue

                if parent_id is None and is_remediation_complete_message(body):
                    if await _forward_remediation_to_thread(ws, body):
                        continue
                    log.warning("remediation.complete at channel root with no mapped thread")
                    continue

                if parent_id is not None:
                    root_id = str(parent_id)
                    session = get(root_id)
                    if session is None:
                        log.debug("Ignoring thread reply without session root=%s", root_id[:12])
                        continue
                    await _handle_thread_followup(
                        ws,
                        http,
                        body,
                        session,
                        mcp_url_str=mcp_url_str,
                        mcp_token=mcp_token,
                        llm_base=llm_base,
                        llm_model=llm_model,
                        llm_key=llm_key,
                    )
                    continue

                root_id = str(mid) if mid is not None else ""
                if not root_id:
                    log.warning("Root message missing id; cannot thread reply")
                    continue

                await _handle_root_message(
                    ws,
                    http,
                    body,
                    ch_id,
                    root_id,
                    mcp_url_str=mcp_url_str,
                    mcp_token=mcp_token,
                    top_k=top_k,
                    llm_base=llm_base,
                    llm_model=llm_model,
                    llm_key=llm_key,
                )


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(levelname)s %(message)s")
    port = int(os.environ.get("HEALTH_PORT", "8080"))
    threading.Thread(target=run_health_server, args=(port,), daemon=True).start()
    log.info("Health server on :%s /healthz", port)
    asyncio.run(run_bot())
