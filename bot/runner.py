"""WebSocket event loop and incident / launch phase handlers."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
from typing import Any

import httpx
import websockets

from bot.aap import (
    aap_build_appendix,
    aap_probe_api,
    aap_try_launch_from_offer,
    extract_aap_candidates,
    pending_launch_by_channel,
)
from bot.chat import (
    body_is_affirmative_launch,
    body_mentions_username,
    chat_login,
    chat_me,
    reply_ws,
    subscribe_payload,
    ws_url,
)
from bot.config import (
    HTTP_CLIENT_LIMITS,
    NOTHING,
    _env,
    _optional_env,
    aap_api_base,
    aap_configured,
    mcp_url,
)
from bot.health import run_health_server, set_ready
from bot.knowledge import mcp_rag_then_search_kb, query_from_channel_body
from bot.llm import kb_fallback_reply, llm_answer, reply_is_non_answer

log = logging.getLogger("itsm-agent-bot")


async def _handle_launch_mention(
    ws: Any, http: httpx.AsyncClient, body: str, ch_id: str, bot_username: str
) -> bool:
    """Phase: launch_confirmation — returns True if the message was consumed."""
    if not body_mentions_username(body, bot_username):
        return False
    if ch_id in pending_launch_by_channel and not body_is_affirmative_launch(body):
        await reply_ws(
            ws,
            "Reply with yes or launch if you want me to run the template I offered, "
            "or send a new incident without @mention.",
        )
        return True
    if not body_is_affirmative_launch(body):
        return False
    if not aap_configured():
        await reply_ws(ws, "AAP launch is not configured (missing API URL or token).")
        return True
    if ch_id not in pending_launch_by_channel:
        await reply_ws(
            ws,
            "There is no job or workflow template waiting to launch. "
            "Post an incident first so I can suggest one from the knowledge base.",
        )
        return True
    offer = pending_launch_by_channel[ch_id]
    try:
        reply = await aap_try_launch_from_offer(ws, http, offer)
    except Exception:
        log.exception("AAP launch")
        reply = "Launch failed due to an internal error."
    await reply_ws(ws, reply)
    return True


async def _handle_incident_message(
    ws: Any,
    http: httpx.AsyncClient,
    body: str,
    ch_id: str,
    author: str,
    *,
    mcp_url_str: str,
    mcp_token: str | None,
    top_k: int,
    llm_base: str,
    llm_model: str,
    llm_key: str | None,
) -> None:
    """Phase: incident_rag_llm — RAG, LLM summary, optional AAP appendix, then reply."""
    query = query_from_channel_body(body)
    log.info(
        "Handling user_id=%s RAG query (len=%s): %s",
        author,
        len(query),
        query[:200].replace("\n", " | "),
    )
    try:
        ok, rows = await mcp_rag_then_search_kb(http, mcp_url_str, mcp_token, query, top_k)
        if not ok:
            log.warning("No KB rows from rag_search_kb or search_kb; sample=%s", query[:80])
            reply = NOTHING
        else:
            log.info("KB rows=%s first_title=%r", len(rows), rows[0].get("title", "")[:60])
            reply = await llm_answer(http, llm_base, llm_model, llm_key, query, rows)
            if reply_is_non_answer(reply):
                log.info("LLM returned empty/negative; using KB excerpt fallback")
                reply = kb_fallback_reply(rows)
            if aap_configured() and (cands := extract_aap_candidates(rows)):
                try:
                    apx, launch_offer = await aap_build_appendix(http, cands, ch_id)
                    if apx:
                        reply = f"{reply.rstrip()}\n\n{apx}"
                    if launch_offer and ch_id:
                        pending_launch_by_channel[ch_id] = launch_offer
                        reply = f"{reply.rstrip()}\n\nDo you want me to launch the job for you?"
                    elif launch_offer:
                        log.warning("AAP launch offer skipped: could not resolve channel_id from event")
                except Exception:
                    log.exception("AAP lookup appendix failed")
    except Exception:
        log.exception("RAG/LLM failed")
        reply = NOTHING
    await reply_ws(ws, reply)


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

        if aap_configured():
            log.info("AAP REST → %s", aap_api_base())
            await aap_probe_api(http)

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

            bot_username = str(me.get("username") or "")
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
                if str(payload.get("user_id", "")) == my_id_str:
                    continue
                body = payload.get("body")
                if not isinstance(body, str) or not body.strip():
                    continue
                ch_id = str(
                    payload.get("channel_id") or ev0.get("channel_id") or _optional_env("CHANNEL_ID") or ""
                )

                if await _handle_launch_mention(ws, http, body, ch_id, bot_username):
                    continue
                await _handle_incident_message(
                    ws,
                    http,
                    body,
                    ch_id,
                    str(payload.get("user_id", "")),
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
