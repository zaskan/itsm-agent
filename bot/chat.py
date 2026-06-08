"""demo-chat REST, WebSocket payloads, and thread replies."""

from __future__ import annotations

import json
import urllib.parse
from typing import Any

import httpx

from bot.config import _optional_env


def ws_url(chat_base: str, token: str) -> str:
    b = chat_base.rstrip("/")
    q = urllib.parse.urlencode({"token": token})
    if b.startswith("https://"):
        return "wss://" + b[len("https://") :] + "/api/v1/ws?" + q
    if b.startswith("http://"):
        return "ws://" + b[len("http://") :] + "/api/v1/ws?" + q
    raise ValueError("CHAT_BASE_URL must start with http:// or https://")


def channel_ref() -> dict[str, str]:
    if name := _optional_env("CHANNEL_NAME"):
        return {"channel_name": name}
    if cid := _optional_env("CHANNEL_ID"):
        return {"channel_id": cid}
    raise RuntimeError("Set CHANNEL_NAME or CHANNEL_ID")


def subscribe_payload() -> dict[str, Any]:
    return {"type": "subscribe", **channel_ref()}


def send_payload(body: str, parent_id: str | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {"type": "send_message", **channel_ref(), "body": body}
    if parent_id:
        payload["parent_id"] = parent_id
    return payload


async def reply_ws(ws: Any, text: str, parent_id: str | None = None) -> None:
    await ws.send(json.dumps(send_payload(text, parent_id=parent_id)))


async def chat_login(client: httpx.AsyncClient, base: str, user: str, password: str) -> str:
    r = await client.post(
        f"{base.rstrip('/')}/api/v1/auth/login",
        json={"username": user, "password": password},
        headers={"Content-Type": "application/json"},
        timeout=60.0,
    )
    r.raise_for_status()
    tok = r.json().get("access_token")
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
