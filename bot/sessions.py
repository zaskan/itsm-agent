"""In-memory thread conversation state keyed by root message id."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

Phase = Literal["collect", "ready", "running"]
WorkflowKind = Literal["incident", "catalog", "generic"]

_sessions: dict[str, "ThreadSession"] = {}


@dataclass
class ThreadSession:
    root_id: str
    channel_id: str
    user_query: str
    kb_rows: list[dict[str, Any]]
    template_candidate: str | None
    missing_fields: list[str]
    catalog_template_name: str | None = None
    workflow_kind: WorkflowKind = "generic"
    collected: dict[str, str] = field(default_factory=dict)
    thread_replies: list[str] = field(default_factory=list)
    phase: Phase = "collect"


def get(root_id: str) -> ThreadSession | None:
    return _sessions.get(root_id)


def put(session: ThreadSession) -> None:
    _sessions[session.root_id] = session


def remove(root_id: str) -> None:
    _sessions.pop(root_id, None)


def append_reply(session: ThreadSession, body: str) -> None:
    session.thread_replies.append(body.strip())
