"""In-memory thread conversation state keyed by root message id."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

Phase = Literal["collect", "ready", "running"]
WorkflowKind = Literal["incident", "catalog", "generic", "lightspeed"]

_sessions: dict[str, "ThreadSession"] = {}
_incident_roots: dict[str, str] = {}


def remember_incident_root(incident_ref: str, root_id: str) -> None:
    ref = incident_ref.strip().upper()
    if ref and root_id:
        _incident_roots[ref] = root_id


def incident_root_for(incident_ref: str) -> str | None:
    return _incident_roots.get(incident_ref.strip().upper())


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
