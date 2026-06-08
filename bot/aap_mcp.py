"""AAP Automation Controller operations via MCP (template resolve, launch, poll)."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from dataclasses import dataclass
from typing import Any, Literal

import httpx

from bot.config import (
    HTTP_CLIENT_LIMITS,
    MAX_AAP_CANDIDATES,
    aap_controller_ui_base,
    aap_job_poll_interval_sec,
    aap_job_poll_timeout_sec,
    aap_mcp_configured,
    aap_mcp_token,
    aap_mcp_url,
    aap_tls_verify_enabled,
)
from bot.mcp import mcp_call_tool, mcp_headers_aap

log = logging.getLogger("itsm-agent-bot")
_TERMINAL = frozenset({"successful", "failed", "error", "canceled", "cancelled"})


@dataclass
class ResolvedTemplate:
    kind: Literal["job", "workflow"]
    template_id: int
    template_name: str


def _strip_kb_noise(s: str) -> str:
    s = s.strip()
    for pat in (r"(?is)\s+Description\s*:.*$", r"(?is)\s+Alert\s+name\s*:.*$", r"(?is)\s+AAP\s+Remediation\s*:.*$"):
        s = re.sub(pat, "", s)
    return s.rstrip(" \t.;:|\"'").strip()


def extract_aap_candidates(kb_rows: list[dict[str, Any]]) -> list[str]:
    blob = "\n".join(
        chunk
        for row in kb_rows[:6]
        for key in ("title", "description")
        if (chunk := str(row.get(key, "") or "").strip())
    )
    patterns = [
        (r"(?is)Launch AAP workflow \*\*([^*]+)\*\*", 1),
        (r"(?is)(?:After confirmation,\s*)?launch\s+\*\*([^*]+)\*\*", 1),
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
        x = _strip_kb_noise(raw.strip().strip('`"\''))
        if "\n" in x:
            x = x.split("\n", 1)[0].strip()
        x = _strip_kb_noise(x)
        low = x.lower()
        if re.fullmatch(r"[a-z][a-z0-9_]*", x):
            return
        if x.startswith("[") or x.startswith("#") or x.upper() in {"INC-*"}:
            return
        if len(x) < 2 or len(x) > 200 or low.startswith(("workflow job template", "job template")) or low in seen:
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


def extract_template_from_kb(kb_rows: list[dict[str, Any]]) -> str | None:
    cands = extract_aap_candidates(kb_rows)
    if cands:
        return cands[0]
    for row in kb_rows[:3]:
        title = str(row.get("title", "") or "").strip()
        if title:
            base = re.sub(r"\s*\([^)]*\)\s*$", "", title).strip()
            if len(base) >= 5:
                return base
    return None


def _search_queries(name: str, *, max_q: int = 2) -> list[str]:
    c = name.strip()
    out, seen = [], set()

    def add(s: str) -> None:
        t = s.strip()
        if len(t) >= 2 and t.lower() not in seen:
            seen.add(t.lower())
            out.append(t)

    add(c)
    stripped = re.sub(r"^\[[^\]]+\]\s*", "", c).strip()
    if stripped and stripped.lower() != c.lower():
        add(stripped)
    parts = stripped.split() if stripped else c.split()
    if len(parts) >= 3:
        add(" ".join(parts[:3]))
    if len(parts) >= 2:
        add(" ".join(parts[:2]))
    return out[:max_q]


def _norm_label(s: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[\[\]]", "", str(s or "").lower())).strip()


def _matches_template_name(row: dict[str, Any], candidate: str) -> bool:
    name = str(row.get("name", "") or "")
    nl, c = name.lower(), candidate.strip().lower()
    if not c or not nl:
        return False
    if c in nl:
        return True
    c_alt = re.sub(r"^\[[^\]]+\]\s*", "", c).strip()
    if c_alt and len(c_alt) >= 3 and c_alt in nl:
        return True
    cn, nn = _norm_label(candidate), _norm_label(name)
    return (len(cn) >= 3 and cn in nn) or (len(nn) >= 3 and nn in cn) or (bool(cn) and cn == nn)


def _pick_match(rows: list[dict[str, Any]], candidate: str) -> dict[str, Any] | None:
    for r in rows:
        if _matches_template_name(r, candidate):
            return r
    return None


def _tool_rows(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, dict) and isinstance(data.get("results"), list):
        return [x for x in data["results"] if isinstance(x, dict)]
    return []


def _mcp_url() -> str:
    url = aap_mcp_url()
    if not url:
        raise RuntimeError("AAP_MCP_BASE_URL not set")
    return url


def _mcp_headers() -> dict[str, str]:
    return mcp_headers_aap(aap_mcp_token())


async def _aap_mcp_call(client: httpx.AsyncClient, tool: str, arguments: dict[str, Any]) -> Any:
    return await mcp_call_tool(client, _mcp_url(), _mcp_headers(), tool, arguments)


async def _list_templates(client: httpx.AsyncClient, kind: str, search: str) -> list[dict[str, Any]]:
    tool = "workflow_job_templates_list" if kind == "workflow" else "job_templates_list"
    data = await _aap_mcp_call(client, tool, {"search": search, "page_size": 25})
    if isinstance(data, dict) and data.get("error"):
        log.warning("AAP MCP %s failed: %s", tool, data.get("error"))
        return []
    return _tool_rows(data)


async def resolve_template(client: httpx.AsyncClient, candidate: str) -> ResolvedTemplate | None:
    w_match: dict[str, Any] | None = None
    j_match: dict[str, Any] | None = None
    for q in _search_queries(candidate):
        if w_match is None:
            w_match = _pick_match(await _list_templates(client, "workflow", q), candidate)
        if j_match is None:
            j_match = _pick_match(await _list_templates(client, "job", q), candidate)
        if w_match or j_match:
            break
    pick = w_match or j_match
    if not pick or pick.get("id") is None:
        return None
    kind: Literal["job", "workflow"] = "workflow" if w_match else "job"
    return ResolvedTemplate(
        kind=kind,
        template_id=int(pick["id"]),
        template_name=str(pick.get("name", "") or pick["id"]),
    )


def _coerce_value(raw: str) -> Any:
    s = raw.strip()
    if s.isdigit():
        return int(s)
    if re.fullmatch(r"-?\d+\.\d+", s):
        return float(s)
    return s


def _extra_vars(collected: dict[str, str]) -> dict[str, Any] | str:
    if not collected:
        return {}
    if len(collected) == 1 and "user_input" in collected:
        return collected["user_input"]
    skip = frozenset({"user_input"})
    return {k: _coerce_value(v) for k, v in collected.items() if k not in skip}


async def launch_template(
    client: httpx.AsyncClient,
    resolved: ResolvedTemplate,
    collected: dict[str, str],
) -> dict[str, Any]:
    extra = _extra_vars(collected)
    request_body: dict[str, Any] = {}
    if extra:
        request_body["extra_vars"] = extra
    if resolved.kind == "workflow":
        tool = "workflow_job_templates_launch_create"
    else:
        tool = "job_templates_launch_create"
    data = await _aap_mcp_call(
        client,
        tool,
        {"id": str(resolved.template_id), "requestBody": request_body},
    )
    if isinstance(data, dict) and data.get("error"):
        raise RuntimeError(str(data.get("detail") or data.get("error")))
    if isinstance(data, dict) and data.get("variables_needed_to_start"):
        raise RuntimeError("; ".join(str(x) for x in data["variables_needed_to_start"]))
    return data if isinstance(data, dict) else {}


def _launched_job(payload: dict[str, Any]) -> tuple[dict[str, Any], Literal["job", "workflow"]] | None:
    if payload.get("id") is not None:
        kind: Literal["job", "workflow"] = (
            "workflow" if "workflow" in str(payload.get("type") or "").lower() else "job"
        )
        return payload, kind
    if isinstance(inner := payload.get("workflow_job"), dict) and inner.get("id") is not None:
        return inner, "workflow"
    if isinstance(inner := payload.get("job"), dict) and inner.get("id") is not None:
        return inner, "job"
    return None


def _job_output_url(rec: dict[str, Any], kind: str) -> str | None:
    if isinstance(hu := rec.get("html_url"), str) and hu.startswith("http"):
        return hu
    if (jid := rec.get("id")) is None or not (base := aap_controller_ui_base()):
        return None
    b = base.rstrip("/")
    if kind == "workflow":
        return f"{b}/#/jobs/workflow/{jid}/output"
    return f"{b}/#/jobs/playbook/{jid}/output"


async def _retrieve_job(client: httpx.AsyncClient, job_id: int, kind: str) -> dict[str, Any]:
    tool = "workflow_jobs_retrieve" if kind == "workflow" else "jobs_retrieve"
    data = await _aap_mcp_call(client, tool, {"id": str(job_id)})
    return data if isinstance(data, dict) else {}


async def _job_stdout(client: httpx.AsyncClient, job_id: int) -> str:
    data = await _aap_mcp_call(client, "jobs_stdout_retrieve", {"id": str(job_id), "format": "txt"})
    if isinstance(data, str):
        return data[:3000]
    if isinstance(data, dict):
        if isinstance(content := data.get("content"), str):
            return content[:3000]
        return json.dumps(data, indent=2)[:3000]
    return ""


async def poll_job_to_completion(client: httpx.AsyncClient, job_id: int, kind: str) -> dict[str, Any]:
    deadline = time.monotonic() + aap_job_poll_timeout_sec()
    while time.monotonic() < deadline:
        rec = await _retrieve_job(client, job_id, kind)
        status = str(rec.get("status") or "").lower()
        if status in _TERMINAL:
            return rec
        await asyncio.sleep(aap_job_poll_interval_sec())
    return {"id": job_id, "status": "timeout"}


async def run_template_and_wait(
    client: httpx.AsyncClient,
    template_name: str,
    collected: dict[str, str],
) -> str:
    if not aap_mcp_configured():
        return "AAP MCP is not configured (missing AAP_MCP_BASE_URL or AAP_MCP_TOKEN)."

    resolved = await resolve_template(client, template_name)
    if resolved is None:
        return f"I could not find an Ansible template matching {template_name!r} in the controller."

    try:
        launch_payload = await launch_template(client, resolved, collected)
    except Exception as e:
        log.exception("AAP MCP launch failed")
        return f"Launch failed: {e}"

    launched = _launched_job(launch_payload)
    if not launched:
        return f"Launch returned an unexpected response: {str(launch_payload)[:400]}"

    job_rec, job_kind = launched
    job_id = int(job_rec["id"])
    kind_label = "workflow job" if job_kind == "workflow" else "job"
    lines = [
        f"Launched {kind_label} template {resolved.template_name} (run id {job_id}).",
    ]
    if url := _job_output_url(job_rec, job_kind):
        lines.append(f"Controller: {url}")

    final = await poll_job_to_completion(client, job_id, job_kind)
    status = str(final.get("status") or "unknown")
    lines.append(f"Final status: {status}.")
    return "\n".join(lines)


async def with_aap_client(base: httpx.AsyncClient, fn: Any) -> Any:
    if aap_tls_verify_enabled():
        return await fn(base)
    async with httpx.AsyncClient(limits=HTTP_CLIENT_LIMITS, follow_redirects=True, verify=False) as c:
        return await fn(c)
