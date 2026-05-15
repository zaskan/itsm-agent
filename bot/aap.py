"""AAP Controller REST: template resolve, launch, job poll."""

from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import httpx

from bot.chat import reply_ws
from bot.config import (
    HTTP_CLIENT_LIMITS,
    MAX_AAP_CANDIDATES,
    aap_api_base,
    aap_api_token,
    aap_controller_ui_base,
    aap_job_poll_interval_sec,
    aap_job_poll_timeout_sec,
    aap_tls_verify_enabled,
)

log = logging.getLogger("itsm-agent-bot")
_TERMINAL = frozenset({"successful", "failed", "error", "canceled", "cancelled"})


@dataclass
class PendingLaunchOffer:
    kind: str  # "workflow" | "job"
    template_id: int
    template_name: str
    channel_id: str


pending_launch_by_channel: dict[str, PendingLaunchOffer] = {}
aap_active_monitor_tasks: dict[str, asyncio.Task[Any]] = {}


def _auth_headers() -> dict[str, str]:
    tok = aap_api_token()
    if not tok:
        raise RuntimeError("AAP API token missing")
    return {"Authorization": f"Bearer {tok}", "Accept": "application/json"}


def _results(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, dict) and isinstance(data.get("results"), list):
        return [x for x in data["results"] if isinstance(x, dict)]
    return []


async def _api(
    client: httpx.AsyncClient, method: str, path: str, **kwargs: Any
) -> dict[str, Any]:
    base = aap_api_base()
    if not base:
        raise RuntimeError("AAP_CONTROLLER_API_URL not set")
    url = f"{base}/{path.lstrip('/')}"
    r = await client.request(method, url, headers=_auth_headers(), timeout=120.0, **kwargs)
    r.raise_for_status()
    return r.json()


async def aap_probe_api(client: httpx.AsyncClient) -> None:
    """Lightweight startup check (GET /config/)."""
    try:
        await _api(client, "GET", "config/")
    except Exception as e:
        log.info("AAP REST probe failed for %s: %s", aap_api_base(), e)


def _search_queries(name: str) -> list[str]:
    c = name.strip()
    out, seen = [], set()

    def add(s: str) -> None:
        t = s.strip()
        if len(t) >= 2 and t.lower() not in seen:
            seen.add(t.lower())
            out.append(t)

    add(c)
    if stripped := re.sub(r"^\[[^\]]+\]\s*", "", c).strip():
        if stripped.lower() != c.lower():
            add(stripped)
    return out[:2]


def _norm_label(s: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[\[\]]", "", str(s or "").lower())).strip()


def _matches_template_name(row: dict[str, Any], candidate: str) -> bool:
    name = str(row.get("name", "") or "")
    nl, c = name.lower(), candidate.strip().lower()
    if not c or not nl:
        return False
    if c in nl:
        return True
    if (c_alt := re.sub(r"^\[[^\]]+\]\s*", "", c).strip()) and len(c_alt) >= 3 and c_alt in nl:
        return True
    cn, nn = _norm_label(candidate), _norm_label(name)
    return (len(cn) >= 3 and cn in nn) or (len(nn) >= 3 and nn in cn) or (bool(cn) and cn == nn)


def _pick_match(rows: list[dict[str, Any]], candidate: str) -> dict[str, Any] | None:
    for r in rows:
        if _matches_template_name(r, candidate):
            return r
    return None


async def _list_templates(
    client: httpx.AsyncClient, kind: str, search: str
) -> list[dict[str, Any]]:
    coll = "workflow_job_templates" if kind == "workflow" else "job_templates"
    q = quote(search, safe="")
    data = await _api(client, "GET", f"{coll}/?search={q}&page_size=25")
    return _results(data)


async def resolve_template(
    client: httpx.AsyncClient, candidate: str, channel_id: str
) -> tuple[list[str], PendingLaunchOffer | None]:
    """Returns appendix lines for this candidate and optional launch offer."""
    lines: list[str] = []
    w_match: dict[str, Any] | None = None
    j_match: dict[str, Any] | None = None

    for q in _search_queries(candidate):
        if w_match is None:
            w_match = _pick_match(await _list_templates(client, "workflow", q), candidate)
        if j_match is None:
            j_match = _pick_match(await _list_templates(client, "job", q), candidate)
        if w_match or j_match:
            break

    w_rows = [w_match] if w_match else []
    j_rows = [j_match] if j_match else []

    if not w_rows and not j_rows:
        lines.append(
            f"- {candidate}: no matching job or workflow job template in AAP "
            f"(KB name may differ slightly from the controller object name)."
        )
        return lines, None

    lines.append(f"- {candidate}: found in AAP")
    for r in j_rows[:5]:
        lines.append(_summarize_row("Job template", r))
    for r in w_rows[:5]:
        lines.append(_summarize_row("Workflow job template", r))

    offer: PendingLaunchOffer | None = None
    pick = w_rows[0] if w_rows else (j_rows[0] if j_rows else None)
    if pick is not None and pick.get("id") is not None:
        offer = PendingLaunchOffer(
            kind="workflow" if w_rows else "job",
            template_id=int(pick["id"]),
            template_name=str(pick.get("name", "") or "").strip() or str(pick["id"]),
            channel_id=channel_id,
        )
    return lines, offer


def _summarize_row(kind: str, r: dict[str, Any]) -> str:
    tid, name = r.get("id", "?"), str(r.get("name", "") or "?").strip()
    desc = str(r.get("description", "") or "").strip()
    if len(desc) > 280:
        desc = desc[:279] + "…"
    line = f"  - {kind}: {name} (id={tid})"
    return f"{line} — {desc}" if desc else line


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


def _job_output_url(rec: dict[str, Any]) -> str | None:
    if isinstance(hu := rec.get("html_url"), str) and hu.startswith("http"):
        return hu
    if (jid := rec.get("id")) is None or not (base := aap_controller_ui_base()):
        return None
    b = base.rstrip("/")
    if "workflow" in str(rec.get("type") or "").lower():
        return f"{b}/#/jobs/workflow/{jid}/output"
    return f"{b}/#/jobs/playbook/{jid}/output"


def _launched_job(payload: dict[str, Any]) -> dict[str, Any] | None:
    if payload.get("id") is not None:
        return payload
    for key in ("workflow_job", "job"):
        if isinstance(inner := payload.get(key), dict) and inner.get("id") is not None:
            return inner
    return None


async def _with_aap_client(base: httpx.AsyncClient, fn: Any) -> Any:
    if aap_tls_verify_enabled():
        return await fn(base)
    async with httpx.AsyncClient(limits=HTTP_CLIENT_LIMITS, follow_redirects=True, verify=False) as c:
        return await fn(c)


async def aap_build_appendix(
    client: httpx.AsyncClient,
    candidates: list[str],
    channel_id: str,
) -> tuple[str, PendingLaunchOffer | None]:
    if not candidates:
        return "", None
    if not aap_tls_verify_enabled():
        log.warning("AAP REST TLS verification disabled; use only on trusted networks.")

    lines: list[str] = ["AAP (job / workflow job templates):"]
    chosen: PendingLaunchOffer | None = None

    async def _run(c: httpx.AsyncClient) -> tuple[str, PendingLaunchOffer | None]:
        nonlocal chosen
        for cand in candidates:
            part, offer = await resolve_template(c, cand, channel_id)
            lines.extend(part)
            if chosen is None and offer is not None:
                chosen = offer
        return "\n".join(lines), chosen

    return await _with_aap_client(client, _run)


async def _launch(client: httpx.AsyncClient, offer: PendingLaunchOffer) -> dict[str, Any]:
    coll = "workflow_job_templates" if offer.kind == "workflow" else "job_templates"
    return await _api(client, "POST", f"{coll}/{offer.template_id}/launch/", json={})


async def _get_job(client: httpx.AsyncClient, job_rec: dict[str, Any]) -> dict[str, Any]:
    jid = int(job_rec["id"])
    coll = "workflow_jobs" if "workflow" in str(job_rec.get("type") or "").lower() else "jobs"
    return await _api(client, "GET", f"{coll}/{jid}/")


async def _monitor_job_and_notify(
    ws: Any, base_http: httpx.AsyncClient, job_rec: dict[str, Any], template_name: str
) -> None:
    try:
        jid = int(job_rec["id"])
    except (TypeError, ValueError, KeyError):
        log.warning("AAP monitor: missing job id in %s", job_rec)
        return

    async def _poll(c: httpx.AsyncClient) -> None:
        deadline = time.monotonic() + aap_job_poll_timeout_sec()
        last_status = ""
        while time.monotonic() < deadline:
            try:
                rec = await _get_job(c, job_rec)
            except Exception as e:
                log.warning("AAP job poll id=%s: %s", jid, e)
                break
            last_status = str(rec.get("status") or "")
            if last_status.lower() in _TERMINAL:
                msg = f"The job {template_name} (run id={jid}) finished {last_status.capitalize()}."
                if url := _job_output_url(rec):
                    msg += f" Details: {url}"
                await reply_ws(ws, msg)
                return
            await asyncio.sleep(aap_job_poll_interval_sec())
        url = _job_output_url({"id": jid, "type": str(job_rec.get("type") or "")})
        tail = f" Run id={jid} last status was {last_status or 'unknown'} when the poll timed out."
        if url:
            tail += f" Check the controller: {url}"
        await reply_ws(ws, f"The job {template_name} is still running or pending.{tail}")

    await _with_aap_client(base_http, _poll)


async def aap_try_launch_from_offer(
    ws: Any, base_http: httpx.AsyncClient, offer: PendingLaunchOffer
) -> str:
    cid = offer.channel_id
    if (prev := aap_active_monitor_tasks.get(cid)) is not None and not prev.done():
        return "A job is already being monitored in this channel; wait for it to finish before launching another."

    async def _do(c: httpx.AsyncClient) -> str:
        try:
            raw = await _launch(c, offer)
        except httpx.HTTPStatusError as e:
            log.error("AAP launch failed offer=%s status=%s", offer, e.response.status_code)
            return f"Launch failed: HTTP {e.response.status_code} — {(e.response.text or '')[:300]}"
        job_rec = _launched_job(raw) if isinstance(raw, dict) else None
        if not job_rec:
            return f"Launch returned an unexpected response: {str(raw)[:400]}"
        try:
            jid = int(job_rec["id"])
        except (TypeError, ValueError, KeyError):
            return "Launch succeeded but the response had no job id."
        pending_launch_by_channel.pop(cid, None)
        kind_label = "workflow job" if offer.kind == "workflow" else "job"
        task = asyncio.create_task(_monitor_job_and_notify(ws, base_http, job_rec, offer.template_name))
        aap_active_monitor_tasks[cid] = task
        task.add_done_callback(lambda _t: aap_active_monitor_tasks.pop(cid, None))
        return (
            f"Launched the {kind_label} template {offer.template_name}. The run id is {jid}. "
            "I will post again when it finishes."
        )

    return await _with_aap_client(base_http, _do)
