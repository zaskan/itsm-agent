"""Generate Ansible remediation playbooks via Ansible Lightspeed API."""

from __future__ import annotations

import logging
import re
from typing import Any

import httpx

from bot.config import litellm_chat_completions_url

log = logging.getLogger("itsm-agent-bot")

_LITELLM_PLAYBOOK_SYSTEM = (
    "You write minimal, safe Ansible playbooks for Linux VM remediation. "
    "Return ONLY valid YAML for a single playbook. "
    "Prefer dedicated ansible.builtin modules (e.g. service, package, file, lineinfile, "
    "systemd, copy, template, yum, dnf, apt) over ansible.builtin.command or "
    "ansible.builtin.shell. Use command or shell only when no suitable module exists."
)

_PLAYBOOK_FENCE_RE = re.compile(r"(?is)^```(?:yaml|yml)?\s*(.*?)\s*```$")
_APACHE_INCIDENT_RE = re.compile(
    r"(?i)\b("
    r"apache\s+application\s+down|http\s+probe\s+failed|httpd\s+(?:is\s+)?down|"
    r"apacheapplicationdown|web\s+server\s+down|503\s+service\s+unavailable"
    r")\b"
)
_DIFFERENT_ISSUE_RE = re.compile(
    r"(?i)\b("
    r"different\s+issue|not\s+apache|something\s+else|other\s+issue|not\s+that|"
    r"not\s+an?\s+apache|isn['']?t\s+apache"
    r")\b"
)


def lightspeed_workflow_name() -> str:
    from bot.config import _optional_env

    return (_optional_env("AAP_LIGHTSPEED_WORKFLOW") or "Lightspeed Remediation").strip()


def lightspeed_api_url() -> str:
    from bot.config import _optional_env

    return (_optional_env("AAP_LIGHTSPEED_API_URL") or "").strip()


def lightspeed_api_token() -> str | None:
    from bot.config import _optional_env

    token = (_optional_env("AAP_LIGHTSPEED_API_TOKEN") or "").strip()
    return token or None


def lightspeed_api_tls_verify() -> bool:
    from bot.config import _optional_env

    return (_optional_env("AAP_LIGHTSPEED_TLS_VERIFY") or "true").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def lightspeed_allow_litellm_fallback() -> bool:
    from bot.config import _optional_env

    raw = (_optional_env("AAP_LIGHTSPEED_ALLOW_LITELLM_FALLBACK") or "true").strip().lower()
    return raw not in ("0", "false", "no")


def apache_troubleshoot_kb_title() -> str:
    from bot.config import _optional_env

    return (
        _optional_env("ITSM_KB_APACHE_TROUBLESHOOT_TITLE")
        or "Troubleshoot Apache application down alert"
    ).strip()


def apache_troubleshoot_job_template() -> str:
    from bot.config import _optional_env

    return (_optional_env("AAP_APACHE_TROUBLESHOOT_JT") or "Troubleshoot apache application").strip()


def apache_troubleshoot_kb_applies(kb_rows: list[dict[str, Any]]) -> bool:
    if not kb_rows:
        return False
    target = apache_troubleshoot_kb_title().lower()
    troubleshoot_jt = apache_troubleshoot_job_template().lower()
    row = kb_rows[0]
    title = str(row.get("title") or "").strip().lower()
    if title == target or target in title:
        return True
    blob = "\n".join(str(row.get(key) or "") for key in ("title", "description")).lower()
    return troubleshoot_jt in blob and "lightspeed remediation" not in blob.split("fallback", 1)[0]


def apache_troubleshoot_incident_applies(user_query: str) -> bool:
    return bool(_APACHE_INCIDENT_RE.search(user_query.strip()))


def apache_troubleshoot_path_applies(kb_rows: list[dict[str, Any]], user_query: str) -> bool:
    return apache_troubleshoot_kb_applies(kb_rows) and apache_troubleshoot_incident_applies(user_query)


def resolve_incident_template(
    kb_rows: list[dict[str, Any]],
    user_query: str,
    template_hint: str | None = None,
    *,
    thread_replies: list[str] | None = None,
) -> str:
    combined = user_query.strip()
    if thread_replies:
        combined = combined + "\n" + "\n".join(thread_replies)
    if _DIFFERENT_ISSUE_RE.search(combined):
        return lightspeed_workflow_name()
    if apache_troubleshoot_path_applies(kb_rows, user_query):
        return apache_troubleshoot_job_template()
    if template_hint and template_hint.strip().lower() == apache_troubleshoot_job_template().lower():
        return lightspeed_workflow_name()
    return lightspeed_workflow_name()


def refresh_incident_session_template(session: Any) -> None:
    session.template_candidate = resolve_incident_template(
        session.kb_rows,
        session.user_query,
        session.template_candidate,
        thread_replies=getattr(session, "thread_replies", None),
    )


def lightspeed_incident_reply(user_query: str, *, workflow_name: str | None = None) -> str:
    from bot.knowledge import parse_incident_from_body

    parsed = parse_incident_from_body(user_query)
    inc = parsed.get("itsm_incident_ref") or "the incident"
    host = parsed.get("vm_name") or "the affected host"
    wf = workflow_name or lightspeed_workflow_name()
    return (
        f"Incident {inc} on {host} has no dedicated KB runbook. "
        f"I am drafting a Lightspeed remediation playbook for review before launching **{wf}**."
    )


def format_playbook_for_chat(playbook: str, *, max_chars: int = 3500) -> str:
    body = playbook.strip()
    if len(body) > max_chars:
        body = body[:max_chars].rstrip() + "\n# ... (truncated)"
    return f"```yaml\n{body}\n```"


def lightspeed_playbook_review_reply(playbook: str, *, workflow_name: str | None = None) -> str:
    wf = workflow_name or lightspeed_workflow_name()
    return (
        "Here is the Lightspeed remediation playbook I drafted:\n\n"
        f"{format_playbook_for_chat(playbook)}\n\n"
        f"Reply **yes** or **launch** when you want me to run **{wf}** with this playbook."
    )


def lightspeed_playbook_generation_failed_reply(user_query: str) -> str:
    from bot.knowledge import parse_incident_from_body

    parsed = parse_incident_from_body(user_query)
    inc = parsed.get("itsm_incident_ref") or "the incident"
    return (
        f"I could not draft a remediation playbook for {inc} right now. "
        "Reply **yes** or **remediate** to try again."
    )


def lightspeed_launch_fields_ready(session: Any) -> bool:
    return bool(str(getattr(session, "collected", {}).get("ansible_playbook") or "").strip())


def incident_confirmation_reply(
    user_query: str,
    kb_rows: list[dict[str, Any]],
    template_hint: str | None = None,
) -> str:
    from bot.knowledge import parse_incident_from_body

    if apache_troubleshoot_path_applies(kb_rows, user_query):
        parsed = parse_incident_from_body(user_query)
        inc = parsed.get("itsm_incident_ref") or "the incident"
        host = parsed.get("vm_name") or "the affected host"
        tmpl = apache_troubleshoot_job_template()
        return (
            f"Incident {inc} indicates the Apache application is down on {host}. "
            f"Reply yes or remediate when you want me to launch {tmpl}."
        )
    return lightspeed_incident_reply(user_query, workflow_name=resolve_incident_template(kb_rows, user_query, template_hint))


def _normalize_playbook_yaml(text: str) -> str:
    s = text.strip()
    if m := _PLAYBOOK_FENCE_RE.match(s):
        s = m.group(1).strip()
    if not s.startswith("---"):
        s = "---\n" + s
    if "hosts:" not in s and "hosts " not in s:
        s = "---\n- hosts: all\n  gather_facts: false\n  tasks:\n" + s
    return s + ("\n" if not s.endswith("\n") else "")


def _build_lightspeed_prompt(
    *,
    user_query: str,
    vm_name: str | None,
    itsm_incident_ref: str | None,
) -> str:
    host = vm_name or "the target VM"
    ref = itsm_incident_ref or "INC-UNKNOWN"
    return (
        f"Incident reference: {ref}\n"
        f"Target VM hostname (AAP limit): {host}\n"
        f"Incident notification:\n{user_query.strip()}\n\n"
        "Write a minimal, safe Ansible remediation playbook for Linux that investigates "
        "and fixes the issue on the VM. Use ansible.builtin modules where possible."
    )


def _assistant_message_content(message: dict[str, Any]) -> str:
    content = message.get("content")
    if isinstance(content, str) and content.strip():
        return content.strip()
    for key in ("reasoning_content", "reasoning"):
        alt = message.get(key)
        if isinstance(alt, str) and alt.strip():
            return alt.strip()
    return ""


async def _generate_via_litellm(
    client: httpx.AsyncClient,
    llm_base: str,
    model: str,
    api_key: str | None,
    *,
    prompt: str,
    itsm_incident_ref: str | None,
) -> str:
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    payload: dict[str, Any] = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": _LITELLM_PLAYBOOK_SYSTEM,
            },
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
        "max_tokens": 2048,
        "extra_body": {"chat_template_kwargs": {"enable_thinking": False}},
    }
    r = await client.post(
        litellm_chat_completions_url(llm_base),
        json=payload,
        headers=headers,
        timeout=120.0,
    )
    r.raise_for_status()
    choices = r.json().get("choices") or []
    if not choices:
        raise RuntimeError("LiteLLM fallback returned no choices for remediation playbook")
    content = _assistant_message_content(choices[0].get("message") or {})
    if not content:
        raise RuntimeError("LiteLLM fallback returned empty remediation playbook")
    ref = itsm_incident_ref or "INC-UNKNOWN"
    playbook = _normalize_playbook_yaml(content)
    log.info("Generated remediation playbook via LiteLLM fallback for %s (%d bytes)", ref, len(playbook))
    return playbook


async def generate_remediation_playbook(
    client: httpx.AsyncClient,
    *,
    user_query: str,
    vm_name: str | None,
    itsm_incident_ref: str | None,
    llm_base: str | None = None,
    llm_model: str | None = None,
    llm_key: str | None = None,
) -> str:
    prompt = _build_lightspeed_prompt(
        user_query=user_query,
        vm_name=vm_name,
        itsm_incident_ref=itsm_incident_ref,
    )
    api_url = lightspeed_api_url()
    token = lightspeed_api_token()
    ref = itsm_incident_ref or "INC-UNKNOWN"

    if not api_url or not token:
        if lightspeed_allow_litellm_fallback() and llm_base:
            return await _generate_via_litellm(
                client,
                llm_base,
                llm_model or "llama-scout-17b",
                llm_key,
                prompt=prompt,
                itsm_incident_ref=itsm_incident_ref,
            )
        raise RuntimeError("Ansible Lightspeed API URL and token are required")

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }
    try:
        async with httpx.AsyncClient(verify=lightspeed_api_tls_verify()) as ls_client:
            r = await ls_client.post(
                api_url,
                json={"text": prompt},
                headers=headers,
                timeout=180.0,
            )
        r.raise_for_status()
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        if lightspeed_allow_litellm_fallback() and llm_base and status in (401, 404, 503):
            log.warning("Lightspeed API returned %s; using LiteLLM fallback for %s", status, ref)
            return await _generate_via_litellm(
                client,
                llm_base,
                llm_model or "llama-scout-17b",
                llm_key,
                prompt=prompt,
                itsm_incident_ref=itsm_incident_ref,
            )
        raise RuntimeError(f"Ansible Lightspeed API request failed with HTTP {status}") from exc

    data = r.json()
    if not isinstance(data, dict):
        raise RuntimeError("Ansible Lightspeed API returned non-JSON response")
    playbook_raw = data.get("playbook")
    if not isinstance(playbook_raw, str) or not playbook_raw.strip():
        raise RuntimeError("Ansible Lightspeed API returned empty playbook")
    playbook = _normalize_playbook_yaml(playbook_raw)
    log.info("Generated Ansible Lightspeed remediation playbook for %s (%d bytes)", ref, len(playbook))
    return playbook


async def ensure_lightspeed_playbook_in_collected(
    client: httpx.AsyncClient,
    collected: dict[str, str],
    *,
    user_query: str,
) -> dict[str, str]:
    from bot.config import _env, _optional_env

    out = dict(collected)
    if str(out.get("ansible_playbook") or "").strip():
        return out
    vm_name = str(out.get("vm_name") or "").strip() or None
    incident_ref = str(out.get("itsm_incident_ref") or "").strip() or None
    out["ansible_playbook"] = await generate_remediation_playbook(
        client,
        user_query=user_query,
        vm_name=vm_name,
        itsm_incident_ref=incident_ref,
        llm_base=_env("LLM_BASE_URL"),
        llm_model=_env("LLM_MODEL", "llama-scout-17b"),
        llm_key=_optional_env("LLM_API_KEY"),
    )
    if vm_name and not str(out.get("limit") or "").strip():
        out["limit"] = vm_name
    return out
