"""LiteLLM structured assessment of KB excerpts."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Literal

import httpx

from bot.config import lightspeed_playbook_max_tokens, litellm_chat_completions_url

log = logging.getLogger("itsm-agent-bot")

Action = Literal["answer", "need_info", "launch_ready", "silent"]

_SYSTEM = (
    "You are an IT support assistant in an operations chat. "
    "Use ONLY the provided knowledge base excerpts and user messages. Never invent facts, steps, or template names. "
    "Respond with a single JSON object (no markdown fences) using this schema:\n"
    '{"action":"answer|need_info|launch_ready|silent","reply":"plain text for the user",'
    '"missing_fields":["field names still required"],"template_name":"exact template name from excerpts or null"}\n'
    "Rules:\n"
    "- action silent: excerpts do not apply to the user's question.\n"
    "- action answer: excerpts apply; reply summarizes applicable guidance only.\n"
    "- action need_info: excerpts describe automation but required inputs are missing; list them in missing_fields.\n"
    "- action launch_ready: all required inputs are present (including thread replies) and a template_name from excerpts can run. "
    "Use launch_ready only when automation should start now — not for informational replies.\n"
    "- When excerpts describe incident remediation ([incident.created], itsm_incident_ref, INC-*), "
    "do NOT mention ITSM catalog service requests or deployment fields (cpus, mem, app_repo). "
    "Ask for confirmation or any missing vm_name / itsm_incident_ref only.\n"
    "- When excerpts describe a different problem than the incident (e.g. Apache down vs memory exhaustion), "
    "use action silent — do not suggest unrelated automation or templates.\n"
    "- When excerpts say to open an ITSM catalog service request, do NOT ask for itsm_change_ref or itsm_service_request_ref; "
    "the bot creates those via ITSM MCP. Only list deployment/catalog field keys still missing (e.g. vm_name, cpus, mem, app_repo).\n"
    "- If Collected values already contain every required catalog field, use action launch_ready — do not ask again for values already "
    "in the user message or Collected values (e.g. vm_name mentioned in the original request).\n"
    "- template_name must appear in the excerpts or be null.\n"
    "- reply must be plain text, no markdown."
)

_KB_EXCERPTS_IRRELEVANT = re.compile(
    r"(?is)"
    r"(?:does\s+not\s+contain|do\s+not\s+contain|not\s+contain(?:ed)?(?:\s+information)?|"
    r"excerpts\s+clearly\s+do\s+not\s+apply|nothing\s+in\s+the\s+(?:provided\s+)?(?:knowledge\s+base\s+)?excerpts|"
    r"cannot\s+(?:find|determine).*from\s+the\s+excerpts|not\s+supported\s+by\s+the\s+excerpts)"
)

_KB_INCIDENT_MISMATCH = re.compile(
    r"(?is)"
    r"(?:available automation is for|automation is (?:only )?for|"
    r"excerpts?(?:\s+do)?\s+not\s+(?:apply|match|cover|address)|"
    r"does\s+not\s+(?:contain|cover|address)|"
    r"do\s+not\s+(?:contain|cover|address)|"
    r"no\s+(?:relevant|matching|applicable)\s+(?:documentation|article|excerpt)|"
    r"not\s+(?:related|relevant)\s+to\s+(?:this|the)\s+incident|"
    r"knowledge base does not)"
)


def kb_decision_inapplicable(decision: LlmDecision) -> bool:
    """True when the LLM indicates KB excerpts do not fit the user's incident."""
    if decision.action == "silent":
        return True
    if _KB_EXCERPTS_IRRELEVANT.search(decision.reply):
        return True
    return bool(_KB_INCIDENT_MISMATCH.search(decision.reply))


@dataclass
class LlmDecision:
    action: Action
    reply: str
    missing_fields: list[str]
    template_name: str | None


def _kb_context(kb_snippets: list[dict[str, Any]]) -> str:
    return "\n".join(
        f"KB excerpt {i} — title: {row.get('title', '')}\n{row.get('description', '')}\n"
        for i, row in enumerate(kb_snippets, start=1)
    )


def _build_user_content(
    user_question: str,
    kb_snippets: list[dict[str, Any]],
    *,
    thread_replies: list[str] | None = None,
    collected: dict[str, str] | None = None,
    template_hint: str | None = None,
) -> str:
    parts = [f"User message:\n{user_question}"]
    if thread_replies:
        parts.append("Thread replies from user:\n" + "\n---\n".join(thread_replies))
    if collected:
        parts.append("Collected values:\n" + json.dumps(collected, indent=2))
    if template_hint:
        parts.append(f"Template from KB excerpts: {template_hint}")
    parts.append(f"Knowledge base excerpts:\n{_kb_context(kb_snippets)}")
    return "\n\n".join(parts)


def _parse_json_object(text: str) -> dict[str, Any] | None:
    s = text.strip()
    if s.startswith("```"):
        s = re.sub(r"(?is)^```(?:json)?\s*", "", s)
        s = re.sub(r"\s*```$", "", s)
    try:
        obj = json.loads(s)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", s, re.DOTALL)
        if not m:
            return None
        try:
            obj = json.loads(m.group(0))
        except json.JSONDecodeError:
            return None
    return obj if isinstance(obj, dict) else None


def _normalize_action(raw: Any) -> Action:
    val = str(raw or "answer").strip().lower()
    if val in ("answer", "need_info", "launch_ready", "silent"):
        return val  # type: ignore[return-value]
    return "answer"


def decision_from_payload(obj: dict[str, Any]) -> LlmDecision:
    action = _normalize_action(obj.get("action"))
    reply = str(obj.get("reply") or "").strip()
    missing = obj.get("missing_fields")
    fields = [str(x).strip() for x in missing if str(x).strip()] if isinstance(missing, list) else []
    tmpl = obj.get("template_name")
    template_name = str(tmpl).strip() if isinstance(tmpl, str) and tmpl.strip() else None
    if action != "silent" and _KB_EXCERPTS_IRRELEVANT.search(reply):
        action = "silent"
    if action == "answer" and not reply:
        action = "silent"
    return LlmDecision(action=action, reply=reply, missing_fields=fields, template_name=template_name)


async def llm_assess(
    client: httpx.AsyncClient,
    llm_base: str,
    model: str,
    api_key: str | None,
    user_question: str,
    kb_snippets: list[dict[str, Any]],
    *,
    thread_replies: list[str] | None = None,
    collected: dict[str, str] | None = None,
    template_hint: str | None = None,
) -> LlmDecision | None:
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    try:
        payload: dict[str, Any] = {
            "model": model,
            "messages": [
                {"role": "system", "content": _SYSTEM},
                {
                    "role": "user",
                    "content": _build_user_content(
                        user_question,
                        kb_snippets,
                        thread_replies=thread_replies,
                        collected=collected,
                        template_hint=template_hint,
                    ),
                },
            ],
            "temperature": 0.1,
            "max_tokens": 1024,
            "response_format": {"type": "json_object"},
            "extra_body": {"chat_template_kwargs": {"enable_thinking": False}},
        }
        r = await client.post(
            litellm_chat_completions_url(llm_base),
            json=payload,
            headers=headers,
            timeout=120.0,
        )
        r.raise_for_status()
        choices = r.json().get("choices")
        if not isinstance(choices, list) or not choices:
            log.warning("LLM response missing choices")
            return None
        message = choices[0].get("message") or {}
        content = message.get("content")
        if not isinstance(content, str) or not content.strip():
            for key in ("reasoning_content", "reasoning"):
                alt = message.get(key)
                if isinstance(alt, str) and alt.strip():
                    content = alt
                    break
        if not isinstance(content, str) or not content.strip():
            log.warning("LLM response missing assistant content")
            return None
        obj = _parse_json_object(content)
        if obj is None:
            log.warning("LLM response not valid JSON: %s", content[:200])
            return None
        return decision_from_payload(obj)
    except Exception:
        log.exception("LLM assess failed")
        return None


def _strip_markdown_fences(text: str) -> str:
    s = text.strip()
    if s.startswith("```"):
        s = re.sub(r"(?is)^```(?:yaml|yml|ansible)?\s*", "", s)
        s = re.sub(r"\s*```$", "", s)
    return s.strip()


def _assistant_content(message: dict[str, Any]) -> str | None:
    content = message.get("content")
    if isinstance(content, str) and content.strip():
        return content.strip()
    for key in ("reasoning_content", "reasoning"):
        alt = message.get(key)
        if isinstance(alt, str) and alt.strip():
            return alt.strip()
    return None


async def llm_generate_playbook(
    client: httpx.AsyncClient,
    llm_base: str,
    model: str,
    api_key: str | None,
    incident_text: str,
) -> str | None:
    """Ask Lightspeed for Ansible remediation tasks only (no KB excerpts)."""
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    prompt = f"{incident_text.strip()}. Suggest a fix. Provide ONLY the ansible code"
    try:
        payload: dict[str, Any] = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.6,
            "max_tokens": lightspeed_playbook_max_tokens(),
            "extra_body": {"chat_template_kwargs": {"enable_thinking": False}},
        }
        r = await client.post(
            litellm_chat_completions_url(llm_base),
            json=payload,
            headers=headers,
            timeout=120.0,
        )
        r.raise_for_status()
        choices = r.json().get("choices")
        if not isinstance(choices, list) or not choices:
            log.warning("Lightspeed playbook response missing choices")
            return None
        content = _assistant_content(choices[0].get("message") or {})
        if not content:
            log.warning("Lightspeed playbook response missing content")
            return None
        playbook = _strip_markdown_fences(content)
        return playbook if playbook else None
    except Exception:
        log.exception("Lightspeed playbook generation failed")
        return None
