"""LiteLLM summarization of KB excerpts."""

from __future__ import annotations

from typing import Any

import httpx

from bot.config import NOTHING, litellm_chat_completions_url

_NON_ANSWERS = frozenset(
    {"nothing matches", "nothing match", "no matches", "no match", "n/a", "none"}
)

_SYSTEM = (
    "You are a concise IT support assistant. The user message is from an operations chat "
    "(often an incident notification). The knowledge base excerpts were retrieved for you — "
    "summarize how they apply (alert names, remediation, links to workflows). "
    "Use only information supported by the excerpts. If excerpts clearly do not apply, say so briefly "
    "in one sentence (do not invent KB content). "
    "Reply in plain text only: no markdown, no headings, no bold, no bullet lists."
)


async def llm_answer(
    client: httpx.AsyncClient,
    llm_base: str,
    model: str,
    api_key: str | None,
    user_question: str,
    kb_snippets: list[dict[str, Any]],
) -> str:
    context = "\n".join(
        f"KB excerpt {i} — title: {row.get('title', '')}\n{row.get('description', '')}\n"
        for i, row in enumerate(kb_snippets, start=1)
    )
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    r = await client.post(
        litellm_chat_completions_url(llm_base),
        json={
            "model": model,
            "messages": [
                {"role": "system", "content": _SYSTEM},
                {"role": "user", "content": f"User message:\n{user_question}\n\nKnowledge base excerpts:\n{context}"},
            ],
            "temperature": 0.2,
            "max_tokens": 1024,
        },
        headers=headers,
        timeout=120.0,
    )
    r.raise_for_status()
    choices = r.json().get("choices")
    if not isinstance(choices, list) or not choices:
        raise RuntimeError("LLM response missing choices")
    content = (choices[0].get("message") or {}).get("content")
    if not isinstance(content, str) or not content.strip():
        raise RuntimeError("LLM response missing assistant content")
    return content.strip()


def reply_is_non_answer(text: str) -> bool:
    s = text.strip().lower()
    return len(s) < 4 or s in _NON_ANSWERS


def kb_fallback_reply(rows: list[dict[str, Any]], *, max_chars: int = 4000) -> str:
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
