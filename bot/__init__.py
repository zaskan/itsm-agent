"""
ITSM-aware demo-chat bot (v1).

Connects to demo-chat over WebSocket, retrieves knowledge via itsm-app MCP ``rag_search_kb``,
summarizes with LiteLLM, and optionally matches / launches AAP job templates via Controller REST API v2.

Confirm launch with ``@<bot_username> yes`` after the bot offers a template.

Environment variables: see README.md (CHAT_*, CHANNEL_*, ITSM_*, LLM_*, optional AAP_*).
"""

from bot.aap import (
    PendingLaunchOffer,
    aap_active_monitor_tasks,
    aap_build_appendix,
    aap_try_launch_from_offer,
    pending_launch_by_channel,
)
from bot.chat import chat_login, chat_me
from bot.config import aap_configured
from bot.knowledge import kb_rows_from_tool_payload, mcp_rag_search_kb, mcp_rag_then_search_kb, rag_has_usable_results
from bot.llm import llm_answer
from bot.runner import main, run_bot

__all__ = [
    "PendingLaunchOffer",
    "aap_active_monitor_tasks",
    "aap_build_appendix",
    "aap_configured",
    "aap_try_launch_from_offer",
    "chat_login",
    "chat_me",
    "kb_rows_from_tool_payload",
    "llm_answer",
    "main",
    "mcp_rag_search_kb",
    "mcp_rag_then_search_kb",
    "pending_launch_by_channel",
    "rag_has_usable_results",
    "run_bot",
]
