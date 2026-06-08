"""
ITSM-aware demo-chat bot.

Connects to demo-chat over WebSocket, retrieves knowledge via itsm-app MCP ``rag_search_kb``,
summarizes with LiteLLM in thread replies, and launches AAP templates via MCP when the user
provides required information in the thread.

Environment variables: see README.md (CHAT_*, CHANNEL_*, ITSM_*, LLM_*, optional AAP_MCP_*).
"""

from bot.aap_mcp import aap_mcp_configured, extract_template_from_kb
from bot.itsm_mcp import catalog_workflow_applies, extract_catalog_from_kb, open_service_request
from bot.chat import chat_login, chat_me
from bot.knowledge import kb_rows_from_tool_payload, mcp_rag_search, rag_has_usable_results
from bot.llm import llm_assess
from bot.runner import main, run_bot

__all__ = [
    "aap_mcp_configured",
    "chat_login",
    "chat_me",
    "catalog_workflow_applies",
    "extract_catalog_from_kb",
    "extract_template_from_kb",
    "kb_rows_from_tool_payload",
    "llm_assess",
    "main",
    "mcp_rag_search",
    "rag_has_usable_results",
    "open_service_request",
    "run_bot",
]
