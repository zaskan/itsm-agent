"""Environment, constants, and AAP MCP configuration."""

from __future__ import annotations

import os

import httpx

PROTOCOL_VERSION = "2024-11-05"
JSON_HEADERS = {"Content-Type": "application/json", "Accept": "application/json"}
HTTP_CLIENT_LIMITS = httpx.Limits(max_keepalive_connections=5, max_connections=10)
MAX_AAP_CANDIDATES = 8


def _env(name: str, default: str | None = None) -> str:
    v = os.environ.get(name, "").strip()
    if v:
        return v
    if default is not None:
        return default
    raise RuntimeError(f"Missing required environment variable: {name}")


def _optional_env(name: str) -> str | None:
    v = os.environ.get(name, "").strip()
    return v or None


def aap_mcp_url() -> str | None:
    raw = _optional_env("AAP_MCP_BASE_URL")
    if not raw:
        return None
    base = raw.rstrip("/")
    return base if base.endswith("/mcp") or base.endswith("/mcp/") else f"{base}/mcp/"


def aap_mcp_token() -> str | None:
    return _optional_env("AAP_MCP_TOKEN")


def aap_mcp_configured() -> bool:
    return bool(aap_mcp_url() and aap_mcp_token())


def aap_job_poll_interval_sec() -> float:
    return max(2.0, float(os.environ.get("AAP_JOB_POLL_INTERVAL_SEC", "5")))


def aap_job_poll_timeout_sec() -> float:
    return max(60.0, float(os.environ.get("AAP_JOB_POLL_TIMEOUT_SEC", str(3600))))


def aap_controller_ui_base() -> str | None:
    return _optional_env("AAP_CONTROLLER_UI_URL")


def aap_tls_verify_enabled() -> bool:
    for key in ("AAP_TLS_VERIFY", "TLS_VERIFY"):
        raw = os.environ.get(key)
        if raw is not None and str(raw).strip() != "":
            return str(raw).strip().lower() not in {"0", "false", "no", "off"}
    return True


def mcp_url(itsm_base: str) -> str:
    return itsm_base.rstrip("/") + "/mcp/"


def litellm_chat_completions_url(llm_base: str) -> str:
    b = llm_base.rstrip("/")
    return f"{b}/chat/completions" if b.endswith("/v1") else f"{b}/v1/chat/completions"
