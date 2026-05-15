"""Entrypoint for `python -m itsm_agent.main` (matches common OpenShift / image CMD).

Configuration and behavior live in the ``bot`` package at the application root (LiteLLM
``/v1/chat/completions`` + itsm-app MCP ``rag_search_kb``). This module only adds
the repo root to ``sys.path`` and delegates so cluster Secrets can keep using
``CHAT_BASE_URL``, ``LLM_*``, and ``ITSM_BASE_URL`` without a separate Pydantic
Settings layer.
"""

from __future__ import annotations

import sys
from pathlib import Path


def _app_root() -> Path:
    # .../src/itsm_agent/main.py -> parents[2] == app root (contains bot/)
    return Path(__file__).resolve().parents[2]


def main() -> None:
    root = _app_root()
    rs = str(root)
    if rs not in sys.path:
        sys.path.insert(0, rs)
    import bot as _bot

    _bot.main()


if __name__ == "__main__":
    main()
