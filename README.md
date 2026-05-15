# ITSM Agent Bot

A channel bot for **demo-chat** that turns incident-style messages into knowledge-base answers and, optionally, Ansible Automation Platform (AAP) job template suggestions and launches.

The bot connects over WebSocket to a single chat channel, runs semantic search against **itsm-app** via MCP, summarizes hits with an OpenAI-compatible LLM (LiteLLM), and can resolve matching AAP job or workflow job templates via **Controller REST API v2** (template names come from KB text; no AAP MCP calls). When a template is found, it asks for confirmation; the user can launch it with a one-line `@<bot_username> yes` reply.

## How it works

1. **Subscribe** — Logs into demo-chat, joins the channel from `CHANNEL_NAME` or `CHANNEL_ID`, and listens for `message_created` events (ignores its own messages).
2. **Incident parsing** — Plain-text bodies with `Title:` / `Description:` lines are condensed into a RAG query; CamelCase tokens are split for fallback keyword search.
3. **Retrieval** — Calls itsm-app MCP `rag_search_kb`. If nothing matches, it may try MCP `search_kb` on title keywords.
4. **Summary** — Sends retrieved chunks to LiteLLM (`/v1/chat/completions`) for a short reply.
5. **AAP appendix (optional)** — For each template name inferred from KB text, looks up templates with a few Controller REST `GET` calls (`workflow_job_templates`, `job_templates`). Appends an **AAP** section and, when a template matches, asks: *Do you want me to launch the job for you?*
6. **Launch on confirm** — If the user mentions `@<bot_username>` with an affirmative reply (`yes`, `launch`, `go ahead`, etc.), the bot launches the offered template, acknowledges with the job id, polls until completion, and posts a follow-up with status and a UI link.

Health endpoints on port `8080` (default):

| Path       | Purpose |
|-----------|---------|
| `/healthz` | Liveness — always 200 |
| `/readyz`  | Readiness — 503 until WebSocket subscribe succeeds |

## Prerequisites

- **demo-chat** — REST + WebSocket API; a bot user and target channel.
- **itsm-app** — MCP at `{ITSM_BASE_URL}/mcp/` with `rag_search_kb` (and optionally `search_kb`).
- **LiteLLM** (or any OpenAI-compatible API) — For summarizing KB hits.
- **AAP Controller API** (optional) — REST at `AAP_CONTROLLER_API_URL` (e.g. `https://ansible-aap…/api/v2`). OAuth2 Bearer token with read templates, launch, and read jobs. No aap-mcp-server required for this bot.

## Configuration

Copy [`k8s/secret_template.yaml`](k8s/secret_template.yaml) to `k8s/secret.yaml` (gitignored), fill in values, and apply. The deployment loads all keys from Secret `itsm-agent-secrets`.

### Required

| Variable | Description |
|----------|-------------|
| `CHAT_BASE_URL` | demo-chat origin, e.g. `http://demo-chat.demo-chat.svc.cluster.local:8000` |
| `CHAT_USERNAME` / `CHAT_PASSWORD` | Bot credentials |
| `CHANNEL_NAME` **or** `CHANNEL_ID` | Channel to join |
| `ITSM_BASE_URL` | itsm-app origin (MCP at `{ITSM_BASE_URL}/mcp/`) |
| `LLM_BASE_URL` | LiteLLM base, with or without `/v1` |
| `LLM_MODEL` | Model id, e.g. `llama-scout-17b` |
| `LLM_API_KEY` | Bearer token for LiteLLM |

### Optional

| Variable | Description |
|----------|-------------|
| `ITSM_MCP_TOKEN` | `X-ITSM-MCP-Token` or Bearer for protected itsm MCP |
| `RAG_TOP_K` | Max KB hits (default `5`) |
| `HEALTH_PORT` | Health server port (default `8080`) |
| `LOG_LEVEL` | Logging level (default `INFO`) |
| `AAP_CONTROLLER_API_URL` | Full Controller REST API base (**no auto-suffix** for AAP 2.6+: e.g. `https://…/api/controller/v2`). Only a bare `https://host` adds legacy `/api/v2`. |
| `AAP_API_TOKEN` | Bearer token for AAP REST (falls back to `AAP_MCP_TOKEN` if unset) |
| `AAP_CONTROLLER_UI_URL` | Controller UI origin for job links when API omits `html_url` |
| `AAP_JOB_POLL_INTERVAL_SEC` / `AAP_JOB_POLL_TIMEOUT_SEC` | Poll tuning (defaults `5` / `3600`) |
| `AAP_TLS_VERIFY` | Set `false` to skip TLS verify for AAP REST only (lab/self-signed) |

See the module docstring in [`bot/__init__.py`](bot/__init__.py) and README for env vars and launch-tool defaults.

## Local run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

export CHAT_BASE_URL=...
export CHAT_USERNAME=...
export CHAT_PASSWORD=...
export CHANNEL_NAME=...
export ITSM_BASE_URL=...
export LLM_BASE_URL=...
export LLM_MODEL=...
export LLM_API_KEY=...
# optional AAP_* ...

PYTHONPATH=src:. python -m itsm_agent.main
# or: PYTHONPATH=src:. python -m bot.runner
```

## Deploy on OpenShift

The manifests assume namespace **`itsm-agent`** and an image built into that namespace’s ImageStream.

### 1. Namespace and secrets

```bash
oc apply -f k8s/namespace.yaml
cp k8s/secret_template.yaml k8s/secret.yaml
# Edit k8s/secret.yaml — replace placeholders; do not commit secret.yaml
oc apply -f k8s/secret.yaml
```

### 2. Build and push image

From the repo root (where `Dockerfile` lives):

```bash
oc project itsm-agent
oc new-build --name=itsm-agent --binary --strategy=docker -n itsm-agent \
  || true
oc start-build itsm-agent --from-dir=. --follow -n itsm-agent
```

Adjust `image:` in [`k8s/deployment.yaml`](k8s/deployment.yaml) if your internal registry path differs from:

`image-registry.openshift-image-registry.svc:5000/itsm-agent/itsm-agent:latest`

### 3. Deploy

```bash
oc apply -f k8s/deployment.yaml
oc rollout status deployment/itsm-agent -n itsm-agent
oc logs -f deployment/itsm-agent -n itsm-agent
```

After a code change, run `oc start-build` again; the deployment uses `imagePullPolicy: Always` so nodes pull the new `:latest` digest.

### 4. Verify

- Readiness: `oc exec deploy/itsm-agent -n itsm-agent -- wget -qO- http://127.0.0.1:8080/readyz`
- Post a test incident in the channel (with `Title:` / `Description:`).
- If AAP is configured and a template matches, confirm with `@<your_bot_username> yes` and check for launch ack + completion message.

## Project layout

| Path | Purpose |
|------|---------|
| [`bot/`](bot/) | Bot package: `runner` (WS loop), `knowledge` (RAG), `llm`, `aap`, `mcp`, `chat`, `config` |
| [`src/itsm_agent/main.py`](src/itsm_agent/main.py) | Container entrypoint (`python -m itsm_agent.main`) |
| [`Dockerfile`](Dockerfile) | Python 3.12 image |
| [`k8s/`](k8s/) | Namespace, Deployment, secret template |

## AAP launch notes

- Set `AAP_CONTROLLER_API_URL` to the Controller **API** (`/api/v2`), not the browser UI or MCP gateway.
- Token needs permission to list/launch job and workflow job templates and read job status.
- Typical incident: **2–4** REST `GET`s for template lookup (not dozens of MCP handshakes).
- Pending launch offers are kept **in memory** per channel. Pod restarts clear pending state.
- Only one monitor task per channel; a second `@bot yes` while a job is monitored is rejected with a short message.

## License

See repository defaults; add a `LICENSE` file if your organization requires one.
