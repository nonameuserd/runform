# Configuration

This document describes the operator-facing and user-facing configuration surfaces that exist in the repository today.

The main distinction is:

- `.akc/project.json` or `.akc/project.yaml` stores repo-scoped defaults
- environment variables provide per-shell or secret-bearing configuration
- some surfaces use explicit JSON config files instead of environment variables

## Important default

AKC does **not** require a global model key for normal local compile, verify, runtime, or viewer workflows.

By default, AKC supports fully local paths such as:

- `akc compile --artifact-only`
- `akc ingest --embedder hash`
- `akc mcp serve --embedder hash`

Provider API keys are only required when you opt into a feature that actually uses them.

That now includes two separate families:

- embedding backends for ingest and MCP retrieval
- hosted generation backends for `compile`, `living-recompile`, `living-webhook-serve`, and `assistant`

## Resolution order

Most CLI surfaces follow this precedence:

1. explicit CLI flags
2. environment variables
3. `.akc/project.json` or `.akc/project.yaml`
4. built-in defaults

The common identity and output settings are:

- `AKC_TENANT_ID`
- `AKC_REPO_ID`
- `AKC_OUTPUTS_ROOT`

Compile and living policy paths can also resolve from:

- `AKC_OPA_POLICY_PATH`
- `AKC_OPA_DECISION_PATH`

## Project config

Repo-scoped defaults live in `.akc/project.json` or `.akc/project.yaml`. `project.json` wins if both files exist.

Current fields loaded by `src/akc/cli/project_config.py`:

| Key | Purpose |
| --- | --- |
| `developer_role_profile` | developer-role hint for repo tooling |
| `adoption_level` | progressive-adoption hint written by `akc init` |
| `tenant_id` | default tenant id |
| `repo_id` | default repo id |
| `outputs_root` | default outputs root |
| `opa_policy_path` | default OPA bundle/file path |
| `opa_decision_path` | default OPA decision path |
| `living_automation_profile` | default living automation profile |
| `ingest_state_path` | default ingest/drift state path |
| `living_unattended_claim` | unattended living claim toggle |
| `assistant_default_format` | assistant output format hint |
| `assistant_session_retention_days` | assistant retention default |
| `assistant_model_hint` | assistant model hint used by hosted assistant backend resolution |
| `llm.backend` | default hosted/offline generation backend |
| `llm.model` | default generation model id |
| `llm.base_url` | optional generation API base URL override |
| `llm.timeout_s` | generation HTTP timeout |
| `llm.max_retries` | generation retry budget for transient HTTP failures |
| `llm.allow_network` | explicit hosted-generation egress opt-in |
| `llm.backend_class` | custom backend class path |
| `memory_policy_path` | weighted-memory policy path |
| `memory_budget_tokens` | shared memory budget |
| `compile_memory_budget_tokens` | compile-specific memory budget |
| `assistant_memory_budget_tokens` | assistant-specific memory budget |
| `memory_pins` | pinned memory ids/labels |
| `memory_boosts` | salience boosts |
| `compile_skills` | default compile skills |
| `compile_skills_mode` | compile skill mode |
| `skill_roots` | extra skill roots |
| `compile_skill_max_file_bytes` | per-file skill ingestion limit |
| `compile_skill_max_total_bytes` | total skill ingestion limit |
| `mutation_paths` | allowed relative mutation prefixes for `scoped_apply` |
| `toolchain` | explicit toolchain override |
| `native_test_mode` | resolve standard test modes through native toolchain |
| `change_scope_deny_categories` | denylist for compile change categories |
| `validation.bindings_path` | operator validator registry path |
| `validation_bindings_path` | flat alias for the same validator registry path |

Minimal example:

```json
{
  "tenant_id": "demo",
  "repo_id": "runform",
  "outputs_root": "./out",
  "opa_policy_path": ".akc/policy/compile.rego",
  "opa_decision_path": "data.akc.compile.allow",
  "llm": {
    "backend": "offline",
    "model": "offline-small",
    "allow_network": false
  },
  "validation": {
    "bindings_path": "configs/validation/validator_bindings.v1.yaml"
  },
  "living_automation_profile": "living_loop_v1",
  "mutation_paths": [
    "docs/",
    "src/"
  ]
}
```

## Core operator settings

These are the most common non-secret variables:

| Variable | Used by |
| --- | --- |
| `AKC_TENANT_ID` | compile, verify, ingest, living, runtime, view |
| `AKC_REPO_ID` | compile, verify, living, runtime, view |
| `AKC_OUTPUTS_ROOT` | compile, verify, runtime, MCP, living, view |
| `AKC_OPA_POLICY_PATH` | compile and living policy evaluation |
| `AKC_OPA_DECISION_PATH` | compile and living policy evaluation |

## Feature toggles and defaults

These variables enable optional surfaces rather than providing credentials:

| Variable | Purpose |
| --- | --- |
| `AKC_WEIGHTED_MEMORY_ENABLED` | enables weighted-memory behavior by default for assistant and compile |
| `AKC_ACTION_PLANE` | exposes the optional `akc action` command tree |

## Embedders and provider keys

OpenAI and Gemini configuration is used by two different surfaces:

- embedding for ingest and MCP indexing
- generation for compile, living, and assistant when a hosted backend is selected

These are separate configuration paths.

## Hosted LLM generation

The normal compile/living/assistant default remains `offline`.

Hosted generation is opt-in:

- select a hosted backend with `--llm-backend` or `AKC_LLM_BACKEND`
- provide a model with `--llm-model` or `AKC_LLM_MODEL`
- provide credentials through CLI or environment variables
- explicitly allow egress with `--llm-allow-network` or `AKC_LLM_ALLOW_NETWORK=1`

Current generic generation settings:

| Variable | Purpose |
| --- | --- |
| `AKC_LLM_BACKEND` | `offline`, `openai`, `anthropic`, `gemini`, or `custom` |
| `AKC_LLM_MODEL` | generation model id |
| `AKC_LLM_BASE_URL` | generic base URL override |
| `AKC_LLM_API_KEY` | generic API key override |
| `AKC_LLM_TIMEOUT_S` | HTTP timeout in seconds |
| `AKC_LLM_MAX_RETRIES` | transient retry count |
| `AKC_LLM_ALLOW_NETWORK` | explicit hosted-generation egress opt-in |
| `AKC_LLM_BACKEND_CLASS` | custom backend class path |

Provider-native credential fallbacks:

| Variable | Purpose |
| --- | --- |
| `AKC_OPENAI_API_KEY` or `OPENAI_API_KEY` | OpenAI generation auth fallback |
| `AKC_OPENAI_BASE_URL` | OpenAI generation base URL fallback |
| `AKC_ANTHROPIC_API_KEY` or `ANTHROPIC_API_KEY` | Anthropic generation auth fallback |
| `AKC_ANTHROPIC_BASE_URL` | Anthropic generation base URL fallback |
| `AKC_GEMINI_API_KEY` or `GEMINI_API_KEY` | Gemini generation auth fallback |
| `AKC_GEMINI_BASE_URL` | Gemini generation base URL fallback |

Current CLI flags on compile, living, and assistant:

- `--llm-backend`
- `--llm-model`
- `--llm-base-url`
- `--llm-api-key`
- `--llm-timeout-s`
- `--llm-max-retries`
- `--llm-allow-network`
- `--llm-backend-class`

Living also keeps the legacy `--llm-mode offline|custom` alias for compatibility.

### OpenAI embedder

Used when `--embedder openai` is selected.

| Variable | Purpose |
| --- | --- |
| `AKC_OPENAI_API_KEY` | required for the OpenAI embedder |
| `AKC_OPENAI_BASE_URL` | optional API base URL override |
| `AKC_OPENAI_EMBED_MODEL` | optional embedding model override |

CLI flags:

- `--openai-api-key`
- `--openai-base-url`
- `--openai-model`

### Gemini embedder

Used when `--embedder gemini` is selected.

| Variable | Purpose |
| --- | --- |
| `AKC_GEMINI_API_KEY` | required for the Gemini embedder |
| `AKC_GEMINI_BASE_URL` | optional API base URL override |
| `AKC_GEMINI_EMBED_MODEL` | optional embedding model override |

CLI flags:

- `--gemini-api-key`
- `--gemini-base-url`
- `--gemini-model`

### Messaging ingest credentials

These are only needed for the corresponding ingest connectors:

| Variable | Connector |
| --- | --- |
| `AKC_SLACK_TOKEN` | Slack |
| `AKC_DISCORD_TOKEN` | Discord |
| `AKC_TELEGRAM_TOKEN` | Telegram |

## MCP server configuration

`akc mcp serve` is a read-only surface over AKC outputs and indexes.

Current environment variables:

| Variable | Purpose |
| --- | --- |
| `AKC_MCP_TRANSPORT` | default transport: `stdio`, `streamable-http`, or `sse` |
| `AKC_MCP_HOST` | host for HTTP transports |
| `AKC_MCP_PORT` | port for HTTP transports |
| `AKC_MCP_STREAMABLE_PATH` | path for streamable HTTP |
| `AKC_OUTPUTS_ROOT` | outputs root to serve from |
| `AKC_MCP_ALLOWED_TENANTS` | comma-separated tenant allowlist |
| `AKC_MCP_TOOL_TOKEN` | shared tool token for requests |
| `AKC_MCP_HTTP_BEARER_TOKEN` | bearer token for HTTP/SSE; disables `tool_token` requirement |
| `AKC_MCP_INDEX_BACKEND` | `memory`, `sqlite`, or `pgvector` |
| `AKC_MCP_INDEX_SQLITE` | explicit SQLite index path |
| `AKC_PG_DSN` | pgvector connection string |
| `AKC_MCP_EMBEDDER` | `hash`, `openai`, `gemini`, or `none` depending on backend |

Notes:

- non-memory index backends cannot use `embedder=none`
- OpenAI and Gemini embedder settings reuse the same `AKC_OPENAI_*` and `AKC_GEMINI_*` variables described above

## Validation and verify

Operator-side validator bindings map intent `validator_stub` ids to concrete observability or mobile validator commands.

Configuration surfaces:

| Setting | Purpose |
| --- | --- |
| `validation.bindings_path` in project config | default validator registry path |
| `validation_bindings_path` in project config | flat alias |
| `--validator-bindings` | per-command override on `akc verify` |
| `AKC_VALIDATION_APP_ID` | injected into validator subprocess environments when a binding specifies `app_id` |

Deployment-provider gating that verify reports today:

| Variable | Purpose |
| --- | --- |
| `AKC_ENABLE_EXTERNAL_DEPLOYMENT_PROVIDER` | enable external deployment-provider reads |
| `AKC_ENABLE_MUTATING_DEPLOYMENT_PROVIDER` | enable mutating deployment-provider actions |
| `AKC_EXEC_ALLOWLIST` | executor allowlist passed into some runtime/verification paths |

## Delivery and store-provider configuration

`akc deliver` uses a mix of generic delivery settings and provider-specific credentials.

### Generic delivery settings

| Variable | Purpose |
| --- | --- |
| `AKC_DELIVERY_PROVIDER_DRY_RUN` | skip outbound provider calls and synthesize success |
| `AKC_DELIVERY_EXECUTE_PROVIDERS` | master toggle for provider execution |
| `AKC_DELIVERY_RELAX_ADAPTER_PREFLIGHT` | relax fail-closed adapter prerequisite checks |
| `AKC_DELIVERY_ENFORCE_ADAPTER_PREFLIGHT` | legacy inverse toggle |
| `AKC_DELIVERY_TARGET_LANE` | default delivery lane in runtime/delivery flows |
| `AKC_DELIVERY_WEB_INVITE_BASE_URL` | public base URL for hosted web invites |
| `AKC_DELIVERY_INVITE_EMAIL_FROM` | sender address for invite email delivery |
| `AKC_DELIVERY_SMTP_URL` | SMTP URL for direct mail transport |
| `SMTP_HOST` | SMTP host fallback |
| `SMTP_USER` | SMTP auth user fallback |
| `SMTP_PASS` | SMTP auth password fallback |
| `SENDGRID_API_KEY` | SendGrid invite mail backend |
| `POSTMARK_API_TOKEN` | Postmark invite mail backend |
| `AWS_SES_REGION` | SES region hint |
| `AWS_ACCESS_KEY_ID` | SES credential path |
| `AWS_PROFILE` | SES credential path |

### Apple / TestFlight

| Variable | Purpose |
| --- | --- |
| `APP_STORE_CONNECT_API_KEY_ID` or `APP_STORE_CONNECT_KEY_ID` | App Store Connect key id |
| `APP_STORE_CONNECT_API_ISSUER_ID` or `APP_STORE_CONNECT_ISSUER_ID` | App Store Connect issuer id |
| `APP_STORE_CONNECT_PRIVATE_KEY_PATH` or `APP_STORE_CONNECT_API_KEY_PATH` | path to the private key file |
| `AKC_DELIVERY_ASC_BETA_GROUP_ID` | required for API-driven TestFlight invitations |

### Firebase App Distribution / Google Play

| Variable | Purpose |
| --- | --- |
| `GOOGLE_APPLICATION_CREDENTIALS` | Google OAuth credentials file |
| `FIREBASE_TOKEN` | Firebase CLI auth fallback |
| `AKC_DELIVERY_FIREBASE_RELEASE_NAME` | Firebase release display name |
| `AKC_DELIVERY_PLAY_PACKAGE_NAME` | Play package name |
| `AKC_DELIVERY_FIREBASE_APP_DIST_GROUPS` | Firebase tester groups |
| `FIREBASE_APP_DISTRIBUTION_GROUPS` | legacy/alternate Firebase tester groups |

Delivery sessions may also emit local prerequisite hints under `.akc/delivery/operator_prereqs.json`.

## Living recompile and webhook configuration

Living workflows resolve repo defaults from project config and use additional webhook settings.

| Variable | Purpose |
| --- | --- |
| `AKC_LIVING_AUTOMATION_PROFILE` | selects the living automation profile |
| `AKC_INGEST_STATE_PATH` | ingest/drift state path when not taken from project config |
| `AKC_AUTOPILOT_INGEST_STATE` | compatibility alias used by `living-doctor` |
| `AKC_LIVING_UNATTENDED_CLAIM` | unattended-living claim toggle |
| `AKC_LIVING_WEBHOOK_SECRET` | required shared secret for `living-webhook-serve` |
| `AKC_LIVING_WEBHOOK_TENANT_ALLOWLIST` | comma-separated tenant allowlist or `*` |
| `AKC_LIVING_WEBHOOK_OUTPUTS_ROOT_ALLOWLIST` | outputs-root allowlist for webhook-triggered recompiles |

## Runtime, telemetry, and secrets

### OTEL export mirrors

AKC writes canonical per-run NDJSON sidecars under `.akc/run/`. These optional environment variables mirror those records elsewhere:

| Variable | Purpose |
| --- | --- |
| `AKC_OTEL_EXPORT_STDOUT` | also write OTEL exports to stdout |
| `AKC_OTEL_EXPORT_HTTP_URL` | POST OTEL exports as JSON |
| `AKC_OTEL_EXPORT_FILE` | append OTEL exports to another file |
| `AKC_OTEL_EXPORT_HTTP_TIMEOUT_SEC` | HTTP timeout override |

### Tenant-scoped secret injection

The executor host-side naming convention is:

```text
AKC_SECRET_<tenant_id>_<secret_name>
```

Inside the sandboxed execution environment, the injected variable name is:

```text
AKC_SECRET_<secret_name>
```

This keeps host secret storage tenant-scoped while giving the runtime a stable in-sandbox name.

## Fleet and control-plane servers

### Fleet

`akc fleet serve` is mostly configured with an explicit JSON config file passed via `--config`.

Current environment variables are intentionally minimal:

| Variable | Purpose |
| --- | --- |
| `AKC_FLEET_CORS_ALLOW_ORIGIN` | CORS allow-origin override |

The fleet config file carries the actual auth and routing data, including API tokens, scopes, roles, and optional tenant allowlists.

### Control bot

`akc control-bot` also uses an explicit JSON config file rather than many loose environment variables.

The current schema requires these top-level sections:

- `schema`
- `server`
- `routing`
- `identity`
- `policy`
- `approval`
- `storage`

Optional channel sections live under `channels`:

- `slack.enabled` with `signing_secret`
- `discord.enabled` with `public_key`
- `telegram.enabled` with `secret_token` and optional `bot_token`
- `whatsapp.enabled` with `verify_token` and `app_secret`

Use:

```bash
akc control-bot validate-config --config /path/to/control-bot.json
```

to validate the file before serving it.

## Practical setups

### Local offline docs/demo setup

No provider credentials required:

```bash
export AKC_TENANT_ID=demo
export AKC_REPO_ID=runform
export AKC_OUTPUTS_ROOT=./out

akc ingest --connector docs --input ./docs --embedder hash --index-backend sqlite
akc compile --artifact-only
akc verify
```

### OpenAI-backed ingest setup

Only the embedder path needs the key:

```bash
export AKC_OPENAI_API_KEY=...
akc ingest --connector docs --input ./docs --embedder openai --index-backend sqlite
```

### Hosted OpenAI compile setup

Generation credentials are only needed when you leave offline mode:

```bash
export OPENAI_API_KEY=...

akc compile \
  --tenant-id demo \
  --repo-id runform \
  --outputs-root ./out \
  --artifact-only \
  --llm-backend openai \
  --llm-model gpt-4.1 \
  --llm-allow-network
```

If `--llm-backend` selects `openai`, `anthropic`, or `gemini` without explicit network opt-in, AKC fails closed before compile starts.

### Gemini-backed MCP server setup

```bash
export AKC_MCP_EMBEDDER=gemini
export AKC_GEMINI_API_KEY=...
akc mcp serve
```

## Related docs

- [Getting started](getting-started.md)
- [CLI command reference](cli-commands.md)
- [Architecture](architecture.md)
- [Validation](validation.md)
- [Delivery architecture](delivery-architecture.md)
- [Artifact contracts](artifact-contracts.md)
