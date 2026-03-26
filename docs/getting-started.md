# Getting started

## Prerequisites

- **Python 3.11+**
- **[uv](https://docs.astral.sh/uv/)** (recommended) or pip

## Install

```bash
git clone https://github.com/nonameuserd/runform.git
cd runform
uv sync
```

The core package stays relatively small: **`jsonschema`**, **`pydantic`**, plus **`windows-curses`** on Windows (TUI). Add connectors, backends, and delivery signing when you need them:

```bash
uv sync --extra ingest-all          # docs/OpenAPI/embedding/vector/messaging extras
uv sync --extra vectorstore-pg      # Postgres + pgvector index backend
uv sync --extra delivery-providers  # JWT / Google OAuth for store APIs (`akc deliver`)
```

For development (pytest, ruff, mypy, pre-commit):

```bash
uv sync --extra dev
uv run pre-commit install
```

### Use the `akc` command

After `uv sync`, the CLI is available as `.venv/bin/akc`. Activate the project environment and run `akc` directly:

```bash
source .venv/bin/activate   # Windows: .venv\Scripts\activate
akc --help
```

If you prefer not to activate, `uv run akc …` runs the same entry point using the project venv. To put `akc` on your user `PATH` without activating (optional): from the repo root run `uv tool install .` (remove later with `uv tool uninstall akc`).

## Bootstrap a project

Create `.akc/project.json`, optional tenant/repo/`outputs_root` defaults, and (by default) a local OPA policy stub under `.akc/policy/compile_tools.rego`:

```bash
akc init
```

`akc init` records **`developer_role_profile` default `emerging`** in the project file. The global CLI fallback when no project file, env var, or flag sets the profile remains **`classic`** (see [Emerging Role Golden Path](#emerging-role-golden-path-opt-in)).

## Run the CLI

Top-level commands (run `akc --help` for the full tree):

| Area                | Commands                                                                                                                                  |
| ------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| Project             | `init`                                                                                                                                    |
| Ingestion & compile | `ingest`, `compile`, `verify`, `eval`                                                                                                     |
| Drift & living      | `drift`, `watch`, `living-recompile`, `living-webhook-serve`, `living-doctor`                                                             |
| Runtime             | `runtime` (`start`, `stop`, `status`, `events`, `reconcile`, `checkpoint`, `replay`, `autopilot`, `coordination-plan`, and related flags) |
| Control plane       | `control`, `metrics`, `policy`, `fleet`                                                                                                   |
| Delivery            | `deliver` (named-recipient sessions; see [delivery-architecture.md](delivery-architecture.md))                                            |
| Viewer              | `view` (TUI / static web / export)                                                                                                        |
| Slack helpers       | `slack list-channels`                                                                                                                     |

Quick inventory:

```bash
akc --help
```

### Reliability SLO gate (staging/prod soak)

For always-on runtime autopilot acceptance, use the reliability scoreboard gate documented in
`docs/runtime-execution.md` under **"Reliability SLO gate (autopilot scoreboards)"**. It covers:

- the scoreboard artifact path and KPI contract
- `scripts/check_reliability_slo_gate.py` usage
- `configs/slo/reliability_scoreboard_targets.json` target configuration
- suggested CI/staging/prod threshold profiles

### Extension points and transparency

- **Connectors and vector stores are pluggable** via CLI flags and ingest modules. The `--connector` choice (docs, openapi, slack, discord, telegram, whatsapp, mcp) and `--index-backend` (memory, sqlite, pgvector) can be extended by adding new connectors or backends.
- **Embedding defaults to offline:** the default embedder is `none`; use `--embedder hash` for deterministic, key-free indexing. **Cloud providers (OpenAI, Gemini) are opt-in only**—use them only when you explicitly set `--embedder openai` or `--embedder gemini` and provide the corresponding API key.
- **Compile** uses an offline LLM backend by default; no API keys are required for the standard ingest → compile → verify path. Examples in this guide use offline or generic options; cloud-backed options are explicitly marked as optional.

### Embedding providers

- **Offline (recommended for tests/demos):** `--embedder hash` (deterministic, no API keys) or default `none`
- **OpenAI-compatible (optional):** `--embedder openai` with `AKC_OPENAI_API_KEY` (and optional `AKC_OPENAI_BASE_URL`, `AKC_OPENAI_EMBED_MODEL`)
- **Gemini (optional):** `--embedder gemini` with `AKC_GEMINI_API_KEY` (and optional `AKC_GEMINI_BASE_URL`, `AKC_GEMINI_EMBED_MODEL`)

### Ingest local docs

```bash
akc ingest \
  --tenant-id tenant-1 \
  --connector docs \
  --input ./docs \
  --no-index
```

Run a fully-offline ingest + index + query:

```bash
akc ingest \
  --tenant-id tenant-1 \
  --connector docs \
  --input ./docs \
  --embedder hash \
  --index-backend memory \
  --query "getting started" \
  -k 3
```

### Ingest an OpenAPI spec

```bash
akc ingest \
  --tenant-id tenant-1 \
  --connector openapi \
  --input ./examples/openapi/petstore.json \
  --no-index
```

### Ingest Slack threads (Q&A heuristic)

Set a token (bot or user token with access to the channel):

```bash
export AKC_SLACK_TOKEN="xoxb-..."
```

Then run ingestion with `--input` set to the channel id (e.g. `C123...`):

```bash
akc ingest \
  --tenant-id tenant-1 \
  --connector slack \
  --input C12345678 \
  --no-index
```

### Ingest Discord threads (Q&A heuristic)

Set a bot token:

```bash
export AKC_DISCORD_TOKEN="..."
```

Then run ingestion with `--input` set to the channel id:

```bash
akc ingest \
  --tenant-id tenant-1 \
  --connector discord \
  --input 123456789012345678 \
  --no-index
```

Notes:

- Discord ingestion is pull/backfill via REST. If your bot cannot read message content due to Discord intent/policy, documents may have limited text.
- You can optionally pass `--discord-guild-id` to help thread listing in some environments.

### Ingest Telegram updates (Bot API update-drain)

Set a bot token:

```bash
export AKC_TELEGRAM_TOKEN="123456:ABC..."
```

Then run ingestion (Telegram bots do not provide true historical backfill; this drains new updates):

```bash
akc ingest \
  --tenant-id tenant-1 \
  --connector telegram \
  --input updates \
  --no-index
```

Notes:

- Incremental progress is tracked via a tenant-scoped Telegram offset state file (separate from the general ingest state), so repeated runs do not reprocess the same updates.
- Use `--telegram-chat-ids` to restrict ingestion to specific chats when needed.

### Ingest WhatsApp Cloud API payloads (stored webhooks)

WhatsApp Cloud API is **webhook-driven** for inbound events. AKC ingests **JSON or JSONL files** (or directories of them) that your stack has already persisted—raw webhook bodies or envelopes that include a `body` object Meta would POST.

Set **`--input`** to one path or **comma-separated** paths (files or directories). Use **`--whatsapp-state-path`** for cross-run **message id dedupe** (recommended for incremental runs). Optional filters: **`--whatsapp-phone-number-id`**, **`--whatsapp-waba-id`**. To enforce **`X-Hub-Signature-256`** on stored envelopes, pass **`--whatsapp-verify-signatures`** and **`--whatsapp-app-secret`** (or **`AKC_WHATSAPP_APP_SECRET`**).

```bash
akc ingest \
  --tenant-id tenant-1 \
  --connector whatsapp \
  --input ./captured/whatsapp \
  --whatsapp-state-path ./.akc/ingest/tenant-1/whatsapp-seen.json \
  --embedder hash \
  --index-backend memory \
  --no-index
```

Notes:

- There is no live subscription to Meta from this connector; you capture webhooks and point **`--input`** at those files.
- See `src/akc/ingest/connectors/messaging/whatsapp_cloud.py` for payload shapes, dedupe behavior, and optional signature verification.

### Compile (Plan → Retrieve → Generate → Execute → Repair)

Run the compile loop for a tenant/repo. All outputs go under `<outputs-root>/<tenant-id>/<repo-id>/` (manifest, `.akc/tests`, code memory).

```bash
akc compile \
  --tenant-id my-tenant \
  --repo-id my-repo \
  --outputs-root ./out
```

Optional: `--mode quick` (default) or `--mode thorough`, `--goal "Your goal"`, `--work-root` to override the executor work directory, and sandbox controls like `--sandbox dev|strong`, `--sandbox-memory-mb`, and `--sandbox-allow-network` (default: deny). In `--sandbox strong`, AKC uses Docker by default and supports `--strong-lane-preference docker|wasm|auto` so developers can explicitly choose Docker or Rust WASM.

#### Compile realization (intent → filesystem)

By default, compile uses **`scoped_apply`**: after a passing candidate, it may apply the strict unified-diff patch under a bounded working tree (`--apply-scope-root` or fallback to `--work-root` / outputs scope). That path is **policy-gated** (OPA must allow the `compile.patch.apply` action) and **fail-closed**: if preflight or policy denies, compile does not mutate the tree.

**Opt-in artifact-only** (`--compile-realization-mode artifact_only`) validates patches and writes outputs under `<outputs-root>/<tenant-id>/<repo-id>/` without applying patches to your source tree.

**Risk profile (what to use when)**

| Profile                           | Settings                                                                                           | When                                                                                                                                                                 |
| --------------------------------- | -------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Intent → filesystem (default)** | `scoped_apply` (omit flag) + policy/scope you trust                                                | Normal AKC flow; compile may land patches after the same tests/verifier gates as a normal run.                                                                       |
| **Conservative**                  | `--compile-realization-mode artifact_only`, enforce policy in CI as needed                         | Learning AKC, shared repos, CI audits, or any environment where working-tree writes are unacceptable.                                                                |
| **Full chain to runtime**         | Above plus promotion/runtime configuration (signed packets, `live_apply` only where policy allows) | Production-style rollout only after staging evidence; see [architecture.md](architecture.md) (compile realization) and [runtime-execution.md](runtime-execution.md). |

Example (default — scoped apply):

```bash
akc compile \
  --tenant-id my-tenant \
  --repo-id my-repo \
  --outputs-root ./out
```

Example (opt-in artifact-only — no working-tree apply):

```bash
akc compile \
  --tenant-id my-tenant \
  --repo-id my-repo \
  --outputs-root ./out \
  --compile-realization-mode artifact_only
```

Example (explicit scope + enforce policy — same realization mode, pinned tree):

The stock `configs/policy/compile_tools.rego` allowlists `llm.complete` and `executor.run` only. For `scoped_apply`, **extend** your Rego so `allowed_actions` includes `compile.patch.apply` (copy the starter file or overlay a package that unions the extra action). Then run:

```bash
akc compile \
  --tenant-id my-tenant \
  --repo-id my-repo \
  --outputs-root ./out \
  --compile-realization-mode scoped_apply \
  --apply-scope-root /absolute/path/to/your/repo \
  --policy-mode enforce \
  --opa-policy-path ./path/to/your/compile_tools_with_apply.rego
```

Without that allowlist entry, the run fails closed with an explicit policy denial (by design).

### Emerging Role Golden Path (opt-in)

AKC supports a profile-oriented UX for “constraints/architecture/debugging” workflows. The **`emerging`** profile is the documented golden path (not the global default): use it when you want deterministic defaults, profile sidecars, and tighter integration with verify, replay plans, and control-plane debugging.

**Baseline:** With no `--developer-role-profile` flag, no `AKC_DEVELOPER_ROLE_PROFILE`, and no `developer_role_profile` in a project file, commands resolve to **`classic`**. **Recommended:** Run **`akc init`** (writes `developer_role_profile: emerging` into `.akc/project.json` by default) or set the env var or project field explicitly.

#### Environment variable and optional project file

- **`AKC_DEVELOPER_ROLE_PROFILE`** — set to `emerging` or `classic`. Omit it to fall through to **`.akc/project.json`** / **`.akc/project.yaml`** (if present), then **`classic`**.
- **Precedence:** `--developer-role-profile` on the CLI **wins**, then `AKC_DEVELOPER_ROLE_PROFILE`, then an optional repo file **`.akc/project.json`** or **`.akc/project.yaml`**, then `classic`.
- **Optional repo config** (when present under the current working directory): `.akc/project.json` (or `.akc/project.yaml` if PyYAML is installed) may include `developer_role_profile`, `tenant_id`, `repo_id`, and `outputs_root`. **`project.json` wins over `project.yaml` if both exist.** Ingest, compile, verify, and **`akc runtime start`** honor **`AKC_TENANT_ID`**, **`AKC_REPO_ID`**, and **`AKC_OUTPUTS_ROOT`** where applicable (CLI > env > project file; runtime uses **`outputs_root`** for bundle discovery and state paths).

Example `.akc/project.json`:

```json
{
  "developer_role_profile": "emerging",
  "tenant_id": "my-tenant",
  "repo_id": "my-repo",
  "outputs_root": "./out"
}
```

#### One copy-paste block (env + minimal flags)

Set the profile once; pass only what the commands still require on the CLI. Scope for **ingest**, **compile**, **`akc verify`**, and **`akc runtime start`** uses the same resolution for **`AKC_TENANT_ID`**, **`AKC_REPO_ID`**, and **`AKC_OUTPUTS_ROOT`**: **CLI > environment > `.akc/project.json`**.

```bash
export AKC_DEVELOPER_ROLE_PROFILE=emerging
export AKC_TENANT_ID=my-tenant
export AKC_REPO_ID=my-repo
export AKC_OUTPUTS_ROOT=./out

akc ingest --connector docs --input ./docs --no-index

akc compile --mode quick

akc verify

akc runtime start
```

Explicit CLI flags in this block (not counting `export`): **`--connector`**, **`--input`**, **`--no-index`** on ingest; **`--mode quick`** on compile — **four** flags total for the full ingest → compile → verify → runtime chain under `emerging`, with profile and scope (including outputs root for verify and runtime) carried by environment variables or `.akc/project.json`.

#### DX success metrics (golden path)

Use these as **documentation-level** success criteria; they are not enforced by telemetry in the OSS CLI.

| Criterion                 | Target                                                                                                                                                                                                                                                                                                                  |
| ------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Time to first success** | Complete **ingest → compile → verify → runtime** under `emerging` in one sitting (local machine or CI) without debugging profile/scope wiring.                                                                                                                                                                          |
| **Flag budget**           | Prefer **env + `.akc/project.json`** so repeated `--developer-role-profile` is unnecessary; aim for **≤ 4 explicit CLI flags** for the full chain as in the block above (adjust if you add optional tools like `--embedder hash`).                                                                                      |
| **Regression signal**     | **`tests/integration/test_emerging_profile_one_command_flow.py`** exercises ingest → compile → verify → runtime with **`AKC_DEVELOPER_ROLE_PROFILE=emerging`** and no per-command `--developer-role-profile`. Use **manual dogfood** or **CI step timing** on that test as a rough duration budget for the golden path. |

What the `emerging` profile does:

- Keeps existing commands and flags valid; the **global** fallback when nothing sets the profile (CLI, env, or project file) remains **`classic`**.
- Resolves profile defaults deterministically and records decisions as artifacts.
- In compile, can bootstrap from the active scoped intent when `--intent-file` is omitted and can auto-seed a deployable empty plan.
- Emits `developer_profile_decisions` sidecars and manifest **`control_plane`** references for audit/debug; **`akc verify`** writes **`verify_developer_context.v1.json`** under `.akc/verification/` when verification runs.

### Production-safe Docker usage

Use the Docker strong lane when you want the default production-oriented OS boundary in the CLI.

Recommended production invocation:

```bash
akc compile \
  --tenant-id my-tenant \
  --repo-id my-repo \
  --outputs-root ./out \
  --sandbox strong \
  --strong-lane-preference docker \
  --policy-mode enforce \
  --opa-policy-path ./configs/policy/compile_tools_prod.rego \
  --sandbox-memory-mb 1024 \
  --docker-pids-limit 256 \
  --docker-user 65532:65532 \
  --docker-tmpfs /tmp
```

What Docker strong uses by default:

- Docker strong is the default `--sandbox strong` lane.
- Network is denied by default.
- Root filesystem is read-only by default.
- Privileges are reduced by default with non-root user `65532:65532`, `no-new-privileges`, and `cap-drop ALL`.
- Temporary writes are directed to `/tmp` via tmpfs by default.
- Memory is capped at `1024 MiB` by default and PID count at `256` by default.
- stdout/stderr capture is capped at `2048 KiB` per stream by default.

Optional Docker hardening controls:

- `--docker-user`
- `--docker-tmpfs` (repeatable)
- `--docker-seccomp-profile`
- `--docker-apparmor-profile`
- `--docker-ulimit-nofile`
- `--docker-ulimit-nproc`
- `--docker-cpus`

Important behavior:

- Docker-only hardening flags fail closed unless Docker strong can actually enforce them.
- If Docker-specific hardening matters, do not use `--strong-lane-preference auto`.
- Absolute seccomp profile paths must exist.
- AppArmor profile names are Linux-only and fail closed when AppArmor is unavailable on the host.

### Production-safe WASM usage

Use the WASM lane when you want deterministic resource controls and capability-based filesystem access instead of a general OS process boundary.

Recommended production invocation:

```bash
akc compile \
  --tenant-id my-tenant \
  --repo-id my-repo \
  --outputs-root ./out \
  --sandbox strong \
  --strong-lane-preference wasm \
  --policy-mode enforce \
  --opa-policy-path ./configs/policy/compile_tools_prod.rego \
  --sandbox-memory-mb 256 \
  --sandbox-cpu-fuel 5000000 \
  --sandbox-stdout-max-kb 256 \
  --sandbox-stderr-max-kb 256 \
  --wasm-preopen-dir /absolute/workspace \
  --wasm-allow-write-dir /absolute/workspace \
  --wasm-fs-normalize-existing-paths \
  --wasm-fs-normalization-profile strict
```

What this buys you:

- real elapsed-time WASM timeouts plus deterministic CPU-fuel/memory/stdout/stderr classification
- fail-closed preflight when the Rust WASM surface is unavailable
- fail-closed policy behavior in `--policy-mode enforce`
- no ambient host filesystem access unless directories are explicitly preopened
- policy can evaluate writable WASM mounts separately from read-only preopens

Current caveats:

- On Windows, strict/prod WASM compile runs fail closed because bounded wall-time enforcement is not supported for the WASM lane.
- `allowed_read_paths` is not supported in the WASM lane. Use explicit preopens and writable subsets instead.
- `--wasm-preopen-dir` and `--wasm-allow-write-dir` are WASM-only flags. Use them only with `--sandbox strong --strong-lane-preference wasm` or `--use-rust-exec --rust-exec-lane wasm`.

For embedded/integration usage, configure filesystem capabilities through `RustExecConfig`:

```python
from akc.compile.rust_bridge import RustExecConfig

rust_cfg = RustExecConfig(
    lane="wasm",
    memory_bytes=256 * 1024 * 1024,
    cpu_fuel=5_000_000,
    stdout_max_bytes=256 * 1024,
    stderr_max_bytes=256 * 1024,
    preopen_dirs=("/absolute/workspace",),
    allowed_write_paths=("/absolute/workspace",),
    wasm_normalize_existing_paths=True,
    wasm_normalization_strict=True,
)
```

### Policy + OPA/Rego (default-deny boundaries)

Compile now enforces default-deny tool authorization with explicit action allowlists and per-call capability tokens. You can run policy checks in:

- `--policy-mode audit_only` (log denied decisions, continue)
- `--policy-mode enforce` (block denied decisions)

Starter Rego policy file: `configs/policy/compile_tools.rego`
Stricter “prod” policy profile: `configs/policy/compile_tools_prod.rego` (blocks `executor.run` unless `repo_id` is explicitly approved).

```bash
# Enforce policy decisions (block on deny)
akc compile \
  --tenant-id my-tenant \
  --repo-id my-repo \
  --outputs-root ./out \
  --policy-mode enforce \
  --opa-policy-path ./configs/policy/compile_tools.rego \
  --opa-decision-path data.akc.allow
```

```bash
# Audit-only mode (record denials without blocking execution)
akc compile \
  --tenant-id my-tenant \
  --repo-id my-repo \
  --outputs-root ./out \
  --policy-mode audit_only \
  --opa-policy-path ./configs/policy/compile_tools.rego
```

The OPA input includes tenant/repo scope, requested action (`llm.complete` or `executor.run`), capability token fields, and action context (for example, test stage). This allows policy-as-code to enforce tenant isolation and stage boundaries without ambient authority.

For Docker strong runs, OPA also receives runtime hardening context for `executor.run`, including:

- backend label (`docker`)
- network mode / network exception
- `read_only_rootfs`
- `no_new_privileges`
- `cap_drop_all`
- user presence and non-root classification
- seccomp/AppArmor profile identifiers
- memory / PID / CPU / ulimit settings
- tmpfs mount list

#### Policy profile switching

Policy “profiles” are selected by pointing `--opa-policy-path` at a different Rego file (you can keep separate profiles per environment or tenant).

```bash
# Dev-ish profile: allow executor.run for compile test stages
akc compile \
  --tenant-id my-tenant \
  --repo-id my-repo \
  --outputs-root ./out \
  --policy-mode enforce \
  --opa-policy-path ./configs/policy/compile_tools.rego
```

```bash
# Prod profile: block executor.run unless repo is on an explicit allowlist
akc compile \
  --tenant-id my-tenant \
  --repo-id my-repo \
  --outputs-root ./out \
  --policy-mode enforce \
  --opa-policy-path ./configs/policy/compile_tools_prod.rego
```

#### Docker rollout stages

Use the rollout in this order:

1. Audit policy only.
   Run Docker strong with `--policy-mode audit_only` first to observe policy denials without blocking execution. This is the right stage for verifying that the emitted Docker hardening context matches your expectations.
2. Enforce policy in CI and release branches.
   Protected CI should run an actual Docker strong compile in `--policy-mode enforce` with the prod policy profile so regressions in Docker hardening become branch-blocking failures.
3. Enforce as the default in the production profile.
   Treat `--sandbox strong --strong-lane-preference docker --policy-mode enforce --opa-policy-path ./configs/policy/compile_tools_prod.rego` as the production baseline. Avoid `auto` fallback in this stage because Docker-specific guarantees must not silently disappear.

### Compiler spine artifacts (IR + replay manifest)

Phase A adds deterministic compiler contracts under:

- `src/akc/ir/`:
  - `schema.py` (`IRDocument`, `IRNode`, `EffectAnnotation`, stable node IDs)
  - `provenance.py` (`ProvenancePointer` for source traceability)
  - `versioning.py` (supported IR schema/format versions)
  - `diff.py` (`diff_ir` for `added`/`removed`/`changed` node IDs)
- `src/akc/run/`:
  - `manifest.py` (`RunManifest`, retrieval snapshots, pass records, stable hash)
  - `replay.py` (`decide_replay_for_pass` for replay call policy)

Replay modes currently modeled:

- `live`: call model + tools normally
- `llm_vcr`: replay model responses, still allow tool execution
- `full_replay`: no model or tool calls
- `partial_replay`: selective rerun with deterministic model replay policy

Replay decision table:

| Replay mode      | Model calls      | Tool calls                     | Manifest requirements                                                            |
| ---------------- | ---------------- | ------------------------------ | -------------------------------------------------------------------------------- |
| `live`           | Yes              | Yes                            | None (optional)                                                                  |
| `llm_vcr`        | No (replayed)    | Yes                            | Cached model payloads (`llm_vcr` or pass metadata with `llm_text`)               |
| `full_replay`    | No (replayed)    | No (replayed)                  | Cached model + execute payloads in prior run manifest                            |
| `partial_replay` | Replayed (model) | Yes (selected passes run live) | Prior manifest recommended; pass list from `--partial-replay-passes` or manifest |

For `partial_replay`, `--partial-replay-passes` takes precedence; if omitted, pass selection falls back to the replay manifest.

Replay examples:

```bash
# Full replay (no model/tool calls)
akc compile \
  --tenant-id my-tenant \
  --repo-id my-repo \
  --outputs-root ./out \
  --replay-mode full_replay \
  --replay-manifest-path ./out/my-tenant/my-repo/.akc/run/<prior-run>.manifest.json
```

```bash
# LLM VCR replay (model output replayed, tools still run)
akc compile \
  --tenant-id my-tenant \
  --repo-id my-repo \
  --outputs-root ./out \
  --replay-mode llm_vcr \
  --replay-manifest-path ./out/my-tenant/my-repo/.akc/run/<prior-run>.manifest.json
```

### Runtime operations

When compile emits `.akc/runtime/<run_id>.runtime_bundle.json`, the runtime CLI can start and inspect a tenant-scoped runtime run:

```bash
akc runtime start \
  --bundle ./out/my-tenant/my-repo/.akc/runtime/<run_id>.runtime_bundle.json \
  --mode simulate \
  --outputs-root ./out
```

Inspect the resulting runtime run:

```bash
akc runtime status \
  --runtime-run-id <runtime_run_id> \
  --outputs-root ./out \
  --tenant-id my-tenant \
  --repo-id my-repo
```

Other operator commands:

- `akc runtime stop --runtime-run-id <id> --outputs-root ./out` (optional `--tenant-id` / `--repo-id` scope hints)
- `akc runtime events --runtime-run-id <id> --outputs-root ./out` (optional scope hints; add `--follow` to stream)
- `akc runtime reconcile --runtime-run-id <id> --outputs-root ./out --dry-run` or `--apply` (optional `--watch` loop and scope hints)
- `akc runtime checkpoint --runtime-run-id <id> --outputs-root ./out` (optional scope hints)
- `akc runtime replay --runtime-run-id <id> --mode runtime_replay|reconcile_replay --outputs-root ./out` (optional scope hints)
- `akc runtime coordination-plan --bundle <path/to/runtime_bundle.json>` — print coordination schedule layers from a bundle
- `akc runtime autopilot` — always-on living recompile + reliability KPI loop (`akc runtime autopilot --help` for flags; see `docs/runtime-execution.md` and `configs/slo/`)

Runtime artifacts stay under the same tenant/repo root:

- `.akc/runtime/<run_id>.runtime_bundle.json`
- `.akc/runtime/<run_id>/<runtime_run_id>/runtime_run.json`
- `.akc/runtime/<run_id>/<runtime_run_id>/{checkpoint,events,queue_snapshot,runtime_evidence,policy_decisions}.json`

### Progressive environment gating (runtime deployment providers)

AKC is **fail-closed by default** for real infrastructure. Even if a runtime bundle requests a real deployment provider, the runtime will fall back to the in-memory provider unless the corresponding environment gate is enabled. Source of truth: `src/akc/runtime/providers/factory.py`.

| “I want…”                                             | Minimum bundle metadata (`RuntimeBundle.metadata`)                                           | Minimum env                                                             | What happens if missing                                                                                                  |
| ----------------------------------------------------- | -------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| **Local runtime in CI/tests (no infra, no mutation)** | Omit `deployment_provider` (or set an unknown `kind`)                                        | _(none)_                                                                | In-memory provider is used.                                                                                              |
| **Observe Docker Compose (read-only)**                | `deployment_provider.kind: "docker_compose_observe"`                                         | `AKC_ENABLE_EXTERNAL_DEPLOYMENT_PROVIDER=1`                             | Falls back to in-memory provider.                                                                                        |
| **Observe Kubernetes (read-only)**                    | `deployment_provider.kind: "kubernetes_observe"`                                             | `AKC_ENABLE_EXTERNAL_DEPLOYMENT_PROVIDER=1`                             | Falls back to in-memory provider.                                                                                        |
| **Apply Docker Compose (mutating)**                   | `deployment_provider.kind: "docker_compose_apply"`                                           | `AKC_ENABLE_MUTATING_DEPLOYMENT_PROVIDER=1`                             | Falls back to in-memory provider unless bundle opts into full replacement mode.                                          |
| **Apply Kubernetes (mutating)**                       | `deployment_provider.kind: "kubernetes_apply"`                                               | `AKC_ENABLE_MUTATING_DEPLOYMENT_PROVIDER=1`                             | Falls back to in-memory provider unless bundle opts into full replacement mode.                                          |
| **Full layer replacement (explicit bundle contract)** | `layer_replacement_mode: "full"` **or** `deployment_provider_contract.mutation_mode: "full"` | observe kinds still require `AKC_ENABLE_EXTERNAL_DEPLOYMENT_PROVIDER=1` | Apply providers are allowed without `AKC_ENABLE_MUTATING...` when full replacement mode is explicitly set on the bundle. |

Copy/paste templates (drop into runtime bundle `metadata`):

```json
{
  "deployment_provider": {
    "kind": "docker_compose_observe",
    "project_dir": ".",
    "compose_files": ["docker-compose.yml"]
  }
}
```

```json
{
  "deployment_provider": {
    "kind": "kubernetes_observe",
    "kube_context": "my-context",
    "namespace": "default"
  }
}
```

```json
{
  "deployment_provider": {
    "kind": "kubernetes_apply",
    "kube_context": "my-context",
    "namespace": "default"
  },
  "deployment_provider_contract": {
    "mutation_mode": "gated"
  }
}
```

```json
{
  "deployment_provider": {
    "kind": "kubernetes_apply",
    "kube_context": "my-context",
    "namespace": "default"
  },
  "layer_replacement_mode": "full",
  "deployment_provider_contract": {
    "mutation_mode": "full"
  }
}
```

```bash
# Partial replay: rerun only execute, replay other passes
akc compile \
  --tenant-id my-tenant \
  --repo-id my-repo \
  --outputs-root ./out \
  --replay-mode partial_replay \
  --partial-replay-passes execute \
  --replay-manifest-path ./out/my-tenant/my-repo/.akc/run/<prior-run>.manifest.json
```

`--partial-replay-passes` accepts a comma-separated pass list:
`plan,retrieve,generate,execute,repair,verify`.
When omitted, pass selection is derived from the replay manifest.

#### Operator replay plan (`akc control replay plan`)

To avoid hand-assembling pass lists after an incident, emit a **machine-readable replay plan** from an existing run manifest. It uses the same mandatory-partial-replay union as `akc control manifest diff` (aligned with intent evaluation modes on the manifest, unless you override modes on the CLI).

```bash
# From a manifest path (stdout: JSON)
akc control replay plan \
  --manifest ./out/my-tenant/my-repo/.akc/run/<run_id>.manifest.json

# Or resolve by run id under an outputs root
akc control replay plan \
  --outputs-root ./out \
  --tenant-id my-tenant \
  --repo-id my-repo \
  --run-id <run_id>

# Optional: same comma-separated modes as manifest diff
akc control replay plan \
  --manifest ./out/my-tenant/my-repo/.akc/run/<run_id>.manifest.json \
  --evaluation-modes tests,manifest_check

# Write a file (stdout JSON also includes written_path when set)
akc control replay plan \
  --manifest ./out/my-tenant/my-repo/.akc/run/<run_id>.manifest.json \
  --out ./replay_plan.json
```

The document includes `intent_replay_context.effective_partial_replay_passes` and `suggested_compile.argv_template` (with a placeholder input path). **Treat it as documentation for CI or runbooks**, not as something a read-only tool should execute. Keep **tenant/repo scope** consistent with the manifest when you run `akc compile`.

`akc control playbook run` also writes a sibling **`<timestamp>.replay_plan.json`** for the **focus** run next to the playbook report under `.akc/control/playbooks/`, and records a summary pointer on the playbook report.

#### Replay troubleshooting

- **`--partial-replay-passes requires --replay-mode partial_replay`**
  - Fix: add `--replay-mode partial_replay` when using `--partial-replay-passes`.
- **Invalid pass name in `--partial-replay-passes`**
  - Fix: use only `plan,retrieve,generate,execute,repair,verify`.
- **Replay run exits early or cannot replay expected pass**
  - Cause: replay manifest is missing required cached payloads for that mode/pass.
  - Fix: provide a valid prior manifest via `--replay-manifest-path` (or rerun once in `live` mode to seed replay artifacts).
- **Replay manifest scope mismatch (tenant/repo)**
  - Cause: manifest tenant/repo does not match current `--tenant-id` / `--repo-id`.
  - Fix: use a manifest emitted for the same tenant/repo scope to preserve isolation guarantees.
- **No manifest found when path is omitted**
  - Cause: there is no prior `.akc/run/*.manifest.json` under the scoped outputs root.
  - Fix: pass `--replay-manifest-path` explicitly or run a baseline compile first.

These contracts are useful for:

- regression tests (stable IR and stable manifest hashing),
- auditing compile behavior,
- deterministic debugging/replay in CI.

### Observability + provenance artifacts

Compile also emits control-plane artifacts for tracing, provenance, and cost accounting:

- Run manifest: `.akc/run/<run_id>.manifest.json`
- Trace spans: `.akc/run/<run_id>.spans.json`
- Cost attribution: `.akc/run/<run_id>.costs.json`
- Human-readable run log: `.akc/run/<run_id>.log.txt`
- IR snapshot: `.akc/ir/<run_id>.json`
- IR diff vs previous run (if available): `.akc/ir/<run_id>.diff.json`
- Policy decisions (if emitted): `.akc/policy/<run_id>_<step_id>.decisions.json`
- Sandbox/test execution outputs: `.akc/tests/*.json` and `.akc/tests/*.txt`

All paths are tenant+repo scoped under:
`<outputs-root>/<tenant-id>/<repo-id>/`

Quick inspect example:

```bash
ls -R ./out/my-tenant/my-repo/.akc/run
```

The trace span schema is OpenTelemetry-compatible (`trace_id`, `span_id`,
`parent_span_id`, start/end unix nanos, attributes, status), and cost records
include both per-run usage (`tokens`, `tool_calls`, `wall_time_ms`) and
attribution keys (`tenant_id`, `repo_id`, `run_id`) for control-plane billing
or quota enforcement.

### Verify

Check emitted artifacts (tests, verifier results) for a tenant/repo. Scope matches ingest/compile: **CLI > env > `.akc/project.json`** for tenant, repo, and outputs root. Preflight output includes resolved scope sources and brief **governance / deployment / exec-allowlist** hints for self-serve debugging.

```bash
# Typical: same exports as ingest/compile, then:
akc verify

# Or pass scope explicitly:
akc verify --tenant-id my-tenant --repo-id my-repo --outputs-root ./out
```

## End-to-end run (ingest → compile → verify)

1. **Ingest** docs (offline, no index):  
   `akc ingest --tenant-id my-tenant --connector docs --input ./docs --embedder hash --no-index`  
   (Or set `AKC_TENANT_ID` and omit `--tenant-id`; see [Emerging Role Golden Path](#emerging-role-golden-path-opt-in) for env + minimal flags.)

2. **Compile**:  
   `akc compile --tenant-id my-tenant --repo-id my-repo --outputs-root ./out`

3. **Verify**:  
   `akc verify` (with the same `AKC_*` scope as ingest/compile), or pass `--tenant-id` / `--repo-id` / `--outputs-root` explicitly.

Outputs and tests live under `./out/my-tenant/my-repo/` (e.g. `manifest.json`, `.akc/tests/`, `.akc/memory.sqlite`). The CLI uses an offline LLM backend by default so no API keys are required for this path. For **`emerging`** profile defaults and a verify-inclusive golden path, use the copy-paste block in [Emerging Role Golden Path](#emerging-role-golden-path-opt-in).

## Project structure

- `src/akc/` — Core package: `ingest/`, `memory/`, `compile/`, `outputs/`, `runtime/`, `delivery/`, `control/`, `living/`, `coordination/`, `ir/`, `run/`, `artifacts/`, `execute/`, `viewer/`, `evals/`, …
- `rust/crates/` — Optional Rust tooling (ingest, executor); used behind feature flags from Python where enabled
- `configs/` — Sample eval suites, SLO targets, policy stubs
- `deploy/` — Reference systemd, Compose, and Kubernetes snippets for autopilot / living flows
- `scripts/` — CI helpers (policy checks, retrieval harness, reliability SLO gate, …)
- `docs/` — Architecture, contracts, and research (`architecture.md`, `artifact-contracts.md`, `coordination-semantics.md`, …)
- `tests/unit/`, `tests/integration/` — Pytest suites
- `examples/` — Fixtures and sample inputs (e.g. OpenAPI petstore)

## Next steps

- Read [architecture.md](architecture.md) for the pipeline and components.
- Read [delivery-architecture.md](delivery-architecture.md) for named-recipient `akc deliver` and how it relates to compile output.
- Read [research.md](research.md) for the research behind the design.
- See [CONTRIBUTING.md](../CONTRIBUTING.md) to run tests and open a PR.
