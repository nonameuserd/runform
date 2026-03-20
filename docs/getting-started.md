# Getting started

## Prerequisites

- **Python 3.11+**
- **[uv](https://docs.astral.sh/uv/)** (recommended) or pip

## Install

```bash
git clone https://github.com/your-org/agentic-knowledge-compiler.git
cd agentic-knowledge-compiler
uv sync
```

For development (pytest, ruff, mypy, pre-commit):

```bash
uv sync --extra dev
uv run pre-commit install
```

## Run the CLI

The CLI provides `ingest`, `compile`, `verify`, `drift`, and `watch`. List commands:

```bash
uv run akc
```

### Extension points and transparency

- **Connectors and vector stores are pluggable** via CLI flags and ingest modules. The `--connector` choice (docs, openapi, slack) and `--index-backend` (memory, sqlite, pgvector) can be extended by adding new connectors or backends.
- **Embedding defaults to offline:** the default embedder is `none`; use `--embedder hash` for deterministic, key-free indexing. **Cloud providers (OpenAI, Gemini) are opt-in only**—use them only when you explicitly set `--embedder openai` or `--embedder gemini` and provide the corresponding API key.
- **Compile** uses an offline LLM backend by default; no API keys are required for the standard ingest → compile → verify path. Examples in this guide use offline or generic options; cloud-backed options are explicitly marked as optional.

### Embedding providers

- **Offline (recommended for tests/demos):** `--embedder hash` (deterministic, no API keys) or default `none`
- **OpenAI-compatible (optional):** `--embedder openai` with `AKC_OPENAI_API_KEY` (and optional `AKC_OPENAI_BASE_URL`, `AKC_OPENAI_EMBED_MODEL`)
- **Gemini (optional):** `--embedder gemini` with `AKC_GEMINI_API_KEY` (and optional `AKC_GEMINI_BASE_URL`, `AKC_GEMINI_EMBED_MODEL`)

### Ingest local docs

```bash
uv run akc ingest \
  --tenant-id tenant-1 \
  --connector docs \
  --input ./docs \
  --no-index
```

Run a fully-offline ingest + index + query:

```bash
uv run akc ingest \
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
uv run akc ingest \
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
uv run akc ingest \
  --tenant-id tenant-1 \
  --connector slack \
  --input C12345678 \
  --no-index
```

### Compile (Plan → Retrieve → Generate → Execute → Repair)

Run the compile loop for a tenant/repo. All outputs go under `<outputs-root>/<tenant-id>/<repo-id>/` (manifest, `.akc/tests`, code memory).

```bash
uv run akc compile \
  --tenant-id my-tenant \
  --repo-id my-repo \
  --outputs-root ./out
```

Optional: `--mode quick` (default) or `--mode thorough`, `--goal "Your goal"`, `--work-root` to override the executor work directory, and sandbox controls like `--sandbox dev|strong`, `--sandbox-memory-mb`, and `--sandbox-allow-network` (default: deny). In `--sandbox strong`, AKC uses Docker by default and supports `--strong-lane-preference docker|wasm|auto` so developers can explicitly choose Docker or Rust WASM.

### Production-safe Docker usage

Use the Docker strong lane when you want the default production-oriented OS boundary in the CLI.

Recommended production invocation:

```bash
uv run akc compile \
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
uv run akc compile \
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
uv run akc compile \
  --tenant-id my-tenant \
  --repo-id my-repo \
  --outputs-root ./out \
  --policy-mode enforce \
  --opa-policy-path ./configs/policy/compile_tools.rego \
  --opa-decision-path data.akc.allow
```

```bash
# Audit-only mode (record denials without blocking execution)
uv run akc compile \
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
uv run akc compile \
  --tenant-id my-tenant \
  --repo-id my-repo \
  --outputs-root ./out \
  --policy-mode enforce \
  --opa-policy-path ./configs/policy/compile_tools.rego
```

```bash
# Prod profile: block executor.run unless repo is on an explicit allowlist
uv run akc compile \
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

| Replay mode | Model calls | Tool calls | Manifest requirements |
|---|---|---|---|
| `live` | Yes | Yes | None (optional) |
| `llm_vcr` | No (replayed) | Yes | Cached model payloads (`llm_vcr` or pass metadata with `llm_text`) |
| `full_replay` | No (replayed) | No (replayed) | Cached model + execute payloads in prior run manifest |
| `partial_replay` | Depends on selected passes | Depends on selected passes | Prior manifest recommended; pass selection from `--partial-replay-passes` or manifest |

For `partial_replay`, `--partial-replay-passes` takes precedence; if omitted, pass selection falls back to the replay manifest.

Replay examples:

```bash
# Full replay (no model/tool calls)
uv run akc compile \
  --tenant-id my-tenant \
  --repo-id my-repo \
  --outputs-root ./out \
  --replay-mode full_replay \
  --replay-manifest-path ./out/my-tenant/my-repo/.akc/run/<prior-run>.manifest.json
```

```bash
# LLM VCR replay (model output replayed, tools still run)
uv run akc compile \
  --tenant-id my-tenant \
  --repo-id my-repo \
  --outputs-root ./out \
  --replay-mode llm_vcr \
  --replay-manifest-path ./out/my-tenant/my-repo/.akc/run/<prior-run>.manifest.json
```

```bash
# Partial replay: rerun only execute, replay other passes
uv run akc compile \
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

Check emitted artifacts (tests, verifier results) for a tenant/repo:

```bash
uv run akc verify \
  --tenant-id my-tenant \
  --repo-id my-repo \
  --outputs-root ./out
```

## End-to-end run (ingest → compile → verify)

1. **Ingest** docs (offline, no index):  
   `uv run akc ingest --tenant-id my-tenant --connector docs --input ./docs --embedder hash --no-index`

2. **Compile**:  
   `uv run akc compile --tenant-id my-tenant --repo-id my-repo --outputs-root ./out`

3. **Verify**:  
   `uv run akc verify --tenant-id my-tenant --repo-id my-repo --outputs-root ./out`

Outputs and tests live under `./out/my-tenant/my-repo/` (e.g. `manifest.json`, `.akc/tests/`, `.akc/memory.sqlite`). The CLI uses an offline LLM backend by default so no API keys are required for this path.

## Project structure

- `src/akc/` — Core package (ingest, memory, compile, outputs)
- `docs/` — Architecture and research docs
- `tests/` — Unit and integration tests
- `examples/` — Example inputs and expected outputs

## Next steps

- Read [architecture.md](architecture.md) for the pipeline and components.
- Read [research.md](research.md) for the research behind the design.
- See [CONTRIBUTING.md](../CONTRIBUTING.md) to run tests and open a PR.
