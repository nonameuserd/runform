# AKC CLI commands

This document lists the `akc` command-line interface as implemented in `src/akc/cli/`. Run `akc --version` for the installed package version.

**Global:** `akc` requires a subcommand. Root parser program name is `akc` with description “Agentic Knowledge Compiler”.

**Optional feature gate**

- **`action` subtree** — Only registered when the environment variable `AKC_ACTION_PLANE=1` is set (intent parse / plan / execute for the action plane).

**Optional install extras**

- **`akc mcp serve`** — Requires the `mcp-serve` extra (`uv sync --extra mcp-serve`). Ingest with the MCP connector may require the `ingest-mcp` extra (see `akc ingest --help`).

**Top-level commands** (see sections below for subcommands):

`init`, `ingest`, `mcp`, `slack`, `drift`, `watch`, `living-recompile`, `living-webhook-serve`, `living-doctor`, `verify`, `compile`, `eval`, `runtime`, `metrics`, `policy`, `control`, **`control-bot`**, `deliver`, `fleet`, `view`, and — when `AKC_ACTION_PLANE=1` — `action`.

---

## `akc init`

Create `.akc/project.json` with repo-scoped defaults (tenant, repo, outputs root, developer role profile) and, by default, a local OPA policy stub under `.akc/policy/`. With `--detect`, also emits `.akc/project_profile.json` from repository analysis. Optional `--adoption-level` can record the progressive-takeover ladder hint in the project file.

Same **Observer** step as [getting-started.md](getting-started.md): `akc init --detect`.

**Progressive takeover (`adoption_level` in `.akc/project.json`)** — informational hint set via `akc init --adoption-level …` (or edited in the project file). Accepted tokens:

| Level | Tokens (examples) | Typical CLI surface |
| ----- | ----------------- | ------------------- |
| 0 — Observer | `observer`, `0`, `read_only` | `init --detect`, `ingest --connector codebase`, `view` |
| 1 — Advisor | `advisor`, `1`, `artifact_only` | `compile --artifact-only` / `--compile-realization-mode artifact_only` |
| 2 — Copilot | `copilot`, `2`, `scoped_apply` | `compile --compile-realization-mode scoped_apply`, `--apply-scope-root`, OPA allow `compile.patch.apply` |
| 3 — Compiler | `compiler`, `3` | `compile` + `runtime` (`start`, `coordination-plan`, living recompile, SLO-gated `runtime autopilot`; see [getting-started.md](getting-started.md)) |
| 4 — Autonomy | `autonomy`, `4`, `full_autonomy` | `runtime autopilot`, fleet automation; policy-only boundaries |

**`--developer-role-profile`** (stored in project.json): `classic` | `emerging` (init default when writing the file: `emerging`; global CLI fallback without a project file can differ — see getting started).

---

## `akc ingest`

Ingest sources into a pluggable vector index (connectors: `docs`, `codebase`, `openapi`, `slack`, `discord`, `telegram`, `whatsapp`, `mcp`). Supports embedders, index backends (`memory`, `sqlite`, `pgvector`), chunking, connector-specific options, assertion index merge, and post-ingest `--query`.

Typical **codebase** indexing (offline / deterministic), as in getting started: `--tenant-id … --connector codebase --input . --embedder hash --index-backend sqlite` (see [getting-started.md](getting-started.md)).

---

## `akc mcp`

Run AKC as an MCP server exposing read-only tools (install extra required).

| Subcommand | Purpose |
|------------|---------|
| `serve` | Start the MCP server (stdio by default, or streamable HTTP / SSE) with optional index backing for queries. |

---

## `akc slack`

Slack-related utilities.

| Subcommand | Purpose |
|------------|---------|
| `list-channels` | List Slack channels (token via flag or `AKC_SLACK_TOKEN`). |

---

## `akc drift`

Detect drift between sources and emitted outputs under an outputs root (`<tenant>/<repo>/manifest.json`), using optional ingest state and living baseline paths. Can `--update-baseline` or emit `text` / `json`.

---

## `akc watch`

Poll an ingest state file and run drift checks with debouncing; optional `--exit-on-drift`.

---

## `akc living-recompile`

When sources drift, run a **safe recompile** of impacted outputs (living loop): policy mode, canary/acceptance budgets, LLM mode (offline or custom backend), OPA paths, and optional baseline update after acceptance.

---

## `akc living-webhook-serve`

HTTP webhook receiver (signed) for fleet `recompile_triggers` / `living_drift` payloads that triggers one-shot living recompile. Bind address, secret, allowlists, and compile-related flags mirror the living recompile surface.

---

## `akc living-doctor`

Validate unattended living wiring: automation profile, paths, eval suite hooks, lease backend expectations (filesystem vs Kubernetes), and related claims.

---

## `akc verify`

Verify emitted artifacts for a tenant/repo: tests, verifier results, optional formal tools (`--dafny`, `--verus`), operational coupling / replay attestation, and optional `--living-unattended` checks (same family as `living-doctor`).

---

## `akc compile`

Run the **compile loop** (plan → retrieve → generate → execute → repair) for a tenant/repo; emit manifest and test artifacts. Default LLM backend is offline. Covers intent files, scoped apply vs artifact-only modes, sandboxing, replay modes, OPA policy, MCP hooks at compile time, promotion modes, and extensive tuning flags.

### Realization (working tree)

| `--compile-realization-mode` | Meaning |
| ---------------------------- | ------- |
| `scoped_apply` | Default. Policy-gated patch apply under `--apply-scope-root` (or `--work-root` / outputs scope when that root is omitted). |
| `artifact_only` | No working-tree writes; patches and related outputs are artifacts only. |

`--artifact-only` is a boolean alias for `--compile-realization-mode artifact_only`.

**Scoped apply extras:** `--rollback-snapshots` / `--no-rollback-snapshots`, `--git-branch-per-run`, `--git-commit`, `--git-commit-message`.

### Cost / coverage preset

| `--mode` | Meaning |
| -------- | ------- |
| `quick` | Lower-cost compile preset (default). |
| `thorough` | Higher-coverage compile preset. |

### Test gate

| `--test-mode` | Meaning |
| ------------- | ------- |
| `smoke` | Shorter validation gate (uses controller test command, default pytest-style, unless overridden). |
| `full` | Full validation gate. |
| `native_smoke` | Derive lint/typecheck/build/test commands from the detected project toolchain; smoke-style scheduling. |
| `native_full` | Same toolchain derivation; full-style scheduling. |

When `--test-mode` is omitted, it defaults from `--mode` (`quick` → smoke, `thorough` → full) unless `adoption_level` in the project requests native validation. `--native-test-mode` forces toolchain-resolved commands while keeping `smoke` vs `full` scheduling when paired with `--test-mode smoke|full`.

### Promotion state machine

| `--promotion-mode` | Meaning |
| ------------------ | ------- |
| `artifact_only` | Promotion machine: artifacts only (default in dev when unset). |
| `staged_apply` | Staged promotion path (default in non-dev when unset). |
| `live_apply` | Live promotion path. |

Related: `--require-deployable-steps` / `--no-require-deployable-steps` (empty deployable plan fail-closed defaults tie to staged/live).

### Replay policy

| `--replay-mode` | Meaning |
| --------------- | ------- |
| `live` | Default: call model and tools live. |
| `llm_vcr` | Replay recorded model I/O. |
| `full_replay` | Replay model + tools. |
| `partial_replay` | Replay model; execute tools live. |

### Tool authorization policy (OPA)

| `--policy-mode` | Meaning |
| --------------- | ------- |
| `enforce` | Default. Denied tool actions block the run. |
| `audit_only` | Denied actions are logged; run may continue. |

Rego path: `--opa-policy-path` (or env / `.akc/project.json`).

### Progressive takeover — same flags as [getting-started.md](getting-started.md)

| Ladder step | Key flags |
| ----------- | --------- |
| **Advisor** | `--artifact-only` (or `--compile-realization-mode artifact_only`), plus `--tenant-id`, `--repo-id`, `--outputs-root`, … |
| **Copilot** | `--compile-realization-mode scoped_apply`, `--apply-scope-root`, `--policy-mode enforce`, `--opa-policy-path` (e.g. `./.akc/policy/compile_tools.rego`; allow `compile.patch.apply` in Rego when applying patches). |

---

## `akc eval`

Run a versioned eval suite (intent→system tasks) with deterministic checks, selective judge scoring, and regression gates against an optional baseline report.

---

## `akc runtime`

Operate the **runtime control plane** from runtime bundles and persisted run state.

| Subcommand | Purpose |
|------------|---------|
| `start` | Start a runtime run from a `runtime_bundle.json` (modes, strict intent authority, coordination overrides, delivery lane). |
| `coordination-plan` | Print deterministic coordination schedule layers from a bundle (read-only). |
| `stop` | Request stop for a runtime run. |
| `status` | Show runtime run status. |
| `events` | Show runtime event transcript (`--follow` supported). |
| `reconcile` | Run reconcile for an existing run (`--dry-run` or `--apply`; optional `--watch` loop). |
| `checkpoint` | Show runtime checkpoint. |
| `replay` | Replay runtime evidence (`runtime_replay` or `reconcile_replay` mode). |
| `autopilot` | Long-running controller: living recompile + reliability KPIs, leases, budgets, optional SLO gate before rollouts. |

### `runtime start`

| Flag | Choices / notes |
| ---- | ---------------- |
| `--mode` | `simulate` — dry/simulated reconcile; `enforce` — normal enforcement; `canary` — canary rollout posture. |
| `--coordination-parallel-dispatch` | `inherit` (default) — use bundle contract; `enabled` / `disabled` — override. |
| `--coordination-max-in-flight-steps` | Integer; cap parallel steps when parallel dispatch is enabled. |
| `--coordination-max-in-flight-per-role` | Integer; per-role cap when parallel dispatch is enabled. |
| `--delivery-target-lane` | `staging` \| `production` — maps health timestamps to delivery lifecycle fields (default: env `AKC_DELIVERY_TARGET_LANE` or staging). |
| `--developer-role-profile` | `classic` \| `emerging` (default: env, then project file, then classic). |
| `--format` | `text` \| `json` — diagnostics / structured policy denial on policy-related failures. |
| `--strict-intent-authority` | Require intent store + projection match before running (see `--help`). |

### `runtime reconcile`

Exactly one of **`--dry-run`** (simulate) or **`--apply`** (mutate) is required.

| Flag | Meaning |
| ---- | ------- |
| `--watch` | Bounded multi-iteration reconcile loop (level-triggered resync). |
| `--watch-interval-sec` | Sleep between iterations (default `5`). |
| `--watch-max-iterations` | Max iterations (default `30`). |
| `--coordination-parallel-dispatch` | Same choices as `start`: `inherit` \| `enabled` \| `disabled`. |
| `--strict-intent-authority` | Same as `start`. |

### `runtime replay`

| `--mode` | Meaning |
| -------- | ------- |
| `runtime_replay` | Replay using runtime evidence path for the run. |
| `reconcile_replay` | Replay reconcile-focused evidence. |

### `runtime autopilot`

| Flag | Choices / notes |
| ---- | ---------------- |
| `--policy-mode` | `enforce` (default) \| `audit_only` — passed through to safe recompile. |
| `--canary-mode` | `quick` (default) \| `thorough` — canary eval depth. |
| `--accept-mode` | `quick` \| `thorough` (default) — acceptance eval depth. |
| `--lease-backend` | `filesystem` (default) \| `k8s` — controller leadership lease. |
| `--env-profile` | `dev` \| `staging` (default) \| `prod` — safety / drift pacing profile; with `--unattended-defaults`, drives autonomy budget matrix. |
| `--living-automation-profile` | `off` \| `living_loop_v1` \| `living_loop_unattended_v1` (or env / `.akc/project.json`). |
| `--unattended-defaults` | Use env-profile matrix for autonomy budgets and drift interval (**requires** `living_loop_unattended_v1`). |

**Reliability SLO gate** (block rollouts until KPIs pass): `--slo-gate`, plus tunables `--slo-min-rollouts`, `--slo-min-policy-compliance-rate`, `--slo-min-rollback-success-rate`, `--slo-max-change-instability-proxy`. When autonomy-level adoption is configured, defaults may align with gated rollouts — see [getting-started.md](getting-started.md) and `akc runtime autopilot --help`.

**Autonomy budgets** (required unless `--unattended-defaults`): `--max-mutations-per-day`, `--max-concurrent-rollouts`, `--rollback-budget-per-day`, `--max-consecutive-rollout-failures`, `--max-rollbacks-per-day-before-escalation`, `--cooldown-after-failure-ms`, `--cooldown-after-policy-deny-ms`.

---

## `akc metrics`

Query control-plane cost metrics from `<outputs_root>/<tenant>/.akc/control/metrics.sqlite`.

---

## `akc policy`

Policy governance metadata and explainability (read-only).

| Subcommand | Purpose |
|------------|---------|
| `explain` | Show policy provenance and recorded decisions for a run (via manifest path or run id + outputs layout); optional audit append. |

---

## `akc control`

Operator **control-plane indexes** under `<outputs_root>/<tenant>/.akc/control/` (for example `operations.sqlite`).

### `akc control runs`

| Subcommand | Purpose |
|------------|---------|
| `list` | List indexed runs with optional filters (time range, intent hash, recompile triggers, runtime evidence). |
| `show` | Show one run and indexed sidecar pointers. |
| `label` → `set` | Set one label key/value on a run row. |

### `akc control index`

| Subcommand | Purpose |
|------------|---------|
| `rebuild` | Rebuild `operations.sqlite` by scanning tenant manifest files. |

### `akc control manifest`

| Subcommand | Purpose |
|------------|---------|
| `diff` | Diff two manifests (paths or run ids): intent hash, control-plane refs, passes, partial replay hints. |

### `akc control replay`

| Subcommand | Purpose |
|------------|---------|
| `forensics` | Summarize `replay_decisions.json` (pass triggers, input snapshots). |
| `plan` | Effective partial-replay pass set and suggested `akc compile` flags (JSON; no execution). |

### `akc control incident`

| Subcommand | Purpose |
|------------|---------|
| `export` | Slim incident bundle: manifest, replay decisions, costs, runtime evidence, knowledge snapshot. |

### `akc control forensics`

| Subcommand | Purpose |
|------------|---------|
| `export` | Cross-signal forensics bundle (replay, coordination, OTel, knowledge, operations index). |

### `akc control playbook`

| Subcommand | Purpose |
|------------|---------|
| `run` | Read-only playbook across two run ids: manifest diff, replay forensics, incident export; writes report under `.akc/control/playbooks/`; optional webhooks / fleet notify. |

### `akc control policy-bundle`

Tenant/repo `policy_bundle.json` lifecycle metadata (not Rego execution here).

| Subcommand | Purpose |
|------------|---------|
| `validate` | Validate against frozen JSON schema. |
| `show` | Print bundle JSON, fingerprint, validation status. |
| `effective-profile` | Resolve effective governance profile from the bundle. |
| `write` | Write a validated bundle from a JSON file; updates operations index and control audit (unless `--no-audit`). |

---

## `akc control-bot`

Dedicated multi-channel operator gateway (**top-level** command; not under `akc control`). Standalone HTTP service, not fleet HTTP.

| Subcommand | Purpose |
|------------|---------|
| `validate-config` | Validate control-bot config (schema + typed checks). |
| `serve` | Run the control-bot gateway HTTP service. |

---

## `akc deliver`

Named-recipient **delivery sessions**: capture a plain-language request, authoritative recipient list, and platform targets under `.akc/delivery/<id>/`. Packaging and release lanes consume compile-time `delivery_plan` / runtime outputs.

**Default action (no subcommand):** `submit` — create session from `--request`, `--recipient` / `--recipients-file`, `--platforms`, `--release-mode`, optional `--compile` to run `akc compile` and bind outputs.

| Subcommand | Purpose |
|------------|---------|
| `status` | Show delivery request + session JSON for a `delivery_id`. |
| `events` | List delivery control-plane events. |
| `resend` | Record a resend request for one recipient. |
| `promote` | Request promotion to a release lane (`beta` or `store`). |
| `gate-pass` | Record human readiness gate (e.g. before store when `release_mode` is `both`). |
| `activation-report` | Ingest client activation JSON (first run / heartbeat). |
| `web-invite-open` | Record a signed web invite open for beta proof. |

---

## `akc fleet`

Fleet control plane: aggregate operations indexes across many `outputs_root` trees — read-only HTTP API, optional static operator dashboard, webhook helpers, automation, and cross-shard policy bundle operations.

| Subcommand | Purpose |
|------------|---------|
| `serve` | Run read-only HTTP query API (stdlib server). |
| `dashboard-serve` | Serve static read-only operator dashboard. |
| `runs` → `list` | List runs merged across shards (JSON to stdout). |
| `runs` → `show` | Show one run across shards (first match). |
| `webhooks-deliver` | POST paged webhooks for `recompile_triggers` / `living_drift` signals. |
| `automation-run` | Bounded cross-shard automation coordinator (control-plane only). |
| `policy-bundle` → `distribute` | Distribute a validated policy bundle revision to eligible shards; optional `--activate`. |
| `policy-bundle` → `drift` | Report shard drift for policy revisions / activation markers. |
| `serve-smoke` | Hidden smoke helper for tests (`argparse.SUPPRESS`). |

---

## `akc view`

Read-only local viewer over plan state and emitted artifacts.

| Subcommand | Purpose |
|------------|---------|
| `tui` | Interactive terminal UI (curses). |
| `web` | Generate a static HTML viewer bundle; optional local `--serve` on loopback. |
| `export` | Export a portable evidence bundle (directory + zip by default). |

---

## `akc action` (requires `AKC_ACTION_PLANE=1`)

| Subcommand | Purpose |
|------------|---------|
| `submit` | Submit a natural-language action request (tenant/repo/channel); optional `--dry-run` or `--simulate`. |
| `status` | Get action intent status by `--intent-id`. |
| `approve` | Approve one pending action step. |
| `replay` | Replay action intent execution (`simulate` or `live`). |
| `dispatch-channel` | Dispatch a channel adapter payload file into the submit flow. |

---

## Source of truth

Command names and help strings are defined in:

- `src/akc/cli/__init__.py` — main parser and most subcommands
- `src/akc/cli/init.py` — `init`
- `src/akc/cli/mcp_serve.py` — `mcp`
- `src/akc/cli/deliver.py` — `deliver`
- `src/akc/cli/fleet.py` — `fleet`

When in doubt, run `akc <command> --help` for the exact flags and defaults for your installed version.
