# AKC CLI Reference

This page summarizes the current `akc` command tree implemented under `src/akc/cli/`.

For exact flags, run `akc <command> --help` or `uv run akc <command> --help`.

## Top-level commands

Current top-level commands:

- `init`
- `assistant`
- `ingest`
- `mcp`
- `slack`
- `drift`
- `watch`
- `living-recompile`
- `living-webhook-serve`
- `living-doctor`
- `verify`
- `compile`
- `eval`
- `runtime`
- `metrics`
- `policy`
- `control`
- `control-bot`
- `deliver`
- `fleet`
- `view`

Optional top-level command:

- `action` only when `AKC_ACTION_PLANE=1`

## Feature-gated or extra-backed surfaces

- `akc mcp serve` requires the `mcp-serve` extra
- `akc ingest --connector mcp` requires the `ingest-mcp` extra
- `akc deliver` store-provider integrations use the `delivery-providers` extra

## Command guide

### `akc init`

Creates `.akc/project.json` and, by default, a local compile policy stub under `.akc/policy/`.

Useful flags:

- `--detect` to write `.akc/project_profile.json`
- `--adoption-level` to store a progressive-adoption hint

### `akc assistant`

CLI assistant surface for planning and executing existing AKC commands.

Key behaviors:

- interactive mode with `akc assistant`
- single-turn mode with `akc assistant -p "..."`
- modes: `plan` or `execute`
- default planner is offline and local
- optional hosted planner support uses the shared LLM flags:
  - `--llm-backend`
  - `--llm-model`
  - `--llm-base-url`
  - `--llm-api-key`
  - `--llm-timeout-s`
  - `--llm-max-retries`
  - `--llm-allow-network`
  - `--llm-backend-class`
- optional weighted-memory flags:
  - `--memory-policy-path`
  - `--memory-pin`
  - `--memory-boost`
  - `--memory-budget-tokens`

### `akc ingest`

Ingests sources into a vector index.

Current connectors:

- `docs`
- `codebase`
- `openapi`
- `slack`
- `discord`
- `telegram`
- `whatsapp`
- `mcp`

Current index backends:

- `memory`
- `sqlite`
- `pgvector`

Embedders:

- `none`
- `hash`
- `openai`
- `gemini`

### `akc mcp`

Runs AKC as a read-only MCP server.

Current subcommand:

- `serve`

### `akc slack`

Slack utilities.

Current subcommand:

- `list-channels`

### `akc drift`

Detect drift between sources and emitted outputs.

### `akc watch`

Poll ingest state and run drift checks.

### `akc living-recompile`

Run safe recompiles when drift is detected.

Hosted LLM support uses the same shared flags as compile. Offline remains the default.

### `akc living-webhook-serve`

Signed webhook receiver for living recompile triggers.

### `akc living-doctor`

Validate unattended living wiring and related assumptions.

### `akc verify`

Verify emitted artifacts for a tenant and repo.

This is the post-compile validation step that pairs naturally with `akc compile`.

Useful validation flags:

- `--execute-validators` to run operator-side observability/mobile validators before operational coupling verification
- `--validator-bindings` to override the validator registry path

See [validation.md](validation.md) for the registry format and `operational_spec` examples.

### `akc compile`

Runs the compile loop:

**Plan -> Retrieve -> Generate -> Execute -> Repair**

Important defaults and flags:

- default realization mode is `scoped_apply`
- `--artifact-only` is the safe alias for `--compile-realization-mode artifact_only`
- `--mode` supports `quick` and `thorough`
- `--test-mode` supports `smoke`, `full`, `native_smoke`, `native_full`
- `--policy-mode` supports `audit_only` and `enforce`
- `--replay-mode` supports `live`, `llm_vcr`, `full_replay`, `partial_replay`

Weighted-memory flags on compile:

- `--memory-policy-path`
- `--memory-pin`
- `--memory-boost`
- `--memory-budget-tokens`

Compile-time skills and MCP integrations are also exposed here:

- `--compile-skills-mode`
- `--compile-skill`
- `--compile-skill-extra-root`
- `--compile-mcp`

Hosted/offline generation backend flags on compile:

- `--llm-backend`
- `--llm-model`
- `--llm-base-url`
- `--llm-api-key`
- `--llm-timeout-s`
- `--llm-max-retries`
- `--llm-allow-network`
- `--llm-backend-class`

Git-aware `scoped_apply` flags:

- `--apply-scope-root` sets the absolute repo/work-tree root allowed to receive patch mutations
- `--git-branch-per-run` creates `akc/compile/<patch_sha_prefix>` before apply
- `--git-commit` stages only touched paths and commits the applied patch
- `--git-commit-message` overrides the default commit message `AKC scoped_apply <sha>`
- `--rollback-snapshots` and `--no-rollback-snapshots` control file-copy snapshots under `.akc/rollback/`

Operational notes:

- Git behavior is only active for `scoped_apply`; `artifact_only` never touches the work tree
- If git flags are requested and `git` is unavailable, or `--apply-scope-root` is not a git repo, AKC fails closed instead of silently downgrading
- Patch application still uses `patch(1)` with strict preflight and mutation-path confinement; Git is optional provenance and rollback hygiene around that path
- Hosted LLM backends are opt-in; offline is still the default
- Hosted backends fail closed unless `--llm-allow-network` or `AKC_LLM_ALLOW_NETWORK=1` is set

### `akc eval`

Runs a versioned evaluation suite with deterministic checks and optional regression gating.

### `akc runtime`

Operates runtime bundles and runtime state.

Current subcommands:

- `start`
- `coordination-plan`
- `stop`
- `status`
- `events`
- `reconcile`
- `checkpoint`
- `replay`
- `autopilot`

See [runtime-execution.md](runtime-execution.md) for the runtime model.

### `akc metrics`

Reads control-plane metrics from the scoped metrics store.

### `akc policy`

Policy governance and decision explainability helpers.

### `akc control`

Operator-oriented control-plane commands for runs, replay forensics, exports, and policy bundles.

### `akc control-bot`

Standalone multi-channel operator gateway.

Current subcommands:

- `validate-config`
- `serve`

### `akc deliver`

Named-recipient delivery sessions and delivery lifecycle operations.

Current subcommands:

- `status`
- `events`
- `resend`
- `promote`
- `gate-pass`
- `activation-report`
- `web-invite-open`

Base command flags support creating a session directly with:

- `--request`
- `--recipient` or `--recipients-file`
- `--compile`
- `--platforms`
- `--release-mode`

### `akc fleet`

Cross-shard control-plane surfaces.

Current subcommands:

- `serve`
- `dashboard-serve`
- `runs`
- `webhooks-deliver`
- `automation-run`
- `policy-bundle`

### `akc view`

Read-only local viewer over plan state and emitted artifacts.

Current subcommands:

- `tui`
- `web`
- `export`

## Safe starting commands

If you are new to the repository, these are the least surprising places to start:

```bash
akc init --detect
akc ingest --tenant-id demo --connector codebase --input . --embedder hash --index-backend sqlite
akc compile --tenant-id demo --repo-id runform --outputs-root ./out --artifact-only
akc verify --tenant-id demo --repo-id runform --outputs-root ./out
akc view --tenant-id demo --repo-id runform --outputs-root ./out web
```
