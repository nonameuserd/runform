# AKC CLI Reference

This page summarizes the current `akc` command tree implemented under `src/akc/cli/`.

For exact flags, run `akc <command> --help` or `uv run akc <command> --help`.

## What’s missing to ship?

Start with:

```bash
akc deliver preflight
```

`preflight` reports the exact repo/env/operator prerequisites missing for execute-mode delivery (web/iOS/Android) and store lanes when applicable.

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
- `provision`
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

- `--detect` to write `.akc/project_profile.json` (prints suggested `mutation_paths` when useful)
- `--detect-respect-gitignore` with `--detect` to skip simple `.gitignore` directory tokens during scans
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

For **one runnable example per connector** (docs, codebase, openapi, messaging, MCP), see [`examples/README.md`](https://github.com/nonameuserd/runform/blob/main/examples/README.md).

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
- Mixed and polyglot repositories are valid compile inputs; built-in authoritative backend materializers cover `typescript_node`, `python_fastapi`, `go`, `rust`, and `java`
- Authoritative backend materialization is fail-closed unless the selected runtime matches detected repo languages, required native validation commands are available, repo anchors are present, and a materializer is available
- Runtime policy can explicitly set `allow_runtime_language_override`, but that only relaxes the language/runtime mismatch check; missing native commands or materializer gaps still block authoritative output
- External generator manifests under `.akc/backend_generators/` (or policy-configured manifest paths) remain the stable extension surface for custom runtimes and org-specific backend generators
- Hosted LLM backends are opt-in; offline is still the default
- Hosted backends fail closed unless `--llm-allow-network` or `AKC_LLM_ALLOW_NETWORK=1` is set

Compile now also emits infrastructure-planning artifacts when a delivery plan indicates cloud provisioning context:

- `infrastructure_synthesis` is an artifact pass after `delivery_plan`
- compile writes `.akc/infra/<run_id>.infra_plan.json`
- compile writes `.akc/infra/<run_id>.iac_manifest.json`
- compile generates deterministic Terraform and AWS CDK workspaces under `.akc/infra/<run_id>/`
- compile still stops at artifact emission plus optional `scoped_apply`; it does not mutate cloud resources

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

If you only run one command first, run `akc deliver preflight`. It uses the same prerequisite probes as packaging/distribution and groups missing items so you can unblock shipping quickly.

Current subcommands:

- `preflight`
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
- `--packaging-mode`
- `--store-submit`
- `--platforms`
- `--release-mode`

Key delivery behaviors:

- `akc deliver preflight` runs the same repo/env/operator prerequisite probes used by delivery packaging and distribution, then groups the missing items for Expo/EAS, App Store Connect/TestFlight, Firebase, and Play
- `--compile` now prefers packaging in `execute` mode by default, then auto-falls back to an inspectable `plan` outcome when delivery/packaging prerequisites are missing
- `--packaging-mode plan` is the explicit plan-only path; it stages outputs but skips automatic distribution
- `--packaging-mode execute` is now the explicit fail-closed path when you want missing prerequisites to block instead of downgrading
- `--store-submit auto` is the default for `store` and `both`; `beta` ignores store submission settings
- submit JSON now includes a `journey` block and `compile_outputs` refs so the caller can see whether the run resolved to live distribution or an inspectable plan
- submit JSON now also includes a structured `preflight` block instead of only a missing-count summary

### `akc provision`

Provision synthesized infrastructure from compile-emitted IaC artifacts.

Delivery plans can include **backend, web, mobile, worker, integration, and shared infrastructure** targets; synthesis emits resources per target (public DNS/TLS only where `exposure_model.public` is true). A checked-in **full-stack** `delivery_plan` shape lives at [`examples/infrastructure/delivery_plan.fullstack.example.json`](https://github.com/nonameuserd/runform/blob/main/examples/infrastructure/delivery_plan.fullstack.example.json).

Current subcommands:

- `plan`
- `apply`
- `status`

Base command flags:

- `--project-dir`

`akc provision plan`:

- requires `--run-id`
- accepts `--backend terraform|aws_cdk`
- accepts `--environment staging|production`
- accepts `--provision-id`
- loads `.akc/infra/<run_id>.infra_plan.json` and `.akc/infra/<run_id>.iac_manifest.json`
- validates provisioning readiness and backend/tool availability
- records evidence under `.akc/provision/<provision_id>/plan.json` and `.akc/provision/<provision_id>/session.json`

`akc provision apply`:

- requires `--provision-id`
- requires `--approve-production` for production sessions
- re-checks the desired fingerprint before mutating infrastructure
- records apply evidence under `.akc/provision/<provision_id>/apply.json`

`akc provision status`:

- accepts `--provision-id`
- or resolves the latest provision session for `--run-id`
- reads stored plan/apply/session artifacts without re-running provider tools

Current backend behavior:

- `terraform` runs `init -backend=false`, then `plan` during `akc provision plan`
- `terraform` runs `apply -auto-approve` during `akc provision apply`
- `aws_cdk` runs `synth`, then `diff --no-color` during `akc provision plan`
- `aws_cdk` runs `deploy --require-approval never` during `akc provision apply`

Important provisioning boundaries:

- v1 provisioning scope is AWS-only
- dual target means one AKC infra IR can emit both Terraform-native and AWS CDK workspaces
- `local` remains non-cloud; provisioning is modeled for `staging` and `production`
- production apply is fail-closed without explicit approval and a matching prior plan fingerprint

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
akc provision plan --project-dir . --run-id <run_id> --backend terraform --environment staging
akc verify --tenant-id demo --repo-id runform --outputs-root ./out
akc view --tenant-id demo --repo-id runform --outputs-root ./out web
```
