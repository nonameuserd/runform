# OSS Evidence Map

This document maps the [OSS direction memo](oss-direction-memo.md) differentiators to concrete AKC repo components.

Conventions:

- **Claim** — a statement we want contributors to treat as part of the product contract.
- **Evidence** — one or more repo files that implement or document the claim today.
- **Status** — whether the claim is implemented, partially covered (ongoing work), or a documented principle without a single implementation anchor.

## Differentiator evidence

| Claim | Evidence | Status |
| --- | --- | --- |
| CLI-first UX exposes core commands (`init`, `ingest`, `compile`, `verify`, `drift`, `watch`) plus optional extensions (`runtime`, `control`, `view`, `deliver`, `living`, `fleet`, …). | `src/akc/cli/__init__.py`; `docs/getting-started.md` | Implemented |
| `akc compile` runs a correctness-aware loop: Plan → Retrieve → Generate → Execute → Repair. | `src/akc/compile/controller.py` (orchestration + stages); `docs/architecture.md` | Implemented |
| The controller is budgeted and bounded (max LLM calls, repair iterations, wall-time). | `src/akc/compile/controller_config.py` (`Budget`); `src/akc/compile/controller.py` (budget checks + loop control) | Implemented |
| Tests are required by default, with policy logic for when tests must be included. | `src/akc/compile/controller_config.py` (`generate_tests_by_default`, `require_tests_for_non_test_changes`, test-mode knobs); `src/akc/compile/controller.py` (`_policy_requires_tests`) | Implemented |
| A repair loop exists and escalates tiers conservatively when candidates fail. | `src/akc/compile/controller.py` (repair stage + `_escalate_tier`) | Implemented |
| Optional deterministic verifier gate can veto promotion after tests pass. | `src/akc/compile/controller.py` (verifier invocation); `src/akc/compile/verifier.py` (`DeterministicVerifier`, `VerifierPolicy`) | Implemented (optional by config) |
| Verifier performs structured checks over candidate patch content and paths (e.g., suspicious paths, `.akc/` touch restrictions). | `src/akc/compile/verifier.py` (`_extract_patch_paths`, `_is_path_suspicious`, `.akc/` veto checks) | Implemented |
| Tenant isolation is treated as a hard requirement in the security model. | `docs/security.md` (tenant_id/run_id required; no cross-tenant caching; isolation goals) | Implemented (security policy) |
| Plan state is tenant+repo scoped and persisted under a predictable `.akc/plan` layout. | `src/akc/memory/plan_state.py` (`default_plan_dir` → `.akc/plan`; tenant/repo safe pathing) | Implemented |
| Output artifacts are written under a tenant+repo scoped directory and enforce “under root” path confinement. | `src/akc/outputs/emitters.py` (`FileSystemEmitter` + `_ensure_under_root`) | Implemented |
| Compile emission produces a bundle manifest (`manifest.json`) and structured evidence (`.akc/tests/*`, verification, intent/design/orchestration/coordination, deployment/workflow outputs) per [artifact contracts](artifact-contracts.md). | `src/akc/compile/session.py`; `src/akc/outputs/emitters.py` (`JsonManifestEmitter`); `src/akc/artifacts/schemas.py` (`SchemaKind`); `tests/unit/test_compile_session_end_to_end_light.py`; `tests/unit/test_artifact_passes.py` | Implemented |
| Emitted JSON uses versioned schema metadata (`schema_version` / `schema_id` patterns) for many artifact kinds. | `src/akc/artifacts/contracts.py` (`ARTIFACT_SCHEMA_VERSION`, `schema_id_for`); `src/akc/artifacts/validate.py`; `docs/artifact-contracts.md` | Implemented (ongoing: expand machine validation) |
| Compile emits a deterministic runtime handoff artifact (`runtime_bundle`) with versioned schema metadata and manifest pass records. | `src/akc/compile/artifact_passes.py` (`run_runtime_bundle_pass`); `src/akc/compile/session.py` (runtime pass registration); `src/akc/artifacts/schemas.py` (`runtime_bundle` schema); `tests/unit/test_artifact_passes.py` | Implemented |
| AKC exposes a tenant-scoped runtime substrate with persistent checkpoints, scheduler snapshots, and runtime operator records under `.akc/runtime/`. | `src/akc/runtime/kernel.py`; `src/akc/runtime/scheduler.py`; `src/akc/runtime/state_store.py`; `src/akc/cli/runtime.py`; `tests/integration/test_compile_runtime_handoff.py` | Implemented |
| Runtime replay and reconcile replay are first-class evidence contracts, not ad hoc debugging behavior. | `src/akc/run/replay.py`; `src/akc/run/manifest.py`; `src/akc/cli/runtime.py`; `tests/unit/test_run_manifest_hashing.py`; `tests/integration/test_runtime_replay_determinism.py` | Implemented |
| Runtime degradation can feed living recompilation, with policy-gated auto-recompile and canary thresholds. | `src/akc/runtime/living_bridge.py`; `src/akc/living/safe_recompile.py`; `tests/unit/test_living_safe_recompile_drift_integration.py`; `tests/integration/test_runtime_living_recompile_bridge.py` | Implemented |
| Execution trust boundary is constrained: untrusted generated/external artifacts are executed in a sandbox, not inside a UI. | `docs/security.md` (threat model + sandbox lanes + what is untrusted) | Principle / documented security model |
| Viewer is local-first and read-only over schema-stable artifacts and plan state (non-execution surface). | `docs/viewer-trust-boundary.md`; `docs/security.md`; `src/akc/cli/view.py`; `src/akc/viewer/`; `tests/unit/test_viewer_export.py`; `src/akc/memory/plan_state.py`; `src/akc/outputs/emitters.py` | Implemented |
| AKC does not position itself as a hosted SaaS (OSS scope out-of-scope). | `GOVERNANCE.md` (Out of scope: hosted SaaS) | Implemented (scope statement) |
| OSS CI and release processes enforce security/correctness requirements, including hardened deployment/workflow contracts and supply-chain checks where configured. | [oss-security-requirements.md](oss-security-requirements.md); `.github/workflows/ci.yml` (`policy-test`, `ci`, `rust`, `rust_bwrap`, `rust_windows`, `docker_hardening`, `wasm_lane`, optional `runtime_*` / `dafny` / `verus`); `.github/workflows/release.yml` (pip-audit, Docker strong compile, SLSA provenance via `slsa-github-generator`); `docs/security.md`; `src/akc/compile/verifier.py` + `tests/unit/test_compile_verifier_unit.py`; `tests/unit/test_artifact_passes.py` + `tests/unit/test_compile_session_end_to_end_light.py`; `scripts/ci_policy_test.py` | Implemented (see security doc for which gates run on every PR vs opt-in) |

## Evidence notes by repo component

### CLI entrypoints

The concrete command surface and argument choices establish the “CLI-first” UX contract:

- `src/akc/cli/__init__.py`
- `docs/getting-started.md`

### Compile correctness gates

The compile controller plus its config/verifier implement the “correctness-aware loop”:

- `src/akc/compile/controller.py` (stages, repair + promotion gate)
- `src/akc/compile/controller_config.py` (budget + tests-by-default knobs)
- `src/akc/compile/verifier.py` (deterministic verifier policy + veto mechanics)

### Tenant isolation and artifact confinement

The security model describes required tenant isolation, while the implementation makes key scoping/confinement choices:

- `docs/security.md` (threat model, tenant isolation guarantees)
- `src/akc/memory/plan_state.py` (tenant+repo scoped `.akc/plan` persistence)
- `src/akc/outputs/emitters.py` (tenant-scoped output directories + under-root enforcement)

### Artifact-first contracts

Compile session emission is responsible for generating:

- patch/evidence artifacts under `.akc/tests/*` (structured JSON plus stdout/stderr text files),
- optional verification record files,
- intent/design/orchestration/coordination artifacts emitted by artifact passes,
- runtime handoff and evidence artifacts under `.akc/runtime/**`,
- hardened deployment artifacts under `.akc/deployment/**` plus generated `.github/workflows/akc_deploy_*.yml`,
- and the bundle `manifest.json` that ties evidence together.

Canonical path and schema documentation:

- `docs/artifact-contracts.md`

Primary implementation sources:

- `src/akc/compile/session.py`
- `src/akc/outputs/emitters.py`
- `src/akc/artifacts/schemas.py`

## What is still evolving (not a “fully frozen OSS contract” yet)

Some OSS direction items remain **directional** or **partially automated**:

- **Schema completeness:** `SchemaKind` and JSON Schemas cover many artifacts; expanding machine validation and freezing incompatible changes is ongoing ([artifact contracts](artifact-contracts.md), `src/akc/artifacts/validate.py`).
- **OSPS-aligned hygiene:** [SECURITY.md](../SECURITY.md), `security-insights.yml`, and [CONTRIBUTING.md](../CONTRIBUTING.md) exist; automation and branch-protection expectations are described in [oss-security-requirements.md](oss-security-requirements.md).
- **CI gate tiers:** default PR jobs are fast; extended gates (e.g. eval/benchmark/retrieval proof suites, `pip-audit`) run on labeled/opt-in workflows or on release—see `.github/workflows/ci.yml` job `if:` conditions.
