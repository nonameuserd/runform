# OSS Evidence Map (Phase 0)

This document maps the OSS direction memo’s differentiators to concrete AKC repo components.

Conventions:

- “Claim” is a statement we want contributors to understand as part of the product contract.
- “Evidence” points to one or more repo files that implement or document the claim today.
- “Status” indicates whether the claim is already implemented or is a “direction/principle” that still needs a dedicated contract doc.

## Differentiator evidence

| Claim | Evidence | Status |
| --- | --- | --- |
| CLI-first UX exposes the core commands (`ingest`, `compile`, `verify`, `drift`, `watch`). | `src/akc/cli/__init__.py`; `docs/getting-started.md` | Implemented |
| `akc compile` runs a correctness-aware loop: Plan -> Retrieve -> Generate -> Execute -> Repair. | `src/akc/compile/controller.py` (orchestration + stages); `docs/architecture.md` | Implemented |
| The controller is budgeted and bounded (max LLM calls, repair iterations, wall-time). | `src/akc/compile/controller_config.py` (`Budget`); `src/akc/compile/controller.py` (budget checks + loop control) | Implemented |
| Tests are required by default, with policy logic for when tests must be included. | `src/akc/compile/controller_config.py` (`generate_tests_by_default`, `require_tests_for_non_test_changes`, test-mode knobs); `src/akc/compile/controller.py` (`_policy_requires_tests`) | Implemented |
| A repair loop exists and escalates tiers conservatively when candidates fail. | `src/akc/compile/controller.py` (repair stage + `_escalate_tier`) | Implemented |
| Optional deterministic verifier gate can veto promotion after tests pass. | `src/akc/compile/controller.py` (verifier invocation); `src/akc/compile/verifier.py` (`DeterministicVerifier`, `VerifierPolicy`) | Implemented (optional by config) |
| Verifier performs structured checks over candidate patch content and paths (e.g., suspicious paths, `.akc/` touch restrictions). | `src/akc/compile/verifier.py` (`_extract_patch_paths`, `_is_path_suspicious`, `.akc/` veto checks) | Implemented |
| Tenant isolation is treated as a hard requirement in the security model. | `docs/security.md` (tenant_id/run_id required; no cross-tenant caching; isolation goals) | Implemented (security policy) |
| Plan state is tenant+repo scoped and persisted under a predictable `.akc/plan` layout. | `src/akc/memory/plan_state.py` (`default_plan_dir` -> `.akc/plan`; tenant/repo safe pathing) | Implemented |
| Output artifacts are written under a tenant+repo scoped directory and enforce “under root” path confinement. | `src/akc/outputs/emitters.py` (`FileSystemEmitter` + `_ensure_under_root`) | Implemented |
| Compile emission produces an artifact manifest (`manifest.json`) and structured evidence in `.akc/tests/*`, verification records, intent/design/orchestration/coordination specs, and hardened deployment/workflow outputs. | `src/akc/compile/session.py` (artifact creation + paths); `src/akc/outputs/emitters.py` (`JsonManifestEmitter`); `tests/unit/test_compile_session_end_to_end_light.py` (tenant-scoped artifact pass emission); `tests/unit/test_artifact_passes.py` (deployment/workflow hardening validation) | Implemented |
| Compile emits a deterministic runtime handoff artifact (`runtime_bundle`) with versioned schema metadata and manifest pass records. | `src/akc/compile/artifact_passes.py` (`run_runtime_bundle_pass`); `src/akc/compile/session.py` (runtime pass registration); `src/akc/artifacts/schemas.py` (`runtime_bundle` schema); `tests/unit/test_artifact_passes.py` (runtime bundle schema validation) | Implemented |
| AKC exposes a tenant-scoped runtime substrate with persistent checkpoints, scheduler snapshots, and runtime operator records under `.akc/runtime/`. | `src/akc/runtime/kernel.py`; `src/akc/runtime/scheduler.py`; `src/akc/runtime/state_store.py`; `src/akc/cli/runtime.py`; `tests/integration/test_compile_runtime_handoff.py` | Implemented |
| Runtime replay and reconcile replay are first-class evidence contracts, not ad hoc debugging behavior. | `src/akc/run/replay.py`; `src/akc/run/manifest.py`; `src/akc/cli/runtime.py`; `tests/unit/test_run_manifest_hashing.py`; `tests/integration/test_runtime_replay_determinism.py` | Implemented |
| Runtime degradation can feed living recompilation, with policy-gated auto-recompile and canary thresholds. | `src/akc/runtime/living_bridge.py`; `src/akc/living/safe_recompile.py`; `tests/unit/test_living_safe_recompile_drift_integration.py`; `tests/integration/test_runtime_living_recompile_bridge.py` | Implemented |
| Execution trust boundary is constrained: untrusted generated/external artifacts are executed in a sandbox, not inside a UI. | `docs/security.md` (threat model + sandbox lanes + what is untrusted) | Principle / documented security model |
| Viewer posture is local-first and read-only over schema-stable artifacts and plan state (non-execution surface). | `docs/viewer-trust-boundary.md` (contract); `docs/security.md` (sandbox + what is untrusted); `src/akc/memory/plan_state.py` (plan persistence); `src/akc/compile/session.py` (artifact emission); `src/akc/outputs/emitters.py` (path confinement + `manifest.json`) | Implemented (contract doc + existing evidence) |
| AKC does not position itself as a hosted SaaS (OSS scope out-of-scope). | `GOVERNANCE.md` (Out of scope: hosted SaaS) | Implemented (scope statement) |
| OSS CI and release processes enforce security/correctness requirements, including hardened generated deployment/workflow contracts. | `docs/oss-security-requirements.md` (checklist); `.github/workflows/ci.yml` (lint/typecheck/test + tenant isolation + cargo vet); `.github/workflows/release.yml` (provenance generation); `docs/security.md` (sandbox model); `src/akc/compile/verifier.py` + `tests/unit/test_compile_verifier_unit.py` (verifier gate); `tests/unit/test_artifact_passes.py` + `tests/unit/test_compile_session_end_to_end_light.py` (artifact-pass hardening + path evidence); `scripts/ci_policy_test.py` (workflow policy checks) | Implemented (documented requirements + existing workflow evidence) |

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

Primary sources:

- `src/akc/compile/session.py`
- `src/akc/outputs/emitters.py`

## What is intentionally not “proven” in Phase 0

Some OSS direction items are directional principles or future phases rather than fully contracted implementation today:

- supply-chain provenance/release attestation mechanics (Phase 2),
- schema freezing + machine validation (Phase 3),
- OSPS-aligned security reporting automation (Phase 1).
