# AKC alignment (implementation vs vision)

This document tracks how the repository implements [akc-vision.md](akc-vision.md). It is code-first: claims below point at packages and tests; anything not backed by those is labeled **aspirational**.

## Summary

- **Architecture matches the vision:** ingest → IR → compiler passes → runtime bundle → kernel → control plane are implemented with contracts and tests (`src/akc/ingest/` … `src/akc/control/`).
- **Compile path:** default realization is **policy-gated scoped apply** to the resolved scope root (`compile_realization_mode` defaults to `scoped_apply` in `src/akc/compile/controller_config.py`; apply in `src/akc/compile/scoped_apply.py`). **`artifact_only`** is opt-in (no working-tree mutation). “Instant” means within compile → policy → promotion/runtime gates, not unconstrained production change.
- **Product-shaped scope (tested):** golden-path flow, deployment gating, domain fixtures, living/recompile bridges, autopilot integration, and benchmark/SLO gate scripts are covered by the evidence links below.
- **Honest gap:** universal hands-off rollout across every domain and cloud is **not** an OSS guarantee; production remains policy- and operator-shaped.

## End goals (vision) → status

| # | Vision ([akc-vision.md](akc-vision.md)) | In-repo status | Primary code |
|---|----------------------------------------|----------------|--------------|
| 1 | Intent → system | **Strong (within gates)** — scoped apply default; `artifact_only` opt-in | `compile/controller_config.py`, `compile/scoped_apply.py`, `compile/controller_policy_runtime.py`, `cli/compile.py` |
| 2 | Software compiled, not manually wired | **Strong** — pass-oriented pipeline, manifests, replay | `compile/artifact_passes.py`, `pass_registry.py`, `run/manifest.py`, `run/replay.py` |
| 3 | Living systems | **Strong primitives** — bridge, triggers, safe recompile; autopilot under test | `runtime/living_bridge.py`, `living/safe_recompile.py`, `run/recompile_triggers.py`, `runtime/autopilot.py` |
| 4 | Executable knowledge | **Partial–strong** — extraction + projection; doc-derived assertions default `limited` | `compile/knowledge_extractor.py`, `knowledge/`, connectors: `ingest/connectors/docs.py`, `openapi/`, `messaging/slack.py` |
| 5 | Replace whole layers | **Partial** — compose/k8s providers behind explicit env gates | `runtime/providers/`, `runtime/providers/factory.py` |
| 6 | Multi-agent infrastructure | **Strong** — coordination spec + runtime worker + bundle contract | `coordination/`, `runtime/coordination/`, `runtime/kernel.py`, `compile/artifact_passes.py` |
| 7 | Deterministic + auditable | **Strong** — manifests, indices, replay, exports | `run/`, `control/operations_index.py`, `control/cost_index.py` |
| 8 | Time compression | **Instrumented / gated** — benchmark + reliability scripts | `scripts/check_benchmark_evidence_gate.py`, `scripts/check_reliability_slo_gate.py` |
| 9 | New developer role | **Emerging** — intent, policy explain, operational verify; `akc init` records **`emerging`** by default | `intent/`, `control/policy_explain.py`, `compile/operational_verify.py`, `cli/init.py` |

## Architecture (vision) → packages

| Layer | Package / entry points |
|-------|-------------------------|
| Inputs | `src/akc/ingest/` (`connectors/`: docs, OpenAPI, Slack) |
| IR | `src/akc/ir/`, `compile/ir_builder.py`, `compile/ir_passes.py`, `ir/workflow_order.py` |
| Compiler passes | `src/akc/compile/` (controller, passes, `operational_verify.py`, `scoped_apply.py`) |
| Runtime | `src/akc/runtime/` (`kernel.py`, scheduler, reconciler, providers, `coordination/`) |
| Control plane | `src/akc/control/` (policy bundle, indices, fleet, forensics) |
| Living / recompile | `src/akc/living/`, `runtime/living_bridge.py`, `cli/living.py` |
| Auditing / replay | `src/akc/run/` (`manifest.py`, `replay.py`, `time_compression.py`) |

## Core problems (vision) → code

| Problem | Notes |
|---------|--------|
| Representation | `src/akc/ir/`; IR in bundles via compile passes (`compile/artifact_passes.py`). |
| Correctness | Verifier/repair, operational IR validation (`compile/ir_operational_validate.py`, `compile/operational_verify.py`); not a single proof for all domains. |
| Control | Policy bundles, budgets, scoped apply, autopilot rollback budgets (`runtime/autopilot.py`). |
| Observability | Manifests, evidence exports, OTEL-oriented paths (`control/otel_export.py`), viewer (`src/akc/viewer/`). |
| Boundaries | Repo/tenant scope, coordination isolation, intent authority (`runtime/intent_authority.py`), tool policy including `compile.patch.apply`. |

## Defaults worth remembering

- **Developer profile:** unset resolution falls back to **`classic`** (`cli/profile_defaults.py`). **`akc init`** writes **`emerging`** into `.akc/project.json` by default (`cli/init.py`).
- **Promotion** (`promotion.py`) uses `artifact_only` / staged / live apply for promotion semantics; that is separate from compile-time `artifact_only` vs `scoped_apply`.

## Evidence (tests, CI, scripts)

Use these to validate **strong** statements above:

| Area | Evidence |
|------|----------|
| Golden path (ingest → compile → verify → runtime) | [`tests/integration/test_emerging_profile_one_command_flow.py`](../tests/integration/test_emerging_profile_one_command_flow.py); [Emerging Role Golden Path](getting-started.md#emerging-role-golden-path-opt-in) |
| Scoped apply | [`tests/integration/test_compile_scoped_apply_realization.py`](../tests/integration/test_compile_scoped_apply_realization.py) |
| Living bridge + triggers | [`tests/integration/test_runtime_living_recompile_bridge.py`](../tests/integration/test_runtime_living_recompile_bridge.py), [`tests/unit/test_recompile_triggers_operational.py`](../tests/unit/test_recompile_triggers_operational.py), [`tests/unit/test_living_automation_profile.py`](../tests/unit/test_living_automation_profile.py) |
| Autopilot | [`tests/integration/test_runtime_autopilot_phase_e.py`](../tests/integration/test_runtime_autopilot_phase_e.py), [`tests/integration/test_runtime_autopilot_lease_single_writer.py`](../tests/integration/test_runtime_autopilot_lease_single_writer.py); [Runtime execution — autopilot](runtime-execution.md) |
| Benchmark / SLO gates | [`tests/unit/test_benchmark_evidence_gate.py`](../tests/unit/test_benchmark_evidence_gate.py), [`tests/unit/test_reliability_slo_gate.py`](../tests/unit/test_reliability_slo_gate.py) |
| Domain fixtures | [`tests/fixtures/knowledge_domains/`](../tests/fixtures/knowledge_domains/), [`domain_coverage_matrix.json`](../tests/fixtures/knowledge_domains/domain_coverage_matrix.json) |
| CI | [`.github/workflows/ci.yml`](../.github/workflows/ci.yml) |

## Aspirational (not promised here)

- Full multi-cloud replacement of arbitrary estates.
- Always-on autonomous operation without deploy, monitoring, and policy wiring.
- One proof bundle for every possible domain without growing fixtures and tests.
