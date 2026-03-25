# AKC alignment: vision vs this repo

This is a concise progress check against [akc-vision.md](akc-vision.md). Claims below are grounded in `src/akc/` and automated tests; anything else is labeled **aspirational**.

## Where we are

The vision is a **compiler + runtime for systems**: ingest knowledge, compile through an IR and passes, ship bundles, run under policy with audit and replay.

**In this repository, that shape is real:** ingestion → memory → compile (plan/retrieve/generate/execute/repair) → outputs/runtime → control plane, with contracts and CI coverage (see [architecture.md](architecture.md)).

**The honest gap:** we are not promising hands-off production across every stack or cloud. “Intent → system” here means **policy-gated, test-backed realization** (default scoped apply; `artifact_only` when you want zero working-tree writes)—not unconstrained auto-deploy everywhere.

---

## Vision goals → status

| Theme | Status | Notes |
|-------|--------|--------|
| **Intent → runnable output** | **Strong (within gates)** | Compile controller, scoped apply, CLI; promotion/delivery paths stay explicit. |
| **Compiled, not hand-wired** | **Strong** | Pass-oriented pipeline, manifests, replay-oriented run records. |
| **Living / recompile** | **Strong primitives** | Living bridge, triggers, safe recompile, autopilot under tests—not unlimited autonomy. |
| **Executable knowledge** | **Partial → strong** | Extraction, projection, connectors (docs, OpenAPI, Slack); domain coverage grows with fixtures. |
| **Replace whole layers** | **Partial** | Compose/Kubernetes-style providers behind explicit configuration; not a universal estate replacer. |
| **Multi-agent / coordination** | **Strong** | Coordination models, runtime worker, bundle contracts wired through compile/runtime. |
| **Deterministic + auditable** | **Strong** | Manifests, indices, replay, cost/operations surfaces, policy explain paths. |
| **Time compression** | **Measured, not magic** | Benchmark and reliability gate scripts; proves workflow speed where we instrument, not all domains. |
| **“New developer role”** | **Emerging** | Intent, policy explain, operational verify; `akc init` defaults toward **emerging** profile. |

---

## Vision architecture → packages

| Vision layer | Where it lives |
|--------------|----------------|
| Inputs | `ingest/` (connectors: docs, OpenAPI, Slack) |
| IR | `ir/`, compile IR builders/passes |
| Compiler passes | `compile/` (controller, verifier, operational checks, scoped apply) |
| Runtime | `runtime/` (kernel, scheduler, reconciler, providers, coordination, living bridge) |
| Control plane | `control/` (policy bundle, fleet, indices, forensics) |
| Outputs / delivery | `outputs/`, `delivery/` (packaging, distribution, activation—product-shaped, gated) |
| Living / audit | `living/`, `run/` (manifest, replay, triggers) |

---

## Core problems (vision) → how we address them

| Problem | Direction in-repo |
|---------|---------------------|
| **Representation** | Versioned IR + bundle contracts; not one universal ontology for all domains. |
| **Correctness** | Operational validation, verifier/repair loops, tests and fixtures—**not** a single proof for arbitrary software. |
| **Control** | Policy bundles, budgets, scoped apply, autopilot rollback limits. |
| **Observability** | Manifests, exports, OTEL-oriented hooks, viewer surfaces. |
| **Boundaries** | Tenant/repo scope, coordination isolation, intent authority, tool policy (e.g. patch apply). |

---

## Evidence (quick pointers)

Use these to validate “strong” claims:

| Area | Where to look |
|------|----------------|
| Golden path | `tests/integration/test_emerging_profile_one_command_flow.py`, [Emerging Role Golden Path](getting-started.md#emerging-role-golden-path-opt-in) |
| Scoped apply | `tests/integration/test_compile_scoped_apply_realization.py` |
| Living + recompile | `tests/integration/test_runtime_living_recompile_bridge.py`, `tests/unit/test_recompile_triggers_operational.py` |
| Autopilot | `tests/integration/test_runtime_autopilot_phase_e.py`, [Runtime — autopilot](runtime-execution.md) |
| Benchmark / SLO gates | `tests/unit/test_benchmark_evidence_gate.py`, `tests/unit/test_reliability_slo_gate.py` |
| Domain fixtures | `tests/fixtures/knowledge_domains/`, `domain_coverage_matrix.json` |
| CI | `.github/workflows/ci.yml` |

---

## Defaults worth remembering

- Compile realization defaults to **policy-gated scoped apply**; **`artifact_only`** is the explicit no-mutation mode (`compile/controller_config.py`, `compile/scoped_apply.py`).
- **Developer profile:** unset resolution falls back to **classic** (`cli/profile_defaults.py`); **`akc init`** writes **emerging** into `.akc/project.json` by default (`cli/init.py`).
- **Promotion** (`promotion.py`) is separate from compile-time `artifact_only` vs `scoped_apply`—staged vs live semantics live there.

---

## Aspirational (not promised here)

- Full multi-cloud replacement of arbitrary estates without operator wiring.
- Always-on autonomous operation without deploy, monitoring, and policy setup.
- One correctness bundle that covers every possible domain without growing fixtures and tests.
