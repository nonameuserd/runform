# AKC alignment: repo reality vs vision

This page reconciles [akc-vision.md](akc-vision.md) with what is implemented in `src/akc/` and exercised by tests. When narrative docs drift, this page should follow the code and automated coverage first.

## Executive summary

AKC is a compiler that turns connected knowledge into code, workflows, and agents. It is already more than a prompt-to-code demo. In this repository it is a repo-scoped system with:

- ingest connectors and indexed knowledge
- intent and IR models
- a pass-oriented compile loop
- policy-gated realization (`scoped_apply` or `artifact_only`)
- runtime bundles, reconcile, replay, and coordination
- living-recompile and autopilot loops
- control-plane, delivery, provision, viewer, and operator-gateway surfaces

The main vision is therefore **aligned in shape**: AKC is a compiler + runtime + control system for turning project knowledge into executable artifacts and operating them under evidence and policy.

The main caveat is scope: the repo does **not** yet justify a blanket claim of "intent to production everywhere." Realization is bounded by repo scope, policy, toolchain detection, provider support, operator inputs, and explicit rollout/provision stages.

## Alignment by theme

| Vision theme                             | Status                                  | What is true in this repo                                                                                                                                                                                                           |
| ---------------------------------------- | --------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Compiler + runtime for systems**       | **Strong**                              | The package map is real, not aspirational: ingest, memory/knowledge, intent, IR, compile, outputs/run, runtime, living, control, delivery, viewer, and operator surfaces all exist. See [architecture.md](architecture.md).         |
| **Intent -> runnable output**            | **Strong inside gates**                 | `akc compile`, `verify`, `runtime`, `deliver`, and `provision` form a concrete path from intent/artifacts to execution and rollout evidence. The path is explicit and policy-bounded rather than "automatic by default."            |
| **Adopt existing codebases**             | **Strong**                              | Existing repositories are first-class inputs: repo detection, native toolchain resolution, native test defaults, scoped patch application, rollback snapshots, and optional Git-aware realization are implemented.                  |
| **Software is compiled, not hand-wired** | **Strong**                              | The compile loop is explicit: plan -> retrieve -> generate -> execute -> repair. Outputs are artifacts with manifests, refs, schemas, and replay hooks rather than opaque controller state.                                         |
| **Living systems**                       | **Strong primitives, bounded autonomy** | Drift-triggered safe recompile, runtime bridge, living automation profiles, autopilot budgets, single-writer leases, and replay/reconcile flows are implemented. The repo still treats autonomy as policy-gated and budgeted.       |
| **Executable knowledge**                 | **Strong and expanding**                | Ingest supports docs, codebase, OpenAPI, Slack, Discord, Telegram, WhatsApp Cloud payloads, and MCP resources. Knowledge snapshots, provenance, mediation, validator evidence, and operator decisions are first-class artifacts.    |
| **Multi-agent infrastructure**           | **Strong**                              | Coordination specs, shared coordination models, runtime worker execution, audit trails, and scheduling semantics are implemented and schema-backed.                                                                                 |
| **Deterministic + auditable**            | **Strong**                              | Run manifests, artifact contracts, replay surfaces, policy explainability, operations indexes, control audit, and compile-apply attestation all support traceability.                                                               |
| **Operator control anywhere**            | **Strong but secondary**                | Slack, Discord, Telegram, and WhatsApp appear in two roles: ingest connectors and the separate `control_bot/` multi-channel operator gateway. They are control-plane edges, not the product core.                                   |
| **Replace entire layers**                | **Partial**                             | AKC can emit runtime, delivery, and infrastructure artifacts, and it has runtime providers plus an explicit `akc provision` surface. It is not yet a universal replacement for arbitrary backend, DevOps, and internal-tool stacks. |
| **Time compression**                     | **Measured, not universal**             | The repo includes benchmark, retrieval, and reliability/SLO gates, but it does not prove uniform speedups across all domains.                                                                                                       |
| **New developer role**                   | **Emerging and real**                   | `classic` and `emerging` developer-role profiles exist. `akc init` writes `emerging` into `.akc/project.json` by default, while CLI fallback remains `classic` when no project or env override exists.                              |

## Boundaries that matter

These distinctions are easy to blur in the vision doc, but they are important in the implementation:

### 1. Compile realization is not the same as rollout

- `akc compile` defaults to `scoped_apply`.
- `--artifact-only` is the explicit no-working-tree-mutation mode.
- Promotion mode is separate from compile realization mode.
- Runtime live mutation can require signed `compile_apply_attestation` that matches the compile run manifest.

This is why "AKC applied a patch" and "AKC rolled out a live deployment" are separate claims in this repo.

### 2. Compile/runtime/provision are separate mutation surfaces

- compile mutates a repo working tree only through bounded `scoped_apply`
- runtime mutates runtime/deployment state through runtime providers and reconcile flows
- cloud mutation is the explicit `akc provision` surface

That separation is one of the clearest signs that the repo favors auditable staged control over one giant automation loop.

### 3. Existing-codebase support is real, but not unbounded

AKC already detects real repositories, resolves native toolchains, and can generate authoritative execution workspaces when the runtime plugin, repo signals, and materializer support line up. Built-in runtime plugins exist for `typescript_node`, `python_fastapi`, `go`, `rust`, and `java`, with external manifests as the extension path.

The limiting factor is not "whether AKC knows other languages exist"; it is whether the repo can justify a safe authoritative materialization for the detected project under the configured policies. In practice that means detected language alignment, required native validation commands, repo anchors, and materializer authority all have to pass before AKC treats the workspace as authoritative. Policy can explicitly allow a runtime/language override, but that does not waive the other readiness gates.

### 4. Read-only surfaces stay read-only

`viewer/`, `fleet`, and `mcp_serve/` are intentionally separate from execution and mutation surfaces. The repo is explicit about trust boundaries here; inspection, indexing, and operator dashboards are not hidden mutation paths.

## Current architecture in one sentence

The repo is best understood as:

**ingest -> knowledge/memory -> intent/IR -> compile -> outputs/run -> runtime/living -> control/delivery/viewer**

That is the actual center of gravity of `src/akc/`, and it matches the high-level architecture in [akc-vision.md](akc-vision.md) more closely than the repo name or individual commands alone might suggest.

## Evidence pointers

Use these as the quickest checks against the stronger claims above.

| Area                                         | Evidence                                                                                                                                                                          |
| -------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Emerging golden path                         | `tests/integration/test_emerging_profile_one_command_flow.py`                                                                                                                     |
| Scoped apply and compile attestation         | `tests/integration/test_compile_scoped_apply_realization.py`                                                                                                                      |
| Compile -> runtime live mutation gate        | `tests/integration/test_compile_runtime_live_mutation_gate.py`                                                                                                                    |
| Compile -> runtime handoff                   | `tests/integration/test_compile_runtime_handoff.py`                                                                                                                               |
| Existing-codebase native toolchains          | `tests/integration/test_existing_codebase_toolchain.py`, `tests/unit/test_toolchain_resolver.py`, `tests/unit/test_execution_workspace.py`                                        |
| Coordination emission and kernel handoff     | `tests/integration/test_compile_coordination_emit_phase4.py`, `tests/integration/test_coordination_handoff_kernel.py`, `tests/integration/test_coordination_plan_v1_v2_parity.py` |
| Runtime operational attestation              | `tests/integration/test_runtime_operational_attestation.py`, `tests/integration/test_verify_operational_coupling.py`                                                              |
| Living recompile                             | `tests/integration/test_runtime_living_recompile_bridge.py`, `tests/unit/test_living_safe_recompile.py`, `tests/unit/test_recompile_triggers_operational.py`                      |
| Autopilot budgets and single-writer behavior | `tests/integration/test_runtime_autopilot_phase_e.py`, `tests/integration/test_runtime_autopilot_lease_single_writer.py`, `tests/unit/test_runtime_autopilot_controller.py`       |
| Delivery and infrastructure synthesis        | `tests/integration/test_delivery_plan_compile_pipeline.py`, `tests/integration/test_infrastructure_synthesis_pipeline.py`, `tests/unit/test_cli_provision.py`                     |
| Control-plane policy lifecycle               | `tests/integration/test_control_policy_bundle_lifecycle.py`, `tests/unit/test_control_policy.py`                                                                                  |
| Operator gateway                             | `tests/unit/test_control_bot_subsystem.py`, `tests/unit/test_control_bot_ingress_auth.py`, `tests/unit/test_control_bot_outbound_adapters.py`                                     |

## Bottom line

The vision statement is directionally right, but the accurate present-tense description is:

> AKC is a policy-gated, artifact-first compiler/runtime/control stack for real repositories, with living and operator workflows already implemented, but with explicit boundaries around safe mutation, rollout, and provisioning.
