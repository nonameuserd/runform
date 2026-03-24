# IR Schema

This document describes the stable persisted shape of the versioned IR in `src/akc/ir/schema.py`.

## Overview

`IRDocument` is the tenant- and repo-scoped compiler IR envelope. It serializes deterministically and carries:

- `schema_kind`
- `schema_version`
- `format_version`
- `tenant_id`
- `repo_id`
- `nodes`

`IRNode` is the typed node unit inside the graph. It supports:

- stable ids via `stable_node_id()`
- typed `kind`
- `depends_on` dependency edges
- `effects` side-effect boundaries
- `provenance` pointers
- optional `contract` for operational semantics

The IR currently supports these node kinds:

- `service`
- `workflow`
- `intent`
- `entity`
- `knowledge` — hub node for the compiled knowledge layer (fingerprints, persisted snapshot path, assertion id list); assertion-level detail remains on `entity` nodes named `knowledge_constraint:*` / `knowledge_decision:*`
- `integration`
- `policy`
- `agent`
- `infrastructure`
- `other`

**Policy / integration cross-refs:** nodes with `kind` `policy` or `integration` may include `depends_on` edges to the `knowledge` hub (or to specific `knowledge_constraint:*` entities) so runtime bundles and viewers can follow the contract spine without scraping opaque plan-step JSON.

**Plan → IR (`build_ir_document_from_plan`):** each `workflow` node emitted from a plan step gets a `contract` with `contract_category="runtime"`, stable `contract_id` derived from `(plan_id, step_id)`, triggers aligned with `compile.runtime.start` and scheduler dispatch (`runtime.action.dispatch` with `source=scheduler`), and a coarse `io_contract` over step inputs/outputs (`objective`, `step_inputs` → `step_artifacts`, `step_status`, `retrieval_snapshot`). `effects.network` reflects intent operating bounds and whether the step has a `retrieval_snapshot`. When an `intent` node is present, it gets `contract_category="acceptance"` with `acceptance.criteria` as `{id, evaluation_mode}` projections of active success criteria (from `IntentStore` hydration or `intent_node_properties` / plan step inputs).

## Operational Contract

Each `IRNode` may optionally carry `contract`. When present, it is an `OperationalContract` and represents typed operational semantics instead of burying runtime behavior in untyped `properties`.

`OperationalContract` fields:

- `contract_id`: stable identifier for the contract
- `contract_category`: one of `runtime`, `deployment`, `authorization`, `acceptance`
- `triggers`: non-empty list of typed trigger definitions
- `io_contract`: required declared input/output keys
- `state_machine`: optional state machine
- `runtime_budget`: optional runtime limits
- `acceptance`: optional JSON object of acceptance criteria metadata

Validation rules:

- `state_machine` is only allowed when `contract_category == "runtime"`.
- `StateTransition.trigger_id`, when set, must reference a declared trigger in `triggers`.
- trigger ids must be unique within a contract.
- transition ids must be unique within a state machine.

## Relationship Between `depends_on`, `effects`, and `contract`

These fields are complementary:

- `depends_on` answers structural readiness: which node ids must exist first.
- `effects` answers capability boundaries: what the node is allowed to do when it runs.
- `contract` answers operational semantics: what can trigger execution, what IO is expected, and what runtime/state constraints apply.

## Example JSON

Agent node with a runtime contract:

```json
{
  "id": "irn_agent_planner",
  "tenant_id": "tenant_1",
  "kind": "agent",
  "name": "PlannerAgent",
  "properties": {},
  "depends_on": ["irn_infra_runtime"],
  "effects": {
    "network": true,
    "fs_read": ["./workspace/"],
    "fs_write": [],
    "secrets": [],
    "tools": ["http_client"]
  },
  "provenance": [],
  "contract": {
    "contract_id": "planner-runtime-contract",
    "contract_category": "runtime",
    "triggers": [
      { "trigger_id": "t_start", "source": "compile.runtime.start", "details": {} }
    ],
    "io_contract": {
      "input_keys": ["objective", "knowledge"],
      "output_keys": ["agent_decision", "evidence"]
    }
  }
}
```

Infrastructure node with a deployment contract:

```json
{
  "id": "irn_infra_runtime",
  "tenant_id": "tenant_1",
  "kind": "infrastructure",
  "name": "RuntimeHost",
  "properties": {},
  "depends_on": [],
  "effects": {
    "network": false,
    "fs_read": [],
    "fs_write": [],
    "secrets": ["KUBECONFIG"],
    "tools": ["kubectl.apply"]
  },
  "provenance": [],
  "contract": {
    "contract_id": "runtime-host-deployment-contract",
    "contract_category": "deployment",
    "triggers": [
      { "trigger_id": "t_deploy", "source": "manual", "details": {} }
    ],
    "io_contract": {
      "input_keys": ["artifact_bundle"],
      "output_keys": ["service_endpoints"]
    }
  }
}
```

## Runtime bundle linkage

The compiled **`runtime_bundle`** JSON (see `RUNTIME_BUNDLE_V1`–`RUNTIME_BUNDLE_V4` in `src/akc/artifacts/schemas.py`) is the runtime handoff envelope. **`schema_version` / `schema_id`**: v1 remains accepted for older on-disk bundles; newer versions add fields while preserving the core JSON shape.

**`ControllerConfig.runtime_bundle_schema_version`** (see `src/akc/compile/controller_config.py`) selects the envelope version emitted by `run_runtime_bundle_pass`. Supported values are **`1`**, **`2`**, **`3`**, and **`4`** (default **`4`**). Semantics at a glance:

| Version | Notes |
|--------|--------|
| 1 | Legacy envelope; minimal IR linkage. |
| 2 | `akc:runtime_bundle:v2` — IR-spine-aligned semantics (`system_ir_ref`, reconciler hash expectations). |
| 3 | Adds optional `embed_system_ir` (inline `system_ir` for air-gapped / debugging). |
| 4 | Current default — adds `coordination_ref` / optional embedded `coordination_spec` for multi-agent handoff. |

Use **`1`–`2`** only for tests or replay of older on-disk artifacts.

**`system_ir_ref`** points at the persisted **`IRDocument`** for the same compile run:

- `path`: repo-relative path to the IR JSON (for example `.akc/ir/<run_id>.json`), resolved from the tenant/repo root that contains `.akc/runtime/<run_id>.runtime_bundle.json`.
- `fingerprint`: SHA-256 over the canonical IR JSON (must match the file).
- `format_version` / `schema_version`: IR document versioning (`IRDocument` fields).

Optional inline **`system_ir`** may embed the full document for small bundles; prefer sidecar + fingerprint for typical runs.

**`referenced_ir_nodes`** / **`referenced_contracts`** are filtered subgraphs of that IR (orchestration/coordination seeds + `depends_on` closure, plus `knowledge` hubs). **`deployment_intents`** is a denormalized projection of deployable nodes (`service`, `integration`, `infrastructure`, `agent`) for adapters; the reconciler prefers **IR node fingerprints** when a parsed `IRDocument` is loaded and `reconcile_desired_state_source` is `ir`.

### Contract expectations by node kind (runtime-facing)

| Node kind | `contract` role | Notes |
|-----------|------------------|-------|
| `workflow` | `contract_category="runtime"` | Required for orchestrated plan steps: stable `contract_id` from `(plan_id, step_id)`; triggers align with `compile.runtime.start` and scheduler dispatch; `io_contract` covers step inputs/outputs. |
| `intent` | `contract_category="acceptance"` | When present, `acceptance.criteria` lists `{id, evaluation_mode}` projections of active success criteria. |
| `service`, `integration`, `infrastructure`, `agent` | Often `deployment` or implicit via `deployment_intents` | Appear in runtime bundle projections; `effects` and `depends_on` inform reconciliation. |
| `knowledge` | Hub metadata | Fingerprints + snapshot path; hard constraints may tighten `runtime_policy_envelope`. |
| `policy` | Policy / authorization | Cross-edges to `knowledge` entities for constraint spine. |
| `entity` | `knowledge_constraint:*` / `knowledge_decision:*` | Assertion-level detail; not deployment targets. |

Compile-time checks for workflow/intent contracts are gated by **`ControllerConfig.ir_operational_structure_policy`** (`off` / `warn` / `error`). **`deployment_intents`** vs deployable **`referenced_ir_nodes`** can be checked when **`ControllerConfig.deployment_intents_ir_alignment_policy`** is set, or (when unset) when **`ir_operational_structure_policy`** is **`warn`** or **`error`** — see `validate_deployment_intents_align_with_ir` in `src/akc/compile/artifact_consistency.py`.
