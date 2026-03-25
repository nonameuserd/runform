# IR Schema

This document describes the stable persisted shape of the versioned IR in `src/akc/ir/schema.py`, with versioning constants in `src/akc/ir/versioning.py` and provenance shapes in `src/akc/ir/provenance.py`.

## Overview

`IRDocument` is the tenant- and repo-scoped compiler IR envelope. It serializes deterministically (sorted keys in JSON files, sorted `depends_on` / provenance / contract triggers) and carries:

- `schema_kind` — always `akc_ir` (`IR_SCHEMA_KIND`)
- `schema_version` — integer; current default `2` (`IR_SCHEMA_VERSION`)
- `format_version` — string; current default `2.0` (`IR_FORMAT_VERSION`)
- `tenant_id`
- `repo_id`
- `nodes`

**Supported version pairs** are enumerated in `SUPPORTED_IR_VERSIONS`: `(schema_version=1, format_version="1.0")` and `(2, "2.0")`. Loading or constructing an `IRDocument` with any other pair fails validation.

All nodes must share the document `tenant_id`, and node `id` values must be unique within the document. Document and node **fingerprints** (`IRDocument.fingerprint()`, `IRNode.fingerprint()`) are SHA-256 over canonical JSON from `to_json_obj()` via `stable_json_fingerprint`.

`IRNode` is the typed node unit inside the graph. It supports:

- stable ids via `stable_node_id()`
- typed `kind`
- `depends_on` dependency edges
- `effects` side-effect boundaries (`EffectAnnotation`: `network`, `fs_read`, `fs_write`, `secrets`, `tools`)
- `provenance` pointers (`ProvenancePointer`; see below)
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

### Provenance pointers (`ProvenancePointer`)

Each entry in `ir_node.provenance` is an object with:

- `tenant_id` — must match the enclosing `IRNode.tenant_id`
- `kind` — one of `doc_chunk`, `message`, `openapi_operation`, `file`, `other`
- `source_id` — non-empty string
- `locator` — optional string
- `sha256` — optional 64-character lowercase hex when set
- `metadata` — optional JSON object

**Plan → IR (`build_ir_document_from_plan` in `src/akc/compile/ir_builder.py`):** each `workflow` node emitted from a plan step gets a `contract` with `contract_category="runtime"`, stable `contract_id` derived from `(plan_id, step_id)` (prefix `opc_rt_`), two triggers — `source="compile.runtime.start"` and `source="scheduler"` with `details.event_type="runtime.action.dispatch"` — and an `io_contract` mapping plan step IO to keys `objective` / `step_inputs` → `step_artifacts` / `step_status` / `retrieval_snapshot`. `effects.network` reflects intent operating bounds and whether the step has a non-empty `retrieval_snapshot`. When an `intent` node is present, it gets `contract_category="acceptance"` with `acceptance.criteria` as `{id, evaluation_mode}` projections of active success criteria (from resolved `IntentSpecV1`, `IntentStore` hydration, or `intent_node_properties` / plan step inputs), plus triggers including `source="compile.acceptance.evaluate"` and IO keys `intent_context` → `acceptance_status`.

## Operational Contract

Each `IRNode` may optionally carry `contract`. When present, it is an `OperationalContract` and represents typed operational semantics instead of burying runtime behavior in untyped `properties`.

`OperationalContract` fields:

- `contract_id`: stable identifier for the contract
- `contract_category`: one of `runtime`, `deployment`, `authorization`, `acceptance`
- `triggers`: non-empty list of `ContractTrigger` objects (`trigger_id`, `source`, `details`)
- `io_contract`: required `IOContract` (see below); must always be set when `contract` is present
- `state_machine`: optional `StateMachineContract` (`initial_state`, non-empty `transitions`)
- `runtime_budget`: optional `OperationalBudget` — at least one of `max_seconds`, `max_steps`, `max_tokens` when present
- `acceptance`: optional JSON object (e.g. `criteria` for acceptance-category contracts)

`IOContract` fields:

- `input_keys`, `output_keys`: non-empty arrays of non-empty strings
- `schema`: optional JSON object — opaque key→JSONValue hints for shapes (not a separate JSON Schema dialect in code)

`StateTransition` fields (each entry in `state_machine.transitions`):

- `transition_id`, `from_state`, `to_state` — required strings
- `trigger_id` — optional; when set, must match a `trigger_id` on the parent `OperationalContract`
- `guard` — optional JSON object

Validation rules:

- `state_machine` is only allowed when `contract_category == "runtime"` (enforced in `IRNode.__post_init__` when a node is constructed or deserialized).
- `StateTransition.trigger_id`, when set, must reference a declared trigger in `triggers`.
- Trigger ids must be unique within a contract; transition ids must be unique within a state machine; a non-null `state_machine` must have non-empty transitions.

Serialization omits JSON keys whose values are `null` (e.g. `effects`, `contract`, optional contract subfields) for a smaller on-disk shape.

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

The compiled **`runtime_bundle`** JSON (see `RUNTIME_BUNDLE_V1`–`RUNTIME_BUNDLE_V4` in `src/akc/artifacts/schemas.py`) is the runtime handoff envelope. The bundle’s own JSON-schema version is distinct from the IR’s `schema_kind` / `schema_version` / `format_version`; runtime bundles use the integer **`schema_version`** field in the shared artifact envelope (default **`RUNTIME_BUNDLE_SCHEMA_VERSION` = 4).

**`ControllerConfig.runtime_bundle_schema_version`** (see `src/akc/compile/controller_config.py`) selects the envelope version emitted by `run_runtime_bundle_pass`. Supported values are **`1`**, **`2`**, **`3`**, and **`4`** (default **`4`**). Semantics at a glance:

| Version | Notes |
|--------|--------|
| 1 | Legacy envelope; core tenant/repo/run fields; `referenced_ir_nodes`, `referenced_contracts`, `spec_hashes`, `deployment_intents`, `runtime_policy_envelope`; optional `system_ir_ref` / inline `system_ir`. |
| 2 | Same JSON shape as v1; version bump marks bundles emitted with IR-spine / reconciler hash expectations. |
| 3 | Adds optional `embed_system_ir` (boolean) when the bundle may carry inline `system_ir` (air-gapped / debugging). |
| 4 | Current default — adds `coordination_ref` (path + fingerprint) and optional embedded `coordination_spec`; optional `reconcile_desired_state_source` (`ir` \| `deployment_intents`), `reconcile_deploy_targets_from_ir_only`, `deployment_intents_ir_alignment`, `deployment_provider_contract`, `workflow_execution_contract`, and `coordination_execution_contract` (parallel dispatch / in-flight limits, etc.). |

Use **`1`–`2`** only for tests or replay of older on-disk artifacts when you need historical compatibility.

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
