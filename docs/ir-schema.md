# IR Schema

This document describes the current persisted IR contract implemented under `src/akc/ir/` and the way compile/runtime surfaces consume it.

The IR is a versioned, tenant-scoped structural graph that sits between plan/intent compilation and later artifact passes such as orchestration, coordination, delivery projection, and runtime-bundle emission.

## Overview

The root object is `IRDocument`.

Current top-level fields:

- `schema_kind` — always `akc_ir`
- `schema_version` — current default `2`
- `format_version` — current default `2.0`
- `tenant_id`
- `repo_id`
- `nodes`

Supported version pairs are currently:

- `(1, "1.0")`
- `(2, "2.0")`

Construction and loading fail closed for unsupported pairs.

Core invariants:

- all nodes must share the document `tenant_id`
- node ids must be unique within the document
- JSON serialization is deterministic
- document and node fingerprints are SHA-256 over canonical JSON via `stable_json_fingerprint`

## Document model

`IRDocument` is defined in `src/akc/ir/schema.py`.

Important behavior:

- `to_json_obj()` sorts nodes by `id`
- `to_json_file()` writes deterministic JSON with sorted keys
- `from_json_obj()` requires `schema_kind == "akc_ir"`
- `from_json_file()` is the stable loader for persisted `.json` IR artifacts

The persisted compile-time IR sidecar is written at:

- `.akc/ir/<run_id>.json`

That same IR is also fingerprinted into the run manifest and referenced from runtime bundles through `system_ir_ref`.

## Node model

`IRNode` is the typed graph node unit.

Current fields:

- `id`
- `tenant_id`
- `kind`
- `name`
- `properties`
- `depends_on`
- optional `effects`
- optional `provenance`
- optional `contract`

Stable ids are typically built with `stable_node_id(kind=..., name=...)`.

Current allowed node kinds:

- `service`
- `workflow`
- `intent`
- `entity`
- `knowledge`
- `integration`
- `policy`
- `agent`
- `infrastructure`
- `other`

Important serialization rules:

- `depends_on` is normalized and sorted
- provenance rows are sorted deterministically
- `null` optional fields are omitted from the persisted JSON

## Effects and provenance

`effects` is an `EffectAnnotation` and captures runtime/policy-relevant capability boundaries:

- `network`
- `fs_read`
- `fs_write`
- `secrets`
- `tools`

`provenance` is a list of `ProvenancePointer` rows.

Current allowed provenance kinds:

- `doc_chunk`
- `message`
- `openapi_operation`
- `file`
- `other`

Each provenance pointer carries an explicit `tenant_id`, and node construction rejects provenance rows whose tenant does not match the enclosing node.

## Operational contracts

Nodes may carry a typed `contract` instead of burying operational meaning in opaque `properties`.

`OperationalContract` currently supports:

- `contract_id`
- `contract_category`
- `triggers`
- `io_contract`
- optional `state_machine`
- optional `runtime_budget`
- optional `acceptance`

Current allowed contract categories:

- `runtime`
- `deployment`
- `authorization`
- `acceptance`

Supporting contract types:

- `ContractTrigger`
- `IOContract`
- `StateMachineContract`
- `StateTransition`
- `OperationalBudget`

Important invariants:

- contracts must have at least one trigger
- contracts must always have an `io_contract`
- trigger ids must be unique within a contract
- `state_machine` is only valid for `contract_category="runtime"`
- transition ids must be unique within a state machine
- when a transition has `trigger_id`, it must reference a declared contract trigger
- `OperationalBudget` must set at least one of `max_seconds`, `max_steps`, or `max_tokens`

## Compile-time IR emission

The shared PlanState-to-IR builder lives in:

- `src/akc/compile/ir_builder.py`

Current compile behavior:

- compile emits a first-class `intent` node when intent context is available
- each plan step becomes a `workflow` node
- workflow nodes carry runtime contracts derived from `(plan_id, step_id)`
- intent nodes carry acceptance contracts derived from active success criteria
- knowledge-layer compilation can emit a `knowledge` hub node plus linked constraint/decision entities

The IR is not just prompt context. It is a shared structural spine used by multiple later passes.

## Runtime-facing linkage

The runtime bundle is a separate artifact contract, but it directly references the persisted IR.

Current linkage points:

- `system_ir_ref`
- optional inline `system_ir`
- `referenced_ir_nodes`
- `referenced_contracts`
- `deployment_intents`

Current runtime-bundle schema versions supported by compile are `1` through `4`, with `4` as the default.

Important current expectations:

- `system_ir_ref.path` points at `.akc/ir/<run_id>.json`
- `system_ir_ref.fingerprint` must match the IR document fingerprint
- `system_ir_ref.format_version` and `schema_version` must match the referenced IR
- `referenced_ir_nodes` is a filtered runtime-facing projection of the full IR
- `deployment_intents` is a denormalized deployable projection used by adapters and reconciliation paths
- when enabled, compile can embed the full IR inline with `embed_system_ir=true`

## Current runtime-bundle additions in v4

Runtime-bundle v4 is the current default and adds more IR-adjacent control data, including:

- `coordination_ref`
- optional `coordination_spec`
- `reconcile_desired_state_source`
- `reconcile_deploy_targets_from_ir_only`
- `deployment_intents_ir_alignment`
- `deployment_provider_contract`
- `workflow_execution_contract`
- `coordination_execution_contract`

Those fields live on the runtime bundle, not on `IRDocument` itself, but they assume the IR remains the structural source of truth.

## Validation and policy hooks

IR structure is not treated as best-effort metadata.

Current compile-time checks include:

- graph/version validation in `src/akc/ir/schema.py`
- operational-structure validation in `src/akc/compile/ir_operational_validate.py`
- runtime-bundle consistency checks in `src/akc/compile/artifact_consistency.py`
- optional deployment-intent alignment checks between `deployment_intents` and deployable `referenced_ir_nodes`

Relevant controller policies:

- `ir_operational_structure_policy`
- `deployment_intents_ir_alignment_policy`

These checks are what keep runtime bundles, reconciliation, and delivery projection aligned with the emitted IR rather than drifting into separate ad-hoc shapes.

## Minimal example

```json
{
  "schema_kind": "akc_ir",
  "schema_version": 2,
  "format_version": "2.0",
  "tenant_id": "tenant_1",
  "repo_id": "repo_1",
  "nodes": [
    {
      "id": "irn_example",
      "tenant_id": "tenant_1",
      "kind": "workflow",
      "name": "CompileStep",
      "properties": {},
      "depends_on": [],
      "contract": {
        "contract_id": "opc_rt_example",
        "contract_category": "runtime",
        "triggers": [
          {
            "trigger_id": "t_compile_runtime_start",
            "source": "compile.runtime.start",
            "details": {}
          }
        ],
        "io_contract": {
          "input_keys": ["objective"],
          "output_keys": ["step_status"]
        }
      }
    }
  ]
}
```

## Related docs

- [architecture.md](architecture.md)
- [artifact-contracts.md](artifact-contracts.md)
- [runtime-execution.md](runtime-execution.md)
