from __future__ import annotations

import json
from pathlib import Path

from akc.ir import ContractTrigger, IOContract, OperationalContract
from akc.runtime.adapters.native import NativeRuntimeAdapter
from akc.runtime.events import RuntimeEventBus
from akc.runtime.kernel import RuntimeKernel
from akc.runtime.models import RuntimeBundle, RuntimeBundleRef, RuntimeContext
from akc.runtime.scheduler import InMemoryRuntimeScheduler
from akc.runtime.state_store import InMemoryRuntimeStateStore
from akc.utils.fingerprint import stable_json_fingerprint


def _coordination_obj() -> dict[str, object]:
    return {
        "spec_version": 1,
        "run_id": "compile-1",
        "tenant_id": "tenant-a",
        "repo_id": "repo-a",
        "coordination_graph": {
            "nodes": [{"node_id": "workflow_000", "kind": "step"}],
            "edges": [
                {
                    "edge_id": "e0",
                    "kind": "depends_on",
                    "src_step_id": "workflow_000",
                    "dst_step_id": "workflow_000",
                }
            ],
        },
        "orchestration_bindings": [],
        "governance": {"max_steps": 1, "allowed_capabilities": [], "execution_allow_network": False},
    }


def _bundle_payload(*, coordination: dict[str, object], orchestration: dict[str, object]) -> dict[str, object]:
    return {
        "schema_version": 4,
        "schema_id": "akc:runtime_bundle:v4",
        "run_id": "compile-1",
        "tenant_id": "tenant-a",
        "repo_id": "repo-a",
        "referenced_ir_nodes": [
            {
                "id": "node-1",
                "tenant_id": "tenant-a",
                "kind": "workflow",
                "name": "Workflow 1",
                "properties": {"order_idx": 0},
                "depends_on": [],
                "contract": {
                    "contract_id": "contract-1",
                    "contract_category": "runtime",
                    "triggers": [
                        {
                            "trigger_id": "kernel_started",
                            "source": "runtime.kernel.started",
                            "details": {"event_type": "runtime.kernel.started"},
                        }
                    ],
                    "io_contract": {
                        "input_keys": ("runtime_run_id",),
                        "output_keys": ("action_id", "action_type", "adapter_id"),
                    },
                },
            }
        ],
        "referenced_contracts": [
            OperationalContract(
                contract_id="contract-1",
                contract_category="runtime",
                triggers=(
                    ContractTrigger(
                        trigger_id="kernel_started",
                        source="runtime.kernel.started",
                        details={"event_type": "runtime.kernel.started"},
                    ),
                ),
                io_contract=IOContract(
                    input_keys=("runtime_run_id",),
                    output_keys=("action_id", "action_type", "adapter_id"),
                ),
            ).to_json_obj()
        ],
        "deployment_intents": [],
        "coordination_spec": coordination,
        "spec_hashes": {
            "orchestration_spec_sha256": stable_json_fingerprint(orchestration),
            "coordination_spec_sha256": stable_json_fingerprint(coordination),
        },
        "intent_ref": {
            "intent_id": "i1",
            "stable_intent_sha256": "a" * 64,
            "semantic_fingerprint": "b" * 16,
            "goal_text_fingerprint": "c" * 16,
        },
        "intent_policy_projection": {
            "intent_id": "i1",
            "stable_intent_sha256": "a" * 64,
            "intent_semantic_fingerprint": "b" * 16,
            "intent_goal_text_fingerprint": "c" * 16,
            "success_criteria_summary": {"count": 0},
        },
        "runtime_policy_envelope": {
            "tenant_id": "tenant-a",
            "repo_id": "repo-a",
            "run_id": "compile-1",
            "tenant_isolation_required": True,
        },
        "coordination_execution_contract": {
            "parallel_dispatch_enabled": True,
            "max_in_flight_steps": 4,
            "max_in_flight_per_role": 2,
            "completion_fold_order": "coordination_step_id",
        },
        "reconcile_desired_state_source": "deployment_intents",
    }


def test_kernel_loads_embedded_coordination_and_enqueues_plan(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    orch = {
        "spec_version": 1,
        "tenant_id": "tenant-a",
        "repo_id": "repo-a",
        "run_id": "compile-1",
        "steps": [
            {
                "step_id": "workflow_000",
                "order_idx": 0,
                "agent_name": "a",
                "role": "writer",
                "inputs": {"ir_node_id": "node-1"},
            }
        ],
    }
    orch_path = repo / ".akc" / "orchestration" / "compile-1.orchestration.json"
    orch_path.parent.mkdir(parents=True)
    orch_path.write_text(json.dumps(orch), encoding="utf-8")

    coord = _coordination_obj()
    payload = _bundle_payload(coordination=coord, orchestration=orch)
    bundle_path = repo / ".akc" / "runtime" / "bundle.json"
    bundle_path.parent.mkdir(parents=True)
    bundle_path.write_text(json.dumps(payload), encoding="utf-8")
    bundle_hash = stable_json_fingerprint(json.loads(bundle_path.read_text(encoding="utf-8")))
    ref = RuntimeBundleRef(
        bundle_path=str(bundle_path),
        manifest_hash=bundle_hash,
        created_at=1,
        source_compile_run_id="compile-1",
    )
    ctx = RuntimeContext(
        tenant_id="tenant-a",
        repo_id="repo-a",
        run_id="compile-1",
        runtime_run_id="rt-1",
        policy_mode="enforce",
        adapter_id="native",
    )
    kernel = RuntimeKernel(
        context=ctx,
        bundle=RuntimeBundle(
            context=ctx,
            ref=ref,
            nodes=(),
            contract_ids=("contract-1",),
            metadata={},
        ),
        adapter=NativeRuntimeAdapter(),
        scheduler=InMemoryRuntimeScheduler(),
        state_store=InMemoryRuntimeStateStore(),
        event_bus=RuntimeEventBus(),
    )
    kernel.load_bundle(ref)
    assert kernel.bundle.metadata.get("coordination_runtime") is not None
    kernel.build_runtime_graph()
    checkpoint = kernel.recover_or_init_checkpoint()
    assert checkpoint.node_states.get("__coordination__", {}).get("plan_enqueued") is True
    pending = kernel.scheduler.pending(context=kernel.context)
    assert len(pending) == 1
    assert pending[0].action_type == "coordination.step"
    pc = pending[0].policy_context
    assert pc is not None
    assert pc.get("run_stage") == "coordination.step"
    assert pc.get("coordination_role_id") == "writer"
    assert "external_identity_metadata" in pc
    assert isinstance(pc.get("external_identity_metadata"), dict)
    event_types = [e.event_type for e in kernel.state_store.list_events(context=kernel.context)]
    assert "runtime.coordination.plan_enqueued" in event_types


def test_kernel_load_bundle_rejects_invalid_coordination_completion_fold_order(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    orch = {
        "spec_version": 1,
        "tenant_id": "tenant-a",
        "repo_id": "repo-a",
        "run_id": "compile-1",
        "steps": [{"step_id": "workflow_000", "order_idx": 0, "agent_name": "a", "role": "writer"}],
    }
    orch_path = repo / ".akc" / "orchestration" / "compile-1.orchestration.json"
    orch_path.parent.mkdir(parents=True)
    orch_path.write_text(json.dumps(orch), encoding="utf-8")
    coord = _coordination_obj()
    payload = _bundle_payload(coordination=coord, orchestration=orch)
    payload["coordination_execution_contract"] = {
        "parallel_dispatch_enabled": True,
        "max_in_flight_steps": 2,
        "max_in_flight_per_role": 1,
        "completion_fold_order": "arrival_order",
    }
    bundle_path = repo / ".akc" / "runtime" / "bundle.json"
    bundle_path.parent.mkdir(parents=True)
    bundle_path.write_text(json.dumps(payload), encoding="utf-8")
    bundle_hash = stable_json_fingerprint(json.loads(bundle_path.read_text(encoding="utf-8")))
    ref = RuntimeBundleRef(
        bundle_path=str(bundle_path),
        manifest_hash=bundle_hash,
        created_at=1,
        source_compile_run_id="compile-1",
    )
    ctx = RuntimeContext(
        tenant_id="tenant-a",
        repo_id="repo-a",
        run_id="compile-1",
        runtime_run_id="rt-1",
        policy_mode="enforce",
        adapter_id="native",
    )
    kernel = RuntimeKernel(
        context=ctx,
        bundle=RuntimeBundle(context=ctx, ref=ref, nodes=(), contract_ids=("contract-1",), metadata={}),
        adapter=NativeRuntimeAdapter(),
        scheduler=InMemoryRuntimeScheduler(),
        state_store=InMemoryRuntimeStateStore(),
        event_bus=RuntimeEventBus(),
    )
    try:
        kernel.load_bundle(ref)
        raise AssertionError("expected ValueError")
    except ValueError as exc:
        assert "completion_fold_order" in str(exc)
