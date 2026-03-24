from __future__ import annotations

import pytest

from akc.ir import ContractTrigger, IOContract, OperationalContract
from akc.runtime.init import create_native_runtime
from akc.runtime.models import RuntimeBundle, RuntimeBundleRef, RuntimeContext, RuntimeNodeRef


def _bundle() -> RuntimeBundle:
    context = RuntimeContext(
        tenant_id="tenant-a",
        repo_id="repo-a",
        run_id="compile-1",
        runtime_run_id="runtime-1",
        policy_mode="enforce",
        adapter_id="native",
    )
    contract = OperationalContract(
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
    )
    return RuntimeBundle(
        context=context,
        ref=RuntimeBundleRef(
            bundle_path=".akc/runtime/runtime_bundle.json",
            manifest_hash="a" * 64,
            created_at=1,
            source_compile_run_id="compile-1",
        ),
        nodes=(RuntimeNodeRef(node_id="node-1", kind="workflow", contract_id="contract-1"),),
        contract_ids=("contract-1",),
        metadata={
            "referenced_ir_nodes": [
                {
                    "id": "node-1",
                    "tenant_id": context.tenant_id,
                    "kind": "workflow",
                    "name": "Workflow 1",
                    "properties": {"order_idx": 0},
                    "depends_on": [],
                    "contract": contract.to_json_obj(),
                }
            ],
            "referenced_contracts": [contract.to_json_obj()],
        },
    )


def test_runtime_e2e_native_reaches_terminal_state() -> None:
    kernel = create_native_runtime(_bundle())

    result = kernel.run_until_terminal(max_iterations=5)

    assert result.status == "terminal"
    assert result.last_checkpoint.node_states["node-1"]["state"] == "completed"
    assert any(event.event_type == "runtime.action.completed" for event in result.emitted_events)


def test_runtime_e2e_native_honors_intent_policy_projection_over_generic_defaults() -> None:
    bundle = _bundle()
    bundle = RuntimeBundle(
        context=bundle.context,
        ref=bundle.ref,
        nodes=bundle.nodes,
        contract_ids=bundle.contract_ids,
        policy_envelope=bundle.policy_envelope,
        metadata={
            **dict(bundle.metadata),
            "intent_policy_projection": {
                "policies": [
                    {
                        "id": "policy.runtime.dispatch_guardrail",
                        "metadata": {"runtime_deny_actions": ["runtime.action.dispatch"]},
                    }
                ]
            },
        },
    )
    kernel = create_native_runtime(bundle)

    # Default-deny may surface on checkpoint/evidence writes before dispatch.
    with pytest.raises(PermissionError, match="runtime policy blocked"):
        kernel.run_until_terminal(max_iterations=5)
