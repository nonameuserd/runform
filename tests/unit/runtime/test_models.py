from __future__ import annotations

import pytest

from akc.ir import (
    ContractTrigger,
    IOContract,
    OperationalBudget,
    OperationalContract,
    StateMachineContract,
    StateTransition,
)
from akc.runtime.contracts import (
    enforce_action_budget,
    is_allowed_transition,
    map_operational_contract,
    validate_action_inputs,
    validate_action_outputs,
)
from akc.runtime.models import (
    ReconcileCondition,
    ReconcileOperation,
    ReconcilePlan,
    ReconcileStatus,
    RuntimeAction,
    RuntimeActionResult,
    RuntimeBundleRef,
    RuntimeCheckpoint,
    RuntimeContext,
    RuntimeEvent,
    RuntimeNodeRef,
    RuntimeTransition,
    build_reconcile_conditions,
)


def test_runtime_models_round_trip() -> None:
    context = RuntimeContext(
        tenant_id="tenant-a",
        repo_id="repo-a",
        run_id="compile-1",
        runtime_run_id="runtime-1",
        policy_mode="enforce",
        adapter_id="native",
    )
    node_ref = RuntimeNodeRef(node_id="node-1", kind="workflow", contract_id="contract-1")
    action = RuntimeAction(
        action_id="action-1",
        action_type="dispatch",
        node_ref=node_ref,
        inputs_fingerprint="abc123",
        idempotency_key="idem-1",
    )
    checkpoint = RuntimeCheckpoint(
        checkpoint_id="cp-1",
        cursor="cursor-1",
        pending_queue=(action,),
        node_states={"node-1": {"state": "running"}},
        replay_token="rt-1",
    )
    event = RuntimeEvent(
        event_id="evt-1",
        event_type="runtime.action.completed",
        timestamp=123,
        context=context,
        payload={"result": {"status": "ok"}},
    )

    assert RuntimeContext.from_json_obj(context.to_json_obj()) == context
    assert RuntimeNodeRef.from_json_obj(node_ref.to_json_obj()) == node_ref
    assert RuntimeAction.from_json_obj(action.to_json_obj()) == action
    assert RuntimeCheckpoint.from_json_obj(checkpoint.to_json_obj()) == checkpoint
    assert RuntimeEvent.from_json_obj(event.to_json_obj()) == event


def test_runtime_transition_and_reconcile_models_round_trip() -> None:
    transition = RuntimeTransition(
        from_state="queued",
        to_state="running",
        trigger_id="start",
        transition_id="t-1",
        occurred_at=10,
    )
    operation = ReconcileOperation(
        operation_id="op-1",
        operation_type="update",
        target="svc/api",
        payload={"replicas": 2},
    )
    plan = ReconcilePlan(resource_id="svc/api", desired_hash="hash-1", operations=(operation,))
    conds = build_reconcile_conditions(
        converged=True,
        health_status="healthy",
        last_error=None,
        rollback_triggered=False,
    )
    status = ReconcileStatus(
        resource_id="svc/api",
        observed_hash="hash-1",
        health_status="healthy",
        converged=True,
        desired_hash="hash-1",
        hash_matched=True,
        health_gate_passed=True,
        last_error=None,
        conditions=conds,
    )
    bundle_ref = RuntimeBundleRef(
        bundle_path=".akc/runtime/runtime_bundle.json",
        manifest_hash="a" * 64,
        created_at=1,
        source_compile_run_id="compile-1",
    )

    assert RuntimeTransition.from_json_obj(transition.to_json_obj()) == transition
    assert ReconcileOperation.from_json_obj(operation.to_json_obj()) == operation
    assert ReconcilePlan.from_json_obj(plan.to_json_obj()) == plan
    assert ReconcileStatus.from_json_obj(status.to_json_obj()) == status
    assert RuntimeBundleRef.from_json_obj(bundle_ref.to_json_obj()) == bundle_ref


def test_reconcile_condition_json_ordering_is_stable() -> None:
    cond = ReconcileCondition(type="degraded", status="false", reason=None, message=None)
    assert list(cond.to_json_obj().keys()) == ["type", "status"]


def test_build_reconcile_conditions_marks_stalled_on_observe_only_error() -> None:
    conds = build_reconcile_conditions(
        converged=False,
        health_status="unknown",
        last_error="observe-only deployment provider: apply disabled",
        rollback_triggered=False,
    )
    by_type = {c.type: c.status for c in conds}
    assert by_type["stalled"] == "true"
    assert by_type["progressing"] == "false"


def test_contract_mapping_exposes_exact_runtime_semantics() -> None:
    contract = OperationalContract(
        contract_id="contract-1",
        contract_category="runtime",
        triggers=(
            ContractTrigger(
                trigger_id="on_dispatch",
                source="scheduler",
                details={"event_type": "runtime.action.dispatch"},
            ),
        ),
        io_contract=IOContract(input_keys=("input_id",), output_keys=("result_id",)),
        state_machine=StateMachineContract(
            initial_state="queued",
            transitions=(
                StateTransition(
                    transition_id="t-1",
                    from_state="queued",
                    to_state="running",
                    trigger_id="on_dispatch",
                ),
            ),
        ),
        runtime_budget=OperationalBudget(max_seconds=2),
    )

    mapping = map_operational_contract(contract)
    event = RuntimeEvent(
        event_id="evt-1",
        event_type="runtime.action.dispatch",
        timestamp=1,
        context=RuntimeContext(
            tenant_id="tenant-a",
            repo_id="repo-a",
            run_id="compile-1",
            runtime_run_id="runtime-1",
            policy_mode="enforce",
            adapter_id="native",
        ),
        payload={"source": "scheduler"},
    )

    assert mapping.io_contract == contract.io_contract
    assert mapping.runtime_budget == contract.runtime_budget
    assert mapping.event_match_predicates["on_dispatch"](event) is True
    assert is_allowed_transition(
        mapping=mapping,
        from_state="queued",
        to_state="running",
        trigger_id="on_dispatch",
    )


def test_contract_mapping_validates_io_and_budget() -> None:
    contract = OperationalContract(
        contract_id="contract-2",
        contract_category="runtime",
        triggers=(ContractTrigger(trigger_id="tick", source="clock"),),
        io_contract=IOContract(input_keys=("input_id",), output_keys=("output_id",)),
        runtime_budget=OperationalBudget(max_seconds=1),
    )
    mapping = map_operational_contract(contract)

    validate_action_inputs(mapping=mapping, payload={"input_id": "x"})
    validate_action_outputs(mapping=mapping, payload={"output_id": "y"})
    enforce_action_budget(
        mapping=mapping,
        result=RuntimeActionResult(status="succeeded", outputs={"output_id": "y"}, duration_ms=100),
    )

    with pytest.raises(ValueError, match="inputs missing keys"):
        validate_action_inputs(mapping=mapping, payload={})
    with pytest.raises(ValueError, match="outputs missing keys"):
        validate_action_outputs(mapping=mapping, payload={})
    with pytest.raises(ValueError, match="exceeded max_seconds"):
        enforce_action_budget(
            mapping=mapping,
            result=RuntimeActionResult(
                status="succeeded",
                outputs={"output_id": "y"},
                duration_ms=2000,
            ),
        )
