from __future__ import annotations

from akc.ir import (
    ContractTrigger,
    IOContract,
    OperationalContract,
    StateMachineContract,
    StateTransition,
)
from akc.runtime.adapters.native import NativeRuntimeAdapter
from akc.runtime.events import RuntimeEventBus
from akc.runtime.kernel import RuntimeKernel
from akc.runtime.models import (
    RuntimeBundle,
    RuntimeBundleRef,
    RuntimeCheckpoint,
    RuntimeContext,
    RuntimeEvent,
    RuntimeNodeRef,
)
from akc.runtime.policy import RuntimeScopeMismatchError
from akc.runtime.scheduler import InMemoryRuntimeScheduler
from akc.runtime.state_store import InMemoryRuntimeStateStore


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
        state_machine=StateMachineContract(
            initial_state="queued",
            transitions=(
                StateTransition(
                    transition_id="t-1",
                    from_state="queued",
                    to_state="running",
                    trigger_id="kernel_started",
                ),
            ),
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


def _kernel(bundle: RuntimeBundle) -> RuntimeKernel:
    return RuntimeKernel(
        context=bundle.context,
        bundle=bundle,
        adapter=NativeRuntimeAdapter(),
        scheduler=InMemoryRuntimeScheduler(),
        state_store=InMemoryRuntimeStateStore(),
        event_bus=RuntimeEventBus(),
    )


def test_kernel_applies_declared_state_machine_transition() -> None:
    bundle = _bundle()
    kernel = _kernel(bundle)
    graph = kernel.build_runtime_graph()
    event = kernel.emit_event(
        event_type="runtime.kernel.started",
        payload={"runtime_run_id": bundle.context.runtime_run_id},
    )
    action = kernel._action_from_event(event=event, node=graph.nodes["node-1"])  # noqa: SLF001
    kernel._pending_inputs[action.action_id] = {  # noqa: SLF001
        "runtime_run_id": bundle.context.runtime_run_id,
        "__event_type": event.event_type,
        "__timestamp": event.timestamp,
    }
    checkpoint = RuntimeCheckpoint(
        checkpoint_id="cp-1",
        cursor="event:0",
        pending_queue=(),
        node_states={"node-1": {"state": "queued"}},
    )

    transition = kernel.apply_transition(
        action=action,
        result=NativeRuntimeAdapter().execute_action(context=bundle.context, action=action),
        checkpoint=checkpoint,
        graph_node=graph.nodes["node-1"],
    )

    assert transition is not None
    assert transition.from_state == "queued"
    assert transition.to_state == "running"
    assert transition.trigger_id == "kernel_started"


def test_kernel_rejects_cross_tenant_event_injection() -> None:
    bundle = _bundle()
    kernel = _kernel(bundle)
    checkpoint = kernel.initialize()
    foreign_event = RuntimeEvent(
        event_id="evt-foreign",
        event_type="runtime.kernel.started",
        timestamp=1,
        context=RuntimeContext(
            tenant_id="tenant-b",
            repo_id=bundle.context.repo_id,
            run_id=bundle.context.run_id,
            runtime_run_id=bundle.context.runtime_run_id,
            policy_mode=bundle.context.policy_mode,
            adapter_id=bundle.context.adapter_id,
        ),
        payload={"runtime_run_id": bundle.context.runtime_run_id},
    )
    key = kernel.state_store._key(bundle.context)  # type: ignore[attr-defined]  # noqa: SLF001
    kernel.state_store._events[key] = [foreign_event]  # type: ignore[attr-defined]  # noqa: SLF001

    try:
        kernel.poll_events(checkpoint)
    except RuntimeScopeMismatchError:
        pass
    else:
        raise AssertionError("expected cross-tenant event injection to be rejected")


def test_kernel_deduplicates_seen_event_ids_across_polls() -> None:
    bundle = _bundle()
    kernel = _kernel(bundle)
    checkpoint = kernel.initialize()
    event = RuntimeEvent(
        event_id="evt-1",
        event_type="runtime.kernel.started",
        timestamp=1,
        context=bundle.context,
        payload={"runtime_run_id": bundle.context.runtime_run_id},
    )
    key = kernel.state_store._key(bundle.context)  # type: ignore[attr-defined]  # noqa: SLF001
    kernel.state_store._events[key] = [event, event]  # type: ignore[attr-defined]  # noqa: SLF001

    _, checkpoint = kernel.poll_events(checkpoint)
    _, checkpoint = kernel.poll_events(checkpoint)

    assert len(checkpoint.pending_queue) == 1
    assert checkpoint.pending_queue[0].action_id == "node-1:evt-1"
