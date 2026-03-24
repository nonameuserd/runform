from __future__ import annotations

from pathlib import Path

from akc.control.tracing import TraceSpan
from akc.ir import ContractTrigger, IOContract, OperationalContract
from akc.runtime.adapters.native import NativeRuntimeAdapter
from akc.runtime.events import RuntimeEventBus
from akc.runtime.kernel import RuntimeKernel
from akc.runtime.models import (
    RuntimeAction,
    RuntimeBundle,
    RuntimeBundleRef,
    RuntimeCheckpoint,
    RuntimeContext,
    RuntimeEvent,
    RuntimeNodeRef,
)
from akc.runtime.scheduler import InMemoryRuntimeScheduler, RuntimeQueueSnapshot
from akc.runtime.state_store import FileSystemRuntimeStateStore


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
            bundle_path=".akc/runtime/bundle.json",
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
                    "tenant_id": "tenant-a",
                    "kind": "workflow",
                    "name": "Workflow 1",
                    "properties": {},
                    "depends_on": [],
                    "contract": contract.to_json_obj(),
                }
            ],
            "referenced_contracts": [contract.to_json_obj()],
        },
    )


def test_filesystem_checkpoint_requeues_pending_without_queue_snapshot(tmp_path: Path) -> None:
    bundle = _bundle()
    store = FileSystemRuntimeStateStore(root=tmp_path)
    action = RuntimeAction(
        action_id="manual-1",
        action_type="workflow.execute",
        node_ref=bundle.nodes[0],
        inputs_fingerprint="fp",
        idempotency_key="idem-1",
    )
    checkpoint = RuntimeCheckpoint(
        checkpoint_id="init",
        cursor="start",
        pending_queue=(action,),
        node_states={"node-1": {"state": "ready"}},
    )
    store.save_checkpoint(context=bundle.context, checkpoint=checkpoint)

    kernel = RuntimeKernel(
        context=bundle.context,
        bundle=bundle,
        adapter=NativeRuntimeAdapter(),
        scheduler=InMemoryRuntimeScheduler(),
        state_store=store,
        event_bus=RuntimeEventBus(),
    )
    kernel.build_runtime_graph()
    kernel.recover_or_init_checkpoint()

    pending = kernel.scheduler.pending(context=bundle.context)
    assert len(pending) == 1
    assert pending[0].action_id == "manual-1"


def test_filesystem_recover_queue_snapshot_wins_over_checkpoint_pending_queue(tmp_path: Path) -> None:
    """When queue_snapshot.json exists, scheduler state comes from it, not checkpoint.pending_queue."""

    bundle = _bundle()
    store = FileSystemRuntimeStateStore(root=tmp_path)
    only_in_checkpoint = RuntimeAction(
        action_id="from-checkpoint-pending",
        action_type="workflow.execute",
        node_ref=bundle.nodes[0],
        inputs_fingerprint="fp-c",
        idempotency_key="idem-c",
    )
    checkpoint = RuntimeCheckpoint(
        checkpoint_id="init",
        cursor="start",
        pending_queue=(only_in_checkpoint,),
        node_states={"node-1": {"state": "ready"}},
    )
    store.save_checkpoint(context=bundle.context, checkpoint=checkpoint)
    store.save_queue_snapshot(context=bundle.context, snapshot=RuntimeQueueSnapshot())

    kernel = RuntimeKernel(
        context=bundle.context,
        bundle=bundle,
        adapter=NativeRuntimeAdapter(),
        scheduler=InMemoryRuntimeScheduler(),
        state_store=store,
        event_bus=RuntimeEventBus(),
    )
    kernel.build_runtime_graph()
    kernel.recover_or_init_checkpoint()

    assert kernel.scheduler.pending(context=bundle.context) == ()


def test_filesystem_checkpoint_save_leaves_no_tmp_files(tmp_path: Path) -> None:
    bundle = _bundle()
    store = FileSystemRuntimeStateStore(root=tmp_path)
    checkpoint = RuntimeCheckpoint(
        checkpoint_id="init",
        cursor="start",
        pending_queue=(),
        node_states={"node-1": {"state": "ready"}},
    )
    store.save_checkpoint(context=bundle.context, checkpoint=checkpoint)
    scope = store._scope_dir(bundle.context)
    assert not any(p.name.startswith(".checkpoint.json.tmp.") for p in scope.iterdir())


def test_filesystem_event_and_trace_writes_leave_no_tmp_files(tmp_path: Path) -> None:
    bundle = _bundle()
    store = FileSystemRuntimeStateStore(root=tmp_path)
    event = RuntimeEvent(
        event_id="evt-1",
        event_type="runtime.kernel.started",
        timestamp=1,
        context=bundle.context,
        payload={},
    )
    store.append_event(context=bundle.context, event=event)
    store.append_trace_span(
        context=bundle.context,
        span=TraceSpan(
            trace_id="t1",
            span_id="s1",
            parent_span_id=None,
            name="n",
            kind="internal",
            start_time_unix_nano=1,
            end_time_unix_nano=2,
            attributes=None,
            status="ok",
        ),
    )
    scope = store._scope_dir(bundle.context)
    assert not any(".tmp." in p.name for p in scope.iterdir())
