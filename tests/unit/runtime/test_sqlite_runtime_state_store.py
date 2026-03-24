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
from akc.runtime.scheduler import InMemoryRuntimeScheduler, RuntimeQueueSnapshot, ScheduledRuntimeAction
from akc.runtime.state_store import SqliteRuntimeStateStore, runtime_state_scope_dir


def _context(*, runtime_run_id: str = "runtime-1") -> RuntimeContext:
    return RuntimeContext(
        tenant_id="tenant-a",
        repo_id="repo-a",
        run_id="compile-1",
        runtime_run_id=runtime_run_id,
        policy_mode="enforce",
        adapter_id="native",
    )


def _bundle(*, runtime_run_id: str = "runtime-1") -> RuntimeBundle:
    context = _context(runtime_run_id=runtime_run_id)
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


def test_sqlite_runtime_state_store_roundtrip(tmp_path: Path) -> None:
    bundle = _bundle()
    store = SqliteRuntimeStateStore(root=tmp_path)
    scope = runtime_state_scope_dir(root=tmp_path, context=bundle.context)
    assert (scope / "runtime_state.sqlite3") == store._db_path(bundle.context)

    action = RuntimeAction(
        action_id="a1",
        action_type="workflow.execute",
        node_ref=bundle.nodes[0],
        inputs_fingerprint="fp",
        idempotency_key="k1",
    )
    checkpoint = RuntimeCheckpoint(
        checkpoint_id="c1",
        cursor="start",
        pending_queue=(action,),
        node_states={"node-1": {"state": "ready"}},
        replay_token="rt",
    )
    store.save_checkpoint(context=bundle.context, checkpoint=checkpoint)
    loaded_cp = store.load_checkpoint(context=bundle.context)
    assert loaded_cp is not None
    assert loaded_cp.checkpoint_id == "c1"
    assert len(loaded_cp.pending_queue) == 1
    assert loaded_cp.pending_queue[0].action_id == "a1"

    snapshot = RuntimeQueueSnapshot(
        queued=(
            ScheduledRuntimeAction(
                action=action,
                priority=0,
                enqueue_ts=1,
                node_class="workflow",
                attempt=0,
                available_at=0,
            ),
        ),
        in_flight=(),
        dead_letters=(),
    )
    store.save_queue_snapshot(context=bundle.context, snapshot=snapshot)
    loaded_q = store.load_queue_snapshot(context=bundle.context)
    assert loaded_q is not None
    assert len(loaded_q.queued) == 1

    ev1 = RuntimeEvent(
        event_id="e1",
        event_type="runtime.kernel.started",
        timestamp=1,
        context=bundle.context,
        payload={"x": 1},
    )
    ev2 = RuntimeEvent(
        event_id="e2",
        event_type="runtime.action.completed",
        timestamp=2,
        context=bundle.context,
        payload={},
    )
    store.append_event(context=bundle.context, event=ev1)
    store.append_event(context=bundle.context, event=ev2)
    events = store.list_events(context=bundle.context)
    assert [e.event_id for e in events] == ["e1", "e2"]

    span = TraceSpan(
        trace_id="t1",
        span_id="s1",
        parent_span_id=None,
        name="n",
        kind="internal",
        start_time_unix_nano=10,
        end_time_unix_nano=20,
        attributes=None,
        status="ok",
    )
    store.append_trace_span(context=bundle.context, span=span)
    spans = store.list_trace_spans(context=bundle.context)
    assert len(spans) == 1
    assert spans[0].trace_id == "t1"


def test_sqlite_runtime_state_store_scopes_per_runtime_run(tmp_path: Path) -> None:
    b1 = _bundle(runtime_run_id="r1")
    b2 = _bundle(runtime_run_id="r2")
    store = SqliteRuntimeStateStore(root=tmp_path)
    store.save_checkpoint(
        context=b1.context,
        checkpoint=RuntimeCheckpoint(
            checkpoint_id="a",
            cursor="x",
            pending_queue=(),
            node_states={},
        ),
    )
    store.save_checkpoint(
        context=b2.context,
        checkpoint=RuntimeCheckpoint(
            checkpoint_id="b",
            cursor="y",
            pending_queue=(),
            node_states={},
        ),
    )
    c1 = store.load_checkpoint(context=b1.context)
    c2 = store.load_checkpoint(context=b2.context)
    assert c1 is not None and c1.checkpoint_id == "a"
    assert c2 is not None and c2.checkpoint_id == "b"


def test_sqlite_kernel_recover_requeues_pending_without_queue_snapshot(tmp_path: Path) -> None:
    bundle = _bundle()
    store = SqliteRuntimeStateStore(root=tmp_path)
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
