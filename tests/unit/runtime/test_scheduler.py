from __future__ import annotations

from akc.runtime.models import RuntimeAction, RuntimeContext, RuntimeNodeRef
from akc.runtime.scheduler import InMemoryRuntimeScheduler


def _context() -> RuntimeContext:
    return RuntimeContext(
        tenant_id="tenant-a",
        repo_id="repo-a",
        run_id="compile-1",
        runtime_run_id="runtime-1",
        policy_mode="enforce",
        adapter_id="native",
    )


def _action(action_id: str) -> RuntimeAction:
    return RuntimeAction(
        action_id=action_id,
        action_type="workflow.execute",
        node_ref=RuntimeNodeRef(node_id=f"node-{action_id}", kind="workflow", contract_id="contract-1"),
        inputs_fingerprint=f"fp-{action_id}",
        idempotency_key=f"idem-{action_id}",
    )


def test_scheduler_orders_by_priority_enqueue_ts_and_action_id() -> None:
    scheduler = InMemoryRuntimeScheduler()
    context = _context()
    scheduler.enqueue(context=context, action=_action("b"), priority=1, enqueue_ts=20)
    scheduler.enqueue(context=context, action=_action("a"), priority=1, enqueue_ts=20)
    scheduler.enqueue(context=context, action=_action("c"), priority=0, enqueue_ts=30)

    first = scheduler.dequeue(
        context=context,
        now_ms=30,
        max_in_flight=10,
        max_in_flight_per_node_class=10,
    )
    scheduler.ack(context=context, action=first, node_class="workflow")  # type: ignore[arg-type]
    second = scheduler.dequeue(
        context=context,
        now_ms=30,
        max_in_flight=10,
        max_in_flight_per_node_class=10,
    )
    scheduler.ack(context=context, action=second, node_class="workflow")  # type: ignore[arg-type]
    third = scheduler.dequeue(
        context=context,
        now_ms=30,
        max_in_flight=10,
        max_in_flight_per_node_class=10,
    )

    assert first is not None and first.action_id == "c"
    assert second is not None and second.action_id == "a"
    assert third is not None and third.action_id == "b"


def test_scheduler_retries_with_backoff_then_dead_letters() -> None:
    scheduler = InMemoryRuntimeScheduler(max_attempts=2, retry_base_delay_ms=1000, retry_jitter_ceiling_ms=1)
    context = _context()
    action = _action("a")
    scheduler.enqueue(context=context, action=action, priority=0, enqueue_ts=10, node_class="workflow")

    dequeued = scheduler.dequeue(
        context=context,
        now_ms=10,
        max_in_flight=10,
        max_in_flight_per_node_class=10,
    )
    assert dequeued is not None
    retried = scheduler.retry(
        context=context,
        action=action,
        node_class="workflow",
        reason="backend_error",
        error="boom",
        now_ms=10,
    )
    assert retried is True
    assert (
        scheduler.dequeue(
            context=context,
            now_ms=100,
            max_in_flight=10,
            max_in_flight_per_node_class=10,
        )
        is None
    )
    dequeued_retry = scheduler.dequeue(
        context=context,
        now_ms=2000,
        max_in_flight=10,
        max_in_flight_per_node_class=10,
    )
    assert dequeued_retry is not None
    retried_again = scheduler.retry(
        context=context,
        action=action,
        node_class="workflow",
        reason="backend_error",
        error="boom",
        now_ms=2000,
    )
    assert retried_again is False
    dead_letters = scheduler.dead_letters(context=context)
    assert len(dead_letters) == 1
    assert dead_letters[0].reason == "backend_error"


def test_scheduler_snapshot_restore_requeues_inflight_for_at_least_once() -> None:
    scheduler = InMemoryRuntimeScheduler()
    context = _context()
    action = _action("a")
    scheduler.enqueue(context=context, action=action, priority=0, enqueue_ts=1, node_class="workflow")
    dequeued = scheduler.dequeue(
        context=context,
        now_ms=1,
        max_in_flight=10,
        max_in_flight_per_node_class=10,
    )
    assert dequeued is not None
    snapshot = scheduler.snapshot(context=context)

    restored = InMemoryRuntimeScheduler()
    restored.restore_snapshot(context=context, snapshot=snapshot)
    pending = restored.pending(context=context)

    assert len(pending) == 1
    assert pending[0].action_id == action.action_id


def test_scheduler_preserves_deterministic_order_under_large_queue_load() -> None:
    scheduler = InMemoryRuntimeScheduler()
    context = _context()
    expected_ids: list[str] = []

    for idx in range(128, 0, -1):
        action_id = f"action-{idx:03d}"
        expected_ids.append(action_id)
        scheduler.enqueue(
            context=context,
            action=_action(action_id),
            priority=idx % 4,
            enqueue_ts=1_000 + idx,
            node_class="workflow",
        )

    expected_ids.sort(key=lambda item: (int(item.split("-")[-1]) % 4, 1_000 + int(item.split("-")[-1]), item))
    observed_ids: list[str] = []
    while True:
        action = scheduler.dequeue(
            context=context,
            now_ms=5_000,
            max_in_flight=512,
            max_in_flight_per_node_class=512,
        )
        if action is None:
            break
        observed_ids.append(action.action_id)
        scheduler.ack(context=context, action=action, node_class="workflow")

    assert observed_ids == expected_ids
