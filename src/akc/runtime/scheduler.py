from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from heapq import heappop, heappush
from typing import Literal, Protocol

from akc.runtime.models import RuntimeAction, RuntimeContext

DeadLetterReason = Literal[
    "policy_denied",
    "budget_exceeded",
    "contract_violation",
    "backend_error",
]


@dataclass(frozen=True, slots=True)
class ScheduledRuntimeAction:
    action: RuntimeAction
    priority: int
    enqueue_ts: int
    node_class: str
    attempt: int = 0
    available_at: int = 0

    def queue_key(self) -> tuple[int, int, str]:
        return (int(self.priority), int(self.enqueue_ts), self.action.action_id)


@dataclass(frozen=True, slots=True)
class RuntimeDeadLetter:
    action: RuntimeAction
    reason: DeadLetterReason
    attempts: int
    error: str
    dead_lettered_at: int
    node_class: str


@dataclass(frozen=True, slots=True)
class RuntimeQueueSnapshot:
    queued: tuple[ScheduledRuntimeAction, ...] = ()
    in_flight: tuple[ScheduledRuntimeAction, ...] = ()
    dead_letters: tuple[RuntimeDeadLetter, ...] = ()


class RuntimeScheduler(Protocol):
    def enqueue(
        self,
        *,
        context: RuntimeContext,
        action: RuntimeAction,
        priority: int = 0,
        enqueue_ts: int = 0,
        node_class: str = "default",
    ) -> None: ...

    def dequeue(
        self,
        *,
        context: RuntimeContext,
        now_ms: int,
        max_in_flight: int,
        max_in_flight_per_node_class: int,
    ) -> RuntimeAction | None: ...

    def ack(self, *, context: RuntimeContext, action: RuntimeAction, node_class: str) -> None: ...

    def retry(
        self,
        *,
        context: RuntimeContext,
        action: RuntimeAction,
        node_class: str,
        reason: DeadLetterReason,
        error: str,
        now_ms: int,
    ) -> bool: ...

    def dead_letter(
        self,
        *,
        context: RuntimeContext,
        action: RuntimeAction,
        node_class: str,
        reason: DeadLetterReason,
        error: str,
        now_ms: int,
    ) -> None: ...

    def pending(self, *, context: RuntimeContext) -> tuple[RuntimeAction, ...]: ...

    def snapshot(self, *, context: RuntimeContext) -> RuntimeQueueSnapshot: ...

    def restore_snapshot(self, *, context: RuntimeContext, snapshot: RuntimeQueueSnapshot) -> None: ...

    def dead_letters(self, *, context: RuntimeContext) -> tuple[RuntimeDeadLetter, ...]: ...


@dataclass(slots=True)
class InMemoryRuntimeScheduler(RuntimeScheduler):
    max_attempts: int = 3
    retry_base_delay_ms: int = 1000
    retry_jitter_ceiling_ms: int = 250
    _queues: dict[str, list[tuple[tuple[int, int, str], ScheduledRuntimeAction]]] = field(default_factory=dict)
    _in_flight: dict[str, dict[str, ScheduledRuntimeAction]] = field(default_factory=dict)
    _dead_letters: dict[str, list[RuntimeDeadLetter]] = field(default_factory=dict)
    _in_flight_total: dict[str, int] = field(default_factory=dict)
    _in_flight_by_node_class: dict[str, dict[str, int]] = field(default_factory=dict)

    def _key(self, context: RuntimeContext) -> str:
        return f"{context.tenant_id.strip()}::{context.repo_id.strip()}::{context.runtime_run_id.strip()}"

    def _jitter_ms(self, *, action_id: str, attempt: int) -> int:
        raw = f"{action_id}:{attempt}".encode()
        digest = hashlib.sha256(raw).hexdigest()
        return int(digest[:8], 16) % max(int(self.retry_jitter_ceiling_ms), 1)

    def _push(self, *, key: str, scheduled: ScheduledRuntimeAction) -> None:
        heappush(self._queues.setdefault(key, []), (scheduled.queue_key(), scheduled))

    def enqueue(
        self,
        *,
        context: RuntimeContext,
        action: RuntimeAction,
        priority: int = 0,
        enqueue_ts: int = 0,
        node_class: str = "default",
    ) -> None:
        scheduled = ScheduledRuntimeAction(
            action=action,
            priority=int(priority),
            enqueue_ts=int(enqueue_ts),
            node_class=str(node_class).strip() or "default",
            attempt=0,
            available_at=int(enqueue_ts),
        )
        self._push(key=self._key(context), scheduled=scheduled)

    def dequeue(
        self,
        *,
        context: RuntimeContext,
        now_ms: int,
        max_in_flight: int,
        max_in_flight_per_node_class: int,
    ) -> RuntimeAction | None:
        key = self._key(context)
        queue = self._queues.get(key)
        if not queue:
            return None
        total = self._in_flight_total.get(key, 0)
        if total >= max_in_flight:
            return None
        deferred: list[tuple[tuple[int, int, str], ScheduledRuntimeAction]] = []
        selected: ScheduledRuntimeAction | None = None
        per_class = self._in_flight_by_node_class.setdefault(key, {})
        while queue:
            queue_key, candidate = heappop(queue)
            if candidate.available_at > int(now_ms):
                deferred.append((queue_key, candidate))
                continue
            class_in_flight = per_class.get(candidate.node_class, 0)
            if class_in_flight < max_in_flight_per_node_class:
                selected = candidate
                self._in_flight.setdefault(key, {})[candidate.action.action_id] = candidate
                self._in_flight_total[key] = total + 1
                per_class[candidate.node_class] = class_in_flight + 1
                break
            deferred.append((queue_key, candidate))
        for item in deferred:
            heappush(queue, item)
        return selected.action if selected is not None else None

    def ack(self, *, context: RuntimeContext, action: RuntimeAction, node_class: str) -> None:
        self._remove_in_flight(context=context, action=action, node_class=node_class)

    def retry(
        self,
        *,
        context: RuntimeContext,
        action: RuntimeAction,
        node_class: str,
        reason: DeadLetterReason,
        error: str,
        now_ms: int,
    ) -> bool:
        key = self._key(context)
        scheduled = self._in_flight.get(key, {}).get(action.action_id)
        if scheduled is None:
            return False
        next_attempt = int(scheduled.attempt) + 1
        self._remove_in_flight(context=context, action=action, node_class=node_class)
        if next_attempt >= int(self.max_attempts):
            self.dead_letter(
                context=context,
                action=action,
                node_class=node_class,
                reason=reason,
                error=error,
                now_ms=now_ms,
            )
            return False
        backoff = int(self.retry_base_delay_ms) * (2 ** max(next_attempt - 1, 0))
        delayed = ScheduledRuntimeAction(
            action=scheduled.action,
            priority=scheduled.priority,
            enqueue_ts=scheduled.enqueue_ts,
            node_class=scheduled.node_class,
            attempt=next_attempt,
            available_at=int(now_ms) + backoff + self._jitter_ms(action_id=action.action_id, attempt=next_attempt),
        )
        self._push(key=key, scheduled=delayed)
        return True

    def dead_letter(
        self,
        *,
        context: RuntimeContext,
        action: RuntimeAction,
        node_class: str,
        reason: DeadLetterReason,
        error: str,
        now_ms: int,
    ) -> None:
        key = self._key(context)
        scheduled = self._in_flight.get(key, {}).get(action.action_id)
        attempts = int(scheduled.attempt) + 1 if scheduled is not None else 1
        self._remove_in_flight(context=context, action=action, node_class=node_class)
        self._dead_letters.setdefault(key, []).append(
            RuntimeDeadLetter(
                action=action,
                reason=reason,
                attempts=attempts,
                error=str(error),
                dead_lettered_at=int(now_ms),
                node_class=str(node_class).strip() or "default",
            )
        )

    def pending(self, *, context: RuntimeContext) -> tuple[RuntimeAction, ...]:
        queue = self._queues.get(self._key(context), [])
        return tuple(scheduled.action for _, scheduled in sorted(queue, key=lambda item: item[0]))

    def snapshot(self, *, context: RuntimeContext) -> RuntimeQueueSnapshot:
        key = self._key(context)
        queued = tuple(scheduled for _, scheduled in sorted(self._queues.get(key, []), key=lambda item: item[0]))
        in_flight = tuple(
            scheduled
            for _, scheduled in sorted(
                self._in_flight.get(key, {}).items(),
                key=lambda item: item[0],
            )
        )
        return RuntimeQueueSnapshot(
            queued=queued,
            in_flight=in_flight,
            dead_letters=tuple(
                sorted(
                    self._dead_letters.get(key, []),
                    key=lambda item: (item.dead_lettered_at, item.action.action_id),
                )
            ),
        )

    def restore_snapshot(self, *, context: RuntimeContext, snapshot: RuntimeQueueSnapshot) -> None:
        key = self._key(context)
        self._queues[key] = []
        self._in_flight[key] = {}
        self._dead_letters[key] = list(snapshot.dead_letters)
        self._in_flight_total[key] = 0
        self._in_flight_by_node_class[key] = {}
        for scheduled in snapshot.queued:
            self._push(key=key, scheduled=scheduled)
        # Replay-safe at-least-once: any previously in-flight action is re-queued.
        for scheduled in snapshot.in_flight:
            replayable = ScheduledRuntimeAction(
                action=scheduled.action,
                priority=scheduled.priority,
                enqueue_ts=scheduled.enqueue_ts,
                node_class=scheduled.node_class,
                attempt=scheduled.attempt,
                available_at=scheduled.available_at,
            )
            self._push(key=key, scheduled=replayable)

    def dead_letters(self, *, context: RuntimeContext) -> tuple[RuntimeDeadLetter, ...]:
        return tuple(self._dead_letters.get(self._key(context), []))

    def _remove_in_flight(self, *, context: RuntimeContext, action: RuntimeAction, node_class: str) -> None:
        key = self._key(context)
        inflight = self._in_flight.setdefault(key, {})
        inflight.pop(action.action_id, None)
        if self._in_flight_total.get(key, 0) > 0:
            self._in_flight_total[key] -= 1
        per_class = self._in_flight_by_node_class.setdefault(key, {})
        class_key = str(node_class).strip() or "default"
        if per_class.get(class_key, 0) > 0:
            per_class[class_key] -= 1
