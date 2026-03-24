from __future__ import annotations

import contextlib
import json
import os
import sqlite3
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from akc.control.otel_export import append_line_to_run_otel_jsonl, mirror_line_to_callbacks
from akc.control.tracing import TraceSpan
from akc.memory.models import normalize_repo_id
from akc.runtime.models import RuntimeAction, RuntimeCheckpoint, RuntimeContext, RuntimeEvent
from akc.runtime.policy import ensure_runtime_context_match, require_runtime_context
from akc.runtime.scheduler import (
    RuntimeDeadLetter,
    RuntimeQueueSnapshot,
    ScheduledRuntimeAction,
)


def _atomic_write_text(path: Path, text: str) -> None:
    """Write ``text`` to ``path`` via a same-directory temp file and ``os.replace``.

    Reduces the chance of readers observing a torn or empty JSON file if the process
    crashes mid-write (POSIX: replace is atomic over the destination path).
    """

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    try:
        tmp.write_text(text, encoding="utf-8")
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            tmp.unlink(missing_ok=True)
        raise


def runtime_state_scope_dir(*, root: str | Path, context: RuntimeContext) -> Path:
    """Tenant/repo/run-scoped directory under ``root`` (same layout as :class:`FileSystemRuntimeStateStore`)."""

    require_runtime_context(context)
    base = Path(root).expanduser()
    return (
        base
        / context.tenant_id.strip()
        / normalize_repo_id(context.repo_id)
        / ".akc"
        / "runtime"
        / context.run_id.strip()
        / context.runtime_run_id.strip()
    )


def _json_int_field(val: object, *, default: int) -> int:
    if isinstance(val, int) and not isinstance(val, bool):
        return val
    return default


def _parse_scheduled_item(item: dict[str, object]) -> ScheduledRuntimeAction:
    action_raw = item.get("action")
    if not isinstance(action_raw, dict):
        raise ValueError("runtime queue snapshot action entries must include action object")
    return ScheduledRuntimeAction(
        action=RuntimeAction.from_json_obj(action_raw),
        priority=_json_int_field(item.get("priority", 0), default=0),
        enqueue_ts=_json_int_field(item.get("enqueue_ts", 0), default=0),
        node_class=str(item.get("node_class", "default")),
        attempt=_json_int_field(item.get("attempt", 0), default=0),
        available_at=_json_int_field(item.get("available_at", 0), default=0),
    )


def _queue_snapshot_to_json_dict(snapshot: RuntimeQueueSnapshot) -> dict[str, object]:
    return {
        "queued": [
            {
                "action": item.action.to_json_obj(),
                "priority": item.priority,
                "enqueue_ts": item.enqueue_ts,
                "node_class": item.node_class,
                "attempt": item.attempt,
                "available_at": item.available_at,
            }
            for item in snapshot.queued
        ],
        "in_flight": [
            {
                "action": item.action.to_json_obj(),
                "priority": item.priority,
                "enqueue_ts": item.enqueue_ts,
                "node_class": item.node_class,
                "attempt": item.attempt,
                "available_at": item.available_at,
            }
            for item in snapshot.in_flight
        ],
        "dead_letters": [
            {
                "action": item.action.to_json_obj(),
                "reason": item.reason,
                "attempts": item.attempts,
                "error": item.error,
                "dead_lettered_at": item.dead_lettered_at,
                "node_class": item.node_class,
            }
            for item in snapshot.dead_letters
        ],
    }


def _queue_snapshot_from_json_dict(raw: Mapping[str, Any]) -> RuntimeQueueSnapshot:
    queued_raw = raw.get("queued", [])
    inflight_raw = raw.get("in_flight", [])
    dead_letters_raw = raw.get("dead_letters", [])
    if not isinstance(queued_raw, list) or not isinstance(inflight_raw, list) or not isinstance(dead_letters_raw, list):
        raise ValueError("runtime queue snapshot fields must be arrays")
    return RuntimeQueueSnapshot(
        queued=tuple(_parse_scheduled_item(item) for item in queued_raw if isinstance(item, dict)),
        in_flight=tuple(_parse_scheduled_item(item) for item in inflight_raw if isinstance(item, dict)),
        dead_letters=tuple(
            RuntimeDeadLetter(
                action=RuntimeAction.from_json_obj(item["action"]),
                reason=str(item.get("reason", "backend_error")),  # type: ignore[arg-type]
                attempts=_json_int_field(item.get("attempts", 1), default=1),
                error=str(item.get("error", "")),
                dead_lettered_at=_json_int_field(item.get("dead_lettered_at", 0), default=0),
                node_class=str(item.get("node_class", "default")),
            )
            for item in dead_letters_raw
            if isinstance(item, dict) and isinstance(item.get("action"), dict)
        ),
    )


def _trace_span_from_json_obj(item: Mapping[str, Any]) -> TraceSpan:
    attrs_raw = item.get("attributes")
    attributes = attrs_raw if isinstance(attrs_raw, dict) else None
    return TraceSpan(
        trace_id=str(item.get("trace_id", "")),
        span_id=str(item.get("span_id", "")),
        parent_span_id=(str(item.get("parent_span_id")) if item.get("parent_span_id") is not None else None),
        name=str(item.get("name", "")),
        kind=str(item.get("kind", "")),
        start_time_unix_nano=_json_int_field(item.get("start_time_unix_nano", 0), default=0),
        end_time_unix_nano=_json_int_field(item.get("end_time_unix_nano", 0), default=0),
        attributes=attributes,
        status=str(item.get("status", "ok")),
    )


_SQLITE_KV_CHECKPOINT = "checkpoint"
_SQLITE_KV_QUEUE = "queue_snapshot"


def _sqlite_init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;

        CREATE TABLE IF NOT EXISTS akc_runtime_kv (
          k TEXT PRIMARY KEY NOT NULL,
          v TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS akc_runtime_events (
          seq INTEGER PRIMARY KEY AUTOINCREMENT,
          payload TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS akc_runtime_trace_spans (
          seq INTEGER PRIMARY KEY AUTOINCREMENT,
          payload TEXT NOT NULL
        );
        """
    )


class RuntimeStateStore(Protocol):
    def load_checkpoint(self, *, context: RuntimeContext) -> RuntimeCheckpoint | None: ...

    def save_checkpoint(self, *, context: RuntimeContext, checkpoint: RuntimeCheckpoint) -> None: ...

    def append_event(self, *, context: RuntimeContext, event: RuntimeEvent) -> None: ...

    def list_events(self, *, context: RuntimeContext) -> tuple[RuntimeEvent, ...]: ...

    def load_queue_snapshot(self, *, context: RuntimeContext) -> RuntimeQueueSnapshot | None: ...

    def save_queue_snapshot(self, *, context: RuntimeContext, snapshot: RuntimeQueueSnapshot) -> None: ...

    def list_dead_letters(self, *, context: RuntimeContext) -> tuple[RuntimeDeadLetter, ...]: ...

    def append_trace_span(self, *, context: RuntimeContext, span: TraceSpan) -> None: ...

    def list_trace_spans(self, *, context: RuntimeContext) -> tuple[TraceSpan, ...]: ...


@dataclass(slots=True)
class InMemoryRuntimeStateStore(RuntimeStateStore):
    _checkpoints: dict[str, RuntimeCheckpoint] = field(default_factory=dict)
    _events: dict[str, list[RuntimeEvent]] = field(default_factory=dict)
    _queue_snapshots: dict[str, RuntimeQueueSnapshot] = field(default_factory=dict)
    _trace_spans: dict[str, list[TraceSpan]] = field(default_factory=dict)

    def _key(self, context: RuntimeContext) -> str:
        require_runtime_context(context)
        return (
            f"{context.tenant_id.strip()}::{context.repo_id.strip()}::{context.run_id.strip()}::"
            f"{context.runtime_run_id.strip()}"
        )

    def load_checkpoint(self, *, context: RuntimeContext) -> RuntimeCheckpoint | None:
        return self._checkpoints.get(self._key(context))

    def save_checkpoint(self, *, context: RuntimeContext, checkpoint: RuntimeCheckpoint) -> None:
        self._checkpoints[self._key(context)] = checkpoint

    def append_event(self, *, context: RuntimeContext, event: RuntimeEvent) -> None:
        self._events.setdefault(self._key(context), []).append(event)

    def list_events(self, *, context: RuntimeContext) -> tuple[RuntimeEvent, ...]:
        return tuple(self._events.get(self._key(context), []))

    def load_queue_snapshot(self, *, context: RuntimeContext) -> RuntimeQueueSnapshot | None:
        return self._queue_snapshots.get(self._key(context))

    def save_queue_snapshot(self, *, context: RuntimeContext, snapshot: RuntimeQueueSnapshot) -> None:
        self._queue_snapshots[self._key(context)] = snapshot

    def list_dead_letters(self, *, context: RuntimeContext) -> tuple[RuntimeDeadLetter, ...]:
        snapshot = self._queue_snapshots.get(self._key(context))
        if snapshot is None:
            return ()
        return snapshot.dead_letters

    def append_trace_span(self, *, context: RuntimeContext, span: TraceSpan) -> None:
        self._trace_spans.setdefault(self._key(context), []).append(span)

    def list_trace_spans(self, *, context: RuntimeContext) -> tuple[TraceSpan, ...]:
        return tuple(self._trace_spans.get(self._key(context), []))


@dataclass(slots=True)
class FileSystemRuntimeStateStore(RuntimeStateStore):
    root: str | Path
    otel_export_extra_callbacks: tuple[Callable[[str], None], ...] = field(default_factory=tuple)

    def _repo_root(self, context: RuntimeContext) -> Path:
        """Parent of ``.akc`` under the normalized repo directory (tenant/repo layout)."""

        return self._scope_dir(context).parent.parent.parent.parent

    def append_run_otel_export_line(self, *, context: RuntimeContext, line: str) -> None:
        """Append one AKC trace export JSON line to ``.akc/run/<compile_run_id>.otel.jsonl``."""

        append_line_to_run_otel_jsonl(
            repo_root=self._repo_root(context),
            compile_run_id=context.run_id,
            line=line,
        )
        mirror_line_to_callbacks(line, self.otel_export_extra_callbacks)

    def _scope_dir(self, context: RuntimeContext) -> Path:
        return runtime_state_scope_dir(root=self.root, context=context)

    def _checkpoint_path(self, context: RuntimeContext) -> Path:
        return self._scope_dir(context) / "checkpoint.json"

    def _events_path(self, context: RuntimeContext) -> Path:
        return self._scope_dir(context) / "events.json"

    def _queue_path(self, context: RuntimeContext) -> Path:
        return self._scope_dir(context) / "queue_snapshot.json"

    def _trace_spans_path(self, context: RuntimeContext) -> Path:
        return self._scope_dir(context) / "runtime_trace_spans.json"

    def load_checkpoint(self, *, context: RuntimeContext) -> RuntimeCheckpoint | None:
        path = self._checkpoint_path(context)
        if not path.exists():
            return None
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("runtime checkpoint file must be an object")
        checkpoint = RuntimeCheckpoint.from_json_obj(raw)
        return checkpoint

    def save_checkpoint(self, *, context: RuntimeContext, checkpoint: RuntimeCheckpoint) -> None:
        path = self._checkpoint_path(context)
        payload = json.dumps(checkpoint.to_json_obj(), indent=2, sort_keys=True)
        _atomic_write_text(path, payload)

    def append_event(self, *, context: RuntimeContext, event: RuntimeEvent) -> None:
        ensure_runtime_context_match(expected=context, actual=event.context)
        path = self._events_path(context)
        path.parent.mkdir(parents=True, exist_ok=True)
        current = list(self.list_events(context=context))
        current.append(event)
        payload = [item.to_json_obj() for item in current]
        _atomic_write_text(path, json.dumps(payload, indent=2, sort_keys=True))

    def list_events(self, *, context: RuntimeContext) -> tuple[RuntimeEvent, ...]:
        path = self._events_path(context)
        if not path.exists():
            return ()
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, list):
            raise ValueError("runtime events file must be an array")
        return tuple(RuntimeEvent.from_json_obj(item) for item in raw if isinstance(item, dict))

    def load_queue_snapshot(self, *, context: RuntimeContext) -> RuntimeQueueSnapshot | None:
        path = self._queue_path(context)
        if not path.exists():
            return None
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("runtime queue snapshot file must be an object")
        return _queue_snapshot_from_json_dict(raw)

    def save_queue_snapshot(self, *, context: RuntimeContext, snapshot: RuntimeQueueSnapshot) -> None:
        path = self._queue_path(context)
        payload = _queue_snapshot_to_json_dict(snapshot)
        _atomic_write_text(path, json.dumps(payload, indent=2, sort_keys=True))

    def list_dead_letters(self, *, context: RuntimeContext) -> tuple[RuntimeDeadLetter, ...]:
        snapshot = self.load_queue_snapshot(context=context)
        if snapshot is None:
            return ()
        return snapshot.dead_letters

    def append_trace_span(self, *, context: RuntimeContext, span: TraceSpan) -> None:
        path = self._trace_spans_path(context)
        path.parent.mkdir(parents=True, exist_ok=True)
        current = list(self.list_trace_spans(context=context))
        current.append(span)
        payload = {
            "tenant_id": context.tenant_id,
            "repo_id": context.repo_id,
            "run_id": context.run_id,
            "runtime_run_id": context.runtime_run_id,
            "trace_id": span.trace_id,
            "spans": [item.to_json_obj() for item in current],
        }
        _atomic_write_text(path, json.dumps(payload, indent=2, sort_keys=True))

    def list_trace_spans(self, *, context: RuntimeContext) -> tuple[TraceSpan, ...]:
        path = self._trace_spans_path(context)
        if not path.exists():
            return ()
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("runtime trace spans file must be an object")
        spans_raw = raw.get("spans", [])
        if not isinstance(spans_raw, list):
            raise ValueError("runtime trace spans file spans field must be an array")
        return tuple(_trace_span_from_json_obj(item) for item in spans_raw if isinstance(item, dict))

    def append_coordination_audit_line(self, *, context: RuntimeContext, line: str) -> None:
        """Append one JSON object per line under ``evidence/coordination_audit.jsonl`` (coordination audit trail)."""

        path = self._scope_dir(context) / "evidence" / "coordination_audit.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(str(line).strip())
            fh.write("\n")


@dataclass(slots=True)
class SqliteRuntimeStateStore:
    """SQLite implementation of :class:`RuntimeStateStore` for one runtime scope.

    Writes ``runtime_state.sqlite3`` under the same directory as
    :class:`FileSystemRuntimeStateStore` would use for ``checkpoint.json`` (tenant /
    repo / ``.akc/runtime/<run_id>/<runtime_run_id>/``). Events and trace spans are
    append-only rows (WAL mode) instead of rewriting large JSON arrays.

    Does not provide :meth:`FileSystemRuntimeStateStore.append_run_otel_export_line` or
    ``append_coordination_audit_line``; :class:`~akc.runtime.kernel.RuntimeKernel` calls
    those via ``getattr`` when present.

    Intended for **single-threaded** use per process (standard ``sqlite3`` module).
    """

    root: str | Path

    def _db_path(self, context: RuntimeContext) -> Path:
        return runtime_state_scope_dir(root=self.root, context=context) / "runtime_state.sqlite3"

    def _connection(self, context: RuntimeContext) -> sqlite3.Connection:
        path = self._db_path(context)
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(path))
        _sqlite_init_schema(conn)
        return conn

    def load_checkpoint(self, *, context: RuntimeContext) -> RuntimeCheckpoint | None:
        with self._connection(context) as conn:
            row = conn.execute(
                "SELECT v FROM akc_runtime_kv WHERE k = ?",
                (_SQLITE_KV_CHECKPOINT,),
            ).fetchone()
        if row is None:
            return None
        raw = json.loads(row[0])
        if not isinstance(raw, dict):
            raise ValueError("runtime checkpoint blob must decode to an object")
        return RuntimeCheckpoint.from_json_obj(raw)

    def save_checkpoint(self, *, context: RuntimeContext, checkpoint: RuntimeCheckpoint) -> None:
        blob = json.dumps(checkpoint.to_json_obj(), sort_keys=True)
        with self._connection(context) as conn:
            conn.execute("DELETE FROM akc_runtime_kv WHERE k = ?", (_SQLITE_KV_CHECKPOINT,))
            conn.execute(
                "INSERT INTO akc_runtime_kv (k, v) VALUES (?, ?)",
                (_SQLITE_KV_CHECKPOINT, blob),
            )

    def append_event(self, *, context: RuntimeContext, event: RuntimeEvent) -> None:
        ensure_runtime_context_match(expected=context, actual=event.context)
        blob = json.dumps(event.to_json_obj(), sort_keys=True)
        with self._connection(context) as conn:
            conn.execute("INSERT INTO akc_runtime_events (payload) VALUES (?)", (blob,))

    def list_events(self, *, context: RuntimeContext) -> tuple[RuntimeEvent, ...]:
        with self._connection(context) as conn:
            rows = conn.execute("SELECT payload FROM akc_runtime_events ORDER BY seq ASC").fetchall()
        events: list[RuntimeEvent] = []
        for (payload,) in rows:
            raw = json.loads(payload)
            if isinstance(raw, dict):
                events.append(RuntimeEvent.from_json_obj(raw))
        return tuple(events)

    def load_queue_snapshot(self, *, context: RuntimeContext) -> RuntimeQueueSnapshot | None:
        with self._connection(context) as conn:
            row = conn.execute(
                "SELECT v FROM akc_runtime_kv WHERE k = ?",
                (_SQLITE_KV_QUEUE,),
            ).fetchone()
        if row is None:
            return None
        raw = json.loads(row[0])
        if not isinstance(raw, dict):
            raise ValueError("runtime queue snapshot blob must decode to an object")
        return _queue_snapshot_from_json_dict(raw)

    def save_queue_snapshot(self, *, context: RuntimeContext, snapshot: RuntimeQueueSnapshot) -> None:
        blob = json.dumps(_queue_snapshot_to_json_dict(snapshot), sort_keys=True)
        with self._connection(context) as conn:
            conn.execute("DELETE FROM akc_runtime_kv WHERE k = ?", (_SQLITE_KV_QUEUE,))
            conn.execute(
                "INSERT INTO akc_runtime_kv (k, v) VALUES (?, ?)",
                (_SQLITE_KV_QUEUE, blob),
            )

    def list_dead_letters(self, *, context: RuntimeContext) -> tuple[RuntimeDeadLetter, ...]:
        snapshot = self.load_queue_snapshot(context=context)
        if snapshot is None:
            return ()
        return snapshot.dead_letters

    def append_trace_span(self, *, context: RuntimeContext, span: TraceSpan) -> None:
        blob = json.dumps(span.to_json_obj(), sort_keys=True)
        with self._connection(context) as conn:
            conn.execute("INSERT INTO akc_runtime_trace_spans (payload) VALUES (?)", (blob,))

    def list_trace_spans(self, *, context: RuntimeContext) -> tuple[TraceSpan, ...]:
        with self._connection(context) as conn:
            rows = conn.execute("SELECT payload FROM akc_runtime_trace_spans ORDER BY seq ASC").fetchall()
        spans: list[TraceSpan] = []
        for (payload,) in rows:
            raw = json.loads(payload)
            if isinstance(raw, dict):
                spans.append(_trace_span_from_json_obj(raw))
        return tuple(spans)
