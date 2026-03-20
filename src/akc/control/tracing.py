"""OpenTelemetry-compatible trace span structures for AKC runs."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass

from akc.memory.models import JSONValue, require_non_empty


@dataclass(frozen=True, slots=True)
class TraceSpan:
    """A minimal hierarchical span record."""

    trace_id: str
    span_id: str
    parent_span_id: str | None
    name: str
    kind: str
    start_time_unix_nano: int
    end_time_unix_nano: int
    attributes: dict[str, JSONValue] | None = None
    status: str = "ok"

    def __post_init__(self) -> None:
        require_non_empty(self.trace_id, name="trace_span.trace_id")
        require_non_empty(self.span_id, name="trace_span.span_id")
        require_non_empty(self.name, name="trace_span.name")
        require_non_empty(self.kind, name="trace_span.kind")
        require_non_empty(self.status, name="trace_span.status")
        if int(self.start_time_unix_nano) <= 0:
            raise ValueError("trace_span.start_time_unix_nano must be > 0")
        if int(self.end_time_unix_nano) < int(self.start_time_unix_nano):
            raise ValueError("trace_span.end_time_unix_nano must be >= start_time_unix_nano")

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "parent_span_id": self.parent_span_id,
            "name": self.name,
            "kind": self.kind,
            "start_time_unix_nano": int(self.start_time_unix_nano),
            "end_time_unix_nano": int(self.end_time_unix_nano),
            "attributes": dict(self.attributes) if self.attributes else None,
            "status": self.status,
        }


def new_trace_id() -> str:
    return uuid.uuid4().hex


def new_span_id() -> str:
    return uuid.uuid4().hex[:16]


def now_unix_nano() -> int:
    return int(time.time() * 1_000_000_000)
