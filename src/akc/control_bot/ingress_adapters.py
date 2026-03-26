from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from typing import Any, Protocol

from akc.control_bot.command_engine import ChannelId, InboundEvent
from akc.memory.models import require_non_empty


class IngressError(Exception):
    """Raised when a channel request cannot be authenticated or normalized."""


@dataclass(frozen=True, slots=True)
class IngressRequest:
    """Channel-agnostic request shape for adapters.

    Adapters should populate this from their HTTP framework (FastAPI, Flask, etc).
    """

    channel: ChannelId
    event_id: str
    principal_id: str
    tenant_id: str
    raw_text: str
    payload: dict[str, Any]
    received_at_ms: int | None = None


class IngressAdapter(Protocol):
    channel: ChannelId

    def parse(self, req: IngressRequest) -> InboundEvent: ...


def _payload_hash(payload: dict[str, Any]) -> str:
    try:
        b = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    except Exception:
        b = repr(payload).encode("utf-8", errors="replace")
    return hashlib.sha256(b).hexdigest()


def build_inbound_event(req: IngressRequest) -> InboundEvent:
    require_non_empty(req.event_id, name="event_id")
    require_non_empty(req.principal_id, name="principal_id")
    require_non_empty(req.tenant_id, name="tenant_id")
    require_non_empty(req.raw_text, name="raw_text")
    received = int(req.received_at_ms if req.received_at_ms is not None else time.time() * 1000)
    ev = InboundEvent(
        channel=req.channel,
        event_id=req.event_id,
        principal_id=req.principal_id,
        tenant_id=req.tenant_id,
        raw_text=req.raw_text,
        payload_hash=_payload_hash(req.payload),
        received_at_ms=received,
    ).normalized()
    ev.validate()
    return ev


@dataclass(slots=True)
class PassthroughIngressAdapter:
    """Adapter used in tests and local prototyping."""

    channel: ChannelId = "unknown"

    def parse(self, req: IngressRequest) -> InboundEvent:
        if req.channel != self.channel:
            raise IngressError("adapter channel mismatch")
        return build_inbound_event(req)
