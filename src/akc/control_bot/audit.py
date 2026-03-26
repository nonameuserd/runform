from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from akc.control.control_audit import append_control_audit_event
from akc.control_bot.command_engine import Command, CommandResult, InboundEvent, Principal
from akc.memory.models import JSONValue, require_non_empty

logger = logging.getLogger(__name__)

_ALLOWED_EVENT_TYPES = frozenset(
    {
        "control.bot.command.received",
        "control.bot.command.denied",
        "control.bot.command.approval_requested",
        "control.bot.command.approved",
        "control.bot.command.executed",
        "control.bot.command.failed",
    }
)


def default_control_bot_audit_log_path(*, state_dir: Path, explicit_path: str | None) -> Path:
    raw = str(explicit_path or "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return state_dir / "control_bot_audit.jsonl"


@dataclass(slots=True)
class ControlBotAuditWriter:
    audit_log_path: Path

    def __post_init__(self) -> None:
        p = Path(self.audit_log_path).expanduser().resolve()
        self.audit_log_path = p
        p.parent.mkdir(parents=True, exist_ok=True)

    def append(
        self,
        *,
        event_type: str,
        event: InboundEvent,
        principal: Principal | None,
        command: Command | None,
        result: CommandResult | None,
        request_hash: str | None,
        reason: str | None = None,
        details: dict[str, JSONValue] | None = None,
    ) -> None:
        et = str(event_type or "").strip()
        if et not in _ALLOWED_EVENT_TYPES:
            raise ValueError(f"unsupported control-bot audit event_type: {et!r}")
        ev = event.normalized()
        ev.validate()
        actor = (
            principal.principal_id.strip()
            if principal is not None and str(principal.principal_id or "").strip()
            else ev.principal_id.strip()
        )
        payload: dict[str, Any] = {
            "schema": "akc.control_bot.audit.v1",
            "event_type": et,
            "ts_ms": int(time.time() * 1000),
            "channel": ev.channel,
            "tenant_id": ev.tenant_id,
            "principal_id": ev.principal_id,
            "event_id": ev.event_id,
            "payload_hash": ev.payload_hash,
            "received_at_ms": int(ev.received_at_ms),
            "actor": actor,
        }
        if request_hash is not None and str(request_hash).strip():
            payload["request_hash"] = str(request_hash).strip().lower()
        if command is not None:
            payload["action_id"] = str(command.action_id).strip()
            payload["parser"] = str(command.parser)
            payload["args"] = dict(command.args or {})
        if result is not None:
            payload["result"] = {
                "ok": bool(result.ok),
                "action_id": str(result.action_id or "").strip(),
                "status": str(result.status),
                "request_id": str(result.request_id) if result.request_id is not None else None,
                "message": str(result.message or "").strip(),
                "data": result.data or {},
            }
        if reason is not None and str(reason).strip():
            payload["reason"] = str(reason).strip()
        if details:
            payload["details"] = dict(details)

        with self.audit_log_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, sort_keys=True, ensure_ascii=False) + "\n")


def maybe_mirror_control_mutation(
    *,
    event: InboundEvent,
    principal: Principal,
    command: Command,
    result: CommandResult,
) -> Path | None:
    """Mirror key control-bot mutations into tenant control audit logs when possible.

    We mirror only mutating/incident/approval surfaces and only when an `outputs_root`
    path can be resolved from command args or command result payload.
    """

    aid = str(command.action_id or "").strip().lower()
    if not (aid.startswith("mutate.") or aid.startswith("incident.") or aid.startswith("approval.")):
        return None

    outputs_root = _outputs_root_from_command_or_result(command=command, result=result)
    if not outputs_root:
        return None

    details: dict[str, Any] = {
        "channel": str(event.channel),
        "event_id": event.event_id,
        "payload_hash": event.payload_hash,
        "action_id": command.action_id,
        "args": dict(command.args or {}),
        "result_status": str(result.status),
        "ok": bool(result.ok),
        "result_data": result.data or {},
    }
    try:
        return append_control_audit_event(
            outputs_root=outputs_root,
            tenant_id=principal.tenant_id,
            action=f"control.bot.{aid}",
            actor=principal.principal_id,
            request_id=result.request_id,
            details=details,
        )
    except Exception as e:  # pragma: no cover
        logger.warning("control-bot mutation mirror failed action=%s tenant=%s err=%s", aid, principal.tenant_id, e)
        return None


def _outputs_root_from_command_or_result(*, command: Command, result: CommandResult) -> str | None:
    args = command.args if isinstance(command.args, dict) else {}
    out = args.get("outputs_root")
    if isinstance(out, str) and out.strip():
        return out.strip()
    data = result.data if isinstance(result.data, dict) else {}
    out2 = data.get("outputs_root")
    if isinstance(out2, str) and out2.strip():
        return out2.strip()
    executed = data.get("executed")
    if isinstance(executed, dict):
        d2 = executed.get("data")
        if isinstance(d2, dict):
            out3 = d2.get("outputs_root")
            if isinstance(out3, str) and out3.strip():
                return out3.strip()
    return None


def stable_audit_request_id(*, event: InboundEvent, command: Command) -> str:
    require_non_empty(event.event_id, name="event.event_id")
    require_non_empty(command.action_id, name="command.action_id")
    rid = f"{event.channel}:{event.event_id}:{command.action_id}"
    return rid.strip()
