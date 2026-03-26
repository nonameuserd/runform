from __future__ import annotations

import json
import logging
import queue
import re
import threading
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from akc.control_bot.actions_v1 import V1ActionDeps, build_action_registry_v1
from akc.control_bot.approval_workflow import ApprovalWorkflow, SqliteApprovalStore, stable_args_fingerprint
from akc.control_bot.audit import (
    ControlBotAuditWriter,
    default_control_bot_audit_log_path,
    maybe_mirror_control_mutation,
)
from akc.control_bot.command_engine import (
    ActionRegistry,
    Command,
    CommandClarificationRequired,
    CommandContext,
    CommandEngine,
    CommandResult,
    InboundEvent,
    PolicyDenied,
    Principal,
    default_now_ms,
)
from akc.control_bot.command_result_store import (
    CommandResultStore,
    InMemoryCommandResultStore,
    SqliteCommandResultStore,
    stable_command_request_hash,
)
from akc.control_bot.config import LoadedControlBotConfig
from akc.control_bot.event_store import InboundEventStore, SqliteInboundEventStore
from akc.control_bot.ingress_adapters import IngressError, IngressRequest, PassthroughIngressAdapter
from akc.control_bot.ingress_auth import (
    IngressAuthContext,
    verify_discord_request,
    verify_slack_request,
    verify_telegram_request,
    verify_whatsapp_request,
    verify_whatsapp_webhook_verification,
)
from akc.control_bot.outbound_response_adapters import (
    DiscordOutboundAdapter,
    OutboundAdapter,
    OutboundMessage,
    SlackOutboundAdapter,
    TelegramOutboundAdapter,
    TextOutboundAdapter,
    WhatsAppOutboundAdapter,
    send_discord_followup_message,
    send_slack_response_message,
    send_telegram_message,
    send_whatsapp_text_message,
)
from akc.control_bot.policy_gate import OPAClient, OPAConfig, PolicyGate, build_role_allowlist
from akc.memory.models import require_non_empty

logger = logging.getLogger(__name__)

_MAX_BODY_BYTES = 256 * 1024


class ControlBotServerError(Exception):
    """Raised for fatal control-bot server misconfiguration or runtime errors."""


@dataclass(frozen=True, slots=True)
class ControlBotServerConfig:
    loaded: LoadedControlBotConfig


@dataclass(slots=True)
class _WorkItem:
    event: InboundEvent
    outbound_target: _OutboundTarget | None = None


@dataclass(frozen=True, slots=True)
class _OutboundTarget:
    channel: str
    data: dict[str, str]
    ephemeral: bool = True

    @property
    def application_id(self) -> str:
        return str(self.data.get("application_id") or "")

    @property
    def interaction_token(self) -> str:
        return str(self.data.get("interaction_token") or "")


@dataclass(slots=True)
class _PerChannelRateLimiter:
    """Simple lock-protected interval limiter for outbound sends."""

    min_interval_s: dict[str, float]
    _next_allowed_s: dict[str, float]
    _lock: threading.Lock

    def __init__(self, *, min_interval_s: dict[str, float]) -> None:
        self.min_interval_s = {str(k): max(0.0, float(v)) for k, v in min_interval_s.items()}
        self._next_allowed_s = {}
        self._lock = threading.Lock()

    def wait(self, channel: str) -> None:
        ch = str(channel or "").strip().lower()
        interval = float(self.min_interval_s.get(ch, 0.0))
        if interval <= 0.0:
            return
        while True:
            with self._lock:
                now = time.monotonic()
                ready = float(self._next_allowed_s.get(ch, 0.0))
                if now >= ready:
                    self._next_allowed_s[ch] = now + interval
                    return
                sleep_s = ready - now
            time.sleep(max(0.001, min(0.25, sleep_s)))


def _json_response(handler: BaseHTTPRequestHandler, *, status: int, body: dict[str, Any]) -> None:
    data = json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


def _empty_response(handler: BaseHTTPRequestHandler, *, status: int) -> None:
    handler.send_response(status)
    handler.send_header("Content-Length", "0")
    handler.end_headers()


def _read_body(handler: BaseHTTPRequestHandler) -> bytes:
    length = int(handler.headers.get("Content-Length", "0") or "0")
    if length <= 0:
        return b""
    if length > _MAX_BODY_BYTES:
        raise ValueError("request body too large")
    return handler.rfile.read(length)


def _lower_headers(handler: BaseHTTPRequestHandler) -> dict[str, str]:
    out: dict[str, str] = {}
    for k, v in handler.headers.items():
        kk = str(k or "").strip().lower()
        if not kk:
            continue
        out[kk] = str(v or "").strip()
    return out


def _sha256_hex(b: bytes) -> str:
    import hashlib

    return hashlib.sha256(b).hexdigest()


def _tenant_for_workspace(*, model: Any, channel: str, workspace_id: str) -> str:
    wid = str(workspace_id or "").strip()
    if not wid:
        raise ValueError("missing workspace_id")
    for ws in tuple(getattr(getattr(model, "routing", None), "workspaces", ()) or ()):
        ws_channel = str(getattr(ws, "channel", "") or "").strip()
        ws_id = str(getattr(ws, "workspace_id", "") or "").strip()
        if ws_channel == channel and ws_id == wid:
            return str(getattr(ws, "tenant_id", "") or "").strip()
    raise ValueError(f"unknown workspace_id for channel={channel}")


def _parse_slack_request(*, body: bytes, headers: dict[str, str], model: Any, endpoint: str) -> IngressRequest:
    """Parse Slack slash-command or interactivity request into an IngressRequest.

    Slack sends `application/x-www-form-urlencoded` bodies for both slash commands and interactivity.
    Interactivity payload is nested as a `payload=` JSON string.
    """

    # Slack retries can re-send the same payload; create a stable event_id from timestamp + raw body hash.
    ts = str(headers.get("x-slack-request-timestamp", "") or "").strip()
    if not ts:
        raise ValueError("missing x-slack-request-timestamp")
    event_id = f"slack:{ts}:{_sha256_hex(body)}"

    form = parse_qs(body.decode("utf-8", errors="replace"), keep_blank_values=True, strict_parsing=False)

    if endpoint == "commands":
        team_id = (form.get("team_id") or [""])[0]
        user_id = (form.get("user_id") or [""])[0]
        text = (form.get("text") or [""])[0]
        command = (form.get("command") or [""])[0]
        raw_text = f"akc {text}".strip() if text.strip() else "akc"
        # If user types the full `akc ...` we don't want to double-prefix.
        if raw_text.lower().startswith("akc akc "):
            raw_text = raw_text[4:].strip()
        if raw_text.lower() == "akc" and command.strip():
            # Empty text; keep it non-empty to satisfy invariant, but deterministic.
            raw_text = "akc status runtime"
        raw_text = _normalize_operator_text(raw_text)

        tenant_id = _tenant_for_workspace(model=model, channel="slack", workspace_id=team_id)
        principal_id = str(user_id or "").strip()
        require_non_empty(principal_id, name="slack.user_id")

        payload: dict[str, Any] = {k: (v[0] if len(v) == 1 else v) for k, v in form.items()}
        payload["slack_team_id"] = team_id
        payload["slack_user_id"] = user_id
        payload["slack_command"] = command

        return IngressRequest(
            channel="slack",
            event_id=event_id,
            principal_id=principal_id,
            tenant_id=tenant_id,
            raw_text=raw_text,
            payload=payload,
            received_at_ms=None,
        )

    if endpoint == "interactivity":
        team_id = (form.get("team_id") or [""])[0]
        # Slack can omit team_id at top-level; interactivity always includes it in JSON payload.
        payload_raw = (form.get("payload") or [""])[0]
        if not payload_raw:
            raise ValueError("missing slack interactivity payload")
        try:
            payload_json = json.loads(payload_raw)
        except Exception as e:
            raise ValueError("invalid slack interactivity payload json") from e
        if not isinstance(payload_json, dict):
            raise ValueError("slack interactivity payload must be a JSON object")
        payload_obj: dict[str, Any] = payload_json

        # Prefer team.id and user.id from payload JSON.
        team_raw = payload_obj.get("team")
        user_raw = payload_obj.get("user")
        team: dict[str, Any] = team_raw if isinstance(team_raw, dict) else {}
        user: dict[str, Any] = user_raw if isinstance(user_raw, dict) else {}
        team_id2 = str(team.get("id") or "").strip()
        if not team_id2:
            team_id2 = str(team_id or "").strip()
        user_id = str(user.get("id") or "").strip()
        require_non_empty(user_id, name="slack.user.id")

        tenant_id = _tenant_for_workspace(model=model, channel="slack", workspace_id=team_id2)

        # v1: treat the "action" as an operator command string if present; otherwise fall back to view/title text.
        raw_text = ""
        actions = payload_obj.get("actions")
        if isinstance(actions, list) and actions and isinstance(actions[0], dict):
            raw_text = str(actions[0].get("value", "") or "").strip()
        if not raw_text:
            raw_text = "akc status runtime"
        raw_text = _normalize_operator_text(raw_text)

        payload_data: dict[str, Any] = {
            "form": {k: (v[0] if len(v) == 1 else v) for k, v in form.items()},
            "payload": payload_obj,
        }
        payload_data["slack_team_id"] = team_id2
        payload_data["slack_user_id"] = user_id

        return IngressRequest(
            channel="slack",
            event_id=event_id,
            principal_id=user_id,
            tenant_id=tenant_id,
            raw_text=raw_text,
            payload=payload_data,
            received_at_ms=None,
        )

    raise ValueError("unknown slack endpoint")


def _parse_event_from_json_payload(payload: dict[str, Any]) -> IngressRequest:
    # This is a minimal v1 envelope for the dedicated service.
    # Channel-specific adapters (Slack/Discord/Telegram/WhatsApp) will populate the same shape.
    channel = str(payload.get("channel", "unknown")).strip().lower() or "unknown"
    if channel not in {"slack", "discord", "telegram", "whatsapp", "unknown"}:
        channel = "unknown"
    event_id = str(payload.get("event_id", "")).strip()
    principal_id = str(payload.get("principal_id", "")).strip()
    tenant_id = str(payload.get("tenant_id", "")).strip()
    raw_text = str(payload.get("raw_text", "")).strip()
    extra_payload = payload.get("payload")
    if extra_payload is None:
        extra_payload = payload
    if not isinstance(extra_payload, dict):
        extra_payload = {"payload": extra_payload}
    return IngressRequest(
        channel=channel,  # type: ignore[arg-type]
        event_id=event_id,
        principal_id=principal_id,
        tenant_id=tenant_id,
        raw_text=raw_text,
        payload=extra_payload,
        received_at_ms=int(payload.get("received_at_ms") or 0) or None,
    )


def _parse_telegram_request(*, payload: dict[str, Any], model: Any) -> IngressRequest:
    """Parse a Telegram webhook update into an IngressRequest.

    Supports:
    - message text commands (including `/akc ...`)
    - callback_query buttons (uses callback_query.data)
    """
    update_id = payload.get("update_id")
    if not isinstance(update_id, int):
        raise ValueError("telegram payload missing update_id")
    event_id = f"telegram:{update_id}"

    # Prefer callback_query payloads (buttons).
    raw_text = ""
    principal_id = ""
    chat_id = ""

    callback = payload.get("callback_query")
    if isinstance(callback, dict):
        frm = callback.get("from")
        if isinstance(frm, dict):
            principal_id = str(frm.get("id") or "").strip()
        data = str(callback.get("data") or "").strip()
        if data:
            raw_text = data
        # Routing: use callback_query.message.chat.id when present.
        msg = callback.get("message")
        if isinstance(msg, dict):
            chat = msg.get("chat")
            if isinstance(chat, dict):
                chat_id = str(chat.get("id") or "").strip()

    if not raw_text:
        msg = payload.get("message") or payload.get("edited_message")
        if isinstance(msg, dict):
            frm = msg.get("from")
            if isinstance(frm, dict) and not principal_id:
                principal_id = str(frm.get("id") or "").strip()
            chat = msg.get("chat")
            if isinstance(chat, dict):
                chat_id = str(chat.get("id") or "").strip()
            text = str(msg.get("text") or "").strip()
            if not text:
                # Some messages (photos, docs) carry command text in caption.
                text = str(msg.get("caption") or "").strip()
            raw_text = text

    require_non_empty(principal_id, name="telegram.from.id")
    require_non_empty(chat_id, name="telegram.chat.id")

    # Tenant routing uses chat_id as the "workspace_id" for telegram.
    tenant_id = _tenant_for_workspace(model=model, channel="telegram", workspace_id=chat_id)

    # Normalize raw_text into a command-engine-friendly string.
    rt = _normalize_operator_text(str(raw_text or "").strip())

    return IngressRequest(
        channel="telegram",
        event_id=event_id,
        principal_id=principal_id,
        tenant_id=tenant_id,
        raw_text=rt,
        payload={"telegram": payload, "telegram_chat_id": chat_id, "telegram_update_id": update_id},
        received_at_ms=None,
    )


def _normalize_operator_text(raw_text: str) -> str:
    rt = str(raw_text or "").strip()
    if not rt:
        return "akc status runtime"
    if rt.startswith("/"):
        head, *tail = rt.split()
        cmd = head.lstrip("/")
        if "@" in cmd:
            cmd = cmd.split("@", 1)[0]
        rest = " ".join(tail).strip()
        rt = (f"akc {rest}".strip() if rest else "akc") if cmd.lower() == "akc" else "akc status runtime"
    if not rt:
        rt = "akc status runtime"
    # WhatsApp command-reply fallback: "approve <request_id>" / "deny <request_id>".
    m = re.match(r"^(approve|deny)\s+([a-zA-Z0-9-]{8,})$", rt, flags=re.IGNORECASE)
    if m:
        decision = m.group(1).lower()
        rid = m.group(2)
        action = "approve" if decision == "approve" else "deny"
        return f"akc approval {action} request_id={rid}"
    if not rt.lower().startswith("akc"):
        rt = f"akc {rt}".strip()
    if rt.lower() == "akc":
        return "akc status runtime"
    return rt


def _slack_outbound_target(req: IngressRequest) -> _OutboundTarget | None:
    payload = req.payload if isinstance(req.payload, dict) else {}
    response_url = str(payload.get("response_url") or "").strip()
    if not response_url:
        form = payload.get("form")
        if isinstance(form, dict):
            response_url = str(form.get("response_url") or "").strip()
    if not response_url:
        p = payload.get("payload")
        if isinstance(p, dict):
            response_url = str(p.get("response_url") or "").strip()
    if not response_url:
        return None
    return _OutboundTarget(channel="slack", data={"response_url": response_url}, ephemeral=True)


def _telegram_outbound_target(req: IngressRequest) -> _OutboundTarget | None:
    payload = req.payload if isinstance(req.payload, dict) else {}
    chat_id = str(payload.get("telegram_chat_id") or "").strip()
    if not chat_id:
        return None
    return _OutboundTarget(channel="telegram", data={"chat_id": chat_id}, ephemeral=False)


def _iter_whatsapp_messages(payload: dict[str, Any]) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    out: list[tuple[dict[str, Any], dict[str, Any]]] = []
    entry = payload.get("entry")
    if not isinstance(entry, list):
        return out
    for e in entry:
        if not isinstance(e, dict):
            continue
        changes = e.get("changes")
        if not isinstance(changes, list):
            continue
        for ch in changes:
            if not isinstance(ch, dict):
                continue
            value = ch.get("value")
            if not isinstance(value, dict):
                continue
            metadata = value.get("metadata")
            phone_number_id = ""
            if isinstance(metadata, dict):
                phone_number_id = str(metadata.get("phone_number_id") or "").strip()
            msgs = value.get("messages")
            if isinstance(msgs, list):
                for m in msgs:
                    if not isinstance(m, dict):
                        continue
                    md: dict[str, Any] = {"phone_number_id": phone_number_id}
                    contacts = value.get("contacts")
                    if isinstance(contacts, list) and contacts and isinstance(contacts[0], dict):
                        wa = str(contacts[0].get("wa_id") or "").strip()
                        if wa:
                            md["wa_id"] = wa
                    out.append((m, md))
    return out


def _extract_whatsapp_text(message: dict[str, Any]) -> str:
    text_obj = message.get("text")
    if isinstance(text_obj, dict):
        body = str(text_obj.get("body") or "").strip()
        if body:
            return body
    interactive = message.get("interactive")
    if isinstance(interactive, dict):
        button_reply = interactive.get("button_reply")
        if isinstance(button_reply, dict):
            title = str(button_reply.get("title") or "").strip()
            if title:
                return title
            bid = str(button_reply.get("id") or "").strip()
            if bid:
                return bid
        list_reply = interactive.get("list_reply")
        if isinstance(list_reply, dict):
            title = str(list_reply.get("title") or "").strip()
            if title:
                return title
            lid = str(list_reply.get("id") or "").strip()
            if lid:
                return lid
    for kind in ("image", "video", "document", "audio", "sticker"):
        obj = message.get(kind)
        if isinstance(obj, dict):
            cap = str(obj.get("caption") or "").strip()
            if cap:
                return cap
    return ""


def _parse_whatsapp_request(
    *,
    payload: dict[str, Any],
    model: Any,
    body: bytes,
) -> tuple[IngressRequest, _OutboundTarget]:
    msgs = _iter_whatsapp_messages(payload)
    if not msgs:
        raise ValueError("whatsapp payload missing messages[]")
    msg, md = msgs[0]
    message_id = str(msg.get("id") or "").strip()
    if not message_id:
        message_id = _sha256_hex(body)
    event_id = f"whatsapp:{message_id}"

    phone_number_id = str(md.get("phone_number_id") or "").strip()
    require_non_empty(phone_number_id, name="whatsapp.metadata.phone_number_id")
    tenant_id = _tenant_for_workspace(model=model, channel="whatsapp", workspace_id=phone_number_id)

    principal_id = str(msg.get("from") or "").strip() or str(md.get("wa_id") or "").strip()
    require_non_empty(principal_id, name="whatsapp.from")

    raw = _extract_whatsapp_text(msg)
    raw_text = _normalize_operator_text(raw)
    req = IngressRequest(
        channel="whatsapp",
        event_id=event_id,
        principal_id=principal_id,
        tenant_id=tenant_id,
        raw_text=raw_text,
        payload={
            "whatsapp": payload,
            "whatsapp_message_id": message_id,
            "whatsapp_phone_number_id": phone_number_id,
            "whatsapp_from": principal_id,
        },
        received_at_ms=None,
    )
    target = _OutboundTarget(
        channel="whatsapp",
        data={"to": principal_id, "phone_number_id": phone_number_id},
        ephemeral=False,
    )
    return req, target


def _discord_interaction_initial_response(*, interaction_type: int) -> dict[str, Any]:
    # Interaction types:
    # - 1: PING (respond with PONG type=1)
    # - 2: APPLICATION_COMMAND (respond quickly; defer via type=5)
    # - 3: MESSAGE_COMPONENT (buttons/selects; defer via type=5)
    if int(interaction_type) == 1:
        return {"type": 1}
    # Defer: "DEFERRED_CHANNEL_MESSAGE_WITH_SOURCE"
    return {"type": 5, "data": {"flags": 64}}


def _discord_flatten_command_tokens(data: dict[str, Any]) -> list[str]:
    tokens: list[str] = []
    name = str(data.get("name") or "").strip()
    if name:
        tokens.append(name)

    def _walk(opts: Any) -> None:
        if not isinstance(opts, list):
            return
        for opt in opts:
            if not isinstance(opt, dict):
                continue
            opt_name = str(opt.get("name") or "").strip()
            opt_type = opt.get("type")
            # Subcommand group / subcommand
            if opt_type in {1, 2}:
                if opt_name:
                    tokens.append(opt_name)
                _walk(opt.get("options"))
                continue
            # Scalar option
            if opt_name and "value" in opt:
                val = str(opt.get("value") or "").strip()
                if val:
                    tokens.append(f"{opt_name}={val}")
                else:
                    tokens.append(opt_name)

    _walk(data.get("options"))
    return tokens


def _parse_discord_request(
    *, payload: dict[str, Any], model: Any
) -> tuple[IngressRequest, _OutboundTarget, dict[str, Any]]:
    """Parse a Discord interaction payload into an IngressRequest + follow-up target.

    Returns (ingress_request, followup_target, initial_response_json).
    """
    # Stable identifiers for routing + follow-up
    interaction_id = str(payload.get("id") or "").strip()
    application_id = str(payload.get("application_id") or "").strip()
    interaction_token = str(payload.get("token") or "").strip()
    require_non_empty(interaction_id, name="discord.interaction.id")
    require_non_empty(application_id, name="discord.application_id")
    require_non_empty(interaction_token, name="discord.interaction.token")

    # Workspace routing: treat guild_id as the "workspace_id" for discord.
    guild_id = str(payload.get("guild_id") or "").strip()
    require_non_empty(guild_id, name="discord.guild_id")
    tenant_id = _tenant_for_workspace(model=model, channel="discord", workspace_id=guild_id)

    # Principal: prefer member.user.id (guild) and fall back to user.id (DM-like).
    principal_id = ""
    member = payload.get("member")
    if isinstance(member, dict):
        user = member.get("user")
        if isinstance(user, dict):
            principal_id = str(user.get("id") or "").strip()
    if not principal_id:
        user = payload.get("user")
        if isinstance(user, dict):
            principal_id = str(user.get("id") or "").strip()
    require_non_empty(principal_id, name="discord.user.id")

    interaction_type = int(payload.get("type") or 0)
    initial = _discord_interaction_initial_response(interaction_type=interaction_type)

    # Build a deterministic raw_text for the command engine.
    raw_text = ""
    data = payload.get("data")
    if isinstance(data, dict):
        # For buttons/components, prefer a stable custom_id value.
        custom_id = str(data.get("custom_id") or "").strip()
        if custom_id:
            raw_text = custom_id
        else:
            toks = _discord_flatten_command_tokens(data)
            raw_text = " ".join(toks).strip()
    if not raw_text:
        raw_text = "status runtime"
    raw_text = _normalize_operator_text(raw_text)

    event_id = f"discord:{interaction_id}"
    req = IngressRequest(
        channel="discord",
        event_id=event_id,
        principal_id=principal_id,
        tenant_id=tenant_id,
        raw_text=raw_text,
        payload={
            "discord": payload,
            "discord_application_id": application_id,
            "discord_interaction_token": interaction_token,
            "discord_guild_id": guild_id,
            "discord_interaction_type": interaction_type,
        },
        received_at_ms=None,
    )
    followup = _OutboundTarget(
        channel="discord",
        data={
            "application_id": application_id,
            "interaction_token": interaction_token,
        },
        ephemeral=True,
    )
    return req, followup, initial


def process_inbound_event(
    *,
    event: InboundEvent,
    principals: dict[str, Any],
    allowed_tenants: set[str],
    engine: CommandEngine,
    approvals: ApprovalWorkflow,
    outbound: OutboundAdapter,
    event_store: InboundEventStore,
    result_store: CommandResultStore | None = None,
    now_ms: int | None = None,
    audit_writer: ControlBotAuditWriter | None = None,
) -> OutboundMessage | None:
    """Process one inbound event, persisting before any execution.

    Persist-first is a correctness/safety invariant: even denied/failed events must be auditable.
    """
    ev = event.normalized()
    ev.validate()

    # Persist *before* any policy/identity checks or execution.
    pres = event_store.persist(ev)
    if bool(getattr(pres, "is_duplicate", False)):
        # Deduplicate inbound events early: retries should be no-ops.
        return None

    if audit_writer is not None:
        audit_writer.append(
            event_type="control.bot.command.received",
            event=ev,
            principal=None,
            command=None,
            result=None,
            request_hash=None,
            details={"dedupe_reason": str(getattr(pres, "reason", "new"))},
        )

    pid = ev.principal_id.strip()
    if pid not in principals:
        if audit_writer is not None:
            audit_writer.append(
                event_type="control.bot.command.denied",
                event=ev,
                principal=None,
                command=None,
                result=None,
                request_hash=None,
                reason="unknown principal_id",
            )
        raise PolicyDenied("unknown principal_id")
    ident = principals[pid]
    ident_tenant = str(getattr(ident, "tenant_id", "") or "").strip()
    if ident_tenant != ev.tenant_id.strip():
        if audit_writer is not None:
            audit_writer.append(
                event_type="control.bot.command.denied",
                event=ev,
                principal=None,
                command=None,
                result=None,
                request_hash=None,
                reason="tenant isolation violated",
            )
        raise ValueError("tenant isolation violated: principal tenant_id != event tenant_id")
    if ev.tenant_id.strip() not in allowed_tenants:
        if audit_writer is not None:
            audit_writer.append(
                event_type="control.bot.command.denied",
                event=ev,
                principal=None,
                command=None,
                result=None,
                request_hash=None,
                reason="tenant not allowed by routing.tenants",
            )
        raise PolicyDenied("tenant not allowed by routing.tenants")

    ms = int(now_ms if now_ms is not None else default_now_ms())
    principal = Principal(principal_id=pid, tenant_id=ident_tenant, roles=tuple(getattr(ident, "roles", ()) or ()))
    ctx = CommandContext(event=ev, principal=principal, now_ms=ms)
    cmd: Command | None = None
    res: CommandResult | None = None
    try:
        cmd = engine.parse(ev.raw_text)
    except CommandClarificationRequired as e:
        # Deterministic clarification response; treat as a finished outcome so retries can replay.
        cmd = Command(action_id="clarification.required", args={}, raw_text=ev.raw_text, parser="nl_fallback")
        res = CommandResult(
            ok=False,
            action_id="clarification.required",
            message=str(e.message or "Ambiguous command."),
            data={"candidates": list(e.candidates)},
            status="clarification_required",
        )
    except Exception as e:
        # Parsing failures are operator-facing errors; persist outcome for audit/replay safety.
        cmd = Command(action_id="parse.error", args={}, raw_text=ev.raw_text, parser="strict")
        res = CommandResult(
            ok=False,
            action_id="parse.error",
            message=str(e) or "could not parse command",
            data={"error_type": type(e).__name__},
            status="error",
        )

    # Idempotency / retry safety:
    # - for a given (tenant,channel,event_id,payload_hash,action,args), only one worker executes
    # - retries can replay the previous finished result (or do nothing if still in-flight)
    store = result_store or InMemoryCommandResultStore()
    if cmd is None:
        raise RuntimeError("internal error: cmd is None after parse handling")
    request_hash = stable_command_request_hash(ev=ev, action_id=cmd.action_id, args=cmd.args)
    finished = store.get_finished(request_hash=request_hash)
    if finished is not None:
        # Discord retries are common; suppress duplicate follow-ups by returning None on replay.
        if ev.channel == "discord":
            return None
        return outbound.render(finished)
    began = store.try_begin(ev=ev, action_id=cmd.action_id, args=cmd.args, request_hash=request_hash)
    if not began:
        # Another worker is processing or has finished but we raced reading; safest is to no-op.
        return None

    # If we already produced a deterministic result during parse (clarification or parse error),
    # mark it finished immediately.
    if res is not None:
        store.finish(request_hash=request_hash, result=res, finished_at_ms=ms)
        if audit_writer is not None:
            audit_writer.append(
                event_type="control.bot.command.failed",
                event=ev,
                principal=principal,
                command=cmd,
                result=res,
                request_hash=request_hash,
            )
        return outbound.render(res)

    if approvals.requires_approval(cmd.action_id):
        # Idempotency: retries of the same inbound event should not create multiple approval rows.
        # Include action_id so different commands in the same payload hash don't collide.
        idempotency_key = f"{ev.channel}:{ev.event_id}:{cmd.action_id}:{ev.payload_hash}"
        req = approvals.create_request(
            tenant_id=principal.tenant_id,
            action_id=cmd.action_id,
            args_hash=stable_args_fingerprint(cmd.args),
            args=dict(cmd.args),
            requester_principal_id=principal.principal_id,
            idempotency_key=idempotency_key,
            now_ms=ms,
        )
        logger.info(
            "approval_requested tenant=%s principal=%s action=%s request_id=%s",
            principal.tenant_id,
            pid,
            cmd.action_id,
            req.request_id,
        )
        res = CommandResult(
            ok=False,
            action_id=cmd.action_id,
            message=(
                "Approval required.\n"
                f"- request_id: {req.request_id}\n"
                f"- action: {cmd.action_id}\n"
                f"- tenant: {principal.tenant_id}"
            ),
            data={"request_id": req.request_id, "action_id": cmd.action_id, "tenant_id": principal.tenant_id},
            request_id=req.request_id,
            status="approval_required",
        )
        store.finish(request_hash=request_hash, result=res, finished_at_ms=ms)
        if audit_writer is not None:
            audit_writer.append(
                event_type="control.bot.command.approval_requested",
                event=ev,
                principal=principal,
                command=cmd,
                result=res,
                request_hash=request_hash,
            )
        return outbound.render(res)

    try:
        res2 = engine.execute(ctx=ctx, cmd=cmd)
    except PolicyDenied as e:
        res2 = CommandResult(
            ok=False,
            action_id=cmd.action_id,
            message=str(e) or "policy denied",
            data={"action_id": cmd.action_id, "tenant_id": principal.tenant_id},
            status="denied",
        )
    except Exception as e:
        res2 = CommandResult(
            ok=False,
            action_id=cmd.action_id,
            message=str(e) or "command execution failed",
            data={"error_type": type(e).__name__, "action_id": cmd.action_id, "tenant_id": principal.tenant_id},
            status="error",
        )
    store.finish(request_hash=request_hash, result=res2, finished_at_ms=ms)
    if audit_writer is not None:
        if cmd.action_id == "approval.approve" and res2.ok:
            audit_writer.append(
                event_type="control.bot.command.approved",
                event=ev,
                principal=principal,
                command=cmd,
                result=res2,
                request_hash=request_hash,
            )
        if cmd.action_id == "approval.deny" and res2.ok:
            audit_writer.append(
                event_type="control.bot.command.denied",
                event=ev,
                principal=principal,
                command=cmd,
                result=res2,
                request_hash=request_hash,
            )
        if res2.ok:
            audit_writer.append(
                event_type="control.bot.command.executed",
                event=ev,
                principal=principal,
                command=cmd,
                result=res2,
                request_hash=request_hash,
            )
        elif str(res2.status) == "denied":
            audit_writer.append(
                event_type="control.bot.command.denied",
                event=ev,
                principal=principal,
                command=cmd,
                result=res2,
                request_hash=request_hash,
            )
        else:
            audit_writer.append(
                event_type="control.bot.command.failed",
                event=ev,
                principal=principal,
                command=cmd,
                result=res2,
                request_hash=request_hash,
            )

    if res2.ok:
        _ = maybe_mirror_control_mutation(event=ev, principal=principal, command=cmd, result=res2)

    msg = outbound.render(res2)
    logger.info("executed tenant=%s principal=%s action=%s ok=%s", principal.tenant_id, pid, res2.action_id, res2.ok)
    return msg


def run_control_bot_server(cfg: ControlBotServerConfig) -> None:
    loaded = cfg.loaded
    model = loaded.model

    principals = model.principal_identities()
    allowed_tenants = {t.strip() for t in model.routing.tenants}

    adapter = PassthroughIngressAdapter(channel="unknown")
    # Build engine first with a placeholder registry; we replace registry after wiring deps.
    engine = CommandEngine(registry=ActionRegistry(handlers={}))
    prefixes = tuple(
        str(p or "").strip() for p in model.approval.requires_approval_action_prefixes if str(p or "").strip()
    )

    def _requires_approval(action_id: str) -> bool:
        aid = str(action_id or "").strip().lower()
        return any(aid.startswith(p.lower()) for p in prefixes)

    approvals = ApprovalWorkflow(
        store=SqliteApprovalStore(sqlite_path=Path(str(model.storage.sqlite_path))),
        requires_approval=_requires_approval,
        default_ttl_ms=int(model.approval.default_ttl_ms),
        allow_self_approval=bool(model.approval.allow_self_approval),
    )
    deps = V1ActionDeps(
        approvals=approvals,
        engine_execute=lambda ctx, action_id, args: engine.execute(
            ctx=ctx,
            cmd=Command(
                action_id=str(action_id),
                args=dict(args),
                raw_text=f"akc {str(action_id).replace('.', ' ')}",
                parser="strict",
            ),
        ),
    )
    registry = build_action_registry_v1(deps=deps)
    engine.registry = registry

    # Policy gate (default deny): require role allowlist; optional OPA hook.
    role_allowlist = build_role_allowlist(getattr(model.policy, "role_allowlist", None))
    opa_client: OPAClient | None = None
    if bool(getattr(getattr(model.policy, "opa", None), "enabled", False)):
        opa_url = str(getattr(model.policy.opa, "policy_path", "") or "").strip()
        if opa_url:
            opa_client = OPAClient(
                cfg=OPAConfig(
                    url=opa_url,
                    decision_path=str(getattr(model.policy.opa, "decision_path", "data.akc.allow") or "data.akc.allow"),
                    timeout_ms=int(getattr(model.policy.opa, "timeout_ms", 1500) or 1500),
                )
            )
    gate = PolicyGate(
        mode=str(getattr(model.policy, "mode", "enforce") or "enforce"),
        role_allowlist=role_allowlist,
        opa=opa_client,
    )
    engine.policy_decide = lambda ctx, cmd: gate.decide(ctx=ctx, cmd=cmd)

    state_dir = Path(str(model.storage.state_dir)).expanduser().resolve()
    state_dir.mkdir(parents=True, exist_ok=True)
    audit_log = default_control_bot_audit_log_path(
        state_dir=state_dir,
        explicit_path=getattr(model.storage, "audit_log_path", None),
    )
    audit_writer = ControlBotAuditWriter(audit_log_path=audit_log)
    event_store: InboundEventStore = SqliteInboundEventStore(sqlite_path=Path(str(model.storage.sqlite_path)))
    result_store: CommandResultStore = SqliteCommandResultStore(sqlite_path=Path(str(model.storage.sqlite_path)))

    outbound_adapters: dict[str, OutboundAdapter] = {
        "slack": SlackOutboundAdapter(),
        "discord": DiscordOutboundAdapter(),
        "telegram": TelegramOutboundAdapter(),
        "whatsapp": WhatsAppOutboundAdapter(),
        "unknown": TextOutboundAdapter(channel="unknown"),
    }
    outbound_rl = _PerChannelRateLimiter(
        min_interval_s={
            # Conservatively low rates; keeps webhook workers responsive under burst.
            "slack": 0.25,
            "discord": 0.4,
            "telegram": 0.1,
            "whatsapp": 0.25,
            "unknown": 0.0,
        }
    )

    q: queue.Queue[_WorkItem] = queue.Queue(maxsize=int(model.server.queue_max))

    def _dispatch_outbound(*, target: _OutboundTarget, msg: OutboundMessage) -> None:
        outbound_rl.wait(target.channel)
        ch = str(target.channel or "").strip().lower()
        if ch == "discord":
            send_discord_followup_message(
                application_id=str(target.data.get("application_id") or ""),
                interaction_token=str(target.data.get("interaction_token") or ""),
                content=msg.text,
                payload=msg.payload,
                ephemeral=bool(target.ephemeral),
            )
            return
        if ch == "slack":
            response_url = str(target.data.get("response_url") or "").strip()
            if not response_url:
                return
            send_slack_response_message(response_url=response_url, text=msg.text, payload=msg.payload)
            return
        if ch == "telegram":
            token = str(getattr(model.channels.telegram, "bot_token", "") or "").strip()
            if not token:
                logger.warning("telegram outbound skipped: channels.telegram.bot_token not configured")
                return
            chat_id = str(target.data.get("chat_id") or "").strip()
            if not chat_id:
                return
            payload = msg.payload if isinstance(msg.payload, dict) else {}
            reply_markup = payload.get("reply_markup")
            rm_obj = reply_markup if isinstance(reply_markup, dict) else None
            send_telegram_message(
                bot_token=token,
                chat_id=chat_id,
                text=msg.text,
                reply_markup=rm_obj,
                api_base_url=str(getattr(model.channels.telegram, "api_base_url", "https://api.telegram.org")),
            )
            return
        if ch == "whatsapp":
            access_token = str(getattr(model.channels.whatsapp, "access_token", "") or "").strip()
            if not access_token:
                logger.warning("whatsapp outbound skipped: channels.whatsapp.access_token not configured")
                return
            to_id = str(target.data.get("to") or "").strip()
            phone_number_id = (
                str(target.data.get("phone_number_id") or "").strip()
                or str(getattr(model.channels.whatsapp, "phone_number_id", "") or "").strip()
            )
            if not to_id or not phone_number_id:
                return
            send_whatsapp_text_message(
                access_token=access_token,
                phone_number_id=phone_number_id,
                to=to_id,
                text=msg.text,
                api_base_url=str(getattr(model.channels.whatsapp, "api_base_url", "https://graph.facebook.com")),
                api_version=str(getattr(model.channels.whatsapp, "api_version", "v19.0")),
            )
            return

    def _process(item: _WorkItem) -> None:
        out_adapter = outbound_adapters.get(str(item.event.channel), outbound_adapters["unknown"])
        msg = process_inbound_event(
            event=item.event,
            principals=principals,
            allowed_tenants=allowed_tenants,
            engine=engine,
            approvals=approvals,
            outbound=out_adapter,
            event_store=event_store,
            result_store=result_store,
            audit_writer=audit_writer,
        )
        if item.outbound_target is not None and msg is not None:
            _dispatch_outbound(target=item.outbound_target, msg=msg)

    def _worker_main(worker_id: int) -> None:
        while True:
            item = q.get()
            try:
                _process(item)
            except Exception as e:
                logger.exception("control-bot worker error worker_id=%s: %s", worker_id, e)
            finally:
                q.task_done()

    for i in range(int(model.server.worker_threads)):
        t = threading.Thread(target=_worker_main, args=(i,), daemon=True)
        t.start()

    class Handler(BaseHTTPRequestHandler):
        server_version = "akc-control-bot/0.1"

        def log_message(self, fmt: str, *args: object) -> None:  # pragma: no cover
            logger.info("%s - %s", self.address_string(), fmt % args)

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path
            if path in {"/healthz", "/livez"}:
                return _json_response(self, status=200, body={"ok": True})
            if path == "/v1/channels/whatsapp/webhook":
                # Meta webhook verification handshake:
                # hub.mode=subscribe&hub.verify_token=...&hub.challenge=...
                qs = parse_qs(parsed.query or "", keep_blank_values=True, strict_parsing=False)
                mode = (qs.get("hub.mode") or [""])[0]
                token = (qs.get("hub.verify_token") or [""])[0]
                challenge = (qs.get("hub.challenge") or [""])[0]

                try:
                    ch = verify_whatsapp_webhook_verification(
                        enabled=bool(model.channels.whatsapp.enabled),
                        expected_verify_token=model.channels.whatsapp.verify_token,
                        mode=mode,
                        verify_token=token,
                        challenge=challenge,
                    )
                except IngressError:
                    return _json_response(self, status=403, body={"ok": False, "error": "verification_failed"})

                data = ch.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                return
            return _json_response(self, status=404, body={"ok": False, "error": "not_found"})

        def do_POST(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            if not path.startswith("/v1/"):
                return _json_response(self, status=404, body={"ok": False, "error": "not_found"})

            if path in {"/v1/channels/slack/commands", "/v1/channels/slack/interactivity"}:
                channel = "slack"
            elif path == "/v1/channels/discord/interactions":
                channel = "discord"
            elif path == "/v1/channels/telegram/webhook":
                channel = "telegram"
            elif path in {"/v1/channels/whatsapp/webhook"}:
                channel = "whatsapp"
            elif path == "/v1/events":
                channel = "unknown"
            else:
                return _json_response(self, status=404, body={"ok": False, "error": "not_found"})

            try:
                body = _read_body(self)
                headers = _lower_headers(self)
                outbound_target: _OutboundTarget | None = None

                auth_ctx = IngressAuthContext(channel=channel, headers=headers, body=body)
                if channel == "slack":
                    verify_slack_request(
                        auth_ctx,
                        enabled=bool(model.channels.slack.enabled),
                        signing_secret=model.channels.slack.signing_secret,
                    )
                elif channel == "discord":
                    verify_discord_request(
                        auth_ctx,
                        enabled=bool(model.channels.discord.enabled),
                        public_key=model.channels.discord.public_key,
                    )
                elif channel == "telegram":
                    verify_telegram_request(
                        auth_ctx,
                        enabled=bool(model.channels.telegram.enabled),
                        secret_token=model.channels.telegram.secret_token,
                    )
                elif channel == "whatsapp":
                    verify_whatsapp_request(
                        auth_ctx,
                        enabled=bool(model.channels.whatsapp.enabled),
                        app_secret=model.channels.whatsapp.app_secret,
                    )

                if channel == "slack":
                    endpoint = "commands" if path.endswith("/commands") else "interactivity"
                    req = _parse_slack_request(body=body, headers=headers, model=model, endpoint=endpoint)
                    outbound_target = _slack_outbound_target(req)
                elif channel == "discord":
                    payload = json.loads(body.decode("utf-8") or "{}")
                    if not isinstance(payload, dict):
                        raise ValueError("payload must be a JSON object")
                    req, followup, initial = _parse_discord_request(payload=payload, model=model)
                    outbound_target = followup
                    # Discord PING must not be deferred or queued.
                    if int(payload.get("type") or 0) == 1:
                        return _json_response(self, status=200, body=initial)
                elif channel == "telegram":
                    payload = json.loads(body.decode("utf-8") or "{}")
                    if not isinstance(payload, dict):
                        raise ValueError("payload must be a JSON object")
                    req = _parse_telegram_request(payload=payload, model=model)
                    outbound_target = _telegram_outbound_target(req)
                elif channel == "whatsapp":
                    payload = json.loads(body.decode("utf-8") or "{}")
                    if not isinstance(payload, dict):
                        raise ValueError("payload must be a JSON object")
                    req, outbound_target = _parse_whatsapp_request(payload=payload, model=model, body=body)
                else:
                    payload = json.loads(body.decode("utf-8") or "{}")
                    if not isinstance(payload, dict):
                        raise ValueError("payload must be a JSON object")
                    payload["channel"] = channel
                    req = _parse_event_from_json_payload(payload)

                require_non_empty(req.tenant_id, name="tenant_id")
                require_non_empty(req.principal_id, name="principal_id")
                require_non_empty(req.event_id, name="event_id")
                require_non_empty(req.raw_text, name="raw_text")

                if channel == "unknown":
                    ev = adapter.parse(req)
                else:
                    ev = PassthroughIngressAdapter(channel=channel).parse(req)  # type: ignore[arg-type]
                try:
                    q.put_nowait(_WorkItem(event=ev, outbound_target=outbound_target))
                except queue.Full:
                    return _json_response(self, status=503, body={"ok": False, "error": "queue_full"})

                # ACK quickly; processing is async.
                if channel == "slack":
                    # Slack expects a response within 3 seconds; keep ACK lightweight.
                    return _empty_response(self, status=200)
                if channel == "discord":
                    return _json_response(self, status=200, body=initial)
                return _json_response(
                    self,
                    status=202,
                    body={"ok": True, "status": "accepted", "event_id": ev.event_id},
                )
            except IngressError as e:
                # Signature/secret token failures should be treated as unauthorized.
                msg = str(e) or "unauthorized"
                return _json_response(self, status=401, body={"ok": False, "error": msg})
            except Exception as e:
                msg = str(e) or "bad_request"
                return _json_response(self, status=400, body={"ok": False, "error": msg})

    host = str(model.server.bind).strip()
    port = int(model.server.port)
    if not host:
        raise ControlBotServerError("server.bind is empty")
    if not (1 <= port <= 65535):
        raise ControlBotServerError("server.port out of range")

    httpd = ThreadingHTTPServer((host, port), Handler)
    logger.info("control-bot serve config=%s bind=%s port=%s", str(loaded.path), host, port)
    try:
        httpd.serve_forever(poll_interval=0.5)
    finally:
        httpd.server_close()
