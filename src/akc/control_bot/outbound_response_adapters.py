from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol
from urllib import request

from akc.control_bot.command_engine import ChannelId, CommandResult
from akc.memory.models import require_non_empty


class OutboundError(Exception):
    """Raised when a channel response cannot be rendered."""


@dataclass(frozen=True, slots=True)
class OutboundMessage:
    channel: ChannelId
    text: str
    ephemeral: bool = True
    payload: dict[str, Any] | None = None

    def validate(self) -> None:
        require_non_empty(self.text, name="text")


class OutboundAdapter(Protocol):
    channel: ChannelId

    def render(self, result: CommandResult) -> OutboundMessage: ...


@dataclass(slots=True)
class TextOutboundAdapter:
    channel: ChannelId = "unknown"
    ephemeral_by_default: bool = True

    def render(self, result: CommandResult) -> OutboundMessage:
        msg = (result.message or "").strip()
        if not msg:
            raise OutboundError("empty result message")
        out = OutboundMessage(channel=self.channel, text=msg, ephemeral=self.ephemeral_by_default)
        out.validate()
        return out


def _approval_actions_for_request_id(request_id: str | None) -> tuple[str, str] | None:
    rid = str(request_id or "").strip()
    if not rid:
        return None
    return (f"akc approval approve request_id={rid}", f"akc approval deny request_id={rid}")


def _status_prefix(result: CommandResult) -> str:
    st = str(result.status or "").strip()
    if st == "executed" and result.ok:
        return "[OK]"
    if st in {"approval_required", "clarification_required"}:
        return "[PENDING]"
    if st == "denied":
        return "[DENIED]"
    return "[ERROR]"


@dataclass(slots=True)
class SlackOutboundAdapter:
    channel: ChannelId = "slack"
    ephemeral_by_default: bool = True

    def render(self, result: CommandResult) -> OutboundMessage:
        msg = str(result.message or "").strip()
        if not msg:
            raise OutboundError("empty result message")
        title = f"{_status_prefix(result)} `{result.action_id}`"
        blocks: list[dict[str, Any]] = [
            {"type": "section", "text": {"type": "mrkdwn", "text": title}},
            {"type": "section", "text": {"type": "mrkdwn", "text": msg}},
        ]
        ap = _approval_actions_for_request_id(result.request_id)
        if ap is not None and result.status == "approval_required":
            approve_cmd, deny_cmd = ap
            blocks.append(
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "style": "primary",
                            "text": {"type": "plain_text", "text": "Approve"},
                            "value": approve_cmd,
                            "action_id": "approval.approve",
                        },
                        {
                            "type": "button",
                            "style": "danger",
                            "text": {"type": "plain_text", "text": "Deny"},
                            "value": deny_cmd,
                            "action_id": "approval.deny",
                        },
                    ],
                }
            )
        payload = {
            "response_type": "ephemeral" if self.ephemeral_by_default else "in_channel",
            "replace_original": False,
            "text": msg,
            "blocks": blocks,
        }
        out = OutboundMessage(
            channel="slack",
            text=msg,
            ephemeral=self.ephemeral_by_default,
            payload=payload,
        )
        out.validate()
        return out


@dataclass(slots=True)
class DiscordOutboundAdapter:
    channel: ChannelId = "discord"
    ephemeral_by_default: bool = True

    def render(self, result: CommandResult) -> OutboundMessage:
        msg = str(result.message or "").strip()
        if not msg:
            raise OutboundError("empty result message")
        color = 0x2EB67D if result.ok else 0xE01E5A
        embed: dict[str, Any] = {
            "title": f"{_status_prefix(result)} {result.action_id}",
            "description": msg,
            "color": color,
        }
        payload: dict[str, Any] = {"content": msg, "embeds": [embed]}
        ap = _approval_actions_for_request_id(result.request_id)
        if ap is not None and result.status == "approval_required":
            approve_cmd, deny_cmd = ap
            payload["components"] = [
                {
                    "type": 1,
                    "components": [
                        {"type": 2, "style": 3, "label": "Approve", "custom_id": approve_cmd},
                        {"type": 2, "style": 4, "label": "Deny", "custom_id": deny_cmd},
                    ],
                }
            ]
        out = OutboundMessage(
            channel="discord",
            text=msg,
            ephemeral=self.ephemeral_by_default,
            payload=payload,
        )
        out.validate()
        return out


@dataclass(slots=True)
class TelegramOutboundAdapter:
    channel: ChannelId = "telegram"
    ephemeral_by_default: bool = False

    def render(self, result: CommandResult) -> OutboundMessage:
        msg = str(result.message or "").strip()
        if not msg:
            raise OutboundError("empty result message")
        payload: dict[str, Any] = {"text": msg}
        ap = _approval_actions_for_request_id(result.request_id)
        if ap is not None and result.status == "approval_required":
            approve_cmd, deny_cmd = ap
            payload["reply_markup"] = {
                "inline_keyboard": [
                    [
                        {"text": "Approve", "callback_data": approve_cmd},
                        {"text": "Deny", "callback_data": deny_cmd},
                    ]
                ]
            }
        out = OutboundMessage(
            channel="telegram",
            text=msg,
            ephemeral=self.ephemeral_by_default,
            payload=payload,
        )
        out.validate()
        return out


@dataclass(slots=True)
class WhatsAppOutboundAdapter:
    channel: ChannelId = "whatsapp"
    ephemeral_by_default: bool = False

    def render(self, result: CommandResult) -> OutboundMessage:
        msg = str(result.message or "").strip()
        if not msg:
            raise OutboundError("empty result message")
        rid = str(result.request_id or "").strip()
        text = msg
        if rid:
            text = (
                f"{msg}\n\n"
                f"request_id: {rid}\n"
                f"Reply with: approve {rid}  (or)  deny {rid}\n"
                f"Fallback full command: akc approval approve request_id={rid}"
            )
        out = OutboundMessage(
            channel="whatsapp",
            text=text,
            ephemeral=self.ephemeral_by_default,
            payload={"text": {"body": text}},
        )
        out.validate()
        return out


def send_discord_followup_message(
    *,
    application_id: str,
    interaction_token: str,
    content: str,
    payload: Mapping[str, Any] | None = None,
    ephemeral: bool = True,
    api_base_url: str = "https://discord.com/api/v10",
    timeout_s: float = 5.0,
) -> None:
    """Send a Discord follow-up message for an interaction."""

    app_id = str(application_id or "").strip()
    tok = str(interaction_token or "").strip()
    msg = str(content or "").strip()
    require_non_empty(app_id, name="application_id")
    require_non_empty(tok, name="interaction_token")
    require_non_empty(msg, name="content")

    base = str(api_base_url or "").strip().rstrip("/")
    require_non_empty(base, name="api_base_url")

    url = f"{base}/webhooks/{app_id}/{tok}"
    body_obj: dict[str, Any]
    if payload is not None:
        body_obj = dict(payload)
        if "content" not in body_obj:
            body_obj["content"] = msg
    else:
        body_obj = {"content": msg}
    if ephemeral and "flags" not in body_obj:
        body_obj["flags"] = 64

    data = (json_dumps_compact(body_obj)).encode("utf-8")
    req = request.Request(url=url, data=data, method="POST")
    req.add_header("Content-Type", "application/json; charset=utf-8")
    req.add_header("Content-Length", str(len(data)))
    try:
        with request.urlopen(req, timeout=float(timeout_s)) as resp:
            _ = resp.status
    except Exception as e:  # pragma: no cover
        raise OutboundError(f"discord follow-up failed: {e}") from e


def send_slack_response_message(
    *,
    response_url: str,
    text: str,
    payload: Mapping[str, Any] | None = None,
    timeout_s: float = 5.0,
) -> None:
    url = str(response_url or "").strip()
    msg = str(text or "").strip()
    require_non_empty(url, name="response_url")
    require_non_empty(msg, name="text")
    body_obj = dict(payload) if payload is not None else {"text": msg, "replace_original": False}
    if "text" not in body_obj:
        body_obj["text"] = msg
    data = json_dumps_compact(body_obj).encode("utf-8")
    req = request.Request(url=url, data=data, method="POST")
    req.add_header("Content-Type", "application/json; charset=utf-8")
    req.add_header("Content-Length", str(len(data)))
    try:
        with request.urlopen(req, timeout=float(timeout_s)) as resp:
            _ = resp.status
    except Exception as e:  # pragma: no cover
        raise OutboundError(f"slack response failed: {e}") from e


def send_telegram_message(
    *,
    bot_token: str,
    chat_id: str,
    text: str,
    reply_markup: Mapping[str, Any] | None = None,
    api_base_url: str = "https://api.telegram.org",
    timeout_s: float = 5.0,
) -> None:
    tok = str(bot_token or "").strip()
    cid = str(chat_id or "").strip()
    msg = str(text or "").strip()
    base = str(api_base_url or "").strip().rstrip("/")
    require_non_empty(tok, name="bot_token")
    require_non_empty(cid, name="chat_id")
    require_non_empty(msg, name="text")
    require_non_empty(base, name="api_base_url")
    url = f"{base}/bot{tok}/sendMessage"
    body_obj: dict[str, Any] = {"chat_id": cid, "text": msg}
    if reply_markup is not None:
        body_obj["reply_markup"] = dict(reply_markup)
    data = json_dumps_compact(body_obj).encode("utf-8")
    req = request.Request(url=url, data=data, method="POST")
    req.add_header("Content-Type", "application/json; charset=utf-8")
    req.add_header("Content-Length", str(len(data)))
    try:
        with request.urlopen(req, timeout=float(timeout_s)) as resp:
            _ = resp.status
    except Exception as e:  # pragma: no cover
        raise OutboundError(f"telegram sendMessage failed: {e}") from e


def send_whatsapp_text_message(
    *,
    access_token: str,
    phone_number_id: str,
    to: str,
    text: str,
    api_base_url: str = "https://graph.facebook.com",
    api_version: str = "v19.0",
    timeout_s: float = 5.0,
) -> None:
    tok = str(access_token or "").strip()
    pnid = str(phone_number_id or "").strip()
    to_id = str(to or "").strip()
    msg = str(text or "").strip()
    base = str(api_base_url or "").strip().rstrip("/")
    ver = str(api_version or "").strip()
    require_non_empty(tok, name="access_token")
    require_non_empty(pnid, name="phone_number_id")
    require_non_empty(to_id, name="to")
    require_non_empty(msg, name="text")
    require_non_empty(base, name="api_base_url")
    require_non_empty(ver, name="api_version")
    url = f"{base}/{ver}/{pnid}/messages"
    body_obj = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": to_id,
        "type": "text",
        "text": {"preview_url": False, "body": msg},
    }
    data = json_dumps_compact(body_obj).encode("utf-8")
    req = request.Request(url=url, data=data, method="POST")
    req.add_header("Authorization", f"Bearer {tok}")
    req.add_header("Content-Type", "application/json; charset=utf-8")
    req.add_header("Content-Length", str(len(data)))
    try:
        with request.urlopen(req, timeout=float(timeout_s)) as resp:
            _ = resp.status
    except Exception as e:  # pragma: no cover
        raise OutboundError(f"whatsapp message send failed: {e}") from e


def json_dumps_compact(obj: object) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
