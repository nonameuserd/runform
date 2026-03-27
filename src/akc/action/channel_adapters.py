from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from akc.action.models import ActionChannel, ActionInboundMessageEnvelopeV1
from akc.memory.models import now_ms


class ChannelAdapterError(ValueError):
    """Raised when channel payload cannot be normalized."""


class ActionChannelAdapter(Protocol):
    channel: ActionChannel

    def parse_inbound(self, payload: dict[str, Any]) -> ActionInboundMessageEnvelopeV1: ...


def _required_str(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ChannelAdapterError(f"payload missing required field: {key}")
    return value.strip()


def _optional_str(payload: dict[str, Any], key: str) -> str | None:
    value = payload.get(key)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


@dataclass(slots=True)
class _BaseAdapter:
    channel: ActionChannel

    def _from_payload(self, payload: dict[str, Any]) -> ActionInboundMessageEnvelopeV1:
        if payload.get("schema_kind") == "action_inbound_message_envelope":
            normalized = ActionInboundMessageEnvelopeV1.from_json_obj(payload)
            if normalized.channel != self.channel:
                raise ChannelAdapterError(
                    f"payload channel mismatch: expected {self.channel}, got {normalized.channel}"
                )
            return normalized
        metadata = payload.get("metadata")
        metadata_obj = dict(metadata) if isinstance(metadata, dict) else {}
        return ActionInboundMessageEnvelopeV1(
            schema_kind="action_inbound_message_envelope",
            schema_version=1,
            channel=self.channel,
            tenant_id=_required_str(payload, "tenant_id"),
            repo_id=_required_str(payload, "repo_id"),
            text=_required_str(payload, "text"),
            actor_id=_optional_str(payload, "actor_id"),
            message_id=_optional_str(payload, "message_id"),
            received_at_ms=(
                int(payload["received_at_ms"]) if isinstance(payload.get("received_at_ms"), int) else now_ms()
            ),
            metadata=metadata_obj,
        )


@dataclass(slots=True)
class CliActionChannelAdapter(_BaseAdapter):
    channel: ActionChannel = "cli"

    def parse_inbound(self, payload: dict[str, Any]) -> ActionInboundMessageEnvelopeV1:
        return self._from_payload(payload)


@dataclass(slots=True)
class SlackActionChannelAdapter(_BaseAdapter):
    channel: ActionChannel = "slack"

    def parse_inbound(self, payload: dict[str, Any]) -> ActionInboundMessageEnvelopeV1:
        return self._from_payload(payload)


@dataclass(slots=True)
class DiscordActionChannelAdapter(_BaseAdapter):
    channel: ActionChannel = "discord"

    def parse_inbound(self, payload: dict[str, Any]) -> ActionInboundMessageEnvelopeV1:
        return self._from_payload(payload)


@dataclass(slots=True)
class TelegramActionChannelAdapter(_BaseAdapter):
    channel: ActionChannel = "telegram"

    def parse_inbound(self, payload: dict[str, Any]) -> ActionInboundMessageEnvelopeV1:
        return self._from_payload(payload)


@dataclass(slots=True)
class WhatsAppActionChannelAdapter(_BaseAdapter):
    channel: ActionChannel = "whatsapp"

    def parse_inbound(self, payload: dict[str, Any]) -> ActionInboundMessageEnvelopeV1:
        return self._from_payload(payload)


class ActionChannelAdapters:
    def __init__(self) -> None:
        self._by_channel: dict[str, ActionChannelAdapter] = {
            "cli": CliActionChannelAdapter(),
            "slack": SlackActionChannelAdapter(),
            "discord": DiscordActionChannelAdapter(),
            "telegram": TelegramActionChannelAdapter(),
            "whatsapp": WhatsAppActionChannelAdapter(),
        }

    def parse_inbound(self, *, channel: str, payload: dict[str, Any]) -> ActionInboundMessageEnvelopeV1:
        adapter = self._by_channel.get(channel.strip().lower())
        if adapter is None:
            raise ChannelAdapterError(f"unsupported channel adapter: {channel!r}")
        return adapter.parse_inbound(payload)
