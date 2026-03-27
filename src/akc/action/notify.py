from __future__ import annotations

from akc.action.models import ActionChannel, ActionIntentV1, ActionOutboundResponseEnvelopeV1
from akc.memory.models import JSONValue


def _notification_channel(raw: str | None) -> ActionChannel:
    normalized = (raw or "cli").strip().lower()
    if normalized in {"cli", "slack", "discord", "telegram", "whatsapp"}:
        return normalized  # type: ignore[return-value]
    return "cli"


def build_notification(
    *,
    intent: ActionIntentV1,
    status: str,
    summary: str,
) -> dict[str, JSONValue]:
    envelope = ActionOutboundResponseEnvelopeV1(
        schema_kind="action_outbound_response_envelope",
        schema_version=1,
        intent_id=intent.intent_id,
        tenant_id=intent.tenant_id,
        repo_id=intent.repo_id,
        channel=_notification_channel(intent.channel),
        status=status,
        summary=summary,
    )
    return envelope.to_json_obj()
