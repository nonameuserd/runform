from __future__ import annotations

from akc.control_bot.command_engine import CommandResult
from akc.control_bot.outbound_response_adapters import SlackOutboundAdapter, WhatsAppOutboundAdapter


def test_slack_outbound_adapter_renders_approval_buttons() -> None:
    adapter = SlackOutboundAdapter()
    result = CommandResult(
        ok=False,
        action_id="mutate.runtime.stop",
        message="Approval required.",
        request_id="req-123",
        status="approval_required",
    )
    msg = adapter.render(result)
    assert msg.channel == "slack"
    assert isinstance(msg.payload, dict)
    blocks = msg.payload.get("blocks")
    assert isinstance(blocks, list) and blocks
    actions = [b for b in blocks if isinstance(b, dict) and b.get("type") == "actions"]
    assert actions
    elems = actions[0].get("elements")
    assert isinstance(elems, list)
    values = {str(e.get("value")) for e in elems if isinstance(e, dict)}
    assert "akc approval approve request_id=req-123" in values
    assert "akc approval deny request_id=req-123" in values


def test_whatsapp_outbound_adapter_includes_text_fallback_command() -> None:
    adapter = WhatsAppOutboundAdapter()
    result = CommandResult(
        ok=False,
        action_id="mutate.runtime.stop",
        message="Approval required.",
        request_id="req-999",
        status="approval_required",
    )
    msg = adapter.render(result)
    assert "request_id: req-999" in msg.text
    assert "Reply with: approve req-999" in msg.text
    assert "akc approval approve request_id=req-999" in msg.text
