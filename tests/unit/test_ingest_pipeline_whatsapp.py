from __future__ import annotations

from pathlib import Path

from akc.ingest.connectors.messaging.whatsapp_cloud import WhatsAppCloudWebhookConnector
from akc.ingest.pipeline import _get_connector


def test_get_connector_whatsapp_builds_from_input_paths(tmp_path: Path) -> None:
    p = tmp_path / "x.jsonl"
    p.write_text('{"entry":[]}\n', encoding="utf-8")
    c = _get_connector(
        "whatsapp",
        tenant_id="t1",
        input_value=str(p),
        connector_options={"state_path": str(tmp_path / "wa.state.json")},
    )
    assert isinstance(c, WhatsAppCloudWebhookConnector)
    assert c.config.payload_paths == (str(p.resolve()),)
