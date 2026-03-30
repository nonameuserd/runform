from __future__ import annotations

import json
from pathlib import Path

from akc.ingest.connectors.mcp.config import load_mcp_ingest_config, resolve_mcp_server
from akc.ingest.connectors.messaging.whatsapp_cloud import WhatsAppCloudWebhookConfig, WhatsAppCloudWebhookConnector


def test_examples_mcp_stdio_fixture_config_parses_and_resolves() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    cfg_path = repo_root / "examples" / "mcp" / "mcp-ingest.stdio-fixture.json"
    cfg = load_mcp_ingest_config(cfg_path)
    assert "stdio_fixture" in cfg.servers

    server = resolve_mcp_server(input_value="stdio_fixture", config_path=cfg_path)
    assert server.transport == "stdio"
    assert server.command == "python"


def test_examples_whatsapp_capture_fixtures_ingest_offline(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    captures = repo_root / "examples" / "messaging" / "whatsapp-captures"
    jsonl_path = captures / "payloads.jsonl"
    signed_path = captures / "payload.signed.envelope.json"
    assert jsonl_path.is_file()
    assert signed_path.is_file()

    # Plain JSONL (2 messages => 2 docs)
    conn = WhatsAppCloudWebhookConnector(
        tenant_id="tenant_fixture",
        config=WhatsAppCloudWebhookConfig(
            payload_paths=(str(jsonl_path),),
            state_path=str(tmp_path / "wa.state.json"),
            max_documents_per_run=10,
        ),
    )
    docs = list(conn.fetch("webhook_payloads"))
    assert len(docs) == 2

    # Signed envelope example (verify_signatures=True)
    envelope = json.loads(signed_path.read_text(encoding="utf-8"))
    assert isinstance(envelope, dict) and "headers" in envelope and "raw_body" in envelope and "body" in envelope

    conn2 = WhatsAppCloudWebhookConnector(
        tenant_id="tenant_fixture",
        config=WhatsAppCloudWebhookConfig(
            payload_paths=(str(signed_path),),
            verify_signatures=True,
            app_secret="demo-secret",
        ),
    )
    docs2 = list(conn2.fetch("webhook_payloads"))
    assert len(docs2) == 1
