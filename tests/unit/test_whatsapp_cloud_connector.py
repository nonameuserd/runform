from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.ingest.connectors.messaging.whatsapp_cloud import (
    WhatsAppCloudWebhookConfig,
    WhatsAppCloudWebhookConnector,
    verify_whatsapp_cloud_signature,
)
from akc.ingest.exceptions import ConnectorError


def _wa_payload(*, message_id: str, text: str = "Hello", phone_number_id: str = "PN1", waba_id: str = "WABA1") -> dict:
    return {
        "object": "whatsapp_business_account",
        "entry": [
            {
                "id": waba_id,
                "changes": [
                    {
                        "field": "messages",
                        "value": {
                            "metadata": {"phone_number_id": phone_number_id},
                            "messages": [
                                {
                                    "id": message_id,
                                    "from": "15551234567",
                                    "timestamp": "1742900000",
                                    "type": "text",
                                    "text": {"body": text},
                                }
                            ],
                        },
                    }
                ],
            }
        ],
    }


def test_whatsapp_connector_ingests_jsonl_and_dedupes(tmp_path: Path) -> None:
    payload_path = tmp_path / "payloads.jsonl"
    state_path = tmp_path / "whatsapp.state.json"

    # Same message id appears twice across payloads (replay).
    payloads = [_wa_payload(message_id="wamid.1", text="One"), _wa_payload(message_id="wamid.1", text="One replay")]
    payload_path.write_text("\n".join(json.dumps(p) for p in payloads) + "\n", encoding="utf-8")

    conn = WhatsAppCloudWebhookConnector(
        tenant_id="tenant-1",
        config=WhatsAppCloudWebhookConfig(
            payload_paths=(str(payload_path),),
            state_path=str(state_path),
            max_documents_per_run=10,
        ),
    )
    docs = list(conn.fetch("webhook_payloads"))
    assert len(docs) == 1
    assert docs[0].metadata["tenant_id"] == "tenant-1"
    assert docs[0].metadata["source_type"] == "messaging"
    assert "One" in docs[0].content

    # Second run should emit nothing due to persisted state.
    docs2 = list(conn.fetch("webhook_payloads"))
    assert docs2 == []


def test_whatsapp_connector_rejects_unknown_source_id(tmp_path: Path) -> None:
    payload_path = tmp_path / "payload.json"
    payload_path.write_text(json.dumps(_wa_payload(message_id="wamid.1")), encoding="utf-8")
    conn = WhatsAppCloudWebhookConnector(
        tenant_id="t",
        config=WhatsAppCloudWebhookConfig(payload_paths=(str(payload_path),)),
    )
    with pytest.raises(ConnectorError, match=r"unknown source_id"):
        list(conn.fetch("nope"))


def test_whatsapp_connector_blocks_cross_tenant_state(tmp_path: Path) -> None:
    payload_path = tmp_path / "payload.json"
    payload_path.write_text(json.dumps(_wa_payload(message_id="wamid.1")), encoding="utf-8")
    state_path = tmp_path / "whatsapp.state.json"
    state_path.write_text(json.dumps({"tenant_id": "other-tenant", "seen_message_ids": ["wamid.x"]}), encoding="utf-8")

    conn = WhatsAppCloudWebhookConnector(
        tenant_id="tenant-1",
        config=WhatsAppCloudWebhookConfig(payload_paths=(str(payload_path),), state_path=str(state_path)),
    )
    with pytest.raises(ConnectorError, match=r"tenant_id mismatch"):
        list(conn.fetch("webhook_payloads"))


def test_verify_whatsapp_signature_helper() -> None:
    app_secret = "secret"
    raw = b'{"hello":"world"}'
    # Expected signature computed with app_secret and raw bytes.
    import hmac
    from hashlib import sha256

    sig = hmac.new(app_secret.encode("utf-8"), raw, sha256).hexdigest()
    assert verify_whatsapp_cloud_signature(app_secret=app_secret, payload_body=raw, signature_header=f"sha256={sig}")
    assert not verify_whatsapp_cloud_signature(
        app_secret=app_secret,
        payload_body=raw,
        signature_header="sha256=deadbeef",
    )


def test_whatsapp_connector_enforces_signature_when_enabled(tmp_path: Path) -> None:
    app_secret = "secret"
    body = _wa_payload(message_id="wamid.1", text="Signed")
    raw_body = json.dumps(body, separators=(",", ":"), sort_keys=True)
    import hmac
    from hashlib import sha256

    sig = hmac.new(app_secret.encode("utf-8"), raw_body.encode("utf-8"), sha256).hexdigest()
    envelope = {"headers": {"X-Hub-Signature-256": f"sha256={sig}"}, "raw_body": raw_body, "body": body}
    payload_path = tmp_path / "payload.json"
    payload_path.write_text(json.dumps(envelope), encoding="utf-8")

    conn = WhatsAppCloudWebhookConnector(
        tenant_id="tenant-1",
        config=WhatsAppCloudWebhookConfig(
            payload_paths=(str(payload_path),),
            verify_signatures=True,
            app_secret=app_secret,
        ),
    )
    docs = list(conn.fetch("webhook_payloads"))
    assert len(docs) == 1
    assert "Signed" in docs[0].content

    # Now break the signature.
    envelope2 = {"headers": {"X-Hub-Signature-256": "sha256=deadbeef"}, "raw_body": raw_body, "body": body}
    payload_path.write_text(json.dumps(envelope2), encoding="utf-8")
    with pytest.raises(ConnectorError, match=r"signature verification failed"):
        list(conn.fetch("webhook_payloads"))


def test_whatsapp_connector_raises_on_invalid_json_file(tmp_path: Path) -> None:
    payload_path = tmp_path / "payload.json"
    payload_path.write_text("{not-json", encoding="utf-8")
    conn = WhatsAppCloudWebhookConnector(
        tenant_id="tenant-1",
        config=WhatsAppCloudWebhookConfig(payload_paths=(str(payload_path),)),
    )
    with pytest.raises(ConnectorError, match=r"invalid JSON in whatsapp payload file"):
        list(conn.fetch("webhook_payloads"))


def test_whatsapp_connector_raises_on_invalid_jsonl_line(tmp_path: Path) -> None:
    payload_path = tmp_path / "payload.jsonl"
    payload_path.write_text('{"ok":true}\n{not-json}\n', encoding="utf-8")
    conn = WhatsAppCloudWebhookConnector(
        tenant_id="tenant-1",
        config=WhatsAppCloudWebhookConfig(payload_paths=(str(payload_path),)),
    )
    with pytest.raises(ConnectorError, match=r"invalid JSONL in whatsapp payload file"):
        list(conn.fetch("webhook_payloads"))


def test_whatsapp_signature_enforcement_requires_headers(tmp_path: Path) -> None:
    body = _wa_payload(message_id="wamid.1", text="Signed")
    envelope = {"body": body}  # missing headers + signature
    payload_path = tmp_path / "payload.json"
    payload_path.write_text(json.dumps(envelope), encoding="utf-8")
    conn = WhatsAppCloudWebhookConnector(
        tenant_id="tenant-1",
        config=WhatsAppCloudWebhookConfig(
            payload_paths=(str(payload_path),),
            verify_signatures=True,
            app_secret="secret",
        ),
    )
    with pytest.raises(ConnectorError, match=r"no headers"):
        list(conn.fetch("webhook_payloads"))


def test_whatsapp_connector_persists_dedupe_ids_even_when_capped(tmp_path: Path) -> None:
    payload_path = tmp_path / "payloads.jsonl"
    state_path = tmp_path / "whatsapp.state.json"
    payloads = [_wa_payload(message_id="wamid.1", text="One"), _wa_payload(message_id="wamid.2", text="Two")]
    payload_path.write_text("\n".join(json.dumps(p) for p in payloads) + "\n", encoding="utf-8")

    conn = WhatsAppCloudWebhookConnector(
        tenant_id="tenant-1",
        config=WhatsAppCloudWebhookConfig(
            payload_paths=(str(payload_path),),
            state_path=str(state_path),
            max_documents_per_run=1,
        ),
    )
    docs1 = list(conn.fetch("webhook_payloads"))
    assert len(docs1) == 1

    # Second run should emit exactly one more (not re-emit the first).
    docs2 = list(conn.fetch("webhook_payloads"))
    assert len(docs2) == 1
    assert docs1[0].metadata["message_id"] != docs2[0].metadata["message_id"]
