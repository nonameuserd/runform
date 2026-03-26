from __future__ import annotations

import hashlib
import hmac
from urllib.parse import urlencode

import pytest

from akc.control_bot.ingress_adapters import IngressError
from akc.control_bot.ingress_auth import (
    IngressAuthContext,
    verify_discord_request,
    verify_slack_request,
    verify_telegram_request,
    verify_whatsapp_request,
    verify_whatsapp_webhook_verification,
)
from akc.control_bot.server import (
    _parse_discord_request,
    _parse_slack_request,
    _parse_telegram_request,
    _parse_whatsapp_request,
)


def test_slack_signature_ok() -> None:
    secret = "shh"
    body = b'{"hello":"world"}'
    ts = "1710000000"
    base = b"v0:" + ts.encode("utf-8") + b":" + body
    mac = hmac.new(secret.encode("utf-8"), base, hashlib.sha256).hexdigest()
    headers = {"x-slack-request-timestamp": ts, "x-slack-signature": f"v0={mac}"}

    verify_slack_request(
        IngressAuthContext(channel="slack", headers=headers, body=body, now_s=int(ts)),
        enabled=True,
        signing_secret=secret,
    )


def test_slack_signature_rejects_stale_timestamp() -> None:
    secret = "shh"
    body = b"{}"
    ts = "1710000000"
    base = b"v0:" + ts.encode("utf-8") + b":" + body
    mac = hmac.new(secret.encode("utf-8"), base, hashlib.sha256).hexdigest()
    headers = {"x-slack-request-timestamp": ts, "x-slack-signature": f"v0={mac}"}

    with pytest.raises(IngressError, match="stale Slack request timestamp"):
        verify_slack_request(
            IngressAuthContext(channel="slack", headers=headers, body=body, now_s=int(ts) + 9999),
            enabled=True,
            signing_secret=secret,
            tolerance_s=10,
        )


def test_slack_signature_rejects_bad_signature() -> None:
    headers = {"x-slack-request-timestamp": "1710000000", "x-slack-signature": "v0=deadbeef"}
    with pytest.raises(IngressError, match="invalid Slack signature"):
        verify_slack_request(
            IngressAuthContext(channel="slack", headers=headers, body=b"{}", now_s=1710000000),
            enabled=True,
            signing_secret="shh",
        )


class _Ws:
    def __init__(self, *, channel: str, workspace_id: str, tenant_id: str) -> None:
        self.channel = channel
        self.workspace_id = workspace_id
        self.tenant_id = tenant_id


class _Model:
    def __init__(self) -> None:
        class _Routing:
            workspaces = (
                _Ws(channel="slack", workspace_id="T123", tenant_id="tenant-a"),
                _Ws(channel="discord", workspace_id="G123", tenant_id="tenant-d"),
                _Ws(channel="telegram", workspace_id="12345", tenant_id="tenant-t"),
                _Ws(channel="whatsapp", workspace_id="PN123", tenant_id="tenant-w"),
            )

        self.routing = _Routing()


def test_slack_parse_commands_form_sets_tenant_principal_and_event_id() -> None:
    body = b"team_id=T123&user_id=U999&command=%2Fakc&text=status+runtime"
    headers = {"x-slack-request-timestamp": "1710000000"}
    req = _parse_slack_request(body=body, headers=headers, model=_Model(), endpoint="commands")
    assert req.channel == "slack"
    assert req.tenant_id == "tenant-a"
    assert req.principal_id == "U999"
    assert req.event_id.startswith("slack:1710000000:")
    assert req.raw_text == "akc status runtime"


def test_slack_parse_interactivity_payload_sets_ids() -> None:
    payload = b'{"team":{"id":"T123"},"user":{"id":"U999"},"actions":[{"value":"status runtime"}]}'
    body = urlencode({"payload": payload.decode("utf-8")}).encode("utf-8")
    headers = {"x-slack-request-timestamp": "1710000001"}
    req = _parse_slack_request(body=body, headers=headers, model=_Model(), endpoint="interactivity")
    assert req.channel == "slack"
    assert req.tenant_id == "tenant-a"
    assert req.principal_id == "U999"
    assert req.raw_text == "akc status runtime"


def test_discord_parse_interaction_sets_ids_and_initial_response() -> None:
    payload = {
        "id": "999",
        "application_id": "app1",
        "type": 2,
        "token": "tok1",
        "guild_id": "G123",
        "member": {"user": {"id": "U777"}},
        "data": {
            "name": "akc",
            "options": [{"name": "status", "type": 1, "options": [{"name": "runtime", "type": 3, "value": "runtime"}]}],
        },
    }
    req, followup, initial = _parse_discord_request(payload=payload, model=_Model())
    assert req.channel == "discord"
    assert req.tenant_id == "tenant-d"
    assert req.principal_id == "U777"
    assert req.event_id == "discord:999"
    assert req.raw_text.startswith("akc")
    assert followup.application_id == "app1"
    assert followup.interaction_token == "tok1"
    assert initial["type"] == 5


def test_discord_parse_ping_returns_pong_initial_response() -> None:
    payload = {
        "id": "1000",
        "application_id": "app1",
        "type": 1,
        "token": "tok1",
        "guild_id": "G123",
        "member": {"user": {"id": "U777"}},
    }
    _req, _followup, initial = _parse_discord_request(payload=payload, model=_Model())
    assert initial == {"type": 1}


def test_telegram_secret_token_ok() -> None:
    verify_telegram_request(
        IngressAuthContext(
            channel="telegram",
            headers={"x-telegram-bot-api-secret-token": "tok"},
            body=b"{}",
        ),
        enabled=True,
        secret_token="tok",
    )


def test_telegram_secret_token_rejects_invalid() -> None:
    with pytest.raises(IngressError, match="invalid Telegram secret token"):
        verify_telegram_request(
            IngressAuthContext(
                channel="telegram",
                headers={"x-telegram-bot-api-secret-token": "nope"},
                body=b"{}",
            ),
            enabled=True,
            secret_token="tok",
        )


def test_telegram_parse_message_command_routes_tenant_and_principal() -> None:
    payload = {
        "update_id": 77,
        "message": {"message_id": 10, "from": {"id": 9001}, "chat": {"id": 12345}, "text": "/akc status runtime"},
    }
    req = _parse_telegram_request(payload=payload, model=_Model())
    assert req.channel == "telegram"
    assert req.event_id == "telegram:77"
    assert req.principal_id == "9001"
    assert req.tenant_id == "tenant-t"
    assert req.raw_text == "akc status runtime"


def test_telegram_parse_callback_query_uses_data_and_chat_id() -> None:
    payload = {
        "update_id": 88,
        "callback_query": {
            "id": "cb1",
            "from": {"id": 42},
            "data": "status runtime",
            "message": {"message_id": 1, "chat": {"id": 12345}},
        },
    }
    req = _parse_telegram_request(payload=payload, model=_Model())
    assert req.channel == "telegram"
    assert req.event_id == "telegram:88"
    assert req.principal_id == "42"
    assert req.tenant_id == "tenant-t"
    assert req.raw_text == "akc status runtime"


def test_whatsapp_signature_ok() -> None:
    secret = "appsecret"
    body = b'{"object":"whatsapp_business_account"}'
    mac = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    headers = {"x-hub-signature-256": f"sha256={mac}"}

    verify_whatsapp_request(
        IngressAuthContext(channel="whatsapp", headers=headers, body=body),
        enabled=True,
        app_secret=secret,
    )


def test_whatsapp_signature_rejects_invalid() -> None:
    headers = {"x-hub-signature-256": "sha256=00"}
    with pytest.raises(IngressError, match="invalid WhatsApp signature"):
        verify_whatsapp_request(
            IngressAuthContext(channel="whatsapp", headers=headers, body=b"{}"),
            enabled=True,
            app_secret="appsecret",
        )


def test_whatsapp_webhook_get_verification_ok() -> None:
    ch = verify_whatsapp_webhook_verification(
        enabled=True,
        expected_verify_token="vtok",
        mode="subscribe",
        verify_token="vtok",
        challenge="12345",
    )
    assert ch == "12345"


def test_whatsapp_webhook_get_verification_rejects_wrong_token() -> None:
    with pytest.raises(IngressError, match="invalid hub.verify_token"):
        verify_whatsapp_webhook_verification(
            enabled=True,
            expected_verify_token="vtok",
            mode="subscribe",
            verify_token="nope",
            challenge="12345",
        )


def test_whatsapp_webhook_get_verification_rejects_wrong_mode() -> None:
    with pytest.raises(IngressError, match="invalid hub.mode"):
        verify_whatsapp_webhook_verification(
            enabled=True,
            expected_verify_token="vtok",
            mode="unsubscribe",
            verify_token="vtok",
            challenge="12345",
        )


def test_whatsapp_webhook_get_verification_rejects_missing_challenge() -> None:
    with pytest.raises(IngressError, match="missing hub.challenge"):
        verify_whatsapp_webhook_verification(
            enabled=True,
            expected_verify_token="vtok",
            mode="subscribe",
            verify_token="vtok",
            challenge="",
        )


def test_discord_signature_ok() -> None:
    # Generate a real Ed25519 keypair at runtime and sign the request.
    try:
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    except Exception as e:  # pragma: no cover
        pytest.skip(f"cryptography missing: {e}")  # noqa: PT012

    priv = Ed25519PrivateKey.generate()
    pub = priv.public_key()
    pub_hex = pub.public_bytes_raw().hex()

    ts = "1710000000"
    body = b'{"type":1}'
    sig_hex = priv.sign(ts.encode("utf-8") + body).hex()
    headers = {"x-signature-timestamp": ts, "x-signature-ed25519": sig_hex}

    verify_discord_request(
        IngressAuthContext(channel="discord", headers=headers, body=body),
        enabled=True,
        public_key=pub_hex,
    )


def test_discord_signature_rejects_invalid() -> None:
    headers = {"x-signature-timestamp": "1710000000", "x-signature-ed25519": "00"}
    with pytest.raises(IngressError, match="invalid Discord signature"):
        verify_discord_request(
            IngressAuthContext(channel="discord", headers=headers, body=b"{}"),
            enabled=True,
            public_key="00",
        )


def test_whatsapp_parse_request_maps_reply_fallback_to_approval_command() -> None:
    payload = {
        "entry": [
            {
                "id": "waba1",
                "changes": [
                    {
                        "value": {
                            "metadata": {"phone_number_id": "PN123"},
                            "contacts": [{"wa_id": "15550001"}],
                            "messages": [
                                {
                                    "id": "wamid.1",
                                    "from": "15550001",
                                    "text": {"body": "approve 2f5ab1b8-9e99-4b8d-bf6b-19f4878cf0ec"},
                                }
                            ],
                        }
                    }
                ],
            }
        ]
    }
    req, target = _parse_whatsapp_request(payload=payload, model=_Model(), body=b"{}")
    assert req.channel == "whatsapp"
    assert req.event_id == "whatsapp:wamid.1"
    assert req.principal_id == "15550001"
    assert req.tenant_id == "tenant-w"
    assert req.raw_text == "akc approval approve request_id=2f5ab1b8-9e99-4b8d-bf6b-19f4878cf0ec"
    assert target.data["to"] == "15550001"
    assert target.data["phone_number_id"] == "PN123"
