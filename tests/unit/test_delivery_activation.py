from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.delivery import store as delivery_store
from akc.delivery.invites import sign_invite_query


def _token_for(session: dict, email: str) -> str:
    per = session["per_recipient"][email]
    tid = per["invite_token_id"]
    assert tid
    return str(tid)


def test_web_beta_active_requires_provider_and_app_proof(tmp_path: Path) -> None:
    summary = delivery_store.create_delivery_session(
        project_dir=tmp_path,
        request_text="beta web",
        recipients=["pat@example.com"],
        platforms=["web"],
        release_mode="beta",
        skip_distribution_preflight=True,
    )
    did = str(summary["delivery_id"])
    sess = delivery_store.load_session(tmp_path, did)
    tid = _token_for(sess, "pat@example.com")
    key = sess["secrets"]["invite_hmac_key"]
    sig = sign_invite_query(delivery_id=did, invite_token_id=tid, key=key)

    delivery_store.ingest_client_activation_report(
        project_dir=tmp_path,
        delivery_id=did,
        payload={
            "delivery_id": did,
            "recipient_token_id": tid,
            "platform": "web",
            "app_version": "1.0.0",
            "first_run_at_unix_ms": 1000,
        },
    )
    s1 = delivery_store.load_session(tmp_path, did)
    assert s1["per_recipient"]["pat@example.com"]["activation_proof"]["app_proof"] == "satisfied"
    assert s1["per_recipient"]["pat@example.com"]["activation_proof"]["provider_proof"] == "pending"

    delivery_store.record_web_invite_opened(
        project_dir=tmp_path,
        delivery_id=did,
        invite_token_id=tid,
        signature=sig,
        payload={"user_agent": "pytest"},
    )
    s2 = delivery_store.load_session(tmp_path, did)
    assert s2["per_recipient"]["pat@example.com"]["status"] == "active"
    assert s2["per_recipient"]["pat@example.com"]["activation_proof"]["status"] == "satisfied"


def test_web_invite_signature_rejects_tampering(tmp_path: Path) -> None:
    summary = delivery_store.create_delivery_session(
        project_dir=tmp_path,
        request_text="x",
        recipients=["a@example.com"],
        platforms=["web"],
        release_mode="beta",
        skip_distribution_preflight=True,
    )
    did = str(summary["delivery_id"])
    sess = delivery_store.load_session(tmp_path, did)
    tid = _token_for(sess, "a@example.com")
    with pytest.raises(ValueError, match="signature"):
        delivery_store.record_web_invite_opened(
            project_dir=tmp_path,
            delivery_id=did,
            invite_token_id=tid,
            signature="deadbeef",
        )


def test_app_report_rejects_unknown_token(tmp_path: Path) -> None:
    summary = delivery_store.create_delivery_session(
        project_dir=tmp_path,
        request_text="x",
        recipients=["a@example.com"],
        platforms=["web"],
        release_mode="beta",
        skip_distribution_preflight=True,
    )
    did = str(summary["delivery_id"])
    with pytest.raises(ValueError, match="recipient_token_id"):
        delivery_store.ingest_client_activation_report(
            project_dir=tmp_path,
            delivery_id=did,
            payload={
                "delivery_id": did,
                "recipient_token_id": "00000000-0000-0000-0000-000000000000",
                "platform": "web",
                "app_version": "1",
                "first_run_at_unix_ms": 1,
            },
        )


def test_app_report_rejects_platform_not_in_recipient_scope(tmp_path: Path) -> None:
    summary = delivery_store.create_delivery_session(
        project_dir=tmp_path,
        request_text="x",
        recipients=["a@example.com"],
        platforms=["web"],
        release_mode="beta",
        skip_distribution_preflight=True,
    )
    did = str(summary["delivery_id"])
    tid = _token_for(delivery_store.load_session(tmp_path, did), "a@example.com")
    with pytest.raises(ValueError, match="not enabled"):
        delivery_store.ingest_client_activation_report(
            project_dir=tmp_path,
            delivery_id=did,
            payload={
                "delivery_id": did,
                "recipient_token_id": tid,
                "platform": "ios",
                "app_version": "1",
                "first_run_at_unix_ms": 1,
            },
        )


def test_store_mode_provider_proof_from_publication_not_anonymous_install(tmp_path: Path) -> None:
    """Named recipients: store lane publication satisfies provider proof (no anonymous install ledger)."""

    summary = delivery_store.create_delivery_session(
        project_dir=tmp_path,
        request_text="store",
        recipients=["a@example.com"],
        platforms=["web"],
        release_mode="store",
        skip_distribution_preflight=True,
    )
    did = str(summary["delivery_id"])
    sess_path = tmp_path / ".akc" / "delivery" / did / "session.json"
    sess = json.loads(sess_path.read_text(encoding="utf-8"))
    sess["per_platform"]["web"]["channels"]["store"]["status"] = "completed"
    sess_path.write_text(json.dumps(sess, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    tid = _token_for(delivery_store.load_session(tmp_path, did), "a@example.com")
    delivery_store.ingest_client_activation_report(
        project_dir=tmp_path,
        delivery_id=did,
        payload={
            "delivery_id": did,
            "recipient_token_id": tid,
            "platform": "web",
            "app_version": "1.0.0",
            "first_run_at_unix_ms": 5,
        },
    )
    s = delivery_store.load_session(tmp_path, did)
    assert s["per_recipient"]["a@example.com"]["status"] == "active"
