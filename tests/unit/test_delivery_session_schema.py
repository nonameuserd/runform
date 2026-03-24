from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from akc.artifacts.contracts import apply_schema_envelope
from akc.artifacts.validate import validate_obj
from akc.delivery import store as delivery_store


def _minimal_session_body(*, delivery_id: str = "a1b2c3d4-e5f6-7890-abcd-ef1234567890") -> dict[str, Any]:
    inv = delivery_store.mint_invite_ids_for_recipients(["alice@example.com"])
    return delivery_store._initial_session_doc(
        delivery_id=delivery_id,
        rec_norm=["alice@example.com"],
        plat_norm=["web"],
        release_mode="beta",
        delivery_version="1.0.0",
        invite_by_email=inv,
        invite_hmac_key="unit-test-hmac-key__________________",
        created_ms=1,
        updated_ms=2,
    )


def test_delivery_session_v1_valid_with_envelope() -> None:
    obj = _minimal_session_body()
    apply_schema_envelope(obj=obj, kind="delivery_session", version=1)
    issues = validate_obj(obj=obj, kind="delivery_session", version=1)
    assert issues == []


def test_delivery_session_v1_rejects_bad_delivery_id() -> None:
    obj = _minimal_session_body(delivery_id="../evil")
    apply_schema_envelope(obj=obj, kind="delivery_session", version=1)
    issues = validate_obj(obj=obj, kind="delivery_session", version=1)
    assert issues


def test_delivery_session_v1_requires_pipeline_stages() -> None:
    obj = _minimal_session_body()
    del obj["pipeline"]["compile"]
    apply_schema_envelope(obj=obj, kind="delivery_session", version=1)
    issues = validate_obj(obj=obj, kind="delivery_session", version=1)
    assert any("compile" in issue.message.lower() or "required" in issue.message.lower() for issue in issues)


def test_create_delivery_session_validates_session_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("AKC_DELIVERY_RELAX_ADAPTER_PREFLIGHT", "1")
    summary = delivery_store.create_delivery_session(
        project_dir=tmp_path,
        request_text="ship it",
        recipients=["bob@example.com"],
        platforms=["web", "ios"],
        release_mode="both",
    )
    sid = summary["delivery_id"]
    session_path = tmp_path / ".akc" / "delivery" / sid / "session.json"
    assert session_path.is_file()
    raw = session_path.read_text(encoding="utf-8")
    assert "delivery_session" in raw or "schema_id" in raw
    assert (tmp_path / ".akc" / "delivery" / sid / "recipients.json").is_file()
    assert (tmp_path / ".akc" / "delivery" / sid / "provider_state.json").is_file()
    assert (tmp_path / ".akc" / "delivery" / sid / "activation_evidence.json").is_file()
    rc = delivery_store.load_recipients_sidecar(tmp_path, sid)
    assert rc["recipients"]["bob@example.com"]["platforms"] == ["web", "ios"]
    assert rc["recipients"]["bob@example.com"]["resend_count"] == 0
    ps = delivery_store.load_provider_state_sidecar(tmp_path, sid)
    assert ps["platforms"]["web"]["status"] == "not_started"
    evs = delivery_store.load_activation_evidence_sidecar(tmp_path, sid)
    assert evs["records"] == []
    loaded = delivery_store.load_session(tmp_path, sid)
    plan = loaded.get("distribution_plan")
    assert isinstance(plan, dict)
    assert plan.get("sequence_phases") == [
        "beta_delivery",
        "human_readiness_gate",
        "store_promotion",
    ]
    assert "enterprise_distribution" in (plan.get("v1_excluded_channels") or [])
    assert loaded["pipeline"]["compile"]["status"] == "not_started"
    assert loaded["per_platform"]["web"]["channels"]["beta"]["status"] == "not_started"
    assert loaded["per_platform"]["web"]["channels"]["store"]["status"] == "not_started"
    bob = loaded["per_recipient"]["bob@example.com"]
    assert bob["activation_proof"]["provider_proof"] == "pending"


def test_create_delivery_session_per_recipient_activation_proof_shape(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("AKC_DELIVERY_RELAX_ADAPTER_PREFLIGHT", "1")
    summary = delivery_store.create_delivery_session(
        project_dir=tmp_path,
        request_text="x",
        recipients=["bob@example.com"],
        platforms=["web"],
        release_mode="store",
    )
    sid = summary["delivery_id"]
    loaded = delivery_store.load_session(tmp_path, sid)
    ap = loaded["per_recipient"]["bob@example.com"]["activation_proof"]
    assert ap["status"] == "pending"
    assert ap["provider_proof"] == "pending"
    assert loaded["per_platform"]["web"]["channels"]["beta"]["status"] == "not_applicable"
    assert loaded["store_release"]["android"]["status"] == "not_applicable"


def test_record_resend_increments_recipients_sidecar_resend_count(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("AKC_DELIVERY_RELAX_ADAPTER_PREFLIGHT", "1")
    summary = delivery_store.create_delivery_session(
        project_dir=tmp_path,
        request_text="x",
        recipients=["a@example.com"],
        platforms=["web"],
        release_mode="beta",
    )
    sid = summary["delivery_id"]
    delivery_store.record_resend(project_dir=tmp_path, delivery_id=sid, recipient="a@example.com")
    rc = delivery_store.load_recipients_sidecar(tmp_path, sid)
    assert rc["recipients"]["a@example.com"]["resend_count"] == 1


def test_record_promote_updates_store_release_lane(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("AKC_DELIVERY_RELAX_ADAPTER_PREFLIGHT", "1")
    summary = delivery_store.create_delivery_session(
        project_dir=tmp_path,
        request_text="x",
        recipients=["a@example.com"],
        platforms=["ios"],
        release_mode="both",
    )
    sid = summary["delivery_id"]
    delivery_store.record_human_readiness_gate_pass(project_dir=tmp_path, delivery_id=sid)
    delivery_store.record_promote(project_dir=tmp_path, delivery_id=sid, lane="store")
    loaded = delivery_store.load_session(tmp_path, sid)
    sr = loaded["store_release"]
    assert sr["status"] == "promotion_requested"
    assert sr["active_promotion_lane"] == "store"
    assert sr["last_promotion_requested_at_unix_ms"] is not None
