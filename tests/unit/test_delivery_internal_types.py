from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from akc.artifacts.contracts import apply_schema_envelope
from akc.delivery import (
    ActivationProof,
    DeliveryModelError,
    DeliveryPlatform,
    DeliveryRequestV1,
    DeliverySession,
    DistributionAdapter,
    PlatformBuildSpec,
    RecipientSpec,
    ReleaseLane,
)
from akc.delivery import store as delivery_store


class _FakeAdapter(DistributionAdapter):
    @property
    def kind(self) -> str:
        return "fake"

    def supported_platforms(self) -> frozenset[DeliveryPlatform]:
        return frozenset({"web"})

    def supported_lanes(self) -> frozenset[ReleaseLane]:
        return frozenset({"beta"})


def test_delivery_request_v1_from_artifact_roundtrip() -> None:
    obj: dict[str, Any] = {
        "delivery_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        "request_text": "ship",
        "platforms": ["web"],
        "recipients": ["alice@example.com"],
        "release_mode": "beta",
        "app_stack": "react_expo_default",
        "delivery_version": "1.0.0",
        "derived_intent_ref": None,
        "required_accounts": [],
        "parsed": {
            "app_goal": "ship",
            "requested_platforms": ["web"],
            "delivery_mode": "beta",
            "recipient_set": ["alice@example.com"],
            "release_lanes": ["beta"],
        },
        "required_human_inputs": [],
        "created_at_unix_ms": 1,
    }
    apply_schema_envelope(obj=obj, kind="delivery_request", version=1)
    req = DeliveryRequestV1.from_artifact(obj)
    assert req.delivery_id == "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
    assert req.platforms == ("web",)
    assert req.recipients == ("alice@example.com",)
    assert req.delivery_version == "1.0.0"


def test_delivery_session_from_artifact_and_recipient_specs() -> None:
    inv = delivery_store.mint_invite_ids_for_recipients(["alice@example.com"])
    obj = delivery_store._initial_session_doc(
        delivery_id="a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        rec_norm=["alice@example.com"],
        plat_norm=["web", "ios"],
        release_mode="both",
        delivery_version="1.0.0",
        invite_by_email=inv,
        invite_hmac_key="unit-test-hmac-key__________________",
        created_ms=1,
        updated_ms=2,
    )
    apply_schema_envelope(obj=obj, kind="delivery_session", version=1)
    sess = DeliverySession.from_artifact(obj)
    assert sess.delivery_version == "1.0.0"
    assert sess.platforms == ("web", "ios")
    specs = sess.recipient_specs()
    assert specs["alice@example.com"].platforms == ("web", "ios")
    assert specs["alice@example.com"].email == "alice@example.com"


def test_recipient_spec_from_sidecar_row() -> None:
    row = {
        "email": "bob@example.com",
        "platforms": ["android"],
        "invite_token_id": None,
        "status": "pending",
        "last_invite_sent_at_unix_ms": None,
        "resend_count": 0,
    }
    spec = RecipientSpec.from_recipients_sidecar_row(row)
    assert spec.platforms == ("android",)


def test_activation_proof_from_mapping() -> None:
    ap = ActivationProof.from_mapping(
        {"status": "pending", "provider_proof": "pending", "app_proof": "pending"},
    )
    assert ap.status == "pending"


def test_platform_build_spec_rejects_blank_tenant() -> None:
    with pytest.raises(DeliveryModelError, match="tenant_id"):
        PlatformBuildSpec(
            tenant_id="  ",
            repo_id="r1",
            delivery_id="ab",
            platform="web",
            delivery_version="1.0.0",
            release_lanes=("beta",),
        )


def test_platform_build_spec_rejects_unsafe_delivery_id() -> None:
    with pytest.raises(DeliveryModelError, match="delivery_id"):
        PlatformBuildSpec(
            tenant_id="t",
            repo_id="r1",
            delivery_id="../x",
            platform="web",
            delivery_version="1.0.0",
            release_lanes=("beta",),
        )


def test_distribution_adapter_fake() -> None:
    ad = _FakeAdapter()
    assert ad.kind == "fake"
    assert ad.preflight(project_dir=Path("."), tenant_id="t", repo_id="r", spec=PlatformBuildSpec(
        tenant_id="t",
        repo_id="r",
        delivery_id="d1",
        platform="web",
        delivery_version="1",
        release_lanes=("beta",),
    )) == []


def test_create_delivery_session_loads_as_typed_models(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("AKC_DELIVERY_RELAX_ADAPTER_PREFLIGHT", "1")
    summary = delivery_store.create_delivery_session(
        project_dir=tmp_path,
        request_text="go",
        recipients=["u@example.com"],
        platforms=["web"],
        release_mode="store",
    )
    did = str(summary["delivery_id"])
    req_doc = delivery_store.load_request(tmp_path, did)
    sess_doc = delivery_store.load_session(tmp_path, did)
    req = DeliveryRequestV1.from_artifact(req_doc)
    sess = DeliverySession.from_artifact(sess_doc)
    assert req.recipients == ("u@example.com",)
    assert req.parsed.get("delivery_mode") == "store"
    assert isinstance(req.required_human_inputs, tuple)
    assert sess.per_platform["web"].store.status == "not_started"
