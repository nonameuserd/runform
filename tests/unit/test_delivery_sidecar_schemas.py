from __future__ import annotations

from typing import Any

from akc.artifacts.contracts import apply_schema_envelope
from akc.artifacts.validate import validate_obj
from akc.delivery import store as delivery_store


def _minimal_recipients_sidecar(*, delivery_id: str = "a1b2c3d4-e5f6-7890-abcd-ef1234567890") -> dict[str, Any]:
    inv = delivery_store.mint_invite_ids_for_recipients(["alice@example.com"])
    return delivery_store._initial_recipients_sidecar_doc(
        delivery_id=delivery_id,
        rec_norm=["alice@example.com"],
        plat_norm=["web"],
        invite_by_email=inv,
        created_ms=1,
        updated_ms=2,
    )


def test_delivery_recipients_v1_valid_with_envelope() -> None:
    obj = _minimal_recipients_sidecar()
    apply_schema_envelope(obj=obj, kind="delivery_recipients", version=1)
    assert validate_obj(obj=obj, kind="delivery_recipients", version=1) == []


def test_delivery_recipients_v1_rejects_bad_delivery_id() -> None:
    obj = _minimal_recipients_sidecar(delivery_id="../evil")
    apply_schema_envelope(obj=obj, kind="delivery_recipients", version=1)
    assert validate_obj(obj=obj, kind="delivery_recipients", version=1) != []


def test_delivery_events_v1_valid_with_envelope() -> None:
    obj = delivery_store._initial_events_doc(delivery_id="a1b2c3d4-e5f6-7890-abcd-ef1234567890", created_ms=1)
    apply_schema_envelope(obj=obj, kind="delivery_events", version=1)
    assert validate_obj(obj=obj, kind="delivery_events", version=1) == []


def test_delivery_provider_state_v1_valid_with_envelope() -> None:
    obj = delivery_store._initial_provider_state_doc(
        delivery_id="a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        plat_norm=["web", "ios"],
        created_ms=1,
        updated_ms=2,
    )
    apply_schema_envelope(obj=obj, kind="delivery_provider_state", version=1)
    assert validate_obj(obj=obj, kind="delivery_provider_state", version=1) == []


def test_delivery_activation_evidence_v1_valid_with_envelope() -> None:
    obj = delivery_store._initial_activation_evidence_doc(
        delivery_id="a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        created_ms=1,
        updated_ms=2,
    )
    apply_schema_envelope(obj=obj, kind="delivery_activation_evidence", version=1)
    assert validate_obj(obj=obj, kind="delivery_activation_evidence", version=1) == []


def test_delivery_activation_evidence_v1_accepts_record() -> None:
    obj = delivery_store._initial_activation_evidence_doc(
        delivery_id="a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        created_ms=1,
        updated_ms=2,
    )
    obj["records"] = [
        {
            "record_id": "r1",
            "recipient_email": "alice@example.com",
            "platform": "web",
            "evidence_kind": "app_first_run",
            "occurred_at_unix_ms": 3,
            "payload": {"delivery_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"},
        },
    ]
    apply_schema_envelope(obj=obj, kind="delivery_activation_evidence", version=1)
    assert validate_obj(obj=obj, kind="delivery_activation_evidence", version=1) == []
