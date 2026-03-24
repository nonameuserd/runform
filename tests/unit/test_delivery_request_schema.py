from __future__ import annotations

from typing import Any, cast

from akc.artifacts.contracts import apply_schema_envelope
from akc.artifacts.validate import validate_obj


def _minimal_request_body(*, delivery_id: str = "a1b2c3d4-e5f6-7890-abcd-ef1234567890") -> dict[str, Any]:
    return {
        "delivery_id": delivery_id,
        "request_text": "ship the app",
        "platforms": ["web"],
        "recipients": ["alice@example.com"],
        "release_mode": "beta",
        "app_stack": "react_expo_default",
        "delivery_version": "1.0.0",
        "derived_intent_ref": None,
        "required_accounts": [],
        "parsed": {
            "app_goal": "ship the app",
            "requested_platforms": ["web"],
            "delivery_mode": "beta",
            "recipient_set": ["alice@example.com"],
            "release_lanes": ["beta"],
            "request_mentions_platforms": [],
            "warnings": [],
        },
        "required_human_inputs": [],
        "created_at_unix_ms": 1,
    }


def test_delivery_request_v1_valid_with_envelope() -> None:
    obj = _minimal_request_body()
    apply_schema_envelope(obj=obj, kind="delivery_request", version=1)
    issues = validate_obj(obj=obj, kind="delivery_request", version=1)
    assert issues == []


def test_delivery_request_v1_rejects_bad_recipient_email() -> None:
    obj = _minimal_request_body()
    obj["recipients"] = ["not-an-email"]
    apply_schema_envelope(obj=obj, kind="delivery_request", version=1)
    issues = validate_obj(obj=obj, kind="delivery_request", version=1)
    assert any("not-an-email" in issue.message or "pattern" in issue.message.lower() for issue in issues)


def test_delivery_request_v1_rejects_invalid_platform() -> None:
    obj = _minimal_request_body()
    obj["platforms"] = ["windows"]
    apply_schema_envelope(obj=obj, kind="delivery_request", version=1)
    issues = validate_obj(obj=obj, kind="delivery_request", version=1)
    assert issues


def test_delivery_request_v1_requires_payload_fields() -> None:
    obj = _minimal_request_body()
    del obj["required_accounts"]
    apply_schema_envelope(obj=obj, kind="delivery_request", version=1)
    issues = validate_obj(obj=obj, kind="delivery_request", version=1)
    assert any("required" in issue.message.lower() for issue in issues)


def test_delivery_request_v1_derived_intent_ref_shape() -> None:
    obj = _minimal_request_body()
    obj["derived_intent_ref"] = cast(
        dict[str, Any],
        {
            "intent_id": "i1",
            "stable_intent_sha256": "a" * 64,
            "semantic_fingerprint": "b" * 16,
            "goal_text_fingerprint": "c" * 16,
        },
    )
    apply_schema_envelope(obj=obj, kind="delivery_request", version=1)
    issues = validate_obj(obj=obj, kind="delivery_request", version=1)
    assert issues == []


def test_delivery_request_v1_rejects_malformed_delivery_id() -> None:
    obj = _minimal_request_body(delivery_id="../evil")
    apply_schema_envelope(obj=obj, kind="delivery_request", version=1)
    issues = validate_obj(obj=obj, kind="delivery_request", version=1)
    assert issues


def test_delivery_request_v1_requires_parsed_and_human_inputs_arrays() -> None:
    obj = _minimal_request_body()
    del obj["parsed"]
    apply_schema_envelope(obj=obj, kind="delivery_request", version=1)
    issues = validate_obj(obj=obj, kind="delivery_request", version=1)
    assert any("parsed" in issue.message.lower() for issue in issues)
