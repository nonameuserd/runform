from __future__ import annotations

import json
from pathlib import Path

from akc.control.policy_bundle import (
    fingerprint_policy_bundle_bytes,
    governance_profile_from_document,
    validate_policy_bundle_document,
)


def _minimal_bundle(**extra: object) -> dict[str, object]:
    doc: dict[str, object] = {
        "schema_kind": "akc_policy_bundle",
        "version": 1,
        "rollout_stage": "observe",
    }
    doc.update(extra)
    return doc


def test_validate_policy_bundle_minimal_ok() -> None:
    assert validate_policy_bundle_document(_minimal_bundle()) == []


def test_validate_policy_bundle_bad_rollout() -> None:
    errs = validate_policy_bundle_document(_minimal_bundle(rollout_stage="block"))
    assert errs


def test_validate_policy_bundle_pins_opa_sha() -> None:
    h = "a" * 64
    assert (
        validate_policy_bundle_document(
            _minimal_bundle(
                pins={"opa_bundle_sha256": h, "rego_path_refs": [".akc/policy/x.rego"]},
                revision_id="rev-1",
            )
        )
        == []
    )


def test_validate_policy_bundle_bad_opa_sha_pattern() -> None:
    errs = validate_policy_bundle_document(_minimal_bundle(pins={"opa_bundle_sha256": "not-hex"}))
    assert errs


def test_validate_policy_bundle_with_provenance_signature() -> None:
    doc = _minimal_bundle(
        revision_id="rev-7",
        provenance={
            "revision": "rev-7",
            "root_owner": "secops",
            "signature": {"key_id": "k-1", "algorithm": "ed25519", "value": "signed-value"},
        },
    )
    assert validate_policy_bundle_document(doc) == []


def test_fingerprint_stable() -> None:
    b = json.dumps(_minimal_bundle(), sort_keys=True).encode("utf-8")
    assert len(fingerprint_policy_bundle_bytes(b)) == 64


def test_policy_bundle_schema_file_exists() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    p = repo_root / "src" / "akc" / "control" / "schemas" / "policy_bundle.v1.schema.json"
    assert p.is_file()


def test_governance_profile_compile_defaults_round_trip() -> None:
    doc = _minimal_bundle(
        governance_profile={
            "compile_defaults": {
                "sandbox": "strong",
                "replay_mode": "vcr",
                "promotion_mode": "artifact_only",
            }
        },
    )
    assert validate_policy_bundle_document(doc) == []
    gp = governance_profile_from_document(doc)
    assert gp.compile_defaults == (
        ("promotion_mode", "artifact_only"),
        ("replay_mode", "vcr"),
        ("sandbox", "strong"),
    )
    assert gp.to_json_obj()["compile_defaults"] == {
        "promotion_mode": "artifact_only",
        "replay_mode": "vcr",
        "sandbox": "strong",
    }
