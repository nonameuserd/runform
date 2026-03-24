from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.runtime.compile_apply_attestation import (
    compile_apply_attestation_denial_for_rollout,
    compile_apply_attestation_fingerprint,
    summarize_compile_apply_from_control_plane,
    verify_compile_apply_attestation_for_rollout,
)

_PATCH = "a" * 64
_PATCH_B = "b" * 64


def _base_packet(
    *,
    patch: str = _PATCH,
    caa_mode: str = "artifact_only",
    caa_applied: bool = False,
) -> dict[str, object]:
    policy_allowed = caa_mode != "scoped_apply" or (caa_mode == "scoped_apply" and caa_applied)
    return {
        "patch_hash_sha256": patch,
        "compile_apply_attestation": {
            "compile_realization_mode": caa_mode,
            "applied": caa_applied,
            "apply_decision_token_id": "",
            "policy_allow_decision": {"allowed": policy_allowed, "reason": "ok"},
            "patch_fingerprint_sha256": patch,
            "scope_root": None,
            "touched_paths": ["x.py"],
        },
    }


def test_verify_accepts_artifact_only_matching_patch() -> None:
    p = _base_packet()
    verify_compile_apply_attestation_for_rollout(
        packet=p,
        manifest_control_plane=None,
    )


def test_verify_rejects_patch_hash_mismatch() -> None:
    p = _base_packet()
    caa = p["compile_apply_attestation"]
    assert isinstance(caa, dict)
    caa["patch_fingerprint_sha256"] = _PATCH_B
    with pytest.raises(ValueError, match="patch_hash_sha256"):
        verify_compile_apply_attestation_for_rollout(packet=p, manifest_control_plane=None)


def test_verify_scoped_apply_requires_applied_and_policy() -> None:
    p = _base_packet(caa_mode="scoped_apply", caa_applied=False)
    with pytest.raises(ValueError, match="applied=true"):
        verify_compile_apply_attestation_for_rollout(packet=p, manifest_control_plane=None)

    p2 = _base_packet(caa_mode="scoped_apply", caa_applied=True)
    caa2 = p2["compile_apply_attestation"]
    assert isinstance(caa2, dict)
    caa2["policy_allow_decision"] = {"allowed": False, "reason": "deny"}
    with pytest.raises(ValueError, match="policy_allow_decision"):
        verify_compile_apply_attestation_for_rollout(packet=p2, manifest_control_plane=None)


def test_verify_manifest_cross_check() -> None:
    p = _base_packet()
    caa = p["compile_apply_attestation"]
    assert isinstance(caa, dict)
    manifest_cp = {"compile_apply_attestation": {**caa, "touched_paths": ["y.py"]}}
    with pytest.raises(ValueError, match="manifest control_plane"):
        verify_compile_apply_attestation_for_rollout(packet=p, manifest_control_plane=manifest_cp)


def test_fingerprint_stable_for_key_order() -> None:
    a = {"compile_realization_mode": "artifact_only", "applied": False, "patch_fingerprint_sha256": _PATCH}
    b = {"applied": False, "patch_fingerprint_sha256": _PATCH, "compile_realization_mode": "artifact_only"}
    assert compile_apply_attestation_fingerprint(a) == compile_apply_attestation_fingerprint(b)


def test_summarize_scoped_apply_denial() -> None:
    s = summarize_compile_apply_from_control_plane(
        {
            "compile_apply_attestation": {
                "compile_realization_mode": "scoped_apply",
                "applied": False,
                "policy_allow_decision": {"allowed": True},
            }
        }
    )
    assert s["compile_apply_denial_reason"] == "compile_scoped_apply_not_applied"


def test_compile_apply_attestation_denial_for_rollout_reads_packet(tmp_path: Path) -> None:
    base = tmp_path / "tenant" / "repo"
    packet_rel = ".akc/promotion/run.packet.json"
    packet_path = base / packet_rel
    packet_path.parent.mkdir(parents=True, exist_ok=True)
    packet_obj = _base_packet()

    packet_path.write_text(json.dumps(packet_obj), encoding="utf-8")
    cp = {
        "promotion_packet_ref": {"path": packet_rel, "sha256": "0" * 64},
        "compile_apply_attestation": packet_obj["compile_apply_attestation"],
    }
    denial = compile_apply_attestation_denial_for_rollout(
        scope_root=base,
        manifest_control_plane=cp,
    )
    assert denial is None
