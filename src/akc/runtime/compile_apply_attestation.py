"""Compile-apply attestation checks for runtime rollout (intent strong path).

Validates signed promotion packet ``compile_apply_attestation`` against the compile
run manifest and enforces scoped-apply policy expectations before live deployment
reconcile.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from akc.path_security import safe_resolve_path
from akc.utils.fingerprint import stable_json_fingerprint

_ATTESTATION_KEYS = (
    "compile_realization_mode",
    "applied",
    "apply_decision_token_id",
    "policy_allow_decision",
    "patch_fingerprint_sha256",
    "scope_root",
    "touched_paths",
)


def compile_apply_attestation_fingerprint(caa: Mapping[str, Any]) -> str:
    """Stable fingerprint for comparing manifest vs signed packet attestations."""

    norm: dict[str, Any] = {}
    for key in _ATTESTATION_KEYS:
        if key not in caa:
            continue
        val = caa[key]
        if key == "touched_paths" and isinstance(val, list):
            norm[key] = sorted(str(x) for x in val)
        elif key == "policy_allow_decision" and isinstance(val, Mapping):
            norm[key] = {str(k): val[k] for k in sorted(val.keys(), key=str)}
        else:
            norm[key] = val
    return stable_json_fingerprint(norm)


def verify_compile_apply_attestation_for_rollout(
    *,
    packet: Mapping[str, Any],
    manifest_control_plane: Mapping[str, Any] | None,
) -> None:
    """Raise ``ValueError`` if compile-apply attestation is missing or inconsistent."""

    caa = packet.get("compile_apply_attestation")
    if not isinstance(caa, dict):
        raise ValueError("live mutation requires compile_apply_attestation in signed promotion packet")
    patch_top = str(packet.get("patch_hash_sha256", "")).strip().lower()
    patch_fp = str(caa.get("patch_fingerprint_sha256", "")).strip().lower()
    if len(patch_fp) != 64 or any(c not in "0123456789abcdef" for c in patch_fp):
        raise ValueError("compile-apply attestation invalid: patch_fingerprint_sha256 must be a 64-char hex sha256")
    if patch_top != patch_fp:
        raise ValueError(
            "compile-apply attestation mismatch: patch_hash_sha256 does not match "
            "compile_apply_attestation.patch_fingerprint_sha256"
        )
    mode = str(caa.get("compile_realization_mode", "scoped_apply")).strip()
    if mode not in {"artifact_only", "scoped_apply"}:
        raise ValueError(
            "compile-apply attestation invalid: compile_realization_mode must be "
            f"artifact_only or scoped_apply, got {mode!r}"
        )
    if mode == "scoped_apply":
        if not bool(caa.get("applied")):
            raise ValueError("live mutation denied: scoped_apply requires compile_apply_attestation.applied=true")
        pol = caa.get("policy_allow_decision")
        if not isinstance(pol, Mapping) or not bool(pol.get("allowed")):
            raise ValueError(
                "live mutation denied: compile_apply_attestation.policy_allow_decision.allowed is not true "
                "for scoped_apply"
            )
    if manifest_control_plane is not None:
        m_caa = manifest_control_plane.get("compile_apply_attestation")
        if isinstance(m_caa, dict):
            fp_m = compile_apply_attestation_fingerprint(m_caa)
            fp_p = compile_apply_attestation_fingerprint(caa)
            if fp_m != fp_p:
                raise ValueError(
                    "compile-apply attestation mismatch: signed promotion packet does not match "
                    "compile run manifest control_plane.compile_apply_attestation"
                )


def compile_apply_attestation_denial_for_rollout(
    *,
    scope_root: Path,
    manifest_control_plane: Mapping[str, Any],
) -> str | None:
    """Load the signed promotion packet and return a denial message, or ``None`` if OK."""

    ref = manifest_control_plane.get("promotion_packet_ref")
    if not isinstance(ref, dict):
        return None
    rel = str(ref.get("path", "")).strip()
    if not rel:
        return None
    packet_path = safe_resolve_path(scope_root / rel)
    if not packet_path.is_file():
        return None
    try:
        raw = json.loads(packet_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(raw, dict):
        return None
    try:
        verify_compile_apply_attestation_for_rollout(
            packet=raw,
            manifest_control_plane=manifest_control_plane,
        )
    except ValueError as exc:
        return str(exc)
    return None


def summarize_compile_apply_from_control_plane(
    control_plane: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Extract scoreboard-friendly compile realization fields from manifest control_plane."""

    empty = {
        "compile_realization_mode": None,
        "compile_apply_applied": None,
        "compile_apply_policy_allowed": None,
        "compile_apply_denial_reason": None,
    }
    if not isinstance(control_plane, dict):
        return dict(empty)
    caa = control_plane.get("compile_apply_attestation")
    if not isinstance(caa, dict):
        return dict(empty)
    mode = str(caa.get("compile_realization_mode", "")).strip() or None
    applied: bool | None = None
    if "applied" in caa:
        applied = bool(caa.get("applied"))
    pol = caa.get("policy_allow_decision")
    policy_allowed: bool | None = None
    if isinstance(pol, Mapping):
        policy_allowed = bool(pol.get("allowed"))
    denial_reason: str | None = None
    if mode == "scoped_apply":
        if applied is False:
            denial_reason = "compile_scoped_apply_not_applied"
        elif policy_allowed is False:
            raw_reason = str((pol or {}).get("reason") or "").strip()
            denial_reason = raw_reason or "compile_apply_policy_denied"
    return {
        "compile_realization_mode": mode,
        "compile_apply_applied": applied,
        "compile_apply_policy_allowed": policy_allowed,
        "compile_apply_denial_reason": denial_reason,
    }
