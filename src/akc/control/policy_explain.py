"""Load policy decisions from a run manifest (inline or ref) for operator explain flows."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from akc.control.operator_workflows import read_repo_relative_file
from akc.control.policy_reason_narrative import describe_policy_reason
from akc.memory.models import JSONValue
from akc.run.manifest import RunManifest


def policy_provenance_from_control_plane(control_plane: dict[str, JSONValue] | None) -> dict[str, str | None]:
    """Return bundle id / git sha / rego version when present on the envelope."""

    cp = control_plane or {}
    out: dict[str, str | None] = {}
    for k in ("policy_bundle_id", "policy_git_sha", "rego_pack_version"):
        raw = cp.get(k)
        s = str(raw).strip() if raw is not None else ""
        out[k] = s or None
    return out


def load_policy_decisions_for_manifest(
    *,
    manifest: RunManifest,
    scope_root: Path,
) -> tuple[list[dict[str, Any]], str]:
    """Return ``(decisions, source_tag)`` where source is ``inline``, ``ref:…``, or ``none``."""

    cp = manifest.control_plane or {}

    ref = cp.get("policy_decisions_ref")
    if isinstance(ref, dict):
        rel = str(ref.get("path", "")).strip()
        if rel:
            try:
                raw_bytes = read_repo_relative_file(scope_root=scope_root, rel_path=rel)
                loaded = json.loads(raw_bytes.decode("utf-8"))
                if isinstance(loaded, list):
                    decisions = [dict(x) for x in loaded if isinstance(x, dict)]
                    if decisions:
                        return decisions, f"ref:{rel}"
            except (OSError, ValueError, json.JSONDecodeError):
                pass

    inline = cp.get("policy_decisions")
    if isinstance(inline, list) and len(inline) > 0:
        decisions = [dict(x) for x in inline if isinstance(x, dict)]
        if decisions:
            return decisions, "inline"

    return [], "none"


def build_policy_explain_payload(
    *,
    manifest: RunManifest,
    scope_root: Path,
) -> dict[str, Any]:
    """Structured explain payload for CLI/viewer (read-only)."""

    decisions, src = load_policy_decisions_for_manifest(manifest=manifest, scope_root=scope_root)
    cp = manifest.control_plane or {}
    provenance = policy_provenance_from_control_plane(cp if isinstance(cp, dict) else None)
    explained = []
    for i, d in enumerate(decisions):
        reason = str(d.get("reason", "") or "")
        explained.append(
            {
                "index": i,
                "action": d.get("action"),
                "allowed": bool(d.get("allowed", False)),
                "reason": reason,
                "reason_detail": describe_policy_reason(reason),
                "source": d.get("source"),
                "mode": d.get("mode"),
                "block": d.get("block"),
                "scope": d.get("scope"),
                "context": d.get("context"),
            }
        )
    return {
        "run_id": manifest.run_id,
        "tenant_id": manifest.tenant_id,
        "repo_id": manifest.repo_id,
        "policy_decisions_source": src,
        "policy_provenance": provenance,
        "decisions": decisions,
        "decisions_explained": explained,
    }
