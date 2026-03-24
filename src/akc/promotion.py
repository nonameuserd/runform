from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping, Sequence
from typing import Any, Literal, cast

from akc.memory.models import JSONValue

PromotionMode = Literal["artifact_only", "staged_apply", "live_apply"]
_PROMOTION_ORDER: dict[PromotionMode, int] = {
    "artifact_only": 0,
    "staged_apply": 1,
    "live_apply": 2,
}
_ALLOWED_MODES: set[str] = set(_PROMOTION_ORDER.keys())


def normalize_promotion_mode(value: str | None) -> PromotionMode | None:
    if value is None:
        return None
    s = str(value).strip().lower()
    if not s:
        return None
    if s not in _ALLOWED_MODES:
        raise ValueError("promotion_mode must be one of: artifact_only, staged_apply, live_apply")
    return cast(PromotionMode, s)


def resolve_default_promotion_mode(*, explicit: str | None, sandbox_mode: str | None) -> PromotionMode:
    direct = normalize_promotion_mode(explicit)
    if direct is not None:
        return direct
    env = str(os.environ.get("AKC_ENV") or os.environ.get("ENVIRONMENT") or "").strip().lower()
    if env in {"dev", "local", "development"}:
        return "artifact_only"
    if env:
        return "staged_apply"
    if str(sandbox_mode or "").strip().lower() != "dev":
        return "staged_apply"
    return "artifact_only"


def validate_promotion_transition(*, src: PromotionMode, dst: PromotionMode) -> bool:
    return _PROMOTION_ORDER[dst] >= _PROMOTION_ORDER[src]


def requires_deployable_steps(*, promotion_mode: PromotionMode, explicit: bool | None) -> bool:
    if explicit is not None:
        return bool(explicit)
    return promotion_mode in {"staged_apply", "live_apply"}


def objective_class_from_metadata(
    objective_metadata: Mapping[str, JSONValue] | None,
    *,
    default_class: str | None,
) -> str | None:
    keys = ("objective_class", "class", "objective_type", "type")
    if objective_metadata is not None:
        for key in keys:
            raw = objective_metadata.get(key)
            if isinstance(raw, str) and raw.strip():
                return raw.strip().lower()
    if default_class is not None and str(default_class).strip():
        return str(default_class).strip().lower()
    return None


def intent_declares_deployable_objective(*, intent: Any) -> bool:
    default_class: str | None = None
    meta = getattr(intent, "metadata", None)
    if isinstance(meta, Mapping):
        raw_default = meta.get("objective_class_default") or meta.get("objective_class")
        if isinstance(raw_default, str) and raw_default.strip():
            default_class = raw_default.strip().lower()
    objectives = tuple(getattr(intent, "objectives", ()) or ())
    if not objectives:
        # Fail-closed: no explicit objective classes means deployable by default.
        return True
    classes: list[str | None] = []
    for objective in objectives:
        obj_meta = getattr(objective, "metadata", None)
        obj_md = obj_meta if isinstance(obj_meta, Mapping) else None
        classes.append(objective_class_from_metadata(obj_md, default_class=default_class))
    # Only explicit analysis_only objectives opt out of deployable semantics.
    return not classes or any(c != "analysis_only" for c in classes)


def canonical_sha256(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def verify_signed_packet(packet: Mapping[str, Any]) -> tuple[bool, str | None]:
    claimed = packet.get("packet_signature_sha256")
    if not isinstance(claimed, str) or len(claimed.strip()) != 64:
        return False, "promotion packet missing packet_signature_sha256"
    payload = dict(packet)
    payload.pop("packet_signature_sha256", None)
    observed = canonical_sha256(payload)
    if observed != claimed.strip().lower():
        return False, "promotion packet signature mismatch"
    return True, None


def latest_policy_allow_decision(decisions: Sequence[Mapping[str, Any]]) -> dict[str, JSONValue]:
    for raw in reversed(list(decisions)):
        if not isinstance(raw, Mapping):
            continue
        if bool(raw.get("allowed", False)):
            return {
                "allowed": True,
                "action": str(raw.get("action", "")),
                "reason": str(raw.get("reason", "")),
                "token_id": str(raw.get("token_id", "")),
            }
    return {"allowed": False, "action": "", "reason": "no_allow_decision", "token_id": ""}


def latest_allow_decision_for_action(decisions: Sequence[Mapping[str, Any]], *, action: str) -> dict[str, JSONValue]:
    """Latest *allowing* decision for a specific action (default deny if none)."""

    want = str(action or "").strip()
    if not want:
        return {"allowed": False, "action": "", "reason": "no_allow_decision", "token_id": ""}
    for raw in reversed(list(decisions)):
        if not isinstance(raw, Mapping):
            continue
        if str(raw.get("action", "")) != want:
            continue
        if bool(raw.get("allowed", False)):
            return {
                "allowed": True,
                "action": want,
                "reason": str(raw.get("reason", "")),
                "token_id": str(raw.get("token_id", "")),
            }
    return {"allowed": False, "action": want, "reason": "no_allow_decision", "token_id": ""}
