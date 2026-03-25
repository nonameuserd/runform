from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Literal

from akc.run.manifest import RuntimeEvidenceRecord

DeliveryTargetLane = Literal["staging", "production"]

_LIFECYCLE_EVENT_TO_KEY_MIN: dict[str, str] = {
    "staging_healthy": "staging_healthy_at",
    "prod_deploy_started": "prod_deploy_started_at",
    "prod_healthy": "prod_healthy_at",
    "approval_wait_started": "approval_wait_started_at",
}


def resolve_delivery_target_lane(
    *,
    cli_value: str | None,
    env_value: str | None,
) -> DeliveryTargetLane:
    """Classify the delivery lane for this runtime invocation (CLI overrides env)."""

    for raw in (cli_value, env_value):
        if raw is None:
            continue
        s = str(raw).strip().lower()
        if s in {"production", "prod"}:
            return "production"
        if s in {"staging", "local", "dev", "development"}:
            return "staging"
    return "staging"


def _merge_ts(target: dict[str, int], key: str, t: int, *, mode: Literal["min", "max"]) -> None:
    if t < 0:
        return
    if mode == "min":
        if key not in target or t < target[key]:
            target[key] = t
    else:
        if key not in target or t > target[key]:
            target[key] = t


def extract_delivery_lifecycle_from_evidence(
    evidence: Sequence[RuntimeEvidenceRecord],
) -> tuple[dict[str, int], int]:
    """Parse ``delivery_lifecycle`` rows: canonical *_at timestamps plus manual-touch tally."""

    ts: dict[str, int] = {}
    manual_touch = 0
    for rec in evidence:
        if rec.evidence_type != "delivery_lifecycle":
            continue
        payload = rec.payload
        event = str(payload.get("event", "")).strip().lower()
        t = int(rec.timestamp)
        key_min = _LIFECYCLE_EVENT_TO_KEY_MIN.get(event)
        if key_min is not None:
            _merge_ts(ts, key_min, t, mode="min")
            continue
        if event == "approval_wait_completed":
            _merge_ts(ts, "approval_wait_completed_at", t, mode="max")
            continue
        if event == "manual_touch":
            delta = payload.get("count")
            if isinstance(delta, int) and not isinstance(delta, bool) and delta > 0:
                manual_touch += int(delta)
            else:
                manual_touch += 1
    return ts, manual_touch


def project_delivery_run_projection(
    *,
    evidence: Sequence[RuntimeEvidenceRecord],
    delivery_lane: DeliveryTargetLane,
    record_started_at_ms: int,
    terminal_health_status: str,
    runtime_healthy_at: int | None,
) -> dict[str, Any]:
    """Fold evidence + coarse exit status into delivery lifecycle keys for the control plane."""

    ts, manual_touch = extract_delivery_lifecycle_from_evidence(evidence)
    has_reconcile = any(rec.evidence_type == "reconcile_outcome" for rec in evidence)
    if delivery_lane == "production" and has_reconcile:
        _merge_ts(ts, "prod_deploy_started_at", int(record_started_at_ms), mode="min")
    th = str(terminal_health_status).strip().lower()
    if th == "healthy" and runtime_healthy_at is not None:
        h = int(runtime_healthy_at)
        if delivery_lane == "staging":
            _merge_ts(ts, "staging_healthy_at", h, mode="min")
        else:
            _merge_ts(ts, "prod_healthy_at", h, mode="min")
    return {"timestamps": ts, "manual_touch_count": int(manual_touch)}
