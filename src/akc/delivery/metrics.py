"""Derived delivery funnel metrics from session state + append-only events."""

from __future__ import annotations

from typing import Any

from akc.delivery.event_types import (
    DELIVERY_ACTIVATION_FIRST_RUN,
    DELIVERY_INVITE_SENT,
    DELIVERY_PROVIDER_INSTALL_DETECTED,
    DELIVERY_RECIPIENT_ACTIVE,
    DELIVERY_STORE_LIVE,
)


def _ms_from_events(events: list[dict[str, Any]], event_type: str) -> int | None:
    ts: list[int] = []
    for row in events:
        if str(row.get("event_type", "")).strip() != event_type:
            continue
        raw = row.get("occurred_at_unix_ms")
        if isinstance(raw, (int, float)) and not isinstance(raw, bool):
            ts.append(int(raw))
    return min(ts) if ts else None


def _recipient_set_from_events(events: list[dict[str, Any]], event_type: str) -> set[str]:
    out: set[str] = set()
    for row in events:
        if str(row.get("event_type", "")).strip() != event_type:
            continue
        payload = row.get("payload")
        if not isinstance(payload, dict):
            continue
        email = str(payload.get("recipient", "")).strip().lower()
        if email:
            out.add(email)
    return out


def compute_delivery_metrics(
    *,
    request: dict[str, Any],
    session: dict[str, Any],
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    """Return metric keys expected by the delivery control plane (nullable when not computable)."""

    t0_raw = request.get("created_at_unix_ms")
    t0 = int(t0_raw) if isinstance(t0_raw, (int, float)) and not isinstance(t0_raw, bool) else None

    invite_ts = _ms_from_events(events, DELIVERY_INVITE_SENT)
    first_active_ts = _ms_from_events(events, DELIVERY_RECIPIENT_ACTIVE)
    store_live_ts = _ms_from_events(events, DELIVERY_STORE_LIVE)

    request_to_invite_sent_ms: float | None = None
    if t0 is not None and invite_ts is not None:
        request_to_invite_sent_ms = float(invite_ts - t0)

    request_to_first_active_ms: float | None = None
    if t0 is not None and first_active_ts is not None:
        request_to_first_active_ms = float(first_active_ts - t0)

    request_to_store_live_ms: float | None = None
    if t0 is not None and store_live_ts is not None:
        request_to_store_live_ms = float(store_live_ts - t0)

    recips_raw = request.get("recipients")
    recipients = [str(x).strip().lower() for x in recips_raw] if isinstance(recips_raw, list) else []
    rollup = session.get("activation_proof")
    fully = 0
    if isinstance(rollup, dict):
        fs = rollup.get("recipients_fully_satisfied")
        if isinstance(fs, (int, float)) and not isinstance(fs, bool):
            fully = int(fs)

    prov_set = _recipient_set_from_events(events, DELIVERY_PROVIDER_INSTALL_DETECTED)
    first_run_set = _recipient_set_from_events(events, DELIVERY_ACTIVATION_FIRST_RUN)
    acceptance_emails = prov_set | first_run_set
    install_emails = _recipient_set_from_events(events, DELIVERY_PROVIDER_INSTALL_DETECTED)

    invite_acceptance_rate: float | None = None
    install_rate: float | None = None
    activation_rate: float | None = None
    if recipients:
        invite_acceptance_rate = float(len(acceptance_emails)) / float(len(recipients))
        install_rate = float(len(install_emails)) / float(len(recipients))
        activation_rate = float(fully) / float(len(recipients))

    return {
        "request_to_invite_sent_ms": request_to_invite_sent_ms,
        "request_to_first_active_ms": request_to_first_active_ms,
        "invite_acceptance_rate": invite_acceptance_rate,
        "install_rate": install_rate,
        "activation_rate": activation_rate,
        "request_to_store_live_ms": request_to_store_live_ms,
    }


def merge_metrics_into_session_doc(session: dict[str, Any], metrics: dict[str, Any]) -> dict[str, Any]:
    """Non-destructive summary block for local session.json (optional mirror)."""

    out = dict(session)
    cm = out.get("control_plane_metrics")
    ctrl = dict(cm) if isinstance(cm, dict) else {}
    for k, v in metrics.items():
        ctrl[k] = v
    out["control_plane_metrics"] = ctrl
    return out
