"""Canonical delivery control-plane event type strings (evidence / timeline)."""

from __future__ import annotations

from typing import Final

# Lifecycle & pipeline
DELIVERY_REQUEST_ACCEPTED: Final[str] = "delivery.request.accepted"
DELIVERY_COMPILE_COMPLETED: Final[str] = "delivery.compile.completed"
DELIVERY_COMPILE_OUTPUTS_BOUND: Final[str] = "delivery.compile.outputs.bound"
DELIVERY_BUILD_PACKAGED: Final[str] = "delivery.build.packaged"
DELIVERY_INVITE_SENT: Final[str] = "delivery.invite.sent"
DELIVERY_PROVIDER_INSTALL_DETECTED: Final[str] = "delivery.provider.install_detected"
DELIVERY_ACTIVATION_FIRST_RUN: Final[str] = "delivery.activation.first_run"
DELIVERY_RECIPIENT_ACTIVE: Final[str] = "delivery.recipient.active"
DELIVERY_STORE_SUBMITTED: Final[str] = "delivery.store.submitted"
DELIVERY_STORE_LIVE: Final[str] = "delivery.store.live"
DELIVERY_FAILED: Final[str] = "delivery.failed"

# Supplementary / operator (still written to events.json; not all are fleet KPI hooks)
DELIVERY_REQUEST_PARSED: Final[str] = "delivery.request.parsed"
DELIVERY_PREFLIGHT_COMPLETED: Final[str] = "delivery.preflight.completed"
DELIVERY_INVITE_RESEND_REQUESTED: Final[str] = "delivery.invite.resend_requested"
DELIVERY_STORE_PROMOTION_REQUESTED: Final[str] = "delivery.store.promotion_requested"
DELIVERY_RECIPIENTS_AMENDED: Final[str] = "delivery.recipients.amended"
DELIVERY_HUMAN_GATE_PASSED: Final[str] = "delivery.human_gate.passed"

CONTROL_PLANE_EVENT_TYPES: Final[frozenset[str]] = frozenset(
    {
        DELIVERY_REQUEST_ACCEPTED,
        DELIVERY_COMPILE_COMPLETED,
        DELIVERY_COMPILE_OUTPUTS_BOUND,
        DELIVERY_BUILD_PACKAGED,
        DELIVERY_INVITE_SENT,
        DELIVERY_PROVIDER_INSTALL_DETECTED,
        DELIVERY_ACTIVATION_FIRST_RUN,
        DELIVERY_RECIPIENT_ACTIVE,
        DELIVERY_STORE_SUBMITTED,
        DELIVERY_STORE_LIVE,
        DELIVERY_FAILED,
        DELIVERY_REQUEST_PARSED,
        DELIVERY_PREFLIGHT_COMPLETED,
        DELIVERY_INVITE_RESEND_REQUESTED,
        DELIVERY_STORE_PROMOTION_REQUESTED,
        DELIVERY_RECIPIENTS_AMENDED,
        DELIVERY_HUMAN_GATE_PASSED,
    },
)


def assert_known_delivery_event_type(event_type: str, *, strict: bool = False) -> None:
    """Optional guard for writers; ``strict`` raises on unknown types."""

    if not strict:
        return
    et = str(event_type).strip()
    if et not in CONTROL_PLANE_EVENT_TYPES:
        raise ValueError(f"unknown delivery event_type: {et!r}")
