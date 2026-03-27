from __future__ import annotations

from hashlib import sha256
from typing import Any

from akc.action.models import ActionIntentV1, ActionPlanStepV1, ActionPlanV1


def build_plan(intent: ActionIntentV1) -> ActionPlanV1:
    steps = _build_steps(intent=intent)
    return ActionPlanV1(
        schema_kind="action_plan",
        schema_version=1,
        intent_id=intent.intent_id,
        steps=steps,
    )


def _build_steps(*, intent: ActionIntentV1) -> tuple[ActionPlanStepV1, ...]:
    lowered = intent.goal.lower()
    entities = dict(intent.entities)

    if _looks_like_schedule_flights(lowered):
        return (
            _step(
                intent=intent,
                step_id="step_1",
                action_type="action.calendar.read",
                provider="google",
                risk_tier="low",
                requires_approval=False,
                inputs={
                    "goal": intent.goal,
                    "time_range": {"date_hint": entities.get("date")},
                },
            ),
            _step(
                intent=intent,
                step_id="step_2",
                action_type="action.flight.search",
                provider="amadeus",
                risk_tier="low",
                requires_approval=False,
                inputs={
                    "goal": intent.goal,
                    "departure_date": entities.get("date"),
                    "origin": entities.get("origin"),
                    "destination": entities.get("destination"),
                },
            ),
            _step(
                intent=intent,
                step_id="step_3",
                action_type="action.flight.book",
                provider="amadeus",
                risk_tier="high",
                requires_approval=True,
                compensation={"mode": "provider_reversal"},
                inputs={
                    "goal": intent.goal,
                    "offer_id": entities.get("offer_id", "offer_demo_1"),
                },
            ),
            _step(
                intent=intent,
                step_id="step_4",
                action_type="action.calendar.write",
                provider="google",
                risk_tier="medium",
                requires_approval=False,
                compensation={"mode": "reversal"},
                inputs={
                    "goal": intent.goal,
                    "event": {"title": "Booked flight", "date": entities.get("date")},
                },
            ),
            _step(
                intent=intent,
                step_id="step_5",
                action_type="action.message.send",
                provider="twilio",
                risk_tier="medium",
                requires_approval=False,
                inputs={
                    "goal": intent.goal,
                    "body": "Your flight is booked and calendar has been updated.",
                },
            ),
        )

    if _looks_like_call(lowered):
        return (
            _step(
                intent=intent,
                step_id="step_1",
                action_type="action.contact.lookup",
                provider="google",
                risk_tier="low",
                requires_approval=False,
                inputs={
                    "goal": intent.goal,
                    "contact_hint": entities.get("contact_hint"),
                },
            ),
            _step(
                intent=intent,
                step_id="step_2",
                action_type="action.call.place",
                provider="twilio",
                risk_tier="medium",
                requires_approval=False,
                inputs={
                    "goal": intent.goal,
                    "to": entities.get("phone"),
                    "contact_source_step_id": "step_1",
                },
            ),
        )

    if _looks_like_contact_lookup(lowered):
        return (
            _step(
                intent=intent,
                step_id="step_1",
                action_type="action.contact.lookup",
                provider="google",
                risk_tier="low",
                requires_approval=False,
                inputs={
                    "goal": intent.goal,
                    "contact_hint": entities.get("contact_hint"),
                },
            ),
        )

    action_type, risk_tier = _classify_action(intent.goal)
    return (
        _step(
            intent=intent,
            step_id="step_1",
            action_type=action_type,
            provider=_provider_for_action(action_type),
            risk_tier=risk_tier,
            requires_approval=risk_tier == "high",
            inputs={"goal": intent.goal, "entities": intent.entities},
        ),
    )


def _step(
    *,
    intent: ActionIntentV1,
    step_id: str,
    action_type: str,
    provider: str,
    risk_tier: str,
    requires_approval: bool,
    inputs: dict[str, Any],
    compensation: dict[str, Any] | None = None,
) -> ActionPlanStepV1:
    return ActionPlanStepV1(
        step_id=step_id,
        action_type=action_type,
        provider=provider,
        inputs=inputs,
        idempotency_key=_idempotency_key(intent_id=intent.intent_id, step_id=step_id, action_type=action_type),
        risk_tier=risk_tier,
        requires_approval=requires_approval,
        compensation=compensation or {"mode": "manual"},
    )


def _idempotency_key(*, intent_id: str, step_id: str, action_type: str) -> str:
    return sha256(f"{intent_id}:{step_id}:{action_type}".encode()).hexdigest()


def _provider_for_action(action_type: str) -> str:
    if action_type in {"action.contact.lookup", "action.calendar.read", "action.calendar.write"}:
        return "google"
    if action_type in {"action.call.place", "action.message.send"}:
        return "twilio"
    if action_type in {"action.flight.search", "action.flight.book"}:
        return "amadeus"
    return "noop"


def _classify_action(goal: str) -> tuple[str, str]:
    lowered = str(goal or "").lower()
    if "flight" in lowered and any(k in lowered for k in ("book", "purchase", "buy", "pay")):
        return "action.flight.book", "high"
    if "flight" in lowered and any(k in lowered for k in ("search", "find", "options")):
        return "action.flight.search", "low"
    if any(k in lowered for k in ("call", "dial")):
        return "action.call.place", "medium"
    if any(k in lowered for k in ("text", "sms", "message", "notify")):
        return "action.message.send", "medium"
    if any(k in lowered for k in ("calendar", "schedule", "meeting", "event")):
        if any(k in lowered for k in ("create", "add", "schedule", "book")):
            return "action.calendar.write", "medium"
        return "action.calendar.read", "low"
    if any(k in lowered for k in ("contact", "phone number", "lookup", "find")):
        return "action.contact.lookup", "low"
    return "action.contact.lookup", "low"


def _looks_like_call(goal_lowered: str) -> bool:
    return any(token in goal_lowered for token in ("call ", "call my", "dial "))


def _looks_like_contact_lookup(goal_lowered: str) -> bool:
    return (
        "find" in goal_lowered and any(token in goal_lowered for token in ("phone", "cellphone", "number", "contact"))
    ) or "contact lookup" in goal_lowered


def _looks_like_schedule_flights(goal_lowered: str) -> bool:
    return "flight" in goal_lowered and "schedule" in goal_lowered
