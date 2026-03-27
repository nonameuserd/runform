from __future__ import annotations

from akc.action.intent_parse import parse_intent
from akc.action.planner import build_plan


def test_parse_intent_extracts_contact_and_date_entities() -> None:
    intent = parse_intent(
        text="schedule flights with my dad on 2026-05-10",
        tenant_id="tenant_a",
        repo_id="repo_a",
        actor_id="user_1",
        channel="cli",
    )
    assert intent.entities["contact_hint"] == "dad"
    assert intent.entities["date"] == "2026-05-10"


def test_build_plan_call_flow_resolves_then_dispatches_call() -> None:
    intent = parse_intent(
        text="call my dad",
        tenant_id="tenant_a",
        repo_id="repo_a",
        actor_id="user_1",
        channel="cli",
    )
    plan = build_plan(intent)
    assert [step.action_type for step in plan.steps] == [
        "action.contact.lookup",
        "action.call.place",
    ]
    assert [step.provider for step in plan.steps] == ["google", "twilio"]


def test_build_plan_contact_lookup_only_for_phone_number_query() -> None:
    intent = parse_intent(
        text="find my mom cellphone number",
        tenant_id="tenant_a",
        repo_id="repo_a",
        actor_id="user_1",
        channel="slack",
    )
    plan = build_plan(intent)
    assert len(plan.steps) == 1
    assert plan.steps[0].action_type == "action.contact.lookup"
    assert plan.steps[0].provider == "google"


def test_build_plan_schedule_flights_has_high_risk_booking_step() -> None:
    intent = parse_intent(
        text="schedule flights on 2026-05-10",
        tenant_id="tenant_a",
        repo_id="repo_a",
        actor_id="user_1",
        channel="cli",
    )
    plan = build_plan(intent)
    action_types = [step.action_type for step in plan.steps]
    assert action_types == [
        "action.calendar.read",
        "action.flight.search",
        "action.flight.book",
        "action.calendar.write",
        "action.message.send",
    ]
    booking_step = plan.steps[2]
    assert booking_step.risk_tier == "high"
    assert booking_step.requires_approval is True
