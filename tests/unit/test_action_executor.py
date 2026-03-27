from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

import pytest

from akc.action.executor import execute_plan
from akc.action.models import ActionIntentV1, ActionPlanStepV1, ActionPlanV1
from akc.action.policy import ActionPolicyContext, evaluate_action_policy
from akc.action.provider_registry import (
    ActionProviderCompensationContext,
    ActionProviderExecutionContext,
    CompensationSupport,
    ProviderErrorKind,
    ProviderExecutionResult,
    ProviderRegistry,
)
from akc.action.store import ActionStore


class _FlakyTransportProvider:
    def __init__(self) -> None:
        self.calls = 0

    def preflight(self, scope: dict[str, str]) -> None:
        _ = scope

    def execute(self, step: ActionPlanStepV1, context: ActionProviderExecutionContext) -> ProviderExecutionResult:
        _ = (step, context)
        self.calls += 1
        if self.calls < 2:
            raise RuntimeError("temporary network failure")
        return ProviderExecutionResult(status="ok", payload={"ok": True}, external_id="ext_1")

    def compensate(self, step: ActionPlanStepV1, context: ActionProviderCompensationContext) -> ProviderExecutionResult:
        _ = (step, context)
        return ProviderExecutionResult(status="ok", payload={"compensated": True})

    def classify_error(self, error: Exception) -> ProviderErrorKind:
        _ = error
        return "retriable_transport"

    def compensation_support(self, step: ActionPlanStepV1) -> CompensationSupport:
        _ = step
        return "reversal"


class _BusinessFailureProvider:
    def preflight(self, scope: dict[str, str]) -> None:
        _ = scope

    def execute(self, step: ActionPlanStepV1, context: ActionProviderExecutionContext) -> ProviderExecutionResult:
        _ = (step, context)
        raise ValueError("booking window closed")

    def compensate(self, step: ActionPlanStepV1, context: ActionProviderCompensationContext) -> ProviderExecutionResult:
        _ = (step, context)
        return ProviderExecutionResult(status="ok", payload={"reverted": True})

    def classify_error(self, error: Exception) -> ProviderErrorKind:
        _ = error
        return "non_retriable_business"

    def compensation_support(self, step: ActionPlanStepV1) -> CompensationSupport:
        _ = step
        return "reversal"


def _intent() -> ActionIntentV1:
    unique = uuid4().hex[:8]
    return ActionIntentV1(
        schema_kind="action_intent",
        schema_version=1,
        intent_id=f"intent_test_{unique}",
        tenant_id="tenant_a",
        repo_id="repo_a",
        actor_id="user_1",
        channel="cli",
        utterance="book a flight",
        goal="book a flight",
    )


def test_execute_plan_retries_retriable_transport_once(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AKC_ACTION_MEDIUM_ALLOWLIST", "action.message.send")
    store = ActionStore(base_dir=tmp_path)
    intent = _intent()
    consent_root = tmp_path / ".akc" / "actions" / "tenant_a" / "repo_a" / "consents"
    consent_root.mkdir(parents=True, exist_ok=True)
    (consent_root / "user_1.json").write_text('{"allow_actions": ["action.message.send"]}', encoding="utf-8")
    plan = ActionPlanV1(
        schema_kind="action_plan",
        schema_version=1,
        intent_id=intent.intent_id,
        steps=(
            ActionPlanStepV1(
                step_id="step_1",
                action_type="action.message.send",
                provider="flaky",
                inputs={"to": "+14155550123"},
                idempotency_key="idem_1",
                risk_tier="medium",
                requires_approval=False,
                compensation={"mode": "manual"},
            ),
        ),
    )
    reg = ProviderRegistry()
    reg.register(name="flaky", provider=_FlakyTransportProvider())
    result = execute_plan(intent=intent, plan=plan, store=store, providers=reg, mode="live")
    rows = store.read_execution(intent_id=intent.intent_id)
    assert result["status"] == "completed"
    assert len(rows) == 1
    assert rows[0]["status"] == "succeeded"
    checkpoint_path = tmp_path / intent.tenant_id / intent.repo_id / ".akc" / "runtime" / intent.intent_id / "action"
    checkpoint_path = checkpoint_path / "checkpoint.json"
    assert checkpoint_path.exists()


def test_execute_plan_non_retriable_stops_and_marks_manual_compensation(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AKC_ACTION_MEDIUM_ALLOWLIST", "action.calendar.write,action.message.send")
    store = ActionStore(base_dir=tmp_path)
    intent = _intent()
    consent_root = tmp_path / ".akc" / "actions" / "tenant_a" / "repo_a" / "consents"
    consent_root.mkdir(parents=True, exist_ok=True)
    (consent_root / "user_1.json").write_text(
        '{"allow_actions": ["action.calendar.write", "action.message.send"]}',
        encoding="utf-8",
    )
    plan = ActionPlanV1(
        schema_kind="action_plan",
        schema_version=1,
        intent_id=intent.intent_id,
        steps=(
            ActionPlanStepV1(
                step_id="step_1",
                action_type="action.calendar.write",
                provider="noop",
                inputs={"title": "Hold"},
                idempotency_key="idem_1",
                risk_tier="medium",
                requires_approval=False,
                compensation={"mode": "manual"},
            ),
            ActionPlanStepV1(
                step_id="step_2",
                action_type="action.message.send",
                provider="biz",
                inputs={"to": "+14155550123", "body": "hello"},
                idempotency_key="idem_2",
                risk_tier="medium",
                requires_approval=False,
                compensation={"mode": "reversal"},
            ),
        ),
    )
    reg = ProviderRegistry()
    reg.register(name="biz", provider=_BusinessFailureProvider())
    result = execute_plan(intent=intent, plan=plan, store=store, providers=reg, mode="live")
    assert result["status"] == "failed"
    assert result["steps"][1]["status"] == "failed"
    assert result["steps"][1]["payload"]["classification"] == "non_retriable_business"
    assert result["compensations"][0]["status"] == "manual_compensations_required"


def test_execute_plan_high_risk_requires_approval_with_policy_narrative(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("AKC_ACTION_MEDIUM_ALLOWLIST", "action.calendar.write")
    store = ActionStore(base_dir=tmp_path)
    intent = _intent()
    plan = ActionPlanV1(
        schema_kind="action_plan",
        schema_version=1,
        intent_id=intent.intent_id,
        steps=(
            ActionPlanStepV1(
                step_id="step_1",
                action_type="action.flight.book",
                provider="noop",
                inputs={"offer_id": "offer_1"},
                idempotency_key="idem_1",
                risk_tier="high",
                requires_approval=True,
                compensation={"mode": "manual"},
            ),
        ),
    )
    result = execute_plan(intent=intent, plan=plan, store=store, providers=ProviderRegistry(), mode="live")
    assert result["status"] == "pending_approval"
    assert result["steps"][0]["status"] == "pending_approval"
    assert result["steps"][0]["reason"] == "policy.action.approval_required"
    assert "requires explicit operator approval" in result["steps"][0]["narrative"]
    policy_decisions = json.loads(
        (
            tmp_path
            / ".akc"
            / "actions"
            / intent.tenant_id
            / intent.repo_id
            / intent.intent_id
            / "policy_decisions.json"
        ).read_text(encoding="utf-8")
    )
    first = policy_decisions["decisions"][0]
    assert first["allowed"] is False
    assert first["reason"] == "policy.action.approval_required"
    assert "requires explicit operator approval" in first["narrative"]


@pytest.mark.parametrize(
    ("action", "risk_tier", "allowlist", "consent", "step_approved", "expected_allowed", "expected_reason"),
    [
        ("action.contact.lookup", "low", "", False, False, True, "policy.allowlist.allow"),
        ("action.call.place", "medium", "action.call.place", True, False, True, "policy.allowlist.allow"),
        (
            "action.call.place",
            "medium",
            "action.call.place",
            False,
            False,
            False,
            "policy.action.user_consent_missing",
        ),
        (
            "action.calendar.write",
            "medium",
            "action.call.place",
            True,
            False,
            False,
            "policy.default_deny.action_not_allowlisted",
        ),
        ("action.flight.book", "high", "", False, False, False, "policy.action.approval_required"),
        ("action.flight.book", "high", "", False, True, True, "policy.allowlist.allow"),
    ],
)
def test_evaluate_action_policy_risk_tier_defaults(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    action: str,
    risk_tier: str,
    allowlist: str,
    consent: bool,
    step_approved: bool,
    expected_allowed: bool,
    expected_reason: str,
) -> None:
    if allowlist:
        monkeypatch.setenv("AKC_ACTION_MEDIUM_ALLOWLIST", allowlist)
    else:
        monkeypatch.delenv("AKC_ACTION_MEDIUM_ALLOWLIST", raising=False)
    consent_root = tmp_path / ".akc" / "actions" / "tenant_a" / "repo_a" / "consents"
    consent_root.mkdir(parents=True, exist_ok=True)
    if consent:
        (consent_root / "user_1.json").write_text(
            json.dumps({"allow_actions": [action]}, sort_keys=True),
            encoding="utf-8",
        )
    decision = evaluate_action_policy(
        policy_ctx=ActionPolicyContext(
            action=action,
            risk_tier=risk_tier,
            intent_id="intent_policy_test",
            step_id="step_1",
            actor_id="user_1",
            channel="cli",
            tenant_id="tenant_a",
            repo_id="repo_a",
            user_has_consent=consent,
            step_is_approved=step_approved,
        ),
        consent_root=consent_root,
    )
    assert decision["allowed"] is expected_allowed
    assert decision["reason"] == expected_reason
