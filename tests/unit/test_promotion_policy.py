from __future__ import annotations

from akc.compile.controller_policy_runtime import COMPILE_PATCH_APPLY_ACTION
from akc.compile.interfaces import TenantRepoScope
from akc.control.policy import (
    CapabilityIssuer,
    DefaultDenyPolicyEngine,
    ToolAuthorizationPolicy,
    ToolAuthorizationRequest,
)
from akc.promotion import latest_allow_decision_for_action, latest_policy_allow_decision


def test_latest_allow_decision_for_action_picks_matching_allow() -> None:
    trace = [
        {"action": "llm.complete", "allowed": True, "token_id": "a", "reason": "ok"},
        {"action": COMPILE_PATCH_APPLY_ACTION, "allowed": True, "token_id": "b", "reason": "ok"},
    ]
    d = latest_allow_decision_for_action(trace, action=COMPILE_PATCH_APPLY_ACTION)
    assert d["allowed"] is True
    assert d["token_id"] == "b"
    assert d["action"] == COMPILE_PATCH_APPLY_ACTION


def test_latest_allow_decision_for_action_default_deny() -> None:
    trace = [{"action": COMPILE_PATCH_APPLY_ACTION, "allowed": False, "token_id": "x", "reason": "deny"}]
    d = latest_allow_decision_for_action(trace, action=COMPILE_PATCH_APPLY_ACTION)
    assert d["allowed"] is False
    assert d["reason"] == "no_allow_decision"


def test_latest_policy_allow_decision_ignores_action() -> None:
    trace = [
        {"action": "executor.run", "allowed": True, "token_id": "e", "reason": "ok"},
    ]
    d = latest_policy_allow_decision(trace)
    assert d["allowed"] is True
    assert d["token_id"] == "e"


def test_compile_patch_apply_not_allowlisted_is_denied() -> None:
    scope = TenantRepoScope(tenant_id="tenant-a", repo_id="repo-b")
    engine = DefaultDenyPolicyEngine(
        issuer=CapabilityIssuer(),
        policy=ToolAuthorizationPolicy(mode="enforce", allow_actions=("llm.complete", "executor.run")),
    )
    token = engine.issuer.issue(scope=scope, action=COMPILE_PATCH_APPLY_ACTION, constraints={})
    dec = engine.authorize(
        req=ToolAuthorizationRequest(
            scope=scope,
            action=COMPILE_PATCH_APPLY_ACTION,
            capability=token,
            context={"plan_id": "p1"},
        )
    )
    assert dec.allowed is False
    assert dec.reason == "policy.default_deny.action_not_allowlisted"
