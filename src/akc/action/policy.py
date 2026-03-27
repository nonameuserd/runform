from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from akc.compile.interfaces import TenantRepoScope
from akc.control.policy import (
    CapabilityIssuer,
    DefaultDenyPolicyEngine,
    PolicyDecision,
    ToolAuthorizationPolicy,
    ToolAuthorizationRequest,
)
from akc.control.policy_reason_narrative import describe_policy_reason

LOW_RISK_ACTIONS: frozenset[str] = frozenset(
    {
        "action.contact.lookup",
        "action.calendar.read",
        "action.flight.search",
    }
)
MEDIUM_RISK_ACTIONS: frozenset[str] = frozenset(
    {
        "action.call.place",
        "action.calendar.write",
        "action.message.send",
    }
)
HIGH_RISK_ACTIONS: frozenset[str] = frozenset(
    {
        "action.flight.book",
    }
)
ALL_ACTIONS: frozenset[str] = LOW_RISK_ACTIONS | MEDIUM_RISK_ACTIONS | HIGH_RISK_ACTIONS


@dataclass(frozen=True, slots=True)
class ActionPolicyContext:
    action: str
    risk_tier: str
    intent_id: str
    step_id: str
    actor_id: str | None
    channel: str | None
    tenant_id: str
    repo_id: str
    user_has_consent: bool
    step_is_approved: bool


def evaluate_action_policy(
    *,
    policy_ctx: ActionPolicyContext,
    consent_root: Path,
) -> dict[str, Any]:
    action = str(policy_ctx.action).strip()
    risk_tier = str(policy_ctx.risk_tier).strip().lower()
    medium_allow = _medium_allowlist_from_env()
    # Low-risk reads auto-execute by default. Medium-risk mutating actions require explicit allowlist.
    # High-risk actions are recognized but blocked unless explicit step approval exists.
    allow_actions = tuple(sorted(LOW_RISK_ACTIONS | HIGH_RISK_ACTIONS | medium_allow))
    scope = TenantRepoScope(tenant_id=policy_ctx.tenant_id, repo_id=policy_ctx.repo_id)
    issuer = CapabilityIssuer(default_ttl_ms=5 * 60 * 1000)
    engine = DefaultDenyPolicyEngine(
        issuer=issuer,
        policy=ToolAuthorizationPolicy(mode="enforce", allow_actions=allow_actions),
    )
    token = issuer.issue(
        scope=scope,
        action=action,
        constraints={
            "risk_tier": policy_ctx.risk_tier,
            "intent_id": policy_ctx.intent_id,
            "step_id": policy_ctx.step_id,
            "actor_id": policy_ctx.actor_id or "",
            "channel": policy_ctx.channel or "",
        },
    )
    decision = engine.authorize(
        req=ToolAuthorizationRequest(
            scope=scope,
            action=action,
            capability=token,
            context={
                "risk_tier": policy_ctx.risk_tier,
                "intent_id": policy_ctx.intent_id,
                "step_id": policy_ctx.step_id,
                "actor_id": policy_ctx.actor_id,
                "channel": policy_ctx.channel,
            },
        )
    )
    if decision.allowed and risk_tier == "medium" and action in MEDIUM_RISK_ACTIONS and not policy_ctx.user_has_consent:
        decision = PolicyDecision(
            allowed=False,
            reason="policy.action.user_consent_missing",
            mode="enforce",
            source="allowlist",
            block=True,
        )
    if decision.allowed and risk_tier == "high" and action in HIGH_RISK_ACTIONS and not policy_ctx.step_is_approved:
        decision = PolicyDecision(
            allowed=False,
            reason="policy.action.approval_required",
            mode="enforce",
            source="capability",
            block=True,
        )
    narrative = _narrative_for_decision(
        decision=decision,
        action=action,
        risk_tier=policy_ctx.risk_tier,
        consent_root=consent_root,
        actor_id=policy_ctx.actor_id,
    )
    row: dict[str, Any] = {
        "evaluated_at_ms": int(time.time() * 1000),
        "intent_id": policy_ctx.intent_id,
        "step_id": policy_ctx.step_id,
        "action": action,
        "risk_tier": policy_ctx.risk_tier,
        "allowed": bool(decision.allowed),
        "reason": decision.reason,
        "mode": decision.mode,
        "source": decision.source,
        "block": bool(decision.block),
        "narrative": narrative,
        "actor_id": policy_ctx.actor_id,
        "channel": policy_ctx.channel,
        "consent_present": bool(policy_ctx.user_has_consent),
        "step_approved": bool(policy_ctx.step_is_approved),
    }
    return row


def has_user_consent(*, consent_root: Path, actor_id: str | None, action: str) -> bool:
    actor = str(actor_id or "").strip()
    if not actor:
        return False
    p = consent_root / f"{actor}.json"
    if not p.exists():
        return False
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    if not isinstance(raw, dict):
        return False
    allowed = raw.get("allow_actions")
    if not isinstance(allowed, list):
        return False
    action_s = str(action).strip()
    return any(str(item).strip() == action_s for item in allowed)


def _medium_allowlist_from_env() -> frozenset[str]:
    raw = str(os.environ.get("AKC_ACTION_MEDIUM_ALLOWLIST", "") or "").strip()
    if not raw:
        return frozenset()
    out = {item.strip() for item in raw.split(",") if item.strip()}
    return frozenset(item for item in out if item in MEDIUM_RISK_ACTIONS)


def _narrative_for_decision(
    *,
    decision: PolicyDecision,
    action: str,
    risk_tier: str,
    consent_root: Path,
    actor_id: str | None,
) -> str:
    if decision.reason == "policy.action.user_consent_missing":
        actor = str(actor_id or "").strip() or "<unknown-actor>"
        return (
            f"Denied `{action}` at risk tier `{risk_tier}` because per-user consent is missing. "
            f"Create `{consent_root / (actor + '.json')}` with allow_actions including `{action}`."
        )
    if decision.reason == "policy.action.approval_required":
        return (
            f"Denied `{action}` at risk tier `{risk_tier}` because this class is irreversible/high-impact "
            "and requires explicit operator approval for the step."
        )
    return describe_policy_reason(decision.reason)
