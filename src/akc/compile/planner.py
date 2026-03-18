"""Planner hooks (Phase 2).

Thin compile-layer helpers that read/write plan state. Phase 3 will implement
the full plan→retrieve→generate→execute→repair controller.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from akc.memory.models import (
    PlanState,
    goal_fingerprint,
    normalize_repo_id,
    now_ms,
    require_non_empty,
)
from akc.memory.plan_state import PlanStateStore


def create_or_resume_plan(
    *,
    tenant_id: str,
    repo_id: str,
    goal: str,
    plan_store: PlanStateStore,
) -> PlanState:
    """Create a new plan, or resume the active plan for tenant+repo."""

    require_non_empty(tenant_id, name="tenant_id")
    repo = normalize_repo_id(repo_id)
    require_non_empty(goal, name="goal")
    active_id = plan_store.get_active_plan_id(tenant_id=tenant_id, repo_id=repo)
    if active_id is not None:
        plan = plan_store.load_plan(tenant_id=tenant_id, repo_id=repo, plan_id=active_id)
        if plan is not None:
            old_fp = goal_fingerprint(plan.goal)
            new_fp = goal_fingerprint(goal)
            # If goal changed, we still resume but persist the updated goal for audit.
            if plan.goal != goal:
                steps2 = []
                for s in plan.steps:
                    inputs = dict(s.inputs or {})
                    inputs.setdefault("constraint_ids", [])
                    inputs.setdefault("goal_fingerprint", old_fp)
                    steps2.append(replace(s, inputs=inputs))
                plan2 = replace(plan, goal=goal, steps=tuple(steps2), updated_at_ms=now_ms())
                plan_store.save_plan(tenant_id=tenant_id, repo_id=repo, plan=plan2)
                return plan2

            # Ensure a stable step input shape even when goal hasn't changed.
            steps2 = []
            changed = False
            for s in plan.steps:
                inputs = dict(s.inputs or {})
                if "constraint_ids" not in inputs:
                    inputs["constraint_ids"] = []
                    changed = True
                if "goal_fingerprint" not in inputs:
                    inputs["goal_fingerprint"] = new_fp
                    changed = True
                steps2.append(replace(s, inputs=inputs))
            if changed:
                plan2 = replace(plan, steps=tuple(steps2), updated_at_ms=now_ms())
                plan_store.save_plan(tenant_id=tenant_id, repo_id=repo, plan=plan2)
                return plan2
            return plan

    plan = plan_store.create_plan(tenant_id=tenant_id, repo_id=repo, goal=goal, initial_steps=None)
    plan_store.set_active_plan(tenant_id=tenant_id, repo_id=repo, plan_id=plan.id)
    return plan


def advance_plan(
    *,
    tenant_id: str,
    repo_id: str,
    plan_id: str,
    plan_store: PlanStateStore,
    feedback: dict[str, Any] | None,
) -> PlanState:
    """Persist feedback and choose a deterministic next_step_id.

    Phase 2 policy: next step is the first `pending` step by order, otherwise None.
    """

    require_non_empty(tenant_id, name="tenant_id")
    repo = normalize_repo_id(repo_id)
    require_non_empty(plan_id, name="plan_id")
    plan = plan_store.load_plan(tenant_id=tenant_id, repo_id=repo, plan_id=plan_id)
    if plan is None:
        raise ValueError("plan not found")
    t = now_ms()
    plan2 = replace(plan, last_feedback=dict(feedback or {}), updated_at_ms=t)
    next_id: str | None = None
    for s in sorted(plan2.steps, key=lambda x: x.order_idx):
        if s.status == "pending":
            next_id = s.id
            break
    plan2 = replace(plan2, next_step_id=next_id, updated_at_ms=t)
    plan_store.save_plan(tenant_id=tenant_id, repo_id=repo, plan=plan2)
    return plan2
