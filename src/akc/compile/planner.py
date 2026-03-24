"""Planner hooks (Phase 2).

Thin compile-layer helpers that read/write plan state. Phase 3 will implement
the full plan→retrieve→generate→execute→repair controller.

Phase 3: inject read-only knowledge view fields into step ``inputs`` from the
materialized ``KnowledgeSnapshot`` (does not replace intent authority).
"""

from __future__ import annotations

import re
from dataclasses import replace
from typing import Any

from akc.knowledge.models import KnowledgeSnapshot
from akc.memory.models import (
    PlanState,
    PlanStep,
    goal_fingerprint,
    normalize_repo_id,
    now_ms,
    require_non_empty,
)
from akc.memory.plan_state import PlanStateStore

_DESTRUCTIVE_STEP_HINT = re.compile(r"(?i)\b(delete|destroy|drop|purge|wipe|remove\s+all|rm\s+-rf)\b")


def format_knowledge_summary(snapshot: KnowledgeSnapshot, *, max_chars: int = 4000) -> str:
    """Single bounded string for planner/LLM context (selected constraints only)."""

    decisions = {d.assertion_id: d for d in snapshot.canonical_decisions}
    lines: list[str] = []
    for c in sorted(snapshot.canonical_constraints, key=lambda x: x.assertion_id):
        dec = decisions.get(c.assertion_id)
        if dec is not None and not dec.selected:
            continue
        lines.append(f"[{c.kind}] {c.subject}: {c.summary}")
    text = "\n".join(lines)
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3] + "..."


def selected_knowledge_assertion_ids(snapshot: KnowledgeSnapshot) -> tuple[str, ...]:
    """Stable assertion ids for constraints that remain selected after mediation."""

    decisions = {d.assertion_id: d for d in snapshot.canonical_decisions}
    out: list[str] = []
    for c in snapshot.canonical_constraints:
        dec = decisions.get(c.assertion_id)
        if dec is not None and not dec.selected:
            continue
        out.append(c.assertion_id)
    return tuple(sorted(out))


def prior_knowledge_snapshot_from_plan(plan: PlanState, *, current_step_id: str) -> KnowledgeSnapshot | None:
    """Latest knowledge snapshot from an earlier plan step (same tenant/repo), if any."""

    require_non_empty(current_step_id, name="current_step_id")
    cur: PlanStep | None = None
    for s in plan.steps:
        if s.id == current_step_id:
            cur = s
            break
    if cur is None:
        return None
    candidates: list[tuple[int, KnowledgeSnapshot]] = []
    for s in plan.steps:
        if s.order_idx >= cur.order_idx:
            continue
        raw = (s.outputs or {}).get("knowledge_snapshot") if s.outputs else None
        if raw is None:
            continue
        if hasattr(raw, "to_json_obj") and callable(getattr(raw, "to_json_obj", None)):
            try:
                raw = raw.to_json_obj()
            except Exception:
                continue
        if not isinstance(raw, dict):
            continue
        try:
            candidates.append((int(s.order_idx), KnowledgeSnapshot.from_json_obj(raw)))
        except Exception:
            continue
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0])
    return candidates[-1][1]


def inject_knowledge_into_plan_step_inputs(*, plan: PlanState, snapshot: KnowledgeSnapshot) -> PlanState:
    """Add ``knowledge_summary`` / ``knowledge_assertion_ids`` to each step's inputs."""

    summary = format_knowledge_summary(snapshot)
    aids = list(selected_knowledge_assertion_ids(snapshot))
    steps2: list[PlanStep] = []
    for s in plan.steps:
        ins = dict(s.inputs or {})
        ins["knowledge_summary"] = summary
        ins["knowledge_assertion_ids"] = aids
        steps2.append(replace(s, inputs=ins))
    return replace(plan, steps=tuple(steps2), updated_at_ms=now_ms())


def annotate_constraint_hints_for_verifier(*, plan: PlanState, snapshot: KnowledgeSnapshot) -> PlanState:
    """When hard constraints forbid destructive operations, flag matching steps for verifier attention."""

    decisions = {d.assertion_id: d for d in snapshot.canonical_decisions}
    destructive_forbidden = False
    for c in snapshot.canonical_constraints:
        if c.kind != "hard":
            continue
        dec = decisions.get(c.assertion_id)
        if dec is not None and not dec.selected:
            continue
        pred = str(c.predicate).strip().lower()
        if pred not in {"forbidden", "must_not_use"}:
            continue
        if _DESTRUCTIVE_STEP_HINT.search(c.summary):
            destructive_forbidden = True
            break
    if not destructive_forbidden:
        return plan
    steps2: list[PlanStep] = []
    for s in plan.steps:
        title = (s.title or "").strip().lower()
        if title and _DESTRUCTIVE_STEP_HINT.search(title):
            ins = dict(s.inputs or {})
            ins["knowledge_verifier_attention"] = True
            steps2.append(replace(s, inputs=ins))
        else:
            steps2.append(s)
    return replace(plan, steps=tuple(steps2), updated_at_ms=now_ms())


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
                    inputs.setdefault("intent_id", "")
                    has_intent_ref = isinstance(inputs.get("intent_ref"), dict)
                    if not has_intent_ref:
                        inputs.setdefault("active_objectives", [])
                        inputs.setdefault("linked_constraints", [])
                        inputs.setdefault("active_success_criteria", [])
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
                if "intent_id" not in inputs:
                    inputs["intent_id"] = ""
                has_intent_ref = isinstance(inputs.get("intent_ref"), dict)
                if not has_intent_ref:
                    if "active_objectives" not in inputs:
                        inputs["active_objectives"] = []
                    if "linked_constraints" not in inputs:
                        inputs["linked_constraints"] = []
                    if "active_success_criteria" not in inputs:
                        inputs["active_success_criteria"] = []
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
