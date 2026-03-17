"""Conflict surfacing utilities for the Phase 2 why-graph."""

from __future__ import annotations

from collections.abc import Iterable

from akc.memory.code_memory import CodeMemoryStore, make_item
from akc.memory.models import (
    CodeMemoryItem,
    ConflictReport,
    PlanState,
    WhyNode,
    goal_fingerprint,
    new_uuid,
    normalize_repo_id,
    now_ms,
    require_non_empty,
)
from akc.memory.why_graph_store import ConstraintKey, WhyGraphStore

_MUTEX: dict[str, set[str]] = {
    "required": {"forbidden"},
    "forbidden": {"required"},
    "allowed": {"must_not_use"},
    "must_use": {"must_not_use"},
    "must_not_use": {"must_use", "allowed"},
}


def _constraint_key(node: WhyNode) -> ConstraintKey | None:
    if node.type != "constraint":
        return None
    p = node.payload
    subject = p.get("subject")
    predicate = p.get("predicate")
    obj = p.get("object")
    polarity = p.get("polarity")
    scope = p.get("scope", "repo")
    if not isinstance(subject, str) or not subject.strip():
        return None
    if not isinstance(predicate, str) or not predicate.strip():
        return None
    if obj is not None and not isinstance(obj, str):
        return None
    if not isinstance(polarity, int) or polarity not in {-1, 1}:
        return None
    if not isinstance(scope, str) or not scope.strip():
        return None
    return ConstraintKey(
        subject=subject.strip(),
        predicate=predicate.strip(),
        object=obj.strip() if isinstance(obj, str) else None,
        polarity=int(polarity),
        scope=scope.strip(),
    )


class ConflictDetector:
    """First-pass conflict surfacing for why-graph constraints."""

    def detect_constraint_contradictions(
        self, *, tenant_id: str, repo_id: str, nodes: Iterable[WhyNode], plan_id: str | None = None
    ) -> list[ConflictReport]:
        require_non_empty(tenant_id, name="tenant_id")
        repo = normalize_repo_id(repo_id)
        seen: dict[tuple[str, str, str | None, str], dict[int, list[str]]] = {}
        predicate_seen: dict[tuple[str, str], list[str]] = {}

        for n in nodes:
            ck = _constraint_key(n)
            if ck is None:
                continue
            k = (ck.subject, ck.predicate, ck.object, ck.scope)
            bucket = seen.setdefault(k, {})
            bucket.setdefault(ck.polarity, []).append(n.id)

            # Secondary check: mutex predicates for same subject+scope.
            predicate_seen.setdefault((ck.subject, ck.scope), []).append(ck.predicate)

        reports: list[ConflictReport] = []
        t = now_ms()
        for (subject, predicate, obj, scope), by_pol in seen.items():
            if 1 in by_pol and -1 in by_pol:
                ids = tuple(by_pol[1] + by_pol[-1])
                summary = (
                    "Contradictory constraint detected: "
                    f"subject={subject!r} predicate={predicate!r} object={obj!r} scope={scope!r}"
                )
                reports.append(
                    ConflictReport(
                        conflict_id=new_uuid(),
                        detected_at_ms=t,
                        severity="high",
                        repo_id=repo,
                        plan_id=plan_id,
                        conflict_type="constraint_contradiction",
                        entities=ids,
                        summary=summary,
                        suggested_actions=(
                            "Identify the authoritative source for this constraint.",
                            "Remove or supersede the incorrect constraint.",
                        ),
                    )
                )

        # Mutex predicate conflicts.
        for (subject, scope), predicates in predicate_seen.items():
            preds = set(predicates)
            for p in list(preds):
                for q in _MUTEX.get(p, set()):
                    if q in preds:
                        summary = (
                            "Mutually exclusive predicates detected for subject "
                            f"{subject!r} scope={scope!r}: {p!r} vs {q!r}"
                        )
                        reports.append(
                            ConflictReport(
                                conflict_id=new_uuid(),
                                detected_at_ms=t,
                                severity="med",
                                repo_id=repo,
                                plan_id=plan_id,
                                conflict_type="constraint_contradiction",
                                entities=(),
                                summary=summary,
                                suggested_actions=(
                                    "Clarify whether the subject is required/forbidden or "
                                    "must_use/must_not_use.",
                                ),
                            )
                        )
        return reports

    def detect_plan_drift(
        self,
        *,
        tenant_id: str,
        repo_id: str,
        plan: PlanState,
        why_graph: WhyGraphStore,
    ) -> list[ConflictReport]:
        """Surface basic plan drift signals (Phase 2)."""

        require_non_empty(tenant_id, name="tenant_id")
        repo = normalize_repo_id(repo_id)
        if plan.tenant_id != tenant_id or normalize_repo_id(plan.repo_id) != repo:
            raise ValueError("tenant_id/repo_id mismatch between arguments and plan")

        if plan.next_step_id is None:
            return []

        step = next((s for s in plan.steps if s.id == plan.next_step_id), None)
        if step is None:
            return [
                ConflictReport(
                    conflict_id=new_uuid(),
                    detected_at_ms=now_ms(),
                    severity="high",
                    repo_id=repo,
                    plan_id=plan.id,
                    conflict_type="plan_drift",
                    entities=(plan.next_step_id,),
                    summary="Plan next_step_id does not exist in plan.steps",
                    suggested_actions=(
                        "Recompute next_step_id from plan steps.",
                        "If the plan was edited, ensure step IDs are consistent.",
                    ),
                )
            ]

        inputs = step.inputs or {}
        expected_fp = goal_fingerprint(plan.goal)
        step_fp = inputs.get("goal_fingerprint")
        reports: list[ConflictReport] = []
        t = now_ms()

        if isinstance(step_fp, str) and step_fp.strip() and step_fp != expected_fp:
            reports.append(
                ConflictReport(
                    conflict_id=new_uuid(),
                    detected_at_ms=t,
                    severity="high",
                    repo_id=repo,
                    plan_id=plan.id,
                    conflict_type="plan_drift",
                    entities=(step.id,),
                    summary=(
                        "Plan step appears authored under a different goal (fingerprint mismatch): "
                        f"step_id={step.id!r}"
                    ),
                    suggested_actions=(
                        "Regenerate or revalidate this step against the current goal.",
                        "Update the step inputs to the current goal_fingerprint if still "
                        "applicable.",
                    ),
                )
            )

        raw_ids = inputs.get("constraint_ids")
        if raw_ids is None or not isinstance(raw_ids, list):
            return reports

        constraint_ids: list[str] = []
        for x in raw_ids:
            if isinstance(x, str) and x.strip():
                constraint_ids.append(x.strip())

        if not constraint_ids:
            return reports

        missing: list[str] = []
        goal_mismatch: list[str] = []
        for cid in constraint_ids:
            node = why_graph.get_node(tenant_id=tenant_id, repo_id=repo, node_id=cid)
            if node is None:
                missing.append(cid)
                continue
            if node.type != "constraint":
                continue
            source = node.payload.get("source")
            if not isinstance(source, dict):
                continue
            src_goal = source.get("goal") if "goal" in source else source.get("plan_goal")
            if isinstance(src_goal, str) and src_goal.strip() and src_goal != plan.goal:
                goal_mismatch.append(cid)

        if missing:
            reports.append(
                ConflictReport(
                    conflict_id=new_uuid(),
                    detected_at_ms=t,
                    severity="med",
                    repo_id=repo,
                    plan_id=plan.id,
                    conflict_type="plan_drift",
                    entities=tuple([step.id] + missing),
                    summary=(
                        "Plan step references constraint nodes missing from why-graph: "
                        f"step_id={step.id!r} missing={missing!r}"
                    ),
                    suggested_actions=(
                        "Re-run retrieval to rebuild constraints for this repo.",
                        "Remove or update stale constraint_ids in the plan step inputs.",
                    ),
                )
            )
        if goal_mismatch:
            reports.append(
                ConflictReport(
                    conflict_id=new_uuid(),
                    detected_at_ms=t,
                    severity="high",
                    repo_id=repo,
                    plan_id=plan.id,
                    conflict_type="plan_drift",
                    entities=tuple([step.id] + goal_mismatch),
                    summary=(
                        "Plan step references constraints sourced from a different goal: "
                        f"step_id={step.id!r} constraint_ids={goal_mismatch!r}"
                    ),
                    suggested_actions=(
                        "Confirm the current goal and supersede outdated constraints.",
                        "Regenerate the plan step with updated constraints.",
                    ),
                )
            )
        return reports

    def store_reports(
        self,
        *,
        tenant_id: str,
        repo_id: str,
        plan_id: str | None,
        reports: Iterable[ConflictReport],
        code_memory: CodeMemoryStore,
    ) -> int:
        repo = normalize_repo_id(repo_id)
        items: list[CodeMemoryItem] = []
        for r in reports:
            items.append(
                make_item(
                    tenant_id=tenant_id,
                    repo_id=repo,
                    artifact_id=None,
                    item_id=r.conflict_id,
                    kind="conflict_report",
                    content=r.summary,
                    metadata={
                        "report": r.to_json_obj(),
                        "plan_id": plan_id,
                    },
                    created_at_ms=r.detected_at_ms,
                    updated_at_ms=r.detected_at_ms,
                )
            )
        return code_memory.upsert_items(
            tenant_id=tenant_id,
            repo_id=repo,
            artifact_id=None,
            items=items,
        )
