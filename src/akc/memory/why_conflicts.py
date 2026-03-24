"""Conflict surfacing utilities for the Phase 2 why-graph."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import replace
from typing import Any, cast

from akc.memory.code_memory import CodeMemoryStore, make_item
from akc.memory.models import (
    CodeMemoryItem,
    ConflictReport,
    PlanState,
    ProvenancePointerJson,
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
        # For mutex conflicts we need to know which node IDs expressed each predicate.
        predicate_seen: dict[tuple[str, str], dict[str, list[str]]] = {}
        node_by_id: dict[str, WhyNode] = {}

        for n in nodes:
            ck = _constraint_key(n)
            if ck is None:
                continue
            node_by_id[n.id] = n
            k = (ck.subject, ck.predicate, ck.object, ck.scope)
            bucket = seen.setdefault(k, {})
            bucket.setdefault(ck.polarity, []).append(n.id)

            # Secondary check: mutex predicates for same subject+scope.
            predicate_seen.setdefault((ck.subject, ck.scope), {}).setdefault(ck.predicate, []).append(n.id)

        def _enrich_with_provenance(
            *, constraint_ids: Iterable[str]
        ) -> tuple[dict[str, list[ProvenancePointerJson]] | None, tuple[str, ...] | None]:
            conflicting_provenance: dict[str, list[ProvenancePointerJson]] = {}
            evidence_doc_ids: set[str] = set()

            for cid in constraint_ids:
                node = node_by_id.get(cid)
                if node is None:
                    continue

                prov_raw = node.payload.get("provenance")
                if isinstance(prov_raw, (list, tuple)) and prov_raw:
                    ptrs: list[ProvenancePointerJson] = []
                    for p in prov_raw:
                        if isinstance(p, dict):
                            ptrs.append(cast(ProvenancePointerJson, p))
                    if ptrs:
                        conflicting_provenance[cid] = ptrs

                evidence_raw = node.payload.get("evidence_doc_ids")
                if isinstance(evidence_raw, (list, tuple)):
                    for d in evidence_raw:
                        if isinstance(d, str) and d.strip():
                            evidence_doc_ids.add(d.strip())

            return (
                conflicting_provenance or None,
                tuple(sorted(evidence_doc_ids)) if evidence_doc_ids else None,
            )

        reports: list[ConflictReport] = []
        t = now_ms()
        for (subject, predicate, obj, scope), by_pol in seen.items():
            if 1 in by_pol and -1 in by_pol:
                ids = tuple(by_pol[1] + by_pol[-1])
                conflicting_provenance, evidence_doc_ids = _enrich_with_provenance(constraint_ids=ids)
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
                        conflicting_provenance=conflicting_provenance,
                        evidence_doc_ids=evidence_doc_ids,
                        participant_assertion_ids=tuple(ids),
                        suggested_actions=(
                            "Identify the authoritative source for this constraint.",
                            "Remove or supersede the incorrect constraint.",
                        ),
                    )
                )

        # Mutex predicate conflicts.
        for (subject, scope), predicates in predicate_seen.items():
            preds = set(predicates.keys())
            for p in list(preds):
                for q in _MUTEX.get(p, set()):
                    if q in preds:
                        ids = tuple(predicates[p] + predicates[q])
                        conflicting_provenance, evidence_doc_ids = _enrich_with_provenance(constraint_ids=ids)
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
                                entities=ids,
                                summary=summary,
                                conflicting_provenance=conflicting_provenance,
                                evidence_doc_ids=evidence_doc_ids,
                                participant_assertion_ids=tuple(ids),
                                suggested_actions=(
                                    "Clarify whether the subject is required/forbidden or must_use/must_not_use.",
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
        intent_store: Any | None = None,
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
                        f"Plan step appears authored under a different goal (fingerprint mismatch): step_id={step.id!r}"
                    ),
                    suggested_actions=(
                        "Regenerate or revalidate this step against the current goal.",
                        "Update the step inputs to the current goal_fingerprint if still applicable.",
                    ),
                )
            )

        # Phase 3: constraint visibility uses typed links stored in
        # `linked_constraints` (each entry is expected to include `constraint_id`).
        # Keep a compatibility fallback for older persisted plans that used
        # `constraint_ids`.
        raw_links = inputs.get("linked_constraints")
        constraint_ids: list[str] = []
        if raw_links is not None and isinstance(raw_links, list):
            for x in raw_links:
                if not isinstance(x, dict):
                    continue
                cid = x.get("constraint_id")
                if isinstance(cid, str) and cid.strip():
                    constraint_ids.append(cid.strip())
        elif (raw_ids := inputs.get("constraint_ids")) is not None and isinstance(raw_ids, list):
            for x in raw_ids:
                if isinstance(x, str) and x.strip():
                    constraint_ids.append(x.strip())
        elif intent_store is not None:
            from akc.intent.plan_step_intent import load_intent_verified_from_plan_step_inputs

            loaded = load_intent_verified_from_plan_step_inputs(
                tenant_id=tenant_id,
                repo_id=repo,
                inputs=inputs,
                intent_store=intent_store,
            )
            if loaded is not None:
                for c in loaded.constraints:
                    cid = getattr(c, "id", None)
                    if isinstance(cid, str) and cid.strip():
                        constraint_ids.append(cid.strip())
            if not constraint_ids:
                return reports
        else:
            return reports

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
                        "Remove or update stale linked_constraints in the plan step inputs.",
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
                        f"step_id={step.id!r} linked_constraint_ids={goal_mismatch!r}"
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


def enrich_conflict_reports_from_mediation(
    reports: Iterable[ConflictReport],
    *,
    mediation_report: Mapping[str, Any],
) -> list[ConflictReport]:
    """Attach ``mediation_rule`` / intent ids when mediation events overlap report participants."""

    events = mediation_report.get("events") if isinstance(mediation_report, Mapping) else None
    if not isinstance(events, list) or not events:
        return list(reports)
    out: list[ConflictReport] = []
    for r in reports:
        if r.conflict_type != "constraint_contradiction":
            out.append(r)
            continue
        e = set(r.entities)
        rule: str | None = r.mediation_rule
        ics: tuple[str, ...] | None = r.intent_constraint_ids
        for ev in events:
            if not isinstance(ev, dict):
                continue
            pids = ev.get("participant_assertion_ids")
            if not isinstance(pids, list):
                continue
            pset = {str(x).strip() for x in pids if isinstance(x, str) and x.strip()}
            if not pset.intersection(e):
                continue
            raw_rule = ev.get("resolution_rule")
            if isinstance(raw_rule, str) and raw_rule.strip():
                rule = raw_rule.strip()
            raw_ic = ev.get("intent_constraint_ids")
            if isinstance(raw_ic, list):
                ics = tuple(sorted({str(x).strip() for x in raw_ic if isinstance(x, str) and x.strip()}))
            break
        out.append(
            replace(
                r,
                participant_assertion_ids=r.participant_assertion_ids or tuple(r.entities),
                mediation_rule=rule,
                intent_constraint_ids=ics,
            )
        )
    return out
