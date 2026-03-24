"""Planner knowledge injection (Phase 3)."""

from __future__ import annotations

from akc.compile.planner import (
    format_knowledge_summary,
    inject_knowledge_into_plan_step_inputs,
    prior_knowledge_snapshot_from_plan,
)
from akc.knowledge.models import CanonicalConstraint, CanonicalDecision, EvidenceMapping, KnowledgeSnapshot
from akc.memory.models import PlanState, PlanStep, now_ms


def _minimal_snapshot(*, summary: str = "rule") -> KnowledgeSnapshot:
    c = CanonicalConstraint(
        subject="s",
        predicate="forbidden",
        object=None,
        polarity=1,
        scope="repo",
        kind="hard",
        summary=summary,
    )
    d = CanonicalDecision(assertion_id=c.assertion_id, selected=True, resolved=True)
    ev = EvidenceMapping(evidence_doc_ids=("d1",), resolved_provenance_pointers=())
    return KnowledgeSnapshot(
        canonical_constraints=(c,),
        canonical_decisions=(d,),
        evidence_by_assertion={c.assertion_id: ev},
    )


def test_inject_knowledge_into_plan_step_inputs_adds_summary_and_ids() -> None:
    t = now_ms()
    snap = _minimal_snapshot(summary="no deletes")
    plan = PlanState(
        id="p1",
        tenant_id="t",
        repo_id="r",
        goal="g",
        status="active",
        created_at_ms=t,
        updated_at_ms=t,
        steps=(
            PlanStep(
                id="s1",
                title="step",
                status="pending",
                order_idx=0,
                inputs={"intent_id": "i1"},
            ),
        ),
        next_step_id="s1",
    )
    out = inject_knowledge_into_plan_step_inputs(plan=plan, snapshot=snap)
    ins = out.steps[0].inputs or {}
    assert "knowledge_summary" in ins
    assert "no deletes" in str(ins["knowledge_summary"])
    assert ins.get("knowledge_assertion_ids") == [snap.canonical_constraints[0].assertion_id]


def test_format_knowledge_summary_non_empty() -> None:
    s = format_knowledge_summary(_minimal_snapshot())
    assert "hard" in s
    assert s.strip()


def test_prior_knowledge_snapshot_from_earlier_step() -> None:
    t = now_ms()
    snap = _minimal_snapshot(summary="prior")
    plan = PlanState(
        id="p1",
        tenant_id="t",
        repo_id="r",
        goal="g",
        status="active",
        created_at_ms=t,
        updated_at_ms=t,
        steps=(
            PlanStep(
                id="s0",
                title="a",
                status="done",
                order_idx=0,
                inputs={},
                outputs={"knowledge_snapshot": snap.to_json_obj()},
            ),
            PlanStep(
                id="s1",
                title="b",
                status="pending",
                order_idx=1,
                inputs={},
            ),
        ),
        next_step_id="s1",
    )
    got = prior_knowledge_snapshot_from_plan(plan, current_step_id="s1")
    assert got is not None
    assert got.canonical_constraints[0].summary == "prior"
