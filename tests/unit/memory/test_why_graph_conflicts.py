from __future__ import annotations

from akc.memory.code_memory import InMemoryCodeMemoryStore
from akc.memory.models import PlanState, PlanStep, WhyNode, goal_fingerprint, now_ms
from akc.memory.why_graph import ConflictDetector, InMemoryWhyGraphStore


def test_why_graph_is_tenant_and_repo_scoped() -> None:
    g = InMemoryWhyGraphStore()
    g.upsert_nodes(
        tenant_id="t1",
        repo_id="repo",
        nodes=[
            WhyNode(
                id="c1",
                type="constraint",
                payload={
                    "subject": "x",
                    "predicate": "required",
                    "polarity": 1,
                    "scope": "repo",
                },
            )
        ],
    )
    g.upsert_nodes(
        tenant_id="t2",
        repo_id="repo",
        nodes=[
            WhyNode(
                id="c1",
                type="constraint",
                payload={
                    "subject": "x",
                    "predicate": "required",
                    "polarity": 1,
                    "scope": "repo",
                },
            )
        ],
    )
    g.upsert_nodes(
        tenant_id="t1",
        repo_id="repo2",
        nodes=[
            WhyNode(
                id="c1",
                type="constraint",
                payload={
                    "subject": "x",
                    "predicate": "required",
                    "polarity": 1,
                    "scope": "repo",
                },
            )
        ],
    )

    assert g.get_node(tenant_id="t1", repo_id="repo", node_id="c1") is not None
    assert g.get_node(tenant_id="t2", repo_id="repo", node_id="c1") is not None
    assert g.get_node(tenant_id="t1", repo_id="repo2", node_id="c1") is not None


def test_conflict_detector_surfaces_contradictory_constraints_and_stores_reports() -> None:
    nodes = [
        WhyNode(
            id="a",
            type="constraint",
            payload={
                "subject": "dependency:psycopg",
                "predicate": "required",
                "object": "psycopg[binary]",
                "polarity": 1,
                "scope": "repo",
                "provenance": [{"doc_id": "docA", "chunk_index": 0}],
                "evidence_doc_ids": ["docA"],
            },
        ),
        WhyNode(
            id="b",
            type="constraint",
            payload={
                "subject": "dependency:psycopg",
                "predicate": "required",
                "object": "psycopg[binary]",
                "polarity": -1,
                "scope": "repo",
                "provenance": [{"doc_id": "docB", "chunk_index": 1}],
                "evidence_doc_ids": ["docB"],
            },
        ),
    ]
    detector = ConflictDetector()
    reports = detector.detect_constraint_contradictions(
        tenant_id="t",
        repo_id="repo",
        nodes=nodes,
        plan_id="p",
    )
    assert reports
    assert any(r.conflict_type == "constraint_contradiction" for r in reports)
    contradiction = next(r for r in reports if r.conflict_type == "constraint_contradiction")
    assert contradiction.conflicting_provenance is not None
    assert set(contradiction.conflicting_provenance.keys()) == {"a", "b"}
    assert contradiction.evidence_doc_ids is not None
    assert set(contradiction.evidence_doc_ids) == {"docA", "docB"}

    mem = InMemoryCodeMemoryStore()
    wrote = detector.store_reports(
        tenant_id="t",
        repo_id="repo",
        plan_id="p",
        reports=reports,
        code_memory=mem,
    )
    assert wrote == len(reports)
    stored = mem.list_items(
        tenant_id="t",
        repo_id="repo",
        kind_filter=("conflict_report",),  # type: ignore[arg-type]
    )
    assert len(stored) == len(reports)


def test_conflict_detector_surfaces_plan_drift_missing_constraints() -> None:
    g = InMemoryWhyGraphStore()
    g.upsert_nodes(
        tenant_id="t",
        repo_id="repo",
        nodes=[
            WhyNode(
                id="c-present",
                type="constraint",
                payload={
                    "subject": "policy:tenant_isolation",
                    "predicate": "required",
                    "polarity": 1,
                    "scope": "repo",
                },
            )
        ],
    )

    t = now_ms()
    plan = PlanState(
        id="p1",
        tenant_id="t",
        repo_id="repo",
        goal="do x",
        status="active",  # type: ignore[arg-type]
        created_at_ms=t,
        updated_at_ms=t,
        steps=(
            PlanStep(
                id="s1",
                title="step",
                status="pending",  # type: ignore[arg-type]
                order_idx=0,
                inputs={
                    "linked_constraints": [
                        {"constraint_id": "c-present"},
                        {"constraint_id": "c-missing"},
                    ],
                    "goal_fingerprint": goal_fingerprint("do x"),
                },
                outputs={},
            ),
        ),
        next_step_id="s1",
        budgets={},
        last_feedback={},
    )

    detector = ConflictDetector()
    reports = detector.detect_plan_drift(tenant_id="t", repo_id="repo", plan=plan, why_graph=g)
    assert reports
    assert any(r.conflict_type == "plan_drift" for r in reports)

    mem = InMemoryCodeMemoryStore()
    wrote = detector.store_reports(
        tenant_id="t",
        repo_id="repo",
        plan_id=plan.id,
        reports=reports,
        code_memory=mem,
    )
    assert wrote == len(reports)


def test_conflict_detector_surfaces_plan_drift_goal_fingerprint_mismatch() -> None:
    g = InMemoryWhyGraphStore()
    t = now_ms()
    plan = PlanState(
        id="p2",
        tenant_id="t",
        repo_id="repo",
        goal="new goal",
        status="active",  # type: ignore[arg-type]
        created_at_ms=t,
        updated_at_ms=t,
        steps=(
            PlanStep(
                id="s1",
                title="step",
                status="pending",  # type: ignore[arg-type]
                order_idx=0,
                inputs={
                    "linked_constraints": [],
                    "goal_fingerprint": goal_fingerprint("old goal"),
                },
                outputs={},
            ),
        ),
        next_step_id="s1",
        budgets={},
        last_feedback={},
    )

    detector = ConflictDetector()
    reports = detector.detect_plan_drift(tenant_id="t", repo_id="repo", plan=plan, why_graph=g)
    assert any(r.conflict_type == "plan_drift" for r in reports)
