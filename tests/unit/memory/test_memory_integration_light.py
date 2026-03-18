from __future__ import annotations

from pathlib import Path

from akc.memory.facade import build_memory
from akc.memory.models import WhyNode
from akc.memory.why_graph import ConflictDetector


def _seed_contradictory_constraints(*, tenant_id: str, repo_id: str) -> list[WhyNode]:
    return [
        WhyNode(
            id="c-required",
            type="constraint",
            payload={
                "subject": "dependency:psycopg",
                "predicate": "required",
                "object": "psycopg[binary]",
                "polarity": 1,
                "scope": "repo",
                "source": {"kind": "user"},
            },
        ),
        WhyNode(
            id="c-forbidden",
            type="constraint",
            payload={
                "subject": "dependency:psycopg",
                "predicate": "required",
                "object": "psycopg[binary]",
                "polarity": -1,
                "scope": "repo",
                "source": {"kind": "user"},
            },
        ),
    ]


def _assert_end_to_end_conflict_flow(*, backend: str, sqlite_path: str | None = None) -> None:
    mem = build_memory(backend=backend, sqlite_path=sqlite_path)

    # Plan creation (so we can attach plan_id to stored reports).
    plan = mem.plan_state.create_plan(
        tenant_id="t",
        repo_id="repo",
        goal="Add dependency policy",
        initial_steps=["collect constraints", "detect conflicts"],
    )

    nodes = _seed_contradictory_constraints(tenant_id="t", repo_id="repo")
    mem.why_graph.upsert_nodes(tenant_id="t", repo_id="repo", nodes=nodes)

    detector = ConflictDetector()
    constraints = mem.why_graph.list_nodes_by_type(
        tenant_id="t",
        repo_id="repo",
        node_type="constraint",
    )
    reports = detector.detect_constraint_contradictions(
        tenant_id="t",
        repo_id="repo",
        nodes=constraints,
        plan_id=plan.id,
    )
    assert reports

    wrote = detector.store_reports(
        tenant_id="t",
        repo_id="repo",
        plan_id=plan.id,
        reports=reports,
        code_memory=mem.code_memory,
    )
    assert wrote == len(reports)

    stored = mem.code_memory.list_items(
        tenant_id="t",
        repo_id="repo",
        kind_filter=("conflict_report",),  # type: ignore[arg-type]
        limit=100,
    )
    assert len(stored) == len(reports)
    assert all(i.metadata.get("plan_id") == plan.id for i in stored)


def test_integration_light_conflict_flow_in_memory() -> None:
    _assert_end_to_end_conflict_flow(backend="memory")


def test_integration_light_conflict_flow_sqlite(tmp_path: Path) -> None:
    _assert_end_to_end_conflict_flow(backend="sqlite", sqlite_path=str(tmp_path / "phase2.sqlite3"))
