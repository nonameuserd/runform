from __future__ import annotations

from pathlib import Path

import pytest

from akc.memory.plan_state import JsonFilePlanStateStore, SQLitePlanStateStore


def test_json_plan_state_round_trip_and_active_pointer(tmp_path) -> None:  # type: ignore[no-untyped-def]
    store = JsonFilePlanStateStore(base_dir=Path(tmp_path))
    plan = store.create_plan(tenant_id="t", repo_id="repo", goal="do x", initial_steps=["a", "b"])
    assert store.get_active_plan_id(tenant_id="t", repo_id="repo") == plan.id

    loaded = store.load_plan(tenant_id="t", repo_id="repo", plan_id=plan.id)
    assert loaded is not None
    assert loaded.goal == "do x"
    assert [s.title for s in loaded.steps] == ["a", "b"]
    assert loaded.next_step_id == loaded.steps[0].id
    assert all(isinstance((s.inputs or {}).get("constraint_ids"), list) for s in loaded.steps)
    assert all(isinstance((s.inputs or {}).get("goal_fingerprint"), str) for s in loaded.steps)

    updated = store.mark_step(
        tenant_id="t", repo_id="repo", plan_id=plan.id, step_id=loaded.steps[0].id, status="done"
    )
    assert [s.status for s in updated.steps][:1] == ["done"]


def test_sqlite_plan_state_round_trip_and_isolation(tmp_path) -> None:  # type: ignore[no-untyped-def]
    db_path = tmp_path / "plan.sqlite3"
    s1 = SQLitePlanStateStore(path=str(db_path))
    p1 = s1.create_plan(tenant_id="t1", repo_id="repo", goal="g", initial_steps=["a"])
    p2 = s1.create_plan(tenant_id="t2", repo_id="repo", goal="g2", initial_steps=["b"])

    s2 = SQLitePlanStateStore(path=str(db_path))
    assert s2.get_active_plan_id(tenant_id="t1", repo_id="repo") == p1.id
    assert s2.get_active_plan_id(tenant_id="t2", repo_id="repo") == p2.id

    l1 = s2.load_plan(tenant_id="t1", repo_id="repo", plan_id=p1.id)
    l2 = s2.load_plan(tenant_id="t2", repo_id="repo", plan_id=p2.id)
    assert l1 is not None and l1.goal == "g"
    assert l2 is not None and l2.goal == "g2"


def test_plan_state_rejects_unknown_step(tmp_path) -> None:  # type: ignore[no-untyped-def]
    store = SQLitePlanStateStore(path=str(tmp_path / "x.sqlite3"))
    plan = store.create_plan(tenant_id="t", repo_id="repo", goal="g", initial_steps=["a"])
    with pytest.raises(Exception, match=r"step not found"):
        store.mark_step(
            tenant_id="t",
            repo_id="repo",
            plan_id=plan.id,
            step_id="missing",
            status="done",
        )
