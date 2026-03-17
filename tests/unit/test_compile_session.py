from __future__ import annotations

from pathlib import Path

from akc.compile import CompileSession
from akc.memory.facade import build_memory


def test_compile_session_plan_and_retrieve_smoke() -> None:
    mem = build_memory(backend="memory")
    s = CompileSession(tenant_id="t1", repo_id="repo1", memory=mem)
    plan = s.plan(goal="Do the thing")
    ctx = s.retrieve(plan=plan, limit=10)

    assert plan.tenant_id == "t1"
    assert plan.repo_id == "repo1"
    assert isinstance(ctx, dict)
    assert "code_memory_items" in ctx
    assert "documents" in ctx
    assert "why_graph" in ctx


def test_compile_session_from_constructors(tmp_path: Path) -> None:
    s1 = CompileSession.from_memory(tenant_id="t1", repo_id="repo1")
    p1 = s1.plan(goal="goal")
    assert p1.repo_id == "repo1"

    db = tmp_path / "mem.sqlite"
    s2 = CompileSession.from_sqlite(tenant_id="t1", repo_id="repo1", sqlite_path=str(db))
    p2 = s2.plan(goal="goal2")
    assert p2.repo_id == "repo1"

    s3 = CompileSession.from_backend(tenant_id="t1", repo_id="repo1", backend="memory")
    p3 = s3.plan(goal="goal3")
    assert p3.repo_id == "repo1"

