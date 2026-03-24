from __future__ import annotations

import sqlite3
from pathlib import Path

from akc.control.cost_index import CostIndex, RunCostRecord


def test_cost_index_upsert_and_tenant_totals(tmp_path: Path) -> None:
    db = tmp_path / "metrics.sqlite"
    idx = CostIndex(sqlite_path=db)

    idx.upsert_run_cost(
        record=RunCostRecord(
            tenant_id="t1",
            repo_id="repo-a",
            run_id="run-1",
            llm_calls=2,
            tool_calls=3,
            input_tokens=100,
            output_tokens=50,
            total_tokens=150,
            wall_time_ms=20,
            estimated_cost_usd=0.12,
            pricing_version="prices-2026-03-20",
            cost_breakdown={"by_pass": {"generate": {"llm_calls": 2}}},
        )
    )
    idx.upsert_run_cost(
        record=RunCostRecord(
            tenant_id="t1",
            repo_id="repo-b",
            run_id="run-2",
            llm_calls=1,
            tool_calls=2,
            input_tokens=200,
            output_tokens=10,
            total_tokens=210,
            wall_time_ms=15,
            estimated_cost_usd=0.08,
        )
    )

    totals = idx.tenant_totals(tenant_id="t1")
    assert totals["runs_count"] == 2
    assert totals["llm_calls"] == 3
    assert totals["tool_calls"] == 5
    assert totals["total_tokens"] == 360
    assert float(totals["estimated_cost_usd"]) == 0.2


def test_cost_index_upsert_replaces_same_run_key(tmp_path: Path) -> None:
    db = tmp_path / "metrics.sqlite"
    idx = CostIndex(sqlite_path=db)
    key = {"tenant_id": "t1", "repo_id": "repo-a", "run_id": "run-1"}

    idx.upsert_run_cost(
        record=RunCostRecord(
            **key,
            llm_calls=1,
            tool_calls=1,
            input_tokens=10,
            output_tokens=10,
            total_tokens=20,
            wall_time_ms=5,
            estimated_cost_usd=0.01,
        )
    )
    idx.upsert_run_cost(
        record=RunCostRecord(
            **key,
            llm_calls=4,
            tool_calls=2,
            input_tokens=40,
            output_tokens=20,
            total_tokens=60,
            wall_time_ms=10,
            estimated_cost_usd=0.03,
        )
    )

    totals = idx.tenant_totals(tenant_id="t1")
    assert totals["runs_count"] == 1
    assert totals["llm_calls"] == 4
    assert totals["tool_calls"] == 2
    assert totals["total_tokens"] == 60


def test_cost_index_list_runs_orders_by_latest_upsert(tmp_path: Path, monkeypatch) -> None:
    # Ensure ordering is based on latest `upsert_run_cost` time rather than SQLite rowid.
    import akc.control.cost_index as cost_index_mod

    db = tmp_path / "metrics.sqlite"
    idx = CostIndex(sqlite_path=db)

    times = iter([1.0, 1.5, 2.0])
    monkeypatch.setattr(cost_index_mod.time, "time", lambda: next(times))

    tenant_id = "t1"
    repo_id = "repo-a"

    idx.upsert_run_cost(
        record=RunCostRecord(
            tenant_id=tenant_id,
            repo_id=repo_id,
            run_id="run-1",
            llm_calls=1,
            tool_calls=0,
            input_tokens=10,
            output_tokens=0,
            total_tokens=10,
            wall_time_ms=1,
            estimated_cost_usd=0.01,
        )
    )
    idx.upsert_run_cost(
        record=RunCostRecord(
            tenant_id=tenant_id,
            repo_id=repo_id,
            run_id="run-2",
            llm_calls=1,
            tool_calls=0,
            input_tokens=10,
            output_tokens=0,
            total_tokens=10,
            wall_time_ms=1,
            estimated_cost_usd=0.01,
        )
    )
    # Update run-1 (same PK) at a later time; it should become the "most recent run".
    idx.upsert_run_cost(
        record=RunCostRecord(
            tenant_id=tenant_id,
            repo_id=repo_id,
            run_id="run-1",
            llm_calls=2,
            tool_calls=0,
            input_tokens=20,
            output_tokens=0,
            total_tokens=20,
            wall_time_ms=2,
            estimated_cost_usd=0.02,
        )
    )

    runs = idx.list_runs(tenant_id=tenant_id, repo_id=repo_id, limit=2)
    assert runs[0]["run_id"] == "run-1"
    assert runs[1]["run_id"] == "run-2"
    assert runs[0]["pricing_version"] is None
    assert runs[0]["cost_breakdown"] == {}


def test_cost_index_migrates_legacy_schema_with_breakdown_columns(tmp_path: Path) -> None:
    db = tmp_path / "metrics.sqlite"
    with sqlite3.connect(str(db)) as conn:
        conn.execute(
            """
            CREATE TABLE run_costs (
                tenant_id TEXT NOT NULL,
                repo_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                llm_calls INTEGER NOT NULL,
                tool_calls INTEGER NOT NULL,
                input_tokens INTEGER NOT NULL,
                output_tokens INTEGER NOT NULL,
                total_tokens INTEGER NOT NULL,
                wall_time_ms INTEGER NOT NULL,
                estimated_cost_usd REAL NOT NULL,
                PRIMARY KEY (tenant_id, repo_id, run_id)
            )
            """
        )

    idx = CostIndex(sqlite_path=db)
    idx.upsert_run_cost(
        record=RunCostRecord(
            tenant_id="t1",
            repo_id="repo-a",
            run_id="run-legacy",
            llm_calls=1,
            tool_calls=1,
            input_tokens=10,
            output_tokens=5,
            total_tokens=15,
            wall_time_ms=2,
            estimated_cost_usd=0.02,
            pricing_version="prices-2026-03-20",
            cost_breakdown={"currency": "USD", "by_component": {"controller": {"llm_calls": 1}}},
        )
    )

    runs = idx.list_runs(tenant_id="t1", repo_id="repo-a", limit=1)
    assert runs[0]["pricing_version"] == "prices-2026-03-20"
    assert runs[0]["cost_breakdown"] == {
        "currency": "USD",
        "by_component": {"controller": {"llm_calls": 1}},
    }
