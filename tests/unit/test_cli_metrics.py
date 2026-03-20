from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.cli import main
from akc.control.cost_index import CostIndex, RunCostRecord


def _seed_metrics(outputs_root: Path) -> None:
    db = outputs_root / "t1" / ".akc" / "control" / "metrics.sqlite"
    idx = CostIndex(sqlite_path=db)
    idx.upsert_run_cost(
        record=RunCostRecord(
            tenant_id="t1",
            repo_id="repo-a",
            run_id="run-1",
            llm_calls=2,
            tool_calls=1,
            input_tokens=100,
            output_tokens=50,
            total_tokens=150,
            wall_time_ms=25,
            estimated_cost_usd=0.12,
        )
    )
    idx.upsert_run_cost(
        record=RunCostRecord(
            tenant_id="t1",
            repo_id="repo-b",
            run_id="run-2",
            llm_calls=1,
            tool_calls=2,
            input_tokens=40,
            output_tokens=20,
            total_tokens=60,
            wall_time_ms=10,
            estimated_cost_usd=0.05,
        )
    )


def test_cli_metrics_json_outputs_rollup(
    capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    _seed_metrics(tmp_path)
    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "metrics",
                "--tenant-id",
                "t1",
                "--outputs-root",
                str(tmp_path),
                "--format",
                "json",
                "--limit",
                "10",
            ]
        )
    assert excinfo.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["tenant_id"] == "t1"
    assert payload["repo_id"] is None
    assert payload["totals"]["runs_count"] == 2
    assert payload["totals"]["total_tokens"] == 210
    assert len(payload["runs"]) == 2


def test_cli_metrics_repo_filter_scopes_totals(
    capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    _seed_metrics(tmp_path)
    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "metrics",
                "--tenant-id",
                "t1",
                "--repo-id",
                "repo-a",
                "--outputs-root",
                str(tmp_path),
                "--format",
                "json",
            ]
        )
    assert excinfo.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["repo_id"] == "repo-a"
    assert payload["totals"]["runs_count"] == 1
    assert payload["totals"]["total_tokens"] == 150
    assert all(r["repo_id"] == "repo-a" for r in payload["runs"])
