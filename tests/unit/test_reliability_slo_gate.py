from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


def _load_module():
    script_path = Path(__file__).resolve().parents[2] / "scripts" / "check_reliability_slo_gate.py"
    spec = importlib.util.spec_from_file_location("check_reliability_slo_gate", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def _write_scoreboard(
    *,
    outputs_root: Path,
    tenant_id: str,
    repo_id: str,
    window_start_ms: int,
    window_end_ms: int,
    policy_compliance_rate: float,
    rollbacks_total: int,
) -> None:
    path = (
        outputs_root
        / tenant_id
        / repo_id
        / ".akc"
        / "autopilot"
        / "scoreboards"
        / f"{window_start_ms}-{window_end_ms}.reliability_scoreboard.v1.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "schema_id": "akc:reliability_scoreboard:v1",
        "tenant_id": tenant_id,
        "repo_id": repo_id,
        "window_start_ms": window_start_ms,
        "window_end_ms": window_end_ms,
        "kpi": {
            "policy_compliance_rate": policy_compliance_rate,
            "rollouts_total": 1,
            "rollouts_with_rollback": 1 if rollbacks_total > 0 else 0,
            "rollbacks_total": rollbacks_total,
            "convergence_latency_ms_avg": 1000.0,
            "mttr_like_repair_latency_ms_avg": 1000.0,
            "failed_promotions_prevented": 0,
        },
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def test_slo_gate_passes_for_two_consecutive_windows(tmp_path: Path) -> None:
    mod = _load_module()
    outputs_root = tmp_path / "out"
    targets_path = tmp_path / "targets.json"
    targets_path.write_text(
        json.dumps(
            {
                "window": {"required_consecutive_windows": 2},
                "kpi_targets": {
                    "policy_compliance_rate": {"gte": 0.95},
                    "rollbacks_total": {"lte": 0},
                },
            }
        ),
        encoding="utf-8",
    )
    _write_scoreboard(
        outputs_root=outputs_root,
        tenant_id="t1",
        repo_id="r1",
        window_start_ms=1000,
        window_end_ms=2000,
        policy_compliance_rate=1.0,
        rollbacks_total=0,
    )
    _write_scoreboard(
        outputs_root=outputs_root,
        tenant_id="t1",
        repo_id="r1",
        window_start_ms=2000,
        window_end_ms=3000,
        policy_compliance_rate=0.99,
        rollbacks_total=0,
    )

    ok, report = mod.check_reliability_slo_gate(
        outputs_root=outputs_root,
        targets_path=targets_path,
        tenant_id="t1",
        repo_id="r1",
    )
    assert ok is True
    assert report["passed"] is True
    assert report["failures"] == []


def test_slo_gate_fails_when_one_of_last_two_windows_violates_target(tmp_path: Path) -> None:
    mod = _load_module()
    outputs_root = tmp_path / "out"
    targets_path = tmp_path / "targets.json"
    targets_path.write_text(
        json.dumps(
            {
                "window": {"required_consecutive_windows": 2},
                "kpi_targets": {
                    "policy_compliance_rate": {"gte": 0.95},
                    "rollbacks_total": {"lte": 0},
                },
            }
        ),
        encoding="utf-8",
    )
    _write_scoreboard(
        outputs_root=outputs_root,
        tenant_id="t1",
        repo_id="r1",
        window_start_ms=1000,
        window_end_ms=2000,
        policy_compliance_rate=1.0,
        rollbacks_total=0,
    )
    _write_scoreboard(
        outputs_root=outputs_root,
        tenant_id="t1",
        repo_id="r1",
        window_start_ms=2000,
        window_end_ms=3000,
        policy_compliance_rate=0.90,
        rollbacks_total=1,
    )

    ok, report = mod.check_reliability_slo_gate(
        outputs_root=outputs_root,
        targets_path=targets_path,
        tenant_id="t1",
        repo_id="r1",
    )
    assert ok is False
    assert report["passed"] is False
    assert len(report["failures"]) >= 1
