from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.artifacts.contracts import schema_id_for
from akc.cli import main
from akc.run import ArtifactPointer, PassRecord, RunManifest


def _write_manifest(
    *,
    path: Path,
    tenant_id: str,
    repo_id: str,
    wall_time_ms: int = 100,
    runtime_bundle_path: str | None = None,
) -> None:
    manifest = RunManifest(
        run_id="run-1",
        tenant_id=tenant_id,
        repo_id=repo_id,
        ir_sha256="a" * 64,
        replay_mode="live",
        passes=(
            PassRecord(name="plan", status="succeeded"),
            PassRecord(name="retrieve", status="succeeded"),
            PassRecord(
                name="generate",
                status="succeeded",
                metadata={"llm_text": "--- a/src/x.py\n+++ b/src/x.py\n@@\n+pass\n"},
            ),
            PassRecord(
                name="execute",
                status="succeeded",
                metadata={"exit_code": 0},
            ),
        ),
        trace_spans=(
            {
                "trace_id": "0123456789abcdef0123456789abcdef",
                "span_id": "0123456789abcdef",
                "parent_span_id": None,
                "name": "compile.run",
                "kind": "internal",
                "start_time_unix_nano": 1,
                "end_time_unix_nano": 2,
                "attributes": {},
                "status": "ok",
            },
        ),
        cost_attribution={
            "repair_iterations": 1,
            "total_tokens": 120,
            "wall_time_ms": wall_time_ms,
        },
        control_plane={
            "lifecycle_timestamps": {
                "intent_received_at": 1_000,
                "compile_started_at": 1_000,
                "compile_completed_at": 2_000,
                "runtime_healthy_at": 5_000,
            },
            "time_compression_metrics": {
                "intent_to_healthy_runtime_ms": 4_000,
                "compile_to_healthy_runtime_ms": 4_000,
                "compression_factor_vs_baseline": 2.0,
            },
        },
        runtime_bundle=(
            ArtifactPointer(path=runtime_bundle_path)
            if isinstance(runtime_bundle_path, str) and runtime_bundle_path.strip()
            else None
        ),
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest.to_json_obj()), encoding="utf-8")


def _write_runtime_bundle(*, path: Path, tenant_id: str, repo_id: str) -> None:
    payload = {
        "schema_version": 1,
        "schema_id": schema_id_for(kind="runtime_bundle"),
        "tenant_id": tenant_id,
        "repo_id": repo_id,
        "run_id": "run-1",
        "referenced_ir_nodes": [],
        "referenced_contracts": [],
        "spec_hashes": {"orchestration_spec_sha256": "a" * 64},
        "deployment_intents": [],
        "runtime_policy_envelope": {},
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_cli_eval_json_passes_for_manifest_tasks(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    manifest_path = tmp_path / "manifest.json"
    _write_manifest(path=manifest_path, tenant_id="t1", repo_id="repo1")
    suite_path = tmp_path / "suite.json"
    suite = {
        "suite_version": "v1",
        "regression_thresholds": {
            "min_success_rate": 1.0,
            "max_avg_repair_iterations": 2.0,
        },
        "tasks": [
            {
                "id": "task-1",
                "tenant_id": "t1",
                "repo_id": "repo1",
                "benchmark_group": "core",
                "baseline_duration_hours": 1.0,
                "target_duration_hours": 0.5,
                "manifest_path": str(manifest_path),
                "checks": {
                    "required_passes": ["plan", "retrieve", "generate", "execute"],
                    "max_repair_iterations": 2,
                    "max_total_tokens": 500,
                    "require_trace_spans": True,
                },
                "judge": {
                    "enabled": True,
                    "expected_keywords": ["src/x.py"],
                    "min_score": 1.0,
                },
            }
        ],
    }
    suite_path.write_text(json.dumps(suite), encoding="utf-8")

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "eval",
                "--suite-path",
                str(suite_path),
                "--outputs-root",
                str(tmp_path / "out"),
                "--format",
                "json",
            ]
        )
    assert excinfo.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is True
    assert payload["summary"]["success_rate"] == 1.0
    assert payload["benchmark_summary"]["groups"]["core"]["sample_count"] == 1


def test_cli_eval_fails_on_regression_gate(tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.json"
    _write_manifest(path=manifest_path, tenant_id="t1", repo_id="repo1", wall_time_ms=180)
    suite_path = tmp_path / "suite.json"
    suite = {
        "suite_version": "v1",
        "regression_thresholds": {
            "min_success_rate": 1.0,
            "max_avg_wall_time_regression_pct": 10.0,
        },
        "tasks": [
            {
                "id": "task-1",
                "tenant_id": "t1",
                "repo_id": "repo1",
                "manifest_path": str(manifest_path),
            }
        ],
    }
    suite_path.write_text(json.dumps(suite), encoding="utf-8")
    baseline_path = tmp_path / "baseline.json"
    baseline_path.write_text(
        json.dumps({"summary": {"success_rate": 1.0, "avg_wall_time_ms": 100.0}}),
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "eval",
                "--suite-path",
                str(suite_path),
                "--outputs-root",
                str(tmp_path / "out"),
                "--baseline-report-path",
                str(baseline_path),
            ]
        )
    assert excinfo.value.code == 2


def test_cli_eval_runs_compile_task_and_exercises_repair_loop(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    suite_path = tmp_path / "suite.json"
    suite = {
        "suite_version": "v1",
        "regression_thresholds": {
            "min_success_rate": 1.0,
            "max_avg_repair_iterations": 2.0,
        },
        "tasks": [
            {
                "id": "compile-repair-task",
                "tenant_id": "t1",
                "repo_id": "repo1",
                "intent": "Intent->system compile",
                "checks": {
                    "required_passes": ["plan", "retrieve", "generate", "execute"],
                    "max_repair_iterations": 2,
                    # Token accounting is prompt-size dependent; keep generous.
                    "max_total_tokens": 1_000_000,
                    "max_wall_time_ms": 30000,
                    "require_trace_spans": True,
                    "require_success": True,
                },
                "judge": {
                    "enabled": True,
                    "expected_keywords": [
                        "src/akc_eval_compiled.py",
                        "tests/test_akc_eval_compiled.py",
                    ],
                    "min_score": 1.0,
                },
            }
        ],
    }
    suite_path.write_text(json.dumps(suite), encoding="utf-8")

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "eval",
                "--suite-path",
                str(suite_path),
                "--outputs-root",
                str(tmp_path / "out"),
                "--format",
                "json",
            ]
        )

    assert excinfo.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is True
    assert payload["summary"]["success_rate"] == 1.0
    assert payload["tasks"][0]["metrics"]["repair_iterations"] == 1.0


def test_cli_eval_runtime_multi_signal_checks_pass(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    outputs_root = tmp_path / "out"
    bundle_path = outputs_root / "t1" / "repo1" / ".akc" / "runtime" / "run-1.runtime_bundle.json"
    _write_runtime_bundle(path=bundle_path, tenant_id="t1", repo_id="repo1")

    manifest_path = tmp_path / "manifest.json"
    _write_manifest(
        path=manifest_path,
        tenant_id="t1",
        repo_id="repo1",
        runtime_bundle_path=".akc/runtime/run-1.runtime_bundle.json",
    )

    suite_path = tmp_path / "suite.json"
    suite = {
        "suite_version": "v1",
        "regression_thresholds": {
            "min_success_rate": 1.0,
            "max_avg_repair_iterations": 2.0,
            "min_runtime_replay_determinism_rate": 1.0,
            "max_avg_runtime_rollbacks_total": 0.0,
        },
        "tasks": [
            {
                "id": "runtime-signal-task",
                "tenant_id": "t1",
                "repo_id": "repo1",
                "manifest_path": str(manifest_path),
                "checks": {
                    "required_passes": ["plan", "retrieve", "generate", "execute"],
                    "max_repair_iterations": 2,
                    "max_total_tokens": 500,
                    "require_trace_spans": True,
                    "require_runtime_replay_determinism": True,
                    "runtime_mode": "simulate",
                    "runtime_reliability_kpis": {
                        "max_rollbacks_total": 0,
                        "max_convergence_latency_ms_avg": 60000,
                        "max_mttr_like_repair_latency_ms_avg": 60000,
                        "require_terminal_health_in": ["healthy", "unknown"],
                    },
                },
                "judge": {
                    "enabled": True,
                    "expected_keywords": ["src/x.py"],
                    "min_score": 1.0,
                },
            }
        ],
    }
    suite_path.write_text(json.dumps(suite), encoding="utf-8")

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "eval",
                "--suite-path",
                str(suite_path),
                "--outputs-root",
                str(outputs_root),
                "--format",
                "json",
            ]
        )
    assert excinfo.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is True
    assert payload["summary"]["runtime_replay_determinism_rate"] == 1.0


def test_cli_eval_schema_validation_failure_is_structured_json(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # Missing required `tasks` field should fail fast with structured JSON.
    suite_path = tmp_path / "bad-suite.json"
    suite_path.write_text(
        json.dumps(
            {
                "suite_version": "v1",
                "regression_thresholds": {"min_success_rate": 1.0},
            }
        ),
        encoding="utf-8",
    )

    report_out = tmp_path / "report.json"
    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "eval",
                "--suite-path",
                str(suite_path),
                "--outputs-root",
                str(tmp_path / "out"),
                "--format",
                "json",
                "--report-out",
                str(report_out),
            ]
        )

    assert excinfo.value.code == 2

    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is False
    assert payload["error"]["type"] in {"ValueError", "RuntimeError"}
    assert "eval suite schema validation failed" in payload["error"]["message"]
    assert report_out.exists()
