from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.cli import main
from akc.run import PassRecord, RunManifest


def _repo_root() -> Path:
    # tests/e2e/<file>.py -> repo root
    return Path(__file__).resolve().parents[2]


def _run_cli(*, argv: list[str], capsys: pytest.CaptureFixture[str]) -> tuple[int, dict]:
    with pytest.raises(SystemExit) as excinfo:
        main(argv)
    assert excinfo.value.code is not None
    payload = json.loads(capsys.readouterr().out)
    assert isinstance(payload, dict)
    return int(excinfo.value.code), payload


def test_eval_suite_yaml_end_to_end(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    suite_path = _repo_root() / "configs/evals/intent_system_v1.yaml"
    outputs_root = tmp_path / "out"
    report_out = tmp_path / "latest-report.json"

    code, payload = _run_cli(
        argv=[
            "eval",
            "--suite-path",
            str(suite_path),
            "--outputs-root",
            str(outputs_root),
            "--report-out",
            str(report_out),
            "--format",
            "json",
        ],
        capsys=capsys,
    )

    assert code == 0
    assert payload["passed"] is True
    assert payload["gate_violations"] == []
    assert report_out.exists()
    written = json.loads(report_out.read_text(encoding="utf-8"))
    assert written["passed"] is True


def test_eval_regression_gate_with_baseline(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    repo_root = _repo_root()
    fixture_manifest = repo_root / "configs/evals/fixtures/sample_run.manifest.json"

    suite_path = tmp_path / "suite.json"
    baseline_path = tmp_path / "baseline.json"
    outputs_root = tmp_path / "out"

    # One task "fails" deterministically via status_override, so success_rate drops.
    suite = {
        "suite_version": "v1",
        "regression_thresholds": {
            "min_success_rate": 0.4,  # allow absolute success rate to pass
            "max_avg_repair_iterations": 2.0,
            "max_success_rate_drop": 0.1,
            "max_avg_wall_time_regression_pct": 10.0,
        },
        "tasks": [
            {
                "id": "golden-pass",
                "tenant_id": "eval-tenant",
                "repo_id": "eval-repo",
                "manifest_path": str(fixture_manifest),
                "status_override": "succeeded",
                "checks": {
                    "required_passes": ["plan", "retrieve", "generate", "execute"],
                    "require_trace_spans": True,
                    "require_unit_tests_passed": True,
                },
            },
            {
                "id": "forced-fail",
                "tenant_id": "eval-tenant",
                "repo_id": "eval-repo",
                "manifest_path": str(fixture_manifest),
                "status_override": "failed",
                "checks": {
                    "required_passes": ["plan", "retrieve", "generate", "execute"],
                    "require_trace_spans": True,
                    "require_unit_tests_passed": True,
                },
            },
        ],
    }
    suite_path.write_text(json.dumps(suite, indent=2, sort_keys=True), encoding="utf-8")
    baseline_path.write_text(
        json.dumps(
            {
                "suite_version": "v1",
                "passed": True,
                "summary": {"success_rate": 1.0, "avg_wall_time_ms": 100.0},
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    code, payload = _run_cli(
        argv=[
            "eval",
            "--suite-path",
            str(suite_path),
            "--outputs-root",
            str(outputs_root),
            "--baseline-report-path",
            str(baseline_path),
            "--format",
            "json",
        ],
        capsys=capsys,
    )

    assert code == 2
    assert payload["passed"] is False
    assert payload["gate_violations"], "expected regression gate violations"


def test_eval_compilation_task_runs_deterministically(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    suite_path = tmp_path / "suite.json"
    outputs_root = tmp_path / "out"

    # Compilation-only task: no manifest_path; harness will run the compile loop.
    suite = {
        "suite_version": "v1",
        "regression_thresholds": {"min_success_rate": 1.0, "max_avg_repair_iterations": 2.0},
        "tasks": [
            {
                "id": "compile-only",
                "tenant_id": "t1",
                "repo_id": "r1",
                "intent": "Compile eval task for e2e",
                "checks": {
                    "require_success": True,
                    "required_passes": ["plan", "retrieve", "generate", "execute"],
                    "require_trace_spans": True,
                    "require_unit_tests_passed": True,
                    "max_repair_iterations": 2,
                    "max_total_tokens": 1_000_000,
                    "max_wall_time_ms": 30000,
                },
            }
        ],
    }
    suite_path.write_text(json.dumps(suite, indent=2, sort_keys=True), encoding="utf-8")

    code, payload = _run_cli(
        argv=[
            "eval",
            "--suite-path",
            str(suite_path),
            "--outputs-root",
            str(outputs_root),
            "--format",
            "json",
        ],
        capsys=capsys,
    )

    assert code == 0
    assert payload["passed"] is True
    assert payload["summary"]["success_rate"] == 1.0


def test_eval_manifest_task_uses_manifest_status_without_override(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # Construct a manifest where `execute` is failed.
    manifest_path = tmp_path / "manifest.json"
    manifest = RunManifest(
        run_id="run-1",
        tenant_id="eval-tenant",
        repo_id="eval-repo",
        ir_sha256="a" * 64,
        replay_mode="live",
        passes=(
            PassRecord(name="plan", status="succeeded"),
            PassRecord(name="retrieve", status="succeeded"),
            PassRecord(name="generate", status="succeeded", metadata={"llm_text": "patch"}),
            PassRecord(name="execute", status="failed", metadata={"exit_code": 1}),
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
        cost_attribution={"repair_iterations": 0, "total_tokens": 1, "wall_time_ms": 1},
    )
    manifest_path.write_text(json.dumps(manifest.to_json_obj()), encoding="utf-8")

    suite_path = tmp_path / "suite.json"
    suite = {
        "suite_version": "v1",
        "regression_thresholds": {
            # Allow regression gates to pass so we can isolate deterministic failures.
            "min_success_rate": 0.0,
            "max_avg_repair_iterations": 10.0,
        },
        "tasks": [
            {
                "id": "manifest-fail",
                "tenant_id": "eval-tenant",
                "repo_id": "eval-repo",
                "manifest_path": str(manifest_path),
                "checks": {
                    "required_passes": ["plan", "retrieve", "generate", "execute"],
                    "require_trace_spans": True,
                    "require_unit_tests_passed": True,
                    # keep require_success default=True
                },
            }
        ],
    }
    suite_path.write_text(json.dumps(suite), encoding="utf-8")

    outputs_root = tmp_path / "out"
    code_expected = 2
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
    assert int(excinfo.value.code) == code_expected
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is False
    assert payload["gate_violations"] == []
    assert payload["tasks"][0]["deterministic_passed"] is False
    assert any(
        "run_status expected succeeded" in s for s in payload["tasks"][0]["deterministic_failures"]
    )
