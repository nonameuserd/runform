from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from akc.compile import Budget, CompileSession, ControllerConfig, SubprocessExecutor, TierConfig
from akc.compile.interfaces import LLMBackend, LLMRequest, LLMResponse, TenantRepoScope
from akc.memory.models import normalize_repo_id, require_non_empty
from akc.run import PassRecord, RunManifest
from akc.run.loader import load_run_manifest

_ALLOWED_EVAL_TASK_PASS_NAMES: tuple[str, ...] = (
    "plan",
    "retrieve",
    "generate",
    "execute",
    "repair",
    "verify",
)

_EVAL_SUITE_SCHEMA_V1: dict[str, Any] = {
    "type": "object",
    "required": ["tasks"],
    "properties": {
        "suite_version": {"type": "string"},
        "regression_thresholds": {
            "type": "object",
            "additionalProperties": True,
            "properties": {
                "min_success_rate": {"type": "number", "minimum": 0},
                "max_avg_repair_iterations": {"type": "number", "minimum": 0},
                "max_success_rate_drop": {"type": "number", "minimum": 0},
                "max_avg_wall_time_regression_pct": {"type": "number", "minimum": 0},
            },
        },
        "tasks": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "required": ["id", "tenant_id", "repo_id"],
                "additionalProperties": True,
                "properties": {
                    "id": {"type": "string", "minLength": 1},
                    "tenant_id": {"type": "string", "minLength": 1},
                    "repo_id": {"type": "string", "minLength": 1},
                    "manifest_path": {"type": "string"},
                    "intent": {"type": "string"},
                    "step": {"type": "string"},
                    "mode": {"type": "string", "enum": ["quick", "thorough"]},
                    "status_override": {"type": "string", "enum": ["succeeded", "failed"]},
                    "checks": {
                        "type": "object",
                        "additionalProperties": True,
                        "properties": {
                            "require_success": {"type": "boolean"},
                            "required_passes": {
                                "type": "array",
                                "minItems": 1,
                                "items": {
                                    "type": "string",
                                    "enum": list(_ALLOWED_EVAL_TASK_PASS_NAMES),
                                },
                            },
                            "max_repair_iterations": {"type": "integer", "minimum": 0},
                            "max_total_tokens": {"type": "integer", "minimum": 0},
                            "max_wall_time_ms": {"type": "integer", "minimum": 0},
                            "require_trace_spans": {"type": "boolean"},
                            "require_unit_tests_passed": {"type": "boolean"},
                        },
                    },
                    "judge": {
                        "type": "object",
                        "additionalProperties": True,
                        "properties": {
                            "enabled": {"type": "boolean"},
                            "expected_keywords": {
                                "type": "array",
                                "items": {"type": "string"},
                            },
                            "min_score": {
                                "type": "number",
                                "minimum": 0,
                                "maximum": 1,
                            },
                        },
                    },
                },
            },
        },
    },
    "additionalProperties": True,
}


@dataclass(frozen=True, slots=True)
class RegressionGateViolation:
    gate: str
    message: str
    actual: float
    expected: float

    def to_json_obj(self) -> dict[str, Any]:
        return {
            "gate": self.gate,
            "message": self.message,
            "actual": float(self.actual),
            "expected": float(self.expected),
        }


@dataclass(frozen=True, slots=True)
class EvalTaskResult:
    task_id: str
    tenant_id: str
    repo_id: str
    run_status: str
    deterministic_passed: bool
    deterministic_failures: tuple[str, ...]
    judge_score: float | None
    judge_reason: str | None
    run_manifest_path: str
    metrics: dict[str, float]

    def to_json_obj(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "tenant_id": self.tenant_id,
            "repo_id": self.repo_id,
            "run_status": self.run_status,
            "deterministic_passed": self.deterministic_passed,
            "deterministic_failures": list(self.deterministic_failures),
            "judge_score": self.judge_score,
            "judge_reason": self.judge_reason,
            "run_manifest_path": self.run_manifest_path,
            "metrics": dict(self.metrics),
        }


@dataclass(frozen=True, slots=True)
class EvalReport:
    suite_version: str
    passed: bool
    summary: dict[str, float]
    tasks: tuple[EvalTaskResult, ...]
    gate_violations: tuple[RegressionGateViolation, ...]

    def to_json_obj(self) -> dict[str, Any]:
        return {
            "suite_version": self.suite_version,
            "passed": self.passed,
            "summary": dict(self.summary),
            "tasks": [t.to_json_obj() for t in self.tasks],
            "gate_violations": [g.to_json_obj() for g in self.gate_violations],
        }


class _OfflineEvalLLM(LLMBackend):
    """Deterministic offline model for eval harness tasks."""

    def complete(
        self,
        *,
        scope: TenantRepoScope,
        stage: str,
        request: LLMRequest,
    ) -> LLMResponse:
        _ = request
        # Make the initial "generate" patch verifier-vetoed (path traversal),
        # and the subsequent "repair" patch safe. This exercises the
        # closed-loop repair path in CI deterministically.
        code_path = (
            "src/../src/akc_eval_compiled.py" if stage == "generate" else "src/akc_eval_compiled.py"
        )
        test_path = "tests/test_akc_eval_compiled.py"
        text = "\n".join(
            [
                f"--- a/{code_path}",
                f"+++ b/{code_path}",
                "@@",
                f"+# eval stage={stage} tenant={scope.tenant_id} repo={scope.repo_id}",
                "",
                f"--- a/{test_path}",
                f"+++ b/{test_path}",
                "@@",
                "+def test_eval_compiled() -> None:",
                "+    assert True",
                "",
            ]
        )
        return LLMResponse(text=text, raw=None, usage=None)


def _mk_eval_controller_config(*, mode: str) -> ControllerConfig:
    tiers: dict[Literal["small", "medium", "large"], TierConfig] = {
        "small": TierConfig(name="small", llm_model="eval-small", temperature=0.0),
        "medium": TierConfig(name="medium", llm_model="eval-medium", temperature=0.0),
        "large": TierConfig(name="large", llm_model="eval-large", temperature=0.0),
    }
    test_mode: Literal["smoke", "full"] = "smoke"
    if mode == "thorough":
        budget = Budget(max_llm_calls=10, max_repairs_per_step=4, max_iterations_total=8)
        test_mode = "full"
    else:
        budget = Budget(max_llm_calls=4, max_repairs_per_step=2, max_iterations_total=4)
    return ControllerConfig(
        tiers=tiers,
        stage_tiers={"generate": "small", "repair": "small"},
        budget=budget,
        test_mode=test_mode,
        # Keep the "execute" stage deterministic even without any repo checkout.
        # The verifier gate is what forces repair in this harness.
        test_command=("python", "-c", "print('ok')"),
        test_timeout_s=2.0,
        tool_allowlist=("llm.complete", "executor.run"),
    )


def _load_suite_obj(path: Path) -> dict[str, Any]:
    suffix = path.suffix.lower()
    text = path.read_text(encoding="utf-8")

    if suffix in {".yaml", ".yml"}:
        try:
            import yaml
        except ImportError as e:  # pragma: no cover
            raise RuntimeError(
                "PyYAML is required for YAML eval suites. Install with: `uv sync --extra dev`."
            ) from e
        raw = yaml.safe_load(text)
    else:
        # Default: JSON.
        raw = json.loads(text)

    if not isinstance(raw, dict):
        raise ValueError("eval suite must parse into a JSON/YAML object")
    return raw


def _validate_suite_schema(*, suite: dict[str, Any]) -> None:
    try:
        from jsonschema import Draft7Validator
    except ImportError as e:  # pragma: no cover
        raise RuntimeError(
            "jsonschema is required for eval suite schema validation. "
            "Install with: `uv sync --extra dev`."
        ) from e

    validator = Draft7Validator(_EVAL_SUITE_SCHEMA_V1)
    errors = sorted(validator.iter_errors(suite), key=lambda err: list(err.path))
    if not errors:
        return

    msgs = []
    for err in errors[:5]:
        where = ".".join(str(p) for p in err.path) if err.path else "<root>"
        msgs.append(f"{where}: {err.message}")
    raise ValueError("eval suite schema validation failed: " + " | ".join(msgs))


def _safe_ratio(n: int, d: int) -> float:
    if d <= 0:
        return 0.0
    return float(n) / float(d)


def _as_int(value: Any, *, default: int = 0) -> int:
    """Best-effort conversion for JSON-shaped numeric values."""
    if isinstance(value, bool):
        # bool is a subclass of int; treat it as non-numeric for eval policies.
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return default
        try:
            return int(s)
        except ValueError:
            try:
                return int(float(s))
            except ValueError:
                return default
    return default


def _as_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            return int(s)
        except ValueError:
            try:
                return int(float(s))
            except ValueError:
                return None
    return None


def _selective_judge_score(
    *,
    task: dict[str, Any],
    manifest: RunManifest,
) -> tuple[float | None, str | None]:
    judge = task.get("judge")
    if not isinstance(judge, dict) or not bool(judge.get("enabled", False)):
        return None, None
    # Selective judge scoring: only tasks with judge.enabled=true are scored.
    # We keep this deterministic/offline by evaluating expected keyword coverage
    # against generated patch text, while preserving a stable score contract.
    expected_keywords = judge.get("expected_keywords")
    if not isinstance(expected_keywords, list) or not expected_keywords:
        return 1.0, "judge enabled without keywords; defaulting to pass"
    patch_text = ""
    for p in manifest.passes:
        md = p.metadata or {}
        raw = md.get("llm_text")
        if isinstance(raw, str) and raw.strip():
            patch_text = raw
            # Keep scanning: when repair exists, the latest best candidate
            # is typically emitted in the final pass' metadata.
    if not patch_text:
        return 0.0, "missing generated patch text for judge scoring"
    lowered = patch_text.lower()
    matched = sum(1 for kw in expected_keywords if str(kw).lower() in lowered)
    score = _safe_ratio(matched, len(expected_keywords))
    reason = f"matched_keywords={matched}/{len(expected_keywords)}"
    return score, reason


def _deterministic_check(
    *,
    task: dict[str, Any],
    manifest: RunManifest,
    run_status: str,
) -> tuple[bool, tuple[str, ...], dict[str, float]]:
    checks_raw = task.get("checks")
    checks: dict[str, Any] = checks_raw if isinstance(checks_raw, dict) else {}
    failures: list[str] = []

    if bool(checks.get("require_success", True)) and run_status != "succeeded":
        failures.append(f"run_status expected succeeded, got {run_status}")

    required_passes_raw = checks.get("required_passes", ["plan", "retrieve", "generate", "execute"])
    required_passes: Any = required_passes_raw
    if isinstance(required_passes, list):
        pass_map = {p.name: p.status for p in manifest.passes}
        for pass_name in required_passes:
            name = str(pass_name)
            if pass_map.get(name) != "succeeded":
                failures.append(f"pass {name} was not succeeded")

    cost_attr_raw = manifest.cost_attribution or {}
    if isinstance(cost_attr_raw, dict):
        cost_attr: dict[str, Any] = cost_attr_raw
    else:
        cost_attr = {}

    repair_iterations = _as_int(cost_attr.get("repair_iterations"), default=0)
    max_repairs = _as_optional_int(checks.get("max_repair_iterations"))
    if max_repairs is not None and repair_iterations > max_repairs:
        failures.append(f"repair_iterations={repair_iterations} > max={max_repairs}")

    total_tokens = _as_int(cost_attr.get("total_tokens"), default=0)
    max_total_tokens = _as_optional_int(checks.get("max_total_tokens"))
    if max_total_tokens is not None and total_tokens > max_total_tokens:
        failures.append(f"total_tokens={total_tokens} > max={max_total_tokens}")

    wall_time_ms = _as_int(cost_attr.get("wall_time_ms"), default=0)
    max_wall_time_ms = _as_optional_int(checks.get("max_wall_time_ms"))
    if max_wall_time_ms is not None and wall_time_ms > max_wall_time_ms:
        failures.append(f"wall_time_ms={wall_time_ms} > max={max_wall_time_ms}")

    # Deterministic "unit tests passed" gate:
    # - When present, the `execute` pass metadata must include `exit_code`.
    # - A successful unit-test run implies `execute.exit_code == 0`.
    require_unit_tests_passed = bool(checks.get("require_unit_tests_passed", True))
    if require_unit_tests_passed:
        execute_pass: PassRecord | None = next(
            (p for p in manifest.passes if p.name == "execute"), None
        )
        if execute_pass is None:
            failures.append("missing execute pass record for unit test gate")
        else:
            md = execute_pass.metadata or {}
            exit_code = _as_optional_int(md.get("exit_code"))
            if exit_code is None:
                failures.append("execute.exit_code missing/invalid for unit test gate")
            elif exit_code != 0:
                failures.append(f"unit tests failed: execute.exit_code={exit_code}")

    if bool(checks.get("require_trace_spans", True)) and len(manifest.trace_spans) <= 0:
        failures.append("expected non-empty trace_spans")

    metrics = {
        "repair_iterations": float(repair_iterations),
        "total_tokens": float(total_tokens),
        "wall_time_ms": float(wall_time_ms),
        "trace_spans_count": float(len(manifest.trace_spans)),
    }
    return (len(failures) == 0), tuple(failures), metrics


def _load_suite(path: Path) -> dict[str, Any]:
    suite = _load_suite_obj(path)
    _validate_suite_schema(suite=suite)
    return suite


def _run_single_task(
    *,
    task: dict[str, Any],
    outputs_root: Path,
    suite_dir: Path,
) -> EvalTaskResult:
    task_id = str(task.get("id", "")).strip()
    tenant_id = str(task.get("tenant_id", "")).strip()
    repo_id = normalize_repo_id(str(task.get("repo_id", "")).strip())
    require_non_empty(task_id, name="eval_task.id")
    require_non_empty(tenant_id, name="eval_task.tenant_id")
    require_non_empty(repo_id, name="eval_task.repo_id")

    manifest_path_raw = task.get("manifest_path")
    run_status = "failed"
    manifest_path: Path
    if isinstance(manifest_path_raw, str) and manifest_path_raw.strip():
        manifest_path = Path(manifest_path_raw).expanduser()
        if not manifest_path.is_absolute():
            manifest_path = (suite_dir / manifest_path).resolve()
        manifest = load_run_manifest(
            path=manifest_path,
            expected_tenant_id=tenant_id,
            expected_repo_id=repo_id,
        )
        computed_status = (
            "failed" if any(p.status == "failed" for p in manifest.passes) else "succeeded"
        )
        run_status = str(task.get("status_override", computed_status))
    else:
        base = outputs_root / tenant_id / repo_id
        memory_db = base / ".akc" / "memory.sqlite"
        memory_db.parent.mkdir(parents=True, exist_ok=True)
        session = CompileSession.from_sqlite(
            tenant_id=tenant_id,
            repo_id=repo_id,
            sqlite_path=str(memory_db),
            index=None,
        )
        plan = session.memory.plan_state.create_plan(
            tenant_id=tenant_id,
            repo_id=repo_id,
            goal=str(task.get("intent", "eval task")),
            initial_steps=[str(task.get("step", "Implement intent"))],
        )
        session.memory.plan_state.set_active_plan(
            tenant_id=tenant_id,
            repo_id=repo_id,
            plan_id=plan.id,
        )
        work_root = outputs_root / tenant_id / repo_id
        executor = SubprocessExecutor(
            work_root=work_root,
            home_under_cwd=True,
            disable_network=True,
        )
        result = session.run(
            goal=str(task.get("intent", "eval task")),
            llm=_OfflineEvalLLM(),
            executor=executor,
            config=_mk_eval_controller_config(mode=str(task.get("mode", "quick"))),
            outputs_root=outputs_root,
        )
        run_status = str(result.status)
        run_dir = outputs_root / tenant_id / repo_id / ".akc" / "run"
        candidates = sorted(
            run_dir.glob("*.manifest.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if not candidates:
            raise ValueError("no run manifest emitted for eval task")
        manifest_path = candidates[0]
        manifest = load_run_manifest(
            path=manifest_path,
            expected_tenant_id=tenant_id,
            expected_repo_id=repo_id,
        )

    deterministic_passed, deterministic_failures, metrics = _deterministic_check(
        task=task, manifest=manifest, run_status=run_status
    )
    judge_score, judge_reason = _selective_judge_score(task=task, manifest=manifest)
    if judge_score is not None:
        min_judge = float((task.get("judge") or {}).get("min_score", 0.0))
        if float(judge_score) < min_judge:
            deterministic_passed = False
            deterministic_failures = tuple(
                list(deterministic_failures)
                + [f"judge_score={judge_score:.3f} below min_score={min_judge:.3f}"]
            )
    return EvalTaskResult(
        task_id=task_id,
        tenant_id=tenant_id,
        repo_id=repo_id,
        run_status=run_status,
        deterministic_passed=deterministic_passed,
        deterministic_failures=deterministic_failures,
        judge_score=judge_score,
        judge_reason=judge_reason,
        run_manifest_path=str(manifest_path),
        metrics=metrics,
    )


def _compute_summary(*, tasks: list[EvalTaskResult]) -> dict[str, float]:
    n = len(tasks)
    passed_count = sum(1 for t in tasks if t.deterministic_passed)
    failed_count = n - passed_count
    avg_repair_iterations = (
        sum(t.metrics.get("repair_iterations", 0.0) for t in tasks) / float(n) if n > 0 else 0.0
    )
    avg_wall_time_ms = (
        sum(t.metrics.get("wall_time_ms", 0.0) for t in tasks) / float(n) if n > 0 else 0.0
    )
    avg_total_tokens = (
        sum(t.metrics.get("total_tokens", 0.0) for t in tasks) / float(n) if n > 0 else 0.0
    )
    judge_scored = [t.judge_score for t in tasks if t.judge_score is not None]
    avg_judge_score = (
        sum(float(x) for x in judge_scored if x is not None) / float(len(judge_scored))
        if judge_scored
        else 0.0
    )
    return {
        "tasks_total": float(n),
        "tasks_passed": float(passed_count),
        "tasks_failed": float(failed_count),
        "success_rate": _safe_ratio(passed_count, n),
        "avg_repair_iterations": float(avg_repair_iterations),
        "avg_wall_time_ms": float(avg_wall_time_ms),
        "avg_total_tokens": float(avg_total_tokens),
        "avg_judge_score": float(avg_judge_score),
    }


def _evaluate_regression_gates(
    *,
    summary: dict[str, float],
    baseline_summary: dict[str, float] | None,
    thresholds: dict[str, Any],
) -> tuple[bool, tuple[RegressionGateViolation, ...]]:
    violations: list[RegressionGateViolation] = []

    min_success_rate = float(thresholds.get("min_success_rate", 1.0))
    actual_success_rate = float(summary.get("success_rate", 0.0))
    if actual_success_rate < min_success_rate:
        violations.append(
            RegressionGateViolation(
                gate="min_success_rate",
                message="success rate below threshold",
                actual=actual_success_rate,
                expected=min_success_rate,
            )
        )

    max_avg_repairs = float(thresholds.get("max_avg_repair_iterations", 3.0))
    actual_avg_repairs = float(summary.get("avg_repair_iterations", 0.0))
    if actual_avg_repairs > max_avg_repairs:
        violations.append(
            RegressionGateViolation(
                gate="max_avg_repair_iterations",
                message="average repair iterations above threshold",
                actual=actual_avg_repairs,
                expected=max_avg_repairs,
            )
        )

    if baseline_summary is not None:
        max_success_rate_drop = float(thresholds.get("max_success_rate_drop", 0.02))
        baseline_success = float(baseline_summary.get("success_rate", 0.0))
        actual_drop = baseline_success - actual_success_rate
        if actual_drop > max_success_rate_drop:
            violations.append(
                RegressionGateViolation(
                    gate="max_success_rate_drop",
                    message="success rate regressed beyond allowed drop",
                    actual=actual_drop,
                    expected=max_success_rate_drop,
                )
            )

        max_wall_time_regress_pct = float(thresholds.get("max_avg_wall_time_regression_pct", 25.0))
        baseline_wall = float(baseline_summary.get("avg_wall_time_ms", 0.0))
        actual_wall = float(summary.get("avg_wall_time_ms", 0.0))
        if baseline_wall > 0.0:
            regress_pct = ((actual_wall - baseline_wall) / baseline_wall) * 100.0
            if regress_pct > max_wall_time_regress_pct:
                violations.append(
                    RegressionGateViolation(
                        gate="max_avg_wall_time_regression_pct",
                        message="average wall time regressed beyond threshold",
                        actual=regress_pct,
                        expected=max_wall_time_regress_pct,
                    )
                )

    return (len(violations) == 0), tuple(violations)


def run_eval_suite(
    *,
    suite_path: str | Path,
    outputs_root: str | Path,
    baseline_report_path: str | Path | None = None,
) -> EvalReport:
    suite_fp = Path(suite_path).expanduser()
    outputs = Path(outputs_root).expanduser()
    suite = _load_suite(suite_fp)

    tasks_raw = suite.get("tasks")
    assert isinstance(tasks_raw, list)  # validated in _load_suite

    task_results = [
        _run_single_task(task=dict(task), outputs_root=outputs, suite_dir=suite_fp.parent)
        for task in tasks_raw
        if isinstance(task, dict)
    ]
    summary = _compute_summary(tasks=task_results)

    baseline_summary: dict[str, float] | None = None
    if baseline_report_path is not None:
        base_fp = Path(baseline_report_path).expanduser()
        if base_fp.exists():
            raw = json.loads(base_fp.read_text(encoding="utf-8"))
            if isinstance(raw, dict) and isinstance(raw.get("summary"), dict):
                baseline_summary = {
                    str(k): float(v)
                    for k, v in dict(raw["summary"]).items()
                    if isinstance(v, (int, float))
                }

    thresholds = dict(suite.get("regression_thresholds") or {})
    gates_ok, gate_violations = _evaluate_regression_gates(
        summary=summary,
        baseline_summary=baseline_summary,
        thresholds=thresholds,
    )
    passed = gates_ok and all(t.deterministic_passed for t in task_results)
    return EvalReport(
        suite_version=str(suite.get("suite_version", "v1")),
        passed=passed,
        summary=summary,
        tasks=tuple(task_results),
        gate_violations=gate_violations,
    )
