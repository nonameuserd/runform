from __future__ import annotations

import json
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from typing import Any, Literal, cast

from akc.compile import Budget, CompileSession, ControllerConfig, SubprocessExecutor, TierConfig
from akc.compile.interfaces import LLMBackend, LLMRequest, LLMResponse, TenantRepoScope
from akc.memory.models import normalize_repo_id, require_non_empty
from akc.run import PassRecord, RunManifest, RuntimeEvidenceRecord, replay_runtime_execution
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
                    "benchmark_group": {"type": "string"},
                    "baseline_duration_hours": {"type": "number", "minimum": 0},
                    "target_duration_hours": {"type": "number", "minimum": 0},
                    "confidence_weight": {"type": "number", "minimum": 0},
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
                            "require_runtime_replay_determinism": {"type": "boolean"},
                            "runtime_mode": {"type": "string", "enum": ["simulate", "enforce", "canary"]},
                            "runtime_reliability_kpis": {
                                "type": "object",
                                "additionalProperties": True,
                                "properties": {
                                    "max_rollbacks_total": {"type": "integer", "minimum": 0},
                                    "max_convergence_latency_ms_avg": {"type": "number", "minimum": 0},
                                    "max_mttr_like_repair_latency_ms_avg": {"type": "number", "minimum": 0},
                                    "require_terminal_health_in": {
                                        "type": "array",
                                        "minItems": 1,
                                        "items": {
                                            "type": "string",
                                            "enum": ["healthy", "unknown", "degraded", "failed"],
                                        },
                                    },
                                },
                            },
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
    benchmark_group: str | None = None
    baseline_duration_hours: float | None = None
    target_duration_hours: float | None = None
    confidence_weight: float | None = None

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
            "benchmark_group": self.benchmark_group,
            "baseline_duration_hours": self.baseline_duration_hours,
            "target_duration_hours": self.target_duration_hours,
            "confidence_weight": self.confidence_weight,
        }


@dataclass(frozen=True, slots=True)
class EvalReport:
    suite_version: str
    passed: bool
    summary: dict[str, float]
    tasks: tuple[EvalTaskResult, ...]
    gate_violations: tuple[RegressionGateViolation, ...]
    benchmark_summary: dict[str, Any] | None = None

    def to_json_obj(self) -> dict[str, Any]:
        return {
            "suite_version": self.suite_version,
            "passed": self.passed,
            "summary": dict(self.summary),
            "tasks": [t.to_json_obj() for t in self.tasks],
            "gate_violations": [g.to_json_obj() for g in self.gate_violations],
            "benchmark_summary": dict(self.benchmark_summary) if isinstance(self.benchmark_summary, dict) else None,
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
        code_path = "src/../src/akc_eval_compiled.py" if stage == "generate" else "src/akc_eval_compiled.py"
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
            raise RuntimeError("PyYAML is required for YAML eval suites. Install with: `uv sync --extra dev`.") from e
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
            "jsonschema is required for eval suite schema validation. Install with: `uv sync --extra dev`."
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


def _percentile(*, values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    if pct <= 0:
        return float(min(values))
    if pct >= 100:
        return float(max(values))
    ordered = sorted(float(x) for x in values)
    idx = int(round((float(pct) / 100.0) * float(len(ordered) - 1)))
    idx = max(0, min(idx, len(ordered) - 1))
    return float(ordered[idx])


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
        execute_pass: PassRecord | None = next((p for p in manifest.passes if p.name == "execute"), None)
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


def _resolve_artifact_path_for_scope(
    *,
    pointer_path: str,
    outputs_root: Path,
    tenant_id: str,
    repo_id: str,
    manifest_path: Path,
) -> Path | None:
    raw = pointer_path.strip()
    if not raw:
        return None
    p = Path(raw).expanduser()
    candidates: list[Path] = []
    if p.is_absolute():
        candidates.append(p)
    scope_root = outputs_root / tenant_id / repo_id
    candidates.extend(
        [
            scope_root / raw,
            manifest_path.parent / raw,
            p,
        ]
    )
    for cand in candidates:
        resolved = cand.resolve()
        if resolved.exists():
            return resolved
    return None


def _runtime_metrics_from_evidence(*, evidence: tuple[RuntimeEvidenceRecord, ...]) -> tuple[dict[str, float], str]:
    rollbacks_total = 0
    terminal_health_status = "unknown"
    convergence_latencies: list[float] = []
    repair_latencies: list[float] = []

    for rec in evidence:
        if rec.evidence_type == "rollback_chain":
            rollbacks_total += 1
        if rec.evidence_type == "terminal_health" and rec.payload.get("aggregate") is True:
            hs = rec.payload.get("health_status")
            if isinstance(hs, str) and hs.strip():
                terminal_health_status = hs.strip().lower()
        if rec.evidence_type == "convergence_certificate" and rec.payload.get("aggregate") is True:
            win_ms = rec.payload.get("window_ms")
            if isinstance(win_ms, (int, float)) and not isinstance(win_ms, bool):
                latency = float(win_ms)
                convergence_latencies.append(latency)
                if rollbacks_total > 0:
                    repair_latencies.append(latency)

    convergence_latency_ms_avg = (
        float(sum(convergence_latencies) / float(len(convergence_latencies))) if convergence_latencies else 0.0
    )
    mttr_like_repair_latency_ms_avg = (
        float(sum(repair_latencies) / float(len(repair_latencies))) if repair_latencies else 0.0
    )

    return (
        {
            "runtime_rollbacks_total": float(rollbacks_total),
            "runtime_convergence_latency_ms_avg": convergence_latency_ms_avg,
            "runtime_mttr_like_repair_latency_ms_avg": mttr_like_repair_latency_ms_avg,
            # Preserve historical numeric metric shape for downstream consumers.
            "runtime_terminal_health_status": 1.0 if terminal_health_status == "healthy" else 0.0,
        },
        terminal_health_status,
    )


def _runtime_multi_signal_check(
    *,
    task: dict[str, Any],
    outputs_root: Path,
    manifest_path: Path,
    manifest: RunManifest,
    tenant_id: str,
    repo_id: str,
) -> tuple[tuple[str, ...], dict[str, float]]:
    checks_raw = task.get("checks")
    checks: dict[str, Any] = checks_raw if isinstance(checks_raw, dict) else {}
    need_replay = bool(checks.get("require_runtime_replay_determinism", False))
    kpis_raw = checks.get("runtime_reliability_kpis")
    kpis: dict[str, Any] = kpis_raw if isinstance(kpis_raw, dict) else {}
    if not need_replay and not kpis:
        return (), {}

    bundle_abs: Path | None = None
    if manifest.runtime_bundle is not None:
        bundle_abs = _resolve_artifact_path_for_scope(
            pointer_path=str(manifest.runtime_bundle.path),
            outputs_root=outputs_root,
            tenant_id=tenant_id,
            repo_id=repo_id,
            manifest_path=manifest_path,
        )
    if bundle_abs is None:
        fallback_bundle = (
            outputs_root / tenant_id / repo_id / ".akc" / "runtime" / f"{manifest.run_id}.runtime_bundle.json"
        )
        if fallback_bundle.exists():
            bundle_abs = fallback_bundle.resolve()
    if bundle_abs is None:
        return ("runtime bundle path from manifest could not be resolved on disk",), {}

    from argparse import Namespace

    from akc.cli.runtime import cmd_runtime_start

    before_records = list((outputs_root / tenant_id / repo_id / ".akc" / "runtime").rglob("runtime_run.json"))
    before_paths = {p.resolve() for p in before_records}

    # Runtime eval signal uses simulate mode by default for deterministic, non-mutating checks.
    runtime_mode = str(checks.get("runtime_mode", "simulate")).strip() or "simulate"
    if runtime_mode not in {"simulate", "enforce", "canary"}:
        runtime_mode = "simulate"
    try:
        with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
            cmd_runtime_start(
                Namespace(
                    bundle=str(bundle_abs),
                    mode=runtime_mode,
                    outputs_root=str(outputs_root),
                    strict_intent_authority=False,
                    verbose=False,
                )
            )
    except SystemExit as exc:
        if int(exc.code or 0) != 0:
            return (f"runtime start for eval exited non-zero: {int(exc.code or 0)}",), {}

    runtime_records = list((outputs_root / tenant_id / repo_id / ".akc" / "runtime").rglob("runtime_run.json"))
    candidates = [p.resolve() for p in runtime_records if p.resolve() not in before_paths]
    if not candidates:
        return ("runtime eval could not find a newly emitted runtime_run.json record",), {}
    latest = max(candidates, key=lambda p: p.stat().st_mtime)
    record = json.loads(latest.read_text(encoding="utf-8"))

    events_path = Path(str(record.get("events_path", ""))).expanduser().resolve()
    evidence_path = Path(str(record.get("runtime_evidence_path", ""))).expanduser().resolve()
    if not events_path.exists() or not evidence_path.exists():
        return ("runtime eval missing events/evidence artifacts after runtime start",), {}

    transcript_raw = json.loads(events_path.read_text(encoding="utf-8"))
    if not isinstance(transcript_raw, list):
        return ("runtime events transcript must be a JSON array",), {}
    transcript = [dict(item) for item in transcript_raw if isinstance(item, dict)]

    evidence_raw = json.loads(evidence_path.read_text(encoding="utf-8"))
    if not isinstance(evidence_raw, list):
        return ("runtime evidence must be a JSON array",), {}
    runtime_evidence = tuple(
        RuntimeEvidenceRecord.from_json_obj(item) for item in evidence_raw if isinstance(item, dict)
    )

    replay_manifest = RunManifest(
        run_id=manifest.run_id,
        tenant_id=manifest.tenant_id,
        repo_id=manifest.repo_id,
        ir_sha256=manifest.ir_sha256,
        replay_mode="runtime_replay",
        passes=manifest.passes,
        runtime_bundle=manifest.runtime_bundle,
        runtime_event_transcript=manifest.runtime_event_transcript,
        runtime_evidence=runtime_evidence,
    )

    failures: list[str] = []
    metrics, terminal_health_status = _runtime_metrics_from_evidence(evidence=runtime_evidence)

    if need_replay:
        replay1 = replay_runtime_execution(manifest=replay_manifest, transcript=transcript)
        replay2 = replay_runtime_execution(manifest=replay_manifest, transcript=transcript)
        payload1 = {
            "runtime_run_id": replay1.runtime_run_id,
            "mode": replay1.mode,
            "transition_count": len(replay1.transitions),
            "reconcile_decision_count": len(replay1.reconcile_decisions),
            "terminal_health_status": replay1.terminal_health_status,
            "transitions": [
                {
                    "event_id": item.event.get("event_id"),
                    "event_type": item.event.get("event_type"),
                    "transition": dict(item.transition) if item.transition is not None else None,
                    "action_decision": item.action_decision,
                    "retry_count": item.retry_count,
                    "budget_burn": dict(item.budget_burn) if item.budget_burn is not None else None,
                }
                for item in replay1.transitions
            ],
            "reconcile_decisions": [
                {
                    "resource_id": item.resource_id,
                    "operation_type": item.operation_type,
                    "applied": item.applied,
                    "rollback_chain": list(item.rollback_chain),
                    "health_status": item.health_status,
                    "payload": dict(item.payload),
                }
                for item in replay1.reconcile_decisions
            ],
        }
        payload2 = {
            "runtime_run_id": replay2.runtime_run_id,
            "mode": replay2.mode,
            "transition_count": len(replay2.transitions),
            "reconcile_decision_count": len(replay2.reconcile_decisions),
            "terminal_health_status": replay2.terminal_health_status,
            "transitions": [
                {
                    "event_id": item.event.get("event_id"),
                    "event_type": item.event.get("event_type"),
                    "transition": dict(item.transition) if item.transition is not None else None,
                    "action_decision": item.action_decision,
                    "retry_count": item.retry_count,
                    "budget_burn": dict(item.budget_burn) if item.budget_burn is not None else None,
                }
                for item in replay2.transitions
            ],
            "reconcile_decisions": [
                {
                    "resource_id": item.resource_id,
                    "operation_type": item.operation_type,
                    "applied": item.applied,
                    "rollback_chain": list(item.rollback_chain),
                    "health_status": item.health_status,
                    "payload": dict(item.payload),
                }
                for item in replay2.reconcile_decisions
            ],
        }
        deterministic_ok = payload1 == payload2
        metrics["runtime_replay_determinism"] = 1.0 if deterministic_ok else 0.0
        if not deterministic_ok:
            failures.append("runtime replay determinism failed: repeated replay payloads differ")

    max_rollbacks = _as_optional_int(kpis.get("max_rollbacks_total"))
    if max_rollbacks is not None and int(metrics.get("runtime_rollbacks_total", 0.0)) > max_rollbacks:
        failures.append(
            f"runtime_rollbacks_total={int(metrics.get('runtime_rollbacks_total', 0.0))} > max={max_rollbacks}"
        )
    max_conv = kpis.get("max_convergence_latency_ms_avg")
    if isinstance(max_conv, (int, float)) and not isinstance(max_conv, bool):
        actual_conv = float(metrics.get("runtime_convergence_latency_ms_avg", 0.0))
        if actual_conv > float(max_conv):
            failures.append(f"runtime_convergence_latency_ms_avg={actual_conv:.3f} > max={float(max_conv):.3f}")
    max_mttr = kpis.get("max_mttr_like_repair_latency_ms_avg")
    if isinstance(max_mttr, (int, float)) and not isinstance(max_mttr, bool):
        actual_mttr = float(metrics.get("runtime_mttr_like_repair_latency_ms_avg", 0.0))
        if actual_mttr > float(max_mttr):
            failures.append(f"runtime_mttr_like_repair_latency_ms_avg={actual_mttr:.3f} > max={float(max_mttr):.3f}")
    allowed_health_raw = kpis.get("require_terminal_health_in")
    if isinstance(allowed_health_raw, list) and allowed_health_raw:
        allowed_health = {str(x).strip().lower() for x in allowed_health_raw if str(x).strip()}
        actual_health = terminal_health_status
        if allowed_health and actual_health not in allowed_health:
            failures.append(f"runtime_terminal_health_status={actual_health!r} not in {sorted(allowed_health)!r}")

    return tuple(failures), metrics


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
        computed_status = "failed" if any(p.status == "failed" for p in manifest.passes) else "succeeded"
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
    runtime_failures, runtime_metrics = _runtime_multi_signal_check(
        task=task,
        outputs_root=outputs_root,
        manifest_path=manifest_path,
        manifest=manifest,
        tenant_id=tenant_id,
        repo_id=repo_id,
    )
    if runtime_failures:
        deterministic_passed = False
        deterministic_failures = tuple(list(deterministic_failures) + list(runtime_failures))
    metrics = {**metrics, **runtime_metrics}
    if isinstance(manifest.control_plane, dict):
        tc_raw = manifest.control_plane.get("time_compression_metrics")
        if isinstance(tc_raw, dict):
            for key in (
                "intent_to_healthy_runtime_ms",
                "compile_to_healthy_runtime_ms",
                "compression_factor_vs_baseline",
            ):
                value = tc_raw.get(key)
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    metrics[key] = float(value)
    judge_score, judge_reason = _selective_judge_score(task=task, manifest=manifest)
    if judge_score is not None:
        min_judge = float((task.get("judge") or {}).get("min_score", 0.0))
        if float(judge_score) < min_judge:
            deterministic_passed = False
            deterministic_failures = tuple(
                list(deterministic_failures) + [f"judge_score={judge_score:.3f} below min_score={min_judge:.3f}"]
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
        benchmark_group=(
            str(task.get("benchmark_group", "")).strip() if str(task.get("benchmark_group", "")).strip() else None
        ),
        baseline_duration_hours=(
            float(baseline_duration_hours_raw)
            if isinstance((baseline_duration_hours_raw := task.get("baseline_duration_hours")), (int, float))
            and not isinstance(baseline_duration_hours_raw, bool)
            else None
        ),
        target_duration_hours=(
            float(target_duration_hours_raw)
            if isinstance((target_duration_hours_raw := task.get("target_duration_hours")), (int, float))
            and not isinstance(target_duration_hours_raw, bool)
            else None
        ),
        confidence_weight=(
            float(confidence_weight_raw)
            if isinstance((confidence_weight_raw := task.get("confidence_weight")), (int, float))
            and not isinstance(confidence_weight_raw, bool)
            else None
        ),
    )


def _benchmark_summary(*, tasks: list[EvalTaskResult]) -> dict[str, Any]:
    groups: dict[str, dict[str, Any]] = {}
    for task in tasks:
        group = str(task.benchmark_group or "").strip()
        if not group:
            continue
        row = groups.setdefault(
            group,
            {
                "task_count": 0,
                "weights_total": 0.0,
                "intent_to_healthy_runtime_ms": [],
                "compile_to_healthy_runtime_ms": [],
                "compression_factor_vs_baseline": [],
                "pass_count": 0,
            },
        )
        row["task_count"] = int(row["task_count"]) + 1
        weight = float(task.confidence_weight) if task.confidence_weight is not None else 1.0
        row["weights_total"] = float(row["weights_total"]) + weight
        if task.deterministic_passed:
            row["pass_count"] = int(row["pass_count"]) + 1
        for metric_name in (
            "intent_to_healthy_runtime_ms",
            "compile_to_healthy_runtime_ms",
            "compression_factor_vs_baseline",
        ):
            metric_value = task.metrics.get(metric_name)
            if isinstance(metric_value, (int, float)) and not isinstance(metric_value, bool):
                cast(list[float], row[metric_name]).append(float(metric_value))
    out_groups: dict[str, Any] = {}
    for group, row in groups.items():
        intent_values = cast(list[float], row["intent_to_healthy_runtime_ms"])
        compile_values = cast(list[float], row["compile_to_healthy_runtime_ms"])
        compression_values = cast(list[float], row["compression_factor_vs_baseline"])
        task_count = int(row["task_count"])
        pass_count = int(row["pass_count"])
        out_groups[group] = {
            "task_count": task_count,
            "sample_count": len(intent_values),
            "pass_rate": _safe_ratio(pass_count, task_count),
            "intent_to_healthy_runtime_ms_p50": _percentile(values=intent_values, pct=50),
            "intent_to_healthy_runtime_ms_p90": _percentile(values=intent_values, pct=90),
            "compile_to_healthy_runtime_ms_p50": _percentile(values=compile_values, pct=50),
            "compile_to_healthy_runtime_ms_p90": _percentile(values=compile_values, pct=90),
            "compression_factor_vs_baseline_avg": (
                float(sum(compression_values) / float(len(compression_values))) if compression_values else 0.0
            ),
        }
    return {"groups": out_groups}


def _compute_summary(*, tasks: list[EvalTaskResult]) -> dict[str, float]:
    n = len(tasks)
    passed_count = sum(1 for t in tasks if t.deterministic_passed)
    failed_count = n - passed_count
    avg_repair_iterations = sum(t.metrics.get("repair_iterations", 0.0) for t in tasks) / float(n) if n > 0 else 0.0
    avg_wall_time_ms = sum(t.metrics.get("wall_time_ms", 0.0) for t in tasks) / float(n) if n > 0 else 0.0
    avg_total_tokens = sum(t.metrics.get("total_tokens", 0.0) for t in tasks) / float(n) if n > 0 else 0.0
    judge_scored = [t.judge_score for t in tasks if t.judge_score is not None]
    avg_judge_score = (
        sum(float(x) for x in judge_scored if x is not None) / float(len(judge_scored)) if judge_scored else 0.0
    )
    runtime_replay_scored = [
        t.metrics["runtime_replay_determinism"] for t in tasks if "runtime_replay_determinism" in t.metrics
    ]
    runtime_replay_determinism_rate = (
        sum(runtime_replay_scored) / float(len(runtime_replay_scored)) if runtime_replay_scored else 0.0
    )
    runtime_rollbacks_scored = [
        t.metrics["runtime_rollbacks_total"] for t in tasks if "runtime_rollbacks_total" in t.metrics
    ]
    avg_runtime_rollbacks_total = (
        sum(runtime_rollbacks_scored) / float(len(runtime_rollbacks_scored)) if runtime_rollbacks_scored else 0.0
    )
    runtime_convergence_scored = [
        t.metrics["runtime_convergence_latency_ms_avg"]
        for t in tasks
        if "runtime_convergence_latency_ms_avg" in t.metrics
    ]
    avg_runtime_convergence_latency_ms = (
        sum(runtime_convergence_scored) / float(len(runtime_convergence_scored)) if runtime_convergence_scored else 0.0
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
        "runtime_replay_determinism_rate": float(runtime_replay_determinism_rate),
        "avg_runtime_rollbacks_total": float(avg_runtime_rollbacks_total),
        "avg_runtime_convergence_latency_ms_avg": float(avg_runtime_convergence_latency_ms),
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

    min_runtime_replay_determinism_rate = float(thresholds.get("min_runtime_replay_determinism_rate", 0.0))
    actual_runtime_replay_determinism_rate = float(summary.get("runtime_replay_determinism_rate", 0.0))
    if actual_runtime_replay_determinism_rate < min_runtime_replay_determinism_rate:
        violations.append(
            RegressionGateViolation(
                gate="min_runtime_replay_determinism_rate",
                message="runtime replay determinism rate below threshold",
                actual=actual_runtime_replay_determinism_rate,
                expected=min_runtime_replay_determinism_rate,
            )
        )

    max_avg_runtime_rollbacks_total = thresholds.get("max_avg_runtime_rollbacks_total")
    if isinstance(max_avg_runtime_rollbacks_total, (int, float)) and not isinstance(
        max_avg_runtime_rollbacks_total, bool
    ):
        actual_avg_runtime_rollbacks_total = float(summary.get("avg_runtime_rollbacks_total", 0.0))
        if actual_avg_runtime_rollbacks_total > float(max_avg_runtime_rollbacks_total):
            violations.append(
                RegressionGateViolation(
                    gate="max_avg_runtime_rollbacks_total",
                    message="average runtime rollback count above threshold",
                    actual=actual_avg_runtime_rollbacks_total,
                    expected=float(max_avg_runtime_rollbacks_total),
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
                    str(k): float(v) for k, v in dict(raw["summary"]).items() if isinstance(v, (int, float))
                }

    thresholds = dict(suite.get("regression_thresholds") or {})
    gates_ok, gate_violations = _evaluate_regression_gates(
        summary=summary,
        baseline_summary=baseline_summary,
        thresholds=thresholds,
    )
    passed = gates_ok and all(t.deterministic_passed for t in task_results)
    benchmark_summary = _benchmark_summary(tasks=task_results)
    return EvalReport(
        suite_version=str(suite.get("suite_version", "v1")),
        passed=passed,
        summary=summary,
        tasks=tuple(task_results),
        gate_violations=gate_violations,
        benchmark_summary=benchmark_summary,
    )
