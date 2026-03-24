"""Compile-time intent success-criteria evaluation (acceptance contract).

Runtime transcripts use ``akc.run.recompile_triggers.normalized_success_criterion_ids_from_runtime_payload``
for optional ``success_criterion_id`` / ``success_criterion_ids`` on events (import kept out of this
package's public ``__init__`` to avoid circular imports with the compile stack).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, cast

from akc.compile.interfaces import ExecutionResult
from akc.intent.models import (
    EvaluationMode,
    OperationalValidityParamsError,
    SuccessCriterion,
    parse_operational_validity_params,
)
from akc.intent.operational_eval import (
    evaluate_operational_spec,
    parse_otel_metric_ndjson_slice,
    parse_otel_ndjson_slice,
)
from akc.memory.models import JSONValue
from akc.run.manifest import RuntimeEvidenceRecord


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


def _as_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        s = value.strip().lower()
        if s in {"1", "true", "yes", "y", "on"}:
            return True
        if s in {"0", "false", "no", "n", "off"}:
            return False
    if isinstance(value, int):
        if value == 0:
            return False
        if value == 1:
            return True
    return None


def _as_str_list(value: Any) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, list):
        out: list[str] = []
        for x in value:
            if isinstance(x, str):
                sx = x.strip()
                if sx:
                    out.append(sx)
        return out
    if isinstance(value, tuple):
        out2: list[str] = []
        for x in value:
            if isinstance(x, str):
                sx = x.strip()
                if sx:
                    out2.append(sx)
        return out2
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        # Best-effort comma-splitting for simple config convenience.
        return [x.strip() for x in s.split(",") if x.strip()]
    return None


def _runtime_evidence_records_from_bundle(bundle: Mapping[str, Any]) -> tuple[RuntimeEvidenceRecord, ...]:
    raw = bundle.get("runtime_evidence_records")
    if not isinstance(raw, list):
        return ()
    out: list[RuntimeEvidenceRecord] = []
    for item in raw:
        if not isinstance(item, Mapping):
            continue
        try:
            out.append(RuntimeEvidenceRecord.from_json_obj(item))
        except ValueError:
            continue
    return tuple(out)


def _ensure_json_value_dict(v: Mapping[str, Any] | None) -> dict[str, JSONValue]:
    if not v:
        return {}
    out: dict[str, JSONValue] = {}
    for k, val in v.items():
        kk = str(k)
        if isinstance(val, (bool, int, float, str)):
            out[kk] = cast(JSONValue, val)
        elif isinstance(val, list):
            # Trust nested JSONValue lists (best-effort).
            out[kk] = cast(JSONValue, val)
        elif isinstance(val, dict):
            out[kk] = cast(JSONValue, val)
        # else: drop unserializable values deterministically
    return out


@dataclass(frozen=True, slots=True)
class IntentAcceptance:
    passed: bool
    failures: tuple[str, ...]
    evaluated_count: int
    per_criterion: tuple[dict[str, JSONValue], ...]

    def to_step_output_obj(self) -> dict[str, JSONValue]:
        return {
            "passed": bool(self.passed),
            "failures": list(self.failures),
            "evaluated_success_criteria": int(self.evaluated_count),
            "checks": [dict(x) for x in self.per_criterion],
        }


def evaluate_intent_success_criteria(
    *,
    success_criteria: Sequence[SuccessCriterion],
    execution: ExecutionResult,
    patch_text: str,
    touched_paths: Sequence[str],
    accounting: Mapping[str, Any],
    wall_time_ms: int,
    verifier_passed: bool,
) -> IntentAcceptance:
    """Deterministically evaluate intent `success_criteria`.

    This is Phase 5: acceptance contract execution.

    Design:
    - Pure-Python, offline, and tenant-safe (no filesystem access).
    - Fail-closed when explicit, parameterized criteria are provided.
    - If `success_criteria` is empty, acceptance is satisfied.
    """

    criteria = tuple(success_criteria or ())
    if not criteria:
        return IntentAcceptance(
            passed=True,
            failures=(),
            evaluated_count=0,
            per_criterion=(),
        )

    patch_lower = str(patch_text or "").lower()
    touched_set = {str(x).strip() for x in touched_paths if str(x).strip()}

    failures: list[str] = []
    checks: list[dict[str, JSONValue]] = []

    for sc in criteria:
        sc_id = str(sc.id)
        mode: EvaluationMode = sc.evaluation_mode
        params = dict(sc.params or {})
        passed = True
        message = ""
        evidence: dict[str, Any] = {}

        if mode == "tests":
            expected_exit = _as_optional_int(params.get("expected_exit_code"))
            required = _as_optional_int(params.get("required_exit_code"))
            effective_expected = expected_exit if expected_exit is not None else required
            if effective_expected is not None:
                passed = int(execution.exit_code) == int(effective_expected)
                message = f"tests expected exit_code={effective_expected}, got {execution.exit_code}"
                evidence["expected_exit_code"] = int(effective_expected)
                evidence["exit_code"] = int(execution.exit_code)
            else:
                # Default: promotable candidate must have passed unit tests.
                passed = int(execution.exit_code) == 0
                message = f"tests require exit_code==0, got {execution.exit_code}"
                evidence["exit_code"] = int(execution.exit_code)

        elif mode == "manifest_check":
            # `manifest_check` is evaluated from compile-time signals we already have.
            require_trace = _as_bool(params.get("require_trace_spans", True))
            if require_trace is None:
                require_trace = True
            trace_spans_count = _as_optional_int(len(accounting.get("trace_spans") or []))
            # For this runtime evaluator, `trace_spans_count` is always a valid int.
            trace_spans_count_i = int(trace_spans_count or 0)

            require_verifier = _as_bool(params.get("require_verifier_passed", True))
            if require_verifier is None:
                require_verifier = True

            if bool(require_verifier) and not bool(verifier_passed):
                passed = False
                message = "manifest_check requires verifier_passed=true"
                evidence["verifier_passed"] = bool(verifier_passed)

            if passed and bool(require_trace) and trace_spans_count_i <= 0:
                passed = False
                message = "manifest_check requires non-empty trace spans"
                evidence["trace_spans_count"] = int(trace_spans_count_i)

        elif mode == "artifact_check":
            expected_keywords = _as_str_list(
                params.get("expected_keywords") if "expected_keywords" in params else params.get("required_keywords")
            )
            forbidden_keywords = _as_str_list(params.get("forbidden_keywords"))
            required_paths = _as_str_list(
                params.get("required_touched_paths")
                if "required_touched_paths" in params
                else params.get("required_files")
            )

            if expected_keywords:
                missing = [kw for kw in expected_keywords if kw.lower() not in patch_lower]
                if missing:
                    passed = False
                    message = "missing required patch keywords"
                    evidence["missing_keywords"] = missing

            if passed and forbidden_keywords:
                forbidden = [kw for kw in forbidden_keywords if kw.lower() in patch_lower]
                if forbidden:
                    passed = False
                    message = "patch contains forbidden keywords"
                    evidence["forbidden_keywords"] = forbidden

            if passed and required_paths:
                missing_paths = [p for p in required_paths if str(p).strip() and str(p).strip() not in touched_set]
                if missing_paths:
                    passed = False
                    message = "missing required touched paths"
                    evidence["missing_touched_paths"] = missing_paths

            if not expected_keywords and not forbidden_keywords and not required_paths:
                # When params are missing, consider the criterion vacuously satisfied.
                # This keeps acceptance backward-compatible while still allowing explicit checks.
                passed = True

        elif mode == "operational_spec":
            try:
                parsed = parse_operational_validity_params(sc.params)
            except OperationalValidityParamsError as e:
                passed = False
                message = f"operational_spec params invalid: {e}"
                evidence["error"] = str(e)
            else:
                if parsed is None:
                    passed = False
                    message = "operational_spec requires structured params (spec_version, window, predicate_kind, …)"
                else:
                    evidence["operational_spec_version"] = int(parsed.spec_version)
                    evidence["evaluation_phase"] = str(parsed.evaluation_phase)
                    if parsed.bundle_schema_version is not None:
                        evidence["expected_bundle_schema_version"] = int(parsed.bundle_schema_version)
                    if parsed.evaluation_phase == "post_runtime":
                        # Deploy-time only: bounded checks are skipped at compile (no fail-closed).
                        passed = True
                        evidence["sub_status"] = "skipped"
                        evidence["note"] = (
                            "operational_spec evaluation_phase=post_runtime: deferred to post-runtime attestation "
                            "(operational_validity_report)"
                        )
                    else:
                        if parsed.window == "rolling_ms":
                            passed = False
                            message = (
                                "operational_spec window=rolling_ms is only evaluated post-runtime "
                                "via operational_evidence_window.v1 rollup (not compile-time accounting bundle)"
                            )
                        elif not isinstance(accounting.get("operational_compile_bundle"), Mapping):
                            passed = False
                            message = (
                                "operational_spec evaluation_phase=compile requires "
                                "accounting.operational_compile_bundle"
                            )
                        else:
                            bundle = cast(Mapping[str, Any], accounting.get("operational_compile_bundle"))
                            records = _runtime_evidence_records_from_bundle(bundle)
                            if not records:
                                passed = False
                                message = (
                                    "operational_spec evaluation_phase=compile requires non-empty "
                                    "operational_compile_bundle.runtime_evidence_records"
                                )
                            else:
                                otel_raw = bundle.get("otel_ndjson_text")
                                otel_text = str(otel_raw) if otel_raw is not None else ""
                                otel_recs = parse_otel_ndjson_slice(otel_text) if otel_text.strip() else None
                                oc = bundle.get("otel_contract")
                                otel_contract = dict(oc) if isinstance(oc, dict) else None
                                bsv_raw = bundle.get("runtime_bundle_schema_version")
                                bsv = (
                                    int(bsv_raw)
                                    if isinstance(bsv_raw, (int, float)) and not isinstance(bsv_raw, bool)
                                    else None
                                )
                                otel_metric_recs = None
                                otel_metric_rej = None
                                om_raw = bundle.get("otel_metric_ndjson_text")
                                if om_raw is not None:
                                    m_out = parse_otel_metric_ndjson_slice(str(om_raw))
                                    otel_metric_recs = m_out.records
                                    otel_metric_rej = m_out.rejected_reason
                                verdict = evaluate_operational_spec(
                                    params=parsed,
                                    evidence=records,
                                    otel_records=otel_recs,
                                    otel_contract=otel_contract,
                                    runtime_bundle_schema_version=bsv,
                                    success_criterion_id=sc_id,
                                    otel_metric_records=otel_metric_recs,
                                    otel_metric_parse_rejected_reason=otel_metric_rej,
                                )
                                passed = bool(verdict.passed)
                                if not passed and verdict.failures:
                                    message = str(verdict.failures[0])
                                elif not passed:
                                    message = "operational_spec compile-time evaluation failed"
                                evidence["operational_verdict"] = cast(JSONValue, verdict.to_step_output_obj())

        elif mode == "metric_threshold":
            max_repairs = _as_optional_int(params.get("max_repair_iterations"))
            max_total_tokens = _as_optional_int(params.get("max_total_tokens"))
            max_wall_time_ms = _as_optional_int(params.get("max_wall_time_ms"))
            min_success = _as_optional_int(params.get("min_success_rate"))

            if max_repairs is not None:
                got = _as_optional_int(accounting.get("repair_iterations"))
                got_i = int(got or 0)
                if got_i > int(max_repairs):
                    passed = False
                    message = f"repair_iterations={got_i} > {max_repairs}"
                    evidence["repair_iterations"] = got_i
                    evidence["max_repair_iterations"] = int(max_repairs)

            if passed and max_total_tokens is not None:
                got2 = _as_optional_int(accounting.get("total_tokens"))
                got2_i = int(got2 or 0)
                if got2_i > int(max_total_tokens):
                    passed = False
                    message = f"total_tokens={got2_i} > {max_total_tokens}"
                    evidence["total_tokens"] = got2_i
                    evidence["max_total_tokens"] = int(max_total_tokens)

            if passed and max_wall_time_ms is not None and int(wall_time_ms) > int(max_wall_time_ms):
                passed = False
                message = f"wall_time_ms={int(wall_time_ms)} > {max_wall_time_ms}"
                evidence["wall_time_ms"] = int(wall_time_ms)
                evidence["max_wall_time_ms"] = int(max_wall_time_ms)

            if passed and min_success is not None:
                # `min_success_rate` is a suite-level metric; we don't have it here.
                # Keep it deterministic and conservative: require explicit params for compile-time only checks.
                passed = False
                message = "metric_threshold.min_success_rate is not supported at compile-time"

        elif mode == "human_gate":
            # Deterministic proxy for manual review.
            # Fail-closed unless explicit approval is provided.
            approved = (
                _as_bool(params.get("approved"))
                or _as_bool(params.get("auto_approved"))
                or _as_bool(params.get("pass_in_ci"))
            )
            passed = bool(approved)
            if not passed:
                message = "human_gate requires approved/auto_approved/pass_in_ci"
                evidence["approved"] = bool(approved)

        else:
            passed = False
            message = f"unsupported evaluation_mode: {mode}"
            evidence["evaluation_mode"] = str(mode)

        check_obj: dict[str, JSONValue] = {
            "success_criterion_id": str(sc_id),
            "evaluation_mode": str(mode),
            "passed": bool(passed),
            "message": str(message) if message else "",
            "evidence": _ensure_json_value_dict(evidence) if evidence else None,
        }
        # Ensure stable serialization: drop empty message
        if not check_obj.get("message"):
            check_obj.pop("message", None)
        checks.append(check_obj)
        if not passed:
            failures.append(f"{sc_id} ({mode}): {message or 'failed'}")

    return IntentAcceptance(
        passed=len(failures) == 0,
        failures=tuple(failures),
        evaluated_count=len(criteria),
        per_criterion=tuple(checks),
    )
