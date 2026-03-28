from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
from typing import Any, Final, Literal, TypeAlias, cast

from akc.artifacts.contracts import apply_schema_envelope
from akc.memory.models import (
    JSONValue,
    goal_fingerprint,
    json_dumps,
    new_uuid,
    normalize_repo_id,
    now_ms,
    require_non_empty,
)
from akc.utils.fingerprint import stable_json_fingerprint

IntentStatus = Literal["draft", "active", "superseded", "archived"]

ConstraintKind = Literal["hard", "soft"]
EvaluationMode = Literal[
    "tests",
    "manifest_check",
    "artifact_check",
    "metric_threshold",
    "operational_spec",
    "quality_contract",
    "human_gate",
]

QualityDimensionId = Literal[
    "taste",
    "domain_knowledge",
    "judgment",
    "instincts",
    "user_empathy",
    "engineering_discipline",
]
ALLOWED_QUALITY_DIMENSION_IDS: tuple[QualityDimensionId, ...] = (
    "taste",
    "domain_knowledge",
    "judgment",
    "instincts",
    "user_empathy",
    "engineering_discipline",
)
CRITICAL_QUALITY_GATE_DIMENSION_IDS: tuple[QualityDimensionId, ...] = (
    "domain_knowledge",
    "judgment",
    "engineering_discipline",
)
QualityEnforcementStage = Literal["advisory", "gate"]

OperationalValidityWindow = Literal["single_run", "rolling_ms"]
OperationalPredicateKind = Literal["threshold", "presence"]
OperationalThresholdComparator = Literal["gte", "lte", "eq"]
OperationalEvaluationPhase = Literal["compile", "post_runtime"]
OperationalCompositeKind = Literal["span_status_fraction", "metric_counter_ratio"]

# Declarative expected_evidence_types entry: metrics sidecar present (see docs/runtime-execution.md).
AKC_OTEL_METRICS_EXPORT_EVIDENCE_TYPE: Final[str] = "akc_otel_metrics_export"


class IntentModelError(Exception):
    """Raised when an Intent model cannot be validated or serialized."""


class OperationalValidityParamsError(IntentModelError):
    """Raised when ``operational_spec`` success-criterion params are invalid."""


# Intent JSON must not carry credentials or ad-hoc external query text; only these keys are accepted.
_ALLOWED_OPERATIONAL_PARAMS_KEYS: Final[frozenset[str]] = frozenset(
    {
        "spec_version",
        "window",
        "predicate_kind",
        "signals",
        "rolling_window_ms",
        "threshold",
        "threshold_comparator",
        "bundle_schema_version",
        "expected_evidence_types",
        "evaluation_phase",
        "max_resync_attempts_bound",
        "reject_failed_aggregate_terminal_health",
        "evidence_rollup_rel_path",
        "otel_metric_signals",
        "composite_predicates",
    }
)
_ALLOWED_SIGNAL_SELECTOR_KEYS: Final[frozenset[str]] = frozenset(
    {"evidence_type", "payload_path", "otel_query_stub", "validator_stub"}
)
_ALLOWED_OTEL_METRIC_SIGNAL_KEYS: Final[frozenset[str]] = frozenset({"metric_name", "attributes"})
_ALLOWED_COMPOSITE_NUMERATOR_KEYS: Final[frozenset[str]] = frozenset({"metric_name", "attributes"})
_ALLOWED_SPAN_STATUS_FRACTION_KEYS: Final[frozenset[str]] = frozenset(
    {
        "kind",
        "status_good_value",
        "comparator",
        "target",
        "max_spans",
        "span_name",
        "span_attributes",
    }
)
_ALLOWED_METRIC_COUNTER_RATIO_KEYS: Final[frozenset[str]] = frozenset(
    {
        "kind",
        "comparator",
        "target",
        "numerator",
        "denominator",
        "max_metric_points",
        "max_metric_series",
    }
)
_OTEL_QUERY_STUB_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[-A-Za-z0-9_.]{1,128}$")


def _reject_unknown_keys(params: Mapping[str, Any], allowed: frozenset[str], *, context: str) -> None:
    for k in params:
        if k not in allowed:
            raise OperationalValidityParamsError(f"{context}: unknown key {k!r}")


def _validate_payload_path_no_secrets(payload_path: str | None) -> None:
    if payload_path is None:
        return
    lower = str(payload_path).lower()
    for needle in ("authorization", "bearer ", "bearer:", "://", "api_key", "password", "secret"):
        if needle in lower:
            raise OperationalValidityParamsError(
                "operational_signal.payload_path must not contain credential-like substrings"
            )


def _validate_evidence_rollup_rel_path(raw: str | None) -> str | None:
    """Require ``.akc/verification/…`` relative paths for cross-run evidence rollups."""

    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    p = Path(s)
    if p.is_absolute():
        raise ValueError("operational_spec evidence_rollup_rel_path must be a relative path")
    if ".." in p.parts:
        raise ValueError("operational_spec evidence_rollup_rel_path must not contain '..' segments")
    parts = p.parts
    if len(parts) < 3 or parts[0] != ".akc" or parts[1] != "verification":
        raise ValueError(
            "operational_spec evidence_rollup_rel_path must be under .akc/verification/ "
            "(operational evidence rollup artifact)"
        )
    return s


def _validate_otel_query_stub_value(stub: str | None) -> None:
    if stub is None:
        return
    if not _OTEL_QUERY_STUB_PATTERN.fullmatch(stub):
        raise OperationalValidityParamsError(
            "operational_signal.otel_query_stub must be an opaque id (1–128 chars, "
            "[-A-Za-z0-9_.] only); external metric query bindings belong in operator config outside intent JSON"
        )


def _validate_validator_stub_value(stub: str | None) -> None:
    if stub is None:
        return
    if not _OTEL_QUERY_STUB_PATTERN.fullmatch(stub):
        raise OperationalValidityParamsError(
            "operational_signal.validator_stub must be an opaque id (1–128 chars, "
            "[-A-Za-z0-9_.] only); validator bindings belong in operator config outside intent JSON"
        )


@dataclass(frozen=True, slots=True)
class OperationalSignalSelector:
    """Selects runtime evidence or telemetry for an operational validity criterion.

    ``payload_path`` is a dotted path (or JSON-pointer-like) into the evidence
    item ``payload`` object — metadata only, not evaluated at intent-parse time.
    ``validator_stub`` is the canonical opaque binding id for operator-configured
    validator execution. ``otel_query_stub`` remains accepted for backward
    compatibility and is normalized to the same binding id semantics.
    """

    evidence_type: str
    payload_path: str | None = None
    validator_stub: str | None = None
    otel_query_stub: str | None = None

    def __post_init__(self) -> None:
        require_non_empty(self.evidence_type, name="operational_signal.evidence_type")
        if self.payload_path is not None:
            require_non_empty(self.payload_path, name="operational_signal.payload_path")
        if self.validator_stub is not None:
            require_non_empty(self.validator_stub, name="operational_signal.validator_stub")
        if self.otel_query_stub is not None:
            require_non_empty(self.otel_query_stub, name="operational_signal.otel_query_stub")
        _validate_validator_stub_value(self.validator_stub)
        _validate_otel_query_stub_value(self.otel_query_stub)
        if (
            self.validator_stub is not None
            and self.otel_query_stub is not None
            and self.validator_stub.strip() != self.otel_query_stub.strip()
        ):
            raise ValueError("operational_signal.validator_stub and otel_query_stub must match when both are set")

    @property
    def binding_stub(self) -> str | None:
        raw = self.validator_stub or self.otel_query_stub
        if raw is None:
            return None
        s = raw.strip()
        return s if s else None

    def to_json_obj(self) -> dict[str, JSONValue]:
        obj: dict[str, JSONValue] = {
            "evidence_type": self.evidence_type,
            "payload_path": self.payload_path,
            "validator_stub": self.binding_stub,
        }
        return {k: v for k, v in obj.items() if v is not None}

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> OperationalSignalSelector:
        if not isinstance(obj, Mapping):
            raise OperationalValidityParamsError("operational signal selector must be a JSON object")
        _reject_unknown_keys(obj, _ALLOWED_SIGNAL_SELECTOR_KEYS, context="operational_signal")
        pp = cast(str | None, obj.get("payload_path"))
        validator_stub_raw = obj.get("validator_stub")
        validator_stub: str | None = None
        if validator_stub_raw is not None:
            stripped = str(validator_stub_raw).strip()
            if stripped:
                _validate_validator_stub_value(stripped)
                validator_stub = stripped
        otel_stub_raw = obj.get("otel_query_stub")
        otel_stub: str | None = None
        if otel_stub_raw is not None:
            stripped = str(otel_stub_raw).strip()
            if stripped:
                _validate_otel_query_stub_value(stripped)
                otel_stub = stripped
        _validate_payload_path_no_secrets(pp)
        try:
            return OperationalSignalSelector(
                evidence_type=str(obj.get("evidence_type", "")),
                payload_path=pp,
                validator_stub=validator_stub,
                otel_query_stub=otel_stub,
            )
        except ValueError as e:
            raise OperationalValidityParamsError(str(e)) from e


def _normalize_string_attributes(raw: object, *, context: str) -> tuple[tuple[str, str], ...]:
    if raw is None:
        return ()
    if not isinstance(raw, dict):
        raise OperationalValidityParamsError(f"{context} must be an object when set")
    out: list[tuple[str, str]] = []
    for k, v in raw.items():
        if not isinstance(k, str) or not k.strip():
            raise OperationalValidityParamsError(f"{context} keys must be non-empty strings")
        if not isinstance(v, str):
            raise OperationalValidityParamsError(f"{context}[{k!r}] must be a string for deterministic matching")
        out.append((k.strip(), v.strip()))
    return tuple(sorted(out))


@dataclass(frozen=True, slots=True)
class OperationalOtelMetricSignalSelector:
    """Selects exported AKC metric NDJSON points by ``metric.name`` and optional attribute equality."""

    metric_name: str
    attributes: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        require_non_empty(self.metric_name, name="otel_metric_signal.metric_name")

    def to_json_obj(self) -> dict[str, JSONValue]:
        obj: dict[str, JSONValue] = {"metric_name": self.metric_name}
        if self.attributes:
            obj["attributes"] = dict(self.attributes)
        return obj

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> OperationalOtelMetricSignalSelector:
        if not isinstance(obj, Mapping):
            raise OperationalValidityParamsError("otel_metric_signal must be a JSON object")
        _reject_unknown_keys(obj, _ALLOWED_OTEL_METRIC_SIGNAL_KEYS, context="otel_metric_signal")
        attrs = _normalize_string_attributes(obj.get("attributes"), context="otel_metric_signal.attributes")
        try:
            return OperationalOtelMetricSignalSelector(metric_name=str(obj.get("metric_name", "")), attributes=attrs)
        except ValueError as e:
            raise OperationalValidityParamsError(str(e)) from e


@dataclass(frozen=True, slots=True)
class OperationalCompositePredicate:
    """Bounded SLI-style predicate evaluated only over exported NDJSON bundles (no PromQL / TSDB)."""

    kind: OperationalCompositeKind
    comparator: OperationalThresholdComparator
    target: float
    status_good_value: str | None = None
    max_spans: int | None = None
    span_name: str | None = None
    span_attributes: tuple[tuple[str, str], ...] = ()
    numerator_metric_name: str | None = None
    numerator_attributes: tuple[tuple[str, str], ...] = ()
    denominator_metric_name: str | None = None
    denominator_attributes: tuple[tuple[str, str], ...] = ()
    max_metric_points: int | None = None
    max_metric_series: int | None = None

    def to_json_obj(self) -> dict[str, JSONValue]:
        base: dict[str, JSONValue] = {
            "kind": self.kind,
            "comparator": self.comparator,
            "target": float(self.target),
        }
        if self.kind == "span_status_fraction":
            base["status_good_value"] = self.status_good_value
            base["max_spans"] = int(self.max_spans) if self.max_spans is not None else None
            if self.span_name is not None:
                base["span_name"] = self.span_name
            if self.span_attributes:
                base["span_attributes"] = dict(self.span_attributes)
        else:
            base["numerator"] = {
                "metric_name": self.numerator_metric_name,
                **({"attributes": dict(self.numerator_attributes)} if self.numerator_attributes else {}),
            }
            base["denominator"] = {
                "metric_name": self.denominator_metric_name,
                **({"attributes": dict(self.denominator_attributes)} if self.denominator_attributes else {}),
            }
            base["max_metric_points"] = int(self.max_metric_points) if self.max_metric_points is not None else None
            base["max_metric_series"] = int(self.max_metric_series) if self.max_metric_series is not None else None
        return {k: v for k, v in base.items() if v is not None}

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> OperationalCompositePredicate:
        if not isinstance(obj, Mapping):
            raise OperationalValidityParamsError("composite_predicate must be a JSON object")
        kind_raw = str(obj.get("kind", "")).strip()
        if kind_raw not in ("span_status_fraction", "metric_counter_ratio"):
            raise OperationalValidityParamsError(
                "composite_predicate.kind must be span_status_fraction or metric_counter_ratio"
            )
        kind = cast(OperationalCompositeKind, kind_raw)
        allowed = (
            _ALLOWED_SPAN_STATUS_FRACTION_KEYS if kind == "span_status_fraction" else _ALLOWED_METRIC_COUNTER_RATIO_KEYS
        )
        _reject_unknown_keys(obj, allowed, context="composite_predicate")
        tc_raw = obj.get("comparator")
        tcs = str(tc_raw).strip() if tc_raw is not None else ""
        if tcs not in ("gte", "lte", "eq"):
            raise OperationalValidityParamsError("composite_predicate.comparator must be gte, lte, or eq")
        comparator = cast(OperationalThresholdComparator, tcs)
        targ_raw = obj.get("target")
        if isinstance(targ_raw, bool) or not isinstance(targ_raw, (int, float)):
            raise OperationalValidityParamsError("composite_predicate.target must be a number")
        target = float(targ_raw)
        if kind == "span_status_fraction":
            sgood = str(obj.get("status_good_value", "")).strip()
            if not sgood:
                raise OperationalValidityParamsError("span_status_fraction requires status_good_value")
            ms = obj.get("max_spans")
            if isinstance(ms, bool) or not isinstance(ms, (int, float)):
                raise OperationalValidityParamsError("span_status_fraction requires integer max_spans >= 1")
            max_spans = int(ms)
            if max_spans < 1:
                raise OperationalValidityParamsError("span_status_fraction.max_spans must be >= 1")
            sn_raw = obj.get("span_name")
            span_name: str | None = None
            if sn_raw is not None:
                if not isinstance(sn_raw, str) or not str(sn_raw).strip():
                    raise OperationalValidityParamsError(
                        "composite_predicate.span_name must be a non-empty string when set"
                    )
                span_name = str(sn_raw).strip()
            sa_raw = obj.get("span_attributes")
            span_attrs = _normalize_string_attributes(sa_raw, context="span_status_fraction.span_attributes")
            if not (0.0 <= target <= 1.0):
                raise OperationalValidityParamsError("span_status_fraction.target must be between 0 and 1 inclusive")
            return OperationalCompositePredicate(
                kind=kind,
                comparator=comparator,
                target=target,
                status_good_value=sgood,
                max_spans=max_spans,
                span_name=span_name,
                span_attributes=span_attrs,
            )
        # metric_counter_ratio
        num_raw = obj.get("numerator")
        den_raw = obj.get("denominator")
        if not isinstance(num_raw, dict) or not isinstance(den_raw, dict):
            raise OperationalValidityParamsError("metric_counter_ratio requires numerator and denominator objects")
        _reject_unknown_keys(num_raw, _ALLOWED_COMPOSITE_NUMERATOR_KEYS, context="composite_predicate.numerator")
        _reject_unknown_keys(den_raw, _ALLOWED_COMPOSITE_NUMERATOR_KEYS, context="composite_predicate.denominator")
        n_name = str(num_raw.get("metric_name", "")).strip()
        d_name = str(den_raw.get("metric_name", "")).strip()
        if not n_name or not d_name:
            raise OperationalValidityParamsError("metric_counter_ratio numerator/denominator require metric_name")
        n_attr = _normalize_string_attributes(
            num_raw.get("attributes"),
            context="composite_predicate.numerator.attributes",
        )
        d_attr = _normalize_string_attributes(
            den_raw.get("attributes"),
            context="composite_predicate.denominator.attributes",
        )
        mpts = obj.get("max_metric_points")
        mser = obj.get("max_metric_series")
        if isinstance(mpts, bool) or not isinstance(mpts, (int, float)):
            raise OperationalValidityParamsError("metric_counter_ratio requires integer max_metric_points >= 1")
        if isinstance(mser, bool) or not isinstance(mser, (int, float)):
            raise OperationalValidityParamsError("metric_counter_ratio requires integer max_metric_series >= 1")
        max_metric_points = int(mpts)
        max_metric_series = int(mser)
        if max_metric_points < 1:
            raise OperationalValidityParamsError("metric_counter_ratio.max_metric_points must be >= 1")
        if max_metric_series < 1:
            raise OperationalValidityParamsError("metric_counter_ratio.max_metric_series must be >= 1")
        if not (0.0 <= target <= 1.0):
            raise OperationalValidityParamsError("metric_counter_ratio.target must be between 0 and 1 inclusive")
        return OperationalCompositePredicate(
            kind=kind,
            comparator=comparator,
            target=target,
            numerator_metric_name=n_name,
            numerator_attributes=n_attr,
            denominator_metric_name=d_name,
            denominator_attributes=d_attr,
            max_metric_points=max_metric_points,
            max_metric_series=max_metric_series,
        )


@dataclass(frozen=True, slots=True)
class OperationalValidityParams:
    """Structured ``params`` for ``evaluation_mode == \"operational_spec\"`` (v1).

    ``spec_version`` versions this params object only (not the intent spec).
    Use ``bundle_schema_version`` to pin expected ``runtime_bundle`` envelope
    versions; use ``expected_evidence_types`` for coarse stream requirements.

    Do not embed credentials or external metric query strings; unknown keys in the
    params object are rejected at parse time.
    """

    spec_version: int
    window: OperationalValidityWindow
    predicate_kind: OperationalPredicateKind
    signals: tuple[OperationalSignalSelector, ...]
    rolling_window_ms: int | None = None
    threshold: float | None = None
    threshold_comparator: OperationalThresholdComparator | None = None
    bundle_schema_version: int | None = None
    expected_evidence_types: tuple[str, ...] = ()
    # compile: evaluate against ``accounting.operational_compile_bundle`` during compile.
    # post_runtime: skip bounded compile-time evaluation (deploy-time attestation only).
    evaluation_phase: OperationalEvaluationPhase = "post_runtime"
    # When set, require reconcile_resource_status.resync_completed_attempts <= bound when rows exist.
    max_resync_attempts_bound: int | None = None
    # When True, fail if aggregate terminal health resolves to ``failed`` (stricter than presence-only).
    reject_failed_aggregate_terminal_health: bool = False
    # Relative to tenant/repo outputs root; required when window=rolling_ms (rollup JSON).
    evidence_rollup_rel_path: str | None = None
    # NDJSON metric selectors (``{run_id}.otel_metrics.jsonl``); evaluated only on exported points.
    otel_metric_signals: tuple[OperationalOtelMetricSignalSelector, ...] = ()
    # Bounded SLI-style predicates over trace/metric NDJSON (no live TSDB queries).
    composite_predicates: tuple[OperationalCompositePredicate, ...] = ()

    def __post_init__(self) -> None:
        if int(self.spec_version) < 1:
            raise ValueError("operational_spec.params.spec_version must be >= 1")
        if self.window == "rolling_ms":
            if self.rolling_window_ms is None or int(self.rolling_window_ms) <= 0:
                raise ValueError("operational_spec window=rolling_ms requires rolling_window_ms > 0")
            if not (self.evidence_rollup_rel_path or "").strip():
                raise ValueError(
                    "operational_spec window=rolling_ms requires evidence_rollup_rel_path "
                    "(operational_evidence_window.v1 rollup under .akc/verification/)"
                )
        elif self.rolling_window_ms is not None:
            raise ValueError("operational_spec rolling_window_ms is only valid when window=rolling_ms")
        elif self.evidence_rollup_rel_path is not None:
            raise ValueError("operational_spec evidence_rollup_rel_path is only valid when window=rolling_ms")
        if self.predicate_kind == "threshold":
            if self.threshold is None or self.threshold_comparator is None:
                raise ValueError(
                    "operational_spec predicate_kind=threshold requires threshold and threshold_comparator"
                )
        else:
            if self.threshold is not None or self.threshold_comparator is not None:
                raise ValueError("operational_spec threshold fields are only valid when predicate_kind=threshold")
        if (
            not self.signals
            and not self.expected_evidence_types
            and not self.otel_metric_signals
            and not self.composite_predicates
        ):
            raise ValueError(
                "operational_spec requires at least one signal, expected_evidence_types entry, "
                "otel_metric_signal, or composite_predicate"
            )
        if self.evaluation_phase not in ("compile", "post_runtime"):
            raise ValueError("operational_spec.params.evaluation_phase must be compile or post_runtime")
        if self.max_resync_attempts_bound is not None and int(self.max_resync_attempts_bound) < 1:
            raise ValueError("operational_spec.params.max_resync_attempts_bound must be >= 1 when set")

    def to_json_obj(self) -> dict[str, JSONValue]:
        obj: dict[str, JSONValue] = {
            "spec_version": int(self.spec_version),
            "window": self.window,
            "rolling_window_ms": int(self.rolling_window_ms) if self.rolling_window_ms is not None else None,
            "predicate_kind": self.predicate_kind,
            "signals": [s.to_json_obj() for s in self.signals],
            "threshold": float(self.threshold) if self.threshold is not None else None,
            "threshold_comparator": self.threshold_comparator,
            "bundle_schema_version": (
                int(self.bundle_schema_version) if self.bundle_schema_version is not None else None
            ),
            "expected_evidence_types": list(self.expected_evidence_types),
        }
        # Omit default so golden fingerprints for operational_eval stay stable.
        if self.evaluation_phase != "post_runtime":
            obj["evaluation_phase"] = self.evaluation_phase
        if self.max_resync_attempts_bound is not None:
            obj["max_resync_attempts_bound"] = int(self.max_resync_attempts_bound)
        if self.reject_failed_aggregate_terminal_health:
            obj["reject_failed_aggregate_terminal_health"] = True
        if self.evidence_rollup_rel_path is not None:
            obj["evidence_rollup_rel_path"] = self.evidence_rollup_rel_path
        if self.otel_metric_signals:
            obj["otel_metric_signals"] = [s.to_json_obj() for s in self.otel_metric_signals]
        if self.composite_predicates:
            obj["composite_predicates"] = [p.to_json_obj() for p in self.composite_predicates]
        return {k: v for k, v in obj.items() if v is not None}

    @staticmethod
    def from_mapping(params: Mapping[str, Any]) -> OperationalValidityParams:
        if not isinstance(params, Mapping):
            raise OperationalValidityParamsError("operational_spec.params must be a JSON object")
        _reject_unknown_keys(params, _ALLOWED_OPERATIONAL_PARAMS_KEYS, context="operational_spec.params")
        spec_version = int(params.get("spec_version", 0) or 0)
        window = str(params.get("window", "")).strip()
        if window not in ("single_run", "rolling_ms"):
            raise OperationalValidityParamsError("operational_spec.params.window must be single_run or rolling_ms")
        pred = str(params.get("predicate_kind", "")).strip()
        if pred not in ("threshold", "presence"):
            raise OperationalValidityParamsError("operational_spec.params.predicate_kind must be threshold or presence")
        raw_signals = params.get("signals")
        signals: tuple[OperationalSignalSelector, ...] = ()
        if raw_signals is not None:
            if not isinstance(raw_signals, list):
                raise OperationalValidityParamsError("operational_spec.params.signals must be an array when set")
            signals = tuple(OperationalSignalSelector.from_json_obj(cast(Mapping[str, Any], x)) for x in raw_signals)
        rw = params.get("rolling_window_ms")
        rolling: int | None = None
        if rw is not None:
            if isinstance(rw, bool) or not isinstance(rw, (int, float)):
                raise OperationalValidityParamsError("operational_spec.params.rolling_window_ms must be an integer")
            rolling = int(rw)
        thr_raw = params.get("threshold")
        threshold: float | None = None
        if thr_raw is not None:
            if isinstance(thr_raw, bool) or not isinstance(thr_raw, (int, float)):
                raise OperationalValidityParamsError("operational_spec.params.threshold must be a number")
            threshold = float(thr_raw)
        tc_raw = params.get("threshold_comparator")
        tc: OperationalThresholdComparator | None = None
        if tc_raw is not None:
            tcs = str(tc_raw).strip()
            if tcs not in ("gte", "lte", "eq"):
                raise OperationalValidityParamsError(
                    "operational_spec.params.threshold_comparator must be gte, lte, or eq"
                )
            tc = cast(OperationalThresholdComparator, tcs)
        bsv = params.get("bundle_schema_version")
        bundle_v: int | None = None
        if bsv is not None:
            if isinstance(bsv, bool) or not isinstance(bsv, (int, float)):
                raise OperationalValidityParamsError("operational_spec.params.bundle_schema_version must be an integer")
            bundle_v = int(bsv)
        ev_raw = params.get("expected_evidence_types")
        ev_types: tuple[str, ...] = ()
        if ev_raw is not None:
            if not isinstance(ev_raw, list):
                raise OperationalValidityParamsError("operational_spec.params.expected_evidence_types must be an array")
            ev_types = tuple(str(x).strip() for x in ev_raw if str(x).strip())
        ep_raw = params.get("evaluation_phase")
        if ep_raw is None:
            eval_phase: OperationalEvaluationPhase = "post_runtime"
        else:
            eps = str(ep_raw).strip()
            if eps not in ("compile", "post_runtime"):
                raise OperationalValidityParamsError(
                    "operational_spec.params.evaluation_phase must be compile or post_runtime"
                )
            eval_phase = cast(OperationalEvaluationPhase, eps)
        mra_raw = params.get("max_resync_attempts_bound")
        max_resync_attempts_bound: int | None = None
        if mra_raw is not None:
            if isinstance(mra_raw, bool) or not isinstance(mra_raw, (int, float)):
                raise OperationalValidityParamsError(
                    "operational_spec.params.max_resync_attempts_bound must be an integer when set"
                )
            max_resync_attempts_bound = int(mra_raw)
        rfh_raw = params.get("reject_failed_aggregate_terminal_health")
        reject_failed_agg = rfh_raw is True
        if rfh_raw is not None and not isinstance(rfh_raw, bool):
            raise OperationalValidityParamsError(
                "operational_spec.params.reject_failed_aggregate_terminal_health must be a boolean when set"
            )
        rollup_raw = params.get("evidence_rollup_rel_path")
        evidence_rollup: str | None = None
        if rollup_raw is not None:
            if not isinstance(rollup_raw, str):
                raise OperationalValidityParamsError(
                    "operational_spec.params.evidence_rollup_rel_path must be a string when set"
                )
            try:
                evidence_rollup = _validate_evidence_rollup_rel_path(rollup_raw)
            except ValueError as e:
                raise OperationalValidityParamsError(str(e)) from e
        oms_raw = params.get("otel_metric_signals")
        otel_metric_signals: tuple[OperationalOtelMetricSignalSelector, ...] = ()
        if oms_raw is not None:
            if not isinstance(oms_raw, list):
                raise OperationalValidityParamsError(
                    "operational_spec.params.otel_metric_signals must be an array when set"
                )
            otel_metric_signals = tuple(
                OperationalOtelMetricSignalSelector.from_json_obj(cast(Mapping[str, Any], x)) for x in oms_raw
            )
        cp_raw = params.get("composite_predicates")
        composite_predicates: tuple[OperationalCompositePredicate, ...] = ()
        if cp_raw is not None:
            if not isinstance(cp_raw, list):
                raise OperationalValidityParamsError(
                    "operational_spec.params.composite_predicates must be an array when set"
                )
            composite_predicates = tuple(
                OperationalCompositePredicate.from_json_obj(cast(Mapping[str, Any], x)) for x in cp_raw
            )
        try:
            return OperationalValidityParams(
                spec_version=spec_version,
                window=cast(OperationalValidityWindow, window),
                predicate_kind=cast(OperationalPredicateKind, pred),
                signals=signals,
                rolling_window_ms=rolling,
                threshold=threshold,
                threshold_comparator=tc,
                bundle_schema_version=bundle_v,
                expected_evidence_types=ev_types,
                evaluation_phase=eval_phase,
                max_resync_attempts_bound=max_resync_attempts_bound,
                reject_failed_aggregate_terminal_health=reject_failed_agg,
                evidence_rollup_rel_path=evidence_rollup,
                otel_metric_signals=otel_metric_signals,
                composite_predicates=composite_predicates,
            )
        except ValueError as e:
            raise OperationalValidityParamsError(str(e)) from e


def parse_operational_validity_params(
    params: Mapping[str, JSONValue] | None,
) -> OperationalValidityParams | None:
    """Parse structured operational params, or return None when ``params`` is empty."""

    if not params:
        return None
    return OperationalValidityParams.from_mapping(cast(Mapping[str, Any], params))


@dataclass(frozen=True, slots=True)
class QualityDimensionSpec:
    """One quality dimension configuration for the intent quality contract."""

    target_score: float
    gate_min_score: float | None = None
    weight: float = 1.0
    evidence_requirements: tuple[str, ...] = ()
    enforcement_stage: QualityEnforcementStage = "advisory"

    def __post_init__(self) -> None:
        t = float(self.target_score)
        if t < 0.0 or t > 1.0:
            raise ValueError("quality_dimension.target_score must be within [0.0, 1.0]")
        if self.gate_min_score is not None:
            gm = float(self.gate_min_score)
            if gm < 0.0 or gm > 1.0:
                raise ValueError("quality_dimension.gate_min_score must be within [0.0, 1.0] when set")
        w = float(self.weight)
        if w <= 0.0:
            raise ValueError("quality_dimension.weight must be > 0")
        if self.enforcement_stage not in ("advisory", "gate"):
            raise ValueError("quality_dimension.enforcement_stage must be advisory or gate")
        for ev in self.evidence_requirements:
            require_non_empty(ev, name="quality_dimension.evidence_requirements[]")

    def to_json_obj(self) -> dict[str, JSONValue]:
        obj: dict[str, JSONValue] = {
            "target_score": float(self.target_score),
            "gate_min_score": float(self.gate_min_score) if self.gate_min_score is not None else None,
            "weight": float(self.weight),
            "evidence_requirements": list(self.evidence_requirements),
            "enforcement_stage": self.enforcement_stage,
        }
        return {k: v for k, v in obj.items() if v is not None}

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> QualityDimensionSpec:
        if not isinstance(obj, Mapping):
            raise IntentModelError("quality_contract dimension must be a JSON object")
        raw_target = obj.get("target_score")
        if isinstance(raw_target, bool) or not isinstance(raw_target, (int, float)):
            raise IntentModelError("quality_contract.target_score must be a number")
        raw_gate = obj.get("gate_min_score")
        gate_min: float | None = None
        if raw_gate is not None:
            if isinstance(raw_gate, bool) or not isinstance(raw_gate, (int, float)):
                raise IntentModelError("quality_contract.gate_min_score must be a number when set")
            gate_min = float(raw_gate)
        raw_weight = obj.get("weight", 1.0)
        if isinstance(raw_weight, bool) or not isinstance(raw_weight, (int, float)):
            raise IntentModelError("quality_contract.weight must be a number")
        raw_evidence = obj.get("evidence_requirements")
        evidence: tuple[str, ...] = ()
        if raw_evidence is not None:
            if not isinstance(raw_evidence, list):
                raise IntentModelError("quality_contract.evidence_requirements must be an array when set")
            evidence = tuple(str(x).strip() for x in raw_evidence if str(x).strip())
        stage_raw = str(obj.get("enforcement_stage", "advisory")).strip().lower()
        if stage_raw not in ("advisory", "gate"):
            raise IntentModelError("quality_contract.enforcement_stage must be advisory or gate")
        return QualityDimensionSpec(
            target_score=float(raw_target),
            gate_min_score=gate_min,
            weight=float(raw_weight),
            evidence_requirements=evidence,
            enforcement_stage=cast(QualityEnforcementStage, stage_raw),
        )


@dataclass(frozen=True, slots=True)
class QualityContract:
    """Intent-level quality contract for human-centric system outcomes."""

    dimensions: Mapping[QualityDimensionId, QualityDimensionSpec]

    def __post_init__(self) -> None:
        keys = {str(k).strip() for k in self.dimensions}
        expected = {str(k) for k in ALLOWED_QUALITY_DIMENSION_IDS}
        if keys != expected:
            missing = sorted(expected - keys)
            extra = sorted(keys - expected)
            parts: list[str] = []
            if missing:
                parts.append(f"missing={missing!r}")
            if extra:
                parts.append(f"extra={extra!r}")
            details = "; ".join(parts) if parts else "invalid quality dimensions"
            raise ValueError(f"quality_contract must define exactly six dimensions ({details})")

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {dim: self.dimensions[dim].to_json_obj() for dim in ALLOWED_QUALITY_DIMENSION_IDS}

    def prompt_compact_obj(self) -> dict[str, JSONValue]:
        """Bounded summary for compile prompt context."""

        out: dict[str, JSONValue] = {}
        for dim in ALLOWED_QUALITY_DIMENSION_IDS:
            d = self.dimensions[dim]
            out[dim] = {
                "target_score": float(d.target_score),
                "gate_min_score": float(d.gate_min_score) if d.gate_min_score is not None else None,
                "weight": float(d.weight),
                "enforcement_stage": d.enforcement_stage,
            }
        return out

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> QualityContract:
        if not isinstance(obj, Mapping):
            raise IntentModelError("quality_contract must be a JSON object")
        dims: dict[QualityDimensionId, QualityDimensionSpec] = {}
        for dim in ALLOWED_QUALITY_DIMENSION_IDS:
            raw = obj.get(dim)
            if not isinstance(raw, Mapping):
                raise IntentModelError(f"quality_contract.{dim} must be an object")
            dims[dim] = QualityDimensionSpec.from_json_obj(cast(Mapping[str, Any], raw))
        return QualityContract(dimensions=dims)

    @staticmethod
    def advisory_defaults(
        *,
        evidence_expectations: Mapping[str, Sequence[str]] | None = None,
    ) -> QualityContract:
        """Default all-dimension advisory profile used for safe global rollout."""

        dims: dict[QualityDimensionId, QualityDimensionSpec] = {}
        for dim in ALLOWED_QUALITY_DIMENSION_IDS:
            raw_expect: tuple[str, ...] = ()
            if isinstance(evidence_expectations, Mapping):
                raw_vals = evidence_expectations.get(dim, ())
                if isinstance(raw_vals, Sequence) and not isinstance(raw_vals, (str, bytes)):
                    raw_expect = tuple(str(x).strip() for x in raw_vals if str(x).strip())
            dims[dim] = QualityDimensionSpec(
                target_score=0.75,
                gate_min_score=0.6,
                weight=1.0,
                evidence_requirements=raw_expect,
                enforcement_stage="advisory",
            )
        return QualityContract(dimensions=dims)

    @staticmethod
    def critical_gate_defaults(
        *,
        evidence_expectations: Mapping[str, Sequence[str]] | None = None,
    ) -> QualityContract:
        """Default profile that hard-gates critical quality dimensions first."""

        gate_dims = set(CRITICAL_QUALITY_GATE_DIMENSION_IDS)
        dims: dict[QualityDimensionId, QualityDimensionSpec] = {}
        for dim in ALLOWED_QUALITY_DIMENSION_IDS:
            raw_expect: tuple[str, ...] = ()
            if isinstance(evidence_expectations, Mapping):
                raw_vals = evidence_expectations.get(dim, ())
                if isinstance(raw_vals, Sequence) and not isinstance(raw_vals, (str, bytes)):
                    raw_expect = tuple(str(x).strip() for x in raw_vals if str(x).strip())
            dims[dim] = QualityDimensionSpec(
                target_score=0.75,
                gate_min_score=0.6,
                weight=1.0,
                evidence_requirements=raw_expect,
                enforcement_stage="gate" if dim in gate_dims else "advisory",
            )
        return QualityContract(dimensions=dims)


def quality_contract_fingerprint(*, quality_contract: QualityContract | None) -> str | None:
    if quality_contract is None:
        return None
    return stable_json_fingerprint(quality_contract.to_json_obj())[:16]


_INTENT_SPEC_SCHEMA_KIND: Final[str] = "intent_spec"
_INTENT_SPEC_DEFAULT_VERSION: Final[int] = 1


def _sorted_by_id(items: tuple[Any, ...]) -> list[Any]:
    # Deterministic ordering for stable hashing.
    # Each item is expected to have an `id: str` attribute.
    return sorted(items, key=lambda x: str(getattr(x, "id", "")))


@dataclass(frozen=True, slots=True)
class Objective:
    id: str
    priority: int
    statement: str
    target: str | None = None
    metadata: Mapping[str, JSONValue] | None = None

    def __post_init__(self) -> None:
        require_non_empty(self.id, name="objective.id")
        if int(self.priority) < 0:
            raise ValueError("objective.priority must be >= 0")
        require_non_empty(self.statement, name="objective.statement")
        if self.target is not None:
            require_non_empty(self.target, name="objective.target")
        if self.metadata is not None:
            json_dumps(cast(JSONValue, dict(self.metadata)))

    def to_json_obj(self) -> dict[str, JSONValue]:
        obj: dict[str, JSONValue] = {
            "id": self.id,
            "priority": int(self.priority),
            "statement": self.statement,
            "target": self.target,
            "metadata": dict(self.metadata) if self.metadata is not None else None,
        }
        return {k: v for k, v in obj.items() if v is not None}

    def to_summary_obj(self) -> dict[str, JSONValue]:
        """Compact, prompt-friendly representation for active objectives."""
        obj: dict[str, JSONValue] = {
            "id": self.id,
            "priority": int(self.priority),
            "statement": self.statement,
            "target": self.target,
        }
        return {k: v for k, v in obj.items() if v is not None}

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> Objective:
        if not isinstance(obj, Mapping):
            raise IntentModelError("objective must be a JSON object")
        md = obj.get("metadata")
        if md is not None and not isinstance(md, dict):
            raise IntentModelError("objective.metadata must be an object when set")
        return Objective(
            id=str(obj.get("id", "")),
            priority=int(obj.get("priority", 0)),
            statement=str(obj.get("statement", "")),
            target=cast(str | None, obj.get("target")),
            metadata=cast(Mapping[str, JSONValue] | None, md),
        )


@dataclass(frozen=True, slots=True)
class Constraint:
    id: str
    kind: ConstraintKind
    statement: str
    metadata: Mapping[str, JSONValue] | None = None

    def __post_init__(self) -> None:
        require_non_empty(self.id, name="constraint.id")
        require_non_empty(self.kind, name="constraint.kind")
        require_non_empty(self.statement, name="constraint.statement")
        if self.metadata is not None:
            json_dumps(cast(JSONValue, dict(self.metadata)))

    def to_json_obj(self) -> dict[str, JSONValue]:
        obj: dict[str, JSONValue] = {
            "id": self.id,
            "kind": self.kind,
            "statement": self.statement,
            "metadata": dict(self.metadata) if self.metadata is not None else None,
        }
        return {k: v for k, v in obj.items() if v is not None}

    def to_summary_obj(self) -> dict[str, JSONValue]:
        """Compact, prompt-friendly representation for active constraints."""
        return {"id": self.id, "kind": self.kind, "summary": self.statement}

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> Constraint:
        if not isinstance(obj, Mapping):
            raise IntentModelError("constraint must be a JSON object")
        md = obj.get("metadata")
        if md is not None and not isinstance(md, dict):
            raise IntentModelError("constraint.metadata must be an object when set")
        return Constraint(
            id=str(obj.get("id", "")),
            kind=cast(ConstraintKind, str(obj.get("kind", "hard"))),
            statement=str(obj.get("statement", "")),
            metadata=cast(Mapping[str, JSONValue] | None, md),
        )


@dataclass(frozen=True, slots=True)
class PolicyRef:
    id: str
    # A stable "where did this policy come from" pointer; in Phase 1 we only
    # treat it as structured text for persistence/fingerprinting.
    source: str | None = None
    requirement: str | None = None
    metadata: Mapping[str, JSONValue] | None = None

    def __post_init__(self) -> None:
        require_non_empty(self.id, name="policy.id")
        if self.source is not None:
            require_non_empty(self.source, name="policy.source")
        if self.requirement is not None:
            require_non_empty(self.requirement, name="policy.requirement")
        if self.metadata is not None:
            json_dumps(cast(JSONValue, dict(self.metadata)))

    def to_json_obj(self) -> dict[str, JSONValue]:
        obj: dict[str, JSONValue] = {
            "id": self.id,
            "source": self.source,
            "requirement": self.requirement,
            "metadata": dict(self.metadata) if self.metadata is not None else None,
        }
        return {k: v for k, v in obj.items() if v is not None}

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> PolicyRef:
        if not isinstance(obj, Mapping):
            raise IntentModelError("policy must be a JSON object")
        md = obj.get("metadata")
        if md is not None and not isinstance(md, dict):
            raise IntentModelError("policy.metadata must be an object when set")
        return PolicyRef(
            id=str(obj.get("id", "")),
            source=cast(str | None, obj.get("source")),
            requirement=cast(str | None, obj.get("requirement")),
            metadata=cast(Mapping[str, JSONValue] | None, md),
        )


@dataclass(frozen=True, slots=True)
class SuccessCriterion:
    id: str
    evaluation_mode: EvaluationMode
    description: str
    # Optional structured parameters; can represent expected test names,
    # metric thresholds, artifact selectors, etc.
    params: Mapping[str, JSONValue] | None = None

    def __post_init__(self) -> None:
        require_non_empty(self.id, name="success_criterion.id")
        require_non_empty(self.evaluation_mode, name="success_criterion.evaluation_mode")
        require_non_empty(self.description, name="success_criterion.description")
        if self.params is not None:
            json_dumps(cast(JSONValue, dict(self.params)))

    def to_json_obj(self) -> dict[str, JSONValue]:
        obj: dict[str, JSONValue] = {
            "id": self.id,
            "evaluation_mode": self.evaluation_mode,
            "description": self.description,
            "params": dict(self.params) if self.params is not None else None,
        }
        return {k: v for k, v in obj.items() if v is not None}

    def to_summary_obj(self) -> dict[str, JSONValue]:
        """Compact, prompt-friendly representation for active acceptance checks."""
        obj: dict[str, JSONValue] = {
            "id": self.id,
            "evaluation_mode": self.evaluation_mode,
            "summary": self.description,
        }
        if self.params is not None:
            obj["params"] = dict(self.params)
        return obj

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> SuccessCriterion:
        if not isinstance(obj, Mapping):
            raise IntentModelError("success_criterion must be a JSON object")
        params = obj.get("params")
        if params is not None and not isinstance(params, dict):
            raise IntentModelError("success_criterion.params must be an object when set")
        return SuccessCriterion(
            id=str(obj.get("id", "")),
            evaluation_mode=cast(EvaluationMode, str(obj.get("evaluation_mode", "human_gate"))),
            description=str(obj.get("description", "")),
            params=cast(Mapping[str, JSONValue] | None, params),
        )


@dataclass(frozen=True, slots=True)
class ConstraintLink:
    """Typed link stored in PlanStep.inputs to reference a Constraint by id."""

    constraint_id: str
    kind: ConstraintKind
    summary: str

    @staticmethod
    def from_constraint(*, constraint: Constraint) -> ConstraintLink:
        return ConstraintLink(
            constraint_id=constraint.id,
            kind=constraint.kind,
            summary=constraint.statement,
        )

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {"constraint_id": self.constraint_id, "kind": self.kind, "summary": self.summary}


@dataclass(frozen=True, slots=True)
class SuccessCriterionLink:
    """Typed link stored in PlanStep.inputs to reference a SuccessCriterion by id."""

    success_criterion_id: str
    evaluation_mode: EvaluationMode
    summary: str

    @staticmethod
    def from_success_criterion(*, sc: SuccessCriterion) -> SuccessCriterionLink:
        return SuccessCriterionLink(
            success_criterion_id=sc.id,
            evaluation_mode=sc.evaluation_mode,
            summary=sc.description,
        )

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "success_criterion_id": self.success_criterion_id,
            "evaluation_mode": self.evaluation_mode,
            "summary": self.summary,
        }


@dataclass(frozen=True, slots=True)
class OperatingBound:
    # Treat bounds as requested authority; higher layers will intersect with
    # controller policy defaults.
    max_seconds: float | None = None
    max_steps: int | None = None
    max_input_tokens: int | None = None
    max_output_tokens: int | None = None
    allow_network: bool = False

    def __post_init__(self) -> None:
        if self.max_seconds is not None and float(self.max_seconds) <= 0:
            raise ValueError("operating_bounds.max_seconds must be > 0 when set")
        if self.max_steps is not None and int(self.max_steps) <= 0:
            raise ValueError("operating_bounds.max_steps must be > 0 when set")
        if self.max_input_tokens is not None and int(self.max_input_tokens) <= 0:
            raise ValueError("operating_bounds.max_input_tokens must be > 0 when set")
        if self.max_output_tokens is not None and int(self.max_output_tokens) <= 0:
            raise ValueError("operating_bounds.max_output_tokens must be > 0 when set")

    def to_json_obj(self) -> dict[str, JSONValue]:
        obj: dict[str, JSONValue] = {
            "max_seconds": float(self.max_seconds) if self.max_seconds is not None else None,
            "max_steps": int(self.max_steps) if self.max_steps is not None else None,
            "max_input_tokens": (int(self.max_input_tokens) if self.max_input_tokens is not None else None),
            "max_output_tokens": (int(self.max_output_tokens) if self.max_output_tokens is not None else None),
            "allow_network": bool(self.allow_network),
        }
        return {k: v for k, v in obj.items() if v is not None or k == "allow_network"}

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> OperatingBound:
        if not isinstance(obj, Mapping):
            raise IntentModelError("operating_bounds must be a JSON object")
        return OperatingBound(
            max_seconds=cast(float | None, obj.get("max_seconds")),
            max_steps=cast(int | None, obj.get("max_steps")),
            max_input_tokens=cast(int | None, obj.get("max_input_tokens")),
            max_output_tokens=cast(int | None, obj.get("max_output_tokens")),
            allow_network=bool(obj.get("allow_network", False)),
        )


@dataclass(frozen=True, slots=True)
class Assumption:
    id: str
    statement: str

    def __post_init__(self) -> None:
        require_non_empty(self.id, name="assumption.id")
        require_non_empty(self.statement, name="assumption.statement")

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {"id": self.id, "statement": self.statement}

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> Assumption:
        if not isinstance(obj, Mapping):
            raise IntentModelError("assumption must be a JSON object")
        return Assumption(id=str(obj.get("id", "")), statement=str(obj.get("statement", "")))


@dataclass(frozen=True, slots=True)
class IntentSpecV1:
    """Versioned, tenant+repo-scoped intent contract."""

    intent_id: str = field(default_factory=new_uuid)
    tenant_id: str = ""
    repo_id: str = ""
    spec_version: int = _INTENT_SPEC_DEFAULT_VERSION
    status: IntentStatus = "draft"

    # Source/trace fields (should not affect semantic drift detection by
    # default; use semantic fingerprint helpers instead).
    title: str | None = None
    goal_statement: str | None = None
    summary: str | None = None
    derived_from_goal_text: bool = False

    # Semantic contract.
    objectives: tuple[Objective, ...] = ()
    constraints: tuple[Constraint, ...] = ()
    policies: tuple[PolicyRef, ...] = ()
    success_criteria: tuple[SuccessCriterion, ...] = ()
    operating_bounds: OperatingBound | None = None
    quality_contract: QualityContract | None = None

    assumptions: tuple[Assumption, ...] = ()
    risk_notes: tuple[str, ...] = ()
    tags: tuple[str, ...] = ()
    metadata: Mapping[str, JSONValue] | None = None

    created_at_ms: int = field(default_factory=now_ms)
    updated_at_ms: int = field(default_factory=now_ms)

    def __post_init__(self) -> None:
        require_non_empty(self.intent_id, name="intent_id")
        require_non_empty(self.tenant_id, name="tenant_id")
        require_non_empty(self.repo_id, name="repo_id")
        if int(self.spec_version) <= 0:
            raise ValueError("intent.spec_version must be > 0")
        require_non_empty(self.status, name="status")

        if self.metadata is not None:
            json_dumps(cast(JSONValue, dict(self.metadata)))

        # Ensure stable nested IDs for audit traces and future step linkage.
        obj_ids = [o.id for o in self.objectives]
        if len(obj_ids) != len(set(obj_ids)):
            raise ValueError("intent.objectives must have unique ids")
        c_ids = [c.id for c in self.constraints]
        if len(c_ids) != len(set(c_ids)):
            raise ValueError("intent.constraints must have unique ids")
        sc_ids = [s.id for s in self.success_criteria]
        if len(sc_ids) != len(set(sc_ids)):
            raise ValueError("intent.success_criteria must have unique ids")
        p_ids = [p.id for p in self.policies]
        if len(p_ids) != len(set(p_ids)):
            raise ValueError("intent.policies must have unique ids")
        a_ids = [a.id for a in self.assumptions]
        if len(a_ids) != len(set(a_ids)):
            raise ValueError("intent.assumptions must have unique ids")

        if (
            (self.goal_statement is None or not self.goal_statement.strip())
            and len(self.objectives) == 0
            and len(self.constraints) == 0
        ):
            raise ValueError("intent must define at least one of goal_statement/objectives/constraints")

        # We allow explicit empty strings only when other semantic fields exist,
        # but still normalize to a non-empty value.
        if self.goal_statement is not None and not isinstance(self.goal_statement, str):
            raise ValueError("intent.goal_statement must be a string")
        # Don't require_non_empty to allow use of "no goal statement" intent.

    def normalized(self) -> IntentSpecV1:
        return IntentSpecV1(
            intent_id=self.intent_id.strip(),
            tenant_id=self.tenant_id.strip(),
            repo_id=normalize_repo_id(self.repo_id),
            spec_version=int(self.spec_version),
            status=self.status,
            title=self.title.strip() if isinstance(self.title, str) else None,
            goal_statement=(self.goal_statement.strip() if isinstance(self.goal_statement, str) else None),
            summary=self.summary.strip() if isinstance(self.summary, str) else None,
            derived_from_goal_text=bool(self.derived_from_goal_text),
            objectives=self.objectives,
            constraints=self.constraints,
            policies=self.policies,
            success_criteria=self.success_criteria,
            operating_bounds=self.operating_bounds,
            quality_contract=self.quality_contract,
            assumptions=self.assumptions,
            risk_notes=self.risk_notes,
            tags=tuple(t.strip() for t in self.tags if isinstance(t, str)),
            metadata=self.metadata,
            created_at_ms=int(self.created_at_ms),
            updated_at_ms=int(self.updated_at_ms),
        )

    def to_json_obj(self) -> dict[str, JSONValue]:
        i = self.normalized()
        obj: dict[str, JSONValue] = {
            "intent_id": i.intent_id,
            "tenant_id": i.tenant_id,
            "repo_id": i.repo_id,
            "spec_version": int(i.spec_version),
            "status": i.status,
            "title": i.title,
            "goal_statement": i.goal_statement,
            "summary": i.summary,
            "derived_from_goal_text": bool(i.derived_from_goal_text),
            "objectives": [o.to_json_obj() for o in i.objectives],
            "constraints": [c.to_json_obj() for c in i.constraints],
            "policies": [p.to_json_obj() for p in i.policies],
            "success_criteria": [s.to_json_obj() for s in i.success_criteria],
            "operating_bounds": (i.operating_bounds.to_json_obj() if i.operating_bounds is not None else None),
            "quality_contract": (i.quality_contract.to_json_obj() if i.quality_contract is not None else None),
            "assumptions": [a.to_json_obj() for a in i.assumptions],
            "risk_notes": list(i.risk_notes),
            "tags": list(i.tags),
            "metadata": dict(i.metadata) if i.metadata is not None else None,
            "created_at_ms": int(i.created_at_ms),
            "updated_at_ms": int(i.updated_at_ms),
        }

        apply_schema_envelope(obj=cast(dict[str, Any], obj), kind=_INTENT_SPEC_SCHEMA_KIND)
        # Validate shape (and nested JSON-serializability).
        json_dumps(cast(JSONValue, obj))
        return obj

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> IntentSpecV1:
        if not isinstance(obj, Mapping):
            raise IntentModelError("intent spec must be a JSON object")

        # We expect `spec_version` to drive Python-level dispatch.
        spec_version = int(obj.get("spec_version", _INTENT_SPEC_DEFAULT_VERSION))
        if spec_version != 1:
            raise IntentModelError(f"unsupported intent spec_version: {spec_version}")

        def _require_arr(name: str) -> list[Mapping[str, Any]]:
            raw = obj.get(name)
            if raw is None:
                return []
            if not isinstance(raw, list):
                raise IntentModelError(f"intent.{name} must be an array")
            out: list[Mapping[str, Any]] = []
            for x in raw:
                if not isinstance(x, dict):
                    raise IntentModelError(f"intent.{name}[] entries must be objects")
                out.append(cast(Mapping[str, Any], x))
            return out

        metadata = obj.get("metadata")
        if metadata is not None and not isinstance(metadata, dict):
            raise IntentModelError("intent.metadata must be an object when set")

        operating_bounds_raw = obj.get("operating_bounds")
        operating_bounds = (
            OperatingBound.from_json_obj(cast(Mapping[str, Any], operating_bounds_raw))
            if isinstance(operating_bounds_raw, dict)
            else None
        )
        quality_contract_raw = obj.get("quality_contract")
        quality_contract = (
            QualityContract.from_json_obj(cast(Mapping[str, Any], quality_contract_raw))
            if isinstance(quality_contract_raw, Mapping)
            else None
        )

        return IntentSpecV1(
            intent_id=str(obj.get("intent_id", "")),
            tenant_id=str(obj.get("tenant_id", "")),
            repo_id=str(obj.get("repo_id", "")),
            spec_version=spec_version,
            status=cast(IntentStatus, str(obj.get("status", "draft"))),
            title=cast(str | None, obj.get("title")),
            goal_statement=cast(str | None, obj.get("goal_statement")),
            summary=cast(str | None, obj.get("summary")),
            derived_from_goal_text=bool(obj.get("derived_from_goal_text", False)),
            objectives=tuple(Objective.from_json_obj(x) for x in _require_arr("objectives")),
            constraints=tuple(Constraint.from_json_obj(x) for x in _require_arr("constraints")),
            policies=tuple(PolicyRef.from_json_obj(x) for x in _require_arr("policies")),
            success_criteria=tuple(SuccessCriterion.from_json_obj(x) for x in _require_arr("success_criteria")),
            operating_bounds=operating_bounds,
            quality_contract=quality_contract,
            assumptions=tuple(Assumption.from_json_obj(x) for x in _require_arr("assumptions")),
            risk_notes=tuple(str(x) for x in (obj.get("risk_notes") or [])),
            tags=tuple(str(x) for x in (obj.get("tags") or [])),
            metadata=cast(Mapping[str, JSONValue] | None, metadata),
            created_at_ms=int(obj.get("created_at_ms", 0)),
            updated_at_ms=int(obj.get("updated_at_ms", 0)),
        )


IntentSpec: TypeAlias = IntentSpecV1


@dataclass(frozen=True, slots=True)
class IntentFingerprint:
    """Fingerprints to support drift detection with separation of concerns.

    - `semantic` is derived from the structured intent contract (objectives,
      constraints, policies, acceptance, bounds, assumptions).
    - `goal_text` is derived from `goal_statement` only.

    Plan/resume/drift code can treat changes in `semantic` as a real intent
    drift, while allowing goal text edits that preserve semantic meaning.
    """

    spec_version: int
    semantic: str
    goal_text: str

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "spec_version": int(self.spec_version),
            "semantic": self.semantic,
            "goal_text": self.goal_text,
        }


def _intent_semantic_fingerprint_payload(*, intent: IntentSpecV1) -> dict[str, JSONValue]:
    i = intent.normalized()
    # Order-insensitive hashing: sort each list by stable IDs.
    return {
        "spec_version": int(i.spec_version),
        "objectives": [o.to_json_obj() for o in _sorted_by_id(i.objectives)],
        "constraints": [c.to_json_obj() for c in _sorted_by_id(i.constraints)],
        "policies": [p.to_json_obj() for p in _sorted_by_id(i.policies)],
        "success_criteria": [s.to_json_obj() for s in _sorted_by_id(i.success_criteria)],
        "operating_bounds": (i.operating_bounds.to_json_obj() if i.operating_bounds is not None else None),
        "quality_contract": (i.quality_contract.to_json_obj() if i.quality_contract is not None else None),
        "assumptions": [a.to_json_obj() for a in _sorted_by_id(i.assumptions)],
    }


def intent_semantic_fingerprint(*, intent: IntentSpecV1) -> str:
    """Stable fingerprint for semantic intent changes (ignores goal text)."""
    payload = _intent_semantic_fingerprint_payload(intent=intent)
    # Keep consistent with existing `goal_fingerprint()` (short, stable).
    h = stable_json_fingerprint(payload)
    return h[:16]


def intent_acceptance_slice_fingerprint(
    *,
    success_criteria: tuple[SuccessCriterion, ...],
    quality_contract: QualityContract | None = None,
) -> str:
    """Stable short fingerprint of the normalized success-criteria slice (acceptance-only contract).

    Used for offline manifest correlation and replay tooling when full intent JSON is absent.
    Schema version is bumped only when the JSON shape of this slice changes.
    """

    payload: dict[str, JSONValue] = {
        "kind": "intent_acceptance_slice",
        "schema_version": 2,
        "success_criteria": [s.to_json_obj() for s in _sorted_by_id(success_criteria)],
        "quality_contract": quality_contract.to_json_obj() if quality_contract is not None else None,
    }
    h = stable_json_fingerprint(payload)
    return h[:16]


def intent_goal_text_fingerprint(*, goal_statement: str | None) -> str:
    """Stable fingerprint for goal text edits (not semantic intent)."""
    if goal_statement is None:
        return ""
    if not isinstance(goal_statement, str) or not goal_statement.strip():
        return ""
    return goal_fingerprint(goal_statement.strip())


def compute_intent_fingerprint(*, intent: IntentSpecV1) -> IntentFingerprint:
    """Compute both semantic and goal-text fingerprints for drift triage."""
    s = intent_semantic_fingerprint(intent=intent)
    g = intent_goal_text_fingerprint(goal_statement=intent.goal_statement)
    return IntentFingerprint(spec_version=int(intent.spec_version), semantic=s, goal_text=g)


def intent_semantic_matches(*, old: IntentSpecV1, new: IntentSpecV1) -> bool:
    """True when semantic intent is unchanged (goal text can still differ)."""
    return intent_semantic_fingerprint(intent=old) == intent_semantic_fingerprint(intent=new)


def intent_goal_text_matches(*, old: IntentSpecV1, new: IntentSpecV1) -> bool:
    """True when goal_statement is unchanged."""
    left = intent_goal_text_fingerprint(goal_statement=old.goal_statement)
    right = intent_goal_text_fingerprint(goal_statement=new.goal_statement)
    return left == right


def stable_intent_sha256(*, intent: IntentSpecV1) -> str:
    """Stable SHA256 of full serialized intent (includes goal text)."""
    obj = intent.normalized().to_json_obj()
    raw = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return sha256(raw).hexdigest()


def deterministic_intent_id_from_semantic_fingerprint(*, semantic_fingerprint: str) -> str:
    """Derive a stable intent_id from semantic intent fingerprints.

    This is used to make goal-only compatibility intents replay/cache-friendly
    across runs (since prompt keys include `intent_id`).
    """
    require_non_empty(semantic_fingerprint, name="semantic_fingerprint")
    # semantic fingerprints are 16-char hex slices; keep ID compact and safe.
    sf = str(semantic_fingerprint).strip().lower()
    if len(sf) != 16 or any(ch not in "0123456789abcdef" for ch in sf):
        raise ValueError("semantic_fingerprint must be a 16-char hex string")
    return f"intent_{sf}"
