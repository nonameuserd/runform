"""Offline, deterministic evaluation of ``operational_spec`` success criteria over exported evidence.

No network I/O. Inputs are normalized ``OperationalValidityParams``, parsed
:class:`~akc.run.manifest.RuntimeEvidenceRecord` objects, optional OTel trace NDJSON
(``parse_otel_ndjson_slice``), optional **metric** NDJSON (``parse_otel_metric_ndjson_slice``),
and optional bundle schema version for pin checks.

See ``docs/runtime-execution.md`` for aggregate terminal-health worst-of ordering,
``convergence_certificate`` evidence, optional resync bounds (``max_resync_attempts_bound``),
and SLI-shaped composite predicates over exported telemetry only (no TSDB / PromQL).
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from akc.artifacts.validate import validate_obj
from akc.intent.models import (
    AKC_OTEL_METRICS_EXPORT_EVIDENCE_TYPE,
    OperationalCompositePredicate,
    OperationalOtelMetricSignalSelector,
    OperationalSignalSelector,
    OperationalValidityParams,
)
from akc.memory.models import JSONValue
from akc.run.manifest import RuntimeEvidenceRecord
from akc.run.replay import terminal_health_aggregate_status
from akc.utils.fingerprint import stable_json_fingerprint

# Documented in docs/runtime-execution.md (highest severity first).
HEALTH_WORST_OF_ORDER: tuple[str, ...] = ("failed", "degraded", "unknown", "healthy")

# Caps for offline NDJSON metric parsing (defense against huge sidecars).
OTEL_METRIC_NDJSON_MAX_LINES: int = 100_000
OTEL_METRIC_NDJSON_MAX_RECORDS: int = 50_000


class OperationalPathScopeError(ValueError):
    """Raised when a filesystem path for operational evaluation escapes tenant/repo outputs."""


class OperationalRollupLoadError(ValueError):
    """Raised when an operational evidence rollup cannot be read, validated, or hashed."""


@dataclass(frozen=True, slots=True)
class OperationalEvidenceRollupMeta:
    """Declared window and export membership for ``window=rolling_ms`` (fingerprint + eval)."""

    window_start_ms: int
    window_end_ms: int
    # Sorted by (path, sha256) for deterministic fingerprints and merge order.
    runtime_evidence_exports: tuple[tuple[str, str], ...]

    def to_fingerprint_obj(self) -> dict[str, Any]:
        return {
            "window_start_ms": int(self.window_start_ms),
            "window_end_ms": int(self.window_end_ms),
            "runtime_evidence_exports": [{"path": p, "sha256": h} for p, h in self.runtime_evidence_exports],
        }


def ensure_path_under_repo_outputs(path: Path, *, repo_outputs_root: Path) -> Path:
    """Resolve ``path`` and ensure it lies under ``repo_outputs_root``.

    ``repo_outputs_root`` is ``<outputs_root>/<tenant_id>/<repo_id>`` (see
    ``docs/artifact-contracts.md``). Callers that load bundle, evidence, or OTel
    sidecars for :func:`evaluate_operational_spec` must enforce this before read.
    """

    resolved = path.expanduser().resolve()
    base = repo_outputs_root.expanduser().resolve()
    try:
        resolved.relative_to(base)
    except ValueError as exc:
        raise OperationalPathScopeError(
            f"operational evaluation path {resolved} is not under tenant/repo outputs {base}"
        ) from exc
    return resolved


def _assert_rollup_file_under_verification(*, rollup_path: Path, repo_outputs_root: Path) -> Path:
    resolved = ensure_path_under_repo_outputs(rollup_path, repo_outputs_root=repo_outputs_root)
    rel = resolved.relative_to(repo_outputs_root.resolve())
    parts = rel.parts
    if len(parts) < 3 or parts[0] != ".akc" or parts[1] != "verification":
        raise OperationalPathScopeError(
            f"operational evidence rollup file must be under .akc/verification, got {rel.as_posix()!r}"
        )
    return resolved


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest().lower()


def _resolve_export_path(*, rel_path: str, repo_outputs_root: Path) -> Path:
    raw = str(rel_path).strip()
    if not raw:
        raise OperationalRollupLoadError("runtime_evidence_exports[].path must be non-empty")
    p = Path(raw)
    if p.is_absolute() or ".." in p.parts:
        raise OperationalRollupLoadError(f"runtime_evidence_exports path must be repo-relative without '..': {raw!r}")
    out = (repo_outputs_root / p).resolve()
    ensure_path_under_repo_outputs(out, repo_outputs_root=repo_outputs_root)
    return out


def load_merged_runtime_evidence_from_rollup_path(
    *,
    rollup_path: Path,
    repo_outputs_root: Path,
) -> tuple[tuple[RuntimeEvidenceRecord, ...], OperationalEvidenceRollupMeta]:
    """Load ``operational_evidence_window.v1``, verify export hashes, merge evidence records (offline).

    Export files must be JSON arrays matching ``runtime_evidence_stream`` v1. Paths are relative to
    ``repo_outputs_root`` (tenant/repo outputs directory).
    """

    root = repo_outputs_root.expanduser().resolve()
    path = _assert_rollup_file_under_verification(rollup_path=rollup_path.expanduser(), repo_outputs_root=root)
    try:
        body = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as e:
        raise OperationalRollupLoadError(f"rollup JSON read failed: {e}") from e
    if not isinstance(body, dict):
        raise OperationalRollupLoadError("operational evidence rollup must be a JSON object")
    issues = validate_obj(obj=body, kind="operational_evidence_window", version=1)
    if issues:
        raise OperationalRollupLoadError(
            "rollup failed schema validation: " + "; ".join(f"{i.path}: {i.message}" for i in issues[:6])
        )

    ws = body.get("window_start_ms")
    we = body.get("window_end_ms")
    if isinstance(ws, bool) or not isinstance(ws, (int, float)):
        raise OperationalRollupLoadError("rollup window_start_ms must be an integer")
    if isinstance(we, bool) or not isinstance(we, (int, float)):
        raise OperationalRollupLoadError("rollup window_end_ms must be an integer")
    start_ms = int(ws)
    end_ms = int(we)
    if end_ms < start_ms:
        raise OperationalRollupLoadError("rollup window_end_ms must be >= window_start_ms")

    raw_exports = body.get("runtime_evidence_exports")
    if not isinstance(raw_exports, list) or not raw_exports:
        raise OperationalRollupLoadError("rollup runtime_evidence_exports must be a non-empty array")

    normalized: list[tuple[str, str]] = []
    for i, item in enumerate(raw_exports):
        if not isinstance(item, dict):
            raise OperationalRollupLoadError(f"runtime_evidence_exports[{i}] must be an object")
        ep = str(item.get("path", "")).strip()
        dig = str(item.get("sha256", "")).strip().lower()
        if len(dig) != 64 or any(c not in "0123456789abcdef" for c in dig):
            raise OperationalRollupLoadError(f"runtime_evidence_exports[{i}].sha256 must be 64-char hex")
        normalized.append((ep, dig))
    exports_sorted = tuple(sorted(normalized, key=lambda x: (x[0], x[1])))
    meta = OperationalEvidenceRollupMeta(
        window_start_ms=start_ms,
        window_end_ms=end_ms,
        runtime_evidence_exports=exports_sorted,
    )

    merged: list[RuntimeEvidenceRecord] = []
    for rel_p, want_digest in exports_sorted:
        exp_path = _resolve_export_path(rel_path=rel_p, repo_outputs_root=root)
        if not exp_path.is_file():
            raise OperationalRollupLoadError(f"export file missing: {rel_p!r}")
        got_digest = _sha256_file(exp_path)
        if got_digest != want_digest:
            raise OperationalRollupLoadError(
                f"sha256 mismatch for export {rel_p!r}: expected {want_digest}, got {got_digest}"
            )
        try:
            raw_ev = json.loads(exp_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as e:
            raise OperationalRollupLoadError(f"failed reading export {rel_p!r}: {e}") from e
        if not isinstance(raw_ev, list):
            raise OperationalRollupLoadError(f"export {rel_p!r} must be a JSON array (runtime evidence stream)")
        ev_issues = validate_obj(obj=raw_ev, kind="runtime_evidence_stream", version=1)
        if ev_issues:
            raise OperationalRollupLoadError(
                f"export {rel_p!r} failed runtime_evidence_stream schema: "
                + "; ".join(f"{i.path}: {i.message}" for i in ev_issues[:4])
            )
        for j, row in enumerate(raw_ev):
            if not isinstance(row, dict):
                raise OperationalRollupLoadError(f"export {rel_p!r}[{j}] must be an object")
            merged.append(RuntimeEvidenceRecord.from_json_obj(row))

    return (tuple(merged), meta)


def health_status_rank(status: str) -> int:
    """Return rank (higher = worse) for known terminal health strings."""

    s = str(status).strip().lower()
    return {"failed": 4, "degraded": 3, "unknown": 2, "healthy": 1}.get(s, 2)


def _sorted_evidence_records(evidence: Sequence[RuntimeEvidenceRecord]) -> tuple[RuntimeEvidenceRecord, ...]:
    return tuple(
        sorted(
            evidence,
            key=lambda r: (r.runtime_run_id, r.timestamp, r.evidence_type, str(r.payload)),
        )
    )


def _parse_otel_line(line: str) -> dict[str, JSONValue] | None:
    line = line.strip()
    if not line:
        return None
    try:
        raw = json.loads(line)
    except json.JSONDecodeError:
        return None
    if not isinstance(raw, dict):
        return None
    return cast(dict[str, JSONValue], raw)


def parse_otel_ndjson_slice(text: str) -> tuple[dict[str, JSONValue], ...]:
    """Parse NDJSON lines into export records (one JSON object per line)."""

    out: list[dict[str, JSONValue]] = []
    for line in text.splitlines():
        rec = _parse_otel_line(line)
        if rec is not None:
            out.append(rec)
    return tuple(out)


@dataclass(frozen=True, slots=True)
class OtelMetricNdjsonParseOutcome:
    """Result of bounded parse for ``{run_id}.otel_metrics.jsonl``."""

    records: tuple[dict[str, JSONValue], ...]
    rejected_reason: str | None

    @property
    def ok(self) -> bool:
        return self.rejected_reason is None


def parse_otel_metric_ndjson_slice(
    text: str,
    *,
    max_lines: int = OTEL_METRIC_NDJSON_MAX_LINES,
    max_records: int = OTEL_METRIC_NDJSON_MAX_RECORDS,
) -> OtelMetricNdjsonParseOutcome:
    """Parse metric export NDJSON lines with strict line/point caps (deterministic, offline).

    Each non-empty line must be a JSON object. Invalid JSON lines are skipped (same tolerance
    as :func:`parse_otel_ndjson_slice`). When caps are exceeded, parsing stops and
    ``rejected_reason`` is set so eval can fail closed without ingesting unbounded data.
    """

    out: list[dict[str, JSONValue]] = []
    for i, line in enumerate(text.splitlines(), start=1):
        if i > max_lines:
            return OtelMetricNdjsonParseOutcome(
                records=(),
                rejected_reason=f"otel_metrics_ndjson exceeds max_lines={max_lines}",
            )
        rec = _parse_otel_line(line)
        if rec is None:
            continue
        out.append(rec)
        if len(out) > max_records:
            return OtelMetricNdjsonParseOutcome(
                records=(),
                rejected_reason=f"otel_metrics_ndjson exceeds max_records={max_records}",
            )
    return OtelMetricNdjsonParseOutcome(records=tuple(out), rejected_reason=None)


def _span_name_from_otel_record(record: Mapping[str, JSONValue]) -> str | None:
    span = record.get("span")
    if isinstance(span, dict):
        name = span.get("name")
        if isinstance(name, str) and name.strip():
            return name.strip()
    return None


def _span_attributes_from_otel_record(record: Mapping[str, JSONValue]) -> dict[str, JSONValue]:
    span = record.get("span")
    if not isinstance(span, dict):
        return {}
    raw = span.get("attributes")
    if isinstance(raw, dict):
        return dict(cast(Mapping[str, JSONValue], raw))
    return {}


def _metric_body_from_record(record: Mapping[str, JSONValue]) -> dict[str, JSONValue] | None:
    m = record.get("metric")
    if isinstance(m, dict):
        return m
    return None


def _metric_value_from_body(metric: Mapping[str, JSONValue]) -> float | None:
    raw_d = metric.get("as_double")
    if isinstance(raw_d, bool) or not isinstance(raw_d, (int, float)):
        raw_d = None
    if raw_d is not None:
        return float(raw_d)
    raw_i = metric.get("as_int")
    if isinstance(raw_i, int) and not isinstance(raw_i, bool):
        return float(raw_i)
    return None


def _metric_attributes_from_body(metric: Mapping[str, JSONValue]) -> dict[str, str]:
    raw = metric.get("attributes")
    if not isinstance(raw, dict):
        return {}
    out: dict[str, str] = {}
    for k, v in raw.items():
        if isinstance(k, str) and k.strip():
            out[k.strip()] = str(v).strip() if v is not None else ""
    return out


def _metric_matches_selector(
    record: Mapping[str, JSONValue],
    selector: OperationalOtelMetricSignalSelector,
) -> bool:
    metric = _metric_body_from_record(record)
    if metric is None:
        return False
    name = str(metric.get("name", "")).strip()
    if name != selector.metric_name.strip():
        return False
    attrs = _metric_attributes_from_body(metric)
    return all(attrs.get(k, "") == want for k, want in selector.attributes)


def _metric_series_fingerprint(metric: Mapping[str, JSONValue]) -> str:
    name = str(metric.get("name", "")).strip()
    attrs = _metric_attributes_from_body(metric)
    return stable_json_fingerprint({"name": name, "attributes": attrs})


def _span_status_from_record(record: Mapping[str, JSONValue]) -> str | None:
    span = record.get("span")
    if not isinstance(span, dict):
        return None
    st = span.get("status")
    return str(st).strip() if isinstance(st, str) else None


def _span_matches_composite_filter(
    record: Mapping[str, JSONValue],
    *,
    span_name: str | None,
    span_attributes: tuple[tuple[str, str], ...],
) -> bool:
    span = record.get("span")
    if not isinstance(span, dict):
        return False
    if span_name is not None:
        n = span.get("name")
        if not isinstance(n, str) or n.strip() != span_name:
            return False
    attrs = _span_attributes_from_otel_record(record)
    for k, want in span_attributes:
        raw = attrs.get(k)
        if not isinstance(raw, str) or raw.strip() != want:
            return False
    return True


def _get_dotted_path(obj: object, path: str) -> JSONValue | None:
    """Resolve ``a.b.c`` from nested dicts; empty path returns None."""

    parts = [p for p in str(path).strip().split(".") if p]
    if not parts:
        return None
    cur: object = obj
    for part in parts:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    if cur is None:
        return None
    if isinstance(cur, (bool, int, float, str)):
        return cast(JSONValue, cur)
    if isinstance(cur, list):
        return cast(JSONValue, cur)
    if isinstance(cur, dict):
        return cast(JSONValue, cur)
    return None


def _coerce_number(value: JSONValue | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, int):
        return float(value)
    if isinstance(value, float):
        return value
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            return float(s)
        except ValueError:
            return None
    return None


def _terminal_health_candidates(
    selector: OperationalSignalSelector,
    evidence: Sequence[RuntimeEvidenceRecord],
) -> tuple[RuntimeEvidenceRecord, ...]:
    et = selector.evidence_type.strip()
    candidates = [r for r in evidence if r.evidence_type == et]
    if et == "terminal_health":
        path = (selector.payload_path or "").strip()
        first_seg = path.split(".", 1)[0] if path else ""
        if path.startswith("aggregate.") or first_seg == "aggregate" or path in ("", "health_status"):
            agg = [r for r in candidates if r.payload.get("aggregate") is True]
            if agg:
                return tuple(sorted(agg, key=lambda r: (r.timestamp, str(r.payload))))
    if et == "convergence_certificate":
        path = (selector.payload_path or "").strip()
        first_seg = path.split(".", 1)[0] if path else ""
        if path.startswith("aggregate.") or first_seg == "aggregate" or path in ("", "converged", "health"):
            agg = [r for r in candidates if r.payload.get("aggregate") is True]
            if agg:
                return tuple(sorted(agg, key=lambda r: (r.timestamp, str(r.payload))))
    return tuple(sorted(candidates, key=lambda r: (r.timestamp, str(r.payload))))


def _select_evidence_for_signal(
    selector: OperationalSignalSelector,
    evidence: Sequence[RuntimeEvidenceRecord],
) -> RuntimeEvidenceRecord | None:
    cands = _terminal_health_candidates(selector, evidence)
    if not cands:
        return None
    return cands[-1]


def _payload_value_for_signal(
    selector: OperationalSignalSelector,
    record: RuntimeEvidenceRecord,
) -> JSONValue | None:
    path = (selector.payload_path or "").strip()
    payload = record.payload
    if not path:
        return cast(JSONValue, dict(payload))
    if path.startswith("aggregate."):
        path = path[len("aggregate.") :]
    return _get_dotted_path(payload, path)


def _value_presence_ok(value: JSONValue | None) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, bool):
        return True
    if isinstance(value, (int, float)):
        return True
    if isinstance(value, list):
        return len(value) > 0
    if isinstance(value, dict):
        return len(value) > 0
    return False


def _compare_threshold(*, got: float, threshold: float, comparator: str) -> bool:
    if comparator == "gte":
        return got >= threshold
    if comparator == "lte":
        return got <= threshold
    if comparator == "eq":
        return math.isclose(got, threshold, rel_tol=0.0, abs_tol=1e-9)
    return False


def _check_otel_contract(
    otel_records: Sequence[Mapping[str, JSONValue]],
    contract: Mapping[str, JSONValue] | None,
) -> tuple[bool, dict[str, JSONValue]]:
    """Optional span/attribute contract checks (strictly opt-in via ``contract``)."""

    if not contract:
        return True, {}
    names_raw = contract.get("required_span_names")
    required_names: tuple[str, ...] = ()
    if isinstance(names_raw, list):
        required_names = tuple(str(x).strip() for x in names_raw if str(x).strip())
    attrs_raw = contract.get("required_attributes")
    required_attrs: dict[str, str] = {}
    if isinstance(attrs_raw, dict):
        for k, v in attrs_raw.items():
            if isinstance(k, str) and k.strip() and isinstance(v, (str, int, float, bool)):
                required_attrs[k.strip()] = str(v) if not isinstance(v, bool) else str(v).lower()

    span_names = [_span_name_from_otel_record(r) for r in otel_records]
    span_names_set = {n for n in span_names if n}

    missing_names = [n for n in required_names if n not in span_names_set]
    if missing_names:
        return False, {
            "missing_span_names": cast(JSONValue, list(missing_names)),
            "observed_span_names": cast(JSONValue, sorted(span_names_set)),
        }

    if not required_attrs:
        return True, {"required_span_names": cast(JSONValue, list(required_names))}

    for rec in otel_records:
        attrs = _span_attributes_from_otel_record(rec)
        ok = True
        for k, want in required_attrs.items():
            if str(attrs.get(k, "")).strip() != want.strip():
                ok = False
                break
        if ok:
            return True, {
                "matched_span": cast(JSONValue, _span_name_from_otel_record(rec)),
                "required_attributes": cast(JSONValue, dict(required_attrs)),
            }

    return False, {
        "required_attributes": cast(JSONValue, dict(required_attrs)),
        "observed_attribute_sets": cast(
            JSONValue,
            [_span_attributes_from_otel_record(r) for r in otel_records],
        ),
    }


@dataclass(frozen=True, slots=True)
class OperationalVerdict:
    """Same shape as :class:`~akc.intent.acceptance.IntentAcceptance` for UI consistency."""

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


def operational_verdict_for_rollup_load_failure(
    *,
    success_criterion_id: str,
    message: str,
) -> OperationalVerdict:
    """Single-criterion failure when rollup I/O or validation breaks before predicate eval."""

    sc_id = str(success_criterion_id or "operational_spec").strip() or "operational_spec"
    row: dict[str, JSONValue] = {
        "success_criterion_id": sc_id,
        "evaluation_mode": "operational_spec",
        "check_name": "operational_evidence_rollup_load",
        "passed": False,
        "message": message,
        "evidence": None,
    }
    return OperationalVerdict(
        passed=False,
        failures=(f"{sc_id} (operational_evidence_rollup_load): {message}",),
        evaluated_count=1,
        per_criterion=(row,),
    )


def operational_eval_inputs_fingerprint(
    *,
    params: OperationalValidityParams,
    evidence: Sequence[RuntimeEvidenceRecord],
    otel_records: Sequence[Mapping[str, JSONValue]] | None = None,
    otel_contract: Mapping[str, JSONValue] | None = None,
    runtime_bundle_schema_version: int | None = None,
    rolling_rollup_meta: OperationalEvidenceRollupMeta | None = None,
    otel_metric_records: Sequence[Mapping[str, JSONValue]] | None = None,
    otel_metric_parse_rejected_reason: str | None = None,
) -> str:
    """Stable fingerprint of evaluator inputs (for replay manifests)."""

    ev = _sorted_evidence_records(evidence)
    otel_sorted = sorted(
        (dict(r) for r in (otel_records or ())),
        key=lambda x: stable_json_fingerprint(x),
    )
    payload: dict[str, Any] = {
        "params": params.to_json_obj(),
        "evidence": [r.to_json_obj() for r in ev],
        "otel_records": otel_sorted,
        "otel_contract": dict(otel_contract) if otel_contract else None,
        "runtime_bundle_schema_version": runtime_bundle_schema_version,
    }
    if otel_metric_records is not None or otel_metric_parse_rejected_reason is not None:
        otel_metric_sorted = sorted(
            (dict(r) for r in (otel_metric_records or ())),
            key=lambda x: stable_json_fingerprint(x),
        )
        payload["otel_metric_records"] = otel_metric_sorted
        payload["otel_metric_parse_rejected_reason"] = otel_metric_parse_rejected_reason
    if rolling_rollup_meta is not None:
        payload["rolling_rollup"] = rolling_rollup_meta.to_fingerprint_obj()
    return stable_json_fingerprint(payload)


def operational_verdict_fingerprint(verdict: OperationalVerdict) -> str:
    """Stable fingerprint of a verdict object."""

    return stable_json_fingerprint(
        {
            "passed": verdict.passed,
            "failures": list(verdict.failures),
            "evaluated_count": verdict.evaluated_count,
            "checks": [dict(x) for x in verdict.per_criterion],
        }
    )


def _metric_body_matches_named_attrs(
    metric: Mapping[str, JSONValue],
    metric_name: str,
    attributes: tuple[tuple[str, str], ...],
) -> bool:
    if str(metric.get("name", "")).strip() != metric_name.strip():
        return False
    attrs = _metric_attributes_from_body(metric)
    return all(attrs.get(k, "") == want for k, want in attributes)


def _evaluate_span_status_fraction(
    otel_records: Sequence[Mapping[str, JSONValue]],
    pred: OperationalCompositePredicate,
) -> tuple[bool, str, dict[str, JSONValue]]:
    if pred.kind != "span_status_fraction" or pred.max_spans is None or pred.status_good_value is None:
        return False, "internal: invalid span_status_fraction predicate", {}
    ordered = sorted(otel_records, key=lambda r: stable_json_fingerprint(dict(r)))
    matched: list[Mapping[str, JSONValue]] = [
        r
        for r in ordered
        if _span_matches_composite_filter(r, span_name=pred.span_name, span_attributes=pred.span_attributes)
    ]
    n = len(matched)
    if n == 0:
        return (
            False,
            "no spans matched filter (ambiguous SLI denominator)",
            {
                "matched_spans": 0,
                "max_spans": pred.max_spans,
            },
        )
    if n > int(pred.max_spans):
        return (
            False,
            f"matched {n} spans exceeds max_spans={pred.max_spans}",
            {"matched_spans": n, "max_spans": pred.max_spans},
        )
    good = sum(1 for r in matched if _span_status_from_record(r) == pred.status_good_value)
    fraction = float(good) / float(n)
    ok = _compare_threshold(got=fraction, threshold=float(pred.target), comparator=str(pred.comparator))
    return (
        ok,
        "" if ok else f"fraction {fraction} does not satisfy {pred.comparator} {pred.target}",
        {
            "good_spans": good,
            "matched_spans": n,
            "fraction": fraction,
            "target": float(pred.target),
            "comparator": pred.comparator,
            "status_good_value": pred.status_good_value,
        },
    )


def _evaluate_metric_counter_ratio(
    otel_metric_records: Sequence[Mapping[str, JSONValue]],
    pred: OperationalCompositePredicate,
) -> tuple[bool, str, dict[str, JSONValue]]:
    if (
        pred.kind != "metric_counter_ratio"
        or not pred.numerator_metric_name
        or not pred.denominator_metric_name
        or pred.max_metric_points is None
        or pred.max_metric_series is None
    ):
        return False, "internal: invalid metric_counter_ratio predicate", {}
    ordered = sorted(otel_metric_records, key=lambda r: stable_json_fingerprint(dict(r)))
    series_keys: set[str] = set()
    points = 0
    num_sum = 0.0
    den_sum = 0.0
    for rec in ordered:
        metric = _metric_body_from_record(rec)
        if metric is None:
            continue
        points += 1
        if points > int(pred.max_metric_points):
            return (
                False,
                f"metric points exceed max_metric_points={pred.max_metric_points}",
                {
                    "points_seen": points,
                    "max_metric_points": pred.max_metric_points,
                },
            )
        series_keys.add(_metric_series_fingerprint(metric))
        if len(series_keys) > int(pred.max_metric_series):
            return (
                False,
                f"distinct metric series exceed max_metric_series={pred.max_metric_series}",
                {
                    "series_seen": len(series_keys),
                    "max_metric_series": pred.max_metric_series,
                },
            )
        val = _metric_value_from_body(metric)
        if val is None:
            continue
        if _metric_body_matches_named_attrs(metric, pred.numerator_metric_name, pred.numerator_attributes):
            num_sum += val
        if _metric_body_matches_named_attrs(metric, pred.denominator_metric_name, pred.denominator_attributes):
            den_sum += val
    if den_sum == 0.0:
        return (
            False,
            "metric ratio denominator sum is zero (ambiguous)",
            {
                "numerator_sum": num_sum,
                "denominator_sum": den_sum,
            },
        )
    ratio = num_sum / den_sum
    ok = _compare_threshold(got=ratio, threshold=float(pred.target), comparator=str(pred.comparator))
    return (
        ok,
        "" if ok else f"ratio {ratio} does not satisfy {pred.comparator} {pred.target}",
        {
            "numerator_sum": num_sum,
            "denominator_sum": den_sum,
            "ratio": ratio,
            "target": float(pred.target),
            "comparator": pred.comparator,
            "points_considered": points,
            "series_distinct": len(series_keys),
        },
    )


def evaluate_operational_spec(
    *,
    params: OperationalValidityParams,
    evidence: Sequence[RuntimeEvidenceRecord],
    otel_records: Sequence[Mapping[str, JSONValue]] | None = None,
    otel_contract: Mapping[str, JSONValue] | None = None,
    runtime_bundle_schema_version: int | None = None,
    success_criterion_id: str | None = None,
    rolling_rollup_meta: OperationalEvidenceRollupMeta | None = None,
    otel_metric_records: Sequence[Mapping[str, JSONValue]] | None = None,
    otel_metric_parse_rejected_reason: str | None = None,
) -> OperationalVerdict:
    """Evaluate operational predicates and named checks over exported evidence."""

    checks: list[dict[str, JSONValue]] = []
    failures: list[str] = []
    sc_id = str(success_criterion_id or "operational_spec")

    def _add_check(
        *,
        check_name: str,
        passed: bool,
        message: str,
        evidence_obj: dict[str, JSONValue] | None,
    ) -> None:
        row: dict[str, JSONValue] = {
            "success_criterion_id": sc_id,
            "evaluation_mode": "operational_spec",
            "check_name": check_name,
            "passed": bool(passed),
            "evidence": evidence_obj if evidence_obj else None,
        }
        if message:
            row["message"] = message
        if not row.get("message"):
            row.pop("message", None)
        checks.append(row)
        if not passed:
            failures.append(f"{sc_id} ({check_name}): {message or 'failed'}")

    ev_sorted = _sorted_evidence_records(evidence)

    if params.window == "single_run" and rolling_rollup_meta is not None:
        _add_check(
            check_name="rolling_rollup_meta",
            passed=False,
            message="rolling_rollup_meta must only be set when params.window is rolling_ms",
            evidence_obj=None,
        )
    elif params.window == "rolling_ms":
        if rolling_rollup_meta is None:
            _add_check(
                check_name="rolling_rollup_meta",
                passed=False,
                message="rolling_ms requires OperationalEvidenceRollupMeta from operational_evidence_window.v1",
                evidence_obj=None,
            )
        else:
            meta = rolling_rollup_meta
            rw = int(params.rolling_window_ms or 0)
            span = int(meta.window_end_ms) - int(meta.window_start_ms)
            win_ok = span >= 0 and span <= rw
            _add_check(
                check_name="rolling_window_span",
                passed=win_ok,
                message=""
                if win_ok
                else (
                    f"rollup window span {span}ms exceeds intent rolling_window_ms={rw} "
                    f"(declared {meta.window_start_ms}..{meta.window_end_ms})"
                ),
                evidence_obj={
                    "rolling_window_ms": rw,
                    "window_start_ms": meta.window_start_ms,
                    "window_end_ms": meta.window_end_ms,
                    "declared_span_ms": span,
                },
            )
            bad_ts = [
                {
                    "runtime_run_id": r.runtime_run_id,
                    "timestamp": r.timestamp,
                    "evidence_type": r.evidence_type,
                }
                for r in ev_sorted
                if r.timestamp < meta.window_start_ms or r.timestamp > meta.window_end_ms
            ]
            ts_ok = len(bad_ts) == 0
            _add_check(
                check_name="rolling_evidence_timestamps",
                passed=ts_ok,
                message=""
                if ts_ok
                else (
                    f"{len(bad_ts)} evidence record(s) have timestamp outside rollup window "
                    f"[{meta.window_start_ms}, {meta.window_end_ms}]"
                ),
                evidence_obj={"out_of_window_records": cast(JSONValue, bad_ts[:32])},
            )

    # --- Named: aggregate health (worst-of ordering exposed)
    agg_status = terminal_health_aggregate_status(evidence=ev_sorted)
    agg_rank = health_status_rank(agg_status) if agg_status else 0
    _add_check(
        check_name="aggregate_terminal_health",
        passed=agg_status is not None,
        message="" if agg_status is not None else "missing terminal_health aggregate row",
        evidence_obj={
            "health_status": agg_status,
            "health_rank": int(agg_rank),
            "worst_of_order": list(HEALTH_WORST_OF_ORDER),
        },
    )

    if params.reject_failed_aggregate_terminal_health:
        not_failed = agg_status is not None and str(agg_status).strip().lower() != "failed"
        _add_check(
            check_name="aggregate_terminal_health_not_failed",
            passed=not_failed,
            message="" if not_failed else "aggregate terminal health is failed",
            evidence_obj={"health_status": agg_status},
        )

    # --- Named: reconcile convergence (all resource rows converged when present)
    res_rows = [r for r in ev_sorted if r.evidence_type == "reconcile_resource_status"]
    non_converged = [r for r in res_rows if r.payload.get("converged") is False]
    conv_ok = len(non_converged) == 0
    _add_check(
        check_name="reconcile_convergence",
        passed=conv_ok,
        message="" if conv_ok else "one or more reconcile_resource_status rows have converged=false",
        evidence_obj={
            "resource_status_count": len(res_rows),
            "non_converged_resource_ids": [
                rid for r in non_converged if (rid := str(r.payload.get("resource_id", "")).strip())
            ],
        },
    )

    if params.max_resync_attempts_bound is not None:
        bound = int(params.max_resync_attempts_bound)
        if not res_rows:
            _add_check(
                check_name="reconcile_resync_attempts_bound",
                passed=False,
                message="max_resync_attempts_bound set but no reconcile_resource_status rows",
                evidence_obj={"bound": bound, "max_observed_attempts": None},
            )
        else:
            max_seen = 0
            for r in res_rows:
                raw_a = r.payload.get("resync_completed_attempts")
                if isinstance(raw_a, int) and not isinstance(raw_a, bool):
                    max_seen = max(max_seen, int(raw_a))
            bound_ok = max_seen <= bound
            _add_check(
                check_name="reconcile_resync_attempts_bound",
                passed=bound_ok,
                message="" if bound_ok else f"reconcile used {max_seen} attempts (exceeds bound {bound})",
                evidence_obj={"bound": bound, "max_observed_attempts": max_seen, "all_converged": conv_ok},
            )

    # --- Convergence certificate aggregate (when intent expects this evidence type)
    want_cc = any(str(et).strip() == "convergence_certificate" for et in params.expected_evidence_types)
    cc_agg = [
        r for r in ev_sorted if r.evidence_type == "convergence_certificate" and r.payload.get("aggregate") is True
    ]
    if cc_agg and want_cc:
        last_cc = cc_agg[-1]
        cc_conv = last_cc.payload.get("converged") is True
        cc_attempts = last_cc.payload.get("attempts")
        cc_ok = cc_conv and isinstance(cc_attempts, int) and not isinstance(cc_attempts, bool)
        _add_check(
            check_name="convergence_certificate_aggregate",
            passed=bool(cc_ok),
            message="" if cc_ok else "missing or non-convergent convergence_certificate aggregate row",
            evidence_obj={
                "converged": last_cc.payload.get("converged"),
                "attempts": cc_attempts,
                "health": last_cc.payload.get("health"),
                "window_ms": last_cc.payload.get("window_ms"),
            },
        )

    # --- Named: reconcile_outcome rows present (informational spine; pass if none or all applied)
    reconcile_outcomes = [r for r in ev_sorted if r.evidence_type == "reconcile_outcome"]
    if reconcile_outcomes:
        not_applied = [r for r in reconcile_outcomes if r.payload.get("applied") is False]
        ro_ok = len(not_applied) == 0
        _add_check(
            check_name="reconcile_outcome_applied",
            passed=ro_ok,
            message="" if ro_ok else "reconcile_outcome has applied=false",
            evidence_obj={
                "reconcile_outcome_count": len(reconcile_outcomes),
                "not_applied_resource_ids": [
                    str(r.payload.get("resource_id", "")).strip()
                    for r in not_applied
                    if str(r.payload.get("resource_id", "")).strip()
                ],
            },
        )

    # --- Optional bundle schema pin
    if params.bundle_schema_version is not None:
        bsv = int(params.bundle_schema_version)
        got = runtime_bundle_schema_version
        pin_ok = got is not None and int(got) == bsv
        _add_check(
            check_name="bundle_schema_version_pin",
            passed=pin_ok,
            message="" if pin_ok else f"runtime_bundle schema_version={got!r} does not match expected {bsv}",
            evidence_obj={"expected": bsv, "observed": got},
        )

    # --- Predicate: expected evidence types (presence)
    types_present = {r.evidence_type for r in ev_sorted}
    otel_m = tuple(otel_metric_records or ())
    for et in params.expected_evidence_types:
        et_s = str(et).strip()
        if not et_s:
            continue
        if et_s == AKC_OTEL_METRICS_EXPORT_EVIDENCE_TYPE:
            if otel_metric_parse_rejected_reason:
                ok = False
                msg = f"metrics sidecar parse failed: {otel_metric_parse_rejected_reason}"
                ev_obj: dict[str, JSONValue] = {
                    "parse_rejected_reason": otel_metric_parse_rejected_reason,
                    "metric_point_count": len(otel_m),
                }
            else:
                ok = len(otel_m) > 0
                msg = "" if ok else "expected non-empty akc_otel_metrics_export bundle (.otel_metrics.jsonl)"
                ev_obj = {"metric_point_count": len(otel_m)}
            _add_check(
                check_name=f"presence_evidence_type:{et_s}",
                passed=ok,
                message=msg,
                evidence_obj=ev_obj,
            )
            continue
        ok = et_s in types_present
        _add_check(
            check_name=f"presence_evidence_type:{et_s}",
            passed=ok,
            message="" if ok else f"missing evidence_type={et_s!r}",
            evidence_obj={"observed_evidence_types": cast(JSONValue, sorted(types_present))},
        )

    # --- Predicate: signals (presence or threshold)
    for sig in params.signals:
        rec = _select_evidence_for_signal(sig, ev_sorted)
        val = _payload_value_for_signal(sig, rec) if rec is not None else None
        if params.predicate_kind == "presence":
            ok = rec is not None and (not sig.payload_path or _value_presence_ok(val))
            _add_check(
                check_name=f"presence_signal:{sig.evidence_type}:{sig.payload_path or '*'}",
                passed=ok,
                message="" if ok else "signal not found or payload path empty",
                evidence_obj={"selected_record": rec.to_json_obj() if rec else None},
            )
        elif params.predicate_kind == "threshold":
            if rec is None:
                _add_check(
                    check_name=f"threshold_signal:{sig.evidence_type}:{sig.payload_path or '*'}",
                    passed=False,
                    message="no evidence record for signal",
                    evidence_obj=None,
                )
                continue
            num = _coerce_number(val)
            thr = params.threshold
            tc = params.threshold_comparator
            if num is None or thr is None or tc is None:
                _add_check(
                    check_name=f"threshold_signal:{sig.evidence_type}",
                    passed=False,
                    message="missing numeric value or threshold params",
                    evidence_obj={"observed": val},
                )
                continue
            ok = _compare_threshold(got=num, threshold=float(thr), comparator=str(tc))
            _add_check(
                check_name=f"threshold_signal:{sig.evidence_type}:{sig.payload_path or '*'}",
                passed=ok,
                message="" if ok else f"value {num} does not satisfy {tc} {thr}",
                evidence_obj={
                    "observed": val,
                    "numeric_value": num,
                    "threshold": float(thr),
                    "comparator": tc,
                },
            )

    # --- OTel metrics NDJSON parse caps (fail closed before metric predicates)
    if otel_metric_parse_rejected_reason:
        _add_check(
            check_name="otel_metrics_ndjson_parse",
            passed=False,
            message=str(otel_metric_parse_rejected_reason),
            evidence_obj={"rejected_reason": otel_metric_parse_rejected_reason},
        )

    # --- OTel metric signals (exported bundle only)
    for idx, ms in enumerate(params.otel_metric_signals):
        if otel_metric_parse_rejected_reason:
            _add_check(
                check_name=f"otel_metric_signal:{idx}:{ms.metric_name}",
                passed=False,
                message="skipped: otel_metrics_ndjson parse rejected",
                evidence_obj=None,
            )
            continue
        matching = [mrow for mrow in otel_m if _metric_matches_selector(mrow, ms)]
        if params.predicate_kind == "presence":
            m_ok = len(matching) > 0
            _add_check(
                check_name=f"otel_metric_signal:{idx}:{ms.metric_name}",
                passed=m_ok,
                message="" if m_ok else "no metric points matched selector",
                evidence_obj={
                    "metric_name": ms.metric_name,
                    "matched_points": len(matching),
                    "selector_attributes": dict(ms.attributes),
                },
            )
        else:
            total = 0.0
            for mrow in matching:
                mb = _metric_body_from_record(mrow)
                if mb is None:
                    continue
                v = _metric_value_from_body(mb)
                if v is not None:
                    total += v
            thr = params.threshold
            tc = params.threshold_comparator
            if thr is None or tc is None:
                _add_check(
                    check_name=f"otel_metric_signal:{idx}:{ms.metric_name}",
                    passed=False,
                    message="missing threshold params for metric signal",
                    evidence_obj={"aggregated_value": total},
                )
                continue
            t_ok = _compare_threshold(got=total, threshold=float(thr), comparator=str(tc))
            _add_check(
                check_name=f"otel_metric_signal:{idx}:{ms.metric_name}",
                passed=t_ok,
                message="" if t_ok else f"aggregated metric value {total} does not satisfy {tc} {thr}",
                evidence_obj={
                    "metric_name": ms.metric_name,
                    "matched_points": len(matching),
                    "aggregated_value": total,
                    "threshold": float(thr),
                    "comparator": tc,
                },
            )

    # --- OTel contract (optional)
    otel_ok, otel_ev = _check_otel_contract(tuple(otel_records or ()), otel_contract)
    if otel_contract:
        _add_check(
            check_name="otel_span_contract",
            passed=otel_ok,
            message="" if otel_ok else "OTel contract check failed",
            evidence_obj=otel_ev,
        )

    # --- Composite SLI-style predicates (bounded; offline bundle only)
    for idx, cp in enumerate(params.composite_predicates):
        if cp.kind == "span_status_fraction":
            ok, msg, ev = _evaluate_span_status_fraction(tuple(otel_records or ()), cp)
            _add_check(
                check_name=f"composite_span_status_fraction:{idx}",
                passed=ok,
                message=msg,
                evidence_obj=ev,
            )
        elif cp.kind == "metric_counter_ratio":
            if otel_metric_parse_rejected_reason:
                _add_check(
                    check_name=f"composite_metric_counter_ratio:{idx}",
                    passed=False,
                    message="metric NDJSON parse rejected before ratio evaluation",
                    evidence_obj={"rejected_reason": otel_metric_parse_rejected_reason},
                )
            else:
                ok2, msg2, ev2 = _evaluate_metric_counter_ratio(otel_m, cp)
                _add_check(
                    check_name=f"composite_metric_counter_ratio:{idx}",
                    passed=ok2,
                    message=msg2,
                    evidence_obj=ev2,
                )

    passed = len(failures) == 0
    return OperationalVerdict(
        passed=passed,
        failures=tuple(failures),
        evaluated_count=len(checks),
        per_criterion=tuple(checks),
    )


def combined_operational_fingerprint(
    *,
    params: OperationalValidityParams,
    evidence: Sequence[RuntimeEvidenceRecord],
    verdict: OperationalVerdict,
    otel_records: Sequence[Mapping[str, JSONValue]] | None = None,
    otel_contract: Mapping[str, JSONValue] | None = None,
    runtime_bundle_schema_version: int | None = None,
    rolling_rollup_meta: OperationalEvidenceRollupMeta | None = None,
    otel_metric_records: Sequence[Mapping[str, JSONValue]] | None = None,
    otel_metric_parse_rejected_reason: str | None = None,
) -> str:
    """Single stable fingerprint for inputs + verdict (replay manifest attestation)."""

    return stable_json_fingerprint(
        {
            "inputs_sha256": operational_eval_inputs_fingerprint(
                params=params,
                evidence=evidence,
                otel_records=otel_records,
                otel_contract=otel_contract,
                runtime_bundle_schema_version=runtime_bundle_schema_version,
                rolling_rollup_meta=rolling_rollup_meta,
                otel_metric_records=otel_metric_records,
                otel_metric_parse_rejected_reason=otel_metric_parse_rejected_reason,
            ),
            "verdict_sha256": operational_verdict_fingerprint(verdict),
        }
    )
