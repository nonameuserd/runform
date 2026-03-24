from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

import jsonschema
import pytest

import akc.control
from akc.compile.controller_config import Budget
from akc.intent.models import OperationalValidityParams, OperationalValidityParamsError
from akc.intent.operational_eval import (
    HEALTH_WORST_OF_ORDER,
    OTEL_METRIC_NDJSON_MAX_LINES,
    OperationalPathScopeError,
    combined_operational_fingerprint,
    ensure_path_under_repo_outputs,
    evaluate_operational_spec,
    health_status_rank,
    operational_eval_inputs_fingerprint,
    operational_verdict_fingerprint,
    parse_otel_metric_ndjson_slice,
    parse_otel_ndjson_slice,
)
from akc.memory.models import JSONValue
from akc.run.manifest import RuntimeEvidenceRecord
from akc.run.replay import terminal_health_aggregate_status

_FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "operational_eval"

# Import graph: load compile config before `akc.intent` (avoids circular import on collection).
_: type[Budget] = Budget


def test_ensure_path_under_repo_outputs_allows_and_rejects(tmp_path: Path) -> None:
    repo_root = tmp_path / "tenant_a" / "repo_1"
    inner = repo_root / "nested"
    inner.mkdir(parents=True)
    ok_file = inner / "f.json"
    ok_file.write_text("{}", encoding="utf-8")
    assert ensure_path_under_repo_outputs(ok_file, repo_outputs_root=repo_root) == ok_file.resolve()

    outside = tmp_path / "other" / "x.json"
    outside.parent.mkdir(parents=True)
    outside.write_text("{}", encoding="utf-8")
    with pytest.raises(OperationalPathScopeError):
        ensure_path_under_repo_outputs(outside, repo_outputs_root=repo_root)


def _load_fixture(name: str) -> dict[str, Any]:
    path = _FIXTURES / name
    return cast(dict[str, Any], json.loads(path.read_text(encoding="utf-8")))


def _params_from_fixture(raw: Mapping[str, Any]) -> OperationalValidityParams:
    return OperationalValidityParams.from_mapping(cast(Mapping[str, Any], raw["params"]))


def _evidence_from_fixture(raw: Mapping[str, Any]) -> tuple[RuntimeEvidenceRecord, ...]:
    items = raw.get("evidence")
    if not isinstance(items, list):
        return ()
    return tuple(RuntimeEvidenceRecord.from_json_obj(cast(Mapping[str, Any], x)) for x in items)


def _otel_from_fixture(raw: Mapping[str, Any]) -> tuple[dict[str, JSONValue], ...] | None:
    o = raw.get("otel_records")
    if o is None:
        return None
    if not isinstance(o, list):
        return None
    return tuple(cast(dict[str, JSONValue], dict(x)) for x in o)


def _otel_metrics_from_fixture(raw: Mapping[str, Any]) -> tuple[dict[str, JSONValue], ...] | None:
    o = raw.get("otel_metric_records")
    if o is None:
        return None
    if not isinstance(o, list):
        return None
    return tuple(cast(dict[str, JSONValue], dict(x)) for x in o)


def _otel_metric_reject_from_fixture(raw: Mapping[str, Any]) -> str | None:
    if "otel_metric_parse_rejected_reason" not in raw:
        return None
    r = raw.get("otel_metric_parse_rejected_reason")
    if r is None:
        return None
    return str(r)


@pytest.mark.parametrize(
    "fixture",
    [
        "healthy_pass.json",
        "fail_convergence.json",
        "otel_contract_pass.json",
        "threshold_retry_budget.json",
        "metric_signal_pass.json",
        "metric_signal_fail.json",
        "composite_span_status_fraction_pass.json",
        "composite_span_status_fraction_fail.json",
        "composite_metric_ratio_pass.json",
        "composite_metric_ratio_fail.json",
    ],
)
def test_operational_eval_golden_matches_fixture(fixture: str) -> None:
    raw = _load_fixture(fixture)
    params = _params_from_fixture(raw)
    evidence = _evidence_from_fixture(raw)
    otel = _otel_from_fixture(raw)
    otel_m = _otel_metrics_from_fixture(raw)
    otel_m_rej = _otel_metric_reject_from_fixture(raw)
    otel_contract = raw.get("otel_contract")
    bundle_v = raw.get("runtime_bundle_schema_version")
    sc_id = str(raw.get("success_criterion_id") or "sc")

    verdict = evaluate_operational_spec(
        params=params,
        evidence=evidence,
        otel_records=otel,
        otel_contract=cast(Mapping[str, JSONValue], otel_contract) if isinstance(otel_contract, dict) else None,
        runtime_bundle_schema_version=int(bundle_v) if isinstance(bundle_v, int) else None,
        success_criterion_id=sc_id,
        otel_metric_records=otel_m,
        otel_metric_parse_rejected_reason=otel_m_rej,
    )

    exp = raw["expected"]
    assert isinstance(exp, dict)
    assert verdict.to_step_output_obj() == exp["step_output"]
    assert operational_eval_inputs_fingerprint(
        params=params,
        evidence=evidence,
        otel_records=otel,
        otel_contract=cast(Mapping[str, JSONValue], otel_contract) if isinstance(otel_contract, dict) else None,
        runtime_bundle_schema_version=int(bundle_v) if isinstance(bundle_v, int) else None,
        otel_metric_records=otel_m,
        otel_metric_parse_rejected_reason=otel_m_rej,
    ) == str(exp["inputs_sha256"])
    assert operational_verdict_fingerprint(verdict) == str(exp["verdict_sha256"])
    assert combined_operational_fingerprint(
        params=params,
        evidence=evidence,
        verdict=verdict,
        otel_records=otel,
        otel_contract=cast(Mapping[str, JSONValue], otel_contract) if isinstance(otel_contract, dict) else None,
        runtime_bundle_schema_version=int(bundle_v) if isinstance(bundle_v, int) else None,
        otel_metric_records=otel_m,
        otel_metric_parse_rejected_reason=otel_m_rej,
    ) == str(exp["combined_sha256"])


def test_operational_eval_deterministic_repeat() -> None:
    raw = _load_fixture("healthy_pass.json")
    params = _params_from_fixture(raw)
    evidence = _evidence_from_fixture(raw)
    a = evaluate_operational_spec(params=params, evidence=evidence, success_criterion_id="sc1")
    b = evaluate_operational_spec(params=params, evidence=evidence, success_criterion_id="sc1")
    assert a == b
    assert operational_verdict_fingerprint(a) == operational_verdict_fingerprint(b)


def test_terminal_health_aggregate_matches_replay_helper() -> None:
    raw = _load_fixture("healthy_pass.json")
    evidence = _evidence_from_fixture(raw)
    assert terminal_health_aggregate_status(evidence=evidence) == "healthy"


def test_health_worst_of_order_documented_sequence() -> None:
    assert HEALTH_WORST_OF_ORDER[0] == "failed"
    assert HEALTH_WORST_OF_ORDER[-1] == "healthy"
    assert health_status_rank("failed") > health_status_rank("healthy")


def test_max_resync_attempts_bound_checks_completed_attempts() -> None:
    params = OperationalValidityParams.from_mapping(
        {
            "spec_version": 1,
            "window": "single_run",
            "predicate_kind": "presence",
            "expected_evidence_types": ["reconcile_outcome"],
            "max_resync_attempts_bound": 2,
        }
    )
    evidence = (
        RuntimeEvidenceRecord(
            evidence_type="reconcile_resource_status",
            timestamp=1,
            runtime_run_id="r1",
            payload={
                "resource_id": "svc",
                "converged": True,
                "conditions": [],
                "observed_hash": "a",
                "health_status": "healthy",
                "resync_completed_attempts": 3,
            },
        ),
        RuntimeEvidenceRecord(
            evidence_type="terminal_health",
            timestamp=2,
            runtime_run_id="r1",
            payload={"resource_id": "__runtime_aggregate__", "health_status": "healthy", "aggregate": True},
        ),
    )
    verdict = evaluate_operational_spec(params=params, evidence=evidence, success_criterion_id="sc")
    assert not verdict.passed
    assert any("reconcile_resync_attempts_bound" in f for f in verdict.failures)


def test_convergence_certificate_aggregate_check_when_expected_type() -> None:
    params = OperationalValidityParams.from_mapping(
        {
            "spec_version": 1,
            "window": "single_run",
            "predicate_kind": "presence",
            "expected_evidence_types": ["convergence_certificate"],
        }
    )
    evidence = (
        RuntimeEvidenceRecord(
            evidence_type="convergence_certificate",
            timestamp=1,
            runtime_run_id="r1",
            payload={
                "certificate_schema_version": 1,
                "resource_id": "__runtime_aggregate__",
                "aggregate": True,
                "desired_hash": "d",
                "observed_hash": "o",
                "health": "healthy",
                "attempts": 1,
                "window_ms": 0,
                "provider_id": "in_memory",
                "policy_mode": "simulate",
                "converged": False,
            },
        ),
        RuntimeEvidenceRecord(
            evidence_type="terminal_health",
            timestamp=2,
            runtime_run_id="r1",
            payload={"resource_id": "__runtime_aggregate__", "health_status": "healthy", "aggregate": True},
        ),
    )
    verdict = evaluate_operational_spec(params=params, evidence=evidence, success_criterion_id="sc")
    assert not verdict.passed
    assert any("convergence_certificate_aggregate" in str(x.get("check_name", "")) for x in verdict.per_criterion)


def test_akc_metric_export_schema_validates_sample_record() -> None:
    pkg = Path(akc.control.__file__).resolve().parent
    schema = json.loads((pkg / "schemas" / "akc_metric_export.v1.schema.json").read_text(encoding="utf-8"))
    sample = json.loads((_FIXTURES / "metric_signal_pass.json").read_text(encoding="utf-8"))["otel_metric_records"][0]
    jsonschema.validate(instance=sample, schema=schema)


def test_parse_otel_ndjson_slice() -> None:
    text = '{"span":{"name":"a"}}\n\nnot-json\n{"span":{"name":"b"}}\n'
    rows = parse_otel_ndjson_slice(text)
    assert len(rows) == 2
    assert rows[0]["span"]["name"] == "a"  # type: ignore[index]


def test_parse_otel_metric_ndjson_slice_respects_line_cap() -> None:
    lines = "\n".join('{"x":1}' for _ in range(OTEL_METRIC_NDJSON_MAX_LINES + 1))
    out = parse_otel_metric_ndjson_slice(lines, max_lines=3, max_records=1000)
    assert out.rejected_reason is not None
    assert "max_lines" in out.rejected_reason
    assert out.records == ()


def test_parse_otel_metric_ndjson_slice_respects_record_cap() -> None:
    lines = "\n".join('{"x":1}' for _ in range(5))
    out = parse_otel_metric_ndjson_slice(lines, max_lines=100, max_records=3)
    assert out.rejected_reason is not None
    assert "max_records" in out.rejected_reason
    assert out.records == ()


def test_operational_params_reject_unbounded_composite() -> None:
    with pytest.raises(OperationalValidityParamsError, match="max_spans"):
        OperationalValidityParams.from_mapping(
            {
                "spec_version": 1,
                "window": "single_run",
                "predicate_kind": "presence",
                "signals": [],
                "composite_predicates": [
                    {
                        "kind": "span_status_fraction",
                        "status_good_value": "ok",
                        "comparator": "gte",
                        "target": 0.9,
                    }
                ],
            }
        )


def test_operational_params_reject_unknown_composite_field() -> None:
    with pytest.raises(OperationalValidityParamsError, match="promql"):
        OperationalValidityParams.from_mapping(
            {
                "spec_version": 1,
                "window": "single_run",
                "predicate_kind": "presence",
                "signals": [],
                "composite_predicates": [
                    {
                        "kind": "span_status_fraction",
                        "status_good_value": "ok",
                        "comparator": "gte",
                        "target": 0.9,
                        "max_spans": 10,
                        "promql": "rate(http[5m])",
                    }
                ],
            }
        )


def test_operational_params_reject_fraction_target_out_of_range() -> None:
    with pytest.raises(OperationalValidityParamsError, match="between 0 and 1"):
        OperationalValidityParams.from_mapping(
            {
                "spec_version": 1,
                "window": "single_run",
                "predicate_kind": "presence",
                "signals": [],
                "composite_predicates": [
                    {
                        "kind": "span_status_fraction",
                        "status_good_value": "ok",
                        "comparator": "gte",
                        "target": 1.5,
                        "max_spans": 10,
                    }
                ],
            }
        )
