from __future__ import annotations

import json
from pathlib import Path

from akc.compile.interfaces import ExecutionResult
from akc.intent.acceptance import evaluate_intent_success_criteria
from akc.intent.models import SuccessCriterion

_FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "operational_eval"


def test_operational_spec_acceptance_validates_params_at_compile() -> None:
    sc = SuccessCriterion(
        id="op1",
        evaluation_mode="operational_spec",
        description="runtime evidence shape",
        params={
            "spec_version": 1,
            "window": "single_run",
            "predicate_kind": "presence",
            "expected_evidence_types": ["reconcile_outcome"],
            "bundle_schema_version": 4,
        },
    )
    ex = ExecutionResult(exit_code=0, stdout="", stderr="", duration_ms=0)
    acc = evaluate_intent_success_criteria(
        success_criteria=(sc,),
        execution=ex,
        patch_text="",
        touched_paths=(),
        accounting={},
        wall_time_ms=1,
        verifier_passed=True,
    )
    assert acc.passed
    assert acc.per_criterion[0]["evaluation_mode"] == "operational_spec"
    ev = acc.per_criterion[0].get("evidence") or {}
    assert ev.get("sub_status") == "skipped"
    assert ev.get("evaluation_phase") == "post_runtime"
    assert "post-runtime attestation" in str(ev.get("note", ""))


def test_operational_spec_compile_phase_fail_closed_without_accounting_bundle() -> None:
    sc = SuccessCriterion(
        id="op_compile",
        evaluation_mode="operational_spec",
        description="needs accounting bundle",
        params={
            "spec_version": 1,
            "window": "single_run",
            "predicate_kind": "presence",
            "expected_evidence_types": ["reconcile_outcome"],
            "evaluation_phase": "compile",
        },
    )
    ex = ExecutionResult(exit_code=0, stdout="", stderr="", duration_ms=0)
    acc = evaluate_intent_success_criteria(
        success_criteria=(sc,),
        execution=ex,
        patch_text="",
        touched_paths=(),
        accounting={},
        wall_time_ms=1,
        verifier_passed=True,
    )
    assert not acc.passed


def test_operational_spec_compile_phase_passes_with_fixture_evidence() -> None:
    raw = json.loads((_FIXTURES / "healthy_pass.json").read_text(encoding="utf-8"))
    params = dict(raw["params"])
    params["evaluation_phase"] = "compile"
    sc = SuccessCriterion(
        id="sc1",
        evaluation_mode="operational_spec",
        description="fixture",
        params=params,
    )
    bundle = {"runtime_evidence_records": raw["evidence"]}
    ex = ExecutionResult(exit_code=0, stdout="", stderr="", duration_ms=0)
    acc = evaluate_intent_success_criteria(
        success_criteria=(sc,),
        execution=ex,
        patch_text="",
        touched_paths=(),
        accounting={"operational_compile_bundle": bundle},
        wall_time_ms=1,
        verifier_passed=True,
    )
    assert acc.passed
    ev = acc.per_criterion[0].get("evidence") or {}
    assert ev.get("evaluation_phase") == "compile"
    ov = ev.get("operational_verdict")
    assert isinstance(ov, dict)
    assert ov.get("passed") is True


def test_operational_spec_compile_rejects_rolling_ms_window() -> None:
    sc = SuccessCriterion(
        id="op_roll",
        evaluation_mode="operational_spec",
        description="rollup only post-runtime",
        params={
            "spec_version": 1,
            "window": "rolling_ms",
            "rolling_window_ms": 3600_000,
            "predicate_kind": "presence",
            "expected_evidence_types": ["terminal_health"],
            "evidence_rollup_rel_path": ".akc/verification/w.json",
            "evaluation_phase": "compile",
        },
    )
    ex = ExecutionResult(exit_code=0, stdout="", stderr="", duration_ms=0)
    bundle = {"runtime_evidence_records": []}
    acc = evaluate_intent_success_criteria(
        success_criteria=(sc,),
        execution=ex,
        patch_text="",
        touched_paths=(),
        accounting={"operational_compile_bundle": bundle},
        wall_time_ms=1,
        verifier_passed=True,
    )
    assert not acc.passed
    note = str(acc.per_criterion[0].get("message", ""))
    assert "rolling_ms" in note.lower() or "post-runtime" in note.lower()


def test_operational_spec_acceptance_fails_on_invalid_params() -> None:
    sc = SuccessCriterion(
        id="op2",
        evaluation_mode="operational_spec",
        description="bad",
        params={"spec_version": 1, "window": "nope", "predicate_kind": "presence"},
    )
    ex = ExecutionResult(exit_code=0, stdout="", stderr="", duration_ms=0)
    acc = evaluate_intent_success_criteria(
        success_criteria=(sc,),
        execution=ex,
        patch_text="",
        touched_paths=(),
        accounting={},
        wall_time_ms=1,
        verifier_passed=True,
    )
    assert not acc.passed
