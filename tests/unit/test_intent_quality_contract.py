from __future__ import annotations

from typing import Literal

from akc.compile.interfaces import ExecutionResult
from akc.intent.acceptance import evaluate_intent_success_criteria
from akc.intent.models import (
    ALLOWED_QUALITY_DIMENSION_IDS,
    QualityContract,
    QualityDimensionSpec,
    quality_contract_fingerprint,
)
from akc.intent.quality import evaluate_quality_contract


def _quality_contract(*, engineering_stage: Literal["advisory", "gate"] = "advisory") -> QualityContract:
    dims: dict[str, QualityDimensionSpec] = {}
    for dim in ALLOWED_QUALITY_DIMENSION_IDS:
        dims[dim] = QualityDimensionSpec(
            target_score=0.75,
            gate_min_score=0.6,
            weight=1.0,
            evidence_requirements=(),
            enforcement_stage="advisory",
        )
    dims["engineering_discipline"] = QualityDimensionSpec(
        target_score=0.8,
        gate_min_score=0.75,
        weight=1.0,
        evidence_requirements=("tests_touched", "execution_passed", "verifier_passed"),
        enforcement_stage=engineering_stage,
    )
    return QualityContract(dimensions=dims)


def test_quality_contract_requires_all_dimensions() -> None:
    dims = {
        "taste": QualityDimensionSpec(target_score=0.7),
        "domain_knowledge": QualityDimensionSpec(target_score=0.7),
    }
    try:
        _ = QualityContract(dimensions=dims)  # type: ignore[arg-type]
    except ValueError as e:
        assert "exactly six dimensions" in str(e)
    else:
        raise AssertionError("expected ValueError for missing dimensions")


def test_quality_contract_fingerprint_is_stable() -> None:
    qc = _quality_contract()
    fp1 = quality_contract_fingerprint(quality_contract=qc)
    fp2 = quality_contract_fingerprint(quality_contract=qc)
    assert fp1 is not None
    assert fp1 == fp2


def test_quality_scorecard_is_deterministic() -> None:
    qc = _quality_contract()
    common = {
        "quality_contract": qc,
        "patch_text": "--- a/app.py\n+++ b/app.py\n@@\n+print('ok')\n",
        "touched_paths": ("app.py",),
        "accounting": {"repair_iterations": 1, "policy_decisions": [], "trace_spans": []},
        "retrieved_context": {"documents": [{"id": "d1"}], "code_memory_items": [{"item_id": "m1"}]},
        "execution_exit_code": 0,
        "verifier_passed": True,
    }
    a = evaluate_quality_contract(**common)
    b = evaluate_quality_contract(**common)
    assert a.to_json_obj() == b.to_json_obj()


def test_intent_acceptance_quality_gate_blocks_when_dimension_fails() -> None:
    qc = _quality_contract(engineering_stage="gate")
    ex = ExecutionResult(exit_code=1, stdout="", stderr="", duration_ms=0)
    acc = evaluate_intent_success_criteria(
        success_criteria=(),
        execution=ex,
        patch_text="--- a/app.py\n+++ b/app.py\n@@\n+print('broken')\n",
        touched_paths=("app.py",),
        accounting={"repair_iterations": 4, "policy_decisions": [], "trace_spans": []},
        wall_time_ms=1,
        verifier_passed=False,
        quality_contract=qc,
        retrieved_context={"documents": [], "code_memory_items": []},
    )
    assert not acc.passed
    assert "engineering_discipline" in acc.quality_gate_failed_dimensions
    assert "policy.quality_contract.gate_failed" in acc.quality_policy_reasons
    assert acc.quality_scorecard is not None


def test_intent_acceptance_without_quality_contract_remains_backward_compatible() -> None:
    ex = ExecutionResult(exit_code=0, stdout="", stderr="", duration_ms=0)
    acc = evaluate_intent_success_criteria(
        success_criteria=(),
        execution=ex,
        patch_text="",
        touched_paths=(),
        accounting={},
        wall_time_ms=1,
        verifier_passed=True,
    )
    assert acc.passed
    assert acc.evaluated_count == 0
    assert acc.quality_scorecard is None
