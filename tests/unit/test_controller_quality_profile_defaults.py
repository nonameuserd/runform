from __future__ import annotations

from akc.compile.controller import _intent_with_profile_quality_defaults
from akc.intent import IntentSpecV1, Objective


def _base_intent() -> IntentSpecV1:
    return IntentSpecV1(
        intent_id="intent-quality-profile-defaults",
        tenant_id="t1",
        repo_id="repo1",
        spec_version=1,
        status="active",
        goal_statement="Improve compile quality contracts",
        objectives=(Objective(id="obj1", priority=1, statement="Ship deterministic defaults"),),
        created_at_ms=1,
        updated_at_ms=1,
    )


def test_profile_quality_defaults_are_advisory_by_default() -> None:
    out = _intent_with_profile_quality_defaults(intent_spec=_base_intent(), profile_mode="classic")
    assert out.quality_contract is not None
    gate_dims = {
        dim_id for dim_id, spec in out.quality_contract.dimensions.items() if str(spec.enforcement_stage) == "gate"
    }
    assert gate_dims == set()


def test_profile_quality_defaults_support_opt_in_critical_gates() -> None:
    out = _intent_with_profile_quality_defaults(
        intent_spec=_base_intent(),
        profile_mode="emerging",
        metadata={"quality_contract_rollout_stage": "phase_b"},
    )
    assert out.quality_contract is not None
    gate_dims = {
        dim_id for dim_id, spec in out.quality_contract.dimensions.items() if str(spec.enforcement_stage) == "gate"
    }
    assert gate_dims == {"domain_knowledge", "judgment", "engineering_discipline"}


def test_profile_quality_defaults_explicit_advisory_stage_remains_advisory() -> None:
    out = _intent_with_profile_quality_defaults(
        intent_spec=_base_intent(),
        profile_mode="emerging",
        metadata={"quality_contract_rollout_stage": "phase_a"},
    )
    assert out.quality_contract is not None
    gate_dims = {
        dim_id for dim_id, spec in out.quality_contract.dimensions.items() if str(spec.enforcement_stage) == "gate"
    }
    assert gate_dims == set()


def test_profile_quality_defaults_apply_metadata_evidence_expectations() -> None:
    out = _intent_with_profile_quality_defaults(
        intent_spec=_base_intent(),
        profile_mode="classic",
        metadata={
            "quality_evidence_expectations": {
                "engineering_discipline": ["tests_touched", "execution_passed"],
                "judgment": ["policy_decisions"],
            }
        },
    )
    assert out.quality_contract is not None
    assert out.quality_contract.dimensions["engineering_discipline"].evidence_requirements == (
        "tests_touched",
        "execution_passed",
    )
    assert out.quality_contract.dimensions["judgment"].evidence_requirements == ("policy_decisions",)
