from __future__ import annotations

from types import SimpleNamespace

from akc.run.intent_replay_mandates import (
    mandatory_partial_replay_passes_for_evaluation_modes,
    mandatory_partial_replay_passes_for_success_criteria,
)


def test_empty_success_criteria_yields_no_mandates() -> None:
    assert mandatory_partial_replay_passes_for_success_criteria(success_criteria=()) == frozenset()


def test_operational_spec_mirrors_metric_threshold_mandates() -> None:
    m = mandatory_partial_replay_passes_for_success_criteria(
        success_criteria=(
            SimpleNamespace(
                evaluation_mode="operational_spec",
                id="op1",
                description="reconcile evidence",
                params={
                    "spec_version": 1,
                    "window": "single_run",
                    "predicate_kind": "presence",
                    "expected_evidence_types": ["reconcile_outcome"],
                },
            ),
        )
    )
    assert m == frozenset({"intent_acceptance", "runtime_bundle", "execute", "repair"})


def test_metric_threshold_includes_runtime_bundle_and_acceptance() -> None:
    m = mandatory_partial_replay_passes_for_success_criteria(
        success_criteria=(
            SimpleNamespace(
                evaluation_mode="metric_threshold",
                id="mt1",
                description="cap repairs",
                params={"max_repair_iterations": 3},
            ),
        )
    )
    assert "runtime_bundle" in m
    assert "intent_acceptance" in m
    assert "execute" in m
    assert "repair" in m


def test_modes_union_is_stable_sorted_in_policy() -> None:
    m = mandatory_partial_replay_passes_for_evaluation_modes(
        modes=("tests", "manifest_check"),
    )
    assert "execute" in m
    assert "verify" in m
    assert "intent_acceptance" in m
