"""Partial replay + intent-derived mandatory passes (no subprocess / no LLM)."""

from __future__ import annotations

from types import SimpleNamespace

from akc.run.intent_replay_mandates import mandatory_partial_replay_passes_for_success_criteria
from akc.run.manifest import RunManifest
from akc.run.replay import decide_replay_for_pass
from akc.run.replay_decisions import resolve_intent_mandatory_partial_replay_passes


def _partial_manifest(*, partial: tuple[str, ...] = ()) -> RunManifest:
    return RunManifest(
        run_id="run-1",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="a" * 64,
        replay_mode="partial_replay",
        partial_replay_passes=partial,
    )


def test_partial_replay_without_mandates_skips_deterministic_artifact_tools() -> None:
    """With empty selection + no success-criteria mandates, artifact passes may reuse cache."""
    m = _partial_manifest()
    d = decide_replay_for_pass(
        manifest=m,
        pass_name="system_design",
        intent_mandatory_partial_replay_passes=frozenset(),
    )
    assert d.should_call_tools is False


def test_partial_replay_operational_spec_mandates_runtime_bundle_execution() -> None:
    m = _partial_manifest()
    mandates = mandatory_partial_replay_passes_for_success_criteria(
        success_criteria=(SimpleNamespace(evaluation_mode="operational_spec", id="sc-op"),)
    )
    assert "runtime_bundle" in mandates
    d = decide_replay_for_pass(
        manifest=m,
        pass_name="runtime_bundle",
        intent_mandatory_partial_replay_passes=mandates,
    )
    assert d.should_call_tools is True
    assert d.trigger_reason == "intent_mandatory_partial_replay"


def test_partial_replay_metric_threshold_mandates_runtime_bundle_execution() -> None:
    """Success-criteria mode metric_threshold forces runtime_bundle (and related) into partial selection."""
    m = _partial_manifest()
    mandates = mandatory_partial_replay_passes_for_success_criteria(
        success_criteria=(SimpleNamespace(evaluation_mode="metric_threshold", id="sc-1"),)
    )
    assert "runtime_bundle" in mandates
    d = decide_replay_for_pass(
        manifest=m,
        pass_name="runtime_bundle",
        intent_mandatory_partial_replay_passes=mandates,
    )
    assert d.should_call_tools is True
    assert d.trigger_reason == "intent_mandatory_partial_replay"


def test_partial_replay_tests_mode_mandates_do_not_include_runtime_bundle() -> None:
    mandates = mandatory_partial_replay_passes_for_success_criteria(
        success_criteria=(SimpleNamespace(evaluation_mode="tests", id="sc-1"),)
    )
    assert "runtime_bundle" not in mandates


def test_resolve_mandates_explicit_overrides_manifest_modes() -> None:
    m = RunManifest(
        run_id="run-1",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="a" * 64,
        replay_mode="partial_replay",
        success_criteria_evaluation_modes=("operational_spec",),
        intent_acceptance_fingerprint="a" * 16,
    )
    explicit = frozenset({"generate"})
    out = resolve_intent_mandatory_partial_replay_passes(
        intent_mandatory_partial_replay_passes=explicit,
        decision_manifest=m,
    )
    assert out == explicit


def test_resolve_mandates_from_manifest_matches_success_criteria_derivation() -> None:
    m = RunManifest(
        run_id="run-1",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="a" * 64,
        replay_mode="partial_replay",
        success_criteria_evaluation_modes=("tests",),
        intent_acceptance_fingerprint="b" * 16,
    )
    from_intent = mandatory_partial_replay_passes_for_success_criteria(
        success_criteria=(SimpleNamespace(evaluation_mode="tests", id="sc-1"),)
    )
    from_manifest = resolve_intent_mandatory_partial_replay_passes(
        intent_mandatory_partial_replay_passes=None,
        decision_manifest=m,
    )
    assert from_manifest == from_intent
