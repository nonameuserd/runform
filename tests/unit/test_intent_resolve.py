from __future__ import annotations

import logging
from pathlib import Path

import pytest

import akc.compile  # noqa: F401
from akc.intent import compile_intent_spec, compute_intent_fingerprint, stable_intent_sha256
from akc.intent.plan_step_intent import INTENT_REF_INPUT_KEY, build_plan_step_intent_ref
from akc.intent.resolve import (
    IntentResolutionError,
    intent_link_summaries_for_prompts,
    intent_reference_summaries_for_prompts,
    resolve_compile_intent_context,
)
from akc.intent.store import JsonFileIntentStore
from akc.utils.fingerprint import stable_json_fingerprint


def test_resolve_prefers_store_over_legacy_step_blobs_when_hashes_match(tmp_path: Path) -> None:
    store = JsonFileIntentStore(base_dir=tmp_path)
    intent = compile_intent_spec(
        tenant_id="t1",
        repo_id="r1",
        goal_statement="Primary goal",
        controller_budget=None,
    )
    store.save_intent(tenant_id="t1", repo_id="r1", intent=intent.normalized())
    fp = compute_intent_fingerprint(intent=intent)
    ref = build_plan_step_intent_ref(
        intent=intent,
        semantic_fingerprint=fp.semantic,
        goal_text_fingerprint=fp.goal_text,
    )
    legacy_noise = [
        {
            "success_criterion_id": "legacy_only",
            "evaluation_mode": "tests",
            "summary": "should not win when store verifies",
        }
    ]
    inputs = {
        "intent_id": intent.intent_id,
        INTENT_REF_INPUT_KEY: ref,
        "active_success_criteria": legacy_noise,
        "active_objectives": [{"id": "noise", "priority": 0, "statement": "noise"}],
    }
    resolved = resolve_compile_intent_context(
        tenant_id="t1",
        repo_id="r1",
        inputs=inputs,
        intent_store=store,
        controller_intent_spec=None,
    )
    assert resolved.source == "store_ref"
    assert len(resolved.spec.success_criteria) == len(intent.normalized().success_criteria)
    assert all(sc.id != "legacy_only" for sc in resolved.spec.success_criteria)
    assert resolved.stable_intent_sha256 == stable_intent_sha256(intent=intent.normalized())


def test_resolve_prefers_verified_store_over_conflicting_controller_spec(tmp_path: Path) -> None:
    """When the store verifies against intent_ref, that normalized spec wins (controller may differ in-memory)."""

    store = JsonFileIntentStore(base_dir=tmp_path)
    intent_a = compile_intent_spec(
        tenant_id="t1",
        repo_id="r1",
        goal_statement="A",
        controller_budget=None,
    )
    store.save_intent(tenant_id="t1", repo_id="r1", intent=intent_a.normalized())
    fp = compute_intent_fingerprint(intent=intent_a)
    ref = build_plan_step_intent_ref(
        intent=intent_a,
        semantic_fingerprint=fp.semantic,
        goal_text_fingerprint=fp.goal_text,
    )
    intent_b = compile_intent_spec(
        tenant_id="t1",
        repo_id="r1",
        goal_statement="B",
        controller_budget=None,
    )
    inputs = {"intent_id": intent_a.intent_id, INTENT_REF_INPUT_KEY: ref}
    resolved = resolve_compile_intent_context(
        tenant_id="t1",
        repo_id="r1",
        inputs=inputs,
        intent_store=store,
        controller_intent_spec=intent_b,
    )
    assert resolved.source == "store_ref"
    assert resolved.spec.goal_statement == "A"


def test_resolve_falls_back_to_controller_when_intent_ref_unverified(tmp_path: Path) -> None:
    store = JsonFileIntentStore(base_dir=tmp_path)
    intent = compile_intent_spec(
        tenant_id="t1",
        repo_id="r1",
        goal_statement="controller carries truth",
        controller_budget=None,
    )
    fp = compute_intent_fingerprint(intent=intent)
    ref = build_plan_step_intent_ref(
        intent=intent,
        semantic_fingerprint=fp.semantic,
        goal_text_fingerprint=fp.goal_text,
    )
    # No save_intent — verification fails, but explicit controller intent is allowed.
    inputs = {"intent_id": intent.intent_id, INTENT_REF_INPUT_KEY: ref}
    resolved = resolve_compile_intent_context(
        tenant_id="t1",
        repo_id="r1",
        inputs=inputs,
        intent_store=store,
        controller_intent_spec=intent,
    )
    assert resolved.source == "controller"
    assert resolved.stable_intent_sha256 == stable_intent_sha256(intent=intent.normalized())


def test_resolve_raises_when_intent_ref_present_but_store_unusable(tmp_path: Path) -> None:
    store = JsonFileIntentStore(base_dir=tmp_path)
    intent = compile_intent_spec(
        tenant_id="t1",
        repo_id="r1",
        goal_statement="orphan ref",
        controller_budget=None,
    )
    fp = compute_intent_fingerprint(intent=intent)
    ref = build_plan_step_intent_ref(
        intent=intent,
        semantic_fingerprint=fp.semantic,
        goal_text_fingerprint=fp.goal_text,
    )
    # No save_intent — artifact missing.
    inputs = {"intent_id": intent.intent_id, INTENT_REF_INPUT_KEY: ref}
    with pytest.raises(IntentResolutionError, match="could not be verified"):
        resolve_compile_intent_context(
            tenant_id="t1",
            repo_id="r1",
            inputs=inputs,
            intent_store=store,
            controller_intent_spec=None,
        )


def test_resolve_legacy_warns_under_outputs_root_flag(
    caplog: pytest.LogCaptureFixture,
) -> None:
    inputs = {
        "intent_id": "intent_legacy_warn",
        "goal_statement": "legacy goal",
        "active_success_criteria": [
            {"success_criterion_id": "sc1", "evaluation_mode": "human_gate", "summary": "gate"},
        ],
    }
    with caplog.at_level(logging.WARNING, logger="akc.intent.resolve"):
        resolved = resolve_compile_intent_context(
            tenant_id="t1",
            repo_id="r1",
            inputs=inputs,
            intent_store=None,
            controller_intent_spec=None,
            warn_legacy_step_blobs_without_intent_ref_under_outputs_root=True,
        )
    assert resolved.source == "legacy_step"
    assert any("Deprecated: intent was resolved from duplicated plan-step fields" in r.message for r in caplog.records)


def test_resolve_legacy_warns_when_intent_store_present_and_outputs_root_warn_enabled(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Mirrors compile sessions with IntentStore: legacy-only steps should log deprecation."""

    store = JsonFileIntentStore(base_dir=tmp_path)
    inputs = {
        "intent_id": "intent_store_legacy",
        "goal_statement": "g",
        "active_success_criteria": [
            {"success_criterion_id": "sc1", "evaluation_mode": "human_gate", "summary": "gate"},
        ],
    }
    with caplog.at_level(logging.WARNING, logger="akc.intent.resolve"):
        resolved = resolve_compile_intent_context(
            tenant_id="t1",
            repo_id="r1",
            inputs=inputs,
            intent_store=store,
            controller_intent_spec=None,
            warn_legacy_step_blobs_without_intent_ref_under_outputs_root=True,
        )
    assert resolved.source == "legacy_step"
    assert any("Deprecated: intent was resolved from duplicated plan-step fields" in r.message for r in caplog.records)


def test_resolve_legacy_step_silent_without_outputs_root_warning_flag(caplog: pytest.LogCaptureFixture) -> None:
    inputs = {
        "intent_id": "intent_legacy_silent",
        "goal_statement": "legacy goal",
        "active_success_criteria": [
            {"success_criterion_id": "sc1", "evaluation_mode": "human_gate", "summary": "gate"},
        ],
    }
    with caplog.at_level(logging.WARNING, logger="akc.intent.resolve"):
        resolved = resolve_compile_intent_context(
            tenant_id="t1",
            repo_id="r1",
            inputs=inputs,
            intent_store=None,
            controller_intent_spec=None,
            warn_legacy_step_blobs_without_intent_ref_under_outputs_root=False,
        )
    assert resolved.source == "legacy_step"
    assert not caplog.records


def test_intent_reference_summaries_fingerprints_match_link_summaries() -> None:
    intent = compile_intent_spec(
        tenant_id="t1",
        repo_id="r1",
        goal_statement="g",
        controller_budget=None,
    )
    n = intent.normalized()
    ao_full, lc_full, asc_full = intent_link_summaries_for_prompts(spec=n)
    ao_ref, lc_ref, asc_ref = intent_reference_summaries_for_prompts(spec=n)
    for full, ref in zip(ao_full, ao_ref, strict=True):
        assert ref["fingerprint"] == stable_json_fingerprint(full)
        assert ref["id"] == full["id"]
    for full, ref in zip(lc_full, lc_ref, strict=True):
        assert ref["fingerprint"] == stable_json_fingerprint(full)
        assert ref["constraint_id"] == full["constraint_id"]
    for full, ref in zip(asc_full, asc_ref, strict=True):
        assert ref["fingerprint"] == stable_json_fingerprint(full)
        assert ref["success_criterion_id"] == full["success_criterion_id"]
