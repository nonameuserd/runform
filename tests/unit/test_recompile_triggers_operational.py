from __future__ import annotations

from akc.artifacts.validate import validate_obj
from akc.run import RunManifest, build_recompile_triggers_payload
from akc.run.recompile_triggers import (
    compute_operational_validity_failed_trigger,
    evaluate_recompile_triggers,
    normalized_success_criterion_ids_from_runtime_payload,
)


def test_operational_validity_failed_trigger_when_intent_matches() -> None:
    trig = compute_operational_validity_failed_trigger(
        operational_validity_failed=True,
        manifest_intent_semantic_fingerprint="a" * 16,
        current_intent_semantic_fingerprint="a" * 16,
        manifest_stable_intent_sha256="b" * 64,
        current_stable_intent_sha256="b" * 64,
    )
    assert trig is not None
    assert trig.kind == "operational_validity_failed"
    assert "success_criterion_id" not in trig.details
    manifest = RunManifest(
        run_id="run-ov",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="0" * 64,
        replay_mode="full_replay",
        intent_semantic_fingerprint="a" * 16,
        stable_intent_sha256="b" * 64,
    )
    payload = build_recompile_triggers_payload(
        tenant_id="tenant-a",
        repo_id="repo-a",
        checked_at_ms=1,
        manifest=manifest,
        current_intent_semantic_fingerprint="a" * 16,
        current_stable_intent_sha256="b" * 64,
        operational_validity_failed=True,
        operational_validity_success_criterion_ids=("sc-z",),
        enable_granular_acceptance_triggers=True,
    )
    assert validate_obj(obj=payload, kind="recompile_triggers", version=1) == []
    kinds = {t["kind"] for t in payload["triggers"]}  # type: ignore[index]
    assert "operational_validity_failed" in kinds
    assert "acceptance_criterion_failed" in kinds


def test_operational_validity_trigger_policy_matrix() -> None:
    block_triggers = evaluate_recompile_triggers(
        manifest_intent_semantic_fingerprint="d" * 16,
        current_intent_semantic_fingerprint="d" * 16,
        manifest_knowledge_semantic_fingerprint="e" * 16,
        current_knowledge_semantic_fingerprint="e" * 16,
        manifest_knowledge_provenance_fingerprint="f" * 16,
        current_knowledge_provenance_fingerprint="f" * 16,
        manifest_stable_intent_sha256="1" * 64,
        current_stable_intent_sha256="1" * 64,
        operational_validity_failed=True,
        operational_validity_failed_trigger_severity="block",
        operational_validity_success_criterion_ids=("x", "y"),
        enable_granular_acceptance_triggers=True,
    )
    advisory_triggers = evaluate_recompile_triggers(
        manifest_intent_semantic_fingerprint="d" * 16,
        current_intent_semantic_fingerprint="d" * 16,
        manifest_knowledge_semantic_fingerprint="e" * 16,
        current_knowledge_semantic_fingerprint="e" * 16,
        manifest_knowledge_provenance_fingerprint="f" * 16,
        current_knowledge_provenance_fingerprint="f" * 16,
        manifest_stable_intent_sha256="1" * 64,
        current_stable_intent_sha256="1" * 64,
        operational_validity_failed=True,
        operational_validity_failed_trigger_severity="advisory",
        operational_validity_success_criterion_ids=("x", "y"),
        enable_granular_acceptance_triggers=True,
    )
    assert {t.kind for t in block_triggers} == {"operational_validity_failed", "acceptance_criterion_failed"}
    assert advisory_triggers == ()


def test_operational_validity_failed_trigger_suppressed_when_intent_semantic_differs() -> None:
    assert (
        compute_operational_validity_failed_trigger(
            operational_validity_failed=True,
            manifest_intent_semantic_fingerprint="a" * 16,
            current_intent_semantic_fingerprint="c" * 16,
            manifest_stable_intent_sha256=None,
            current_stable_intent_sha256=None,
        )
        is None
    )


def test_operational_validity_failed_trigger_suppressed_when_stable_intent_differs() -> None:
    assert (
        compute_operational_validity_failed_trigger(
            operational_validity_failed=True,
            manifest_intent_semantic_fingerprint="a" * 16,
            current_intent_semantic_fingerprint="a" * 16,
            manifest_stable_intent_sha256="b" * 64,
            current_stable_intent_sha256="c" * 64,
        )
        is None
    )


def test_evaluate_recompile_triggers_includes_operational_when_flag_set() -> None:
    triggers = evaluate_recompile_triggers(
        manifest_intent_semantic_fingerprint="d" * 16,
        current_intent_semantic_fingerprint="d" * 16,
        manifest_knowledge_semantic_fingerprint="e" * 16,
        current_knowledge_semantic_fingerprint="e" * 16,
        manifest_knowledge_provenance_fingerprint="f" * 16,
        current_knowledge_provenance_fingerprint="f" * 16,
        manifest_stable_intent_sha256="1" * 64,
        current_stable_intent_sha256="1" * 64,
        operational_validity_failed=True,
    )
    kinds = {t.kind for t in triggers}
    assert "operational_validity_failed" in kinds


def test_operational_validity_failed_trigger_includes_success_criterion_ids() -> None:
    trig = compute_operational_validity_failed_trigger(
        operational_validity_failed=True,
        manifest_intent_semantic_fingerprint="a" * 16,
        current_intent_semantic_fingerprint="a" * 16,
        manifest_stable_intent_sha256="b" * 64,
        current_stable_intent_sha256="b" * 64,
        operational_validity_success_criterion_ids=("sc-1", "sc-2"),
    )
    assert trig is not None
    assert trig.details.get("success_criterion_ids") == ["sc-1", "sc-2"]
    assert "success_criterion_id" not in trig.details


def test_operational_validity_failed_trigger_single_id_sets_both_keys() -> None:
    trig = compute_operational_validity_failed_trigger(
        operational_validity_failed=True,
        manifest_intent_semantic_fingerprint="a" * 16,
        current_intent_semantic_fingerprint="a" * 16,
        manifest_stable_intent_sha256="b" * 64,
        current_stable_intent_sha256="b" * 64,
        operational_validity_success_criterion_ids=("sc-op",),
    )
    assert trig is not None
    assert trig.details.get("success_criterion_id") == "sc-op"
    assert trig.details.get("success_criterion_ids") == ["sc-op"]


def test_evaluate_granular_acceptance_triggers_per_criterion() -> None:
    triggers = evaluate_recompile_triggers(
        manifest_intent_semantic_fingerprint="d" * 16,
        current_intent_semantic_fingerprint="d" * 16,
        manifest_knowledge_semantic_fingerprint="e" * 16,
        current_knowledge_semantic_fingerprint="e" * 16,
        manifest_knowledge_provenance_fingerprint="f" * 16,
        current_knowledge_provenance_fingerprint="f" * 16,
        manifest_stable_intent_sha256="1" * 64,
        current_stable_intent_sha256="1" * 64,
        operational_validity_failed=True,
        operational_validity_success_criterion_ids=("x", "y"),
        enable_granular_acceptance_triggers=True,
    )
    kinds = [t.kind for t in triggers]
    assert kinds.count("acceptance_criterion_failed") == 2
    ids = {t.details.get("success_criterion_id") for t in triggers if t.kind == "acceptance_criterion_failed"}
    assert ids == {"x", "y"}


def test_normalized_success_criterion_ids_from_runtime_payload_order() -> None:
    assert normalized_success_criterion_ids_from_runtime_payload(
        {"success_criterion_ids": ["b", "a"], "success_criterion_id": "c"}
    ) == ("b", "a", "c")
