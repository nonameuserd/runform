from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from akc.artifacts.validate import validate_obj
from akc.run import (
    ArtifactPointer,
    PassRecord,
    PassReplayDecisionRecord,
    ReconcileReplayDecision,
    RetrievalSnapshot,
    RunManifest,
    RuntimeEvidenceRecord,
    build_recompile_triggers_payload,
    build_replay_decisions_payload,
    decide_replay_for_pass,
    find_latest_run_manifest,
    load_run_manifest,
    replay_runtime_execution,
)
from akc.run.intent_replay_mandates import mandatory_partial_replay_passes_for_success_criteria


def _manifest() -> RunManifest:
    return RunManifest(
        run_id="run_001",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="a" * 64,
        replay_mode="llm_vcr",
        stable_intent_sha256="1" * 64,
        retrieval_snapshots=(
            RetrievalSnapshot(
                source="vector_index",
                query="billing retries",
                top_k=3,
                item_ids=("chunk-1", "chunk-2", "chunk-3"),
            ),
        ),
        passes=(
            PassRecord(name="plan", status="succeeded", output_sha256="b" * 64),
            PassRecord(name="generate", status="succeeded", output_sha256="c" * 64),
        ),
        model="offline",
        model_params={"temperature": 0},
        tool_params={"test_mode": "smoke"},
        partial_replay_passes=("execute",),
        llm_vcr={"k1": "--- a/x\n+++ b/x\n@@\n+X\n"},
        budgets={"max_llm_calls": 3},
        output_hashes={"manifest.json": "d" * 64},
        control_plane={"stable_intent_sha256": "1" * 64},
        runtime_bundle=ArtifactPointer(
            path=".akc/runtime/run_001.runtime_bundle.json",
            sha256="e" * 64,
        ),
        runtime_event_transcript=ArtifactPointer(
            path=".akc/runtime/run_001.events.jsonl",
            sha256="f" * 64,
        ),
        runtime_evidence=(
            RuntimeEvidenceRecord(
                evidence_type="action_decision",
                timestamp=10,
                runtime_run_id="runtime-001",
                payload={"action_id": "node-a:event-1", "decision": "allowed"},
            ),
        ),
        trace_spans=(
            {
                "trace_id": "abcd" * 8,
                "span_id": "0123456789abcdef",
                "parent_span_id": None,
                "name": "compile.run",
                "kind": "internal",
                "start_time_unix_nano": 1,
                "end_time_unix_nano": 2,
                "attributes": {"tenant_id": "tenant-a"},
                "status": "ok",
            },
        ),
        cost_attribution={
            "tenant_id": "tenant-a",
            "repo_id": "repo-a",
            "run_id": "run_001",
            "total_tokens": 42,
            "wall_time_ms": 12,
        },
    )


def test_manifest_hash_is_stable_for_same_payload() -> None:
    m1 = _manifest()
    m2 = _manifest()
    assert m1.to_json_obj() == m2.to_json_obj()
    assert m1.stable_hash() == m2.stable_hash()


def test_manifest_hash_changes_on_relevant_change() -> None:
    m1 = _manifest()
    m2 = RunManifest(
        run_id=m1.run_id,
        tenant_id=m1.tenant_id,
        repo_id=m1.repo_id,
        ir_sha256=m1.ir_sha256,
        replay_mode=m1.replay_mode,
        retrieval_snapshots=m1.retrieval_snapshots,
        passes=(
            PassRecord(name="plan", status="succeeded", output_sha256="d" * 64),
            PassRecord(name="generate", status="succeeded", output_sha256="c" * 64),
        ),
        model=m1.model,
        model_params=m1.model_params,
        runtime_bundle=ArtifactPointer(
            path=".akc/runtime/run_001.runtime_bundle.v2.json",
            sha256="e" * 64,
        ),
    )
    assert m1.stable_hash() != m2.stable_hash()


def test_replay_modes_resolve_expected_call_policy() -> None:
    live = RunManifest(
        run_id="run_live",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="1" * 64,
        replay_mode="live",
    )
    full = RunManifest(
        run_id="run_replay",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="2" * 64,
        replay_mode="full_replay",
    )
    d_live = decide_replay_for_pass(manifest=live, pass_name="generate")
    d_full = decide_replay_for_pass(manifest=full, pass_name="generate")
    partial = RunManifest(
        run_id="run_partial",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="3" * 64,
        replay_mode="partial_replay",
        partial_replay_passes=("execute",),
    )
    d_partial_generate = decide_replay_for_pass(manifest=partial, pass_name="generate")
    d_partial_execute = decide_replay_for_pass(manifest=partial, pass_name="execute")

    assert d_live.should_call_model is True
    assert d_live.should_call_tools is True
    assert d_full.should_call_model is False
    assert d_full.should_call_tools is False
    assert d_partial_generate.should_call_model is False
    assert d_partial_generate.should_call_tools is True
    assert d_partial_execute.should_call_model is False
    assert d_partial_execute.should_call_tools is True


def test_runtime_replay_modes_skip_compile_pass_execution() -> None:
    runtime_manifest = RunManifest(
        run_id="run_runtime_replay",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="d" * 64,
        replay_mode="runtime_replay",
    )
    reconcile_manifest = RunManifest(
        run_id="run_reconcile_replay",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="e" * 64,
        replay_mode="reconcile_replay",
    )
    runtime_decision = decide_replay_for_pass(manifest=runtime_manifest, pass_name="generate")
    reconcile_decision = decide_replay_for_pass(
        manifest=reconcile_manifest,
        pass_name="deployment_config",
    )
    assert runtime_decision.should_call_model is False
    assert runtime_decision.should_call_tools is False
    assert reconcile_decision.should_call_model is False
    assert reconcile_decision.should_call_tools is False


def test_replay_forces_recompile_on_intent_semantic_mismatch() -> None:
    manifest = RunManifest(
        run_id="run_replay_intent_mismatch",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="1" * 64,
        replay_mode="full_replay",
        intent_semantic_fingerprint="a" * 16,
    )
    d = decide_replay_for_pass(
        manifest=manifest,
        pass_name="generate",
        current_intent_semantic_fingerprint="b" * 16,
    )
    assert d.should_call_model is True
    assert d.should_call_tools is True
    assert d.trigger is not None
    assert d.trigger_reason == "intent_semantic_changed"


def test_replay_forces_recompile_on_knowledge_semantic_mismatch_generate_repair_only() -> None:
    manifest = RunManifest(
        run_id="run_replay_knowledge_sem_mismatch",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="1" * 64,
        replay_mode="full_replay",
        knowledge_semantic_fingerprint="a" * 16,
    )

    d_generate = decide_replay_for_pass(
        manifest=manifest,
        pass_name="generate",
        current_knowledge_semantic_fingerprint="b" * 16,
    )
    assert d_generate.should_call_model is True
    assert d_generate.should_call_tools is True

    d_repair = decide_replay_for_pass(
        manifest=manifest,
        pass_name="repair",
        current_knowledge_semantic_fingerprint="b" * 16,
    )
    assert d_repair.should_call_model is True
    assert d_repair.should_call_tools is True

    # The controller loop doesn't call decide_replay_for_pass() for "execute".
    # Still, this ensures we don't treat knowledge drift as a hard trigger for
    # unrelated cached stages.
    d_execute = decide_replay_for_pass(
        manifest=manifest,
        pass_name="execute",
        current_knowledge_semantic_fingerprint="b" * 16,
    )
    assert d_execute.should_call_model is False
    assert d_execute.should_call_tools is False


def test_partial_replay_only_selected_passes_are_rerun() -> None:
    partial = RunManifest(
        run_id="run_partial_selected",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="4" * 64,
        replay_mode="partial_replay",
        partial_replay_passes=("generate",),
    )

    d_generate = decide_replay_for_pass(manifest=partial, pass_name="generate")
    d_repair = decide_replay_for_pass(manifest=partial, pass_name="repair")
    d_execute = decide_replay_for_pass(manifest=partial, pass_name="execute")

    assert d_generate.should_call_model is True
    assert d_generate.should_call_tools is False
    assert d_repair.should_call_model is False
    assert d_repair.should_call_tools is False
    assert d_execute.should_call_model is False
    assert d_execute.should_call_tools is False
    assert d_repair.trigger_reason == "replay_cache_hit"


def test_replay_decisions_payload_validates() -> None:
    manifest = RunManifest(
        run_id="run_replay_payload",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="1" * 64,
        replay_mode="partial_replay",
        partial_replay_passes=("generate",),
        intent_semantic_fingerprint="a" * 16,
        stable_intent_sha256="f" * 64,
        knowledge_semantic_fingerprint="b" * 16,
        knowledge_provenance_fingerprint="c" * 16,
    )
    mandates = mandatory_partial_replay_passes_for_success_criteria(
        success_criteria=(SimpleNamespace(evaluation_mode="tests"),)
    )
    payload = build_replay_decisions_payload(
        run_id="run_replay_payload",
        tenant_id="tenant-a",
        repo_id="repo-a",
        replay_mode="partial_replay",
        decision_manifest=manifest,
        baseline_manifest=manifest,
        replay_source_run_id="baseline-run",
        current_intent_semantic_fingerprint="a" * 16,
        current_knowledge_semantic_fingerprint="b" * 16,
        current_knowledge_provenance_fingerprint="d" * 16,
        current_stable_intent_sha256="f" * 64,
        intent_mandatory_partial_replay_passes=mandates,
    )
    assert validate_obj(obj=payload, kind="replay_decisions", version=1) == []
    record = PassReplayDecisionRecord.from_json_obj(payload["decisions"][0])  # type: ignore[arg-type]
    assert record.pass_name
    assert record.inputs_snapshot["baseline_present"] is True
    assert record.inputs_snapshot["manifest_stable_intent_sha256"] == "f" * 64
    assert record.inputs_snapshot["current_stable_intent_sha256"] == "f" * 64
    assert record.inputs_snapshot["intent_mandatory_partial_replay_passes"]


def test_replay_decisions_payload_records_absent_baseline_for_live_mode() -> None:
    payload = build_replay_decisions_payload(
        run_id="run_live_payload",
        tenant_id="tenant-a",
        repo_id="repo-a",
        replay_mode="live",
        decision_manifest=None,
        baseline_manifest=None,
        replay_source_run_id=None,
        current_intent_semantic_fingerprint="a" * 16,
        current_knowledge_semantic_fingerprint="b" * 16,
        current_knowledge_provenance_fingerprint="c" * 16,
    )
    assert validate_obj(obj=payload, kind="replay_decisions", version=1) == []
    record = PassReplayDecisionRecord.from_json_obj(payload["decisions"][0])  # type: ignore[arg-type]
    assert record.trigger_reason == "live_mode"
    assert record.inputs_snapshot["baseline_present"] is False
    assert record.inputs_snapshot["manifest_intent_semantic_fingerprint"] is None
    assert record.inputs_snapshot["effective_partial_replay_passes"] == []


def test_recompile_triggers_payload_validates() -> None:
    manifest = RunManifest(
        run_id="run_replay_payload",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="1" * 64,
        replay_mode="full_replay",
        intent_semantic_fingerprint="a" * 16,
    )
    payload = build_recompile_triggers_payload(
        tenant_id="tenant-a",
        repo_id="repo-a",
        run_id="run_replay_payload",
        checked_at_ms=123,
        source="compile_session",
        manifest=manifest,
        current_intent_semantic_fingerprint="b" * 16,
    )
    assert validate_obj(obj=payload, kind="recompile_triggers", version=1) == []


def test_partial_replay_stable_intent_byte_change_forces_model_passes() -> None:
    """Goal text or other non-semantic edits change stable_intent_sha256; replay must not reuse LLM cache."""
    manifest = RunManifest(
        run_id="run_stable_drift",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="a" * 64,
        replay_mode="partial_replay",
        intent_semantic_fingerprint="c" * 16,
        stable_intent_sha256="a" * 64,
        partial_replay_passes=(),
    )
    d = decide_replay_for_pass(
        manifest=manifest,
        pass_name="generate",
        current_intent_semantic_fingerprint="c" * 16,
        current_stable_intent_sha256="b" * 64,
    )
    assert d.should_call_model is True
    assert d.should_call_tools is True
    assert d.trigger is not None
    assert d.trigger.kind == "intent_stable_changed"


def test_recompile_triggers_payload_records_intent_stable_changed() -> None:
    manifest = RunManifest(
        run_id="run_stable_trigger",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="1" * 64,
        replay_mode="full_replay",
        intent_semantic_fingerprint="d" * 16,
        stable_intent_sha256="a" * 64,
    )
    payload = build_recompile_triggers_payload(
        tenant_id="tenant-a",
        repo_id="repo-a",
        run_id="run_stable_trigger",
        checked_at_ms=456,
        source="compile_session",
        manifest=manifest,
        current_intent_semantic_fingerprint="d" * 16,
        current_stable_intent_sha256="b" * 64,
    )
    assert validate_obj(obj=payload, kind="recompile_triggers", version=1) == []
    kinds = {t["kind"] for t in payload["triggers"]}  # type: ignore[index]
    assert "intent_stable_changed" in kinds


def test_partial_replay_deterministic_deployment_pass_only_reruns_when_selected() -> None:
    partial = RunManifest(
        run_id="run_partial_deploy",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="4" * 64,
        replay_mode="partial_replay",
        partial_replay_passes=("deployment_config",),
    )
    d_selected = decide_replay_for_pass(manifest=partial, pass_name="deployment_config")
    d_not_selected = decide_replay_for_pass(
        manifest=RunManifest(
            run_id="run_partial_none",
            tenant_id="tenant-a",
            repo_id="repo-a",
            ir_sha256="5" * 64,
            replay_mode="partial_replay",
            partial_replay_passes=("generate",),
        ),
        pass_name="deployment_config",
    )
    assert d_selected.should_call_model is False
    assert d_selected.should_call_tools is True
    assert d_not_selected.should_call_model is False
    assert d_not_selected.should_call_tools is False


def test_llm_vcr_replay_for_artifact_pass_keeps_tools_and_skips_model() -> None:
    manifest = RunManifest(
        run_id="run_llm_vcr_artifact",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="6" * 64,
        replay_mode="llm_vcr",
        llm_vcr={"artifact-key": '{"spec_version":1}'},
    )
    d = decide_replay_for_pass(manifest=manifest, pass_name="system_design")
    assert d.should_call_model is False
    assert d.should_call_tools is True


def test_full_replay_for_artifact_pass_skips_model_and_tools() -> None:
    manifest = RunManifest(
        run_id="run_full_replay_artifact",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="7" * 64,
        replay_mode="full_replay",
    )
    d = decide_replay_for_pass(manifest=manifest, pass_name="orchestration_spec")
    assert d.should_call_model is False
    assert d.should_call_tools is False


def test_partial_replay_for_artifact_pass_respects_selected_set() -> None:
    selected_manifest = RunManifest(
        run_id="run_partial_artifact_yes",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="8" * 64,
        replay_mode="partial_replay",
        partial_replay_passes=("agent_coordination",),
    )
    not_selected_manifest = RunManifest(
        run_id="run_partial_artifact_no",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="9" * 64,
        replay_mode="partial_replay",
        partial_replay_passes=("generate",),
    )
    d_selected = decide_replay_for_pass(manifest=selected_manifest, pass_name="agent_coordination")
    d_not_selected = decide_replay_for_pass(
        manifest=not_selected_manifest,
        pass_name="agent_coordination",
    )
    assert d_selected.should_call_model is False
    assert d_selected.should_call_tools is True
    assert d_not_selected.should_call_model is False
    assert d_not_selected.should_call_tools is False


def test_partial_replay_intent_mandates_force_runtime_bundle_for_operational_spec() -> None:
    mandates = mandatory_partial_replay_passes_for_success_criteria(
        success_criteria=(SimpleNamespace(evaluation_mode="operational_spec"),)
    )
    partial = RunManifest(
        run_id="run_partial_op",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="a" * 64,
        replay_mode="partial_replay",
        partial_replay_passes=("generate",),
    )
    d = decide_replay_for_pass(
        manifest=partial,
        pass_name="runtime_bundle",
        intent_mandatory_partial_replay_passes=mandates,
    )
    assert d.should_call_tools is True
    assert d.trigger_reason == "intent_mandatory_partial_replay"


def test_partial_replay_intent_mandates_force_runtime_bundle_for_metric_threshold() -> None:
    mandates = mandatory_partial_replay_passes_for_success_criteria(
        success_criteria=(SimpleNamespace(evaluation_mode="metric_threshold"),)
    )
    partial = RunManifest(
        run_id="run_partial_metric",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="a" * 64,
        replay_mode="partial_replay",
        partial_replay_passes=("generate",),
    )
    d = decide_replay_for_pass(
        manifest=partial,
        pass_name="runtime_bundle",
        intent_mandatory_partial_replay_passes=mandates,
    )
    assert d.should_call_tools is True
    assert d.trigger_reason == "intent_mandatory_partial_replay"


def test_partial_replay_runtime_bundle_pass_respects_selected_set() -> None:
    selected_manifest = RunManifest(
        run_id="run_partial_runtime_bundle_yes",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="a" * 64,
        replay_mode="partial_replay",
        partial_replay_passes=("runtime_bundle",),
    )
    not_selected_manifest = RunManifest(
        run_id="run_partial_runtime_bundle_no",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="b" * 64,
        replay_mode="partial_replay",
        partial_replay_passes=("generate",),
    )
    d_selected = decide_replay_for_pass(manifest=selected_manifest, pass_name="runtime_bundle")
    d_not_selected = decide_replay_for_pass(
        manifest=not_selected_manifest,
        pass_name="runtime_bundle",
    )
    assert d_selected.should_call_model is False
    assert d_selected.should_call_tools is True
    assert d_not_selected.should_call_model is False
    assert d_not_selected.should_call_tools is False


def test_manifest_rejects_unknown_replay_mode() -> None:
    with pytest.raises(ValueError, match="run_manifest.replay_mode must be one of"):
        RunManifest(
            run_id="run_bad",
            tenant_id="tenant-a",
            repo_id="repo-a",
            ir_sha256="1" * 64,
            replay_mode="random_mode",  # type: ignore[arg-type]
        )


def test_manifest_rejects_unknown_partial_replay_pass() -> None:
    with pytest.raises(ValueError, match="partial_replay_passes"):
        RunManifest(
            run_id="run_bad_partial",
            tenant_id="tenant-a",
            repo_id="repo-a",
            ir_sha256="1" * 64,
            replay_mode="partial_replay",
            partial_replay_passes=("random_pass",),
        )


def test_pass_record_rejects_unknown_status() -> None:
    with pytest.raises(ValueError, match="pass_record.status must be one of"):
        PassRecord(name="plan", status="unknown")  # type: ignore[arg-type]


def test_runtime_bundle_pass_record_requires_expected_metadata_shape() -> None:
    record = PassRecord(
        name="runtime_bundle",
        status="succeeded",
        output_sha256="a" * 64,
        metadata={
            "artifact_group": "runtime",
            "artifact_paths": [".akc/runtime/run-1.runtime_bundle.json"],
            "artifact_hashes": {".akc/runtime/run-1.runtime_bundle.json": "b" * 64},
            "runtime_bundle_path": ".akc/runtime/run-1.runtime_bundle.json",
            "referenced_node_count": 3,
            "referenced_contract_count": 2,
            "deployment_intent_count": 1,
            "orchestration_spec_sha256": "c" * 64,
            "coordination_spec_sha256": "d" * 64,
        },
    )
    assert record.metadata is not None
    assert record.metadata["runtime_bundle_path"] == ".akc/runtime/run-1.runtime_bundle.json"


def test_run_manifest_rejects_invalid_stable_intent_sha256() -> None:
    with pytest.raises(ValueError, match="run_manifest.stable_intent_sha256"):
        RunManifest(
            run_id="run_bad_intent_sha",
            tenant_id="tenant-a",
            repo_id="repo-a",
            ir_sha256="1" * 64,
            replay_mode="live",
            stable_intent_sha256="bad",
        )


def test_runtime_bundle_pass_record_rejects_invalid_metadata_shape() -> None:
    with pytest.raises(ValueError, match="runtime_bundle metadata.runtime_bundle_path"):
        PassRecord(
            name="runtime_bundle",
            status="succeeded",
            output_sha256="a" * 64,
            metadata={
                "artifact_group": "runtime",
                "artifact_paths": [".akc/runtime/run-1.runtime_bundle.json"],
                "artifact_hashes": {".akc/runtime/run-1.runtime_bundle.json": "b" * 64},
                "runtime_bundle_path": ".akc/runtime/other.runtime_bundle.json",
                "referenced_node_count": 3,
                "referenced_contract_count": 2,
                "deployment_intent_count": 1,
                "orchestration_spec_sha256": "c" * 64,
                "coordination_spec_sha256": "d" * 64,
            },
        )


def test_replay_decision_rejects_empty_pass_name() -> None:
    manifest = RunManifest(
        run_id="run_x",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="f" * 64,
        replay_mode="live",
    )
    with pytest.raises(ValueError, match="replay.pass_name"):
        decide_replay_for_pass(manifest=manifest, pass_name="")


def test_run_manifest_roundtrip_from_json_obj_and_file(tmp_path: Path) -> None:
    manifest = _manifest()
    obj = manifest.to_json_obj()
    parsed = RunManifest.from_json_obj(obj)
    assert parsed.to_json_obj() == obj

    fp = tmp_path / "run.manifest.json"
    fp.write_text(json.dumps(obj), encoding="utf-8")
    parsed_file = RunManifest.from_json_file(fp)
    assert parsed_file.to_json_obj() == obj


def test_run_manifest_knowledge_mediation_pointer_roundtrip() -> None:
    med_sha = "9" * 64
    m = RunManifest(
        run_id="run_km",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="a" * 64,
        replay_mode="live",
        knowledge_mediation=ArtifactPointer(path=".akc/knowledge/mediation.json", sha256=med_sha),
    )
    obj = m.to_json_obj()
    back = RunManifest.from_json_obj(obj)
    assert back.knowledge_mediation is not None
    assert back.knowledge_mediation.path == ".akc/knowledge/mediation.json"
    assert back.knowledge_mediation.sha256 == med_sha
    assert back.to_json_obj() == obj


def test_runtime_replay_reconstructs_transitions_and_budget_burn() -> None:
    event = {
        "event_id": "evt-1",
        "event_type": "runtime.action.completed",
        "timestamp": 20,
        "context": {
            "tenant_id": "tenant-a",
            "repo_id": "repo-a",
            "run_id": "run-runtime",
            "runtime_run_id": "runtime-001",
            "policy_mode": "enforce",
            "adapter_id": "native",
        },
        "payload": {
            "action": {
                "action_id": "node-a:evt-1",
                "action_type": "agent.execute",
                "node_ref": {"node_id": "node-a", "kind": "agent", "contract_id": "c-1"},
                "inputs_fingerprint": "in-1",
                "idempotency_key": "idem-1",
            },
            "transition": {
                "from_state": "ready",
                "to_state": "completed",
                "trigger_id": "runtime.action.completed",
                "transition_id": "node-a:completed",
                "occurred_at": 20,
            },
        },
    }
    manifest = RunManifest(
        run_id="run-runtime",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="1" * 64,
        replay_mode="runtime_replay",
        runtime_bundle=ArtifactPointer(path=".akc/runtime/run-runtime.bundle.json", sha256="2" * 64),
        runtime_event_transcript=ArtifactPointer(
            path=".akc/runtime/run-runtime.events.jsonl",
            sha256="3" * 64,
        ),
        runtime_evidence=(
            RuntimeEvidenceRecord(
                evidence_type="action_decision",
                timestamp=10,
                runtime_run_id="runtime-001",
                payload={"action_id": "node-a:evt-1", "decision": "allowed"},
            ),
            RuntimeEvidenceRecord(
                evidence_type="retry_budget",
                timestamp=15,
                runtime_run_id="runtime-001",
                payload={
                    "action_id": "node-a:evt-1",
                    "retry_count": 2,
                    "budget_burn": {"attempts": 2, "tokens": 7},
                },
            ),
            RuntimeEvidenceRecord(
                evidence_type="transition_application",
                timestamp=20,
                runtime_run_id="runtime-001",
                payload={
                    "action_id": "node-a:evt-1",
                    "transition": dict(event["payload"]["transition"]),
                },
            ),
            RuntimeEvidenceRecord(
                evidence_type="terminal_health",
                timestamp=30,
                runtime_run_id="runtime-001",
                payload={"resource_id": "node-a", "health_status": "healthy"},
            ),
        ),
        control_plane={"runtime_run_id": "runtime-001"},
    )

    replay = replay_runtime_execution(manifest=manifest, transcript=(event,))

    assert replay.mode == "runtime_replay"
    assert replay.runtime_run_id == "runtime-001"
    assert len(replay.transitions) == 1
    assert replay.transitions[0].transition is not None
    assert replay.transitions[0].transition["to_state"] == "completed"
    assert replay.transitions[0].action_decision == "allowed"
    assert replay.transitions[0].retry_count == 2
    assert replay.transitions[0].budget_burn == {"attempts": 2, "tokens": 7}
    assert replay.terminal_health_status == "healthy"


def test_reconcile_replay_reconstructs_dry_run_decisions() -> None:
    manifest = RunManifest(
        run_id="run-reconcile",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="4" * 64,
        replay_mode="reconcile_replay",
        runtime_bundle=ArtifactPointer(path=".akc/runtime/run-reconcile.bundle.json", sha256="5" * 64),
        runtime_event_transcript=ArtifactPointer(
            path=".akc/runtime/run-reconcile.events.jsonl",
            sha256="6" * 64,
        ),
        runtime_evidence=(
            RuntimeEvidenceRecord(
                evidence_type="rollback_chain",
                timestamp=10,
                runtime_run_id="runtime-002",
                payload={"resource_id": "svc-a", "chain": ["hash-prev", "hash-old"]},
            ),
            RuntimeEvidenceRecord(
                evidence_type="reconcile_outcome",
                timestamp=20,
                runtime_run_id="runtime-002",
                payload={
                    "resource_id": "svc-a",
                    "operation_type": "update",
                    "applied": False,
                    "health_status": "degraded",
                    "reason": "dry-run",
                },
            ),
            RuntimeEvidenceRecord(
                evidence_type="terminal_health",
                timestamp=30,
                runtime_run_id="runtime-002",
                payload={"resource_id": "svc-a", "health_status": "degraded"},
            ),
        ),
        control_plane={"runtime_run_id": "runtime-002"},
    )

    replay = replay_runtime_execution(manifest=manifest)

    assert replay.mode == "reconcile_replay"
    assert replay.transitions == ()
    assert replay.terminal_health_status == "degraded"
    assert replay.reconcile_decisions == (
        ReconcileReplayDecision(
            resource_id="svc-a",
            operation_type="update",
            applied=False,
            rollback_chain=("hash-prev", "hash-old"),
            health_status="degraded",
            payload={
                "resource_id": "svc-a",
                "operation_type": "update",
                "applied": False,
                "health_status": "degraded",
                "reason": "dry-run",
            },
        ),
    )


def test_run_manifest_rejects_invalid_trace_span_shape() -> None:
    with pytest.raises(ValueError, match="trace_span\\.trace_id"):
        RunManifest(
            run_id="run_bad_trace",
            tenant_id="tenant-a",
            repo_id="repo-a",
            ir_sha256="1" * 64,
            replay_mode="live",
            trace_spans=(
                {
                    "trace_id": "",
                    "span_id": "s1",
                    "name": "compile.run",
                    "kind": "internal",
                    "start_time_unix_nano": 1,
                    "end_time_unix_nano": 2,
                },
            ),
        )


def test_loader_enforces_scope_checks(tmp_path: Path) -> None:
    manifest = _manifest()
    fp = tmp_path / "run.manifest.json"
    fp.write_text(json.dumps(manifest.to_json_obj()), encoding="utf-8")

    loaded = load_run_manifest(path=fp, expected_tenant_id="tenant-a", expected_repo_id="repo-a")
    assert loaded.run_id == manifest.run_id

    with pytest.raises(ValueError, match="tenant_id does not match"):
        load_run_manifest(path=fp, expected_tenant_id="tenant-b")

    with pytest.raises(ValueError, match="repo_id does not match"):
        load_run_manifest(path=fp, expected_repo_id="repo-b")


def test_find_latest_run_manifest_prefers_newest_file(tmp_path: Path) -> None:
    run_dir = tmp_path / "tenant-a" / "repo-a" / ".akc" / "run"
    run_dir.mkdir(parents=True, exist_ok=True)
    old_fp = run_dir / "old.manifest.json"
    new_fp = run_dir / "new.manifest.json"
    old_fp.write_text("{}", encoding="utf-8")
    new_fp.write_text("{}", encoding="utf-8")
    os.utime(old_fp, (1, 1))
    os.utime(new_fp, (2, 2))

    latest = find_latest_run_manifest(outputs_root=tmp_path, tenant_id="tenant-a", repo_id="repo-a")
    assert latest is not None
    assert latest.name == "new.manifest.json"
