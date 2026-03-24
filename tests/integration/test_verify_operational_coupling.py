"""Integration: verify couples intent authority, runtime evidence, report attestation, and strict manifest checks."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.artifacts.contracts import schema_id_for
from akc.cli import main
from akc.cli.runtime import _maybe_write_operational_validity_report, _pointer_for_json_file
from akc.intent import IntentSpecV1, JsonFileIntentStore, Objective, OperatingBound, SuccessCriterion
from akc.intent.policy_projection import build_handoff_intent_ref, project_runtime_intent_projection
from akc.run.manifest import ArtifactPointer, RunManifest, RuntimeEvidenceRecord


def _intent(*, tenant_id: str, repo_id: str) -> IntentSpecV1:
    return IntentSpecV1(
        intent_id="intent-op-verify",
        tenant_id=tenant_id,
        repo_id=repo_id,
        spec_version=1,
        status="active",
        title="Operational verify",
        goal_statement="Verify runtime operational attestation",
        summary="Operational verify coupling",
        derived_from_goal_text=False,
        objectives=(Objective(id="obj-1", priority=1, statement="stay healthy", target="runtime"),),
        constraints=(),
        policies=(),
        success_criteria=(
            SuccessCriterion(
                id="sc-op-1",
                evaluation_mode="operational_spec",
                description="runtime health",
                params={
                    "spec_version": 1,
                    "window": "single_run",
                    "predicate_kind": "presence",
                    "signals": [
                        {"evidence_type": "terminal_health", "payload_path": "health_status"},
                    ],
                    "expected_evidence_types": ["terminal_health"],
                    "evaluation_phase": "post_runtime",
                },
            ),
        ),
        operating_bounds=OperatingBound(max_seconds=None, max_steps=None, allow_network=False),
        assumptions=(),
        risk_notes=(),
        tags=(),
        metadata=None,
        created_at_ms=1,
        updated_at_ms=2,
    )


def _bundle_payload(*, tenant_id: str, repo_id: str, run_id: str, intent: IntentSpecV1) -> dict[str, object]:
    return {
        "schema_version": 1,
        "schema_id": schema_id_for(kind="runtime_bundle"),
        "tenant_id": tenant_id,
        "repo_id": repo_id,
        "run_id": run_id,
        "intent_ref": build_handoff_intent_ref(intent=intent),
        "intent_policy_projection": project_runtime_intent_projection(intent=intent).to_json_obj(),
        "referenced_ir_nodes": [],
        "referenced_contracts": [],
        "spec_hashes": {
            "orchestration_spec_sha256": "a" * 64,
            "coordination_spec_sha256": "b" * 64,
        },
        "deployment_intents": [],
        "runtime_policy_envelope": {},
    }


def _seed_runtime_artifacts(tmp_path: Path) -> tuple[Path, str, str, str]:
    outputs_root = tmp_path / "out"
    tenant_id = "tenant-a"
    repo_id = "repo-a"
    run_id = "compile-1"
    scope_root = outputs_root / tenant_id / repo_id
    runtime_run_id = "runtime-1"

    intent = _intent(tenant_id=tenant_id, repo_id=repo_id)
    # Match compile session + operational verify: store root is top-level outputs_root.
    JsonFileIntentStore(base_dir=outputs_root).save_intent(tenant_id=tenant_id, repo_id=repo_id, intent=intent)

    bundle_path = scope_root / ".akc" / "runtime" / f"{run_id}.runtime_bundle.json"
    bundle_path.parent.mkdir(parents=True, exist_ok=True)
    bundle_payload = _bundle_payload(tenant_id=tenant_id, repo_id=repo_id, run_id=run_id, intent=intent)
    bundle_path.write_text(json.dumps(bundle_payload), encoding="utf-8")

    runtime_scope = scope_root / ".akc" / "runtime" / run_id / runtime_run_id
    runtime_scope.mkdir(parents=True, exist_ok=True)
    evidence_path = runtime_scope / "runtime_evidence.json"
    evidence = (
        RuntimeEvidenceRecord(
            evidence_type="terminal_health",
            timestamp=1,
            runtime_run_id=runtime_run_id,
            payload={
                "resource_id": "__runtime_aggregate__",
                "health_status": "healthy",
                "aggregate": True,
                "runtime_status": "terminal",
                "kernel_terminal_status": "terminal",
                "tenant_id": tenant_id,
                "repo_id": repo_id,
                "compile_run_id": run_id,
            },
        ),
    )
    evidence_path.write_text(json.dumps([item.to_json_obj() for item in evidence], indent=2), encoding="utf-8")

    policy_decisions_path = runtime_scope / "policy_decisions.json"
    policy_decisions_path.write_text("[]", encoding="utf-8")

    record = {
        "tenant_id": tenant_id,
        "repo_id": repo_id,
        "run_id": run_id,
        "runtime_run_id": runtime_run_id,
        "outputs_root": str(outputs_root.resolve()),
        "bundle_path": str(bundle_path.resolve()),
        "scope_dir": str(runtime_scope.resolve()),
        "runtime_evidence_path": str(evidence_path.resolve()),
        "policy_decisions_path": str(policy_decisions_path.resolve()),
    }
    summary = _maybe_write_operational_validity_report(record, evidence=evidence)
    assert summary.ran_evaluation is True
    assert summary.passed_all is True

    manifest_path = scope_root / ".akc" / "run" / f"{run_id}.manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    control_plane = {
        "runtime_run_id": runtime_run_id,
        "runtime_evidence_ref": _pointer_for_json_file(evidence_path, record=record).to_json_obj(),
        "policy_decisions_ref": _pointer_for_json_file(policy_decisions_path, record=record).to_json_obj(),
        "operational_validity_report_ref": _pointer_for_json_file(
            runtime_scope / "operational_validity_report.json", record=record
        ).to_json_obj(),
        "operational_validity_passed": bool(record["operational_validity_passed"]),
        "operational_validity_fingerprint_sha256": str(record["operational_validity_fingerprint_sha256"]),
        "schema_id": "akc:control_plane_envelope:v1",
        "schema_version": 1,
    }
    manifest = RunManifest(
        run_id=run_id,
        tenant_id=tenant_id,
        repo_id=repo_id,
        ir_sha256="a" * 64,
        replay_mode="live",
        runtime_bundle=ArtifactPointer(
            path=f".akc/runtime/{run_id}.runtime_bundle.json",
            sha256=_pointer_for_json_file(bundle_path, record=record).sha256,
        ),
        runtime_evidence=evidence,
        control_plane=control_plane,
    )
    manifest_path.write_text(json.dumps(manifest.to_json_obj(), indent=2), encoding="utf-8")
    return outputs_root, tenant_id, repo_id, run_id


def test_verify_operational_coupling_passes_with_runtime_report_and_strict_manifest(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    outputs_root, tenant_id, repo_id, run_id = _seed_runtime_artifacts(tmp_path)
    capsys.readouterr()

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "verify",
                "--tenant-id",
                tenant_id,
                "--repo-id",
                repo_id,
                "--outputs-root",
                str(outputs_root),
                "--run-id",
                run_id,
                "--mode",
                "strict",
                "--show-findings",
            ]
        )
    assert excinfo.value.code == 0
    out = capsys.readouterr().out
    assert "Operational verification for run_id=compile-1:" in out
    assert "authority: recomputed" in out
    assert "strict_manifest_consistency: checked" in out
    assert "passed: True" in out


def test_verify_operational_coupling_supports_verify_only_without_report(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    outputs_root, tenant_id, repo_id, run_id = _seed_runtime_artifacts(tmp_path)
    capsys.readouterr()

    report_path = next(outputs_root.rglob("operational_validity_report.json"))
    report_path.unlink()
    manifest_path = outputs_root / tenant_id / repo_id / ".akc" / "run" / f"{run_id}.manifest.json"
    manifest = RunManifest.from_json_file(manifest_path)
    cp = dict(manifest.control_plane or {})
    cp.pop("operational_validity_report_ref", None)
    cp.pop("operational_validity_fingerprint_sha256", None)
    cp.pop("operational_validity_passed", None)
    updated = RunManifest(
        run_id=manifest.run_id,
        tenant_id=manifest.tenant_id,
        repo_id=manifest.repo_id,
        ir_sha256=manifest.ir_sha256,
        replay_mode=manifest.replay_mode,
        intent_semantic_fingerprint=manifest.intent_semantic_fingerprint,
        intent_goal_text_fingerprint=manifest.intent_goal_text_fingerprint,
        stable_intent_sha256=manifest.stable_intent_sha256,
        success_criteria_evaluation_modes_schema_version=manifest.success_criteria_evaluation_modes_schema_version,
        success_criteria_evaluation_modes=manifest.success_criteria_evaluation_modes,
        intent_acceptance_fingerprint=manifest.intent_acceptance_fingerprint,
        knowledge_semantic_fingerprint=manifest.knowledge_semantic_fingerprint,
        knowledge_provenance_fingerprint=manifest.knowledge_provenance_fingerprint,
        knowledge_snapshot=manifest.knowledge_snapshot,
        knowledge_mediation=manifest.knowledge_mediation,
        ir_document=manifest.ir_document,
        ir_format_version=manifest.ir_format_version,
        retrieval_snapshots=manifest.retrieval_snapshots,
        passes=manifest.passes,
        model=manifest.model,
        model_params=manifest.model_params,
        tool_params=manifest.tool_params,
        partial_replay_passes=manifest.partial_replay_passes,
        llm_vcr=manifest.llm_vcr,
        budgets=manifest.budgets,
        output_hashes=manifest.output_hashes,
        runtime_bundle=manifest.runtime_bundle,
        runtime_event_transcript=manifest.runtime_event_transcript,
        runtime_evidence=manifest.runtime_evidence,
        trace_spans=manifest.trace_spans,
        control_plane=cp,
        cost_attribution=manifest.cost_attribution,
        manifest_version=manifest.manifest_version,
    )
    manifest_path.write_text(json.dumps(updated.to_json_obj(), indent=2, sort_keys=True), encoding="utf-8")

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "verify",
                "--tenant-id",
                tenant_id,
                "--repo-id",
                repo_id,
                "--outputs-root",
                str(outputs_root),
                "--run-id",
                run_id,
                "--mode",
                "strict",
                "--show-findings",
            ]
        )
    assert excinfo.value.code == 0
    out = capsys.readouterr().out
    assert "report_present: no" in out
    assert "strict_manifest_consistency: skipped" in out
    assert "[warning] operational.report_missing_recomputed" in out
    assert "passed: True" in out


def test_verify_operational_coupling_rejects_report_intent_ref_divergence(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    outputs_root, tenant_id, repo_id, run_id = _seed_runtime_artifacts(tmp_path)
    capsys.readouterr()

    report_path = next(outputs_root.rglob("operational_validity_report.json"))
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["intent_ref"]["intent_id"] = "intent-other"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "verify",
                "--tenant-id",
                tenant_id,
                "--repo-id",
                repo_id,
                "--outputs-root",
                str(outputs_root),
                "--run-id",
                run_id,
                "--mode",
                "strict",
            ]
        )
    assert excinfo.value.code == 2
    err = capsys.readouterr().err
    assert "tenant/repo trust boundary violated for tenant-a/repo-a" in err
    assert "runtime bundle intent_ref diverges from operational_validity_report intent_ref" in err
