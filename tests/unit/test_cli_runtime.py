from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.artifacts.contracts import schema_id_for
from akc.artifacts.validate import validate_artifact_json
from akc.cli import main
from akc.promotion import canonical_sha256
from akc.run import RunManifest


def _write_runtime_bundle(
    path: Path,
    *,
    tenant_id: str = "tenant-a",
    repo_id: str = "repo-a",
    stable_intent_sha256: str | None = None,
    deployment_provider_kind: str | None = None,
    deployment_provider_contract: dict[str, object] | None = None,
    deployment_provider: dict[str, object] | None = None,
    layer_replacement_mode: str | None = None,
) -> None:
    payload: dict[str, object] = {
        "schema_version": 1,
        "schema_id": schema_id_for(kind="runtime_bundle"),
        "tenant_id": tenant_id,
        "repo_id": repo_id,
        "run_id": "run-1",
        "referenced_ir_nodes": [],
        "referenced_contracts": [],
        "spec_hashes": {
            "orchestration_spec_sha256": "a" * 64,
            "coordination_spec_sha256": "b" * 64,
        },
        "deployment_intents": [],
        "runtime_policy_envelope": {},
    }
    if stable_intent_sha256 is not None:
        payload["intent_ref"] = {
            "intent_id": "intent-1",
            "stable_intent_sha256": stable_intent_sha256,
            "semantic_fingerprint": "1" * 16,
            "goal_text_fingerprint": "2" * 16,
        }
    if deployment_provider_kind is not None:
        payload["deployment_provider"] = {"kind": deployment_provider_kind}
    if deployment_provider is not None:
        payload["deployment_provider"] = deployment_provider
    if deployment_provider_contract is not None:
        payload["deployment_provider_contract"] = deployment_provider_contract
    if layer_replacement_mode is not None:
        payload["layer_replacement_mode"] = layer_replacement_mode
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_compile_manifest_with_packet(
    *,
    outputs_root: Path,
    tenant_id: str,
    repo_id: str,
    run_id: str,
    promotion_mode: str,
    allow: bool = True,
) -> None:
    base = outputs_root / tenant_id / repo_id
    packet_rel = f".akc/promotion/{run_id}_step-1.packet.json"
    packet_obj: dict[str, object] = {
        "schema_version": 1,
        "schema_id": schema_id_for(kind="promotion_packet"),
        "packet_version": 1,
        "run_ref": {
            "tenant_id": tenant_id,
            "repo_id": repo_id,
            "run_id": run_id,
            "step_id": "step-1",
        },
        "intent_ref": {
            "intent_id": "intent-1",
            "stable_intent_sha256": "c" * 64,
            "semantic_fingerprint": "1" * 16,
            "goal_text_fingerprint": "2" * 16,
        },
        "promotion_mode": promotion_mode,
        "promotion_state": promotion_mode,
        "patch_hash_sha256": "d" * 64,
        "touched_paths": ["src/example.py"],
        "required_tests": [{"stage": "tests_smoke", "command": ["pytest", "-q"], "exit_code": 0, "passed": True}],
        "verifier_result": {"passed": True},
        "policy_decision_trace": [{"allowed": True, "token_id": "tok-1", "reason": "ok"}],
        "policy_allow_decision": {"allowed": bool(allow), "token_id": "tok-1", "reason": "ok"},
        "compile_apply_attestation": {
            "compile_realization_mode": "artifact_only",
            "applied": False,
            "apply_decision_token_id": "",
            "policy_allow_decision": {"allowed": False, "reason": "artifact_only"},
            "patch_fingerprint_sha256": "d" * 64,
            "scope_root": None,
            "touched_paths": ["src/example.py"],
        },
        "apply_target_metadata": {"deployment_provider": {"kind": "kubernetes_apply"}},
        "issued_at_ms": 1,
    }
    packet_obj["packet_signature_sha256"] = canonical_sha256(
        {k: v for k, v in packet_obj.items() if k != "packet_signature_sha256"}
    )
    packet_path = base / packet_rel
    packet_path.parent.mkdir(parents=True, exist_ok=True)
    packet_path.write_text(json.dumps(packet_obj), encoding="utf-8")
    manifest_path = base / ".akc" / "run" / f"{run_id}.manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest = RunManifest(
        run_id=run_id,
        tenant_id=tenant_id,
        repo_id=repo_id,
        ir_sha256="1" * 64,
        replay_mode="live",
        control_plane={
            "promotion_mode": promotion_mode,
            "promotion_state": promotion_mode,
            "promotion_packet_ref": {"path": packet_rel, "sha256": canonical_sha256(packet_obj)},
            "compile_apply_attestation": dict(packet_obj["compile_apply_attestation"]),
        },
    )
    manifest_path.write_text(json.dumps(manifest.to_json_obj()), encoding="utf-8")


def _load_runtime_record(outputs_root: Path) -> dict[str, object]:
    record_path = next(outputs_root.rglob("runtime_run.json"))
    return json.loads(record_path.read_text(encoding="utf-8"))


def test_cli_runtime_start_status_and_replay(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    outputs_root = tmp_path / "out"
    bundle_path = outputs_root / "tenant-a" / "repo-a" / ".akc" / "runtime" / "run-1.runtime_bundle.json"
    _write_runtime_bundle(bundle_path)

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "runtime",
                "start",
                "--bundle",
                str(bundle_path),
                "--mode",
                "simulate",
                "--outputs-root",
                str(outputs_root),
            ]
        )
    assert excinfo.value.code == 0

    record = _load_runtime_record(outputs_root)
    runtime_run_id = str(record["runtime_run_id"])
    out = capsys.readouterr().out
    assert f"runtime_run_id: {runtime_run_id}" in out
    assert "status: terminal" in out

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "runtime",
                "status",
                "--runtime-run-id",
                runtime_run_id,
                "--outputs-root",
                str(outputs_root),
                "--tenant-id",
                "tenant-a",
                "--repo-id",
                "repo-a",
            ]
        )
    assert excinfo.value.code == 0
    status_out = capsys.readouterr().out
    assert "checkpoint_present: yes" in status_out
    assert "runtime_evidence_count:" in status_out

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "runtime",
                "replay",
                "--runtime-run-id",
                runtime_run_id,
                "--mode",
                "runtime_replay",
                "--outputs-root",
                str(outputs_root),
                "--tenant-id",
                "tenant-a",
                "--repo-id",
                "repo-a",
            ]
        )
    assert excinfo.value.code == 0
    replay_out = json.loads(capsys.readouterr().out)
    assert replay_out["runtime_run_id"] == runtime_run_id
    assert replay_out["mode"] == "runtime_replay"
    # Aggregate terminal health prefers kernel outcome when there is no reconcile surface.
    assert replay_out["terminal_health_status"] == "healthy"


def test_cli_runtime_start_emerging_profile_can_resolve_latest_bundle_and_defaults(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    outputs_root = tmp_path / "out"
    bundle_path = outputs_root / "tenant-a" / "repo-a" / ".akc" / "runtime" / "run-1.runtime_bundle.json"
    _write_runtime_bundle(bundle_path)

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "runtime",
                "start",
                "--outputs-root",
                str(outputs_root),
                "--developer-role-profile",
                "emerging",
            ]
        )
    assert excinfo.value.code == 0
    out = capsys.readouterr().out
    assert "status: terminal" in out

    record = _load_runtime_record(outputs_root)
    assert record["developer_role_profile"] == "emerging"
    ref = record.get("developer_profile_decisions_ref")
    assert isinstance(ref, dict)
    assert str(ref.get("path", "")).endswith("developer_profile_decisions.json")


def test_cli_runtime_start_emerging_resolves_outputs_root_from_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    outputs_root = tmp_path / "out"
    bundle_path = outputs_root / "tenant-a" / "repo-a" / ".akc" / "runtime" / "run-1.runtime_bundle.json"
    _write_runtime_bundle(bundle_path)
    monkeypatch.setenv("AKC_OUTPUTS_ROOT", str(outputs_root))

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "runtime",
                "start",
                "--developer-role-profile",
                "emerging",
            ]
        )
    assert excinfo.value.code == 0
    assert "status: terminal" in capsys.readouterr().out

    record = _load_runtime_record(outputs_root)
    ref = record.get("developer_profile_decisions_ref")
    assert isinstance(ref, dict)
    rel = str(ref.get("path", ""))
    scope_root = outputs_root / "tenant-a" / "repo-a"
    decisions = json.loads((scope_root / rel).read_text(encoding="utf-8"))
    assert decisions["resolved"]["outputs_root"]["source"] == "env"


def test_cli_runtime_start_records_coordination_execution_overrides(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    outputs_root = tmp_path / "out"
    bundle_path = outputs_root / "tenant-a" / "repo-a" / ".akc" / "runtime" / "run-1.runtime_bundle.json"
    _write_runtime_bundle(bundle_path)
    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "runtime",
                "start",
                "--bundle",
                str(bundle_path),
                "--mode",
                "simulate",
                "--outputs-root",
                str(outputs_root),
                "--coordination-parallel-dispatch",
                "enabled",
                "--coordination-max-in-flight-steps",
                "3",
                "--coordination-max-in-flight-per-role",
                "1",
            ]
        )
    assert excinfo.value.code == 0
    _ = capsys.readouterr()
    record = _load_runtime_record(outputs_root)
    policy = record.get("coordination_execution_policy")
    assert isinstance(policy, dict)
    assert policy["parallel_dispatch_enabled"] is True
    assert policy["max_in_flight_steps"] == 3
    assert policy["max_in_flight_per_role"] == 1
    assert policy["completion_fold_order"] == "coordination_step_id"


def test_cli_runtime_start_updates_existing_compile_run_manifest(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    outputs_root = tmp_path / "out"
    bundle_path = outputs_root / "tenant-a" / "repo-a" / ".akc" / "runtime" / "run-1.runtime_bundle.json"
    _write_runtime_bundle(bundle_path)
    run_manifest_path = outputs_root / "tenant-a" / "repo-a" / ".akc" / "run" / "run-1.manifest.json"
    run_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    run_manifest = RunManifest(
        run_id="run-1",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="1" * 64,
        replay_mode="live",
        control_plane={"policy_decisions": [{"allowed": True, "reason": "compile"}]},
    )
    run_manifest_path.write_text(json.dumps(run_manifest.to_json_obj()), encoding="utf-8")

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "runtime",
                "start",
                "--bundle",
                str(bundle_path),
                "--mode",
                "simulate",
                "--outputs-root",
                str(outputs_root),
            ]
        )
    assert excinfo.value.code == 0
    runtime_run_id = str(_load_runtime_record(outputs_root)["runtime_run_id"])
    capsys.readouterr()

    updated = RunManifest.from_json_file(run_manifest_path)
    assert updated.replay_mode == "live"
    assert updated.runtime_bundle is not None
    assert updated.runtime_bundle.path == ".akc/runtime/run-1.runtime_bundle.json"
    assert updated.runtime_event_transcript is not None
    assert updated.runtime_event_transcript.path == f".akc/runtime/run-1/{runtime_run_id}/events.json"
    assert updated.runtime_evidence
    assert updated.control_plane is not None
    assert updated.control_plane["policy_decisions"] == [{"allowed": True, "reason": "compile"}]
    assert updated.control_plane["runtime_run_id"] == runtime_run_id
    evidence_ref = updated.control_plane["runtime_evidence_ref"]
    assert isinstance(evidence_ref, dict)
    assert evidence_ref["path"] == f".akc/runtime/run-1/{runtime_run_id}/runtime_evidence.json"
    policy_ref = updated.control_plane["policy_decisions_ref"]
    assert isinstance(policy_ref, dict)
    assert policy_ref["path"] == f".akc/runtime/run-1/{runtime_run_id}/policy_decisions.json"


def test_runtime_start_live_mutation_requires_signed_live_apply_packet(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    outputs_root = tmp_path / "out"
    bundle_path = outputs_root / "tenant-a" / "repo-a" / ".akc" / "runtime" / "run-1.runtime_bundle.json"
    _write_runtime_bundle(bundle_path, deployment_provider_kind="kubernetes_apply")

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "runtime",
                "start",
                "--bundle",
                str(bundle_path),
                "--mode",
                "enforce",
                "--outputs-root",
                str(outputs_root),
            ]
        )
    assert excinfo.value.code == 2
    cap = capsys.readouterr()
    assert "requires a signed promotion packet" in cap.out + cap.err


def test_runtime_start_live_mutation_fails_when_compile_apply_manifest_mismatch(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    outputs_root = tmp_path / "out"
    bundle_path = outputs_root / "tenant-a" / "repo-a" / ".akc" / "runtime" / "run-1.runtime_bundle.json"
    _write_runtime_bundle(bundle_path, deployment_provider_kind="kubernetes_apply")
    _write_compile_manifest_with_packet(
        outputs_root=outputs_root,
        tenant_id="tenant-a",
        repo_id="repo-a",
        run_id="run-1",
        promotion_mode="live_apply",
        allow=True,
    )
    manifest_path = outputs_root / "tenant-a" / "repo-a" / ".akc" / "run" / "run-1.manifest.json"
    loaded = json.loads(manifest_path.read_text(encoding="utf-8"))
    cp = loaded.get("control_plane")
    assert isinstance(cp, dict)
    caa = cp.get("compile_apply_attestation")
    assert isinstance(caa, dict)
    caa["patch_fingerprint_sha256"] = "e" * 64
    manifest_path.write_text(json.dumps(loaded), encoding="utf-8")

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "runtime",
                "start",
                "--bundle",
                str(bundle_path),
                "--mode",
                "enforce",
                "--outputs-root",
                str(outputs_root),
            ]
        )
    assert excinfo.value.code == 2
    cap = capsys.readouterr()
    assert "compile-apply attestation mismatch" in cap.out + cap.err


def test_runtime_start_live_mutation_accepts_valid_live_apply_packet(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    outputs_root = tmp_path / "out"
    bundle_path = outputs_root / "tenant-a" / "repo-a" / ".akc" / "runtime" / "run-1.runtime_bundle.json"
    _write_runtime_bundle(bundle_path, deployment_provider_kind="kubernetes_apply")
    _write_compile_manifest_with_packet(
        outputs_root=outputs_root,
        tenant_id="tenant-a",
        repo_id="repo-a",
        run_id="run-1",
        promotion_mode="live_apply",
        allow=True,
    )

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "runtime",
                "start",
                "--bundle",
                str(bundle_path),
                "--mode",
                "enforce",
                "--outputs-root",
                str(outputs_root),
            ]
        )
    assert excinfo.value.code == 0
    assert "status: terminal" in capsys.readouterr().out


def test_runtime_evidence_schema_validation_requires_payload_keys() -> None:
    valid = [
        {
            "evidence_type": "action_decision",
            "timestamp": 1,
            "runtime_run_id": "runtime-1",
            "payload": {"action_id": "action-1", "decision": "allowed"},
        },
        {
            "evidence_type": "terminal_health",
            "timestamp": 2,
            "runtime_run_id": "runtime-1",
            "payload": {"resource_id": "svc-1", "health_status": "healthy"},
        },
        {
            "evidence_type": "reconcile_resource_status",
            "timestamp": 3,
            "runtime_run_id": "runtime-1",
            "payload": {
                "resource_id": "svc-1",
                "converged": True,
                "conditions": [{"type": "degraded", "status": "false"}],
                "observed_hash": "abc",
                "health_status": "healthy",
            },
        },
        {
            "evidence_type": "convergence_certificate",
            "timestamp": 4,
            "runtime_run_id": "runtime-1",
            "payload": {
                "resource_id": "svc-1",
                "certificate_schema_version": 1,
                "desired_hash": "d1",
                "observed_hash": "o1",
                "health": "healthy",
                "attempts": 1,
                "window_ms": 0,
                "provider_id": "in_memory",
                "policy_mode": "simulate",
                "converged": True,
            },
        },
        {
            "evidence_type": "provider_capability_snapshot",
            "timestamp": 5,
            "runtime_run_id": "runtime-1",
            "payload": {
                "provider_id": "in_memory",
                "mutation_mode": "observe_only",
                "rollback_mode": "none",
                "rollback_determinism": "deterministic",
            },
        },
        {
            "evidence_type": "rollback_attempt",
            "timestamp": 6,
            "runtime_run_id": "runtime-1",
            "payload": {"resource_id": "svc-1", "rollback_target_hash": "h1"},
        },
        {
            "evidence_type": "rollback_result",
            "timestamp": 7,
            "runtime_run_id": "runtime-1",
            "payload": {"resource_id": "svc-1", "rollback_outcome": "rollback_applied"},
        },
    ]
    assert validate_artifact_json(obj=valid, kind="runtime_evidence_stream", enabled=True) == []

    with pytest.raises(ValueError, match="runtime_evidence_stream"):
        validate_artifact_json(
            obj=[
                {
                    "evidence_type": "retry_budget",
                    "timestamp": 3,
                    "runtime_run_id": "runtime-1",
                    "payload": {"action_id": "action-1", "budget_burn": {"retries": 1}},
                }
            ],
            kind="runtime_evidence_stream",
            enabled=True,
        )


def test_runtime_start_preflight_contract_fails_when_required_env_missing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    outputs_root = tmp_path / "out"
    bundle_path = outputs_root / "tenant-a" / "repo-a" / ".akc" / "runtime" / "run-1.runtime_bundle.json"
    _write_runtime_bundle(
        bundle_path,
        deployment_provider_contract={
            "kind": "in_memory",
            "mutation_mode": "observe_only",
            "rollback_mode": "none",
            "rollback_determinism": "deterministic",
            "required_env_flags": ["AKC_ENABLE_EXTERNAL_DEPLOYMENT_PROVIDER"],
            "required_policy_actions": [],
        },
    )

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "runtime",
                "start",
                "--bundle",
                str(bundle_path),
                "--mode",
                "enforce",
                "--outputs-root",
                str(outputs_root),
            ]
        )
    assert excinfo.value.code == 2
    cap = capsys.readouterr()
    assert "missing required env flags set to 1" in cap.out + cap.err


def test_runtime_start_preflight_contract_fails_when_required_policy_action_missing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    outputs_root = tmp_path / "out"
    bundle_path = outputs_root / "tenant-a" / "repo-a" / ".akc" / "runtime" / "run-1.runtime_bundle.json"
    _write_runtime_bundle(
        bundle_path,
        deployment_provider_contract={
            "kind": "in_memory",
            "mutation_mode": "observe_only",
            "rollback_mode": "none",
            "rollback_determinism": "deterministic",
            "required_env_flags": [],
            "required_policy_actions": ["runtime.action.execute.nonexistent"],
        },
    )

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "runtime",
                "start",
                "--bundle",
                str(bundle_path),
                "--mode",
                "enforce",
                "--outputs-root",
                str(outputs_root),
            ]
        )
    assert excinfo.value.code == 2
    cap = capsys.readouterr()
    assert "runtime policy does not allow required actions" in cap.out + cap.err


def test_runtime_start_full_layer_replacement_requires_deterministic_rollback_map(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    outputs_root = tmp_path / "out"
    bundle_path = outputs_root / "tenant-a" / "repo-a" / ".akc" / "runtime" / "run-1.runtime_bundle.json"
    _write_runtime_bundle(
        bundle_path,
        deployment_provider={
            "kind": "kubernetes_apply",
            "namespace": "default",
            "resource_map": {"svc-1": "deploy-1"},
            "resource_kind": "deployment",
            "apply_manifest_path": "app.yaml",
            "apply_manifest_sha256": "a" * 64,
        },
        layer_replacement_mode="full",
    )
    _write_compile_manifest_with_packet(
        outputs_root=outputs_root,
        tenant_id="tenant-a",
        repo_id="repo-a",
        run_id="run-1",
        promotion_mode="live_apply",
        allow=True,
    )

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "runtime",
                "start",
                "--bundle",
                str(bundle_path),
                "--mode",
                "enforce",
                "--outputs-root",
                str(outputs_root),
            ]
        )
    assert excinfo.value.code == 2
    cap = capsys.readouterr()
    assert "deterministic rollback snapshots" in cap.out + cap.err


def test_runtime_replay_synthetic_manifest_includes_stable_intent_from_bundle(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    outputs_root = tmp_path / "out"
    bundle_path = outputs_root / "tenant-a" / "repo-a" / ".akc" / "runtime" / "run-1.runtime_bundle.json"
    intent_sha = "c" * 64
    _write_runtime_bundle(bundle_path, stable_intent_sha256=intent_sha)

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "runtime",
                "start",
                "--bundle",
                str(bundle_path),
                "--mode",
                "simulate",
                "--outputs-root",
                str(outputs_root),
            ]
        )
    assert excinfo.value.code == 0
    runtime_run_id = str(_load_runtime_record(outputs_root)["runtime_run_id"])
    capsys.readouterr()

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "runtime",
                "replay",
                "--runtime-run-id",
                runtime_run_id,
                "--mode",
                "reconcile_replay",
                "--outputs-root",
                str(outputs_root),
                "--tenant-id",
                "tenant-a",
                "--repo-id",
                "repo-a",
            ]
        )
    assert excinfo.value.code == 0
    capsys.readouterr()

    from akc.cli.runtime import _compile_run_manifest_path, _load_runtime_evidence, _run_manifest_for_replay

    record = _load_runtime_record(outputs_root)
    assert not _compile_run_manifest_path(record).exists()
    ev = _load_runtime_evidence(record)
    synthetic = _run_manifest_for_replay(
        record=record,
        replay_mode="reconcile_replay",
        runtime_evidence=ev,
        runtime_events=[],
    )
    assert synthetic.stable_intent_sha256 == intent_sha
    assert synthetic.control_plane is not None
    assert synthetic.control_plane.get("stable_intent_sha256") == intent_sha
    assert synthetic.control_plane.get("runtime_run_id") == runtime_run_id
    assert any(
        row.payload.get("stable_intent_sha256") == intent_sha
        for row in ev
        if row.evidence_type == "terminal_health" and row.payload.get("aggregate") is True
    )


def test_cli_runtime_scope_mismatch_error_includes_hint(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    outputs_root = tmp_path / "out"
    bundle_path = outputs_root / "tenant-a" / "repo-a" / ".akc" / "runtime" / "run-1.runtime_bundle.json"
    _write_runtime_bundle(bundle_path)

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "runtime",
                "start",
                "--bundle",
                str(bundle_path),
                "--mode",
                "simulate",
                "--outputs-root",
                str(outputs_root),
            ]
        )
    assert excinfo.value.code == 0
    capsys.readouterr()

    runtime_run_id = str(_load_runtime_record(outputs_root)["runtime_run_id"])

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "runtime",
                "status",
                "--runtime-run-id",
                runtime_run_id,
                "--outputs-root",
                str(outputs_root),
                "--tenant-id",
                "tenant-b",
                "--repo-id",
                "repo-a",
            ]
        )
    assert excinfo.value.code == 2
    cap = capsys.readouterr()
    combined = cap.out + cap.err
    assert "Runtime CLI error:" in combined
    assert "runtime scope mismatch" in combined
    assert "matching scope hints" in combined
