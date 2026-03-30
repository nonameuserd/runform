from __future__ import annotations

import json
from pathlib import Path

from akc.delivery import store as delivery_store
from akc.delivery.compile_handoff import (
    extract_web_distribution_hints,
    load_compile_handoff,
    platform_spec_metadata_from_handoff,
    run_manifest_path,
)
from akc.run.manifest import PassRecord, RunManifest
from akc.utils.fingerprint import stable_json_fingerprint


def test_load_compile_handoff_empty_run_id(tmp_path: Path) -> None:
    h = load_compile_handoff(project_dir=tmp_path, compile_run_id=None)
    assert h["compile_run_id"] is None
    assert h["manifest_present"] is False


def test_load_compile_handoff_reads_delivery_plan_and_web_hints(tmp_path: Path) -> None:
    rid = "run-handoff-1"
    (tmp_path / ".akc" / "run").mkdir(parents=True)
    manifest = RunManifest(
        run_id=rid,
        tenant_id="t1",
        repo_id="r1",
        ir_sha256="a" * 64,
        replay_mode="live",
        stable_intent_sha256="b" * 64,
        intent_semantic_fingerprint="c" * 16,
        intent_goal_text_fingerprint="d" * 16,
        passes=(
            PassRecord(
                name="delivery_plan",
                status="succeeded",
                metadata={
                    "delivery_plan_path": f".akc/deployment/{rid}.delivery_plan.json",
                },
            ),
            PassRecord(
                name="execution_workspace",
                status="succeeded",
                metadata={
                    "execution_workspace_manifest_path": f".akc/execution/{rid}.execution_workspace_manifest.json",
                },
            ),
        ),
    )
    mpath = run_manifest_path(project_dir=tmp_path, compile_run_id=rid)
    mpath.write_text(json.dumps(manifest.to_json_obj()), encoding="utf-8")

    plan = {
        "schema_version": 1,
        "kind": "delivery_plan",
        "run_id": rid,
        "tenant_id": "t1",
        "repo_id": "r1",
        "targets": [
            {
                "target_class": "web_app",
                "target_id": "n1",
                "name": "ui",
                "domain": "app.example.com",
            },
        ],
        "promotion_readiness": {"status": "blocked", "blocking_inputs": ["x"]},
    }
    ddir = tmp_path / ".akc" / "deployment"
    ddir.mkdir(parents=True, exist_ok=True)
    (ddir / f"{rid}.delivery_plan.json").write_text(json.dumps(plan), encoding="utf-8")
    edir = tmp_path / ".akc" / "execution"
    edir.mkdir(parents=True, exist_ok=True)
    execution_manifest = {
        "run_id": rid,
        "tenant_id": "t1",
        "repo_id": "r1",
        "artifact_role": "authoritative_generated_workspace",
        "generation_mode": "backend_ir_materialized_workspace",
        "practical_generation_proof": True,
        "workspace_root": f".akc/execution/{rid}/workspace",
        "package_manager": "npm",
        "toolchain": {"node": "node", "eas_cli": "eas", "package_manager": "npm"},
        "runtime_profile": "typescript_node",
        "expo": {"project_id": "expo-project-1"},
        "build_profiles": {"beta": "preview", "store": "production"},
        "frontend_integration_summary": {"targets": [{"targetId": "api1", "integrationMode": "diagnostics_only"}]},
        "practical_backend_generation": {
            "api_contract_refs": [
                {
                    "target_id": "api1",
                    "openapi_rel_path": f".akc/backend/{rid}.api1.openapi.json",
                    "fingerprint": "f" * 64,
                }
            ]
        },
        "build_entrypoints": {},
        "expected_outputs": {},
        "targets": [],
        "generated_files": [],
        "workspace_fingerprint": "e" * 64,
    }
    (edir / f"{rid}.execution_workspace_manifest.json").write_text(json.dumps(execution_manifest), encoding="utf-8")

    h = load_compile_handoff(project_dir=tmp_path, compile_run_id=rid)
    assert h["manifest_present"] is True
    assert h["delivery_plan_loaded"] is True
    assert h["derived_intent_ref"] == {
        "intent_id": rid,
        "stable_intent_sha256": "b" * 64,
        "semantic_fingerprint": "c" * 16,
        "goal_text_fingerprint": "d" * 16,
    }
    assert h["delivery_plan_ref"] == {
        "path": f".akc/deployment/{rid}.delivery_plan.json",
        "fingerprint": stable_json_fingerprint(plan),
    }
    hints = h["web_distribution_hints"]
    assert hints["suggested_base_urls"] == ["https://app.example.com"]
    assert h["execution_workspace_loaded"] is True
    assert h["execution_workspace_ref"] == {
        "path": f".akc/execution/{rid}.execution_workspace_manifest.json",
        "fingerprint": stable_json_fingerprint(execution_manifest),
    }

    meta = platform_spec_metadata_from_handoff(h)
    assert meta["web_invite_base_url"] == "https://app.example.com"
    assert meta["runtime_profile"] == "typescript_node"
    assert meta["package_manager"] == "npm"
    assert meta["artifact_role"] == "authoritative_generated_workspace"
    assert meta["generation_mode"] == "backend_ir_materialized_workspace"
    assert meta["practical_generation_proof"] is True
    assert meta["execution_workspace_ref"]["path"].endswith(".execution_workspace_manifest.json")
    assert meta["api_contract_refs"][0]["openapi_rel_path"].endswith(".api1.openapi.json")
    assert meta["frontend_integration_summary"]["targets"][0]["integrationMode"] == "diagnostics_only"


def test_extract_web_distribution_hints_skips_non_web() -> None:
    h = extract_web_distribution_hints(
        {
            "targets": [
                {"target_class": "backend_service", "domain": "api.example.com"},
            ],
        },
    )
    assert h["web_targets"] == []
    assert h["suggested_base_urls"] == []


def test_compile_handoff_persistence_keeps_execution_workspace_lineage(tmp_path: Path) -> None:
    summary = delivery_store.create_delivery_session(
        project_dir=tmp_path,
        request_text="ship",
        recipients=["a@example.com"],
        platforms=["web"],
        release_mode="beta",
        skip_distribution_preflight=True,
    )
    delivery_id = str(summary["delivery_id"])
    handoff = {
        "compile_run_id": "run-1",
        "manifest_present": True,
        "manifest_rel_path": ".akc/run/run-1.manifest.json",
        "delivery_plan_rel_path": ".akc/deployment/run-1.delivery_plan.json",
        "delivery_plan_loaded": True,
        "delivery_plan_ref": {"path": ".akc/deployment/run-1.delivery_plan.json", "fingerprint": "a" * 64},
        "promotion_readiness": {"status": "ready"},
        "runtime_bundle_rel_path": ".akc/runtime/run-1.runtime_bundle.json",
        "execution_workspace_rel_path": ".akc/execution/run-1.execution_workspace_manifest.json",
        "execution_workspace_loaded": True,
        "execution_workspace_ref": {
            "path": ".akc/execution/run-1.execution_workspace_manifest.json",
            "fingerprint": "b" * 64,
        },
        "execution_workspace_hints": {
            "workspace_root": ".akc/execution/run-1/workspace",
            "runtime_profile": "typescript_node",
            "requested_runtime_plugin": "go",
            "requested_runtime_source": "repo_local",
            "materialization_status": "blocked",
            "materializer_kind": "command",
            "package_manager": "npm",
        },
    }

    delivery_store.update_delivery_request_compile_handoff(
        project_dir=tmp_path,
        delivery_id=delivery_id,
        handoff=handoff,
    )
    delivery_store.update_session_compile_handoff(
        project_dir=tmp_path,
        delivery_id=delivery_id,
        handoff=handoff,
    )

    request = delivery_store.load_request(tmp_path, delivery_id)
    session = delivery_store.load_session(tmp_path, delivery_id)
    for doc in (request, session):
        ref = doc["compile_outputs_ref"]
        assert ref["execution_workspace_rel_path"] == ".akc/execution/run-1.execution_workspace_manifest.json"
        assert ref["execution_workspace_loaded"] is True
        assert ref["execution_workspace_ref"]["path"].endswith(".execution_workspace_manifest.json")
        assert ref["execution_workspace_hints"]["workspace_root"] == ".akc/execution/run-1/workspace"
        assert ref["execution_workspace_hints"]["requested_runtime_plugin"] == "go"


def test_load_compile_handoff_includes_infra_refs_and_latest_provision_summary(tmp_path: Path) -> None:
    rid = "run-handoff-infra"
    (tmp_path / ".akc" / "run").mkdir(parents=True)
    manifest = RunManifest(
        run_id=rid,
        tenant_id="t1",
        repo_id="r1",
        ir_sha256="a" * 64,
        replay_mode="live",
        passes=(
            PassRecord(
                name="infrastructure_synthesis",
                status="succeeded",
                metadata={
                    "infra_plan_path": f".akc/infra/{rid}.infra_plan.json",
                    "iac_manifest_path": f".akc/infra/{rid}.iac_manifest.json",
                },
            ),
        ),
    )
    mpath = run_manifest_path(project_dir=tmp_path, compile_run_id=rid)
    mpath.write_text(json.dumps(manifest.to_json_obj()), encoding="utf-8")
    infra_dir = tmp_path / ".akc" / "infra"
    infra_dir.mkdir(parents=True, exist_ok=True)
    infra_plan = {
        "run_id": rid,
        "tenant_id": "t1",
        "repo_id": "r1",
        "cloud_provider": "aws",
        "supported_iac_backends": ["terraform", "aws_cdk"],
        "preferred_iac_backend": "terraform",
        "provisioning_environments": ["staging", "production"],
        "workload_targets": [],
        "resources": [],
        "provisioning_readiness": {"status": "ready"},
    }
    iac_manifest = {
        "run_id": rid,
        "tenant_id": "t1",
        "repo_id": "r1",
        "cloud_provider": "aws",
        "supported_backends": ["terraform", "aws_cdk"],
        "preferred_backend": "terraform",
        "infra_plan_ref": {"path": f".akc/infra/{rid}.infra_plan.json", "fingerprint": "f" * 64},
        "provisioning_readiness": {"status": "ready"},
        "environments": ["staging", "production"],
        "workspaces": {},
    }
    (infra_dir / f"{rid}.infra_plan.json").write_text(json.dumps(infra_plan), encoding="utf-8")
    (infra_dir / f"{rid}.iac_manifest.json").write_text(json.dumps(iac_manifest), encoding="utf-8")
    provision_dir = tmp_path / ".akc" / "provision" / "prov-1"
    provision_dir.mkdir(parents=True, exist_ok=True)
    session = {
        "provision_id": "prov-1",
        "compile_run_id": rid,
        "tenant_id": "t1",
        "repo_id": "r1",
        "backend": "terraform",
        "environment": "staging",
        "status": "planned",
        "desired_fingerprint": "a" * 64,
        "created_at_ms": 1,
        "updated_at_ms": 2,
    }
    (provision_dir / "session.json").write_text(json.dumps(session), encoding="utf-8")

    handoff = load_compile_handoff(project_dir=tmp_path, compile_run_id=rid)
    assert handoff["infra_plan_loaded"] is True
    assert handoff["iac_manifest_loaded"] is True
    assert handoff["infra_plan_ref"]["path"].endswith(".infra_plan.json")
    assert handoff["iac_manifest_ref"]["path"].endswith(".iac_manifest.json")
    assert handoff["latest_provision_summary_ref"]["path"].endswith("session.json")
