from __future__ import annotations

import json
from pathlib import Path

import yaml

from akc.artifacts.validate import validate_obj
from akc.compile.artifact_passes import run_delivery_plan_pass, run_deployment_config_pass, run_runtime_bundle_pass
from akc.intent import IntentSpecV1, OperatingBound, PolicyRef, SuccessCriterion
from akc.ir import IRDocument, IRNode
from akc.runtime.models import RuntimeBundle, RuntimeBundleRef, RuntimeContext
from akc.runtime.providers.compose import DockerComposeObserveProvider
from akc.runtime.providers.factory import create_deployment_provider
from akc.runtime.providers.kubernetes import KubernetesObserveProvider

_FIXTURE = Path(__file__).resolve().parent.parent / "fixtures" / "delivery" / "three_tier_system_ir.json"


def _intent(tenant: str, repo: str) -> IntentSpecV1:
    return IntentSpecV1(
        intent_id="intent_pipe",
        tenant_id=tenant,
        repo_id=repo,
        goal_statement="Ship three-tier system",
        operating_bounds=OperatingBound(allow_network=False, max_output_tokens=256),
        policies=(PolicyRef(id="policy.net", source="security", requirement="egress"),),
        success_criteria=(
            SuccessCriterion(id="sc1", evaluation_mode="tests", description="tests"),
        ),
    )


def test_golden_ir_compile_pipeline_delivery_plan_bundle_and_deployment_artifacts(tmp_path: Path) -> None:
    ir_document = IRDocument.from_json_file(_FIXTURE)
    intent = _intent("tenant_golden", "repo_golden")
    run_id = "golden_e2e"
    orch = json.dumps({"run_id": run_id, "tenant_id": "tenant_golden", "repo_id": "repo_golden"})
    coord = orch

    dp = run_delivery_plan_pass(
        run_id=run_id,
        ir_document=ir_document,
        intent_spec=intent,
        orchestration_spec_text=orch,
        coordination_spec_text=coord,
    )
    plan_obj = json.loads(dp.artifact_json.text())
    assert validate_obj(obj=plan_obj, kind="delivery_plan", version=1) == []
    assert plan_obj["promotion_readiness"]["status"] == "blocked"
    assert "production_manual_approval_gate" in plan_obj["promotion_readiness"]["promotion_blockers"]
    assert len(plan_obj["targets"]) == 4

    dc = run_deployment_config_pass(
        run_id=run_id,
        ir_document=ir_document,
        intent_spec=intent,
        orchestration_spec_text=orch,
        coordination_spec_text=coord,
        delivery_plan_text=dp.artifact_json.text(),
    )
    assert dc.metadata.get("delivery_plan_ref") is not None
    paths = {dc.artifact_docker_compose.path, dc.artifact_k8s_deployment.path, dc.artifact_github_actions.path}
    assert ".akc/deployment/docker-compose.yml" in paths
    extras = {a.path for a in dc.additional_artifacts}
    assert ".akc/deployment/gitops/flux-kustomization.yml" in extras

    bundle = run_runtime_bundle_pass(
        run_id=run_id,
        ir_document=ir_document,
        intent_spec=intent,
        orchestration_spec_text=orch,
        coordination_spec_text=coord,
        delivery_plan_text=dp.artifact_json.text(),
    )
    bundle_obj = json.loads(bundle.artifact_json.text())
    assert bundle_obj.get("delivery_plan_ref", {}).get("path", "").endswith(".delivery_plan.json")
    assert bundle_obj["promotion_readiness"]["status"] == "blocked"

    for art in [dp.artifact_json, dc.artifact_docker_compose, bundle.artifact_json]:
        target = tmp_path / art.path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(art.text(), encoding="utf-8")


def test_public_surface_without_cloud_blocks_promotion_but_still_emits_plan() -> None:
    ir_document = IRDocument(
        tenant_id="t1",
        repo_id="r1",
        nodes=(
            IRNode(
                id="svc_web",
                tenant_id="t1",
                kind="service",
                name="marketing site",
                properties={"public": True, "domain": "example.com"},
            ),
        ),
    )
    intent = _intent("t1", "r1")
    dp = run_delivery_plan_pass(
        run_id="run_blk",
        ir_document=ir_document,
        intent_spec=intent,
        orchestration_spec_text='{"run_id":"run_blk","tenant_id":"t1","repo_id":"r1"}',
        coordination_spec_text='{"run_id":"run_blk","tenant_id":"t1","repo_id":"r1"}',
    )
    obj = json.loads(dp.artifact_json.text())
    assert obj["promotion_readiness"]["status"] == "blocked"
    assert obj["promotion_readiness"]["is_promotion_ready"] is False
    assert "cloud_credentials" in obj["promotion_readiness"].get("blocking_inputs", [])
    assert "production_manual_approval_gate" in obj["promotion_readiness"].get("promotion_blockers", [])
    assert validate_obj(obj=obj, kind="delivery_plan", version=1) == []


def test_runtime_bundle_legacy_lane_without_delivery_plan_ref() -> None:
    ir_document = IRDocument(
        tenant_id="tenant_a",
        repo_id="repo_a",
        nodes=(
            IRNode(
                id="wf1",
                tenant_id="tenant_a",
                kind="workflow",
                name="deploy",
                properties={"order_idx": 0},
            ),
        ),
    )
    intent = IntentSpecV1(
        intent_id="i1",
        tenant_id="tenant_a",
        repo_id="repo_a",
        goal_statement="g",
        operating_bounds=OperatingBound(allow_network=False, max_output_tokens=128),
        policies=(),
        success_criteria=(),
    )
    orch = '{"run_id":"lr","tenant_id":"tenant_a","repo_id":"repo_a","steps":[{"inputs":{"ir_node_id":"wf1"}}]}'
    coord = '{"run_id":"lr","tenant_id":"tenant_a","repo_id":"repo_a","orchestration_bindings":[]}'
    out = run_runtime_bundle_pass(
        run_id="lr",
        ir_document=ir_document,
        intent_spec=intent,
        orchestration_spec_text=orch,
        coordination_spec_text=coord,
        delivery_plan_text=None,
    )
    obj = json.loads(out.artifact_json.text())
    assert obj.get("delivery_plan_ref") is None
    assert obj.get("promotion_readiness", {}).get("status") == "unknown"


def test_staging_compose_provider_ingests_bundle_with_delivery_handoff(tmp_path: Path, monkeypatch: object) -> None:
    monkeypatch.setenv("AKC_ENABLE_EXTERNAL_DEPLOYMENT_PROVIDER", "1")
    repo = tmp_path / "repo"
    bundle_path = repo / ".akc" / "runtime" / "b.json"
    bundle_path.parent.mkdir(parents=True)
    bundle_path.write_text("{}", encoding="utf-8")
    (repo / ".akc" / "deployment").mkdir(parents=True, exist_ok=True)
    (repo / ".akc" / "deployment" / "docker-compose.yml").write_text(
        "services:\n  api-backend:\n    image: x\n", encoding="utf-8"
    )
    metadata = {
        "delivery_plan_ref": {"path": ".akc/deployment/run_x.delivery_plan.json", "fingerprint": "c" * 64},
        "promotion_readiness": {"status": "ready"},
        "deployment_provider": {
            "kind": "docker_compose_observe",
            "project": "akc-staging",
            "compose_files": [
                ".akc/deployment/docker-compose.yml",
                ".akc/deployment/compose/docker-compose.staging.yml",
            ],
            "service_map": {"1_api": "api-backend"},
        },
        "deployment_observe_hash_contract": {"version": 1},
    }
    ctx = RuntimeContext(
        tenant_id="tenant_golden",
        repo_id="repo_golden",
        run_id="compile-1",
        runtime_run_id="rt-1",
        policy_mode="enforce",
        adapter_id="native",
    )
    bundle = RuntimeBundle(
        context=ctx,
        ref=RuntimeBundleRef(
            bundle_path=str(bundle_path),
            manifest_hash="b" * 64,
            created_at=1,
            source_compile_run_id="compile-1",
        ),
        nodes=(),
        contract_ids=(),
        metadata=metadata,
    )
    client = create_deployment_provider(bundle)
    assert isinstance(client, DockerComposeObserveProvider)
    assert client.delivery_handoff_context.get("delivery_plan_ref", {}).get("path", "").endswith(
        ".delivery_plan.json"
    )
    assert client.compose_files == (
        ".akc/deployment/docker-compose.yml",
        ".akc/deployment/compose/docker-compose.staging.yml",
    )


def test_staging_kubernetes_observe_provider_wires_gitops_path_context(tmp_path: Path, monkeypatch: object) -> None:
    monkeypatch.setenv("AKC_ENABLE_EXTERNAL_DEPLOYMENT_PROVIDER", "1")
    p = tmp_path / "r" / ".akc" / "runtime" / "bk.json"
    p.parent.mkdir(parents=True)
    p.write_text("{}", encoding="utf-8")
    md = {
        "delivery_plan_ref": {"path": ".akc/deployment/x.delivery_plan.json", "fingerprint": "d" * 64},
        "deployment_provider": {
            "kind": "kubernetes_observe",
            "namespace": "staging",
            "resource_map": {"1_api": "pod-1"},
            "resource_kind": "pod",
        },
    }
    prov = KubernetesObserveProvider.from_bundle_metadata(md, bundle_path=p)
    assert prov.delivery_handoff_context["delivery_plan_ref"]["fingerprint"] == "d" * 64


def test_production_handoff_workflow_has_approval_gated_environment() -> None:
    ir_document = IRDocument.from_json_file(_FIXTURE)
    intent = _intent("tenant_golden", "repo_golden")
    run_id = "wf_gate"
    orch = json.dumps({"run_id": run_id, "tenant_id": "tenant_golden", "repo_id": "repo_golden"})
    dp = run_delivery_plan_pass(
        run_id=run_id,
        ir_document=ir_document,
        intent_spec=intent,
        orchestration_spec_text=orch,
        coordination_spec_text=orch,
    )
    dc = run_deployment_config_pass(
        run_id=run_id,
        ir_document=ir_document,
        intent_spec=intent,
        orchestration_spec_text=orch,
        coordination_spec_text=orch,
        delivery_plan_text=dp.artifact_json.text(),
    )
    wf = yaml.safe_load(dc.artifact_github_actions.text())
    assert wf["jobs"]["deploy_prod"]["needs"] == ["approval"]
    assert wf["jobs"]["deploy_prod"]["environment"]["name"] == "production"
    approval = wf["jobs"]["approval"]
    assert approval["environment"]["name"] == "production"
    rollback_if = str(wf["jobs"]["rollback"].get("if", ""))
    assert "needs.deploy_prod.result" in rollback_if
    assert "failure" in rollback_if.lower()
