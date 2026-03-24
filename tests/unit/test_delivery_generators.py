from __future__ import annotations

from pathlib import Path

import yaml

from akc.artifacts.validate import validate_obj
from akc.compile.artifact_passes import run_delivery_plan_pass, run_deployment_config_pass
from akc.compile.delivery_projection import build_delivery_plan
from akc.intent import IntentSpecV1, OperatingBound, PolicyRef, SuccessCriterion
from akc.ir import IRDocument, IRNode
from akc.runtime.bundle_delivery import build_delivery_handoff_context

_FIXTURE = Path(__file__).resolve().parent.parent / "fixtures" / "delivery" / "three_tier_system_ir.json"


def _intent_spec(*, tenant: str = "tenant_golden", repo: str = "repo_golden") -> IntentSpecV1:
    return IntentSpecV1(
        intent_id="intent_golden",
        tenant_id=tenant,
        repo_id=repo,
        goal_statement="Golden three-tier delivery",
        operating_bounds=OperatingBound(allow_network=False, max_output_tokens=256),
        policies=(PolicyRef(id="policy.network", source="security", requirement="Constrain egress"),),
        success_criteria=(
            SuccessCriterion(
                id="success.tests",
                evaluation_mode="tests",
                description="tests",
            ),
        ),
    )


def test_build_delivery_plan_golden_ir_target_classes_and_dependencies() -> None:
    ir_document = IRDocument.from_json_file(_FIXTURE)
    intent = _intent_spec()
    plan = build_delivery_plan(
        run_id="golden_run",
        ir_document=ir_document,
        intent_obj=intent.to_json_obj(),
        orchestration_obj={},
        coordination_obj={},
    )
    assert validate_obj(obj=plan, kind="delivery_plan", version=1) == []
    by_id = {str(t["target_id"]): t for t in plan["targets"]}
    assert by_id["1_api"]["target_class"] == "backend_service"
    assert by_id["2_web"]["target_class"] == "web_app"
    assert by_id["3_worker"]["target_class"] == "worker"
    assert by_id["4_redis"]["target_class"] == "infrastructure_component"
    assert by_id["1_api"]["depends_on"] == ["4_redis"]
    assert by_id["2_web"]["depends_on"] == ["1_api"]
    assert by_id["3_worker"]["depends_on"] == ["1_api"]
    assert plan["promotion_readiness"]["status"] == "blocked"
    assert "production_manual_approval_gate" in plan["promotion_readiness"]["promotion_blockers"]
    paths = by_id["2_web"]["supported_delivery_paths"]
    assert paths["staging"] == ["direct_apply", "workflow_handoff"]


def test_deployment_config_from_plan_sets_k8s_labels_resources_and_probes() -> None:
    ir_document = IRDocument.from_json_file(_FIXTURE)
    intent = _intent_spec()
    orch = '{"run_id":"golden_run","tenant_id":"tenant_golden","repo_id":"repo_golden"}'
    dp = run_delivery_plan_pass(
        run_id="golden_run",
        ir_document=ir_document,
        intent_spec=intent,
        orchestration_spec_text=orch,
        coordination_spec_text=orch,
    )
    dep = run_deployment_config_pass(
        run_id="golden_run",
        ir_document=ir_document,
        intent_spec=intent,
        orchestration_spec_text=orch,
        coordination_spec_text=orch,
        delivery_plan_text=dp.artifact_json.text(),
    )
    deploy_docs = list(yaml.safe_load_all(dep.artifact_k8s_deployment.text()))
    deploy_by_name = {d["metadata"]["name"]: d for d in deploy_docs if d.get("kind") == "Deployment"}
    assert set(deploy_by_name) == {
        "api-backend-golden_run",
        "web-frontend-golden_run",
        "background-worker-golden_run",
        "redis-cache-golden_run",
    }
    api_dep = deploy_by_name["api-backend-golden_run"]
    pod_labels = api_dep["spec"]["template"]["metadata"]["labels"]
    assert pod_labels.get("app.kubernetes.io/name") == "api-backend"
    assert pod_labels.get("app.kubernetes.io/managed-by") == "akc"
    assert pod_labels.get("app.kubernetes.io/part-of") == "repo_golden"
    assert pod_labels.get("app.kubernetes.io/component") == "backend-service"
    container = api_dep["spec"]["template"]["spec"]["containers"][0]
    assert container["readinessProbe"]["httpGet"]["path"] == "/ready"
    assert container["resources"]["requests"]["cpu"] == "300m"
    assert container["resources"]["requests"]["memory"] == "384Mi"
    assert container["resources"]["limits"]["cpu"] == "750m"
    sec = api_dep["spec"]["template"]["spec"]["containers"][0]["securityContext"]
    assert sec["allowPrivilegeEscalation"] is False
    assert sec["readOnlyRootFilesystem"] is True
    web_dep = deploy_by_name["web-frontend-golden_run"]
    web_c = web_dep["spec"]["template"]["spec"]["containers"][0]
    assert web_c["ports"][0]["containerPort"] == 3000
    assert web_c["readinessProbe"]["httpGet"]["path"] == "/healthz"
    worker_dep = deploy_by_name["background-worker-golden_run"]
    wk_c = worker_dep["spec"]["template"]["spec"]["containers"][0]
    assert wk_c["ports"][0]["containerPort"] == 9090
    assert wk_c["readinessProbe"]["httpGet"]["path"] == "/worker-health"


def test_deployment_config_per_target_k8s_secrets_isolated() -> None:
    ir_document = IRDocument(
        tenant_id="t_sec",
        repo_id="r_sec",
        nodes=(
            IRNode(
                id="api_n",
                tenant_id="t_sec",
                kind="service",
                name="public api",
                properties={
                    "public": False,
                    "cloud_account": "c1",
                    "secrets": ["API_TOKEN"],
                    "secrets_provisioned_in_store": True,
                },
            ),
            IRNode(
                id="worker_n",
                tenant_id="t_sec",
                kind="service",
                name="queue worker",
                properties={
                    "public": False,
                    "cloud_account": "c1",
                    "secrets": ["QUEUE_PASSWORD"],
                    "secrets_provisioned_in_store": True,
                },
            ),
        ),
    )
    intent = _intent_spec(tenant="t_sec", repo="r_sec")
    orch = '{"run_id":"sec_run","tenant_id":"t_sec","repo_id":"r_sec"}'
    dp = run_delivery_plan_pass(
        run_id="sec_run",
        ir_document=ir_document,
        intent_spec=intent,
        orchestration_spec_text=orch,
        coordination_spec_text=orch,
    )
    dep = run_deployment_config_pass(
        run_id="sec_run",
        ir_document=ir_document,
        intent_spec=intent,
        orchestration_spec_text=orch,
        coordination_spec_text=orch,
        delivery_plan_text=dp.artifact_json.text(),
    )
    assert dep.metadata.get("k8s_secret_manifest_count") == 2
    sec_art = next(a for a in dep.additional_artifacts if a.path == ".akc/deployment/k8s/secrets.yml")
    sec_docs = [x for x in yaml.safe_load_all(sec_art.text()) if isinstance(x, dict)]
    assert {s["metadata"]["name"] for s in sec_docs if s.get("kind") == "Secret"} == {
        "akc-secrets-sec_run-public-api",
        "akc-secrets-sec_run-queue-worker",
    }
    api_secret = next(s for s in sec_docs if s["metadata"]["name"] == "akc-secrets-sec_run-public-api")
    assert api_secret["stringData"] == {"API_TOKEN": "<set-in-secret-store>"}
    deploy_docs = [x for x in yaml.safe_load_all(dep.artifact_k8s_deployment.text()) if isinstance(x, dict)]
    by_name = {d["metadata"]["name"]: d for d in deploy_docs if d.get("kind") == "Deployment"}
    api_container = by_name["public-api-sec_run"]["spec"]["template"]["spec"]["containers"][0]
    refs = {
        e["valueFrom"]["secretKeyRef"]["name"]
        for e in api_container.get("env", [])
        if isinstance(e, dict) and "valueFrom" in e
    }
    assert refs == {"akc-secrets-sec_run-public-api"}
    worker_container = by_name["queue-worker-sec_run"]["spec"]["template"]["spec"]["containers"][0]
    wrefs = {
        e["valueFrom"]["secretKeyRef"]["name"]
        for e in worker_container.get("env", [])
        if isinstance(e, dict) and "valueFrom" in e
    }
    assert wrefs == {"akc-secrets-sec_run-queue-worker"}
    base_k = next(a for a in dep.additional_artifacts if a.path.endswith("k8s/base/kustomization.yml"))
    assert "secrets.yml" in base_k.text()


def test_deployment_config_skips_mobile_client_in_compose_when_mixed_with_backend() -> None:
    ir_document = IRDocument(
        tenant_id="t_m",
        repo_id="r_m",
        nodes=(
            IRNode(
                id="api1",
                tenant_id="t_m",
                kind="service",
                name="api",
                properties={"cloud_account": "c1", "public": False},
            ),
            IRNode(
                id="ios1",
                tenant_id="t_m",
                kind="service",
                name="ios shopper app",
                properties={"cloud_account": "c1", "public": False},
            ),
        ),
    )
    intent = _intent_spec(tenant="t_m", repo="r_m")
    orch = '{"run_id":"mrun","tenant_id":"t_m","repo_id":"r_m"}'
    dp = run_delivery_plan_pass(
        run_id="mrun",
        ir_document=ir_document,
        intent_spec=intent,
        orchestration_spec_text=orch,
        coordination_spec_text=orch,
    )
    dep = run_deployment_config_pass(
        run_id="mrun",
        ir_document=ir_document,
        intent_spec=intent,
        orchestration_spec_text=orch,
        coordination_spec_text=orch,
        delivery_plan_text=dp.artifact_json.text(),
    )
    compose = yaml.safe_load(dep.artifact_docker_compose.text())
    assert "api" in compose["services"]
    assert "ios-shopper-app" not in compose["services"]


def test_deployment_config_compose_multi_service_depends_on_restart_health() -> None:
    ir_document = IRDocument.from_json_file(_FIXTURE)
    intent = _intent_spec()
    orch = '{"run_id":"golden_run","tenant_id":"tenant_golden","repo_id":"repo_golden"}'
    dp = run_delivery_plan_pass(
        run_id="golden_run",
        ir_document=ir_document,
        intent_spec=intent,
        orchestration_spec_text=orch,
        coordination_spec_text=orch,
    )
    dep = run_deployment_config_pass(
        run_id="golden_run",
        ir_document=ir_document,
        intent_spec=intent,
        orchestration_spec_text=orch,
        coordination_spec_text=orch,
        delivery_plan_text=dp.artifact_json.text(),
    )
    compose = yaml.safe_load(dep.artifact_docker_compose.text())
    services = compose["services"]
    assert "background-worker" in services
    assert services["background-worker"]["restart"] == "unless-stopped"
    assert services["web-frontend"]["ports"] == ["3000:3000"]
    assert services["background-worker"]["ports"] == ["9090:9090"]
    deps = services["background-worker"].get("depends_on", {})
    assert deps.get("api-backend", {}).get("condition") == "service_healthy"
    assert "healthcheck" in services["background-worker"]
    staging = next(a for a in dep.additional_artifacts if a.path.endswith("docker-compose.staging.yml"))
    overlay = yaml.safe_load(staging.text())
    for svc in ("api-backend", "web-frontend", "background-worker", "redis-cache"):
        assert svc in overlay["services"]
        assert overlay["services"][svc]["environment"]["AKC_ENVIRONMENT"] == "staging"


def test_deployment_config_github_workflow_environments_oidc_permissions() -> None:
    ir_document = IRDocument.from_json_file(_FIXTURE)
    intent = _intent_spec()
    orch = '{"run_id":"golden_run","tenant_id":"tenant_golden","repo_id":"repo_golden"}'
    dp = run_delivery_plan_pass(
        run_id="golden_run",
        ir_document=ir_document,
        intent_spec=intent,
        orchestration_spec_text=orch,
        coordination_spec_text=orch,
    )
    dep = run_deployment_config_pass(
        run_id="golden_run",
        ir_document=ir_document,
        intent_spec=intent,
        orchestration_spec_text=orch,
        coordination_spec_text=orch,
        delivery_plan_text=dp.artifact_json.text(),
    )
    wf = yaml.safe_load(dep.artifact_github_actions.text())
    assert wf["permissions"] == {"contents": "read"}
    pub = wf["jobs"]["publish"]
    assert "id-token" in pub["permissions"]
    assert wf["jobs"]["deploy_staging"]["environment"]["name"] == "staging"
    assert wf["jobs"]["approval"]["environment"]["name"] == "production"
    publish_steps = pub["steps"]
    assert any("OIDC" in str(s.get("name", "")) for s in publish_steps)


def test_deployment_config_gitops_templates_point_at_production_overlay() -> None:
    ir_document = IRDocument.from_json_file(_FIXTURE)
    intent = _intent_spec()
    orch = '{"run_id":"golden_run","tenant_id":"tenant_golden","repo_id":"repo_golden"}'
    dp = run_delivery_plan_pass(
        run_id="golden_run",
        ir_document=ir_document,
        intent_spec=intent,
        orchestration_spec_text=orch,
        coordination_spec_text=orch,
    )
    dep = run_deployment_config_pass(
        run_id="golden_run",
        ir_document=ir_document,
        intent_spec=intent,
        orchestration_spec_text=orch,
        coordination_spec_text=orch,
        delivery_plan_text=dp.artifact_json.text(),
    )
    flux = next(a for a in dep.additional_artifacts if "flux-kustomization" in a.path)
    argo = next(a for a in dep.additional_artifacts if "argo-application" in a.path)
    flux_obj = yaml.safe_load(flux.text())
    assert flux_obj["kind"] == "Kustomization"
    assert flux_obj["spec"]["path"] == ".akc/deployment/k8s/overlays/production"
    argo_obj = yaml.safe_load(argo.text())
    assert argo_obj["kind"] == "Application"
    assert argo_obj["spec"]["source"]["path"] == ".akc/deployment/k8s/overlays/production"


def test_build_delivery_handoff_context_includes_plan_ref_when_metadata_present() -> None:
    meta = {
        "delivery_plan_ref": {"path": ".akc/deployment/run_x.delivery_plan.json", "fingerprint": "a" * 64},
        "promotion_readiness": {"status": "blocked"},
        "deployment_intents": [
            {"node_id": "1", "target_class": "web_app"},
            {"node_id": "2", "target_class": "unknown"},
        ],
    }
    ctx = build_delivery_handoff_context(meta)
    assert ctx["delivery_plan_ref"]["path"].endswith(".delivery_plan.json")
    assert ctx["promotion_readiness_status"] == "blocked"
    assert ctx["deployment_intent_count"] == 2
    assert ctx["deployment_intents_enriched_rows"] == 1
