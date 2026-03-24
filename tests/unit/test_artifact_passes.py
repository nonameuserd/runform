from __future__ import annotations

import json
from dataclasses import dataclass

from akc.artifacts.schemas import RUNTIME_BUNDLE_SCHEMA_VERSION
from akc.artifacts.validate import validate_obj
from akc.compile.artifact_passes import (
    _validate_k8s_restricted_security_context,
    _validate_workflow_policy,
    build_patch_artifact_prompt_envelope,
    parse_patch_artifact_metadata,
    parse_patch_artifact_strict,
    parse_patch_artifact_vcr_cache,
    run_agent_coordination_pass,
    run_delivery_plan_pass,
    run_deployment_config_pass,
    run_orchestration_spec_pass,
    run_runtime_bundle_pass,
    run_system_design_pass,
)
from akc.intent import (
    IntentSpecV1,
    OperatingBound,
    PolicyRef,
    SuccessCriterion,
    stable_intent_sha256,
)
from akc.ir import EffectAnnotation, IRDocument, IRNode
from akc.pass_registry import ARTIFACT_PASS_ORDER, CONTROLLER_LOOP_PASS_ORDER, assert_expected_artifact_pass_order
from akc.run.manifest import REPLAYABLE_PASSES
from akc.run.vcr import llm_vcr_prompt_key


@dataclass(frozen=True)
class _FakeOperatingBounds:
    allow_network: bool = False
    max_output_tokens: int | None = None


@dataclass(frozen=True)
class _FakeIntent:
    operating_bounds: _FakeOperatingBounds | None
    constraints: tuple[object, ...]
    success_criteria: tuple[object, ...]


def _intent_spec(*, allow_network: bool) -> IntentSpecV1:
    return IntentSpecV1(
        intent_id="intent_abc",
        tenant_id="tenant_a",
        repo_id="repo_a",
        goal_statement="Deploy runtime artifacts with intent provenance",
        operating_bounds=OperatingBound(allow_network=allow_network, max_output_tokens=256),
        policies=(PolicyRef(id="policy.network", source="security", requirement="Constrain egress"),),
        success_criteria=(
            SuccessCriterion(
                id="success.tests",
                evaluation_mode="tests",
                description="Artifact pass tests must remain green",
            ),
        ),
    )


def test_parse_patch_artifact_strict_accepts_valid_unified_diff() -> None:
    patch_text = "\n".join(
        [
            "--- a/src/example.py",
            "+++ b/src/example.py",
            "@@",
            "+VALUE = 1",
            "",
            "--- a/tests/test_example.py",
            "+++ b/tests/test_example.py",
            "@@",
            "+def test_example():",
            "+    assert VALUE == 1",
            "",
        ]
    )
    parsed = parse_patch_artifact_strict(text=patch_text)
    assert parsed is not None
    assert parsed.patch_text == patch_text
    assert set(parsed.touched_paths) == {"src/example.py", "tests/test_example.py"}


def test_parse_patch_artifact_strict_rejects_markdown_fenced_diff() -> None:
    fenced = "```diff\n--- a/src/x.py\n+++ b/src/x.py\n@@\n+X = 1\n```"
    assert parse_patch_artifact_strict(text=fenced) is None


def test_parse_patch_artifact_strict_rejects_header_count_mismatch() -> None:
    bad = "\n".join(
        [
            "--- a/src/example.py",
            "@@",
            "+VALUE = 1",
            "",
        ]
    )
    assert parse_patch_artifact_strict(text=bad) is None


def test_parse_patch_artifact_strict_rejects_dev_null_only_diff() -> None:
    bad = "\n".join(
        [
            "--- /dev/null",
            "+++ /dev/null",
            "@@",
            "+noop",
            "",
        ]
    )
    assert parse_patch_artifact_strict(text=bad) is None


def test_parse_patch_artifact_vcr_cache_fails_closed_for_missing_or_invalid_values() -> None:
    assert parse_patch_artifact_vcr_cache(llm_vcr=None, prompt_key="k") is None
    assert parse_patch_artifact_vcr_cache(llm_vcr={}, prompt_key="k") is None
    assert parse_patch_artifact_vcr_cache(llm_vcr={"k": "```diff\n--- a/x\n+++ b/x\n```"}, prompt_key="k") is None


def test_parse_patch_artifact_metadata_parses_valid_llm_text_only() -> None:
    good = "\n".join(["--- a/src/x.py", "+++ b/src/x.py", "@@", "+X = 1", ""])
    parsed = parse_patch_artifact_metadata(metadata={"llm_text": good})
    assert parsed is not None
    assert parsed.patch_text == good
    assert parsed.touched_paths == ("src/x.py",)
    assert parse_patch_artifact_metadata(metadata={"llm_text": 1}) is None
    assert parse_patch_artifact_metadata(metadata={"other": good}) is None


def test_build_prompt_envelope_uses_llm_vcr_prompt_key_contract() -> None:
    env = build_patch_artifact_prompt_envelope(
        user_prompt="GEN_PROMPT",
        tier_name="small",
        tier_model="fake-small",
        plan_id="p1",
        step_id="s1",
        replay_mode="live",
        temperature=0.0,
        max_output_tokens=123,
    )
    expected = llm_vcr_prompt_key(
        messages=env.llm_request.messages,
        temperature=env.llm_request.temperature,
        max_output_tokens=env.llm_request.max_output_tokens,
        metadata=env.llm_request.metadata,
    )
    assert env.prompt_key == expected


def test_run_system_design_pass_emits_design_artifacts() -> None:
    ir_doc = IRDocument(
        tenant_id="t1",
        repo_id="r1",
        nodes=(
            IRNode(
                id="n1",
                tenant_id="t1",
                kind="workflow",
                name="step one",
                properties={"order_idx": 0, "status": "pending"},
            ),
        ),
    )
    intent = _FakeIntent(
        operating_bounds=_FakeOperatingBounds(allow_network=False, max_output_tokens=512),
        constraints=(),
        success_criteria=(),
    )
    res = run_system_design_pass(
        run_id="run_1",
        ir_document=ir_doc,
        intent_spec=intent,
        knowledge_snapshot={"canonical_constraints": [{"assertion_id": "a1"}]},
    )
    assert res.artifact_json.path == ".akc/design/run_1.system_design.json"
    assert "system_id" in res.artifact_json.text()
    assert res.artifact_md is not None
    assert res.artifact_md.path == ".akc/design/run_1.system_design.md"
    assert res.output_sha256 == res.artifact_json.sha256_hex()


def test_run_orchestration_spec_pass_emits_json_and_stubs() -> None:
    ir_doc = IRDocument(
        tenant_id="t1",
        repo_id="r1",
        nodes=(
            IRNode(
                id="n1",
                tenant_id="t1",
                kind="workflow",
                name="step one",
                properties={"order_idx": 0, "status": "pending"},
            ),
        ),
    )
    intent = _FakeIntent(
        operating_bounds=_FakeOperatingBounds(allow_network=False, max_output_tokens=512),
        constraints=(),
        success_criteria=(),
    )
    res = run_orchestration_spec_pass(run_id="run_1", ir_document=ir_doc, intent_spec=intent)
    assert res.artifact_json.path == ".akc/orchestration/run_1.orchestration.json"
    assert '"tenant_id": "t1"' in res.artifact_json.text()
    assert '"repo_id": "r1"' in res.artifact_json.text()
    assert '"state_machine"' in res.artifact_json.text()
    assert '"io_contract"' in res.artifact_json.text()
    assert '"trigger_sources"' in res.artifact_json.text()
    assert res.artifact_python_stub.path == ".akc/orchestration/run_1.orchestrator.py"
    assert "def run_orchestrator(" in res.artifact_python_stub.text()
    assert "tenant/repo scope mismatch" in res.artifact_python_stub.text()
    assert res.artifact_typescript_stub.path == ".akc/orchestration/run_1.orchestrator.ts"
    assert "export function runOrchestrator(" in res.artifact_typescript_stub.text()
    assert "tenant/repo scope mismatch" in res.artifact_typescript_stub.text()
    assert res.output_sha256 == res.artifact_json.sha256_hex()


def test_run_agent_coordination_pass_emits_graph_and_protocol_stubs() -> None:
    ir_doc = IRDocument(
        tenant_id="t1",
        repo_id="r1",
        nodes=(
            IRNode(
                id="n1",
                tenant_id="t1",
                kind="workflow",
                name="step one",
                properties={"order_idx": 0, "status": "pending"},
            ),
        ),
    )
    intent = _FakeIntent(
        operating_bounds=_FakeOperatingBounds(allow_network=False, max_output_tokens=512),
        constraints=(),
        success_criteria=(),
    )
    res = run_agent_coordination_pass(run_id="run_1", ir_document=ir_doc, intent_spec=intent)
    assert res.artifact_json.path == ".akc/agents/run_1.coordination.json"
    text = res.artifact_json.text()
    assert '"tenant_id": "t1"' in text
    assert '"repo_id": "r1"' in text
    assert '"coordination_graph"' in text
    assert '"orchestration_bindings"' in text
    assert '"agent_spec"' in text
    assert res.artifact_python_stub.path == ".akc/agents/run_1.coordination_protocol.py"
    py_text = res.artifact_python_stub.text()
    assert "def run_coordination_protocol(" in py_text
    assert "from akc.coordination.protocol import" in py_text
    assert "load_coordination_spec_file" in py_text
    assert res.artifact_typescript_stub.path == ".akc/agents/run_1.coordination_protocol.ts"
    ts_text = res.artifact_typescript_stub.text()
    assert "export function runCoordinationProtocol(" in ts_text
    assert "export function loadCoordinationSpec" in ts_text
    assert "tenant/repo scope mismatch" in ts_text
    assert res.output_sha256 == res.artifact_json.sha256_hex()


def test_run_deployment_config_pass_emits_hardened_configs() -> None:
    ir_doc = IRDocument(
        tenant_id="tenant_a",
        repo_id="repo_a",
        nodes=(
            IRNode(
                id="n1",
                tenant_id="tenant_a",
                kind="workflow",
                name="deploy",
                properties={"order_idx": 0, "status": "ready"},
            ),
        ),
    )
    intent = _intent_spec(allow_network=False)
    res = run_deployment_config_pass(
        run_id="run_1",
        ir_document=ir_doc,
        intent_spec=intent,
        orchestration_spec_text='{"run_id":"run_1","tenant_id":"tenant_a","repo_id":"repo_a"}',
        coordination_spec_text='{"run_id":"run_1","tenant_id":"tenant_a","repo_id":"repo_a"}',
    )
    assert res.artifact_docker_compose.path == ".akc/deployment/docker-compose.yml"
    assert "read_only: true" in res.artifact_docker_compose.text()
    assert "no-new-privileges:true" in res.artifact_docker_compose.text()
    assert "AKC_ROLLOUT_STRATEGY: rolling" in res.artifact_docker_compose.text()
    assert "healthcheck:" in res.artifact_docker_compose.text()
    assert "env_file:" in res.artifact_docker_compose.text()
    assert res.artifact_k8s_deployment.path == ".akc/deployment/k8s/deployment.yml"
    deployment_text = res.artifact_k8s_deployment.text()
    assert "runAsNonRoot: true" in deployment_text
    assert "seccompProfile:" in deployment_text
    assert "RuntimeDefault" in deployment_text
    assert "allowPrivilegeEscalation: false" in deployment_text
    assert "readOnlyRootFilesystem: true" in deployment_text
    assert "readinessProbe:" in deployment_text
    assert "livenessProbe:" in deployment_text
    assert "startupProbe:" in deployment_text
    assert "app.kubernetes.io/managed-by: akc" in deployment_text
    assert res.artifact_k8s_service.path == ".akc/deployment/k8s/service.yml"
    assert res.artifact_k8s_configmap.path == ".akc/deployment/k8s/configmap.yml"
    configmap_text = res.artifact_k8s_configmap.text()
    assert "ORCHESTRATION_SPEC_JSON" in configmap_text
    assert "COORDINATION_SPEC_JSON" in configmap_text
    assert res.artifact_github_actions.path == ".github/workflows/akc_deploy_run_1.yml"
    workflow_text = res.artifact_github_actions.text()
    assert "pull_request_target" not in workflow_text
    assert "permissions:" in workflow_text
    assert "deploy_staging:" in workflow_text
    assert "health_check:" in workflow_text
    assert "approval:" in workflow_text
    assert "deploy_prod:" in workflow_text
    assert "rollback:" in workflow_text
    assert "contents: read" in workflow_text
    assert "Require production approval" in workflow_text
    assert "OIDC" in workflow_text
    additional_paths = {artifact.path for artifact in res.additional_artifacts}
    assert ".akc/deployment/compose/app.env" in additional_paths
    assert ".akc/deployment/compose/app-config.json" in additional_paths
    assert ".akc/deployment/compose/docker-compose.staging.yml" in additional_paths
    assert ".akc/deployment/compose/docker-compose.production.yml" in additional_paths
    assert ".akc/deployment/k8s/base/kustomization.yml" in additional_paths
    assert ".akc/deployment/k8s/secrets.yml" not in additional_paths
    kustom_txt = next(
        a.text() for a in res.additional_artifacts if a.path.endswith("k8s/base/kustomization.yml")
    )
    assert "secrets.yml" not in kustom_txt
    assert res.metadata.get("k8s_secret_manifest_count") == 0
    assert ".akc/deployment/k8s/overlays/staging/kustomization.yml" in additional_paths
    assert ".akc/deployment/k8s/overlays/production/kustomization.yml" in additional_paths
    assert ".akc/deployment/gitops/flux-kustomization.yml" in additional_paths
    assert ".akc/deployment/gitops/argo-application.yml" in additional_paths
    staging_overlay = next(
        artifact
        for artifact in res.additional_artifacts
        if artifact.path.endswith("compose/docker-compose.staging.yml")
    )
    assert "AKC_ENVIRONMENT: staging" in staging_overlay.text()
    flux_template = next(
        artifact for artifact in res.additional_artifacts if artifact.path.endswith("gitops/flux-kustomization.yml")
    )
    assert "kind: Kustomization" in flux_template.text()
    argo_template = next(
        artifact for artifact in res.additional_artifacts if artifact.path.endswith("gitops/argo-application.yml")
    )
    assert "kind: Application" in argo_template.text()
    docker_metadata = res.artifact_docker_compose.metadata
    assert docker_metadata is not None
    intent_projection = docker_metadata.get("intent_projection")
    assert isinstance(intent_projection, dict)
    assert intent_projection["intent_id"] == "intent_abc"
    assert intent_projection["policy_ids"] == ["policy.network"]
    expected_stable = stable_intent_sha256(intent=intent.normalized())
    intent_ref = docker_metadata.get("intent_ref")
    assert isinstance(intent_ref, dict)
    assert intent_ref["intent_id"] == "intent_abc"
    assert intent_ref["stable_intent_sha256"] == expected_stable
    assert intent_ref["stable_intent_sha256"] == intent_projection["stable_intent_sha256"]
    assert isinstance(intent_ref.get("semantic_fingerprint"), str)
    assert isinstance(intent_ref.get("goal_text_fingerprint"), str)
    assert res.metadata["intent_id"] == "intent_abc"
    assert res.metadata["stable_intent_sha256"] == expected_stable
    assert res.metadata["intent_ref"]["stable_intent_sha256"] == expected_stable
    compose_text = res.artifact_docker_compose.text()
    assert "com.akc.run.intent-id:" in compose_text
    assert "com.akc.run.stable-intent-sha256:" in compose_text
    assert f'akc.run/intent-id: "{intent.intent_id}"' in deployment_text or (
        f"akc.run/intent-id: {intent.intent_id}" in deployment_text
    )
    assert "akc.run/stable-intent-sha256:" in deployment_text
    for art in (
        res.artifact_k8s_deployment,
        res.artifact_k8s_service,
        res.artifact_k8s_configmap,
        res.artifact_github_actions,
    ):
        md = art.metadata
        assert isinstance(md, dict)
        assert md.get("intent_ref") == intent_ref
    assert res.output_sha256 == res.artifact_docker_compose.sha256_hex()


def test_run_deployment_config_pass_canary_rollout_marks_direct_apply_support() -> None:
    ir_doc = IRDocument(
        tenant_id="tenant_a",
        repo_id="repo_a",
        nodes=(
            IRNode(
                id="svc_canary",
                tenant_id="tenant_a",
                kind="service",
                name="api",
                properties={"cloud_account": "acct-1"},
            ),
        ),
    )
    intent = _intent_spec(allow_network=False)
    delivery_plan_obj = {
        "run_id": "run_canary",
        "tenant_id": "tenant_a",
        "repo_id": "repo_a",
        "targets": [
            {
                "target_id": "svc_canary",
                "rollout_recovery_policy": {"strategy": "canary"},
                "supported_delivery_paths": {
                    "local": ["direct_apply"],
                    "staging": ["direct_apply", "workflow_handoff"],
                    "production": ["gitops_handoff", "workflow_handoff"],
                },
            }
        ],
        "environments": ["local", "staging", "production"],
        "delivery_paths": {},
        "required_human_inputs": [],
        "promotion_readiness": {"status": "ready"},
    }
    res = run_deployment_config_pass(
        run_id="run_canary",
        ir_document=ir_doc,
        intent_spec=intent,
        orchestration_spec_text='{"run_id":"run_canary","tenant_id":"tenant_a","repo_id":"repo_a"}',
        coordination_spec_text='{"run_id":"run_canary","tenant_id":"tenant_a","repo_id":"repo_a"}',
        delivery_plan_text=json.dumps(delivery_plan_obj),
    )
    assert res.metadata["rollout_strategy"] == "canary"
    assert res.metadata["canary_direct_apply_supported"] is True
    compose_text = res.artifact_docker_compose.text()
    assert "AKC_ROLLOUT_STRATEGY: canary" in compose_text
    assert "AKC_CANARY_DIRECT_APPLY_SUPPORTED:" in compose_text


def test_run_delivery_plan_pass_projects_ops_contracts_and_missing_inputs() -> None:
    ir_doc = IRDocument(
        tenant_id="tenant_a",
        repo_id="repo_a",
        nodes=(
            IRNode(
                id="svc_web",
                tenant_id="tenant_a",
                kind="service",
                name="web frontend",
                properties={"public": True},
            ),
            IRNode(
                id="svc_worker",
                tenant_id="tenant_a",
                kind="service",
                name="background worker",
                properties={},
            ),
        ),
    )
    intent = _intent_spec(allow_network=False)
    res = run_delivery_plan_pass(
        run_id="run_delivery",
        ir_document=ir_doc,
        intent_spec=intent,
        orchestration_spec_text='{"run_id":"run_delivery","steps":[]}',
        coordination_spec_text='{"run_id":"run_delivery","orchestration_bindings":[]}',
    )
    obj = json.loads(res.artifact_json.text())
    assert validate_obj(obj=obj, kind="delivery_plan", version=1) == []
    assert res.artifact_json.path == ".akc/deployment/run_delivery.delivery_plan.json"
    assert obj["promotion_readiness"]["status"] == "blocked"
    assert obj["promotion_readiness"]["is_promotion_ready"] is False
    assert obj["promotion_readiness"]["default_promotion_environment"] == "production"
    assert obj["environments"] == ["local", "staging", "production"]
    env_model = {row["environment"]: row for row in obj["environment_model"]}
    assert env_model["local"]["preferred_runtime"] == "docker_compose"
    assert env_model["local"]["preferred_delivery_path"] == "direct_apply"
    assert env_model["staging"]["reconcile_mode"] == "direct"
    assert env_model["staging"]["supported_delivery_paths"] == ["direct_apply", "workflow_handoff"]
    assert env_model["production"]["preferred_delivery_path"] == "workflow_handoff"
    assert env_model["production"]["gitops_compatible_handoff"] is True
    assert env_model["production"]["approval_gates_required"] is True
    assert env_model["production"]["readiness_checks_required"] is True
    target = next(t for t in obj["targets"] if t["target_id"] == "svc_web")
    assert target["build_contract"]["artifact_type"] == "container_image"
    assert target["runtime_contract"]["runtime"] == "container"
    assert target["exposure_model"]["public"] is True
    assert target["operational_config"]["probes"]["readiness"]["path"] == "/healthz"
    assert target["operational_config"]["security_context"]["pod"]["runAsNonRoot"] is True
    assert "secrets_placeholders" in target["operational_config"]
    assert "domain_name" in [item["id"] for item in obj["required_human_inputs"]]
    assert "cloud_credentials" in [item["id"] for item in obj["required_human_inputs"]]
    pb = obj["promotion_readiness"]["promotion_blockers"]
    assert "production_manual_approval_gate" in pb
    assert "domain_name" in pb
    domain_item = next(i for i in obj["required_human_inputs"] if i["id"] == "domain_name")
    assert domain_item["status"] == "missing"
    assert domain_item["ask_order"] == 10
    assert domain_item["ui_prompt"]["audience"] == "non_technical"
    assert "question" in domain_item["ui_prompt"]
    assert domain_item["answer_binding"]["property"] == "domain"
    assert "svc_web" in domain_item["answer_binding"]["target_ids"]
    orders = [int(i["ask_order"]) for i in obj["required_human_inputs"]]
    assert orders == sorted(orders)
    assert res.artifact_summary_md is not None
    assert res.artifact_summary_md.path == ".akc/design/run_delivery.delivery_summary.md"
    md = res.artifact_summary_md.text()
    assert "## What AKC inferred" in md
    assert "## What will be deployed where" in md
    assert "## Approvals and gates" in md
    assert "## Information we still need from you" in md
    assert "## What healthy means for this system" in md


def test_run_delivery_plan_pass_adds_health_question_when_endpoint_unknown() -> None:
    ir_doc = IRDocument(
        tenant_id="tenant_a",
        repo_id="repo_a",
        nodes=(
            IRNode(
                id="svc_api",
                tenant_id="tenant_a",
                kind="service",
                name="api",
                properties={
                    "public": False,
                    "cloud_account": "acct-9",
                    "health_endpoint_known": False,
                },
            ),
        ),
    )
    intent = _intent_spec(allow_network=False)
    res = run_delivery_plan_pass(
        run_id="run_health_q",
        ir_document=ir_doc,
        intent_spec=intent,
        orchestration_spec_text='{"run_id":"run_health_q","steps":[]}',
        coordination_spec_text='{"run_id":"run_health_q","orchestration_bindings":[]}',
    )
    obj = json.loads(res.artifact_json.text())
    assert validate_obj(obj=obj, kind="delivery_plan", version=1) == []
    assert obj["promotion_readiness"]["status"] == "blocked"
    ids = [i["id"] for i in obj["required_human_inputs"]]
    assert ids == ["health_check_url"]
    health_item = obj["required_human_inputs"][0]
    assert health_item["answer_binding"]["property"] == "health_path"
    assert "svc_api" in health_item["answer_binding"]["target_ids"]
    assert "Health check address" in res.artifact_summary_md.text()


def test_run_delivery_plan_pass_batches_configuration_secrets_when_declared() -> None:
    ir_doc = IRDocument(
        tenant_id="tenant_a",
        repo_id="repo_a",
        nodes=(
            IRNode(
                id="svc_api",
                tenant_id="tenant_a",
                kind="service",
                name="api",
                properties={
                    "public": False,
                    "cloud_account": "acct-99",
                    "secrets": ["API_KEY", "DB_PASSWORD"],
                },
            ),
        ),
    )
    intent = _intent_spec(allow_network=False)
    res = run_delivery_plan_pass(
        run_id="run_secrets_batch",
        ir_document=ir_doc,
        intent_spec=intent,
        orchestration_spec_text='{"run_id":"run_secrets_batch","steps":[]}',
        coordination_spec_text='{"run_id":"run_secrets_batch","orchestration_bindings":[]}',
    )
    obj = json.loads(res.artifact_json.text())
    assert validate_obj(obj=obj, kind="delivery_plan", version=1) == []
    ids = [i["id"] for i in obj["required_human_inputs"]]
    assert "configuration_secrets" in ids
    sec_item = next(i for i in obj["required_human_inputs"] if i["id"] == "configuration_secrets")
    assert sec_item["answer_binding"]["property"] == "secrets_provisioned_in_store"
    assert "svc_api" in sec_item["answer_binding"]["target_ids"]
    by_t = sec_item["context"]["secret_requirements_by_target"]
    assert set(by_t["svc_api"]) == {"API_KEY", "DB_PASSWORD"}
    assert "Secret names" in res.artifact_summary_md.text()
    assert "secrets_provisioned_in_store" in res.artifact_summary_md.text()


def test_run_delivery_plan_pass_no_configuration_secrets_when_acknowledged() -> None:
    ir_doc = IRDocument(
        tenant_id="tenant_a",
        repo_id="repo_a",
        nodes=(
            IRNode(
                id="svc_api",
                tenant_id="tenant_a",
                kind="service",
                name="api",
                properties={
                    "public": False,
                    "cloud_account": "acct-99",
                    "secrets": ["API_KEY"],
                    "secrets_provisioned_in_store": True,
                },
            ),
        ),
    )
    intent = _intent_spec(allow_network=False)
    res = run_delivery_plan_pass(
        run_id="run_secrets_ok",
        ir_document=ir_doc,
        intent_spec=intent,
        orchestration_spec_text='{"run_id":"run_secrets_ok","steps":[]}',
        coordination_spec_text='{"run_id":"run_secrets_ok","orchestration_bindings":[]}',
    )
    obj = json.loads(res.artifact_json.text())
    assert validate_obj(obj=obj, kind="delivery_plan", version=1) == []
    assert obj["required_human_inputs"] == []
    assert obj["promotion_readiness"]["status"] == "blocked"
    assert obj["promotion_readiness"]["is_promotion_ready"] is False
    assert "production_manual_approval_gate" in obj["promotion_readiness"]["promotion_blockers"]


def test_run_delivery_plan_pass_batches_configuration_env_when_declared() -> None:
    ir_doc = IRDocument(
        tenant_id="tenant_a",
        repo_id="repo_a",
        nodes=(
            IRNode(
                id="svc_api",
                tenant_id="tenant_a",
                kind="service",
                name="api",
                properties={
                    "public": False,
                    "cloud_account": "acct-77",
                    "env": ["LOG_LEVEL", "FEATURE_FOO"],
                },
            ),
        ),
    )
    intent = _intent_spec(allow_network=False)
    res = run_delivery_plan_pass(
        run_id="run_env_batch",
        ir_document=ir_doc,
        intent_spec=intent,
        orchestration_spec_text='{"run_id":"run_env_batch","steps":[]}',
        coordination_spec_text='{"run_id":"run_env_batch","orchestration_bindings":[]}',
    )
    obj = json.loads(res.artifact_json.text())
    assert validate_obj(obj=obj, kind="delivery_plan", version=1) == []
    ids = [i["id"] for i in obj["required_human_inputs"]]
    assert ids == ["configuration_env"]
    env_item = obj["required_human_inputs"][0]
    assert env_item["answer_binding"]["property"] == "env_config_provisioned"
    by_t = env_item["context"]["env_requirements_by_target"]
    assert set(by_t["svc_api"]) == {"FEATURE_FOO", "LOG_LEVEL"}
    assert "Non-secret env keys" in res.artifact_summary_md.text()


def test_run_delivery_plan_pass_no_configuration_env_when_acknowledged() -> None:
    ir_doc = IRDocument(
        tenant_id="tenant_a",
        repo_id="repo_a",
        nodes=(
            IRNode(
                id="svc_api",
                tenant_id="tenant_a",
                kind="service",
                name="api",
                properties={
                    "public": False,
                    "cloud_account": "acct-77",
                    "env": ["REGION"],
                    "env_config_provisioned": True,
                },
            ),
        ),
    )
    intent = _intent_spec(allow_network=False)
    res = run_delivery_plan_pass(
        run_id="run_env_ok",
        ir_document=ir_doc,
        intent_spec=intent,
        orchestration_spec_text='{"run_id":"run_env_ok","steps":[]}',
        coordination_spec_text='{"run_id":"run_env_ok","orchestration_bindings":[]}',
    )
    obj = json.loads(res.artifact_json.text())
    assert obj["required_human_inputs"] == []
    assert obj["promotion_readiness"]["status"] == "blocked"
    assert "production_manual_approval_gate" in obj["promotion_readiness"]["promotion_blockers"]


def test_run_delivery_plan_pass_orders_configuration_secrets_before_env() -> None:
    ir_doc = IRDocument(
        tenant_id="tenant_a",
        repo_id="repo_a",
        nodes=(
            IRNode(
                id="svc_api",
                tenant_id="tenant_a",
                kind="service",
                name="api",
                properties={
                    "public": False,
                    "cloud_account": "acct-1",
                    "secrets": ["TOKEN"],
                    "env": ["LOG_LEVEL"],
                },
            ),
        ),
    )
    intent = _intent_spec(allow_network=False)
    res = run_delivery_plan_pass(
        run_id="run_both_cfg",
        ir_document=ir_doc,
        intent_spec=intent,
        orchestration_spec_text='{"run_id":"run_both_cfg","steps":[]}',
        coordination_spec_text='{"run_id":"run_both_cfg","orchestration_bindings":[]}',
    )
    obj = json.loads(res.artifact_json.text())
    ids = [i["id"] for i in obj["required_human_inputs"]]
    assert ids == ["configuration_secrets", "configuration_env"]
    assert obj["required_human_inputs"][0]["ask_order"] < obj["required_human_inputs"][1]["ask_order"]


def test_run_delivery_plan_pass_blocks_on_production_approval_gate_when_inputs_satisfied() -> None:
    ir_doc = IRDocument(
        tenant_id="tenant_a",
        repo_id="repo_a",
        nodes=(
            IRNode(
                id="svc_api",
                tenant_id="tenant_a",
                kind="service",
                name="api backend",
                properties={"public": False, "cloud_account": "acct-123"},
            ),
        ),
    )
    intent = _intent_spec(allow_network=False)
    res = run_delivery_plan_pass(
        run_id="run_ready",
        ir_document=ir_doc,
        intent_spec=intent,
        orchestration_spec_text='{"run_id":"run_ready","steps":[]}',
        coordination_spec_text='{"run_id":"run_ready","orchestration_bindings":[]}',
    )
    obj = json.loads(res.artifact_json.text())
    assert obj["required_human_inputs"] == []
    assert obj["promotion_readiness"]["status"] == "blocked"
    assert obj["promotion_readiness"]["is_promotion_ready"] is False
    assert obj["promotion_readiness"]["promotion_blockers"] == ["production_manual_approval_gate"]
    assert obj["promotion_readiness"]["production_manual_approval_required"] is True
    assert res.artifact_summary_md.path.endswith(".delivery_summary.md")
    md = res.artifact_summary_md.text()
    assert "fail-closed" in md
    assert "production_manual_approval_gate" in md


def test_run_runtime_bundle_pass_emits_versioned_runtime_bundle_schema() -> None:
    ir_doc = IRDocument(
        tenant_id="tenant_a",
        repo_id="repo_a",
        nodes=(
            IRNode(
                id="workflow-1",
                tenant_id="tenant_a",
                kind="workflow",
                name="deploy workflow",
                properties={"order_idx": 0, "status": "ready"},
                depends_on=("service-1",),
            ),
            IRNode(
                id="service-1",
                tenant_id="tenant_a",
                kind="service",
                name="api",
                properties={"step_id": "deploy-api"},
                effects=EffectAnnotation(network=False),
            ),
        ),
    )
    intent = _intent_spec(allow_network=True)
    delivery_plan = run_delivery_plan_pass(
        run_id="run_1",
        ir_document=ir_doc,
        intent_spec=intent,
        orchestration_spec_text=(
            '{"run_id":"run_1","tenant_id":"tenant_a","repo_id":"repo_a",'
            '"steps":[{"inputs":{"ir_node_id":"workflow-1"}}]}'
        ),
        coordination_spec_text=(
            '{"run_id":"run_1","tenant_id":"tenant_a","repo_id":"repo_a",'
            '"orchestration_bindings":[{"orchestration_step_ids":["workflow_000"]}]}'
        ),
    )

    result = run_runtime_bundle_pass(
        run_id="run_1",
        ir_document=ir_doc,
        intent_spec=intent,
        orchestration_spec_text=(
            '{"run_id":"run_1","tenant_id":"tenant_a","repo_id":"repo_a",'
            '"steps":[{"inputs":{"ir_node_id":"workflow-1"}}]}'
        ),
        coordination_spec_text=(
            '{"run_id":"run_1","tenant_id":"tenant_a","repo_id":"repo_a",'
            '"orchestration_bindings":[{"orchestration_step_ids":["workflow_000"]}]}'
        ),
        delivery_plan_text=delivery_plan.artifact_json.text(),
    )
    bundle_obj = json.loads(result.artifact_json.text())

    issues = validate_obj(obj=bundle_obj, kind="runtime_bundle", version=RUNTIME_BUNDLE_SCHEMA_VERSION)

    assert issues == []
    assert bundle_obj["schema_version"] == RUNTIME_BUNDLE_SCHEMA_VERSION
    assert bundle_obj["schema_id"] == f"akc:runtime_bundle:v{RUNTIME_BUNDLE_SCHEMA_VERSION}"
    assert bundle_obj.get("embed_system_ir") is False
    assert bundle_obj["system_ir_ref"]["path"] == ".akc/ir/run_1.json"
    assert bundle_obj["coordination_ref"]["path"] == ".akc/agents/run_1.coordination.json"
    assert bundle_obj["delivery_plan_ref"]["path"] == ".akc/deployment/run_1.delivery_plan.json"
    assert bundle_obj["coordination_ref"]["fingerprint"] == bundle_obj["spec_hashes"]["coordination_spec_sha256"]
    assert bundle_obj["system_ir_ref"]["fingerprint"] == ir_doc.fingerprint()
    assert bundle_obj["system_ir_ref"]["format_version"] == ir_doc.format_version
    assert "system_ir" not in bundle_obj
    assert bundle_obj["runtime_policy_envelope"]["adapter_fallback_mode"] == "native"
    assert bundle_obj["intent_ref"]["intent_id"] == "intent_abc"
    assert bundle_obj["intent_ref"]["stable_intent_sha256"] == result.metadata["stable_intent_sha256"]
    assert (
        bundle_obj["intent_policy_projection"]["stable_intent_sha256"]
        == bundle_obj["intent_ref"]["stable_intent_sha256"]
    )
    assert bundle_obj["intent_policy_projection"]["policies"] == [
        {
            "id": "policy.network",
            "requirement": "Constrain egress",
            "source": "security",
        }
    ]
    assert bundle_obj.get("reconcile_desired_state_source") == "ir"
    assert bundle_obj["runtime_policy_envelope"].get("require_reconcile_evidence") is True
    assert "reconciler.health_check" in bundle_obj.get("runtime_evidence_expectations", [])
    assert bundle_obj["deployment_provider_contract"]["kind"] == "in_memory"
    assert bundle_obj["deployment_provider_contract"]["mutation_mode"] == "observe_only"
    assert bundle_obj["workflow_execution_contract"]["allowed_routes"] == [
        "delegate_adapter",
        "noop",
        "subprocess",
        "http",
    ]
    assert bundle_obj["coordination_execution_contract"] == {
        "parallel_dispatch_enabled": True,
        "max_in_flight_steps": 4,
        "max_in_flight_per_role": 2,
        "completion_fold_order": "coordination_step_id",
    }
    assert "reconcile_deploy_targets_from_ir_only" not in bundle_obj
    assert "deployment_intents_ir_alignment" not in bundle_obj


def test_run_runtime_bundle_pass_v4_emits_phase_d_reconcile_metadata_when_requested() -> None:
    ir_doc = IRDocument(
        tenant_id="tenant_a",
        repo_id="repo_a",
        nodes=(
            IRNode(
                id="workflow-1",
                tenant_id="tenant_a",
                kind="workflow",
                name="deploy workflow",
                properties={"order_idx": 0, "status": "ready"},
                depends_on=("service-1",),
            ),
            IRNode(
                id="service-1",
                tenant_id="tenant_a",
                kind="service",
                name="api",
                properties={"step_id": "deploy-api"},
                effects=EffectAnnotation(network=False),
            ),
        ),
    )
    intent = _intent_spec(allow_network=False)
    delivery_plan = run_delivery_plan_pass(
        run_id="run_phase_d",
        ir_document=ir_doc,
        intent_spec=intent,
        orchestration_spec_text=(
            '{"run_id":"run_phase_d","tenant_id":"tenant_a","repo_id":"repo_a",'
            '"steps":[{"inputs":{"ir_node_id":"workflow-1"}}]}'
        ),
        coordination_spec_text=(
            '{"run_id":"run_phase_d","tenant_id":"tenant_a","repo_id":"repo_a",'
            '"orchestration_bindings":[{"orchestration_step_ids":["workflow_000"]}]}'
        ),
    )
    result = run_runtime_bundle_pass(
        run_id="run_phase_d",
        ir_document=ir_doc,
        intent_spec=intent,
        orchestration_spec_text=(
            '{"run_id":"run_phase_d","tenant_id":"tenant_a","repo_id":"repo_a",'
            '"steps":[{"inputs":{"ir_node_id":"workflow-1"}}]}'
        ),
        coordination_spec_text=(
            '{"run_id":"run_phase_d","tenant_id":"tenant_a","repo_id":"repo_a",'
            '"orchestration_bindings":[{"orchestration_step_ids":["workflow_000"]}]}'
        ),
        delivery_plan_text=delivery_plan.artifact_json.text(),
        runtime_bundle_schema_version=4,
        reconcile_deploy_targets_from_ir_only=True,
        deployment_intents_ir_alignment="strict",
    )
    bundle_obj = json.loads(result.artifact_json.text())
    assert validate_obj(obj=bundle_obj, kind="runtime_bundle", version=4) == []
    assert bundle_obj.get("reconcile_deploy_targets_from_ir_only") is True
    assert bundle_obj.get("deployment_intents_ir_alignment") == "strict"


def test_run_runtime_bundle_pass_without_success_criteria_still_prefers_ir_reconcile_lane() -> None:
    ir_doc = IRDocument(
        tenant_id="tenant_a",
        repo_id="repo_a",
        nodes=(
            IRNode(
                id="workflow-1",
                tenant_id="tenant_a",
                kind="workflow",
                name="deploy workflow",
                properties={"order_idx": 0, "status": "ready"},
                depends_on=("service-1",),
            ),
            IRNode(
                id="service-1",
                tenant_id="tenant_a",
                kind="service",
                name="api",
                properties={"step_id": "deploy-api"},
                effects=EffectAnnotation(network=False),
            ),
        ),
    )
    intent_no_sc = IntentSpecV1(
        intent_id="intent_min",
        tenant_id="tenant_a",
        repo_id="repo_a",
        goal_statement="goal",
        operating_bounds=OperatingBound(allow_network=False, max_output_tokens=256),
        policies=(PolicyRef(id="policy.network", source="security", requirement="Constrain egress"),),
        success_criteria=(),
    )
    result = run_runtime_bundle_pass(
        run_id="run_no_sc",
        ir_document=ir_doc,
        intent_spec=intent_no_sc,
        orchestration_spec_text=(
            '{"run_id":"run_no_sc","tenant_id":"tenant_a","repo_id":"repo_a",'
            '"steps":[{"inputs":{"ir_node_id":"workflow-1"}}]}'
        ),
        coordination_spec_text=(
            '{"run_id":"run_no_sc","tenant_id":"tenant_a","repo_id":"repo_a",'
            '"orchestration_bindings":[{"orchestration_step_ids":["workflow_000"]}]}'
        ),
        delivery_plan_text=None,
    )
    bundle_obj = json.loads(result.artifact_json.text())
    assert bundle_obj.get("reconcile_desired_state_source") == "ir"
    assert bundle_obj["runtime_policy_envelope"].get("require_reconcile_evidence") is None


def test_run_runtime_bundle_pass_embed_system_ir_true_includes_inline_ir() -> None:
    ir_doc = IRDocument(
        tenant_id="tenant_a",
        repo_id="repo_a",
        nodes=(
            IRNode(
                id="workflow-1",
                tenant_id="tenant_a",
                kind="workflow",
                name="deploy workflow",
                properties={"order_idx": 0, "status": "ready"},
            ),
        ),
    )
    intent = _intent_spec(allow_network=False)
    result = run_runtime_bundle_pass(
        run_id="run_embed",
        ir_document=ir_doc,
        intent_spec=intent,
        orchestration_spec_text=(
            '{"run_id":"run_embed","tenant_id":"tenant_a","repo_id":"repo_a",'
            '"steps":[{"inputs":{"ir_node_id":"workflow-1"}}]}'
        ),
        coordination_spec_text=(
            '{"run_id":"run_embed","tenant_id":"tenant_a","repo_id":"repo_a",'
            '"orchestration_bindings":[{"orchestration_step_ids":["workflow_000"]}]}'
        ),
        embed_system_ir=True,
        runtime_bundle_schema_version=RUNTIME_BUNDLE_SCHEMA_VERSION,
    )
    bundle_obj = json.loads(result.artifact_json.text())
    assert validate_obj(obj=bundle_obj, kind="runtime_bundle", version=RUNTIME_BUNDLE_SCHEMA_VERSION) == []
    assert bundle_obj.get("embed_system_ir") is True
    assert "system_ir" in bundle_obj
    assert bundle_obj["system_ir"]["tenant_id"] == "tenant_a"
    assert result.metadata.get("embed_system_ir") is True


def test_run_runtime_bundle_pass_v2_omits_embed_system_ir_field() -> None:
    ir_doc = IRDocument(
        tenant_id="tenant_a",
        repo_id="repo_a",
        nodes=(
            IRNode(
                id="workflow-1",
                tenant_id="tenant_a",
                kind="workflow",
                name="deploy workflow",
                properties={"order_idx": 0, "status": "ready"},
            ),
        ),
    )
    intent = _intent_spec(allow_network=False)
    result = run_runtime_bundle_pass(
        run_id="run_v2",
        ir_document=ir_doc,
        intent_spec=intent,
        orchestration_spec_text=(
            '{"run_id":"run_v2","tenant_id":"tenant_a","repo_id":"repo_a",'
            '"steps":[{"inputs":{"ir_node_id":"workflow-1"}}]}'
        ),
        coordination_spec_text=(
            '{"run_id":"run_v2","tenant_id":"tenant_a","repo_id":"repo_a",'
            '"orchestration_bindings":[{"orchestration_step_ids":["workflow_000"]}]}'
        ),
        embed_system_ir=True,
        runtime_bundle_schema_version=2,
    )
    bundle_obj = json.loads(result.artifact_json.text())
    assert validate_obj(obj=bundle_obj, kind="runtime_bundle", version=2) == []
    assert bundle_obj["schema_version"] == 2
    assert "embed_system_ir" not in bundle_obj
    assert "system_ir" in bundle_obj


def test_runtime_bundle_pass_surfaces_knowledge_network_guardrails_from_ir() -> None:
    aid = "assertion_ir_network_guard"
    ir_doc = IRDocument(
        tenant_id="tenant_a",
        repo_id="repo_a",
        nodes=(
            IRNode(
                id="workflow-1",
                tenant_id="tenant_a",
                kind="workflow",
                name="deploy workflow",
                properties={"order_idx": 0, "status": "ready"},
            ),
            IRNode(
                id="kc-net",
                tenant_id="tenant_a",
                kind="entity",
                name=f"knowledge_constraint:{aid}",
                properties={
                    "assertion_id": aid,
                    "predicate": "forbidden",
                    "kind": "hard",
                    "summary": "Public internet egress is forbidden for this workload.",
                },
            ),
            IRNode(
                id="kd-net",
                tenant_id="tenant_a",
                kind="entity",
                name=f"knowledge_decision:{aid}",
                properties={
                    "assertion_id": aid,
                    "selected": True,
                    "resolved": True,
                },
            ),
        ),
    )
    intent = _intent_spec(allow_network=True)
    result = run_runtime_bundle_pass(
        run_id="run_k",
        ir_document=ir_doc,
        intent_spec=intent,
        orchestration_spec_text=(
            '{"run_id":"run_k","tenant_id":"tenant_a","repo_id":"repo_a",'
            '"steps":[{"inputs":{"ir_node_id":"workflow-1"}}]}'
        ),
        coordination_spec_text=(
            '{"run_id":"run_k","tenant_id":"tenant_a","repo_id":"repo_a",'
            '"orchestration_bindings":[{"orchestration_step_ids":["workflow_000"]}]}'
        ),
    )
    bundle_obj = json.loads(result.artifact_json.text())
    renv = bundle_obj["runtime_policy_envelope"]
    assert renv.get("knowledge_network_egress_forbidden") is True
    assert renv.get("allow_network") is False
    assert renv.get("knowledge_network_tightening") is True
    assert isinstance(renv.get("knowledge_hard_constraints"), list)


def test_workflow_validator_rejects_prohibited_pr_patterns() -> None:
    bad_workflow = {
        "name": "Bad",
        "on": {"pull_request_target": {}},
        "jobs": {
            "test": {
                "runs-on": "ubuntu-latest",
                "permissions": {"contents": "write"},
                "steps": [{"run": "echo ${{ secrets.MY_SECRET }}"}],
            }
        },
    }
    try:
        _validate_workflow_policy(workflow_obj=bad_workflow)
        raise AssertionError("expected workflow validator to fail")
    except ValueError as exc:
        assert "pull_request_target" in str(exc)


def test_k8s_validator_rejects_missing_restricted_security_context() -> None:
    bad_deployment = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": "bad"},
        "spec": {
            "template": {
                "spec": {
                    "containers": [
                        {
                            "name": "bad",
                            "image": "bad:latest",
                            "securityContext": {"allowPrivilegeEscalation": True},
                        }
                    ]
                }
            }
        },
    }
    try:
        _validate_k8s_restricted_security_context(deployment_obj=bad_deployment)
        raise AssertionError("expected k8s validator to fail")
    except ValueError:
        pass


def test_replayable_passes_concat_controller_and_artifact_order() -> None:
    assert REPLAYABLE_PASSES == CONTROLLER_LOOP_PASS_ORDER + ARTIFACT_PASS_ORDER


def test_assert_expected_artifact_pass_order_accepts_registry_order() -> None:
    assert_expected_artifact_pass_order(actual=list(ARTIFACT_PASS_ORDER))


def test_assert_expected_artifact_pass_order_rejects_drift() -> None:
    try:
        assert_expected_artifact_pass_order(actual=["system_design", "runtime_bundle"])
        raise AssertionError("expected RuntimeError")
    except RuntimeError as exc:
        assert "artifact pass order mismatch" in str(exc).lower()
