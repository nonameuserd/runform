from __future__ import annotations

import json

from akc.artifacts.schemas import RUNTIME_BUNDLE_SCHEMA_VERSION
from akc.artifacts.validate import validate_obj
from akc.compile.artifact_passes import run_deployment_config_pass, run_runtime_bundle_pass
from akc.intent import (
    IntentSpecV1,
    OperatingBound,
    PolicyRef,
    SuccessCriterion,
    stable_intent_sha256,
)
from akc.ir import EffectAnnotation, IRDocument, IRNode


def _intent() -> IntentSpecV1:
    return IntentSpecV1(
        intent_id="intent_handoff_parity",
        tenant_id="tenant_a",
        repo_id="repo_a",
        goal_statement="Runtime bundle and deployment must share intent correlation",
        operating_bounds=OperatingBound(allow_network=False, max_output_tokens=128),
        policies=(PolicyRef(id="policy.net", source="security", requirement="Isolate"),),
        success_criteria=(
            SuccessCriterion(
                id="success.tests",
                evaluation_mode="tests",
                description="Green tests",
            ),
        ),
    )


def _ir() -> IRDocument:
    return IRDocument(
        tenant_id="tenant_a",
        repo_id="repo_a",
        nodes=(
            IRNode(
                id="workflow-1",
                tenant_id="tenant_a",
                kind="workflow",
                name="deploy",
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


def test_runtime_bundle_and_deployment_share_stable_intent_sha256() -> None:
    """Phase D: one compile intent → identical handoff correlation on bundle and deployment artifacts."""

    run_id = "run_handoff_parity"
    orch = (
        '{"run_id":"run_handoff_parity","tenant_id":"tenant_a","repo_id":"repo_a",'
        '"steps":[{"inputs":{"ir_node_id":"workflow-1"}}]}'
    )
    coord = '{"run_id":"run_handoff_parity","tenant_id":"tenant_a","repo_id":"repo_a"}'
    ir_doc = _ir()
    intent = _intent()

    bundle_res = run_runtime_bundle_pass(
        run_id=run_id,
        ir_document=ir_doc,
        intent_spec=intent,
        orchestration_spec_text=orch,
        coordination_spec_text=coord,
        runtime_bundle_schema_version=RUNTIME_BUNDLE_SCHEMA_VERSION,
    )
    deploy_res = run_deployment_config_pass(
        run_id=run_id,
        ir_document=ir_doc,
        intent_spec=intent,
        orchestration_spec_text=orch,
        coordination_spec_text=coord,
    )

    bundle_obj = json.loads(bundle_res.artifact_json.text())
    assert validate_obj(obj=bundle_obj, kind="runtime_bundle", version=RUNTIME_BUNDLE_SCHEMA_VERSION) == []

    bundle_ref = bundle_obj["intent_ref"]
    deploy_ref = deploy_res.metadata.get("intent_ref")
    assert isinstance(deploy_ref, dict)

    assert bundle_ref == deploy_ref
    sha = str(bundle_ref["stable_intent_sha256"])
    assert sha == stable_intent_sha256(intent=intent.normalized())
    assert sha == deploy_res.metadata["stable_intent_sha256"]
    assert sha == bundle_res.metadata["stable_intent_sha256"]

    compose_md = deploy_res.artifact_docker_compose.metadata
    assert isinstance(compose_md, dict)
    assert compose_md["intent_ref"] == bundle_ref

    compose_yaml = deploy_res.artifact_docker_compose.text()
    assert f"com.akc.run.stable-intent-sha256: {sha}" in compose_yaml or (
        f"com.akc.run.stable-intent-sha256: '{sha}'" in compose_yaml
    )
