from __future__ import annotations

import json
from pathlib import Path

from akc.artifacts.validate import validate_obj
from akc.compile.artifact_passes import (
    run_delivery_plan_pass,
    run_infrastructure_synthesis_pass,
    run_runtime_bundle_pass,
)
from akc.intent import IntentSpecV1, OperatingBound, PolicyRef, SuccessCriterion
from akc.ir import IRDocument

_FIXTURE = Path(__file__).resolve().parent.parent / "fixtures" / "delivery" / "three_tier_system_ir.json"


def _intent(tenant: str, repo: str) -> IntentSpecV1:
    return IntentSpecV1(
        intent_id="intent_infra",
        tenant_id=tenant,
        repo_id=repo,
        goal_statement="Provision supporting cloud resources",
        operating_bounds=OperatingBound(allow_network=False, max_output_tokens=256),
        policies=(PolicyRef(id="policy.net", source="security", requirement="egress"),),
        success_criteria=(SuccessCriterion(id="sc1", evaluation_mode="tests", description="tests"),),
    )


def test_delivery_pipeline_emits_infrastructure_artifacts_and_runtime_refs() -> None:
    ir_document = IRDocument.from_json_file(_FIXTURE)
    intent = _intent("tenant_golden", "repo_golden")
    run_id = "infra_e2e"
    orch = json.dumps({"run_id": run_id, "tenant_id": "tenant_golden", "repo_id": "repo_golden"})
    coord = orch

    delivery = run_delivery_plan_pass(
        run_id=run_id,
        ir_document=ir_document,
        intent_spec=intent,
        orchestration_spec_text=orch,
        coordination_spec_text=coord,
    )
    infra = run_infrastructure_synthesis_pass(
        run_id=run_id,
        ir_document=ir_document,
        delivery_plan_text=delivery.artifact_json.text(),
    )
    assert validate_obj(obj=json.loads(infra.artifact_infra_plan_json.text()), kind="infra_plan", version=1) == []
    assert validate_obj(obj=json.loads(infra.artifact_iac_manifest_json.text()), kind="iac_manifest", version=1) == []

    bundle = run_runtime_bundle_pass(
        run_id=run_id,
        ir_document=ir_document,
        intent_spec=intent,
        orchestration_spec_text=orch,
        coordination_spec_text=coord,
        delivery_plan_text=delivery.artifact_json.text(),
        infra_plan_text=infra.artifact_infra_plan_json.text(),
        iac_manifest_text=infra.artifact_iac_manifest_json.text(),
        execution_workspace_manifest_text=None,
    )
    bundle_obj = json.loads(bundle.artifact_json.text())
    assert bundle_obj["infra_plan_ref"]["path"] == f".akc/infra/{run_id}.infra_plan.json"
    assert bundle_obj["iac_manifest_ref"]["path"] == f".akc/infra/{run_id}.iac_manifest.json"
    assert bundle_obj["provisioning_readiness"]["status"] in {"ready", "blocked"}
