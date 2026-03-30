from __future__ import annotations

import json

from akc.artifacts.validate import validate_obj
from akc.compile.infrastructure_synthesis import build_infrastructure_synthesis
from akc.ir import IRDocument, IRNode
from akc.runtime.bundle_delivery import build_delivery_handoff_context


def _ir_document() -> IRDocument:
    return IRDocument(
        tenant_id="tenant_a",
        repo_id="repo_a",
        nodes=(
            IRNode(
                id="svc_api",
                tenant_id="tenant_a",
                kind="service",
                name="api backend",
                properties={
                    "public": True,
                    "domain": "api.example.com",
                    "cloud_account": "acct-1",
                    "aws_account_id": "123456789012",
                    "aws_region": "us-east-1",
                    "route53_zone": "example.com",
                    "terraform_state_backend": "s3://infra-state/repo_a",
                    "cdk_bootstrap_completed": True,
                    "cluster_destination": "eks-staging",
                },
            ),
            IRNode(
                id="svc_web",
                tenant_id="tenant_a",
                kind="service",
                name="web frontend",
                properties={
                    "public": True,
                    "domain": "app.example.com",
                    "cloud_account": "acct-1",
                    "aws_account_id": "123456789012",
                    "aws_region": "us-east-1",
                    "route53_zone": "example.com",
                    "terraform_state_backend": "s3://infra-state/repo_a",
                    "cdk_bootstrap_completed": True,
                    "cluster_destination": "eks-staging",
                },
                depends_on=("svc_api",),
            ),
            IRNode(
                id="infra_cache",
                tenant_id="tenant_a",
                kind="infrastructure",
                name="redis cache",
                properties={
                    "cloud_account": "acct-1",
                    "aws_account_id": "123456789012",
                    "aws_region": "us-east-1",
                    "terraform_state_backend": "s3://infra-state/repo_a",
                    "cdk_bootstrap_completed": True,
                    "cluster_destination": "eks-staging",
                },
            ),
        ),
    )


def _delivery_plan() -> dict[str, object]:
    return {
        "run_id": "run-1",
        "tenant_id": "tenant_a",
        "repo_id": "repo_a",
        "targets": [
            {
                "target_id": "svc_api",
                "name": "api backend",
                "target_class": "backend_service",
                "node_kind": "service",
                "domain": "api.example.com",
                "cloud_account": "acct-1",
                "aws_account_id": "123456789012",
                "aws_region": "us-east-1",
                "route53_zone": "example.com",
                "terraform_state_backend": "s3://infra-state/repo_a",
                "cdk_bootstrap_completed": True,
                "cluster_destination": "eks-staging",
                "depends_on": ["infra_cache"],
                "exposure_model": {"public": True},
                "config_secrets_contract": {"required_env": [], "required_secrets": ["API_TOKEN"]},
            },
            {
                "target_id": "svc_web",
                "name": "web frontend",
                "target_class": "web_app",
                "node_kind": "service",
                "domain": "app.example.com",
                "cloud_account": "acct-1",
                "aws_account_id": "123456789012",
                "aws_region": "us-east-1",
                "route53_zone": "example.com",
                "terraform_state_backend": "s3://infra-state/repo_a",
                "cdk_bootstrap_completed": True,
                "cluster_destination": "eks-staging",
                "depends_on": ["svc_api"],
                "exposure_model": {"public": True},
                "config_secrets_contract": {"required_env": [], "required_secrets": []},
            },
            {
                "target_id": "infra_cache",
                "name": "redis cache",
                "target_class": "infrastructure_component",
                "node_kind": "infrastructure",
                "cloud_account": "acct-1",
                "aws_account_id": "123456789012",
                "aws_region": "us-east-1",
                "terraform_state_backend": "s3://infra-state/repo_a",
                "cdk_bootstrap_completed": True,
                "cluster_destination": "eks-staging",
                "depends_on": [],
                "exposure_model": {"public": False},
                "config_secrets_contract": {"required_env": [], "required_secrets": []},
            },
        ],
        "required_human_inputs": [],
        "promotion_readiness": {"status": "ready"},
    }


def test_build_infrastructure_synthesis_emits_valid_artifacts() -> None:
    infra_plan, iac_manifest, artifacts = build_infrastructure_synthesis(
        run_id="run-1",
        ir_document=_ir_document(),
        delivery_plan_obj=_delivery_plan(),
    )
    assert validate_obj(obj=infra_plan, kind="infra_plan", version=1) == []
    assert validate_obj(obj=iac_manifest, kind="iac_manifest", version=1) == []
    assert infra_plan["provisioning_readiness"]["status"] == "ready"
    assert iac_manifest["preferred_backend"] == "terraform"
    paths = {artifact.path for artifact in artifacts}
    assert ".akc/infra/run-1.infra_plan.json" in paths
    assert ".akc/infra/run-1.iac_manifest.json" in paths
    assert ".akc/infra/run-1/terraform/main.tf.json" in paths
    assert ".akc/infra/run-1/aws-cdk/bin/app.ts" in paths
    tf_main = next(artifact for artifact in artifacts if artifact.path.endswith("/terraform/main.tf.json"))
    tf_obj = json.loads(tf_main.text())
    assert "resource" in tf_obj
    cdk_stack = next(
        artifact for artifact in artifacts if "/aws-cdk/lib/" in artifact.path and artifact.path.endswith(".ts")
    )
    assert "cluster_endpoint" in cdk_stack.text()


def test_build_infrastructure_synthesis_blocks_unsupported_infrastructure() -> None:
    ir_document = IRDocument(
        tenant_id="tenant_a",
        repo_id="repo_a",
        nodes=(
            IRNode(
                id="infra_weird",
                tenant_id="tenant_a",
                kind="infrastructure",
                name="custom broker appliance",
                properties={"cloud_account": "acct-1"},
            ),
        ),
    )
    delivery_plan = {
        "run_id": "run-2",
        "tenant_id": "tenant_a",
        "repo_id": "repo_a",
        "targets": [
            {
                "target_id": "infra_weird",
                "name": "custom broker appliance",
                "target_class": "infrastructure_component",
                "node_kind": "infrastructure",
                "cloud_account": "acct-1",
                "exposure_model": {"public": False},
                "config_secrets_contract": {"required_env": [], "required_secrets": []},
            }
        ],
        "required_human_inputs": [],
        "promotion_readiness": {"status": "ready"},
    }
    infra_plan, _, _ = build_infrastructure_synthesis(
        run_id="run-2",
        ir_document=ir_document,
        delivery_plan_obj=delivery_plan,
    )
    assert infra_plan["provisioning_readiness"]["status"] == "blocked"
    assert (
        "unsupported_infrastructure_component:infra_weird"
        in infra_plan["provisioning_readiness"]["provisioning_blockers"]
    )


def test_runtime_delivery_handoff_context_includes_infra_refs() -> None:
    context = build_delivery_handoff_context(
        {
            "delivery_plan_ref": {"path": ".akc/deployment/run-1.delivery_plan.json", "fingerprint": "a" * 64},
            "execution_workspace_ref": {
                "path": ".akc/execution/run-1.execution_workspace_manifest.json",
                "fingerprint": "b" * 64,
            },
            "infra_plan_ref": {"path": ".akc/infra/run-1.infra_plan.json", "fingerprint": "c" * 64},
            "iac_manifest_ref": {"path": ".akc/infra/run-1.iac_manifest.json", "fingerprint": "d" * 64},
            "provisioning_readiness": {"status": "blocked"},
        }
    )
    assert context["infra_plan_ref"]["path"].endswith(".infra_plan.json")
    assert context["iac_manifest_ref"]["path"].endswith(".iac_manifest.json")
    assert context["provisioning_readiness_status"] == "blocked"
