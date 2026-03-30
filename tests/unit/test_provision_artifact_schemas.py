from __future__ import annotations

from akc.artifacts.validate import validate_obj


def test_infra_plan_schema_accepts_minimal_valid_object() -> None:
    obj = {
        "run_id": "run-1",
        "tenant_id": "tenant_a",
        "repo_id": "repo_a",
        "cloud_provider": "aws",
        "supported_iac_backends": ["terraform", "aws_cdk"],
        "preferred_iac_backend": "terraform",
        "provisioning_environments": ["staging", "production"],
        "workload_targets": [],
        "resources": [],
        "provisioning_readiness": {"status": "ready"},
    }
    assert validate_obj(obj=obj, kind="infra_plan", version=1) == []


def test_iac_manifest_schema_accepts_minimal_valid_object() -> None:
    obj = {
        "run_id": "run-1",
        "tenant_id": "tenant_a",
        "repo_id": "repo_a",
        "cloud_provider": "aws",
        "supported_backends": ["terraform", "aws_cdk"],
        "preferred_backend": "terraform",
        "infra_plan_ref": {"path": ".akc/infra/run-1.infra_plan.json", "fingerprint": "a" * 64},
        "provisioning_readiness": {"status": "ready"},
        "environments": ["staging", "production"],
        "workspaces": {},
    }
    assert validate_obj(obj=obj, kind="iac_manifest", version=1) == []


def test_provision_plan_schema_accepts_minimal_valid_object() -> None:
    obj = {
        "provision_id": "prov-1",
        "compile_run_id": "run-1",
        "tenant_id": "tenant_a",
        "repo_id": "repo_a",
        "backend": "terraform",
        "environment": "staging",
        "status": "ready",
        "desired_fingerprint": "a" * 64,
        "commands": [],
        "created_at_ms": 1,
    }
    assert validate_obj(obj=obj, kind="provision_plan", version=1) == []


def test_provision_apply_schema_accepts_minimal_valid_object() -> None:
    obj = {
        "provision_id": "prov-1",
        "compile_run_id": "run-1",
        "backend": "terraform",
        "environment": "staging",
        "status": "applied",
        "desired_fingerprint": "a" * 64,
        "commands": [],
        "created_at_ms": 1,
    }
    assert validate_obj(obj=obj, kind="provision_apply", version=1) == []


def test_provision_session_schema_accepts_minimal_valid_object() -> None:
    obj = {
        "provision_id": "prov-1",
        "compile_run_id": "run-1",
        "tenant_id": "tenant_a",
        "repo_id": "repo_a",
        "backend": "terraform",
        "environment": "staging",
        "status": "planned",
        "desired_fingerprint": "a" * 64,
        "created_at_ms": 1,
        "updated_at_ms": 1,
    }
    assert validate_obj(obj=obj, kind="provision_session", version=1) == []
