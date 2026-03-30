from __future__ import annotations

import json
import stat
from argparse import Namespace
from pathlib import Path

from akc.cli.provision import cmd_provision_apply, cmd_provision_plan, cmd_provision_status
from akc.compile.infrastructure_synthesis import build_infrastructure_synthesis
from akc.ir import IRDocument, IRNode


def _seed_infra_outputs(project_dir: Path, *, run_id: str) -> None:
    ir_document = IRDocument(
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
        ),
    )
    delivery_plan = {
        "run_id": run_id,
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
                "depends_on": [],
                "exposure_model": {"public": True},
                "config_secrets_contract": {"required_env": [], "required_secrets": []},
            }
        ],
        "required_human_inputs": [],
        "promotion_readiness": {"status": "ready"},
    }
    _, _, artifacts = build_infrastructure_synthesis(
        run_id=run_id, ir_document=ir_document, delivery_plan_obj=delivery_plan
    )
    for artifact in artifacts:
        target = project_dir / artifact.path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(artifact.text(), encoding="utf-8")


def _write_fake_tool(tmp_path: Path, *, name: str) -> Path:
    tool = tmp_path / name
    tool.write_text(
        "\n".join(
            [
                "#!/bin/sh",
                "printf '%s\\n' \"$@\"",
                "exit 0",
                "",
            ]
        ),
        encoding="utf-8",
    )
    tool.chmod(tool.stat().st_mode | stat.S_IEXEC)
    return tool


def test_cli_provision_plan_apply_and_status(tmp_path: Path, monkeypatch: object, capsys: object) -> None:
    run_id = "run-1"
    _seed_infra_outputs(tmp_path, run_id=run_id)
    fake_terraform = _write_fake_tool(tmp_path, name="terraform")
    monkeypatch.setenv("AKC_TERRAFORM_BIN", str(fake_terraform))

    code = cmd_provision_plan(
        Namespace(
            project_dir=tmp_path,
            run_id=run_id,
            backend="terraform",
            environment="staging",
            provision_id="prov-1",
        )
    )
    assert code == 0
    plan_out = json.loads(capsys.readouterr().out)
    assert plan_out["ok"] is True
    assert plan_out["status"] == "ready"

    code = cmd_provision_apply(
        Namespace(
            project_dir=tmp_path,
            provision_id="prov-1",
            approve_production=False,
        )
    )
    assert code == 0
    apply_out = json.loads(capsys.readouterr().out)
    assert apply_out["ok"] is True
    assert apply_out["status"] == "applied"

    code = cmd_provision_status(Namespace(project_dir=tmp_path, provision_id="prov-1", run_id=None))
    assert code == 0
    status_out = json.loads(capsys.readouterr().out)
    assert status_out["session"]["status"] == "applied"
    assert status_out["plan"]["backend"] == "terraform"
    assert status_out["apply"]["status"] == "applied"


def test_cli_provision_apply_blocks_production_without_approval(
    tmp_path: Path, monkeypatch: object, capsys: object
) -> None:
    run_id = "run-prod"
    _seed_infra_outputs(tmp_path, run_id=run_id)
    fake_terraform = _write_fake_tool(tmp_path, name="terraform")
    monkeypatch.setenv("AKC_TERRAFORM_BIN", str(fake_terraform))

    plan_code = cmd_provision_plan(
        Namespace(
            project_dir=tmp_path,
            run_id=run_id,
            backend="terraform",
            environment="production",
            provision_id="prov-prod",
        )
    )
    assert plan_code == 0
    _ = capsys.readouterr()

    apply_code = cmd_provision_apply(
        Namespace(
            project_dir=tmp_path,
            provision_id="prov-prod",
            approve_production=False,
        )
    )
    assert apply_code == 2
    apply_out = json.loads(capsys.readouterr().out)
    assert apply_out["status"] == "blocked"
    assert "production_approval_required" in apply_out["issues"]
