from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from typing import Any, cast

from akc.artifacts.contracts import apply_schema_envelope
from akc.ir import IRDocument
from akc.memory.models import JSONValue
from akc.outputs.models import OutputArtifact
from akc.utils.fingerprint import stable_json_fingerprint

VALID_IAC_BACKENDS: tuple[str, str] = ("terraform", "aws_cdk")
_PROVISIONING_ENVIRONMENTS: tuple[str, str] = ("staging", "production")


def _slug(value: str) -> str:
    out = "".join(ch.lower() if ch.isalnum() else "-" for ch in str(value))
    while "--" in out:
        out = out.replace("--", "-")
    return out.strip("-") or "generated"


def _pascal_identifier(value: str) -> str:
    parts = [part for part in re.split(r"[^A-Za-z0-9]+", str(value)) if part]
    if not parts:
        return "Generated"
    out = "".join(part[:1].upper() + part[1:] for part in parts)
    return f"Generated{out}" if out[:1].isdigit() else out


def _as_str(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _is_public_target(target: Mapping[str, Any]) -> bool:
    exposure = target.get("exposure_model")
    return bool(isinstance(exposure, Mapping) and exposure.get("public"))


def _recognized_infra_kind(name: str) -> str | None:
    n = _slug(name)
    if any(token in n for token in ("redis", "cache")):
        return "elasticache_redis"
    if any(token in n for token in ("postgres", "postgresql", "database", "db")):
        return "rds_postgres"
    if any(token in n for token in ("bucket", "storage", "blob")):
        return "s3_bucket"
    if any(token in n for token in ("queue", "sqs", "jobs")):
        return "sqs_queue"
    return None


def _shared_resources(*, repo_id: str) -> list[dict[str, JSONValue]]:
    repo_slug = _slug(repo_id)
    return [
        {"resource_id": "aws_network_vpc", "resource_kind": "vpc", "provider": "aws", "scope": "shared"},
        {
            "resource_id": "aws_network_public_subnets",
            "resource_kind": "public_subnets",
            "provider": "aws",
            "scope": "shared",
        },
        {
            "resource_id": "aws_network_private_subnets",
            "resource_kind": "private_subnets",
            "provider": "aws",
            "scope": "shared",
        },
        {
            "resource_id": "aws_network_nat_gateway",
            "resource_kind": "nat_gateway",
            "provider": "aws",
            "scope": "shared",
        },
        {
            "resource_id": f"{repo_slug}_eks_cluster",
            "resource_kind": "eks_cluster",
            "provider": "aws",
            "scope": "shared",
            "outputs": ["cluster_name", "cluster_endpoint", "cluster_oidc_provider_arn"],
        },
        {
            "resource_id": f"{repo_slug}_eks_node_group",
            "resource_kind": "eks_managed_node_group",
            "provider": "aws",
            "scope": "shared",
        },
    ]


def _target_resource_rows(*, targets: Sequence[Mapping[str, Any]]) -> tuple[list[dict[str, JSONValue]], list[str]]:
    resources: list[dict[str, JSONValue]] = []
    unsupported: list[str] = []
    for target in targets:
        target_id = _as_str(target.get("target_id")) or "unknown"
        target_name = _as_str(target.get("name")) or target_id
        target_class = _as_str(target.get("target_class")) or "unknown"
        target_slug = _slug(target_name)
        resources.append(
            {
                "resource_id": f"{target_slug}_ecr_repo",
                "resource_kind": "ecr_repository",
                "provider": "aws",
                "scope": "target",
                "target_id": target_id,
                "target_class": target_class,
                "outputs": ["repository_url"],
            }
        )
        resources.append(
            {
                "resource_id": f"{target_slug}_log_group",
                "resource_kind": "cloudwatch_log_group",
                "provider": "aws",
                "scope": "target",
                "target_id": target_id,
                "target_class": target_class,
            }
        )
        resources.append(
            {
                "resource_id": f"{target_slug}_irsa_role",
                "resource_kind": "iam_role_irsa",
                "provider": "aws",
                "scope": "target",
                "target_id": target_id,
                "target_class": target_class,
            }
        )

        config_contract = target.get("config_secrets_contract")
        raw_secrets = config_contract.get("required_secrets") if isinstance(config_contract, Mapping) else None
        if isinstance(raw_secrets, Sequence) and not isinstance(raw_secrets, (str, bytes)):
            for secret_name in sorted({str(item).strip() for item in raw_secrets if str(item).strip()}):
                resources.append(
                    {
                        "resource_id": f"{target_slug}_secret_{_slug(secret_name)}",
                        "resource_kind": "secrets_manager_placeholder",
                        "provider": "aws",
                        "scope": "target",
                        "target_id": target_id,
                        "secret_name": secret_name,
                    }
                )

        if _is_public_target(target):
            dom = _as_str(target.get("domain")) or f"{target_slug}.example.com"
            resources.extend(
                [
                    {
                        "resource_id": f"{target_slug}_route53_record",
                        "resource_kind": "route53_record",
                        "provider": "aws",
                        "scope": "target",
                        "target_id": target_id,
                        "domain": dom,
                    },
                    {
                        "resource_id": f"{target_slug}_acm_certificate",
                        "resource_kind": "acm_certificate",
                        "provider": "aws",
                        "scope": "target",
                        "target_id": target_id,
                        "domain": dom,
                    },
                ]
            )

        if str(target.get("node_kind")) == "infrastructure":
            recognized = _recognized_infra_kind(target_name)
            if recognized is None:
                unsupported.append(target_id)
                continue
            outputs: list[str] = []
            if recognized == "elasticache_redis":
                outputs = ["redis_primary_endpoint", "redis_port"]
            elif recognized == "rds_postgres":
                outputs = ["postgres_endpoint", "postgres_port", "postgres_db_name"]
            elif recognized == "s3_bucket":
                outputs = ["bucket_name"]
            elif recognized == "sqs_queue":
                outputs = ["queue_url", "queue_arn"]
            resources.append(
                {
                    "resource_id": f"{target_slug}_{recognized}",
                    "resource_kind": recognized,
                    "provider": "aws",
                    "scope": "target",
                    "target_id": target_id,
                    "outputs": cast(JSONValue, outputs),
                }
            )
    return resources, unsupported


def _delivery_targets(delivery_plan_obj: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(delivery_plan_obj, Mapping):
        return []
    raw_targets = delivery_plan_obj.get("targets")
    if not isinstance(raw_targets, Sequence) or isinstance(raw_targets, (str, bytes)):
        return []
    return [dict(cast(dict[str, Any], row)) for row in raw_targets if isinstance(row, Mapping)]


def _provisioning_readiness(
    *,
    delivery_plan_obj: Mapping[str, Any] | None,
    unsupported_target_ids: Sequence[str],
) -> dict[str, JSONValue]:
    blocking_inputs: list[str] = []
    if isinstance(delivery_plan_obj, Mapping):
        raw_reqs = delivery_plan_obj.get("required_human_inputs")
        if isinstance(raw_reqs, Sequence) and not isinstance(raw_reqs, (str, bytes)):
            for item in raw_reqs:
                if not isinstance(item, Mapping):
                    continue
                blocking_for = item.get("blocking_for")
                if not isinstance(blocking_for, Sequence) or isinstance(blocking_for, (str, bytes)):
                    continue
                if any(str(env).strip() in _PROVISIONING_ENVIRONMENTS for env in blocking_for):
                    iid = _as_str(item.get("id"))
                    if iid:
                        blocking_inputs.append(iid)
    blockers = sorted(set(blocking_inputs))
    blockers.extend(f"unsupported_infrastructure_component:{tid}" for tid in sorted(set(unsupported_target_ids)))
    status = "blocked" if blockers else "ready"
    return {
        "status": status,
        "blocking_inputs": cast(JSONValue, sorted(set(blocking_inputs))),
        "provisioning_blockers": cast(JSONValue, blockers),
        "supported_environments": cast(JSONValue, list(_PROVISIONING_ENVIRONMENTS)),
        "default_environment": "staging",
        "default_backend": "terraform",
        "is_provisioning_ready": status == "ready",
    }


def _terraform_backend_config(repo_slug: str) -> dict[str, JSONValue]:
    return {
        "terraform": {
            "backend": {
                "s3": {
                    "bucket": "${var.terraform_state_bucket}",
                    "key": f"{repo_slug}/terraform.tfstate",
                    "region": "${var.aws_region}",
                    "dynamodb_table": "${var.terraform_state_lock_table}",
                    "encrypt": True,
                }
            }
        }
    }


def _terraform_provider_config(repo_slug: str) -> dict[str, JSONValue]:
    return {
        "terraform": {
            "required_version": ">= 1.6.0",
            "required_providers": {
                "aws": {
                    "source": "hashicorp/aws",
                    "version": "~> 5.0",
                }
            },
        },
        "provider": {
            "aws": {
                "region": "${var.aws_region}",
                "default_tags": {
                    "tags": {
                        "managed_by": "akc",
                        "repo_id": repo_slug,
                    }
                },
            }
        },
    }


def _terraform_variables_config() -> dict[str, JSONValue]:
    return {
        "variable": {
            "aws_region": {"type": "string"},
            "aws_account_id": {"type": "string"},
            "terraform_state_bucket": {"type": "string"},
            "terraform_state_lock_table": {"type": "string"},
            "route53_zone_name": {"type": "string", "default": ""},
            "cluster_destination": {"type": "string", "default": ""},
        }
    }


def _terraform_main_config(*, repo_id: str, resources: Sequence[Mapping[str, Any]]) -> dict[str, JSONValue]:
    repo_slug = _slug(repo_id)
    tf_resources: dict[str, dict[str, JSONValue]] = {
        "aws_vpc": {
            f"{repo_slug}_main": {
                "cidr_block": "10.0.0.0/16",
                "enable_dns_support": True,
                "enable_dns_hostnames": True,
            }
        }
    }
    for row in resources:
        kind = _as_str(row.get("resource_kind")) or "generic"
        rid = _as_str(row.get("resource_id")) or "generated"
        if kind == "ecr_repository":
            tf_resources.setdefault("aws_ecr_repository", {})[rid] = {"name": rid.replace("_", "-")}
        elif kind == "cloudwatch_log_group":
            tf_resources.setdefault("aws_cloudwatch_log_group", {})[rid] = {
                "name": f"/aws/akc/{rid.replace('_', '-')}",
                "retention_in_days": 14,
            }
        elif kind == "iam_role_irsa":
            tf_resources.setdefault("aws_iam_role", {})[rid] = {
                "name": rid.replace("_", "-"),
                "assume_role_policy": json.dumps(
                    {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Action": "sts:AssumeRoleWithWebIdentity",
                                "Effect": "Allow",
                                "Principal": {"Federated": "oidc-provider-placeholder"},
                            }
                        ],
                    }
                ),
            }
        elif kind == "secrets_manager_placeholder":
            tf_resources.setdefault("aws_secretsmanager_secret", {})[rid] = {"name": rid.replace("_", "-")}
        elif kind == "route53_record":
            tf_resources.setdefault("aws_route53_record", {})[rid] = {
                "zone_id": "${var.route53_zone_name}",
                "name": _as_str(row.get("domain")) or rid.replace("_", "-"),
                "type": "CNAME",
                "ttl": 300,
                "records": ["k8s-ingress-placeholder.internal"],
            }
        elif kind == "acm_certificate":
            tf_resources.setdefault("aws_acm_certificate", {})[rid] = {
                "domain_name": _as_str(row.get("domain")) or rid.replace("_", "-"),
                "validation_method": "DNS",
            }
        elif kind == "elasticache_redis":
            tf_resources.setdefault("aws_elasticache_replication_group", {})[rid] = {
                "replication_group_id": rid.replace("_", "-"),
                "engine": "redis",
                "node_type": "cache.t4g.small",
                "num_cache_clusters": 1,
            }
        elif kind == "rds_postgres":
            tf_resources.setdefault("aws_db_instance", {})[rid] = {
                "identifier": rid.replace("_", "-"),
                "engine": "postgres",
                "instance_class": "db.t4g.micro",
                "allocated_storage": 20,
                "username": "akc",
                "password": "replace-me",
                "skip_final_snapshot": True,
            }
        elif kind == "s3_bucket":
            tf_resources.setdefault("aws_s3_bucket", {})[rid] = {"bucket": rid.replace("_", "-")}
        elif kind == "sqs_queue":
            tf_resources.setdefault("aws_sqs_queue", {})[rid] = {"name": rid.replace("_", "-")}
        elif kind == "eks_cluster":
            tf_resources.setdefault("aws_eks_cluster", {})[rid] = {
                "name": rid.replace("_", "-"),
                "role_arn": "arn:aws:iam::${var.aws_account_id}:role/akc-eks-cluster",
                "vpc_config": {"subnet_ids": ["subnet-private-a", "subnet-private-b"]},
            }
        elif kind == "eks_managed_node_group":
            tf_resources.setdefault("aws_eks_node_group", {})[rid] = {
                "cluster_name": f"{repo_slug}-eks-cluster",
                "node_group_name": rid.replace("_", "-"),
                "node_role_arn": "arn:aws:iam::${var.aws_account_id}:role/akc-eks-nodegroup",
                "subnet_ids": ["subnet-private-a", "subnet-private-b"],
                "scaling_config": {"desired_size": 2, "min_size": 2, "max_size": 4},
            }
    return {"resource": cast(JSONValue, tf_resources)}


def _terraform_outputs_config(*, repo_id: str, resources: Sequence[Mapping[str, Any]]) -> dict[str, JSONValue]:
    repo_slug = _slug(repo_id)
    outputs: dict[str, JSONValue] = {
        "cluster_name": {"value": f"{repo_slug}-cluster"},
        "cluster_endpoint": {"value": "https://cluster-endpoint.example.internal"},
        "cluster_oidc_provider_arn": {"value": "arn:aws:iam::${var.aws_account_id}:oidc-provider/placeholder"},
    }
    for row in resources:
        rid = _as_str(row.get("resource_id")) or "generated"
        for output_name in cast(list[str], row.get("outputs") or []):
            outputs[output_name] = {"value": f"${{{rid}.{output_name}}}"}
    return {"output": outputs}


def _terraform_readme(*, repo_id: str) -> str:
    return "\n".join(
        [
            f"# AKC Terraform workspace for {repo_id}",
            "",
            "This workspace is generated by AKC infrastructure synthesis.",
            "Review variables and backend settings before running real provider commands.",
            "",
        ]
    )


def _cdk_package_json() -> dict[str, JSONValue]:
    return {
        "name": "akc-generated-iac",
        "private": True,
        "type": "module",
        "scripts": {
            "build": "tsc -p tsconfig.json",
            "synth": "cdk synth",
            "diff": "cdk diff",
            "deploy": "cdk deploy --require-approval never",
        },
        "dependencies": {
            "aws-cdk-lib": "^2.0.0",
            "constructs": "^10.0.0",
        },
        "devDependencies": {
            "typescript": "^5.0.0",
            "@types/node": "^20.0.0",
        },
    }


def _cdk_json() -> dict[str, JSONValue]:
    return {"app": "npx ts-node --esm bin/app.ts"}


def _tsconfig_json() -> dict[str, JSONValue]:
    return {
        "compilerOptions": {
            "target": "ES2022",
            "module": "NodeNext",
            "moduleResolution": "NodeNext",
            "strict": True,
            "esModuleInterop": True,
            "skipLibCheck": True,
            "outDir": "dist",
        },
        "include": ["bin/**/*.ts", "lib/**/*.ts"],
    }


def _cdk_bin_app(*, stack_id: str) -> str:
    return "\n".join(
        [
            "import * as cdk from 'aws-cdk-lib';",
            f"import {{ {stack_id} }} from '../lib/{_slug(stack_id)}.js';",
            "",
            "const app = new cdk.App();",
            f"new {stack_id}(app, '{stack_id}');",
            "",
        ]
    )


def _cdk_stack_source(
    *, repo_id: str, targets: Sequence[Mapping[str, Any]], resources: Sequence[Mapping[str, Any]]
) -> str:
    stack_id = f"{_pascal_identifier(repo_id)}InfraStack"
    lines = [
        "import * as cdk from 'aws-cdk-lib';",
        "import { Construct } from 'constructs';",
        "",
        f"export class {stack_id} extends cdk.Stack {{",
        "  constructor(scope: Construct, id: string, props?: cdk.StackProps) {",
        "    super(scope, id, props);",
        "",
        "    // Shared cluster outputs expected by AKC handoff surfaces.",
        "    new cdk.CfnOutput(this, 'cluster_name', { value: `${this.stackName}-cluster` });",
        "    new cdk.CfnOutput(this, 'cluster_endpoint', { value: 'https://cluster-endpoint.example.internal' });",
    ]
    for row in resources:
        for output_name in cast(list[str], row.get("outputs") or []):
            if output_name in {"cluster_name", "cluster_endpoint"}:
                continue
            lines.append(f"    new cdk.CfnOutput(this, '{output_name}', {{ value: '{output_name}-placeholder' }});")
    lines.extend(
        [
            "",
            "    // Target summary retained for operator review.",
            f"    new cdk.CfnOutput(this, 'repo_id', {{ value: '{repo_id}' }});",
            f"    new cdk.CfnOutput(this, 'target_count', {{ value: '{len(targets)}' }});",
            "  }",
            "}",
            "",
        ]
    )
    return "\n".join(lines)


def _infra_summary_markdown(
    *,
    run_id: str,
    infra_plan_obj: Mapping[str, Any],
    iac_manifest_obj: Mapping[str, Any],
) -> str:
    resources = infra_plan_obj.get("resources")
    readiness = infra_plan_obj.get("provisioning_readiness")
    lines = [
        "# Infrastructure summary",
        "",
        f"- **run_id:** `{run_id}`",
        f"- **cloud provider:** `{_as_str(infra_plan_obj.get('cloud_provider')) or 'aws'}`",
        f"- **preferred backend:** `{_as_str(iac_manifest_obj.get('preferred_backend')) or 'terraform'}`",
        "",
    ]
    if isinstance(readiness, Mapping):
        lines.append("## Provisioning readiness")
        lines.append("")
        lines.append(f"- **status:** `{_as_str(readiness.get('status')) or 'unknown'}`")
        blockers = readiness.get("provisioning_blockers")
        if isinstance(blockers, Sequence) and not isinstance(blockers, (str, bytes)):
            blocker_text = ", ".join(f"`{str(item).strip()}`" for item in blockers if str(item).strip())
            if blocker_text:
                lines.append(f"- **blockers:** {blocker_text}")
        lines.append("")
    lines.append("## Generated resources")
    lines.append("")
    if isinstance(resources, Sequence) and not isinstance(resources, (str, bytes)):
        for row in resources:
            if not isinstance(row, Mapping):
                continue
            rid = _as_str(row.get("resource_id")) or "generated"
            kind = _as_str(row.get("resource_kind")) or "unknown"
            lines.append(f"- `{rid}`: {kind}")
    lines.append("")
    lines.append("## Generated workspaces")
    lines.append("")
    workspaces = iac_manifest_obj.get("workspaces")
    if isinstance(workspaces, Mapping):
        for backend in sorted(workspaces):
            row = workspaces.get(backend)
            if not isinstance(row, Mapping):
                continue
            root = _as_str(row.get("workspace_root")) or ""
            lines.append(f"- **{backend}:** `{root}`")
    lines.append("")
    return "\n".join(lines)


def build_infrastructure_synthesis(
    *,
    run_id: str,
    ir_document: IRDocument,
    delivery_plan_obj: Mapping[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, Any], tuple[OutputArtifact, ...]]:
    targets = _delivery_targets(delivery_plan_obj)
    resources = _shared_resources(repo_id=ir_document.repo_id)
    target_resources, unsupported_targets = _target_resource_rows(targets=targets)
    resources.extend(target_resources)
    provisioning_readiness = _provisioning_readiness(
        delivery_plan_obj=delivery_plan_obj,
        unsupported_target_ids=unsupported_targets,
    )
    infra_plan_payload = {
        "run_id": run_id,
        "tenant_id": ir_document.tenant_id,
        "repo_id": ir_document.repo_id,
        "cloud_provider": "aws",
        "supported_iac_backends": list(VALID_IAC_BACKENDS),
        "preferred_iac_backend": "terraform",
        "provisioning_environments": list(_PROVISIONING_ENVIRONMENTS),
        "workload_targets": [
            {
                "target_id": row.get("target_id"),
                "name": row.get("name"),
                "target_class": row.get("target_class"),
                "node_kind": row.get("node_kind"),
                "public": _is_public_target(row),
                "domain": row.get("domain"),
                "depends_on": list(cast(list[Any], row.get("depends_on") or [])),
            }
            for row in targets
        ],
        "resources": resources,
        "unsupported_target_ids": sorted(set(unsupported_targets)),
        "provisioning_readiness": provisioning_readiness,
        "inputs_fingerprint": stable_json_fingerprint(
            {
                "ir": ir_document.to_json_obj(),
                "delivery_plan": dict(delivery_plan_obj) if isinstance(delivery_plan_obj, Mapping) else None,
            }
        ),
    }
    infra_plan_obj = apply_schema_envelope(obj=infra_plan_payload, kind="infra_plan", version=1)

    base_root = f".akc/infra/{run_id}"
    repo_slug = _slug(ir_document.repo_id)
    terraform_files = (
        f"{base_root}/terraform/backend.tf.json",
        f"{base_root}/terraform/providers.tf.json",
        f"{base_root}/terraform/variables.tf.json",
        f"{base_root}/terraform/main.tf.json",
        f"{base_root}/terraform/outputs.tf.json",
        f"{base_root}/terraform/README.md",
    )
    stack_id = f"{_pascal_identifier(ir_document.repo_id)}InfraStack"
    aws_cdk_files = (
        f"{base_root}/aws-cdk/package.json",
        f"{base_root}/aws-cdk/cdk.json",
        f"{base_root}/aws-cdk/tsconfig.json",
        f"{base_root}/aws-cdk/bin/app.ts",
        f"{base_root}/aws-cdk/lib/{_slug(stack_id)}.ts",
    )
    iac_manifest_payload = {
        "run_id": run_id,
        "tenant_id": ir_document.tenant_id,
        "repo_id": ir_document.repo_id,
        "cloud_provider": "aws",
        "supported_backends": list(VALID_IAC_BACKENDS),
        "preferred_backend": "terraform",
        "infra_plan_ref": {
            "path": f"{base_root}.infra_plan.json",
            "fingerprint": stable_json_fingerprint(infra_plan_obj),
        },
        "provisioning_readiness": provisioning_readiness,
        "environments": list(_PROVISIONING_ENVIRONMENTS),
        "workspaces": {
            "terraform": {
                "workspace_root": f"{base_root}/terraform",
                "entrypoint": "main.tf.json",
                "tool_bin_env_var": "AKC_TERRAFORM_BIN",
                "plan_command": ["terraform", "plan", "-refresh=false", "-lock=false", "-input=false"],
                "apply_command": ["terraform", "apply", "-auto-approve"],
                "files": list(terraform_files),
            },
            "aws_cdk": {
                "workspace_root": f"{base_root}/aws-cdk",
                "entrypoint": "bin/app.ts",
                "tool_bin_env_var": "AKC_CDK_BIN",
                "plan_command": ["cdk", "synth"],
                "apply_command": ["cdk", "deploy", "--require-approval", "never"],
                "files": list(aws_cdk_files),
            },
        },
        "output_contract": {
            "stable_outputs": [
                "cluster_name",
                "cluster_endpoint",
                "cluster_oidc_provider_arn",
                "repository_url",
                "redis_primary_endpoint",
                "postgres_endpoint",
                "bucket_name",
                "queue_url",
            ]
        },
    }
    iac_manifest_obj = apply_schema_envelope(obj=iac_manifest_payload, kind="iac_manifest", version=1)

    summary_text = _infra_summary_markdown(
        run_id=run_id,
        infra_plan_obj=infra_plan_obj,
        iac_manifest_obj=iac_manifest_obj,
    )
    artifacts = (
        OutputArtifact.from_json(
            path=f"{base_root}.infra_plan.json",
            obj=infra_plan_obj,
            metadata={"run_id": run_id, "kind": "infra_plan"},
        ),
        OutputArtifact.from_json(
            path=f"{base_root}.iac_manifest.json",
            obj=iac_manifest_obj,
            metadata={"run_id": run_id, "kind": "iac_manifest"},
        ),
        OutputArtifact.from_text(
            path=f".akc/design/{run_id}.infra_summary.md",
            text=summary_text,
            media_type="text/markdown; charset=utf-8",
            metadata={"run_id": run_id, "kind": "infra_summary_markdown"},
        ),
        OutputArtifact.from_json(
            path=terraform_files[0],
            obj=_terraform_backend_config(repo_slug),
            metadata={"run_id": run_id, "kind": "infra_terraform_backend"},
        ),
        OutputArtifact.from_json(
            path=terraform_files[1],
            obj=_terraform_provider_config(repo_slug),
            metadata={"run_id": run_id, "kind": "infra_terraform_provider"},
        ),
        OutputArtifact.from_json(
            path=terraform_files[2],
            obj=_terraform_variables_config(),
            metadata={"run_id": run_id, "kind": "infra_terraform_variables"},
        ),
        OutputArtifact.from_json(
            path=terraform_files[3],
            obj=_terraform_main_config(repo_id=ir_document.repo_id, resources=resources),
            metadata={"run_id": run_id, "kind": "infra_terraform_main"},
        ),
        OutputArtifact.from_json(
            path=terraform_files[4],
            obj=_terraform_outputs_config(repo_id=ir_document.repo_id, resources=resources),
            metadata={"run_id": run_id, "kind": "infra_terraform_outputs"},
        ),
        OutputArtifact.from_text(
            path=terraform_files[5],
            text=_terraform_readme(repo_id=ir_document.repo_id),
            media_type="text/markdown; charset=utf-8",
            metadata={"run_id": run_id, "kind": "infra_terraform_readme"},
        ),
        OutputArtifact.from_json(
            path=aws_cdk_files[0],
            obj=_cdk_package_json(),
            metadata={"run_id": run_id, "kind": "infra_aws_cdk_package_json"},
        ),
        OutputArtifact.from_json(
            path=aws_cdk_files[1], obj=_cdk_json(), metadata={"run_id": run_id, "kind": "infra_aws_cdk_cdk_json"}
        ),
        OutputArtifact.from_json(
            path=aws_cdk_files[2], obj=_tsconfig_json(), metadata={"run_id": run_id, "kind": "infra_aws_cdk_tsconfig"}
        ),
        OutputArtifact.from_text(
            path=aws_cdk_files[3],
            text=_cdk_bin_app(stack_id=stack_id),
            media_type="text/plain; charset=utf-8",
            metadata={"run_id": run_id, "kind": "infra_aws_cdk_bin"},
        ),
        OutputArtifact.from_text(
            path=aws_cdk_files[4],
            text=_cdk_stack_source(repo_id=ir_document.repo_id, targets=targets, resources=resources),
            media_type="text/plain; charset=utf-8",
            metadata={"run_id": run_id, "kind": "infra_aws_cdk_stack"},
        ),
    )
    return infra_plan_obj, iac_manifest_obj, artifacts
