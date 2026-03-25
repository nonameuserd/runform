from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any, Literal, cast

from akc.artifacts.contracts import apply_schema_envelope
from akc.ir import IRDocument, IRNode
from akc.memory.models import JSONValue, json_value_as_int
from akc.utils.fingerprint import stable_json_fingerprint

TargetClass = str
EnvironmentClass = str

_DEPLOYABLE_NODE_KINDS: frozenset[str] = frozenset({"service", "integration", "infrastructure", "agent", "workflow"})
_V1_ENVIRONMENTS: tuple[EnvironmentClass, EnvironmentClass, EnvironmentClass] = (
    "local",
    "staging",
    "production",
)


def _as_non_empty_str(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _as_bool(value: Any, *, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    return default


def _target_class_for_node(node: IRNode) -> TargetClass:
    n = node.name.lower()
    if "worker" in n or "queue" in n or "background" in n:
        return "worker"
    if "frontend" in n or "web" in n or "ui" in n:
        return "web_app"
    if "mobile" in n or "ios" in n or "android" in n:
        return "mobile_client"
    if node.kind == "integration":
        return "integration"
    if node.kind == "infrastructure":
        return "infrastructure_component"
    if node.kind in {"service", "agent"}:
        return "backend_service"
    return "backend_service"


def _supported_delivery_paths(*, target_class: TargetClass, environment: str) -> list[str]:
    if environment == "local":
        return ["direct_apply"]
    if environment == "staging":
        return ["direct_apply", "workflow_handoff"]
    if target_class == "mobile_client":
        return ["workflow_handoff"]
    return ["gitops_handoff", "workflow_handoff"]


def _environment_model() -> list[dict[str, JSONValue]]:
    return [
        {
            "environment": "local",
            "preferred_runtime": "docker_compose",
            "preferred_delivery_path": "direct_apply",
            "supported_delivery_paths": ["direct_apply"],
            "reconcile_mode": "direct",
            "approval_gates_required": False,
            "readiness_checks_required": False,
        },
        {
            "environment": "staging",
            "preferred_runtime": "kubernetes_or_compose",
            "preferred_delivery_path": "direct_apply",
            "supported_delivery_paths": ["direct_apply", "workflow_handoff"],
            "reconcile_mode": "direct",
            "approval_gates_required": False,
            "readiness_checks_required": True,
        },
        {
            "environment": "production",
            "preferred_runtime": "kubernetes",
            "preferred_delivery_path": "workflow_handoff",
            "supported_delivery_paths": ["gitops_handoff", "workflow_handoff"],
            "gitops_compatible_handoff": True,
            "reconcile_mode": "workflow_or_gitops",
            "approval_gates_required": True,
            "readiness_checks_required": True,
        },
    ]


def _build_target_contract(*, node: IRNode, target_class: TargetClass) -> dict[str, JSONValue]:
    props = node.properties
    effects = node.effects.to_json_obj() if node.effects is not None else {}
    container_image = _as_non_empty_str(props.get("image")) or "ghcr.io/example/akc-app:latest"
    port_raw = props.get("port")
    service_port = int(port_raw) if isinstance(port_raw, int) else 8080
    public = bool(props.get("public", target_class in {"web_app", "backend_service"}))
    cpu_request = _as_non_empty_str(props.get("cpu_request")) or "250m"
    mem_request = _as_non_empty_str(props.get("memory_request")) or "256Mi"
    cpu_limit = _as_non_empty_str(props.get("cpu_limit")) or "1000m"
    mem_limit = _as_non_empty_str(props.get("memory_limit")) or "1Gi"
    health_path = _as_non_empty_str(props.get("health_path")) or "/healthz"
    replicas_raw = props.get("replicas")
    replicas = int(replicas_raw) if isinstance(replicas_raw, int) else 1
    startup_raw = props.get("startup_seconds")
    startup_seconds = int(startup_raw) if isinstance(startup_raw, int) else 30
    health_known = _as_bool(props.get("health_endpoint_known"), default=True)
    env_keys: list[str] = []
    raw_env = props.get("env")
    if isinstance(raw_env, Sequence) and not isinstance(raw_env, (str, bytes)):
        env_keys = sorted(str(v) for v in raw_env if str(v).strip())
    secret_keys: list[str] = []
    raw_secrets = props.get("secrets")
    if isinstance(raw_secrets, Sequence) and not isinstance(raw_secrets, (str, bytes)):
        secret_keys = sorted(str(v) for v in raw_secrets if str(v).strip())

    logging_enabled = _as_bool(props.get("logging_enabled"), default=True)
    tracing_enabled = _as_bool(props.get("tracing_enabled"), default=True)
    metrics_enabled = _as_bool(props.get("metrics_enabled"), default=True)

    return {
        "target_class": target_class,
        "build_contract": {
            "artifact_type": "container_image",
            "image": container_image,
            "build_context": ".",
        },
        "runtime_contract": {
            "runtime": "container",
            "port": service_port,
            "network_effect_required": bool(effects.get("network", False)),
        },
        "exposure_model": {
            "public": public,
            "port": service_port,
            "transport": "http",
        },
        "config_secrets_contract": {
            "required_env": cast(JSONValue, env_keys),
            "required_secrets": cast(JSONValue, secret_keys),
            "secret_injection_mode": "platform_secret_store",
        },
        "health_contract": {
            "readiness_path": health_path,
            "liveness_path": health_path,
            "startup_path": health_path,
            "expected_startup_seconds": startup_seconds,
            "health_endpoint_known": health_known,
        },
        "scaling_resources": {
            "replicas": max(1, replicas),
            "requests": {"cpu": cpu_request, "memory": mem_request},
            "limits": {"cpu": cpu_limit, "memory": mem_limit},
        },
        "observability_contract": {
            "logs": {"enabled": logging_enabled, "structured": True},
            "metrics": {"enabled": metrics_enabled, "port": service_port},
            "tracing": {"enabled": tracing_enabled},
        },
        "rollout_recovery_policy": {
            "strategy": "rolling",
            "auto_rollback_on_health_failure": True,
        },
        "operational_config": {
            "probes": {
                "known_endpoint": health_known,
                "readiness": {"path": health_path, "port": service_port, "period_seconds": 10},
                "liveness": {"path": health_path, "port": service_port, "period_seconds": 15},
                "startup": {"path": health_path, "port": service_port, "failure_threshold": 30},
            },
            "restart": {"compose_policy": "unless-stopped", "kubernetes_policy": "Always"},
            "resources": {
                "requests": {"cpu": cpu_request, "memory": mem_request},
                "limits": {"cpu": cpu_limit, "memory": mem_limit},
                "replicas": max(1, replicas),
            },
            "security_context": {
                "pod": {"runAsNonRoot": True},
                "container": {
                    "allowPrivilegeEscalation": False,
                    "readOnlyRootFilesystem": True,
                    "capabilities": {"drop": ["ALL"]},
                },
            },
            "observability_toggles": {
                "logging_enabled": logging_enabled,
                "tracing_enabled": tracing_enabled,
                "metrics_enabled": metrics_enabled,
            },
            "environment_variables": cast(JSONValue, env_keys),
            "secrets_placeholders": cast(
                JSONValue, [f"{key}=<set-in-secret-store>" for key in secret_keys]
            ),
            "alert_health_expectations": {
                "staging": "No sustained readiness failures for 10m before production gate.",
                "production": "Page on liveness failures > 3 in 5m and latency/SLO breaches.",
            },
        },
    }


def _collect_required_human_inputs(*, targets: Sequence[Mapping[str, Any]]) -> list[dict[str, JSONValue]]:
    """Return only **missing** inputs so a UI can render this list as the question queue.

    Each entry includes `ask_order` (stable wizard ordering), `ui_prompt` (plain-language), and
    `answer_binding` (where compile/IR expects the value). Only entries with status ``missing`` are
    emitted — re-compile stays authoritative; there is no parallel product-side planning model.
    """
    domain_for: list[str] = []
    cloud_for: list[str] = []
    health_path_for: list[str] = []
    secrets_unacked_for: list[str] = []
    secret_names_by_target: dict[str, list[str]] = {}
    env_unacked_for: list[str] = []
    env_keys_by_target: dict[str, list[str]] = {}
    app_store_for: list[str] = []

    for target in targets:
        tid = str(target.get("target_id", "")).strip()
        if not tid:
            continue
        exposure = target.get("exposure_model")
        if isinstance(exposure, Mapping) and bool(exposure.get("public")):
            domain = target.get("domain")
            if not isinstance(domain, str) or not domain.strip():
                domain_for.append(tid)
        ca = target.get("cloud_account")
        if not isinstance(ca, str) or not ca.strip():
            cloud_for.append(tid)
        hc = target.get("health_contract")
        if isinstance(hc, Mapping) and hc.get("health_endpoint_known") is False:
            health_path_for.append(tid)
        csc = target.get("config_secrets_contract")
        if isinstance(csc, Mapping):
            raw_sec = csc.get("required_secrets")
            if isinstance(raw_sec, Sequence) and not isinstance(raw_sec, (str, bytes)):
                names = sorted({str(x).strip() for x in raw_sec if str(x).strip()})
                if names:
                    secret_names_by_target[tid] = names
                    if not _as_bool(target.get("secrets_provisioned_in_store"), default=False):
                        secrets_unacked_for.append(tid)
            raw_env = csc.get("required_env")
            if isinstance(raw_env, Sequence) and not isinstance(raw_env, (str, bytes)):
                ekeys = sorted({str(x).strip() for x in raw_env if str(x).strip()})
                if ekeys:
                    env_keys_by_target[tid] = ekeys
                    if not _as_bool(target.get("env_config_provisioned"), default=False):
                        env_unacked_for.append(tid)
        if str(target.get("target_class")) == "mobile_client":
            acct = target.get("app_store_account")
            if not isinstance(acct, str) or not acct.strip():
                app_store_for.append(tid)

    reqs: list[dict[str, JSONValue]] = []
    if domain_for:
        reqs.append(
            cast(
                dict[str, JSONValue],
                {
                    "id": "domain_name",
                    "status": "missing",
                    "ask_order": 10,
                    "blocking_for": ["production"],
                    "reason": "Public targets require a DNS domain before production exposure.",
                    "ui_prompt": {
                        "audience": "non_technical",
                        "title": "Public web address",
                        "question": ("What DNS hostname should people use to reach the public part of this system?"),
                        "help_text": (
                            "You can enter a hostname such as app.example.com. "
                            "AKC stores this on the affected components in the compile model "
                            "(not a separate plan file)."
                        ),
                        "value_kind": "hostname",
                        "sensitive": False,
                    },
                    "answer_binding": {
                        "kind": "ir_node_property",
                        "property": "domain",
                        "scope": "listed_targets",
                        "target_ids": sorted(set(domain_for)),
                    },
                },
            )
        )
    if cloud_for:
        reqs.append(
            cast(
                dict[str, JSONValue],
                {
                    "id": "cloud_credentials",
                    "status": "missing",
                    "ask_order": 20,
                    "blocking_for": ["staging", "production"],
                    "reason": "A deploy-capable cloud account identifier is required before remote deploy.",
                    "ui_prompt": {
                        "audience": "non_technical",
                        "title": "Where should this run in the cloud?",
                        "question": (
                            "Which cloud account, subscription, or project should deployments use for staging "
                            "and production?"
                        ),
                        "help_text": (
                            "Provide the identifier your organization uses (account ID, subscription ID, "
                            "or project ref). Platform-specific credentials stay in your secret store; "
                            "this value anchors the compile model."
                        ),
                        "value_kind": "account_identifier",
                        "sensitive": False,
                    },
                    "answer_binding": {
                        "kind": "ir_node_property",
                        "property": "cloud_account",
                        "scope": "listed_targets",
                        "target_ids": sorted(set(cloud_for)),
                    },
                },
            )
        )
    if health_path_for:
        reqs.append(
            cast(
                dict[str, JSONValue],
                {
                    "id": "health_check_url",
                    "status": "missing",
                    "ask_order": 30,
                    "blocking_for": ["staging", "production"],
                    "reason": "Health checks need a real HTTP path before staging/production gates can succeed.",
                    "ui_prompt": {
                        "audience": "non_technical",
                        "title": "Health check address",
                        "question": (
                            "What URL path should AKC use to verify this component is running (for example "
                            "ready for traffic)?"
                        ),
                        "help_text": (
                            "Enter a path such as /healthz or /ready. This updates the component record in the "
                            'compile model so probes and "healthy" checks match your app.'
                        ),
                        "value_kind": "url_path",
                        "sensitive": False,
                    },
                    "answer_binding": {
                        "kind": "ir_node_property",
                        "property": "health_path",
                        "scope": "listed_targets",
                        "target_ids": sorted(set(health_path_for)),
                    },
                },
            )
        )
    if secrets_unacked_for:
        reqs.append(
            cast(
                dict[str, JSONValue],
                {
                    "id": "configuration_secrets",
                    "status": "missing",
                    "ask_order": 35,
                    "blocking_for": ["staging", "production"],
                    "reason": "Declared secret keys must exist in the platform secret store before remote deploy.",
                    "context": {
                        "secret_requirements_by_target": {
                            tid: list(secret_names_by_target.get(tid, [])) for tid in sorted(set(secrets_unacked_for))
                        },
                    },
                    "ui_prompt": {
                        "audience": "non_technical",
                        "title": "Secrets in your organization's vault",
                        "question": (
                            "These components list secret names that must be created in your secret store "
                            "(for example cloud vault or CI secrets) before staging or production deploy. "
                            "When that is done for each component, confirm below."
                        ),
                        "help_text": (
                            "Secret values stay in your vault — AKC only needs the key names and a per-component "
                            "confirmation (IR property **secrets_provisioned_in_store**) after operators "
                            "have created matching entries in your secret store."
                        ),
                        "value_kind": "acknowledge_secrets_provisioned",
                        "sensitive": False,
                    },
                    "answer_binding": {
                        "kind": "ir_node_property",
                        "property": "secrets_provisioned_in_store",
                        "scope": "listed_targets",
                        "target_ids": sorted(set(secrets_unacked_for)),
                    },
                },
            )
        )
    if env_unacked_for:
        reqs.append(
            cast(
                dict[str, JSONValue],
                {
                    "id": "configuration_env",
                    "status": "missing",
                    "ask_order": 36,
                    "blocking_for": ["staging", "production"],
                    "reason": "Declared non-secret environment keys need platform configuration before remote deploy.",
                    "context": {
                        "env_requirements_by_target": {
                            tid: list(env_keys_by_target.get(tid, [])) for tid in sorted(set(env_unacked_for))
                        },
                    },
                    "ui_prompt": {
                        "audience": "non_technical",
                        "title": "Non-secret configuration (environment variables)",
                        "question": (
                            "These components expect configuration keys (not treated as secrets). They should "
                            "exist in your deployment platform — for example config maps, parameter store, or "
                            "workflow environment definitions — before staging or production."
                        ),
                        "help_text": (
                            "AKC lists **names only** here. After operators have set the corresponding values "
                            "in the right place for each environment, confirm with IR property "
                            "**env_config_provisioned** on each affected component."
                        ),
                        "value_kind": "acknowledge_env_config_provisioned",
                        "sensitive": False,
                    },
                    "answer_binding": {
                        "kind": "ir_node_property",
                        "property": "env_config_provisioned",
                        "scope": "listed_targets",
                        "target_ids": sorted(set(env_unacked_for)),
                    },
                },
            )
        )
    if app_store_for:
        reqs.append(
            cast(
                dict[str, JSONValue],
                {
                    "id": "app_store_account",
                    "status": "missing",
                    "ask_order": 40,
                    "blocking_for": ["production"],
                    "reason": "Mobile targets require store enrollment before production release.",
                    "ui_prompt": {
                        "audience": "non_technical",
                        "title": "App store presence",
                        "question": ("Which Apple/Google developer or store enrollment should mobile releases use?"),
                        "help_text": (
                            "Mobile packaging is modeled-only in this step; capturing the account ties future "
                            "store workflows to your org."
                        ),
                        "value_kind": "store_account",
                        "sensitive": False,
                    },
                    "answer_binding": {
                        "kind": "ir_node_property",
                        "property": "app_store_account",
                        "scope": "listed_targets",
                        "target_ids": sorted(set(app_store_for)),
                    },
                },
            )
        )
    return sorted(
        reqs,
        key=lambda r: (json_value_as_int(r.get("ask_order"), default=99), str(r.get("id", ""))),
    )


def build_delivery_plan(
    *,
    run_id: str,
    ir_document: IRDocument,
    intent_obj: Mapping[str, Any],
    orchestration_obj: Mapping[str, Any],
    coordination_obj: Mapping[str, Any],
) -> dict[str, JSONValue]:
    targets: list[dict[str, JSONValue]] = []
    for node in sorted(ir_document.nodes, key=lambda n: n.id):
        if node.kind not in _DEPLOYABLE_NODE_KINDS:
            continue
        target_class = _target_class_for_node(node)
        projected = _build_target_contract(node=node, target_class=target_class)
        target: dict[str, JSONValue] = {
            "target_id": node.id,
            "node_kind": node.kind,
            "name": node.name,
            "depends_on": list(node.depends_on),
            **projected,
            "supported_delivery_paths": cast(
                JSONValue,
                {
                    "local": _supported_delivery_paths(target_class=target_class, environment="local"),
                    "staging": _supported_delivery_paths(target_class=target_class, environment="staging"),
                    "production": _supported_delivery_paths(target_class=target_class, environment="production"),
                },
            ),
            "domain": node.properties.get("domain"),
            "cloud_account": node.properties.get("cloud_account"),
            "app_store_account": node.properties.get("app_store_account"),
            "secrets_provisioned_in_store": _as_bool(
                node.properties.get("secrets_provisioned_in_store"), default=False
            ),
            "env_config_provisioned": _as_bool(node.properties.get("env_config_provisioned"), default=False),
        }
        targets.append(target)

    required_human_inputs = _collect_required_human_inputs(targets=targets)
    env_model_rows = _environment_model()
    prod_row = next((r for r in env_model_rows if str(r.get("environment")) == "production"), {})
    production_manual_approval_required = bool(prod_row.get("approval_gates_required"))
    promotion_blockers: list[str] = [str(item["id"]) for item in required_human_inputs]
    if production_manual_approval_required:
        promotion_blockers.append("production_manual_approval_gate")
    promotion_readiness_status: Literal["ready", "blocked"] = "blocked" if promotion_blockers else "ready"
    promotion_ready = promotion_readiness_status == "ready"
    delivery_paths = {
        "local": ["direct_apply"],
        "staging": ["direct_apply", "workflow_handoff"],
        "production": ["gitops_handoff", "workflow_handoff"],
    }
    out: dict[str, JSONValue] = {
        "run_id": run_id,
        "tenant_id": ir_document.tenant_id,
        "repo_id": ir_document.repo_id,
        "inputs_fingerprint": stable_json_fingerprint(
            {
                "ir": ir_document.to_json_obj(),
                "intent": dict(intent_obj),
                "orchestration": dict(orchestration_obj),
                "coordination": dict(coordination_obj),
            }
        ),
        "targets": cast(JSONValue, targets),
        "environments": cast(JSONValue, list(_V1_ENVIRONMENTS)),
        "environment_model": cast(JSONValue, list(env_model_rows)),
        "delivery_paths": cast(JSONValue, delivery_paths),
        "operational_profiles": cast(
            JSONValue, {"default": {"rollout_strategy": "rolling", "health_required": True}}
        ),
        "required_human_inputs": cast(JSONValue, required_human_inputs),
        "promotion_readiness": cast(
            JSONValue,
            {
                "status": promotion_readiness_status,
                "blocking_inputs": [item["id"] for item in required_human_inputs],
                "promotion_blockers": promotion_blockers,
                "production_manual_approval_required": production_manual_approval_required,
                "is_promotion_ready": promotion_ready,
                "default_promotion_environment": "production",
            },
        ),
    }
    apply_schema_envelope(obj=out, kind="delivery_plan", version=1)
    return out


def render_delivery_summary_markdown(*, run_id: str, delivery_plan: Mapping[str, Any]) -> str:
    """Plain-language companion for operators; authoritative contract remains `delivery_plan` JSON."""

    def _s(val: Any) -> str:
        if val is None:
            return ""
        return str(val).strip()

    lines: list[str] = [
        "# Delivery summary",
        "",
        "This page is a **human-readable companion** to the compile-time delivery plan. The authoritative",
        f"contract is `.akc/deployment/{run_id}.delivery_plan.json` (and the runtime bundle that references it).",
        "",
        f"- **run_id:** `{run_id}`",
        f"- **tenant_id:** `{_s(delivery_plan.get('tenant_id'))}`",
        f"- **repo_id:** `{_s(delivery_plan.get('repo_id'))}`",
        "",
    ]

    pr = delivery_plan.get("promotion_readiness")
    if isinstance(pr, Mapping):
        st = _s(pr.get("status")) or "unknown"
        lines.append("## Promotion readiness")
        lines.append("")
        blockers = pr.get("promotion_blockers")
        blocker_ids: list[str] = []
        if isinstance(blockers, Sequence) and not isinstance(blockers, (str, bytes)):
            blocker_ids = sorted({_s(x) for x in blockers if _s(x)})
        if st == "ready":
            lines.append(
                "- AKC considers this compile **ready for promotion** to the default target environment "
                f"(`{_s(pr.get('default_promotion_environment')) or 'production'}`): compile-time gates and the "
                "production approval model are satisfied for this projection."
            )
        else:
            lines.append(
                "- Promotion is **fail-closed** at compile time: blocking human inputs and/or production "
                "approval gates must be cleared before treating the handoff as promotion-ready."
            )
            bi = pr.get("blocking_inputs")
            if isinstance(bi, Sequence) and not isinstance(bi, (str, bytes)):
                ids = sorted({_s(x) for x in bi if _s(x)})
                if ids:
                    lines.append(f"- **Blocking human-input ids:** {', '.join(f'`{i}`' for i in ids)}")
            if blocker_ids:
                lines.append(
                    f"- **All promotion blockers (inputs + gates):** {', '.join(f'`{i}`' for i in blocker_ids)}"
                )
            if bool(pr.get("production_manual_approval_required")):
                lines.append(
                    "- **Production manual approval:** the environment model requires reviewer approval (for example "
                    "a GitHub Environment) before production deploy jobs — that gate is reflected in "
                    "`promotion_blockers` until your workflow records completion."
                )
        lines.append("")

    raw_targets = delivery_plan.get("targets")
    targets: list[Mapping[str, Any]] = []
    if isinstance(raw_targets, Sequence) and not isinstance(raw_targets, (str, bytes)):
        for t in raw_targets:
            if isinstance(t, Mapping):
                targets.append(t)

    lines.append("## What AKC inferred")
    lines.append("")
    if not targets:
        lines.append("- No deployable targets were projected from the current IR.")
    else:
        for t in sorted(targets, key=lambda x: _s(x.get("target_id"))):
            tid = _s(t.get("target_id")) or "(unknown)"
            nm = _s(t.get("name")) or tid
            tclass = _s(t.get("target_class")) or "unknown"
            nk = _s(t.get("node_kind")) or "unknown"
            exp = t.get("exposure_model")
            pub = bool(isinstance(exp, Mapping) and exp.get("public"))
            roll = t.get("rollout_recovery_policy")
            strat = _s(roll.get("strategy")) if isinstance(roll, Mapping) else "rolling"
            bc = t.get("build_contract")
            img = _s(bc.get("image")) if isinstance(bc, Mapping) else ""
            lines.append(
                f"- **`{nm}`** (`{tid}`, {nk} → _{tclass}_) — public exposure: **{'yes' if pub else 'no'}**, "
                f"rollout: _{strat}_."
            )
            if img:
                lines.append(f"  - Container image reference (may be placeholder): `{img}`")
    lines.append("")

    env_rows = delivery_plan.get("environment_model")
    lines.append("## What will be deployed where")
    lines.append("")
    if isinstance(env_rows, Sequence) and not isinstance(env_rows, (str, bytes)):
        for row in env_rows:
            if not isinstance(row, Mapping):
                continue
            env = _s(row.get("environment"))
            pref_rt = _s(row.get("preferred_runtime")).replace("_", " ")
            pref_dp = _s(row.get("preferred_delivery_path")).replace("_", " ")
            approvals = bool(row.get("approval_gates_required"))
            readiness = bool(row.get("readiness_checks_required"))
            lines.append(f"### {env or 'environment'}")
            lines.append("")
            lines.append(f"- **Preferred runtime:** {pref_rt or '—'} — **default delivery path:** {pref_dp or '—'}.")
            lines.append(
                f"- **Human approvals:** {'required before changes apply' if approvals else 'not required by model'}."
            )
            lines.append(
                f"- **Readiness checks:** {'expected after deploy' if readiness else 'relaxed in this environment'}."
            )
            sdp = row.get("supported_delivery_paths")
            if isinstance(sdp, Sequence) and not isinstance(sdp, (str, bytes)):
                opts = ", ".join(_s(p).replace("_", " ") for p in sdp if _s(p))
                if opts:
                    lines.append(f"- **Supported delivery paths:** {opts}.")
            lines.append("")
    else:
        lines.append("_Environment model not present on this plan._")
        lines.append("")

    lines.append("Per-component paths by environment:")
    lines.append("")
    if not targets:
        lines.append("- _(none)_")
    else:
        for t in sorted(targets, key=lambda x: _s(x.get("target_id"))):
            nm = _s(t.get("name")) or _s(t.get("target_id"))
            sdp = t.get("supported_delivery_paths")
            if not isinstance(sdp, Mapping):
                lines.append(f"- **{nm}:** _(no path matrix)_")
                continue
            parts: list[str] = []
            for ek in ("local", "staging", "production"):
                pv = sdp.get(ek)
                if isinstance(pv, Sequence) and not isinstance(pv, (str, bytes)):
                    paths = ", ".join(_s(p).replace("_", " ") for p in pv if _s(p))
                    if paths:
                        parts.append(f"{ek}: {paths}")
            lines.append(f"- **{nm}:** " + ("; ".join(parts) if parts else "—"))
    lines.append("")

    lines.append("## Approvals and gates")
    lines.append("")
    prod_approval = False
    if isinstance(env_rows, Sequence) and not isinstance(env_rows, (str, bytes)):
        for row in env_rows:
            if isinstance(row, Mapping) and _s(row.get("environment")) == "production":
                prod_approval = bool(row.get("approval_gates_required"))
    if prod_approval:
        lines.append(
            "- **Production:** the model expects **manual approval** (for example GitHub Environment reviewers) "
            "before production rollout when using workflow handoff."
        )
    else:
        lines.append("- **Production approvals:** not flagged as required by the environment model on this plan.")
    lines.append(
        "- **Staging:** typically fast feedback with direct apply; human approval gates are not required by default."
    )
    lines.append("- **Local:** direct apply on a developer machine or local engine; no production-style approval gate.")
    lines.append("")

    reqs = delivery_plan.get("required_human_inputs")
    req_items: list[Mapping[str, Any]] = []
    if isinstance(reqs, Sequence) and not isinstance(reqs, (str, bytes)):
        req_items = [x for x in reqs if isinstance(x, Mapping)]
    lines.append("## Information we still need from you")
    lines.append("")
    lines.append(
        "_Only **missing** items appear in `required_human_inputs` (sorted by `ask_order`). "
        "Use `ui_prompt` for copy and `answer_binding` to know where answers belong in the compile model "
        "— not as separate product plans._"
    )
    lines.append("")
    if not req_items:
        lines.append(
            "- **No missing human-input rows** in `required_human_inputs` for this compile — see *Promotion "
            "readiness* above for production approval gates and other compile-time blockers."
        )
    else:
        for item in req_items:
            uid = _s(item.get("id"))
            blocks = item.get("blocking_for")
            bf = ""
            if isinstance(blocks, Sequence) and not isinstance(blocks, (str, bytes)):
                bf = ", ".join(_s(b) for b in blocks if _s(b))
            raw_ui = item.get("ui_prompt")
            ui = raw_ui if isinstance(raw_ui, Mapping) else {}
            title = _s(ui.get("title"))
            question = _s(ui.get("question"))
            head = f"**{title}** (`{uid}`)" if title else f"**`{uid}`**"
            lines.append(f"- {head}")
            if bf:
                lines.append(f"  - Blocks: {bf}")
            if question:
                lines.append(f"  - {question}")
            bind = item.get("answer_binding")
            if isinstance(bind, Mapping):
                prop = _s(bind.get("property"))
                tids = bind.get("target_ids")
                if isinstance(tids, Sequence) and not isinstance(tids, (str, bytes)):
                    tlist = ", ".join(f"`{_s(x)}`" for x in tids if _s(x))
                    if prop and tlist:
                        lines.append(f"  - _Answer lands in IR property `{prop}` on:_ {tlist}")
            ctx = item.get("context")
            if isinstance(ctx, Mapping):
                sbt = ctx.get("secret_requirements_by_target")
                if isinstance(sbt, Mapping) and sbt:
                    lines.append("  - **Secret names** (create in your org secret store; AKC only tracks names + ack):")
                    for comp_id in sorted(sbt.keys(), key=str):
                        raw_names = sbt.get(comp_id)
                        if isinstance(raw_names, Sequence) and not isinstance(raw_names, (str, bytes)):
                            labels = ", ".join(f"`{_s(n)}`" for n in raw_names if _s(n))
                        else:
                            labels = ""
                        disp = next(
                            (_s(t.get("name")) for t in targets if _s(t.get("target_id")) == _s(comp_id)),
                            _s(comp_id),
                        )
                        lines.append(f"    - **{disp}** (`{_s(comp_id)}`): {labels or '—'}")
                ebt = ctx.get("env_requirements_by_target")
                if isinstance(ebt, Mapping) and ebt:
                    lines.append("  - **Non-secret env keys** (set values in your platform; AKC tracks names + ack):")
                    for comp_id in sorted(ebt.keys(), key=str):
                        raw_keys = ebt.get(comp_id)
                        if isinstance(raw_keys, Sequence) and not isinstance(raw_keys, (str, bytes)):
                            labels = ", ".join(f"`{_s(n)}`" for n in raw_keys if _s(n))
                        else:
                            labels = ""
                        disp = next(
                            (_s(t.get("name")) for t in targets if _s(t.get("target_id")) == _s(comp_id)),
                            _s(comp_id),
                        )
                        lines.append(f"    - **{disp}** (`{_s(comp_id)}`): {labels or '—'}")
            lines.append("")

    lines.append("## What healthy means for this system")
    lines.append("")
    if not targets:
        lines.append("- Define health endpoints in the IR before AKC can describe probes and checks here.")
    else:
        lines.append(
            "AKC treats a service as **healthy** when orchestration checks succeed on the paths below, "
            "within the startup window, and when observability expectations are met."
        )
        lines.append("")
        shared_expect: dict[str, str] = {}
        for t in sorted(targets, key=lambda x: _s(x.get("target_id"))):
            oc = t.get("operational_config")
            if isinstance(oc, Mapping):
                ahe = oc.get("alert_health_expectations")
                if isinstance(ahe, Mapping):
                    for k, v in ahe.items():
                        ks = _s(k)
                        if ks and _s(v):
                            shared_expect[ks] = _s(v)
                    break
        for t in sorted(targets, key=lambda x: _s(x.get("target_id"))):
            nm = _s(t.get("name")) or _s(t.get("target_id"))
            hc = t.get("health_contract")
            if isinstance(hc, Mapping):
                rp = _s(hc.get("readiness_path"))
                lp = _s(hc.get("liveness_path"))
                su = hc.get("expected_startup_seconds")
                known = hc.get("health_endpoint_known")
                known_s = "known" if known is not False else "unspecified / inferred default"
                lines.append(f"### `{nm}`")
                lines.append("")
                lines.append(
                    f"- **Readiness:** HTTP GET `{rp or '/healthz'}` responds success; **liveness:** "
                    f"`{lp or rp or '/healthz'}`; **startup budget:** ~{su if isinstance(su, int) else 30}s; "
                    f"endpoint certainty: _{known_s}_."
                )
                lines.append("")
        if shared_expect:
            lines.append("### Cross-cutting operator expectations")
            lines.append("")
            for k in sorted(shared_expect):
                lines.append(f"- **{k}:** {shared_expect[k]}")
            lines.append("")
    lines.append("---")
    lines.append("")
    lines.append(
        "_Generated by AKC `delivery_plan` projection. Re-compile after supplying answers so "
        "`required_human_inputs` shrinks._"
    )
    lines.append("")
    return "\n".join(lines)


def parse_json_artifact_text(text: str) -> dict[str, Any]:
    obj = json.loads(text)
    if not isinstance(obj, dict):
        raise ValueError("artifact text must decode to a JSON object")
    return obj
