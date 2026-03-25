from __future__ import annotations

import argparse
import importlib
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Literal

from akc.compile.interfaces import LLMBackend
from akc.living.automation_profile import resolve_living_automation_profile
from akc.living.dispatch import living_recompile_execute
from akc.living.webhook_receiver import LivingWebhookServerConfig, run_living_webhook_server

from .common import configure_logging
from .profile_defaults import resolve_developer_role_profile, resolve_optional_project_string
from .project_config import load_akc_project_config


def _parse_policy_mode(raw: object) -> Literal["audit_only", "enforce"]:
    v = str(raw or "enforce").strip()
    if v == "audit_only":
        return "audit_only"
    if v == "enforce":
        return "enforce"
    raise SystemExit(f"invalid --policy-mode: {raw!r} (expected audit_only|enforce)")


def _parse_canary_accept_mode(raw: object) -> Literal["quick", "thorough"]:
    v = str(raw or "").strip()
    if v == "quick":
        return "quick"
    if v == "thorough":
        return "thorough"
    raise SystemExit(f"invalid mode: {raw!r} (expected quick|thorough)")


def _parse_canary_test_mode(raw: object) -> Literal["smoke", "full"]:
    v = str(raw or "").strip()
    if v == "smoke":
        return "smoke"
    if v == "full":
        return "full"
    raise SystemExit(f"invalid --canary-test-mode: {raw!r} (expected smoke|full)")


def _load_llm_backend_class(*, class_path: str) -> LLMBackend:
    raw = str(class_path).strip()
    if not raw:
        raise ValueError("llm backend class path must be non-empty")
    if ":" in raw:
        module_name, class_name = raw.split(":", 1)
    else:
        module_name, _, class_name = raw.rpartition(".")
    module_name = module_name.strip()
    class_name = class_name.strip()
    if not module_name or not class_name:
        raise ValueError("invalid llm backend class path; expected '<module>:<Class>' or '<module>.<Class>'")
    mod = importlib.import_module(module_name)
    cls = getattr(mod, class_name, None)
    if cls is None:
        raise ValueError(f"llm backend class not found: {raw}")
    inst = cls()
    if not isinstance(inst, LLMBackend):
        raise ValueError(f"llm backend does not implement LLMBackend: {raw}")
    return inst


def cmd_living_recompile(args: argparse.Namespace) -> int:
    configure_logging(verbose=bool(getattr(args, "verbose", False)))
    cwd = Path.cwd()
    proj = load_akc_project_config(cwd)
    tenant_r = resolve_optional_project_string(
        cli_value=getattr(args, "tenant_id", None),
        env_key="AKC_TENANT_ID",
        file_value=proj.tenant_id if proj is not None else None,
        env=os.environ,
    )
    repo_r = resolve_optional_project_string(
        cli_value=getattr(args, "repo_id", None),
        env_key="AKC_REPO_ID",
        file_value=proj.repo_id if proj is not None else None,
        env=os.environ,
    )
    outputs_r = resolve_optional_project_string(
        cli_value=getattr(args, "outputs_root", None),
        env_key="AKC_OUTPUTS_ROOT",
        file_value=proj.outputs_root if proj is not None else None,
        env=os.environ,
    )
    if tenant_r.value is None:
        raise SystemExit(
            "Missing tenant id: provide --tenant-id, set AKC_TENANT_ID, or add tenant_id to .akc/project.json"
        )
    if repo_r.value is None:
        raise SystemExit("Missing repo id: provide --repo-id, set AKC_REPO_ID, or add repo_id to .akc/project.json")
    if outputs_r.value is None:
        raise SystemExit(
            "Missing outputs root: provide --outputs-root, set AKC_OUTPUTS_ROOT, "
            "or add outputs_root to .akc/project.json"
        )
    args.tenant_id = tenant_r.value
    args.repo_id = repo_r.value
    args.outputs_root = outputs_r.value

    opa_policy_r = resolve_optional_project_string(
        cli_value=getattr(args, "opa_policy_path", None),
        env_key="AKC_OPA_POLICY_PATH",
        file_value=proj.opa_policy_path if proj is not None else None,
        env=os.environ,
    )
    opa_decision_r = resolve_optional_project_string(
        cli_value=getattr(args, "opa_decision_path", None),
        env_key="AKC_OPA_DECISION_PATH",
        file_value=proj.opa_decision_path if proj is not None else None,
        env=os.environ,
    )
    opa_policy_effective = opa_policy_r.value
    opa_decision_effective: str = str(opa_decision_r.value or "data.akc.allow").strip() or "data.akc.allow"

    developer_role_profile = resolve_developer_role_profile(
        cli_value=getattr(args, "developer_role_profile", None),
        cwd=cwd,
        env=os.environ,
        project=proj,
    ).value
    canary_test_mode_raw = getattr(args, "canary_test_mode", "smoke")
    if developer_role_profile == "emerging" and str(canary_test_mode_raw).strip() == "smoke":
        canary_test_mode_raw = "full"

    living_profile = resolve_living_automation_profile(
        cli_value=getattr(args, "living_automation_profile", None),
        env=os.environ,
        project_value=proj.living_automation_profile if proj is not None else None,
    )

    llm_backend: LLMBackend | None = None
    llm_mode = str(getattr(args, "llm_mode", "offline"))
    if llm_mode == "custom":
        class_path = str(getattr(args, "llm_backend_class", "")).strip()
        if not class_path:
            raise SystemExit("--llm-backend-class is required when --llm-mode custom")
        try:
            llm_backend = _load_llm_backend_class(class_path=class_path)
        except Exception as e:
            raise SystemExit(f"failed to load custom llm backend: {e}") from e

    goal_s = str(getattr(args, "goal", "") or "").strip() or "Compile repository"
    code = living_recompile_execute(
        tenant_id=str(args.tenant_id),
        repo_id=str(args.repo_id),
        outputs_root=Path(str(args.outputs_root)),
        ingest_state_path=Path(str(args.ingest_state)),
        baseline_path=(
            Path(str(args.baseline_path)).expanduser() if getattr(args, "baseline_path", None) is not None else None
        ),
        eval_suite_path=Path(str(getattr(args, "eval_suite_path", "configs/evals/intent_system_v1.json"))),
        goal=goal_s,
        policy_mode=_parse_policy_mode(getattr(args, "policy_mode", "enforce")),
        canary_mode=_parse_canary_accept_mode(getattr(args, "canary_mode", "quick")),
        accept_mode=_parse_canary_accept_mode(getattr(args, "accept_mode", "thorough")),
        canary_test_mode=_parse_canary_test_mode(canary_test_mode_raw),
        allow_network=bool(getattr(args, "allow_network", False)),
        llm_backend=llm_backend,
        update_baseline_on_accept=bool(getattr(args, "update_baseline_on_accept", True)),
        skip_other_pending=bool(getattr(args, "skip_other_pending", True)),
        opa_policy_path=opa_policy_effective,
        opa_decision_path=opa_decision_effective,
        living_automation_profile=living_profile,
    )
    return int(code)


def _parse_tenant_allowlist_frozen(cli_value: str | None, env: Mapping[str, str]) -> frozenset[str]:
    raw = (str(cli_value).strip() if cli_value is not None else "") or str(
        env.get("AKC_LIVING_WEBHOOK_TENANT_ALLOWLIST", "") or ""
    ).strip()
    if not raw or raw == "*":
        return frozenset({"*"})
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return frozenset(parts) if parts else frozenset({"*"})


def _parse_living_webhook_outputs_root_allowlist(
    *,
    cli_value: str | None,
    env: Mapping[str, str],
    cwd: Path,
    project_outputs_root: str | None,
) -> frozenset[Path]:
    """Filesystem roots that webhook payload ``outputs_root`` may resolve under."""

    raw = (str(cli_value).strip() if cli_value is not None else "") or str(
        env.get("AKC_LIVING_WEBHOOK_OUTPUTS_ROOT_ALLOWLIST", "") or ""
    ).strip()
    paths: list[Path] = []
    if raw:
        for part in raw.split(","):
            p = part.strip()
            if not p:
                continue
            pp = Path(p)
            paths.append((cwd / pp).resolve() if not pp.is_absolute() else pp.resolve())
    elif project_outputs_root and str(project_outputs_root).strip():
        pp = Path(str(project_outputs_root).strip())
        paths.append((cwd / pp).resolve() if not pp.is_absolute() else pp.resolve())
    if not paths:
        raise SystemExit(
            "Missing webhook outputs root allowlist: use --outputs-root-allowlist, set "
            "AKC_LIVING_WEBHOOK_OUTPUTS_ROOT_ALLOWLIST, or set outputs_root in .akc/project.json"
        )
    return frozenset(paths)


def cmd_living_webhook_serve(args: argparse.Namespace) -> int:
    """Run an HMAC-authenticated HTTP receiver that maps fleet webhook payloads to ``living_recompile_execute``."""

    configure_logging(verbose=bool(getattr(args, "verbose", False)))
    cwd = Path(str(getattr(args, "cwd", "") or ".")).expanduser().resolve()
    proj = load_akc_project_config(cwd)

    secret = (getattr(args, "secret", None) or os.environ.get("AKC_LIVING_WEBHOOK_SECRET", "")).strip()
    if not secret:
        raise SystemExit("Missing webhook shared secret: use --secret or set AKC_LIVING_WEBHOOK_SECRET")

    ingest_r = resolve_optional_project_string(
        cli_value=getattr(args, "ingest_state", None),
        env_key="AKC_INGEST_STATE_PATH",
        file_value=proj.ingest_state_path if proj is not None else None,
        env=os.environ,
    )
    if ingest_r.value is None:
        raise SystemExit(
            "Missing ingest state path: use --ingest-state, set AKC_INGEST_STATE_PATH, "
            "or add ingest_state_path to .akc/project.json"
        )
    ingest_path = Path(str(ingest_r.value)).expanduser().resolve()

    opa_policy_r = resolve_optional_project_string(
        cli_value=getattr(args, "opa_policy_path", None),
        env_key="AKC_OPA_POLICY_PATH",
        file_value=proj.opa_policy_path if proj is not None else None,
        env=os.environ,
    )
    opa_decision_r = resolve_optional_project_string(
        cli_value=getattr(args, "opa_decision_path", None),
        env_key="AKC_OPA_DECISION_PATH",
        file_value=proj.opa_decision_path if proj is not None else None,
        env=os.environ,
    )
    opa_policy_effective = opa_policy_r.value
    opa_decision_effective: str = str(opa_decision_r.value or "data.akc.allow").strip() or "data.akc.allow"

    developer_role_profile = resolve_developer_role_profile(
        cli_value=getattr(args, "developer_role_profile", None),
        cwd=cwd,
        env=os.environ,
        project=proj,
    ).value
    canary_test_mode_raw = getattr(args, "canary_test_mode", "smoke")
    if developer_role_profile == "emerging" and str(canary_test_mode_raw).strip() == "smoke":
        canary_test_mode_raw = "full"

    living_profile = resolve_living_automation_profile(
        cli_value=getattr(args, "living_automation_profile", None),
        env=os.environ,
        project_value=proj.living_automation_profile if proj is not None else None,
    )

    llm_backend: LLMBackend | None = None
    llm_mode = str(getattr(args, "llm_mode", "offline"))
    if llm_mode == "custom":
        class_path = str(getattr(args, "llm_backend_class", "")).strip()
        if not class_path:
            raise SystemExit("--llm-backend-class is required when --llm-mode custom")
        try:
            llm_backend = _load_llm_backend_class(class_path=class_path)
        except Exception as e:
            raise SystemExit(f"failed to load custom llm backend: {e}") from e

    allowlist = _parse_tenant_allowlist_frozen(getattr(args, "tenant_allowlist", None), os.environ)
    outputs_allow = _parse_living_webhook_outputs_root_allowlist(
        cli_value=getattr(args, "outputs_root_allowlist", None),
        env=os.environ,
        cwd=cwd,
        project_outputs_root=proj.outputs_root if proj is not None else None,
    )

    eval_suite_s = str(getattr(args, "eval_suite_path", "configs/evals/intent_system_v1.json")).strip()
    eval_suite_p = Path(eval_suite_s)
    eval_suite_p = (cwd / eval_suite_p).resolve() if not eval_suite_p.is_absolute() else eval_suite_p.resolve()
    goal_s = str(getattr(args, "goal", "") or "").strip() or "Compile repository"

    cfg = LivingWebhookServerConfig(
        bind_host=str(getattr(args, "bind", "127.0.0.1")).strip() or "127.0.0.1",
        port=int(getattr(args, "port", 8787)),
        secret=secret,
        ingest_state_path=ingest_path,
        tenant_allowlist=allowlist,
        outputs_root_allowlist=outputs_allow,
        living_automation_profile=living_profile,
        opa_policy_path=opa_policy_effective,
        opa_decision_path=opa_decision_effective,
        llm_backend=llm_backend,
        eval_suite_path=eval_suite_p,
        goal=goal_s,
        policy_mode=str(getattr(args, "policy_mode", "enforce")),
        canary_mode=str(getattr(args, "canary_mode", "quick")),
        accept_mode=str(getattr(args, "accept_mode", "thorough")),
        canary_test_mode=str(canary_test_mode_raw),
        allow_network=bool(getattr(args, "allow_network", False)),
        update_baseline_on_accept=bool(getattr(args, "update_baseline_on_accept", True)),
        skip_other_pending=bool(getattr(args, "skip_other_pending", True)),
    )

    try:
        run_living_webhook_server(cfg)
    except KeyboardInterrupt:
        return 0
    return 0
