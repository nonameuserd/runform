"""Validation for unattended living posture (paths, eval hooks, profile/claim alignment)."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Mapping
from pathlib import Path

from akc.living.automation_profile import (
    LIVING_AUTOMATION_PROFILE_ENV,
    resolve_living_automation_profile,
)

from .common import configure_logging
from .profile_defaults import resolve_optional_project_string
from .project_config import AkcProjectConfig, load_akc_project_config

LIVING_UNATTENDED_CLAIM_ENV = "AKC_LIVING_UNATTENDED_CLAIM"
AKC_AUTOPILOT_LEASE_BACKEND_ENV = "AKC_AUTOPILOT_LEASE_BACKEND"
AKC_AUTOPILOT_LEASE_NAMESPACE_ENV = "AKC_AUTOPILOT_LEASE_NAMESPACE"
AKC_AUTOPILOT_EXPECT_REPLICAS_ENV = "AKC_AUTOPILOT_EXPECT_REPLICAS"


def _truthy_env(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _unattended_claim_from_env(env: Mapping[str, str]) -> bool:
    return _truthy_env(env.get(LIVING_UNATTENDED_CLAIM_ENV))


def _unattended_claim_from_project(project: AkcProjectConfig | None) -> bool:
    if project is None or project.living_unattended_claim is None:
        return False
    return bool(project.living_unattended_claim)


def _resolve_ingest_state_path(
    *,
    cli_value: str | None,
    env: Mapping[str, str],
    project: AkcProjectConfig | None,
) -> tuple[str | None, str]:
    """Return (path or None, source label)."""

    r = resolve_optional_project_string(
        cli_value=cli_value,
        env_key="AKC_INGEST_STATE_PATH",
        file_value=project.ingest_state_path if project is not None else None,
        env=env,
    )
    if r.value is not None:
        return r.value, r.source
    alt = str(env.get("AKC_AUTOPILOT_INGEST_STATE", "")).strip()
    if alt:
        return alt, "env:AKC_AUTOPILOT_INGEST_STATE"
    return None, "unset"


def lease_single_writer_warnings(
    *,
    env: Mapping[str, str],
    lease_backend: str | None = None,
    lease_namespace: str | None = None,
    expect_replicas: int | None = None,
) -> list[str]:
    """Return non-fatal warning lines for autopilot single-writer / lease posture.

    Filesystem leases use ``fcntl`` locks on a path under ``outputs_root``; they coordinate
    processes on one host sharing that mount. Kubernetes Lease objects coordinate across pods
    when ``lease_backend=k8s`` and ``lease_namespace`` is set (see ``autopilot.py``).
    """

    warnings: list[str] = []
    raw_backend = (
        str(lease_backend).strip()
        if isinstance(lease_backend, str) and lease_backend.strip()
        else str(env.get(AKC_AUTOPILOT_LEASE_BACKEND_ENV, "") or "").strip()
    )
    backend = raw_backend.lower() if raw_backend else "filesystem"
    if backend not in {"filesystem", "k8s"}:
        warnings.append(
            f"WARNING: invalid lease backend {raw_backend!r} ({AKC_AUTOPILOT_LEASE_BACKEND_ENV}); "
            "expected filesystem or k8s — defaulting checks to filesystem"
        )
        backend = "filesystem"

    ns = (
        str(lease_namespace).strip()
        if isinstance(lease_namespace, str) and lease_namespace.strip()
        else str(env.get(AKC_AUTOPILOT_LEASE_NAMESPACE_ENV, "") or "").strip()
    )

    exp_rep: int
    if expect_replicas is not None:
        exp_rep = max(1, int(expect_replicas))
    else:
        raw_er = str(env.get(AKC_AUTOPILOT_EXPECT_REPLICAS_ENV, "1") or "").strip()
        try:
            exp_rep = max(1, int(raw_er))
        except ValueError:
            exp_rep = 1
            warnings.append(
                f"WARNING: invalid {AKC_AUTOPILOT_EXPECT_REPLICAS_ENV}={raw_er!r}; using 1 for replica checks"
            )

    if str(env.get("KUBERNETES_SERVICE_HOST", "") or "").strip() and backend == "filesystem":
        warnings.append(
            "WARNING: lease_backend=filesystem while running in a Kubernetes pod "
            "(KUBERNETES_SERVICE_HOST is set): flock coordination does not extend across nodes. "
            "Use --lease-backend k8s with --lease-namespace (coordination.k8s.io Lease), "
            "or run exactly one replica with a shared filesystem lease path."
        )

    if exp_rep > 1 and backend == "filesystem":
        warnings.append(
            "WARNING: expect_replicas>1 with lease_backend=filesystem: single-writer is only safe "
            "when all controllers share one host/mount; otherwise use lease_backend=k8s and a "
            f"non-empty {AKC_AUTOPILOT_LEASE_NAMESPACE_ENV} / --lease-namespace for cluster-wide leadership."
        )

    if backend == "k8s" and not ns:
        warnings.append(
            f"WARNING: lease_backend=k8s requires --lease-namespace or {AKC_AUTOPILOT_LEASE_NAMESPACE_ENV} "
            "so Lease objects are namespaced; without it, autopilot cannot acquire a distributed lease."
        )

    return warnings


def eval_suite_has_living_hooks(path: Path) -> tuple[bool, str]:
    """True when suite JSON includes eval hooks needed for unattended policy gates."""

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return False, f"eval suite not readable as JSON: {exc}"
    if not isinstance(raw, dict):
        return False, "eval suite must be a JSON object"
    pol = raw.get("living_recompile_policy")
    thr = raw.get("runtime_canary_thresholds")
    has_pol = isinstance(pol, dict) and len(pol) > 0
    has_thr = isinstance(thr, dict) and len(thr) > 0
    if has_pol or has_thr:
        return True, "ok"
    return False, "missing non-empty living_recompile_policy and runtime_canary_thresholds"


def run_living_unattended_checks(
    *,
    cwd: Path,
    env: Mapping[str, str],
    project: AkcProjectConfig | None,
    tenant_id: str,
    repo_id: str,
    outputs_root: Path,
    eval_suite_path: Path | None,
    ingest_state_path: Path | None,
    relaxed_baseline: bool,
    living_automation_profile_cli: str | None = None,
    lease_backend: str | None = None,
    lease_namespace: str | None = None,
    expect_replicas: int | None = None,
) -> tuple[bool, list[str]]:
    """Return (ok, lines) for unattended / claim validation. Tenant-scoped paths only."""

    lines: list[str] = []
    ok = True

    profile = resolve_living_automation_profile(
        cli_value=living_automation_profile_cli,
        env=env,
        project_value=project.living_automation_profile if project is not None else None,
    )

    claim = _unattended_claim_from_env(env) or _unattended_claim_from_project(project)
    if claim and profile.id != "living_loop_unattended_v1":
        ok = False
        lines.append(
            f"Unattended claim is set ({LIVING_UNATTENDED_CLAIM_ENV} or project living_unattended_claim) "
            f"but {LIVING_AUTOMATION_PROFILE_ENV} / project living_automation_profile is not "
            "living_loop_unattended_v1"
        )

    from akc.path_security import safe_resolve_scoped_path

    scope_root = safe_resolve_scoped_path(outputs_root, tenant_id, repo_id)
    if profile.id == "living_loop_unattended_v1":
        if not scope_root.is_dir():
            ok = False
            lines.append(f"outputs scope directory missing: {scope_root}")
        baseline = scope_root / ".akc" / "living" / "baseline.json"
        if scope_root.is_dir() and not baseline.is_file():
            msg = f"living baseline not found (expected {baseline})"
            if relaxed_baseline:
                lines.append(f"WARNING: {msg}")
            else:
                ok = False
                lines.append(msg)

        if ingest_state_path is None:
            ing_s, ingest_src = _resolve_ingest_state_path(cli_value=None, env=env, project=project)
            from akc.path_security import safe_resolve_path

            ingest_resolved = safe_resolve_path(ing_s) if ing_s else None
        else:
            from akc.path_security import safe_resolve_path

            ingest_resolved = safe_resolve_path(ingest_state_path)
            ingest_src = "cli"
        if ingest_resolved is None or not ingest_resolved.is_file():
            ok = False
            lines.append(
                "ingest state path missing: set --ingest-state-path, "
                "AKC_INGEST_STATE_PATH / AKC_AUTOPILOT_INGEST_STATE, or project ingest_state_path"
            )
        else:
            lines.append(f"ingest_state: {ingest_resolved} (source: {ingest_src})")

        ep = eval_suite_path
        if ep is None:
            default = Path("configs/evals/intent_system_v1.json")
            from akc.path_security import safe_resolve_scoped_path

            ep = safe_resolve_scoped_path(cwd, str(default)) if not default.is_absolute() else default
        else:
            from akc.path_security import safe_resolve_path

            ep = safe_resolve_path(ep)
        if not ep.is_file():
            ok = False
            lines.append(f"eval suite not found: {ep}")
        else:
            hooks_ok, hooks_msg = eval_suite_has_living_hooks(ep)
            if hooks_ok:
                lines.append(f"eval suite hooks: ok ({ep})")
            else:
                ok = False
                lines.append(f"eval suite hooks: {hooks_msg} ({ep})")

        for w in lease_single_writer_warnings(
            env=env,
            lease_backend=lease_backend,
            lease_namespace=lease_namespace,
            expect_replicas=expect_replicas,
        ):
            lines.append(w)

    lines.insert(0, f"living_automation_profile: {profile.id} ({LIVING_AUTOMATION_PROFILE_ENV} / project)")
    if claim:
        lines.insert(1, "unattended_claim: active (env or project)")
    return ok, lines


def cmd_living_doctor(args: argparse.Namespace) -> int:
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

    ingest_arg = getattr(args, "ingest_state_path", None)
    ingest_p = Path(str(ingest_arg)).expanduser().resolve() if ingest_arg else None
    eval_arg = getattr(args, "eval_suite_path", None)
    eval_p = Path(str(eval_arg)).expanduser().resolve() if eval_arg else None

    ok, lines = run_living_unattended_checks(
        cwd=cwd,
        env=os.environ,
        project=proj,
        tenant_id=str(tenant_r.value),
        repo_id=str(repo_r.value),
        outputs_root=Path(outputs_r.value),
        eval_suite_path=eval_p,
        ingest_state_path=ingest_p,
        relaxed_baseline=bool(getattr(args, "relaxed_baseline", False)),
        living_automation_profile_cli=getattr(args, "living_automation_profile", None),
        lease_backend=getattr(args, "lease_backend", None),
        lease_namespace=getattr(args, "lease_namespace", None),
        expect_replicas=getattr(args, "expect_replicas", None),
    )
    print("Living unattended checks:")
    for line in lines:
        print(f"  {line}")
    if ok:
        print("Living doctor: ok.")
        return 0
    print("Living doctor: failed.", file=sys.stderr)
    return 2
