"""Coordination isolation: per-role cwd, policy_context projection, network tightening."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, cast

from akc.coordination.models import (
    CoordinationGovernance,
    CoordinationParseError,
    FilesystemScopeSpec,
    RoleIsolationProfile,
)
from akc.memory.models import JSONValue, normalize_repo_id, require_non_empty
from akc.path_security import safe_resolve_path
from akc.runtime.action_routing import tenant_scoped_runtime_cwd
from akc.runtime.coordination.external_identity import stub_external_identity_metadata
from akc.runtime.models import RuntimeContext


def normalize_role_id_for_path(role_id: str) -> str:
    """Sanitize a coordination role name for use as a single path segment."""

    s = str(role_id).strip()
    require_non_empty(s, name="coordination.role_id")
    if any(sep in s for sep in ("/", "\\")) or ".." in s:
        raise ValueError("coordination role_id contains unsafe path characters")
    return s


def role_scoped_runtime_cwd(*, context: RuntimeContext, outputs_root: str | Path, role_id: str) -> Path:
    """``.../.akc/runtime/<run_id>/<runtime_run_id>/roles/<role_id>/``."""

    rid = normalize_role_id_for_path(role_id)
    return tenant_scoped_runtime_cwd(context=context, outputs_root=outputs_root) / "roles" / rid


def resolve_coordination_subprocess_cwd(
    *,
    role_cwd: Path,
    filesystem_scope: FilesystemScopeSpec,
) -> Path:
    """Writable workdir for subprocess: ``role_cwd`` / ``scratch_subdir`` when set, else ``role_cwd``."""

    role_cwd.mkdir(parents=True, exist_ok=True)
    sub = filesystem_scope.scratch_subdir
    if sub is None or not str(sub).strip():
        return role_cwd
    work = role_cwd / sub
    work.mkdir(parents=True, exist_ok=True)
    return work


def subprocess_cwd_for_runtime_action(
    *,
    context: RuntimeContext,
    outputs_root: str | Path,
    action_policy_context: Mapping[str, JSONValue] | None,
) -> Path:
    """Default tenant runtime cwd, or per-role cwd when coordination policy_context is present."""

    base = tenant_scoped_runtime_cwd(context=context, outputs_root=outputs_root)
    if not action_policy_context:
        return base
    raw_role = action_policy_context.get("coordination_role_id")
    if not isinstance(raw_role, str) or not raw_role.strip():
        return base
    role = normalize_role_id_for_path(raw_role)
    role_cwd = role_scoped_runtime_cwd(context=context, outputs_root=outputs_root, role_id=role)
    fs_raw = action_policy_context.get("coordination_filesystem_scope")
    ro_list: tuple[str, ...] = ()
    scratch: str | None = "scratch"
    if isinstance(fs_raw, Mapping):
        ror = fs_raw.get("read_only_roots")
        if isinstance(ror, list):
            ro_list = tuple(str(x).strip() for x in ror if str(x).strip())
        ss = fs_raw.get("scratch_subdir")
        if ss is None:
            scratch = None
        elif isinstance(ss, str) and ss.strip():
            scratch = ss.strip()
        else:
            scratch = "scratch"
    fs_spec = FilesystemScopeSpec(read_only_roots=ro_list, scratch_subdir=scratch)
    return resolve_coordination_subprocess_cwd(role_cwd=role_cwd, filesystem_scope=fs_spec)


def tenant_repo_root(*, context: RuntimeContext, outputs_root: str | Path) -> Path:
    """``<outputs_root>/<tenant>/<repo_id>`` (same base as :func:`tenant_scoped_runtime_cwd`)."""

    base = safe_resolve_path(outputs_root)
    return base / context.tenant_id.strip() / normalize_repo_id(context.repo_id)


def resolve_read_only_root_paths(
    *,
    repo_root: Path,
    read_only_roots: tuple[str, ...],
) -> tuple[Path, ...]:
    """Resolve ``read_only_roots`` (repo-relative POSIX segments) under ``repo_root`` (fail-closed)."""

    out: list[Path] = []
    for rel in read_only_roots:
        r = str(rel).strip().replace("\\", "/").lstrip("/")
        p = (repo_root / r).resolve()
        try:
            p.relative_to(repo_root.resolve())
        except ValueError as exc:
            raise CoordinationParseError(f"read_only_roots path {rel!r} escapes repo root") from exc
        out.append(p)
    return tuple(out)


def effective_coordination_execution_allow_network(
    *,
    bundle_allow_network: bool,
    governance: CoordinationGovernance | None,
    role_profile: RoleIsolationProfile | None,
) -> bool:
    """Combine bundle + governance + per-role tightening (AND semantics; ``None`` inherits)."""

    effective = bool(bundle_allow_network)
    if governance is not None and governance.execution_allow_network is not None:
        effective = effective and bool(governance.execution_allow_network)
    if role_profile is not None and role_profile.execution_allow_network is not None:
        effective = effective and bool(role_profile.execution_allow_network)
    return effective


def role_isolation_profile_for_role(
    *,
    governance: CoordinationGovernance | None,
    role_name: str,
    fallback_tools: tuple[str, ...],
) -> RoleIsolationProfile | None:
    """Return the declared profile or a conservative default using ``fallback_tools``."""

    if governance is None:
        return None
    if role_name in governance.role_profiles:
        return governance.role_profiles[role_name]
    return RoleIsolationProfile(
        filesystem_scope=FilesystemScopeSpec(read_only_roots=(".",), scratch_subdir="scratch"),
        allowed_tools=tuple(sorted(fallback_tools)),
        execution_allow_network=None,
    )


def build_coordination_policy_context(
    *,
    run_stage: str,
    coordination_step_id: str,
    role_name: str,
    role_profile: RoleIsolationProfile | None,
    bundle_allow_network: bool,
    governance: CoordinationGovernance | None,
    coordination_spec_sha256: str,
) -> dict[str, JSONValue]:
    """Policy / audit context for coordinator-emitted actions (PolicyWrappedExecutor-style hooks)."""

    require_non_empty(run_stage, name="run_stage")
    require_non_empty(coordination_step_id, name="coordination_step_id")
    require_non_empty(role_name, name="coordination.role_name")
    eff_net = effective_coordination_execution_allow_network(
        bundle_allow_network=bundle_allow_network,
        governance=governance,
        role_profile=role_profile,
    )
    fs_obj: dict[str, JSONValue] = {}
    tools: list[str] = []
    if role_profile is not None:
        fs_obj = {
            "read_only_roots": list(role_profile.filesystem_scope.read_only_roots),
            "scratch_subdir": role_profile.filesystem_scope.scratch_subdir,
        }
        tools = list(role_profile.allowed_tools)
    identity = stub_external_identity_metadata()
    return {
        "run_stage": run_stage,
        "coordination_step_id": coordination_step_id,
        "coordination_role_id": role_name,
        "coordination_spec_sha256": coordination_spec_sha256,
        "coordination_execution_allow_network_effective": eff_net,
        "coordination_filesystem_scope": fs_obj,
        "coordination_allowed_tools": cast(JSONValue, tools),
        "external_identity_metadata": identity,
    }


def tools_for_role_from_coordination_raw(
    *,
    coordination_raw: Mapping[str, Any],
    role_name: str,
) -> tuple[str, ...]:
    """Read ``tools`` from ``agent_roles`` entry matching ``role_name``."""

    ar = coordination_raw.get("agent_roles")
    if not isinstance(ar, Sequence) or isinstance(ar, (str, bytes)):
        return ()
    want = str(role_name).strip()
    for item in ar:
        if not isinstance(item, Mapping):
            continue
        if str(item.get("name", "")).strip() != want:
            continue
        raw_tools = item.get("tools")
        if isinstance(raw_tools, Sequence) and not isinstance(raw_tools, (str, bytes)):
            return tuple(sorted({str(x).strip() for x in raw_tools if str(x).strip()}))
    return ()


def validate_role_profiles_network_vs_bundle(
    *,
    bundle_allow_network: bool,
    governance: Mapping[str, Any] | None,
) -> tuple[str, ...]:
    """Compile-time guard: per-role network cannot claim broader access than the bundle."""

    issues: list[str] = []
    if governance is None or not isinstance(governance, Mapping):
        return ()
    rp = governance.get("role_profiles")
    if not isinstance(rp, Mapping):
        return ()
    for role_key, prof_raw in rp.items():
        if not isinstance(prof_raw, Mapping):
            continue
        net = prof_raw.get("execution_allow_network")
        if isinstance(net, bool) and net and not bundle_allow_network:
            issues.append(
                f"governance.role_profiles[{role_key!r}].execution_allow_network cannot be true "
                "when bundle allow_network is false"
            )
    return tuple(issues)
