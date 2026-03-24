from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, TypeAlias

from .project_config import AkcProjectConfig, load_akc_project_config

DeveloperRoleProfile: TypeAlias = Literal["classic", "emerging"]

ProfileValueSource: TypeAlias = Literal[
    "cli",
    "env",
    "project_file",
    "governance",
    "profile_default",
    "legacy_default",
]


def normalize_developer_role_profile(value: object) -> DeveloperRoleProfile:
    raw = str(value or "classic").strip().lower()
    if raw == "emerging":
        return "emerging"
    return "classic"


@dataclass(frozen=True, slots=True)
class ResolvedValue:
    value: Any
    source: ProfileValueSource


def _governance_compile_defaults(governance_profile: object) -> dict[str, Any]:
    """Derive compile defaults from governance: ``rollout_stage`` first, then explicit ``compile_defaults``.

    Explicit keys from the policy bundle's ``governance_profile.compile_defaults`` win over
    stage-derived values so enterprise policy can set sandbox/replay/promotion org-wide without
    per-developer env files.
    """

    out: dict[str, Any] = {}
    stage = ""
    if governance_profile is not None:
        stage = str(getattr(governance_profile, "rollout_stage", "") or "").strip().lower()
    if stage in {"prod", "production"}:
        out["sandbox"] = "strong"
        out["strong_lane_preference"] = "docker"
        out["policy_mode"] = "enforce"
        out["promotion_mode"] = "staged_apply"
    elif stage in {"staging", "preprod"}:
        out["sandbox"] = "strong"
        out["strong_lane_preference"] = "docker"
        out["policy_mode"] = "enforce"

    if governance_profile is not None:
        cd = getattr(governance_profile, "compile_defaults", None)
        pairs: list[tuple[str, str]] = []
        if isinstance(cd, Mapping):
            pairs = [(str(k), str(v)) for k, v in cd.items()]
        elif cd is not None:
            try:
                pairs = [(str(a), str(b)) for a, b in cd]
            except (TypeError, ValueError):
                pairs = []
        for key, val in pairs:
            k = key.strip()
            v = val.strip()
            if not k or not v:
                continue
            out[k] = v

    return out


def _resolve_by_precedence(
    *,
    cli_value: Any,
    legacy_default: Any,
    governance_value: Any = None,
    profile_default: Any = None,
) -> ResolvedValue:
    if cli_value != legacy_default:
        return ResolvedValue(value=cli_value, source="cli")
    if governance_value is not None:
        return ResolvedValue(value=governance_value, source="governance")
    if profile_default is not None:
        return ResolvedValue(value=profile_default, source="profile_default")
    return ResolvedValue(value=legacy_default, source="legacy_default")


def resolve_optional_project_string(
    *,
    cli_value: str | None,
    env_key: str,
    file_value: str | None,
    env: Mapping[str, str],
) -> ResolvedValue:
    """Resolve a string with precedence: CLI > env > project file > unset.

    When nothing applies, returns ``ResolvedValue(value=None, source="legacy_default")``.
    """

    if cli_value is not None and str(cli_value).strip() != "":
        return ResolvedValue(value=str(cli_value).strip(), source="cli")
    env_raw = env.get(env_key)
    if env_raw is not None and str(env_raw).strip() != "":
        return ResolvedValue(value=str(env_raw).strip(), source="env")
    if file_value is not None and str(file_value).strip() != "":
        return ResolvedValue(value=str(file_value).strip(), source="project_file")
    return ResolvedValue(value=None, source="legacy_default")


def resolve_developer_role_profile(
    *,
    cli_value: str | None,
    cwd: Path,
    env: Mapping[str, str],
    project: AkcProjectConfig | None = None,
) -> ResolvedValue:
    """Resolve developer role profile: CLI > ``AKC_DEVELOPER_ROLE_PROFILE`` > project file > classic."""

    legacy_default: DeveloperRoleProfile = "classic"
    if cli_value is not None:
        return ResolvedValue(value=normalize_developer_role_profile(cli_value), source="cli")
    env_raw = env.get("AKC_DEVELOPER_ROLE_PROFILE")
    if env_raw is not None and str(env_raw).strip() != "":
        return ResolvedValue(value=normalize_developer_role_profile(env_raw), source="env")
    proj = project if project is not None else load_akc_project_config(cwd)
    if proj is not None and proj.developer_role_profile is not None and str(proj.developer_role_profile).strip() != "":
        return ResolvedValue(
            value=normalize_developer_role_profile(proj.developer_role_profile),
            source="project_file",
        )
    return ResolvedValue(value=legacy_default, source="legacy_default")


def resolve_compile_profile_defaults(
    *,
    profile: DeveloperRoleProfile,
    governance_profile: object,
    sandbox: str,
    strong_lane_preference: str,
    policy_mode: str,
    replay_mode: str,
    promotion_mode: str | None,
    stored_assertion_index: str,
) -> dict[str, ResolvedValue]:
    gov = _governance_compile_defaults(governance_profile)
    profile_defaults: dict[str, Any] = {}
    if profile == "emerging":
        profile_defaults = {
            "sandbox": "dev",
            "strong_lane_preference": "docker",
            "policy_mode": "enforce",
            "replay_mode": "live",
            "promotion_mode": "artifact_only",
            "stored_assertion_index": "merge",
            "intent_bootstrap_from_store": True,
            "auto_seed_deployable_step": True,
        }
    resolved: dict[str, ResolvedValue] = {
        "sandbox": _resolve_by_precedence(
            cli_value=sandbox,
            legacy_default="dev",
            governance_value=gov.get("sandbox"),
            profile_default=profile_defaults.get("sandbox"),
        ),
        "strong_lane_preference": _resolve_by_precedence(
            cli_value=strong_lane_preference,
            legacy_default="docker",
            governance_value=gov.get("strong_lane_preference"),
            profile_default=profile_defaults.get("strong_lane_preference"),
        ),
        "policy_mode": _resolve_by_precedence(
            cli_value=policy_mode,
            legacy_default="enforce",
            governance_value=gov.get("policy_mode"),
            profile_default=profile_defaults.get("policy_mode"),
        ),
        "replay_mode": _resolve_by_precedence(
            cli_value=replay_mode,
            legacy_default="live",
            governance_value=gov.get("replay_mode"),
            profile_default=profile_defaults.get("replay_mode"),
        ),
        "promotion_mode": _resolve_by_precedence(
            cli_value=promotion_mode,
            legacy_default=None,
            governance_value=gov.get("promotion_mode"),
            profile_default=profile_defaults.get("promotion_mode"),
        ),
        "stored_assertion_index": _resolve_by_precedence(
            cli_value=stored_assertion_index,
            legacy_default="off",
            governance_value=gov.get("stored_assertion_index"),
            profile_default=profile_defaults.get("stored_assertion_index"),
        ),
        "intent_bootstrap_from_store": ResolvedValue(
            value=bool(profile_defaults.get("intent_bootstrap_from_store", False)),
            source="profile_default" if profile == "emerging" else "legacy_default",
        ),
        "auto_seed_deployable_step": ResolvedValue(
            value=bool(profile_defaults.get("auto_seed_deployable_step", False)),
            source="profile_default" if profile == "emerging" else "legacy_default",
        ),
    }
    return resolved


def resolve_ingest_profile_defaults(
    *,
    profile: DeveloperRoleProfile,
    tenant_id: str,
    cwd: Path,
    no_index: bool,
    index_backend: str,
    embedder: str,
    sqlite_path: str | None,
    assertion_index_root: str | None,
) -> dict[str, ResolvedValue]:
    if profile != "emerging":
        return {
            "index_backend": ResolvedValue(index_backend, "legacy_default"),
            "embedder": ResolvedValue(embedder, "legacy_default"),
            "sqlite_path": ResolvedValue(sqlite_path, "legacy_default"),
            "assertion_index_root": ResolvedValue(assertion_index_root, "legacy_default"),
        }
    if no_index:
        return {
            "index_backend": ResolvedValue(index_backend, "cli"),
            "embedder": ResolvedValue(embedder, "cli"),
            "sqlite_path": ResolvedValue(sqlite_path, "cli"),
            "assertion_index_root": ResolvedValue(assertion_index_root, "cli"),
        }
    default_sqlite = str((cwd / ".akc" / "ingest" / tenant_id / "index.sqlite3").resolve())
    default_assertion_root = str((cwd / "out").resolve())
    return {
        "index_backend": _resolve_by_precedence(
            cli_value=index_backend,
            legacy_default="memory",
            profile_default="sqlite",
        ),
        "embedder": _resolve_by_precedence(
            cli_value=embedder,
            legacy_default="none",
            profile_default="hash",
        ),
        "sqlite_path": ResolvedValue(
            sqlite_path if sqlite_path is not None else default_sqlite,
            "cli" if sqlite_path is not None else "profile_default",
        ),
        "assertion_index_root": ResolvedValue(
            assertion_index_root if assertion_index_root is not None else default_assertion_root,
            "cli" if assertion_index_root is not None else "profile_default",
        ),
    }


def resolve_runtime_start_profile_defaults(
    *,
    profile: DeveloperRoleProfile,
    mode: str | None,
    bundle: str | None,
) -> dict[str, ResolvedValue]:
    if profile != "emerging":
        return {
            "mode": ResolvedValue(mode, "cli"),
            "bundle": ResolvedValue(bundle, "cli"),
            "coordination_parallel_dispatch": ResolvedValue("inherit", "legacy_default"),
            "coordination_max_in_flight_steps": ResolvedValue(None, "legacy_default"),
            "coordination_max_in_flight_per_role": ResolvedValue(None, "legacy_default"),
        }
    return {
        "mode": ResolvedValue(
            mode if mode is not None else "simulate",
            "cli" if mode is not None else "profile_default",
        ),
        "bundle": ResolvedValue(bundle, "cli" if bundle is not None else "profile_default"),
        "coordination_parallel_dispatch": ResolvedValue("enabled", "profile_default"),
        "coordination_max_in_flight_steps": ResolvedValue(2, "profile_default"),
        "coordination_max_in_flight_per_role": ResolvedValue(1, "profile_default"),
    }
