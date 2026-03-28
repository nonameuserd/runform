from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from akc.llm.config import LlmProjectConfig


@dataclass(frozen=True, slots=True)
class AkcProjectConfig:
    """Repo-scoped defaults under ``.akc/project.{json,yaml}`` (optional)."""

    developer_role_profile: str | None = None
    # Progressive takeover: adoption ladder level hint (Level 0..4).
    # This is informational today (used by tooling/UX); compile still requires explicit flags.
    adoption_level: str | None = None
    tenant_id: str | None = None
    repo_id: str | None = None
    outputs_root: str | None = None
    opa_policy_path: str | None = None
    opa_decision_path: str | None = None
    living_automation_profile: str | None = None
    ingest_state_path: str | None = None
    living_unattended_claim: bool | None = None
    assistant_default_format: str | None = None
    assistant_session_retention_days: int | None = None
    assistant_model_hint: str | None = None
    llm: LlmProjectConfig | None = None
    memory_policy_path: str | None = None
    memory_budget_tokens: int | None = None
    compile_memory_budget_tokens: int | None = None
    assistant_memory_budget_tokens: int | None = None
    memory_pins: tuple[str, ...] = ()
    memory_boosts: dict[str, float] | None = None
    compile_skills: tuple[str, ...] = ()
    compile_skills_mode: str | None = None
    skill_roots: tuple[str, ...] = ()
    compile_skill_max_file_bytes: int | None = None
    compile_skill_max_total_bytes: int | None = None
    # Safe realization: allowlist relative path prefixes the compiler may mutate
    # under scoped_apply. Defaults are controlled by ControllerConfig; project.json
    # may tighten or widen explicitly.
    mutation_paths: tuple[str, ...] | None = None
    # Optional toolchain override for `akc compile` / native test execution.
    # This is a best-effort mapping that is resolved into a `ToolchainProfile`
    # by `akc.adopt.toolchain.resolve_toolchain_profile()`.
    toolchain: dict[str, Any] | None = None
    # When true, `smoke` / `full` test modes resolve commands from the toolchain like native_smoke/native_full.
    native_test_mode: bool | None = None
    # Fail-closed scoped_apply guard: deny categories from `akc.compile.change_scope`.
    change_scope_deny_categories: tuple[str, ...] | None = None
    # Optional operator-side validator binding registry used by verify/runtime validation.
    validation_bindings_path: str | None = None


def _coerce_str(data: Mapping[str, Any], key: str) -> str | None:
    v = data.get(key)
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def _coerce_str_list(data: Mapping[str, Any], key: str) -> tuple[str, ...]:
    v = data.get(key)
    if v is None:
        return ()
    if not isinstance(v, list):
        raise ValueError(f"{key} must be a JSON array of strings when set")
    out: list[str] = []
    for item in v:
        s = str(item).strip()
        if s:
            out.append(s)
    return tuple(out)


def _coerce_optional_str_list(data: Mapping[str, Any], key: str) -> tuple[str, ...] | None:
    if key not in data:
        return None
    return _coerce_str_list(data, key)


def _coerce_optional_positive_int(data: Mapping[str, Any], key: str) -> int | None:
    v = data.get(key)
    if v is None:
        return None
    if isinstance(v, bool):
        raise ValueError(f"{key} must be a positive integer when set, not a boolean")
    if isinstance(v, int):
        i = int(v)
    elif isinstance(v, float):
        if not float(v).is_integer():
            raise ValueError(f"{key} must be an integer when set")
        i = int(v)
    else:
        s = str(v).strip()
        if not s:
            return None
        try:
            i = int(s, 10)
        except ValueError as exc:
            raise ValueError(f"{key} must be a positive integer when set") from exc
    if i <= 0:
        raise ValueError(f"{key} must be > 0 when set")
    return i


def _coerce_optional_bool(data: Mapping[str, Any], key: str) -> bool | None:
    v = data.get(key)
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "on"):
        return True
    if s in ("0", "false", "no", "off"):
        return False
    return None


_SCOPE_DENY_ALLOWED: frozenset[str] = frozenset(("code", "config", "ci", "infra", "dependency", "docs", "other"))


def _coerce_optional_change_scope_deny(data: Mapping[str, Any], key: str) -> tuple[str, ...] | None:
    if key not in data:
        return None
    v = data[key]
    if v is None:
        return None
    if not isinstance(v, list):
        raise ValueError(f"{key} must be a JSON array of category strings when set")
    out: list[str] = []
    for item in v:
        s = str(item).strip().lower()
        if not s:
            continue
        if s not in _SCOPE_DENY_ALLOWED:
            raise ValueError(
                f"{key} unknown category {item!r}; expected one of {sorted(_SCOPE_DENY_ALLOWED)}",
            )
        out.append(s)
    return tuple(out)


def _coerce_optional_mapping(data: Mapping[str, Any], key: str) -> dict[str, Any] | None:
    v = data.get(key)
    if v is None:
        return None
    if not isinstance(v, dict):
        raise ValueError(f"{key} must be a JSON object when set")
    out: dict[str, Any] = {}
    for mk, mv in v.items():
        ks = str(mk).strip()
        if not ks:
            continue
        out[ks] = mv
    return out


def _coerce_nested_str(data: Mapping[str, Any], *, parent_key: str, child_key: str) -> str | None:
    parent = data.get(parent_key)
    if not isinstance(parent, Mapping):
        return None
    v = parent.get(child_key)
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def _coerce_nested_mapping(data: Mapping[str, Any], *, parent_key: str) -> Mapping[str, Any] | None:
    parent = data.get(parent_key)
    if not isinstance(parent, Mapping):
        return None
    return parent


def _coerce_optional_float_mapping(data: Mapping[str, Any], key: str) -> dict[str, float] | None:
    v = data.get(key)
    if v is None:
        return None
    if not isinstance(v, dict):
        raise ValueError(f"{key} must be a JSON object when set")
    out: dict[str, float] = {}
    for mk, mv in v.items():
        ks = str(mk).strip()
        if not ks:
            continue
        if isinstance(mv, bool) or not isinstance(mv, (int, float)):
            raise ValueError(f"{key}.{ks} must be a finite number")
        fv = float(mv)
        if not (fv < float("inf") and fv > float("-inf")):
            raise ValueError(f"{key}.{ks} must be a finite number")
        out[ks] = fv
    return out


def _coerce_nested_optional_positive_int(data: Mapping[str, Any], *, parent_key: str, child_key: str) -> int | None:
    parent = data.get(parent_key)
    if not isinstance(parent, Mapping):
        return None
    return _coerce_optional_positive_int(parent, child_key)


_LLM_PROJECT_SECRET_KEYS: frozenset[str] = frozenset(
    {
        "api_key",
        "apikey",
        "authorization",
        "openai_api_key",
        "anthropic_api_key",
        "gemini_api_key",
        "secret",
        "secrets",
        "token",
        "password",
        "credentials",
    }
)


def _reject_llm_project_secrets(llm: Mapping[str, Any]) -> None:
    """LLM secrets must not appear in repo-backed project config (CLI/env only)."""
    for raw_key in llm:
        key = str(raw_key).strip()
        lk = key.lower().replace("-", "_")
        if lk in _LLM_PROJECT_SECRET_KEYS:
            raise ValueError(
                f"llm.{key}: storing credentials in .akc/project.json is not allowed; "
                "use --llm-api-key or provider environment variables instead"
            )
        if lk.endswith("_api_key") or lk.endswith("_secret") or lk.endswith("_token"):
            raise ValueError(
                f"llm.{key}: storing credentials in .akc/project.json is not allowed; "
                "use --llm-api-key or provider environment variables instead"
            )


def _coerce_nested_optional_float(data: Mapping[str, Any], *, parent_key: str, child_key: str) -> float | None:
    parent = data.get(parent_key)
    if not isinstance(parent, Mapping):
        return None
    value = parent.get(child_key)
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"{parent_key}.{child_key} must be a number when set")
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError as exc:
        raise ValueError(f"{parent_key}.{child_key} must be a number when set") from exc


def load_akc_project_config(cwd: Path) -> AkcProjectConfig | None:
    """Load ``.akc/project.json`` or ``.akc/project.yaml`` when present.

    Precedence between files: ``project.json`` wins if both exist.
    YAML requires PyYAML (optional dev extra).
    """

    base = cwd / ".akc"
    json_path = base / "project.json"
    yaml_path = base / "project.yaml"
    data: dict[str, Any] | None = None
    if json_path.is_file():
        raw = json_path.read_text(encoding="utf-8")
        loaded = json.loads(raw)
        if not isinstance(loaded, dict):
            raise ValueError(f"{json_path} must contain a JSON object")
        data = loaded
    elif yaml_path.is_file():
        try:
            import yaml
        except ImportError as exc:
            raise RuntimeError(
                "Found .akc/project.yaml but PyYAML is not installed. "
                "Use .akc/project.json or install PyYAML (e.g. uv sync --extra dev)."
            ) from exc
        raw = yaml_path.read_text(encoding="utf-8")
        loaded = yaml.safe_load(raw)
        if loaded is None:
            data = {}
        elif not isinstance(loaded, dict):
            raise ValueError(f"{yaml_path} must contain a YAML mapping at the top level")
        else:
            data = loaded
    if data is None:
        return None
    llm_mapping = _coerce_nested_mapping(data, parent_key="llm")
    if llm_mapping is not None:
        _reject_llm_project_secrets(llm_mapping)
    llm_cfg = (
        LlmProjectConfig(
            backend=_coerce_nested_str(data, parent_key="llm", child_key="backend"),
            model=_coerce_nested_str(data, parent_key="llm", child_key="model"),
            base_url=_coerce_nested_str(data, parent_key="llm", child_key="base_url"),
            timeout_s=_coerce_nested_optional_float(data, parent_key="llm", child_key="timeout_s"),
            max_retries=_coerce_nested_optional_positive_int(data, parent_key="llm", child_key="max_retries"),
            allow_network=_coerce_optional_bool(dict(llm_mapping or {}), "allow_network"),
            backend_class=_coerce_nested_str(data, parent_key="llm", child_key="backend_class"),
        )
        if llm_mapping is not None
        else None
    )
    return AkcProjectConfig(
        developer_role_profile=_coerce_str(data, "developer_role_profile"),
        adoption_level=_coerce_str(data, "adoption_level"),
        tenant_id=_coerce_str(data, "tenant_id"),
        repo_id=_coerce_str(data, "repo_id"),
        outputs_root=_coerce_str(data, "outputs_root"),
        opa_policy_path=_coerce_str(data, "opa_policy_path"),
        opa_decision_path=_coerce_str(data, "opa_decision_path"),
        living_automation_profile=_coerce_str(data, "living_automation_profile"),
        ingest_state_path=_coerce_str(data, "ingest_state_path"),
        living_unattended_claim=_coerce_optional_bool(data, "living_unattended_claim"),
        assistant_default_format=_coerce_str(data, "assistant_default_format"),
        assistant_session_retention_days=_coerce_optional_positive_int(data, "assistant_session_retention_days"),
        assistant_model_hint=_coerce_str(data, "assistant_model_hint"),
        llm=llm_cfg,
        memory_policy_path=_coerce_str(data, "memory_policy_path"),
        memory_budget_tokens=_coerce_optional_positive_int(data, "memory_budget_tokens"),
        compile_memory_budget_tokens=_coerce_optional_positive_int(data, "compile_memory_budget_tokens"),
        assistant_memory_budget_tokens=_coerce_optional_positive_int(data, "assistant_memory_budget_tokens"),
        memory_pins=_coerce_str_list(data, "memory_pins"),
        memory_boosts=_coerce_optional_float_mapping(data, "memory_boosts"),
        compile_skills=_coerce_str_list(data, "compile_skills"),
        compile_skills_mode=_coerce_str(data, "compile_skills_mode"),
        skill_roots=_coerce_str_list(data, "skill_roots"),
        compile_skill_max_file_bytes=_coerce_optional_positive_int(data, "compile_skill_max_file_bytes"),
        compile_skill_max_total_bytes=_coerce_optional_positive_int(data, "compile_skill_max_total_bytes"),
        mutation_paths=_coerce_optional_str_list(data, "mutation_paths"),
        toolchain=_coerce_optional_mapping(data, "toolchain"),
        native_test_mode=_coerce_optional_bool(data, "native_test_mode"),
        change_scope_deny_categories=_coerce_optional_change_scope_deny(data, "change_scope_deny_categories"),
        validation_bindings_path=(
            _coerce_nested_str(data, parent_key="validation", child_key="bindings_path")
            or _coerce_str(data, "validation_bindings_path")
        ),
    )
