from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class AkcProjectConfig:
    """Repo-scoped defaults under ``.akc/project.{json,yaml}`` (optional)."""

    developer_role_profile: str | None = None
    tenant_id: str | None = None
    repo_id: str | None = None
    outputs_root: str | None = None
    opa_policy_path: str | None = None
    opa_decision_path: str | None = None
    living_automation_profile: str | None = None
    ingest_state_path: str | None = None
    living_unattended_claim: bool | None = None
    compile_skills: tuple[str, ...] = ()
    compile_skills_mode: str | None = None
    skill_roots: tuple[str, ...] = ()
    compile_skill_max_file_bytes: int | None = None
    compile_skill_max_total_bytes: int | None = None


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
    return AkcProjectConfig(
        developer_role_profile=_coerce_str(data, "developer_role_profile"),
        tenant_id=_coerce_str(data, "tenant_id"),
        repo_id=_coerce_str(data, "repo_id"),
        outputs_root=_coerce_str(data, "outputs_root"),
        opa_policy_path=_coerce_str(data, "opa_policy_path"),
        opa_decision_path=_coerce_str(data, "opa_decision_path"),
        living_automation_profile=_coerce_str(data, "living_automation_profile"),
        ingest_state_path=_coerce_str(data, "ingest_state_path"),
        living_unattended_claim=_coerce_optional_bool(data, "living_unattended_claim"),
        compile_skills=_coerce_str_list(data, "compile_skills"),
        compile_skills_mode=_coerce_str(data, "compile_skills_mode"),
        skill_roots=_coerce_str_list(data, "skill_roots"),
        compile_skill_max_file_bytes=_coerce_optional_positive_int(data, "compile_skill_max_file_bytes"),
        compile_skill_max_total_bytes=_coerce_optional_positive_int(data, "compile_skill_max_total_bytes"),
    )
