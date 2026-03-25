"""Glue discovery + selection + formatting for the compile controller."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from akc.compile.controller_config import ControllerConfig
from akc.compile.skills.discovery import build_skill_catalog
from akc.compile.skills.prompt import format_skill_system_preamble
from akc.compile.skills.selection import select_activated_skills
from akc.intent.models import IntentSpec


def build_compile_skill_system_append(
    *,
    config: ControllerConfig,
    project_root: Path | None,
    intent_spec: IntentSpec,
    goal: str,
    effective_max_input_tokens: int | None,
) -> tuple[str | None, dict[str, Any]]:
    """Return optional system suffix and audit metadata for accounting/manifests."""

    if config.compile_skills_mode == "off":
        return None, {"compile_skills_mode": "off", "compile_skills_active": []}

    catalog = build_skill_catalog(config=config, project_root=project_root)
    selected, _explicit = select_activated_skills(
        catalog=catalog,
        config=config,
        goal=goal,
        intent_spec=intent_spec,
    )
    text = format_skill_system_preamble(
        manifests=selected,
        max_total_bytes=int(config.compile_skill_max_total_bytes),
        max_input_tokens=effective_max_input_tokens,
    )
    audit_list = [
        {
            "name": m.name,
            "sha256": m.content_sha256,
            "path_kind": m.path_kind,
        }
        for m in selected
    ]
    meta: dict[str, Any] = {
        "compile_skills_mode": config.compile_skills_mode,
        "compile_skills_active": audit_list,
    }
    if not text.strip():
        return None, meta
    return text, meta
