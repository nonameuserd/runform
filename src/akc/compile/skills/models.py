"""Agent Skills-style manifests for compile-time prompt injection."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

SkillPathKind = Literal["builtin", "project", "extra", "env"]


@dataclass(frozen=True, slots=True)
class SkillManifest:
    """One discovered ``SKILL.md`` package (Agent Skills layout)."""

    name: str
    description: str
    disable_model_invocation: bool
    body_text: str
    skill_root: str
    skill_md_path: str
    content_sha256: str
    path_kind: SkillPathKind
    license: str | None = None
    compatibility: str | None = None
    extra_frontmatter: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SkillCatalog:
    """All skills visible for a compile run (deduped by canonical name)."""

    by_name: dict[str, SkillManifest]
    discovery_order: tuple[str, ...]
