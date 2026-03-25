"""Agent Skills discovery/selection for compile prompt injection."""

from akc.compile.skills.discovery import build_skill_catalog, split_yaml_frontmatter
from akc.compile.skills.models import SkillCatalog, SkillManifest
from akc.compile.skills.pipeline import build_compile_skill_system_append
from akc.compile.skills.selection import select_activated_skills

__all__ = [
    "SkillCatalog",
    "SkillManifest",
    "build_compile_skill_system_append",
    "build_skill_catalog",
    "select_activated_skills",
    "split_yaml_frontmatter",
]
