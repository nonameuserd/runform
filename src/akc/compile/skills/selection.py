"""Select which skill bodies to inject for a compile iteration."""

from __future__ import annotations

import re

from akc.compile.controller_config import ControllerConfig
from akc.compile.skills.models import SkillCatalog, SkillManifest
from akc.intent.models import IntentSpec


def build_intent_text_for_skill_scoring(*, intent_spec: IntentSpec) -> str:
    """Compact text from intent for keyword overlap scoring (Phase 2)."""

    parts: list[str] = []
    if intent_spec.goal_statement:
        parts.append(intent_spec.goal_statement)
    for o in intent_spec.objectives[:12]:
        parts.append(o.statement)
    for c in intent_spec.constraints[:12]:
        parts.append(c.statement)
    for sc in intent_spec.success_criteria[:12]:
        parts.append(sc.description)
    return "\n".join(parts)


_TOKEN_RE = re.compile(r"[A-Za-z0-9_]{3,}")


def _tokens(text: str) -> set[str]:
    return {m.group(0).lower() for m in _TOKEN_RE.finditer(text)}


def _score_skill(*, description: str, goal: str, intent_blob: str) -> int:
    if not description.strip():
        return 0
    d = _tokens(description)
    g = _tokens(goal) | _tokens(intent_blob)
    if not d or not g:
        return 0
    return len(d & g)


def _resolve_skill_name(catalog: SkillCatalog, requested: str) -> str | None:
    req = requested.strip()
    if not req:
        return None
    if req in catalog.by_name:
        return req
    lower = req.lower()
    for name in catalog.discovery_order:
        if name.lower() == lower:
            return name
    return None


def select_activated_skills(
    *,
    catalog: SkillCatalog,
    config: ControllerConfig,
    goal: str,
    intent_spec: IntentSpec,
) -> tuple[tuple[SkillManifest, ...], frozenset[str]]:
    """Return manifests to inject (ordered) and the explicit allow set (for disable-model rules)."""

    mode = config.compile_skills_mode
    if mode == "off":
        return (), frozenset()

    intent_blob = build_intent_text_for_skill_scoring(intent_spec=intent_spec)
    explicit_requested: list[str] = []
    for raw in config.compile_skill_allowlist:
        s = str(raw).strip()
        if not s:
            continue
        resolved = _resolve_skill_name(catalog, s)
        if resolved is not None:
            explicit_requested.append(resolved)
    explicit_set = frozenset(explicit_requested)

    builtin_name: str | None = None
    for n in catalog.discovery_order:
        m = catalog.by_name.get(n)
        if m is not None and m.path_kind == "builtin":
            builtin_name = n
            break

    ordered: list[SkillManifest] = []
    seen: set[str] = set()

    def append_name(name: str) -> None:
        if name in seen:
            return
        man = catalog.by_name.get(name)
        if man is None:
            return
        seen.add(name)
        ordered.append(man)

    if mode == "default_only":
        if builtin_name:
            append_name(builtin_name)
        return tuple(ordered), explicit_set

    if builtin_name:
        append_name(builtin_name)

    for name in explicit_requested:
        append_name(name)

    if mode == "explicit":
        return tuple(ordered), explicit_set

    # auto: add scored candidates from catalog
    scored: list[tuple[int, str]] = []
    for name in catalog.discovery_order:
        if name in seen:
            continue
        man = catalog.by_name[name]
        if man.disable_model_invocation and name not in explicit_set:
            continue
        sc = _score_skill(description=man.description, goal=goal, intent_blob=intent_blob)
        if sc > 0:
            scored.append((sc, name))
    scored.sort(key=lambda t: (-t[0], t[1]))
    for _sc, name in scored:
        append_name(name)

    return tuple(ordered), explicit_set
