"""Discover ``SKILL.md`` trees under configured roots (path-safe, byte-capped)."""

from __future__ import annotations

import hashlib
import os
import re
from collections.abc import Iterable
from pathlib import Path

from akc.compile.controller_config import ControllerConfig
from akc.compile.skills.models import SkillCatalog, SkillManifest, SkillPathKind

_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")


def split_yaml_frontmatter(raw: str) -> tuple[dict[str, str], str]:
    """Parse a minimal YAML-like frontmatter block (no PyYAML dependency).

    Only supports ``key: value`` lines; values are stripped scalars (quoted or plain).
    """

    lines = raw.splitlines()
    if not lines or lines[0].strip() != "---":
        return {}, raw
    end = None
    for i in range(1, len(lines)):
        if lines[i].strip() == "---":
            end = i
            break
    if end is None:
        return {}, raw
    fm_lines = lines[1:end]
    body = "\n".join(lines[end + 1 :])
    meta: dict[str, str] = {}
    for line in fm_lines:
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if ":" not in s:
            continue
        key, _, rest = s.partition(":")
        k = key.strip()
        v = rest.strip()
        if len(v) >= 2 and ((v[0] == v[-1] == '"') or (v[0] == v[-1] == "'")):
            v = v[1:-1]
        if k:
            meta[k] = v
    return meta, body


def _parse_bool(raw: str) -> bool:
    s = raw.strip().lower()
    return s in {"1", "true", "yes", "on"}


def _safe_skill_dir_name(name: str) -> bool:
    return bool(_NAME_RE.match(name))


def _resolve_path(path: Path) -> Path | None:
    try:
        return path.expanduser().resolve(strict=False)
    except OSError:
        return None


def _is_under_root(*, resolved_path: Path, resolved_root: Path) -> bool:
    try:
        return resolved_path.is_relative_to(resolved_root)
    except (OSError, ValueError):
        return False


def _read_utf8_limited_bytes(path: Path, *, max_bytes: int) -> str:
    cap = max(0, int(max_bytes))
    with path.open("rb") as f:
        raw = f.read(cap)
    return raw.decode("utf-8", errors="replace")


def _sha256_utf8(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _parse_manifest_from_text(
    *,
    text: str,
    skill_dir_resolved: Path,
    skill_md_resolved: Path,
    path_kind: SkillPathKind,
) -> SkillManifest:
    meta, body = split_yaml_frontmatter(text)
    dir_name = skill_dir_resolved.name
    name = (meta.get("name") or "").strip() or dir_name
    description = (meta.get("description") or "").strip()
    dmi_raw = meta.get("disable-model-invocation") or meta.get("disable_model_invocation") or "false"
    disable_model_invocation = _parse_bool(str(dmi_raw))
    license_v = meta.get("license")
    compat = meta.get("compatibility")
    known = {
        "name",
        "description",
        "license",
        "compatibility",
        "disable-model-invocation",
        "disable_model_invocation",
    }
    extra = {k: v for k, v in meta.items() if k not in known}
    root_s = str(skill_dir_resolved)
    md_s = str(skill_md_resolved)
    return SkillManifest(
        name=name,
        description=description,
        disable_model_invocation=disable_model_invocation,
        body_text=body.strip(),
        skill_root=root_s,
        skill_md_path=md_s,
        content_sha256=_sha256_utf8(text),
        path_kind=path_kind,
        license=license_v.strip() if isinstance(license_v, str) and license_v.strip() else None,
        compatibility=compat.strip() if isinstance(compat, str) and compat.strip() else None,
        extra_frontmatter=extra,
    )


def _load_builtin_default(*, max_file_bytes: int) -> SkillManifest | None:
    try:
        from importlib.resources import files
    except ImportError:
        return None
    root = files("akc.compile.skills.bundled.akc_default")
    try:
        raw_bytes = root.joinpath("SKILL.md").read_bytes()
    except (FileNotFoundError, OSError):
        return None
    cap = max(0, int(max_file_bytes))
    raw_bytes = raw_bytes[:cap]
    text = raw_bytes.decode("utf-8", errors="replace")
    meta, body = split_yaml_frontmatter(text)
    name = (meta.get("name") or "").strip() or "akc-default"
    description = (meta.get("description") or "").strip()
    dmi_raw = meta.get("disable-model-invocation") or meta.get("disable_model_invocation") or "false"
    disable_model_invocation = _parse_bool(str(dmi_raw))
    license_v = meta.get("license")
    compat = meta.get("compatibility")
    known = {
        "name",
        "description",
        "license",
        "compatibility",
        "disable-model-invocation",
        "disable_model_invocation",
    }
    extra = {k: v for k, v in meta.items() if k not in known}
    return SkillManifest(
        name=name,
        description=description,
        disable_model_invocation=disable_model_invocation,
        body_text=body.strip(),
        skill_root="builtin:akc-default",
        skill_md_path="builtin:akc-default/SKILL.md",
        content_sha256=_sha256_utf8(text),
        path_kind="builtin",
        license=license_v.strip() if isinstance(license_v, str) and license_v.strip() else None,
        compatibility=compat.strip() if isinstance(compat, str) and compat.strip() else None,
        extra_frontmatter=extra,
    )


def _project_skill_scan_anchor(*, project_resolved: Path, relative: Path) -> Path | None:
    anchor = _resolve_path(project_resolved / relative)
    if anchor is None or not anchor.is_dir():
        return None
    if not _is_under_root(resolved_path=anchor, resolved_root=project_resolved):
        return None
    return anchor


def _standalone_skill_scan_anchor(path: Path) -> Path | None:
    anchor = _resolve_path(path)
    if anchor is None or not anchor.is_dir():
        return None
    return anchor


def _scan_skill_root(
    *,
    scan_anchor: Path,
    path_kind: SkillPathKind,
    max_file_bytes: int,
    boundary_root: Path,
) -> list[SkillManifest]:
    """Load immediate child skill dirs under ``scan_anchor``.

    ``boundary_root`` is the resolved directory that may not be escaped (project root, extra root,
    or env root). Discovery lists only direct children of ``scan_anchor``, but those entries may
    be symlinks; resolved skill dirs and ``SKILL.md`` paths must stay under ``boundary_root`` so
    hops outside the allowed tree are ignored.
    """

    out: list[SkillManifest] = []
    if not scan_anchor.is_dir():
        return out
    try:
        if not _is_under_root(resolved_path=scan_anchor, resolved_root=boundary_root):
            return out
        for child in sorted(scan_anchor.iterdir(), key=lambda p: p.name):
            if not child.is_dir():
                continue
            if not _safe_skill_dir_name(child.name):
                continue
            child_r = _resolve_path(child)
            if child_r is None or not child_r.is_dir():
                continue
            if not _is_under_root(resolved_path=child_r, resolved_root=boundary_root):
                continue
            skill_md = child / "SKILL.md"
            if not skill_md.is_file():
                continue
            md_r = _resolve_path(skill_md)
            if md_r is None or not md_r.is_file():
                continue
            if not _is_under_root(resolved_path=md_r, resolved_root=boundary_root):
                continue
            try:
                text = _read_utf8_limited_bytes(md_r, max_bytes=max_file_bytes)
            except OSError:
                continue
            out.append(
                _parse_manifest_from_text(
                    text=text,
                    skill_dir_resolved=child_r,
                    skill_md_resolved=md_r,
                    path_kind=path_kind,
                )
            )
    except OSError:
        return out
    return out


def _merge_into_catalog(
    catalog: dict[str, SkillManifest],
    order: list[str],
    manifests: Iterable[SkillManifest],
) -> None:
    for m in manifests:
        key = m.name.strip()
        if not key:
            continue
        if key in catalog:
            continue
        catalog[key] = m
        order.append(key)


def build_skill_catalog(
    *,
    config: ControllerConfig,
    project_root: Path | None,
) -> SkillCatalog:
    """Load builtin + on-disk skills from standard roots and optional extras."""

    max_file = int(config.compile_skill_max_file_bytes)
    by_name: dict[str, SkillManifest] = {}
    order: list[str] = []

    builtin = _load_builtin_default(max_file_bytes=max_file)
    if builtin is not None:
        _merge_into_catalog(by_name, order, [builtin])

    project_resolved: Path | None = None
    if project_root is not None:
        project_resolved = _resolve_path(Path(project_root))
        if project_resolved is None:
            project_resolved = None

    def scan_at_anchor(anchor: Path, boundary: Path, kind: SkillPathKind) -> None:
        found = _scan_skill_root(
            scan_anchor=anchor,
            path_kind=kind,
            max_file_bytes=max_file,
            boundary_root=boundary,
        )
        _merge_into_catalog(by_name, order, found)

    if project_resolved is not None:
        for rel in (Path(".akc") / "skills", Path(".cursor") / "skills"):
            anchor = _project_skill_scan_anchor(project_resolved=project_resolved, relative=rel)
            if anchor is not None:
                scan_at_anchor(anchor, project_resolved, "project")
        for rel_entry in config.compile_skill_relative_roots:
            s = str(rel_entry).strip()
            if not s or s.startswith("..") or Path(s).is_absolute():
                continue
            anchor = _project_skill_scan_anchor(project_resolved=project_resolved, relative=Path(s))
            if anchor is not None:
                scan_at_anchor(anchor, project_resolved, "project")

    env_raw = os.environ.get("AKC_SKILLS_ROOT")
    if env_raw and str(env_raw).strip():
        env_anchor = _standalone_skill_scan_anchor(Path(env_raw))
        if env_anchor is not None:
            scan_at_anchor(env_anchor, env_anchor, "env")

    for extra in config.compile_skill_extra_roots:
        exp = _resolve_path(extra)
        if exp is None or not str(exp).strip():
            continue
        anchor = _standalone_skill_scan_anchor(exp)
        if anchor is not None:
            scan_at_anchor(anchor, anchor, "extra")

    return SkillCatalog(by_name=by_name, discovery_order=tuple(order))
