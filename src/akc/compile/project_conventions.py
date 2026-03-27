from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any


def load_project_conventions(*, project_root: Path) -> Mapping[str, Any] | None:
    """Load `conventions` from `.akc/project_profile.json` (best-effort)."""
    profile_path = project_root / ".akc" / "project_profile.json"
    if not profile_path.is_file():
        return None
    try:
        raw = json.loads(profile_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(raw, dict):
        return None
    conventions = raw.get("conventions")
    if isinstance(conventions, dict):
        return conventions
    return None


def _cap_utf8(text: str, *, max_bytes: int) -> str:
    """Cap text to <= `max_bytes` without corrupting UTF-8 sequences."""
    if max_bytes <= 0:
        return ""
    raw = text.encode("utf-8", errors="replace")
    if len(raw) <= max_bytes:
        return text
    cut = raw[:max_bytes]
    # Trim possible continuation bytes at the end.
    while cut and (cut[-1] & 0b1100_0000) == 0b1000_0000:
        cut = cut[:-1]
    return cut.decode("utf-8", errors="replace")


def render_conventions_system_preamble(conventions: Mapping[str, Any], *, max_bytes: int = 2200) -> str:
    """Render a compact system-message section guiding generated code."""

    layout = conventions.get("layout")
    naming = conventions.get("naming")
    imports = conventions.get("imports")
    tests = conventions.get("tests")

    def _kv_line(section_obj_: Mapping[str, Any], key: str) -> str | None:
        v = section_obj_.get(key)
        if isinstance(v, str) and v.strip():
            return f"- {key}: {v}"
        return None

    def _append_keys(
        *,
        section_obj_: Mapping[str, Any],
        keys: tuple[str, ...],
    ) -> None:
        for key in keys:
            line = _kv_line(section_obj_=section_obj_, key=key)
            if line is not None:
                parts.append(line)

    parts: list[str] = []
    parts.append("Coding conventions snapshot (best-effort).")
    parts.append("Follow these patterns when generating code under the active tenant/repo scope.")

    # Layout + module structure.
    if isinstance(layout, Mapping) and layout:
        parts.append("Directory structure:")
        _append_keys(
            section_obj_=layout,
            keys=(
                "primary_code_dir",
                "has_src",
                "has_lib",
                "has_tests_dir",
                "python_package_depth_mode",
                "python_package_depth_ratio",
                "python_package_depth_max",
            ),
        )

    # Naming.
    if isinstance(naming, Mapping) and naming:
        parts.append("Naming (file stems):")
        _append_keys(
            section_obj_=naming,
            keys=("snake_case_ratio", "camelcase_ratio", "kebab_case_ratio"),
        )

        # Optional AST-driven identifier naming ratios.
        id_keys = (
            "identifier_snake_case_ratio",
            "identifier_camelcase_ratio",
            "identifier_kebab_case_ratio",
        )
        if any(k in naming for k in id_keys):
            parts.append("Naming (identifiers):")
            _append_keys(section_obj_=naming, keys=id_keys)

    # Imports.
    if isinstance(imports, Mapping) and imports:
        parts.append("Import style:")
        _append_keys(
            section_obj_=imports,
            keys=(
                "import_preference",
                "relative_import_ratio",
                "absolute_import_ratio",
                "aliasing_ratio",
            ),
        )

    # Tests organization.
    if isinstance(tests, Mapping) and tests:
        parts.append("Test organization:")
        _append_keys(
            section_obj_=tests,
            keys=(
                "test_org_preference",
                "tests_separate_count",
                "tests_colocated_count",
                "tests_colocated_ratio",
            ),
        )

    parts.append(
        "If evidence is missing for a category, make the best effort that matches the project style you observe "
        "in-context."
    )

    return _cap_utf8("\n".join(parts).strip() + "\n", max_bytes=max_bytes)


def build_conventions_system_preamble(*, project_root: Path | None) -> str | None:
    """Load + render a conventions system preamble for compile prompts."""
    if project_root is None:
        return None
    try:
        resolved = project_root.expanduser().resolve()
    except OSError:
        resolved = project_root
    conv = load_project_conventions(project_root=resolved)
    if conv is None:
        return None
    return render_conventions_system_preamble(conv)
