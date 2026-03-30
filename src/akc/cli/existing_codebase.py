"""Heuristics for treating real repositories as first-class compile inputs.

See docs/akc-vision.md (Critical Path: Existing Codebases).
"""

from __future__ import annotations

import json
import os
from pathlib import Path

# When set to "1", `akc compile` uses legacy implicit test modes (pytest-oriented
# smoke/full) unless `--test-mode` is passed explicitly.
FORCE_LEGACY_COMPILE_TEST_MODE_ENV = "AKC_COMPILE_FORCE_LEGACY_TEST_MODE"

# Root manifests that strongly imply a native toolchain (polyglot / non-trivial).
_ROOT_NATIVE_MANIFEST_FILES: frozenset[str] = frozenset(
    {
        "package.json",
        "pnpm-workspace.yaml",
        "nx.json",
        "turbo.json",
        "Cargo.toml",
        "go.mod",
        "pom.xml",
        "build.gradle",
        "build.gradle.kts",
    },
)


def force_legacy_compile_test_mode_from_env() -> bool:
    return os.environ.get(FORCE_LEGACY_COMPILE_TEST_MODE_ENV, "").strip() == "1"


def project_signals_native_toolchain_tests(
    project_root: Path,
    *,
    native_test_mode: bool | None = None,
) -> bool:
    """Return True when the repo should default to native_smoke/native_full (see compile CLI).

    Precedence for callers: explicit CLI ``--test-mode`` overrides; this only guides defaults.
    """

    if native_test_mode is True:
        return True

    root = project_root.expanduser().resolve()
    profile_path = root / ".akc" / "project_profile.json"
    if profile_path.is_file():
        try:
            raw = json.loads(profile_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            raw = None
        if isinstance(raw, dict):
            bc = raw.get("build_commands")
            if isinstance(bc, list) and len(bc) > 0:
                return True
            langs = raw.get("languages")
            if isinstance(langs, list) and len(langs) > 0:
                return True
            package_managers = raw.get("package_managers")
            if isinstance(package_managers, list) and len(package_managers) > 0:
                return True
            architecture_hints = raw.get("architecture_hints")
            if isinstance(architecture_hints, dict) and bool(architecture_hints.get("monorepo")):
                return True

    try:
        from akc.adopt.detect import detect_project_profile

        extracted = detect_project_profile(root=root)
    except Exception:
        extracted = None

    if extracted is not None and extracted.build_commands:
        return True
    if extracted is not None and (extracted.languages or extracted.package_managers):
        return True
    if extracted is not None and bool(extracted.architecture_hints.get("monorepo")):
        return True

    return any((root / name).is_file() for name in _ROOT_NATIVE_MANIFEST_FILES)
