from __future__ import annotations

import json
from pathlib import Path

from akc.cli.existing_codebase import (
    FORCE_LEGACY_COMPILE_TEST_MODE_ENV,
    project_signals_native_toolchain_tests,
)


def test_signals_native_when_project_profile_has_build_commands(tmp_path: Path) -> None:
    akc = tmp_path / ".akc"
    akc.mkdir(parents=True)
    (akc / "project_profile.json").write_text(
        json.dumps({"build_commands": [{"command": ["npm", "test"], "kind": "test"}]}),
        encoding="utf-8",
    )
    assert project_signals_native_toolchain_tests(tmp_path) is True


def test_signals_native_when_package_json_at_root(tmp_path: Path) -> None:
    (tmp_path / "package.json").write_text('{"name":"x"}', encoding="utf-8")
    assert project_signals_native_toolchain_tests(tmp_path) is True


def test_signals_false_for_empty_docs_only_tree(tmp_path: Path) -> None:
    (tmp_path / "README.md").write_text("hello", encoding="utf-8")
    assert project_signals_native_toolchain_tests(tmp_path) is False


def test_signals_native_for_workspace_manifest_without_root_package_json(tmp_path: Path) -> None:
    (tmp_path / "packages" / "api").mkdir(parents=True)
    (tmp_path / "packages" / "api" / "package.json").write_text('{"name":"api"}', encoding="utf-8")
    (tmp_path / "pnpm-workspace.yaml").write_text("packages:\n  - packages/*\n", encoding="utf-8")
    assert project_signals_native_toolchain_tests(tmp_path) is True


def test_native_test_mode_flag_signals(tmp_path: Path) -> None:
    assert project_signals_native_toolchain_tests(tmp_path, native_test_mode=True) is True


def test_force_legacy_env_constant_is_stable() -> None:
    assert FORCE_LEGACY_COMPILE_TEST_MODE_ENV == "AKC_COMPILE_FORCE_LEGACY_TEST_MODE"
