from __future__ import annotations

import json
from pathlib import Path

from akc.adopt.profile import (
    BuildCommand,
    CISystem,
    ConventionSnapshot,
    LanguageEntry,
    ProjectProfile,
)
from akc.adopt.toolchain import resolve_toolchain_profile
from akc.cli.project_config import load_akc_project_config


def _profile(
    *,
    root: Path,
    languages: list[LanguageEntry],
    package_managers: list[str],
    build_commands: list[BuildCommand],
) -> ProjectProfile:
    return ProjectProfile(
        root=root,
        languages=languages,
        package_managers=package_managers,
        build_commands=build_commands,
        ci_systems=[CISystem(name="none")],
        conventions=ConventionSnapshot(),
        entry_points=[],
        architecture_hints={},
    )


def test_conventional_python_when_extracted_has_no_test_command(tmp_path: Path) -> None:
    profile = _profile(
        root=tmp_path,
        languages=[LanguageEntry(language="python", percent=100.0, bytes=1, files=1)],
        package_managers=["pip_or_py"],
        build_commands=[],
    )

    resolved = resolve_toolchain_profile(extracted_profile=profile, explicit_toolchain=None)
    assert resolved.language == "python"
    assert resolved.package_manager == "pip"
    assert resolved.test_command == ["pytest", "-x"]
    assert resolved.lint_command == ["ruff", "check", "."]
    assert resolved.format_command == ["ruff", "format", "--check", "."]
    # Required binaries are best-effort; at minimum we should see the core runtime.
    assert "python3" in resolved.required_binaries
    assert "pytest" in resolved.required_binaries


def test_extracted_test_command_overrides_conventional(tmp_path: Path) -> None:
    profile = _profile(
        root=tmp_path,
        languages=[LanguageEntry(language="python", percent=100.0, bytes=1, files=1)],
        package_managers=["pip_or_py"],
        build_commands=[BuildCommand(command=("pytest", "-q"), kind="test", source="detected")],
    )

    resolved = resolve_toolchain_profile(extracted_profile=profile, explicit_toolchain=None)
    assert resolved.test_command == ["pytest", "-q"]
    # Unprovided fields should still be conventional defaults.
    assert resolved.lint_command == ["ruff", "check", "."]


def test_explicit_overrides_language_and_test_command(tmp_path: Path) -> None:
    profile = _profile(
        root=tmp_path,
        languages=[LanguageEntry(language="python", percent=100.0, bytes=1, files=1)],
        package_managers=["pip_or_py"],
        build_commands=[BuildCommand(command=("pytest", "-q"), kind="test", source="detected")],
    )

    resolved = resolve_toolchain_profile(
        extracted_profile=profile,
        explicit_toolchain={
            "language": "rust",
            "package_manager": "cargo",
            "test_command": ["cargo", "test", "--nocapture"],
            "build_command": ["cargo", "build"],
        },
    )

    assert resolved.language == "rust"
    assert resolved.package_manager == "cargo"
    assert resolved.test_command == ["cargo", "test", "--nocapture"]
    assert resolved.build_command == ["cargo", "build"]
    assert resolved.lint_command is None
    assert resolved.format_command is None
    assert "cargo" in resolved.required_binaries
    assert "python3" not in resolved.required_binaries


def test_extracted_package_manager_infers_pnpm_install_command(tmp_path: Path) -> None:
    (tmp_path / "pnpm-lock.yaml").write_text("lockfileVersion: 6\n", encoding="utf-8")

    profile = _profile(
        root=tmp_path,
        languages=[LanguageEntry(language="javascript", percent=100.0, bytes=1, files=1)],
        package_managers=["npm_or_node", "pnpm"],
        build_commands=[],
    )

    resolved = resolve_toolchain_profile(extracted_profile=profile, explicit_toolchain=None)
    assert resolved.language == "javascript"
    assert resolved.package_manager == "pnpm"
    assert resolved.install_command == ["pnpm", "install", "--frozen-lockfile"]


def test_project_config_loads_toolchain_mapping(tmp_path: Path) -> None:
    (tmp_path / ".akc").mkdir(parents=True, exist_ok=True)
    (tmp_path / ".akc" / "project.json").write_text(
        json.dumps(
            {
                "tenant_id": "t",
                "repo_id": "r",
                "outputs_root": "out",
                "toolchain": {"language": "python", "test_command": ["pytest", "-x"]},
            }
        ),
        encoding="utf-8",
    )

    cfg = load_akc_project_config(tmp_path)
    assert cfg is not None
    assert cfg.toolchain == {"language": "python", "test_command": ["pytest", "-x"]}
