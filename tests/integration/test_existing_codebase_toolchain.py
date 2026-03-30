"""Light integration checks for native toolchain resolution on fixture layouts."""

from __future__ import annotations

from pathlib import Path

from akc.adopt.detect import detect_project_profile
from akc.adopt.toolchain import resolve_toolchain_profile
from akc.cli.existing_codebase import project_signals_native_toolchain_tests


def test_rust_fixture_resolves_cargo_test(tmp_path: Path) -> None:
    (tmp_path / "src" / "lib.rs").parent.mkdir(parents=True)
    (tmp_path / "src" / "lib.rs").write_text("pub fn x() -> i32 { 1 }\n", encoding="utf-8")
    (tmp_path / "Cargo.toml").write_text(
        '[package]\nname = "t"\nversion = "0.1.0"\nedition = "2021"\n',
        encoding="utf-8",
    )

    profile = detect_project_profile(root=tmp_path)
    resolved = resolve_toolchain_profile(extracted_profile=profile, explicit_toolchain=None)
    assert resolved.test_command[0] == "cargo"
    assert project_signals_native_toolchain_tests(tmp_path) is True


def test_node_fixture_signals_native(tmp_path: Path) -> None:
    (tmp_path / "package.json").write_text('{"scripts":{"test":"jest"}}', encoding="utf-8")
    assert project_signals_native_toolchain_tests(tmp_path) is True


def test_monorepo_fixture_prefers_ci_workspace_command(tmp_path: Path) -> None:
    (tmp_path / ".github" / "workflows").mkdir(parents=True)
    (tmp_path / "packages" / "web" / "src").mkdir(parents=True)
    (tmp_path / "pnpm-workspace.yaml").write_text("packages:\n  - packages/*\n", encoding="utf-8")
    (tmp_path / "package.json").write_text(
        '{"packageManager":"pnpm@10","workspaces":["packages/*"],"scripts":{"ci:test":"pnpm nx run-many -t test"}}',
        encoding="utf-8",
    )
    (tmp_path / "packages" / "web" / "src" / "index.ts").write_text("export const x = 1;\n", encoding="utf-8")
    (tmp_path / ".github" / "workflows" / "ci.yml").write_text(
        """
        jobs:
          verify:
            steps:
              - name: Workspace tests
                run: pnpm turbo run test
        """.lstrip(),
        encoding="utf-8",
    )

    profile = detect_project_profile(root=tmp_path)
    resolved = resolve_toolchain_profile(extracted_profile=profile, explicit_toolchain=None)
    assert resolved.package_manager == "pnpm"
    assert resolved.test_command == ["pnpm", "turbo", "run", "test"]
