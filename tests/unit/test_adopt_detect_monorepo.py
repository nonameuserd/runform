from __future__ import annotations

from pathlib import Path

from akc.adopt.detect import detect_project_profile


def test_detect_merges_scripts_from_multiple_package_json(tmp_path: Path) -> None:
    (tmp_path / "packages" / "a").mkdir(parents=True)
    (tmp_path / "packages" / "b").mkdir(parents=True)
    (tmp_path / "package.json").write_text(
        '{"scripts":{"test":"jest --rootDir packages/a"}}',
        encoding="utf-8",
    )
    (tmp_path / "packages" / "a" / "package.json").write_text(
        '{"scripts":{"test":"node -e \\"process.exit(0)\\""}}',
        encoding="utf-8",
    )
    (tmp_path / "packages" / "b" / "package.json").write_text(
        '{"scripts":{"lint":"eslint ."}}',
        encoding="utf-8",
    )

    profile = detect_project_profile(root=tmp_path)
    kinds = {c.kind for c in profile.build_commands}
    assert "test" in kinds
    assert "lint" in kinds
    assert profile.architecture_hints.get("suggested_mutation_paths")


def test_detect_respect_gitignore_skips_listed_dir(tmp_path: Path) -> None:
    (tmp_path / "node_modules" / "x").mkdir(parents=True)
    (tmp_path / "node_modules" / "x" / "package.json").write_text("{}", encoding="utf-8")
    (tmp_path / "good" / "pkg").mkdir(parents=True)
    (tmp_path / "good" / "pkg" / "package.json").write_text(
        '{"scripts":{"test":"jest"}}',
        encoding="utf-8",
    )
    (tmp_path / ".gitignore").write_text("node_modules\n", encoding="utf-8")

    with_git = detect_project_profile(root=tmp_path, respect_gitignore=True)
    without = detect_project_profile(root=tmp_path, respect_gitignore=False)
    assert len(with_git.build_commands) >= 1
    assert len(without.build_commands) >= len(with_git.build_commands)


def test_detect_workspace_scripts_and_ci_commands(tmp_path: Path) -> None:
    (tmp_path / ".github" / "workflows").mkdir(parents=True)
    (tmp_path / "packages" / "web").mkdir(parents=True)
    (tmp_path / "pnpm-workspace.yaml").write_text("packages:\n  - packages/*\n", encoding="utf-8")
    (tmp_path / "package.json").write_text(
        """
        {
          "packageManager": "pnpm@10.0.0",
          "workspaces": ["packages/*"],
          "scripts": {
            "ci:test": "pnpm nx run-many -t test",
            "check-types": "tsc --noEmit",
            "fmt:check": "prettier --check ."
          }
        }
        """,
        encoding="utf-8",
    )
    (tmp_path / ".github" / "workflows" / "ci.yml").write_text(
        """
        jobs:
          test:
            steps:
              - name: Monorepo build
                run: pnpm turbo run build
        """.lstrip(),
        encoding="utf-8",
    )

    profile = detect_project_profile(root=tmp_path)
    cmds = {(cmd.kind, cmd.command) for cmd in profile.build_commands}
    assert "pnpm" in profile.package_managers
    assert profile.architecture_hints.get("monorepo") is True
    assert "pnpm_workspaces" in profile.architecture_hints.get("workspace_tools", [])
    assert ("test", ("pnpm", "nx", "run-many", "-t", "test")) in cmds
    assert ("typecheck", ("tsc", "--noEmit")) in cmds
    assert ("format", ("prettier", "--check", ".")) in cmds
    assert ("build", ("pnpm", "turbo", "run", "build")) in cmds
