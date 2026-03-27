from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

from akc.compile.scoped_apply import (
    git_scoped_apply_recovery_notes,
    preflight_scoped_apply,
    run_scoped_apply_pipeline,
)


def test_git_recovery_notes_include_branch_and_snapshot() -> None:
    notes = git_scoped_apply_recovery_notes(
        scope_root="/repo",
        accounting_git={"branch_per_run": True, "branch_name": "akc/compile/abc", "commit": True},
        rollback_snapshot_ref={"path": ".akc/rollback_snapshots/abc"},
    )
    assert any("checkout -" in n for n in notes)
    assert any("reset --hard HEAD~1" in n for n in notes)
    assert any("rollback_snapshots" in n for n in notes)


def test_artifact_only_skips_apply() -> None:
    r = run_scoped_apply_pipeline(
        compile_realization_mode="artifact_only",
        apply_scope_root=None,
        patch_text="--- a/x\n+++ b/x\n@@\n+1\n",
        patch_sha256="abc",
    )
    assert r["attempted"] is False
    assert r["applied"] is False
    assert r["deny_reason"] is None


def test_scoped_apply_denies_without_scope_root() -> None:
    r = run_scoped_apply_pipeline(
        compile_realization_mode="scoped_apply",
        apply_scope_root=None,
        patch_text="--- a/x\n+++ b/x\n@@\n+1\n",
        patch_sha256="abc",
    )
    assert r["attempted"] is True
    assert r["applied"] is False
    assert r["deny_reason"] == "apply_scope_root_unset"
    assert r["policy_blocked"] is True


def test_preflight_rejects_dot_akc(tmp_path: Path) -> None:
    patch = "--- a/.akc/foo\n+++ b/.akc/foo\n@@\n+1\n"
    ok, reason, parsed = preflight_scoped_apply(patch_text=patch, scope_root=tmp_path)
    assert ok is False
    assert reason == "patch_touches_internal_artifacts"
    assert parsed is None


def test_preflight_rejects_traversal(tmp_path: Path) -> None:
    patch = "--- a/../etc/passwd\n+++ b/../etc/passwd\n@@\n+1\n"
    ok, reason, parsed = preflight_scoped_apply(patch_text=patch, scope_root=tmp_path)
    assert ok is False
    assert parsed is None


def test_preflight_rejects_outside_mutation_allowlist(tmp_path: Path) -> None:
    patch = "--- a/README.md\n+++ b/README.md\n@@\n+1\n"
    ok, reason, parsed = preflight_scoped_apply(
        patch_text=patch,
        scope_root=tmp_path,
        mutation_paths=("src/", "tests/"),
    )
    assert ok is False
    assert reason == "path_not_in_mutation_allowlist"
    assert parsed is None


def test_scoped_apply_applies_minimal_patch(tmp_path: Path) -> None:
    (tmp_path / "src").mkdir(parents=True)
    (tmp_path / "src" / "f.txt").write_text("a\n", encoding="utf-8")
    patch = "\n".join(
        [
            "--- a/src/f.txt",
            "+++ b/src/f.txt",
            "@@ -1 +1 @@",
            "-a",
            "+b",
            "",
        ]
    )
    r = run_scoped_apply_pipeline(
        compile_realization_mode="scoped_apply",
        apply_scope_root=str(tmp_path.resolve()),
        patch_text=patch,
        patch_sha256="deadbeef",
        mutation_paths=("src/", "tests/"),
    )
    if shutil_which_patch() is None:
        pytest.skip("patch binary not available")
    assert r["applied"] is True, r
    assert (tmp_path / "src" / "f.txt").read_text(encoding="utf-8").strip() == "b"
    assert r["files"]
    assert r["files"][0]["path"] == "src/f.txt"
    assert r["files"][0]["sha256_before"] is not None
    assert r["files"][0]["sha256_after"] is not None


def test_scoped_apply_applies_patch_without_trailing_newline(tmp_path: Path) -> None:
    (tmp_path / "src").mkdir(parents=True)
    (tmp_path / "src" / "f.txt").write_text("a\n", encoding="utf-8")
    patch = "\n".join(
        [
            "--- a/src/f.txt",
            "+++ b/src/f.txt",
            "@@ -1 +1 @@",
            "-a",
            "+b",
        ]
    )
    r = run_scoped_apply_pipeline(
        compile_realization_mode="scoped_apply",
        apply_scope_root=str(tmp_path.resolve()),
        patch_text=patch,
        patch_sha256="deadbeef",
        mutation_paths=("src/", "tests/"),
    )
    if shutil_which_patch() is None:
        pytest.skip("patch binary not available")
    assert r["applied"] is True, r
    assert (tmp_path / "src" / "f.txt").read_text(encoding="utf-8").strip() == "b"


def shutil_which_patch() -> str | None:
    return shutil.which("patch")


def test_scoped_apply_git_branch_and_commit(tmp_path: Path) -> None:
    if shutil_which_patch() is None:
        pytest.skip("patch binary not available")
    if shutil.which("git") is None:
        pytest.skip("git binary not available")
    repo = tmp_path.resolve()
    subprocess.run(["git", "init"], cwd=repo, check=True, capture_output=True)
    subprocess.run(
        ["git", "config", "user.email", "akc-test@example.com"],
        cwd=repo,
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "akc-test"],
        cwd=repo,
        check=True,
        capture_output=True,
    )
    (repo / "src").mkdir(parents=True)
    (repo / "src" / "f.txt").write_text("a\n", encoding="utf-8")
    subprocess.run(["git", "add", "src/f.txt"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "commit", "-m", "init"], cwd=repo, check=True, capture_output=True)

    patch = "\n".join(
        [
            "--- a/src/f.txt",
            "+++ b/src/f.txt",
            "@@ -1 +1 @@",
            "-a",
            "+b",
            "",
        ]
    )
    r = run_scoped_apply_pipeline(
        compile_realization_mode="scoped_apply",
        apply_scope_root=str(repo),
        patch_text=patch,
        patch_sha256="deadbeefcafe",
        mutation_paths=("src/", "tests/"),
        git_branch_per_run=True,
        git_commit=True,
        git_commit_message="AKC test commit",
    )
    assert r.get("applied") is True, r
    assert (repo / "src" / "f.txt").read_text(encoding="utf-8").strip() == "b"
    branch = subprocess.run(
        ["git", "branch", "--show-current"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert branch.startswith("akc/compile/")
    head_msg = subprocess.run(
        ["git", "log", "-1", "--format=%s"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert head_msg == "AKC test commit"
