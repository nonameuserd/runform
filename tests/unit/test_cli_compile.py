"""Unit tests for the `akc compile` CLI path.

Tests invoke the CLI entrypoint via akc.cli.main([...]) with offline/fake
backends and assert exit codes, output layout, and tenant isolation.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from akc.cli import main
from akc.memory.facade import build_memory


def _write_minimal_repo(root: Path) -> None:
    """Write a minimal Python package with passing pytest."""
    pkg = root / "src"
    tests = root / "tests"
    pkg.mkdir(parents=True, exist_ok=True)
    tests.mkdir(parents=True, exist_ok=True)
    (pkg / "__init__.py").write_text("", encoding="utf-8")
    (pkg / "module.py").write_text("VALUE = 1\n", encoding="utf-8")
    (tests / "test_module.py").write_text(
        "from src import module\n\n"
        "def test_smoke() -> None:\n"
        "    assert module.VALUE == 1\n",
        encoding="utf-8",
    )


def _write_failing_repo(root: Path) -> None:
    """Write a minimal repo whose tests always fail (for exit code 2 tests)."""
    pkg = root / "src"
    tests = root / "tests"
    pkg.mkdir(parents=True, exist_ok=True)
    tests.mkdir(parents=True, exist_ok=True)
    (pkg / "__init__.py").write_text("", encoding="utf-8")
    (tests / "test_fail.py").write_text("def test_always_fails(): assert False\n", encoding="utf-8")


def _seed_plan_with_one_step(
    *,
    tenant_id: str,
    repo_id: str,
    outputs_root: Path,
    goal: str = "Compile repository",
) -> None:
    """Pre-seed SQLite memory with an active plan that has one step.

    The CLI uses <outputs_root>/<tenant_id>/<repo_id>/.akc/memory.sqlite.
    Seeding a plan with one step causes the compile loop to run and emit
    manifest + .akc/tests when tests pass.
    """
    base = outputs_root / tenant_id / repo_id
    memory_db = base / ".akc" / "memory.sqlite"
    memory_db.parent.mkdir(parents=True, exist_ok=True)

    mem = build_memory(backend="sqlite", sqlite_path=str(memory_db))
    plan = mem.plan_state.create_plan(
        tenant_id=tenant_id,
        repo_id=repo_id,
        goal=goal,
        initial_steps=["Implement goal"],
    )
    mem.plan_state.set_active_plan(tenant_id=tenant_id, repo_id=repo_id, plan_id=plan.id)


def _executor_cwd(outputs_root: Path, tenant_id: str, repo_id: str) -> Path:
    """Path where the executor runs tests: work_root/tenant_id/repo_id (CLI sets work_root=base)."""
    base = outputs_root / tenant_id / repo_id
    return base / tenant_id / repo_id


def test_cli_compile_quick_mode_emits_manifest_and_tests(tmp_path: Path) -> None:
    """With a pre-seeded plan and passing tests, compile exits 0 and emits manifest + .akc/tests."""
    tenant_id = "t1"
    repo_id = "repo1"
    outputs_root = tmp_path
    base = outputs_root / tenant_id / repo_id
    _write_minimal_repo(_executor_cwd(outputs_root, tenant_id, repo_id))
    _seed_plan_with_one_step(tenant_id=tenant_id, repo_id=repo_id, outputs_root=outputs_root)

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "compile",
                "--tenant-id",
                tenant_id,
                "--repo-id",
                repo_id,
                "--outputs-root",
                str(outputs_root),
                "--mode",
                "quick",
            ]
        )
    assert excinfo.value.code == 0

    manifest = base / "manifest.json"
    assert manifest.exists(), "expected manifest.json to be emitted"

    tests_dir = base / ".akc" / "tests"
    assert tests_dir.is_dir()
    assert any(tests_dir.rglob("*.json")), "expected structured test artifacts under .akc/tests"


def test_cli_compile_empty_plan_exits_success_no_manifest(tmp_path: Path) -> None:
    """When the plan has no steps, compile exits 0 but does not emit a manifest."""
    tenant_id = "t1"
    repo_id = "repo1"
    outputs_root = tmp_path
    base = outputs_root / tenant_id / repo_id
    _write_minimal_repo(_executor_cwd(outputs_root, tenant_id, repo_id))
    # Do not seed a plan; create_or_resume_plan will create an empty plan.

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "compile",
                "--tenant-id",
                tenant_id,
                "--repo-id",
                repo_id,
                "--outputs-root",
                str(outputs_root),
                "--mode",
                "quick",
            ]
        )
    assert excinfo.value.code == 0

    # Empty plan succeeds immediately; session only emits manifest when best_candidate is set.
    manifest = base / "manifest.json"
    assert not manifest.exists(), "empty plan should not emit manifest"


def test_cli_compile_failing_tests_exit_code_2(tmp_path: Path) -> None:
    """When tests always fail, compile exits with code 2."""
    tenant_id = "t1"
    repo_id = "repo1"
    outputs_root = tmp_path
    _write_failing_repo(_executor_cwd(outputs_root, tenant_id, repo_id))
    _seed_plan_with_one_step(tenant_id=tenant_id, repo_id=repo_id, outputs_root=outputs_root)

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "compile",
                "--tenant-id",
                tenant_id,
                "--repo-id",
                repo_id,
                "--outputs-root",
                str(outputs_root),
                "--mode",
                "quick",
            ]
        )
    assert excinfo.value.code == 2


def test_cli_compile_missing_required_args_exits_non_zero() -> None:
    """Missing --tenant-id or --outputs-root results in non-zero exit (usage error)."""
    with pytest.raises(SystemExit) as excinfo:
        main(["compile", "--repo-id", "r1", "--outputs-root", "/tmp/out"])
    assert excinfo.value.code != 0

    with pytest.raises(SystemExit) as excinfo:
        main(["compile", "--tenant-id", "t1", "--repo-id", "r1"])
    assert excinfo.value.code != 0


def test_cli_compile_tenant_isolation(tmp_path: Path) -> None:
    """Outputs and artifacts are scoped under <outputs_root>/<tenant>/<repo>."""
    outputs_root = tmp_path
    t1_base = outputs_root / "tenant_a" / "repo1"
    t2_base = outputs_root / "tenant_b" / "repo1"
    _write_minimal_repo(_executor_cwd(outputs_root, "tenant_a", "repo1"))
    _seed_plan_with_one_step(tenant_id="tenant_a", repo_id="repo1", outputs_root=outputs_root)
    _write_minimal_repo(_executor_cwd(outputs_root, "tenant_b", "repo1"))
    _seed_plan_with_one_step(tenant_id="tenant_b", repo_id="repo1", outputs_root=outputs_root)

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "compile",
                "--tenant-id",
                "tenant_a",
                "--repo-id",
                "repo1",
                "--outputs-root",
                str(outputs_root),
                "--mode",
                "quick",
            ]
        )
    assert excinfo.value.code == 0
    with pytest.raises(SystemExit) as excinfo2:
        main(
            [
                "compile",
                "--tenant-id",
                "tenant_b",
                "--repo-id",
                "repo1",
                "--outputs-root",
                str(outputs_root),
                "--mode",
                "quick",
            ]
        )
    assert excinfo2.value.code == 0

    assert (t1_base / "manifest.json").exists()
    assert (t2_base / "manifest.json").exists()
    assert (t1_base / ".akc" / "tests").is_dir()
    assert (t2_base / ".akc" / "tests").is_dir()
    # No cross-contamination: no manifest at outputs root.
    assert not (outputs_root / "manifest.json").exists()


def test_cli_compile_thorough_mode_emits_manifest(tmp_path: Path) -> None:
    """--mode thorough runs and emits manifest when tests pass."""
    tenant_id = "t1"
    repo_id = "repo1"
    outputs_root = tmp_path
    base = outputs_root / tenant_id / repo_id
    _write_minimal_repo(_executor_cwd(outputs_root, tenant_id, repo_id))
    _seed_plan_with_one_step(tenant_id=tenant_id, repo_id=repo_id, outputs_root=outputs_root)

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "compile",
                "--tenant-id",
                tenant_id,
                "--repo-id",
                repo_id,
                "--outputs-root",
                str(outputs_root),
                "--mode",
                "thorough",
            ]
        )
    assert excinfo.value.code == 0
    assert (base / "manifest.json").exists()


def test_cli_compile_custom_goal_used(tmp_path: Path) -> None:
    """--goal is passed through and compile succeeds with it."""
    tenant_id = "t1"
    repo_id = "repo1"
    outputs_root = tmp_path
    base = outputs_root / tenant_id / repo_id
    _write_minimal_repo(_executor_cwd(outputs_root, tenant_id, repo_id))
    _seed_plan_with_one_step(tenant_id=tenant_id, repo_id=repo_id, outputs_root=outputs_root)

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "compile",
                "--tenant-id",
                tenant_id,
                "--repo-id",
                repo_id,
                "--outputs-root",
                str(outputs_root),
                "--goal",
                "Implement feature X",
                "--mode",
                "quick",
            ]
        )
    assert excinfo.value.code == 0
    assert (base / "manifest.json").exists()

