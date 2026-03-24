"""Integration tests for the `akc compile` CLI path.

Uses a temporary directory, runs `akc compile` with offline backends, and
confirms compile terminates successfully, manifest is emitted, and
.akc/tests (or other verification artifacts) are present.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.cli import main
from akc.memory.facade import build_memory


def _write_minimal_repo(root: Path) -> None:
    """Minimal Python package with passing pytest."""
    pkg = root / "src"
    tests = root / "tests"
    pkg.mkdir(parents=True, exist_ok=True)
    tests.mkdir(parents=True, exist_ok=True)
    (pkg / "__init__.py").write_text("", encoding="utf-8")
    (pkg / "module.py").write_text("VALUE = 1\n", encoding="utf-8")
    (tests / "test_module.py").write_text(
        "from src import module\n\ndef test_smoke() -> None:\n    assert module.VALUE == 1\n",
        encoding="utf-8",
    )


def _executor_cwd(outputs_root: Path, tenant_id: str, repo_id: str) -> Path:
    """Path where the executor runs tests (work_root/tenant_id/repo_id)."""
    base = outputs_root / tenant_id / repo_id
    return base / tenant_id / repo_id


def _seed_plan_with_one_step(
    *,
    tenant_id: str,
    repo_id: str,
    outputs_root: Path,
    goal: str = "Compile repository",
) -> None:
    """Pre-seed SQLite memory with an active plan that has one step."""
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


def test_cli_compile_integration_manifest_and_artifacts(tmp_path: Path) -> None:
    """Run akc compile in a temp dir; confirm scoped artifacts and manifest linkage."""
    tenant_id = "int-tenant"
    repo_id = "int-repo"
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
    assert excinfo.value.code == 0, "compile should exit 0 on success"

    manifest = base / "manifest.json"
    assert manifest.exists(), "manifest.json should be emitted"
    manifest_obj = json.loads(manifest.read_text(encoding="utf-8"))
    manifest_artifact_paths = {str(item.get("path")) for item in manifest_obj.get("artifacts", [])}

    tests_dir = base / ".akc" / "tests"
    assert tests_dir.is_dir(), ".akc/tests should exist"
    assert any(tests_dir.rglob("*.json")), "structured test artifacts under .akc/tests"

    run_dir = base / ".akc" / "run"
    expected_sidecars = {
        ".spans.json",
        ".otel.jsonl",
        ".costs.json",
        ".replay_decisions.json",
        ".recompile_triggers.json",
    }
    for suffix in expected_sidecars:
        matched = sorted(run_dir.glob(f"*{suffix}"))
        assert matched, f"expected at least one {suffix} under the tenant-scoped run dir"
        for path in matched:
            relpath = path.relative_to(base).as_posix()
            assert relpath in manifest_artifact_paths

    # Patches are emitted under .akc/patches when a candidate is accepted.
    assert (base / ".akc").is_dir(), ".akc directory should exist"
