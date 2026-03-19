from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.artifacts.validate import validate_obj
from akc.cli import main
from akc.memory.facade import build_memory


def _write_minimal_repo(root: Path) -> None:
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


def _seed_plan_with_one_step(*, tenant_id: str, repo_id: str, outputs_root: Path) -> None:
    base = outputs_root / tenant_id / repo_id
    memory_db = base / ".akc" / "memory.sqlite"
    memory_db.parent.mkdir(parents=True, exist_ok=True)
    mem = build_memory(backend="sqlite", sqlite_path=str(memory_db))
    plan = mem.plan_state.create_plan(
        tenant_id=tenant_id,
        repo_id=repo_id,
        goal="Compile repository",
        initial_steps=["Implement goal"],
    )
    mem.plan_state.set_active_plan(tenant_id=tenant_id, repo_id=repo_id, plan_id=plan.id)


def _executor_cwd(outputs_root: Path, tenant_id: str, repo_id: str) -> Path:
    base = outputs_root / tenant_id / repo_id
    return base / tenant_id / repo_id


def test_manifest_and_evidence_json_validate(tmp_path: Path) -> None:
    tenant_id = "schema-tenant"
    repo_id = "schema-repo"
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
                "--schema-version",
                "1",
                "--mode",
                "quick",
            ]
        )
    assert excinfo.value.code == 0

    manifest_path = base / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert validate_obj(obj=manifest, kind="manifest", version=1) == []

    tests_dir = base / ".akc" / "tests"
    for p in sorted(tests_dir.rglob("*.json")):
        payload = json.loads(p.read_text(encoding="utf-8"))
        issues = validate_obj(obj=payload, kind="execution_stage", version=1)
        assert issues == [], f"{p} schema issues: {issues}"

    ver_dir = base / ".akc" / "verification"
    if ver_dir.is_dir():
        for p in sorted(ver_dir.rglob("*.json")):
            payload = json.loads(p.read_text(encoding="utf-8"))
            issues = validate_obj(obj=payload, kind="verifier_result", version=1)
            assert issues == [], f"{p} schema issues: {issues}"
