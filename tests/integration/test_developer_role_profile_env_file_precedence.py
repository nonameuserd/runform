from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.cli import main


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


def _executor_cwd(outputs_root: Path, tenant_id: str, repo_id: str) -> Path:
    base = outputs_root / tenant_id / repo_id
    return base / tenant_id / repo_id


def test_compile_uses_developer_role_from_project_json_when_flag_omitted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """File default applies when CLI omits --developer-role-profile."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".akc").mkdir(parents=True)
    tenant_id = "t-file"
    repo_id = "r-file"
    outputs_root = tmp_path / "out"
    (tmp_path / ".akc" / "project.json").write_text(
        json.dumps({"developer_role_profile": "emerging"}),
        encoding="utf-8",
    )
    _write_minimal_repo(_executor_cwd(outputs_root, tenant_id, repo_id))

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
    base = outputs_root / tenant_id / repo_id
    run_manifest_path = next((base / ".akc" / "run").glob("*.manifest.json"))
    payload = json.loads(run_manifest_path.read_text(encoding="utf-8"))
    cp = payload.get("control_plane") or {}
    assert cp.get("developer_role_profile") == "emerging"


def test_compile_scope_fields_from_project_json_without_cli_flags(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """tenant_id, repo_id, outputs_root can come from .akc/project.json."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".akc").mkdir(parents=True)
    tenant_id = "t-scope"
    repo_id = "r-scope"
    outputs_root = tmp_path / "out"
    (tmp_path / ".akc" / "project.json").write_text(
        json.dumps(
            {
                "tenant_id": tenant_id,
                "repo_id": repo_id,
                "outputs_root": str(outputs_root),
                "developer_role_profile": "classic",
            }
        ),
        encoding="utf-8",
    )
    _write_minimal_repo(_executor_cwd(outputs_root, tenant_id, repo_id))

    with pytest.raises(SystemExit) as excinfo:
        main(["compile", "--mode", "quick"])
    assert excinfo.value.code == 0
    base = outputs_root / tenant_id / repo_id
    assert (base / "manifest.json").exists()


def test_compile_env_overrides_project_file_developer_role(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """AKC_DEVELOPER_ROLE_PROFILE wins over .akc/project.json."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".akc").mkdir(parents=True)
    tenant_id = "t-env"
    repo_id = "r-env"
    outputs_root = tmp_path / "out"
    (tmp_path / ".akc" / "project.json").write_text(
        json.dumps({"developer_role_profile": "emerging"}),
        encoding="utf-8",
    )
    monkeypatch.setenv("AKC_DEVELOPER_ROLE_PROFILE", "classic")
    _write_minimal_repo(_executor_cwd(outputs_root, tenant_id, repo_id))

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
    base = outputs_root / tenant_id / repo_id
    run_manifest_path = next((base / ".akc" / "run").glob("*.manifest.json"))
    payload = json.loads(run_manifest_path.read_text(encoding="utf-8"))
    cp = payload.get("control_plane") or {}
    assert cp.get("developer_role_profile") == "classic"
