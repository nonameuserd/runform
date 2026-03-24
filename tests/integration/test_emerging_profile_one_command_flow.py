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


def test_emerging_profile_end_to_end_ingest_compile_verify_runtime_flow(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Golden path: env carries developer-role profile; minimal CLI flags (see docs/getting-started.md)."""

    outputs_root = tmp_path / "out"
    tenant_id = "tenant-emerging"
    repo_id = "repo-emerging"
    docs_dir = tmp_path / "docs"
    docs_dir.mkdir(parents=True, exist_ok=True)
    (docs_dir / "guide.md").write_text("# Service guide\n\nUse deterministic defaults.\n", encoding="utf-8")

    _write_minimal_repo(_executor_cwd(outputs_root, tenant_id, repo_id))

    monkeypatch.setenv("AKC_DEVELOPER_ROLE_PROFILE", "emerging")
    monkeypatch.setenv("AKC_TENANT_ID", tenant_id)
    monkeypatch.setenv("AKC_REPO_ID", repo_id)
    monkeypatch.setenv("AKC_OUTPUTS_ROOT", str(outputs_root))

    with pytest.raises(SystemExit) as ingest_exit:
        main(
            [
                "ingest",
                "--connector",
                "docs",
                "--input",
                str(docs_dir),
                "--no-index",
            ]
        )
    assert ingest_exit.value.code == 0

    with pytest.raises(SystemExit) as compile_exit:
        main(
            [
                "compile",
                "--mode",
                "quick",
            ]
        )
    assert compile_exit.value.code == 0

    with pytest.raises(SystemExit) as verify_exit:
        main(["verify"])
    assert verify_exit.value.code == 0

    verif_dir = outputs_root / tenant_id / repo_id / ".akc" / "verification"
    dev_ctx = verif_dir / "verify_developer_context.v1.json"
    assert dev_ctx.is_file()
    ctx_obj = json.loads(dev_ctx.read_text(encoding="utf-8"))
    assert ctx_obj.get("developer_role_profile") == "emerging"
    assert ctx_obj.get("tenant_id_resolution_source") == "env"
    assert ctx_obj.get("repo_id_resolution_source") == "env"
    assert ctx_obj.get("outputs_root_resolution_source") == "env"

    with pytest.raises(SystemExit) as runtime_exit:
        main(["runtime", "start"])
    assert runtime_exit.value.code == 0

    scope = outputs_root / tenant_id / repo_id
    run_manifests = sorted((scope / ".akc" / "run").glob("*.manifest.json"))
    assert run_manifests
    latest_manifest = json.loads(run_manifests[-1].read_text(encoding="utf-8"))
    control_plane = latest_manifest.get("control_plane") or {}
    assert control_plane.get("developer_role_profile") == "emerging"
    assert isinstance(control_plane.get("developer_profile_decisions_ref"), dict)
    assert isinstance(control_plane.get("runtime_run_id"), str)
