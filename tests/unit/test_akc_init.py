from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from akc.cli import main
from akc.cli.init import _default_repo_id_for_path, _slug, cmd_init
from akc.cli.project_config import load_akc_project_config


def test_slug_normalizes() -> None:
    assert _slug("My Repo!") == "my-repo"
    assert _slug("___") == "repo"


def test_default_repo_id_for_path(tmp_path: Path) -> None:
    assert _default_repo_id_for_path(tmp_path / "deep" / "My-Project") == "my-project"


def test_cmd_init_writes_project_and_policy_stub(tmp_path: Path) -> None:
    ns = SimpleNamespace(
        directory=str(tmp_path),
        force=False,
        tenant_id="t1",
        repo_id="r1",
        outputs_root="out",
        developer_role_profile="emerging",
        policy_stub=True,
    )
    assert cmd_init(ns) == 0
    cfg = load_akc_project_config(tmp_path)
    assert cfg is not None
    assert cfg.developer_role_profile == "emerging"
    assert cfg.tenant_id == "t1"
    assert cfg.repo_id == "r1"
    assert cfg.outputs_root == "out"
    assert cfg.opa_policy_path == ".akc/policy/compile_tools.rego"
    assert cfg.opa_decision_path == "data.akc.allow"
    stub = tmp_path / ".akc" / "policy" / "compile_tools.rego"
    assert stub.is_file()
    assert "package akc" in stub.read_text(encoding="utf-8")


def test_cmd_init_no_policy_stub(tmp_path: Path) -> None:
    ns = SimpleNamespace(
        directory=str(tmp_path),
        force=False,
        tenant_id=None,
        repo_id=None,
        outputs_root=None,
        developer_role_profile="classic",
        policy_stub=False,
    )
    assert cmd_init(ns) == 0
    raw = json.loads((tmp_path / ".akc" / "project.json").read_text(encoding="utf-8"))
    assert "opa_policy_path" not in raw
    assert raw["developer_role_profile"] == "classic"


def test_cmd_init_refuses_overwrite_without_force(tmp_path: Path) -> None:
    akc = tmp_path / ".akc"
    akc.mkdir(parents=True)
    (akc / "project.json").write_text("{}", encoding="utf-8")
    ns = SimpleNamespace(
        directory=str(tmp_path),
        force=False,
        tenant_id=None,
        repo_id=None,
        outputs_root=None,
        developer_role_profile="emerging",
        policy_stub=False,
    )
    assert cmd_init(ns) == 2


def test_main_init_entrypoint(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    with pytest.raises(SystemExit) as exc:
        main(["init", "--no-policy-stub"])
    assert exc.value.code == 0
    cfg = load_akc_project_config(tmp_path)
    assert cfg is not None
    assert cfg.developer_role_profile == "emerging"
