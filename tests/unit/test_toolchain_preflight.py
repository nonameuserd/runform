from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from akc.adopt.toolchain import ToolchainPreflightError, ToolchainProfile, preflight_toolchain
from akc.compile import CompileSession
from akc.compile.controller_config import ControllerConfig, TierConfig


def _minimal_controller_config(*, toolchain: dict | None = None) -> ControllerConfig:
    cfg = ControllerConfig(
        tiers={"small": TierConfig(name="small", llm_model="offline-small", temperature=0.0)},
        stage_tiers={"generate": "small", "repair": "small"},
    )
    if toolchain is not None:
        cfg = replace(cfg, toolchain=toolchain)
    return cfg


def test_preflight_toolchain_missing_binary_fail_closed() -> None:
    prof = ToolchainProfile(
        language="python",
        package_manager="pip",
        test_command=["pytest", "-x"],
        typecheck_command=None,
        build_command=None,
        lint_command=None,
        format_command=None,
        install_command=None,
        required_binaries=["definitely_missing_binary_akc_test_12345"],
    )
    res = preflight_toolchain(prof)
    assert res.ok is False
    assert "definitely_missing_binary_akc_test_12345" in res.missing


def test_preflight_toolchain_version_probe_failure_fail_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    prof = ToolchainProfile(
        language="python",
        package_manager="pip",
        test_command=["pytest", "-x"],
        typecheck_command=None,
        build_command=None,
        lint_command=None,
        format_command=None,
        install_command=None,
        required_binaries=["somebin"],
    )

    monkeypatch.setattr("akc.adopt.toolchain.shutil.which", lambda _b: "/usr/bin/somebin")

    class _Proc:
        def __init__(self) -> None:
            self.returncode = 1
            self.stdout = ""
            self.stderr = ""

    monkeypatch.setattr("akc.adopt.toolchain.subprocess.run", lambda *_a, **_k: _Proc())

    res = preflight_toolchain(prof, timeout_s=0.01)
    assert res.ok is False
    assert "somebin" in res.version_errors


def test_compile_session_refuses_to_start_when_toolchain_preflight_fails(tmp_path: Path) -> None:
    # Ensure project_root exists; content doesn't matter because we provide explicit required_binaries.
    (tmp_path / "pyproject.toml").write_text("[project]\nname='x'\nversion='0.0.0'\n", encoding="utf-8")

    session = CompileSession.from_sqlite(
        tenant_id="t",
        repo_id="r",
        sqlite_path=str(tmp_path / "mem.sqlite"),
        index=None,
    )
    cfg = _minimal_controller_config(
        toolchain={
            "language": "python",
            "test_command": ["pytest", "-x"],
            "required_binaries": ["definitely_missing_binary_akc_test_67890"],
        }
    )
    with pytest.raises(ToolchainPreflightError):
        session.run(
            goal="do thing",
            llm=object(),  # preflight happens before llm is used
            executor=None,
            config=cfg,
            outputs_root=None,
            project_root=tmp_path,
        )
