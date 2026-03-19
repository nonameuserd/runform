from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from akc.compile.executors import DockerExecutor, SubprocessExecutor
from akc.compile.interfaces import ExecutionRequest, TenantRepoScope


def test_subprocess_executor_runs_command_in_scoped_workdir(tmp_path: Path) -> None:
    ex = SubprocessExecutor(work_root=tmp_path)
    scope = TenantRepoScope(tenant_id="t1", repo_id="repo1")

    res = ex.run(scope=scope, request=ExecutionRequest(command=["python", "-c", "print('ok')"]))

    assert res.exit_code == 0
    assert "ok" in res.stdout
    assert res.duration_ms is not None and res.duration_ms >= 0
    # Ensure it created and used the scoped cwd.
    assert (tmp_path / "t1" / "repo1").exists()


def test_subprocess_executor_rejects_cwd_escape(tmp_path: Path) -> None:
    ex = SubprocessExecutor(work_root=tmp_path)
    scope = TenantRepoScope(tenant_id="t1", repo_id="repo1")

    outside = tmp_path.parent
    with pytest.raises(ValueError, match="within executor work_root"):
        ex.run(
            scope=scope,
            request=ExecutionRequest(
                command=["python", "-c", "print('x')"],
                cwd=str(outside),
            ),
        )


def test_docker_executor_builds_expected_docker_command(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    calls: dict[str, Any] = {}

    @dataclass
    class _CP:
        returncode: int = 0
        stdout: str = "ok"
        stderr: str = ""

    def _fake_run(cmd: list[str], **kwargs: Any) -> _CP:
        calls["cmd"] = cmd
        calls["kwargs"] = kwargs
        return _CP()

    import akc.compile.executors as executors_mod

    monkeypatch.setattr(executors_mod.subprocess, "run", _fake_run)

    ex = DockerExecutor(work_root=tmp_path, image="python:3.12-slim", disable_network=True)
    scope = TenantRepoScope(tenant_id="t1", repo_id="repo1")
    res = ex.run(
        scope=scope,
        request=ExecutionRequest(
            command=["python", "-c", "print('ok')"],
            env={"X": "1"},
        ),
    )

    assert res.exit_code == 0
    cmd = calls["cmd"]
    assert cmd[0:3] == ["docker", "run", "--rm"]
    assert "--network" in cmd and "none" in cmd
    assert ex.image in cmd
    # The container workdir should be present and command appended.
    assert "-w" in cmd and ex.container_workdir in cmd
    # The image is appended immediately before the command.
    assert cmd[-4] == ex.image
    assert cmd[-3:] == ["python", "-c", "print('ok')"]


def test_docker_executor_rejects_cwd_escape(tmp_path: Path) -> None:
    ex = DockerExecutor(work_root=tmp_path)
    scope = TenantRepoScope(tenant_id="t1", repo_id="repo1")

    outside = tmp_path.parent
    with pytest.raises(ValueError, match="within executor work_root"):
        ex.run(
            scope=scope,
            request=ExecutionRequest(
                command=["python", "-c", "print('x')"],
                cwd=str(outside),
            ),
        )
