from __future__ import annotations

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


def test_subprocess_executor_truncates_output_to_caps(tmp_path: Path) -> None:
    ex = SubprocessExecutor(work_root=tmp_path, stdout_max_bytes=10, stderr_max_bytes=10)
    scope = TenantRepoScope(tenant_id="t1", repo_id="repo1")

    res = ex.run(
        scope=scope,
        request=ExecutionRequest(
            command=[
                "python",
                "-c",
                "import sys; sys.stdout.write('a'*100); sys.stderr.write('b'*100)",
            ]
        ),
    )

    assert res.exit_code == 0
    assert res.stdout.startswith("a" * 10)
    assert "\n...[truncated]..." in res.stdout
    assert res.stderr.startswith("b" * 10)
    assert "\n...[truncated]..." in res.stderr


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


def test_subprocess_executor_does_not_create_cwd_outside_root(tmp_path: Path) -> None:
    ex = SubprocessExecutor(work_root=tmp_path)
    scope = TenantRepoScope(tenant_id="t1", repo_id="repo1")

    outside_target = tmp_path.parent / f"akc_test_outside_{tmp_path.name}_do_not_create"
    assert not outside_target.exists()

    with pytest.raises(ValueError, match="within executor work_root"):
        ex.run(
            scope=scope,
            request=ExecutionRequest(
                command=["python", "-c", "print('x')"],
                cwd=str(outside_target),
            ),
        )

    # Critical: rejecting the cwd must not create host directories.
    assert not outside_target.exists()


def test_docker_executor_builds_expected_docker_command(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    calls: dict[str, Any] = {}

    def _fake_helper(
        *,
        command: list[str],
        cwd: str,
        env: Any,
        stdin_text: Any,
        timeout_s: Any,
        preexec_fn: Any,
        stdout_max_bytes: Any,
        stderr_max_bytes: Any,
    ) -> tuple[int, str, str, int]:
        # Capture the constructed docker command.
        calls["cmd"] = command
        calls["cwd"] = cwd
        calls["env"] = env
        calls["stdin_text"] = stdin_text
        calls["timeout_s"] = timeout_s
        calls["preexec_fn"] = preexec_fn
        calls["stdout_max_bytes"] = stdout_max_bytes
        calls["stderr_max_bytes"] = stderr_max_bytes
        return 0, "ok", "", 1

    import akc.compile.executors as executors_mod

    monkeypatch.setattr(executors_mod, "_run_subprocess_capture_output_with_limits", _fake_helper)

    ex = DockerExecutor(
        work_root=tmp_path,
        image="python:3.12-slim",
        disable_network=True,
        memory_bytes=256 * 1024 * 1024,
        pids_limit=64,
        cpus=1.0,
        read_only_rootfs=True,
        no_new_privileges=True,
        cap_drop_all=True,
        user="65532:65532",
        tmpfs_mounts=("/tmp", "/var/tmp"),
        seccomp_profile="/profiles/default-seccomp.json",
        apparmor_profile="akc-default",
        ulimit_nofile="1024:2048",
        ulimit_nproc="512",
    )
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
    assert "--read-only" in cmd
    assert "--security-opt" in cmd and "no-new-privileges" in cmd
    assert "--cap-drop" in cmd and "ALL" in cmd
    assert "--memory" in cmd
    assert "--pids-limit" in cmd
    assert "--cpus" in cmd
    assert "--user" in cmd and "65532:65532" in cmd
    assert cmd.count("--tmpfs") == 2
    assert "/tmp" in cmd and "/var/tmp" in cmd
    assert cmd.count("--security-opt") == 3
    assert "seccomp=/profiles/default-seccomp.json" in cmd
    assert "apparmor=akc-default" in cmd
    assert cmd.count("--ulimit") == 2
    assert "nofile=1024:2048" in cmd
    assert "nproc=512" in cmd
    assert "PYTHONDONTWRITEBYTECODE=1" in cmd
    assert f"PYTHONPYCACHEPREFIX={ex.container_workdir}/.pycache" in cmd
    assert f"PYTEST_ADDOPTS=--cache-dir={ex.container_workdir}/.pytest_cache" in cmd
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


def test_docker_executor_returns_streaming_helper_outputs_and_respects_caps(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    ex = DockerExecutor(work_root=tmp_path, stdout_max_bytes=10, stderr_max_bytes=10)
    scope = TenantRepoScope(tenant_id="t1", repo_id="repo1")

    def _fake_helper(
        *,
        command: list[str],
        cwd: str,
        env: Any,
        stdin_text: Any,
        timeout_s: Any,
        preexec_fn: Any,
        stdout_max_bytes: Any,
        stderr_max_bytes: Any,
    ) -> tuple[int, str, str, int]:
        assert stdout_max_bytes == 10
        assert stderr_max_bytes == 10
        return 0, ("a" * 10 + "\n...[truncated]..."), ("b" * 10 + "\n...[truncated]..."), 1

    import akc.compile.executors as executors_mod

    monkeypatch.setattr(executors_mod, "_run_subprocess_capture_output_with_limits", _fake_helper)

    res = ex.run(
        scope=scope,
        request=ExecutionRequest(
            command=["python", "-c", "print('ok')"],
        ),
    )

    assert res.exit_code == 0
    assert "\n...[truncated]..." in res.stdout
    assert "\n...[truncated]..." in res.stderr


def test_docker_executor_run_dir_is_bind_mounted_writable_for_non_root_user(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    calls: dict[str, Any] = {}

    def _fake_helper(
        *,
        command: list[str],
        cwd: str,
        env: Any,
        stdin_text: Any,
        timeout_s: Any,
        preexec_fn: Any,
        stdout_max_bytes: Any,
        stderr_max_bytes: Any,
    ) -> tuple[int, str, str, int]:
        calls["cmd"] = command
        return 0, "ok", "", 1

    import akc.compile.executors as executors_mod

    monkeypatch.setattr(executors_mod, "_run_subprocess_capture_output_with_limits", _fake_helper)

    ex = DockerExecutor(work_root=tmp_path)
    scope = TenantRepoScope(tenant_id="t1", repo_id="repo1")
    run_id = "run_123"
    res = ex.run(
        scope=scope,
        request=ExecutionRequest(
            command=["python", "-c", "print('ok')"],
            run_id=run_id,
        ),
    )

    assert res.exit_code == 0
    cmd = calls["cmd"]
    run_dir = tmp_path / "t1" / "repo1" / run_id
    assert f"{str(run_dir.resolve())}:{ex.container_run_dir}:rw" in cmd
    assert f"HOME={ex.container_run_dir}" in cmd
    assert f"PYTEST_ADDOPTS=--cache-dir={ex.container_run_dir}/.pytest_cache" in cmd


def test_docker_executor_does_not_create_cwd_outside_root(tmp_path: Path) -> None:
    ex = DockerExecutor(work_root=tmp_path)
    scope = TenantRepoScope(tenant_id="t1", repo_id="repo1")

    outside_target = tmp_path.parent / f"akc_test_outside_{tmp_path.name}_docker_do_not_create"
    assert not outside_target.exists()

    with pytest.raises(ValueError, match="within executor work_root"):
        ex.run(
            scope=scope,
            request=ExecutionRequest(
                command=["python", "-c", "print('x')"],
                cwd=str(outside_target),
            ),
        )

    # Critical: rejecting the cwd must not create host directories.
    assert not outside_target.exists()


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"user": "65532:bad group"}, "docker user"),
        ({"tmpfs_mounts": ("tmp",)}, "absolute container path"),
        ({"tmpfs_mounts": ("/../tmp",)}, "unsafe path segments"),
        ({"tmpfs_mounts": ("/",)}, "cannot target container root"),
        ({"seccomp_profile": "bad profile"}, "cannot contain whitespace"),
        ({"seccomp_profile": "/tmp/../seccomp.json"}, "unsafe path segments"),
        ({"apparmor_profile": "/etc/apparmor.d/profile"}, "profile identifier"),
        ({"apparmor_profile": "bad profile"}, "cannot contain whitespace"),
        ({"ulimit_nofile": "10:5"}, "soft limit cannot exceed hard limit"),
        ({"ulimit_nofile": "0"}, "greater than zero"),
        ({"ulimit_nproc": "abc"}, "must be '<soft>' or '<soft>:<hard>'"),
        ({"ulimit_nproc": "1024:1048577"}, "cannot exceed"),
    ],
)
def test_docker_executor_rejects_invalid_hardening_values_before_launch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    kwargs: dict[str, Any],
    match: str,
) -> None:
    called = False

    def _fake_helper(**_kwargs: Any) -> tuple[int, str, str, int]:
        nonlocal called
        called = True
        return 0, "ok", "", 1

    import akc.compile.executors as executors_mod

    monkeypatch.setattr(executors_mod, "_run_subprocess_capture_output_with_limits", _fake_helper)

    ex = DockerExecutor(work_root=tmp_path, **kwargs)
    scope = TenantRepoScope(tenant_id="t1", repo_id="repo1")

    with pytest.raises(ValueError, match=match):
        ex.run(
            scope=scope,
            request=ExecutionRequest(command=["python", "-c", "print('ok')"]),
        )

    assert called is False
