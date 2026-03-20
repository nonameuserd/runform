from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

from akc.compile.executors import DockerExecutor
from akc.compile.interfaces import ExecutionRequest, TenantRepoScope


def _docker_runtime_available() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        proc = subprocess.run(
            ["docker", "info"],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return proc.returncode == 0


pytestmark = pytest.mark.skipif(
    not _docker_runtime_available(),
    reason="Docker runtime integration tests require a reachable local Docker daemon",
)


def _executor(tmp_path: Path) -> DockerExecutor:
    return DockerExecutor(
        work_root=tmp_path,
        image="python:3.12-slim",
        disable_network=True,
        read_only_rootfs=True,
        no_new_privileges=True,
        cap_drop_all=True,
        user="65532:65532",
        tmpfs_mounts=("/tmp",),
        stdout_max_bytes=128 * 1024,
        stderr_max_bytes=128 * 1024,
    )


def _scope() -> TenantRepoScope:
    return TenantRepoScope(tenant_id="docker-int", repo_id="hardening")


def test_docker_runtime_process_runs_as_non_root(tmp_path: Path) -> None:
    executor = _executor(tmp_path)

    result = executor.run(
        scope=_scope(),
        request=ExecutionRequest(
            command=[
                "python",
                "-c",
                "import os; print(f'{os.getuid()}:{os.getgid()}')",
            ]
        ),
    )

    assert result.exit_code == 0, result.stderr
    assert result.stdout.strip() == "65532:65532"


def test_docker_runtime_read_only_rootfs_blocks_writes_outside_tmpfs(tmp_path: Path) -> None:
    executor = _executor(tmp_path)

    result = executor.run(
        scope=_scope(),
        request=ExecutionRequest(
            command=[
                "python",
                "-c",
                (
                    "from pathlib import Path; "
                    "Path('/var/tmp/akc-blocked.txt').write_text('blocked', encoding='utf-8')"
                ),
            ]
        ),
    )

    assert result.exit_code != 0
    assert "Read-only file system" in result.stderr or "Errno 30" in result.stderr


def test_docker_runtime_tmpfs_mount_is_writable(tmp_path: Path) -> None:
    executor = _executor(tmp_path)

    result = executor.run(
        scope=_scope(),
        request=ExecutionRequest(
            command=[
                "python",
                "-c",
                (
                    "from pathlib import Path; "
                    "target = Path('/tmp/akc-write-ok.txt'); "
                    "target.write_text('ok', encoding='utf-8'); "
                    "print(target.read_text(encoding='utf-8'))"
                ),
            ]
        ),
    )

    assert result.exit_code == 0, result.stderr
    assert result.stdout.strip() == "ok"


def test_docker_runtime_network_isolation_blocks_outbound_connections(tmp_path: Path) -> None:
    executor = _executor(tmp_path)

    result = executor.run(
        scope=_scope(),
        request=ExecutionRequest(
            command=[
                "python",
                "-c",
                (
                    "import socket, sys; "
                    "sock = socket.socket(); "
                    "sock.settimeout(1.0); "
                    "try:\n"
                    "    sock.connect(('1.1.1.1', 53))\n"
                    "except OSError as exc:\n"
                    "    print(type(exc).__name__)\n"
                    "    sys.exit(0)\n"
                    "sys.exit('unexpected network access')"
                ),
            ]
        ),
    )

    assert result.exit_code == 0, result.stderr
    assert result.stdout.strip() in {"ConnectionRefusedError", "OSError", "TimeoutError"}
