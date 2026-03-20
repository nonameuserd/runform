from __future__ import annotations

from akc.compile.executors import DockerExecutor
from akc.execute.factory import SandboxFactoryConfig, create_sandbox_executor
from akc.execute.strong import SandboxStrongConfig, create_strong_underlying_executor


def test_create_strong_underlying_executor_forwards_docker_hardening() -> None:
    executor = create_strong_underlying_executor(
        cfg=SandboxStrongConfig(
            strong_lane_preference="docker",
            docker_user="1234:1234",
            docker_tmpfs_mounts=("/tmp", "/var/tmp"),
            docker_seccomp_profile="/profiles/seccomp.json",
            docker_apparmor_profile="akc-profile",
            docker_ulimit_nofile="1024:2048",
            docker_ulimit_nproc="512",
        )
    )

    assert isinstance(executor, DockerExecutor)
    assert executor.user == "1234:1234"
    assert executor.tmpfs_mounts == ("/tmp", "/var/tmp")
    assert executor.seccomp_profile == "/profiles/seccomp.json"
    assert executor.apparmor_profile == "akc-profile"
    assert executor.ulimit_nofile == "1024:2048"
    assert executor.ulimit_nproc == "512"


def test_create_sandbox_executor_forwards_docker_hardening() -> None:
    executor = create_sandbox_executor(
        cfg=SandboxFactoryConfig(
            sandbox_mode="strong",
            strong_lane_preference="docker",
            docker_user="1234:1234",
            docker_tmpfs_mounts=("/tmp",),
            docker_seccomp_profile="/profiles/seccomp.json",
            docker_apparmor_profile="akc-profile",
            docker_ulimit_nofile="1024",
            docker_ulimit_nproc="256:512",
        )
    )

    underlying = executor.underlying
    assert isinstance(underlying, DockerExecutor)
    assert underlying.user == "1234:1234"
    assert underlying.tmpfs_mounts == ("/tmp",)
    assert underlying.seccomp_profile == "/profiles/seccomp.json"
    assert underlying.apparmor_profile == "akc-profile"
    assert underlying.ulimit_nofile == "1024"
    assert underlying.ulimit_nproc == "256:512"
