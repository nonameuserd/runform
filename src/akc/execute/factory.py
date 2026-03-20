from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from akc.compile.interfaces import Executor

from .dev import DevSandboxConfig, DevSandboxExecutor
from .secrets import SecretsScopeConfig
from .strong import SandboxStrongConfig, StrongSandboxExecutor, create_strong_underlying_executor

SandboxDevConfig = DevSandboxConfig

__all__ = [
    "SandboxDevConfig",
    "SandboxFactoryConfig",
    "SandboxStrongConfig",
    "create_sandbox_executor",
]


@dataclass(frozen=True, slots=True)
class SandboxFactoryConfig:
    """Single config object for selecting a sandbox boundary."""

    sandbox_mode: Literal["dev", "strong"] = "dev"
    work_root: str | None = None
    allow_network: bool = False
    memory_bytes: int | None = 1024 * 1024 * 1024
    cpu_fuel: int | None = None
    stdout_max_bytes: int | None = 2 * 1024 * 1024
    stderr_max_bytes: int | None = 2 * 1024 * 1024

    # Strong settings.
    docker_image: str = "python:3.12-slim"
    docker_pids_limit: int = 256
    docker_cpus: float | None = None
    docker_user: str | None = "65532:65532"
    docker_tmpfs_mounts: tuple[str, ...] = ("/tmp",)
    docker_seccomp_profile: str | None = None
    docker_apparmor_profile: str | None = None
    docker_ulimit_nofile: str | None = None
    docker_ulimit_nproc: str | None = None
    rust_exec_mode: Literal["cli", "pyo3"] = "cli"
    strong_lane_preference: Literal["docker", "wasm", "auto"] = "docker"
    rust_available_override: bool | None = None
    wasm_normalize_existing_paths: bool = False
    wasm_normalization_strict: bool = True
    preopen_dirs: tuple[str, ...] = ()
    allowed_write_paths: tuple[str, ...] = ()

    secrets_scope: SecretsScopeConfig | None = None


def create_sandbox_executor(*, cfg: SandboxFactoryConfig) -> Executor:
    """Create an executor implementing the selected secure sandbox."""
    if cfg.sandbox_mode == "dev":
        dev_cfg = DevSandboxConfig(
            work_root=cfg.work_root,
            allow_network=cfg.allow_network,
            memory_bytes=cfg.memory_bytes,
            stdout_max_bytes=cfg.stdout_max_bytes,
            stderr_max_bytes=cfg.stderr_max_bytes,
            secrets_scope=cfg.secrets_scope,
        )
        return DevSandboxExecutor(cfg=dev_cfg)

    strong_cfg = SandboxStrongConfig(
        work_root=cfg.work_root,
        allow_network=cfg.allow_network,
        memory_bytes=cfg.memory_bytes,
        cpu_fuel=cfg.cpu_fuel,
        stdout_max_bytes=cfg.stdout_max_bytes,
        stderr_max_bytes=cfg.stderr_max_bytes,
        docker_image=cfg.docker_image,
        docker_pids_limit=cfg.docker_pids_limit,
        docker_cpus=cfg.docker_cpus,
        docker_user=cfg.docker_user,
        docker_tmpfs_mounts=cfg.docker_tmpfs_mounts,
        docker_seccomp_profile=cfg.docker_seccomp_profile,
        docker_apparmor_profile=cfg.docker_apparmor_profile,
        docker_ulimit_nofile=cfg.docker_ulimit_nofile,
        docker_ulimit_nproc=cfg.docker_ulimit_nproc,
        rust_exec_mode=cfg.rust_exec_mode,
        strong_lane_preference=cfg.strong_lane_preference,
        rust_available_override=cfg.rust_available_override,
        wasm_normalize_existing_paths=cfg.wasm_normalize_existing_paths,
        wasm_normalization_strict=cfg.wasm_normalization_strict,
        preopen_dirs=cfg.preopen_dirs,
        allowed_write_paths=cfg.allowed_write_paths,
        secrets_scope=cfg.secrets_scope,
    )
    underlying = create_strong_underlying_executor(cfg=strong_cfg)
    return StrongSandboxExecutor(cfg=strong_cfg, underlying=underlying)
