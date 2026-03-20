from __future__ import annotations

import importlib.util
import shutil
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

from akc.compile.execute.rust_executor import RustExecutor
from akc.compile.executors import DockerExecutor
from akc.compile.interfaces import ExecutionRequest, ExecutionResult, Executor, TenantRepoScope
from akc.compile.rust_bridge import BackendMode, RustExecConfig

from .secrets import SecretsScopeConfig


@dataclass(frozen=True, slots=True)
class SandboxStrongConfig:
    """Configuration for the strong sandbox boundary."""

    work_root: str | None = None
    allow_network: bool = False
    memory_bytes: int | None = 1024 * 1024 * 1024
    cpu_fuel: int | None = None
    stdout_max_bytes: int | None = 2 * 1024 * 1024
    stderr_max_bytes: int | None = 2 * 1024 * 1024

    # Docker boundary settings (used when Rust WASM lane is unavailable).
    docker_image: str = "python:3.12-slim"
    docker_pids_limit: int = 256
    docker_cpus: float | None = None
    docker_user: str | None = "65532:65532"
    docker_tmpfs_mounts: tuple[str, ...] = ("/tmp",)
    docker_seccomp_profile: str | None = None
    docker_apparmor_profile: str | None = None
    docker_ulimit_nofile: str | None = None
    docker_ulimit_nproc: str | None = None

    # Rust executor selection for the WASM lane when available.
    rust_exec_mode: BackendMode = "cli"
    # Selection policy for strong lane backend resolution:
    # - docker: always use Docker boundary
    # - wasm: require Rust WASM lane (fail closed when unavailable)
    # - auto: prefer Docker, fallback to WASM when Docker is unavailable
    strong_lane_preference: Literal["docker", "wasm", "auto"] = "docker"
    # Optional test-only override for the Rust surface availability probe.
    rust_available_override: bool | None = None
    wasm_normalize_existing_paths: bool = False
    wasm_normalization_strict: bool = True
    preopen_dirs: tuple[str, ...] = ()
    allowed_write_paths: tuple[str, ...] = ()

    # Tenant-scoped secrets injector (optional).
    secrets_scope: SecretsScopeConfig | None = None


def _rust_exec_available(*, mode: BackendMode, exec_bin: str = "akc-exec") -> bool:
    """Best-effort availability probe for Rust execution surfaces."""
    if mode == "pyo3":
        return importlib.util.find_spec("akc_rust") is not None
    return shutil.which(exec_bin) is not None


def create_strong_underlying_executor(*, cfg: SandboxStrongConfig) -> Executor:
    """Create the underlying strong-lane executor."""
    preference = str(cfg.strong_lane_preference).strip().lower()
    if preference not in {"docker", "wasm", "auto"}:
        raise ValueError("strong_lane_preference must be one of: docker, wasm, auto")
    rust_available = (
        bool(cfg.rust_available_override)
        if cfg.rust_available_override is not None
        else _rust_exec_available(mode=cfg.rust_exec_mode)
    )
    if preference == "wasm" and not rust_available:
        raise RuntimeError(
            "strong lane preference 'wasm' requires Rust execution surface "
            "(install akc-exec/akc_rust or choose --strong-lane-preference docker|auto)"
        )

    should_use_rust = False
    if preference == "wasm":
        should_use_rust = True
    elif preference == "docker":
        should_use_rust = False
    elif preference == "auto":
        # Keep auto permissive: Docker first, WASM as fallback path when Docker
        # is not usable on the host.
        should_use_rust = not shutil.which("docker") and rust_available

    if should_use_rust:
        rust_cfg = RustExecConfig(
            mode=cfg.rust_exec_mode,
            lane="wasm",
            allow_network=bool(cfg.allow_network),
            memory_bytes=cfg.memory_bytes,
            cpu_fuel=cfg.cpu_fuel,
            stdout_max_bytes=cfg.stdout_max_bytes,
            stderr_max_bytes=cfg.stderr_max_bytes,
            allowed_write_paths=cfg.allowed_write_paths,
            preopen_dirs=cfg.preopen_dirs,
            wasm_normalize_existing_paths=cfg.wasm_normalize_existing_paths,
            wasm_normalization_strict=cfg.wasm_normalization_strict,
        )
        return RustExecutor(rust_cfg=rust_cfg, work_root=cfg.work_root)
    return DockerExecutor(
        work_root=cfg.work_root,
        image=cfg.docker_image,
        disable_network=not bool(cfg.allow_network),
        memory_bytes=cfg.memory_bytes,
        pids_limit=cfg.docker_pids_limit,
        cpus=cfg.docker_cpus,
        user=cfg.docker_user,
        tmpfs_mounts=cfg.docker_tmpfs_mounts,
        seccomp_profile=cfg.docker_seccomp_profile,
        apparmor_profile=cfg.docker_apparmor_profile,
        ulimit_nofile=cfg.docker_ulimit_nofile,
        ulimit_nproc=cfg.docker_ulimit_nproc,
        stdout_max_bytes=cfg.stdout_max_bytes,
        stderr_max_bytes=cfg.stderr_max_bytes,
    )


@dataclass(frozen=True, slots=True)
class StrongSandboxExecutor(Executor):
    """Strong sandbox executor wrapper with tenant-scoped secrets injection."""

    cfg: SandboxStrongConfig
    underlying: Executor

    def _merge_env(
        self,
        *,
        scope: TenantRepoScope,
        request: ExecutionRequest,
    ) -> Mapping[str, str] | None:
        base_env = dict(request.env or {})
        if self.cfg.secrets_scope is None:
            return base_env or None
        secrets_env = self.cfg.secrets_scope.resolve_env_for_scope(scope=scope)
        if not secrets_env:
            return base_env or None
        base_env.update(secrets_env)
        return base_env

    def run(self, *, scope: TenantRepoScope, request: ExecutionRequest) -> ExecutionResult:
        env = self._merge_env(scope=scope, request=request)
        effective_request = ExecutionRequest(
            command=request.command,
            cwd=request.cwd,
            env=env,
            timeout_s=request.timeout_s,
            stdin_text=request.stdin_text,
            run_id=request.run_id,
        )
        return self.underlying.run(scope=scope, request=effective_request)
