from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from akc.compile.executors import SubprocessExecutor
from akc.compile.interfaces import ExecutionRequest, ExecutionResult, Executor, TenantRepoScope

from .secrets import SecretsScopeConfig


@dataclass(frozen=True, slots=True)
class DevSandboxConfig:
    """Configuration for the best-effort local subprocess sandbox."""

    work_root: str | None = None
    allow_network: bool = False
    memory_bytes: int | None = 1024 * 1024 * 1024
    stdout_max_bytes: int | None = 2 * 1024 * 1024
    stderr_max_bytes: int | None = 2 * 1024 * 1024
    home_under_cwd: bool = True
    secrets_scope: SecretsScopeConfig | None = None


@dataclass(frozen=True, slots=True)
class DevSandboxExecutor(Executor):
    """Dev sandbox executor wrapper.

    It injects tenant-scoped secrets into the sandbox environment and delegates
    to `SubprocessExecutor` for resource caps + IO size clamps.
    """

    cfg: DevSandboxConfig

    def _merge_env(
        self,
        *,
        scope: TenantRepoScope,
        request: ExecutionRequest,
    ) -> Mapping[str, str] | None:
        base_env = dict(request.env or {})
        if self.cfg.secrets_scope is None:
            return base_env or None
        # Tenant-scoped secrets injection.
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
        underlying = SubprocessExecutor(
            work_root=self.cfg.work_root,
            disable_network=not bool(self.cfg.allow_network),
            memory_bytes=self.cfg.memory_bytes,
            stdout_max_bytes=self.cfg.stdout_max_bytes,
            stderr_max_bytes=self.cfg.stderr_max_bytes,
            home_under_cwd=self.cfg.home_under_cwd,
        )
        return underlying.run(scope=scope, request=effective_request)
