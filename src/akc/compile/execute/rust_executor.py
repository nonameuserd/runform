from __future__ import annotations

import os
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from akc.compile.interfaces import ExecutionRequest, ExecutionResult, Executor, TenantRepoScope
from akc.compile.rust_bridge import RustExecConfig, run_exec_with_rust


@contextmanager
def _temporary_env(updates: Mapping[str, str]) -> None:
    """Temporarily update os.environ and restore previous values."""

    sentinel = object()
    previous: dict[str, str | object] = {}
    try:
        for k, v in updates.items():
            previous[k] = os.environ.get(k, sentinel)
            os.environ[k] = v
        yield
    finally:
        for k, old in previous.items():
            if old is sentinel:
                os.environ.pop(k, None)
            else:
                os.environ[k] = str(old)


@dataclass(frozen=True, slots=True)
class RustExecutor(Executor):
    """Rust-backed sandboxed executor used during the Execute phase.

    Notes:
    - The controller remains the orchestrator; Rust is an implementation detail
      behind the `Executor` protocol.
    - Tenant isolation is enforced in Rust by workspace containment.
    """

    rust_cfg: RustExecConfig = RustExecConfig()
    # If set, this is mapped into Rust's `AKC_EXEC_ROOT` so workdirs are
    # namespaced under the Python work_root (per tenant/repo).
    work_root: str | Path | None = None

    # Optional explicit allowlist for executable programs. When unset and
    # `allow_requested_program_only=True`, we allow only request.command[0].
    allowed_programs: tuple[str, ...] | None = None
    allow_requested_program_only: bool = True

    # Rust clears env for the child process; propagate PATH so standard toolchains
    # (python/pytest) can be resolved on dev machines.
    propagate_host_path: bool = True

    def _effective_allowlist(self, request: ExecutionRequest) -> tuple[str, ...]:
        if self.allowed_programs is not None:
            return self.allowed_programs
        if self.allow_requested_program_only:
            return (str(request.command[0]),)
        return ()

    def _effective_request(self, request: ExecutionRequest) -> ExecutionRequest:
        if not self.propagate_host_path:
            return request
        env: dict[str, str] = dict(request.env or {})
        host_path = os.environ.get("PATH")
        if host_path:
            env["PATH"] = host_path
        # If we didn't actually add anything, keep the original request to avoid
        # pointless allocations.
        if env == (request.env or {}):
            return request
        return ExecutionRequest(
            command=request.command,
            cwd=request.cwd,
            env=env,
            timeout_s=request.timeout_s,
            stdin_text=request.stdin_text,
        )

    def run(self, *, scope: TenantRepoScope, request: ExecutionRequest) -> ExecutionResult:  # type: ignore[override]
        start = time.monotonic()

        env_updates: dict[str, str] = {}
        if self.work_root is not None:
            env_updates["AKC_EXEC_ROOT"] = str(Path(self.work_root).expanduser().resolve())

        allowlist = self._effective_allowlist(request=request)
        if allowlist:
            env_updates["AKC_EXEC_ALLOWLIST"] = ":".join(list(allowlist))

        effective_request = self._effective_request(request=request)
        with _temporary_env(env_updates):
            rust_res = run_exec_with_rust(cfg=self.rust_cfg, scope=scope, request=effective_request)

        duration_ms = int((time.monotonic() - start) * 1000.0)
        return ExecutionResult(
            exit_code=int(rust_res.exit_code),
            stdout=str(rust_res.stdout or ""),
            stderr=str(rust_res.stderr or ""),
            duration_ms=duration_ms,
        )

