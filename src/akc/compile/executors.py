"""Execution backends for the Phase 3 compile loop.

The compile controller depends on the `Executor` protocol (see `interfaces.py`).
This module provides concrete executors:
- `SubprocessExecutor` (default): runs commands via `subprocess` in a scoped workdir.
- `DockerExecutor` (optional): runs commands inside a Docker container with a mounted workdir.

Both implementations enforce conservative tenant+repo isolation by ensuring all
execution happens within a per-scope directory rooted under an allowed work root.
"""

from __future__ import annotations

import os
import subprocess
import tempfile
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from akc.compile.interfaces import ExecutionRequest, ExecutionResult, Executor, TenantRepoScope
from akc.memory.models import require_non_empty


@dataclass(frozen=True, slots=True)
class StageRunResult:
    """Structured result for a named execution stage (e.g. tests).

    This is a thin wrapper around `ExecutionResult` that records the command and stage name.
    """

    stage: str
    command: list[str]
    result: ExecutionResult

    def to_json_obj(self) -> dict[str, object]:
        return {
            "stage": self.stage,
            "command": list(self.command),
            "exit_code": int(self.result.exit_code),
            "stdout": self.result.stdout,
            "stderr": self.result.stderr,
            "duration_ms": self.result.duration_ms,
        }


def run_stage(
    *,
    executor: Executor,
    scope: TenantRepoScope,
    stage: str,
    command: list[str],
    timeout_s: float | None = None,
    cwd: str | None = None,
    env: Mapping[str, str] | None = None,
    stdin_text: str | None = None,
) -> StageRunResult:
    """Run a command as a named stage and return structured results."""

    require_non_empty(stage, name="stage")
    if not command:
        raise ValueError("command must be non-empty")
    res = executor.run(
        scope=scope,
        request=ExecutionRequest(
            command=list(command),
            timeout_s=timeout_s,
            cwd=cwd,
            env=env,
            stdin_text=stdin_text,
        ),
    )
    return StageRunResult(stage=str(stage), command=list(command), result=res)


def _default_work_root() -> Path:
    return Path(tempfile.gettempdir()) / "akc-exec"


def _scope_dir(*, work_root: Path, scope: TenantRepoScope) -> Path:
    require_non_empty(scope.tenant_id, name="scope.tenant_id")
    require_non_empty(scope.repo_id, name="scope.repo_id")
    # Keep it simple and stable; repo_id is already normalized.
    return work_root / scope.tenant_id / scope.repo_id


def _ensure_under_root(*, root: Path, p: Path) -> None:
    root_r = root.resolve()
    p_r = p.resolve()
    try:
        p_r.relative_to(root_r)
    except ValueError as e:  # pragma: no cover (covered by tests via ValueError)
        raise ValueError("execution cwd must be within executor work_root") from e


def _sanitize_env(
    base: Mapping[str, str] | None, extra: Mapping[str, str] | None
) -> dict[str, str]:
    env: dict[str, str] = {}
    if base:
        env.update({str(k): str(v) for k, v in base.items()})
    if extra:
        env.update({str(k): str(v) for k, v in extra.items()})
    # Provide a minimal PATH by default to avoid inheriting surprising host env.
    env.setdefault("PATH", os.environ.get("PATH", ""))
    return env


@dataclass(frozen=True, slots=True)
class SubprocessExecutor(Executor):
    """Run commands via `subprocess.run` in an isolated per-scope workdir."""

    work_root: str | Path | None = None
    base_env: Mapping[str, str] | None = None

    def _work_root_path(self) -> Path:
        return Path(self.work_root) if self.work_root is not None else _default_work_root()

    def run(self, *, scope: TenantRepoScope, request: ExecutionRequest) -> ExecutionResult:
        root = self._work_root_path()
        root.mkdir(parents=True, exist_ok=True)

        effective_cwd = (
            Path(request.cwd)
            if request.cwd is not None
            else _scope_dir(work_root=root, scope=scope)
        )
        effective_cwd.mkdir(parents=True, exist_ok=True)
        _ensure_under_root(root=root, p=effective_cwd)

        env = _sanitize_env(self.base_env, request.env)

        started = time.monotonic()
        try:
            cp = subprocess.run(
                list(request.command),
                cwd=str(effective_cwd),
                env=env,
                input=request.stdin_text,
                text=True,
                capture_output=True,
                timeout=float(request.timeout_s) if request.timeout_s is not None else None,
                check=False,
            )
            dur_ms = int((time.monotonic() - started) * 1000.0)
            return ExecutionResult(
                exit_code=int(cp.returncode),
                stdout=cp.stdout or "",
                stderr=cp.stderr or "",
                duration_ms=dur_ms,
            )
        except subprocess.TimeoutExpired as e:
            dur_ms = int((time.monotonic() - started) * 1000.0)
            out = (
                (e.stdout or "")
                if isinstance(e.stdout, str) or e.stdout is None
                else e.stdout.decode("utf-8", "replace")
            )
            err = (
                (e.stderr or "")
                if isinstance(e.stderr, str) or e.stderr is None
                else e.stderr.decode("utf-8", "replace")
            )
            return ExecutionResult(
                exit_code=124, stdout=out, stderr=err or "timed out", duration_ms=dur_ms
            )
        except FileNotFoundError as e:
            dur_ms = int((time.monotonic() - started) * 1000.0)
            return ExecutionResult(exit_code=127, stdout="", stderr=str(e), duration_ms=dur_ms)


@dataclass(frozen=True, slots=True)
class DockerExecutor(Executor):
    """Run commands inside a Docker container with a mounted workdir.

    Notes:
    - This executor requires the `docker` CLI to be installed and available on PATH.
    - It enforces isolation by mounting only the effective workdir and disabling networking.
    """

    image: str = "python:3.12-slim"
    work_root: str | Path | None = None
    container_workdir: str = "/work"
    base_env: Mapping[str, str] | None = None
    disable_network: bool = True

    def _work_root_path(self) -> Path:
        return Path(self.work_root) if self.work_root is not None else _default_work_root()

    def run(self, *, scope: TenantRepoScope, request: ExecutionRequest) -> ExecutionResult:
        root = self._work_root_path()
        root.mkdir(parents=True, exist_ok=True)

        effective_cwd = (
            Path(request.cwd)
            if request.cwd is not None
            else _scope_dir(work_root=root, scope=scope)
        )
        effective_cwd.mkdir(parents=True, exist_ok=True)
        _ensure_under_root(root=root, p=effective_cwd)

        env = _sanitize_env(self.base_env, request.env)

        docker_cmd: list[str] = [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{str(effective_cwd.resolve())}:{self.container_workdir}:rw",
            "-w",
            self.container_workdir,
        ]
        if self.disable_network:
            docker_cmd += ["--network", "none"]
        for k, v in env.items():
            # Avoid passing empty keys; keep explicit.
            if str(k).strip():
                docker_cmd += ["-e", f"{k}={v}"]
        docker_cmd.append(self.image)
        docker_cmd += list(request.command)

        started = time.monotonic()
        try:
            cp = subprocess.run(
                docker_cmd,
                cwd=str(effective_cwd),
                input=request.stdin_text,
                text=True,
                capture_output=True,
                timeout=float(request.timeout_s) if request.timeout_s is not None else None,
                check=False,
            )
            dur_ms = int((time.monotonic() - started) * 1000.0)
            return ExecutionResult(
                exit_code=int(cp.returncode),
                stdout=cp.stdout or "",
                stderr=cp.stderr or "",
                duration_ms=dur_ms,
            )
        except subprocess.TimeoutExpired as e:
            dur_ms = int((time.monotonic() - started) * 1000.0)
            out = (
                (e.stdout or "")
                if isinstance(e.stdout, str) or e.stdout is None
                else e.stdout.decode("utf-8", "replace")
            )
            err = (
                (e.stderr or "")
                if isinstance(e.stderr, str) or e.stderr is None
                else e.stderr.decode("utf-8", "replace")
            )
            return ExecutionResult(
                exit_code=124, stdout=out, stderr=err or "timed out", duration_ms=dur_ms
            )
        except FileNotFoundError as e:
            dur_ms = int((time.monotonic() - started) * 1000.0)
            return ExecutionResult(exit_code=127, stdout="", stderr=str(e), duration_ms=dur_ms)
