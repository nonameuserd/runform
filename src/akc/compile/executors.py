"""Execution backends for the Phase 3 compile loop.

The compile controller depends on the `Executor` protocol (see `interfaces.py`).
This module provides concrete executors:
- `SubprocessExecutor` (default): runs commands via `subprocess` in a scoped workdir.
- `DockerExecutor` (optional): runs commands inside a Docker container with a mounted workdir.

Both implementations enforce conservative tenant+repo isolation by ensuring all
execution happens within a per-scope directory rooted under an allowed work root.
"""

from __future__ import annotations

import contextlib
import os
import platform
import re
import selectors
import subprocess
import tempfile
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any, cast

from akc.compile.interfaces import ExecutionRequest, ExecutionResult, Executor, TenantRepoScope
from akc.control.policy import PolicyWrappedExecutor
from akc.memory.models import JSONValue, require_non_empty


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
    executor: Executor | PolicyWrappedExecutor,
    scope: TenantRepoScope,
    stage: str,
    command: list[str],
    timeout_s: float | None = None,
    cwd: str | None = None,
    env: Mapping[str, str] | None = None,
    stdin_text: str | None = None,
    # Optional execution run identifier for tenant-scoped namespace selection.
    run_id: str | None = None,
    # Optional policy-control parameters for capability-wrapper executors.
    policy_context: Mapping[str, JSONValue] | None = None,
    policy_ttl_ms: int | None = None,
    policy_base_capability: Any | None = None,
) -> StageRunResult:
    """Run a command as a named stage and return structured results."""

    require_non_empty(stage, name="stage")
    if not command:
        raise ValueError("command must be non-empty")
    req = ExecutionRequest(
        command=list(command),
        timeout_s=timeout_s,
        cwd=cwd,
        env=env,
        stdin_text=stdin_text,
        run_id=run_id,
    )
    # Allow duck-typed capability wrappers that implement `run_with_stage`.
    if hasattr(executor, "run_with_stage"):
        res = executor.run_with_stage(
            scope=scope,
            stage=str(stage),
            request=req,
            context=policy_context,
            ttl_ms=policy_ttl_ms,
            base_capability=policy_base_capability,
        )
    else:
        res = executor.run(scope=scope, request=req)
    return StageRunResult(stage=str(stage), command=list(command), result=res)


def _default_work_root() -> Path:
    return Path(tempfile.gettempdir()) / "akc-exec"


def _scope_dir(*, work_root: Path, scope: TenantRepoScope) -> Path:
    require_non_empty(scope.tenant_id, name="scope.tenant_id")
    require_non_empty(scope.repo_id, name="scope.repo_id")
    # Keep it simple and stable; repo_id is already normalized.
    return work_root / scope.tenant_id / scope.repo_id


_RUN_ID_ALLOWED_CHARS = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")
_SAFE_DOCKER_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]*$")
_SAFE_DOCKER_USER_PART_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]*$|^[0-9]+$")
_MAX_DOCKER_ULIMIT_VALUE = 1_048_576


def _validate_run_id(run_id: str) -> str:
    """Validate/normalize a run_id for safe directory naming."""
    s = str(run_id).strip()
    require_non_empty(s, name="run_id")
    if not s:
        raise ValueError("run_id must be non-empty")
    if any(ch not in _RUN_ID_ALLOWED_CHARS for ch in s):
        raise ValueError("run_id contains invalid characters for executor namespace")
    return s


def _run_dir(*, work_root: Path, scope: TenantRepoScope, run_id: str) -> Path:
    run_id2 = _validate_run_id(run_id)
    return _scope_dir(work_root=work_root, scope=scope) / run_id2


def _validate_docker_user(user: str | None) -> str | None:
    if user is None:
        return None
    value = str(user).strip()
    require_non_empty(value, name="docker user")
    parts = value.split(":")
    if len(parts) > 2:
        raise ValueError("docker user must be '<uid>' or '<uid>:<gid>'")
    if any(not part or _SAFE_DOCKER_USER_PART_RE.fullmatch(part) is None for part in parts):
        raise ValueError("docker user must contain only numeric ids or safe user/group identifiers")
    return value


def _validate_docker_tmpfs_mounts(tmpfs_mounts: tuple[str, ...]) -> tuple[str, ...]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_mount in tmpfs_mounts:
        mount = str(raw_mount).strip()
        require_non_empty(mount, name="docker tmpfs mount")
        if not mount.startswith("/"):
            raise ValueError("docker tmpfs mount must be an absolute container path")
        mount_path = Path(mount)
        if str(mount_path) != mount:
            raise ValueError("docker tmpfs mount must be normalized")
        if mount == "/":
            raise ValueError("docker tmpfs mount cannot target container root")
        if any(part in {"", ".", ".."} for part in mount_path.parts[1:]):
            raise ValueError("docker tmpfs mount contains unsafe path segments")
        if any(ch.isspace() for ch in mount):
            raise ValueError("docker tmpfs mount cannot contain whitespace")
        if mount not in seen:
            seen.add(mount)
            normalized.append(mount)
    return tuple(normalized)


def _validate_docker_security_identifier(
    value: str | None,
    *,
    field_name: str,
    allow_absolute_path: bool,
) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    require_non_empty(normalized, name=field_name)
    if any(ch.isspace() for ch in normalized):
        raise ValueError(f"{field_name} cannot contain whitespace")
    if normalized.startswith("/"):
        if not allow_absolute_path:
            raise ValueError(f"{field_name} must be a profile identifier")
        profile_path = Path(normalized)
        if str(profile_path) != normalized:
            raise ValueError(f"{field_name} must be a normalized absolute path")
        if any(part in {"", ".", ".."} for part in profile_path.parts[1:]):
            raise ValueError(f"{field_name} contains unsafe path segments")
        return normalized
    if _SAFE_DOCKER_IDENTIFIER_RE.fullmatch(normalized) is None:
        raise ValueError(f"{field_name} must contain only alphanumerics plus . _ : @ / -")
    return normalized


def _is_default_docker_seccomp_profile(value: str | None) -> bool:
    return str(value or "").strip().lower() == "runtime/default"


def _is_default_docker_apparmor_profile(value: str | None) -> bool:
    return str(value or "").strip().lower() == "docker-default"


def _validate_docker_ulimit(value: str | None, *, field_name: str) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    require_non_empty(normalized, name=field_name)
    if any(ch.isspace() for ch in normalized):
        raise ValueError(f"{field_name} cannot contain whitespace")
    parts = normalized.split(":")
    if len(parts) not in {1, 2} or any(not part.isdigit() for part in parts):
        raise ValueError(f"{field_name} must be '<soft>' or '<soft>:<hard>'")
    soft = int(parts[0])
    hard = int(parts[1]) if len(parts) == 2 else soft
    if soft <= 0 or hard <= 0:
        raise ValueError(f"{field_name} must be greater than zero")
    if soft > hard:
        raise ValueError(f"{field_name} soft limit cannot exceed hard limit")
    if soft > _MAX_DOCKER_ULIMIT_VALUE or hard > _MAX_DOCKER_ULIMIT_VALUE:
        raise ValueError(f"{field_name} cannot exceed {_MAX_DOCKER_ULIMIT_VALUE}")
    return normalized if len(parts) == 2 else str(soft)


def _ensure_under_scope_dir(*, work_root: Path, scope: TenantRepoScope, p: Path) -> None:
    scope_d = _scope_dir(work_root=work_root, scope=scope).resolve()
    # `p` may not exist yet when validating `request.cwd`; use strict=False so
    # this remains a pure validation step (avoid creating outside dirs).
    p_r = p.resolve(strict=False)
    try:
        p_r.relative_to(scope_d)
    except ValueError as e:  # pragma: no cover (covered by tests indirectly)
        # Keep error message stable for older tests.
        raise ValueError("execution cwd must be within executor work_root") from e


def _ensure_under_root(*, root: Path, p: Path) -> None:
    root_r = root.resolve()
    # `p` may not exist yet when validating `request.cwd`; use strict=False so
    # this remains a pure validation step (avoid creating outside dirs).
    p_r = p.resolve(strict=False)
    try:
        p_r.relative_to(root_r)
    except ValueError as e:  # pragma: no cover (covered by tests via ValueError)
        raise ValueError("execution cwd must be within executor work_root") from e


def _sanitize_env(
    base: Mapping[str, str] | None,
    extra: Mapping[str, str] | None,
) -> dict[str, str]:
    env: dict[str, str] = {}
    if base:
        env.update({str(k): str(v) for k, v in base.items()})
    if extra:
        env.update({str(k): str(v) for k, v in extra.items()})
    # Provide a minimal PATH by default to avoid inheriting surprising host env.
    env.setdefault("PATH", os.environ.get("PATH", ""))
    return env


def _apply_network_policy(env: dict[str, str], *, disable_network: bool) -> dict[str, str]:
    """Best-effort network policy for non-container subprocess execution.

    Note: A local subprocess cannot reliably have network blocked on all platforms
    without OS-level sandboxing (container/VM/WASM). We still do best-effort
    defense-in-depth by clearing common proxy variables to avoid ambient egress.
    """

    if not disable_network:
        return env
    # Clear common proxy env vars. Keep explicit empty strings so child processes
    # don't inherit proxy config from other sources.
    for k in (
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "NO_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "no_proxy",
    ):
        env[k] = ""
    return env


def _cap_output(text: str, *, max_bytes: int | None) -> str:
    if max_bytes is None:
        return text
    n = int(max_bytes)
    if n <= 0:
        return ""
    b = (text or "").encode("utf-8", "replace")
    if len(b) <= n:
        return text or ""
    clipped = b[:n]
    return clipped.decode("utf-8", "replace") + "\n...[truncated]..."


def _run_subprocess_capture_output_with_limits(
    *,
    command: list[str],
    cwd: str,
    env: Mapping[str, str] | None,
    stdin_text: str | None,
    timeout_s: float | None,
    preexec_fn: Callable[[], None] | None,
    stdout_max_bytes: int | None,
    stderr_max_bytes: int | None,
) -> tuple[int, str, str, int]:
    """Run a local subprocess while streaming stdout/stderr with caps.

    Unlike `subprocess.run(..., capture_output=True)` this prevents unbounded
    host memory usage when the child process produces excessive output.
    """

    stdout_cap = None if stdout_max_bytes is None else int(stdout_max_bytes)
    stderr_cap = None if stderr_max_bytes is None else int(stderr_max_bytes)

    stdout_bytes = bytearray()
    stderr_bytes = bytearray()
    stdout_truncated = False
    stderr_truncated = False

    def _update_cap(
        *,
        cap: int | None,
        buf: bytearray,
        chunk: bytes,
        truncated_flag: bool,
    ) -> bool:
        if cap is None:
            buf.extend(chunk)
            return truncated_flag
        n = int(cap)
        if n <= 0:
            # "No capture": discard everything (but note truncation).
            return truncated_flag or bool(chunk)
        if len(buf) < n:
            remain = n - len(buf)
            buf.extend(chunk[:remain])
            return truncated_flag or (len(chunk) > remain)
        return True

    started = time.monotonic()
    deadline = started + float(timeout_s) if timeout_s is not None else None
    timed_out = False

    proc = subprocess.Popen(
        command,
        cwd=cwd,
        env=None if env is None else dict(env),
        stdin=subprocess.PIPE if stdin_text is not None else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=False,
        preexec_fn=preexec_fn,
    )

    try:
        if stdin_text is not None and proc.stdin is not None:
            # Best-effort; caller-supplied stdin should already be budgeted.
            proc.stdin.write(stdin_text.encode("utf-8", "replace"))
            proc.stdin.close()

        sel = selectors.DefaultSelector()
        assert proc.stdout is not None
        assert proc.stderr is not None
        sel.register(proc.stdout, selectors.EVENT_READ, data="stdout")
        sel.register(proc.stderr, selectors.EVENT_READ, data="stderr")

        while sel.get_map():
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    timed_out = True
                    proc.kill()
                    break
                sel_timeout = min(0.1, remaining)
            else:
                sel_timeout = 0.1

            events = sel.select(timeout=sel_timeout)
            for key, _mask in events:
                stream = key.data
                pipe = cast(IO[bytes], key.fileobj)
                chunk = pipe.read(65536)
                if not chunk:
                    sel.unregister(key.fileobj)
                    with contextlib.suppress(Exception):
                        pipe.close()
                    continue
                if stream == "stdout":
                    stdout_truncated = _update_cap(
                        cap=stdout_cap,
                        buf=stdout_bytes,
                        chunk=chunk,
                        truncated_flag=stdout_truncated,
                    )
                else:
                    stderr_truncated = _update_cap(
                        cap=stderr_cap,
                        buf=stderr_bytes,
                        chunk=chunk,
                        truncated_flag=stderr_truncated,
                    )

        # If we timed out, the kill may have left pipes readable briefly.
        if timed_out:
            for _ in range(20):
                if not sel.get_map():
                    break
                events = sel.select(timeout=0.05)
                for key, _mask in events:
                    stream = key.data
                    pipe = cast(IO[bytes], key.fileobj)
                    chunk = pipe.read(65536)
                    if not chunk:
                        sel.unregister(key.fileobj)
                        with contextlib.suppress(Exception):
                            pipe.close()
                        continue
                    if stream == "stdout":
                        stdout_truncated = _update_cap(
                            cap=stdout_cap,
                            buf=stdout_bytes,
                            chunk=chunk,
                            truncated_flag=stdout_truncated,
                        )
                    else:
                        stderr_truncated = _update_cap(
                            cap=stderr_cap,
                            buf=stderr_bytes,
                            chunk=chunk,
                            truncated_flag=stderr_truncated,
                        )

        # Ensure we reap the process.
        try:
            proc.wait(timeout=0.5 if timed_out else None)
        except Exception:
            proc.wait()

    finally:
        try:
            if proc.stdout is not None:
                proc.stdout.close()
        except Exception:
            pass
        try:
            if proc.stderr is not None:
                proc.stderr.close()
        except Exception:
            pass
        try:
            if proc.stdin is not None and not proc.stdin.closed:
                proc.stdin.close()
        except Exception:
            pass

    dur_ms = int((time.monotonic() - started) * 1000.0)
    exit_code = 124 if timed_out else int(proc.returncode or 0)

    stdout_text = stdout_bytes.decode("utf-8", "replace")
    stderr_text = stderr_bytes.decode("utf-8", "replace")

    if stdout_cap is not None:
        if int(stdout_cap) <= 0:
            stdout_text = ""
        elif stdout_truncated:
            stdout_text = stdout_text + "\n...[truncated]..."
    if stderr_cap is not None:
        if int(stderr_cap) <= 0:
            stderr_text = ""
        elif stderr_truncated:
            stderr_text = stderr_text + "\n...[truncated]..."
    if timed_out and not stderr_text:
        stderr_text = "timed out"

    return exit_code, stdout_text, stderr_text, dur_ms


def _subprocess_preexec_fn(
    *,
    memory_bytes: int | None,
    cpu_time_s: float | None,
) -> Callable[[], None] | None:
    """Return a preexec_fn that applies best-effort rlimits (POSIX only)."""

    if platform.system().lower() not in {"linux", "darwin"}:
        return None
    try:
        import resource  # POSIX-only
    except Exception:
        return None

    mem = int(memory_bytes) if memory_bytes is not None else None
    cpu = float(cpu_time_s) if cpu_time_s is not None else None

    def _fn() -> None:
        # Avoid core dumps.
        with contextlib.suppress(Exception):
            resource.setrlimit(resource.RLIMIT_CORE, (0, 0))

        # CPU time limit (seconds). This is defense-in-depth alongside wall timeout.
        if cpu is not None and cpu > 0:
            lim = int(max(1.0, cpu))
            with contextlib.suppress(Exception):
                resource.setrlimit(resource.RLIMIT_CPU, (lim, lim))

        # Address space limit (bytes). On some platforms (notably macOS),
        # RLIMIT_AS may be best-effort; still apply when possible.
        if mem is not None and mem > 0:
            for name in ("RLIMIT_AS", "RLIMIT_RSS", "RLIMIT_DATA"):
                r = getattr(resource, name, None)
                if r is None:
                    continue
                try:
                    resource.setrlimit(r, (mem, mem))
                except Exception:
                    continue

    return _fn


@dataclass(frozen=True, slots=True)
class SubprocessExecutor(Executor):
    """Run commands via `subprocess.run` in an isolated per-scope workdir."""

    work_root: str | Path | None = None
    base_env: Mapping[str, str] | None = None
    # Sandbox knobs (best-effort for local subprocess execution).
    disable_network: bool = True
    memory_bytes: int | None = 1024 * 1024 * 1024  # 1 GiB default cap
    # If set, caps captured stdout/stderr to avoid runaway logs.
    stdout_max_bytes: int | None = 2 * 1024 * 1024
    stderr_max_bytes: int | None = 2 * 1024 * 1024
    # If true, force HOME and XDG dirs under the effective cwd (tenant-scoped).
    home_under_cwd: bool = True

    def _work_root_path(self) -> Path:
        return Path(self.work_root) if self.work_root is not None else _default_work_root()

    def run(self, *, scope: TenantRepoScope, request: ExecutionRequest) -> ExecutionResult:
        root = self._work_root_path()
        root.mkdir(parents=True, exist_ok=True)

        scope_cwd = _scope_dir(work_root=root, scope=scope)
        scope_cwd.mkdir(parents=True, exist_ok=True)
        effective_cwd = scope_cwd if request.cwd is None else Path(request.cwd).expanduser()
        # Validate first (do not create outside directories on validation failure).
        _ensure_under_root(root=root, p=effective_cwd)
        if request.cwd is not None:
            _ensure_under_scope_dir(work_root=root, scope=scope, p=effective_cwd)
        effective_cwd.mkdir(parents=True, exist_ok=True)

        run_dir: Path | None = None
        if request.run_id is not None:
            run_dir = _run_dir(work_root=root, scope=scope, run_id=request.run_id)
            run_dir.mkdir(parents=True, exist_ok=True)
            # The run dir is bind-mounted into Docker for HOME/XDG/cache writes.
            # Make it writable to the configured non-root container user.
            with contextlib.suppress(OSError):
                run_dir.chmod(0o777)

        env = _sanitize_env(self.base_env, request.env)
        env = _apply_network_policy(env, disable_network=bool(self.disable_network))
        if self.home_under_cwd:
            # Keep subprocess home/config under a tenant-scoped per-run namespace
            # when `run_id` is provided (otherwise fall back to historical behavior).
            home_dir = run_dir if run_dir is not None else effective_cwd
            env["HOME"] = str(home_dir)
            env["XDG_CACHE_HOME"] = str(home_dir / ".cache")
            env["XDG_CONFIG_HOME"] = str(home_dir / ".config")
            env["XDG_STATE_HOME"] = str(home_dir / ".state")
        env["AKC_TENANT_ID"] = str(scope.tenant_id)
        env["AKC_REPO_ID"] = str(scope.repo_id)
        if request.run_id is not None:
            env["AKC_RUN_ID"] = str(request.run_id)

        started = time.monotonic()
        try:
            preexec_fn = _subprocess_preexec_fn(
                memory_bytes=self.memory_bytes,
                cpu_time_s=float(request.timeout_s) if request.timeout_s is not None else None,
            )
            exit_code, stdout, stderr, dur_ms = _run_subprocess_capture_output_with_limits(
                command=list(request.command),
                cwd=str(effective_cwd),
                env=env,
                stdin_text=request.stdin_text,
                timeout_s=float(request.timeout_s) if request.timeout_s is not None else None,
                preexec_fn=preexec_fn,
                stdout_max_bytes=self.stdout_max_bytes,
                stderr_max_bytes=self.stderr_max_bytes,
            )
            return ExecutionResult(
                exit_code=int(exit_code),
                stdout=str(stdout),
                stderr=str(stderr),
                duration_ms=dur_ms,
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
    container_run_dir: str = "/run"
    base_env: Mapping[str, str] | None = None
    disable_network: bool = True
    # Defense-in-depth knobs (enforced by container runtime where available).
    memory_bytes: int | None = 1024 * 1024 * 1024  # 1 GiB default cap
    pids_limit: int | None = 256
    cpus: float | None = None
    read_only_rootfs: bool = True
    no_new_privileges: bool = True
    cap_drop_all: bool = True
    user: str | None = "65532:65532"
    tmpfs_mounts: tuple[str, ...] = ("/tmp",)
    seccomp_profile: str | None = None
    apparmor_profile: str | None = None
    ulimit_nofile: str | None = None
    ulimit_nproc: str | None = None
    stdout_max_bytes: int | None = 2 * 1024 * 1024
    stderr_max_bytes: int | None = 2 * 1024 * 1024

    def _work_root_path(self) -> Path:
        return Path(self.work_root) if self.work_root is not None else _default_work_root()

    def run(self, *, scope: TenantRepoScope, request: ExecutionRequest) -> ExecutionResult:
        root = self._work_root_path()
        root.mkdir(parents=True, exist_ok=True)

        scope_cwd = _scope_dir(work_root=root, scope=scope)
        scope_cwd.mkdir(parents=True, exist_ok=True)
        effective_cwd = scope_cwd if request.cwd is None else Path(request.cwd).expanduser()
        # Validate first (do not create outside directories on validation failure).
        _ensure_under_root(root=root, p=effective_cwd)
        if request.cwd is not None:
            _ensure_under_scope_dir(work_root=root, scope=scope, p=effective_cwd)
        effective_cwd.mkdir(parents=True, exist_ok=True)

        run_dir: Path | None = None
        if request.run_id is not None:
            run_dir = _run_dir(work_root=root, scope=scope, run_id=request.run_id)
            run_dir.mkdir(parents=True, exist_ok=True)
            # The run dir is bind-mounted into Docker for HOME/XDG/cache writes.
            # Make it writable to the configured non-root container user.
            with contextlib.suppress(OSError):
                run_dir.chmod(0o777)

        env = _sanitize_env(self.base_env, request.env)
        env["AKC_TENANT_ID"] = str(scope.tenant_id)
        env["AKC_REPO_ID"] = str(scope.repo_id)
        if request.run_id is not None:
            env["AKC_RUN_ID"] = str(request.run_id)

        home_dir = self.container_run_dir if run_dir is not None else self.container_workdir
        env["HOME"] = home_dir
        env["XDG_CACHE_HOME"] = str(home_dir) + "/.cache"
        env["XDG_CONFIG_HOME"] = str(home_dir) + "/.config"
        env["XDG_STATE_HOME"] = str(home_dir) + "/.state"
        # Keep common Python/pytest write paths off the bind-mounted repo so
        # non-root container users can run against read-mostly source trees.
        env.setdefault("PYTHONDONTWRITEBYTECODE", "1")
        env.setdefault("PYTHONPYCACHEPREFIX", str(home_dir) + "/.pycache")
        env.setdefault("PYTEST_ADDOPTS", f"--override-ini=cache_dir={home_dir}/.pytest_cache")

        docker_user = _validate_docker_user(self.user)
        tmpfs_mounts = _validate_docker_tmpfs_mounts(self.tmpfs_mounts)
        seccomp_profile = _validate_docker_security_identifier(
            self.seccomp_profile,
            field_name="docker seccomp profile",
            allow_absolute_path=True,
        )
        apparmor_profile = _validate_docker_security_identifier(
            self.apparmor_profile,
            field_name="docker apparmor profile",
            allow_absolute_path=False,
        )
        ulimit_nofile = _validate_docker_ulimit(
            self.ulimit_nofile,
            field_name="docker ulimit nofile",
        )
        ulimit_nproc = _validate_docker_ulimit(
            self.ulimit_nproc,
            field_name="docker ulimit nproc",
        )

        docker_cmd: list[str] = [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{str(effective_cwd.resolve())}:{self.container_workdir}:rw",
            "-w",
            self.container_workdir,
        ]
        if run_dir is not None:
            docker_cmd += ["-v", f"{str(run_dir.resolve())}:{self.container_run_dir}:rw"]
        if self.disable_network:
            docker_cmd += ["--network", "none"]
        if self.memory_bytes is not None:
            docker_cmd += ["--memory", str(int(self.memory_bytes))]
        if self.pids_limit is not None:
            docker_cmd += ["--pids-limit", str(int(self.pids_limit))]
        if self.cpus is not None:
            docker_cmd += ["--cpus", str(float(self.cpus))]
        if docker_user is not None:
            docker_cmd += ["--user", docker_user]
        for mount in tmpfs_mounts:
            docker_cmd += ["--tmpfs", mount]
        if self.read_only_rootfs:
            docker_cmd += ["--read-only"]
        if self.no_new_privileges:
            docker_cmd += ["--security-opt", "no-new-privileges"]
        if seccomp_profile is not None and not _is_default_docker_seccomp_profile(seccomp_profile):
            docker_cmd += ["--security-opt", f"seccomp={seccomp_profile}"]
        if apparmor_profile is not None and not _is_default_docker_apparmor_profile(apparmor_profile):
            docker_cmd += ["--security-opt", f"apparmor={apparmor_profile}"]
        if self.cap_drop_all:
            docker_cmd += ["--cap-drop", "ALL"]
        if ulimit_nofile is not None:
            docker_cmd += ["--ulimit", f"nofile={ulimit_nofile}"]
        if ulimit_nproc is not None:
            docker_cmd += ["--ulimit", f"nproc={ulimit_nproc}"]
        for k, v in sorted(env.items()):
            # Avoid passing empty keys; keep explicit.
            if str(k).strip():
                docker_cmd += ["-e", f"{k}={v}"]
        docker_cmd.append(self.image)
        docker_cmd += list(request.command)

        try:
            exit_code, stdout, stderr, _dur_ms = _run_subprocess_capture_output_with_limits(
                command=docker_cmd,
                cwd=str(effective_cwd),
                env=None,  # Let docker CLI inherit host environment.
                stdin_text=request.stdin_text,
                timeout_s=float(request.timeout_s) if request.timeout_s is not None else None,
                preexec_fn=None,
                stdout_max_bytes=self.stdout_max_bytes,
                stderr_max_bytes=self.stderr_max_bytes,
            )
            return ExecutionResult(
                exit_code=int(exit_code),
                stdout=str(stdout),
                stderr=str(stderr),
                duration_ms=int(_dur_ms),
            )
        except FileNotFoundError as e:
            return ExecutionResult(exit_code=127, stdout="", stderr=str(e), duration_ms=None)
