from __future__ import annotations

import json
import logging
import os
import subprocess
import time
import uuid
from dataclasses import dataclass
from typing import Any, Literal

from akc.compile.interfaces import ExecutionRequest, ExecutionResult, TenantRepoScope

logger = logging.getLogger(__name__)

BackendMode = Literal["cli", "pyo3"]
ExecLane = Literal["process", "wasm"]

_TENANT_ID_ALLOWED_CHARS = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
)


def _validate_tenant_id(tenant_id: str) -> None:
    if not tenant_id:
        raise ValueError("tenant_id is required for Rust executor")
    if any(ch not in _TENANT_ID_ALLOWED_CHARS for ch in tenant_id):
        raise ValueError("tenant_id contains invalid characters for Rust executor")


def _emit(level: str, event: str, **fields: object) -> None:
    """Emit compact structured log lines (JSON) without payloads."""
    try:
        level_norm = level
        if level_norm == "warning":
            level_norm = "warn"

        if level_norm not in {"error", "warn", "info", "debug", "trace"}:
            level_norm = "info"

        ts_unix_ms = int(time.time() * 1000.0)
        pid = os.getpid()

        tenant_id = fields.pop("tenant_id", None)
        run_id = fields.pop("run_id", None)

        payload = {
            "ts_unix_ms": ts_unix_ms,
            "level": level_norm,
            "event": event,
            "pid": pid,
            "tenant_id": tenant_id,
            "run_id": run_id,
            **fields,
        }
        msg = json.dumps(
            payload,
            sort_keys=True,
            default=str,
            separators=(",", ":"),
        )
        if level_norm == "debug":
            logger.debug(msg)
        elif level_norm == "warn":
            logger.warning(msg)
        elif level_norm == "error":
            logger.error(msg)
        else:
            logger.info(msg)
    except Exception:
        # Logging must never break compilation/execution.
        pass


@dataclass(frozen=True, slots=True)
class RustExecConfig:
    """Configuration for the Rust-backed executor and ingest adapters.

    This is intentionally minimal for v1; additional limits/capabilities can be
    threaded through as we expand the Rust side.
    """

    mode: BackendMode = "cli"
    lane: ExecLane = "process"
    exec_bin: str = "akc-exec"
    ingest_bin: str = "akc-ingest"
    # Capabilities / policy toggles (v1).
    #
    # These map directly into `akc_protocol` request fields so the Rust executor
    # can enforce them consistently.
    allow_network: bool = False
    # Optional output/memory limits. For v1 we plumb these through to the Rust
    # executor, even though the controller may not yet provide all knobs.
    memory_bytes: int | None = None
    stdout_max_bytes: int | None = None
    stderr_max_bytes: int | None = None


@dataclass(frozen=True, slots=True)
class IngestRequest:
    """High-level ingest request.

    v1 supports a `docs` ingest kind that maps to `akc_protocol::IngestKind::Docs`.
    Tenant/run scoping is provided separately via `TenantRepoScope`.
    """

    @dataclass(frozen=True, slots=True)
    class Docs:
        """Ingest docs parameters for `akc-ingest`."""

        input_paths: tuple[str, ...]
        max_chunk_chars: int | None = None
        source_root: str | None = None

    docs: Docs | None = None


@dataclass(frozen=True, slots=True)
class IngestResult:
    """Result of an ingest operation."""

    ok: bool
    # Optional human-readable error when `ok` is False (validation/policy/CLI error).
    error: str | None = None
    # Optional normalized records produced by a successful ingest.
    # Stored as dicts to avoid duplicating the full protocol schema in Python.
    records: list[dict[str, Any]] | None = None


def _chunk_record_from_any(obj: object) -> dict[str, Any]:
    """Best-effort normalization of a single `ChunkRecord` JSON object."""

    if not isinstance(obj, dict):
        raise ValueError("ingest record must be a JSON object")
    # Rust emits keys like: tenant_id, source_id, chunk_id, content, metadata, fingerprint.
    return dict(obj)


def _request_from_scope_and_execution(
    *, cfg: RustExecConfig, scope: TenantRepoScope, request: ExecutionRequest
) -> dict:
    """Translate the existing ExecutionRequest into the akc_protocol ExecRequest schema."""

    _validate_tenant_id(scope.tenant_id)

    # For v1, we use the existing tenant_id as-is and generate a fresh, UUID-like
    # run_id for each execution. The shape here must stay in lockstep with
    # `akc_protocol::ExecRequest`.
    run_id = uuid.uuid4().hex

    return {
        "tenant_id": scope.tenant_id,
        "run_id": run_id,
        # Matches `ExecLane::Process` (serde `rename_all = "snake_case"` + `tag = "type"`),
        # see `akc_protocol::ExecLane`.
        "lane": {"type": cfg.lane},
        "capabilities": {"network": bool(cfg.allow_network)},
        "limits": {
            "wall_time_ms": int(request.timeout_s * 1000.0)
            if request.timeout_s is not None
            else None,
            "memory_bytes": cfg.memory_bytes,
            "stdout_max_bytes": cfg.stdout_max_bytes,
            "stderr_max_bytes": cfg.stderr_max_bytes,
        },
        # Command/cwd/env/stdin are forwarded so the Rust executor can spawn a
        # sandboxed process for the `process` lane.
        "command": list(request.command),
        "cwd": request.cwd,
        "env": dict(request.env) if request.env is not None else {},
        "stdin_text": request.stdin_text,
    }


def _ingest_request_from_scope(*, scope: TenantRepoScope, request: IngestRequest) -> dict:
    """Translate the ingest request into the akc_protocol IngestRequest schema."""

    _validate_tenant_id(scope.tenant_id)

    run_id = uuid.uuid4().hex
    payload: dict[str, Any] = {
        "tenant_id": scope.tenant_id,
        "run_id": run_id,
    }

    if request.docs is not None:
        if not request.docs.input_paths:
            raise ValueError("IngestRequest.docs.input_paths must be non-empty")
        payload["kind"] = {
            "type": "docs",
            "input_paths": list(request.docs.input_paths),
            "max_chunk_chars": request.docs.max_chunk_chars,
            "source_root": request.docs.source_root,
        }

    return payload


def run_exec_via_cli(
    *, cfg: RustExecConfig, scope: TenantRepoScope, request: ExecutionRequest
) -> ExecutionResult:
    """Call the `akc-exec` CLI with a JSON request and map the response into ExecutionResult."""

    payload = _request_from_scope_and_execution(cfg=cfg, scope=scope, request=request)
    run_id = payload["run_id"]
    program = request.command[0] if request.command else ""
    limits = payload.get("limits") or {}
    network_requested = (payload.get("capabilities") or {}).get("network", False)
    _emit(
        "info",
        "exec_surface_start",
        surface="cli",
        tenant_id=scope.tenant_id,
        run_id=run_id,
        lane=cfg.lane,
        program=program,
        network_requested=network_requested,
        wall_time_ms=limits.get("wall_time_ms"),
        stdout_max_bytes=limits.get("stdout_max_bytes"),
        stderr_max_bytes=limits.get("stderr_max_bytes"),
    )

    try:
        proc = subprocess.run(
            [cfg.exec_bin],
            input=json.dumps(payload),
            text=True,
            capture_output=True,
            check=False,
        )
    except FileNotFoundError as exc:
        _emit(
            "error",
            "exec_cli_binary_not_found",
            surface="cli",
            tenant_id=scope.tenant_id,
            run_id=run_id,
            error_kind="binary_not_found",
            exit_code=30,
            binary=cfg.exec_bin,
            stderr_bytes=len(str(exc)),
        )
        return ExecutionResult(
            exit_code=30,
            stdout="",
            stderr=str(exc),
            duration_ms=None,
        )

    # When the Rust executor succeeds (exit code 0), it should emit an `ExecResponse`
    # JSON object on stdout. We prefer that structured result over the raw CLI status,
    # while still retaining meaningful non-zero exit codes for validation/policy errors.
    if proc.returncode == 0 and proc.stdout:
        try:
            response = json.loads(proc.stdout)
        except json.JSONDecodeError:
            _emit(
                "error",
                "exec_cli_invalid_json",
                surface="cli",
                tenant_id=scope.tenant_id,
                run_id=run_id,
                error_kind="invalid_json",
                exit_code=proc.returncode,
                stderr_bytes=len(proc.stderr or ""),
            )
            # Fall back to treating the CLI like a simple process wrapper.
            return ExecutionResult(
                exit_code=int(proc.returncode),
                stdout=proc.stdout or "",
                stderr=proc.stderr or "",
                duration_ms=None,
            )

        ok = bool(response.get("ok", False))
        exit_code = int(response.get("exit_code", 0))
        stdout_val = str(response.get("stdout", ""))
        stderr_val = str(response.get("stderr", ""))
        _emit(
            "info" if ok else "warn",
            "exec_surface_complete",
            surface="cli",
            tenant_id=scope.tenant_id,
            run_id=run_id,
            ok=ok,
            exit_code=exit_code,
            stdout_bytes=len(stdout_val),
            stderr_bytes=len(stderr_val),
        )

        return ExecutionResult(
            exit_code=exit_code,
            stdout=stdout_val,
            stderr=stderr_val,
            duration_ms=None,
        )

    # Non-zero CLI exit codes are reserved for meta-errors (validation, policy denied,
    # internal errors). We surface them directly to the caller.
    _emit(
        "error",
        "exec_surface_error",
        surface="cli",
        tenant_id=scope.tenant_id,
        run_id=run_id,
        error_kind="unknown_nonzero_exit_code",
        exit_code=proc.returncode,
    )
    return ExecutionResult(
        exit_code=int(proc.returncode),
        stdout=proc.stdout or "",
        stderr=proc.stderr or "",
        duration_ms=None,
    )


def run_exec_via_pyo3(
    *, cfg: RustExecConfig, scope: TenantRepoScope, request: ExecutionRequest
) -> ExecutionResult:
    """Call the `akc_rust` PyO3 module with a JSON request and map the response."""

    # Import lazily to avoid hard dependency when Rust is disabled.
    try:
        import akc_rust  # type: ignore[import-not-found]
    except Exception as exc:  # pragma: no cover - import failure surfaced to caller
        raise RuntimeError("akc_rust PyO3 module is not available") from exc

    payload = _request_from_scope_and_execution(cfg=cfg, scope=scope, request=request)
    run_id = payload["run_id"]
    program = request.command[0] if request.command else ""
    limits = payload.get("limits") or {}
    network_requested = (payload.get("capabilities") or {}).get("network", False)
    _emit(
        "info",
        "exec_surface_start",
        surface="pyo3",
        tenant_id=scope.tenant_id,
        run_id=run_id,
        lane=cfg.lane,
        program=program,
        network_requested=network_requested,
        wall_time_ms=limits.get("wall_time_ms"),
        stdout_max_bytes=limits.get("stdout_max_bytes"),
        stderr_max_bytes=limits.get("stderr_max_bytes"),
    )
    response_json = akc_rust.run_exec_json(json.dumps(payload))
    response = json.loads(response_json)

    ok = bool(response.get("ok", False))
    exit_code = int(response.get("exit_code", 0))
    stdout_val = str(response.get("stdout", ""))
    stderr_val = str(response.get("stderr", ""))
    _emit(
        "info" if ok else "warn",
        "exec_surface_complete",
        surface="pyo3",
        tenant_id=scope.tenant_id,
        run_id=run_id,
        ok=ok,
        exit_code=exit_code,
        stdout_bytes=len(stdout_val),
        stderr_bytes=len(stderr_val),
    )

    return ExecutionResult(
        exit_code=exit_code,
        stdout=stdout_val,
        stderr=stderr_val,
        duration_ms=None,
    )


def run_ingest_via_cli(
    *, cfg: RustExecConfig, scope: TenantRepoScope, request: IngestRequest
) -> IngestResult:
    """Call the `akc-ingest` CLI with a JSON request and map the response."""

    payload = _ingest_request_from_scope(scope=scope, request=request)
    run_id = payload["run_id"]
    kind_label = "docs" if request.docs is not None else "none"
    input_paths_count = len(request.docs.input_paths) if request.docs is not None else 0
    _emit(
        "info",
        "ingest_surface_start",
        surface="cli",
        tenant_id=scope.tenant_id,
        run_id=run_id,
        kind=kind_label,
        input_paths_count=input_paths_count,
    )

    try:
        proc = subprocess.run(
            [cfg.ingest_bin],
            input=json.dumps(payload),
            text=True,
            capture_output=True,
            check=False,
        )
    except FileNotFoundError as exc:
        _emit(
            "error",
            "ingest_cli_binary_not_found",
            surface="cli",
            tenant_id=scope.tenant_id,
            run_id=run_id,
            error_kind="binary_not_found",
            exit_code=30,
            binary=cfg.ingest_bin,
            stderr_bytes=len(str(exc)),
        )
        return IngestResult(ok=False, error=str(exc))

    if proc.returncode == 0 and proc.stdout:
        try:
            response = json.loads(proc.stdout)
        except json.JSONDecodeError:
            _emit(
                "error",
                "ingest_cli_invalid_json",
                surface="cli",
                tenant_id=scope.tenant_id,
                run_id=run_id,
                error_kind="invalid_json",
                exit_code=proc.returncode,
                stderr_bytes=len(proc.stderr or ""),
            )
            return IngestResult(ok=False, error="invalid JSON from akc-ingest")

        ok = bool(response.get("ok", False))
        records_val = response.get("records", []) if isinstance(response, dict) else []
        records_list: list[dict[str, Any]] = []
        if isinstance(records_val, list):
            for rec in records_val:
                records_list.append(_chunk_record_from_any(rec))
        records_count = len(records_list)
        _emit(
            "info" if ok else "warn",
            "ingest_surface_complete",
            surface="cli",
            tenant_id=scope.tenant_id,
            run_id=run_id,
            ok=ok,
            records=records_count,
        )
        return IngestResult(ok=ok, records=records_list if ok else None)

    # Non-zero exit codes are meta-errors (validation, policy denied, internal).
    error_msg = proc.stderr or f"akc-ingest exited with code {proc.returncode}"
    _emit(
        "error",
        "ingest_surface_error",
        surface="cli",
        tenant_id=scope.tenant_id,
        run_id=run_id,
        error_kind="unknown_nonzero_exit_code",
        exit_code=proc.returncode,
    )
    return IngestResult(ok=False, error=error_msg, records=None)


def run_ingest_via_pyo3(
    *, cfg: RustExecConfig, scope: TenantRepoScope, request: IngestRequest
) -> IngestResult:
    """Call the `akc_rust` PyO3 module ingest entrypoint."""

    _ = cfg  # reserved for future configuration options

    try:
        import akc_rust  # type: ignore[import-not-found]
    except Exception as exc:  # pragma: no cover - import failure surfaced to caller
        raise RuntimeError("akc_rust PyO3 module is not available") from exc

    payload = _ingest_request_from_scope(scope=scope, request=request)
    run_id = payload["run_id"]
    kind_label = "docs" if request.docs is not None else "none"
    input_paths_count = len(request.docs.input_paths) if request.docs is not None else 0
    _emit(
        "info",
        "ingest_surface_start",
        surface="pyo3",
        tenant_id=scope.tenant_id,
        run_id=run_id,
        kind=kind_label,
        input_paths_count=input_paths_count,
    )
    response_json = akc_rust.ingest_json(json.dumps(payload))
    response = json.loads(response_json)

    ok = bool(response.get("ok", False))
    records_val = response.get("records", []) if isinstance(response, dict) else []
    records_list: list[dict[str, Any]] = []
    if isinstance(records_val, list):
        for rec in records_val:
            records_list.append(_chunk_record_from_any(rec))
    records_count = len(records_list)
    _emit(
        "info" if ok else "warn",
        "ingest_surface_complete",
        surface="pyo3",
        tenant_id=scope.tenant_id,
        run_id=run_id,
        ok=ok,
        records=records_count,
    )
    return IngestResult(ok=ok, records=records_list if ok else None)


def run_exec_with_rust(
    *, cfg: RustExecConfig, scope: TenantRepoScope, request: ExecutionRequest
) -> ExecutionResult:
    """Dispatch to either the CLI or PyO3-backed executor."""

    if cfg.mode == "cli":
        return run_exec_via_cli(cfg=cfg, scope=scope, request=request)
    if cfg.mode == "pyo3":
        return run_exec_via_pyo3(cfg=cfg, scope=scope, request=request)
    raise ValueError(f"Unsupported Rust backend mode: {cfg.mode!r}")


def run_ingest_with_rust(
    *, cfg: RustExecConfig, scope: TenantRepoScope, request: IngestRequest
) -> IngestResult:
    """Dispatch to either the CLI or PyO3-backed ingest adapter."""

    if cfg.mode == "cli":
        return run_ingest_via_cli(cfg=cfg, scope=scope, request=request)
    if cfg.mode == "pyo3":
        return run_ingest_via_pyo3(cfg=cfg, scope=scope, request=request)
    raise ValueError(f"Unsupported Rust backend mode: {cfg.mode!r}")
