from __future__ import annotations

import json
import subprocess
import uuid
from dataclasses import dataclass
from typing import Any

import pytest

from akc.compile import rust_bridge as rust_bridge_mod
from akc.compile.interfaces import ExecutionRequest, ExecutionResult, TenantRepoScope
from akc.compile.rust_bridge import (
    IngestRequest,
    IngestResult,
    RustExecConfig,
    WasmExecError,
    run_exec_via_cli,
    run_exec_via_pyo3,
    run_ingest_via_cli,
    run_ingest_via_pyo3,
)


@dataclass
class _FakeAkcRustModule:
    calls: list[dict[str, Any]]

    def run_exec_json(self, payload: str) -> str:  # noqa: D401 - test helper
        self.calls.append(json.loads(payload))
        # Echo back a simple, deterministic response.
        return json.dumps(
            {
                "tenant_id": "tenant-a",
                "run_id": "repo-a",
                "ok": True,
                "exit_code": 0,
                "stdout": "ok",
                "stderr": "",
            }
        )

    def ingest_json(self, payload: str) -> str:  # noqa: D401 - test helper
        self.calls.append(json.loads(payload))
        return json.dumps(
            {
                "tenant_id": "tenant-a",
                "run_id": "some-run",
                "ok": True,
            }
        )


def _make_scope_and_request() -> tuple[TenantRepoScope, ExecutionRequest]:
    scope = TenantRepoScope(tenant_id="tenant-a", repo_id="repo-a")
    req = ExecutionRequest(command=["echo", "hi"], timeout_s=1.5)
    return scope, req


def test_run_exec_via_pyo3_happy_path(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeAkcRustModule(calls=[])

    # Patch the akc_rust module import to use a fake implementation.
    monkeypatch.setitem(
        __import__("sys").modules,
        "akc_rust",
        fake,
    )

    scope, req = _make_scope_and_request()
    res: ExecutionResult = run_exec_via_pyo3(
        cfg=RustExecConfig(mode="pyo3"), scope=scope, request=req
    )

    assert res.exit_code == 0
    assert res.stdout == "ok"
    assert res.stderr == ""

    assert len(fake.calls) == 1
    sent = fake.calls[0]
    assert sent["tenant_id"] == "tenant-a"
    # A fresh, UUID-like run_id should be generated per call and must be non-empty.
    assert isinstance(sent["run_id"], str)
    assert sent["run_id"]
    # The protocol enforces path-safe characters (alnum, '-', '_'); UUID hex satisfies this.
    assert all(ch.isalnum() or ch in "-_" for ch in sent["run_id"])
    assert sent["lane"]["type"] == "process"
    assert sent["capabilities"]["network"] is False
    assert sent["limits"]["wall_time_ms"] == 1500
    assert sent["limits"]["cpu_fuel"] is None
    # New fs_policy fields are always emitted for CLI/PyO3 parity.
    assert "fs_policy" in sent
    assert sent["fs_policy"]["allowed_read_paths"] == []
    assert sent["fs_policy"]["allowed_write_paths"] == []
    assert sent["fs_policy"]["preopen_dirs"] == []


def test_run_exec_via_cli_does_not_crash_without_binary(monkeypatch: pytest.MonkeyPatch) -> None:
    scope, req = _make_scope_and_request()

    # Point to a clearly non-existent binary; we only assert that the wrapper returns
    # an ExecutionResult.
    cfg = RustExecConfig(mode="cli", exec_bin="__definitely_missing_binary__")

    res = run_exec_via_cli(cfg=cfg, scope=scope, request=req)
    assert isinstance(res, ExecutionResult)
    assert res.exit_code != 0


def test_run_exec_via_cli_prefers_structured_error_response_even_on_nonzero_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scope, req = _make_scope_and_request()

    def _fake_run(*args: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        _ = args, kwargs
        return subprocess.CompletedProcess(
            args=["akc-exec"],
            returncode=20,
            stdout=json.dumps(
                {
                    "tenant_id": "tenant-a",
                    "run_id": "run-1",
                    "ok": False,
                    "exit_code": 20,
                    "stdout": "",
                    "stderr": "policy denied",
                }
            ),
            stderr="legacy stderr that should not win",
        )

    monkeypatch.setattr(rust_bridge_mod.subprocess, "run", _fake_run)

    res = run_exec_via_cli(cfg=RustExecConfig(mode="cli"), scope=scope, request=req)
    assert res.exit_code == 20
    assert res.stdout == ""
    assert res.stderr == "policy denied"


def test_run_exec_via_pyo3_accepts_structured_error_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @dataclass
    class _FakeAkcRustErrorModule:
        calls: list[dict[str, Any]]

        def run_exec_json(self, payload: str) -> str:
            self.calls.append(json.loads(payload))
            return json.dumps(
                {
                    "tenant_id": "tenant-a",
                    "run_id": "run-1",
                    "ok": False,
                    "exit_code": 20,
                    "stdout": "",
                    "stderr": "policy denied",
                }
            )

    fake = _FakeAkcRustErrorModule(calls=[])
    monkeypatch.setitem(__import__("sys").modules, "akc_rust", fake)

    scope, req = _make_scope_and_request()
    res = run_exec_via_pyo3(cfg=RustExecConfig(mode="pyo3"), scope=scope, request=req)

    assert res.exit_code == 20
    assert res.stdout == ""
    assert res.stderr == "policy denied"


def test_run_ingest_via_pyo3_happy_path(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeAkcRustModule(calls=[])

    monkeypatch.setitem(
        __import__("sys").modules,
        "akc_rust",
        fake,
    )

    scope, _ = _make_scope_and_request()
    ingest_req = IngestRequest()

    res: IngestResult = run_ingest_via_pyo3(
        cfg=RustExecConfig(mode="pyo3"), scope=scope, request=ingest_req
    )

    assert res.ok is True
    assert len(fake.calls) == 1
    sent = fake.calls[0]
    assert sent["tenant_id"] == "tenant-a"
    assert isinstance(sent["run_id"], str)
    assert sent["run_id"]


def test_run_ingest_via_pyo3_maps_messaging_kind(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeAkcRustModule(calls=[])

    monkeypatch.setitem(
        __import__("sys").modules,
        "akc_rust",
        fake,
    )

    scope, _ = _make_scope_and_request()
    ingest_req = IngestRequest(
        messaging=IngestRequest.Messaging(export_path="/abs/path/to/export.jsonl")
    )

    _ = run_ingest_via_pyo3(cfg=RustExecConfig(mode="pyo3"), scope=scope, request=ingest_req)

    assert len(fake.calls) == 1
    sent = fake.calls[0]
    assert sent["tenant_id"] == "tenant-a"
    assert "run_id" in sent and isinstance(sent["run_id"], str) and sent["run_id"]
    assert sent["kind"]["type"] == "messaging"
    assert sent["kind"]["export_path"] == "/abs/path/to/export.jsonl"


def test_run_ingest_via_pyo3_maps_api_kind(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeAkcRustModule(calls=[])

    monkeypatch.setitem(
        __import__("sys").modules,
        "akc_rust",
        fake,
    )

    scope, _ = _make_scope_and_request()
    ingest_req = IngestRequest(api=IngestRequest.Api(openapi_path="/abs/path/to/openapi.json"))

    _ = run_ingest_via_pyo3(cfg=RustExecConfig(mode="pyo3"), scope=scope, request=ingest_req)

    assert len(fake.calls) == 1
    sent = fake.calls[0]
    assert sent["tenant_id"] == "tenant-a"
    assert "run_id" in sent and isinstance(sent["run_id"], str) and sent["run_id"]
    assert sent["kind"]["type"] == "api"
    assert sent["kind"]["openapi_path"] == "/abs/path/to/openapi.json"


def test_run_ingest_via_cli_does_not_crash_without_binary() -> None:
    scope, _ = _make_scope_and_request()
    ingest_req = IngestRequest()

    cfg = RustExecConfig(mode="cli", ingest_bin="__definitely_missing_binary__")

    res = run_ingest_via_cli(cfg=cfg, scope=scope, request=ingest_req)
    assert isinstance(res, IngestResult)
    assert res.ok is False


def test_rust_bridge_rejects_invalid_tenant_id_characters() -> None:
    scope = TenantRepoScope(tenant_id="evil/tenant", repo_id="repo-a")
    req = ExecutionRequest(command=["echo", "hi"], timeout_s=0.1)
    with pytest.raises(ValueError, match="tenant_id"):
        _ = run_exec_via_cli(
            cfg=RustExecConfig(mode="cli", exec_bin="__definitely_missing_binary__"),
            scope=scope,
            request=req,
        )


def test_rust_bridge_rejects_missing_tenant_id() -> None:
    req = ExecutionRequest(command=["echo", "hi"], timeout_s=0.1)
    with pytest.raises(ValueError, match="tenant_id"):
        scope = TenantRepoScope(tenant_id="", repo_id="repo-a")
        _ = run_exec_via_cli(
            cfg=RustExecConfig(mode="cli", exec_bin="__definitely_missing_binary__"),
            scope=scope,
            request=req,
        )


def test_rust_bridge_rejects_relative_fs_policy_paths() -> None:
    scope, req = _make_scope_and_request()
    cfg = RustExecConfig(
        mode="pyo3",
        allowed_read_paths=("relative/path",),
    )
    with pytest.raises(ValueError, match="absolute paths"):
        _ = run_exec_via_pyo3(cfg=cfg, scope=scope, request=req)


def test_rust_bridge_rejects_wasm_lane_allowlist_paths() -> None:
    scope, req = _make_scope_and_request()
    cfg = RustExecConfig(
        mode="pyo3",
        lane="wasm",
        allowed_read_paths=("/tmp",),
    )
    with pytest.raises(ValueError, match="allowed_read_paths"):
        _ = run_exec_via_pyo3(cfg=cfg, scope=scope, request=req)


def test_rust_bridge_allows_wasm_write_paths_for_preopens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeAkcRustModule(calls=[])
    monkeypatch.setitem(__import__("sys").modules, "akc_rust", fake)
    scope, req = _make_scope_and_request()
    cfg = RustExecConfig(
        mode="pyo3",
        lane="wasm",
        preopen_dirs=("/tmp",),
        allowed_write_paths=("/tmp",),
    )

    _ = run_exec_via_pyo3(cfg=cfg, scope=scope, request=req)
    sent = fake.calls[0]
    assert sent["lane"]["type"] == "wasm"
    assert sent["fs_policy"]["preopen_dirs"] == ["/tmp"]
    assert sent["fs_policy"]["allowed_write_paths"] == ["/tmp"]


def test_rust_bridge_rejects_wasm_write_paths_without_preopens() -> None:
    scope, req = _make_scope_and_request()
    cfg = RustExecConfig(
        mode="pyo3",
        lane="wasm",
        allowed_write_paths=("/tmp",),
    )
    with pytest.raises(ValueError, match="preopen_dirs"):
        _ = run_exec_via_pyo3(cfg=cfg, scope=scope, request=req)


def test_rust_bridge_rejects_wasm_write_paths_not_in_preopens() -> None:
    scope, req = _make_scope_and_request()
    cfg = RustExecConfig(
        mode="pyo3",
        lane="wasm",
        preopen_dirs=("/tmp/a",),
        allowed_write_paths=("/tmp/b",),
    )
    with pytest.raises(ValueError, match="subset of preopen_dirs"):
        _ = run_exec_via_pyo3(cfg=cfg, scope=scope, request=req)


def test_rust_bridge_rejects_non_positive_cpu_fuel() -> None:
    scope, req = _make_scope_and_request()
    cfg = RustExecConfig(mode="pyo3", lane="wasm", cpu_fuel=0)
    with pytest.raises(ValueError, match="cpu_fuel"):
        _ = run_exec_via_pyo3(cfg=cfg, scope=scope, request=req)


def test_rust_bridge_wasm_normalization_allows_canonical_subset_match(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    fake = _FakeAkcRustModule(calls=[])
    monkeypatch.setitem(__import__("sys").modules, "akc_rust", fake)

    real_dir = tmp_path / "real"
    real_dir.mkdir()
    alias_dir = tmp_path / "alias"
    alias_dir.symlink_to(real_dir, target_is_directory=True)

    scope, req = _make_scope_and_request()
    cfg = RustExecConfig(
        mode="pyo3",
        lane="wasm",
        preopen_dirs=(str(real_dir),),
        allowed_write_paths=(str(alias_dir),),
        wasm_normalize_existing_paths=True,
        wasm_normalization_strict=True,
    )

    _ = run_exec_via_pyo3(cfg=cfg, scope=scope, request=req)
    sent = fake.calls[0]
    canonical = str(real_dir.resolve())
    assert sent["fs_policy"]["preopen_dirs"] == [canonical]
    assert sent["fs_policy"]["allowed_write_paths"] == [canonical]


def test_rust_bridge_wasm_normalization_strict_rejects_missing_path() -> None:
    scope, req = _make_scope_and_request()
    missing = f"/tmp/akc-bridge-missing-preopen-dir-{uuid.uuid4().hex}"
    cfg = RustExecConfig(
        mode="pyo3",
        lane="wasm",
        preopen_dirs=(missing,),
        allowed_write_paths=(missing,),
        wasm_normalize_existing_paths=True,
        wasm_normalization_strict=True,
    )
    with pytest.raises(ValueError, match="does not exist"):
        _ = run_exec_via_pyo3(cfg=cfg, scope=scope, request=req)


def test_rust_bridge_wasm_normalization_relaxed_keeps_unresolved_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeAkcRustModule(calls=[])
    monkeypatch.setitem(__import__("sys").modules, "akc_rust", fake)

    scope, req = _make_scope_and_request()
    missing = f"/tmp/akc-bridge-relaxed-unresolved-{uuid.uuid4().hex}"
    cfg = RustExecConfig(
        mode="pyo3",
        lane="wasm",
        preopen_dirs=(missing,),
        allowed_write_paths=(missing,),
        wasm_normalize_existing_paths=True,
        wasm_normalization_strict=False,
    )

    _ = run_exec_via_pyo3(cfg=cfg, scope=scope, request=req)
    sent = fake.calls[0]
    assert sent["fs_policy"]["preopen_dirs"] == [missing]
    assert sent["fs_policy"]["allowed_write_paths"] == [missing]


def test_parse_wasm_error_marker_success() -> None:
    stderr = "AKC_WASM_ERROR code=WASM_TIMEOUT exit_code=124 message=wasm wall-time budget exceeded"
    parsed = rust_bridge_mod._parse_wasm_error(stderr, exit_code=124)
    assert parsed == WasmExecError(
        code="WASM_TIMEOUT",
        exit_code=124,
        message="wasm wall-time budget exceeded",
    )


def test_parse_wasm_error_marker_rejects_mismatched_exit_code() -> None:
    stderr = (
        "AKC_WASM_ERROR code=WASM_CPU_FUEL_EXHAUSTED exit_code=137 "
        "message=wasm cpu/fuel budget exhausted"
    )
    parsed = rust_bridge_mod._parse_wasm_error(stderr, exit_code=124)
    assert parsed is None


def test_ensure_wasm_error_marker_synthesizes_marker_for_known_exit_code() -> None:
    stderr = "wasm wall-time budget exceeded"
    normalized = rust_bridge_mod._ensure_wasm_error_marker(stderr, exit_code=124)
    parsed = rust_bridge_mod._parse_wasm_error(normalized, exit_code=124)
    assert parsed == WasmExecError(
        code="WASM_TIMEOUT",
        exit_code=124,
        message="wasm wall-time budget exceeded",
    )


def test_run_exec_via_pyo3_wasm_normalizes_marker_from_exit_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @dataclass
    class _FakeAkcRustWasmFailureModule:
        calls: list[dict[str, Any]]

        def run_exec_json(self, payload: str) -> str:
            self.calls.append(json.loads(payload))
            return json.dumps(
                {
                    "tenant_id": "tenant-a",
                    "run_id": "repo-a",
                    "ok": False,
                    "exit_code": 124,
                    "stdout": "",
                    "stderr": "wasm wall-time budget exceeded",
                }
            )

    fake = _FakeAkcRustWasmFailureModule(calls=[])
    monkeypatch.setitem(__import__("sys").modules, "akc_rust", fake)
    scope, req = _make_scope_and_request()
    cfg = RustExecConfig(mode="pyo3", lane="wasm")

    res = run_exec_via_pyo3(cfg=cfg, scope=scope, request=req)
    assert res.exit_code == 124
    assert res.stderr.startswith("AKC_WASM_ERROR code=WASM_TIMEOUT exit_code=124")
