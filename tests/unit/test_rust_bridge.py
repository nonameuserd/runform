from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import pytest

from akc.compile.interfaces import ExecutionRequest, ExecutionResult, TenantRepoScope
from akc.compile.rust_bridge import (
    IngestRequest,
    IngestResult,
    RustExecConfig,
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


def test_run_exec_via_cli_does_not_crash_without_binary(monkeypatch: pytest.MonkeyPatch) -> None:
    scope, req = _make_scope_and_request()

    # Point to a clearly non-existent binary; we only assert that the wrapper returns
    # an ExecutionResult.
    cfg = RustExecConfig(mode="cli", exec_bin="__definitely_missing_binary__")

    res = run_exec_via_cli(cfg=cfg, scope=scope, request=req)
    assert isinstance(res, ExecutionResult)
    assert res.exit_code != 0


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
