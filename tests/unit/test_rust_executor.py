from __future__ import annotations

import os
from pathlib import Path

import pytest

from akc.compile.execute.rust_executor import RustExecutor
from akc.compile.interfaces import ExecutionRequest, ExecutionResult, TenantRepoScope
from akc.compile.rust_bridge import RustExecConfig


def test_rust_executor_sets_allowlist_and_root_and_computes_duration_ms(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    scope = TenantRepoScope(tenant_id="t1", repo_id="repo1")
    req = ExecutionRequest(command=["python", "-c", "print('ok')"], timeout_s=1.0)

    # Set a pre-existing env var so we can assert restoration.
    monkeypatch.setenv("AKC_EXEC_ALLOWLIST", "echo")
    monkeypatch.delenv("AKC_EXEC_ROOT", raising=False)

    import akc.compile.execute.rust_executor as rust_exec_mod

    def _fake_run_exec_with_rust(
        *, cfg: RustExecConfig, scope: TenantRepoScope, request: ExecutionRequest
    ) -> ExecutionResult:  # type: ignore[override]
        assert cfg.allow_network is False
        assert os.environ.get("AKC_EXEC_ALLOWLIST") == "python"
        assert os.environ.get("AKC_EXEC_ROOT") == str(Path(tmp_path).expanduser().resolve())
        # Rust clears env; wrapper should at least propagate PATH for tool resolution.
        assert request.env is not None
        assert "PATH" in request.env
        return ExecutionResult(exit_code=0, stdout="ok", stderr="", duration_ms=None)

    monkeypatch.setattr(rust_exec_mod, "run_exec_with_rust", _fake_run_exec_with_rust)

    ex = RustExecutor(rust_cfg=RustExecConfig(mode="cli"), work_root=tmp_path)
    res = ex.run(scope=scope, request=req)

    assert res.exit_code == 0
    assert res.stdout == "ok"
    assert res.stderr == ""
    assert res.duration_ms is not None and res.duration_ms >= 0

    # Ensure env restoration happens after the call.
    assert os.environ.get("AKC_EXEC_ALLOWLIST") == "echo"
    assert os.environ.get("AKC_EXEC_ROOT") is None


@pytest.mark.parametrize("mode", ["cli", "pyo3"])
def test_rust_executor_plumbs_fs_policy_and_limits_and_keeps_surface_parity(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, mode: str
) -> None:
    scope = TenantRepoScope(tenant_id="t1", repo_id="repo1")
    req = ExecutionRequest(command=["python", "-c", "print('ok')"], timeout_s=2.0)

    import akc.compile.execute.rust_executor as rust_exec_mod

    def _fake_run_exec_with_rust(
        *, cfg: RustExecConfig, scope: TenantRepoScope, request: ExecutionRequest
    ) -> ExecutionResult:
        assert cfg.mode == mode
        # Capability fields (deny-by-default unless explicitly enabled).
        assert cfg.allow_network is False
        # Limits are passed via config to rust_bridge payload.
        assert cfg.memory_bytes == 123
        assert cfg.stdout_max_bytes == 456
        assert cfg.stderr_max_bytes == 789
        # FS policy capability fields must be preserved exactly.
        assert cfg.allowed_read_paths == ("/etc/hosts",)
        assert cfg.allowed_write_paths == ("/tmp/out.txt",)
        assert cfg.preopen_dirs == ()

        # Tenant isolation: RustExecutor sets workspace root in env (namespacing).
        assert os.environ.get("AKC_EXEC_ROOT") == str(Path(tmp_path).expanduser().resolve())
        # RustExecutor allowlist defaults to the requested program only.
        assert os.environ.get("AKC_EXEC_ALLOWLIST") == "python"

        # RustExecutor should still propagate host PATH into request.env (when present).
        assert request.env is not None
        assert "PATH" in request.env

        return ExecutionResult(exit_code=0, stdout="ok", stderr="", duration_ms=None)

    monkeypatch.setattr(rust_exec_mod, "run_exec_with_rust", _fake_run_exec_with_rust)

    ex = RustExecutor(
        rust_cfg=RustExecConfig(
            mode=mode,  # parity across surfaces: only this should differ
            allow_network=False,
            memory_bytes=123,
            stdout_max_bytes=456,
            stderr_max_bytes=789,
            allowed_read_paths=("/etc/hosts",),
            allowed_write_paths=("/tmp/out.txt",),
        ),
        work_root=tmp_path,
    )
    res = ex.run(scope=scope, request=req)
    assert res.exit_code == 0
