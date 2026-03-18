from __future__ import annotations

import os
from pathlib import Path

import pytest

from akc.compile.execute.rust_executor import RustExecutor
from akc.compile.interfaces import ExecutionRequest, ExecutionResult, TenantRepoScope
from akc.compile.rust_bridge import RustExecConfig


def test_rust_executor_sets_allowlist_and_root_and_computes_duration_ms(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    scope = TenantRepoScope(tenant_id="t1", repo_id="repo1")
    req = ExecutionRequest(command=["python", "-c", "print('ok')"], timeout_s=1.0)

    # Set a pre-existing env var so we can assert restoration.
    monkeypatch.setenv("AKC_EXEC_ALLOWLIST", "echo")
    monkeypatch.delenv("AKC_EXEC_ROOT", raising=False)

    import akc.compile.execute.rust_executor as rust_exec_mod

    def _fake_run_exec_with_rust(*, cfg: RustExecConfig, scope: TenantRepoScope, request: ExecutionRequest) -> ExecutionResult:  # type: ignore[override]
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

