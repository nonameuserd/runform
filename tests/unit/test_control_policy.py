from __future__ import annotations

import json
import subprocess
from pathlib import Path

from akc.compile.interfaces import TenantRepoScope
from akc.control import CapabilityIssuer, OpaInput, SubprocessOpaEvaluator


def test_subprocess_opa_evaluator_passes_policy_and_decision_path(monkeypatch) -> None:
    called: dict[str, object] = {}

    def _fake_run(*args, **kwargs):  # type: ignore[no-untyped-def]
        called["cmd"] = args[0]
        called["stdin"] = kwargs.get("input")
        return subprocess.CompletedProcess(
            args=args[0],
            returncode=0,
            stdout=json.dumps({"result": [{"expressions": [{"value": True}]}]}),
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", _fake_run)

    scope = TenantRepoScope(tenant_id="t1", repo_id="repo1")
    issuer = CapabilityIssuer()
    token = issuer.issue(scope=scope, action="executor.run")
    evaluator = SubprocessOpaEvaluator(
        policy_path="/tmp/policy.rego",
        decision_path="data.akc.allow",
    )
    ok, reason = evaluator.evaluate(
        opa_input=OpaInput(
            mode="enforce",
            scope=scope,
            action="executor.run",
            capability=token,
            context={"stage": "tests_full"},
        )
    )

    assert ok is True
    assert reason == "policy.opa.allow"
    cmd = list(called["cmd"])  # type: ignore[arg-type]
    assert "--stdin-input" in cmd
    assert "--data" in cmd
    assert "/tmp/policy.rego" in cmd
    assert "data.akc.allow" in cmd
    assert isinstance(called.get("stdin"), str)


def test_subprocess_opa_evaluator_denies_on_false_decision(monkeypatch) -> None:
    calls = 0

    def _fake_run(*args, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal calls
        _ = kwargs
        calls += 1
        stdout = (
            json.dumps({"result": [{"expressions": [{"value": False}]}]})
            if calls == 1
            else json.dumps({"result": [{"expressions": [{"value": "policy.executor.stage_not_allowed"}]}]})
        )
        return subprocess.CompletedProcess(
            args=args[0],
            returncode=0,
            stdout=stdout,
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", _fake_run)

    scope = TenantRepoScope(tenant_id="t1", repo_id="repo1")
    token = CapabilityIssuer().issue(scope=scope, action="llm.complete")
    ok, reason = SubprocessOpaEvaluator(
        policy_path="/tmp/policy.rego",
        decision_path="data.akc.allow",
    ).evaluate(
        opa_input=OpaInput(
            mode="audit_only",
            scope=scope,
            action="llm.complete",
            capability=token,
            context=None,
        )
    )

    assert ok is False
    assert reason == "policy.executor.stage_not_allowed"
    assert calls == 2


def test_prod_rego_keeps_repo_allowlist_and_execution_stage_guard() -> None:
    policy_path = Path("configs/policy/compile_tools_prod.rego")
    text = policy_path.read_text(encoding="utf-8")

    assert "approved_executor_repos[input.scope.repo_id]" in text
    assert 'allowed_executor_stages := {"tests_smoke", "tests_full"}' in text
    assert "allowed_executor_stages[input.context.stage]" in text
    assert "policy.prod.wasm.unsupported_control_required" in text
    assert "policy.prod.wasm.disallowed_preopen_path" in text
    assert "policy.prod.wasm.disallowed_writable_preopen_path" in text
    assert "policy.prod.wasm.network_requires_explicit_exception" in text
    assert "policy.prod.docker.read_only_rootfs_required" in text
    assert "policy.prod.docker.no_new_privileges_required" in text
    assert "policy.prod.docker.cap_drop_all_required" in text
    assert "policy.prod.docker.non_root_user_required" in text
    assert "policy.prod.docker.seccomp_profile_required" in text
    assert "policy.prod.docker.seccomp_profile_unconfined" in text
    assert "policy.prod.docker.apparmor_profile_required" in text
    assert "policy.prod.docker.apparmor_profile_unconfined" in text
    assert "policy.prod.docker.tmpfs_tmp_required" in text
    assert "policy.prod.docker.memory_limit_required" in text
    assert "policy.prod.docker.pids_limit_required" in text
    assert "policy.prod.docker.ulimit_nofile_required" in text
    assert "policy.prod.docker.ulimit_nproc_required" in text
    assert "policy.prod.docker.network_requires_explicit_exception" in text


def test_base_rego_requires_wasm_policy_context_contract() -> None:
    policy_path = Path("configs/policy/compile_tools.rego")
    text = policy_path.read_text(encoding="utf-8")

    assert 'input.context.backend == "wasm"' in text
    assert "policy.executor.wasm_context_missing" in text
    assert "policy.executor.wasm.writable_preopen_dirs_missing" in text
    assert "policy.executor.wasm.read_only_preopen_dirs_missing" in text
    assert "policy.executor.wasm.limits_tuple_missing" in text
    assert "policy.executor.wasm.platform_profile_missing" in text
    assert 'input.context.backend == "docker"' in text
    assert "policy.executor.docker_context_missing" in text
    assert "policy.executor.docker.no_new_privileges_missing" in text
    assert "policy.executor.docker.security_profiles_missing" in text
    assert "policy.executor.docker.limits_missing" in text
