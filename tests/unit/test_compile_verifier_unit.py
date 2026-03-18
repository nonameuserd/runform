from __future__ import annotations

from collections.abc import Mapping

from akc.compile.controller_config import Budget
from akc.compile.interfaces import ExecutionResult, TenantRepoScope
from akc.compile.verifier import (
    DeterministicVerifier,
    VerifierFinding,
    VerifierPolicy,
)


def _mk_scope() -> TenantRepoScope:
    return TenantRepoScope(tenant_id="t1", repo_id="repo1")


def _mk_exec(exit_code: int = 0) -> ExecutionResult:
    return ExecutionResult(exit_code=int(exit_code), stdout="ok", stderr="", duration_ms=1)


def _mk_budget(max_llm_calls: int = 5) -> Budget:
    return Budget(max_llm_calls=max_llm_calls, max_repairs_per_step=1, max_iterations_total=2)


def _run(
    *,
    patch: str,
    policy: VerifierPolicy,
    execution: ExecutionResult | None = None,
    accounting: Mapping[str, int] | None = None,
) -> tuple[bool, list[VerifierFinding]]:
    v = DeterministicVerifier()
    res = v.verify(
        scope=_mk_scope(),
        plan_id="p1",
        step_id="s1",
        candidate_patch=patch,
        execution=execution,
        accounting=accounting or {"llm_calls": 0},
        budget=_mk_budget(),
        policy=policy,
    )
    return res.passed, list(res.findings)


def test_verifier_policy_disabled_always_passes_and_emits_no_findings() -> None:
    passed, findings = _run(
        patch="--- a/x.py\n+++ b/x.py\n",
        policy=VerifierPolicy(enabled=False),
        execution=_mk_exec(0),
    )
    assert passed is True
    assert findings == []


def test_verifier_path_traversal_is_reported_and_blocks_when_strict() -> None:
    patch = "\n".join(
        [
            "--- a/src/ok.py",
            "+++ b/../evil.py",
            "@@",
            "+print('nope')",
            "",
        ]
    )
    passed, findings = _run(patch=patch, policy=VerifierPolicy(strict=True), execution=_mk_exec(0))
    assert passed is False
    codes = {f.code for f in findings}
    assert "patch.path_suspicious" in codes


def test_verifier_missing_execution_is_warning_and_blocks_when_strict_only() -> None:
    policy = VerifierPolicy(strict=True)
    passed, findings = _run(patch="--- a/x.py\n+++ b/x.py\n", policy=policy, execution=None)
    assert passed is False
    assert any(f.code == "execution.missing" and f.severity == "warning" for f in findings)

    relaxed = VerifierPolicy(strict=False)
    passed2, findings2 = _run(patch="--- a/x.py\n+++ b/x.py\n", policy=relaxed, execution=None)
    assert passed2 is True
    assert any(f.code == "execution.missing" for f in findings2)


def test_verifier_budget_accounting_exceeded_emits_error_and_blocks() -> None:
    policy = VerifierPolicy(strict=True)
    passed, findings = _run(
        patch="--- a/x.py\n+++ b/x.py\n",
        policy=policy,
        execution=_mk_exec(0),
        accounting={"llm_calls": 10},
    )
    assert passed is False
    assert any(f.code == "budget.llm_calls_exceeded" for f in findings)
