"""Integration: ``operational_verifier_findings`` in controller accounting reaches the verifier + manifest."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pytest

from akc.compile import ControllerConfig, CostRates, TierConfig
from akc.compile.controller_config import Budget
from akc.compile.interfaces import (
    ExecutionRequest,
    ExecutionResult,
    Executor,
    LLMBackend,
    LLMRequest,
    LLMResponse,
    TenantRepoScope,
)
from akc.compile.session import CompileSession
from akc.intent import IntentSpecV1, Objective, OperatingBound, SuccessCriterion
from akc.run import RunManifest


@dataclass(frozen=True)
class _FixedLLM(LLMBackend):
    patch_text: str

    def complete(  # type: ignore[override]
        self,
        *,
        scope: TenantRepoScope,
        stage: str,
        request: LLMRequest,
    ) -> LLMResponse:
        _ = (scope, stage, request)
        return LLMResponse(text=self.patch_text, raw=None, usage=None)


@dataclass
class _ScriptedExecutor(Executor):
    exit_codes: list[int]
    calls: int = 0

    def run(  # type: ignore[override]
        self,
        *,
        scope: TenantRepoScope,
        request: ExecutionRequest,
    ) -> ExecutionResult:
        _ = (scope, request)
        code = self.exit_codes[min(self.calls, len(self.exit_codes) - 1)]
        self.calls += 1
        return ExecutionResult(exit_code=int(code), stdout=f"call={self.calls}", stderr="", duration_ms=1)


def _mk_config(*, accounting_overlay: dict[str, object]) -> ControllerConfig:
    tiers = {
        "small": TierConfig(name="small", llm_model="fake-small", temperature=0.0),
    }
    return ControllerConfig(
        tiers=tiers,
        stage_tiers={"generate": "small", "repair": "small"},
        budget=Budget(max_llm_calls=2, max_repairs_per_step=0, max_iterations_total=2),
        test_mode="full",
        tool_allowlist=("llm.complete", "executor.run"),
        metadata={
            "execute_command": ["python", "-c", "print('ok')"],
            "execute_timeout_s": 1.0,
        },
        cost_rates=CostRates(),
        accounting_overlay=accounting_overlay,
    )


def _patch_that_touches_tests(*, keyword: str) -> str:
    return "\n".join(
        [
            "--- a/src/fake_module.py",
            "+++ b/src/fake_module.py",
            "@@ -1 +1 @@",
            f"+# {keyword}",
            "",
            "--- a/tests/test_fake_module.py",
            "+++ b/tests/test_fake_module.py",
            "@@ -1 +1 @@",
            "+def test_fake_module():",
            "+    assert True",
            "",
        ]
    )


def _intent_artifact_acceptance(*, tenant_id: str, repo_id: str, expected_keyword: str) -> IntentSpecV1:
    return IntentSpecV1(
        intent_id="intent_op_verify",
        tenant_id=tenant_id,
        repo_id=repo_id,
        spec_version=1,
        status="active",
        title="t",
        goal_statement="Goal",
        summary="s",
        derived_from_goal_text=False,
        objectives=(Objective(id="obj1", priority=1, statement="do it", target="achieve"),),
        constraints=(),
        policies=(),
        success_criteria=(
            SuccessCriterion(
                id="sc1",
                evaluation_mode="artifact_check",
                description="patch must include expected keyword",
                params={"expected_keywords": [expected_keyword]},
            ),
        ),
        operating_bounds=OperatingBound(max_seconds=None, max_steps=None, allow_network=False),
        assumptions=(),
        risk_notes=(),
        tags=(),
        metadata=None,
        created_at_ms=1,
        updated_at_ms=2,
    )


def test_compile_operational_verifier_findings_in_accounting_vetoes_verify_pass(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``operational_verifier_findings`` in ``accounting_overlay`` must reach the deterministic verifier."""

    tenant_id = "t1"
    repo_id = "r1"
    keyword = "EXPECTED_KEYWORD_PRESENT"
    intent = _intent_artifact_acceptance(tenant_id=tenant_id, repo_id=repo_id, expected_keyword=keyword)
    monkeypatch.setattr("akc.compile.controller.compile_intent_spec", lambda **_: intent)

    overlay: dict[str, object] = {
        "llm_calls": 0,
        "operational_verifier_findings": [
            {
                "code": "operational.attestation_failed",
                "message": "integration: precomputed operational gate from accounting overlay",
                "severity": "error",
            }
        ],
    }
    cfg = _mk_config(accounting_overlay=overlay)
    llm = _FixedLLM(patch_text=_patch_that_touches_tests(keyword=keyword))
    ex = _ScriptedExecutor(exit_codes=[0])

    session = CompileSession.from_memory(tenant_id=tenant_id, repo_id=repo_id)
    plan = session.memory.plan_state.create_plan(
        tenant_id=tenant_id,
        repo_id=repo_id,
        goal="Goal",
        initial_steps=["step1"],
    )
    session.memory.plan_state.set_active_plan(tenant_id=tenant_id, repo_id=repo_id, plan_id=plan.id)

    res = session.run(
        goal="Goal",
        llm=llm,
        executor=ex,
        config=cfg,
        outputs_root=tmp_path,
    )

    assert res.compile_succeeded is False
    assert res.status in {"failed", "budget_exhausted"}

    manifest_path = tmp_path / tenant_id / repo_id / ".akc" / "run" / f"{res.plan.id}.manifest.json"
    manifest = RunManifest.from_json_file(manifest_path)
    by_name = {p.name: p for p in manifest.passes}
    # When the overall compile does not succeed, pass records use a failed umbrella status, but
    # execute metadata still reflects the real test exit code (here: 0).
    exec_md = by_name["execute"].metadata or {}
    assert int(exec_md.get("exit_code", -1)) == 0
    assert by_name["execute"].status == "failed"
    assert by_name["verify"].status == "failed"
    assert by_name["intent_acceptance"].status == "skipped"

    ver_dir = tmp_path / tenant_id / repo_id / ".akc" / "verification"
    ver_files = sorted(ver_dir.glob("*.json"))
    assert len(ver_files) == 1
    verifier_path = ver_files[0]
    raw = json.loads(verifier_path.read_text(encoding="utf-8"))
    findings = raw.get("findings")
    assert isinstance(findings, list)
    codes = {str(f.get("code", "")) for f in findings if isinstance(f, dict)}
    assert "operational.attestation_failed" in codes
