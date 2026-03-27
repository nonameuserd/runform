from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

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
from akc.intent import IntentSpecV1, Objective, OperatingBound, QualityContract, QualityDimensionSpec, SuccessCriterion
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
        # Keep it simple: we only care about exit_code deterministically.
        _ = (scope, request)
        code = self.exit_codes[min(self.calls, len(self.exit_codes) - 1)]
        self.calls += 1
        return ExecutionResult(exit_code=int(code), stdout=f"call={self.calls}", stderr="", duration_ms=1)


def _mk_config(*, max_repairs_per_step: int, accounting_overlay: dict[str, object] | None = None) -> ControllerConfig:
    tiers = {
        "small": TierConfig(name="small", llm_model="fake-small", temperature=0.0),
    }
    return ControllerConfig(
        tiers=tiers,
        stage_tiers={"generate": "small", "repair": "small"},
        budget=Budget(max_llm_calls=2, max_repairs_per_step=max_repairs_per_step, max_iterations_total=2),
        test_mode="full",
        tool_allowlist=("llm.complete", "executor.run"),
        # Provide an explicit command even though our scripted executor ignores it.
        metadata={
            "execute_command": ["python", "-c", "print('ok')"],
            "execute_timeout_s": 1.0,
        },
        cost_rates=CostRates(),
        accounting_overlay=accounting_overlay,
    )


def _mk_intent_with_artifact_acceptance(
    *,
    tenant_id: str,
    repo_id: str,
    expected_keyword: str,
) -> IntentSpecV1:
    return IntentSpecV1(
        intent_id="intent_phase5_artifact",
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
        quality_contract=_quality_contract(engineering_stage="advisory"),
        assumptions=(),
        risk_notes=(),
        tags=(),
        metadata=None,
        created_at_ms=1,
        updated_at_ms=2,
    )


def _patch_that_touches_tests(*, keyword: str) -> str:
    # Deterministic unified diff with valid file headers and touched paths.
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


def _quality_contract(*, engineering_stage: Literal["advisory", "gate"]) -> QualityContract:
    dims: dict[str, QualityDimensionSpec] = {}
    for dim in (
        "taste",
        "domain_knowledge",
        "judgment",
        "instincts",
        "user_empathy",
        "engineering_discipline",
    ):
        dims[dim] = QualityDimensionSpec(
            target_score=0.75,
            gate_min_score=0.6,
            weight=1.0,
            evidence_requirements=(),
            enforcement_stage="advisory",
        )
    dims["engineering_discipline"] = QualityDimensionSpec(
        target_score=0.8,
        gate_min_score=0.99,
        weight=1.0,
        evidence_requirements=("nonexistent_signal",),
        enforcement_stage=engineering_stage,
    )
    return QualityContract(dimensions=dims)


def test_intent_acceptance_failure_separates_compile_and_intent_satisfied(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tenant_id = "t1"
    repo_id = "r1"

    # Compile tests must pass, but intent acceptance must fail on keyword mismatch.
    intent = _mk_intent_with_artifact_acceptance(
        tenant_id=tenant_id,
        repo_id=repo_id,
        expected_keyword="THIS_KEYWORD_SHOULD_NOT_BE_IN_PATCH",
    )

    monkeypatch.setattr("akc.compile.controller.compile_intent_spec", lambda **_: intent)

    cfg = _mk_config(max_repairs_per_step=1)
    llm = _FixedLLM(patch_text=_patch_that_touches_tests(keyword="different"))
    ex = _ScriptedExecutor(exit_codes=[0, 0])

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

    assert res.compile_succeeded is True
    assert res.intent_satisfied is False
    assert res.status in {"failed", "budget_exhausted"}

    # Manifest should reflect:
    # - `execute` succeeded (compile gate passed)
    # - `intent_acceptance` failed (acceptance gate rejected)
    manifest_path = tmp_path / tenant_id / repo_id / ".akc" / "run" / f"{res.plan.id}.manifest.json"
    manifest = RunManifest.from_json_file(manifest_path)
    by_name = {p.name: p for p in manifest.passes}

    assert by_name["execute"].status == "succeeded"
    assert by_name["intent_acceptance"].status == "failed"
    assert by_name["execute"].metadata is not None
    assert int(by_name["execute"].metadata.get("exit_code", -1)) == 0


def test_intent_acceptance_success_returns_succeeded(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    tenant_id = "t1"
    repo_id = "r1"

    expected = "EXPECTED_KEYWORD_PRESENT"
    intent = _mk_intent_with_artifact_acceptance(
        tenant_id=tenant_id,
        repo_id=repo_id,
        expected_keyword=expected,
    )
    monkeypatch.setattr("akc.compile.controller.compile_intent_spec", lambda **_: intent)

    cfg = _mk_config(max_repairs_per_step=1)
    llm = _FixedLLM(patch_text=_patch_that_touches_tests(keyword=expected))
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

    assert res.compile_succeeded is True
    assert res.intent_satisfied is True
    assert res.status == "succeeded"

    manifest_path = tmp_path / tenant_id / repo_id / ".akc" / "run" / f"{res.plan.id}.manifest.json"
    manifest = RunManifest.from_json_file(manifest_path)
    by_name = {p.name: p for p in manifest.passes}
    assert by_name["intent_acceptance"].status == "succeeded"


def test_intent_acceptance_operational_compile_phase_matches_fixture_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Compile with operational_spec + evaluation_phase=compile uses accounting overlay evidence."""

    tenant_id = "t1"
    repo_id = "r1"
    fixture_path = Path(__file__).resolve().parents[1] / "fixtures" / "operational_eval" / "healthy_pass.json"
    raw = json.loads(fixture_path.read_text(encoding="utf-8"))
    params = dict(raw["params"])
    params["evaluation_phase"] = "compile"
    intent = IntentSpecV1(
        intent_id="intent_op_compile",
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
                evaluation_mode="operational_spec",
                description="operational gate",
                params=params,
            ),
        ),
        operating_bounds=OperatingBound(max_seconds=None, max_steps=None, allow_network=False),
        quality_contract=_quality_contract(engineering_stage="advisory"),
        assumptions=(),
        risk_notes=(),
        tags=(),
        metadata=None,
        created_at_ms=1,
        updated_at_ms=2,
    )
    monkeypatch.setattr("akc.compile.controller.compile_intent_spec", lambda **_: intent)

    overlay = {
        "operational_compile_bundle": {
            "runtime_evidence_records": raw["evidence"],
        }
    }
    cfg = _mk_config(max_repairs_per_step=1, accounting_overlay=overlay)
    llm = _FixedLLM(patch_text=_patch_that_touches_tests(keyword="EXPECTED_KEYWORD_PRESENT"))
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

    assert res.compile_succeeded is True
    assert res.intent_satisfied is True
    assert res.status == "succeeded"

    manifest_path = tmp_path / tenant_id / repo_id / ".akc" / "run" / f"{res.plan.id}.manifest.json"
    manifest = RunManifest.from_json_file(manifest_path)
    by_name = {p.name: p for p in manifest.passes}
    assert by_name["verify"].status == "succeeded"
    assert by_name["intent_acceptance"].status == "succeeded"


def test_quality_contract_advisory_does_not_block_compile(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    tenant_id = "t1"
    repo_id = "r1"
    intent = _mk_intent_with_artifact_acceptance(
        tenant_id=tenant_id,
        repo_id=repo_id,
        expected_keyword="EXPECTED_KEYWORD_PRESENT",
    )
    intent = IntentSpecV1.from_json_obj(
        {
            **intent.to_json_obj(),
            "quality_contract": _quality_contract(engineering_stage="advisory").to_json_obj(),
        }
    ).normalized()
    monkeypatch.setattr("akc.compile.controller.compile_intent_spec", lambda **_: intent)

    cfg = _mk_config(max_repairs_per_step=1)
    llm = _FixedLLM(patch_text=_patch_that_touches_tests(keyword="EXPECTED_KEYWORD_PRESENT"))
    ex = _ScriptedExecutor(exit_codes=[0])
    session = CompileSession.from_memory(tenant_id=tenant_id, repo_id=repo_id)
    plan = session.memory.plan_state.create_plan(
        tenant_id=tenant_id,
        repo_id=repo_id,
        goal="Goal",
        initial_steps=["step1"],
    )
    session.memory.plan_state.set_active_plan(tenant_id=tenant_id, repo_id=repo_id, plan_id=plan.id)

    res = session.run(goal="Goal", llm=llm, executor=ex, config=cfg, outputs_root=tmp_path)
    assert res.status == "succeeded"
    assert res.intent_satisfied is True


def test_quality_contract_gate_blocks_compile(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    tenant_id = "t1"
    repo_id = "r1"
    intent = _mk_intent_with_artifact_acceptance(
        tenant_id=tenant_id,
        repo_id=repo_id,
        expected_keyword="EXPECTED_KEYWORD_PRESENT",
    )
    intent = IntentSpecV1.from_json_obj(
        {
            **intent.to_json_obj(),
            "quality_contract": _quality_contract(engineering_stage="gate").to_json_obj(),
        }
    ).normalized()
    monkeypatch.setattr("akc.compile.controller.compile_intent_spec", lambda **_: intent)

    cfg = _mk_config(max_repairs_per_step=1)
    llm = _FixedLLM(patch_text=_patch_that_touches_tests(keyword="EXPECTED_KEYWORD_PRESENT"))
    ex = _ScriptedExecutor(exit_codes=[0, 0])
    session = CompileSession.from_memory(tenant_id=tenant_id, repo_id=repo_id)
    plan = session.memory.plan_state.create_plan(
        tenant_id=tenant_id,
        repo_id=repo_id,
        goal="Goal",
        initial_steps=["step1"],
    )
    session.memory.plan_state.set_active_plan(tenant_id=tenant_id, repo_id=repo_id, plan_id=plan.id)

    res = session.run(goal="Goal", llm=llm, executor=ex, config=cfg, outputs_root=tmp_path)
    assert res.compile_succeeded is True
    assert res.intent_satisfied is False
    manifest_path = tmp_path / tenant_id / repo_id / ".akc" / "run" / f"{res.plan.id}.manifest.json"
    manifest = RunManifest.from_json_file(manifest_path)
    by_name = {p.name: p for p in manifest.passes}
    assert by_name["intent_acceptance"].status == "failed"
