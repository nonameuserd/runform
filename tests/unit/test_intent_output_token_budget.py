from __future__ import annotations

from dataclasses import dataclass

from akc.compile import ControllerConfig, CostRates, TierConfig, run_compile_loop
from akc.compile.controller_config import Budget
from akc.compile.interfaces import (
    ExecutionRequest,
    ExecutionResult,
    LLMBackend,
    LLMRequest,
    LLMResponse,
    TenantRepoScope,
)
from akc.intent import (
    IntentSpecV1,
    Objective,
    OperatingBound,
    PolicyRef,
    SuccessCriterion,
)
from akc.memory.facade import build_memory


@dataclass(frozen=True)
class _FixedUsageLLM(LLMBackend):
    patch_text: str
    usage: dict[str, int]

    def complete(  # type: ignore[override]
        self,
        *,
        scope: TenantRepoScope,
        stage: str,
        request: LLMRequest,
    ) -> LLMResponse:
        _ = (scope, stage, request)
        return LLMResponse(text=self.patch_text, raw=None, usage=self.usage)


@dataclass(frozen=True)
class _AlwaysPassExecutor:
    def run(  # type: ignore[override]
        self,
        *,
        scope: TenantRepoScope,
        request: ExecutionRequest,
    ) -> ExecutionResult:
        _ = (scope, request)
        return ExecutionResult(exit_code=0, stdout="ok", stderr="", duration_ms=1)


def _mk_intent(
    *,
    tenant_id: str,
    repo_id: str,
    max_output_tokens: int | None,
    policies: tuple[PolicyRef, ...] = (),
    success_criteria: tuple[SuccessCriterion, ...] = (),
) -> IntentSpecV1:
    return IntentSpecV1(
        intent_id="intent_token_budget",
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
        policies=policies,
        success_criteria=success_criteria,
        operating_bounds=OperatingBound(
            max_seconds=None,
            max_steps=None,
            max_input_tokens=None,
            max_output_tokens=max_output_tokens,
            allow_network=False,
        ),
        assumptions=(),
        risk_notes=(),
        tags=(),
        metadata=None,
        created_at_ms=1,
        updated_at_ms=2,
    )


def _mk_cfg(*, max_iterations_total: int) -> ControllerConfig:
    tiers = {
        "small": TierConfig(name="small", llm_model="fake", temperature=0.0),
        "medium": TierConfig(name="medium", llm_model="fake", temperature=0.0),
    }
    return ControllerConfig(
        tiers=tiers,
        stage_tiers={"generate": "small", "repair": "small"},
        budget=Budget(
            max_llm_calls=100,
            max_repairs_per_step=10,
            max_iterations_total=max_iterations_total,
        ),
        test_mode="full",
        tool_allowlist=("llm.complete", "executor.run"),
        metadata={"execute_command": ["pytest", "-q"], "execute_timeout_s": 1.0},
        cost_rates=CostRates(),
    )


def _touching_patch_text(*, keyword: str) -> str:
    # Must touch at least one test file so tests-by-default promotion doesn't get vetoed.
    return "\n".join(
        [
            "--- a/src/fake_module.py",
            "+++ b/src/fake_module.py",
            "@@ -1 +1 @@",
            "+# stage_keyword=" + keyword,
            "--- a/tests/test_fake_module.py",
            "+++ b/tests/test_fake_module.py",
            "@@ -1 +1 @@",
            "+def test_fake_module():",
            "+    assert True",
        ]
    )


def test_intent_max_output_tokens_is_a_cumulative_budget() -> None:
    tenant_id = "t1"
    repo_id = "r1"

    mem = build_memory(backend="memory")
    plan = mem.plan_state.create_plan(
        tenant_id=tenant_id,
        repo_id=repo_id,
        goal="Goal",
        initial_steps=["step1"],
    )
    mem.plan_state.set_active_plan(tenant_id=tenant_id, repo_id=repo_id, plan_id=plan.id)

    # Acceptance always fails, so the controller iterates until it exhausts
    # the intent/budget envelopes.
    intent_fail_cap = _mk_intent(
        tenant_id=tenant_id,
        repo_id=repo_id,
        max_output_tokens=25,
        success_criteria=(
            SuccessCriterion(
                id="sc1",
                evaluation_mode="artifact_check",
                description="must not include keyword",
                params={"expected_keywords": ["NEVER_PRESENT"]},
            ),
        ),
    )
    intent_fail_big = _mk_intent(
        tenant_id=tenant_id,
        repo_id=repo_id,
        max_output_tokens=1000,
        success_criteria=intent_fail_cap.success_criteria,
    )

    llm = _FixedUsageLLM(
        patch_text=_touching_patch_text(keyword="nope"),
        usage={"input_tokens": 1, "output_tokens": 10},
    )
    ex = _AlwaysPassExecutor()
    cfg = _mk_cfg(max_iterations_total=6)

    res_cap = run_compile_loop(
        tenant_id=tenant_id,
        repo_id=repo_id,
        goal="Goal",
        plan_store=mem.plan_state,
        code_memory=mem.code_memory,
        why_graph=mem.why_graph,
        index=None,
        llm=llm,
        executor=ex,
        config=cfg,
        intent_spec=intent_fail_cap,
    )

    # Re-run with a much larger output token cap.
    mem2 = build_memory(backend="memory")
    plan2 = mem2.plan_state.create_plan(
        tenant_id=tenant_id,
        repo_id=repo_id,
        goal="Goal",
        initial_steps=["step1"],
    )
    mem2.plan_state.set_active_plan(tenant_id=tenant_id, repo_id=repo_id, plan_id=plan2.id)

    res_big = run_compile_loop(
        tenant_id=tenant_id,
        repo_id=repo_id,
        goal="Goal",
        plan_store=mem2.plan_state,
        code_memory=mem2.code_memory,
        why_graph=mem2.why_graph,
        index=None,
        llm=llm,
        executor=ex,
        config=cfg,
        intent_spec=intent_fail_big,
    )

    llm_calls_cap = int(res_cap.accounting.get("llm_calls", 0))
    llm_calls_big = int(res_big.accounting.get("llm_calls", 0))

    # With 10 output tokens per LLM call and a cap of 25, we should stop
    # substantially earlier than the controller's iteration budget (6).
    assert llm_calls_cap < llm_calls_big


def test_intent_policies_are_exposed_to_policy_engine() -> None:
    tenant_id = "t1"
    repo_id = "r1"

    mem = build_memory(backend="memory")
    plan = mem.plan_state.create_plan(
        tenant_id=tenant_id,
        repo_id=repo_id,
        goal="Goal",
        initial_steps=["step1"],
    )
    mem.plan_state.set_active_plan(tenant_id=tenant_id, repo_id=repo_id, plan_id=plan.id)

    intent = _mk_intent(
        tenant_id=tenant_id,
        repo_id=repo_id,
        max_output_tokens=None,
        policies=(PolicyRef(id="p1", source="cfg", requirement="net=false"),),
        success_criteria=(),
    )

    llm = _FixedUsageLLM(
        patch_text=_touching_patch_text(keyword="ok"),
        usage={"input_tokens": 1, "output_tokens": 1},
    )
    ex = _AlwaysPassExecutor()
    cfg = _mk_cfg(max_iterations_total=2)

    res = run_compile_loop(
        tenant_id=tenant_id,
        repo_id=repo_id,
        goal="Goal",
        plan_store=mem.plan_state,
        code_memory=mem.code_memory,
        why_graph=mem.why_graph,
        index=None,
        llm=llm,
        executor=ex,
        config=cfg,
        intent_spec=intent,
    )

    decisions = res.accounting.get("policy_decisions") or []
    llm_decs = [d for d in decisions if isinstance(d, dict) and d.get("action") == "llm.complete"]
    assert llm_decs
    constraints = llm_decs[0].get("constraints") or {}
    assert "intent_policies" in constraints
    policies = constraints.get("intent_policies") or []
    assert isinstance(policies, list) and policies
    assert isinstance(policies[0], dict) and policies[0].get("id") == "p1"
