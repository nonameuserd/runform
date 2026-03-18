from __future__ import annotations

from dataclasses import dataclass

from akc.compile import ControllerConfig, TierConfig, run_compile_loop
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
from akc.memory.facade import build_memory


@dataclass(frozen=True)
class _FakeLLM(LLMBackend):
    """Deterministic fake backend that echoes tier+stage."""

    def complete(  # type: ignore[override]
        self,
        *,
        scope: TenantRepoScope,
        stage: str,
        request: LLMRequest,
    ) -> LLMResponse:
        tier = None
        if request.metadata is not None:
            tier = request.metadata.get("tier")
        # Return a minimal, valid unified diff that touches both code + tests so
        # the controller's default "tests generated" heuristic and the verifier
        # patch-format checks can pass deterministically.
        text = "\n".join(
            [
                "--- a/src/fake_module.py",
                "+++ b/src/fake_module.py",
                "@@",
                f"+# stage={stage} tier={tier} tenant={scope.tenant_id} repo={scope.repo_id}",
                "",
                "--- a/tests/test_fake_module.py",
                "+++ b/tests/test_fake_module.py",
                "@@",
                "+def test_smoke():",
                "+    assert True",
                "",
            ]
        )
        return LLMResponse(text=text, raw=None, usage=None)


@dataclass
class _ScriptedLLM(LLMBackend):
    texts: list[str]
    calls: int = 0

    def complete(  # type: ignore[override]
        self,
        *,
        scope: TenantRepoScope,
        stage: str,
        request: LLMRequest,
    ) -> LLMResponse:
        _ = scope
        _ = stage
        _ = request
        txt = self.texts[min(self.calls, len(self.texts) - 1)]
        self.calls += 1
        return LLMResponse(text=txt, raw=None, usage=None)


@dataclass
class _ScriptedExecutor(Executor):
    """Executor that returns scripted exit codes sequentially."""

    exit_codes: list[int]
    calls: int = 0

    def run(  # type: ignore[override]
        self,
        *,
        scope: TenantRepoScope,
        request: ExecutionRequest,
    ) -> ExecutionResult:
        _ = scope
        _ = request
        code = self.exit_codes[min(self.calls, len(self.exit_codes) - 1)]
        self.calls += 1
        return ExecutionResult(
            exit_code=int(code),
            stdout=f"call={self.calls}",
            stderr="",
            duration_ms=1,
        )


def _mk_config(
    *,
    max_llm_calls: int = 10,
    max_repairs: int = 3,
    require_tests_for_non_test_changes: bool | None = None,
) -> ControllerConfig:
    tiers = {
        "small": TierConfig(name="small", llm_model="fake-small", temperature=0.0),
        "medium": TierConfig(name="medium", llm_model="fake-medium", temperature=0.0),
        "large": TierConfig(name="large", llm_model="fake-large", temperature=0.0),
    }
    kwargs: dict = {
        "tiers": tiers,
        "stage_tiers": {"generate": "small", "repair": "small"},
        "budget": Budget(
            max_llm_calls=max_llm_calls,
            max_repairs_per_step=max_repairs,
            max_iterations_total=max_repairs + 1,
        ),
        "test_mode": "full",
        "metadata": {"execute_command": ["pytest", "-q"], "execute_timeout_s": 1.0},
    }
    if require_tests_for_non_test_changes is not None:
        kwargs["require_tests_for_non_test_changes"] = require_tests_for_non_test_changes
    return ControllerConfig(**kwargs)


def test_controller_persists_best_candidate_and_marks_step_done_on_success() -> None:
    mem = build_memory(backend="memory")
    plan = mem.plan_state.create_plan(
        tenant_id="t1",
        repo_id="repo1",
        goal="Goal",
        initial_steps=["step1"],
    )
    mem.plan_state.set_active_plan(tenant_id="t1", repo_id="repo1", plan_id=plan.id)

    llm = _FakeLLM()
    ex = _ScriptedExecutor(exit_codes=[1, 0])
    cfg = _mk_config(max_llm_calls=10, max_repairs=3)

    res = run_compile_loop(
        tenant_id="t1",
        repo_id="repo1",
        goal="Goal",
        plan_store=mem.plan_state,
        code_memory=mem.code_memory,
        why_graph=mem.why_graph,
        index=None,
        llm=llm,
        executor=ex,
        config=cfg,
    )

    assert res.status == "succeeded"
    assert res.best_candidate is not None
    # Reload plan to ensure persistence.
    loaded = mem.plan_state.load_plan(tenant_id="t1", repo_id="repo1", plan_id=res.plan.id)
    assert loaded is not None
    step = next(s for s in loaded.steps if s.id == plan.steps[0].id)
    assert step.status == "done"
    out = dict(step.outputs or {})
    assert "best_candidate" in out
    assert out["best_candidate"]["execution"]["exit_code"] == 0
    assert out["best_candidate"]["execution"]["stage"] == "tests_full"
    assert out["best_candidate"]["execution"]["command"]
    # On success we persist patch + test_result artifacts into code memory and
    # record their ids on the step outputs.
    assert out.get("code_memory_item_ids") == [
        f"{plan.id}:{step.id}:patch",
        f"{plan.id}:{step.id}:test_result",
    ]
    items = mem.code_memory.list_items(
        tenant_id="t1",
        repo_id="repo1",
        artifact_id=plan.id,
        limit=10,
    )
    kinds = {i.kind for i in items}
    assert "patch" in kinds
    assert "test_result" in kinds


def test_controller_escalates_generation_tier_after_failures() -> None:
    mem = build_memory(backend="memory")
    plan = mem.plan_state.create_plan(
        tenant_id="t1",
        repo_id="repo1",
        goal="Goal",
        initial_steps=["step1"],
    )
    mem.plan_state.set_active_plan(tenant_id="t1", repo_id="repo1", plan_id=plan.id)

    llm = _FakeLLM()
    # Force 3 failed executes, then success (so we see escalation).
    ex = _ScriptedExecutor(exit_codes=[1, 1, 1, 0])
    cfg = _mk_config(max_llm_calls=10, max_repairs=5)

    res = run_compile_loop(
        tenant_id="t1",
        repo_id="repo1",
        goal="Goal",
        plan_store=mem.plan_state,
        code_memory=mem.code_memory,
        why_graph=mem.why_graph,
        index=None,
        llm=llm,
        executor=ex,
        config=cfg,
    )

    assert res.status == "succeeded"
    assert res.accounting["tier_history"]
    tiers = [
        e["tier"]
        for e in res.accounting["tier_history"]
        if e["stage"] in {"generate", "repair"}
    ]
    # Starts at small and should reach at least medium after failures (given all tiers present).
    assert "small" in tiers
    assert "medium" in tiers or "large" in tiers


def test_controller_halts_when_llm_call_budget_exhausted() -> None:
    mem = build_memory(backend="memory")
    plan = mem.plan_state.create_plan(
        tenant_id="t1",
        repo_id="repo1",
        goal="Goal",
        initial_steps=["step1"],
    )
    mem.plan_state.set_active_plan(tenant_id="t1", repo_id="repo1", plan_id=plan.id)

    llm = _FakeLLM()
    ex = _ScriptedExecutor(exit_codes=[1, 1, 1, 1, 1])
    cfg = _mk_config(max_llm_calls=1, max_repairs=10)

    res = run_compile_loop(
        tenant_id="t1",
        repo_id="repo1",
        goal="Goal",
        plan_store=mem.plan_state,
        code_memory=mem.code_memory,
        why_graph=mem.why_graph,
        index=None,
        llm=llm,
        executor=ex,
        config=cfg,
    )

    assert res.status in {"failed", "budget_exhausted"}
    assert res.accounting["llm_calls"] == 1


def test_controller_halts_when_max_repairs_per_step_is_zero() -> None:
    mem = build_memory(backend="memory")
    plan = mem.plan_state.create_plan(
        tenant_id="t1",
        repo_id="repo1",
        goal="Goal",
        initial_steps=["step1"],
    )
    mem.plan_state.set_active_plan(tenant_id="t1", repo_id="repo1", plan_id=plan.id)

    llm = _FakeLLM()
    ex = _ScriptedExecutor(exit_codes=[1, 1, 1])
    cfg = _mk_config(max_llm_calls=10, max_repairs=0)

    res = run_compile_loop(
        tenant_id="t1",
        repo_id="repo1",
        goal="Goal",
        plan_store=mem.plan_state,
        code_memory=mem.code_memory,
        why_graph=mem.why_graph,
        index=None,
        llm=llm,
        executor=ex,
        config=cfg,
    )

    assert res.status in {"failed", "budget_exhausted"}
    # One generate attempt is made, but no repair iteration should run.
    assert res.accounting["llm_calls"] == 1
    assert res.accounting["repair_iterations"] == 0


def test_controller_escalation_stops_at_largest_tier() -> None:
    mem = build_memory(backend="memory")
    plan = mem.plan_state.create_plan(
        tenant_id="t1",
        repo_id="repo1",
        goal="Goal",
        initial_steps=["step1"],
    )
    mem.plan_state.set_active_plan(tenant_id="t1", repo_id="repo1", plan_id=plan.id)

    llm = _FakeLLM()
    # Many failures then success; tier should reach large and stay there.
    ex = _ScriptedExecutor(exit_codes=[1, 1, 1, 1, 1, 0])
    cfg = _mk_config(max_llm_calls=20, max_repairs=10)

    res = run_compile_loop(
        tenant_id="t1",
        repo_id="repo1",
        goal="Goal",
        plan_store=mem.plan_state,
        code_memory=mem.code_memory,
        why_graph=mem.why_graph,
        index=None,
        llm=llm,
        executor=ex,
        config=cfg,
    )

    assert res.status == "succeeded"
    tiers = [
        e["tier"]
        for e in res.accounting["tier_history"]
        if e["stage"] in {"generate", "repair"}
    ]
    assert "large" in tiers
    assert tiers[-1] == "large"


def test_controller_smoke_then_full_gate_promotes_only_if_full_passes() -> None:
    mem = build_memory(backend="memory")
    plan = mem.plan_state.create_plan(
        tenant_id="t1",
        repo_id="repo1",
        goal="Goal",
        initial_steps=["step1"],
    )
    mem.plan_state.set_active_plan(tenant_id="t1", repo_id="repo1", plan_id=plan.id)

    llm = _FakeLLM()
    # First iteration:
    # - smoke passes
    # - full fails (forces repair)
    # Second iteration:
    # - smoke passes
    # - full passes (promotion)
    ex = _ScriptedExecutor(exit_codes=[0, 1, 0, 0])

    cfg = _mk_config(max_llm_calls=10, max_repairs=3)
    cfg = ControllerConfig(
        tiers=cfg.tiers,
        stage_tiers=cfg.stage_tiers,
        budget=cfg.budget,
        test_mode="smoke",
        metadata={
            "execute_command": ["python", "-c", "print('smoke')"],
            "full_test_command": ["python", "-c", "print('full')"],
            "execute_timeout_s": 1.0,
            "full_test_timeout_s": 1.0,
        },
    )

    res = run_compile_loop(
        tenant_id="t1",
        repo_id="repo1",
        goal="Goal",
        plan_store=mem.plan_state,
        code_memory=mem.code_memory,
        why_graph=mem.why_graph,
        index=None,
        llm=llm,
        executor=ex,
        config=cfg,
    )

    assert res.status == "succeeded"
    loaded = mem.plan_state.load_plan(tenant_id="t1", repo_id="repo1", plan_id=res.plan.id)
    assert loaded is not None
    step = next(s for s in loaded.steps if s.id == plan.steps[0].id)
    out = dict(step.outputs or {})
    assert out["best_candidate"]["execution"]["stage"] == "tests_full"
    assert out["last_tests_smoke"]["stage"] == "tests_smoke"
    assert out["last_tests_full"]["stage"] == "tests_full"


def test_controller_smoke_full_runs_every_n_iterations_and_on_budget_boundary() -> None:
    mem = build_memory(backend="memory")
    plan = mem.plan_state.create_plan(
        tenant_id="t1",
        repo_id="repo1",
        goal="Goal",
        initial_steps=["step1"],
    )
    mem.plan_state.set_active_plan(tenant_id="t1", repo_id="repo1", plan_id=plan.id)

    llm = _FakeLLM()
    # Iteration 1: smoke pass, full skipped (n=2)
    # Iteration 2: smoke pass, full pass -> promotion
    ex = _ScriptedExecutor(exit_codes=[0, 0, 0])

    base = _mk_config(max_llm_calls=10, max_repairs=3)
    cfg = ControllerConfig(
        tiers=base.tiers,
        stage_tiers=base.stage_tiers,
        budget=base.budget,
        test_mode="smoke",
        full_test_every_n_iterations=2,
        metadata={
            "execute_command": ["python", "-c", "print('smoke')"],
            "full_test_command": ["python", "-c", "print('full')"],
            "execute_timeout_s": 1.0,
            "full_test_timeout_s": 1.0,
        },
    )

    res = run_compile_loop(
        tenant_id="t1",
        repo_id="repo1",
        goal="Goal",
        plan_store=mem.plan_state,
        code_memory=mem.code_memory,
        why_graph=mem.why_graph,
        index=None,
        llm=llm,
        executor=ex,
        config=cfg,
    )

    assert res.status == "succeeded"
    assert ex.calls == 3  # smoke, then smoke+full


def test_verifier_gate_vetoes_promotion_and_triggers_repair() -> None:
    mem = build_memory(backend="memory")
    plan = mem.plan_state.create_plan(
        tenant_id="t1",
        repo_id="repo1",
        goal="Goal",
        initial_steps=["step1"],
    )
    mem.plan_state.set_active_plan(tenant_id="t1", repo_id="repo1", plan_id=plan.id)

    bad_patch = "\n".join(
        [
            "--- a/../evil.py",
            "+++ b/../evil.py",
            "@@",
            "+print('nope')",
            "",
        ]
    )
    good_patch = "\n".join(
        [
            "--- a/src/good.py",
            "+++ b/src/good.py",
            "@@",
            "+print('ok')",
            "",
        ]
    )
    llm = _ScriptedLLM(texts=[bad_patch, good_patch])
    ex = _ScriptedExecutor(exit_codes=[0, 0])
    # Disable policy so the verifier gate (not the tests-generated policy) vetoes the
    # first candidate; good_patch has no test path and would otherwise be policy-vetoed.
    cfg = _mk_config(max_llm_calls=10, max_repairs=2, require_tests_for_non_test_changes=False)

    res = run_compile_loop(
        tenant_id="t1",
        repo_id="repo1",
        goal="Goal",
        plan_store=mem.plan_state,
        code_memory=mem.code_memory,
        why_graph=mem.why_graph,
        index=None,
        llm=llm,
        executor=ex,
        config=cfg,
    )

    assert res.status == "succeeded"
    assert res.accounting["repair_iterations"] == 1
    loaded = mem.plan_state.load_plan(tenant_id="t1", repo_id="repo1", plan_id=res.plan.id)
    assert loaded is not None
    step = next(s for s in loaded.steps if s.id == plan.steps[0].id)
    out = dict(step.outputs or {})
    assert isinstance(out.get("last_verification"), dict)
    assert out["last_verification"]["passed"] is True


def test_tests_generated_policy_vetoes_promotion_when_non_test_paths_change_without_tests() -> None:
    mem = build_memory(backend="memory")
    plan = mem.plan_state.create_plan(
        tenant_id="t1",
        repo_id="repo1",
        goal="Goal",
        initial_steps=["step1"],
    )
    mem.plan_state.set_active_plan(tenant_id="t1", repo_id="repo1", plan_id=plan.id)

    # First candidate: touches src/ only (no tests) but tests pass -> should be vetoed by policy.
    no_tests_patch = "\n".join(
        [
            "--- a/src/only_code.py",
            "+++ b/src/only_code.py",
            "@@",
            "+print('x')",
            "",
        ]
    )
    # Second candidate: includes a test path -> promotable.
    with_tests_patch = "\n".join(
        [
            "--- a/src/only_code.py",
            "+++ b/src/only_code.py",
            "@@",
            "+print('y')",
            "",
            "--- a/tests/test_only_code.py",
            "+++ b/tests/test_only_code.py",
            "@@",
            "+def test_ok():",
            "+    assert True",
            "",
        ]
    )
    llm = _ScriptedLLM(texts=[no_tests_patch, with_tests_patch])
    ex = _ScriptedExecutor(exit_codes=[0, 0])
    cfg = _mk_config(max_llm_calls=10, max_repairs=2)

    res = run_compile_loop(
        tenant_id="t1",
        repo_id="repo1",
        goal="Goal",
        plan_store=mem.plan_state,
        code_memory=mem.code_memory,
        why_graph=mem.why_graph,
        index=None,
        llm=llm,
        executor=ex,
        config=cfg,
    )

    assert res.status == "succeeded"
    assert res.accounting["repair_iterations"] == 1
    loaded = mem.plan_state.load_plan(tenant_id="t1", repo_id="repo1", plan_id=res.plan.id)
    assert loaded is not None
    step = next(s for s in loaded.steps if s.id == plan.steps[0].id)
    out = dict(step.outputs or {})
    # Policy failure should be recorded.
    assert isinstance(out.get("last_policy_failure"), dict)
    assert out["last_policy_failure"]["code"] == "policy.missing_tests"
