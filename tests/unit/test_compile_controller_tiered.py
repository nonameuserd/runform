from __future__ import annotations

from dataclasses import dataclass, replace

import pytest

from akc.compile import ControllerConfig, CostRates, TierConfig, run_compile_loop
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
from akc.compile.rust_bridge import RustExecConfig
from akc.memory.facade import build_memory
from akc.run import PassRecord, RunManifest


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


@dataclass
class _ScriptedWasmExecutor(_ScriptedExecutor):
    rust_cfg: RustExecConfig = RustExecConfig(
        mode="pyo3",
        lane="wasm",
        allow_network=True,
        memory_bytes=123,
        cpu_fuel=456,
        stdout_max_bytes=789,
        stderr_max_bytes=321,
        preopen_dirs=("/safe/workspace",),
        allowed_write_paths=("/safe/workspace",),
        wasm_normalization_strict=True,
    )


@dataclass
class _ScriptedDockerExecutor(_ScriptedExecutor):
    disable_network: bool = True
    memory_bytes: int | None = 2048
    pids_limit: int | None = 64
    cpus: float | None = 1.5
    read_only_rootfs: bool = True
    no_new_privileges: bool = True
    cap_drop_all: bool = True
    user: str | None = "65532:65532"
    tmpfs_mounts: tuple[str, ...] = ("/tmp", "/run")
    seccomp_profile: str | None = "runtime/default"
    apparmor_profile: str | None = "akc-default"
    ulimit_nofile: str | None = "1024:2048"
    ulimit_nproc: str | None = "256"


@dataclass
class _NoCallLLM(LLMBackend):
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
        self.calls += 1
        raise AssertionError("LLM must not be called in full_replay mode")


@dataclass
class _NoCallExecutor(Executor):
    calls: int = 0

    def run(  # type: ignore[override]
        self,
        *,
        scope: TenantRepoScope,
        request: ExecutionRequest,
    ) -> ExecutionResult:
        _ = scope
        _ = request
        self.calls += 1
        raise AssertionError("Executor must not be called in full_replay mode")


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
        "tool_allowlist": ("llm.complete", "executor.run"),
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
        e["tier"] for e in res.accounting["tier_history"] if e["stage"] in {"generate", "repair"}
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
        e["tier"] for e in res.accounting["tier_history"] if e["stage"] in {"generate", "repair"}
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
        policy_mode=cfg.policy_mode,
        tool_allowlist=cfg.tool_allowlist,
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
        policy_mode=base.policy_mode,
        tool_allowlist=base.tool_allowlist,
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


def test_verifier_gate_vetoes_promotion_and_triggers_repair_under_strict_monotonicity() -> None:
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
    assert ex.calls == 2
    loaded = mem.plan_state.load_plan(tenant_id="t1", repo_id="repo1", plan_id=res.plan.id)
    assert loaded is not None
    step = next(s for s in loaded.steps if s.id == plan.steps[0].id)
    out = dict(step.outputs or {})
    assert isinstance(out.get("last_verification"), dict)
    assert out["last_verification"]["passed"] is True
    assert out.get("last_monotonic_failure") is None


def test_verifier_gate_can_be_disabled_and_is_well_specified_in_outputs() -> None:
    mem = build_memory(backend="memory")
    plan = mem.plan_state.create_plan(
        tenant_id="t1",
        repo_id="repo1",
        goal="Goal",
        initial_steps=["step1"],
    )
    mem.plan_state.set_active_plan(tenant_id="t1", repo_id="repo1", plan_id=plan.id)

    # This patch would be rejected by the deterministic verifier (suspicious path),
    # but when verifier is disabled it must not veto promotion.
    suspicious_patch = "\n".join(
        [
            "--- a/../evil.py",
            "+++ b/../evil.py",
            "@@",
            "+print('nope')",
            "",
        ]
    )
    llm = _ScriptedLLM(texts=[suspicious_patch])
    ex = _ScriptedExecutor(exit_codes=[0])
    cfg = _mk_config(max_llm_calls=5, max_repairs=0, require_tests_for_non_test_changes=False)
    cfg = ControllerConfig(
        tiers=cfg.tiers,
        stage_tiers=cfg.stage_tiers,
        budget=cfg.budget,
        test_mode=cfg.test_mode,
        policy_mode=cfg.policy_mode,
        tool_allowlist=cfg.tool_allowlist,
        metadata=cfg.metadata,
        verifier_enabled=False,
        verifier_strict=True,
        require_tests_for_non_test_changes=False,
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
    v = out.get("last_verification")
    assert isinstance(v, dict)
    assert v.get("policy", {}).get("enabled") is False
    assert v.get("passed") is True


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
    assert out.get("last_monotonic_failure") is None
    assert out.get("last_verification", {}).get("passed") is True


def test_repair_requires_strict_monotonic_improvement() -> None:
    mem = build_memory(backend="memory")
    plan = mem.plan_state.create_plan(
        tenant_id="t1",
        repo_id="repo1",
        goal="Goal",
        initial_steps=["step1"],
    )
    mem.plan_state.set_active_plan(tenant_id="t1", repo_id="repo1", plan_id=plan.id)

    # First candidate is verifier-vetoed but has a passing execution score (1000).
    bad_patch = "\n".join(
        [
            "--- a/../evil.py",
            "+++ b/../evil.py",
            "@@",
            "+print('nope')",
            "",
        ]
    )
    # Second candidate is a clean repair with the same passing execution score.
    # Strict monotonicity should reject equal-score repair attempts.
    equal_score_patch = "\n".join(
        [
            "--- a/src/equal_score.py",
            "+++ b/src/equal_score.py",
            "@@",
            "+print('ok')",
            "",
            "--- a/tests/test_equal_score.py",
            "+++ b/tests/test_equal_score.py",
            "@@",
            "+def test_equal_score():",
            "+    assert True",
            "",
        ]
    )
    llm = _ScriptedLLM(texts=[bad_patch, equal_score_patch])
    ex = _ScriptedExecutor(exit_codes=[0, 0])
    cfg = _mk_config(max_llm_calls=10, max_repairs=1, require_tests_for_non_test_changes=False)

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
    assert out.get("last_monotonic_failure") is None
    assert out.get("last_verification", {}).get("passed") is True


def test_full_replay_mode_uses_cached_candidate_and_cached_execution() -> None:
    mem = build_memory(backend="memory")
    plan = mem.plan_state.create_plan(
        tenant_id="t1",
        repo_id="repo1",
        goal="Goal",
        initial_steps=["step1"],
    )
    mem.plan_state.set_active_plan(tenant_id="t1", repo_id="repo1", plan_id=plan.id)

    cached_patch = "\n".join(
        [
            "--- a/src/replayed.py",
            "+++ b/src/replayed.py",
            "@@",
            "+VALUE = 1",
            "",
            "--- a/tests/test_replayed.py",
            "+++ b/tests/test_replayed.py",
            "@@",
            "+def test_replayed():",
            "+    assert VALUE == 1",
            "",
        ]
    )
    cached_outputs = {
        "best_candidate": {
            "llm_text": cached_patch,
            "touched_paths": ["src/replayed.py", "tests/test_replayed.py"],
        },
        "last_tests_full": {
            "stage": "tests_full",
            "command": ["pytest", "-q"],
            "exit_code": 0,
            "stdout": "cached pass",
            "stderr": "",
            "duration_ms": 1,
        },
    }
    s = plan.steps[0]
    seeded_step = replace(s, outputs=cached_outputs)
    seeded_plan = replace(plan, steps=(seeded_step,))
    mem.plan_state.save_plan(tenant_id="t1", repo_id="repo1", plan=seeded_plan)

    llm = _NoCallLLM()
    ex = _NoCallExecutor()
    cfg = _mk_config(max_llm_calls=5, max_repairs=0)
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
        replay_mode="full_replay",
    )

    assert res.status == "succeeded"
    assert llm.calls == 0
    assert ex.calls == 0


def test_full_replay_mode_can_use_manifest_payloads_without_step_cache() -> None:
    mem = build_memory(backend="memory")
    plan = mem.plan_state.create_plan(
        tenant_id="t1",
        repo_id="repo1",
        goal="Goal",
        initial_steps=["step1"],
    )
    mem.plan_state.set_active_plan(tenant_id="t1", repo_id="repo1", plan_id=plan.id)

    replay_patch = "\n".join(
        [
            "--- a/src/from_manifest.py",
            "+++ b/src/from_manifest.py",
            "@@",
            "+VALUE = 2",
            "",
            "--- a/tests/test_from_manifest.py",
            "+++ b/tests/test_from_manifest.py",
            "@@",
            "+def test_from_manifest():",
            "+    assert True",
            "",
        ]
    )
    replay_manifest = RunManifest(
        run_id="previous-run",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256="a" * 64,
        replay_mode="full_replay",
        passes=(
            PassRecord(name="generate", status="succeeded", metadata={"llm_text": replay_patch}),
            PassRecord(
                name="execute",
                status="succeeded",
                metadata={
                    "stage": "tests_full",
                    "command": ["pytest", "-q"],
                    "exit_code": 0,
                    "stdout": "manifest replay pass",
                    "stderr": "",
                    "duration_ms": 1,
                },
            ),
        ),
    )

    llm = _NoCallLLM()
    ex = _NoCallExecutor()
    cfg = _mk_config(max_llm_calls=5, max_repairs=0)
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
        replay_mode="full_replay",
        replay_manifest=replay_manifest,
    )

    assert res.status == "succeeded"
    assert llm.calls == 0
    assert ex.calls == 0


def test_llm_vcr_mode_uses_prompt_keyed_cache_without_llm_calls() -> None:
    mem = build_memory(backend="memory")
    plan = mem.plan_state.create_plan(
        tenant_id="t1",
        repo_id="repo1",
        goal="Goal",
        initial_steps=["step1"],
    )
    mem.plan_state.set_active_plan(tenant_id="t1", repo_id="repo1", plan_id=plan.id)

    replay_patch = "\n".join(
        [
            "--- a/src/from_vcr.py",
            "+++ b/src/from_vcr.py",
            "@@",
            "+VALUE = 7",
            "",
            "--- a/tests/test_from_vcr.py",
            "+++ b/tests/test_from_vcr.py",
            "@@",
            "+def test_from_vcr():",
            "+    assert True",
            "",
        ]
    )

    # Use a baseline pass payload fallback so replay stays deterministic
    # without requiring reconstruction of exact prompt keys in test setup.
    replay_manifest = RunManifest(
        run_id="vcr-run",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256="e" * 64,
        replay_mode="llm_vcr",
        passes=(
            PassRecord(name="generate", status="succeeded", metadata={"llm_text": replay_patch}),
        ),
    )
    llm = _NoCallLLM()
    ex = _ScriptedExecutor(exit_codes=[0])
    cfg = _mk_config(max_llm_calls=5, max_repairs=0)

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
        replay_mode="llm_vcr",
        replay_manifest=replay_manifest,
    )

    assert res.status == "succeeded"
    assert llm.calls == 0
    assert ex.calls == 1


def test_controller_halts_when_tool_call_budget_exhausted() -> None:
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
    cfg = _mk_config(max_llm_calls=10, max_repairs=3)
    cfg = ControllerConfig(
        tiers=cfg.tiers,
        stage_tiers=cfg.stage_tiers,
        budget=Budget(
            max_llm_calls=10,
            max_repairs_per_step=3,
            max_iterations_total=4,
            max_tool_calls=1,
        ),
        test_mode=cfg.test_mode,
        policy_mode=cfg.policy_mode,
        tool_allowlist=cfg.tool_allowlist,
        metadata=cfg.metadata,
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

    assert res.status in {"failed", "budget_exhausted"}
    assert int(res.accounting.get("tool_calls", 0)) == 1


def test_controller_halts_when_cost_budget_exhausted() -> None:
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
    cfg = _mk_config(max_llm_calls=10, max_repairs=3)
    cfg = ControllerConfig(
        tiers=cfg.tiers,
        stage_tiers=cfg.stage_tiers,
        budget=Budget(
            max_llm_calls=10,
            max_repairs_per_step=3,
            max_iterations_total=4,
            max_cost_usd=0.0001,
        ),
        test_mode=cfg.test_mode,
        policy_mode=cfg.policy_mode,
        tool_allowlist=cfg.tool_allowlist,
        cost_rates=CostRates(
            input_per_1k_tokens_usd=0.01,
            output_per_1k_tokens_usd=0.01,
            tool_call_usd=0.001,
        ),
        metadata=dict(cfg.metadata or {}),
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

    assert res.status in {"failed", "budget_exhausted"}
    assert float(res.accounting.get("estimated_cost_usd", 0.0)) > 0.0001


def test_policy_enforce_blocks_default_deny_without_allowlist() -> None:
    mem = build_memory(backend="memory")
    plan = mem.plan_state.create_plan(
        tenant_id="t1",
        repo_id="repo1",
        goal="Goal",
        initial_steps=["step1"],
    )
    mem.plan_state.set_active_plan(tenant_id="t1", repo_id="repo1", plan_id=plan.id)

    llm = _FakeLLM()
    ex = _ScriptedExecutor(exit_codes=[0])
    cfg = _mk_config(max_llm_calls=3, max_repairs=0)
    cfg = ControllerConfig(
        tiers=cfg.tiers,
        stage_tiers=cfg.stage_tiers,
        budget=cfg.budget,
        test_mode=cfg.test_mode,
        policy_mode="enforce",
        tool_allowlist=(),
        metadata=cfg.metadata,
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
    assert res.status in {"failed", "budget_exhausted"}
    assert int(res.accounting.get("llm_calls", 0)) == 0
    assert int(res.accounting.get("tool_calls", 0)) == 0
    decisions = list(res.accounting.get("policy_decisions", []))
    assert decisions
    assert decisions[0]["allowed"] is False
    assert decisions[0]["block"] is True


def test_policy_audit_only_records_default_deny_but_allows_progress() -> None:
    mem = build_memory(backend="memory")
    plan = mem.plan_state.create_plan(
        tenant_id="t1",
        repo_id="repo1",
        goal="Goal",
        initial_steps=["step1"],
    )
    mem.plan_state.set_active_plan(tenant_id="t1", repo_id="repo1", plan_id=plan.id)

    llm = _FakeLLM()
    ex = _ScriptedExecutor(exit_codes=[0])
    cfg = _mk_config(max_llm_calls=3, max_repairs=0)
    cfg = ControllerConfig(
        tiers=cfg.tiers,
        stage_tiers=cfg.stage_tiers,
        budget=cfg.budget,
        test_mode=cfg.test_mode,
        policy_mode="audit_only",
        tool_allowlist=(),
        metadata=cfg.metadata,
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
    decisions = list(res.accounting.get("policy_decisions", []))
    assert decisions
    assert any(d["allowed"] is False and d["block"] is False for d in decisions)


def test_policy_executor_attentuates_capabilities_with_stage_constraints() -> None:
    mem = build_memory(backend="memory")
    plan = mem.plan_state.create_plan(
        tenant_id="t1",
        repo_id="repo1",
        goal="Goal",
        initial_steps=["step1"],
    )
    mem.plan_state.set_active_plan(tenant_id="t1", repo_id="repo1", plan_id=plan.id)

    llm = _FakeLLM()
    ex = _ScriptedExecutor(exit_codes=[0])
    cfg = _mk_config(max_llm_calls=2, max_repairs=0)

    # Custom policy engine to validate capability token constraints passed from the
    # controller via capability attenuation + policy-wrapped executor.
    from dataclasses import dataclass

    from akc.control.policy import CapabilityIssuer, PolicyDecision, ToolAuthorizationRequest

    @dataclass
    class _RecordingPolicyEngine:
        issuer: CapabilityIssuer
        executor_constraints: list[dict[str, object]]

        def authorize(self, *, req: ToolAuthorizationRequest) -> PolicyDecision:
            if req.action == "executor.run":
                constraints = dict(req.capability.constraints or {})
                self.executor_constraints.append(constraints)
            return PolicyDecision(
                allowed=True,
                reason="ok",
                mode="enforce",
                source="test",
                block=False,
            )

    engine = _RecordingPolicyEngine(
        issuer=CapabilityIssuer(),
        executor_constraints=[],
    )

    expected_step_id = plan.steps[0].id
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
        policy_engine=engine,
    )

    assert res.status == "succeeded"
    assert engine.executor_constraints, "expected executor.run policy calls"

    # Controller issues a base executor capability, then attenuates it with stage
    # constraints for each execute gate.
    for constraints in engine.executor_constraints:
        assert constraints.get("plan_id") == plan.id
        assert constraints.get("step_id") == expected_step_id
        assert constraints.get("stage") == "tests_full"


def test_policy_executor_includes_wasm_controls_in_policy_context_and_constraints() -> None:
    mem = build_memory(backend="memory")
    plan = mem.plan_state.create_plan(
        tenant_id="t1",
        repo_id="repo1",
        goal="Goal",
        initial_steps=["step1"],
    )
    mem.plan_state.set_active_plan(tenant_id="t1", repo_id="repo1", plan_id=plan.id)

    llm = _FakeLLM()
    ex = _ScriptedWasmExecutor(exit_codes=[0])
    cfg = replace(
        _mk_config(max_llm_calls=2, max_repairs=0),
        metadata={
            "execute_command": ["pytest", "-q"],
            "execute_timeout_s": 1.0,
            "wasm_network_exception": "ticket-123",
        },
    )

    from dataclasses import dataclass

    from akc.control.policy import CapabilityIssuer, PolicyDecision, ToolAuthorizationRequest

    @dataclass
    class _RecordingPolicyEngine:
        issuer: CapabilityIssuer
        executor_contexts: list[dict[str, object]]
        executor_constraints: list[dict[str, object]]

        def authorize(self, *, req: ToolAuthorizationRequest) -> PolicyDecision:
            if req.action == "executor.run":
                self.executor_contexts.append(dict(req.context or {}))
                self.executor_constraints.append(dict(req.capability.constraints or {}))
            return PolicyDecision(
                allowed=True,
                reason="ok",
                mode="enforce",
                source="test",
                block=False,
            )

    engine = _RecordingPolicyEngine(
        issuer=CapabilityIssuer(),
        executor_contexts=[],
        executor_constraints=[],
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
        policy_engine=engine,
    )

    assert res.status == "succeeded"
    assert engine.executor_contexts
    ctx = engine.executor_contexts[0]
    wasm = dict(ctx["wasm"])  # type: ignore[arg-type]
    profile = dict(wasm["platform_capability_profile"])  # type: ignore[arg-type]
    assert ctx["backend"] == "wasm"
    assert wasm["network_enabled"] is True
    assert wasm["network_exception"] == "ticket-123"
    assert wasm["preopen_dirs"] == ["/safe/workspace"]
    assert wasm["writable_preopen_dirs"] == ["/safe/workspace"]
    assert wasm["read_only_preopen_dirs"] == []
    assert wasm["limits_tuple"] == [1000, 123, 456, 789, 321]
    assert profile["profile"] == "strict"
    assert "wall_time_ms" in profile["required_controls"]  # type: ignore[operator]

    constraints = engine.executor_constraints[0]
    assert constraints["backend"] == "wasm"
    assert constraints["stage"] == "tests_full"
    assert constraints["wasm"] == ctx["wasm"]


def test_policy_executor_marks_enforce_wasm_profile_strict_even_with_relaxed_normalization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mem = build_memory(backend="memory")
    plan = mem.plan_state.create_plan(
        tenant_id="t1",
        repo_id="repo1",
        goal="Goal",
        initial_steps=["step1"],
    )
    mem.plan_state.set_active_plan(tenant_id="t1", repo_id="repo1", plan_id=plan.id)

    llm = _FakeLLM()
    ex = _ScriptedWasmExecutor(
        exit_codes=[0],
        rust_cfg=RustExecConfig(
            mode="pyo3",
            lane="wasm",
            allow_network=False,
            memory_bytes=123,
            cpu_fuel=None,
            stdout_max_bytes=789,
            stderr_max_bytes=321,
            preopen_dirs=("/safe/workspace", "/safe/cache"),
            allowed_write_paths=("/safe/workspace",),
            wasm_normalization_strict=False,
        ),
    )
    cfg = replace(
        _mk_config(max_llm_calls=2, max_repairs=0),
        policy_mode="enforce",
        metadata={
            "execute_command": ["pytest", "-q"],
            "execute_timeout_s": 1.0,
        },
    )

    from dataclasses import dataclass

    from akc.control.policy import CapabilityIssuer, PolicyDecision, ToolAuthorizationRequest

    @dataclass
    class _RecordingPolicyEngine:
        issuer: CapabilityIssuer
        executor_contexts: list[dict[str, object]]

        def authorize(self, *, req: ToolAuthorizationRequest) -> PolicyDecision:
            if req.action == "executor.run":
                self.executor_contexts.append(dict(req.context or {}))
            return PolicyDecision(
                allowed=True,
                reason="ok",
                mode="enforce",
                source="test",
                block=False,
            )

    engine = _RecordingPolicyEngine(
        issuer=CapabilityIssuer(),
        executor_contexts=[],
    )
    monkeypatch.setattr("akc.compile.controller.sys.platform", "win32")

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
        policy_engine=engine,
    )

    assert res.status == "succeeded"
    wasm = dict(engine.executor_contexts[0]["wasm"])  # type: ignore[arg-type]
    profile = dict(wasm["platform_capability_profile"])  # type: ignore[arg-type]
    assert profile["profile"] == "strict"
    assert profile["unsupported_controls"] == ["wall_time_ms"]
    assert wasm["writable_preopen_dirs"] == ["/safe/workspace"]
    assert wasm["read_only_preopen_dirs"] == ["/safe/cache"]


def test_policy_executor_includes_docker_controls_in_policy_context_and_constraints() -> None:
    mem = build_memory(backend="memory")
    plan = mem.plan_state.create_plan(
        tenant_id="t1",
        repo_id="repo1",
        goal="Goal",
        initial_steps=["step1"],
    )
    mem.plan_state.set_active_plan(tenant_id="t1", repo_id="repo1", plan_id=plan.id)

    llm = _FakeLLM()
    ex = _ScriptedDockerExecutor(exit_codes=[0])
    cfg = replace(
        _mk_config(max_llm_calls=2, max_repairs=0),
        metadata={
            "execute_command": ["pytest", "-q"],
            "execute_timeout_s": 1.0,
            "docker_network_exception": "tenant-approved-ticket",
        },
    )

    from dataclasses import dataclass

    from akc.control.policy import CapabilityIssuer, PolicyDecision, ToolAuthorizationRequest

    @dataclass
    class _RecordingPolicyEngine:
        issuer: CapabilityIssuer
        executor_contexts: list[dict[str, object]]
        executor_constraints: list[dict[str, object]]

        def authorize(self, *, req: ToolAuthorizationRequest) -> PolicyDecision:
            if req.action == "executor.run":
                self.executor_contexts.append(dict(req.context or {}))
                self.executor_constraints.append(dict(req.capability.constraints or {}))
            return PolicyDecision(
                allowed=True,
                reason="ok",
                mode="enforce",
                source="test",
                block=False,
            )

    engine = _RecordingPolicyEngine(
        issuer=CapabilityIssuer(),
        executor_contexts=[],
        executor_constraints=[],
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
        policy_engine=engine,
    )

    assert res.status == "succeeded"
    assert engine.executor_contexts
    ctx = engine.executor_contexts[0]
    docker = dict(ctx["docker"])  # type: ignore[arg-type]
    limits = dict(docker["limits"])  # type: ignore[arg-type]
    profiles = dict(docker["security_profiles"])  # type: ignore[arg-type]
    assert ctx["backend"] == "docker"
    assert docker["network_enabled"] is False
    assert docker["network_mode"] == "none"
    assert docker["network_exception"] == "tenant-approved-ticket"
    assert docker["read_only_rootfs"] is True
    assert docker["no_new_privileges"] is True
    assert docker["cap_drop_all"] is True
    assert docker["user_present"] is True
    assert docker["user_is_non_root"] is True
    assert profiles["seccomp"] == "runtime/default"
    assert profiles["apparmor"] == "akc-default"
    platform = dict(docker["platform"])  # type: ignore[arg-type]
    assert platform["os"] in {"linux", "darwin", "windows"}
    assert isinstance(platform["apparmor_available"], bool)
    assert limits["memory_bytes"] == 2048
    assert limits["pids_limit"] == 64
    assert limits["cpus"] == 1.5
    assert limits["ulimit_nofile"] == "1024:2048"
    assert limits["ulimit_nproc"] == "256"
    assert docker["tmpfs_mounts"] == ["/tmp", "/run"]

    constraints = engine.executor_constraints[0]
    assert constraints["backend"] == "docker"
    assert constraints["stage"] == "tests_full"
    assert constraints["docker"] == ctx["docker"]


def test_policy_denied_executor_run_is_persisted_with_scope_and_context() -> None:
    mem = build_memory(backend="memory")
    plan = mem.plan_state.create_plan(
        tenant_id="tenant-a",
        repo_id="repo-prod",
        goal="Goal",
        initial_steps=["step1"],
    )
    mem.plan_state.set_active_plan(
        tenant_id="tenant-a",
        repo_id="repo-prod",
        plan_id=plan.id,
    )

    llm = _FakeLLM()
    ex = _ScriptedDockerExecutor(exit_codes=[0], disable_network=False, user="0:0")
    cfg = _mk_config(max_llm_calls=2, max_repairs=0)

    from dataclasses import dataclass

    from akc.control.policy import CapabilityIssuer, PolicyDecision, ToolAuthorizationRequest

    @dataclass
    class _DenyingPolicyEngine:
        issuer: CapabilityIssuer

        def authorize(self, *, req: ToolAuthorizationRequest) -> PolicyDecision:
            if req.action == "executor.run":
                return PolicyDecision(
                    allowed=False,
                    reason="policy.prod.docker.non_root_user_required",
                    mode="enforce",
                    source="opa",
                    block=True,
                )
            return PolicyDecision(
                allowed=True,
                reason="ok",
                mode="enforce",
                source="opa",
                block=False,
            )

    res = run_compile_loop(
        tenant_id="tenant-a",
        repo_id="repo-prod",
        goal="Goal",
        plan_store=mem.plan_state,
        code_memory=mem.code_memory,
        why_graph=mem.why_graph,
        index=None,
        llm=llm,
        executor=ex,
        config=cfg,
        policy_engine=_DenyingPolicyEngine(issuer=CapabilityIssuer()),
    )

    assert res.status == "failed"
    decisions = list(res.accounting.get("policy_decisions", []))
    assert decisions
    deny = decisions[-1]
    assert deny["allowed"] is False
    assert deny["scope"] == {"tenant_id": "tenant-a", "repo_id": "repo-prod"}
    assert deny["reason"] == "policy.prod.docker.non_root_user_required"
    assert deny["context"]["backend"] == "docker"
    assert deny["context"]["docker"]["user_is_non_root"] is False
    assert deny["context"]["docker"]["network_mode"] == "enabled"

    loaded = mem.plan_state.load_plan(
        tenant_id="tenant-a",
        repo_id="repo-prod",
        plan_id=res.plan.id,
    )
    assert loaded is not None
    step = next(s for s in loaded.steps if s.id == plan.steps[0].id)
    out = dict(step.outputs or {})
    failure = dict(out["last_policy_failure"])
    assert failure["reason"] == "policy.prod.docker.non_root_user_required"
    assert failure["scope"] == {"tenant_id": "tenant-a", "repo_id": "repo-prod"}
    assert failure["context"]["docker"]["user"] == "0:0"
