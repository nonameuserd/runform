"""Phase 3 ARCS-style budgeted tiered controller.

This module orchestrates Plan → Retrieve → Generate → Execute → Repair.

Design goals:
- Enforce tenant+repo isolation by threading scope everywhere.
- Enforce a conservative budget (LLM calls, repair iterations, wall time).
- Persist progress into PlanState (step status + best candidate + accounting).
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Any, Literal

from akc.compile.controller_config import ControllerConfig, TierConfig
from akc.compile.executors import run_stage
from akc.compile.interfaces import (
    ExecutionResult,
    Executor,
    LLMBackend,
    LLMMessage,
    LLMRequest,
    LLMResponse,
    TenantRepoScope,
)
from akc.compile.planner import advance_plan
from akc.compile.repair import build_repair_prompt, parse_execution_failure
from akc.compile.retriever import retrieve_context
from akc.compile.verifier import DeterministicVerifier, VerifierPolicy
from akc.memory.code_memory import make_item
from akc.memory.models import PlanState, PlanStep, PlanStepStatus, now_ms, require_non_empty
from akc.memory.plan_state import PlanStateStore

RunStatus = Literal["succeeded", "failed", "budget_exhausted"]


@dataclass(frozen=True, slots=True)
class Candidate:
    """A single generate→execute attempt."""

    tier: str
    stage: str
    llm_text: str
    touched_paths: tuple[str, ...]
    test_paths: tuple[str, ...]
    execution: ExecutionResult | None
    execution_stage: str | None
    execution_command: list[str] | None
    score: int
    attempt_idx: int
    created_at_ms: int

    def to_json_obj(self) -> dict[str, Any]:
        exe = None
        if self.execution is not None:
            exe = {
                "stage": self.execution_stage,
                "command": list(self.execution_command or []),
                "exit_code": int(self.execution.exit_code),
                "stdout": self.execution.stdout,
                "stderr": self.execution.stderr,
                "duration_ms": self.execution.duration_ms,
            }
        return {
            "tier": self.tier,
            "stage": self.stage,
            "llm_text": self.llm_text,
            "touched_paths": list(self.touched_paths),
            "test_paths": list(self.test_paths),
            "has_test_changes": bool(self.test_paths),
            "execution": exe,
            "score": int(self.score),
            "attempt_idx": int(self.attempt_idx),
            "created_at_ms": int(self.created_at_ms),
        }


@dataclass(frozen=True, slots=True)
class ControllerResult:
    status: RunStatus
    plan: PlanState
    best_candidate: Candidate | None
    accounting: dict[str, Any]


def _tier_order(name: str) -> int:
    if name == "small":
        return 0
    if name == "medium":
        return 1
    if name == "large":
        return 2
    return 99


def _escalate_tier(*, current: TierConfig, config: ControllerConfig) -> TierConfig:
    """Escalate tier conservatively: small→medium→large (if available)."""

    tiers = sorted(config.tiers.values(), key=lambda t: _tier_order(t.name))
    for idx, t in enumerate(tiers):
        if t.name == current.name:
            if idx + 1 < len(tiers):
                return tiers[idx + 1]
            return current
    return current


def _best_of(a: Candidate | None, b: Candidate) -> Candidate:
    if a is None:
        return b
    # Higher score wins; tie-break deterministically on attempt_idx.
    if b.score > a.score:
        return b
    if b.score < a.score:
        return a
    return b if b.attempt_idx >= a.attempt_idx else a


def _extract_patch_paths(patch_text: str) -> list[str]:
    """Extract touched file paths from a unified diff.

    Best-effort and deterministic: returns stable sorted unique paths.
    """
    paths: set[str] = set()
    for raw in (patch_text or "").splitlines():
        line = raw.strip()
        # Common forms:
        # --- a/foo.py
        # +++ b/foo.py
        if line.startswith("+++ "):
            p = line[4:].strip()
            if p.startswith("b/"):
                p = p[2:]
            if p and p != "/dev/null":
                paths.add(p)
        elif line.startswith("--- "):
            p = line[4:].strip()
            if p.startswith("a/"):
                p = p[2:]
            if p and p != "/dev/null":
                paths.add(p)
    return sorted(paths)


def _is_test_path(p: str) -> bool:
    p2 = str(p or "").replace("\\", "/")
    parts = [seg for seg in p2.split("/") if seg]
    if not parts:
        return False
    if parts[0] in {"test", "tests"}:
        return True
    leaf = parts[-1]
    if leaf.startswith("test_") and leaf.endswith(".py"):
        return True
    return bool(leaf.endswith("_test.py"))


def _policy_requires_tests(
    *,
    touched_paths: list[str],
    require_tests_for_non_test_changes: bool,
) -> tuple[bool, dict[str, Any]]:
    """Return (ok, evidence) for the tests-generated-by-default heuristic."""

    tests = [p for p in touched_paths if _is_test_path(p)]
    non_tests = [p for p in touched_paths if not _is_test_path(p)]
    if not require_tests_for_non_test_changes:
        return True, {
            "touched_paths": touched_paths,
            "test_paths": tests,
            "non_test_paths": non_tests,
        }
    # Only require tests if the patch touches at least one non-test path.
    if non_tests and not tests:
        return False, {
            "touched_paths": touched_paths,
            "test_paths": tests,
            "non_test_paths": non_tests,
        }
    return True, {
        "touched_paths": touched_paths,
        "test_paths": tests,
        "non_test_paths": non_tests,
    }


def _score_execution(result: ExecutionResult | None) -> int:
    if result is None:
        return 0
    # Simple monotone scoring: pass >> fail. Keep it stable for tests.
    return 1000 if int(result.exit_code) == 0 else 10


def _derive_full_test_command(smoke_command: list[str]) -> list[str]:
    """Derive a 'full' test command from a smoke command deterministically.

    Policy:
    - If '-q' is present, drop it (common 'smoke' speed + noiseless mode).
    - Otherwise keep the command as-is.
    """

    if not smoke_command:
        raise ValueError("smoke_command must be non-empty")
    return [c for c in smoke_command if c != "-q"]


def _update_step(
    *,
    plan: PlanState,
    step_id: str,
    mutate: Callable[[PlanStep], PlanStep],
) -> PlanState:
    require_non_empty(step_id, name="step_id")
    steps2: list[PlanStep] = []
    found = False
    for s in plan.steps:
        if s.id != step_id:
            steps2.append(s)
            continue
        steps2.append(mutate(s))
        found = True
    if not found:
        raise ValueError("step not found")
    return replace(plan, steps=tuple(steps2), updated_at_ms=now_ms())


def _set_step_outputs(
    *,
    plan: PlanState,
    step_id: str,
    outputs_patch: dict[str, Any],
) -> PlanState:
    def _mutate(s: PlanStep) -> PlanStep:
        out = dict(s.outputs or {})
        out.update(dict(outputs_patch))
        return replace(s, outputs=out)

    return _update_step(plan=plan, step_id=step_id, mutate=_mutate)


def _set_step_status(
    *,
    plan: PlanState,
    step_id: str,
    status: PlanStepStatus,
    notes: str | None = None,
) -> PlanState:
    t = now_ms()

    def _mutate(s: PlanStep) -> PlanStep:
        started = s.started_at_ms
        finished = s.finished_at_ms
        if status == "in_progress" and started is None:
            started = t
        if status in {"done", "failed", "skipped"} and finished is None:
            finished = t
        return PlanStep(
            id=s.id,
            title=s.title,
            status=status,
            order_idx=s.order_idx,
            started_at_ms=started,
            finished_at_ms=finished,
            notes=notes if notes is not None else s.notes,
            inputs=s.inputs,
            outputs=s.outputs,
        )

    return _update_step(plan=plan, step_id=step_id, mutate=_mutate)


def run_compile_loop(
    *,
    tenant_id: str,
    repo_id: str,
    goal: str,
    plan_store: PlanStateStore,
    code_memory: Any,  # CodeMemoryStore (kept untyped here to avoid import cycle)
    why_graph: Any,
    index: Any,
    llm: LLMBackend,
    executor: Executor,
    config: ControllerConfig,
) -> ControllerResult:
    """Run the Phase 3 compile loop for the active plan step.

    This is intentionally conservative and dependency-free: it does not apply patches
    to a working tree yet; instead it treats the LLM output as the artifact and uses
    the executor to validate it (tests, linters, etc.) based on the provided config.
    """

    require_non_empty(tenant_id, name="tenant_id")
    require_non_empty(repo_id, name="repo_id")
    require_non_empty(goal, name="goal")
    scope = TenantRepoScope(tenant_id=tenant_id, repo_id=repo_id)

    plan = advance_plan(
        tenant_id=tenant_id,
        repo_id=repo_id,
        plan_id=plan_store.get_active_plan_id(tenant_id=tenant_id, repo_id=repo_id)
        or plan_store.create_plan(tenant_id=tenant_id, repo_id=repo_id, goal=goal).id,
        plan_store=plan_store,
        feedback=None,
    )

    step_id = plan.next_step_id
    if step_id is None:
        return ControllerResult(status="succeeded", plan=plan, best_candidate=None, accounting={})

    plan = _set_step_status(plan=plan, step_id=step_id, status="in_progress")
    plan_store.save_plan(tenant_id=tenant_id, repo_id=repo_id, plan=plan)

    budget = config.budget
    start = time.monotonic()
    accounting: dict[str, Any] = {
        "llm_calls": 0,
        "repair_iterations": 0,
        "iterations_total": 0,
        "started_at_ms": now_ms(),
        "tier_history": [],
    }

    best: Candidate | None = None
    verifier = DeterministicVerifier()

    # Retrieve once per step for now (keeps budgeting simple).
    ctx = retrieve_context(
        tenant_id=tenant_id,
        repo_id=repo_id,
        plan=plan,
        code_memory=code_memory,
        why_graph=why_graph,
        index=index,
        limit=20,
    )

    gen_tier = config.tier_for_stage(stage="generate")
    # Tests-by-default: run tests for every candidate evaluation.
    # Prefer explicit config, but remain backward-compatible with older metadata keys.
    smoke_command = list(
        config.test_command
        if config.test_command is not None
        else (config.metadata or {}).get("execute_command") or ["python", "-m", "pytest", "-q"]
    )
    smoke_timeout_s_raw = (
        config.test_timeout_s
        if config.test_timeout_s is not None
        else (config.metadata or {}).get("execute_timeout_s")
    )
    smoke_timeout_s_f = float(smoke_timeout_s_raw) if smoke_timeout_s_raw is not None else None

    full_command_raw = (config.metadata or {}).get("full_test_command")
    full_command = (
        list(full_command_raw)
        if isinstance(full_command_raw, (list, tuple)) and full_command_raw
        else _derive_full_test_command(smoke_command)
    )
    full_timeout_s_raw = (config.metadata or {}).get("full_test_timeout_s")
    full_timeout_s_f = (
        float(full_timeout_s_raw) if full_timeout_s_raw is not None else smoke_timeout_s_f
    )

    smoke_stage_name = "tests_smoke"
    full_stage_name = "tests_full"

    def _wall_budget_ok() -> bool:
        if budget.max_wall_time_s is None:
            return True
        return (time.monotonic() - start) <= float(budget.max_wall_time_s)

    # Generate → Execute loop with bounded repairs.
    current_tier = gen_tier
    stage: str = "generate"
    last_exec: ExecutionResult | None = None
    max_repairs = int(budget.effective_max_repairs_per_step())
    max_iters_total = int(budget.max_iterations_total)
    repairs_used = 0
    while True:
        if accounting["iterations_total"] >= max_iters_total:
            break
        if accounting["llm_calls"] >= int(budget.max_llm_calls) or not _wall_budget_ok():
            break

        # ARCS-style: use a single tier knob for both generate and repair, escalating on failures.
        tier_cfg = current_tier
        accounting["tier_history"].append({"stage": stage, "tier": tier_cfg.name})

        max_out = budget.max_output_tokens
        if max_out is None:
            max_out = tier_cfg.default_max_output_tokens

        if stage == "generate":
            test_policy = {
                "tests_generated_by_default": bool(config.generate_tests_by_default),
                "require_tests_for_non_test_changes": bool(
                    config.require_tests_for_non_test_changes
                ),
                "smoke_test_command": list(smoke_command),
                "full_test_command": list(full_command),
            }
            prompt = (
                f"Goal:\n{goal}\n\n"
                f"Plan:\n{plan.to_json_obj()}\n\n"
                f"Retrieved context:\n{ctx}\n\n"
                f"Test policy:\n{test_policy}\n\n"
                f"Stage: {stage}\n\n"
                "Output format:\n"
                "- Return ONLY a unified diff (git-style) patch.\n"
                "- Do not include prose, explanations, or Markdown fences.\n"
                "- The patch must be tenant-safe: never read/write outside this repo "
                "and never mix tenants.\n"
                "- By default, include relevant test changes in the same patch "
                "(add/update tests that cover your change).\n"
            )
        else:
            # Repair stage: parse failure and build a more structured prompt.
            step = next(s for s in plan.steps if s.id == step_id)
            assert last_exec is not None
            failure = parse_execution_failure(result=last_exec)
            # If the previous iteration was vetoed by the verifier, the controller
            # stores its structured result in step outputs. Thread it into repair context.
            verifier_fb = None
            try:
                step_outputs = dict(step.outputs or {})
                last_ver = step_outputs.get("last_verification")
                if isinstance(last_ver, dict):
                    verifier_fb = last_ver
            except Exception:
                verifier_fb = None
            prompt = build_repair_prompt(
                goal=goal,
                plan_json=plan.to_json_obj(),
                step_id=step_id,
                step_title=step.title,
                retrieved_context=ctx,
                last_generation_text=best.llm_text if best is not None else "",
                failure=failure,
                verifier_feedback=verifier_fb,
            )

        resp: LLMResponse = llm.complete(
            scope=scope,
            stage="generate" if stage == "generate" else "repair",
            request=LLMRequest(
                messages=[
                    LLMMessage(role="system", content="You are an AKC compile loop assistant."),
                    LLMMessage(role="user", content=prompt),
                ],
                temperature=float(tier_cfg.temperature),
                max_output_tokens=int(max_out) if max_out is not None else None,
                metadata={
                    "tier": tier_cfg.name,
                    "tier_model": tier_cfg.llm_model,
                    "plan_id": plan.id,
                    "step_id": step_id,
                },
            ),
        )
        accounting["llm_calls"] += 1
        accounting["iterations_total"] += 1

        touched_paths = _extract_patch_paths(resp.text)
        ok_tests_policy, policy_evidence = _policy_requires_tests(
            touched_paths=touched_paths,
            require_tests_for_non_test_changes=bool(config.require_tests_for_non_test_changes),
        )

        # Execute the produced artifact (executor decides what it means).
        # Tests-by-default:
        # - full mode: run one test command per iteration
        # - smoke mode: run smoke each iteration, and only run full on smoke pass ("promotion gate")
        smoke_res = run_stage(
            executor=executor,
            scope=scope,
            stage=smoke_stage_name if config.test_mode == "smoke" else full_stage_name,
            command=list(smoke_command if config.test_mode == "smoke" else full_command),
            timeout_s=smoke_timeout_s_f if config.test_mode == "smoke" else full_timeout_s_f,
        )
        exec_result = smoke_res.result
        last_exec = exec_result

        full_res = None
        should_run_full = False
        if config.test_mode == "smoke" and int(exec_result.exit_code) == 0:
            n = config.full_test_every_n_iterations
            if n is None:
                should_run_full = True
            else:
                it = int(accounting["iterations_total"])
                # Run full periodically and on the last allowed iteration so we don't
                # exit without a final full-gate signal.
                should_run_full = (it % int(n) == 0) or (it >= max_iters_total)
        if should_run_full:
            full_res = run_stage(
                executor=executor,
                scope=scope,
                stage=full_stage_name,
                command=list(full_command),
                timeout_s=full_timeout_s_f,
            )
            exec_result = full_res.result
            last_exec = exec_result

        cand = Candidate(
            tier=str(tier_cfg.name),
            stage=stage,
            llm_text=resp.text,
            touched_paths=tuple(touched_paths),
            test_paths=tuple([p for p in touched_paths if _is_test_path(p)]),
            execution=exec_result,
            execution_stage=(full_res.stage if full_res is not None else smoke_res.stage),
            execution_command=list(full_res.command if full_res is not None else smoke_res.command),
            score=_score_execution(exec_result),
            attempt_idx=int(accounting["iterations_total"]),
            created_at_ms=now_ms(),
        )
        best = _best_of(best, cand)

        # Persist "best so far" into plan step outputs (monotonic).
        failure_json = None
        if int(exec_result.exit_code) != 0:
            failure_json = parse_execution_failure(result=exec_result).to_json_obj()
        plan = _set_step_outputs(
            plan=plan,
            step_id=step_id,
            outputs_patch={
                "best_candidate": best.to_json_obj(),
                "last_tests_smoke": smoke_res.to_json_obj(),
                "last_tests_full": full_res.to_json_obj() if full_res is not None else None,
                "accounting": dict(accounting),
                "last_failure": failure_json,
            },
        )
        plan_store.save_plan(tenant_id=tenant_id, repo_id=repo_id, plan=plan)

        # Promotion gate:
        # - full mode: a passing full stage can promote
        # - smoke mode: only a passing *full* stage can promote
        #   (smoke-only passes are not promotable)
        promotable = int(exec_result.exit_code) == 0 and (
            config.test_mode != "smoke" or full_res is not None
        )
        if promotable and not ok_tests_policy:
            # Treat as a policy failure eligible for repair; store evidence on the step.
            policy_msg = (
                "policy violation: patch changes non-test code but does not add/update tests; "
                "include relevant tests in the patch (tests generated by default)"
            )
            policy_exec = ExecutionResult(exit_code=2, stdout="", stderr=policy_msg, duration_ms=0)
            last_exec = policy_exec
            plan = _set_step_outputs(
                plan=plan,
                step_id=step_id,
                outputs_patch={
                    "last_policy_failure": {
                        "code": "policy.missing_tests",
                        "message": policy_msg,
                        "evidence": policy_evidence,
                    }
                },
            )
            plan_store.save_plan(tenant_id=tenant_id, repo_id=repo_id, plan=plan)
            if repairs_used >= max_repairs:
                break
            repairs_used += 1
            accounting["repair_iterations"] = repairs_used
            stage = "repair"
            current_tier = _escalate_tier(current=current_tier, config=config)
            continue
        if promotable:
            # Phase 5 verifier gate: can veto promotion even after tests pass.
            policy = VerifierPolicy(
                enabled=bool(config.verifier_enabled),
                strict=bool(config.verifier_strict),
            )
            vres = verifier.verify(
                scope=scope,
                plan_id=plan.id,
                step_id=step_id,
                candidate_patch=resp.text,
                execution=exec_result,
                accounting=accounting,
                budget=config.budget,
                policy=policy,
            )
            plan = _set_step_outputs(
                plan=plan,
                step_id=step_id,
                outputs_patch={"last_verification": vres.to_json_obj()},
            )
            plan_store.save_plan(tenant_id=tenant_id, repo_id=repo_id, plan=plan)
            if not vres.passed:
                # Treat verifier veto as a failure eligible for repair (budgeted).
                last_exec = exec_result
                if repairs_used >= max_repairs:
                    break
                repairs_used += 1
                accounting["repair_iterations"] = repairs_used
                stage = "repair"
                current_tier = _escalate_tier(current=current_tier, config=config)
                continue

            # Success: persist artifacts into code memory and mark step done.
            tms = now_ms()
            patch_paths = touched_paths
            patch_item_id = f"{plan.id}:{step_id}:patch"
            test_item_id = f"{plan.id}:{step_id}:test_result"
            smoke_item_id = f"{plan.id}:{step_id}:test_smoke_result"
            full_item_id = f"{plan.id}:{step_id}:test_full_result"
            code_memory.upsert_items(
                tenant_id=tenant_id,
                repo_id=repo_id,
                artifact_id=plan.id,
                items=[
                    make_item(
                        tenant_id=tenant_id,
                        repo_id=repo_id,
                        artifact_id=plan.id,
                        item_id=patch_item_id,
                        kind="patch",
                        content=resp.text,
                        metadata={
                            "plan_id": plan.id,
                            "step_id": step_id,
                            "tier": tier_cfg.name,
                            "stage": str(stage),
                            "paths": patch_paths,
                        },
                        created_at_ms=tms,
                        updated_at_ms=tms,
                    ),
                    make_item(
                        tenant_id=tenant_id,
                        repo_id=repo_id,
                        artifact_id=plan.id,
                        item_id=smoke_item_id,
                        kind="test_smoke_result",
                        content=smoke_res.result.stdout
                        + ("\n" + smoke_res.result.stderr if smoke_res.result.stderr else ""),
                        metadata={
                            "plan_id": plan.id,
                            "step_id": step_id,
                            "stage": smoke_res.stage,
                            "exit_code": int(smoke_res.result.exit_code),
                            "duration_ms": smoke_res.result.duration_ms,
                            "command": list(smoke_res.command),
                            "paths": patch_paths,
                        },
                        created_at_ms=tms,
                        updated_at_ms=tms,
                    ),
                    make_item(
                        tenant_id=tenant_id,
                        repo_id=repo_id,
                        artifact_id=plan.id,
                        item_id=full_item_id,
                        kind="test_full_result",
                        content=exec_result.stdout
                        + ("\n" + exec_result.stderr if exec_result.stderr else ""),
                        metadata={
                            "plan_id": plan.id,
                            "step_id": step_id,
                            "stage": (full_res.stage if full_res is not None else smoke_res.stage),
                            "exit_code": int(exec_result.exit_code),
                            "duration_ms": exec_result.duration_ms,
                            "command": list(
                                full_res.command if full_res is not None else smoke_res.command
                            ),
                            "paths": patch_paths,
                        },
                        created_at_ms=tms,
                        updated_at_ms=tms,
                    ),
                    make_item(
                        tenant_id=tenant_id,
                        repo_id=repo_id,
                        artifact_id=plan.id,
                        item_id=test_item_id,
                        kind="test_result",
                        content=exec_result.stdout
                        + ("\n" + exec_result.stderr if exec_result.stderr else ""),
                        metadata={
                            "plan_id": plan.id,
                            "step_id": step_id,
                            "stage": (full_res.stage if full_res is not None else smoke_res.stage),
                            "exit_code": int(exec_result.exit_code),
                            "duration_ms": exec_result.duration_ms,
                            "command": list(
                                full_res.command if full_res is not None else smoke_res.command
                            ),
                            "paths": patch_paths,
                        },
                        created_at_ms=tms,
                        updated_at_ms=tms,
                    ),
                ],
            )

            plan = _set_step_status(plan=plan, step_id=step_id, status="done")
            plan = _set_step_outputs(
                plan=plan,
                step_id=step_id,
                outputs_patch={
                    "code_memory_item_ids": [patch_item_id, test_item_id],
                    "code_memory_test_item_ids": [smoke_item_id, full_item_id],
                },
            )
            plan_store.save_plan(tenant_id=tenant_id, repo_id=repo_id, plan=plan)
            plan = advance_plan(
                tenant_id=tenant_id,
                repo_id=repo_id,
                plan_id=plan.id,
                plan_store=plan_store,
                feedback={"status": "passed", "step_id": step_id},
            )
            return ControllerResult(
                status="succeeded",
                plan=plan,
                best_candidate=best,
                accounting=accounting,
            )

        # Smoke-only pass without a full gate:
        # keep iterating without consuming a repair.
        if (
            config.test_mode == "smoke"
            and int(smoke_res.result.exit_code) == 0
            and full_res is None
        ):
            stage = "generate"
            continue

        # Failure: iterate repair if budget allows.
        if repairs_used >= max_repairs:
            break
        repairs_used += 1
        accounting["repair_iterations"] = repairs_used
        stage = "repair"
        # ARCS-style: escalate generation tier after a failed execute.
        current_tier = _escalate_tier(current=current_tier, config=config)

    # If we reach here we did not succeed.
    status: RunStatus = "budget_exhausted"
    if accounting["llm_calls"] < int(budget.max_llm_calls) and _wall_budget_ok():
        status = "failed"

    plan = _set_step_status(
        plan=plan,
        step_id=step_id,
        status="failed",
        notes="compile loop did not produce a passing candidate within budget",
    )
    plan = replace(
        plan,
        last_feedback={"status": str(status), "step_id": step_id},
        updated_at_ms=now_ms(),
    )
    plan_store.save_plan(tenant_id=tenant_id, repo_id=repo_id, plan=plan)
    plan = advance_plan(
        tenant_id=tenant_id,
        repo_id=repo_id,
        plan_id=plan.id,
        plan_store=plan_store,
        feedback={"status": str(status), "step_id": step_id},
    )
    return ControllerResult(status=status, plan=plan, best_candidate=best, accounting=accounting)
