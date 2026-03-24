from __future__ import annotations

import time
from collections.abc import Callable, Set
from dataclasses import replace
from pathlib import Path
from typing import Any

from akc.compile.artifact_passes import parse_patch_artifact_strict
from akc.compile.controller_config import Budget, ControllerConfig
from akc.compile.controller_patch_utils import (
    _best_of,
    _escalate_tier,
    _estimate_cost_usd,
    _estimate_token_count,
    _is_test_path,
    _policy_requires_tests,
    _score_candidate,
)
from akc.compile.controller_plan_helpers import (
    _replayed_execution_stage,
    _set_step_outputs,
    _set_step_status,
)
from akc.compile.controller_policy_runtime import COMPILE_PATCH_APPLY_ACTION
from akc.compile.controller_types import Candidate, ControllerResult, RunStatus
from akc.compile.executors import StageRunResult, run_stage
from akc.compile.interfaces import ExecutionResult
from akc.compile.ir_builder import IntentContractShape, build_ir_document_from_plan
from akc.compile.ir_passes import DefaultIRGeneratePromptPass, DefaultIRRepairPromptPass
from akc.compile.ir_prompt_context import intent_prompt_context_from_ir_and_resolve
from akc.compile.patch_emitter import (
    ModelCallNeeded,
    ResolvedPatch,
    StageName,
    patch_sha256_hex,
    resolve_patch_candidate_from_prompt,
)
from akc.compile.planner import advance_plan
from akc.compile.repair import parse_execution_failure
from akc.compile.scoped_apply import ScopedApplyAccounting, run_scoped_apply_pipeline
from akc.compile.verifier import DeterministicVerifier, VerifierPolicy
from akc.control.policy import PolicyEngine, ToolAuthorizationError, ToolAuthorizationRequest
from akc.control.tracing import new_span_id, now_unix_nano
from akc.intent.acceptance import evaluate_intent_success_criteria
from akc.intent.models import IntentSpec
from akc.intent.resolve import ResolvedIntentContext
from akc.intent.store import IntentStore
from akc.memory.code_memory import make_item
from akc.memory.models import now_ms
from akc.memory.plan_state import PlanStateStore
from akc.run.manifest import ReplayMode, RunManifest
from akc.run.replay import decide_replay_for_pass

AppendSpanFn = Callable[..., None]


def run_budgeted_generate_execute_repair_loop(
    *,
    tenant_id: str,
    repo_id: str,
    plan_store: PlanStateStore,
    scope: Any,
    plan: Any,
    step_id: str,
    code_memory: Any,
    config: ControllerConfig,
    ctx: dict[str, Any],
    intent_spec: IntentSpec,
    intent_id: str,
    intent_contract: dict[str, Any],
    intent_fingerprint: Any,
    knowledge_semantic_fingerprint: str | None,
    knowledge_provenance_fingerprint: str | None,
    bounds_projection: Any,
    goal: str,
    active_objectives: list[Any],
    linked_constraints: list[Any],
    active_success_criteria: list[Any],
    policy_llm: Any,
    policy_executor: Any,
    exec_base_capability: Any,
    policy_engine: PolicyEngine,
    replay_mode: ReplayMode,
    replay_manifest: RunManifest | None,
    intent_mandatory_partial_replay_passes: Set[str] | None,
    current_stable_intent_sha256: str | None,
    verifier: DeterministicVerifier,
    gen_tier: Any,
    smoke_command: list[str],
    full_command: list[str],
    smoke_stage_name: str,
    full_stage_name: str,
    smoke_timeout_s_effective: float | None,
    full_timeout_s_effective: float | None,
    smoke_policy_context: dict[str, Any],
    full_policy_context: dict[str, Any],
    effective_max_wall_time_s: float | None,
    effective_max_steps: int,
    effective_max_input_tokens: int | None,
    effective_max_output_tokens: int | None,
    budget: Budget,
    start: float,
    accounting: dict[str, Any],
    append_span: AppendSpanFn,
    best_initial: Candidate | None,
    compile_succeeded_seen_initial: bool,
    intent_satisfied_initial: bool,
    step_span_id: str,
    run_span_id: str,
    step_started_ns: int,
    run_started_ns: int,
    resolved_intent_context: ResolvedIntentContext | None = None,
    intent_store: IntentStore | None = None,
    intent_contract_shape: IntentContractShape = "full",
) -> ControllerResult:
    """ARCS-style bounded tiered generate/execute/repair loop."""

    _append_span = append_span

    def _record_policy_failure(
        *,
        action: str,
        reason: str,
        stage_name: str,
        context: dict[str, Any] | None,
    ) -> Any:
        plan2 = _set_step_outputs(
            plan=plan,
            step_id=step_id,
            outputs_patch={
                "policy_decisions": list(accounting["policy_decisions"]),
                "last_policy_failure": {
                    "code": "policy.authorization_denied",
                    "action": action,
                    "stage": stage_name,
                    "message": f"policy authorization denied for {action}: {reason}",
                    "reason": reason,
                    "context": dict(context or {}),
                    "scope": {
                        "tenant_id": tenant_id,
                        "repo_id": repo_id,
                    },
                },
            },
        )
        plan_store.save_plan(tenant_id=tenant_id, repo_id=repo_id, plan=plan2)
        return plan2

    def _wall_budget_ok() -> bool:
        if effective_max_wall_time_s is None:
            return True
        return (time.monotonic() - start) <= float(effective_max_wall_time_s)

    def _token_budget_ok() -> bool:
        if effective_max_input_tokens is not None and int(accounting["input_tokens"]) >= int(
            effective_max_input_tokens
        ):
            return False
        if effective_max_output_tokens is not None and int(accounting["output_tokens"]) >= int(
            effective_max_output_tokens
        ):
            return False
        return not (
            budget.max_total_tokens is not None and int(accounting["total_tokens"]) >= int(budget.max_total_tokens)
        )

    def _tool_budget_ok() -> bool:
        if budget.max_tool_calls is None:
            return True
        return int(accounting["tool_calls"]) < int(budget.max_tool_calls)

    def _refresh_estimated_cost() -> None:
        md = dict(config.metadata or {})
        # Backward compatibility: allow legacy metadata keys when explicit cost rates are unset.
        in_rate = float(config.cost_rates.input_per_1k_tokens_usd)
        out_rate = float(config.cost_rates.output_per_1k_tokens_usd)
        tool_rate = float(config.cost_rates.tool_call_usd)
        if in_rate == 0.0:
            in_rate = float(md.get("cost_input_per_1k_tokens_usd", 0.0) or 0.0)
        if out_rate == 0.0:
            out_rate = float(md.get("cost_output_per_1k_tokens_usd", 0.0) or 0.0)
        if tool_rate == 0.0:
            tool_rate = float(md.get("cost_tool_call_usd", 0.0) or 0.0)
        accounting["estimated_cost_usd"] = float(
            _estimate_cost_usd(
                input_tokens=int(accounting["input_tokens"]),
                output_tokens=int(accounting["output_tokens"]),
                tool_calls=int(accounting["tool_calls"]),
                input_per_1k_tokens_usd=in_rate,
                output_per_1k_tokens_usd=out_rate,
                tool_call_usd=tool_rate,
            )
        )

    def _cost_budget_ok() -> bool:
        if budget.max_cost_usd is None:
            return True
        return float(accounting["estimated_cost_usd"]) <= float(budget.max_cost_usd)

    # Generate → Execute loop with bounded repairs.
    current_tier = gen_tier
    stage: StageName = "generate"
    last_exec: ExecutionResult | None = None
    best: Candidate | None = best_initial
    compile_succeeded_seen = compile_succeeded_seen_initial
    intent_satisfied = intent_satisfied_initial
    max_repairs = int(budget.effective_max_repairs_per_step())
    max_iters_total = int(effective_max_steps)
    repairs_used = 0

    # IR-first prompt passes (compiler pass contract).
    generate_prompt_pass = DefaultIRGeneratePromptPass()
    repair_prompt_pass = DefaultIRRepairPromptPass()

    while True:
        if accounting["iterations_total"] >= max_iters_total:
            break
        if (
            accounting["llm_calls"] >= int(budget.max_llm_calls)
            or not _wall_budget_ok()
            or not _token_budget_ok()
            or not _cost_budget_ok()
        ):
            break

        # ARCS-style: use a single tier knob for both generate and repair, escalating on failures.
        tier_cfg = current_tier
        accounting["tier_history"].append({"stage": stage, "tier": tier_cfg.name})

        max_out = effective_max_output_tokens
        if max_out is None:
            max_out = tier_cfg.default_max_output_tokens

        # Build IR for the current plan state so passes can consume IR.
        # Phase 6/intent-layer: include a first-class intent node derived
        # from the normalized intent contract + fingerprints.
        intent_node_properties = dict(intent_contract)
        intent_node_properties["intent_semantic_fingerprint"] = intent_fingerprint.semantic
        intent_node_properties["intent_goal_text_fingerprint"] = intent_fingerprint.goal_text
        ir_doc = build_ir_document_from_plan(
            plan=plan,
            intent_node_properties=intent_node_properties,
            intent_store=intent_store,
            controller_intent_spec=intent_spec,
            resolved_intent_context=resolved_intent_context,
            warn_legacy_step_blobs_without_intent_ref_under_outputs_root=bool(intent_store is not None),
            intent_contract_shape=intent_contract_shape,
        )
        if resolved_intent_context is not None:
            ipc = intent_prompt_context_from_ir_and_resolve(
                ir_doc=ir_doc,
                resolved=resolved_intent_context,
                reference_first=(intent_contract_shape == "reference_first"),
            )
            ao, lc, asc = ipc.active_objectives, ipc.linked_constraints, ipc.active_success_criteria
        else:
            ao, lc, asc = active_objectives, linked_constraints, active_success_criteria

        test_policy: dict[str, Any] | None = None
        step_title: str | None = None
        failure: Any | None = None
        verifier_fb: dict[str, Any] | None = None

        if stage == "generate":
            test_policy = {
                "tests_generated_by_default": bool(config.generate_tests_by_default),
                "require_tests_for_non_test_changes": bool(config.require_tests_for_non_test_changes),
                "smoke_test_command": list(smoke_command),
                "full_test_command": list(full_command),
            }
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
            step_title = step.title

        effective_replay_manifest = replay_manifest or RunManifest(
            run_id=plan.id,
            tenant_id=tenant_id,
            repo_id=repo_id,
            ir_sha256="0" * 64,
            replay_mode=replay_mode,  # validated by RunManifest
        )
        pass_decision = decide_replay_for_pass(
            manifest=effective_replay_manifest,
            pass_name=stage,
            current_intent_semantic_fingerprint=intent_fingerprint.semantic,
            current_stable_intent_sha256=current_stable_intent_sha256,
            current_knowledge_semantic_fingerprint=knowledge_semantic_fingerprint,
            current_knowledge_provenance_fingerprint=knowledge_provenance_fingerprint,
            intent_mandatory_partial_replay_passes=intent_mandatory_partial_replay_passes,
        )

        last_generation_text = best.llm_text if best is not None else ""

        patch_resolution: ModelCallNeeded | ResolvedPatch | None = resolve_patch_candidate_from_prompt(
            stage=stage,
            plan=plan,
            ir_doc=ir_doc,
            step_id=step_id,
            tier_name=tier_cfg.name,
            tier_model=tier_cfg.llm_model,
            temperature=float(tier_cfg.temperature),
            max_output_tokens=int(max_out) if max_out is not None else None,
            intent_id=intent_id,
            active_objectives=list(ao),
            linked_constraints=list(lc),
            active_success_criteria=list(asc),
            goal=goal,
            retrieved_context=ctx,
            test_policy=test_policy,
            step_title=step_title,
            last_generation_text=last_generation_text,
            failure=failure,
            verifier_feedback=verifier_fb,
            replay_mode=replay_mode,
            replay_manifest=effective_replay_manifest,
            should_call_model=pass_decision.should_call_model,
            generate_prompt_pass=generate_prompt_pass,
            repair_prompt_pass=repair_prompt_pass,
        )

        if patch_resolution is None:
            break

        prompt_key = patch_resolution.prompt_key
        patch_text: str
        patch_sha256: str
        if isinstance(patch_resolution, ModelCallNeeded):
            try:
                resp = policy_llm.complete(
                    scope=scope,
                    stage=patch_resolution.llm_stage,
                    request=patch_resolution.llm_request,
                    token_constraints={
                        "plan_id": plan.id,
                        "step_id": step_id,
                        "tier": tier_cfg.name,
                        "replay_mode": replay_mode,
                        "intent_operating_bounds_effective": dict(bounds_projection.effective),
                        "intent_policies": [p.to_json_obj() for p in intent_spec.policies]
                        if intent_spec.policies
                        else [],
                    },
                )
            except ToolAuthorizationError as exc:
                plan = _record_policy_failure(
                    action="llm.complete",
                    reason=exc.decision.reason,
                    stage_name=patch_resolution.llm_stage,
                    context={
                        "stage": patch_resolution.llm_stage,
                        "intent_operating_bounds_effective": dict(bounds_projection.effective),
                    },
                )
                break
            accounting["llm_calls"] += 1
            usage = dict(resp.usage or {})
            in_tok = (
                int(usage.get("input_tokens", 0))
                if "input_tokens" in usage
                else _estimate_token_count(patch_resolution.user_prompt)
            )
            out_tok = (
                int(usage.get("output_tokens", 0)) if "output_tokens" in usage else _estimate_token_count(resp.text)
            )
            accounting["input_tokens"] += in_tok
            accounting["output_tokens"] += out_tok
            accounting["total_tokens"] += in_tok + out_tok
            _refresh_estimated_cost()
            _append_span(
                span_id=new_span_id(),
                parent_span_id=step_span_id,
                name="compile.llm.complete",
                kind="client",
                start_ns=now_unix_nano() - 1,
                end_ns=now_unix_nano(),
                attributes={
                    "stage": stage,
                    "tier": tier_cfg.name,
                    "model": tier_cfg.llm_model,
                    "input_tokens": in_tok,
                    "output_tokens": out_tok,
                    "prompt_key": prompt_key,
                    "gen_ai.request.model": tier_cfg.llm_model,
                    "gen_ai.input_tokens": in_tok,
                    "gen_ai.output_tokens": out_tok,
                },
            )
            parsed_live = parse_patch_artifact_strict(text=resp.text)
            if parsed_live is None:
                # Fail-closed: invalid model artifact cannot enter execute/repair loop.
                break
            patch_text = parsed_live.patch_text
            touched_paths = list(parsed_live.touched_paths)
            patch_sha256 = patch_sha256_hex(patch_text=patch_text)
        else:
            patch_text = patch_resolution.candidate.patch_text
            touched_paths = list(patch_resolution.candidate.touched_paths)
            patch_sha256 = patch_resolution.patch_sha256

        accounting["iterations_total"] += 1
        ok_tests_policy, policy_evidence = _policy_requires_tests(
            touched_paths=touched_paths,
            require_tests_for_non_test_changes=bool(config.require_tests_for_non_test_changes),
        )

        # Execute the produced artifact (executor decides what it means).
        # Tests-by-default:
        # - full mode: run one test command per iteration
        # - smoke mode: run smoke each iteration, and only run full on smoke pass ("promotion gate")
        if pass_decision.should_call_tools:
            if not _tool_budget_ok():
                break
            try:
                smoke_res = run_stage(
                    executor=policy_executor,
                    scope=scope,
                    stage=smoke_stage_name if config.test_mode == "smoke" else full_stage_name,
                    command=list(smoke_command if config.test_mode == "smoke" else full_command),
                    timeout_s=(smoke_timeout_s_effective if config.test_mode == "smoke" else full_timeout_s_effective),
                    run_id=plan.id,
                    policy_context=(smoke_policy_context if config.test_mode == "smoke" else full_policy_context),
                    policy_base_capability=exec_base_capability,
                )
            except ToolAuthorizationError as exc:
                smoke_policy_ctx = smoke_policy_context if config.test_mode == "smoke" else full_policy_context
                plan = _record_policy_failure(
                    action="executor.run",
                    reason=exc.decision.reason,
                    stage_name=smoke_stage_name if config.test_mode == "smoke" else full_stage_name,
                    context=smoke_policy_ctx,
                )
                break
            accounting["tool_calls"] += 1
            _refresh_estimated_cost()
            exec_result = smoke_res.result
            last_exec = exec_result
            smoke_end_ns = now_unix_nano()
            smoke_start_ns = smoke_end_ns - max(1, int(exec_result.duration_ms or 0) * 1_000_000)
            _append_span(
                span_id=new_span_id(),
                parent_span_id=step_span_id,
                name=f"compile.executor.{smoke_res.stage}",
                kind="client",
                start_ns=smoke_start_ns,
                end_ns=smoke_end_ns,
                attributes={
                    "command": list(smoke_res.command),
                    "exit_code": int(exec_result.exit_code),
                    "duration_ms": exec_result.duration_ms,
                },
                status="ok" if int(exec_result.exit_code) == 0 else "error",
            )
        else:
            cached_exec = _replayed_execution_stage(
                plan=plan, step_id=step_id, replay_manifest=effective_replay_manifest
            )
            if cached_exec is None:
                break
            cached_stage, cached_result, cached_command = cached_exec
            smoke_res = StageRunResult(
                stage=cached_stage,
                command=list(cached_command),
                result=cached_result,
            )
            exec_result = cached_result
            last_exec = exec_result

        full_res = None
        should_run_full = False
        if pass_decision.should_call_tools and config.test_mode == "smoke" and int(exec_result.exit_code) == 0:
            n = config.full_test_every_n_iterations
            if n is None:
                should_run_full = True
            else:
                it = int(accounting["iterations_total"])
                # Run full periodically and on the last allowed iteration so we don't
                # exit without a final full-gate signal.
                should_run_full = (it % int(n) == 0) or (it >= max_iters_total)
        if should_run_full:
            if not _tool_budget_ok():
                break
            try:
                full_res = run_stage(
                    executor=policy_executor,
                    scope=scope,
                    stage=full_stage_name,
                    command=list(full_command),
                    timeout_s=full_timeout_s_effective,
                    run_id=plan.id,
                    policy_context=full_policy_context,
                    policy_base_capability=exec_base_capability,
                )
            except ToolAuthorizationError as exc:
                full_policy_ctx = full_policy_context
                plan = _record_policy_failure(
                    action="executor.run",
                    reason=exc.decision.reason,
                    stage_name=full_stage_name,
                    context=full_policy_ctx,
                )
                break
            accounting["tool_calls"] += 1
            _refresh_estimated_cost()
            exec_result = full_res.result
            last_exec = exec_result
            full_end_ns = now_unix_nano()
            full_start_ns = full_end_ns - max(1, int(exec_result.duration_ms or 0) * 1_000_000)
            _append_span(
                span_id=new_span_id(),
                parent_span_id=step_span_id,
                name=f"compile.executor.{full_res.stage}",
                kind="client",
                start_ns=full_start_ns,
                end_ns=full_end_ns,
                attributes={
                    "command": list(full_res.command),
                    "exit_code": int(exec_result.exit_code),
                    "duration_ms": exec_result.duration_ms,
                },
                status="ok" if int(exec_result.exit_code) == 0 else "error",
            )

        promotable = int(exec_result.exit_code) == 0 and (config.test_mode != "smoke" or full_res is not None)

        # Compute verifier result early so monotonic "improvement" can take it
        # into account. We only persist the verification output if we reach the
        # promotion gate (i.e. not vetoed earlier by monotonic/policy).
        verifier_result = None
        if promotable and ok_tests_policy:
            verifier_policy = VerifierPolicy(
                enabled=bool(config.verifier_enabled),
                strict=bool(config.verifier_strict),
            )
            verifier_result = verifier.verify(
                scope=scope,
                plan_id=plan.id,
                step_id=step_id,
                candidate_patch=patch_text,
                execution=exec_result,
                accounting=accounting,
                budget=config.budget,
                policy=verifier_policy,
            )

        previous_best = best
        cand = Candidate(
            tier=str(tier_cfg.name),
            stage=stage,
            llm_text=patch_text,
            touched_paths=tuple(touched_paths),
            test_paths=tuple([p for p in touched_paths if _is_test_path(p)]),
            execution=exec_result,
            execution_stage=(full_res.stage if full_res is not None else smoke_res.stage),
            execution_command=list(full_res.command if full_res is not None else smoke_res.command),
            score=_score_candidate(
                execution=exec_result,
                ok_tests_policy=ok_tests_policy,
                promotable=promotable,
                verifier_passed=(verifier_result.passed if verifier_result is not None else None),
            ),
            attempt_idx=int(accounting["iterations_total"]),
            created_at_ms=now_ms(),
        )
        best = _best_of(best, cand)
        accounting["best_score"] = int(best.score)
        # ARCS-style monotonicity: repair candidates must strictly improve score.
        improved = previous_best is None or cand.score > int(previous_best.score)

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
                "policy": {
                    "mode": config.policy_mode,
                    "allowlist": list(config.tool_allowlist),
                },
                "policy_decisions": list(accounting["policy_decisions"]),
                "last_prompt_key": prompt_key,
                "last_patch_sha256": patch_sha256,
                "last_failure": failure_json,
            },
        )
        plan_store.save_plan(tenant_id=tenant_id, repo_id=repo_id, plan=plan)

        if promotable and not improved and stage == "repair":
            monotonic_msg = "monotonic improvement violated: repair candidate did not improve candidate score"
            monotonic_exec = ExecutionResult(exit_code=3, stdout="", stderr=monotonic_msg, duration_ms=0)
            last_exec = monotonic_exec
            plan = _set_step_outputs(
                plan=plan,
                step_id=step_id,
                outputs_patch={
                    "last_monotonic_failure": {
                        "code": "repair.non_improving",
                        "message": monotonic_msg,
                        "best_score": int(previous_best.score) if previous_best is not None else None,
                        "candidate_score": int(cand.score),
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
            assert verifier_result is not None  # ok_tests_policy ensured above
            vres = verifier_result
            plan = _set_step_outputs(
                plan=plan,
                step_id=step_id,
                outputs_patch={"last_verification": vres.to_json_obj()},
            )
            plan_store.save_plan(tenant_id=tenant_id, repo_id=repo_id, plan=plan)
            _append_span(
                span_id=new_span_id(),
                parent_span_id=step_span_id,
                name="compile.verify",
                kind="internal",
                start_ns=now_unix_nano() - 1,
                end_ns=now_unix_nano(),
                attributes={
                    "passed": bool(vres.passed),
                    "strict": bool(config.verifier_strict),
                },
                status="ok" if bool(vres.passed) else "error",
            )
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

            # Phase 5: intent acceptance contract execution (deterministic).
            compile_succeeded_seen = True
            acceptance = evaluate_intent_success_criteria(
                success_criteria=(
                    resolved_intent_context.spec.success_criteria
                    if resolved_intent_context is not None
                    else intent_spec.success_criteria
                ),
                execution=exec_result,
                patch_text=patch_text,
                touched_paths=touched_paths,
                accounting=accounting,
                wall_time_ms=int((time.monotonic() - start) * 1000.0),
                verifier_passed=bool(vres.passed),
            )
            plan = _set_step_outputs(
                plan=plan,
                step_id=step_id,
                outputs_patch={"last_intent_acceptance": acceptance.to_step_output_obj()},
            )
            plan_store.save_plan(tenant_id=tenant_id, repo_id=repo_id, plan=plan)
            _append_span(
                span_id=new_span_id(),
                parent_span_id=step_span_id,
                name="compile.intent_acceptance",
                kind="internal",
                start_ns=now_unix_nano() - 1,
                end_ns=now_unix_nano(),
                attributes={
                    "passed": bool(acceptance.passed),
                    "evaluated_success_criteria": int(acceptance.evaluated_count),
                },
                status="ok" if bool(acceptance.passed) else "error",
            )
            if not acceptance.passed:
                # Treat acceptance failure as a repair-eligible failure.
                intent_msg = (
                    "intent.acceptance_failed: " + "; ".join(list(acceptance.failures)[:3])
                    if acceptance.failures
                    else "intent.acceptance_failed"
                )
                last_exec = ExecutionResult(exit_code=4, stdout="", stderr=intent_msg, duration_ms=0)
                intent_satisfied = False
                if repairs_used >= max_repairs:
                    break
                repairs_used += 1
                accounting["repair_iterations"] = repairs_used
                stage = "repair"
                current_tier = _escalate_tier(current=current_tier, config=config)
                continue

            intent_satisfied = True

            if config.compile_realization_mode == "scoped_apply":
                apply_policy_ctx: dict[str, Any] = {
                    "plan_id": plan.id,
                    "step_id": step_id,
                    "patch_sha256": patch_sha256,
                    "compile_realization_mode": "scoped_apply",
                }
                apply_token = policy_engine.issuer.issue(
                    scope=scope,
                    action=COMPILE_PATCH_APPLY_ACTION,
                    constraints={
                        "patch_sha256": patch_sha256,
                        "plan_id": plan.id,
                        "step_id": step_id,
                    },
                )
                apply_decision = policy_engine.authorize(
                    req=ToolAuthorizationRequest(
                        scope=scope,
                        action=COMPILE_PATCH_APPLY_ACTION,
                        capability=apply_token,
                        context=apply_policy_ctx,
                    )
                )
                accounting["policy_decisions"].append(
                    {
                        "action": COMPILE_PATCH_APPLY_ACTION,
                        "scope": {"tenant_id": scope.tenant_id, "repo_id": scope.repo_id},
                        "token_id": str(apply_token.token_id),
                        "constraints": dict(apply_token.constraints or {}),
                        "context": dict(apply_policy_ctx),
                        "allowed": bool(apply_decision.allowed),
                        "reason": str(apply_decision.reason),
                        "source": str(apply_decision.source),
                        "mode": str(apply_decision.mode),
                        "block": bool(apply_decision.block),
                    }
                )
                if not apply_decision.allowed:
                    scope_root_str: str | None = None
                    ar = str(config.apply_scope_root or "").strip()
                    if ar:
                        scope_root_str = str(Path(ar).expanduser().resolve())
                    csa_obj = ScopedApplyAccounting(
                        compile_realization_mode="scoped_apply",
                        attempted=True,
                        applied=False,
                        deny_reason="policy.compile.patch.apply_denied",
                        reject_reason=str(apply_decision.reason),
                        policy_blocked=True,
                        scope_root=scope_root_str,
                        patch_sha256=patch_sha256,
                        patch_binary=None,
                        files=(),
                    ).to_json_obj()
                else:
                    csa_obj = run_scoped_apply_pipeline(
                        compile_realization_mode=config.compile_realization_mode,
                        apply_scope_root=config.apply_scope_root,
                        patch_text=patch_text,
                        patch_sha256=patch_sha256,
                    )
            else:
                csa_obj = run_scoped_apply_pipeline(
                    compile_realization_mode=config.compile_realization_mode,
                    apply_scope_root=config.apply_scope_root,
                    patch_text=patch_text,
                    patch_sha256=patch_sha256,
                )
            accounting["compile_scoped_apply"] = csa_obj
            csa_applied = bool(isinstance(csa_obj, dict) and csa_obj.get("applied"))
            _append_span(
                span_id=new_span_id(),
                parent_span_id=step_span_id,
                name="compile.scoped_apply",
                kind="internal",
                start_ns=now_unix_nano() - 1,
                end_ns=now_unix_nano(),
                attributes={
                    "mode": str(config.compile_realization_mode),
                    "applied": csa_applied,
                },
                status="ok",
            )

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
                        content=patch_text,
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
                        kind="test_result",
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
                        content=exec_result.stdout + ("\n" + exec_result.stderr if exec_result.stderr else ""),
                        metadata={
                            "plan_id": plan.id,
                            "step_id": step_id,
                            "stage": (full_res.stage if full_res is not None else smoke_res.stage),
                            "exit_code": int(exec_result.exit_code),
                            "duration_ms": exec_result.duration_ms,
                            "command": list(full_res.command if full_res is not None else smoke_res.command),
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
                        content=exec_result.stdout + ("\n" + exec_result.stderr if exec_result.stderr else ""),
                        metadata={
                            "plan_id": plan.id,
                            "step_id": step_id,
                            "stage": (full_res.stage if full_res is not None else smoke_res.stage),
                            "exit_code": int(exec_result.exit_code),
                            "duration_ms": exec_result.duration_ms,
                            "command": list(full_res.command if full_res is not None else smoke_res.command),
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
            accounting["finished_at_ms"] = now_ms()
            accounting["wall_time_ms"] = int((time.monotonic() - start) * 1000.0)
            run_end_ns = now_unix_nano()
            _append_span(
                span_id=step_span_id,
                parent_span_id=run_span_id,
                name="compile.step",
                kind="internal",
                start_ns=step_started_ns,
                end_ns=run_end_ns,
                attributes={"step_id": step_id, "status": "succeeded"},
            )
            _append_span(
                span_id=run_span_id,
                parent_span_id=None,
                name="compile.run",
                kind="internal",
                start_ns=run_started_ns,
                end_ns=run_end_ns,
                attributes={
                    "tenant_id": tenant_id,
                    "repo_id": repo_id,
                    "plan_id": plan.id,
                    "status": "succeeded",
                },
            )
            return ControllerResult(
                status="succeeded",
                plan=plan,
                best_candidate=best,
                accounting=accounting,
                compile_succeeded=True,
                intent_satisfied=True,
            )

        # Smoke-only pass without a full gate:
        # keep iterating without consuming a repair.
        if config.test_mode == "smoke" and int(smoke_res.result.exit_code) == 0 and full_res is None:
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
    if accounting["llm_calls"] < int(budget.max_llm_calls) and _wall_budget_ok() and _cost_budget_ok():
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
    accounting["finished_at_ms"] = now_ms()
    accounting["wall_time_ms"] = int((time.monotonic() - start) * 1000.0)
    run_end_ns = now_unix_nano()
    _append_span(
        span_id=step_span_id,
        parent_span_id=run_span_id,
        name="compile.step",
        kind="internal",
        start_ns=step_started_ns,
        end_ns=run_end_ns,
        attributes={"step_id": step_id, "status": str(status)},
        status="error",
    )
    _append_span(
        span_id=run_span_id,
        parent_span_id=None,
        name="compile.run",
        kind="internal",
        start_ns=run_started_ns,
        end_ns=run_end_ns,
        attributes={
            "tenant_id": tenant_id,
            "repo_id": repo_id,
            "plan_id": plan.id,
            "status": str(status),
        },
        status="error",
    )
    return ControllerResult(
        status=status,
        plan=plan,
        best_candidate=best,
        accounting=accounting,
        compile_succeeded=bool(compile_succeeded_seen),
        intent_satisfied=bool(intent_satisfied),
    )
