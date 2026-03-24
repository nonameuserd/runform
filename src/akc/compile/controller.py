"""Phase 3 ARCS-style budgeted tiered controller.

This module orchestrates Plan → Retrieve → Generate → Execute → Repair.

Design goals:
- Enforce tenant+repo isolation by threading scope everywhere.
- Enforce a conservative budget (LLM calls, repair iterations, wall time).
- Persist progress into PlanState (step status + best candidate + accounting).
"""

from __future__ import annotations

import sys
import time
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

from akc.compile.controller_budget_loop import run_budgeted_generate_execute_repair_loop
from akc.compile.controller_config import ControllerConfig
from akc.compile.controller_patch_utils import (
    _derive_full_test_command,
    _estimate_cost_usd,
)
from akc.compile.controller_plan_helpers import (
    _replayed_retrieval_context,
    _set_step_outputs,
    _set_step_status,
)
from akc.compile.controller_policy_runtime import setup_policy_runtime
from akc.compile.controller_types import Candidate, ControllerResult
from akc.compile.interfaces import (
    Executor,
    LLMBackend,
    TenantRepoScope,
)
from akc.compile.ir_builder import build_ir_document_from_plan
from akc.compile.ir_prompt_context import (
    build_reference_intent_contract_for_retrieval,
    effective_intent_contract_shape_for_compile_prompts,
    intent_prompt_context_from_ir_and_resolve,
)
from akc.compile.knowledge_extractor import (
    build_intent_constraint_ids_by_assertion,
    extract_knowledge_snapshot,
)
from akc.compile.planner import (
    advance_plan,
    annotate_constraint_hints_for_verifier,
    inject_knowledge_into_plan_step_inputs,
    prior_knowledge_snapshot_from_plan,
)
from akc.compile.provenance_mapper import (
    build_doc_id_to_provenance_map,
    build_retrieval_documents_item_ids_and_provenance,
)
from akc.compile.retriever import boost_retrieved_documents_for_knowledge_evidence, retrieve_context
from akc.compile.rust_bridge import RustExecConfig
from akc.compile.why_graph_writer import upsert_knowledge_snapshot_into_why_graph
from akc.control.policy import (
    PolicyEngine,
)
from akc.control.tracing import TraceSpan, new_span_id, new_trace_id, now_unix_nano
from akc.execute.strong import create_strong_underlying_executor
from akc.intent import (
    compile_intent_spec,
    compute_intent_fingerprint,
    project_intent_operating_bounds_to_policy_context,
    project_stage_timeout_s,
)
from akc.intent.models import ConstraintLink, IntentSpec, SuccessCriterionLink
from akc.intent.plan_step_intent import build_plan_step_intent_ref
from akc.intent.resolve import resolve_compile_intent_context
from akc.intent.store import IntentStore
from akc.knowledge import knowledge_provenance_fingerprint, knowledge_semantic_fingerprint
from akc.knowledge.persistence import KNOWLEDGE_MEDIATION_RELPATH, write_knowledge_mediation_report_artifact
from akc.memory.models import (
    JSONValue,
    PlanStep,
    json_value_as_float,
    json_value_as_int,
    json_value_as_optional_float,
    json_value_as_optional_int,
    now_ms,
    require_non_empty,
)
from akc.memory.plan_state import PlanStateStore
from akc.memory.why_conflicts import enrich_conflict_reports_from_mediation
from akc.memory.why_graph import ConflictDetector
from akc.promotion import intent_declares_deployable_objective
from akc.run.intent_replay_mandates import mandatory_partial_replay_passes_for_success_criteria
from akc.run.manifest import ReplayMode, RunManifest


def _hard_allow_network_from_executor(*, executor: Executor) -> bool:
    """Infer the hard executor/network allow setting.

    This is used to intersect intent `allow_network` requests with the
    configured sandbox/network hard defaults.
    """

    # Strong wrapper: cfg.allow_network indicates the hard allowed state.
    cfg = getattr(executor, "cfg", None)
    if cfg is not None and hasattr(cfg, "allow_network"):
        try:
            return bool(cfg.allow_network)
        except Exception:
            pass

    rust_cfg = getattr(executor, "rust_cfg", None)
    if rust_cfg is not None and hasattr(rust_cfg, "allow_network"):
        try:
            return bool(rust_cfg.allow_network)
        except Exception:
            pass

    disable_network = getattr(executor, "disable_network", None)
    if disable_network is not None:
        return not bool(disable_network)

    # Fail-closed: if we can't infer, assume "network denied".
    return False


def _apply_effective_allow_network_to_executor(*, executor: Executor, allow_network: bool) -> Executor:
    """Return an executor with network tightened to `allow_network`.

    Defense-in-depth: intent bounds should never widen network authority beyond
    the hard sandbox/executor defaults.
    """

    hard_allow = _hard_allow_network_from_executor(executor=executor)
    if bool(hard_allow) == bool(allow_network):
        return executor

    cfg = getattr(executor, "cfg", None)
    if cfg is not None and hasattr(cfg, "allow_network") and hasattr(executor, "underlying"):
        # Strong sandbox wrapper: rebuild underlying so the tightened network
        # policy is enforced at runtime.
        new_cfg = replace(cfg, allow_network=bool(allow_network))
        new_underlying = create_strong_underlying_executor(cfg=new_cfg)
        return cast(Executor, replace(cast(Any, executor), cfg=new_cfg, underlying=new_underlying))

    if cfg is not None and hasattr(cfg, "allow_network"):
        # Dev sandbox wrapper: underlying is created per-run based on cfg.
        new_cfg = replace(cfg, allow_network=bool(allow_network))
        return cast(Executor, replace(cast(Any, executor), cfg=new_cfg))

    rust_cfg = getattr(executor, "rust_cfg", None)
    if rust_cfg is not None and hasattr(rust_cfg, "allow_network"):
        new_rust_cfg = replace(rust_cfg, allow_network=bool(allow_network))
        return cast(Executor, replace(cast(Any, executor), rust_cfg=new_rust_cfg))

    if hasattr(executor, "disable_network"):
        disable_network = not bool(allow_network)
        return cast(Executor, replace(cast(Any, executor), disable_network=disable_network))

    # Unknown executor: can't tighten at runtime.
    # Authorization will still be narrowed via policy context.
    return executor


def _platform_label() -> str:
    if sys.platform.startswith("linux"):
        return "linux"
    if sys.platform == "darwin":
        return "darwin"
    if sys.platform in {"win32", "cygwin"}:
        return "windows"
    return sys.platform


def _docker_apparmor_available() -> bool:
    if _platform_label() != "linux":
        return False
    try:
        from pathlib import Path

        return (
            Path("/sys/module/apparmor/parameters/enabled").read_text(encoding="utf-8").strip().lower().startswith("y")
        )
    except OSError:
        return False


def _unwrap_executor_for_policy(executor: Executor) -> Any:
    underlying = getattr(executor, "underlying", None)
    return underlying if underlying is not None else executor


def _executor_backend_label(executor: Executor) -> str:
    effective = _unwrap_executor_for_policy(executor)
    rust_cfg = getattr(effective, "rust_cfg", None)
    if isinstance(rust_cfg, RustExecConfig):
        return "wasm" if rust_cfg.lane == "wasm" else "process"
    cls_name = effective.__class__.__name__.lower()
    if "docker" in cls_name:
        return "docker"
    if "subprocess" in cls_name:
        return "process"
    return "unknown"


def _wasm_policy_context(
    *,
    executor: Executor,
    timeout_s: float | None,
    policy_mode: str,
    metadata: dict[str, Any],
) -> dict[str, Any] | None:
    effective = _unwrap_executor_for_policy(executor)
    rust_cfg = getattr(effective, "rust_cfg", None)
    if not isinstance(rust_cfg, RustExecConfig) or rust_cfg.lane != "wasm":
        return None

    network_exception_raw = metadata.get("wasm_network_exception")
    if network_exception_raw is None:
        network_exception_raw = metadata.get("policy_wasm_network_exception")
    network_exception = str(network_exception_raw).strip() if network_exception_raw else ""
    strict_profile = policy_mode == "enforce" or bool(rust_cfg.wasm_normalization_strict)
    writable_preopen_dirs = list(rust_cfg.allowed_write_paths)
    writable_preopen_dir_set = set(writable_preopen_dirs)
    read_only_preopen_dirs = [path for path in rust_cfg.preopen_dirs if path not in writable_preopen_dir_set]

    required_controls = [
        name
        for name, enabled in (
            ("wall_time_ms", timeout_s is not None),
            ("memory_bytes", rust_cfg.memory_bytes is not None),
            ("cpu_fuel", rust_cfg.cpu_fuel is not None),
            ("stdout_max_bytes", rust_cfg.stdout_max_bytes is not None),
            ("stderr_max_bytes", rust_cfg.stderr_max_bytes is not None),
        )
        if enabled
    ]
    unsupported_controls: list[str] = []
    if _platform_label() == "windows" and strict_profile and timeout_s is not None:
        unsupported_controls.append("wall_time_ms")

    limits = {
        "wall_time_ms": int(timeout_s * 1000.0) if timeout_s is not None else None,
        "memory_bytes": rust_cfg.memory_bytes,
        "cpu_fuel": rust_cfg.cpu_fuel,
        "stdout_max_bytes": rust_cfg.stdout_max_bytes,
        "stderr_max_bytes": rust_cfg.stderr_max_bytes,
    }
    return {
        "network_enabled": bool(rust_cfg.allow_network),
        "network_mode": "enabled" if bool(rust_cfg.allow_network) else "none",
        "network_exception": network_exception or None,
        "preopen_dirs": list(rust_cfg.preopen_dirs),
        "writable_preopen_dirs": writable_preopen_dirs,
        "read_only_preopen_dirs": read_only_preopen_dirs,
        "limits": limits,
        "limits_tuple": [
            limits["wall_time_ms"],
            limits["memory_bytes"],
            limits["cpu_fuel"],
            limits["stdout_max_bytes"],
            limits["stderr_max_bytes"],
        ],
        "platform_capability_profile": {
            "os": _platform_label(),
            "profile": "strict" if strict_profile else "relaxed",
            "required_controls": required_controls,
            "unsupported_controls": unsupported_controls,
        },
    }


def _docker_policy_context(
    *,
    executor: Executor,
    metadata: dict[str, Any],
) -> dict[str, Any] | None:
    effective = _unwrap_executor_for_policy(executor)
    if "docker" not in effective.__class__.__name__.lower():
        return None

    user_raw = getattr(effective, "user", None)
    user = str(user_raw).strip() if user_raw is not None else ""
    seccomp_raw = getattr(effective, "seccomp_profile", None)
    seccomp_profile = str(seccomp_raw).strip() if seccomp_raw is not None else ""
    apparmor_raw = getattr(effective, "apparmor_profile", None)
    apparmor_profile = str(apparmor_raw).strip() if apparmor_raw is not None else ""
    ulimit_nofile_raw = getattr(effective, "ulimit_nofile", None)
    ulimit_nofile = str(ulimit_nofile_raw).strip() if ulimit_nofile_raw is not None else ""
    ulimit_nproc_raw = getattr(effective, "ulimit_nproc", None)
    ulimit_nproc = str(ulimit_nproc_raw).strip() if ulimit_nproc_raw is not None else ""
    network_exception_raw = metadata.get("docker_network_exception")
    if network_exception_raw is None:
        network_exception_raw = metadata.get("policy_docker_network_exception")
    network_exception = str(network_exception_raw).strip() if network_exception_raw else ""

    network_enabled = not bool(getattr(effective, "disable_network", False))
    apparmor_available = _docker_apparmor_available()
    effective_seccomp_profile = seccomp_profile or "runtime/default"
    effective_apparmor_profile = apparmor_profile or ("docker-default" if apparmor_available else "")
    return {
        "network_enabled": network_enabled,
        "network_mode": "enabled" if network_enabled else "none",
        "network_exception": network_exception or None,
        "read_only_rootfs": bool(getattr(effective, "read_only_rootfs", False)),
        "no_new_privileges": bool(getattr(effective, "no_new_privileges", False)),
        "cap_drop_all": bool(getattr(effective, "cap_drop_all", False)),
        "user": user or None,
        "user_present": bool(user),
        "user_is_non_root": bool(user) and user not in {"0", "0:0", "root", "root:root"},
        "security_profiles": {
            "seccomp": effective_seccomp_profile,
            "apparmor": effective_apparmor_profile or None,
        },
        "platform": {
            "os": _platform_label(),
            "apparmor_available": apparmor_available,
        },
        "limits": {
            "memory_bytes": getattr(effective, "memory_bytes", None),
            "pids_limit": getattr(effective, "pids_limit", None),
            "cpus": getattr(effective, "cpus", None),
            "ulimit_nofile": ulimit_nofile or None,
            "ulimit_nproc": ulimit_nproc or None,
        },
        "tmpfs_mounts": list(getattr(effective, "tmpfs_mounts", ()) or ()),
    }


def _build_executor_policy_context(
    *,
    executor: Executor,
    stage: str,
    command: list[str],
    timeout_s: float | None,
    replay_mode: ReplayMode,
    policy_mode: str,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    backend = _executor_backend_label(executor)
    ctx: dict[str, Any] = {
        "stage": stage,
        "command": list(command),
        "replay_mode": replay_mode,
        "backend": backend,
    }
    wasm_ctx = _wasm_policy_context(
        executor=executor,
        timeout_s=timeout_s,
        policy_mode=policy_mode,
        metadata=metadata,
    )
    if wasm_ctx is not None:
        ctx["wasm"] = wasm_ctx
    docker_ctx = _docker_policy_context(executor=executor, metadata=metadata)
    if docker_ctx is not None:
        ctx["docker"] = docker_ctx
    return ctx


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
    replay_mode: ReplayMode = "live",
    replay_manifest: RunManifest | None = None,
    policy_engine: PolicyEngine | None = None,
    intent_spec: IntentSpec | None = None,
    intent_file: str | Path | None = None,
    intent_store: IntentStore | None = None,
    knowledge_artifact_root: str | Path | None = None,
) -> ControllerResult:
    """Run the Phase 3 compile loop for the active plan step.

    By default ``ControllerConfig.compile_realization_mode`` is ``scoped_apply``: after a
    passing candidate, a policy-gated fail-closed apply path may mutate files under
    ``apply_scope_root`` (see :mod:`akc.compile.scoped_apply`). Set
    ``compile_realization_mode`` to ``artifact_only`` to skip working-tree apply and
    treat LLM output as artifacts only; the executor still validates candidates (tests,
    linters, etc.).
    """

    require_non_empty(tenant_id, name="tenant_id")
    require_non_empty(repo_id, name="repo_id")
    require_non_empty(goal, name="goal")
    require_non_empty(replay_mode, name="replay_mode")

    # Phase 2 normalization:
    # - For explicit intent-backed runs, session may pass a precompiled intent.
    # - For goal-only compatibility, compile from goal text.
    if intent_spec is None:
        if intent_file is not None:
            intent_spec = compile_intent_spec(
                tenant_id=tenant_id,
                repo_id=repo_id,
                intent_file=intent_file,
                controller_budget=config.budget,
            )
        else:
            intent_spec = compile_intent_spec(
                tenant_id=tenant_id,
                repo_id=repo_id,
                goal_statement=goal,
                controller_budget=config.budget,
            )
    intent_fingerprint = compute_intent_fingerprint(intent=intent_spec)
    if intent_store is not None:
        persisted = intent_spec.normalized()
        intent_store.save_intent(tenant_id=tenant_id, repo_id=repo_id, intent=persisted)
        intent_store.set_active_intent(tenant_id=tenant_id, repo_id=repo_id, intent_id=persisted.intent_id)
    plan_step_intent_ref: dict[str, Any] | None = None
    if intent_store is not None:
        plan_step_intent_ref = build_plan_step_intent_ref(
            intent=intent_spec,
            semantic_fingerprint=intent_fingerprint.semantic,
            goal_text_fingerprint=intent_fingerprint.goal_text,
        )
    goal_statement = (
        intent_spec.goal_statement
        if intent_spec.goal_statement is not None
        else (intent_spec.objectives[0].statement if intent_spec.objectives else goal)
    )
    # Ensure all downstream planning and retrieval uses the normalized goal.
    goal = goal_statement
    scope = TenantRepoScope(tenant_id=tenant_id, repo_id=repo_id)

    # Phase 4: Policy/runtime projection.
    #
    # Project intent operating bounds into an authorization-ready policy context
    # (and record every narrowing decision for auditability). Also tighten
    # executor network authority when needed so intent cannot widen runtime
    # network access beyond hard sandbox defaults.
    hard_allow_network = _hard_allow_network_from_executor(executor=executor)
    bounds_projection = project_intent_operating_bounds_to_policy_context(
        intent_bounds=intent_spec.operating_bounds,
        controller_budget=config.budget,
        hard_allow_network=hard_allow_network,
    )
    executor = _apply_effective_allow_network_to_executor(
        executor=executor,
        allow_network=bool(bounds_projection.effective.get("allow_network")),
    )
    effective_max_wall_time_s = json_value_as_optional_float(bounds_projection.effective.get("max_seconds"))
    effective_max_steps = json_value_as_int(bounds_projection.effective.get("max_steps"), default=0)
    effective_max_input_tokens = json_value_as_optional_int(bounds_projection.effective.get("max_input_tokens"))
    effective_max_output_tokens = json_value_as_optional_int(bounds_projection.effective.get("max_output_tokens"))
    intent_policy_context = bounds_projection.to_policy_context()
    # Provide intent policy refs to the policy engine as structured context.
    # Enforcement is still owned by the policy engine (allowlist/OPA).
    if intent_spec.policies:
        intent_policy_context["intent_policies"] = [p.to_json_obj() for p in intent_spec.policies]

    plan = advance_plan(
        tenant_id=tenant_id,
        repo_id=repo_id,
        plan_id=plan_store.get_active_plan_id(tenant_id=tenant_id, repo_id=repo_id)
        or plan_store.create_plan(tenant_id=tenant_id, repo_id=repo_id, goal=goal).id,
        plan_store=plan_store,
        feedback=None,
    )

    intent_id = intent_spec.intent_id
    active_objectives_for_steps = [o.to_summary_obj() for o in intent_spec.objectives]
    linked_constraints_for_steps = [
        ConstraintLink.from_constraint(constraint=c).to_json_obj() for c in intent_spec.constraints
    ]
    active_success_criteria_for_steps = [
        SuccessCriterionLink.from_success_criterion(sc=sc).to_json_obj() for sc in intent_spec.success_criteria
    ]
    steps2: list[PlanStep] = []
    for s in plan.steps:
        inputs = dict(s.inputs or {})
        inputs["intent_id"] = intent_id
        if plan_step_intent_ref is not None:
            inputs["intent_ref"] = dict(plan_step_intent_ref)
            for _legacy in ("active_objectives", "linked_constraints", "active_success_criteria"):
                inputs.pop(_legacy, None)
        else:
            inputs["active_objectives"] = list(active_objectives_for_steps)
            inputs["linked_constraints"] = list(linked_constraints_for_steps)
            inputs["active_success_criteria"] = list(active_success_criteria_for_steps)
        steps2.append(replace(s, inputs=inputs))
    plan = replace(plan, steps=tuple(steps2))

    first_step_inputs: dict[str, Any] = dict(plan.steps[0].inputs or {}) if plan.steps else {}
    resolved_intent = resolve_compile_intent_context(
        tenant_id=tenant_id,
        repo_id=repo_id,
        inputs=first_step_inputs,
        intent_store=intent_store,
        controller_intent_spec=intent_spec,
        fallback_goal_statement=goal,
        warn_legacy_step_blobs_without_intent_ref_under_outputs_root=bool(intent_store is not None),
    )
    intent_contract_shape = effective_intent_contract_shape_for_compile_prompts(
        policy=config.compile_prompt_intent_contract_policy,
        intent_store=intent_store,
        first_step_inputs=first_step_inputs,
    )

    step_id = plan.next_step_id
    if step_id is None:
        md = dict(config.metadata or {})
        profile_mode = str(md.get("developer_role_profile", "classic")).strip().lower()
        auto_seed_deployable = bool(md.get("developer_profile_auto_seed_step", False))
        require_deployable_steps = bool(md.get("require_deployable_steps", False))
        deployable_intent = intent_declares_deployable_objective(intent=intent_spec)
        if auto_seed_deployable and profile_mode == "emerging" and deployable_intent:
            seeded_step_id = "step_emerging_bootstrap"
            seeded_inputs: dict[str, Any] = {"intent_id": intent_id}
            if plan_step_intent_ref is not None:
                seeded_inputs["intent_ref"] = dict(plan_step_intent_ref)
            else:
                seeded_inputs["active_objectives"] = list(active_objectives_for_steps)
                seeded_inputs["linked_constraints"] = list(linked_constraints_for_steps)
                seeded_inputs["active_success_criteria"] = list(active_success_criteria_for_steps)
            seeded_step = PlanStep(
                id=seeded_step_id,
                title="Implement intent",
                status="pending",
                order_idx=len(plan.steps),
                inputs=seeded_inputs,
                outputs={},
            )
            plan = replace(
                plan,
                steps=tuple(list(plan.steps) + [seeded_step]),
                next_step_id=seeded_step_id,
                updated_at_ms=now_ms(),
            )
            plan_store.save_plan(tenant_id=tenant_id, repo_id=repo_id, plan=plan)
            step_id = seeded_step_id
        else:
            if require_deployable_steps and deployable_intent:
                fail_accounting: dict[str, Any] = {
                    "empty_plan_fail_closed": True,
                    "empty_plan_reason": (
                        "deployable intent produced no actionable compile steps; "
                        "set objective_class=analysis_only to allow artifact-only analysis success"
                    ),
                    "policy_mode": config.policy_mode,
                }
                return ControllerResult(
                    status="failed",
                    plan=plan,
                    best_candidate=None,
                    accounting=fail_accounting,
                    compile_succeeded=False,
                    intent_satisfied=False,
                )
            return ControllerResult(
                status="succeeded",
                plan=plan,
                best_candidate=None,
                accounting={},
                compile_succeeded=True,
                intent_satisfied=True,
            )

    plan = _set_step_status(plan=plan, step_id=step_id, status="in_progress")
    plan_store.save_plan(tenant_id=tenant_id, repo_id=repo_id, plan=plan)

    budget = config.budget
    start = time.monotonic()
    accounting: dict[str, Any] = {
        "llm_calls": 0,
        "tool_calls": 0,
        "input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
        "repair_iterations": 0,
        "iterations_total": 0,
        "started_at_ms": now_ms(),
        "tier_history": [],
        "best_score": 0,
        "estimated_cost_usd": 0.0,
        "policy_mode": config.policy_mode,
        "policy_decisions": [],
        "policy_operating_bounds_projection": bounds_projection.to_audit_artifact(),
        "trace_spans": [],
    }
    trace_id = new_trace_id()
    run_span_id = new_span_id()
    step_span_id = new_span_id()
    accounting["trace_id"] = trace_id
    accounting["run_span_id"] = run_span_id
    accounting["step_span_id"] = step_span_id
    if config.accounting_overlay:
        accounting.update(dict(config.accounting_overlay))
    run_started_ns = now_unix_nano()
    step_started_ns = now_unix_nano()

    def _append_span(
        *,
        span_id: str,
        parent_span_id: str | None,
        name: str,
        kind: str,
        start_ns: int,
        end_ns: int,
        attributes: dict[str, Any] | None = None,
        status: str = "ok",
    ) -> None:
        span_attrs: dict[str, JSONValue] | None = None
        if attributes:
            span_attrs = {str(k): cast(JSONValue, v) for k, v in attributes.items()}
        span = TraceSpan(
            trace_id=trace_id,
            span_id=span_id,
            parent_span_id=parent_span_id,
            name=name,
            kind=kind,
            start_time_unix_nano=int(start_ns),
            end_time_unix_nano=int(end_ns),
            attributes=span_attrs,
            status=status,
        )
        accounting["trace_spans"].append(span.to_json_obj())

    best: Candidate | None = None
    compile_succeeded_seen = False
    intent_satisfied = False

    verifier, policy_llm, policy_executor, exec_base_capability, effective_policy_engine = setup_policy_runtime(
        config=config,
        llm=llm,
        executor=executor,
        policy_engine=policy_engine,
        scope=scope,
        plan_id=plan.id,
        step_id=step_id,
        accounting=accounting,
    )

    # Structured intent for IR + retrieval (IR-first spine; PlanState stays execution state).
    if intent_contract_shape == "reference_first":
        intent_contract = build_reference_intent_contract_for_retrieval(
            intent_spec=intent_spec,
            resolved=resolved_intent,
            intent_semantic_fingerprint=intent_fingerprint.semantic,
            intent_goal_text_fingerprint=intent_fingerprint.goal_text,
            operating_bounds_effective=dict(bounds_projection.effective),
            first_step_inputs=first_step_inputs,
        )
    else:
        intent_contract = {
            "intent_contract_shape": "full",
            "intent_id": intent_spec.intent_id,
            "spec_version": int(intent_spec.spec_version),
            "goal_statement": intent_spec.goal_statement,
            "active_objectives": [o.to_summary_obj() for o in intent_spec.objectives],
            "linked_constraints": [
                ConstraintLink.from_constraint(constraint=c).to_json_obj() for c in intent_spec.constraints
            ],
            "active_success_criteria": [
                SuccessCriterionLink.from_success_criterion(sc=sc).to_json_obj() for sc in intent_spec.success_criteria
            ],
            "operating_bounds_requested": (
                intent_spec.operating_bounds.to_json_obj() if intent_spec.operating_bounds is not None else None
            ),
            "operating_bounds": dict(bounds_projection.effective),
        }
    intent_node_properties_for_ir: dict[str, Any] = dict(intent_contract)
    intent_node_properties_for_ir["intent_semantic_fingerprint"] = intent_fingerprint.semantic
    intent_node_properties_for_ir["intent_goal_text_fingerprint"] = intent_fingerprint.goal_text
    ir_doc_for_knowledge = build_ir_document_from_plan(
        plan=plan,
        intent_node_properties=intent_node_properties_for_ir,
        intent_store=intent_store,
        controller_intent_spec=intent_spec,
        resolved_intent_context=resolved_intent,
        warn_legacy_step_blobs_without_intent_ref_under_outputs_root=bool(intent_store is not None),
        intent_contract_shape=intent_contract_shape,
    )

    # Retrieve once per step for now (keeps budgeting simple). In replay modes,
    # prefer the persisted retrieval snapshot over live memory/index access.
    retrieve_span_id = new_span_id()
    retrieve_start_ns = now_unix_nano()
    replayed_ctx = _replayed_retrieval_context(replay_manifest=replay_manifest, goal=goal)
    prior_knowledge = prior_knowledge_snapshot_from_plan(plan, current_step_id=step_id)
    if replayed_ctx is not None:
        ctx = replayed_ctx
    else:
        ctx = retrieve_context(
            tenant_id=tenant_id,
            repo_id=repo_id,
            plan=plan,
            code_memory=code_memory,
            why_graph=why_graph,
            index=index,
            limit=20,
            ir_document=ir_doc_for_knowledge,
            knowledge_snapshot_for_query=prior_knowledge,
            knowledge_query_budget_chars=800,
        )

    ctx = dict(ctx)
    ctx["intent_contract"] = intent_contract
    _append_span(
        span_id=retrieve_span_id,
        parent_span_id=step_span_id,
        name="compile.retrieve",
        kind="internal",
        start_ns=retrieve_start_ns,
        end_ns=now_unix_nano(),
        attributes={
            "top_k": 20,
            "stage": "retrieve",
            "replayed_from_manifest": replayed_ctx is not None,
        },
    )
    # Phase 3 knowledge layer: unify intent constraints with retrieved evidence.
    # This runs before the generate/repair loop so later stages can treat
    # knowledge semantics as first-class.
    documents_for_evidence = ctx.get("documents") or []
    doc_id_to_provenance = build_doc_id_to_provenance_map(tenant_id=tenant_id, documents=documents_for_evidence)
    use_llm_knowledge = config.knowledge_extraction_mode in ("llm", "hybrid") and policy_llm is not None
    knowledge_mediation_report: dict[str, Any] = {}
    knowledge_snapshot = extract_knowledge_snapshot(
        tenant_id=tenant_id,
        repo_id=repo_id,
        intent_spec=intent_spec,
        retrieved_context=ctx,
        retrieval_provenance_by_doc_id=doc_id_to_provenance,
        llm=policy_llm,
        use_llm=use_llm_knowledge,
        ir_document=ir_doc_for_knowledge,
        doc_derived_assertions_mode=config.doc_derived_assertions_mode,
        doc_derived_max_assertions=int(config.doc_derived_max_assertions),
        doc_derived_patterns=config.doc_derived_patterns,
        knowledge_evidence_weighting=config.knowledge_evidence_weighting,
        knowledge_unresolved_conflict_policy=config.knowledge_unresolved_conflict_policy,
        compile_now_ms=int(time.time() * 1000),
        mediation_report_out=knowledge_mediation_report,
        knowledge_artifact_root=knowledge_artifact_root,
        knowledge_conflict_normalization=config.knowledge_conflict_normalization,
        knowledge_embedding_clustering_enabled=bool(config.knowledge_embedding_clustering_enabled),
        knowledge_embedding_clustering_threshold=float(config.knowledge_embedding_clustering_threshold),
        stored_assertion_index_mode=config.stored_assertion_index_mode,
        stored_assertion_index_max_rows=int(config.stored_assertion_index_max_rows),
        apply_operator_knowledge_decisions=bool(config.apply_operator_knowledge_decisions),
    )

    if replayed_ctx is None:
        ctx["documents"] = boost_retrieved_documents_for_knowledge_evidence(
            ctx.get("documents"),
            snapshot=knowledge_snapshot,
        )
    retrieval_item_ids, retrieval_provenance = build_retrieval_documents_item_ids_and_provenance(
        tenant_id=tenant_id,
        documents=ctx.get("documents") or [],
    )

    plan = inject_knowledge_into_plan_step_inputs(plan=plan, snapshot=knowledge_snapshot)
    plan = annotate_constraint_hints_for_verifier(plan=plan, snapshot=knowledge_snapshot)
    plan_store.save_plan(tenant_id=tenant_id, repo_id=repo_id, plan=plan)

    ir_doc_for_compile_prompts = build_ir_document_from_plan(
        plan=plan,
        intent_node_properties=intent_node_properties_for_ir,
        intent_store=intent_store,
        controller_intent_spec=intent_spec,
        resolved_intent_context=resolved_intent,
        warn_legacy_step_blobs_without_intent_ref_under_outputs_root=bool(intent_store is not None),
        intent_contract_shape=intent_contract_shape,
    )
    ipc0 = intent_prompt_context_from_ir_and_resolve(
        ir_doc=ir_doc_for_compile_prompts,
        resolved=resolved_intent,
        reference_first=(intent_contract_shape == "reference_first"),
    )
    active_objectives = list(ipc0.active_objectives)
    linked_constraints = list(ipc0.linked_constraints)
    active_success_criteria = list(ipc0.active_success_criteria)

    intent_assertion_map = build_intent_constraint_ids_by_assertion(
        intent_spec=intent_spec,
        repo_id=repo_id,
        documents=documents_for_evidence,
    )
    knowledge_mediation_fingerprint: str | None = None
    if knowledge_artifact_root is not None and knowledge_mediation_report:
        knowledge_mediation_fingerprint = write_knowledge_mediation_report_artifact(
            knowledge_artifact_root,
            tenant_id=tenant_id,
            repo_id=repo_id,
            mediation_report=knowledge_mediation_report,
        )

    # Knowledge-layer fingerprints are replay invalidation triggers.
    # Slice to match manifest fingerprint width (16-char hex).
    knowledge_semantic_fp_full = knowledge_semantic_fingerprint(snapshot=knowledge_snapshot)
    knowledge_provenance_fp_full = knowledge_provenance_fingerprint(snapshot=knowledge_snapshot)
    knowledge_semantic_fingerprint_16 = knowledge_semantic_fp_full[:16]
    knowledge_provenance_fingerprint_16 = knowledge_provenance_fp_full[:16]

    # Persist canonical constraint/decision assertions into the why-graph.
    # This provides provenance-bearing nodes/edges for later contradiction surfacing.
    if why_graph is not None:
        upsert_knowledge_snapshot_into_why_graph(
            tenant_id=tenant_id,
            repo_id=repo_id,
            why_graph=why_graph,
            snapshot=knowledge_snapshot,
            plan_goal=goal,
            intent_id=intent_id,
            knowledge_unresolved_conflict_policy=config.knowledge_unresolved_conflict_policy,
            knowledge_conflict_normalization=config.knowledge_conflict_normalization,
            knowledge_embedding_clustering_enabled=bool(config.knowledge_embedding_clustering_enabled),
            knowledge_embedding_clustering_threshold=float(config.knowledge_embedding_clustering_threshold),
            documents=documents_for_evidence,
            intent_constraint_ids_by_assertion=intent_assertion_map,
        )

        # Phase 2.5: surface and persist contradiction reports with provenance
        # evidence from the conflicting constraint nodes' payloads.
        new_constraint_ids = {str(c.assertion_id) for c in knowledge_snapshot.canonical_constraints}
        constraint_nodes: list[Any] = []
        for cid in new_constraint_ids:
            node = why_graph.get_node(
                tenant_id=tenant_id,
                repo_id=repo_id,
                node_id=cid,
            )
            if node is not None and getattr(node, "type", None) == "constraint":
                constraint_nodes.append(node)
        detector = ConflictDetector()
        reports = detector.detect_constraint_contradictions(
            tenant_id=tenant_id,
            repo_id=repo_id,
            nodes=constraint_nodes,
            plan_id=plan.id,
        )
        reports = enrich_conflict_reports_from_mediation(
            reports,
            mediation_report=knowledge_mediation_report,
        )
        detector.store_reports(
            tenant_id=tenant_id,
            repo_id=repo_id,
            plan_id=plan.id,
            reports=reports,
            code_memory=code_memory,
        )
    for item in ctx.get("code_memory_items") or []:
        if isinstance(item, dict):
            item_id = item.get("item_id")
            if isinstance(item_id, str) and item_id.strip():
                retrieval_item_ids.append(item_id.strip())
    plan = _set_step_outputs(
        plan=plan,
        step_id=step_id,
        outputs_patch={
            "retrieval_snapshot": {
                "source": "compile_retriever",
                "query": goal,
                "top_k": 20,
                "item_ids": sorted(set(retrieval_item_ids)),
                "provenance": list(retrieval_provenance),
                "replayed_from_manifest": replayed_ctx is not None,
            },
            "knowledge_snapshot": knowledge_snapshot.to_json_obj(),
            "knowledge_intent_assertion_ids": sorted(intent_assertion_map.keys()),
            "knowledge_mediation_report": dict(knowledge_mediation_report),
            "knowledge_mediation_artifact_relpath": KNOWLEDGE_MEDIATION_RELPATH
            if knowledge_mediation_fingerprint is not None
            else None,
            "knowledge_mediation_fingerprint": knowledge_mediation_fingerprint,
            "knowledge_semantic_fingerprint": knowledge_semantic_fingerprint_16,
            "knowledge_provenance_fingerprint": knowledge_provenance_fingerprint_16,
        },
    )
    plan_store.save_plan(tenant_id=tenant_id, repo_id=repo_id, plan=plan)

    gen_tier = config.tier_for_stage(stage="generate")
    # Tests-by-default: run tests for every candidate evaluation.
    # Prefer explicit config, but remain backward-compatible with older metadata keys.
    smoke_command = list(
        config.test_command
        if config.test_command is not None
        else (config.metadata or {}).get("execute_command") or ["python", "-m", "pytest", "-q"]
    )
    smoke_timeout_s_raw = (
        config.test_timeout_s if config.test_timeout_s is not None else (config.metadata or {}).get("execute_timeout_s")
    )
    smoke_timeout_s_f = float(smoke_timeout_s_raw) if smoke_timeout_s_raw is not None else None

    full_command_raw = (config.metadata or {}).get("full_test_command")
    full_command = (
        list(full_command_raw)
        if isinstance(full_command_raw, (list, tuple)) and full_command_raw
        else _derive_full_test_command(smoke_command)
    )
    full_timeout_s_raw = (config.metadata or {}).get("full_test_timeout_s")
    full_timeout_s_f = float(full_timeout_s_raw) if full_timeout_s_raw is not None else smoke_timeout_s_f

    smoke_stage_name = "tests_smoke"
    full_stage_name = "tests_full"
    policy_metadata = dict(config.metadata or {})

    smoke_timeout_s_effective, smoke_stage_decision = project_stage_timeout_s(
        stage_timeout_s=smoke_timeout_s_f,
        intent_max_seconds=effective_max_wall_time_s,
    )
    full_timeout_s_effective, full_stage_decision = project_stage_timeout_s(
        stage_timeout_s=full_timeout_s_f,
        intent_max_seconds=effective_max_wall_time_s,
    )

    stage_timeout_decisions: list[dict[str, Any]] = []
    if smoke_stage_decision is not None:
        d = dict(smoke_stage_decision)
        d["stage"] = smoke_stage_name
        stage_timeout_decisions.append(d)
    if full_stage_decision is not None:
        d = dict(full_stage_decision)
        d["stage"] = full_stage_name
        stage_timeout_decisions.append(d)
    if stage_timeout_decisions:
        proj = accounting.get("policy_operating_bounds_projection")
        if isinstance(proj, dict):
            proj["stage_timeout_decisions"] = stage_timeout_decisions

    smoke_policy_context = dict(
        _build_executor_policy_context(
            executor=executor,
            stage=smoke_stage_name,
            command=list(smoke_command),
            timeout_s=smoke_timeout_s_effective,
            replay_mode=replay_mode,
            policy_mode=config.policy_mode,
            metadata=policy_metadata,
        ),
        **intent_policy_context,
    )
    full_policy_context = dict(
        _build_executor_policy_context(
            executor=executor,
            stage=full_stage_name,
            command=list(full_command),
            timeout_s=full_timeout_s_effective,
            replay_mode=replay_mode,
            policy_mode=config.policy_mode,
            metadata=policy_metadata,
        ),
        **intent_policy_context,
    )

    def _wall_budget_ok() -> bool:
        if effective_max_wall_time_s is None:
            return True
        return (time.monotonic() - start) <= float(effective_max_wall_time_s)

    def _token_budget_ok() -> bool:
        if effective_max_input_tokens is not None and int(accounting["input_tokens"]) >= effective_max_input_tokens:
            return False
        if effective_max_output_tokens is not None and int(accounting["output_tokens"]) >= effective_max_output_tokens:
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

    intent_mandatory_partial_replay_passes = mandatory_partial_replay_passes_for_success_criteria(
        success_criteria=resolved_intent.spec.success_criteria
    )
    current_stable_intent_sha256 = resolved_intent.stable_intent_sha256

    return run_budgeted_generate_execute_repair_loop(
        tenant_id=tenant_id,
        repo_id=repo_id,
        plan_store=plan_store,
        scope=scope,
        plan=plan,
        step_id=step_id,
        code_memory=code_memory,
        config=config,
        ctx=ctx,
        intent_spec=intent_spec,
        intent_id=intent_id,
        intent_contract=intent_contract,
        intent_fingerprint=intent_fingerprint,
        bounds_projection=bounds_projection,
        goal=goal,
        active_objectives=active_objectives,
        linked_constraints=linked_constraints,
        active_success_criteria=active_success_criteria,
        knowledge_semantic_fingerprint=knowledge_semantic_fingerprint_16,
        knowledge_provenance_fingerprint=knowledge_provenance_fingerprint_16,
        policy_llm=policy_llm,
        policy_executor=policy_executor,
        exec_base_capability=exec_base_capability,
        policy_engine=effective_policy_engine,
        replay_mode=replay_mode,
        replay_manifest=replay_manifest,
        intent_mandatory_partial_replay_passes=intent_mandatory_partial_replay_passes,
        current_stable_intent_sha256=current_stable_intent_sha256,
        verifier=verifier,
        gen_tier=gen_tier,
        smoke_command=smoke_command,
        full_command=full_command,
        smoke_stage_name=smoke_stage_name,
        full_stage_name=full_stage_name,
        smoke_timeout_s_effective=smoke_timeout_s_effective,
        full_timeout_s_effective=full_timeout_s_effective,
        smoke_policy_context=smoke_policy_context,
        full_policy_context=full_policy_context,
        effective_max_wall_time_s=effective_max_wall_time_s,
        effective_max_steps=effective_max_steps,
        effective_max_input_tokens=effective_max_input_tokens,
        effective_max_output_tokens=effective_max_output_tokens,
        budget=budget,
        start=start,
        accounting=accounting,
        append_span=_append_span,
        best_initial=best,
        compile_succeeded_seen_initial=compile_succeeded_seen,
        intent_satisfied_initial=intent_satisfied,
        step_span_id=step_span_id,
        run_span_id=run_span_id,
        step_started_ns=step_started_ns,
        run_started_ns=run_started_ns,
        resolved_intent_context=resolved_intent,
        intent_store=intent_store,
        intent_contract_shape=intent_contract_shape,
    )
