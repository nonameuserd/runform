from __future__ import annotations

import json
import time
from hashlib import sha256
from typing import Any, Literal

from akc.action.approvals import is_approved
from akc.action.models import ActionExecutionRecordV1, ActionIntentV1, ActionPlanV1
from akc.action.policy import ActionPolicyContext, evaluate_action_policy, has_user_consent
from akc.action.provider_registry import (
    ActionProviderCompensationContext,
    ActionProviderExecutionContext,
    ProviderRegistry,
)
from akc.action.store import ActionStore
from akc.runtime.models import RuntimeAction, RuntimeCheckpoint, RuntimeContext, RuntimeNodeRef
from akc.runtime.scheduler import InMemoryRuntimeScheduler
from akc.runtime.state_store import FileSystemRuntimeStateStore

_MUTATING_ACTION_PREFIXES: tuple[str, ...] = (
    "action.call.",
    "action.message.",
    "action.calendar.write",
    "action.flight.book",
)
_MAX_RETRY_ATTEMPTS = 3
_RETRY_BASE_DELAY_MS = 500
_RETRY_JITTER_CEILING_MS = 200
_RETRY_MAX_DELAY_MS = 5_000


def _is_mutating_action(action_type: str) -> bool:
    action = str(action_type).strip()
    return action.startswith(_MUTATING_ACTION_PREFIXES)


def _require_same_scope(*, intent: ActionIntentV1, scope: dict[str, str]) -> None:
    if scope.get("tenant_id") != intent.tenant_id:
        raise ValueError("provider scope tenant_id mismatch")
    if scope.get("repo_id") != intent.repo_id:
        raise ValueError("provider scope repo_id mismatch")


def _build_runtime_context(intent: ActionIntentV1) -> RuntimeContext:
    return RuntimeContext(
        tenant_id=intent.tenant_id,
        repo_id=intent.repo_id,
        run_id=intent.intent_id,
        runtime_run_id="action",
        policy_mode="enforce",
        adapter_id="action_plane",
    )


def _runtime_action_for_step(
    *,
    step_id: str,
    action_type: str,
    idempotency_key: str,
    inputs: dict[str, Any],
) -> RuntimeAction:
    return RuntimeAction(
        action_id=step_id,
        action_type=action_type,
        node_ref=RuntimeNodeRef(node_id=step_id, kind="action_step", contract_id="action.v1"),
        inputs_fingerprint=sha256(json.dumps(inputs, sort_keys=True).encode("utf-8")).hexdigest(),
        idempotency_key=idempotency_key,
    )


def _load_or_init_checkpoint(
    *,
    state_store: FileSystemRuntimeStateStore,
    scheduler: InMemoryRuntimeScheduler,
    context: RuntimeContext,
    plan: ActionPlanV1,
) -> RuntimeCheckpoint:
    checkpoint = state_store.load_checkpoint(context=context)
    if checkpoint is not None:
        for action in checkpoint.pending_queue:
            scheduler.enqueue(
                context=context,
                action=action,
                enqueue_ts=int(time.time() * 1000),
                node_class="action_step",
            )
        return checkpoint
    queue: list[RuntimeAction] = []
    now_ms = int(time.time() * 1000)
    for step in plan.steps:
        runtime_action = _runtime_action_for_step(
            step_id=step.step_id,
            action_type=step.action_type,
            idempotency_key=step.idempotency_key,
            inputs=step.inputs,
        )
        queue.append(runtime_action)
        scheduler.enqueue(context=context, action=runtime_action, enqueue_ts=now_ms, node_class="action_step")
    checkpoint = RuntimeCheckpoint(
        checkpoint_id="action_init",
        cursor="step:0",
        pending_queue=tuple(queue),
        node_states={},
        replay_token=context.run_id,
    )
    state_store.save_checkpoint(context=context, checkpoint=checkpoint)
    state_store.save_queue_snapshot(context=context, snapshot=scheduler.snapshot(context=context))
    return checkpoint


def _save_runtime_progress(
    *,
    state_store: FileSystemRuntimeStateStore,
    scheduler: InMemoryRuntimeScheduler,
    context: RuntimeContext,
    cursor: str,
    replay_token: str,
) -> None:
    pending = scheduler.pending(context=context)
    checkpoint = RuntimeCheckpoint(
        checkpoint_id=f"action_{cursor}",
        cursor=cursor,
        pending_queue=pending,
        node_states={},
        replay_token=replay_token,
    )
    state_store.save_checkpoint(context=context, checkpoint=checkpoint)
    state_store.save_queue_snapshot(context=context, snapshot=scheduler.snapshot(context=context))


def execute_plan(
    *,
    intent: ActionIntentV1,
    plan: ActionPlanV1,
    store: ActionStore,
    providers: ProviderRegistry,
    mode: str = "live",
) -> dict[str, Any]:
    status = "completed"
    step_results: list[dict[str, Any]] = []
    policy_decisions: list[dict[str, Any]] = []
    executed_steps: list[tuple[str, str]] = []
    compensations: list[dict[str, Any]] = []
    store.bind_intent_scope(
        intent_id=intent.intent_id,
        tenant_id=intent.tenant_id,
        repo_id=intent.repo_id,
    )
    action_dir = store.action_dir(intent_id=intent.intent_id)
    consent_root = store.consent_root(tenant_id=intent.tenant_id, repo_id=intent.repo_id)
    step_by_id = {step.step_id: step for step in plan.steps}
    runtime_context = _build_runtime_context(intent)
    scheduler = InMemoryRuntimeScheduler(
        max_attempts=_MAX_RETRY_ATTEMPTS,
        retry_base_delay_ms=_RETRY_BASE_DELAY_MS,
        retry_jitter_ceiling_ms=_RETRY_JITTER_CEILING_MS,
        retry_max_delay_ms=_RETRY_MAX_DELAY_MS,
    )
    runtime_root = store.workspace_root()
    state_store = FileSystemRuntimeStateStore(root=runtime_root)
    _ = _load_or_init_checkpoint(state_store=state_store, scheduler=scheduler, context=runtime_context, plan=plan)
    idle_polls = 0
    while True:
        runtime_action = scheduler.dequeue(
            context=runtime_context,
            now_ms=int(time.time() * 1000),
            max_in_flight=1,
            max_in_flight_per_node_class=1,
        )
        if runtime_action is None:
            if not scheduler.pending(context=runtime_context):
                break
            idle_polls += 1
            if idle_polls > 400:
                status = "failed"
                step_results.append(
                    {
                        "step_id": "runtime_scheduler",
                        "status": "failed",
                        "reason": "scheduler_stalled",
                        "narrative": "Runtime scheduler did not make progress before timeout.",
                        "remediation_suggestions": [
                            "Replay the intent to resume from latest checkpoint.",
                            "Inspect provider health and pending runtime queue snapshot.",
                        ],
                    }
                )
                break
            time.sleep(0.05)
            continue
        idle_polls = 0
        step = step_by_id.get(runtime_action.action_id)
        if step is None:
            scheduler.ack(context=runtime_context, action=runtime_action, node_class="action_step")
            _save_runtime_progress(
                state_store=state_store,
                scheduler=scheduler,
                context=runtime_context,
                cursor=f"step:{runtime_action.action_id}",
                replay_token=intent.intent_id,
            )
            continue
        started_at_ms = int(time.time() * 1000)
        step_approved = is_approved(action_dir=action_dir, step_id=step.step_id)
        consent_present = has_user_consent(
            consent_root=consent_root,
            actor_id=intent.actor_id,
            action=step.action_type,
        )
        policy_decision = evaluate_action_policy(
            policy_ctx=ActionPolicyContext(
                action=step.action_type,
                risk_tier=step.risk_tier,
                intent_id=intent.intent_id,
                step_id=step.step_id,
                actor_id=intent.actor_id,
                channel=intent.channel,
                tenant_id=intent.tenant_id,
                repo_id=intent.repo_id,
                user_has_consent=consent_present,
                step_is_approved=step_approved,
            ),
            consent_root=consent_root,
        )
        policy_decisions.append(policy_decision)
        if not bool(policy_decision.get("allowed", False)):
            reason = str(policy_decision.get("reason", "policy.deny"))
            is_approval_block = reason == "policy.action.approval_required"
            status = "pending_approval" if is_approval_block else "failed"
            step_results.append(
                {
                    "step_id": step.step_id,
                    "status": "pending_approval" if is_approval_block else "denied",
                    "reason": reason,
                    "narrative": str(policy_decision.get("narrative", "")),
                    "policy_decision_ref": f"decision:{intent.intent_id}:{step.step_id}:policy",
                }
            )
            if is_approval_block:
                scheduler.ack(context=runtime_context, action=runtime_action, node_class="action_step")
                _save_runtime_progress(
                    state_store=state_store,
                    scheduler=scheduler,
                    context=runtime_context,
                    cursor=f"step:{step.step_id}:approval",
                    replay_token=intent.intent_id,
                )
            else:
                scheduler.dead_letter(
                    context=runtime_context,
                    action=runtime_action,
                    node_class="action_step",
                    reason="policy_denied",
                    error=reason,
                    now_ms=int(time.time() * 1000),
                )
                _save_runtime_progress(
                    state_store=state_store,
                    scheduler=scheduler,
                    context=runtime_context,
                    cursor=f"step:{step.step_id}:denied",
                    replay_token=intent.intent_id,
                )
            break
        if mode == "simulate":
            payload: dict[str, object] = {"simulated": True, "inputs": step.inputs}
            external_ids: tuple[str, ...] = ()
            step_status = "simulated"
            scheduler.ack(context=runtime_context, action=runtime_action, node_class="action_step")
        else:
            provider = providers.get(step.provider)
            try:
                if _is_mutating_action(step.action_type) and not step.idempotency_key.strip():
                    raise ValueError("mutating action step requires idempotency_key")
                scope = {"tenant_id": intent.tenant_id, "repo_id": intent.repo_id}
                provider.preflight(scope)
                _require_same_scope(intent=intent, scope=scope)
                exec_mode: Literal["live", "simulate"] = "simulate" if mode == "simulate" else "live"
                res = provider.execute(
                    step,
                    ActionProviderExecutionContext(
                        intent_id=intent.intent_id,
                        tenant_id=intent.tenant_id,
                        repo_id=intent.repo_id,
                        idempotency_key=step.idempotency_key,
                        mode=exec_mode,
                    ),
                )
                payload = res.payload
                external_ids = (str(res.external_id),) if res.external_id else ()
                step_status = "succeeded" if res.status == "ok" else "failed"
                scheduler.ack(context=runtime_context, action=runtime_action, node_class="action_step")
                if step_status == "failed":
                    status = "failed"
            except Exception as exc:  # pragma: no cover - error branch is exercised by tests with custom providers
                error_kind = provider.classify_error(exc)
                is_retriable = error_kind == "retriable_transport"
                if is_retriable:
                    should_retry = scheduler.retry(
                        context=runtime_context,
                        action=runtime_action,
                        node_class="action_step",
                        reason="backend_error",
                        error=str(exc),
                        now_ms=int(time.time() * 1000),
                    )
                    if should_retry:
                        _save_runtime_progress(
                            state_store=state_store,
                            scheduler=scheduler,
                            context=runtime_context,
                            cursor=f"step:{step.step_id}:retry",
                            replay_token=intent.intent_id,
                        )
                        continue
                else:
                    scheduler.dead_letter(
                        context=runtime_context,
                        action=runtime_action,
                        node_class="action_step",
                        reason="backend_error",
                        error=str(exc),
                        now_ms=int(time.time() * 1000),
                    )
                status = "failed"
                payload = {
                    "error": str(exc),
                    "classification": error_kind,
                    "remediation_suggestions": (
                        [
                            "Check provider credentials and tenant/repo scope.",
                            "Validate business preconditions for this action step.",
                            "Replay after correcting inputs or policy approvals.",
                            "Consider operator override or plan amendment for non-reversible steps.",
                        ]
                        if error_kind != "retriable_transport"
                        else ["Transient transport failure detected; retried within bounded backoff budget."]
                    ),
                }
                external_ids = ()
                step_status = "failed"

        request_digest = sha256(json.dumps(step.inputs, sort_keys=True).encode("utf-8")).hexdigest()
        response_digest = sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()
        attempt = store.next_attempt(intent_id=intent.intent_id, step_id=step.step_id)
        rec = ActionExecutionRecordV1(
            schema_kind="action_execution_record",
            schema_version=1,
            intent_id=intent.intent_id,
            step_id=step.step_id,
            status=step_status,
            attempt=attempt,
            provider=step.provider,
            decision_token_refs=(f"decision:{intent.intent_id}:{step.step_id}:{attempt}",),
            request_digest=request_digest,
            response_digest=response_digest,
            external_ids=external_ids,
            started_at_ms=started_at_ms,
            completed_at_ms=int(time.time() * 1000),
        )
        store.append_execution(rec)
        step_results.append(
            {
                "step_id": step.step_id,
                "status": step_status,
                "provider": step.provider,
                "payload": payload,
                "policy_decision_ref": f"decision:{intent.intent_id}:{step.step_id}:policy",
            }
        )
        _save_runtime_progress(
            state_store=state_store,
            scheduler=scheduler,
            context=runtime_context,
            cursor=f"step:{step.step_id}:{step_status}",
            replay_token=intent.intent_id,
        )
        if step_status == "succeeded":
            executed_steps.append((step.step_id, step.provider))
        if step_status == "failed":
            for succeeded_step_id, provider_name in reversed(executed_steps):
                prior_step = step_by_id[succeeded_step_id]
                comp_provider = providers.get(provider_name)
                declared_mode = str(prior_step.compensation.get("mode", "manual"))
                supports_reversal = comp_provider.compensation_support(prior_step) == "reversal"
                if declared_mode in {"reversal", "provider_reversal"} and supports_reversal:
                    try:
                        comp_res = comp_provider.compensate(
                            prior_step,
                            ActionProviderCompensationContext(
                                intent_id=intent.intent_id,
                                tenant_id=intent.tenant_id,
                                repo_id=intent.repo_id,
                                failed_step_id=step.step_id,
                            ),
                        )
                        compensations.append(
                            {
                                "step_id": succeeded_step_id,
                                "status": "completed" if comp_res.status == "ok" else "failed",
                                "mode": declared_mode,
                                "provider": provider_name,
                                "payload": comp_res.payload,
                            }
                        )
                    except Exception as comp_exc:  # pragma: no cover
                        compensations.append(
                            {
                                "step_id": succeeded_step_id,
                                "status": "failed",
                                "mode": declared_mode,
                                "provider": provider_name,
                                "payload": {"error": str(comp_exc)},
                            }
                        )
                else:
                    compensations.append(
                        {
                            "step_id": succeeded_step_id,
                            "status": "manual_compensations_required",
                            "mode": "manual",
                            "provider": provider_name,
                            "payload": {
                                "note": (
                                    "Provider does not advertise safe reversal for this step; "
                                    "manual compensation required."
                                )
                            },
                        }
                    )
            break
    store.write_policy_decisions(intent_id=intent.intent_id, decisions=policy_decisions)
    result: dict[str, Any] = {
        "intent_id": intent.intent_id,
        "status": status,
        "steps": step_results,
        "mode": mode,
        "compensations": compensations,
    }
    store.write_result(intent_id=intent.intent_id, result=result)
    return result
