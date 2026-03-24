from __future__ import annotations

from typing import Any, Literal

from akc.compile.controller_config import ControllerConfig
from akc.compile.interfaces import Executor, LLMBackend, TenantRepoScope
from akc.compile.verifier import DeterministicVerifier
from akc.control.policy import (
    CapabilityAttenuator,
    CapabilityIssuer,
    DefaultDenyPolicyEngine,
    PolicyEngine,
    PolicyWrappedExecutor,
    PolicyWrappedLLMBackend,
    SubprocessOpaEvaluator,
    ToolAuthorizationPolicy,
)
from akc.runtime.policy import RUNTIME_POLICY_ACTIONS

OperationalValidityFailedTriggerSeverity = Literal["block", "advisory"]

# Explicit capability action for compile working-tree mutation (scoped apply).
COMPILE_PATCH_APPLY_ACTION = "compile.patch.apply"


def setup_policy_runtime(
    *,
    config: ControllerConfig,
    llm: LLMBackend,
    executor: Executor,
    policy_engine: PolicyEngine | None,
    scope: TenantRepoScope,
    plan_id: str,
    step_id: str,
    accounting: dict[str, Any],
) -> tuple[DeterministicVerifier, PolicyWrappedLLMBackend, PolicyWrappedExecutor, Any, PolicyEngine]:
    """Create verifier + policy-wrapped LLM/executor.

    This centralizes policy/runtime wiring so `controller.py` stays focused on
    orchestration and state threading.
    """

    verifier = DeterministicVerifier()
    effective_policy_engine = policy_engine or DefaultDenyPolicyEngine(
        issuer=CapabilityIssuer(),
        policy=ToolAuthorizationPolicy(
            mode=config.policy_mode,
            allow_actions=tuple(config.tool_allowlist),
            opa=(
                SubprocessOpaEvaluator(
                    policy_path=config.opa_policy_path,
                    decision_path=config.opa_decision_path,
                )
                if config.opa_policy_path is not None
                else None
            ),
        ),
    )

    def _observe_policy_decision(
        action: str,
        token: Any,
        decision: Any,
        context: dict[str, Any] | None,
    ) -> None:
        # Persist a stable, manifest-friendly decision shape for auditing.
        accounting["policy_decisions"].append(
            {
                "action": action,
                "scope": {
                    "tenant_id": scope.tenant_id,
                    "repo_id": scope.repo_id,
                },
                "token_id": str(getattr(token, "token_id", "")),
                "constraints": dict(getattr(token, "constraints", {}) or {}),
                "context": dict(context or {}),
                "allowed": bool(getattr(decision, "allowed", False)),
                "reason": str(getattr(decision, "reason", "")),
                "source": str(getattr(decision, "source", "")),
                "mode": str(getattr(decision, "mode", "")),
                "block": bool(getattr(decision, "block", False)),
            }
        )

    policy_llm = PolicyWrappedLLMBackend(
        backend=llm,
        policy_engine=effective_policy_engine,
        issuer=effective_policy_engine.issuer,
        decision_observer=_observe_policy_decision,
    )
    attenuator = CapabilityAttenuator()
    exec_base_capability = effective_policy_engine.issuer.issue(
        scope=scope,
        action="executor.run",
        constraints={"plan_id": plan_id, "step_id": step_id},
    )
    policy_executor = PolicyWrappedExecutor(
        executor=executor,
        policy_engine=effective_policy_engine,
        issuer=effective_policy_engine.issuer,
        attenuator=attenuator,
        decision_observer=_observe_policy_decision,
    )

    return verifier, policy_llm, policy_executor, exec_base_capability, effective_policy_engine


def compile_policy_actions() -> tuple[str, ...]:
    """Policy action names used by the compile loop (mutation and tool calls)."""

    return (COMPILE_PATCH_APPLY_ACTION,)


def runtime_policy_actions() -> tuple[str, ...]:
    return RUNTIME_POLICY_ACTIONS


def operational_validity_failed_trigger_severity(
    *, config: ControllerConfig
) -> OperationalValidityFailedTriggerSeverity:
    """Return the runtime/living severity tier for `operational_validity_failed`.

    `block` preserves legacy trigger emission; `advisory` keeps the failure
    visible in reports and fleet reads but suppresses recompile triggers.
    """

    return config.operational_validity_failed_trigger_severity
