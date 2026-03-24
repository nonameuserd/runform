from __future__ import annotations

import pytest

from akc.control.policy import CapabilityIssuer, DefaultDenyPolicyEngine, ToolAuthorizationPolicy
from akc.runtime.adapters import RuntimeAdapterRegistry
from akc.runtime.adapters.native import NativeRuntimeAdapter
from akc.runtime.models import RuntimeBundle, RuntimeBundleRef, RuntimeContext, RuntimeNodeRef
from akc.runtime.policy import (
    RuntimePolicyRuntime,
    RuntimeScopeMismatchError,
    ensure_runtime_context_match,
)
from akc.runtime.reconciler import DeploymentReconciler


def _context(*, tenant_id: str = "tenant-a", repo_id: str = "repo-a") -> RuntimeContext:
    return RuntimeContext(
        tenant_id=tenant_id,
        repo_id=repo_id,
        run_id="compile-1",
        runtime_run_id="runtime-1",
        policy_mode="enforce",
        adapter_id="native",
    )


def _bundle() -> RuntimeBundle:
    context = _context()
    return RuntimeBundle(
        context=context,
        ref=RuntimeBundleRef(
            bundle_path=".akc/runtime/runtime_bundle.json",
            manifest_hash="a" * 64,
            created_at=1,
            source_compile_run_id="compile-1",
        ),
        nodes=(RuntimeNodeRef(node_id="svc-api", kind="service", contract_id="contract-1"),),
        contract_ids=("contract-1",),
        metadata={
            "deployment_intents": [
                {
                    "node_id": "svc-api",
                    "kind": "service",
                    "name": "api",
                    "depends_on": [],
                    "effects": None,
                    "contract_id": "contract-1",
                }
            ]
        },
    )


def test_runtime_context_enforcement_rejects_cross_tenant_access() -> None:
    with pytest.raises(RuntimeScopeMismatchError, match="scope mismatch"):
        ensure_runtime_context_match(expected=_context(), actual=_context(tenant_id="tenant-b"))


def test_unscoped_adapter_execution_is_denied() -> None:
    registry = RuntimeAdapterRegistry()
    registry.register(adapter_id="native", factory=NativeRuntimeAdapter)

    with pytest.raises(PermissionError, match="not allowed by policy"):
        registry.create(
            adapter_id="native",
            context=_context(),
            policy_allowlist=("hybrid",),
        )


def test_reconcile_apply_is_denied_without_capability_grant() -> None:
    context = _context()
    engine = DefaultDenyPolicyEngine(
        issuer=CapabilityIssuer(),
        policy=ToolAuthorizationPolicy(
            mode="enforce",
            allow_actions=("runtime.event.consume",),
        ),
    )
    policy_runtime = RuntimePolicyRuntime(
        context=context,
        policy_engine=engine,
        issuer=engine.issuer,
        decision_log=[],
    )
    reconciler = DeploymentReconciler(mode="enforce", policy_runtime=policy_runtime)

    with pytest.raises(PermissionError, match="service.reconcile.apply"):
        reconciler.reconcile_with_evidence(bundle=_bundle())
