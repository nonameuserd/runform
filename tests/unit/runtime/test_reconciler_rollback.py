from __future__ import annotations

from akc.runtime.models import RuntimeBundle, RuntimeBundleRef, RuntimeContext, RuntimeNodeRef
from akc.runtime.reconciler import (
    DeploymentReconciler,
    InMemoryDeploymentProviderClient,
    ObservedResource,
)


def _bundle(name: str) -> RuntimeBundle:
    context = RuntimeContext(
        tenant_id="tenant-a",
        repo_id="repo-a",
        run_id="compile-1",
        runtime_run_id="runtime-1",
        policy_mode="enforce",
        adapter_id="native",
    )
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
                    "name": name,
                    "depends_on": [],
                    "effects": None,
                    "contract_id": "contract-1",
                }
            ]
        },
    )


class _FailingObserveProvider(InMemoryDeploymentProviderClient):
    def observe(self, *, resource_id: str, resource_class: str):  # type: ignore[override]
        observed = super().observe(resource_id=resource_id, resource_class=resource_class)
        if observed is None:
            return None
        return ObservedResource(
            resource_id=observed.resource_id,
            resource_class=observed.resource_class,
            observed_hash="drifted-hash",
            health_status="failed",
            payload=observed.payload,
        )


def test_reconciler_rolls_back_to_last_desired_hash_when_convergence_fails() -> None:
    provider = _FailingObserveProvider(
        observed={
            "svc-api": ObservedResource(
                resource_id="svc-api",
                resource_class="service",
                observed_hash="old-hash",
                health_status="healthy",
            )
        }
    )
    reconciler = DeploymentReconciler(mode="enforce", provider=provider)
    reconciler.desired_hash_history["svc-api"] = ["old-hash"]

    statuses, evidence = reconciler.reconcile_with_evidence(bundle=_bundle("api-v2"))

    assert statuses[0].converged is False
    assert evidence[0].rollback_triggered is True
    assert evidence[0].rollback_target_hash == "old-hash"
    assert provider.observed["svc-api"].observed_hash == "old-hash"


def test_reconciler_does_not_rollback_without_previous_desired_hash() -> None:
    provider = _FailingObserveProvider(
        observed={
            "svc-api": ObservedResource(
                resource_id="svc-api",
                resource_class="service",
                observed_hash="old-hash",
                health_status="healthy",
            )
        }
    )
    reconciler = DeploymentReconciler(mode="enforce", provider=provider)

    statuses, evidence = reconciler.reconcile_with_evidence(bundle=_bundle("api-v2"))

    assert statuses[0].converged is False
    assert evidence[0].rollback_triggered is False
