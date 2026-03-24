from __future__ import annotations

from akc.runtime.models import RuntimeBundle, RuntimeBundleRef, RuntimeContext, RuntimeNodeRef
from akc.runtime.reconciler import (
    DeploymentReconciler,
    InMemoryDeploymentProviderClient,
    ObservedResource,
)


def _bundle(name: str = "api") -> RuntimeBundle:
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


def test_runtime_reconciler_converges_from_create_to_noop() -> None:
    provider = InMemoryDeploymentProviderClient()
    reconciler = DeploymentReconciler(mode="enforce", provider=provider)

    first_statuses, _ = reconciler.reconcile_with_evidence(bundle=_bundle("api-v1"))
    second_statuses, second_evidence = reconciler.reconcile_with_evidence(bundle=_bundle("api-v1"))

    assert first_statuses[0].converged is True
    assert second_statuses[0].converged is True
    assert second_evidence[0].operations[0].operation.operation_type == "noop"


def test_runtime_reconciler_strict_gate_delete_absent_still_converges() -> None:
    provider = InMemoryDeploymentProviderClient(
        observed={
            "svc-stale": ObservedResource(
                resource_id="svc-stale",
                resource_class="service",
                observed_hash="stale-hash",
                health_status="healthy",
            )
        }
    )
    base = _bundle("api-v1")
    meta = dict(base.metadata)
    meta["reconcile_health_gate"] = "strict"
    meta["reconcile_health_unknown_grace_ms"] = 0
    bundle = RuntimeBundle(
        context=base.context,
        ref=base.ref,
        nodes=base.nodes,
        contract_ids=base.contract_ids,
        metadata=meta,
    )
    reconciler = DeploymentReconciler(mode="enforce", provider=provider)
    statuses, _ = reconciler.reconcile_with_evidence(bundle=bundle)
    by_resource = {status.resource_id: status for status in statuses}
    assert by_resource["svc-stale"].observed_hash == "absent"
    assert by_resource["svc-stale"].converged is True


def test_runtime_reconciler_deletes_observed_only_resource() -> None:
    provider = InMemoryDeploymentProviderClient(
        observed={
            "svc-stale": ObservedResource(
                resource_id="svc-stale",
                resource_class="service",
                observed_hash="stale-hash",
                health_status="healthy",
            )
        }
    )
    reconciler = DeploymentReconciler(mode="enforce", provider=provider)

    statuses, evidence = reconciler.reconcile_with_evidence(bundle=_bundle("api-v1"))

    by_resource = {status.resource_id: status for status in statuses}
    stale_evidence = next(item for item in evidence if item.resource_id == "svc-stale")
    assert by_resource["svc-stale"].observed_hash == "absent"
    assert stale_evidence.operations[0].operation.operation_type == "delete"
