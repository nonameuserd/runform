from __future__ import annotations

from akc.runtime.models import RuntimeBundle, RuntimeBundleRef, RuntimeContext, RuntimeNodeRef
from akc.runtime.reconciler import (
    DeploymentReconciler,
    InMemoryDeploymentProviderClient,
    ObservedResource,
)


def _bundle(*resource_ids: str) -> RuntimeBundle:
    context = RuntimeContext(
        tenant_id="tenant-a",
        repo_id="repo-a",
        run_id="compile-1",
        runtime_run_id="runtime-1",
        policy_mode="enforce",
        adapter_id="native",
    )
    resources = resource_ids or ("svc-api",)
    return RuntimeBundle(
        context=context,
        ref=RuntimeBundleRef(
            bundle_path=".akc/runtime/runtime_bundle.json",
            manifest_hash="a" * 64,
            created_at=1,
            source_compile_run_id="compile-1",
        ),
        nodes=tuple(
            RuntimeNodeRef(node_id=resource_id, kind="service", contract_id="contract-1") for resource_id in resources
        ),
        contract_ids=("contract-1",),
        metadata={
            "deployment_intents": [
                {
                    "node_id": resource_id,
                    "kind": "service",
                    "name": resource_id,
                    "depends_on": [],
                    "effects": None,
                    "contract_id": "contract-1",
                }
                for resource_id in resources
            ]
        },
    )


def test_reconciler_build_plan_includes_delete_for_observed_only_resource() -> None:
    provider = InMemoryDeploymentProviderClient(
        observed={
            "svc-api": ObservedResource(
                resource_id="svc-api",
                resource_class="service",
                observed_hash="old-hash",
                health_status="healthy",
            ),
            "svc-stale": ObservedResource(
                resource_id="svc-stale",
                resource_class="service",
                observed_hash="stale-hash",
                health_status="healthy",
            ),
        }
    )
    reconciler = DeploymentReconciler(mode="enforce", provider=provider)

    plans = reconciler.build_plan(bundle=_bundle("svc-api"))

    by_resource = {plan.resource_id: plan for plan in plans}
    assert by_resource["svc-stale"].operations[0].operation_type == "delete"


def test_reconciler_build_plan_marks_matching_observed_resource_noop() -> None:
    bundle = _bundle("svc-api")
    provider = InMemoryDeploymentProviderClient()
    reconciler = DeploymentReconciler(mode="enforce", provider=provider)
    desired_state = reconciler.build_desired_state(bundle=bundle)
    desired_hash = str(desired_state["svc-api"]["desired_hash"])
    provider.observed["svc-api"] = ObservedResource(
        resource_id="svc-api",
        resource_class="service",
        observed_hash=desired_hash,
        health_status="healthy",
        payload=desired_state["svc-api"],
    )

    plans = reconciler.build_plan(bundle=bundle)

    assert len(plans) == 1
    assert plans[0].operations[0].operation_type == "noop"
