from __future__ import annotations

from collections.abc import Mapping

from akc.runtime.models import RuntimeBundle, RuntimeBundleRef, RuntimeContext, RuntimeNodeRef
from akc.runtime.reconciler import (
    DeploymentReconciler,
    InMemoryDeploymentProviderClient,
    ObservedResource,
    ProviderOperationResult,
)


def _bundle(
    *,
    resource_ids: tuple[str, ...],
    strategy: Mapping,
    enrich_targets: bool = False,
) -> RuntimeBundle:
    context = RuntimeContext(
        tenant_id="tenant-a",
        repo_id="repo-a",
        run_id="compile-1",
        runtime_run_id="runtime-1",
        policy_mode="enforce",
        adapter_id="native",
    )
    nodes = tuple(RuntimeNodeRef(node_id=rid, kind="service", contract_id="contract-1") for rid in resource_ids)
    return RuntimeBundle(
        context=context,
        ref=RuntimeBundleRef(
            bundle_path=".akc/runtime/pg_canary_bundle.json",
            manifest_hash="a" * 64,
            created_at=1,
            source_compile_run_id="compile-1",
        ),
        nodes=nodes,
        contract_ids=("contract-1",),
        metadata={
            "deployment_intents": [
                {
                    "node_id": rid,
                    "kind": "service",
                    "name": f"api-{rid}",
                    "depends_on": [],
                    "effects": None,
                    "contract_id": "contract-1",
                    **(
                        {
                            "target_class": "backend_service",
                            "environment_support": ["local", "staging", "production"],
                            "delivery_paths": {
                                "local": ["direct_apply"],
                                "staging": ["direct_apply", "workflow_handoff"],
                                "production": ["gitops_handoff", "workflow_handoff"],
                            },
                            "operational_profile_fingerprint": f"fp-{rid}",
                        }
                        if enrich_targets
                        else {}
                    ),
                }
                for rid in resource_ids
            ],
            "reconcile_canary_strategy": dict(strategy),
        },
    )


class _FailingApplyProvider(InMemoryDeploymentProviderClient):
    def __init__(self, *, observed: Mapping[str, ObservedResource], failing_ids: tuple[str, ...]) -> None:
        super().__init__(observed=dict(observed))
        self.failing_ids = set(failing_ids)
        self.apply_calls: list[str] = []

    def apply(self, *, operation):  # type: ignore[override]
        payload = dict(operation.payload)
        resource_id = str(payload.get("resource_id", operation.target)).strip()
        self.apply_calls.append(resource_id)
        if resource_id in self.failing_ids and operation.operation_type in {"create", "update"}:
            resource_class = str(payload.get("resource_class", "service")).strip() or "service"
            # Force drift after apply: hash mismatch + failed health.
            self.observed[resource_id] = ObservedResource(
                resource_id=resource_id,
                resource_class=resource_class,
                observed_hash="drifted-hash",
                health_status="failed",
                payload=payload,
                health_conditions=(),
            )
            return ProviderOperationResult(
                operation=operation,
                applied=True,
                observed_hash="drifted-hash",
                health_status="failed",
                error=None,
                evidence={"forced_fail": True},
            )
        return super().apply(operation=operation)


def test_progressive_canary_abort_failure_triggers_rollback_and_prevents_full_promotion() -> None:
    resource_ids = ("svc-a", "svc-b", "svc-c")
    old_hash = "old-hash"
    observed = {
        rid: ObservedResource(
            resource_id=rid,
            resource_class="service",
            observed_hash=old_hash,
            health_status="healthy",
            payload={},
            health_conditions=(),
        )
        for rid in resource_ids
    }
    provider = _FailingApplyProvider(observed=observed, failing_ids=("svc-a",))

    reconciler = DeploymentReconciler(
        mode="canary",
        provider=provider,
        canary_limit=1,
    )
    # Ensure rollback history is deterministic.
    reconciler.desired_hash_history["svc-a"] = [old_hash]
    reconciler.desired_hash_history["svc-b"] = [old_hash]
    reconciler.desired_hash_history["svc-c"] = [old_hash]

    statuses, evidence = reconciler.reconcile_with_evidence(
        bundle=_bundle(resource_ids=resource_ids, strategy={"enabled": True}),
    )

    assert [s.resource_id for s in statuses] == ["svc-a", "svc-b", "svc-c"]

    # Failure should abort before promoting canary to full.
    assert provider.apply_calls == ["svc-a"]

    # Canary failing resource should roll back to the prior successful desired hash.
    assert evidence[0].rollback_triggered is True
    assert evidence[0].rollback_target_hash == old_hash
    assert provider.observed["svc-a"].observed_hash == old_hash
    assert provider.observed["svc-a"].health_status == "healthy"

    # Held resources should never be mutated on canary abort.
    assert evidence[1].operations[0].operation.operation_type == "noop"
    assert evidence[2].operations[0].operation.operation_type == "noop"
    assert provider.observed["svc-b"].observed_hash == old_hash
    assert provider.observed["svc-c"].observed_hash == old_hash


def test_progressive_canary_with_enriched_delivery_plan_style_intents_same_abort_semantics() -> None:
    resource_ids = ("svc-a", "svc-b", "svc-c")
    old_hash = "old-hash"
    observed = {
        rid: ObservedResource(
            resource_id=rid,
            resource_class="service",
            observed_hash=old_hash,
            health_status="healthy",
            payload={},
            health_conditions=(),
        )
        for rid in resource_ids
    }
    provider = _FailingApplyProvider(observed=observed, failing_ids=("svc-a",))
    reconciler = DeploymentReconciler(mode="canary", provider=provider, canary_limit=1)
    reconciler.desired_hash_history["svc-a"] = [old_hash]
    reconciler.desired_hash_history["svc-b"] = [old_hash]
    reconciler.desired_hash_history["svc-c"] = [old_hash]

    statuses, evidence = reconciler.reconcile_with_evidence(
        bundle=_bundle(resource_ids=resource_ids, strategy={"enabled": True}, enrich_targets=True),
    )

    assert [s.resource_id for s in statuses] == ["svc-a", "svc-b", "svc-c"]
    assert provider.apply_calls == ["svc-a"]
    assert evidence[0].rollback_triggered is True
    assert evidence[0].rollback_target_hash == old_hash


def test_progressive_canary_failure_rollback_success_rate_ge_95() -> None:
    resource_ids = ("svc-a", "svc-b", "svc-c")
    old_hash = "old-hash"
    trials = 20
    successes = 0
    for _ in range(trials):
        observed = {
            rid: ObservedResource(
                resource_id=rid,
                resource_class="service",
                observed_hash=old_hash,
                health_status="healthy",
                payload={},
                health_conditions=(),
            )
            for rid in resource_ids
        }
        provider = _FailingApplyProvider(observed=observed, failing_ids=("svc-a",))
        reconciler = DeploymentReconciler(mode="canary", provider=provider, canary_limit=1)
        reconciler.desired_hash_history["svc-a"] = [old_hash]

        reconciler.reconcile_with_evidence(bundle=_bundle(resource_ids=resource_ids, strategy={"enabled": True}))
        obs = provider.observed["svc-a"]
        if obs.observed_hash == old_hash and obs.health_status == "healthy":
            successes += 1

    assert successes / trials >= 0.95
