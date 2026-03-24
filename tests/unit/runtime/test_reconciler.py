from __future__ import annotations

from typing import NoReturn

from akc.runtime.models import ObservedHealthCondition, RuntimeBundle, RuntimeBundleRef, RuntimeContext, RuntimeNodeRef
from akc.runtime.reconciler import (
    DeploymentReconciler,
    InMemoryDeploymentProviderClient,
    ObservedResource,
    ReconcileOperation,
    health_gate_passes,
)
from akc.utils.fingerprint import stable_json_fingerprint


def _bundle(*, intent_hash_suffix: str = "1") -> RuntimeBundle:
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
            bundle_path=".akc/runtime/compile-1.runtime_bundle.json",
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
                    "name": f"api-{intent_hash_suffix}",
                    "depends_on": [],
                    "effects": None,
                    "contract_id": "contract-1",
                }
            ]
        },
    )


def test_reconciler_simulate_produces_diff_without_writes() -> None:
    provider = InMemoryDeploymentProviderClient()
    reconciler = DeploymentReconciler(mode="simulate", provider=provider)

    statuses, evidence = reconciler.reconcile_with_evidence(bundle=_bundle())

    assert len(statuses) == 1
    assert statuses[0].converged is False
    assert evidence[0].operations[0].applied is False
    assert provider.observed == {}


def test_reconciler_enforce_with_observe_only_never_calls_apply() -> None:
    class ObserveOnly(InMemoryDeploymentProviderClient):
        observe_only = True

        def apply(self, *, operation: ReconcileOperation) -> NoReturn:  # type: ignore[override]
            raise AssertionError(f"apply must not run for observe_only providers (op={operation.operation_id})")

    provider = ObserveOnly()
    reconciler = DeploymentReconciler(mode="enforce", provider=provider)

    statuses, evidence = reconciler.reconcile_with_evidence(bundle=_bundle())

    assert len(statuses) == 1
    assert statuses[0].converged is False
    assert evidence[0].operations[0].applied is False
    assert evidence[0].operations[0].evidence.get("observe_only_provider") is True
    assert provider.observed == {}


def test_reconciler_enforce_skips_rollback_when_observe_only() -> None:
    class StaleObserve(InMemoryDeploymentProviderClient):
        observe_only = True

        def observe(self, *, resource_id: str, resource_class: str):  # type: ignore[override]
            return ObservedResource(
                resource_id=resource_id,
                resource_class=resource_class,
                observed_hash="stale-hash",
                health_status="healthy",
            )

    reconciler = DeploymentReconciler(mode="enforce", provider=StaleObserve())
    reconciler.desired_hash_history["svc-api"] = ["prior-hash"]
    statuses, evidence = reconciler.reconcile_with_evidence(bundle=_bundle(intent_hash_suffix="9"))

    assert statuses[0].converged is False
    assert evidence[0].rollback_triggered is False


def test_reconciler_enforce_applies_create_and_converges() -> None:
    provider = InMemoryDeploymentProviderClient()
    reconciler = DeploymentReconciler(mode="enforce", provider=provider)

    statuses, evidence = reconciler.reconcile_with_evidence(bundle=_bundle())

    assert statuses[0].converged is True
    assert statuses[0].health_status == "healthy"
    assert statuses[0].hash_matched is True
    assert statuses[0].health_gate_passed is True
    assert evidence[0].operations[0].operation.operation_type == "create"
    assert evidence[0].resync_attempt == 1
    assert "svc-api" in provider.observed


def test_reconciler_canary_holds_resources_past_canary_limit() -> None:
    provider = InMemoryDeploymentProviderClient()
    bundle = RuntimeBundle(
        context=_bundle().context,
        ref=_bundle().ref,
        nodes=(
            RuntimeNodeRef(node_id="svc-a", kind="service", contract_id="contract-1"),
            RuntimeNodeRef(node_id="svc-b", kind="service", contract_id="contract-1"),
        ),
        contract_ids=("contract-1",),
        metadata={
            "deployment_intents": [
                {
                    "node_id": "svc-a",
                    "kind": "service",
                    "name": "a",
                    "depends_on": [],
                    "effects": None,
                    "contract_id": "contract-1",
                },
                {
                    "node_id": "svc-b",
                    "kind": "service",
                    "name": "b",
                    "depends_on": [],
                    "effects": None,
                    "contract_id": "contract-1",
                },
            ]
        },
    )
    reconciler = DeploymentReconciler(mode="canary", provider=provider, canary_limit=1)

    statuses, evidence = reconciler.reconcile_with_evidence(bundle=bundle)

    assert len(statuses) == 2
    assert evidence[0].operations[0].operation.operation_type == "create"
    assert evidence[1].operations[0].operation.operation_type == "noop"


def _intent_dict(*, suffix: str = "1") -> dict[str, object]:
    return {
        "node_id": "svc-api",
        "kind": "service",
        "name": f"api-{suffix}",
        "depends_on": [],
        "effects": None,
        "contract_id": "contract-1",
    }


def _bundle_strict(*, grace_ms: int = 0) -> RuntimeBundle:
    b = _bundle()
    meta = dict(b.metadata)
    meta["reconcile_health_gate"] = "strict"
    meta["reconcile_health_unknown_grace_ms"] = grace_ms
    return RuntimeBundle(
        context=b.context,
        ref=b.ref,
        nodes=b.nodes,
        contract_ids=b.contract_ids,
        metadata=meta,
    )


def test_health_gate_strict_unknown_fails_without_grace() -> None:
    assert not health_gate_passes(
        gate="strict",
        health_status="unknown",
        observed_conditions=(),
        unknown_grace_ms=0,
        resync_elapsed_wait_ms=0,
    )


def test_health_gate_strict_unknown_passes_inside_grace() -> None:
    assert health_gate_passes(
        gate="strict",
        health_status="unknown",
        observed_conditions=(),
        unknown_grace_ms=60_000,
        resync_elapsed_wait_ms=0,
    )


def test_health_gate_strict_unknown_ready_condition() -> None:
    conds = (ObservedHealthCondition(type="Ready", status="true", reason="KubeletReady"),)
    assert health_gate_passes(
        gate="strict",
        health_status="unknown",
        observed_conditions=conds,
        unknown_grace_ms=0,
        resync_elapsed_wait_ms=0,
    )


def test_reconciler_strict_unknown_not_converged_when_hash_matches() -> None:
    desired_hash = stable_json_fingerprint(dict(_intent_dict()))
    provider = InMemoryDeploymentProviderClient(
        observed={
            "svc-api": ObservedResource(
                resource_id="svc-api",
                resource_class="service",
                observed_hash=desired_hash,
                health_status="unknown",
            )
        }
    )
    reconciler = DeploymentReconciler(mode="simulate", provider=provider)
    statuses, _ = reconciler.reconcile_with_evidence(bundle=_bundle_strict(grace_ms=0))
    assert statuses[0].hash_matched is True
    assert statuses[0].health_gate_passed is False
    assert statuses[0].converged is False
    assert statuses[0].reconcile_health_gate == "strict"


def test_reconciler_strict_unknown_converges_with_ready_row() -> None:
    desired_hash = stable_json_fingerprint(dict(_intent_dict()))
    provider = InMemoryDeploymentProviderClient(
        observed={
            "svc-api": ObservedResource(
                resource_id="svc-api",
                resource_class="service",
                observed_hash=desired_hash,
                health_status="unknown",
                health_conditions=(ObservedHealthCondition(type="Ready", status="true"),),
            )
        }
    )
    reconciler = DeploymentReconciler(mode="simulate", provider=provider)
    statuses, _ = reconciler.reconcile_with_evidence(bundle=_bundle_strict(grace_ms=0))
    assert statuses[0].converged is True


def test_reconciler_hash_match_with_unhealthy_probe_is_not_converged() -> None:
    class UnhealthyAfterMatch(InMemoryDeploymentProviderClient):
        def observe(self, *, resource_id: str, resource_class: str):  # type: ignore[override]
            observed = super().observe(resource_id=resource_id, resource_class=resource_class)
            if observed is None:
                return None
            return ObservedResource(
                resource_id=observed.resource_id,
                resource_class=observed.resource_class,
                observed_hash=observed.observed_hash,
                health_status="failed",
                payload=observed.payload,
            )

    reconciler = DeploymentReconciler(mode="enforce", provider=UnhealthyAfterMatch())
    statuses, _ = reconciler.reconcile_with_evidence(bundle=_bundle())
    assert statuses[0].hash_matched is True
    assert statuses[0].health_gate_passed is False
    assert statuses[0].converged is False


def test_reconciler_rolls_back_to_previous_desired_hash_on_failed_convergence() -> None:
    provider = InMemoryDeploymentProviderClient(
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

    class FailingProvider(InMemoryDeploymentProviderClient):
        def apply(self, *, operation):  # type: ignore[override]
            return super().apply(operation=operation)

        def observe(self, *, resource_id: str, resource_class: str):  # type: ignore[override]
            observed = super().observe(resource_id=resource_id, resource_class=resource_class)
            if observed is None:
                return None
            return ObservedResource(
                resource_id=observed.resource_id,
                resource_class=observed.resource_class,
                observed_hash="mismatched-after-apply",
                health_status="failed",
                payload=observed.payload,
            )

    failing_provider = FailingProvider(observed=provider.observed)
    reconciler.provider = failing_provider

    statuses, evidence = reconciler.reconcile_with_evidence(bundle=_bundle(intent_hash_suffix="2"))

    assert statuses[0].converged is False
    assert evidence[0].rollback_triggered is True
    assert evidence[0].rollback_target_hash == "old-hash"
