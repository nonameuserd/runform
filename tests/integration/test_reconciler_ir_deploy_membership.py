"""Phase D: IR-only deployable membership + strict deployment_intents alignment."""

from __future__ import annotations

import pytest

from akc.ir.schema import IRDocument, IRNode
from akc.runtime.models import RuntimeBundle, RuntimeBundleRef, RuntimeContext, RuntimeNodeRef
from akc.runtime.reconciler import DeploymentReconciler, InMemoryDeploymentProviderClient


def test_reconcile_ir_only_mode_builds_desired_state_without_deployment_intent_rows() -> None:
    context = RuntimeContext(
        tenant_id="tenant-a",
        repo_id="repo-a",
        run_id="compile-1",
        runtime_run_id="runtime-1",
        policy_mode="enforce",
        adapter_id="native",
    )
    api = IRNode(
        id="svc-api",
        tenant_id="tenant-a",
        kind="service",
        name="api",
        properties={},
        depends_on=(),
    )
    worker = IRNode(
        id="svc-worker",
        tenant_id="tenant-a",
        kind="service",
        name="worker",
        properties={},
        depends_on=(),
    )
    ir = IRDocument(
        tenant_id="tenant-a",
        repo_id="repo-a",
        nodes=(api, worker),
    )
    bundle = RuntimeBundle(
        context=context,
        ref=RuntimeBundleRef(
            bundle_path=".akc/runtime/compile-1.runtime_bundle.json",
            manifest_hash="a" * 64,
            created_at=1,
            source_compile_run_id="compile-1",
        ),
        nodes=(
            RuntimeNodeRef(node_id="svc-api", kind="service", contract_id="unknown"),
            RuntimeNodeRef(node_id="svc-worker", kind="service", contract_id="unknown"),
        ),
        contract_ids=(),
        metadata={
            "referenced_ir_nodes": [{"id": "svc-api"}, {"id": "svc-worker"}],
            "deployment_intents": [],
            "reconcile_desired_state_source": "ir",
            "reconcile_deploy_targets_from_ir_only": True,
        },
        ir_document=ir,
    )
    reconciler = DeploymentReconciler(mode="enforce", provider=InMemoryDeploymentProviderClient())
    statuses, _ = reconciler.reconcile_with_evidence(bundle=bundle)
    assert len(statuses) == 2
    assert {s.resource_id for s in statuses} == {"svc-api", "svc-worker"}


def test_reconcile_strict_alignment_raises_when_deployment_intents_empty() -> None:
    context = RuntimeContext(
        tenant_id="tenant-a",
        repo_id="repo-a",
        run_id="compile-1",
        runtime_run_id="runtime-1",
        policy_mode="enforce",
        adapter_id="native",
    )
    api = IRNode(
        id="svc-api",
        tenant_id="tenant-a",
        kind="service",
        name="api",
        properties={},
        depends_on=(),
    )
    ir = IRDocument(
        tenant_id="tenant-a",
        repo_id="repo-a",
        nodes=(api,),
    )
    bundle = RuntimeBundle(
        context=context,
        ref=RuntimeBundleRef(
            bundle_path=".akc/runtime/compile-1.runtime_bundle.json",
            manifest_hash="a" * 64,
            created_at=1,
            source_compile_run_id="compile-1",
        ),
        nodes=(RuntimeNodeRef(node_id="svc-api", kind="service", contract_id="unknown"),),
        contract_ids=(),
        metadata={
            "referenced_ir_nodes": [{"id": "svc-api"}],
            "deployment_intents": [],
            "reconcile_desired_state_source": "ir",
            "deployment_intents_ir_alignment": "strict",
        },
        ir_document=ir,
    )
    reconciler = DeploymentReconciler(mode="simulate", provider=InMemoryDeploymentProviderClient())
    with pytest.raises(ValueError, match="deployment_intents_ir_alignment=strict"):
        reconciler.reconcile_with_evidence(bundle=bundle)


def test_reconcile_strict_alignment_raises_when_deployment_intent_row_shape_drifts() -> None:
    """Strict mode compares full JSON projection; a wrong ``name`` fails closed."""
    context = RuntimeContext(
        tenant_id="tenant-a",
        repo_id="repo-a",
        run_id="compile-1",
        runtime_run_id="runtime-1",
        policy_mode="enforce",
        adapter_id="native",
    )
    api = IRNode(
        id="svc-api",
        tenant_id="tenant-a",
        kind="service",
        name="api",
        properties={},
        depends_on=(),
    )
    ir = IRDocument(
        tenant_id="tenant-a",
        repo_id="repo-a",
        nodes=(api,),
    )
    bundle = RuntimeBundle(
        context=context,
        ref=RuntimeBundleRef(
            bundle_path=".akc/runtime/compile-1.runtime_bundle.json",
            manifest_hash="a" * 64,
            created_at=1,
            source_compile_run_id="compile-1",
        ),
        nodes=(RuntimeNodeRef(node_id="svc-api", kind="service", contract_id="unknown"),),
        contract_ids=(),
        metadata={
            "referenced_ir_nodes": [{"id": "svc-api"}],
            "deployment_intents": [
                {
                    "node_id": "svc-api",
                    "kind": "service",
                    "name": "wrong-name",
                    "depends_on": [],
                    "effects": None,
                    "contract_id": None,
                }
            ],
            "reconcile_desired_state_source": "ir",
            "deployment_intents_ir_alignment": "strict",
        },
        ir_document=ir,
    )
    reconciler = DeploymentReconciler(mode="simulate", provider=InMemoryDeploymentProviderClient())
    with pytest.raises(ValueError, match="deployment_intents_ir_alignment=strict"):
        reconciler.reconcile_with_evidence(bundle=bundle)
