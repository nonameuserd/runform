"""Phase 4: reconciler desired state from IR fingerprints vs legacy deployment_intents."""

from __future__ import annotations

from dataclasses import replace

import pytest

from akc.ir.schema import IRDocument, IRNode
from akc.runtime.models import (
    RuntimeBundle,
    RuntimeBundleRef,
    RuntimeContext,
    RuntimeNodeRef,
    validate_runtime_ir_bundle_alignment,
)
from akc.runtime.reconciler import (
    DeploymentReconciler,
    ir_projection_deployment_intents,
    validate_strict_deployment_intents_align_with_ir,
)
from akc.utils.fingerprint import stable_json_fingerprint


def _ir_bundle(*, use_ir_source: bool) -> tuple[RuntimeBundle, IRNode]:
    context = RuntimeContext(
        tenant_id="tenant-a",
        repo_id="repo-a",
        run_id="compile-1",
        runtime_run_id="runtime-1",
        policy_mode="enforce",
        adapter_id="native",
    )
    node = IRNode(
        id="svc-api",
        tenant_id="tenant-a",
        kind="service",
        name="api",
        properties={"tier": "web"},
        depends_on=(),
    )
    ir = IRDocument(
        tenant_id="tenant-a",
        repo_id="repo-a",
        nodes=(node,),
    )
    meta: dict[str, object] = {
        "deployment_intents": [
            {
                "node_id": "svc-api",
                "kind": "service",
                "name": "api",
                "depends_on": [],
                "effects": None,
                "contract_id": "c1",
            }
        ],
    }
    if use_ir_source:
        meta["reconcile_desired_state_source"] = "ir"
    return (
        RuntimeBundle(
            context=context,
            ref=RuntimeBundleRef(
                bundle_path=".akc/runtime/compile-1.runtime_bundle.json",
                manifest_hash="a" * 64,
                created_at=1,
                source_compile_run_id="compile-1",
            ),
            nodes=(RuntimeNodeRef(node_id="svc-api", kind="service", contract_id="c1"),),
            contract_ids=("c1",),
            metadata=meta,
            ir_document=ir,
        ),
        node,
    )


def test_build_desired_state_legacy_uses_denormalized_intent_hash() -> None:
    bundle, _node = _ir_bundle(use_ir_source=False)
    reconciler = DeploymentReconciler()
    raw_intent = bundle.metadata["deployment_intents"][0]
    expected = stable_json_fingerprint(dict(raw_intent))
    desired = reconciler.build_desired_state(bundle=bundle)
    assert desired["svc-api"]["desired_hash"] == expected
    assert desired["svc-api"]["desired_state_source"] == "deployment_intents"


def test_build_desired_state_from_ir_uses_node_fingerprint() -> None:
    bundle, node = _ir_bundle(use_ir_source=True)
    reconciler = DeploymentReconciler()
    desired = reconciler.build_desired_state(bundle=bundle)
    assert desired["svc-api"]["desired_hash"] == node.fingerprint()
    assert desired["svc-api"]["desired_state_source"] == "ir"
    assert desired["svc-api"]["intent"] == node.to_json_obj()


def test_build_desired_state_from_ir_direct() -> None:
    bundle, node = _ir_bundle(use_ir_source=True)
    reconciler = DeploymentReconciler()
    assert bundle.ir_document is not None
    desired = reconciler.build_desired_state_from_ir(ir=bundle.ir_document, bundle=bundle)
    assert desired["svc-api"]["desired_hash"] == node.fingerprint()


def test_runtime_bundle_rejects_ir_tenant_repo_mismatch() -> None:
    bundle, _node = _ir_bundle(use_ir_source=True)
    assert bundle.ir_document is not None
    n0 = bundle.ir_document.nodes[0]
    bad_node = replace(n0, tenant_id="tenant-b")
    bad_ir = IRDocument(
        tenant_id="tenant-b",
        repo_id=bundle.context.repo_id,
        nodes=(bad_node,),
    )
    with pytest.raises(ValueError, match="tenant isolation"):
        RuntimeBundle(
            context=bundle.context,
            ref=bundle.ref,
            nodes=bundle.nodes,
            contract_ids=bundle.contract_ids,
            metadata=bundle.metadata,
            ir_document=bad_ir,
        )


def test_build_desired_state_ir_only_enumerates_deployables_without_deployment_intents_rows() -> None:
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
    reconciler = DeploymentReconciler()
    desired = reconciler.build_desired_state_from_ir(ir=ir, bundle=bundle)
    assert set(desired.keys()) == {"svc-api", "svc-worker"}
    assert desired["svc-api"]["desired_hash"] == api.fingerprint()


def test_strict_deployment_intents_alignment_raises_on_ir_drift() -> None:
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
            "deployment_intents_ir_alignment": "strict",
        },
        ir_document=ir,
    )
    with pytest.raises(ValueError, match="deployment_intents_ir_alignment=strict"):
        validate_strict_deployment_intents_align_with_ir(ir=ir, bundle=bundle)


def test_ir_projection_matches_bundle_deployment_intents_under_strict() -> None:
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
    base = RuntimeBundle(
        context=context,
        ref=RuntimeBundleRef(
            bundle_path=".akc/runtime/compile-1.runtime_bundle.json",
            manifest_hash="a" * 64,
            created_at=1,
            source_compile_run_id="compile-1",
        ),
        nodes=(RuntimeNodeRef(node_id="svc-api", kind="service", contract_id="c1"),),
        contract_ids=("c1",),
        metadata={
            "referenced_ir_nodes": [{"id": "svc-api"}],
            "deployment_intents": [],
        },
        ir_document=ir,
    )
    projection = ir_projection_deployment_intents(ir=ir, bundle=base)
    bundle = RuntimeBundle(
        context=context,
        ref=base.ref,
        nodes=base.nodes,
        contract_ids=base.contract_ids,
        metadata={
            **dict(base.metadata),
            "deployment_intents": projection,
            "deployment_intents_ir_alignment": "strict",
        },
        ir_document=ir,
    )
    validate_strict_deployment_intents_align_with_ir(ir=ir, bundle=bundle)


def test_validate_runtime_ir_bundle_alignment_rejects_repo_mismatch() -> None:
    bundle, _node = _ir_bundle(use_ir_source=True)
    assert bundle.ir_document is not None
    bad_ir = IRDocument(
        tenant_id=bundle.context.tenant_id,
        repo_id="other-repo",
        nodes=bundle.ir_document.nodes,
    )
    with pytest.raises(ValueError, match="repo_id"):
        validate_runtime_ir_bundle_alignment(ir=bad_ir, bundle=bundle)
