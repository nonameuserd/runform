from __future__ import annotations

import pytest

from akc.ir import EffectAnnotation, IRDocument, IRNode, ProvenancePointer, diff_ir, stable_node_id


def _sample_ir() -> IRDocument:
    node_a = IRNode(
        id=stable_node_id(kind="service", name="billing"),
        tenant_id="tenant-a",
        kind="service",
        name="billing",
        properties={"language": "python", "runtime": "uvicorn"},
        depends_on=(),
        effects=EffectAnnotation(network=True, tools=("openapi.fetch",)),
        provenance=(
            ProvenancePointer(
                tenant_id="tenant-a",
                kind="doc_chunk",
                source_id="docs/architecture.md#billing",
                locator="L20-L39",
            ),
        ),
    )
    node_b = IRNode(
        id=stable_node_id(kind="workflow", name="invoice_generation"),
        tenant_id="tenant-a",
        kind="workflow",
        name="invoice_generation",
        properties={"trigger": "cron"},
        depends_on=(node_a.id,),
    )
    return IRDocument(tenant_id="tenant-a", repo_id="repo-a", nodes=(node_b, node_a))


def test_ir_fingerprint_stable_across_node_order() -> None:
    ir1 = _sample_ir()
    ir2 = IRDocument(
        tenant_id=ir1.tenant_id,
        repo_id=ir1.repo_id,
        nodes=tuple(reversed(ir1.nodes)),
    )

    assert ir1.to_json_obj() == ir2.to_json_obj()
    assert ir1.fingerprint() == ir2.fingerprint()


def test_ir_diff_detects_changed_node() -> None:
    before = _sample_ir()
    changed_node = IRNode(
        id=before.nodes[0].id,
        tenant_id="tenant-a",
        kind=before.nodes[0].kind,
        name=before.nodes[0].name,
        properties={"trigger": "hourly"},
        depends_on=before.nodes[0].depends_on,
    )
    after = IRDocument(
        tenant_id=before.tenant_id,
        repo_id=before.repo_id,
        nodes=(changed_node, before.nodes[1]),
    )

    d = diff_ir(before=before, after=after)
    assert d.added == ()
    assert d.removed == ()
    assert d.changed == (changed_node.id,)


def test_ir_node_rejects_cross_tenant_provenance_pointer() -> None:
    with pytest.raises(ValueError, match="provenance pointers must match ir_node.tenant_id"):
        IRNode(
            id=stable_node_id(kind="service", name="payments"),
            tenant_id="tenant-a",
            kind="service",
            name="payments",
            properties={},
            provenance=(
                ProvenancePointer(
                    tenant_id="tenant-b",
                    kind="doc_chunk",
                    source_id="docs/architecture.md#payments",
                ),
            ),
        )


def test_ir_node_rejects_unknown_kind() -> None:
    with pytest.raises(ValueError, match="ir_node.kind must be one of"):
        IRNode(
            id=stable_node_id(kind="service", name="billing"),
            tenant_id="tenant-a",
            kind="unknown",  # type: ignore[arg-type]
            name="billing",
            properties={},
        )
