"""Unit tests for deterministic workflow ordering used by coordination emit and step resolution."""

from __future__ import annotations

from akc.ir import IRDocument, IRNode
from akc.ir.workflow_order import sorted_workflow_nodes_for_coordination_emit, workflow_coordination_layer_key


def test_workflow_coordination_layer_key_order_idx() -> None:
    assert workflow_coordination_layer_key({"order_idx": 2}) == "o:000000002"


def test_workflow_coordination_layer_key_parallel_group_overrides_order() -> None:
    assert workflow_coordination_layer_key({"order_idx": 9, "coordination_parallel_group": "alpha"}) == "g:alpha"


def test_sorted_workflow_nodes_tie_break_by_id_same_layer() -> None:
    doc = IRDocument(
        tenant_id="t",
        repo_id="r",
        nodes=(
            IRNode(id="z", tenant_id="t", kind="workflow", name="z", properties={"order_idx": 0}),
            IRNode(id="a", tenant_id="t", kind="workflow", name="a", properties={"order_idx": 0}),
        ),
    )
    ordered = sorted_workflow_nodes_for_coordination_emit(doc.nodes)
    assert [n.id for n in ordered] == ["a", "z"]


def test_coordination_parallel_group_sorts_before_order_layers() -> None:
    """Lexicographic layer keys: ``g:`` < ``o:`` so explicit groups sort before numeric order_idx layers."""

    doc = IRDocument(
        tenant_id="t",
        repo_id="r",
        nodes=(
            IRNode(
                id="late-order",
                tenant_id="t",
                kind="workflow",
                name="x",
                properties={"order_idx": 99},
            ),
            IRNode(
                id="group-a",
                tenant_id="t",
                kind="workflow",
                name="y",
                properties={"order_idx": 0, "coordination_parallel_group": "early"},
            ),
        ),
    )
    ordered = sorted_workflow_nodes_for_coordination_emit(doc.nodes)
    assert [n.id for n in ordered] == ["group-a", "late-order"]
