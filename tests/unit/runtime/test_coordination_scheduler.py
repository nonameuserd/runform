from __future__ import annotations

import pytest

from akc.coordination.models import (
    CoordinationGraph,
    CoordinationGraphEdge,
    CoordinationGraphNode,
    _lower_edges_for_scheduling,
)
from akc.runtime.coordination.models import (
    CoordinationCycleError,
    CoordinationParseError,
    CoordinationReservedEdgeRequiresSpecV2,
    CoordinationScheduleLayer,
    CoordinationUnsupportedEdgeKind,
    ParsedCoordinationSpec,
    parse_coordination_obj,
    schedule_coordination_layers,
)


def _minimal_spec(*, edges: list[dict[str, str]], nodes: list[dict[str, str]] | None = None) -> ParsedCoordinationSpec:
    base_nodes = nodes or []
    obj = {
        "spec_version": 1,
        "run_id": "r1",
        "tenant_id": "t1",
        "repo_id": "p1",
        "coordination_graph": {
            "nodes": base_nodes,
            "edges": edges,
        },
        "orchestration_bindings": [],
        "governance": {"max_steps": 10, "allowed_capabilities": ["x"], "execution_allow_network": False},
    }
    return parse_coordination_obj(obj)


def test_depends_on_linear_ordering() -> None:
    spec = _minimal_spec(
        nodes=[
            {"node_id": "workflow_000", "kind": "step"},
            {"node_id": "workflow_001", "kind": "step"},
        ],
        edges=[
            {"edge_id": "e0", "kind": "depends_on", "src_step_id": "workflow_000", "dst_step_id": "workflow_001"},
        ],
    )
    sched = schedule_coordination_layers(spec)
    assert [layer.step_ids for layer in sched.layers] == [("workflow_000",), ("workflow_001",)]
    assert sched.step_order == ("workflow_000", "workflow_001")


def test_parallel_roots_sorted_ids() -> None:
    spec = _minimal_spec(
        nodes=[
            {"node_id": "workflow_001", "kind": "step"},
            {"node_id": "workflow_000", "kind": "step"},
        ],
        edges=[],
    )
    sched = schedule_coordination_layers(spec)
    assert len(sched.layers) == 1
    assert sched.layers[0] == CoordinationScheduleLayer(layer_index=0, step_ids=("workflow_000", "workflow_001"))


def test_self_loop_single_step() -> None:
    spec = _minimal_spec(
        nodes=[{"node_id": "workflow_000", "kind": "step"}],
        edges=[
            {"edge_id": "e0", "kind": "depends_on", "src_step_id": "workflow_000", "dst_step_id": "workflow_000"},
        ],
    )
    sched = schedule_coordination_layers(spec)
    assert sched.layers == (CoordinationScheduleLayer(layer_index=0, step_ids=("workflow_000",)),)


def test_cycle_raises() -> None:
    spec = _minimal_spec(
        nodes=[
            {"node_id": "a", "kind": "step"},
            {"node_id": "b", "kind": "step"},
        ],
        edges=[
            {"edge_id": "e0", "kind": "depends_on", "src_step_id": "a", "dst_step_id": "b"},
            {"edge_id": "e1", "kind": "depends_on", "src_step_id": "b", "dst_step_id": "a"},
        ],
    )
    with pytest.raises(CoordinationCycleError):
        schedule_coordination_layers(spec)


def test_reserved_parallel_kind_raises_on_v1() -> None:
    spec = _minimal_spec(
        nodes=[{"node_id": "workflow_000", "kind": "step"}],
        edges=[
            {"edge_id": "e0", "kind": "parallel", "src_step_id": "workflow_000", "dst_step_id": "workflow_000"},
        ],
    )
    with pytest.raises(CoordinationReservedEdgeRequiresSpecV2):
        schedule_coordination_layers(spec)


def test_v2_parallel_self_loop_schedules_single_layer() -> None:
    obj = {
        "spec_version": 2,
        "run_id": "r1",
        "tenant_id": "t1",
        "repo_id": "p1",
        "coordination_graph": {
            "nodes": [{"node_id": "workflow_000", "kind": "step"}],
            "edges": [
                {"edge_id": "e0", "kind": "parallel", "src_step_id": "workflow_000", "dst_step_id": "workflow_000"},
            ],
        },
        "orchestration_bindings": [],
        "governance": {"max_steps": 10, "allowed_capabilities": ["x"], "execution_allow_network": False},
    }
    v2 = parse_coordination_obj(obj)
    sched = schedule_coordination_layers(v2)
    assert sched.layers == (CoordinationScheduleLayer(layer_index=0, step_ids=("workflow_000",)),)


def test_v2_handoff_chain_matches_depends_on() -> None:
    nodes = [
        {"node_id": "a", "kind": "step"},
        {"node_id": "b", "kind": "step"},
        {"node_id": "c", "kind": "step"},
    ]
    base = {
        "spec_version": 2,
        "run_id": "r1",
        "tenant_id": "t1",
        "repo_id": "p1",
        "coordination_graph": {"nodes": nodes, "edges": []},
        "orchestration_bindings": [],
        "governance": {"max_steps": 10, "allowed_capabilities": ["x"], "execution_allow_network": False},
    }
    dep = parse_coordination_obj(
        {
            **base,
            "spec_version": 1,
            "coordination_graph": {
                "nodes": nodes,
                "edges": [
                    {"edge_id": "e1", "kind": "depends_on", "src_step_id": "a", "dst_step_id": "b"},
                    {"edge_id": "e2", "kind": "depends_on", "src_step_id": "b", "dst_step_id": "c"},
                ],
            },
        }
    )
    handoff = parse_coordination_obj(
        {
            **base,
            "coordination_graph": {
                "nodes": nodes,
                "edges": [
                    {
                        "edge_id": "e1",
                        "kind": "handoff",
                        "src_step_id": "a",
                        "dst_step_id": "b",
                        "metadata": {"handoff_id": "h_ab"},
                    },
                    {
                        "edge_id": "e2",
                        "kind": "delegate",
                        "src_step_id": "b",
                        "dst_step_id": "c",
                        "metadata": {"delegate_target": "stub_worker"},
                    },
                ],
            },
        }
    )
    assert [layer.step_ids for layer in schedule_coordination_layers(dep).layers] == [
        layer.step_ids for layer in schedule_coordination_layers(handoff).layers
    ]


def test_unknown_edge_kind_on_v2_raises() -> None:
    obj = {
        "spec_version": 2,
        "run_id": "r1",
        "tenant_id": "t1",
        "repo_id": "p1",
        "coordination_graph": {
            "nodes": [{"node_id": "a", "kind": "step"}],
            "edges": [
                {"edge_id": "e0", "kind": "custom_edge", "src_step_id": "a", "dst_step_id": "a"},
            ],
        },
        "orchestration_bindings": [],
        "governance": {"max_steps": 10, "allowed_capabilities": ["x"], "execution_allow_network": False},
    }
    spec = parse_coordination_obj(obj)
    with pytest.raises(CoordinationUnsupportedEdgeKind, match="custom_edge"):
        schedule_coordination_layers(spec)


def test_v2_fork_join_layers_match_depends_on_fixture_shape() -> None:
    """Fork (parallel) + join (barrier) lowers to the same layers as explicit depends_on."""

    nodes = [
        {"node_id": "branch_a", "kind": "step"},
        {"node_id": "branch_b", "kind": "step"},
        {"node_id": "fork", "kind": "step"},
        {"node_id": "join", "kind": "step"},
    ]
    base = {
        "spec_version": 2,
        "run_id": "r1",
        "tenant_id": "t1",
        "repo_id": "p1",
        "coordination_graph": {"nodes": nodes, "edges": []},
        "orchestration_bindings": [],
        "governance": {"max_steps": 10, "allowed_capabilities": ["x"], "execution_allow_network": False},
    }
    v2 = parse_coordination_obj(
        {
            **base,
            "coordination_graph": {
                "nodes": nodes,
                "edges": [
                    {"edge_id": "e_fork_a", "kind": "parallel", "src_step_id": "fork", "dst_step_id": "branch_a"},
                    {"edge_id": "e_fork_b", "kind": "parallel", "src_step_id": "fork", "dst_step_id": "branch_b"},
                    {"edge_id": "e_a_join", "kind": "barrier", "src_step_id": "branch_a", "dst_step_id": "join"},
                    {"edge_id": "e_b_join", "kind": "barrier", "src_step_id": "branch_b", "dst_step_id": "join"},
                ],
            },
        }
    )
    v1_equiv = parse_coordination_obj(
        {
            **base,
            "spec_version": 1,
            "coordination_graph": {
                "nodes": nodes,
                "edges": [
                    {"edge_id": "d1", "kind": "depends_on", "src_step_id": "fork", "dst_step_id": "branch_a"},
                    {"edge_id": "d2", "kind": "depends_on", "src_step_id": "fork", "dst_step_id": "branch_b"},
                    {"edge_id": "d3", "kind": "depends_on", "src_step_id": "branch_a", "dst_step_id": "join"},
                    {"edge_id": "d4", "kind": "depends_on", "src_step_id": "branch_b", "dst_step_id": "join"},
                ],
            },
        }
    )
    assert [layer.step_ids for layer in schedule_coordination_layers(v2).layers] == [
        layer.step_ids for layer in schedule_coordination_layers(v1_equiv).layers
    ]
    assert [layer.step_ids for layer in schedule_coordination_layers(v2).layers] == [
        ("fork",),
        ("branch_a", "branch_b"),
        ("join",),
    ]


def test_v2_cycle_from_mixed_lowered_kinds_raises() -> None:
    spec = parse_coordination_obj(
        {
            "spec_version": 2,
            "run_id": "r1",
            "tenant_id": "t1",
            "repo_id": "p1",
            "coordination_graph": {
                "nodes": [
                    {"node_id": "a", "kind": "step"},
                    {"node_id": "b", "kind": "step"},
                ],
                "edges": [
                    {"edge_id": "e0", "kind": "parallel", "src_step_id": "a", "dst_step_id": "b"},
                    {
                        "edge_id": "e1",
                        "kind": "handoff",
                        "src_step_id": "b",
                        "dst_step_id": "a",
                        "metadata": {"handoff_id": "h_ba"},
                    },
                ],
            },
            "orchestration_bindings": [],
            "governance": {"max_steps": 10, "allowed_capabilities": ["x"], "execution_allow_network": False},
        }
    )
    with pytest.raises(CoordinationCycleError, match="cycle"):
        schedule_coordination_layers(spec)


def test_v2_delegate_missing_delegate_target_raises_stable_message() -> None:
    spec = parse_coordination_obj(
        {
            "spec_version": 2,
            "run_id": "r1",
            "tenant_id": "t1",
            "repo_id": "p1",
            "coordination_graph": {
                "nodes": [{"node_id": "a", "kind": "step"}, {"node_id": "b", "kind": "step"}],
                "edges": [
                    {
                        "edge_id": "e_del",
                        "kind": "delegate",
                        "src_step_id": "a",
                        "dst_step_id": "b",
                        "metadata": {},
                    },
                ],
            },
            "orchestration_bindings": [],
            "governance": {"max_steps": 10, "allowed_capabilities": ["x"], "execution_allow_network": False},
        }
    )
    with pytest.raises(CoordinationParseError, match=r"edge 'e_del' \(delegate\).*delegate_target"):
        schedule_coordination_layers(spec)


def test_v2_handoff_missing_handoff_id_raises_stable_message() -> None:
    spec = parse_coordination_obj(
        {
            "spec_version": 2,
            "run_id": "r1",
            "tenant_id": "t1",
            "repo_id": "p1",
            "coordination_graph": {
                "nodes": [{"node_id": "a", "kind": "step"}, {"node_id": "b", "kind": "step"}],
                "edges": [
                    {
                        "edge_id": "e1",
                        "kind": "handoff",
                        "src_step_id": "a",
                        "dst_step_id": "b",
                        "metadata": {},
                    },
                ],
            },
            "orchestration_bindings": [],
            "governance": {"max_steps": 10, "allowed_capabilities": ["x"], "execution_allow_network": False},
        }
    )
    with pytest.raises(CoordinationParseError, match=r"edge 'e1' \(handoff\).*handoff_id"):
        schedule_coordination_layers(spec)


def test_lower_edges_for_scheduling_order_is_deterministic() -> None:
    g = CoordinationGraph(
        nodes=(
            CoordinationGraphNode(node_id="a", kind="step"),
            CoordinationGraphNode(node_id="b", kind="step"),
            CoordinationGraphNode(node_id="c", kind="step"),
        ),
        edges=(
            CoordinationGraphEdge(edge_id="z", kind="parallel", src_step_id="a", dst_step_id="c"),
            CoordinationGraphEdge(edge_id="m", kind="parallel", src_step_id="a", dst_step_id="b"),
        ),
    )
    lowered = _lower_edges_for_scheduling(graph=g, spec_version=2)
    assert [x.from_edge_id for x in lowered] == ["m", "z"]


def test_schedule_includes_provenance_jsonable() -> None:
    spec = parse_coordination_obj(
        {
            "spec_version": 2,
            "run_id": "r1",
            "tenant_id": "t1",
            "repo_id": "p1",
            "coordination_graph": {
                "nodes": [{"node_id": "a", "kind": "step"}, {"node_id": "b", "kind": "step"}],
                "edges": [
                    {"edge_id": "e1", "kind": "depends_on", "src_step_id": "a", "dst_step_id": "b"},
                ],
            },
            "orchestration_bindings": [],
            "governance": {"max_steps": 10, "allowed_capabilities": ["x"], "execution_allow_network": False},
        }
    )
    sched = schedule_coordination_layers(spec)
    assert sched.layer_reason == ("kahn_layer:0", "kahn_layer:1")
    assert sched.lowered_precedence_edges == (
        {
            "src_step_id": "a",
            "dst_step_id": "b",
            "lowered_from_edge_ids": ["e1"],
            "original_kinds": ["depends_on"],
        },
    )


def test_parse_binds_graph() -> None:
    obj = {
        "spec_version": 1,
        "run_id": "r1",
        "tenant_id": "t1",
        "repo_id": "p1",
        "coordination_graph": {
            "nodes": [{"node_id": "workflow_000", "kind": "step", "label": "s"}],
            "edges": [],
        },
        "orchestration_bindings": [
            {"role_name": "planner", "agent_name": "a", "orchestration_step_ids": ["workflow_000"]}
        ],
        "governance": {"max_steps": 1, "allowed_capabilities": ["llm.complete"], "execution_allow_network": False},
    }
    spec = parse_coordination_obj(obj)
    assert spec.orchestration_bindings[0].orchestration_step_ids == ("workflow_000",)
    assert isinstance(spec.graph, CoordinationGraph)
