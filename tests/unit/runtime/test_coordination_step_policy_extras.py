from __future__ import annotations

import pytest

from akc.coordination.models import parse_coordination_obj
from akc.runtime.coordination.step_policy_extras import (
    coordination_step_policy_extras,
    normalize_agent_output_sha256_hex,
)


def test_handoff_predecessor_missing_raises() -> None:
    obj = parse_coordination_obj(
        {
            "spec_version": 2,
            "run_id": "r1",
            "tenant_id": "t",
            "repo_id": "r",
            "coordination_graph": {
                "nodes": [
                    {"node_id": "a", "kind": "step"},
                    {"node_id": "b", "kind": "step"},
                ],
                "edges": [
                    {
                        "edge_id": "e1",
                        "kind": "handoff",
                        "src_step_id": "a",
                        "dst_step_id": "b",
                        "metadata": {"handoff_id": "h1"},
                    }
                ],
            },
            "orchestration_bindings": [{"role_name": "x", "agent_name": "y", "orchestration_step_ids": ["a"]}],
            "agent_roles": [{"name": "x"}],
        }
    )
    with pytest.raises(ValueError, match="handoff from predecessor"):
        coordination_step_policy_extras(
            parsed=obj,
            step_id="b",
            coordination_step_outputs={},
        )


def test_handoff_predecessor_populates_sorted_sha_list() -> None:
    sha_a = "a" * 64
    sha_c = "c" * 64
    obj = parse_coordination_obj(
        {
            "spec_version": 2,
            "run_id": "r1",
            "tenant_id": "t",
            "repo_id": "r",
            "coordination_graph": {
                "nodes": [
                    {"node_id": "c_step", "kind": "step"},
                    {"node_id": "a_step", "kind": "step"},
                    {"node_id": "dst", "kind": "step"},
                ],
                "edges": [
                    {
                        "edge_id": "e2",
                        "kind": "handoff",
                        "src_step_id": "c_step",
                        "dst_step_id": "dst",
                        "metadata": {"handoff_id": "h2"},
                    },
                    {
                        "edge_id": "e1",
                        "kind": "handoff",
                        "src_step_id": "a_step",
                        "dst_step_id": "dst",
                        "metadata": {"handoff_id": "h1"},
                    },
                ],
            },
            "orchestration_bindings": [{"role_name": "x", "agent_name": "y", "orchestration_step_ids": ["dst"]}],
            "agent_roles": [{"name": "x"}],
        }
    )
    out = coordination_step_policy_extras(
        parsed=obj,
        step_id="dst",
        coordination_step_outputs={"a_step": sha_a.upper(), "c_step": sha_c},
    )
    preds = out["coordination_handoff_predecessor_output_sha256s"]
    assert preds == [
        {"predecessor_step_id": "a_step", "agent_worker_output_sha256": sha_a},
        {"predecessor_step_id": "c_step", "agent_worker_output_sha256": sha_c},
    ]
    assert out["coordination_edge_kind"] == "handoff"
    assert out["coordination_handoff_id"] == "h1,h2"


def test_normalize_agent_output_sha256_hex_accepts_uppercase() -> None:
    assert normalize_agent_output_sha256_hex("A" * 64) == "a" * 64


def test_delegate_kind_metadata_overrides_url_inference() -> None:
    obj = parse_coordination_obj(
        {
            "spec_version": 2,
            "run_id": "r1",
            "tenant_id": "t",
            "repo_id": "r",
            "coordination_graph": {
                "nodes": [{"node_id": "x", "kind": "step"}, {"node_id": "y", "kind": "step"}],
                "edges": [
                    {
                        "edge_id": "d1",
                        "kind": "delegate",
                        "src_step_id": "y",
                        "dst_step_id": "x",
                        "metadata": {
                            "delegate_target": "https://example.test/run",
                            "delegate_kind": "custom",
                        },
                    },
                ],
            },
            "orchestration_bindings": [{"role_name": "z", "agent_name": "w", "orchestration_step_ids": ["x"]}],
            "agent_roles": [{"name": "z"}],
        }
    )
    out = coordination_step_policy_extras(parsed=obj, step_id="x", coordination_step_outputs={})
    assert out["coordination_delegate_kind"] == "custom"


def test_delegate_kind_http_inferred_from_https_target() -> None:
    obj = parse_coordination_obj(
        {
            "spec_version": 2,
            "run_id": "r1",
            "tenant_id": "t",
            "repo_id": "r",
            "coordination_graph": {
                "nodes": [{"node_id": "x", "kind": "step"}, {"node_id": "y", "kind": "step"}],
                "edges": [
                    {
                        "edge_id": "d1",
                        "kind": "delegate",
                        "src_step_id": "y",
                        "dst_step_id": "x",
                        "metadata": {"delegate_target": "https://example.test/run"},
                    },
                ],
            },
            "orchestration_bindings": [{"role_name": "z", "agent_name": "w", "orchestration_step_ids": ["x"]}],
            "agent_roles": [{"name": "z"}],
        }
    )
    out = coordination_step_policy_extras(parsed=obj, step_id="x", coordination_step_outputs={})
    assert out["coordination_delegate_kind"] == "http"


def test_parallel_and_delegate_metadata_sorted() -> None:
    obj = parse_coordination_obj(
        {
            "spec_version": 2,
            "run_id": "r1",
            "tenant_id": "t",
            "repo_id": "r",
            "coordination_graph": {
                "nodes": [{"node_id": "x", "kind": "step"}, {"node_id": "y", "kind": "step"}],
                "edges": [
                    {
                        "edge_id": "p2",
                        "kind": "parallel",
                        "src_step_id": "y",
                        "dst_step_id": "x",
                        "metadata": {"parallel_group_id": "g1"},
                    },
                    {
                        "edge_id": "d1",
                        "kind": "delegate",
                        "src_step_id": "y",
                        "dst_step_id": "x",
                        "metadata": {"delegate_target": "http"},
                    },
                ],
            },
            "orchestration_bindings": [{"role_name": "z", "agent_name": "w", "orchestration_step_ids": ["x"]}],
            "agent_roles": [{"name": "z"}],
        }
    )
    out = coordination_step_policy_extras(parsed=obj, step_id="x", coordination_step_outputs={})
    assert out["coordination_delegate_edges"] == [
        {"edge_id": "d1", "from_step_id": "y", "delegate_target": "http"},
    ]
    assert out["coordination_parallel_edges"] == [
        {"edge_id": "p2", "from_step_id": "y", "parallel_group_id": "g1"},
    ]
    assert out["coordination_edge_kind"] == "delegate,parallel"
    assert out["coordination_delegate_kind"] == "opaque"
