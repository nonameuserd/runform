"""Unit tests for cross-artifact consistency validators (JSON fixtures, no LLM)."""

from __future__ import annotations

import json

import pytest

from akc.compile.artifact_consistency import (
    collect_cross_artifact_consistency_issues,
    effective_allow_network_for_handoff,
    validate_bundle_vs_deployment_compose_network,
    validate_coordination_orchestration_consistency,
    validate_deployment_intents_align_with_ir,
    validate_orchestration_ir_references,
    validate_runtime_bundle_coordination_inline,
    validate_runtime_bundle_coordination_ref,
    validate_runtime_bundle_spec_hashes,
)
from akc.compile.controller_config import ControllerConfig, TierConfig
from akc.intent.models import IntentSpecV1
from akc.intent.policy_projection import project_runtime_intent_projection
from akc.ir import IRDocument, IRNode
from akc.utils.fingerprint import stable_json_fingerprint


def _orch_step(*, step_id: str, ir_node_id: str | None) -> dict[str, object]:
    base: dict[str, object] = {
        "step_id": step_id,
        "order_idx": 0,
        "agent_name": "a",
        "role": "writer",
    }
    if ir_node_id is not None:
        base["inputs"] = {"ir_node_id": ir_node_id}
    return base


def test_validate_orchestration_ir_references_ok() -> None:
    orch = {"steps": [_orch_step(step_id="workflow_000", ir_node_id="wf-1")]}
    issues = validate_orchestration_ir_references(orchestration_obj=orch, ir_node_ids={"wf-1"})
    assert issues == ()


def test_validate_orchestration_ir_references_unknown_node() -> None:
    orch = {"steps": [_orch_step(step_id="workflow_000", ir_node_id="missing")]}
    issues = validate_orchestration_ir_references(orchestration_obj=orch, ir_node_ids={"wf-1"})
    assert len(issues) == 1
    assert "unknown" in issues[0] or "not present" in issues[0]


def test_validate_coordination_bindings_reference_orchestration_steps() -> None:
    orch = {"steps": [_orch_step(step_id="workflow_000", ir_node_id=None)]}
    coord = {
        "orchestration_bindings": [
            {"role_name": "planner", "agent_name": "ag", "orchestration_step_ids": ["workflow_000"]}
        ],
        "coordination_graph": {
            "edges": [
                {
                    "edge_id": "e0",
                    "kind": "depends_on",
                    "src_step_id": "workflow_000",
                    "dst_step_id": "workflow_000",
                }
            ]
        },
    }
    assert validate_coordination_orchestration_consistency(orchestration_obj=orch, coordination_obj=coord) == ()


def test_validate_coordination_flags_unknown_step_in_binding() -> None:
    orch = {"steps": [_orch_step(step_id="workflow_000", ir_node_id=None)]}
    coord = {
        "orchestration_bindings": [
            {"role_name": "planner", "agent_name": "ag", "orchestration_step_ids": ["not-a-step"]}
        ]
    }
    issues = validate_coordination_orchestration_consistency(orchestration_obj=orch, coordination_obj=coord)
    assert any("unknown step_id" in msg for msg in issues)


def test_validate_coordination_flags_unknown_edge_step() -> None:
    orch = {"steps": [_orch_step(step_id="workflow_000", ir_node_id=None)]}
    coord = {
        "coordination_graph": {
            "edges": [
                {
                    "edge_id": "e0",
                    "kind": "depends_on",
                    "src_step_id": "bad",
                    "dst_step_id": "workflow_000",
                }
            ]
        }
    }
    issues = validate_coordination_orchestration_consistency(orchestration_obj=orch, coordination_obj=coord)
    assert any("src_step_id" in msg and "bad" in msg for msg in issues)


def test_validate_runtime_bundle_coordination_ref_ok() -> None:
    coord_fp = "a" * 64
    bundle = {
        "run_id": "r1",
        "spec_hashes": {"coordination_spec_sha256": coord_fp},
        "coordination_ref": {"path": ".akc/agents/r1.coordination.json", "fingerprint": coord_fp},
    }
    assert validate_runtime_bundle_coordination_ref(bundle) == ()


def test_validate_runtime_bundle_coordination_ref_fingerprint_mismatch() -> None:
    bundle = {
        "run_id": "r1",
        "spec_hashes": {"coordination_spec_sha256": "a" * 64},
        "coordination_ref": {"path": ".akc/agents/r1.coordination.json", "fingerprint": "b" * 64},
    }
    issues = validate_runtime_bundle_coordination_ref(bundle)
    assert issues and "fingerprint" in issues[0]


def test_validate_runtime_bundle_coordination_ref_wrong_path_for_run_id() -> None:
    bundle = {
        "run_id": "r1",
        "spec_hashes": {"coordination_spec_sha256": "a" * 64},
        "coordination_ref": {"path": ".akc/agents/other.coordination.json", "fingerprint": "a" * 64},
    }
    issues = validate_runtime_bundle_coordination_ref(bundle)
    assert issues and "coordination_ref.path" in issues[0]


def test_validate_runtime_bundle_coordination_inline_mismatch() -> None:
    bundle = {
        "spec_hashes": {"coordination_spec_sha256": "a" * 64},
        "coordination_spec": {"run_id": "x"},
    }
    issues = validate_runtime_bundle_coordination_inline(bundle)
    assert issues and "coordination_spec" in issues[0]


def test_validate_runtime_bundle_spec_hashes_ok() -> None:
    orch_obj = {"run_id": "r1", "steps": []}
    coord_obj = {"run_id": "r1", "orchestration_bindings": []}
    orch_text = json.dumps(orch_obj, sort_keys=True)
    coord_text = json.dumps(coord_obj, sort_keys=True)
    bundle = {
        "spec_hashes": {
            "orchestration_spec_sha256": stable_json_fingerprint(orch_obj),
            "coordination_spec_sha256": stable_json_fingerprint(coord_obj),
        }
    }
    assert (
        validate_runtime_bundle_spec_hashes(
            bundle_obj=bundle,
            orchestration_json_text=orch_text,
            coordination_json_text=coord_text,
        )
        == ()
    )


def test_validate_runtime_bundle_spec_hashes_mismatch() -> None:
    orch_obj = {"a": 1}
    coord_obj = {"b": 2}
    bundle = {
        "spec_hashes": {
            "orchestration_spec_sha256": "0" * 64,
            "coordination_spec_sha256": "0" * 64,
        }
    }
    issues = validate_runtime_bundle_spec_hashes(
        bundle_obj=bundle,
        orchestration_json_text=json.dumps(orch_obj),
        coordination_json_text=json.dumps(coord_obj),
    )
    assert len(issues) == 2


@pytest.mark.parametrize(
    "bad_text",
    ["{", "null"],
)
def test_validate_runtime_bundle_spec_hashes_invalid_json(bad_text: str) -> None:
    issues = validate_runtime_bundle_spec_hashes(
        bundle_obj={"spec_hashes": {}},
        orchestration_json_text=bad_text,
        coordination_json_text="{}",
    )
    assert issues and "decode" in issues[0].lower()


def test_collect_cross_artifact_consistency_issues_end_to_end_ok() -> None:
    wf = IRNode(
        id="wf-1",
        tenant_id="tenant-a",
        name="w",
        kind="workflow",
        properties={"order_idx": 0, "status": "pending"},
    )
    ir = IRDocument(tenant_id="tenant-a", repo_id="repo-a", nodes=(wf,))
    intent_spec = IntentSpecV1(
        tenant_id="tenant-a",
        repo_id="repo-a",
        goal_statement="test goal for consistency",
    )
    proj = project_runtime_intent_projection(intent=intent_spec)
    allow_network, _renv = effective_allow_network_for_handoff(ir_document=ir, intent_spec=intent_spec)
    orch = {
        "run_id": "r1",
        "tenant_id": "tenant-a",
        "repo_id": "repo-a",
        "steps": [_orch_step(step_id="workflow_000", ir_node_id="wf-1")],
    }
    coord = {
        "spec_version": 1,
        "run_id": "r1",
        "tenant_id": "tenant-a",
        "repo_id": "repo-a",
        "orchestration_bindings": [
            {"role_name": "planner", "agent_name": "ag", "orchestration_step_ids": ["workflow_000"]}
        ],
        "coordination_graph": {"edges": []},
    }
    orch_text = json.dumps(orch, sort_keys=True)
    coord_text = json.dumps(coord, sort_keys=True)
    bundle = {
        "run_id": "r1",
        "tenant_id": "tenant-a",
        "repo_id": "repo-a",
        "intent_ref": {
            "intent_id": proj.intent_id,
            "stable_intent_sha256": proj.stable_intent_sha256,
            "semantic_fingerprint": proj.intent_semantic_fingerprint,
            "goal_text_fingerprint": proj.intent_goal_text_fingerprint,
        },
        "intent_policy_projection": proj.to_json_obj(),
        "system_ir_ref": {
            "path": ".akc/ir/r1.json",
            "fingerprint": ir.fingerprint(),
            "format_version": ir.format_version,
            "schema_version": int(ir.schema_version),
        },
        "runtime_policy_envelope": {
            "tenant_id": "tenant-a",
            "repo_id": "repo-a",
            "run_id": "r1",
            "allow_network": allow_network,
        },
        "spec_hashes": {
            "orchestration_spec_sha256": stable_json_fingerprint(json.loads(orch_text)),
            "coordination_spec_sha256": stable_json_fingerprint(json.loads(coord_text)),
        },
        "coordination_ref": {
            "path": ".akc/agents/r1.coordination.json",
            "fingerprint": stable_json_fingerprint(json.loads(coord_text)),
        },
    }
    net_flag = "true" if allow_network else "false"
    compose_yaml = f"""
services:
  akc-app:
    environment:
      AKC_ALLOW_NETWORK: "{net_flag}"
"""
    assert (
        collect_cross_artifact_consistency_issues(
            ir_document=ir,
            orchestration_json_text=orch_text,
            coordination_json_text=coord_text,
            runtime_bundle_obj=bundle,
            intent_spec=intent_spec,
            deployment_docker_compose_yaml=compose_yaml,
        )
        == ()
    )


def test_validate_deployment_intents_align_with_ir_ok() -> None:
    bundle = {
        "referenced_ir_nodes": [
            {"id": "svc-1", "kind": "service"},
            {"id": "wf-1", "kind": "workflow"},
        ],
        "deployment_intents": [
            {
                "node_id": "svc-1",
                "kind": "service",
                "name": "api",
                "depends_on": [],
                "effects": None,
                "contract_id": None,
            }
        ],
    }
    assert validate_deployment_intents_align_with_ir(bundle) == ()


def test_validate_deployment_intents_align_with_ir_flags_missing_row() -> None:
    bundle = {
        "referenced_ir_nodes": [
            {"id": "a", "kind": "agent"},
            {"id": "b", "kind": "agent"},
        ],
        "deployment_intents": [
            {"node_id": "a", "kind": "agent"},
        ],
    }
    issues = validate_deployment_intents_align_with_ir(bundle)
    assert issues and "missing deployment rows" in issues[0] and "b" in issues[0]


def test_validate_deployment_intents_align_with_ir_flags_kind_mismatch() -> None:
    bundle = {
        "referenced_ir_nodes": [{"id": "x", "kind": "service"}],
        "deployment_intents": [{"node_id": "x", "kind": "integration"}],
    }
    issues = validate_deployment_intents_align_with_ir(bundle)
    assert any("does not match" in msg for msg in issues)


def test_effective_deployment_intents_ir_alignment_policy() -> None:
    tiers = {"small": TierConfig(name="small", llm_model="m")}
    base_kw = {"tiers": tiers, "tool_allowlist": ("llm.complete",)}
    c1 = ControllerConfig(**base_kw)
    assert c1.effective_deployment_intents_ir_alignment_policy() == "warn"
    c2 = ControllerConfig(**base_kw, ir_operational_structure_policy="off")
    assert c2.effective_deployment_intents_ir_alignment_policy() == "off"
    c3 = ControllerConfig(
        **base_kw,
        ir_operational_structure_policy="off",
        deployment_intents_ir_alignment_policy="error",
    )
    assert c3.effective_deployment_intents_ir_alignment_policy() == "error"


def test_validate_bundle_vs_deployment_compose_network_flags_bundle_false_compose_true() -> None:
    bundle = {
        "runtime_policy_envelope": {
            "allow_network": False,
        }
    }
    issues = validate_bundle_vs_deployment_compose_network(
        bundle_obj=bundle,
        compose_yaml_text='AKC_ALLOW_NETWORK: "true"\n',
    )
    assert issues and "AKC_ALLOW_NETWORK" in issues[0]
