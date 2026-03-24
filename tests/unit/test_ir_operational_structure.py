from __future__ import annotations

from hashlib import sha256

from akc.compile.ir_builder import build_ir_document_from_plan
from akc.compile.ir_operational_validate import (
    validate_ir_graph_integrity,
    validate_ir_operational_structure,
)
from akc.ir import EffectAnnotation, IRDocument, IRNode, stable_node_id
from akc.knowledge.persistence import KNOWLEDGE_SNAPSHOT_RELPATH
from akc.memory.models import PlanState, PlanStep, now_ms


def test_validate_ir_operational_structure_flags_missing_workflow_contract() -> None:
    ir = IRDocument(
        tenant_id="t1",
        repo_id="r1",
        nodes=(
            IRNode(
                id=stable_node_id(kind="workflow", name="only"),
                tenant_id="t1",
                kind="workflow",
                name="step",
                properties={"plan_id": "p", "step_id": "s"},
            ),
        ),
    )
    issues = validate_ir_operational_structure(ir)
    assert any("missing OperationalContract" in msg for msg in issues)


def test_build_ir_emits_runtime_and_acceptance_contracts() -> None:
    created = now_ms()
    plan = PlanState(
        id="plan_x",
        tenant_id="tenant-a",
        repo_id="repo-a",
        goal="do the thing",
        status="active",
        created_at_ms=created,
        updated_at_ms=created,
        steps=(
            PlanStep(
                id="step_a",
                title="generate",
                status="pending",
                order_idx=0,
                inputs={
                    "intent_id": "intent-1",
                    "active_success_criteria": [
                        {
                            "success_criterion_id": "sc-1",
                            "evaluation_mode": "tests",
                            "summary": "tests pass",
                        }
                    ],
                },
                outputs={"retrieval_snapshot": {"query": "q", "source": "idx"}},
            ),
        ),
        next_step_id="step_a",
        budgets={},
        last_feedback={},
    )
    ir = build_ir_document_from_plan(plan=plan, intent_node_properties=None)
    assert validate_ir_operational_structure(ir) == ()

    intent_n = next(n for n in ir.nodes if n.kind == "intent")
    assert intent_n.contract is not None
    assert intent_n.contract.contract_category == "acceptance"
    crit = intent_n.contract.acceptance or {}
    raw_criteria = crit.get("criteria")
    assert isinstance(raw_criteria, list) and len(raw_criteria) == 1
    assert set(raw_criteria[0].keys()) == {"id", "evaluation_mode"}
    assert raw_criteria[0]["id"] == "sc-1"
    assert raw_criteria[0]["evaluation_mode"] == "tests"

    wf = next(n for n in ir.nodes if n.kind == "workflow")
    assert wf.contract is not None
    assert wf.contract.contract_category == "runtime"
    expected_wf_cid = f"opc_rt_{sha256(f'runtime::{plan.id}::step_a'.encode()).hexdigest()[:24]}"
    assert wf.contract.contract_id == expected_wf_cid
    assert wf.effects is not None
    assert wf.effects.network is True

    expected_acc_cid = f"opc_acc_{sha256(b'accept::tenant-a::intent-1').hexdigest()[:24]}"
    assert intent_n.contract.contract_id == expected_acc_cid


def test_validate_ir_graph_integrity_unknown_depends_on() -> None:
    wf_id = stable_node_id(kind="workflow", name="w")
    ir = IRDocument(
        tenant_id="t1",
        repo_id="r1",
        nodes=(
            IRNode(
                id=wf_id,
                tenant_id="t1",
                kind="workflow",
                name="step",
                properties={"plan_id": "p", "step_id": "s"},
                depends_on=("missing-node",),
            ),
        ),
    )
    issues = validate_ir_graph_integrity(ir)
    assert any("unknown node id" in msg for msg in issues)


def test_validate_ir_graph_integrity_detects_cycle() -> None:
    a = stable_node_id(kind="workflow", name="a")
    b = stable_node_id(kind="workflow", name="b")
    ir = IRDocument(
        tenant_id="t1",
        repo_id="r1",
        nodes=(
            IRNode(
                id=a,
                tenant_id="t1",
                kind="workflow",
                name="a",
                properties={"plan_id": "p", "step_id": "s1"},
                depends_on=(b,),
            ),
            IRNode(
                id=b,
                tenant_id="t1",
                kind="workflow",
                name="b",
                properties={"plan_id": "p", "step_id": "s2"},
                depends_on=(a,),
            ),
        ),
    )
    issues = validate_ir_graph_integrity(ir)
    assert any("cycle" in msg for msg in issues)


def test_validate_ir_graph_integrity_knowledge_hub_payloads() -> None:
    hub_id = stable_node_id(kind="knowledge", name="layer:test")
    ir = IRDocument(
        tenant_id="t1",
        repo_id="r1",
        nodes=(
            IRNode(
                id=hub_id,
                tenant_id="t1",
                kind="knowledge",
                name="knowledge_layer",
                properties={
                    "persisted_snapshot_relpath": KNOWLEDGE_SNAPSHOT_RELPATH,
                    "knowledge_semantic_fingerprint_16": "a" * 16,
                    "knowledge_assertion_ids": [],
                },
            ),
        ),
    )
    assert validate_ir_graph_integrity(ir) == ()

    ir_bad = IRDocument(
        tenant_id="t1",
        repo_id="r1",
        nodes=(
            IRNode(
                id=hub_id,
                tenant_id="t1",
                kind="knowledge",
                name="knowledge_layer",
                properties={"knowledge_assertion_ids": "not-a-list"},
            ),
        ),
    )
    bad = validate_ir_graph_integrity(ir_bad)
    assert any("persisted_snapshot_relpath" in msg or "knowledge_assertion_ids" in msg for msg in bad)


def test_validate_ir_graph_integrity_deployable_requires_effects_or_contract() -> None:
    sid = stable_node_id(kind="service", name="api")
    ir = IRDocument(
        tenant_id="t1",
        repo_id="r1",
        nodes=(
            IRNode(
                id=sid,
                tenant_id="t1",
                kind="service",
                name="api",
                properties={},
            ),
        ),
    )
    assert any("neither effects nor" in msg for msg in validate_ir_graph_integrity(ir))

    ir_ok = IRDocument(
        tenant_id="t1",
        repo_id="r1",
        nodes=(
            IRNode(
                id=sid,
                tenant_id="t1",
                kind="service",
                name="api",
                properties={},
                effects=EffectAnnotation(),
            ),
        ),
    )
    assert validate_ir_graph_integrity(ir_ok) == ()


def test_validate_ir_graph_integrity_policy_metadata_unknown_key() -> None:
    ir = IRDocument(
        tenant_id="t1",
        repo_id="r1",
        nodes=(
            IRNode(
                id="pol-1",
                tenant_id="t1",
                kind="policy",
                name="p",
                properties={"metadata": {"extra_field": [], "runtime_deny_actions": ["a.b"]}},
            ),
        ),
    )
    issues = validate_ir_graph_integrity(ir)
    assert any("unknown key" in msg and "extra_field" in msg for msg in issues)


def test_validate_ir_graph_integrity_policy_metadata_action_list_types() -> None:
    ir = IRDocument(
        tenant_id="t1",
        repo_id="r1",
        nodes=(
            IRNode(
                id="pol-1",
                tenant_id="t1",
                kind="policy",
                name="p",
                properties={"metadata": {"runtime_deny_actions": "not-a-list"}},
            ),
        ),
    )
    issues = validate_ir_graph_integrity(ir)
    assert any("runtime_deny_actions" in msg and "list of strings" in msg for msg in issues)


def test_validate_ir_graph_integrity_policy_top_level_action_keys() -> None:
    ir_ok = IRDocument(
        tenant_id="t1",
        repo_id="r1",
        nodes=(
            IRNode(
                id="pol-1",
                tenant_id="t1",
                kind="policy",
                name="p",
                properties={"runtime_deny_actions": ["x.y"]},
            ),
        ),
    )
    assert validate_ir_graph_integrity(ir_ok) == ()
