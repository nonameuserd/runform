from __future__ import annotations

import pytest

from akc.compile.ir_builder import build_ir_document_from_plan
from akc.ir import EffectAnnotation, IRDocument, IRNode, ProvenancePointer, diff_ir, stable_node_id
from akc.ir.schema import (
    ContractTrigger,
    IOContract,
    OperationalBudget,
    OperationalContract,
    StateMachineContract,
    StateTransition,
)
from akc.knowledge import (
    CanonicalConstraint,
    CanonicalDecision,
    EvidenceMapping,
    KnowledgeSnapshot,
)
from akc.memory.models import PlanState, PlanStep, now_ms


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


def _legacy_v1_ir_obj() -> dict[str, object]:
    return {
        "schema_kind": "akc_ir",
        "schema_version": 1,
        "format_version": "1.0",
        "tenant_id": "tenant-a",
        "repo_id": "repo-a",
        "nodes": [
            {
                "id": stable_node_id(kind="service", name="billing"),
                "tenant_id": "tenant-a",
                "kind": "service",
                "name": "billing",
                "properties": {"language": "python"},
                "depends_on": [],
                "provenance": [],
            }
        ],
    }


def test_ir_fingerprint_stable_across_node_order() -> None:
    ir1 = _sample_ir()
    ir2 = IRDocument(
        tenant_id=ir1.tenant_id,
        repo_id=ir1.repo_id,
        nodes=tuple(reversed(ir1.nodes)),
    )

    assert ir1.to_json_obj() == ir2.to_json_obj()
    assert ir1.fingerprint() == ir2.fingerprint()


def test_ir_document_emits_latest_schema_version() -> None:
    ir = _sample_ir()
    payload = ir.to_json_obj()

    assert payload["schema_version"] == 2
    assert payload["format_version"] == "2.0"


def test_ir_document_reads_legacy_v1_ir() -> None:
    ir = IRDocument.from_json_obj(_legacy_v1_ir_obj())

    assert ir.schema_version == 1
    assert ir.format_version == "1.0"
    assert len(ir.nodes) == 1
    assert ir.nodes[0].contract is None


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


def test_ir_node_fingerprint_uses_deterministic_json_contract_ordering() -> None:
    tenant_id = "tenant-a"
    node_id = stable_node_id(kind="agent", name="planner")
    trigger_a = ContractTrigger(trigger_id="a-trigger", source="manual", details={"k": "v"})
    trigger_b = ContractTrigger(trigger_id="b-trigger", source="event", details={"topic": "x"})
    t_a = StateTransition(
        transition_id="a-transition",
        from_state="idle",
        to_state="running",
        trigger_id="a-trigger",
    )
    t_b = StateTransition(
        transition_id="b-transition",
        from_state="running",
        to_state="idle",
        trigger_id="b-trigger",
    )

    contract_unsorted = OperationalContract(
        contract_id="agent-contract",
        contract_category="runtime",
        triggers=(trigger_b, trigger_a),
        io_contract=IOContract(input_keys=("task",), output_keys=("result",)),
        state_machine=StateMachineContract(initial_state="idle", transitions=(t_b, t_a)),
    )
    contract_sorted = OperationalContract(
        contract_id="agent-contract",
        contract_category="runtime",
        triggers=(trigger_a, trigger_b),
        io_contract=IOContract(input_keys=("task",), output_keys=("result",)),
        state_machine=StateMachineContract(initial_state="idle", transitions=(t_a, t_b)),
    )

    node_unsorted = IRNode(
        id=node_id,
        tenant_id=tenant_id,
        kind="agent",
        name="planner",
        properties={"role": "planner"},
        contract=contract_unsorted,
    )
    node_sorted = IRNode(
        id=node_id,
        tenant_id=tenant_id,
        kind="agent",
        name="planner",
        properties={"role": "planner"},
        contract=contract_sorted,
    )

    assert node_unsorted.to_json_obj() == node_sorted.to_json_obj()
    assert node_unsorted.fingerprint() == node_sorted.fingerprint()


def test_diff_ir_does_not_mark_changed_for_equivalent_contract_ordering() -> None:
    tenant_id = "tenant-a"
    repo_id = "repo-a"
    node_id = stable_node_id(kind="agent", name="executor")

    trigger_a = ContractTrigger(trigger_id="a-trigger", source="manual")
    trigger_b = ContractTrigger(trigger_id="b-trigger", source="event")
    t_a = StateTransition(
        transition_id="a-transition",
        from_state="idle",
        to_state="running",
        trigger_id="a-trigger",
    )
    t_b = StateTransition(
        transition_id="b-transition",
        from_state="running",
        to_state="idle",
        trigger_id="b-trigger",
    )
    common_io = IOContract(input_keys=("task",), output_keys=("result",))

    before_node = IRNode(
        id=node_id,
        tenant_id=tenant_id,
        kind="agent",
        name="executor",
        properties={"role": "executor"},
        contract=OperationalContract(
            contract_id="executor-contract",
            contract_category="runtime",
            triggers=(trigger_b, trigger_a),
            io_contract=common_io,
            state_machine=StateMachineContract(initial_state="idle", transitions=(t_b, t_a)),
        ),
    )
    after_node = IRNode(
        id=node_id,
        tenant_id=tenant_id,
        kind="agent",
        name="executor",
        properties={"role": "executor"},
        contract=OperationalContract(
            contract_id="executor-contract",
            contract_category="runtime",
            triggers=(trigger_a, trigger_b),
            io_contract=common_io,
            state_machine=StateMachineContract(initial_state="idle", transitions=(t_a, t_b)),
        ),
    )
    before = IRDocument(tenant_id=tenant_id, repo_id=repo_id, nodes=(before_node,))
    after = IRDocument(tenant_id=tenant_id, repo_id=repo_id, nodes=(after_node,))

    d = diff_ir(before=before, after=after)
    assert d.added == ()
    assert d.removed == ()
    assert d.changed == ()


def test_ir_node_contract_roundtrip_includes_contract_key() -> None:
    tenant_id = "tenant-a"
    node_id = stable_node_id(kind="agent", name="roundtrip_agent")

    trigger_a = ContractTrigger(
        trigger_id="a-trigger",
        source="manual",
        details={"k": "v"},
    )
    trigger_b = ContractTrigger(
        trigger_id="b-trigger",
        source="event",
        details={"topic": "jobs.created"},
    )

    t_a = StateTransition(
        transition_id="a-transition",
        from_state="idle",
        to_state="running",
        trigger_id="a-trigger",
        guard={"max_retries": 3},
    )
    t_b = StateTransition(
        transition_id="b-transition",
        from_state="running",
        to_state="done",
        trigger_id="b-trigger",
        guard={"ok": True},
    )

    io_contract = IOContract(
        input_keys=("task", "context"),
        output_keys=("result", "summary"),
        schema={"task": {"type": "string"}},
    )

    contract = OperationalContract(
        contract_id="agent-contract",
        contract_category="runtime",
        triggers=(trigger_a, trigger_b),
        io_contract=io_contract,
        state_machine=StateMachineContract(initial_state="idle", transitions=(t_a, t_b)),
        runtime_budget=OperationalBudget(max_tokens=1000),
        acceptance={"requires_approval": True, "priority": "high"},
    )

    node = IRNode(
        id=node_id,
        tenant_id=tenant_id,
        kind="agent",
        name="roundtrip_agent",
        properties={"role": "planner"},
        contract=contract,
    )

    node_json = node.to_json_obj()
    assert "contract" in node_json

    reconstructed = IRNode.from_json_obj(node_json)
    assert reconstructed == node


def test_ir_contract_trigger_state_ordering_stable_on_document_fingerprint() -> None:
    tenant_id = "tenant-a"
    repo_id = "repo-a"

    node_id = stable_node_id(kind="agent", name="executor")

    trigger_a = ContractTrigger(trigger_id="a-trigger", source="manual", details={"k": "v"})
    trigger_b = ContractTrigger(trigger_id="b-trigger", source="event", details={"topic": "x"})

    t_a = StateTransition(
        transition_id="a-transition",
        from_state="idle",
        to_state="running",
        trigger_id="a-trigger",
        guard={"max": 1},
    )
    t_b = StateTransition(
        transition_id="b-transition",
        from_state="running",
        to_state="idle",
        trigger_id="b-trigger",
        guard={"seen": True},
    )

    common_io = IOContract(input_keys=("task",), output_keys=("result",))

    contract_unsorted = OperationalContract(
        contract_id="executor-contract",
        contract_category="runtime",
        triggers=(trigger_b, trigger_a),
        io_contract=common_io,
        state_machine=StateMachineContract(initial_state="idle", transitions=(t_b, t_a)),
    )
    contract_sorted = OperationalContract(
        contract_id="executor-contract",
        contract_category="runtime",
        triggers=(trigger_a, trigger_b),
        io_contract=common_io,
        state_machine=StateMachineContract(initial_state="idle", transitions=(t_a, t_b)),
    )

    node_unsorted = IRNode(
        id=node_id,
        tenant_id=tenant_id,
        kind="agent",
        name="executor",
        properties={"role": "executor"},
        contract=contract_unsorted,
    )
    node_sorted = IRNode(
        id=node_id,
        tenant_id=tenant_id,
        kind="agent",
        name="executor",
        properties={"role": "executor"},
        contract=contract_sorted,
    )

    assert node_unsorted.to_json_obj() == node_sorted.to_json_obj()

    doc_unsorted = IRDocument(tenant_id=tenant_id, repo_id=repo_id, nodes=(node_unsorted,))
    doc_sorted = IRDocument(tenant_id=tenant_id, repo_id=repo_id, nodes=(node_sorted,))
    assert doc_unsorted.to_json_obj() == doc_sorted.to_json_obj()
    assert doc_unsorted.fingerprint() == doc_sorted.fingerprint()


def test_ir_diff_detects_contract_change_for_existing_node() -> None:
    tenant_id = "tenant-a"
    repo_id = "repo-a"
    node_id = stable_node_id(kind="agent", name="executor")

    trigger_a = ContractTrigger(trigger_id="a-trigger", source="manual")
    t_a = StateTransition(
        transition_id="a-transition",
        from_state="idle",
        to_state="running",
        trigger_id="a-trigger",
    )
    state_machine = StateMachineContract(initial_state="idle", transitions=(t_a,))
    common_inputs = ("task",)

    before_node = IRNode(
        id=node_id,
        tenant_id=tenant_id,
        kind="agent",
        name="executor",
        properties={"role": "executor"},
        contract=OperationalContract(
            contract_id="executor-contract",
            contract_category="runtime",
            triggers=(trigger_a,),
            io_contract=IOContract(input_keys=common_inputs, output_keys=("result",)),
            state_machine=state_machine,
        ),
    )

    # Contract change: update output keys (same node id).
    after_node = IRNode(
        id=node_id,
        tenant_id=tenant_id,
        kind="agent",
        name="executor",
        properties={"role": "executor"},
        contract=OperationalContract(
            contract_id="executor-contract",
            contract_category="runtime",
            triggers=(trigger_a,),
            io_contract=IOContract(input_keys=common_inputs, output_keys=("result", "extra")),
            state_machine=state_machine,
        ),
    )

    before = IRDocument(tenant_id=tenant_id, repo_id=repo_id, nodes=(before_node,))
    after = IRDocument(tenant_id=tenant_id, repo_id=repo_id, nodes=(after_node,))

    d = diff_ir(before=before, after=after)
    assert d.added == ()
    assert d.removed == ()
    assert d.changed == (node_id,)


def test_ir_node_from_json_without_contract_sets_contract_none() -> None:
    node_id = stable_node_id(kind="agent", name="no_contract")
    node = IRNode(
        id=node_id,
        tenant_id="tenant-a",
        kind="agent",
        name="no_contract",
        properties={"role": "legacy"},
        contract=None,
    )
    node_json = node.to_json_obj()
    assert "contract" not in node_json

    reconstructed = IRNode.from_json_obj(node_json)
    assert reconstructed.contract is None


def test_ir_node_rejects_non_runtime_state_machine_contract() -> None:
    trigger = ContractTrigger(trigger_id="deploy", source="manual")
    transition = StateTransition(
        transition_id="deploy-start",
        from_state="ready",
        to_state="running",
        trigger_id="deploy",
    )

    with pytest.raises(ValueError, match="state_machine must be None unless contract_category == 'runtime'"):
        IRNode(
            id=stable_node_id(kind="infrastructure", name="runtime-host"),
            tenant_id="tenant-a",
            kind="infrastructure",
            name="runtime-host",
            properties={},
            contract=OperationalContract(
                contract_id="deploy-contract",
                contract_category="deployment",
                triggers=(trigger,),
                io_contract=IOContract(input_keys=("artifact_bundle",), output_keys=("endpoint",)),
                state_machine=StateMachineContract(initial_state="ready", transitions=(transition,)),
            ),
        )


def test_operational_contract_rejects_unknown_contract_category() -> None:
    with pytest.raises(ValueError, match="contract_category must be one of"):
        OperationalContract(
            contract_id="bad-category",
            contract_category="runntime",  # type: ignore[arg-type]
            triggers=(ContractTrigger(trigger_id="start", source="manual"),),
            io_contract=IOContract(input_keys=("task",), output_keys=("result",)),
        )


def test_operational_contract_rejects_transition_trigger_not_declared() -> None:
    with pytest.raises(ValueError, match="transition trigger_id must reference"):
        OperationalContract(
            contract_id="bad-trigger-ref",
            contract_category="runtime",
            triggers=(ContractTrigger(trigger_id="known-trigger", source="manual"),),
            io_contract=IOContract(input_keys=("task",), output_keys=("result",)),
            state_machine=StateMachineContract(
                initial_state="idle",
                transitions=(
                    StateTransition(
                        transition_id="start",
                        from_state="idle",
                        to_state="running",
                        trigger_id="missing-trigger",
                    ),
                ),
            ),
        )


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


def test_ir_builder_emits_first_class_intent_node() -> None:
    tenant_id = "tenant-a"
    repo_id = "repo-a"
    plan_created = now_ms()
    plan = PlanState(
        id="plan_1",
        tenant_id=tenant_id,
        repo_id=repo_id,
        goal="some goal",
        status="active",
        created_at_ms=plan_created,
        updated_at_ms=plan_created,
        steps=(
            PlanStep(
                id="step_1",
                title="step",
                status="pending",
                order_idx=0,
                inputs={
                    "intent_id": "intent_123",
                    "active_objectives": [],
                    "linked_constraints": [],
                    "active_success_criteria": [],
                },
                outputs={},
            ),
        ),
        next_step_id="step_1",
        budgets={},
        last_feedback={},
    )

    intent_semantic = "a" * 16
    intent_goal_text = "b" * 16
    ir = build_ir_document_from_plan(
        plan=plan,
        intent_node_properties={
            "intent_id": "intent_123",
            "intent_semantic_fingerprint": intent_semantic,
            "intent_goal_text_fingerprint": intent_goal_text,
        },
    )

    intent_nodes = [n for n in ir.nodes if n.kind == "intent"]
    assert len(intent_nodes) == 1
    assert intent_nodes[0].properties.get("intent_semantic_fingerprint") == intent_semantic

    expected_intent_node_id = stable_node_id(kind="intent", name="intent:intent_123")
    workflow_nodes = [n for n in ir.nodes if n.kind == "workflow"]
    assert workflow_nodes
    # The first workflow node should depend on the intent contract node.
    assert expected_intent_node_id in tuple(workflow_nodes[0].depends_on)
    assert intent_nodes[0].contract is not None
    assert intent_nodes[0].contract.contract_category == "acceptance"
    assert workflow_nodes[0].contract is not None
    assert workflow_nodes[0].contract.contract_category == "runtime"


def test_ir_builder_emits_knowledge_hub_when_fingerprints_present() -> None:
    tenant_id = "tenant-a"
    repo_id = "repo-a"
    plan_created = now_ms()
    c = CanonicalConstraint(
        subject="x",
        predicate="must",
        object=None,
        polarity=1,
        scope="t",
        kind="hard",
        summary="do x",
    )
    d = CanonicalDecision(assertion_id=c.assertion_id, selected=True, resolved=True)
    snap = KnowledgeSnapshot(
        canonical_constraints=(c,),
        canonical_decisions=(d,),
        evidence_by_assertion={c.assertion_id: EvidenceMapping(evidence_doc_ids=(), resolved_provenance_pointers=())},
    )
    snap_body = snap.to_json_obj()
    plan = PlanState(
        id="plan_k",
        tenant_id=tenant_id,
        repo_id=repo_id,
        goal="g",
        status="active",
        created_at_ms=plan_created,
        updated_at_ms=plan_created,
        steps=(
            PlanStep(
                id="s1",
                title="retrieve",
                status="done",
                order_idx=0,
                inputs={"intent_id": "i1"},
                outputs={
                    "knowledge_semantic_fingerprint": "a" * 16,
                    "knowledge_provenance_fingerprint": "b" * 16,
                    "knowledge_snapshot": snap_body,
                },
            ),
        ),
        next_step_id="s1",
        budgets={},
        last_feedback={},
    )
    ir = build_ir_document_from_plan(
        plan=plan,
        intent_node_properties={"intent_id": "i1"},
    )
    hubs = [n for n in ir.nodes if n.kind == "knowledge"]
    assert len(hubs) == 1
    assert hubs[0].name == "knowledge_layer"
    assert hubs[0].properties.get("knowledge_semantic_fingerprint_16") == "a" * 16
    assert "persisted_snapshot_relpath" in hubs[0].properties
    entities = [n for n in ir.nodes if n.name.startswith("knowledge_constraint:")]
    assert entities
    assert hubs[0].id in entities[0].depends_on
    workflow_nodes = [n for n in ir.nodes if n.kind == "workflow"]
    assert workflow_nodes[0].depends_on == (hubs[0].id,)
    assert workflow_nodes[0].contract is not None
    assert workflow_nodes[0].contract.contract_category == "runtime"
