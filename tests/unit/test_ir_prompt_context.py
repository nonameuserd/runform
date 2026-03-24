from __future__ import annotations

from akc.compile.ir_builder import build_ir_document_from_plan
from akc.compile.ir_passes import DefaultIRGeneratePromptPass, DefaultIRRepairPromptPass
from akc.compile.ir_prompt_context import (
    build_reference_intent_contract_for_retrieval,
    compact_ir_document_for_prompt,
    effective_intent_contract_shape_for_compile_prompts,
    intent_prompt_context_from_ir_and_resolve,
    ir_intent_knowledge_anchor_for_prompt,
    ir_structural_hints_for_retrieval_query,
    plan_execution_trace_for_prompt,
)
from akc.compile.repair import FailureSummary
from akc.intent import compile_intent_spec, compute_intent_fingerprint
from akc.intent.plan_step_intent import build_plan_step_intent_ref
from akc.intent.resolve import ResolvedIntentContext, resolve_compile_intent_context
from akc.intent.store import JsonFileIntentStore
from akc.ir import IRDocument
from akc.ir.schema import IRNode
from akc.memory.models import PlanState, PlanStep, now_ms


def test_ir_structural_hints_sorted_and_stable() -> None:
    t = now_ms()
    plan = PlanState(
        id="p",
        tenant_id="tenant-a",
        repo_id="repo-a",
        goal="goal",
        status="active",
        created_at_ms=t,
        updated_at_ms=t,
        steps=(
            PlanStep(
                id="step_a",
                title="generate",
                status="pending",
                order_idx=0,
                inputs={"intent_id": "intent-1"},
            ),
        ),
        next_step_id="step_a",
    )
    ir = build_ir_document_from_plan(plan=plan, intent_node_properties=None)
    h = ir_structural_hints_for_retrieval_query(ir)
    assert h == ir_structural_hints_for_retrieval_query(ir)
    assert "intent:" in h and "workflow:" in h


def test_compact_ir_node_order_matches_ir_document_to_json_order() -> None:
    t = now_ms()
    plan = PlanState(
        id="plan_x",
        tenant_id="tenant-a",
        repo_id="repo-a",
        goal="goal",
        status="active",
        created_at_ms=t,
        updated_at_ms=t,
        steps=(
            PlanStep(
                id="step_a",
                title="generate",
                status="pending",
                order_idx=0,
                inputs={"intent_id": "intent-1"},
            ),
        ),
        next_step_id="step_a",
    )
    ir = build_ir_document_from_plan(plan=plan, intent_node_properties=None)
    compact = compact_ir_document_for_prompt(ir)
    full_nodes = ir.to_json_obj()["nodes"]
    assert isinstance(full_nodes, list)
    compact_nodes = compact["nodes"]
    assert isinstance(compact_nodes, list)
    full_ids = [str(n["id"]) for n in full_nodes if isinstance(n, dict)]
    compact_ids = [str(n["id"]) for n in compact_nodes if isinstance(n, dict)]
    assert compact_ids == full_ids


def test_plan_trace_excludes_step_inputs_outputs_and_budgets() -> None:
    t = now_ms()
    secret = "SECRET_STEP_PAYLOAD_SHOULD_NOT_APPEAR_IN_PROMPT"
    plan = PlanState(
        id="plan_budget",
        tenant_id="t",
        repo_id="r",
        goal="g",
        status="active",
        created_at_ms=t,
        updated_at_ms=t,
        steps=(
            PlanStep(
                id="s1",
                title="step",
                status="in_progress",
                order_idx=0,
                inputs={"blob": secret},
                outputs={"out": "x"},
                notes="n",
            ),
        ),
        next_step_id="s1",
        budgets={"max_llm_calls": 99},
        last_feedback={"step_id": "s1"},
    )
    trace = plan_execution_trace_for_prompt(plan)
    blob = str(trace)
    assert secret not in blob
    assert "budgets" not in blob
    assert "last_feedback" not in blob
    assert "inputs" not in blob
    assert "outputs" not in blob
    assert "notes" not in blob
    assert "s1" in blob and "in_progress" in blob


def test_default_ir_generate_prompt_excludes_full_plan_json() -> None:
    t = now_ms()
    secret = "NEVER_EMBED_THIS_IN_GENERATE_PROMPT"
    plan = PlanState(
        id="plan_full",
        tenant_id="t",
        repo_id="r",
        goal="goal text",
        status="active",
        created_at_ms=t,
        updated_at_ms=t,
        steps=(
            PlanStep(
                id="st",
                title="t",
                status="pending",
                order_idx=0,
                inputs={"payload": secret},
            ),
        ),
        next_step_id="st",
        budgets={"k": "v"},
    )
    ir = build_ir_document_from_plan(plan=plan, intent_node_properties=None)
    p = DefaultIRGeneratePromptPass().build_prompt(
        ir_doc=ir,
        intent_id="intent-1",
        active_objectives=[],
        linked_constraints=[],
        active_success_criteria=[],
        goal="g",
        plan=plan,
        retrieved_context={},
        test_policy={},
        stage="generate",
    )
    assert secret not in p
    assert '"budgets"' not in p
    assert "Plan execution trace" in p
    assert "IR (compact structural graph)" in p


def test_default_ir_repair_prompt_excludes_full_plan_json() -> None:
    t = now_ms()
    secret = "NEVER_EMBED_THIS_IN_REPAIR_PROMPT"
    plan = PlanState(
        id="plan_rep",
        tenant_id="t",
        repo_id="r",
        goal="goal text",
        status="active",
        created_at_ms=t,
        updated_at_ms=t,
        steps=(
            PlanStep(
                id="st",
                title="t",
                status="pending",
                order_idx=0,
                inputs={"payload": secret},
            ),
        ),
        next_step_id="st",
    )
    ir = IRDocument(tenant_id="t", repo_id="r", nodes=())
    p = DefaultIRRepairPromptPass().build_prompt(
        ir_doc=ir,
        intent_id="intent-1",
        active_objectives=[],
        linked_constraints=[],
        active_success_criteria=[],
        goal="g",
        plan=plan,
        step_id="st",
        step_title="t",
        retrieved_context={},
        last_generation_text="diff --git a/x b/x",
        failure=FailureSummary(exit_code=1),
        verifier_feedback=None,
    )
    assert secret not in p
    assert "Plan JSON" not in p
    assert '"inputs"' not in p


def test_effective_intent_shape_auto_enables_reference_with_store_and_intent_ref(tmp_path) -> None:
    store = JsonFileIntentStore(base_dir=tmp_path)
    intent = compile_intent_spec(
        tenant_id="t",
        repo_id="r",
        goal_statement="g",
        controller_budget=None,
    )
    store.save_intent(tenant_id="t", repo_id="r", intent=intent.normalized())
    fp = compute_intent_fingerprint(intent=intent)
    ref = build_plan_step_intent_ref(
        intent=intent,
        semantic_fingerprint=fp.semantic,
        goal_text_fingerprint=fp.goal_text,
    )
    inputs = {"intent_id": intent.intent_id, "intent_ref": ref}
    assert (
        effective_intent_contract_shape_for_compile_prompts(
            policy="auto",
            intent_store=store,
            first_step_inputs=inputs,
        )
        == "reference_first"
    )
    assert (
        effective_intent_contract_shape_for_compile_prompts(
            policy="auto",
            intent_store=None,
            first_step_inputs=inputs,
        )
        == "full"
    )


def test_intent_prompt_context_reference_rows_use_fingerprints() -> None:
    t = now_ms()
    plan = PlanState(
        id="p",
        tenant_id="tenant-a",
        repo_id="repo-a",
        goal="goal",
        status="active",
        created_at_ms=t,
        updated_at_ms=t,
        steps=(
            PlanStep(
                id="step_a",
                title="generate",
                status="pending",
                order_idx=0,
                inputs={"intent_id": "intent-1"},
            ),
        ),
        next_step_id="step_a",
    )
    intent = compile_intent_spec(
        tenant_id="tenant-a",
        repo_id="repo-a",
        goal_statement="goal",
        controller_budget=None,
    )
    resolved = ResolvedIntentContext(
        spec=intent.normalized(),
        source="controller",
        stable_intent_sha256="a" * 64,
    )
    ir = build_ir_document_from_plan(
        plan=plan,
        intent_node_properties={
            "intent_id": intent.intent_id,
            "goal_statement": intent.goal_statement,
        },
        controller_intent_spec=intent,
        resolved_intent_context=resolved,
        intent_contract_shape="reference_first",
    )
    ctx = intent_prompt_context_from_ir_and_resolve(
        ir_doc=ir,
        resolved=resolved,
        reference_first=True,
    )
    assert ctx.active_objectives and "fingerprint" in ctx.active_objectives[0]
    p = DefaultIRGeneratePromptPass().build_prompt(
        ir_doc=ir,
        intent_id=intent.intent_id,
        active_objectives=ctx.active_objectives,
        linked_constraints=ctx.linked_constraints,
        active_success_criteria=ctx.active_success_criteria,
        goal="g",
        plan=plan,
        retrieved_context={},
        test_policy={},
        stage="generate",
    )
    assert "fingerprint" in p


def test_reference_intent_contract_stable_under_irrelevant_input_noise(tmp_path) -> None:
    store = JsonFileIntentStore(base_dir=tmp_path)
    intent = compile_intent_spec(
        tenant_id="t",
        repo_id="r",
        goal_statement="stable",
        controller_budget=None,
    )
    store.save_intent(tenant_id="t", repo_id="r", intent=intent.normalized())
    fp = compute_intent_fingerprint(intent=intent)
    ref = build_plan_step_intent_ref(
        intent=intent,
        semantic_fingerprint=fp.semantic,
        goal_text_fingerprint=fp.goal_text,
    )
    resolved = resolve_compile_intent_context(
        tenant_id="t",
        repo_id="r",
        inputs={
            "intent_id": intent.intent_id,
            "intent_ref": dict(ref),
            "active_objectives": [{"id": "noise", "statement": "ignore me"}],
        },
        intent_store=store,
        controller_intent_spec=None,
    )
    a = build_reference_intent_contract_for_retrieval(
        intent_spec=intent,
        resolved=resolved,
        intent_semantic_fingerprint=fp.semantic,
        intent_goal_text_fingerprint=fp.goal_text,
        operating_bounds_effective={"allow_network": False},
        first_step_inputs={"intent_ref": dict(ref), "extra": 1},
    )
    b = build_reference_intent_contract_for_retrieval(
        intent_spec=intent,
        resolved=resolved,
        intent_semantic_fingerprint=fp.semantic,
        intent_goal_text_fingerprint=fp.goal_text,
        operating_bounds_effective={"allow_network": False},
        first_step_inputs={"intent_ref": dict(ref), "extra": 2, "active_objectives": [{"id": "x"}]},
    )
    assert a == b


def test_ir_intent_knowledge_anchor_includes_intent_constraint_ids_and_knowledge_hub() -> None:
    ir = IRDocument(
        tenant_id="tenant-a",
        repo_id="repo-a",
        nodes=(
            IRNode(
                id="intent-main",
                tenant_id="tenant-a",
                kind="intent",
                name="intent_contract",
                properties={
                    "linked_constraints": [
                        {"constraint_id": "c_alpha", "kind": "hard", "summary": "alpha required"},
                    ]
                },
            ),
            IRNode(
                id="khub",
                tenant_id="tenant-a",
                kind="knowledge",
                name="hub",
                properties={
                    "knowledge_semantic_fingerprint_16": "a" * 16,
                    "knowledge_provenance_fingerprint_16": "b" * 16,
                    "knowledge_assertion_ids": ["assertion_hub_1"],
                },
            ),
        ),
    )
    anchor = ir_intent_knowledge_anchor_for_prompt(ir)
    intent_nodes = anchor["intent_nodes"]
    assert isinstance(intent_nodes, list) and intent_nodes
    row0 = intent_nodes[0]
    assert isinstance(row0, dict)
    assert row0.get("constraint_ids") == ["c_alpha"]
    kn = anchor["knowledge_nodes"]
    assert isinstance(kn, list) and kn
    assert kn[0].get("knowledge_assertion_ids") == ["assertion_hub_1"]
