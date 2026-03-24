from __future__ import annotations

from pathlib import Path

import akc.compile  # noqa: F401 — initialize compile graph before `akc.intent` package __init__
from akc.intent import compile_intent_spec, compute_intent_fingerprint, stable_intent_sha256
from akc.intent.plan_step_intent import (
    INTENT_REF_INPUT_KEY,
    build_plan_step_intent_ref,
    load_intent_verified_from_plan_step_inputs,
)
from akc.intent.store import JsonFileIntentStore
from akc.memory.models import PlanState, PlanStep


def test_build_and_load_plan_step_intent_ref_round_trip(tmp_path: Path) -> None:
    store = JsonFileIntentStore(base_dir=tmp_path)
    intent = compile_intent_spec(
        tenant_id="t1",
        repo_id="r1",
        goal_statement="Ship the feature",
        controller_budget=None,
    )
    fp = compute_intent_fingerprint(intent=intent)
    ref = build_plan_step_intent_ref(
        intent=intent,
        semantic_fingerprint=fp.semantic,
        goal_text_fingerprint=fp.goal_text,
    )
    assert ref["intent_id"] == intent.intent_id
    assert ref["stable_intent_sha256"] == stable_intent_sha256(intent=intent.normalized())

    store.save_intent(tenant_id="t1", repo_id="r1", intent=intent.normalized())
    inputs = {"intent_id": intent.intent_id, INTENT_REF_INPUT_KEY: ref}
    loaded = load_intent_verified_from_plan_step_inputs(
        tenant_id="t1",
        repo_id="r1",
        inputs=inputs,
        intent_store=store,
    )
    assert loaded is not None
    assert loaded.intent_id == intent.intent_id


def test_load_intent_ref_rejects_stable_hash_mismatch(tmp_path: Path) -> None:
    store = JsonFileIntentStore(base_dir=tmp_path)
    intent = compile_intent_spec(
        tenant_id="t1",
        repo_id="r1",
        goal_statement="A",
        controller_budget=None,
    )
    store.save_intent(tenant_id="t1", repo_id="r1", intent=intent.normalized())
    fp = compute_intent_fingerprint(intent=intent)
    ref = build_plan_step_intent_ref(
        intent=intent,
        semantic_fingerprint=fp.semantic,
        goal_text_fingerprint=fp.goal_text,
    )
    ref_bad = dict(ref)
    ref_bad["stable_intent_sha256"] = "a" * 64
    out = load_intent_verified_from_plan_step_inputs(
        tenant_id="t1",
        repo_id="r1",
        inputs={INTENT_REF_INPUT_KEY: ref_bad},
        intent_store=store,
    )
    assert out is None


def test_ir_builder_hydrates_intent_node_from_store(tmp_path: Path) -> None:
    from akc.compile.ir_builder import build_ir_document_from_plan

    store = JsonFileIntentStore(base_dir=tmp_path)
    intent = compile_intent_spec(
        tenant_id="t1",
        repo_id="r1",
        goal_statement="Goal with constraint",
        controller_budget=None,
    )
    store.save_intent(tenant_id="t1", repo_id="r1", intent=intent.normalized())
    fp = compute_intent_fingerprint(intent=intent)
    ref = build_plan_step_intent_ref(
        intent=intent,
        semantic_fingerprint=fp.semantic,
        goal_text_fingerprint=fp.goal_text,
    )
    plan = PlanState(
        id="p1",
        tenant_id="t1",
        repo_id="r1",
        goal="Goal with constraint",
        status="active",
        created_at_ms=0,
        updated_at_ms=0,
        steps=(
            PlanStep(
                id="s1",
                title="step",
                status="pending",
                order_idx=0,
                inputs={"intent_id": intent.intent_id, INTENT_REF_INPUT_KEY: ref},
                outputs={},
            ),
        ),
        next_step_id="s1",
    )
    ir = build_ir_document_from_plan(plan=plan, intent_node_properties=None, intent_store=store)
    intent_nodes = [n for n in ir.nodes if n.kind == "intent"]
    assert len(intent_nodes) == 1
    props = intent_nodes[0].properties
    assert isinstance(props.get("active_objectives"), list)
    assert len(props["active_objectives"]) >= 1
