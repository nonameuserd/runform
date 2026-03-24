"""Phase 5: golden IR emission, runtime bundle IR ref round-trip, kernel graph equivalence."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.artifacts.contracts import schema_id_for
from akc.artifacts.schemas import RUNTIME_BUNDLE_SCHEMA_VERSION
from akc.artifacts.validate import validate_obj
from akc.compile.controller_config import ControllerConfig, TierConfig
from akc.compile.ir_builder import build_ir_document_from_plan
from akc.compile.ir_operational_validate import validate_ir_operational_structure
from akc.memory.models import PlanState, PlanStep, now_ms
from akc.runtime.adapters.native import NativeRuntimeAdapter
from akc.runtime.events import RuntimeEventBus
from akc.runtime.kernel import RuntimeGraph, RuntimeKernel
from akc.runtime.models import (
    RuntimeBundle,
    RuntimeBundleRef,
    RuntimeContext,
    load_ir_document_from_bundle_payload,
)
from akc.runtime.scheduler import InMemoryRuntimeScheduler
from akc.runtime.state_store import InMemoryRuntimeStateStore


def test_controller_config_rejects_invalid_runtime_bundle_schema_version() -> None:
    tiers = {"medium": TierConfig(name="medium", llm_model="fake", temperature=0.0)}
    with pytest.raises(ValueError, match="runtime_bundle_schema_version"):
        ControllerConfig(tiers=tiers, runtime_bundle_schema_version=99)


def _golden_plan() -> PlanState:
    created = now_ms()
    return PlanState(
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


def test_golden_ir_emission_workflow_and_intent_contracts() -> None:
    """Fingerprint gate for PlanState→IR emission (workflow runtime + intent acceptance)."""
    ir = build_ir_document_from_plan(plan=_golden_plan(), intent_node_properties=None)
    assert validate_ir_operational_structure(ir) == ()
    # If emission changes, update this fingerprint and re-read contract expectations in docs/ir-schema.md
    assert ir.fingerprint() == ("3d672b8829632b6cbd9ea5d24809b07d91aeeb1ffc5e7b17a78a04a85b7d9c1e")
    wf = next(n for n in ir.nodes if n.kind == "workflow")
    intent_n = next(n for n in ir.nodes if n.kind == "intent")
    assert wf.contract is not None and wf.contract.contract_category == "runtime"
    assert intent_n.contract is not None and intent_n.contract.contract_category == "acceptance"


def test_runtime_bundle_system_ir_ref_round_trip(tmp_path: Path) -> None:
    ir = build_ir_document_from_plan(plan=_golden_plan(), intent_node_properties=None)
    repo = tmp_path / "scope"
    bundle_dir = repo / ".akc" / "runtime"
    ir_dir = repo / ".akc" / "ir"
    bundle_dir.mkdir(parents=True)
    ir_dir.mkdir(parents=True)
    ir_path = ir_dir / "run_x.json"
    ir.to_json_file(ir_path)
    bundle_json = bundle_dir / "run_x.runtime_bundle.json"
    # Emitted bundles use repo-relative paths (see run_runtime_bundle_pass).
    rel_ir = ".akc/ir/run_x.json"
    payload = {
        "schema_version": RUNTIME_BUNDLE_SCHEMA_VERSION,
        "schema_id": schema_id_for(kind="runtime_bundle", version=RUNTIME_BUNDLE_SCHEMA_VERSION),
        "tenant_id": ir.tenant_id,
        "repo_id": ir.repo_id,
        "run_id": "run_x",
        "system_ir_ref": {
            "path": rel_ir,
            "fingerprint": ir.fingerprint(),
            "format_version": ir.format_version,
            "schema_version": ir.schema_version,
        },
        "referenced_ir_nodes": [],
        "referenced_contracts": [],
        "spec_hashes": {"orchestration_spec_sha256": "a" * 64, "coordination_spec_sha256": "b" * 64},
        "deployment_intents": [],
        "runtime_policy_envelope": {},
    }
    bundle_json.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    issues = validate_obj(obj=payload, kind="runtime_bundle", version=RUNTIME_BUNDLE_SCHEMA_VERSION)
    assert issues == []

    loaded = load_ir_document_from_bundle_payload(
        payload=payload,
        bundle_ref=RuntimeBundleRef(
            bundle_path=str(bundle_json),
            manifest_hash="0" * 64,
            created_at=0,
            source_compile_run_id="run_x",
        ),
        context=RuntimeContext(
            tenant_id=ir.tenant_id,
            repo_id=ir.repo_id,
            run_id="run_x",
            runtime_run_id="rt1",
            policy_mode="enforce",
            adapter_id="native",
        ),
    )
    assert loaded is not None
    assert loaded.fingerprint() == ir.fingerprint()


def test_kernel_runtime_graph_equivalence_legacy_json_vs_ir_loaded() -> None:
    ir = build_ir_document_from_plan(plan=_golden_plan(), intent_node_properties=None)
    referenced = [n.to_json_obj() for n in sorted(ir.nodes, key=lambda x: x.id)]
    ctx = RuntimeContext(
        tenant_id=ir.tenant_id,
        repo_id=ir.repo_id,
        run_id="run_x",
        runtime_run_id="rt1",
        policy_mode="simulate",
        adapter_id="native",
    )
    ref = RuntimeBundleRef(
        bundle_path=".akc/runtime/run_x.runtime_bundle.json",
        manifest_hash="c" * 64,
        created_at=1,
        source_compile_run_id="run_x",
    )
    meta: dict[str, object] = {
        "referenced_ir_nodes": referenced,
        "referenced_contracts": [],
        "deployment_intents": [],
        "schema_version": 2,
        "reconcile_desired_state_source": "deployment_intents",
    }

    def build_graph(*, bundle: RuntimeBundle) -> RuntimeGraph:
        kernel = RuntimeKernel(
            context=ctx,
            bundle=bundle,
            adapter=NativeRuntimeAdapter(),
            scheduler=InMemoryRuntimeScheduler(),
            state_store=InMemoryRuntimeStateStore(),
            event_bus=RuntimeEventBus(),
        )
        return kernel.build_runtime_graph()

    g_legacy = build_graph(
        bundle=RuntimeBundle(
            context=ctx,
            ref=ref,
            nodes=(),
            contract_ids=(),
            metadata=meta,
            ir_document=None,
        )
    )
    g_ir = build_graph(
        bundle=RuntimeBundle(
            context=ctx,
            ref=ref,
            nodes=(),
            contract_ids=(),
            metadata=meta,
            ir_document=ir,
        )
    )
    assert set(g_legacy.nodes.keys()) == set(g_ir.nodes.keys())
    for nid in g_legacy.nodes:
        assert g_legacy.nodes[nid].ir_node.fingerprint() == g_ir.nodes[nid].ir_node.fingerprint()
