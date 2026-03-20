from __future__ import annotations

from akc.ir import IRDocument, IRNode, ProvenancePointer, stable_node_id
from akc.living.safe_recompile import (
    compute_changed_source_ids,
    compute_impacted_workflow_step_ids,
)


def test_compute_changed_source_ids_detects_value_and_added_changes() -> None:
    baseline = {
        # docs connector source_id is a file path in the ingestion state.
        "t1::docs::/x/a.md": {"kind": "docs", "path": "/x/a.md", "mtime_ns": 1, "size": 10},
        "t1::openapi::/x/spec.yaml": {
            "kind": "openapi_file",
            "path": "/x/spec.yaml",
            "mtime_ns": 1,
            "size": 20,
        },
    }
    current = {
        # a.md changed mtime_ns
        "t1::docs::/x/a.md": {"kind": "docs", "path": "/x/a.md", "mtime_ns": 2, "size": 10},
        # spec.yaml unchanged
        "t1::openapi::/x/spec.yaml": {
            "kind": "openapi_file",
            "path": "/x/spec.yaml",
            "mtime_ns": 1,
            "size": 20,
        },
        # new source added
        "t1::docs::/x/b.md": {"kind": "docs", "path": "/x/b.md", "mtime_ns": 1, "size": 5},
    }

    changed = compute_changed_source_ids(
        tenant_id="t1",
        baseline_sources_by_key=baseline,
        current_state_by_key=current,
    )
    assert changed == {"docs::/x/a.md", "docs::/x/b.md"}


def test_compute_impacted_workflow_step_ids_propagates_via_depends_on() -> None:
    tenant_id = "t1"
    plan_id = "plan_1"

    node1_id = stable_node_id(kind="workflow", name=f"{plan_id}:step1")
    node2_id = stable_node_id(kind="workflow", name=f"{plan_id}:step2")

    node1 = IRNode(
        id=node1_id,
        tenant_id=tenant_id,
        kind="workflow",
        name="step1",
        properties={
            "plan_id": plan_id,
            "step_id": "step1",
            "status": "done",
            "order_idx": 0,
        },
        depends_on=(),
        provenance=(
            ProvenancePointer(
                tenant_id=tenant_id,
                kind="doc_chunk",
                source_id="docs::/x/a.md",
                locator="/x/a.md",
            ),
        ),
    )
    node2 = IRNode(
        id=node2_id,
        tenant_id=tenant_id,
        kind="workflow",
        name="step2",
        properties={
            "plan_id": plan_id,
            "step_id": "step2",
            "status": "done",
            "order_idx": 1,
        },
        depends_on=(node1_id,),
        provenance=(
            ProvenancePointer(
                tenant_id=tenant_id,
                kind="doc_chunk",
                source_id="docs::/x/other.md",
                locator="/x/other.md",
            ),
        ),
    )

    ir = IRDocument(tenant_id=tenant_id, repo_id="r1", nodes=(node2, node1))
    impacted = compute_impacted_workflow_step_ids(
        ir=ir,
        changed_source_ids={"docs::/x/a.md"},
    )
    assert impacted == {"step1", "step2"}


def test_compute_impacted_workflow_step_ids_returns_empty_when_no_provenance_match() -> None:
    tenant_id = "t1"
    plan_id = "plan_1"
    node1_id = stable_node_id(kind="workflow", name=f"{plan_id}:step1")

    node1 = IRNode(
        id=node1_id,
        tenant_id=tenant_id,
        kind="workflow",
        name="step1",
        properties={"plan_id": plan_id, "step_id": "step1", "status": "done", "order_idx": 0},
        depends_on=(),
        provenance=(
            ProvenancePointer(
                tenant_id=tenant_id,
                kind="doc_chunk",
                source_id="docs::/x/a.md",
                locator="/x/a.md",
            ),
        ),
    )
    ir = IRDocument(tenant_id=tenant_id, repo_id="r1", nodes=(node1,))
    impacted = compute_impacted_workflow_step_ids(
        ir=ir,
        changed_source_ids={"docs::/x/z.md"},
    )
    assert impacted == set()
