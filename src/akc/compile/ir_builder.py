from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from akc.ir import IRDocument, IRNode, ProvenancePointer, stable_node_id
from akc.memory.models import PlanState, require_non_empty


def build_ir_document_from_plan(*, plan: PlanState) -> IRDocument:
    """Build an IRDocument representing the current plan graph.

    This is the shared "PlanState -> IR" builder so compiler passes can consume IR
    instead of owning ad-hoc IR emission logic.
    """

    require_non_empty(plan.tenant_id, name="plan.tenant_id")
    require_non_empty(plan.repo_id, name="plan.repo_id")

    nodes: list[IRNode] = []
    prev_id: str | None = None

    # Deterministic node ordering by `order_idx`.
    for step in sorted(plan.steps, key=lambda s: int(s.order_idx)):
        node_id = stable_node_id(kind="workflow", name=f"{plan.id}:{step.id}:{step.title}")

        provenance: tuple[ProvenancePointer, ...] = ()
        step_outputs: Mapping[str, Any] = dict(step.outputs or {})
        snap_raw = step_outputs.get("retrieval_snapshot")
        if isinstance(snap_raw, dict):
            prov_raw = snap_raw.get("provenance")
            if isinstance(prov_raw, Sequence) and not isinstance(prov_raw, (str, bytes)):
                parsed: list[ProvenancePointer] = []
                for p in prov_raw:
                    if isinstance(p, dict):
                        try:
                            parsed.append(ProvenancePointer.from_json_obj(p))
                        except Exception:
                            # Best-effort IR emission; drift mapping can still
                            # fall back to conservative recompilation.
                            continue
                provenance = tuple(parsed)

        nodes.append(
            IRNode(
                id=node_id,
                tenant_id=plan.tenant_id,
                kind="workflow",
                name=step.title,
                properties=_build_ir_node_properties(
                    plan_id=plan.id,
                    step_id=step.id,
                    status=step.status,
                    order_idx=step.order_idx,
                    step_outputs=step_outputs,
                ),
                depends_on=(prev_id,) if prev_id is not None else (),
                provenance=provenance,
            )
        )
        prev_id = node_id

    return IRDocument(tenant_id=plan.tenant_id, repo_id=plan.repo_id, nodes=tuple(nodes))


def _build_ir_node_properties(
    *,
    plan_id: str,
    step_id: str,
    status: Any,
    order_idx: int,
    step_outputs: Mapping[str, Any],
) -> dict[str, Any]:
    """Attach patch-layer provenance metadata into IR node properties."""

    props: dict[str, Any] = {
        "plan_id": plan_id,
        "step_id": step_id,
        "status": status,
        "order_idx": order_idx,
    }

    last_prompt_key = step_outputs.get("last_prompt_key")
    if isinstance(last_prompt_key, str) and last_prompt_key.strip():
        props["last_prompt_key"] = last_prompt_key.strip()

    last_patch_sha256 = step_outputs.get("last_patch_sha256")
    if isinstance(last_patch_sha256, str) and last_patch_sha256.strip():
        props["last_patch_sha256"] = last_patch_sha256.strip()

    best_candidate = step_outputs.get("best_candidate")
    if isinstance(best_candidate, dict):
        touched_paths_raw = best_candidate.get("touched_paths")
        if isinstance(touched_paths_raw, list):
            touched_paths = [str(x) for x in touched_paths_raw if str(x).strip()]
            if touched_paths:
                props["last_touched_paths"] = touched_paths

    return props
