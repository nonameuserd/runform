"""Deterministic workflow node ordering for coordination emit and runtime step resolution.

Compile-time ``run_agent_coordination_pass`` and runtime ``resolve_step_to_ir_node_id`` must
use the same ordering so ``workflow_<NNN>`` indices align with IR workflow nodes.

Parallel layers:
- Nodes sharing the same ``order_idx`` (IR ``properties.order_idx``) are one parallel layer.
- Optional ``properties.coordination_parallel_group`` (non-empty string): nodes with the same
  value share a layer regardless of ``order_idx`` (deterministic tie-break: ``node.id``).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from akc.ir.schema import IRNode


def workflow_coordination_layer_key(properties: Mapping[str, object]) -> str:
    """Stable key for parallel fork layers (sorts lexicographically with ``o:`` / ``g:`` prefixes)."""

    raw = properties.get("coordination_parallel_group")
    if isinstance(raw, str) and raw.strip():
        return f"g:{raw.strip()}"
    oi = properties.get("order_idx", 0)
    oi_int = int(oi) if isinstance(oi, int) else 0
    return f"o:{oi_int:09d}"


def sorted_workflow_nodes_for_coordination_emit(nodes: Sequence[IRNode]) -> tuple[IRNode, ...]:
    """Workflow nodes only, ordered by coordination layer key then node id."""

    wf = [n for n in nodes if n.kind == "workflow"]
    return tuple(sorted(wf, key=lambda n: (workflow_coordination_layer_key(n.properties), n.id)))
