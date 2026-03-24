"""Map orchestration ``step_id`` values to IR node ids for :class:`~akc.runtime.models.RuntimeAction` routing."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

from akc.ir.schema import IRDocument
from akc.ir.workflow_order import sorted_workflow_nodes_for_coordination_emit
from akc.memory.models import require_non_empty


class CoordinationStepResolveError(ValueError):
    """Cannot resolve a coordination step to a runtime IR node."""


_WORKFLOW_STEP_RE = re.compile(r"^workflow_(\d+)$")


def resolve_step_to_role_name(
    *,
    step_id: str,
    orchestration_obj: Mapping[str, Any] | None,
) -> str | None:
    """Return orchestration step ``role`` / ``role_name`` for ``step_id``, if present."""

    sid = str(step_id).strip()
    if not sid or orchestration_obj is None:
        return None
    steps = orchestration_obj.get("steps")
    if not isinstance(steps, Sequence) or isinstance(steps, (str, bytes)):
        return None
    for step in steps:
        if not isinstance(step, Mapping):
            continue
        if str(step.get("step_id", "")).strip() != sid:
            continue
        for key in ("role", "role_name"):
            raw = step.get(key)
            if isinstance(raw, str) and raw.strip():
                return raw.strip()
    return None


def resolve_step_to_ir_node_id(
    *,
    step_id: str,
    ir_document: IRDocument | None,
    orchestration_obj: Mapping[str, Any] | None,
) -> str:
    """Resolve ``step_id`` to an IR node id.

    Precedence:
    1. Orchestration step ``inputs.ir_node_id`` (or legacy top-level ``ir_node_id``).
    2. Deterministic index mapping from ``workflow_<NNN>`` to workflow IR nodes sorted by
       :func:`akc.ir.workflow_order.sorted_workflow_nodes_for_coordination_emit` (parallel layer
       key then node id).
    """

    sid = str(step_id).strip()
    require_non_empty(sid, name="step_id")

    if orchestration_obj is not None:
        steps = orchestration_obj.get("steps")
        if isinstance(steps, Sequence) and not isinstance(steps, (str, bytes)):
            for step in steps:
                if not isinstance(step, Mapping):
                    continue
                if str(step.get("step_id", "")).strip() != sid:
                    continue
                irn = _read_ir_node_id_from_step(step)
                if irn:
                    return irn.strip()

    if ir_document is not None:
        m = _WORKFLOW_STEP_RE.match(sid)
        if m:
            idx = int(m.group(1))

            wf_nodes = list(sorted_workflow_nodes_for_coordination_emit(ir_document.nodes))
            if 0 <= idx < len(wf_nodes):
                return wf_nodes[idx].id

    raise CoordinationStepResolveError(f"cannot resolve coordination step_id {sid!r} to an IR node id")


def _read_ir_node_id_from_step(step: Mapping[str, Any]) -> str | None:
    inputs = step.get("inputs")
    if isinstance(inputs, Mapping):
        v = inputs.get("ir_node_id")
        if isinstance(v, str) and v.strip():
            return v.strip()
    v = step.get("ir_node_id")
    if isinstance(v, str) and v.strip():
        return v.strip()
    return None
