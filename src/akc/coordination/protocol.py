"""Load coordination JSON from disk and schedule steps with the same logic as the runtime kernel."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from akc.coordination.models import (
    CoordinationParseError,
    CoordinationSchedule,
    ParsedCoordinationSpec,
    parse_coordination_obj,
    schedule_coordination_layers,
)


def load_coordination_spec_file(
    *,
    path: str | Path,
    tenant_id: str,
    repo_id: str,
) -> ParsedCoordinationSpec:
    """Parse ``path``, enforce tenant/repo scope, and validate SDK-required fields."""

    raw_text = Path(path).read_text(encoding="utf-8")
    obj = json.loads(raw_text)
    if not isinstance(obj, Mapping):
        raise CoordinationParseError("coordination spec must be a JSON object")
    roles = obj.get("agent_roles")
    if not isinstance(roles, list) or not roles:
        raise CoordinationParseError("agent_roles must be a non-empty list")
    bindings_raw = obj.get("orchestration_bindings")
    if not isinstance(bindings_raw, list) or not bindings_raw:
        raise CoordinationParseError("orchestration_bindings must be a non-empty list")
    spec = parse_coordination_obj(obj)
    if spec.tenant_id != tenant_id or spec.repo_id != repo_id:
        raise CoordinationParseError("tenant/repo scope mismatch for coordination spec")
    if not spec.orchestration_bindings:
        raise CoordinationParseError("orchestration_bindings must be a non-empty list")
    return spec


def schedule_coordination(spec: ParsedCoordinationSpec) -> CoordinationSchedule:
    """Deterministic topological layers (matches :class:`~akc.coordination.models.CoordinationScheduler`)."""

    return schedule_coordination_layers(spec)


def coordination_schedule_to_jsonable(schedule: CoordinationSchedule) -> dict[str, Any]:
    """JSON-serialize a schedule (for CLIs and cross-language tests)."""

    out: dict[str, Any] = {
        "layers": [
            {"layer_index": int(layer.layer_index), "step_ids": [str(s) for s in layer.step_ids]}
            for layer in schedule.layers
        ],
        "step_order": [str(s) for s in schedule.step_order],
    }
    if schedule.layer_reason:
        out["layer_reason"] = [str(x) for x in schedule.layer_reason]
    if schedule.lowered_precedence_edges:
        out["lowered_precedence_edges"] = [dict(x) for x in schedule.lowered_precedence_edges]
    return out
