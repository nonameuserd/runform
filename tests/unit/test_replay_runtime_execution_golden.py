"""Golden fixtures for ``replay_runtime_execution`` (runtime vs reconcile replay branching)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from akc.run import RuntimeReplayResult, replay_runtime_execution
from akc.run.manifest import RunManifest

_FIXTURES = Path(__file__).resolve().parent.parent / "fixtures" / "runtime_replay"


def _replay_to_golden_dict(replay: RuntimeReplayResult) -> dict[str, Any]:
    return {
        "runtime_run_id": replay.runtime_run_id,
        "mode": replay.mode,
        "transition_count": len(replay.transitions),
        "reconcile_decision_count": len(replay.reconcile_decisions),
        "terminal_health_status": replay.terminal_health_status,
        "transitions": [
            {
                "event_id": item.event.get("event_id"),
                "event_type": item.event.get("event_type"),
                "transition": dict(item.transition) if item.transition is not None else None,
                "action_decision": item.action_decision,
                "retry_count": item.retry_count,
                "budget_burn": dict(item.budget_burn) if item.budget_burn is not None else None,
            }
            for item in replay.transitions
        ],
        "reconcile_decisions": [
            {
                "resource_id": item.resource_id,
                "operation_type": item.operation_type,
                "applied": item.applied,
                "rollback_chain": list(item.rollback_chain),
                "health_status": item.health_status,
                "payload": dict(item.payload),
            }
            for item in replay.reconcile_decisions
        ],
    }


@pytest.mark.parametrize(
    ("fixture_name", "replay_mode", "expected_key"),
    [
        ("multi_action_reconcile_two_modes.json", "runtime_replay", "expected_runtime_replay"),
        ("multi_action_reconcile_two_modes.json", "reconcile_replay", "expected_reconcile_replay"),
    ],
)
def test_replay_runtime_execution_matches_golden_fixture(
    fixture_name: str, replay_mode: str, expected_key: str
) -> None:
    raw = json.loads((_FIXTURES / fixture_name).read_text(encoding="utf-8"))
    template = dict(raw["manifest_template"])
    template["replay_mode"] = replay_mode
    manifest = RunManifest.from_json_obj(template)
    replay = replay_runtime_execution(manifest=manifest, transcript=tuple(raw["transcript"]))
    actual = _replay_to_golden_dict(replay)
    expected = raw[expected_key]
    assert json.dumps(actual, sort_keys=True) == json.dumps(expected, sort_keys=True)
