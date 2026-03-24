from __future__ import annotations

import json
from pathlib import Path

from akc.artifacts.contracts import apply_schema_envelope
from akc.artifacts.validate import validate_artifact_json
from akc.control.otel_export import autopilot_scope_event_to_export_obj, export_obj_to_json_line


def _sample_budget_state_dict(*, human_escalation_required: bool = False) -> dict[str, object]:
    return {
        "window_start_ms": 1,
        "mutations_count": 0,
        "rollbacks_count": 0,
        "consecutive_failures": 0,
        "active_rollouts": 0,
        "human_escalation_required": human_escalation_required,
        "cooldown_until_ms": 0,
    }


def test_autopilot_decision_artifact_validates() -> None:
    body: dict[str, object] = {
        "tenant_id": "t1",
        "repo_id": "r1",
        "controller_id": "c1",
        "env_profile": "staging",
        "decision_at_ms": 100,
        "attempt_id": "a1",
        "decision": "escalation_hold",
        "budget_state": _sample_budget_state_dict(human_escalation_required=True),
    }
    apply_schema_envelope(obj=body, kind="autopilot_decision", version=1)
    assert validate_artifact_json(obj=body, kind="autopilot_decision", version=1, enabled=True) == []


def test_autopilot_human_escalation_artifact_validates() -> None:
    body: dict[str, object] = {
        "tenant_id": "t1",
        "repo_id": "r1",
        "generated_at_ms": 10,
        "reason": "autonomy_budget_escalation",
        "budget_state": _sample_budget_state_dict(),
    }
    apply_schema_envelope(obj=body, kind="autopilot_human_escalation", version=1)
    assert validate_artifact_json(obj=body, kind="autopilot_human_escalation", version=1, enabled=True) == []


def test_autopilot_otel_export_line_json() -> None:
    rec = autopilot_scope_event_to_export_obj(
        tenant_id="t1",
        repo_id="r1",
        span_name="akc.autopilot.compile_failed",
        attributes={
            "akc.autopilot.decision": "compile_failed",
            "akc.autopilot.human_escalation_required": False,
            "akc.autopilot.consecutive_failures": 2,
        },
        now_ms=1000,
    )
    line = export_obj_to_json_line(rec)
    parsed = json.loads(line)
    assert parsed["source"] == "runtime.autopilot_scope"
    assert parsed["resource"]["attributes"]["akc.tenant_id"] == "t1"
    assert parsed["resource"]["attributes"]["akc.repo_id"] == "r1"


def test_operator_alert_contract_fixture_paths_exist() -> None:
    repo = Path(__file__).resolve().parents[2]
    contract_path = repo / "configs" / "slo" / "operator_alert_contract.v1.json"
    raw = json.loads(contract_path.read_text(encoding="utf-8"))
    fix_rel = str(raw.get("threshold_fixture") or "").strip()
    assert fix_rel
    assert (repo / fix_rel).is_file()
