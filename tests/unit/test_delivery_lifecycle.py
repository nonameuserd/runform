from __future__ import annotations

from akc.run.delivery_lifecycle import (
    extract_delivery_lifecycle_from_evidence,
    project_delivery_run_projection,
    resolve_delivery_target_lane,
)
from akc.run.manifest import RuntimeEvidenceRecord


def test_resolve_delivery_target_lane_cli_overrides_env() -> None:
    assert resolve_delivery_target_lane(cli_value="production", env_value="staging") == "production"
    assert resolve_delivery_target_lane(cli_value=None, env_value="prod") == "production"
    assert resolve_delivery_target_lane(cli_value=None, env_value=None) == "staging"


def test_extract_delivery_lifecycle_counts_manual_touch_and_approval_max() -> None:
    ev = (
        RuntimeEvidenceRecord(
            evidence_type="delivery_lifecycle",
            timestamp=100,
            runtime_run_id="r1",
            payload={"event": "approval_wait_started"},
        ),
        RuntimeEvidenceRecord(
            evidence_type="delivery_lifecycle",
            timestamp=50,
            runtime_run_id="r1",
            payload={"event": "approval_wait_started"},
        ),
        RuntimeEvidenceRecord(
            evidence_type="delivery_lifecycle",
            timestamp=300,
            runtime_run_id="r1",
            payload={"event": "approval_wait_completed"},
        ),
        RuntimeEvidenceRecord(
            evidence_type="delivery_lifecycle",
            timestamp=400,
            runtime_run_id="r1",
            payload={"event": "approval_wait_completed"},
        ),
        RuntimeEvidenceRecord(
            evidence_type="delivery_lifecycle",
            timestamp=10,
            runtime_run_id="r1",
            payload={"event": "manual_touch", "count": 2},
        ),
        RuntimeEvidenceRecord(
            evidence_type="delivery_lifecycle",
            timestamp=11,
            runtime_run_id="r1",
            payload={"event": "manual_touch"},
        ),
    )
    ts, touches = extract_delivery_lifecycle_from_evidence(ev)
    assert ts["approval_wait_started_at"] == 50
    assert ts["approval_wait_completed_at"] == 400
    assert touches == 3


def test_project_delivery_run_projection_staging_lane() -> None:
    ev = (
        RuntimeEvidenceRecord(
            evidence_type="reconcile_outcome",
            timestamp=1,
            runtime_run_id="r1",
            payload={
                "resource_id": "x",
                "operation_type": "apply",
                "applied": True,
                "health_status": "healthy",
            },
        ),
    )
    proj = project_delivery_run_projection(
        evidence=ev,
        delivery_lane="staging",
        record_started_at_ms=10,
        terminal_health_status="healthy",
        runtime_healthy_at=200,
    )
    assert proj["timestamps"]["staging_healthy_at"] == 200
    assert "prod_healthy_at" not in proj["timestamps"]


def test_project_delivery_run_projection_production_lane_sets_deploy_start() -> None:
    ev = (
        RuntimeEvidenceRecord(
            evidence_type="reconcile_outcome",
            timestamp=1,
            runtime_run_id="r1",
            payload={
                "resource_id": "x",
                "operation_type": "apply",
                "applied": True,
                "health_status": "healthy",
            },
        ),
    )
    proj = project_delivery_run_projection(
        evidence=ev,
        delivery_lane="production",
        record_started_at_ms=99,
        terminal_health_status="healthy",
        runtime_healthy_at=500,
    )
    assert proj["timestamps"]["prod_deploy_started_at"] == 99
    assert proj["timestamps"]["prod_healthy_at"] == 500
