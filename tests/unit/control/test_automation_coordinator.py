from __future__ import annotations

import json
from pathlib import Path

from akc.control.automation_coordinator import run_fleet_automation_coordinator
from akc.control.fleet_config import FleetConfig, FleetShardConfig
from akc.control.operations_index import OperationsIndex, operations_sqlite_path
from akc.run.manifest import PassRecord, RunManifest, RuntimeEvidenceRecord


def _hex64(c: str = "a") -> str:
    return (c * 64)[:64]


def _seed_manifest(*, outputs_root: Path, run_id: str, failed: bool = False) -> None:
    scope = outputs_root / "t1" / "repo1"
    run_dir = scope / ".akc" / "run"
    run_dir.mkdir(parents=True, exist_ok=True)
    passes = (PassRecord(name="verify", status="failed"),) if failed else ()
    evidence = (
        RuntimeEvidenceRecord(
            evidence_type="terminal_health",
            timestamp=1,
            runtime_run_id="rt1",
            payload={"aggregate": True, "health_status": "failed"},
        ),
    )
    manifest = RunManifest(
        run_id=run_id,
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64("1"),
        replay_mode="live",
        stable_intent_sha256=_hex64("2"),
        passes=passes,
        runtime_evidence=evidence,
    )
    man_path = run_dir / f"{run_id}.manifest.json"
    man_path.write_text(json.dumps(manifest.to_json_obj()), encoding="utf-8")
    OperationsIndex.upsert_from_manifest_path(man_path, outputs_root=outputs_root)


def _fleet_cfg(shard_root: Path) -> FleetConfig:
    return FleetConfig(
        version=1,
        shards=(
            FleetShardConfig(
                id="s1",
                outputs_root=shard_root,
                tenant_allowlist=("*",),
            ),
        ),
        api_tokens=(),
        allow_anonymous_read=False,
        webhooks=(),
        webhook_state_path=None,
    )


def test_automation_dedupe_checkpoint_prevents_reexecution(tmp_path: Path) -> None:
    _seed_manifest(outputs_root=tmp_path, run_id="r1")
    cfg = _fleet_cfg(tmp_path)

    first = run_fleet_automation_coordinator(
        cfg,
        tenants=["t1"],
        actions=("metadata_tag_write",),
        max_actions=10,
        max_retries=3,
        policy_version="p1",
        now_ms=1000,
    )
    assert any(o.action == "metadata_tag_write" and o.status == "executed" for o in first)

    second = run_fleet_automation_coordinator(
        cfg,
        tenants=["t1"],
        actions=("metadata_tag_write",),
        max_actions=10,
        max_retries=3,
        policy_version="p1",
        now_ms=2000,
    )
    assert any(o.action == "metadata_tag_write" and o.status == "skipped" for o in second)

    db = operations_sqlite_path(outputs_root=tmp_path, tenant_id="t1")
    idx = OperationsIndex(db)
    run = idx.get_run(tenant_id="t1", repo_id="repo1", run_id="r1")
    assert run is not None
    labels = run.get("labels")
    assert isinstance(labels, dict)
    assert labels.get("fleet.automated") == "true"


def test_automation_dead_letter_after_bounded_retries(tmp_path: Path) -> None:
    _seed_manifest(outputs_root=tmp_path, run_id="r-fail", failed=True)
    cfg = _fleet_cfg(tmp_path)

    first = run_fleet_automation_coordinator(
        cfg,
        tenants=["t1"],
        actions=("incident_workflow_orchestration",),
        max_actions=10,
        max_retries=2,
        base_backoff_ms=50,
        policy_version="p1",
        now_ms=1000,
    )
    assert any(o.status == "failed" and o.checkpoint_status == "pending" for o in first)

    second = run_fleet_automation_coordinator(
        cfg,
        tenants=["t1"],
        actions=("incident_workflow_orchestration",),
        max_actions=10,
        max_retries=2,
        base_backoff_ms=50,
        policy_version="p1",
        now_ms=1200,
    )
    dead = [o for o in second if o.checkpoint_status == "dead_letter"]
    assert dead
    rel = dead[0].dead_letter_relpath
    assert isinstance(rel, str) and rel
    dead_letter_path = tmp_path / "t1" / rel
    assert dead_letter_path.is_file()
