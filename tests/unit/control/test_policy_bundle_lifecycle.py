from __future__ import annotations

import json
from pathlib import Path

from akc.control.control_audit import control_audit_jsonl_path
from akc.control.fleet_config import FleetShardConfig
from akc.control.policy_bundle import distribute_policy_bundle_document, policy_bundle_drift_report


def _bundle(*, revision_id: str, rollout_stage: str = "enforce") -> dict[str, object]:
    return {
        "schema_kind": "akc_policy_bundle",
        "version": 1,
        "revision_id": revision_id,
        "rollout_stage": rollout_stage,
        "pins": {"opa_bundle_sha256": "a" * 64},
        "provenance": {
            "revision": revision_id,
            "root_owner": "platform-security",
            "signature": {"key_id": "k1", "algorithm": "ed25519", "value": "sig"},
        },
    }


def _shard(root: Path, sid: str) -> FleetShardConfig:
    return FleetShardConfig(id=sid, outputs_root=root, tenant_allowlist=("*",))


def test_policy_bundle_distribute_and_activate_with_rollback_marker(tmp_path: Path) -> None:
    s1 = tmp_path / "s1"
    s2 = tmp_path / "s2"
    shards = (_shard(s1, "s1"), _shard(s2, "s2"))

    first = distribute_policy_bundle_document(
        shards=shards,
        tenant_id="t1",
        repo_id="repo1",
        document=_bundle(revision_id="rev-1"),
        actor="ops",
        activate=True,
        now_ms=1000,
        request_id="req-1",
    )
    assert len(first) == 2
    assert all(bool(x["activation_requested"]) for x in first)
    assert all(not bool(x["rollback_marker"]) for x in first)

    second = distribute_policy_bundle_document(
        shards=shards,
        tenant_id="t1",
        repo_id="repo1",
        document=_bundle(revision_id="rev-2"),
        actor="ops",
        activate=True,
        now_ms=2000,
        request_id="req-2",
    )
    assert len(second) == 2
    assert all(bool(x["rollback_marker"]) for x in second)
    assert all(str(x["rollback_from_revision_id"]) == "rev-1" for x in second)

    marker_path = s1 / "t1" / "repo1" / ".akc" / "control" / "policy_bundle.activation.json"
    marker = json.loads(marker_path.read_text(encoding="utf-8"))
    assert marker["bundle_revision_id"] == "rev-2"
    assert marker["rollback_marker"] is True
    assert marker["rollback_from_revision_id"] == "rev-1"

    audit_path = control_audit_jsonl_path(outputs_root=s1, tenant_id="t1")
    lines = [json.loads(line) for line in audit_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert any(item["action"] == "policy_bundle.distribute" for item in lines)
    assert any(item["action"] == "policy_bundle.activate" for item in lines)
    activate_rows = [item for item in lines if item["action"] == "policy_bundle.activate"]
    assert activate_rows[-1]["details"]["rollback_marker"] is True


def test_policy_bundle_drift_report_detects_version_divergence(tmp_path: Path) -> None:
    s1 = tmp_path / "s1"
    s2 = tmp_path / "s2"
    shards = (_shard(s1, "s1"), _shard(s2, "s2"))
    distribute_policy_bundle_document(
        shards=shards,
        tenant_id="t1",
        repo_id="repo1",
        document=_bundle(revision_id="rev-1"),
        actor="ops",
        activate=True,
        now_ms=1000,
        request_id="req-1",
    )
    distribute_policy_bundle_document(
        shards=(_shard(s2, "s2"),),
        tenant_id="t1",
        repo_id="repo1",
        document=_bundle(revision_id="rev-2"),
        actor="ops",
        activate=True,
        now_ms=2000,
        request_id="req-2",
    )

    report = policy_bundle_drift_report(shards=shards, tenant_id="t1", repo_id="repo1", generated_at_ms=3000)
    assert report["drift_detected"] is True
    assert sorted(report["distinct_versions"]) == ["rev-1", "rev-2"]
    rows = report["shards"]
    assert isinstance(rows, list) and len(rows) == 2
    assert any(bool(item["diverged"]) for item in rows if item["shard_id"] == "s2")
