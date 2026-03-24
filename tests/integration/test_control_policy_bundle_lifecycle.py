"""Integration: fleet policy-bundle distribution/activation lifecycle and drift report."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.cli import main


def test_fleet_policy_bundle_lifecycle_end_to_end(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    shard_a = tmp_path / "shard-a"
    shard_b = tmp_path / "shard-b"
    cfg = tmp_path / "fleet.json"
    cfg.write_text(
        json.dumps(
            {
                "version": 1,
                "allow_anonymous_read": True,
                "shards": [
                    {"id": "a", "outputs_root": str(shard_a)},
                    {"id": "b", "outputs_root": str(shard_b)},
                ],
            }
        ),
        encoding="utf-8",
    )
    bundle = tmp_path / "policy_bundle.json"
    bundle.write_text(
        json.dumps(
            {
                "schema_kind": "akc_policy_bundle",
                "version": 1,
                "revision_id": "rev-10",
                "rollout_stage": "enforce",
                "pins": {"opa_bundle_sha256": "f" * 64},
                "provenance": {
                    "revision": "rev-10",
                    "root_owner": "governance",
                    "signature": {"key_id": "k1", "algorithm": "ed25519", "value": "sig"},
                },
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as exc:
        main(
            [
                "fleet",
                "policy-bundle",
                "distribute",
                "--config",
                str(cfg),
                "--tenant-id",
                "t1",
                "--repo-id",
                "repo1",
                "--from-file",
                str(bundle),
                "--activate",
            ]
        )
    assert exc.value.code == 0
    out = json.loads(capsys.readouterr().out)
    assert out["write_count"] == 2

    with pytest.raises(SystemExit) as exc2:
        main(
            [
                "fleet",
                "policy-bundle",
                "drift",
                "--config",
                str(cfg),
                "--tenant-id",
                "t1",
                "--repo-id",
                "repo1",
            ]
        )
    assert exc2.value.code == 0
    drift = json.loads(capsys.readouterr().out)
    assert drift["drift_detected"] is False
    assert drift["reference_version"] == "rev-10"
