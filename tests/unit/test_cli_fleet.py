from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.cli import main
from akc.control.operations_index import OperationsIndex
from akc.run.manifest import RunManifest


def _hex64(c: str = "a") -> str:
    return (c * 64)[:64]


def test_cli_fleet_serve_smoke(tmp_path: Path) -> None:
    m = RunManifest(
        run_id="run-1",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64(),
        replay_mode="live",
    )
    scope = tmp_path / "t1" / "repo1"
    rd = scope / ".akc" / "run"
    rd.mkdir(parents=True, exist_ok=True)
    mp = rd / "run-1.manifest.json"
    mp.write_text(json.dumps(m.to_json_obj()), encoding="utf-8")
    OperationsIndex.upsert_from_manifest_path(mp, outputs_root=tmp_path)
    cfg_path = tmp_path / "fleet.json"
    cfg_path.write_text(
        json.dumps(
            {
                "version": 1,
                "allow_anonymous_read": True,
                "shards": [{"id": "s0", "outputs_root": str(tmp_path)}],
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(SystemExit) as exc:
        main(["fleet", "serve-smoke", "--config", str(cfg_path)])
    assert exc.value.code == 0


def test_cli_fleet_policy_bundle_distribute_and_drift(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    s1 = tmp_path / "s1"
    s2 = tmp_path / "s2"
    cfg_path = tmp_path / "fleet.json"
    cfg_path.write_text(
        json.dumps(
            {
                "version": 1,
                "allow_anonymous_read": True,
                "shards": [
                    {"id": "s1", "outputs_root": str(s1)},
                    {"id": "s2", "outputs_root": str(s2)},
                ],
            }
        ),
        encoding="utf-8",
    )
    bundle_path = tmp_path / "bundle.json"
    bundle_path.write_text(
        json.dumps(
            {
                "schema_kind": "akc_policy_bundle",
                "version": 1,
                "revision_id": "rev-1",
                "rollout_stage": "enforce",
                "pins": {"opa_bundle_sha256": "a" * 64},
                "provenance": {"revision": "rev-1", "root_owner": "secops"},
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
                str(cfg_path),
                "--tenant-id",
                "t1",
                "--repo-id",
                "repo1",
                "--from-file",
                str(bundle_path),
                "--activate",
                "--actor",
                "ops",
            ]
        )
    assert exc.value.code == 0
    dist = json.loads(capsys.readouterr().out)
    assert dist["write_count"] == 2

    s2_bundle = s2 / "t1" / "repo1" / ".akc" / "control" / "policy_bundle.json"
    s2_doc = json.loads(s2_bundle.read_text(encoding="utf-8"))
    s2_doc["revision_id"] = "rev-2"
    s2_bundle.write_text(json.dumps(s2_doc, sort_keys=True) + "\n", encoding="utf-8")

    with pytest.raises(SystemExit) as exc2:
        main(
            [
                "fleet",
                "policy-bundle",
                "drift",
                "--config",
                str(cfg_path),
                "--tenant-id",
                "t1",
                "--repo-id",
                "repo1",
            ]
        )
    assert exc2.value.code == 2
    drift = json.loads(capsys.readouterr().out)
    assert drift["drift_detected"] is True
