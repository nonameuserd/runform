from __future__ import annotations

import json
import os
import time
from pathlib import Path

from akc.control.fleet_catalog import fleet_get_run, fleet_list_runs_merged, shard_accepts_tenant
from akc.control.fleet_config import FleetShardConfig
from akc.control.operations_index import OperationsIndex
from akc.run.manifest import RunManifest


def _hex64(c: str = "a") -> str:
    return (c * 64)[:64]


def _write_manifest(*, root: Path, manifest: RunManifest, tenant: str, repo: str) -> Path:
    scope = root / tenant / repo
    run_dir = scope / ".akc" / "run"
    run_dir.mkdir(parents=True, exist_ok=True)
    mp = run_dir / f"{manifest.run_id}.manifest.json"
    mp.write_text(json.dumps(manifest.to_json_obj()), encoding="utf-8")
    return mp


def test_shard_accepts_tenant_allowlist() -> None:
    sh = FleetShardConfig(id="s", outputs_root=Path("/tmp"), tenant_allowlist=("t1",))
    assert shard_accepts_tenant(sh, "t1") is True
    assert shard_accepts_tenant(sh, "t2") is False
    star = FleetShardConfig(id="s", outputs_root=Path("/tmp"), tenant_allowlist=("*",))
    assert shard_accepts_tenant(star, "any") is True


def test_fleet_merge_order_across_shards(tmp_path: Path) -> None:
    root_a = tmp_path / "cell_a"
    root_b = tmp_path / "cell_b"
    ma = RunManifest(
        run_id="old-run",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64(),
        replay_mode="live",
    )
    mb = RunManifest(
        run_id="new-run",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64("b"),
        replay_mode="live",
    )
    pa = _write_manifest(root=root_a, manifest=ma, tenant="t1", repo="repo1")
    pb = _write_manifest(root=root_b, manifest=mb, tenant="t1", repo="repo1")
    old_t = time.time() - 60
    new_t = time.time()
    os.utime(pa, (old_t, old_t))
    os.utime(pb, (new_t, new_t))
    OperationsIndex.upsert_from_manifest_path(pa, outputs_root=root_a)
    OperationsIndex.upsert_from_manifest_path(pb, outputs_root=root_b)
    shards = (
        FleetShardConfig(id="a", outputs_root=root_a, tenant_allowlist=("*",)),
        FleetShardConfig(id="b", outputs_root=root_b, tenant_allowlist=("*",)),
    )
    merged = fleet_list_runs_merged(shards, tenant_id="t1", limit=10)
    assert [r["run_id"] for r in merged] == ["new-run", "old-run"]
    assert merged[0]["shard_id"] == "b"
    assert merged[1]["shard_id"] == "a"


def test_fleet_get_run_first_shard_wins(tmp_path: Path) -> None:
    root_a = tmp_path / "a"
    root_b = tmp_path / "b"
    m = RunManifest(
        run_id="same",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64(),
        replay_mode="live",
    )
    pa = _write_manifest(root=root_a, manifest=m, tenant="t1", repo="repo1")
    pb = _write_manifest(root=root_b, manifest=m, tenant="t1", repo="repo1")
    OperationsIndex.upsert_from_manifest_path(pa, outputs_root=root_a)
    OperationsIndex.upsert_from_manifest_path(pb, outputs_root=root_b)
    shards = (
        FleetShardConfig(id="first", outputs_root=root_a, tenant_allowlist=("*",)),
        FleetShardConfig(id="second", outputs_root=root_b, tenant_allowlist=("*",)),
    )
    row = fleet_get_run(shards, tenant_id="t1", repo_id="repo1", run_id="same")
    assert row is not None
    assert row["shard_id"] == "first"
