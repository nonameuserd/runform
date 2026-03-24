"""Integration: artifact lowering order and output hash wiring for a seeded compile."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from akc.cli import main
from akc.pass_registry import ARTIFACT_PASS_ORDER
from akc.run import RunManifest
from tests.unit.test_cli_compile import (
    _executor_cwd,
    _seed_plan_with_one_step,
    _write_minimal_repo,
)


def _sha256_hex_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def test_seeded_compile_manifest_artifact_pass_order_matches_registry(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    tenant_id = "tenant-a"
    repo_id = "repo-a"
    outputs_root = tmp_path / "out"
    base = outputs_root / tenant_id / repo_id

    _write_minimal_repo(_executor_cwd(outputs_root, tenant_id, repo_id))
    _seed_plan_with_one_step(tenant_id=tenant_id, repo_id=repo_id, outputs_root=outputs_root)

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "compile",
                "--tenant-id",
                tenant_id,
                "--repo-id",
                repo_id,
                "--outputs-root",
                str(outputs_root),
                "--mode",
                "quick",
            ]
        )
    assert excinfo.value.code == 0
    capsys.readouterr()

    bundle_manifest = json.loads((base / "manifest.json").read_text(encoding="utf-8"))
    ap_md = bundle_manifest.get("metadata", {}).get("artifact_passes", {})
    assert ap_md.get("order") == list(ARTIFACT_PASS_ORDER)

    run_manifest_path = next(base.joinpath(".akc", "run").glob("*.manifest.json"))
    run_manifest = RunManifest.from_json_file(run_manifest_path)
    artifact_names = [p.name for p in run_manifest.passes if p.name in ARTIFACT_PASS_ORDER]
    assert artifact_names == list(ARTIFACT_PASS_ORDER)

    out_hashes = run_manifest.output_hashes or {}
    ap_out = ap_md.get("output_hashes") or {}
    assert isinstance(out_hashes, dict) and isinstance(ap_out, dict)
    for rel_path, digest in ap_out.items():
        assert out_hashes.get(rel_path) == digest, f"artifact_passes hash mismatch for {rel_path}"

    for pass_name in ARTIFACT_PASS_ORDER:
        rec = next(p for p in run_manifest.passes if p.name == pass_name)
        md = rec.metadata or {}
        paths = md.get("artifact_paths")
        hashes = md.get("artifact_hashes")
        assert isinstance(paths, list) and paths
        assert isinstance(hashes, dict)
        for rel in paths:
            rel_s = str(rel).strip()
            assert rel_s in out_hashes
            assert out_hashes[rel_s] == hashes[rel_s]
            full = base / rel_s
            assert full.is_file(), f"missing artifact {rel_s}"
            disk_hex = _sha256_hex_bytes(full.read_bytes())
            assert disk_hex == out_hashes[rel_s]
