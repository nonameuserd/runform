"""End-to-end: compile (live_apply + scoped_apply) → runtime enforce exercises promotion + compile-apply gate.

``_mutating_provider_kind_from_bundle`` only inspects bundle metadata; without
``AKC_ENABLE_MUTATING_DEPLOYMENT_PROVIDER=1`` the reconciler still uses the
in-memory provider, but the CLI must load the signed packet and pass
``verify_compile_apply_attestation_for_rollout`` before reconcile.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from akc.cli import main
from akc.run import RunManifest
from akc.runtime.compile_apply_attestation import verify_compile_apply_attestation_for_rollout
from tests.unit.test_cli_compile import (
    _executor_cwd,
    _seed_plan_with_one_step,
    _write_minimal_repo,
)


@pytest.mark.skipif(shutil.which("patch") is None, reason="patch(1) not available")
def test_compile_live_apply_scoped_apply_then_runtime_enforce_passes_promotion_and_attestation_gate(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    tenant_id = "t1"
    repo_id = "repo1"
    outputs_root = tmp_path
    base = outputs_root / tenant_id / repo_id

    _write_minimal_repo(_executor_cwd(outputs_root, tenant_id, repo_id))
    _seed_plan_with_one_step(tenant_id=tenant_id, repo_id=repo_id, outputs_root=outputs_root)

    with pytest.raises(SystemExit) as exc_compile:
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
                "--compile-realization-mode",
                "scoped_apply",
                "--promotion-mode",
                "live_apply",
            ]
        )
    assert exc_compile.value.code == 0
    capsys.readouterr()

    manifest_path = next((base / ".akc" / "run").glob("*.manifest.json"))
    manifest = RunManifest.from_json_file(manifest_path)
    assert manifest.control_plane is not None
    cp = dict(manifest.control_plane)
    ref = cp.get("promotion_packet_ref")
    assert isinstance(ref, dict)
    packet_path = base / str(ref.get("path"))
    packet = json.loads(packet_path.read_text(encoding="utf-8"))
    assert packet.get("promotion_mode") == "live_apply"
    caa = packet.get("compile_apply_attestation")
    assert isinstance(caa, dict)
    assert caa.get("compile_realization_mode") == "scoped_apply"
    assert caa.get("applied") is True
    verify_compile_apply_attestation_for_rollout(packet=packet, manifest_control_plane=cp)

    bundle_path = next((base / ".akc" / "runtime").glob("*.runtime_bundle.json"))
    bundle_obj = json.loads(bundle_path.read_text(encoding="utf-8"))
    bundle_obj["deployment_provider"] = {"kind": "docker_compose_apply"}
    bundle_path.write_text(json.dumps(bundle_obj, indent=2, sort_keys=True), encoding="utf-8")

    with pytest.raises(SystemExit) as exc_rt:
        main(
            [
                "runtime",
                "start",
                "--bundle",
                str(bundle_path),
                "--mode",
                "enforce",
                "--outputs-root",
                str(outputs_root),
            ]
        )
    assert exc_rt.value.code == 0
    out = capsys.readouterr().out
    assert "Runtime CLI error" not in out
