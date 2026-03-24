"""Integration tests for intent strong path: compile realization and promotion attestation.

Covers artifact_only vs scoped_apply, policy-gated denial (OPA), and manifest↔packet
compile_apply_attestation consistency required by runtime live-mutation gates.
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

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _run_compile(
    tmp_path: Path,
    *,
    compile_realization_mode: str,
    extra_args: list[str] | None = None,
) -> Path:
    tenant_id = "t1"
    repo_id = "repo1"
    outputs_root = tmp_path
    base = outputs_root / tenant_id / repo_id
    _write_minimal_repo(_executor_cwd(outputs_root, tenant_id, repo_id))
    _seed_plan_with_one_step(tenant_id=tenant_id, repo_id=repo_id, outputs_root=outputs_root)

    argv = [
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
        compile_realization_mode,
    ]
    if extra_args:
        argv.extend(extra_args)

    with pytest.raises(SystemExit) as excinfo:
        main(argv)
    assert excinfo.value.code == 0
    return base


def _load_run_manifest(base: Path) -> RunManifest:
    run_manifest_path = next((base / ".akc" / "run").glob("*.manifest.json"))
    return RunManifest.from_json_file(run_manifest_path)


def test_compile_artifact_only_skips_apply_and_attestation_verifies_against_manifest(
    tmp_path: Path,
) -> None:
    base = _run_compile(tmp_path, compile_realization_mode="artifact_only")
    run_manifest = _load_run_manifest(base)
    assert run_manifest.control_plane is not None
    cp = dict(run_manifest.control_plane)
    ref = cp.get("promotion_packet_ref")
    assert isinstance(ref, dict)
    rel = str(ref.get("path", "")).strip()
    assert rel.startswith(".akc/promotion/")
    packet_path = base / rel
    assert packet_path.is_file()
    packet = json.loads(packet_path.read_text(encoding="utf-8"))
    caa = cp.get("compile_apply_attestation")
    assert isinstance(caa, dict)
    assert caa.get("compile_realization_mode") == "artifact_only"
    assert caa.get("applied") is False
    assert packet.get("compile_apply_attestation") == caa
    verify_compile_apply_attestation_for_rollout(packet=packet, manifest_control_plane=cp)

    scoped_ref = cp.get("compile_scoped_apply_ref")
    assert isinstance(scoped_ref, dict)
    scoped_path = base / str(scoped_ref.get("path", ""))
    assert scoped_path.is_file()
    scoped_obj = json.loads(scoped_path.read_text(encoding="utf-8"))
    assert scoped_obj.get("attempted") is False
    assert scoped_obj.get("applied") is False


@pytest.mark.skipif(shutil.which("patch") is None, reason="patch(1) not available")
def test_compile_scoped_apply_without_opa_applies_patch_and_attestation_matches_manifest(
    tmp_path: Path,
) -> None:
    base = _run_compile(tmp_path, compile_realization_mode="scoped_apply")
    run_manifest = _load_run_manifest(base)
    assert run_manifest.control_plane is not None
    cp = dict(run_manifest.control_plane)
    ref = cp.get("promotion_packet_ref")
    assert isinstance(ref, dict)
    packet = json.loads((base / str(ref.get("path"))).read_text(encoding="utf-8"))
    caa = cp.get("compile_apply_attestation")
    assert isinstance(caa, dict)
    assert caa.get("compile_realization_mode") == "scoped_apply"
    assert caa.get("applied") is True
    pol = caa.get("policy_allow_decision")
    assert isinstance(pol, dict)
    assert pol.get("allowed") is True
    assert packet.get("patch_hash_sha256") == caa.get("patch_fingerprint_sha256")
    verify_compile_apply_attestation_for_rollout(packet=packet, manifest_control_plane=cp)

    scoped_path = base / str(cp["compile_scoped_apply_ref"]["path"])
    scoped_obj = json.loads(scoped_path.read_text(encoding="utf-8"))
    assert scoped_obj.get("attempted") is True
    assert scoped_obj.get("applied") is True
    assert scoped_obj.get("policy_blocked") is False


@pytest.mark.skipif(shutil.which("patch") is None, reason="patch(1) not available")
@pytest.mark.skipif(shutil.which("opa") is None, reason="opa CLI not available")
def test_compile_scoped_apply_policy_denied_when_opa_rejects_compile_patch_apply(
    tmp_path: Path,
) -> None:
    policy_path = _REPO_ROOT / "configs/policy/compile_tools.rego"
    assert policy_path.is_file()
    base = _run_compile(
        tmp_path,
        compile_realization_mode="scoped_apply",
        extra_args=[
            "--opa-policy-path",
            str(policy_path),
            "--opa-decision-path",
            "data.akc.allow",
            "--policy-mode",
            "enforce",
        ],
    )
    run_manifest = _load_run_manifest(base)
    assert run_manifest.control_plane is not None
    cp = dict(run_manifest.control_plane)
    scoped_path = base / str(cp["compile_scoped_apply_ref"]["path"])
    scoped_obj = json.loads(scoped_path.read_text(encoding="utf-8"))
    assert scoped_obj.get("attempted") is True
    assert scoped_obj.get("applied") is False
    assert scoped_obj.get("policy_blocked") is True
    assert scoped_obj.get("deny_reason") == "policy.compile.patch.apply_denied"

    caa = cp.get("compile_apply_attestation")
    assert isinstance(caa, dict)
    assert caa.get("compile_realization_mode") == "scoped_apply"
    assert caa.get("applied") is False
    pol = caa.get("policy_allow_decision")
    assert isinstance(pol, dict)
    assert pol.get("allowed") is False
