from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from akc.cli import main
from akc.run import RunManifest
from tests.unit.test_cli_compile import (
    _executor_cwd,
    _seed_plan_with_one_step,
    _write_minimal_repo,
)


def _write_intent_file(
    *,
    path: Path,
    tenant_id: str,
    repo_id: str,
    intent_id: str,
    goal_statement: str,
    expected_patch_keyword: str,
    allow_network: bool,
) -> None:
    # This is an explicit intent-file (spec_version=1) so scope + bounds are
    # validated before compilation.
    data: dict[str, Any] = {
        "schema_version": 1,
        "spec_version": 1,
        "intent_id": intent_id,
        "tenant_id": tenant_id,
        "repo_id": repo_id,
        "status": "active",
        "title": "t",
        "goal_statement": goal_statement,
        "summary": "s",
        "derived_from_goal_text": False,
        "objectives": [],
        "constraints": [],
        "policies": [],
        "success_criteria": [
            {
                "id": "sc1",
                "evaluation_mode": "artifact_check",
                "description": "patch must/not include a keyword",
                "params": {"expected_keywords": [expected_patch_keyword]},
            }
        ],
        "operating_bounds": {
            "max_seconds": 10.0,
            "max_steps": 4,
            "max_input_tokens": None,
            "max_output_tokens": None,
            "allow_network": allow_network,
        },
        "assumptions": [],
        "risk_notes": [],
        "tags": [],
        "metadata": None,
        "created_at_ms": 1,
        "updated_at_ms": 2,
    }

    path.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")


def _latest_run_manifest_path(base: Path) -> Path:
    run_dir = base / ".akc" / "run"
    candidates = sorted(run_dir.glob("*.manifest.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    assert candidates, f"no run manifests found under {run_dir}"
    return candidates[0]


def _read_intent_artifact(*, base: Path, run_id: str) -> dict[str, Any]:
    fp = base / ".akc" / "intent" / f"{run_id}.json"
    assert fp.exists(), f"missing intent artifact: {fp}"
    return json.loads(fp.read_text(encoding="utf-8"))


def _projection_effective_allow_network(*, proj_path: Path) -> bool:
    raw = json.loads(proj_path.read_text(encoding="utf-8"))
    assert isinstance(raw, dict)
    projection = raw.get("projection")
    assert isinstance(projection, dict)
    effective = projection.get("effective")
    assert isinstance(effective, dict)
    return bool(effective.get("allow_network"))


def test_cli_compile_goal_only_emits_compat_intent_artifact(tmp_path: Path) -> None:
    tenant_id = "t_goal_only"
    repo_id = "repo1"

    outputs_root = tmp_path
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

    manifest_path = _latest_run_manifest_path(base=base)
    manifest = RunManifest.from_json_file(manifest_path)
    intent_obj = _read_intent_artifact(base=base, run_id=manifest.run_id)

    assert intent_obj.get("derived_from_goal_text") is True
    assert intent_obj.get("tenant_id") == tenant_id
    assert intent_obj.get("repo_id") == repo_id


def test_cli_compile_intent_file_applies_intent_acceptance_and_policy_narrowing(tmp_path: Path) -> None:
    tenant_id = "t_intent_file"
    repo_id = "repo1"
    outputs_root = tmp_path
    base = outputs_root / tenant_id / repo_id

    _write_minimal_repo(_executor_cwd(outputs_root, tenant_id, repo_id))
    _seed_plan_with_one_step(tenant_id=tenant_id, repo_id=repo_id, outputs_root=outputs_root)

    intent_file = tmp_path / "intent.json"
    _write_intent_file(
        path=intent_file,
        tenant_id=tenant_id,
        repo_id=repo_id,
        intent_id="intent_fail_acceptance",
        goal_statement="Intent goal should drive acceptance",
        expected_patch_keyword="THIS_KEYWORD_SHOULD_NOT_BE_IN_PATCH",
        allow_network=True,
    )

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
                "thorough",
                "--intent-file",
                str(intent_file),
            ]
        )
    assert excinfo.value.code == 2

    manifest_path = _latest_run_manifest_path(base=base)
    manifest = RunManifest.from_json_file(manifest_path)
    by_name = {p.name: p for p in manifest.passes}
    assert by_name["intent_acceptance"].status == "failed"

    # Policy projection should hard-deny allow_network because the CLI default
    # sandbox has allow_network=false.
    proj_dir = base / ".akc" / "policy"
    projection_files = sorted(proj_dir.glob(f"{manifest.run_id}_*.operating_bounds_projection.json"))
    assert projection_files
    assert all(_projection_effective_allow_network(proj_path=p) is False for p in projection_files)

    # Intent artifact should match the explicit intent_id from the intent-file.
    intent_obj = _read_intent_artifact(base=base, run_id=manifest.run_id)
    assert intent_obj.get("intent_id") == "intent_fail_acceptance"

    # IR should reflect *effective* (intersected) operating bounds, not the
    # requested ones. The CLI sandbox default deny should force allow_network=false.
    ir_path = base / ".akc" / "ir" / f"{manifest.run_id}.json"
    ir = json.loads(ir_path.read_text(encoding="utf-8"))
    intent_nodes = [n for n in ir.get("nodes", []) if isinstance(n, dict) and n.get("kind") == "intent"]
    assert len(intent_nodes) == 1
    props = intent_nodes[0].get("properties") or {}
    operating_bounds = props.get("operating_bounds") or {}
    assert operating_bounds.get("allow_network") is False


def test_cli_compile_intent_replay_uses_stored_intent_artifact(tmp_path: Path) -> None:
    tenant_id = "t_intent_replay"
    repo_id = "repo1"
    outputs_root = tmp_path
    base = outputs_root / tenant_id / repo_id

    _write_minimal_repo(_executor_cwd(outputs_root, tenant_id, repo_id))
    _seed_plan_with_one_step(tenant_id=tenant_id, repo_id=repo_id, outputs_root=outputs_root)

    intent_file = tmp_path / "intent.json"
    _write_intent_file(
        path=intent_file,
        tenant_id=tenant_id,
        repo_id=repo_id,
        intent_id="intent_replay_contract",
        goal_statement="Intent goal A",
        expected_patch_keyword="THIS_KEYWORD_SHOULD_NOT_BE_IN_PATCH",
        allow_network=True,
    )

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
                "thorough",
                "--intent-file",
                str(intent_file),
            ]
        )
    assert excinfo.value.code == 2

    prev_manifest_path = _latest_run_manifest_path(base=base)
    prev_manifest = RunManifest.from_json_file(prev_manifest_path)
    assert prev_manifest.run_id

    # Re-run in full_replay with a different goal text and without intent-file.
    # The session should load the stored intent contract for the replay run,
    # so intent_acceptance still fails.
    with pytest.raises(SystemExit) as excinfo2:
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
                "thorough",
                "--replay-mode",
                "full_replay",
                "--replay-manifest-path",
                str(prev_manifest_path),
                "--goal",
                "Completely different goal text",
            ]
        )
    assert excinfo2.value.code == 2

    latest_manifest_path = _latest_run_manifest_path(base=base)
    latest_manifest = RunManifest.from_json_file(latest_manifest_path)
    by_name = {p.name: p for p in latest_manifest.passes}
    assert by_name["intent_acceptance"].status == "failed"

    intent_obj = _read_intent_artifact(base=base, run_id=latest_manifest.run_id)
    assert intent_obj.get("intent_id") == "intent_replay_contract"


def test_cli_compile_emerging_profile_bootstraps_from_active_intent_store(tmp_path: Path) -> None:
    tenant_id = "t_emerging_bootstrap"
    repo_id = "repo1"
    outputs_root = tmp_path
    base = outputs_root / tenant_id / repo_id

    _write_minimal_repo(_executor_cwd(outputs_root, tenant_id, repo_id))
    _seed_plan_with_one_step(tenant_id=tenant_id, repo_id=repo_id, outputs_root=outputs_root)

    intent_file = tmp_path / "intent_bootstrap.json"
    _write_intent_file(
        path=intent_file,
        tenant_id=tenant_id,
        repo_id=repo_id,
        intent_id="intent_bootstrap_profile",
        goal_statement="Stored intent should drive the next compile",
        expected_patch_keyword="THIS_KEYWORD_SHOULD_NOT_BE_IN_PATCH",
        allow_network=False,
    )

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
                "thorough",
                "--intent-file",
                str(intent_file),
            ]
        )
    assert excinfo.value.code == 2

    with pytest.raises(SystemExit) as excinfo2:
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
                "thorough",
                "--developer-role-profile",
                "emerging",
                "--goal",
                "Different goal text that should be ignored by active intent bootstrap",
            ]
        )
    assert excinfo2.value.code == 2

    latest_manifest_path = _latest_run_manifest_path(base=base)
    latest_manifest = RunManifest.from_json_file(latest_manifest_path)
    by_name = {p.name: p for p in latest_manifest.passes}
    assert by_name["intent_acceptance"].status == "failed"
    intent_obj = _read_intent_artifact(base=base, run_id=latest_manifest.run_id)
    assert intent_obj.get("intent_id") == "intent_bootstrap_profile"


def test_cli_compile_intent_replay_fails_closed_if_intent_artifact_missing(tmp_path: Path) -> None:
    tenant_id = "t_intent_replay_missing"
    repo_id = "repo1"
    outputs_root = tmp_path
    base = outputs_root / tenant_id / repo_id

    _write_minimal_repo(_executor_cwd(outputs_root, tenant_id, repo_id))
    _seed_plan_with_one_step(tenant_id=tenant_id, repo_id=repo_id, outputs_root=outputs_root)

    intent_file = tmp_path / "intent.json"
    _write_intent_file(
        path=intent_file,
        tenant_id=tenant_id,
        repo_id=repo_id,
        intent_id="intent_replay_contract_missing",
        goal_statement="Intent goal A",
        expected_patch_keyword="THIS_KEYWORD_SHOULD_NOT_BE_IN_PATCH",
        allow_network=True,
    )

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
                "thorough",
                "--intent-file",
                str(intent_file),
            ]
        )
    assert excinfo.value.code == 2

    prev_manifest_path = _latest_run_manifest_path(base=base)
    prev_manifest = RunManifest.from_json_file(prev_manifest_path)

    # Delete the stored intent artifact; the manifest still advertises intent fingerprints,
    # so replay should fail closed instead of silently falling back to goal-only.
    intent_artifact_path = base / ".akc" / "intent" / f"{prev_manifest.run_id}.json"
    assert intent_artifact_path.exists()
    intent_artifact_path.unlink()

    with pytest.raises(SystemExit) as excinfo2:
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
                "thorough",
                "--replay-mode",
                "full_replay",
                "--replay-manifest-path",
                str(prev_manifest_path),
                "--goal",
                "Completely different goal text",
            ]
        )
    assert excinfo2.value.code == 2


def test_cli_compile_rejects_malformed_intent_file(tmp_path: Path) -> None:
    tenant_id = "t_bad_intent"
    repo_id = "repo1"
    outputs_root = tmp_path

    bad_intent = tmp_path / "intent_bad.json"
    # Tenant mismatch should fail fast in compile_intent_spec_from_file.
    bad_intent.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "spec_version": 1,
                "intent_id": "intent_bad_1",
                "tenant_id": "tenant_wrong",
                "repo_id": repo_id,
                "status": "draft",
                "goal_statement": "Goal",
                "derived_from_goal_text": False,
                "objectives": [],
                "constraints": [],
                "policies": [],
                "success_criteria": [],
                "operating_bounds": None,
                "assumptions": [],
                "risk_notes": [],
                "tags": [],
                "metadata": None,
                "created_at_ms": 1,
                "updated_at_ms": 2,
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

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
                "--intent-file",
                str(bad_intent),
            ]
        )
    assert excinfo.value.code == 2
