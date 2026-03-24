"""Runtime CLI writes operational_validity_report + manifest/control_plane pointers when intent requests it."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.artifacts.contracts import schema_id_for
from akc.cli import main
from akc.control.operations_index import OperationsIndex, operations_sqlite_path
from akc.ir import ContractTrigger, IOContract, OperationalContract
from akc.run.manifest import RunManifest
from akc.utils.fingerprint import stable_json_fingerprint


def _bundle_with_operational_intent(*, run_id: str) -> dict[str, object]:
    tenant_id = "tenant-a"
    repo_id = "repo-a"
    contract = OperationalContract(
        contract_id="contract-1",
        contract_category="runtime",
        triggers=(
            ContractTrigger(
                trigger_id="kernel_started",
                source="runtime.kernel.started",
                details={"event_type": "runtime.kernel.started"},
            ),
        ),
        io_contract=IOContract(
            input_keys=("runtime_run_id",),
            output_keys=("action_id", "action_type", "adapter_id"),
        ),
    )
    node = {
        "id": "node-1",
        "tenant_id": tenant_id,
        "kind": "workflow",
        "name": "Workflow 1",
        "properties": {"order_idx": 0},
        "depends_on": [],
        "contract": contract.to_json_obj(),
    }
    return {
        "schema_version": 1,
        "schema_id": schema_id_for(kind="runtime_bundle"),
        "tenant_id": tenant_id,
        "repo_id": repo_id,
        "run_id": run_id,
        "intent_ref": {
            "intent_id": "intent-1",
            "stable_intent_sha256": "c" * 64,
            "semantic_fingerprint": "d" * 16,
            "goal_text_fingerprint": "e" * 16,
        },
        "intent_policy_projection": {
            "intent_id": "intent-1",
            "spec_version": 1,
            "intent_semantic_fingerprint": "d" * 16,
            "intent_goal_text_fingerprint": "e" * 16,
            "stable_intent_sha256": "c" * 64,
            "operating_bounds_effective": {},
            "policies": [],
            "success_criteria_summary": {
                "count": 1,
                "evaluation_modes": ["operational_spec"],
                "criteria": [
                    {
                        "id": "sc-op-1",
                        "evaluation_mode": "operational_spec",
                        "summary": "runtime health",
                        "params": {
                            "spec_version": 1,
                            "window": "single_run",
                            "predicate_kind": "presence",
                            "signals": [
                                {"evidence_type": "terminal_health", "payload_path": "health_status"},
                            ],
                            "expected_evidence_types": ["reconcile_outcome", "convergence_certificate"],
                            "evaluation_phase": "post_runtime",
                        },
                    }
                ],
            },
        },
        "referenced_ir_nodes": [node],
        "referenced_contracts": [contract.to_json_obj()],
        "spec_hashes": {
            "orchestration_spec_sha256": "a" * 64,
            "coordination_spec_sha256": "b" * 64,
        },
        "deployment_intents": [
            {
                "node_id": "node-1",
                "kind": "workflow",
                "name": "Workflow 1",
                "depends_on": [],
                "effects": None,
                "contract_id": "contract-1",
            }
        ],
        "runtime_policy_envelope": {},
    }


def test_runtime_operational_attestation_manifest_and_index(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    outputs_root = tmp_path / "out"
    tenant_id = "tenant-a"
    repo_id = "repo-a"
    run_id = "compile-1"
    base = outputs_root / tenant_id / repo_id
    bundle_path = base / ".akc" / "runtime" / f"{run_id}.runtime_bundle.json"
    bundle_path.parent.mkdir(parents=True, exist_ok=True)
    bundle_path.write_text(json.dumps(_bundle_with_operational_intent(run_id=run_id)), encoding="utf-8")

    manifest_path = base / ".akc" / "run" / f"{run_id}.manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    seed_manifest = RunManifest(
        run_id=run_id,
        tenant_id=tenant_id,
        repo_id=repo_id,
        ir_sha256="a" * 64,
        replay_mode="live",
    )
    manifest_path.write_text(json.dumps(seed_manifest.to_json_obj(), indent=2), encoding="utf-8")

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "runtime",
                "start",
                "--bundle",
                str(bundle_path),
                "--mode",
                "simulate",
                "--outputs-root",
                str(outputs_root),
            ]
        )
    assert excinfo.value.code == 0
    capsys.readouterr()

    record_path = next(outputs_root.rglob("runtime_run.json"))
    record = json.loads(record_path.read_text(encoding="utf-8"))
    scope_dir = Path(str(record["scope_dir"]))
    report_path = scope_dir / "operational_validity_report.json"
    assert report_path.is_file()

    report_obj = json.loads(report_path.read_text(encoding="utf-8"))
    assert report_obj.get("schema_id") == "akc:operational_validity_report:v1"
    assert record.get("operational_validity_passed") == report_obj.get("passed")

    file_fp = stable_json_fingerprint(report_obj)

    updated = json.loads(manifest_path.read_text(encoding="utf-8"))
    cp = updated.get("control_plane")
    assert isinstance(cp, dict)
    ref = cp.get("operational_validity_report_ref")
    assert isinstance(ref, dict)
    ref_sha = str(ref.get("sha256", "")).strip().lower()
    assert len(ref_sha) == 64
    assert ref_sha == file_fp
    assert cp.get("operational_validity_passed") == report_obj.get("passed")

    round_m = RunManifest.from_json_file(manifest_path)
    manifest_path.write_text(json.dumps(round_m.to_json_obj(), indent=2, sort_keys=True), encoding="utf-8")
    round2 = RunManifest.from_json_file(manifest_path)
    cp2 = round2.control_plane or {}
    ref2 = cp2.get("operational_validity_report_ref")
    assert isinstance(ref2, dict)
    assert str(ref2.get("sha256", "")).strip().lower() == ref_sha

    OperationsIndex.upsert_from_manifest_path(manifest_path, outputs_root=outputs_root)
    idx = OperationsIndex(operations_sqlite_path(outputs_root=outputs_root, tenant_id=tenant_id))
    row = idx.get_run(tenant_id=tenant_id, repo_id=repo_id, run_id=run_id)
    assert row is not None
    assert row.get("operational_validity_passed") == report_obj.get("passed")
