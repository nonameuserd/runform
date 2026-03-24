from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.artifacts.contracts import schema_id_for
from akc.cli import main
from akc.ir import ContractTrigger, IOContract, OperationalContract
from akc.run.manifest import RunManifest


def _write_runtime_bundle(path: Path) -> None:
    payload = {
        "schema_version": 1,
        "schema_id": schema_id_for(kind="runtime_bundle"),
        "tenant_id": "tenant-a",
        "repo_id": "repo-a",
        "run_id": "run-1",
        "referenced_ir_nodes": [],
        "referenced_contracts": [],
        "spec_hashes": {"orchestration_spec_sha256": "a" * 64},
        "deployment_intents": [],
        "runtime_policy_envelope": {},
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_operational_runtime_bundle(path: Path, *, run_id: str) -> None:
    """Bundle with ``operational_spec`` / post_runtime criteria (mirrors operational attestation integration)."""

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
    payload = {
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
                            "expected_evidence_types": ["reconcile_outcome"],
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
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _runtime_run_id(outputs_root: Path) -> str:
    record_path = next(outputs_root.rglob("runtime_run.json"))
    record = json.loads(record_path.read_text(encoding="utf-8"))
    return str(record["runtime_run_id"])


def test_runtime_replay_is_deterministic_for_same_record(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    outputs_root = tmp_path / "out"
    bundle_path = outputs_root / "tenant-a" / "repo-a" / ".akc" / "runtime" / "run-1.runtime_bundle.json"
    _write_runtime_bundle(bundle_path)

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
    runtime_run_id = _runtime_run_id(outputs_root)

    replays: list[dict[str, object]] = []
    for _ in range(2):
        with pytest.raises(SystemExit) as excinfo:
            main(
                [
                    "runtime",
                    "replay",
                    "--runtime-run-id",
                    runtime_run_id,
                    "--mode",
                    "runtime_replay",
                    "--outputs-root",
                    str(outputs_root),
                    "--tenant-id",
                    "tenant-a",
                    "--repo-id",
                    "repo-a",
                ]
            )
        assert excinfo.value.code == 0
        replays.append(json.loads(capsys.readouterr().out))

    assert replays[0] == replays[1]


def test_runtime_replay_is_deterministic_with_operational_attestation(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Attestation does not break deterministic ``runtime replay`` (replay determinism roadmap)."""

    outputs_root = tmp_path / "out"
    run_id = "compile-op-1"
    bundle_path = outputs_root / "tenant-a" / "repo-a" / ".akc" / "runtime" / f"{run_id}.runtime_bundle.json"
    _write_operational_runtime_bundle(bundle_path, run_id=run_id)

    manifest_path = outputs_root / "tenant-a" / "repo-a" / ".akc" / "run" / f"{run_id}.manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    seed_manifest = RunManifest(
        run_id=run_id,
        tenant_id="tenant-a",
        repo_id="repo-a",
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
    runtime_run_id = _runtime_run_id(outputs_root)

    replays: list[dict[str, object]] = []
    for _ in range(2):
        with pytest.raises(SystemExit) as excinfo:
            main(
                [
                    "runtime",
                    "replay",
                    "--runtime-run-id",
                    runtime_run_id,
                    "--mode",
                    "runtime_replay",
                    "--outputs-root",
                    str(outputs_root),
                    "--tenant-id",
                    "tenant-a",
                    "--repo-id",
                    "repo-a",
                ]
            )
        assert excinfo.value.code == 0
        replays.append(json.loads(capsys.readouterr().out))

    assert replays[0] == replays[1]
