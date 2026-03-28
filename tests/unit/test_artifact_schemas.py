from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.artifacts.schemas import (
    CONVERGENCE_CERTIFICATE_V1,
    DEVICE_CAPTURE_RESULT_V1,
    MOBILE_JOURNEY_RESULT_V1,
    OBSERVABILITY_QUERY_RESULT_V1,
    OPERATIONAL_ASSURANCE_RESULT_V1,
    OPERATIONAL_EVIDENCE_WINDOW_V1,
    OPERATIONAL_VALIDITY_REPORT_V1,
    PROMOTION_PACKET_V1,
    RUNTIME_BUNDLE_SCHEMA_VERSION,
)
from akc.artifacts.validate import validate_obj
from akc.cli import main
from akc.memory.facade import build_memory


def _write_minimal_repo(root: Path) -> None:
    pkg = root / "src"
    tests = root / "tests"
    pkg.mkdir(parents=True, exist_ok=True)
    tests.mkdir(parents=True, exist_ok=True)
    (pkg / "__init__.py").write_text("", encoding="utf-8")
    (pkg / "module.py").write_text("VALUE = 1\n", encoding="utf-8")
    (tests / "test_module.py").write_text(
        "from src import module\n\ndef test_smoke() -> None:\n    assert module.VALUE == 1\n",
        encoding="utf-8",
    )


def _seed_plan_with_one_step(*, tenant_id: str, repo_id: str, outputs_root: Path) -> None:
    base = outputs_root / tenant_id / repo_id
    memory_db = base / ".akc" / "memory.sqlite"
    memory_db.parent.mkdir(parents=True, exist_ok=True)
    mem = build_memory(backend="sqlite", sqlite_path=str(memory_db))
    plan = mem.plan_state.create_plan(
        tenant_id=tenant_id,
        repo_id=repo_id,
        goal="Compile repository",
        initial_steps=["Implement goal"],
    )
    mem.plan_state.set_active_plan(tenant_id=tenant_id, repo_id=repo_id, plan_id=plan.id)


def _executor_cwd(outputs_root: Path, tenant_id: str, repo_id: str) -> Path:
    base = outputs_root / tenant_id / repo_id
    return base / tenant_id / repo_id


def test_operational_validity_report_minimal_validates() -> None:
    obj = {
        "schema_version": 1,
        "schema_id": "akc:operational_validity_report:v1",
        "tenant_id": "t",
        "repo_id": "r",
        "run_id": "run-1",
        "evaluated_at_ms": 1,
        "passed": True,
        "operational_spec_version": 1,
    }
    assert validate_obj(obj=obj, kind="operational_validity_report", version=1) == []


def test_operational_evidence_window_minimal_validates() -> None:
    obj = {
        "schema_version": 1,
        "schema_id": "akc:operational_evidence_window:v1",
        "window_start_ms": 0,
        "window_end_ms": 1000,
        "runtime_evidence_exports": [
            {"path": ".akc/runtime/run1/r1/runtime_evidence.json", "sha256": "a" * 64},
        ],
    }
    assert validate_obj(obj=obj, kind="operational_evidence_window", version=1) == []


def test_observability_query_result_minimal_validates() -> None:
    obj = {
        "schema_version": 1,
        "schema_id": "akc:observability_query_result:v1",
        "binding_id": "obs.login",
        "query_kind": "logql_query",
        "target": "loki-main",
        "window_start_ms": 1,
        "window_end_ms": 2,
        "status": "ok",
        "summary": {"count": 1},
        "attachments": [],
        "fingerprint_sha256": "a" * 64,
    }
    assert validate_obj(obj=obj, kind="observability_query_result", version=1) == []


def test_mobile_journey_result_minimal_validates() -> None:
    obj = {
        "schema_version": 1,
        "schema_id": "akc:mobile_journey_result:v1",
        "binding_id": "mobile.login.android",
        "platform": "android",
        "device_id": "emulator-5554",
        "journey_id": "login",
        "status": "passed",
        "started_at_ms": 1,
        "ended_at_ms": 2,
        "assertions_passed": 1,
        "assertions_failed": 0,
        "artifacts": [],
        "fingerprint_sha256": "b" * 64,
    }
    assert validate_obj(obj=obj, kind="mobile_journey_result", version=1) == []


def test_device_capture_result_minimal_validates() -> None:
    obj = {
        "schema_version": 1,
        "schema_id": "akc:device_capture_result:v1",
        "binding_id": "android.failure.screenshot",
        "platform": "android",
        "capture_kind": "screenshot",
        "status": "ok",
        "artifact_path": ".akc/verification/validators/run-1/attachments/screen.png",
        "metadata": {},
        "fingerprint_sha256": "c" * 64,
    }
    assert validate_obj(obj=obj, kind="device_capture_result", version=1) == []


def test_operational_evidence_window_json_file_matches_python_schema() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "akc"
        / "artifacts"
        / "schemas"
        / "operational_evidence_window.v1.schema.json"
    )
    on_disk = json.loads(path.read_text(encoding="utf-8"))
    assert on_disk == OPERATIONAL_EVIDENCE_WINDOW_V1


def test_operational_validity_report_json_file_matches_python_schema() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "akc"
        / "artifacts"
        / "schemas"
        / "operational_validity_report.v1.schema.json"
    )
    on_disk = json.loads(path.read_text(encoding="utf-8"))
    assert on_disk == OPERATIONAL_VALIDITY_REPORT_V1


def test_operational_assurance_result_json_file_matches_python_schema() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "akc"
        / "artifacts"
        / "schemas"
        / "operational_assurance_result.v1.schema.json"
    )
    on_disk = json.loads(path.read_text(encoding="utf-8"))
    assert on_disk == OPERATIONAL_ASSURANCE_RESULT_V1


def test_observability_query_result_json_file_matches_python_schema() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "akc"
        / "artifacts"
        / "schemas"
        / "observability_query_result.v1.schema.json"
    )
    on_disk = json.loads(path.read_text(encoding="utf-8"))
    assert on_disk == OBSERVABILITY_QUERY_RESULT_V1


def test_mobile_journey_result_json_file_matches_python_schema() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "akc"
        / "artifacts"
        / "schemas"
        / "mobile_journey_result.v1.schema.json"
    )
    on_disk = json.loads(path.read_text(encoding="utf-8"))
    assert on_disk == MOBILE_JOURNEY_RESULT_V1


def test_device_capture_result_json_file_matches_python_schema() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "akc"
        / "artifacts"
        / "schemas"
        / "device_capture_result.v1.schema.json"
    )
    on_disk = json.loads(path.read_text(encoding="utf-8"))
    assert on_disk == DEVICE_CAPTURE_RESULT_V1


def test_convergence_certificate_json_file_matches_python_schema() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "akc"
        / "artifacts"
        / "schemas"
        / "convergence_certificate.v1.schema.json"
    )
    on_disk = json.loads(path.read_text(encoding="utf-8"))
    assert on_disk == CONVERGENCE_CERTIFICATE_V1


def test_promotion_packet_json_file_matches_python_schema() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "akc"
        / "artifacts"
        / "schemas"
        / "promotion_packet.v1.schema.json"
    )
    on_disk = json.loads(path.read_text(encoding="utf-8"))
    assert on_disk == PROMOTION_PACKET_V1


def test_manifest_and_evidence_json_validate(tmp_path: Path) -> None:
    tenant_id = "schema-tenant"
    repo_id = "schema-repo"
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
                "--schema-version",
                "1",
                "--mode",
                "quick",
            ]
        )
    assert excinfo.value.code == 0

    manifest_path = base / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert validate_obj(obj=manifest, kind="manifest", version=1) == []

    run_dir = base / ".akc" / "run"
    for suffix, kind in (
        (".spans.json", "run_trace_spans"),
        (".costs.json", "run_cost_attribution"),
        (".replay_decisions.json", "replay_decisions"),
        (".recompile_triggers.json", "recompile_triggers"),
    ):
        matched = sorted(run_dir.glob(f"*{suffix}"))
        assert matched, f"expected at least one {suffix} artifact"
        for p in matched:
            payload = json.loads(p.read_text(encoding="utf-8"))
            issues = validate_obj(obj=payload, kind=kind, version=1)
            assert issues == [], f"{p} schema issues: {issues}"

    tests_dir = base / ".akc" / "tests"
    for p in sorted(tests_dir.rglob("*.json")):
        payload = json.loads(p.read_text(encoding="utf-8"))
        issues = validate_obj(obj=payload, kind="execution_stage", version=1)
        assert issues == [], f"{p} schema issues: {issues}"

    ver_dir = base / ".akc" / "verification"
    if ver_dir.is_dir():
        for p in sorted(ver_dir.rglob("*.json")):
            payload = json.loads(p.read_text(encoding="utf-8"))
            issues = validate_obj(obj=payload, kind="verifier_result", version=1)
            assert issues == [], f"{p} schema issues: {issues}"

    runtime_dir = base / ".akc" / "runtime"
    for p in sorted(runtime_dir.glob("*.runtime_bundle.json")):
        payload = json.loads(p.read_text(encoding="utf-8"))
        issues = validate_obj(
            obj=payload,
            kind="runtime_bundle",
            version=int(payload.get("schema_version", RUNTIME_BUNDLE_SCHEMA_VERSION)),
        )
        assert issues == [], f"{p} schema issues: {issues}"

    promotion_dir = base / ".akc" / "promotion"
    if promotion_dir.is_dir():
        for p in sorted(promotion_dir.glob("*.packet.json")):
            payload = json.loads(p.read_text(encoding="utf-8"))
            issues = validate_obj(obj=payload, kind="promotion_packet", version=1)
            assert issues == [], f"{p} schema issues: {issues}"
