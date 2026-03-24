"""``akc.coordination.protocol`` and emitted coordination protocol stubs."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from akc.compile.artifact_passes import _coordination_typescript_sdk_template, run_agent_coordination_pass
from akc.coordination.models import CoordinationParseError, CoordinationScheduler, parse_coordination_obj
from akc.coordination.protocol import load_coordination_spec_file, schedule_coordination
from akc.ir import IRDocument, IRNode
from tests.unit.test_artifact_passes import _FakeIntent, _FakeOperatingBounds


def test_parse_coordination_accepts_coordination_spec_version_alias() -> None:
    base = {
        "spec_version": 2,
        "run_id": "r",
        "tenant_id": "a",
        "repo_id": "b",
        "coordination_spec_version": 2,
        "agent_roles": [{"name": "x"}],
        "orchestration_bindings": [{"role_name": "planner", "agent_name": "ag", "orchestration_step_ids": ["s"]}],
        "coordination_graph": {"nodes": [{"node_id": "s", "kind": "step"}], "edges": []},
    }
    spec = parse_coordination_obj(base)
    assert spec.spec_version == 2


def test_parse_coordination_rejects_mismatched_version_fields() -> None:
    bad = {
        "spec_version": 1,
        "coordination_spec_version": 2,
        "run_id": "r",
        "tenant_id": "a",
        "repo_id": "b",
        "agent_roles": [{"name": "x"}],
        "orchestration_bindings": [{"role_name": "planner", "agent_name": "ag", "orchestration_step_ids": ["s"]}],
        "coordination_graph": {"nodes": [{"node_id": "s", "kind": "step"}], "edges": []},
    }
    with pytest.raises(CoordinationParseError, match="must match when both are present"):
        parse_coordination_obj(bad)


def test_load_coordination_spec_file_enforces_tenant_repo(tmp_path: Path) -> None:
    p = tmp_path / "c.json"
    p.write_text(
        '{"spec_version":1,"run_id":"r","tenant_id":"a","repo_id":"b",'
        '"agent_roles":[{"name":"x"}],'
        '"orchestration_bindings":[{"role_name":"planner","agent_name":"ag","orchestration_step_ids":["s"]}],'
        '"coordination_graph":{"nodes":[{"node_id":"s","kind":"step","label":"w"}],'
        '"edges":[]}}',
        encoding="utf-8",
    )
    with pytest.raises(CoordinationParseError, match="tenant/repo scope mismatch"):
        load_coordination_spec_file(path=p, tenant_id="x", repo_id="b")


def test_schedule_matches_scheduler_for_emitted_coordination(tmp_path: Path) -> None:
    ir_doc = IRDocument(
        tenant_id="t1",
        repo_id="r1",
        nodes=(
            IRNode(
                id="n1",
                tenant_id="t1",
                kind="workflow",
                name="step one",
                properties={"order_idx": 0, "status": "pending"},
            ),
        ),
    )
    intent = _FakeIntent(
        operating_bounds=_FakeOperatingBounds(allow_network=False, max_output_tokens=512),
        constraints=(),
        success_criteria=(),
    )
    res = run_agent_coordination_pass(run_id="run_1", ir_document=ir_doc, intent_spec=intent)
    path = tmp_path / "coordination.json"
    path.write_text(res.artifact_json.text(), encoding="utf-8")
    spec = load_coordination_spec_file(path=path, tenant_id="t1", repo_id="r1")
    assert schedule_coordination(spec) == CoordinationScheduler(spec).schedule()


def test_typescript_sdk_template_is_present() -> None:
    text = _coordination_typescript_sdk_template()
    assert "export function loadCoordinationSpec" in text
    assert "export function scheduleCoordination" in text


def test_emitted_coordination_protocol_py_main_is_executable(tmp_path: Path) -> None:
    ir_doc = IRDocument(
        tenant_id="t1",
        repo_id="r1",
        nodes=(
            IRNode(
                id="n1",
                tenant_id="t1",
                kind="workflow",
                name="step one",
                properties={"order_idx": 0, "status": "pending"},
            ),
        ),
    )
    intent = _FakeIntent(
        operating_bounds=_FakeOperatingBounds(allow_network=False, max_output_tokens=512),
        constraints=(),
        success_criteria=(),
    )
    res = run_agent_coordination_pass(run_id="run_1", ir_document=ir_doc, intent_spec=intent)
    spec_path = tmp_path / "coordination.json"
    spec_path.write_text(res.artifact_json.text(), encoding="utf-8")
    proto = tmp_path / "coordination_protocol.py"
    proto.write_text(res.artifact_python_stub.text(), encoding="utf-8")
    completed = subprocess.run(
        [
            sys.executable,
            str(proto),
            "--tenant-id",
            "t1",
            "--repo-id",
            "r1",
            "--spec-path",
            str(spec_path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, (completed.stdout, completed.stderr)
    data = json.loads(completed.stdout)
    assert "step_order" in data and "layers" in data
    assert isinstance(data["step_order"], list)
