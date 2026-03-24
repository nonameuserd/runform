"""Phase 4: compile-time coordination emit (fork/join + role handoffs) and CLI coordination-plan."""

from __future__ import annotations

import json
from argparse import Namespace
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

import pytest

from akc.cli.runtime import cmd_runtime_coordination_plan
from akc.compile.artifact_passes import run_agent_coordination_pass
from akc.coordination.models import parse_coordination_obj, schedule_coordination_layers
from akc.ir import IRDocument, IRNode
from akc.utils.fingerprint import stable_json_fingerprint

_FIXTURES = Path(__file__).resolve().parent.parent / "fixtures" / "coordination"


def _fake_intent() -> object:
    from dataclasses import dataclass

    @dataclass
    class _Bounds:
        allow_network: bool
        max_output_tokens: int | None

    @dataclass
    class _Intent:
        operating_bounds: _Bounds
        constraints: tuple[object, ...]
        success_criteria: tuple[object, ...]

    return _Intent(
        operating_bounds=_Bounds(allow_network=False, max_output_tokens=512),
        constraints=(),
        success_criteria=(),
    )


def test_compile_emit_phase4_fixtures_validate_against_schema() -> None:
    jsonschema = pytest.importorskip("jsonschema")
    repo_root = Path(__file__).resolve().parents[2]
    schema_path = repo_root / "src" / "akc" / "coordination" / "schemas" / "agent_coordination_spec.v1.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    validator = jsonschema.Draft202012Validator(schema)
    for name in ("compile_emit_parallel_fork_v2.json", "compile_emit_barrier_join_v2.json"):
        path = _FIXTURES / name
        assert path.is_file()
        obj = json.loads(path.read_text(encoding="utf-8"))
        validator.validate(obj)
        sched = schedule_coordination_layers(parse_coordination_obj(obj))
        assert len(sched.layers) >= 1


def test_run_agent_coordination_pass_emits_fork_parallel_and_barrier_join_from_ir() -> None:
    """IR: one step then two parallel siblings (fork); then join into one step (barrier)."""

    ir_doc = IRDocument(
        tenant_id="t1",
        repo_id="r1",
        nodes=(
            IRNode(
                id="n_fork",
                tenant_id="t1",
                kind="workflow",
                name="fork",
                properties={"order_idx": 0, "status": "pending"},
            ),
            IRNode(
                id="n_a",
                tenant_id="t1",
                kind="workflow",
                name="a",
                properties={"order_idx": 1, "status": "pending"},
            ),
            IRNode(
                id="n_b",
                tenant_id="t1",
                kind="workflow",
                name="b",
                properties={"order_idx": 1, "status": "pending"},
            ),
            IRNode(
                id="n_join",
                tenant_id="t1",
                kind="workflow",
                name="join",
                properties={"order_idx": 2, "status": "pending"},
            ),
        ),
    )
    res = run_agent_coordination_pass(run_id="run_p4", ir_document=ir_doc, intent_spec=_fake_intent())  # type: ignore[arg-type]
    obj = json.loads(res.artifact_json.text())
    assert obj["spec_version"] == 2
    kinds = {e["kind"] for e in obj["coordination_graph"]["edges"]}
    assert "parallel" in kinds
    assert "barrier" in kinds
    assert "handoff" not in kinds
    sched = schedule_coordination_layers(parse_coordination_obj(obj))
    assert [list(layer.step_ids) for layer in sched.layers] == [
        ["workflow_000"],
        ["workflow_001", "workflow_002"],
        ["workflow_003"],
    ]


def test_run_agent_coordination_pass_handoff_between_sequential_single_layers() -> None:
    ir_doc = IRDocument(
        tenant_id="t1",
        repo_id="r1",
        nodes=(
            IRNode(
                id="a",
                tenant_id="t1",
                kind="workflow",
                name="a",
                properties={"order_idx": 0, "status": "pending"},
            ),
            IRNode(
                id="b",
                tenant_id="t1",
                kind="workflow",
                name="b",
                properties={"order_idx": 1, "status": "pending"},
            ),
        ),
    )
    res = run_agent_coordination_pass(run_id="run_lin", ir_document=ir_doc, intent_spec=_fake_intent())  # type: ignore[arg-type]
    obj = json.loads(res.artifact_json.text())
    assert obj["spec_version"] == 2
    edges = obj["coordination_graph"]["edges"]
    assert len(edges) == 1
    assert edges[0]["kind"] == "handoff"
    assert edges[0]["metadata"]["handoff_id"] == "handoff_workflow_000_to_workflow_001"
    assert edges[0]["metadata"]["from_role"] == "planner"
    assert edges[0]["metadata"]["to_role"] == "retriever"


def test_cli_coordination_plan_succeeds_on_compile_emit_fixture(tmp_path: Path) -> None:
    src = _FIXTURES / "compile_emit_parallel_fork_v2.json"
    coord = json.loads(src.read_text(encoding="utf-8"))
    bundle = {
        "run_id": coord["run_id"],
        "tenant_id": coord["tenant_id"],
        "repo_id": coord["repo_id"],
        "coordination_spec": coord,
        "spec_hashes": {"coordination_spec_sha256": stable_json_fingerprint(coord)},
    }
    p = tmp_path / "bundle.json"
    p.write_text(json.dumps(bundle), encoding="utf-8")
    buf = StringIO()
    with redirect_stdout(buf):
        code = cmd_runtime_coordination_plan(Namespace(bundle=str(p), verbose=False))
    assert code == 0
    out = json.loads(buf.getvalue())
    assert out["layers"][0]["step_ids"] == ["workflow_000"]
    assert set(out["layers"][1]["step_ids"]) == {"workflow_001", "workflow_002"}
