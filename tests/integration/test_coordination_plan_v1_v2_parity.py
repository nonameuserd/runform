"""Phase 0 acceptance: schema examples + CLI coordination-plan parity (v1 vs lowered v2)."""

from __future__ import annotations

import json
from argparse import Namespace
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

import pytest

from akc.cli.runtime import cmd_runtime_coordination_plan
from akc.coordination.models import parse_coordination_obj, schedule_coordination_layers
from akc.utils.fingerprint import stable_json_fingerprint

_FIXTURES = Path(__file__).resolve().parent.parent / "fixtures" / "coordination"


def _coordination_fixtures() -> tuple[Path, Path]:
    v1 = _FIXTURES / "fork_join_v1_depends_on.json"
    v2 = _FIXTURES / "fork_join_v2_reserved_edges.json"
    assert v1.is_file() and v2.is_file()
    return v1, v2


def test_coordination_fork_join_fixtures_validate_against_schema() -> None:
    jsonschema = pytest.importorskip("jsonschema")
    repo_root = Path(__file__).resolve().parents[2]
    schema_path = repo_root / "src" / "akc" / "coordination" / "schemas" / "agent_coordination_spec.v1.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    validator = jsonschema.Draft202012Validator(schema)
    for path in _coordination_fixtures():
        obj = json.loads(path.read_text(encoding="utf-8"))
        validator.validate(obj)


def test_schedule_layers_parity_v1_vs_v2_fixtures() -> None:
    v1p, v2p = _coordination_fixtures()
    s1 = schedule_coordination_layers(parse_coordination_obj(json.loads(v1p.read_text(encoding="utf-8"))))
    s2 = schedule_coordination_layers(parse_coordination_obj(json.loads(v2p.read_text(encoding="utf-8"))))
    assert [list(layer.step_ids) for layer in s1.layers] == [list(layer.step_ids) for layer in s2.layers]
    assert list(s1.step_order) == list(s2.step_order)
    assert [list(layer.step_ids) for layer in s1.layers] == [["fork"], ["branch_a", "branch_b"], ["join"]]


def test_cli_coordination_plan_layers_match_v1_and_v2_fixtures(tmp_path: Path) -> None:
    v1p, v2p = _coordination_fixtures()
    outs: list[dict[str, object]] = []
    for src in (v1p, v2p):
        coord = json.loads(src.read_text(encoding="utf-8"))
        bundle = {
            "run_id": coord["run_id"],
            "tenant_id": coord["tenant_id"],
            "repo_id": coord["repo_id"],
            "coordination_spec": coord,
            "spec_hashes": {"coordination_spec_sha256": stable_json_fingerprint(coord)},
        }
        p = tmp_path / f"bundle_{src.stem}.json"
        p.write_text(json.dumps(bundle, indent=2), encoding="utf-8")
        buf = StringIO()
        with redirect_stdout(buf):
            code = cmd_runtime_coordination_plan(Namespace(bundle=str(p), verbose=False))
        assert code == 0
        outs.append(json.loads(buf.getvalue()))
    assert outs[0]["layers"] == outs[1]["layers"]
    assert outs[0]["step_order"] == outs[1]["step_order"]
    assert outs[0]["layers"] == [
        {"layer_index": 0, "step_ids": ["fork"]},
        {"layer_index": 1, "step_ids": ["branch_a", "branch_b"]},
        {"layer_index": 2, "step_ids": ["join"]},
    ]
