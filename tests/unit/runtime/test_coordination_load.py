"""Tests for coordination bundle loading and fingerprint checks."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.coordination.models import CoordinationParseError
from akc.runtime.coordination.load import load_coordination_for_bundle
from akc.utils.fingerprint import stable_json_fingerprint


def test_load_coordination_rejects_ref_fingerprint_vs_file_mismatch(tmp_path: Path) -> None:
    coord_obj = {
        "spec_version": 1,
        "run_id": "r1",
        "tenant_id": "t",
        "repo_id": "r",
        "coordination_graph": {"nodes": [], "edges": []},
        "orchestration_bindings": [],
    }
    repo = tmp_path / "tenant" / "repo"
    agents = repo / ".akc" / "agents"
    agents.mkdir(parents=True)
    coord_path = agents / "r1.coordination.json"
    coord_path.write_text(json.dumps(coord_obj, sort_keys=True), encoding="utf-8")

    bundle_path = repo / ".akc" / "runtime" / "x.runtime_bundle.json"
    bundle_path.parent.mkdir(parents=True)
    fp = stable_json_fingerprint(coord_obj)
    payload = {
        "run_id": "r1",
        "coordination_ref": {
            "path": ".akc/agents/r1.coordination.json",
            "fingerprint": "a" * 64,
        },
        "spec_hashes": {"coordination_spec_sha256": fp},
    }
    bundle_path.write_text("{}", encoding="utf-8")

    with pytest.raises(CoordinationParseError, match="coordination_ref.fingerprint"):
        load_coordination_for_bundle(bundle_path=bundle_path, payload=payload)


def test_load_coordination_ok_when_ref_matches_file_and_spec_hash(tmp_path: Path) -> None:
    coord_obj = {
        "spec_version": 1,
        "run_id": "r1",
        "tenant_id": "t",
        "repo_id": "r",
        "coordination_graph": {"nodes": [], "edges": []},
        "orchestration_bindings": [],
    }
    repo = tmp_path / "tenant" / "repo"
    agents = repo / ".akc" / "agents"
    agents.mkdir(parents=True)
    coord_path = agents / "r1.coordination.json"
    coord_path.write_text(json.dumps(coord_obj, sort_keys=True), encoding="utf-8")

    bundle_path = repo / ".akc" / "runtime" / "x.runtime_bundle.json"
    bundle_path.parent.mkdir(parents=True)
    fp = stable_json_fingerprint(coord_obj)
    payload = {
        "run_id": "r1",
        "coordination_ref": {
            "path": ".akc/agents/r1.coordination.json",
            "fingerprint": fp,
        },
        "spec_hashes": {"coordination_spec_sha256": fp},
    }
    bundle_path.write_text("{}", encoding="utf-8")

    loaded = load_coordination_for_bundle(bundle_path=bundle_path, payload=payload)
    assert loaded is not None
    assert loaded.fingerprint_sha256 == fp
