from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from akc.run import (
    PassRecord,
    RetrievalSnapshot,
    RunManifest,
    decide_replay_for_pass,
    find_latest_run_manifest,
    load_run_manifest,
)


def _manifest() -> RunManifest:
    return RunManifest(
        run_id="run_001",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="a" * 64,
        replay_mode="llm_vcr",
        retrieval_snapshots=(
            RetrievalSnapshot(
                source="vector_index",
                query="billing retries",
                top_k=3,
                item_ids=("chunk-1", "chunk-2", "chunk-3"),
            ),
        ),
        passes=(
            PassRecord(name="plan", status="succeeded", output_sha256="b" * 64),
            PassRecord(name="generate", status="succeeded", output_sha256="c" * 64),
        ),
        model="offline",
        model_params={"temperature": 0},
        tool_params={"test_mode": "smoke"},
        partial_replay_passes=("execute",),
        llm_vcr={"k1": "--- a/x\n+++ b/x\n@@\n+X\n"},
        budgets={"max_llm_calls": 3},
        output_hashes={"manifest.json": "d" * 64},
        trace_spans=(
            {
                "trace_id": "abcd" * 8,
                "span_id": "0123456789abcdef",
                "parent_span_id": None,
                "name": "compile.run",
                "kind": "internal",
                "start_time_unix_nano": 1,
                "end_time_unix_nano": 2,
                "attributes": {"tenant_id": "tenant-a"},
                "status": "ok",
            },
        ),
        cost_attribution={
            "tenant_id": "tenant-a",
            "repo_id": "repo-a",
            "run_id": "run_001",
            "total_tokens": 42,
            "wall_time_ms": 12,
        },
    )


def test_manifest_hash_is_stable_for_same_payload() -> None:
    m1 = _manifest()
    m2 = _manifest()
    assert m1.to_json_obj() == m2.to_json_obj()
    assert m1.stable_hash() == m2.stable_hash()


def test_manifest_hash_changes_on_relevant_change() -> None:
    m1 = _manifest()
    m2 = RunManifest(
        run_id=m1.run_id,
        tenant_id=m1.tenant_id,
        repo_id=m1.repo_id,
        ir_sha256=m1.ir_sha256,
        replay_mode=m1.replay_mode,
        retrieval_snapshots=m1.retrieval_snapshots,
        passes=(
            PassRecord(name="plan", status="succeeded", output_sha256="d" * 64),
            PassRecord(name="generate", status="succeeded", output_sha256="c" * 64),
        ),
        model=m1.model,
        model_params=m1.model_params,
    )
    assert m1.stable_hash() != m2.stable_hash()


def test_replay_modes_resolve_expected_call_policy() -> None:
    live = RunManifest(
        run_id="run_live",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="1" * 64,
        replay_mode="live",
    )
    full = RunManifest(
        run_id="run_replay",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="2" * 64,
        replay_mode="full_replay",
    )
    d_live = decide_replay_for_pass(manifest=live, pass_name="generate")
    d_full = decide_replay_for_pass(manifest=full, pass_name="generate")
    partial = RunManifest(
        run_id="run_partial",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="3" * 64,
        replay_mode="partial_replay",
        partial_replay_passes=("execute",),
    )
    d_partial_generate = decide_replay_for_pass(manifest=partial, pass_name="generate")
    d_partial_execute = decide_replay_for_pass(manifest=partial, pass_name="execute")

    assert d_live.should_call_model is True
    assert d_live.should_call_tools is True
    assert d_full.should_call_model is False
    assert d_full.should_call_tools is False
    assert d_partial_generate.should_call_model is False
    assert d_partial_generate.should_call_tools is True
    assert d_partial_execute.should_call_model is False
    assert d_partial_execute.should_call_tools is True


def test_partial_replay_only_selected_passes_are_rerun() -> None:
    partial = RunManifest(
        run_id="run_partial_selected",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="4" * 64,
        replay_mode="partial_replay",
        partial_replay_passes=("generate",),
    )

    d_generate = decide_replay_for_pass(manifest=partial, pass_name="generate")
    d_repair = decide_replay_for_pass(manifest=partial, pass_name="repair")
    d_execute = decide_replay_for_pass(manifest=partial, pass_name="execute")

    assert d_generate.should_call_model is True
    assert d_generate.should_call_tools is False
    assert d_repair.should_call_model is False
    assert d_repair.should_call_tools is False
    assert d_execute.should_call_model is False
    assert d_execute.should_call_tools is False


def test_manifest_rejects_unknown_replay_mode() -> None:
    with pytest.raises(ValueError, match="run_manifest.replay_mode must be one of"):
        RunManifest(
            run_id="run_bad",
            tenant_id="tenant-a",
            repo_id="repo-a",
            ir_sha256="1" * 64,
            replay_mode="random_mode",  # type: ignore[arg-type]
        )


def test_manifest_rejects_unknown_partial_replay_pass() -> None:
    with pytest.raises(ValueError, match="partial_replay_passes"):
        RunManifest(
            run_id="run_bad_partial",
            tenant_id="tenant-a",
            repo_id="repo-a",
            ir_sha256="1" * 64,
            replay_mode="partial_replay",
            partial_replay_passes=("random_pass",),
        )


def test_pass_record_rejects_unknown_status() -> None:
    with pytest.raises(ValueError, match="pass_record.status must be one of"):
        PassRecord(name="plan", status="unknown")  # type: ignore[arg-type]


def test_replay_decision_rejects_empty_pass_name() -> None:
    manifest = RunManifest(
        run_id="run_x",
        tenant_id="tenant-a",
        repo_id="repo-a",
        ir_sha256="f" * 64,
        replay_mode="live",
    )
    with pytest.raises(ValueError, match="replay.pass_name"):
        decide_replay_for_pass(manifest=manifest, pass_name="")


def test_run_manifest_roundtrip_from_json_obj_and_file(tmp_path: Path) -> None:
    manifest = _manifest()
    obj = manifest.to_json_obj()
    parsed = RunManifest.from_json_obj(obj)
    assert parsed.to_json_obj() == obj

    fp = tmp_path / "run.manifest.json"
    fp.write_text(json.dumps(obj), encoding="utf-8")
    parsed_file = RunManifest.from_json_file(fp)
    assert parsed_file.to_json_obj() == obj


def test_run_manifest_rejects_invalid_trace_span_shape() -> None:
    with pytest.raises(ValueError, match="trace_span\\.trace_id"):
        RunManifest(
            run_id="run_bad_trace",
            tenant_id="tenant-a",
            repo_id="repo-a",
            ir_sha256="1" * 64,
            replay_mode="live",
            trace_spans=(
                {
                    "trace_id": "",
                    "span_id": "s1",
                    "name": "compile.run",
                    "kind": "internal",
                    "start_time_unix_nano": 1,
                    "end_time_unix_nano": 2,
                },
            ),
        )


def test_loader_enforces_scope_checks(tmp_path: Path) -> None:
    manifest = _manifest()
    fp = tmp_path / "run.manifest.json"
    fp.write_text(json.dumps(manifest.to_json_obj()), encoding="utf-8")

    loaded = load_run_manifest(path=fp, expected_tenant_id="tenant-a", expected_repo_id="repo-a")
    assert loaded.run_id == manifest.run_id

    with pytest.raises(ValueError, match="tenant_id does not match"):
        load_run_manifest(path=fp, expected_tenant_id="tenant-b")

    with pytest.raises(ValueError, match="repo_id does not match"):
        load_run_manifest(path=fp, expected_repo_id="repo-b")


def test_find_latest_run_manifest_prefers_newest_file(tmp_path: Path) -> None:
    run_dir = tmp_path / "tenant-a" / "repo-a" / ".akc" / "run"
    run_dir.mkdir(parents=True, exist_ok=True)
    old_fp = run_dir / "old.manifest.json"
    new_fp = run_dir / "new.manifest.json"
    old_fp.write_text("{}", encoding="utf-8")
    new_fp.write_text("{}", encoding="utf-8")
    os.utime(old_fp, (1, 1))
    os.utime(new_fp, (2, 2))

    latest = find_latest_run_manifest(outputs_root=tmp_path, tenant_id="tenant-a", repo_id="repo-a")
    assert latest is not None
    assert latest.name == "new.manifest.json"
