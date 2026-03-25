from __future__ import annotations

import json
from pathlib import Path

from akc.delivery.compile_handoff import (
    extract_web_distribution_hints,
    load_compile_handoff,
    platform_spec_metadata_from_handoff,
    run_manifest_path,
)
from akc.run.manifest import PassRecord, RunManifest
from akc.utils.fingerprint import stable_json_fingerprint


def test_load_compile_handoff_empty_run_id(tmp_path: Path) -> None:
    h = load_compile_handoff(project_dir=tmp_path, compile_run_id=None)
    assert h["compile_run_id"] is None
    assert h["manifest_present"] is False


def test_load_compile_handoff_reads_delivery_plan_and_web_hints(tmp_path: Path) -> None:
    rid = "run-handoff-1"
    (tmp_path / ".akc" / "run").mkdir(parents=True)
    manifest = RunManifest(
        run_id=rid,
        tenant_id="t1",
        repo_id="r1",
        ir_sha256="a" * 64,
        replay_mode="live",
        stable_intent_sha256="b" * 64,
        intent_semantic_fingerprint="c" * 16,
        intent_goal_text_fingerprint="d" * 16,
        passes=(
            PassRecord(
                name="delivery_plan",
                status="succeeded",
                metadata={
                    "delivery_plan_path": f".akc/deployment/{rid}.delivery_plan.json",
                },
            ),
        ),
    )
    mpath = run_manifest_path(project_dir=tmp_path, compile_run_id=rid)
    mpath.write_text(json.dumps(manifest.to_json_obj()), encoding="utf-8")

    plan = {
        "schema_version": 1,
        "kind": "delivery_plan",
        "run_id": rid,
        "tenant_id": "t1",
        "repo_id": "r1",
        "targets": [
            {
                "target_class": "web_app",
                "target_id": "n1",
                "name": "ui",
                "domain": "app.example.com",
            },
        ],
        "promotion_readiness": {"status": "blocked", "blocking_inputs": ["x"]},
    }
    ddir = tmp_path / ".akc" / "deployment"
    ddir.mkdir(parents=True, exist_ok=True)
    (ddir / f"{rid}.delivery_plan.json").write_text(json.dumps(plan), encoding="utf-8")

    h = load_compile_handoff(project_dir=tmp_path, compile_run_id=rid)
    assert h["manifest_present"] is True
    assert h["delivery_plan_loaded"] is True
    assert h["derived_intent_ref"] == {
        "intent_id": rid,
        "stable_intent_sha256": "b" * 64,
        "semantic_fingerprint": "c" * 16,
        "goal_text_fingerprint": "d" * 16,
    }
    assert h["delivery_plan_ref"] == {
        "path": f".akc/deployment/{rid}.delivery_plan.json",
        "fingerprint": stable_json_fingerprint(plan),
    }
    hints = h["web_distribution_hints"]
    assert hints["suggested_base_urls"] == ["https://app.example.com"]

    meta = platform_spec_metadata_from_handoff(h)
    assert meta["web_invite_base_url"] == "https://app.example.com"


def test_extract_web_distribution_hints_skips_non_web() -> None:
    h = extract_web_distribution_hints(
        {
            "targets": [
                {"target_class": "backend_service", "domain": "api.example.com"},
            ],
        },
    )
    assert h["web_targets"] == []
    assert h["suggested_base_urls"] == []
