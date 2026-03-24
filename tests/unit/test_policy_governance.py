from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.artifacts.validate import validate_obj
from akc.cli import main
from akc.control.operations_index import OperationsIndex, operations_sqlite_path
from akc.control.policy_explain import build_policy_explain_payload, load_policy_decisions_for_manifest
from akc.control.policy_provenance import apply_env_policy_provenance, merge_policy_provenance_for_compile_control_plane
from akc.control.policy_reason_narrative import describe_policy_reason
from akc.run.manifest import RunManifest


def _hex64(c: str = "a") -> str:
    return (c * 64)[:64]


def test_control_plane_schema_accepts_policy_provenance() -> None:
    obj = {
        "schema_version": 1,
        "schema_id": "akc:control_plane_envelope:v1",
        "stable_intent_sha256": _hex64("b"),
        "policy_bundle_id": "bundle-2025-03",
        "policy_git_sha": "deadbeef" * 5,
        "rego_pack_version": "1.2.3",
        "policy_decisions": [],
    }
    assert validate_obj(obj=obj, kind="control_plane_envelope", version=1) == []


def test_merge_policy_provenance_env_and_git_backfill(monkeypatch: pytest.MonkeyPatch) -> None:
    import akc.control.policy_provenance as pp

    monkeypatch.setenv("AKC_POLICY_BUNDLE_ID", "bun1")
    monkeypatch.delenv("AKC_POLICY_GIT_SHA", raising=False)
    monkeypatch.setenv("AKC_REGO_PACK_VERSION", "9.9.9")
    monkeypatch.setattr(pp, "_try_git_head_near", lambda _p: "abc123deadbeef")

    cp: dict[str, object] = {"stable_intent_sha256": _hex64()}
    merge_policy_provenance_for_compile_control_plane(cp, opa_policy_path="/tmp/policy.rego")

    assert cp["policy_bundle_id"] == "bun1"
    assert cp["rego_pack_version"] == "9.9.9"
    assert cp["policy_git_sha"] == "abc123deadbeef"


def test_apply_env_overrides_existing(monkeypatch: pytest.MonkeyPatch) -> None:
    cp = {"policy_git_sha": "old"}
    monkeypatch.setenv("AKC_POLICY_GIT_SHA", "newsha")
    apply_env_policy_provenance(cp)
    assert cp["policy_git_sha"] == "newsha"


def test_describe_policy_reason_known_and_unknown() -> None:
    assert "allowlist" in describe_policy_reason("policy.default_deny.action_not_allowlisted").lower()
    assert "prod/base" in describe_policy_reason("policy.prod.docker.memory_limit_required").lower()
    assert "custom.vendor.reason" in describe_policy_reason("custom.vendor.reason")


def test_load_policy_decisions_prefers_ref_when_non_empty(tmp_path: Path) -> None:
    scope = tmp_path / "t1" / "repo1"
    run_dir = scope / ".akc" / "run"
    run_dir.mkdir(parents=True)
    rt_dir = scope / ".akc" / "runtime" / "run-1" / "rt1"
    rt_dir.mkdir(parents=True)
    (rt_dir / "policy_decisions.json").write_text(
        json.dumps([{"action": "x", "allowed": True, "reason": "policy.opa.allow"}]),
        encoding="utf-8",
    )
    m = RunManifest(
        run_id="run-1",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64(),
        replay_mode="live",
        control_plane={
            "schema_version": 1,
            "schema_id": "akc:control_plane_envelope:v1",
            "stable_intent_sha256": _hex64(),
            "policy_decisions": [{"action": "y", "allowed": False, "reason": "inline"}],
            "policy_decisions_ref": {
                "path": ".akc/runtime/run-1/rt1/policy_decisions.json",
                "sha256": _hex64("c"),
            },
        },
    )
    decisions, src = load_policy_decisions_for_manifest(manifest=m, scope_root=scope)
    assert src.startswith("ref:")
    assert decisions[0]["action"] == "x"


def test_build_policy_explain_payload(tmp_path: Path) -> None:
    scope = tmp_path / "t1" / "repo1"
    (scope / ".akc" / "run").mkdir(parents=True)
    m = RunManifest(
        run_id="run-1",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64(),
        replay_mode="live",
        control_plane={
            "schema_version": 1,
            "policy_bundle_id": "pb1",
            "policy_git_sha": "abc",
            "rego_pack_version": "0.0.1",
            "policy_decisions": [
                {
                    "action": "llm.complete",
                    "allowed": True,
                    "reason": "policy.opa.allow",
                    "source": "opa",
                    "mode": "enforce",
                    "block": False,
                }
            ],
        },
    )
    payload = build_policy_explain_payload(manifest=m, scope_root=scope)
    assert payload["policy_provenance"]["policy_bundle_id"] == "pb1"
    assert len(payload["decisions_explained"]) == 1
    assert "opa" in payload["decisions_explained"][0]["reason_detail"].lower()


def test_operations_index_knowledge_decisions_and_policy_columns(tmp_path: Path) -> None:
    m = RunManifest(
        run_id="run-1",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64(),
        replay_mode="live",
        control_plane={
            "schema_version": 1,
            "policy_bundle_id": "pb",
            "policy_git_sha": "gitsha",
            "rego_pack_version": "1.0",
            "stable_intent_sha256": _hex64(),
        },
    )
    scope = tmp_path / "t1" / "repo1"
    run_dir = scope / ".akc" / "run"
    run_dir.mkdir(parents=True)
    know = scope / ".akc" / "knowledge"
    know.mkdir(parents=True)
    (know / "decisions.json").write_text(json.dumps({"overrides": []}), encoding="utf-8")
    mp = run_dir / "run-1.manifest.json"
    mp.write_text(json.dumps(m.to_json_obj()), encoding="utf-8")

    OperationsIndex.upsert_from_manifest_path(mp, outputs_root=tmp_path)
    idx = OperationsIndex(operations_sqlite_path(outputs_root=tmp_path, tenant_id="t1"))
    row = idx.list_runs(tenant_id="t1", limit=5)[0]
    assert row["policy_bundle_id"] == "pb"
    assert row["policy_git_sha"] == "gitsha"
    assert row["rego_pack_version"] == "1.0"

    full = idx.get_run(tenant_id="t1", repo_id="repo1", run_id="run-1")
    assert full is not None
    kd = full.get("knowledge_decisions")
    assert isinstance(kd, dict)
    assert kd.get("decisions_rel_path") == ".akc/knowledge/decisions.json"
    assert isinstance(kd.get("fingerprint_sha256"), str) and len(kd["fingerprint_sha256"]) == 64


def test_cli_policy_explain_json(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    m = RunManifest(
        run_id="run-1",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64(),
        replay_mode="live",
        control_plane={
            "schema_version": 1,
            "policy_decisions": [{"action": "executor.run", "allowed": False, "reason": "capability.expired"}],
        },
    )
    rd = tmp_path / "t1" / "repo1" / ".akc" / "run"
    rd.mkdir(parents=True)
    mp = rd / "run-1.manifest.json"
    mp.write_text(json.dumps(m.to_json_obj()), encoding="utf-8")

    with pytest.raises(SystemExit) as exc:
        main(["policy", "explain", "--manifest", str(mp), "--format", "json"])
    assert exc.value.code == 0
    out = json.loads(capsys.readouterr().out)
    assert out["policy_decisions_source"] == "inline"
    assert out["decisions_explained"][0]["reason"] == "capability.expired"


def test_control_audit_append(tmp_path: Path) -> None:
    from akc.control.control_audit import append_control_audit_event, control_audit_jsonl_path

    p = append_control_audit_event(
        outputs_root=tmp_path,
        tenant_id="t1",
        action="policy.explain",
        details={"run_id": "r1"},
        actor="tester",
    )
    assert p == control_audit_jsonl_path(outputs_root=tmp_path, tenant_id="t1")
    lines = p.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    row = json.loads(lines[0])
    assert row["action"] == "policy.explain"
    assert row["actor"] == "tester"
    assert row["details"]["run_id"] == "r1"
