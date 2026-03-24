from __future__ import annotations

from pathlib import Path

from akc.compile.controller_types import ControllerResult
from akc.control.policy_denial_explain import (
    compile_extract_policy_denial,
    policy_provenance_from_env,
)
from akc.memory.models import PlanState, PlanStep, now_ms


def test_policy_provenance_from_env_reads_known_keys(monkeypatch) -> None:
    monkeypatch.setenv("AKC_POLICY_BUNDLE_ID", "b1")
    monkeypatch.setenv("AKC_POLICY_GIT_SHA", "abc")
    monkeypatch.delenv("AKC_REGO_PACK_VERSION", raising=False)
    p = policy_provenance_from_env()
    assert p["policy_bundle_id"] == "b1"
    assert p["policy_git_sha"] == "abc"
    assert p["rego_pack_version"] is None


def test_compile_extract_policy_denial_scoped_apply_blocked(tmp_path: Path) -> None:
    plan = PlanState(
        id="run-1",
        tenant_id="t1",
        repo_id="r1",
        goal="g",
        status="running",
        created_at_ms=now_ms(),
        updated_at_ms=now_ms(),
        steps=(PlanStep(id="s1", title="step", status="in_progress", order_idx=0, notes=""),),
        next_step_id="s1",
    )
    accounting = {
        "policy_decisions": [],
        "compile_scoped_apply": {
            "compile_realization_mode": "scoped_apply",
            "attempted": True,
            "applied": False,
            "deny_reason": "policy.compile.patch.apply_denied",
            "reject_reason": "policy.opa.deny",
            "policy_blocked": True,
            "scope_root": str(tmp_path),
            "patch_sha256": "a" * 64,
            "patch_binary": None,
            "files": [],
        },
    }
    result = ControllerResult(
        status="failed",
        plan=plan,
        best_candidate=None,
        accounting=accounting,
        compile_succeeded=False,
        intent_satisfied=False,
    )
    d = compile_extract_policy_denial(
        result,
        scope_root=tmp_path,
        tenant_id="t1",
        repo_id="r1",
        outputs_root=str(tmp_path / "out"),
        opa_policy_path="/tmp/p.rego",
        opa_decision_path="data.akc.allow",
    )
    assert d is not None
    assert d["schema_id"] == "akc:policy_denial_explain:v1"
    assert d["run_kind"] == "compile"
    assert d["run_id"] == "run-1"
    assert d["decision_path"] == "data.akc.allow"
    assert "akc control runs show" in str(d["control_followup_cli"])
    assert "scoped" in d["doc_anchor_suggestion"].lower() or "getting-started" in d["doc_anchor_suggestion"]
