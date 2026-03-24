"""Phase B: manifest-backed success-criterion modes must match live intent replay mandates."""

from __future__ import annotations

from akc.compile import ControllerConfig  # noqa: F401 — prime compile graph before akc.intent
from akc.intent import IntentSpecV1, SuccessCriterion, intent_acceptance_slice_fingerprint
from akc.run.intent_replay_mandates import mandatory_partial_replay_passes_for_success_criteria
from akc.run.manifest import RunManifest
from akc.run.replay_decisions import build_replay_decisions_payload


def _hex64(c: str = "a") -> str:
    return (c * 64)[:64]


def _decision_core(decisions: list[object]) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    for d in decisions:
        if not isinstance(d, dict):
            continue
        snap = d.get("inputs_snapshot")
        core_snap = {}
        if isinstance(snap, dict):
            core_snap = {
                "intent_mandatory_partial_replay_passes": snap.get("intent_mandatory_partial_replay_passes"),
            }
        out.append(
            {
                "pass_name": d.get("pass_name"),
                "replay_mode": d.get("replay_mode"),
                "should_call_model": d.get("should_call_model"),
                "should_call_tools": d.get("should_call_tools"),
                "trigger_reason": d.get("trigger_reason"),
                "inputs_snapshot": core_snap,
            }
        )
    return out


def test_replay_decisions_parity_intent_explicit_vs_manifest_modes() -> None:
    """Mandatory pass union matches whether derived from IntentSpec or manifest fields."""

    sc = SuccessCriterion(id="c1", evaluation_mode="tests", description="unit tests pass")
    intent = IntentSpecV1(
        tenant_id="t1",
        repo_id="repo1",
        goal_statement="Ship with tests",
        success_criteria=(sc,),
    )
    mandates = mandatory_partial_replay_passes_for_success_criteria(success_criteria=intent.success_criteria)
    modes = tuple(sorted({str(sc.evaluation_mode) for sc in intent.success_criteria}))
    acceptance_fp = intent_acceptance_slice_fingerprint(success_criteria=intent.success_criteria)

    base = RunManifest(
        run_id="run-1",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64("1"),
        replay_mode="partial_replay",
        stable_intent_sha256=_hex64("b"),
        intent_semantic_fingerprint="a" * 16,
        knowledge_semantic_fingerprint="b" * 16,
        knowledge_provenance_fingerprint="c" * 16,
        partial_replay_passes=(),
        success_criteria_evaluation_modes=modes,
        intent_acceptance_fingerprint=acceptance_fp,
    )

    payload_explicit = build_replay_decisions_payload(
        run_id="run-1",
        tenant_id="t1",
        repo_id="repo1",
        replay_mode="partial_replay",
        decision_manifest=base,
        baseline_manifest=base,
        replay_source_run_id="run-1",
        current_intent_semantic_fingerprint="a" * 16,
        current_knowledge_semantic_fingerprint="b" * 16,
        current_knowledge_provenance_fingerprint="c" * 16,
        current_stable_intent_sha256=_hex64("b"),
        audit_manifest=base,
        intent_mandatory_partial_replay_passes=mandates,
    )
    payload_manifest = build_replay_decisions_payload(
        run_id="run-1",
        tenant_id="t1",
        repo_id="repo1",
        replay_mode="partial_replay",
        decision_manifest=base,
        baseline_manifest=base,
        replay_source_run_id="run-1",
        current_intent_semantic_fingerprint="a" * 16,
        current_knowledge_semantic_fingerprint="b" * 16,
        current_knowledge_provenance_fingerprint="c" * 16,
        current_stable_intent_sha256=_hex64("b"),
        audit_manifest=base,
        intent_mandatory_partial_replay_passes=None,
    )

    dec_ex = payload_explicit.get("decisions")
    dec_mf = payload_manifest.get("decisions")
    assert isinstance(dec_ex, list) and isinstance(dec_mf, list)
    assert _decision_core(dec_ex) == _decision_core(dec_mf)
