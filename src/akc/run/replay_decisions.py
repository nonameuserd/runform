from __future__ import annotations

from collections.abc import Mapping, Set
from dataclasses import dataclass
from typing import Any, cast

from akc.artifacts.contracts import apply_schema_envelope
from akc.memory.models import JSONValue
from akc.run.intent_replay_mandates import mandatory_partial_replay_passes_for_evaluation_modes
from akc.run.manifest import (
    REPLAYABLE_PASSES,
    SUCCESS_CRITERIA_EVALUATION_MODES_SCHEMA_VERSION,
    ReplayMode,
    RunManifest,
)
from akc.run.recompile_triggers import (
    OperationalValidityFailedTriggerSeverity,
    RecompileTrigger,
    evaluate_recompile_triggers,
)
from akc.run.replay import ReplayDecision, decide_replay_for_pass


def resolve_intent_mandatory_partial_replay_passes(
    *,
    intent_mandatory_partial_replay_passes: Set[str] | None,
    decision_manifest: RunManifest | None,
) -> frozenset[str]:
    """Resolve mandatory partial-replay passes from explicit intent or manifest-backed modes."""

    if intent_mandatory_partial_replay_passes is not None:
        return frozenset(intent_mandatory_partial_replay_passes)
    if decision_manifest is None:
        return frozenset()
    modes = decision_manifest.success_criteria_evaluation_modes
    if not modes:
        return frozenset()
    scm_ver = decision_manifest.success_criteria_evaluation_modes_schema_version
    if scm_ver is not None and int(scm_ver) != SUCCESS_CRITERIA_EVALUATION_MODES_SCHEMA_VERSION:
        return frozenset()
    return mandatory_partial_replay_passes_for_evaluation_modes(modes=modes)


@dataclass(frozen=True, slots=True)
class PassReplayDecisionRecord:
    pass_name: str
    replay_mode: str
    should_call_model: bool
    should_call_tools: bool
    trigger_reason: str
    trigger: RecompileTrigger | None
    inputs_snapshot: dict[str, JSONValue]

    @classmethod
    def from_replay_decision(
        cls,
        *,
        decision: ReplayDecision,
        inputs_snapshot: dict[str, JSONValue],
    ) -> PassReplayDecisionRecord:
        return cls(
            pass_name=decision.pass_name,
            replay_mode=decision.mode,
            should_call_model=bool(decision.should_call_model),
            should_call_tools=bool(decision.should_call_tools),
            trigger_reason=str(decision.trigger_reason or "none"),
            trigger=decision.trigger,
            inputs_snapshot=inputs_snapshot,
        )

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "pass_name": self.pass_name,
            "replay_mode": self.replay_mode,
            "should_call_model": self.should_call_model,
            "should_call_tools": self.should_call_tools,
            "trigger_reason": self.trigger_reason,
            "trigger": self.trigger.to_json_obj() if self.trigger is not None else None,
            "inputs_snapshot": dict(self.inputs_snapshot),
        }

    @classmethod
    def from_json_obj(cls, obj: Mapping[str, Any]) -> PassReplayDecisionRecord:
        trigger_raw = obj.get("trigger")
        trigger: RecompileTrigger | None = None
        if isinstance(trigger_raw, Mapping):
            details_raw = trigger_raw.get("details", {})
            trigger = RecompileTrigger(
                kind=str(trigger_raw.get("kind", "")).strip(),  # type: ignore[arg-type]
                details=dict(details_raw) if isinstance(details_raw, Mapping) else {},
            )
        inputs_snapshot_raw = obj.get("inputs_snapshot")
        return cls(
            pass_name=str(obj.get("pass_name", "")),
            replay_mode=str(obj.get("replay_mode", "")),
            should_call_model=bool(obj.get("should_call_model")),
            should_call_tools=bool(obj.get("should_call_tools")),
            trigger_reason=str(obj.get("trigger_reason", "")),
            trigger=trigger,
            inputs_snapshot=(dict(inputs_snapshot_raw) if isinstance(inputs_snapshot_raw, Mapping) else {}),
        )


def _manifest_for_decision_eval(
    *,
    tenant_id: str,
    repo_id: str,
    replay_mode: ReplayMode,
    decision_manifest: RunManifest | None,
) -> RunManifest:
    if decision_manifest is not None:
        return decision_manifest
    return RunManifest(
        run_id="no-replay-baseline",
        tenant_id=tenant_id,
        repo_id=repo_id,
        ir_sha256="0" * 64,
        replay_mode=replay_mode,
    )


def collect_pass_replay_decision_records(
    *,
    tenant_id: str,
    repo_id: str,
    replay_mode: ReplayMode,
    decision_manifest: RunManifest | None,
    baseline_manifest: RunManifest | None,
    current_intent_semantic_fingerprint: str | None,
    current_knowledge_semantic_fingerprint: str | None,
    current_knowledge_provenance_fingerprint: str | None,
    current_stable_intent_sha256: str | None = None,
    intent_mandatory_partial_replay_passes: Set[str] | None = None,
    passes: tuple[str, ...] = REPLAYABLE_PASSES,
    audit_manifest: RunManifest | None = None,
) -> tuple[PassReplayDecisionRecord, ...]:
    manifest = _manifest_for_decision_eval(
        tenant_id=tenant_id,
        repo_id=repo_id,
        replay_mode=replay_mode,
        decision_manifest=decision_manifest,
    )
    resolved_mandates = resolve_intent_mandatory_partial_replay_passes(
        intent_mandatory_partial_replay_passes=intent_mandatory_partial_replay_passes,
        decision_manifest=decision_manifest,
    )
    audit_for_contract = audit_manifest if audit_manifest is not None else manifest
    mandate_resolution = (
        "explicit"
        if intent_mandatory_partial_replay_passes is not None
        else (
            "manifest_modes"
            if decision_manifest is not None and bool(decision_manifest.success_criteria_evaluation_modes)
            else "none"
        )
    )
    records: list[PassReplayDecisionRecord] = []
    for pass_name in passes:
        decision = decide_replay_for_pass(
            manifest=manifest,
            pass_name=pass_name,
            current_intent_semantic_fingerprint=current_intent_semantic_fingerprint,
            current_stable_intent_sha256=current_stable_intent_sha256,
            current_knowledge_semantic_fingerprint=current_knowledge_semantic_fingerprint,
            current_knowledge_provenance_fingerprint=current_knowledge_provenance_fingerprint,
            intent_mandatory_partial_replay_passes=resolved_mandates,
        )
        mandatory_list = sorted(resolved_mandates)
        records.append(
            PassReplayDecisionRecord.from_replay_decision(
                decision=decision,
                inputs_snapshot={
                    "baseline_present": baseline_manifest is not None,
                    "baseline_run_id": (baseline_manifest.run_id if baseline_manifest is not None else None),
                    "decision_run_id": manifest.run_id,
                    "manifest_intent_semantic_fingerprint": (
                        baseline_manifest.intent_semantic_fingerprint if baseline_manifest is not None else None
                    ),
                    "current_intent_semantic_fingerprint": current_intent_semantic_fingerprint,
                    "manifest_stable_intent_sha256": (
                        baseline_manifest.stable_intent_sha256 if baseline_manifest is not None else None
                    ),
                    "current_stable_intent_sha256": current_stable_intent_sha256,
                    "manifest_knowledge_semantic_fingerprint": (
                        baseline_manifest.knowledge_semantic_fingerprint if baseline_manifest is not None else None
                    ),
                    "current_knowledge_semantic_fingerprint": (current_knowledge_semantic_fingerprint),
                    "manifest_knowledge_provenance_fingerprint": (
                        baseline_manifest.knowledge_provenance_fingerprint if baseline_manifest is not None else None
                    ),
                    "current_knowledge_provenance_fingerprint": (current_knowledge_provenance_fingerprint),
                    "baseline_partial_replay_passes": (
                        list(baseline_manifest.partial_replay_passes) if baseline_manifest is not None else []
                    ),
                    "effective_partial_replay_passes": list(manifest.partial_replay_passes),
                    "intent_mandatory_partial_replay_passes": cast(JSONValue, mandatory_list),
                    "intent_mandatory_resolution": mandate_resolution,
                    "manifest_success_criteria_evaluation_modes": (
                        list(audit_for_contract.success_criteria_evaluation_modes)
                        if audit_for_contract.success_criteria_evaluation_modes
                        else []
                    ),
                    "manifest_intent_acceptance_fingerprint": audit_for_contract.intent_acceptance_fingerprint,
                },
            )
        )
    return tuple(records)


def build_replay_decisions_payload(
    *,
    run_id: str,
    tenant_id: str,
    repo_id: str,
    replay_mode: ReplayMode,
    decision_manifest: RunManifest | None,
    baseline_manifest: RunManifest | None,
    replay_source_run_id: str | None,
    current_intent_semantic_fingerprint: str | None,
    current_knowledge_semantic_fingerprint: str | None,
    current_knowledge_provenance_fingerprint: str | None,
    current_stable_intent_sha256: str | None = None,
    intent_mandatory_partial_replay_passes: Set[str] | None = None,
    passes: tuple[str, ...] = REPLAYABLE_PASSES,
    audit_manifest: RunManifest | None = None,
) -> dict[str, JSONValue]:
    decisions = [
        record.to_json_obj()
        for record in collect_pass_replay_decision_records(
            tenant_id=tenant_id,
            repo_id=repo_id,
            replay_mode=replay_mode,
            decision_manifest=decision_manifest,
            baseline_manifest=baseline_manifest,
            current_intent_semantic_fingerprint=current_intent_semantic_fingerprint,
            current_knowledge_semantic_fingerprint=current_knowledge_semantic_fingerprint,
            current_knowledge_provenance_fingerprint=current_knowledge_provenance_fingerprint,
            current_stable_intent_sha256=current_stable_intent_sha256,
            intent_mandatory_partial_replay_passes=intent_mandatory_partial_replay_passes,
            passes=passes,
            audit_manifest=audit_manifest,
        )
    ]

    payload: dict[str, JSONValue] = {
        "run_id": run_id,
        "tenant_id": tenant_id,
        "repo_id": repo_id,
        "replay_source_run_id": replay_source_run_id,
        "replay_mode": replay_mode,
        "decisions": cast(JSONValue, decisions),
    }
    return apply_schema_envelope(obj=payload, kind="replay_decisions")


def build_recompile_triggers_payload(
    *,
    tenant_id: str,
    repo_id: str,
    checked_at_ms: int,
    run_id: str | None = None,
    check_id: str | None = None,
    source: str | None = None,
    manifest: RunManifest | None = None,
    current_intent_semantic_fingerprint: str | None = None,
    current_knowledge_semantic_fingerprint: str | None = None,
    current_knowledge_provenance_fingerprint: str | None = None,
    current_stable_intent_sha256: str | None = None,
    operational_validity_failed: bool = False,
    operational_validity_failed_trigger_severity: OperationalValidityFailedTriggerSeverity = "block",
    operational_validity_success_criterion_ids: tuple[str, ...] | None = None,
    enable_granular_acceptance_triggers: bool = False,
) -> dict[str, JSONValue]:
    triggers = evaluate_recompile_triggers(
        manifest_intent_semantic_fingerprint=(manifest.intent_semantic_fingerprint if manifest is not None else None),
        current_intent_semantic_fingerprint=current_intent_semantic_fingerprint,
        manifest_knowledge_semantic_fingerprint=(
            manifest.knowledge_semantic_fingerprint if manifest is not None else None
        ),
        current_knowledge_semantic_fingerprint=current_knowledge_semantic_fingerprint,
        manifest_knowledge_provenance_fingerprint=(
            manifest.knowledge_provenance_fingerprint if manifest is not None else None
        ),
        current_knowledge_provenance_fingerprint=current_knowledge_provenance_fingerprint,
        manifest_stable_intent_sha256=(manifest.stable_intent_sha256 if manifest is not None else None),
        current_stable_intent_sha256=current_stable_intent_sha256,
        operational_validity_failed=operational_validity_failed,
        operational_validity_failed_trigger_severity=operational_validity_failed_trigger_severity,
        operational_validity_success_criterion_ids=operational_validity_success_criterion_ids,
        enable_granular_acceptance_triggers=enable_granular_acceptance_triggers,
    )
    payload: dict[str, JSONValue] = {
        "tenant_id": tenant_id,
        "repo_id": repo_id,
        "run_id": run_id,
        "check_id": check_id,
        "checked_at_ms": int(checked_at_ms),
        "source": source,
        "triggers": [trigger.to_json_obj() for trigger in triggers],
    }
    return apply_schema_envelope(obj=payload, kind="recompile_triggers")
