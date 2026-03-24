from __future__ import annotations

from collections.abc import Mapping, Sequence, Set
from dataclasses import dataclass
from typing import Any, cast

from akc.memory.models import JSONValue, require_non_empty
from akc.pass_registry import ARTIFACT_PASS_ORDER
from akc.run.manifest import REPLAYABLE_PASSES, ReplayMode, RunManifest, RuntimeEvidenceRecord
from akc.run.recompile_triggers import (
    RecompileTrigger,
    compute_intent_semantic_changed_trigger,
    compute_intent_stable_changed_trigger,
    compute_knowledge_provenance_changed_trigger,
    compute_knowledge_semantic_changed_trigger,
)

_REPLAYABLE_PASS_SET: frozenset[str] = frozenset(REPLAYABLE_PASSES)


@dataclass(frozen=True, slots=True)
class ReplayDecision:
    """Resolved replay behavior for a pass within a run."""

    pass_name: str
    mode: ReplayMode
    should_call_model: bool
    should_call_tools: bool
    trigger: RecompileTrigger | None = None
    trigger_reason: str | None = None


@dataclass(frozen=True, slots=True)
class RuntimeReplayTransition:
    event: Mapping[str, JSONValue]
    transition: Mapping[str, JSONValue] | None
    action_decision: str | None
    retry_count: int
    budget_burn: Mapping[str, JSONValue] | None = None


@dataclass(frozen=True, slots=True)
class ReconcileReplayDecision:
    resource_id: str
    operation_type: str
    applied: bool
    rollback_chain: tuple[str, ...]
    health_status: str | None
    payload: Mapping[str, JSONValue]


@dataclass(frozen=True, slots=True)
class RuntimeReplayResult:
    mode: ReplayMode
    runtime_run_id: str
    transitions: tuple[RuntimeReplayTransition, ...]
    reconcile_decisions: tuple[ReconcileReplayDecision, ...]
    terminal_health_status: str | None


def decide_replay_for_pass(
    *,
    manifest: RunManifest,
    pass_name: str,
    current_intent_semantic_fingerprint: str | None = None,
    current_stable_intent_sha256: str | None = None,
    current_knowledge_semantic_fingerprint: str | None = None,
    current_knowledge_provenance_fingerprint: str | None = None,
    intent_mandatory_partial_replay_passes: Set[str] | None = None,
) -> ReplayDecision:
    require_non_empty(pass_name, name="replay.pass_name")
    mode = manifest.replay_mode

    # Phase 6: semantic intent changes are a hard recompilation trigger.
    # When replay baseline intent differs, never reuse cached candidates/results.
    model_passes = {"generate", "repair"}
    tool_passes = {"execute"}
    deterministic_passes = frozenset(
        {
            "plan",
            "retrieve",
            "verify",
            "intent_acceptance",
            *ARTIFACT_PASS_ORDER,
        }
    )

    # When we don't have a special case for an unknown pass name, default to
    # conservative behavior by treating it as needing tool re-execution.
    trigger = compute_intent_semantic_changed_trigger(
        manifest_intent_semantic_fingerprint=manifest.intent_semantic_fingerprint,
        current_intent_semantic_fingerprint=current_intent_semantic_fingerprint,
    )
    knowledge_sem_trigger = compute_knowledge_semantic_changed_trigger(
        manifest_knowledge_semantic_fingerprint=manifest.knowledge_semantic_fingerprint,
        current_knowledge_semantic_fingerprint=current_knowledge_semantic_fingerprint,
    )
    knowledge_prov_trigger = compute_knowledge_provenance_changed_trigger(
        manifest_knowledge_provenance_fingerprint=manifest.knowledge_provenance_fingerprint,
        current_knowledge_provenance_fingerprint=current_knowledge_provenance_fingerprint,
    )

    # Intent semantic changes are a hard recompilation trigger across all passes.
    if trigger is not None:
        should_call_model = pass_name in model_passes
        should_call_tools = True
        return ReplayDecision(
            pass_name=pass_name,
            mode=mode,
            should_call_model=should_call_model,
            should_call_tools=should_call_tools,
            trigger=trigger,
            trigger_reason=trigger.kind,
        )

    stable_trigger = compute_intent_stable_changed_trigger(
        manifest_stable_intent_sha256=manifest.stable_intent_sha256,
        current_stable_intent_sha256=current_stable_intent_sha256,
    )
    if stable_trigger is not None:
        should_call_model = pass_name in model_passes
        should_call_tools = True
        return ReplayDecision(
            pass_name=pass_name,
            mode=mode,
            should_call_model=should_call_model,
            should_call_tools=should_call_tools,
            trigger=stable_trigger,
            trigger_reason=stable_trigger.kind,
        )

    # Knowledge semantics/provenance changes invalidate cached *generation* prompts
    # (and thus the patch candidates). Only force regeneration for the passes
    # that actually call the model+executor in the controller loop.
    if pass_name in model_passes and (knowledge_sem_trigger is not None or knowledge_prov_trigger is not None):
        return ReplayDecision(
            pass_name=pass_name,
            mode=mode,
            should_call_model=True,
            should_call_tools=True,
            trigger=(knowledge_sem_trigger or knowledge_prov_trigger),
            trigger_reason=(
                knowledge_sem_trigger.kind
                if knowledge_sem_trigger is not None
                else (knowledge_prov_trigger.kind if knowledge_prov_trigger is not None else None)
            ),
        )

    if mode == "live":
        should_call_model = pass_name in model_passes
        should_call_tools = True
        return ReplayDecision(
            pass_name=pass_name,
            mode=mode,
            should_call_model=should_call_model,
            should_call_tools=should_call_tools,
            trigger_reason="live_mode",
        )
    if mode == "llm_vcr":
        # Only suppress the LLM; keep deterministic/tool computations.
        should_call_model = False
        should_call_tools = True
        return ReplayDecision(
            pass_name=pass_name,
            mode=mode,
            should_call_model=should_call_model,
            should_call_tools=should_call_tools,
            trigger_reason="llm_vcr_mode",
        )
    if mode in {"full_replay", "runtime_replay", "reconcile_replay"}:
        return ReplayDecision(
            pass_name=pass_name,
            mode=mode,
            should_call_model=False,
            should_call_tools=False,
            trigger_reason=f"{mode}_mode",
        )
    # partial_replay: rerun passes listed on the manifest, unioned with intent-
    # derived mandatory passes (success-criteria evaluation modes).
    base_selected = set(manifest.partial_replay_passes)
    mandatory_raw = set(intent_mandatory_partial_replay_passes or ())
    mandatory = {p for p in mandatory_raw if p in _REPLAYABLE_PASS_SET}
    selected = base_selected | mandatory

    def _partial_trigger_reason(*, pname: str) -> str:
        in_base = pname in base_selected
        in_man = pname in mandatory
        if not (in_base or in_man):
            return "replay_cache_hit"
        if in_man and not in_base:
            return "intent_mandatory_partial_replay"
        return "partial_replay_selection"

    rerun_execute = "execute" in selected

    if pass_name in tool_passes:
        return ReplayDecision(
            pass_name=pass_name,
            mode=mode,
            should_call_model=False,
            should_call_tools=pass_name in selected,
            trigger_reason=_partial_trigger_reason(pname=pass_name),
        )

    if pass_name in model_passes:
        # In controller flow, "execute" is coupled to generate/repair attempts; avoid
        # re-running tools unless execute is explicitly selected.
        should_call_model = pass_name in selected
        if should_call_model or rerun_execute:
            tr = _partial_trigger_reason(pname=pass_name) if should_call_model else "partial_replay_selection"
        else:
            tr = "replay_cache_hit"
        return ReplayDecision(
            pass_name=pass_name,
            mode=mode,
            should_call_model=should_call_model,
            should_call_tools=rerun_execute,
            trigger_reason=tr,
        )

    if pass_name in deterministic_passes:
        return ReplayDecision(
            pass_name=pass_name,
            mode=mode,
            should_call_model=False,
            should_call_tools=pass_name in selected,
            trigger_reason=_partial_trigger_reason(pname=pass_name),
        )

    return ReplayDecision(
        pass_name=pass_name,
        mode=mode,
        should_call_model=False,
        should_call_tools=True,
        trigger_reason="unknown_pass_default",
    )


def replay_runtime_execution(
    *,
    manifest: RunManifest,
    transcript: Sequence[Mapping[str, Any]] | None = None,
) -> RuntimeReplayResult:
    if manifest.replay_mode not in {"runtime_replay", "reconcile_replay"}:
        raise ValueError("runtime replay requires replay_mode runtime_replay or reconcile_replay")
    runtime_run_id = _resolve_runtime_run_id(manifest)
    if not runtime_run_id:
        raise ValueError("runtime replay requires a runtime_run_id in runtime evidence or control_plane")
    events = _coerce_runtime_events(transcript or _runtime_events_from_manifest(manifest))
    evidence = _runtime_evidence_for_run(manifest.runtime_evidence, runtime_run_id=runtime_run_id)

    transitions = (
        _reconstruct_runtime_transitions(events=events, evidence=evidence)
        if manifest.replay_mode == "runtime_replay"
        else ()
    )
    reconcile_decisions = _reconstruct_reconcile_decisions(evidence=evidence)
    terminal_health = _terminal_health_status(evidence=evidence)
    return RuntimeReplayResult(
        mode=manifest.replay_mode,
        runtime_run_id=runtime_run_id,
        transitions=transitions,
        reconcile_decisions=reconcile_decisions,
        terminal_health_status=terminal_health,
    )


def _resolve_runtime_run_id(manifest: RunManifest) -> str | None:
    if manifest.runtime_evidence:
        return manifest.runtime_evidence[0].runtime_run_id
    cp = manifest.control_plane or {}
    runtime_run_id = cp.get("runtime_run_id")
    if isinstance(runtime_run_id, str) and runtime_run_id.strip():
        return runtime_run_id.strip()
    return None


def _runtime_events_from_manifest(manifest: RunManifest) -> tuple[Mapping[str, Any], ...]:
    cp = manifest.control_plane or {}
    raw = cp.get("runtime_events", [])
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        return ()
    return tuple(item for item in raw if isinstance(item, Mapping))


def _coerce_runtime_events(
    transcript: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, JSONValue], ...]:
    out: list[dict[str, JSONValue]] = []
    for item in transcript:
        if not isinstance(item, Mapping):
            raise ValueError("runtime replay transcript items must be runtime events")
        out.append(_coerce_runtime_event(item))
    return tuple(sorted(out, key=lambda event: (_event_timestamp(event), _event_id(event))))


def _runtime_evidence_for_run(
    evidence: Sequence[RuntimeEvidenceRecord], *, runtime_run_id: str
) -> tuple[RuntimeEvidenceRecord, ...]:
    return tuple(
        sorted(
            (record for record in evidence if record.runtime_run_id == runtime_run_id),
            key=lambda record: (record.timestamp, record.evidence_type),
        )
    )


def _reconstruct_runtime_transitions(
    *,
    events: Sequence[Mapping[str, JSONValue]],
    evidence: Sequence[RuntimeEvidenceRecord],
) -> tuple[RuntimeReplayTransition, ...]:
    decisions_by_action: dict[str, str] = {}
    retry_state: dict[str, tuple[int, Mapping[str, JSONValue] | None]] = {}
    transitions_by_action: dict[str, dict[str, JSONValue]] = {}

    for record in evidence:
        payload = record.payload
        action_id = _payload_str(payload, "action_id")
        if not action_id:
            continue
        if record.evidence_type == "action_decision":
            decisions_by_action[action_id] = _payload_str(payload, "decision") or "unknown"
        elif record.evidence_type == "retry_budget":
            retry_count = _payload_int(payload, "retry_count")
            budget_burn_raw = payload.get("budget_burn")
            budget_burn: Mapping[str, JSONValue] | None = (
                cast(Mapping[str, JSONValue], budget_burn_raw) if isinstance(budget_burn_raw, Mapping) else None
            )
            retry_state[action_id] = (retry_count, budget_burn)
        elif record.evidence_type == "transition_application":
            transition_payload = payload.get("transition")
            if isinstance(transition_payload, Mapping):
                transitions_by_action[action_id] = _coerce_runtime_transition(transition_payload)

    reconstructed: list[RuntimeReplayTransition] = []
    for event in events:
        action_id = _action_id_from_event(event)
        if not action_id:
            continue
        retry_count, budget_burn = retry_state.get(action_id, (0, None))
        reconstructed.append(
            RuntimeReplayTransition(
                event=event,
                transition=transitions_by_action.get(action_id, _transition_from_event(event)),
                action_decision=decisions_by_action.get(action_id),
                retry_count=retry_count,
                budget_burn=budget_burn,
            )
        )
    return tuple(reconstructed)


def _reconstruct_reconcile_decisions(
    *, evidence: Sequence[RuntimeEvidenceRecord]
) -> tuple[ReconcileReplayDecision, ...]:
    rollback_chains: dict[str, tuple[str, ...]] = {}
    health_by_resource: dict[str, str] = {}
    decisions: list[ReconcileReplayDecision] = []

    for record in evidence:
        payload = record.payload
        resource_id = _payload_str(payload, "resource_id")
        if not resource_id:
            continue
        if record.evidence_type == "rollback_chain":
            raw_chain = payload.get("chain", [])
            if isinstance(raw_chain, Sequence) and not isinstance(raw_chain, (str, bytes)):
                rollback_chains[resource_id] = tuple(str(item).strip() for item in raw_chain if str(item).strip())
        elif record.evidence_type == "terminal_health":
            health = _payload_str(payload, "health_status")
            if health:
                health_by_resource[resource_id] = health
        elif record.evidence_type == "reconcile_outcome":
            decisions.append(
                ReconcileReplayDecision(
                    resource_id=resource_id,
                    operation_type=_payload_str(payload, "operation_type") or "unknown",
                    applied=bool(payload.get("applied", False)),
                    rollback_chain=rollback_chains.get(resource_id, ()),
                    health_status=(_payload_str(payload, "health_status") or health_by_resource.get(resource_id)),
                    payload=dict(payload),
                )
            )
    return tuple(decisions)


def _terminal_health_status(*, evidence: Sequence[RuntimeEvidenceRecord]) -> str | None:
    ordered = tuple(evidence)
    for record in reversed(ordered):
        if record.evidence_type != "terminal_health":
            continue
        if record.payload.get("aggregate") is True:
            return _payload_str(record.payload, "health_status")
    for record in reversed(ordered):
        if record.evidence_type == "terminal_health":
            return _payload_str(record.payload, "health_status")
    return None


def terminal_health_aggregate_status(*, evidence: Sequence[RuntimeEvidenceRecord]) -> str | None:
    """Return aggregate terminal health from evidence (``__runtime_aggregate__`` row when present).

    Ordering of severity follows ``docs/runtime-execution.md`` (worst-of:
    failed → degraded → unknown → healthy). Replay and operational evaluation
    use the same resolution as :func:`replay_runtime_execution`.
    """

    return _terminal_health_status(evidence=evidence)


def _action_id_from_event(event: Mapping[str, JSONValue]) -> str | None:
    payload_raw = event.get("payload")
    payload = payload_raw if isinstance(payload_raw, Mapping) else {}
    payload_action = payload.get("action")
    if isinstance(payload_action, Mapping):
        action_id = payload_action.get("action_id")
        if isinstance(action_id, str) and action_id.strip():
            return action_id.strip()
    action_id = payload.get("action_id")
    if isinstance(action_id, str) and action_id.strip():
        return action_id.strip()
    return None


def _transition_from_event(event: Mapping[str, JSONValue]) -> dict[str, JSONValue] | None:
    payload_raw = event.get("payload")
    payload = payload_raw if isinstance(payload_raw, Mapping) else {}
    raw = payload.get("transition")
    if not isinstance(raw, Mapping):
        return None
    return _coerce_runtime_transition(raw)


def _coerce_runtime_event(item: Mapping[str, Any]) -> dict[str, JSONValue]:
    event_id = str(item.get("event_id", "")).strip()
    event_type = str(item.get("event_type", "")).strip()
    if not event_id:
        raise ValueError("runtime replay transcript event_id must be non-empty")
    if not event_type:
        raise ValueError("runtime replay transcript event_type must be non-empty")
    timestamp_raw = item.get("timestamp", 0)
    if isinstance(timestamp_raw, bool) or not isinstance(timestamp_raw, int) or timestamp_raw < 0:
        raise ValueError("runtime replay transcript timestamp must be an integer >= 0")
    payload_raw = item.get("payload", {})
    if not isinstance(payload_raw, Mapping):
        raise ValueError("runtime replay transcript payload must be an object")
    context_raw = item.get("context", {})
    if not isinstance(context_raw, Mapping):
        raise ValueError("runtime replay transcript context must be an object")
    return {
        "event_id": event_id,
        "event_type": event_type,
        "timestamp": timestamp_raw,
        "context": dict(context_raw),
        "payload": dict(payload_raw),
    }


def _coerce_runtime_transition(item: Mapping[str, Any]) -> dict[str, JSONValue]:
    required = ("from_state", "to_state", "trigger_id", "transition_id")
    out: dict[str, JSONValue] = {}
    for key in required:
        value = str(item.get(key, "")).strip()
        if not value:
            raise ValueError(f"runtime replay transition {key} must be non-empty")
        out[key] = value
    occurred_at = item.get("occurred_at", 0)
    if isinstance(occurred_at, bool) or not isinstance(occurred_at, int) or occurred_at < 0:
        raise ValueError("runtime replay transition occurred_at must be an integer >= 0")
    out["occurred_at"] = occurred_at
    return out


def _event_id(event: Mapping[str, JSONValue]) -> str:
    value = event.get("event_id")
    return value if isinstance(value, str) else ""


def _event_timestamp(event: Mapping[str, JSONValue]) -> int:
    value = event.get("timestamp")
    return value if isinstance(value, int) else 0


def _payload_str(payload: Mapping[str, JSONValue], key: str) -> str | None:
    value = payload.get(key)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _payload_int(payload: Mapping[str, JSONValue], key: str) -> int:
    value = payload.get(key)
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            try:
                return int(stripped)
            except ValueError:
                return 0
    return 0
