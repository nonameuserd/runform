from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from akc.memory.models import JSONValue, now_ms, require_non_empty

ActionStatus = Literal["submitted", "pending_approval", "running", "completed", "failed"]
ActionChannel = Literal["cli", "slack", "discord", "telegram", "whatsapp"]


def _json_obj(value: dict[str, Any]) -> dict[str, JSONValue]:
    return value  # runtime validation occurs at serialization boundaries


def _normalize_channel(value: str) -> ActionChannel:
    normalized = value.strip().lower()
    allowed: tuple[str, ...] = ("cli", "slack", "discord", "telegram", "whatsapp")
    if normalized not in allowed:
        raise ValueError(f"unsupported channel: {value!r}")
    return normalized  # type: ignore[return-value]


@dataclass(frozen=True, slots=True)
class ActionInboundMessageEnvelopeV1:
    schema_kind: str
    schema_version: int
    channel: ActionChannel
    tenant_id: str
    repo_id: str
    text: str
    actor_id: str | None = None
    message_id: str | None = None
    received_at_ms: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        require_non_empty(self.schema_kind, name="schema_kind")
        require_non_empty(self.tenant_id, name="tenant_id")
        require_non_empty(self.repo_id, name="repo_id")
        require_non_empty(self.text, name="text")

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "schema_kind": self.schema_kind,
            "schema_version": int(self.schema_version),
            "channel": self.channel,
            "tenant_id": self.tenant_id,
            "repo_id": self.repo_id,
            "text": self.text,
            "actor_id": self.actor_id,
            "message_id": self.message_id,
            "received_at_ms": self.received_at_ms,
            "metadata": _json_obj(self.metadata),
        }

    @staticmethod
    def from_json_obj(obj: dict[str, Any]) -> ActionInboundMessageEnvelopeV1:
        return ActionInboundMessageEnvelopeV1(
            schema_kind=str(obj.get("schema_kind", "action_inbound_message_envelope")),
            schema_version=int(obj.get("schema_version", 1)),
            channel=_normalize_channel(str(obj.get("channel", "cli"))),
            tenant_id=str(obj.get("tenant_id", "")),
            repo_id=str(obj.get("repo_id", "")),
            text=str(obj.get("text", "")),
            actor_id=str(obj["actor_id"]) if isinstance(obj.get("actor_id"), str) else None,
            message_id=str(obj["message_id"]) if isinstance(obj.get("message_id"), str) else None,
            received_at_ms=int(obj["received_at_ms"]) if isinstance(obj.get("received_at_ms"), int) else None,
            metadata=dict(obj.get("metadata", {}) or {}),
        )


@dataclass(frozen=True, slots=True)
class ActionOutboundResponseEnvelopeV1:
    schema_kind: str
    schema_version: int
    intent_id: str
    status: str
    summary: str
    channel: ActionChannel
    tenant_id: str
    repo_id: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        require_non_empty(self.schema_kind, name="schema_kind")
        require_non_empty(self.intent_id, name="intent_id")
        require_non_empty(self.status, name="status")
        require_non_empty(self.summary, name="summary")
        require_non_empty(self.tenant_id, name="tenant_id")
        require_non_empty(self.repo_id, name="repo_id")

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "schema_kind": self.schema_kind,
            "schema_version": int(self.schema_version),
            "intent_id": self.intent_id,
            "status": self.status,
            "summary": self.summary,
            "channel": self.channel,
            "tenant_id": self.tenant_id,
            "repo_id": self.repo_id,
            "metadata": _json_obj(self.metadata),
        }

    @staticmethod
    def from_json_obj(obj: dict[str, Any]) -> ActionOutboundResponseEnvelopeV1:
        return ActionOutboundResponseEnvelopeV1(
            schema_kind=str(obj.get("schema_kind", "action_outbound_response_envelope")),
            schema_version=int(obj.get("schema_version", 1)),
            intent_id=str(obj.get("intent_id", "")),
            status=str(obj.get("status", "")),
            summary=str(obj.get("summary", "")),
            channel=_normalize_channel(str(obj.get("channel", "cli"))),
            tenant_id=str(obj.get("tenant_id", "")),
            repo_id=str(obj.get("repo_id", "")),
            metadata=dict(obj.get("metadata", {}) or {}),
        )


@dataclass(frozen=True, slots=True)
class ActionIntentV1:
    schema_kind: str
    schema_version: int
    intent_id: str
    tenant_id: str
    repo_id: str
    actor_id: str | None
    channel: str | None
    utterance: str
    goal: str
    entities: dict[str, Any] = field(default_factory=dict)
    constraints: dict[str, Any] = field(default_factory=dict)
    risk_summary: str = "low"

    def __post_init__(self) -> None:
        require_non_empty(self.schema_kind, name="schema_kind")
        require_non_empty(self.intent_id, name="intent_id")
        require_non_empty(self.tenant_id, name="tenant_id")
        require_non_empty(self.repo_id, name="repo_id")
        require_non_empty(self.utterance, name="utterance")
        require_non_empty(self.goal, name="goal")

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "schema_kind": self.schema_kind,
            "schema_version": int(self.schema_version),
            "intent_id": self.intent_id,
            "tenant_id": self.tenant_id,
            "repo_id": self.repo_id,
            "actor_id": self.actor_id,
            "channel": self.channel,
            "utterance": self.utterance,
            "goal": self.goal,
            "entities": _json_obj(self.entities),
            "constraints": _json_obj(self.constraints),
            "risk_summary": self.risk_summary,
        }

    @staticmethod
    def from_json_obj(obj: dict[str, Any]) -> ActionIntentV1:
        return ActionIntentV1(
            schema_kind=str(obj.get("schema_kind", "action_intent")),
            schema_version=int(obj.get("schema_version", 1)),
            intent_id=str(obj.get("intent_id", "")),
            tenant_id=str(obj.get("tenant_id", "")),
            repo_id=str(obj.get("repo_id", "")),
            actor_id=str(obj["actor_id"]) if isinstance(obj.get("actor_id"), str) else None,
            channel=str(obj["channel"]) if isinstance(obj.get("channel"), str) else None,
            utterance=str(obj.get("utterance", "")),
            goal=str(obj.get("goal", "")),
            entities=dict(obj.get("entities", {}) or {}),
            constraints=dict(obj.get("constraints", {}) or {}),
            risk_summary=str(obj.get("risk_summary", "low")),
        )


@dataclass(frozen=True, slots=True)
class ActionPlanStepV1:
    step_id: str
    action_type: str
    provider: str
    inputs: dict[str, Any]
    idempotency_key: str
    risk_tier: str
    requires_approval: bool
    compensation: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        require_non_empty(self.step_id, name="step_id")
        require_non_empty(self.action_type, name="action_type")
        require_non_empty(self.provider, name="provider")
        require_non_empty(self.idempotency_key, name="idempotency_key")
        require_non_empty(self.risk_tier, name="risk_tier")

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "step_id": self.step_id,
            "action_type": self.action_type,
            "provider": self.provider,
            "inputs": _json_obj(self.inputs),
            "idempotency_key": self.idempotency_key,
            "risk_tier": self.risk_tier,
            "requires_approval": bool(self.requires_approval),
            "compensation": _json_obj(self.compensation),
        }

    @staticmethod
    def from_json_obj(obj: dict[str, Any]) -> ActionPlanStepV1:
        return ActionPlanStepV1(
            step_id=str(obj.get("step_id", "")),
            action_type=str(obj.get("action_type", "")),
            provider=str(obj.get("provider", "noop")),
            inputs=dict(obj.get("inputs", {}) or {}),
            idempotency_key=str(obj.get("idempotency_key", "")),
            risk_tier=str(obj.get("risk_tier", "low")),
            requires_approval=bool(obj.get("requires_approval", False)),
            compensation=dict(obj.get("compensation", {}) or {}),
        )


@dataclass(frozen=True, slots=True)
class ActionPlanV1:
    schema_kind: str
    schema_version: int
    intent_id: str
    steps: tuple[ActionPlanStepV1, ...]

    def __post_init__(self) -> None:
        require_non_empty(self.schema_kind, name="schema_kind")
        require_non_empty(self.intent_id, name="intent_id")

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "schema_kind": self.schema_kind,
            "schema_version": int(self.schema_version),
            "intent_id": self.intent_id,
            "steps": [s.to_json_obj() for s in self.steps],
        }

    @staticmethod
    def from_json_obj(obj: dict[str, Any]) -> ActionPlanV1:
        steps_raw = obj.get("steps", [])
        steps = tuple(ActionPlanStepV1.from_json_obj(s) for s in steps_raw if isinstance(s, dict))
        return ActionPlanV1(
            schema_kind=str(obj.get("schema_kind", "action_plan")),
            schema_version=int(obj.get("schema_version", 1)),
            intent_id=str(obj.get("intent_id", "")),
            steps=steps,
        )


@dataclass(frozen=True, slots=True)
class ActionExecutionRecordV1:
    schema_kind: str
    schema_version: int
    intent_id: str
    step_id: str
    status: str
    attempt: int
    provider: str
    request_digest: str
    response_digest: str
    decision_token_refs: tuple[str, ...] = ()
    external_ids: tuple[str, ...] = ()
    started_at_ms: int = field(default_factory=now_ms)
    completed_at_ms: int = field(default_factory=now_ms)

    def __post_init__(self) -> None:
        require_non_empty(self.schema_kind, name="schema_kind")
        require_non_empty(self.intent_id, name="intent_id")
        require_non_empty(self.step_id, name="step_id")
        require_non_empty(self.status, name="status")
        require_non_empty(self.provider, name="provider")
        if int(self.attempt) <= 0:
            raise ValueError("attempt must be >= 1")
        require_non_empty(self.request_digest, name="request_digest")
        require_non_empty(self.response_digest, name="response_digest")

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "schema_kind": self.schema_kind,
            "schema_version": int(self.schema_version),
            "intent_id": self.intent_id,
            "step_id": self.step_id,
            "status": self.status,
            "attempt": int(self.attempt),
            "provider": self.provider,
            "decision_token_refs": list(self.decision_token_refs),
            "request_digest": self.request_digest,
            "response_digest": self.response_digest,
            "external_ids": list(self.external_ids),
            "started_at_ms": int(self.started_at_ms),
            "completed_at_ms": int(self.completed_at_ms),
        }

    @staticmethod
    def from_json_obj(obj: dict[str, Any]) -> ActionExecutionRecordV1:
        refs_raw = obj.get("decision_token_refs", [])
        ext_raw = obj.get("external_ids", [])
        return ActionExecutionRecordV1(
            schema_kind=str(obj.get("schema_kind", "action_execution_record")),
            schema_version=int(obj.get("schema_version", 1)),
            intent_id=str(obj.get("intent_id", "")),
            step_id=str(obj.get("step_id", "")),
            status=str(obj.get("status", "")),
            attempt=int(obj.get("attempt", 1)),
            provider=str(obj.get("provider", "")),
            decision_token_refs=tuple(str(x) for x in refs_raw if isinstance(x, str)),
            request_digest=str(obj.get("request_digest", "")),
            response_digest=str(obj.get("response_digest", "")),
            external_ids=tuple(str(x) for x in ext_raw if isinstance(x, str)),
            started_at_ms=int(obj.get("started_at_ms", now_ms())),
            completed_at_ms=int(obj.get("completed_at_ms", now_ms())),
        )
