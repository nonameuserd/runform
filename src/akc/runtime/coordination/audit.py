"""Coordination audit records, runtime event payload helpers, and optional OTel JSON mapping."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from akc.memory.models import JSONValue
from akc.runtime.models import RuntimeAction, RuntimeActionResult, RuntimeBundle, RuntimeContext
from akc.utils.fingerprint import stable_json_fingerprint

# Optional RuntimeEvent.payload keys (backward compatible; omit when unknown).
RUNTIME_EVENT_COORDINATION_PAYLOAD_KEYS: tuple[str, ...] = (
    "coordination_spec_sha256",
    "role_id",
    "graph_step_id",
    "parent_event_id",
    "input_sha256",
    "output_sha256",
    "policy_envelope_sha256",
    "orchestration_spec_sha256",
    "coordination_spec_version",
    "otel_trace",
    # Phase 5 — audit / observability (optional; appended for consumer compatibility).
    "coordination_edge_kind",
    "handoff_id",
    "delegate_kind",
    "lowered_precedence_hash",
)


def lowered_precedence_edges_fingerprint(lowered_precedence_edges: Sequence[Mapping[str, Any]]) -> str:
    """Stable digest of scheduler ``lowered_precedence_edges`` (forensics / cross-run comparison)."""

    rows = [dict(x) for x in lowered_precedence_edges]
    return stable_json_fingerprint({"lowered_precedence_edges": rows})


def _optional_trimmed_str(value: object) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _hex64_digest(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    s = value.strip().lower()
    if len(s) != 64 or any(ch not in "0123456789abcdef" for ch in s):
        return None
    return s


def policy_envelope_sha256(*, policy_envelope: Mapping[str, JSONValue]) -> str:
    """Stable digest of the runtime policy envelope (matches bundle materialization)."""

    return stable_json_fingerprint(dict(policy_envelope))


def orchestration_spec_sha256_from_bundle_metadata(metadata: Mapping[str, JSONValue]) -> str | None:
    specs = metadata.get("spec_hashes")
    if not isinstance(specs, Mapping):
        return None
    raw = specs.get("orchestration_spec_sha256")
    if not isinstance(raw, str):
        return None
    s = raw.strip().lower()
    if len(s) == 64 and all(ch in "0123456789abcdef" for ch in s):
        return s
    return None


def _hex16_from_event_id(event_id: str) -> str:
    """16 hex chars (8 bytes) span id derived deterministically from ``event_id``."""

    h = hashlib.sha256(event_id.encode("utf-8")).hexdigest()
    return h[:16]


@dataclass(frozen=True, slots=True)
class OtelTraceJson:
    """Minimal OTel-style trace context for export to standard collectors (JSON only, no SDK)."""

    trace_id: str
    span_id: str
    parent_span_id: str | None = None

    def to_json_obj(self) -> dict[str, JSONValue]:
        out: dict[str, JSONValue] = {
            "trace_id": self.trace_id,
            "span_id": self.span_id,
        }
        if self.parent_span_id is not None:
            out["parent_span_id"] = self.parent_span_id
        return out


def otel_trace_json_from_akc_event(
    *,
    trace_id: str,
    event_id: str,
    parent_event_id: str | None = None,
) -> OtelTraceJson:
    """Map AKC event IDs to OTel-style ``trace_id`` / ``span_id`` (hex).

    ``trace_id`` should be the per-run runtime trace id (e.g. kernel ``_runtime_trace_id``) so
    spans correlate; span ids are derived from event ids.
    """

    tid = str(trace_id).strip()
    span_id = _hex16_from_event_id(event_id)
    parent_span_id = _hex16_from_event_id(parent_event_id) if parent_event_id else None
    return OtelTraceJson(trace_id=tid, span_id=span_id, parent_span_id=parent_span_id)


@dataclass(frozen=True, slots=True)
class CoordinationAuditRecord:
    """Append-only coordination audit line (JSON object per line in ``evidence/coordination_audit.jsonl``)."""

    record_version: int
    timestamp_ms: int
    event_id: str
    event_type: str
    compile_run_id: str
    runtime_run_id: str
    tenant_id: str
    repo_id: str
    coordination_spec_sha256: str
    role_id: str
    graph_step_id: str
    action_id: str
    idempotency_key: str
    policy_envelope_sha256: str
    input_sha256: str
    output_sha256: str | None
    bundle_manifest_hash: str
    parent_event_id: str | None = None
    orchestration_spec_sha256: str | None = None
    coordination_spec_version: int | None = None
    sequence: int | None = None
    otel_trace: OtelTraceJson | None = None
    coordination_edge_kind: str | None = None
    handoff_id: str | None = None
    delegate_kind: str | None = None
    lowered_precedence_hash: str | None = None

    def to_json_obj(self) -> dict[str, JSONValue]:
        out: dict[str, JSONValue] = {
            "record_version": int(self.record_version),
            "timestamp_ms": int(self.timestamp_ms),
            "event_id": self.event_id.strip(),
            "event_type": self.event_type.strip(),
            "compile_run_id": self.compile_run_id.strip(),
            "runtime_run_id": self.runtime_run_id.strip(),
            "tenant_id": self.tenant_id.strip(),
            "repo_id": self.repo_id.strip(),
            "coordination_spec_sha256": self.coordination_spec_sha256.strip().lower(),
            "role_id": self.role_id.strip(),
            "graph_step_id": self.graph_step_id.strip(),
            "action_id": self.action_id.strip(),
            "idempotency_key": self.idempotency_key.strip(),
            "policy_envelope_sha256": self.policy_envelope_sha256.strip().lower(),
            "input_sha256": self.input_sha256.strip().lower(),
            "bundle_manifest_hash": self.bundle_manifest_hash.strip().lower(),
        }
        if self.output_sha256 is not None:
            out["output_sha256"] = self.output_sha256.strip().lower()
        if self.parent_event_id is not None:
            out["parent_event_id"] = self.parent_event_id.strip()
        if self.orchestration_spec_sha256 is not None:
            out["orchestration_spec_sha256"] = self.orchestration_spec_sha256.strip().lower()
        if self.coordination_spec_version is not None:
            out["coordination_spec_version"] = int(self.coordination_spec_version)
        if self.sequence is not None:
            out["sequence"] = int(self.sequence)
        if self.otel_trace is not None:
            out["otel_trace"] = self.otel_trace.to_json_obj()
        if self.coordination_edge_kind is not None:
            out["coordination_edge_kind"] = self.coordination_edge_kind.strip()
        if self.handoff_id is not None:
            out["handoff_id"] = self.handoff_id.strip()
        if self.delegate_kind is not None:
            out["delegate_kind"] = self.delegate_kind.strip()
        if self.lowered_precedence_hash is not None:
            out["lowered_precedence_hash"] = self.lowered_precedence_hash.strip().lower()
        return out

    def to_json_line(self) -> str:
        return json.dumps(self.to_json_obj(), sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def coordination_audit_record_from_action_event(
    *,
    context: RuntimeContext,
    bundle: RuntimeBundle,
    event_id: str,
    event_type: str,
    timestamp_ms: int,
    action: RuntimeAction,
    result: RuntimeActionResult | None,
    parent_event_id: str | None,
    policy_envelope_sha256_digest: str,
    orchestration_spec_sha256: str | None,
    coordination_spec_version: int | None,
    sequence: int,
    trace_id: str,
) -> CoordinationAuditRecord | None:
    """Build an audit record for coordination-scoped actions; returns None if not a coordination step."""

    pc = action.policy_context
    if pc is None:
        return None
    step = str(pc.get("coordination_step_id", "")).strip()
    role = str(pc.get("coordination_role_id", "")).strip()
    spec_sha = str(pc.get("coordination_spec_sha256", "")).strip().lower()
    if not step or not role or len(spec_sha) != 64:
        return None
    out_hash: str | None = None
    if result is not None:
        out_hash = stable_json_fingerprint(result.to_json_obj())
    otel = otel_trace_json_from_akc_event(
        trace_id=trace_id,
        event_id=event_id,
        parent_event_id=parent_event_id,
    )
    edge_kind = _optional_trimmed_str(pc.get("coordination_edge_kind"))
    handoff_summary = _optional_trimmed_str(pc.get("coordination_handoff_id"))
    delegate_kinds = _optional_trimmed_str(pc.get("coordination_delegate_kind"))
    lowered_hash = _hex64_digest(pc.get("coordination_lowered_precedence_hash"))
    return CoordinationAuditRecord(
        record_version=1,
        timestamp_ms=int(timestamp_ms),
        event_id=event_id,
        event_type=event_type,
        compile_run_id=context.run_id.strip(),
        runtime_run_id=context.runtime_run_id.strip(),
        tenant_id=context.tenant_id.strip(),
        repo_id=context.repo_id.strip(),
        coordination_spec_sha256=spec_sha,
        role_id=role,
        graph_step_id=step,
        action_id=action.action_id.strip(),
        idempotency_key=action.idempotency_key.strip(),
        policy_envelope_sha256=policy_envelope_sha256_digest,
        input_sha256=action.inputs_fingerprint.strip().lower(),
        output_sha256=out_hash,
        bundle_manifest_hash=bundle.ref.manifest_hash.strip().lower(),
        parent_event_id=parent_event_id,
        orchestration_spec_sha256=orchestration_spec_sha256,
        coordination_spec_version=coordination_spec_version,
        sequence=int(sequence),
        otel_trace=otel,
        coordination_edge_kind=edge_kind,
        handoff_id=handoff_summary,
        delegate_kind=delegate_kinds,
        lowered_precedence_hash=lowered_hash,
    )


def merge_coordination_telemetry_into_payload(
    *,
    event_type: str,
    policy_envelope_sha256_digest: str,
    orchestration_spec_sha256: str | None,
    coordination_spec_version: int | None,
    trace_id: str,
    event_id: str,
    action: RuntimeAction | None,
    result: RuntimeActionResult | None,
    parent_event_id: str | None,
    lowered_precedence_hash: str | None = None,
) -> dict[str, JSONValue]:
    """Return optional coordination / audit keys to merge into a :class:`RuntimeEvent` payload."""

    if action is not None:
        pc = action.policy_context
        if not isinstance(pc, dict) or not str(pc.get("coordination_step_id", "")).strip():
            return {}
    elif event_type != "runtime.coordination.plan_enqueued":
        return {}

    out: dict[str, JSONValue] = {
        "policy_envelope_sha256": policy_envelope_sha256_digest,
    }
    if orchestration_spec_sha256 is not None:
        out["orchestration_spec_sha256"] = orchestration_spec_sha256
    if coordination_spec_version is not None:
        out["coordination_spec_version"] = int(coordination_spec_version)
    lowered_merged = _hex64_digest(lowered_precedence_hash) if lowered_precedence_hash is not None else None
    if action is None:
        if lowered_merged is not None:
            out["lowered_precedence_hash"] = lowered_merged
        out["otel_trace"] = otel_trace_json_from_akc_event(
            trace_id=trace_id,
            event_id=event_id,
            parent_event_id=parent_event_id,
        ).to_json_obj()
        return out
    pc = action.policy_context or {}
    step = str(pc.get("coordination_step_id", "")).strip()
    role = str(pc.get("coordination_role_id", "")).strip()
    spec_sha = str(pc.get("coordination_spec_sha256", "")).strip().lower()
    if step:
        out["graph_step_id"] = step
    if role:
        out["role_id"] = role
    if len(spec_sha) == 64:
        out["coordination_spec_sha256"] = spec_sha
    if parent_event_id is not None:
        out["parent_event_id"] = parent_event_id
    out["input_sha256"] = action.inputs_fingerprint.strip().lower()
    if result is not None:
        out["output_sha256"] = stable_json_fingerprint(result.to_json_obj())
    ek = _optional_trimmed_str(pc.get("coordination_edge_kind"))
    if ek is not None:
        out["coordination_edge_kind"] = ek
    hid = _optional_trimmed_str(pc.get("coordination_handoff_id"))
    if hid is not None:
        out["handoff_id"] = hid
    dk = _optional_trimmed_str(pc.get("coordination_delegate_kind"))
    if dk is not None:
        out["delegate_kind"] = dk
    lp = _hex64_digest(pc.get("coordination_lowered_precedence_hash"))
    if lp is None:
        lp = lowered_merged
    if lp is not None:
        out["lowered_precedence_hash"] = lp
    out["otel_trace"] = otel_trace_json_from_akc_event(
        trace_id=trace_id,
        event_id=event_id,
        parent_event_id=parent_event_id,
    ).to_json_obj()
    return out
