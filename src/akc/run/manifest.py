from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

from akc.memory.models import JSONValue, json_value_as_int, require_non_empty
from akc.pass_registry import ARTIFACT_PASS_ORDER, CONTROLLER_LOOP_PASS_ORDER
from akc.utils.fingerprint import stable_json_fingerprint

ReplayMode = Literal[
    "live",
    "llm_vcr",
    "full_replay",
    "partial_replay",
    "runtime_replay",
    "reconcile_replay",
]
RUN_MANIFEST_VERSION = 1
# Version of ``success_criteria_evaluation_modes`` + ``intent_acceptance_fingerprint`` fields.
SUCCESS_CRITERIA_EVALUATION_MODES_SCHEMA_VERSION = 1
ALLOWED_REPLAY_MODES: tuple[str, ...] = (
    "live",
    "llm_vcr",
    "full_replay",
    "partial_replay",
    "runtime_replay",
    "reconcile_replay",
)
ALLOWED_PASS_STATUSES: tuple[str, ...] = ("succeeded", "failed", "skipped")

# Replay contracts for `partial_replay_passes` — see `akc.pass_registry` for ordered tuples.
REPLAYABLE_PASSES: tuple[str, ...] = CONTROLLER_LOOP_PASS_ORDER + ARTIFACT_PASS_ORDER
if len(REPLAYABLE_PASSES) != len(set(REPLAYABLE_PASSES)):
    raise RuntimeError("REPLAYABLE_PASSES must not contain duplicate pass names")


def _validate_runtime_bundle_metadata(metadata: Mapping[str, Any]) -> None:
    required_str_fields = (
        "artifact_group",
        "runtime_bundle_path",
        "orchestration_spec_sha256",
        "coordination_spec_sha256",
    )
    for field in required_str_fields:
        value = metadata.get(field)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"runtime_bundle metadata.{field} must be a non-empty string")
    for field in ("referenced_node_count", "referenced_contract_count", "deployment_intent_count"):
        value = metadata.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"runtime_bundle metadata.{field} must be an integer >= 0")
    artifact_paths = metadata.get("artifact_paths")
    if not isinstance(artifact_paths, Sequence) or isinstance(artifact_paths, (str, bytes)):
        raise ValueError("runtime_bundle metadata.artifact_paths must be an array")
    if not any(str(path).strip() == str(metadata["runtime_bundle_path"]).strip() for path in artifact_paths):
        raise ValueError("runtime_bundle metadata.runtime_bundle_path must appear in artifact_paths")
    artifact_hashes = metadata.get("artifact_hashes")
    if not isinstance(artifact_hashes, Mapping):
        raise ValueError("runtime_bundle metadata.artifact_hashes must be an object")
    for field in ("orchestration_spec_sha256", "coordination_spec_sha256"):
        digest = str(metadata.get(field, "")).strip().lower()
        if len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest):
            raise ValueError(f"runtime_bundle metadata.{field} must be a 64-char hex sha256")


def _validate_sha256_hex(value: str, *, name: str) -> str:
    s = value.strip().lower()
    if len(s) != 64 or any(ch not in "0123456789abcdef" for ch in s):
        raise ValueError(f"{name} must be a 64-char hex sha256 string")
    return s


def _validate_json_mapping(value: Any, *, what: str) -> dict[str, JSONValue]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{what} must be an object")
    out: dict[str, JSONValue] = {}
    for key, item in value.items():
        if not isinstance(key, str):
            raise ValueError(f"{what} keys must be strings")
        out[key] = cast(JSONValue, item)
    return out


@dataclass(frozen=True, slots=True)
class RetrievalSnapshot:
    source: str
    query: str
    top_k: int
    item_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        require_non_empty(self.source, name="retrieval_snapshot.source")
        require_non_empty(self.query, name="retrieval_snapshot.query")
        if int(self.top_k) <= 0:
            raise ValueError("retrieval_snapshot.top_k must be > 0")

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "source": self.source.strip(),
            "query": self.query,
            "top_k": int(self.top_k),
            "item_ids": [str(x) for x in self.item_ids],
        }

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> RetrievalSnapshot:
        item_ids_raw = obj.get("item_ids") or []
        if not isinstance(item_ids_raw, Sequence) or isinstance(item_ids_raw, (str, bytes)):
            raise ValueError("retrieval_snapshot.item_ids must be an array")
        return RetrievalSnapshot(
            source=str(obj.get("source", "")),
            query=str(obj.get("query", "")),
            top_k=int(obj.get("top_k", 0)),
            item_ids=tuple(str(x) for x in item_ids_raw),
        )


@dataclass(frozen=True, slots=True)
class PassRecord:
    name: str
    status: Literal["succeeded", "failed", "skipped"]
    output_sha256: str | None = None
    metadata: dict[str, JSONValue] | None = None

    def __post_init__(self) -> None:
        require_non_empty(self.name, name="pass_record.name")
        require_non_empty(self.status, name="pass_record.status")
        if self.status not in ALLOWED_PASS_STATUSES:
            raise ValueError(f"pass_record.status must be one of {ALLOWED_PASS_STATUSES}; got {self.status!r}")
        if self.output_sha256 is not None:
            s = self.output_sha256.strip().lower()
            if len(s) != 64 or any(ch not in "0123456789abcdef" for ch in s):
                raise ValueError("pass_record.output_sha256 must be a 64-char hex string when set")
            object.__setattr__(self, "output_sha256", s)
        if self.name == "runtime_bundle" and self.metadata is not None:
            _validate_runtime_bundle_metadata(self.metadata)

    def to_json_obj(self) -> dict[str, JSONValue]:
        out: dict[str, JSONValue] = {
            "name": self.name.strip(),
            "status": self.status,
            "output_sha256": self.output_sha256,
            "metadata": dict(self.metadata) if self.metadata else None,
        }
        return {k: v for k, v in out.items() if v is not None}

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> PassRecord:
        metadata = obj.get("metadata")
        if metadata is not None and not isinstance(metadata, dict):
            raise ValueError("pass_record.metadata must be an object when set")
        return PassRecord(
            name=str(obj.get("name", "")),
            status=str(obj.get("status", "")),  # type: ignore[arg-type]
            output_sha256=(str(obj.get("output_sha256")) if obj.get("output_sha256") is not None else None),
            metadata=dict(metadata) if isinstance(metadata, dict) else None,
        )


@dataclass(frozen=True, slots=True)
class ArtifactPointer:
    path: str
    sha256: str | None = None

    def __post_init__(self) -> None:
        require_non_empty(self.path, name="artifact_pointer.path")
        if self.sha256 is not None:
            object.__setattr__(
                self,
                "sha256",
                _validate_sha256_hex(self.sha256, name="artifact_pointer.sha256"),
            )

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "path": self.path.strip(),
            "sha256": self.sha256,
        }

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> ArtifactPointer:
        return ArtifactPointer(
            path=str(obj.get("path", "")),
            sha256=(str(obj.get("sha256")) if obj.get("sha256") is not None else None),
        )


RuntimeEvidenceType = Literal[
    "action_decision",
    "transition_application",
    "retry_budget",
    "reconcile_outcome",
    "reconcile_resource_status",
    "rollback_chain",
    "provider_capability_snapshot",
    "rollback_attempt",
    "rollback_result",
    "terminal_health",
    "convergence_certificate",
]
ALLOWED_RUNTIME_EVIDENCE_TYPES: tuple[str, ...] = (
    "action_decision",
    "transition_application",
    "retry_budget",
    "reconcile_outcome",
    "reconcile_resource_status",
    "rollback_chain",
    "provider_capability_snapshot",
    "rollback_attempt",
    "rollback_result",
    "terminal_health",
    "convergence_certificate",
)


@dataclass(frozen=True, slots=True)
class RuntimeEvidenceRecord:
    evidence_type: RuntimeEvidenceType
    timestamp: int
    runtime_run_id: str
    payload: dict[str, JSONValue]

    def __post_init__(self) -> None:
        require_non_empty(self.evidence_type, name="runtime_evidence_record.evidence_type")
        if self.evidence_type not in ALLOWED_RUNTIME_EVIDENCE_TYPES:
            raise ValueError(
                "runtime_evidence_record.evidence_type must be one of "
                f"{ALLOWED_RUNTIME_EVIDENCE_TYPES}; got {self.evidence_type!r}"
            )
        require_non_empty(self.runtime_run_id, name="runtime_evidence_record.runtime_run_id")
        if isinstance(self.timestamp, bool) or not isinstance(self.timestamp, int) or self.timestamp < 0:
            raise ValueError("runtime_evidence_record.timestamp must be an integer >= 0")
        _validate_json_mapping(self.payload, what="runtime_evidence_record.payload")

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "evidence_type": self.evidence_type,
            "timestamp": int(self.timestamp),
            "runtime_run_id": self.runtime_run_id.strip(),
            "payload": dict(self.payload),
        }

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> RuntimeEvidenceRecord:
        return RuntimeEvidenceRecord(
            evidence_type=cast(RuntimeEvidenceType, str(obj.get("evidence_type", "")).strip()),
            timestamp=json_value_as_int(cast(JSONValue | None, obj.get("timestamp")), default=-1),
            runtime_run_id=str(obj.get("runtime_run_id", "")).strip(),
            payload=_validate_json_mapping(obj.get("payload", {}), what="runtime_evidence_record.payload"),
        )


@dataclass(frozen=True, slots=True)
class RunManifest:
    """Replay/audit contract for one compiler run."""

    run_id: str
    tenant_id: str
    repo_id: str
    ir_sha256: str
    replay_mode: ReplayMode
    # Phase 6: intent layer fingerprints (used to invalidate replay/living caches).
    # These are semantic fingerprints intended to be stable across runs even when
    # generated intent IDs differ.
    intent_semantic_fingerprint: str | None = None
    intent_goal_text_fingerprint: str | None = None
    stable_intent_sha256: str | None = None
    # Success-criterion evaluation modes (from IntentSpec.success_criteria only; not plan steps).
    # Enables offline partial-replay mandate reconstruction without intent JSON on disk.
    success_criteria_evaluation_modes_schema_version: int | None = None
    success_criteria_evaluation_modes: tuple[str, ...] = ()
    intent_acceptance_fingerprint: str | None = None
    # Phase 6: knowledge layer fingerprints (used to invalidate replay when
    # canonical constraint evidence/provenance changes).
    knowledge_semantic_fingerprint: str | None = None
    knowledge_provenance_fingerprint: str | None = None
    # Durable `.akc/knowledge/snapshot.json` pointer (content hash matches output_hashes entry).
    knowledge_snapshot: ArtifactPointer | None = None
    # Durable `.akc/knowledge/mediation.json` pointer (stable_json_fingerprint matches output_hashes entry).
    knowledge_mediation: ArtifactPointer | None = None
    # Durable `.akc/ir/<run_id>.json` pointer (sha256 matches `ir_sha256` when set).
    ir_document: ArtifactPointer | None = None
    ir_format_version: str | None = None
    retrieval_snapshots: tuple[RetrievalSnapshot, ...] = ()
    passes: tuple[PassRecord, ...] = ()
    model: str | None = None
    model_params: dict[str, JSONValue] | None = None
    tool_params: dict[str, JSONValue] | None = None
    partial_replay_passes: tuple[str, ...] = ()
    llm_vcr: dict[str, str] | None = None
    budgets: dict[str, JSONValue] | None = None
    output_hashes: dict[str, str] | None = None
    runtime_bundle: ArtifactPointer | None = None
    runtime_event_transcript: ArtifactPointer | None = None
    runtime_evidence: tuple[RuntimeEvidenceRecord, ...] = ()
    trace_spans: tuple[dict[str, JSONValue], ...] = ()
    control_plane: dict[str, JSONValue] | None = None
    cost_attribution: dict[str, JSONValue] | None = None
    manifest_version: int = RUN_MANIFEST_VERSION

    def __post_init__(self) -> None:
        require_non_empty(self.run_id, name="run_manifest.run_id")
        require_non_empty(self.tenant_id, name="run_manifest.tenant_id")
        require_non_empty(self.repo_id, name="run_manifest.repo_id")
        require_non_empty(self.ir_sha256, name="run_manifest.ir_sha256")
        if self.replay_mode not in ALLOWED_REPLAY_MODES:
            raise ValueError(
                f"run_manifest.replay_mode must be one of {ALLOWED_REPLAY_MODES}; got {self.replay_mode!r}"
            )
        s = self.ir_sha256.strip().lower()
        if len(s) != 64 or any(ch not in "0123456789abcdef" for ch in s):
            raise ValueError("run_manifest.ir_sha256 must be a 64-char hex string")
        object.__setattr__(self, "ir_sha256", s)

        def _maybe_set_fp(field: str, value: str | None) -> str | None:
            if value is None:
                return None
            if not isinstance(value, str):
                raise ValueError(f"run_manifest.{field} must be a string when set")
            sv = value.strip().lower()
            if not sv:
                return None
            # Current intent fingerprints are 16-char hex slices (stable_json_fingerprint).
            if len(sv) != 16 or any(ch not in "0123456789abcdef" for ch in sv):
                raise ValueError(f"run_manifest.{field} must be a 16-char hex fingerprint when set")
            return sv

        object.__setattr__(
            self,
            "intent_semantic_fingerprint",
            _maybe_set_fp("intent_semantic_fingerprint", self.intent_semantic_fingerprint),
        )
        object.__setattr__(
            self,
            "intent_goal_text_fingerprint",
            _maybe_set_fp("intent_goal_text_fingerprint", self.intent_goal_text_fingerprint),
        )
        if self.stable_intent_sha256 is not None:
            object.__setattr__(
                self,
                "stable_intent_sha256",
                _validate_sha256_hex(self.stable_intent_sha256, name="run_manifest.stable_intent_sha256"),
            )

        modes_cleaned = tuple(
            sorted({str(m).strip() for m in self.success_criteria_evaluation_modes if str(m).strip()})
        )
        object.__setattr__(self, "success_criteria_evaluation_modes", modes_cleaned)
        if modes_cleaned:
            sv_raw = self.success_criteria_evaluation_modes_schema_version
            if sv_raw is None:
                object.__setattr__(
                    self,
                    "success_criteria_evaluation_modes_schema_version",
                    SUCCESS_CRITERIA_EVALUATION_MODES_SCHEMA_VERSION,
                )
            else:
                if not isinstance(sv_raw, int) or isinstance(sv_raw, bool):
                    raise ValueError("run_manifest.success_criteria_evaluation_modes_schema_version must be an int")
                if int(sv_raw) != SUCCESS_CRITERIA_EVALUATION_MODES_SCHEMA_VERSION:
                    raise ValueError(
                        "unsupported success_criteria_evaluation_modes_schema_version="
                        f"{sv_raw}; expected {SUCCESS_CRITERIA_EVALUATION_MODES_SCHEMA_VERSION}"
                    )
                object.__setattr__(self, "success_criteria_evaluation_modes_schema_version", int(sv_raw))
        else:
            object.__setattr__(self, "success_criteria_evaluation_modes_schema_version", None)

        object.__setattr__(
            self,
            "intent_acceptance_fingerprint",
            _maybe_set_fp("intent_acceptance_fingerprint", self.intent_acceptance_fingerprint),
        )

        object.__setattr__(
            self,
            "knowledge_semantic_fingerprint",
            _maybe_set_fp("knowledge_semantic_fingerprint", self.knowledge_semantic_fingerprint),
        )
        object.__setattr__(
            self,
            "knowledge_provenance_fingerprint",
            _maybe_set_fp("knowledge_provenance_fingerprint", self.knowledge_provenance_fingerprint),
        )

        if self.ir_document is not None:
            if not isinstance(self.ir_document, ArtifactPointer):
                raise ValueError("run_manifest.ir_document must be an ArtifactPointer when set")
            if self.ir_document.sha256 is not None and self.ir_document.sha256.strip().lower() != self.ir_sha256:
                raise ValueError("run_manifest.ir_document.sha256 must match ir_sha256 when both are set")

        if self.ir_format_version is not None:
            if not isinstance(self.ir_format_version, str) or not self.ir_format_version.strip():
                raise ValueError("run_manifest.ir_format_version must be a non-empty string when set")
            object.__setattr__(self, "ir_format_version", self.ir_format_version.strip())

        if int(self.manifest_version) != RUN_MANIFEST_VERSION:
            raise ValueError(
                f"unsupported run manifest version={self.manifest_version}; expected {RUN_MANIFEST_VERSION}"
            )
        if self.partial_replay_passes:
            cleaned = tuple(str(p).strip() for p in self.partial_replay_passes if str(p).strip())
            invalid = [p for p in cleaned if p not in REPLAYABLE_PASSES]
            if invalid:
                raise ValueError(f"run_manifest.partial_replay_passes contains unsupported pass names: {invalid}")
            object.__setattr__(self, "partial_replay_passes", cleaned)
        if self.llm_vcr is not None and not isinstance(self.llm_vcr, dict):
            raise ValueError("run_manifest.llm_vcr must be an object when set")
        if self.output_hashes is not None:
            if not isinstance(self.output_hashes, dict):
                raise ValueError("run_manifest.output_hashes must be an object when set")
            for path, digest in self.output_hashes.items():
                key = str(path).strip()
                if not key:
                    raise ValueError("run_manifest.output_hashes keys must be non-empty")
                _validate_sha256_hex(str(digest), name="run_manifest.output_hashes values")
        if self.runtime_bundle is not None and not isinstance(self.runtime_bundle, ArtifactPointer):
            raise ValueError("run_manifest.runtime_bundle must be an ArtifactPointer when set")
        if self.knowledge_snapshot is not None and not isinstance(self.knowledge_snapshot, ArtifactPointer):
            raise ValueError("run_manifest.knowledge_snapshot must be an ArtifactPointer when set")
        if self.knowledge_mediation is not None and not isinstance(self.knowledge_mediation, ArtifactPointer):
            raise ValueError("run_manifest.knowledge_mediation must be an ArtifactPointer when set")
        if self.runtime_event_transcript is not None and not isinstance(self.runtime_event_transcript, ArtifactPointer):
            raise ValueError("run_manifest.runtime_event_transcript must be an ArtifactPointer when set")
        for record in self.runtime_evidence:
            if not isinstance(record, RuntimeEvidenceRecord):
                raise ValueError("run_manifest.runtime_evidence[] must be a RuntimeEvidenceRecord")
        if self.trace_spans:
            # Import lazily to avoid akc.run.manifest -> akc.control (package) -> operations_index -> manifest cycles.
            from akc.control.tracing import TraceSpan

            for span in self.trace_spans:
                if not isinstance(span, dict):
                    raise ValueError("run_manifest.trace_spans[] must be an object")
                # Validate OpenTelemetry-compatible span shape and timing.
                start_time = json_value_as_int(span.get("start_time_unix_nano"), default=0)
                end_time = json_value_as_int(span.get("end_time_unix_nano"), default=0)
                attrs_raw = span.get("attributes")
                attributes = attrs_raw if isinstance(attrs_raw, dict) else None
                TraceSpan(
                    trace_id=str(span.get("trace_id", "")),
                    span_id=str(span.get("span_id", "")),
                    parent_span_id=(
                        str(span.get("parent_span_id")) if span.get("parent_span_id") is not None else None
                    ),
                    name=str(span.get("name", "")),
                    kind=str(span.get("kind", "")),
                    start_time_unix_nano=start_time,
                    end_time_unix_nano=end_time,
                    attributes=attributes,
                    status=str(span.get("status", "ok") or "ok"),
                )
        if self.cost_attribution is not None and not isinstance(self.cost_attribution, dict):
            raise ValueError("run_manifest.cost_attribution must be an object when set")
        if self.control_plane is not None and not isinstance(self.control_plane, dict):
            raise ValueError("run_manifest.control_plane must be an object when set")

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "schema_kind": "run_manifest",
            "manifest_version": int(self.manifest_version),
            "run_id": self.run_id.strip(),
            "tenant_id": self.tenant_id.strip(),
            "repo_id": self.repo_id.strip(),
            "ir_sha256": self.ir_sha256,
            "intent_semantic_fingerprint": self.intent_semantic_fingerprint,
            "intent_goal_text_fingerprint": self.intent_goal_text_fingerprint,
            "stable_intent_sha256": self.stable_intent_sha256,
            "success_criteria_evaluation_modes_schema_version": (
                int(self.success_criteria_evaluation_modes_schema_version)
                if self.success_criteria_evaluation_modes_schema_version is not None
                else None
            ),
            "success_criteria_evaluation_modes": list(self.success_criteria_evaluation_modes),
            "intent_acceptance_fingerprint": self.intent_acceptance_fingerprint,
            "knowledge_semantic_fingerprint": self.knowledge_semantic_fingerprint,
            "knowledge_provenance_fingerprint": self.knowledge_provenance_fingerprint,
            "knowledge_snapshot": (
                self.knowledge_snapshot.to_json_obj() if self.knowledge_snapshot is not None else None
            ),
            "knowledge_mediation": (
                self.knowledge_mediation.to_json_obj() if self.knowledge_mediation is not None else None
            ),
            "ir_document": (self.ir_document.to_json_obj() if self.ir_document is not None else None),
            "ir_format_version": self.ir_format_version,
            "replay_mode": self.replay_mode,
            "retrieval_snapshots": [r.to_json_obj() for r in self.retrieval_snapshots],
            "passes": [p.to_json_obj() for p in self.passes],
            "model": (self.model.strip() if isinstance(self.model, str) and self.model.strip() else None),
            "model_params": dict(self.model_params) if self.model_params else None,
            "tool_params": dict(self.tool_params) if self.tool_params else None,
            "partial_replay_passes": list(self.partial_replay_passes),
            "llm_vcr": dict(self.llm_vcr) if self.llm_vcr else None,
            "budgets": dict(self.budgets) if self.budgets else None,
            "output_hashes": dict(self.output_hashes) if self.output_hashes else None,
            "runtime_bundle": (self.runtime_bundle.to_json_obj() if self.runtime_bundle is not None else None),
            "runtime_event_transcript": (
                self.runtime_event_transcript.to_json_obj() if self.runtime_event_transcript is not None else None
            ),
            "runtime_evidence": [record.to_json_obj() for record in self.runtime_evidence],
            "trace_spans": [dict(x) for x in self.trace_spans],
            "control_plane": dict(self.control_plane) if self.control_plane else None,
            "cost_attribution": dict(self.cost_attribution) if self.cost_attribution else None,
        }

    def stable_hash(self) -> str:
        payload = self.to_json_obj()
        return stable_json_fingerprint(payload)

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> RunManifest:
        if str(obj.get("schema_kind", "")) != "run_manifest":
            raise ValueError("run_manifest.schema_kind must be run_manifest")
        snapshots_raw = obj.get("retrieval_snapshots") or []
        if not isinstance(snapshots_raw, Sequence) or isinstance(snapshots_raw, (str, bytes)):
            raise ValueError("run_manifest.retrieval_snapshots must be an array")
        passes_raw = obj.get("passes") or []
        if not isinstance(passes_raw, Sequence) or isinstance(passes_raw, (str, bytes)):
            raise ValueError("run_manifest.passes must be an array")
        model_params_raw = obj.get("model_params")
        if model_params_raw is not None and not isinstance(model_params_raw, dict):
            raise ValueError("run_manifest.model_params must be an object when set")
        tool_params_raw = obj.get("tool_params")
        if tool_params_raw is not None and not isinstance(tool_params_raw, dict):
            raise ValueError("run_manifest.tool_params must be an object when set")
        partial_replay_passes_raw = obj.get("partial_replay_passes") or []
        if not isinstance(partial_replay_passes_raw, Sequence) or isinstance(partial_replay_passes_raw, (str, bytes)):
            raise ValueError("run_manifest.partial_replay_passes must be an array")
        llm_vcr_raw = obj.get("llm_vcr")
        if llm_vcr_raw is not None and not isinstance(llm_vcr_raw, dict):
            raise ValueError("run_manifest.llm_vcr must be an object when set")
        budgets_raw = obj.get("budgets")
        if budgets_raw is not None and not isinstance(budgets_raw, dict):
            raise ValueError("run_manifest.budgets must be an object when set")
        output_hashes_raw = obj.get("output_hashes")
        if output_hashes_raw is not None and not isinstance(output_hashes_raw, dict):
            raise ValueError("run_manifest.output_hashes must be an object when set")
        runtime_bundle_raw = obj.get("runtime_bundle")
        if runtime_bundle_raw is not None and not isinstance(runtime_bundle_raw, dict):
            raise ValueError("run_manifest.runtime_bundle must be an object when set")
        runtime_event_transcript_raw = obj.get("runtime_event_transcript")
        if runtime_event_transcript_raw is not None and not isinstance(runtime_event_transcript_raw, dict):
            raise ValueError("run_manifest.runtime_event_transcript must be an object when set")
        runtime_evidence_raw = obj.get("runtime_evidence") or []
        if not isinstance(runtime_evidence_raw, Sequence) or isinstance(runtime_evidence_raw, (str, bytes)):
            raise ValueError("run_manifest.runtime_evidence must be an array")
        trace_spans_raw = obj.get("trace_spans") or []
        if not isinstance(trace_spans_raw, Sequence) or isinstance(trace_spans_raw, (str, bytes)):
            raise ValueError("run_manifest.trace_spans must be an array")
        control_plane_raw = obj.get("control_plane")
        if control_plane_raw is not None and not isinstance(control_plane_raw, dict):
            raise ValueError("run_manifest.control_plane must be an object when set")
        cost_attr_raw = obj.get("cost_attribution")
        if cost_attr_raw is not None and not isinstance(cost_attr_raw, dict):
            raise ValueError("run_manifest.cost_attribution must be an object when set")

        intent_semantic_fp_raw = obj.get("intent_semantic_fingerprint")
        intent_semantic_fp = (
            str(intent_semantic_fp_raw).strip().lower()
            if isinstance(intent_semantic_fp_raw, str) and intent_semantic_fp_raw.strip()
            else None
        )
        intent_goal_text_fp_raw = obj.get("intent_goal_text_fingerprint")
        intent_goal_text_fp = (
            str(intent_goal_text_fp_raw).strip().lower()
            if isinstance(intent_goal_text_fp_raw, str) and intent_goal_text_fp_raw.strip()
            else None
        )
        stable_intent_sha256_raw = obj.get("stable_intent_sha256")
        stable_intent_sha256 = (
            str(stable_intent_sha256_raw).strip().lower()
            if isinstance(stable_intent_sha256_raw, str) and stable_intent_sha256_raw.strip()
            else None
        )

        modes_raw = obj.get("success_criteria_evaluation_modes") or []
        if not isinstance(modes_raw, Sequence) or isinstance(modes_raw, (str, bytes)):
            raise ValueError("run_manifest.success_criteria_evaluation_modes must be an array")
        modes_tuple = tuple(str(x) for x in modes_raw)
        scm_schema_raw = obj.get("success_criteria_evaluation_modes_schema_version")
        scm_schema: int | None = None
        if scm_schema_raw is not None:
            if isinstance(scm_schema_raw, bool) or not isinstance(scm_schema_raw, int):
                raise ValueError(
                    "run_manifest.success_criteria_evaluation_modes_schema_version must be an int when set"
                )
            scm_schema = int(scm_schema_raw)

        intent_acceptance_fp_raw = obj.get("intent_acceptance_fingerprint")
        intent_acceptance_fp = (
            str(intent_acceptance_fp_raw).strip().lower()
            if isinstance(intent_acceptance_fp_raw, str) and intent_acceptance_fp_raw.strip()
            else None
        )

        knowledge_semantic_fp_raw = obj.get("knowledge_semantic_fingerprint")
        knowledge_semantic_fp = (
            str(knowledge_semantic_fp_raw).strip().lower()
            if isinstance(knowledge_semantic_fp_raw, str) and knowledge_semantic_fp_raw.strip()
            else None
        )
        knowledge_provenance_fp_raw = obj.get("knowledge_provenance_fingerprint")
        knowledge_provenance_fp = (
            str(knowledge_provenance_fp_raw).strip().lower()
            if isinstance(knowledge_provenance_fp_raw, str) and knowledge_provenance_fp_raw.strip()
            else None
        )

        knowledge_snapshot_raw = obj.get("knowledge_snapshot")
        knowledge_snapshot: ArtifactPointer | None = None
        if knowledge_snapshot_raw is not None:
            if not isinstance(knowledge_snapshot_raw, dict):
                raise ValueError("run_manifest.knowledge_snapshot must be an object when set")
            knowledge_snapshot = ArtifactPointer.from_json_obj(knowledge_snapshot_raw)

        knowledge_mediation_raw = obj.get("knowledge_mediation")
        knowledge_mediation: ArtifactPointer | None = None
        if knowledge_mediation_raw is not None:
            if not isinstance(knowledge_mediation_raw, dict):
                raise ValueError("run_manifest.knowledge_mediation must be an object when set")
            knowledge_mediation = ArtifactPointer.from_json_obj(knowledge_mediation_raw)

        ir_document_raw = obj.get("ir_document")
        ir_document: ArtifactPointer | None = None
        if ir_document_raw is not None:
            if not isinstance(ir_document_raw, dict):
                raise ValueError("run_manifest.ir_document must be an object when set")
            ir_document = ArtifactPointer.from_json_obj(ir_document_raw)

        ir_format_version_raw = obj.get("ir_format_version")
        ir_format_version: str | None = None
        if isinstance(ir_format_version_raw, str) and ir_format_version_raw.strip():
            ir_format_version = str(ir_format_version_raw).strip()

        snapshots: list[RetrievalSnapshot] = []
        for snap in snapshots_raw:
            if not isinstance(snap, dict):
                raise ValueError("run_manifest.retrieval_snapshots[] must be an object")
            snapshots.append(RetrievalSnapshot.from_json_obj(snap))
        passes: list[PassRecord] = []
        for rec in passes_raw:
            if not isinstance(rec, dict):
                raise ValueError("run_manifest.passes[] must be an object")
            passes.append(PassRecord.from_json_obj(rec))
        runtime_evidence: list[RuntimeEvidenceRecord] = []
        for record in runtime_evidence_raw:
            if not isinstance(record, dict):
                raise ValueError("run_manifest.runtime_evidence[] must be an object")
            runtime_evidence.append(RuntimeEvidenceRecord.from_json_obj(record))
        for span in trace_spans_raw:
            if not isinstance(span, dict):
                raise ValueError("run_manifest.trace_spans[] must be an object")
        return RunManifest(
            run_id=str(obj.get("run_id", "")),
            tenant_id=str(obj.get("tenant_id", "")),
            repo_id=str(obj.get("repo_id", "")),
            ir_sha256=str(obj.get("ir_sha256", "")),
            intent_semantic_fingerprint=intent_semantic_fp,
            intent_goal_text_fingerprint=intent_goal_text_fp,
            stable_intent_sha256=stable_intent_sha256,
            success_criteria_evaluation_modes_schema_version=scm_schema,
            success_criteria_evaluation_modes=modes_tuple,
            intent_acceptance_fingerprint=intent_acceptance_fp,
            knowledge_semantic_fingerprint=knowledge_semantic_fp,
            knowledge_provenance_fingerprint=knowledge_provenance_fp,
            knowledge_snapshot=knowledge_snapshot,
            knowledge_mediation=knowledge_mediation,
            ir_document=ir_document,
            ir_format_version=ir_format_version,
            replay_mode=str(obj.get("replay_mode", "")),  # type: ignore[arg-type]
            retrieval_snapshots=tuple(snapshots),
            passes=tuple(passes),
            model=(str(obj.get("model")) if obj.get("model") is not None else None),
            model_params=(dict(model_params_raw) if isinstance(model_params_raw, dict) else None),
            tool_params=(dict(tool_params_raw) if isinstance(tool_params_raw, dict) else None),
            partial_replay_passes=tuple(str(x) for x in partial_replay_passes_raw),
            llm_vcr=(dict(llm_vcr_raw) if isinstance(llm_vcr_raw, dict) else None),
            budgets=(dict(budgets_raw) if isinstance(budgets_raw, dict) else None),
            output_hashes=(dict(output_hashes_raw) if isinstance(output_hashes_raw, dict) else None),
            runtime_bundle=(
                ArtifactPointer.from_json_obj(runtime_bundle_raw) if isinstance(runtime_bundle_raw, dict) else None
            ),
            runtime_event_transcript=(
                ArtifactPointer.from_json_obj(runtime_event_transcript_raw)
                if isinstance(runtime_event_transcript_raw, dict)
                else None
            ),
            runtime_evidence=tuple(runtime_evidence),
            trace_spans=tuple(dict(x) for x in trace_spans_raw),
            control_plane=(dict(control_plane_raw) if isinstance(control_plane_raw, dict) else None),
            cost_attribution=(dict(cost_attr_raw) if isinstance(cost_attr_raw, dict) else None),
            manifest_version=int(obj.get("manifest_version", RUN_MANIFEST_VERSION)),
        )

    @staticmethod
    def from_json_file(path: str | Path) -> RunManifest:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("run_manifest file must contain a JSON object")
        return RunManifest.from_json_obj(raw)
