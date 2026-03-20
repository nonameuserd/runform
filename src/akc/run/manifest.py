from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from akc.control.tracing import TraceSpan
from akc.memory.models import JSONValue, require_non_empty
from akc.utils.fingerprint import stable_json_fingerprint

ReplayMode = Literal["live", "llm_vcr", "full_replay", "partial_replay"]
RUN_MANIFEST_VERSION = 1
ALLOWED_REPLAY_MODES: tuple[str, ...] = (
    "live",
    "llm_vcr",
    "full_replay",
    "partial_replay",
)
ALLOWED_PASS_STATUSES: tuple[str, ...] = ("succeeded", "failed", "skipped")
REPLAYABLE_PASSES: tuple[str, ...] = ("plan", "retrieve", "generate", "execute", "repair", "verify")


def _json_value_to_int(value: JSONValue | None, *, default: int) -> int:
    """Best-effort conversion for numeric fields persisted in JSON."""
    if value is None:
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        try:
            s = value.strip()
            return int(s) if s else default
        except ValueError:
            return default
    return default


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
            raise ValueError(
                f"pass_record.status must be one of {ALLOWED_PASS_STATUSES}; got {self.status!r}"
            )
        if self.output_sha256 is not None:
            s = self.output_sha256.strip().lower()
            if len(s) != 64 or any(ch not in "0123456789abcdef" for ch in s):
                raise ValueError("pass_record.output_sha256 must be a 64-char hex string when set")
            object.__setattr__(self, "output_sha256", s)

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
            output_sha256=(
                str(obj.get("output_sha256")) if obj.get("output_sha256") is not None else None
            ),
            metadata=dict(metadata) if isinstance(metadata, dict) else None,
        )


@dataclass(frozen=True, slots=True)
class RunManifest:
    """Replay/audit contract for one compiler run."""

    run_id: str
    tenant_id: str
    repo_id: str
    ir_sha256: str
    replay_mode: ReplayMode
    retrieval_snapshots: tuple[RetrievalSnapshot, ...] = ()
    passes: tuple[PassRecord, ...] = ()
    model: str | None = None
    model_params: dict[str, JSONValue] | None = None
    tool_params: dict[str, JSONValue] | None = None
    partial_replay_passes: tuple[str, ...] = ()
    llm_vcr: dict[str, str] | None = None
    budgets: dict[str, JSONValue] | None = None
    output_hashes: dict[str, str] | None = None
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
                "run_manifest.replay_mode must be one of "
                f"{ALLOWED_REPLAY_MODES}; got {self.replay_mode!r}"
            )
        s = self.ir_sha256.strip().lower()
        if len(s) != 64 or any(ch not in "0123456789abcdef" for ch in s):
            raise ValueError("run_manifest.ir_sha256 must be a 64-char hex string")
        object.__setattr__(self, "ir_sha256", s)
        if int(self.manifest_version) != RUN_MANIFEST_VERSION:
            raise ValueError(
                f"unsupported run manifest version={self.manifest_version}; "
                f"expected {RUN_MANIFEST_VERSION}"
            )
        if self.partial_replay_passes:
            cleaned = tuple(str(p).strip() for p in self.partial_replay_passes if str(p).strip())
            invalid = [p for p in cleaned if p not in REPLAYABLE_PASSES]
            if invalid:
                raise ValueError(
                    f"run_manifest.partial_replay_passes contains unsupported pass names: {invalid}"
                )
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
                s2 = str(digest).strip().lower()
                if len(s2) != 64 or any(ch not in "0123456789abcdef" for ch in s2):
                    raise ValueError(
                        "run_manifest.output_hashes values must be 64-char hex sha256 strings"
                    )
        for span in self.trace_spans:
            if not isinstance(span, dict):
                raise ValueError("run_manifest.trace_spans[] must be an object")
            # Validate OpenTelemetry-compatible span shape and timing.
            start_time = _json_value_to_int(span.get("start_time_unix_nano"), default=0)
            end_time = _json_value_to_int(span.get("end_time_unix_nano"), default=0)
            attrs_raw = span.get("attributes")
            attributes = attrs_raw if isinstance(attrs_raw, dict) else None
            TraceSpan(
                trace_id=str(span.get("trace_id", "")),
                span_id=str(span.get("span_id", "")),
                parent_span_id=(
                    str(span.get("parent_span_id"))
                    if span.get("parent_span_id") is not None
                    else None
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
            "replay_mode": self.replay_mode,
            "retrieval_snapshots": [r.to_json_obj() for r in self.retrieval_snapshots],
            "passes": [p.to_json_obj() for p in self.passes],
            "model": (
                self.model.strip() if isinstance(self.model, str) and self.model.strip() else None
            ),
            "model_params": dict(self.model_params) if self.model_params else None,
            "tool_params": dict(self.tool_params) if self.tool_params else None,
            "partial_replay_passes": list(self.partial_replay_passes),
            "llm_vcr": dict(self.llm_vcr) if self.llm_vcr else None,
            "budgets": dict(self.budgets) if self.budgets else None,
            "output_hashes": dict(self.output_hashes) if self.output_hashes else None,
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
        if not isinstance(partial_replay_passes_raw, Sequence) or isinstance(
            partial_replay_passes_raw, (str, bytes)
        ):
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
        trace_spans_raw = obj.get("trace_spans") or []
        if not isinstance(trace_spans_raw, Sequence) or isinstance(trace_spans_raw, (str, bytes)):
            raise ValueError("run_manifest.trace_spans must be an array")
        control_plane_raw = obj.get("control_plane")
        if control_plane_raw is not None and not isinstance(control_plane_raw, dict):
            raise ValueError("run_manifest.control_plane must be an object when set")
        cost_attr_raw = obj.get("cost_attribution")
        if cost_attr_raw is not None and not isinstance(cost_attr_raw, dict):
            raise ValueError("run_manifest.cost_attribution must be an object when set")
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
        for span in trace_spans_raw:
            if not isinstance(span, dict):
                raise ValueError("run_manifest.trace_spans[] must be an object")
        return RunManifest(
            run_id=str(obj.get("run_id", "")),
            tenant_id=str(obj.get("tenant_id", "")),
            repo_id=str(obj.get("repo_id", "")),
            ir_sha256=str(obj.get("ir_sha256", "")),
            replay_mode=str(obj.get("replay_mode", "")),  # type: ignore[arg-type]
            retrieval_snapshots=tuple(snapshots),
            passes=tuple(passes),
            model=(str(obj.get("model")) if obj.get("model") is not None else None),
            model_params=(dict(model_params_raw) if isinstance(model_params_raw, dict) else None),
            tool_params=(dict(tool_params_raw) if isinstance(tool_params_raw, dict) else None),
            partial_replay_passes=tuple(str(x) for x in partial_replay_passes_raw),
            llm_vcr=(dict(llm_vcr_raw) if isinstance(llm_vcr_raw, dict) else None),
            budgets=(dict(budgets_raw) if isinstance(budgets_raw, dict) else None),
            output_hashes=(
                dict(output_hashes_raw) if isinstance(output_hashes_raw, dict) else None
            ),
            trace_spans=tuple(dict(x) for x in trace_spans_raw),
            control_plane=(
                dict(control_plane_raw) if isinstance(control_plane_raw, dict) else None
            ),
            cost_attribution=(dict(cost_attr_raw) if isinstance(cost_attr_raw, dict) else None),
            manifest_version=int(obj.get("manifest_version", RUN_MANIFEST_VERSION)),
        )

    @staticmethod
    def from_json_file(path: str | Path) -> RunManifest:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("run_manifest file must contain a JSON object")
        return RunManifest.from_json_obj(raw)
