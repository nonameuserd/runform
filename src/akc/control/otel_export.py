"""AKC trace export for OTel-shaped JSONL (no OpenTelemetry SDK dependency).

Canonical on-disk sink: ``<repo>/.akc/run/<compile_run_id>.otel.jsonl`` (one JSON object
per line). Versioned record shape is documented in
``src/akc/control/schemas/akc_trace_export.v1.schema.json``.

``run_trace_spans`` (``.spans.json``) remains the schema-enveloped manifest sidecar; this
module is the portable bridge for log shippers and collectors that expect NDJSON traces.

Fleet / hosted observability: tail or ship these ``*.otel.jsonl`` files (or HTTP sinks from
:class:`~akc.control.otel_export.HttpPostOtelExportSink`) into Grafana, Loki, or an OTLP
collector—no vendor-specific wiring lives in core AKC.

**Autopilot (optional):** :func:`autopilot_scope_event_to_export_obj` emits ``source=runtime.autopilot_scope``
lines when ``AKC_OTEL_EXPORT_*`` mirrors are set and/or ``AKC_AUTOPILOT_OTEL_STDOUT=1``.

**Runtime extra sinks (environment, off by default):** ``akc runtime`` wires
:class:`~akc.runtime.state_store.FileSystemRuntimeStateStore` with
:func:`otel_export_extra_callbacks_from_env` so each appended line is mirrored when:

- ``AKC_OTEL_EXPORT_STDOUT=1`` — duplicate lines to stdout
- ``AKC_OTEL_EXPORT_HTTP_URL`` — POST each line as JSON (failures are swallowed)
- ``AKC_OTEL_EXPORT_FILE`` — append to an extra path (e.g. a shared NDJSON drop)
- ``AKC_OTEL_EXPORT_HTTP_TIMEOUT_SEC`` — optional timeout for HTTP (default 10)
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol

from akc.control.tracing import TraceSpan
from akc.memory.models import JSONValue

AKC_TRACE_EXPORT_VERSION: int = 1
OTEL_SERVICE_NAME: str = "akc"

ExportSource = Literal[
    "compile.trace_span",
    "runtime.trace_span",
    "runtime.coordination_audit",
    "runtime.autopilot_scope",
]


def _coerce_json_int(value: object, *, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return default


def stable_intent_sha256_from_mapping(intent_ref: Mapping[str, Any] | None) -> str | None:
    """Return lower-case hex ``stable_intent_sha256`` from a bundle ``intent_ref`` object."""

    if intent_ref is None:
        return None
    raw = str(intent_ref.get("stable_intent_sha256", "")).strip().lower()
    if len(raw) == 64 and all(ch in "0123456789abcdef" for ch in raw):
        return raw
    return None


def intent_observability_resource_attrs(
    intent_projection: Mapping[str, Any] | None,
) -> dict[str, JSONValue]:
    """Map intent projection ``observability`` slice (from runtime intent projection) to resource attrs."""

    if intent_projection is None:
        return {}
    raw_summary = intent_projection.get("success_criteria_summary")
    if not isinstance(raw_summary, Mapping):
        return {}
    obs = raw_summary.get("observability")
    if not isinstance(obs, Mapping):
        return {}
    out: dict[str, JSONValue] = {}
    stubs = obs.get("otel_query_stubs")
    if isinstance(stubs, Sequence) and not isinstance(stubs, (str, bytes)):
        joined = ",".join(sorted({str(s).strip() for s in stubs if str(s).strip()}))
        if joined:
            out["akc.intent.otel_query_stubs"] = joined
    tags = obs.get("intent_trace_tags")
    if isinstance(tags, Sequence) and not isinstance(tags, (str, bytes)):
        tag_joined = ",".join(sorted({str(t).strip() for t in tags if str(t).strip()}))
        if tag_joined:
            out["akc.intent.trace_tags"] = tag_joined
    return out


def build_resource_attributes(
    *,
    tenant_id: str,
    repo_id: str,
    run_id: str,
    stable_intent_sha256: str | None = None,
    runtime_run_id: str | None = None,
    intent_projection: Mapping[str, Any] | None = None,
) -> dict[str, JSONValue]:
    """Resource attributes for export records (tenant/repo isolation + correlation)."""

    attrs: dict[str, JSONValue] = {
        "service.name": OTEL_SERVICE_NAME,
        "akc.tenant_id": tenant_id.strip(),
        "akc.repo_id": repo_id.strip(),
        "akc.run_id": run_id.strip(),
    }
    if stable_intent_sha256 is not None:
        attrs["akc.stable_intent_sha256"] = stable_intent_sha256.strip().lower()
    if runtime_run_id is not None and str(runtime_run_id).strip():
        attrs["akc.runtime_run_id"] = str(runtime_run_id).strip()
    attrs.update(intent_observability_resource_attrs(intent_projection))
    return attrs


def _span_correlation_attributes(
    *,
    tenant_id: str,
    repo_id: str,
    run_id: str,
    stable_intent_sha256: str | None,
    runtime_run_id: str | None,
) -> dict[str, JSONValue]:
    out: dict[str, JSONValue] = {
        "akc.tenant_id": tenant_id.strip(),
        "akc.repo_id": repo_id.strip(),
        "akc.run_id": run_id.strip(),
    }
    if stable_intent_sha256 is not None:
        out["akc.stable_intent_sha256"] = stable_intent_sha256.strip().lower()
    if runtime_run_id is not None and str(runtime_run_id).strip():
        out["akc.runtime_run_id"] = str(runtime_run_id).strip()
    return out


def _merge_span_attributes(
    base: dict[str, JSONValue] | None,
    correlation: dict[str, JSONValue],
) -> dict[str, JSONValue]:
    merged: dict[str, JSONValue] = dict(correlation)
    if base:
        for key, value in base.items():
            merged.setdefault(key, value)
    return merged


def trace_span_dict_to_export_obj(
    span: Mapping[str, JSONValue],
    *,
    tenant_id: str,
    repo_id: str,
    run_id: str,
    source: ExportSource,
    stable_intent_sha256: str | None = None,
    runtime_run_id: str | None = None,
    intent_projection: Mapping[str, Any] | None = None,
) -> dict[str, JSONValue]:
    """Build one ``akc_trace_export`` record from a :class:`TraceSpan`-compatible dict."""

    correlation = _span_correlation_attributes(
        tenant_id=tenant_id,
        repo_id=repo_id,
        run_id=run_id,
        stable_intent_sha256=stable_intent_sha256,
        runtime_run_id=runtime_run_id,
    )
    raw_attrs = span.get("attributes")
    base_attrs = dict(raw_attrs) if isinstance(raw_attrs, dict) else None
    raw_parent = span.get("parent_span_id", None)
    parent_span_id: str | None
    if raw_parent is None:
        parent_span_id = None
    else:
        ps = str(raw_parent).strip()
        parent_span_id = ps or None
    span_body: dict[str, JSONValue] = {
        "trace_id": str(span.get("trace_id", "")).strip(),
        "span_id": str(span.get("span_id", "")).strip(),
        "parent_span_id": parent_span_id,
        "name": str(span.get("name", "")).strip(),
        "kind": str(span.get("kind", "")).strip(),
        "start_time_unix_nano": _coerce_json_int(span.get("start_time_unix_nano"), default=0),
        "end_time_unix_nano": _coerce_json_int(span.get("end_time_unix_nano"), default=0),
        "attributes": _merge_span_attributes(base_attrs, correlation),
        "status": str(span.get("status", "ok")).strip(),
    }
    return {
        "akc_trace_export_version": int(AKC_TRACE_EXPORT_VERSION),
        "source": source,
        "resource": {
            "attributes": build_resource_attributes(
                tenant_id=tenant_id,
                repo_id=repo_id,
                run_id=run_id,
                stable_intent_sha256=stable_intent_sha256,
                runtime_run_id=runtime_run_id,
                intent_projection=intent_projection,
            )
        },
        "span": span_body,
    }


def trace_span_to_export_obj(
    span: TraceSpan,
    *,
    tenant_id: str,
    repo_id: str,
    run_id: str,
    source: ExportSource,
    stable_intent_sha256: str | None = None,
    runtime_run_id: str | None = None,
    intent_projection: Mapping[str, Any] | None = None,
) -> dict[str, JSONValue]:
    return trace_span_dict_to_export_obj(
        span.to_json_obj(),
        tenant_id=tenant_id,
        repo_id=repo_id,
        run_id=run_id,
        source=source,
        stable_intent_sha256=stable_intent_sha256,
        runtime_run_id=runtime_run_id,
        intent_projection=intent_projection,
    )


def autopilot_scope_event_to_export_obj(
    *,
    tenant_id: str,
    repo_id: str,
    span_name: str,
    attributes: Mapping[str, JSONValue],
    now_ms: int,
) -> dict[str, JSONValue]:
    """One NDJSON trace record for autopilot scope observability (escalation, lease, budgets).

    Uses ``akc.run_id=autopilot`` as a stable logical run id for log routing; tenant/repo isolate scope.
    """

    tid = str(uuid.uuid4()).replace("-", "")
    sid = str(uuid.uuid4()).replace("-", "")
    ts_ns = max(int(now_ms), 0) * 1_000_000
    base_attrs: dict[str, JSONValue] = {
        **_span_correlation_attributes(
            tenant_id=tenant_id,
            repo_id=repo_id,
            run_id="autopilot",
            stable_intent_sha256=None,
            runtime_run_id=None,
        ),
        "akc.autopilot.span": str(span_name).strip(),
    }
    for k, v in attributes.items():
        base_attrs[str(k)] = v
    span_body: dict[str, JSONValue] = {
        "trace_id": tid,
        "span_id": sid,
        "parent_span_id": None,
        "name": str(span_name).strip(),
        "kind": "internal",
        "start_time_unix_nano": ts_ns,
        "end_time_unix_nano": ts_ns,
        "attributes": base_attrs,
        "status": "ok",
    }
    return {
        "akc_trace_export_version": int(AKC_TRACE_EXPORT_VERSION),
        "source": "runtime.autopilot_scope",
        "resource": {
            "attributes": build_resource_attributes(
                tenant_id=tenant_id,
                repo_id=repo_id,
                run_id="autopilot",
                stable_intent_sha256=None,
                runtime_run_id=None,
                intent_projection=None,
            )
        },
        "span": span_body,
    }


def coordination_audit_record_to_export_obj(
    record: Any,
    *,
    stable_intent_sha256: str | None = None,
    intent_projection: Mapping[str, Any] | None = None,
) -> dict[str, JSONValue] | None:
    """Map a :class:`CoordinationAuditRecord` to an export object (coordination span)."""

    otel = getattr(record, "otel_trace", None)
    if otel is None:
        return None
    trace_id = str(getattr(otel, "trace_id", "")).strip()
    span_id = str(getattr(otel, "span_id", "")).strip()
    parent = getattr(otel, "parent_span_id", None)
    parent_span_id = str(parent).strip() if parent is not None else None
    ts_ms = int(getattr(record, "timestamp_ms", 0))
    ts_ns = max(ts_ms, 0) * 1_000_000
    tenant_id = str(getattr(record, "tenant_id", "")).strip()
    repo_id = str(getattr(record, "repo_id", "")).strip()
    run_id = str(getattr(record, "compile_run_id", "")).strip()
    runtime_run_id = str(getattr(record, "runtime_run_id", "")).strip()
    event_type = str(getattr(record, "event_type", "")).strip()
    span_name = f"akc.coordination.{event_type}" if event_type else "akc.coordination.audit"
    attrs: dict[str, JSONValue] = {
        **_span_correlation_attributes(
            tenant_id=tenant_id,
            repo_id=repo_id,
            run_id=run_id,
            stable_intent_sha256=stable_intent_sha256,
            runtime_run_id=runtime_run_id,
        ),
        "akc.coordination.event_id": str(getattr(record, "event_id", "")).strip(),
        "akc.coordination.role_id": str(getattr(record, "role_id", "")).strip(),
        "akc.coordination.graph_step_id": str(getattr(record, "graph_step_id", "")).strip(),
        "akc.coordination.action_id": str(getattr(record, "action_id", "")).strip(),
    }
    ek = getattr(record, "coordination_edge_kind", None)
    if isinstance(ek, str) and ek.strip():
        attrs["akc.coordination.edge_kind"] = ek.strip()
    hid = getattr(record, "handoff_id", None)
    if isinstance(hid, str) and hid.strip():
        attrs["akc.coordination.handoff_id"] = hid.strip()
    dk = getattr(record, "delegate_kind", None)
    if isinstance(dk, str) and dk.strip():
        attrs["akc.coordination.delegate_kind"] = dk.strip()
    lph = getattr(record, "lowered_precedence_hash", None)
    if isinstance(lph, str) and lph.strip():
        attrs["akc.coordination.lowered_precedence_hash"] = lph.strip().lower()
    span_body: dict[str, JSONValue] = {
        "trace_id": trace_id,
        "span_id": span_id,
        "parent_span_id": parent_span_id,
        "name": span_name,
        "kind": "internal",
        "start_time_unix_nano": ts_ns,
        "end_time_unix_nano": ts_ns,
        "attributes": attrs,
        "status": "ok",
    }
    return {
        "akc_trace_export_version": int(AKC_TRACE_EXPORT_VERSION),
        "source": "runtime.coordination_audit",
        "resource": {
            "attributes": build_resource_attributes(
                tenant_id=tenant_id,
                repo_id=repo_id,
                run_id=run_id,
                stable_intent_sha256=stable_intent_sha256,
                runtime_run_id=runtime_run_id,
                intent_projection=intent_projection,
            )
        },
        "span": span_body,
    }


def export_obj_to_json_line(obj: Mapping[str, JSONValue]) -> str:
    return json.dumps(dict(obj), sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def build_compile_trace_export_text(
    *,
    spans: Sequence[Mapping[str, JSONValue]],
    tenant_id: str,
    repo_id: str,
    run_id: str,
    stable_intent_sha256: str | None,
) -> str:
    """NDJSON body for ``.otel.jsonl`` at compile emit time (no runtime spans yet)."""

    lines: list[str] = []
    for span in spans:
        if not isinstance(span, Mapping):
            continue
        rec = trace_span_dict_to_export_obj(
            span,
            tenant_id=tenant_id,
            repo_id=repo_id,
            run_id=run_id,
            source="compile.trace_span",
            stable_intent_sha256=stable_intent_sha256,
            runtime_run_id=None,
        )
        lines.append(export_obj_to_json_line(rec))
    return ("\n".join(lines) + ("\n" if lines else "")) if lines else ""


def append_line_to_run_otel_jsonl(*, repo_root: Path, compile_run_id: str, line: str) -> None:
    """Append one export line under ``<repo_root>/.akc/run/<compile_run_id>.otel.jsonl``."""

    path = Path(repo_root).expanduser() / ".akc" / "run" / f"{str(compile_run_id).strip()}.otel.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(str(line).strip())
        fh.write("\n")


class OtelExportSink(Protocol):
    def write_line(self, line: str) -> None: ...


@dataclass(frozen=True, slots=True)
class StdoutOtelExportSink:
    """Emit each record as one stdout line (optional operator wiring)."""

    def write_line(self, line: str) -> None:
        sys.stdout.write(str(line).strip())
        sys.stdout.write("\n")
        sys.stdout.flush()


@dataclass(frozen=True, slots=True)
class FileAppendOtelExportSink:
    """Append to a fixed path (optional secondary sink)."""

    path: Path

    def write_line(self, line: str) -> None:
        p = Path(self.path).expanduser()
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("a", encoding="utf-8") as fh:
            fh.write(str(line).strip())
            fh.write("\n")


@dataclass(frozen=True, slots=True)
class HttpPostOtelExportSink:
    """POST each record as ``application/json`` (off by default; can be noisy)."""

    url: str
    timeout_sec: float = 10.0

    def write_line(self, line: str) -> None:
        data = str(line).strip().encode("utf-8")
        req = urllib.request.Request(
            self.url,
            data=data,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "User-Agent": "akc-otel-export/1",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=float(self.timeout_sec)) as resp:
                _ = resp.read()
        except urllib.error.URLError:
            return


@dataclass(frozen=True, slots=True)
class MultiOtelExportSink:
    sinks: tuple[OtelExportSink, ...]

    def write_line(self, line: str) -> None:
        for sink in self.sinks:
            sink.write_line(line)


def mirror_line_to_callbacks(line: str, callbacks: Sequence[Callable[[str], None]]) -> None:
    for cb in callbacks:
        cb(line)


def _env_truthy(raw: str | None) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "on"}


def otel_export_extra_callbacks_from_env(
    *,
    environ: Mapping[str, str] | None = None,
) -> tuple[Callable[[str], None], ...]:
    """Build optional OTel JSONL mirror callbacks from process environment (empty by default)."""

    env: Mapping[str, str] = environ if environ is not None else os.environ
    sinks: list[OtelExportSink] = []
    if _env_truthy(env.get("AKC_OTEL_EXPORT_STDOUT")):
        sinks.append(StdoutOtelExportSink())
    url = str(env.get("AKC_OTEL_EXPORT_HTTP_URL", "") or "").strip()
    if url:
        timeout_raw = str(env.get("AKC_OTEL_EXPORT_HTTP_TIMEOUT_SEC", "") or "").strip()
        try:
            timeout_sec = float(timeout_raw) if timeout_raw else 10.0
        except ValueError:
            timeout_sec = 10.0
        sinks.append(HttpPostOtelExportSink(url=url, timeout_sec=timeout_sec))
    mirror = str(env.get("AKC_OTEL_EXPORT_FILE", "") or "").strip()
    if mirror:
        sinks.append(FileAppendOtelExportSink(path=Path(mirror)))
    return tuple(s.write_line for s in sinks)
