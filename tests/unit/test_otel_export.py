from __future__ import annotations

import json
from pathlib import Path

import jsonschema

import akc.control
from akc.control.otel_export import (
    AKC_TRACE_EXPORT_VERSION,
    HttpPostOtelExportSink,
    MultiOtelExportSink,
    append_line_to_run_otel_jsonl,
    build_compile_trace_export_text,
    build_resource_attributes,
    coordination_audit_record_to_export_obj,
    export_obj_to_json_line,
    otel_export_extra_callbacks_from_env,
    stable_intent_sha256_from_mapping,
    trace_span_dict_to_export_obj,
    trace_span_to_export_obj,
)
from akc.control.tracing import TraceSpan
from akc.runtime.coordination.audit import CoordinationAuditRecord, otel_trace_json_from_akc_event
from akc.runtime.models import RuntimeContext
from akc.runtime.state_store import FileSystemRuntimeStateStore


def _export_schema() -> dict:
    pkg = Path(akc.control.__file__).resolve().parent
    schema_path = pkg / "schemas" / "akc_trace_export.v1.schema.json"
    return json.loads(schema_path.read_text(encoding="utf-8"))


def _assert_record_valid(obj: dict) -> None:
    jsonschema.validate(instance=obj, schema=_export_schema())


def test_trace_span_export_has_correlation_on_resource_and_span() -> None:
    span = TraceSpan(
        trace_id="a" * 32,
        span_id="b" * 16,
        parent_span_id=None,
        name="compile.test",
        kind="internal",
        start_time_unix_nano=100,
        end_time_unix_nano=200,
        attributes={"custom": "x"},
        status="ok",
    )
    rec = trace_span_to_export_obj(
        span,
        tenant_id="t1",
        repo_id="r1",
        run_id="run-1",
        source="compile.trace_span",
        stable_intent_sha256="c" * 64,
        runtime_run_id=None,
    )
    _assert_record_valid(rec)
    assert rec["akc_trace_export_version"] == AKC_TRACE_EXPORT_VERSION
    ra = rec["resource"]["attributes"]
    assert ra["akc.tenant_id"] == "t1"
    assert ra["akc.repo_id"] == "r1"
    assert ra["akc.run_id"] == "run-1"
    assert ra["akc.stable_intent_sha256"] == "c" * 64
    sa = rec["span"]["attributes"]
    assert sa["akc.tenant_id"] == "t1"
    assert sa["akc.repo_id"] == "r1"
    assert sa["akc.run_id"] == "run-1"
    assert sa["akc.stable_intent_sha256"] == "c" * 64
    assert sa["custom"] == "x"


def test_runtime_span_includes_runtime_run_id() -> None:
    span = TraceSpan(
        trace_id="a" * 32,
        span_id="b" * 16,
        parent_span_id=None,
        name="runtime.kernel.run",
        kind="internal",
        start_time_unix_nano=10,
        end_time_unix_nano=20,
        status="ok",
    )
    rec = trace_span_to_export_obj(
        span,
        tenant_id="t1",
        repo_id="r1",
        run_id="compile-run",
        source="runtime.trace_span",
        stable_intent_sha256=None,
        runtime_run_id="rr-1",
    )
    _assert_record_valid(rec)
    assert rec["resource"]["attributes"]["akc.runtime_run_id"] == "rr-1"
    assert rec["span"]["attributes"]["akc.runtime_run_id"] == "rr-1"


def test_coordination_audit_export() -> None:
    otel = otel_trace_json_from_akc_event(trace_id="t" * 32, event_id="e1", parent_event_id="p0")
    rec = CoordinationAuditRecord(
        record_version=1,
        timestamp_ms=1700000000000,
        event_id="e1",
        event_type="runtime.action.completed",
        compile_run_id="cr",
        runtime_run_id="rr",
        tenant_id="ten",
        repo_id="rep",
        coordination_spec_sha256="a" * 64,
        role_id="leader",
        graph_step_id="s1",
        action_id="act",
        idempotency_key="idem",
        policy_envelope_sha256="b" * 64,
        input_sha256="c" * 64,
        output_sha256="d" * 64,
        bundle_manifest_hash="e" * 64,
        parent_event_id="p0",
        otel_trace=otel,
    )
    out = coordination_audit_record_to_export_obj(rec, stable_intent_sha256="f" * 64)
    assert out is not None
    _assert_record_valid(out)
    assert out["source"] == "runtime.coordination_audit"
    assert out["span"]["name"] == "akc.coordination.runtime.action.completed"
    assert out["resource"]["attributes"]["akc.stable_intent_sha256"] == "f" * 64


def test_coordination_audit_export_phase5_attributes() -> None:
    otel = otel_trace_json_from_akc_event(trace_id="t" * 32, event_id="e1", parent_event_id=None)
    lowered = "1" * 64
    rec = CoordinationAuditRecord(
        record_version=1,
        timestamp_ms=1700000000000,
        event_id="e1",
        event_type="runtime.action.completed",
        compile_run_id="cr",
        runtime_run_id="rr",
        tenant_id="ten",
        repo_id="rep",
        coordination_spec_sha256="a" * 64,
        role_id="leader",
        graph_step_id="s1",
        action_id="act",
        idempotency_key="idem",
        policy_envelope_sha256="b" * 64,
        input_sha256="c" * 64,
        output_sha256="d" * 64,
        bundle_manifest_hash="e" * 64,
        otel_trace=otel,
        coordination_edge_kind="handoff",
        handoff_id="h1",
        delegate_kind="http",
        lowered_precedence_hash=lowered,
    )
    out = coordination_audit_record_to_export_obj(rec, stable_intent_sha256="f" * 64)
    assert out is not None
    attrs = out["span"]["attributes"]
    assert attrs["akc.coordination.edge_kind"] == "handoff"
    assert attrs["akc.coordination.handoff_id"] == "h1"
    assert attrs["akc.coordination.delegate_kind"] == "http"
    assert attrs["akc.coordination.lowered_precedence_hash"] == lowered


def test_build_compile_trace_export_text_ndjson() -> None:
    spans = [
        {
            "trace_id": "a" * 32,
            "span_id": "b" * 16,
            "parent_span_id": None,
            "name": "n",
            "kind": "internal",
            "start_time_unix_nano": 1,
            "end_time_unix_nano": 2,
            "attributes": None,
            "status": "ok",
        }
    ]
    text = build_compile_trace_export_text(
        spans=spans,
        tenant_id="t",
        repo_id="r",
        run_id="rid",
        stable_intent_sha256="ab" * 32,
    )
    line = text.strip().split("\n")[0]
    obj = json.loads(line)
    _assert_record_valid(obj)


def test_trace_span_dict_preserves_parent_none() -> None:
    d = trace_span_dict_to_export_obj(
        {
            "trace_id": "a" * 32,
            "span_id": "b" * 16,
            "name": "x",
            "kind": "internal",
            "start_time_unix_nano": 5,
            "end_time_unix_nano": 6,
            "status": "ok",
        },
        tenant_id="t",
        repo_id="r",
        run_id="run",
        source="compile.trace_span",
        stable_intent_sha256=None,
    )
    assert d["span"]["parent_span_id"] is None
    _assert_record_valid(d)


def test_stable_intent_sha256_from_mapping() -> None:
    assert stable_intent_sha256_from_mapping({"stable_intent_sha256": "Aa" * 32}) == "aa" * 32
    assert stable_intent_sha256_from_mapping({"stable_intent_sha256": "short"}) is None


def test_append_line_to_run_otel_jsonl(tmp_path: Path) -> None:
    append_line_to_run_otel_jsonl(repo_root=tmp_path, compile_run_id="r1", line='{"x":1}')
    path = tmp_path / ".akc" / "run" / "r1.otel.jsonl"
    assert path.read_text(encoding="utf-8").strip() == '{"x":1}'


def test_filesystem_state_store_otel_path_matches_repo_layout(tmp_path: Path) -> None:
    ctx = RuntimeContext(
        tenant_id="ten",
        repo_id="rep",
        run_id="compile1",
        runtime_run_id="rt1",
        policy_mode="default",
        adapter_id="native",
    )
    store = FileSystemRuntimeStateStore(root=tmp_path)
    store.append_run_otel_export_line(context=ctx, line='{"z":2}')
    otel = tmp_path / "ten" / "rep" / ".akc" / "run" / "compile1.otel.jsonl"
    assert otel.read_text(encoding="utf-8").strip() == '{"z":2}'


def test_build_resource_attributes_minimal() -> None:
    a = build_resource_attributes(tenant_id="t", repo_id="r", run_id="run")
    assert set(a.keys()) == {"service.name", "akc.tenant_id", "akc.repo_id", "akc.run_id"}


def test_build_resource_attributes_merges_intent_observability() -> None:
    projection = {
        "success_criteria_summary": {
            "observability": {
                "otel_query_stubs": ["stub_b", "stub_a"],
                "intent_trace_tags": ["intent.oteld_stub:stub_a"],
            }
        }
    }
    a = build_resource_attributes(
        tenant_id="t",
        repo_id="r",
        run_id="run",
        stable_intent_sha256="a" * 64,
        intent_projection=projection,
    )
    assert a["akc.intent.otel_query_stubs"] == "stub_a,stub_b"
    assert a["akc.intent.trace_tags"] == "intent.oteld_stub:stub_a"


def test_sink_types_smoke() -> None:
    MultiOtelExportSink(sinks=()).write_line("{}")
    # HTTP sink should not raise on unreachable URL (swallows URLError)
    HttpPostOtelExportSink(url="http://127.0.0.1:9/akc-otel-drop").write_line('{"ok":true}')


def test_otel_export_extra_callbacks_from_env_empty() -> None:
    cbs = otel_export_extra_callbacks_from_env(environ={})
    assert cbs == ()


def test_otel_export_extra_callbacks_from_env_file_mirror(tmp_path: Path) -> None:
    mirror = tmp_path / "mirror.jsonl"
    cbs = otel_export_extra_callbacks_from_env(
        environ={"AKC_OTEL_EXPORT_FILE": str(mirror)},
    )
    assert len(cbs) == 1
    cbs[0]('{"k":1}')
    assert mirror.read_text(encoding="utf-8").strip() == '{"k":1}'


def test_export_obj_to_json_line_sorted_keys() -> None:
    obj = trace_span_to_export_obj(
        TraceSpan(
            trace_id="a" * 32,
            span_id="b" * 16,
            parent_span_id=None,
            name="n",
            kind="internal",
            start_time_unix_nano=1,
            end_time_unix_nano=2,
            status="ok",
        ),
        tenant_id="t",
        repo_id="r",
        run_id="run",
        source="compile.trace_span",
    )
    line = export_obj_to_json_line(obj)
    assert line.index("akc_trace_export_version") < line.index("resource")
