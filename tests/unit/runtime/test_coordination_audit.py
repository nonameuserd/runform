"""Coordination audit records and OTel JSON mapping."""

from __future__ import annotations

from akc.runtime.coordination.audit import (
    CoordinationAuditRecord,
    coordination_audit_record_from_action_event,
    lowered_precedence_edges_fingerprint,
    merge_coordination_telemetry_into_payload,
    otel_trace_json_from_akc_event,
    policy_envelope_sha256,
)
from akc.runtime.models import (
    RuntimeAction,
    RuntimeActionResult,
    RuntimeBundle,
    RuntimeBundleRef,
    RuntimeContext,
    RuntimeNodeRef,
)


def test_policy_envelope_sha256_stable() -> None:
    h1 = policy_envelope_sha256(policy_envelope={"allow_network": True})
    h2 = policy_envelope_sha256(policy_envelope={"allow_network": True})
    assert h1 == h2
    assert len(h1) == 64


def test_otel_trace_json_parent() -> None:
    o = otel_trace_json_from_akc_event(
        trace_id="a" * 32,
        event_id="run-1:runtime.action.completed:99",
        parent_event_id="run-1:runtime.action.policy_allowed:1",
    )
    assert o.trace_id == "a" * 32
    assert len(o.span_id) == 16
    assert o.parent_span_id is not None
    assert len(o.parent_span_id) == 16


def test_merge_coordination_skips_non_coordination_action() -> None:
    action = RuntimeAction(
        action_id="a1",
        action_type="service.execute",
        node_ref=RuntimeNodeRef(node_id="n1", kind="service", contract_id="c"),
        inputs_fingerprint="b" * 64,
        idempotency_key="k1",
        policy_context=None,
    )
    merged = merge_coordination_telemetry_into_payload(
        event_type="runtime.action.completed",
        policy_envelope_sha256_digest="c" * 64,
        orchestration_spec_sha256=None,
        coordination_spec_version=None,
        trace_id="t" * 32,
        event_id="e1",
        action=action,
        result=RuntimeActionResult(status="succeeded"),
        parent_event_id=None,
    )
    assert merged == {}


def test_merge_coordination_plan_enqueued() -> None:
    lowered = "d" * 64
    merged = merge_coordination_telemetry_into_payload(
        event_type="runtime.coordination.plan_enqueued",
        policy_envelope_sha256_digest="c" * 64,
        orchestration_spec_sha256="a" * 64,
        coordination_spec_version=1,
        trace_id="t" * 32,
        event_id="e1",
        action=None,
        result=None,
        parent_event_id=None,
        lowered_precedence_hash=lowered,
    )
    assert merged["policy_envelope_sha256"] == "c" * 64
    assert merged["orchestration_spec_sha256"] == "a" * 64
    assert merged["coordination_spec_version"] == 1
    assert merged["lowered_precedence_hash"] == lowered
    assert "otel_trace" in merged


def test_lowered_precedence_edges_fingerprint_stable() -> None:
    edges = (
        {
            "dst_step_id": "b",
            "lowered_from_edge_ids": ["e1"],
            "original_kinds": ["depends_on"],
            "src_step_id": "a",
        },
    )
    assert len(lowered_precedence_edges_fingerprint(edges)) == 64


def test_merge_coordination_telemetry_phase5_keys_from_policy_context() -> None:
    lowered = "f" * 64
    action = RuntimeAction(
        action_id="a1",
        action_type="coordination.step",
        node_ref=RuntimeNodeRef(node_id="n1", kind="service", contract_id="c"),
        inputs_fingerprint="b" * 64,
        idempotency_key="k1",
        policy_context={
            "coordination_step_id": "s1",
            "coordination_role_id": "role-a",
            "coordination_spec_sha256": "a" * 64,
            "coordination_edge_kind": "handoff,parallel",
            "coordination_handoff_id": "h1",
            "coordination_delegate_kind": "http",
            "coordination_lowered_precedence_hash": lowered,
        },
    )
    merged = merge_coordination_telemetry_into_payload(
        event_type="runtime.action.completed",
        policy_envelope_sha256_digest="c" * 64,
        orchestration_spec_sha256=None,
        coordination_spec_version=2,
        trace_id="t" * 32,
        event_id="e1",
        action=action,
        result=RuntimeActionResult(status="succeeded"),
        parent_event_id=None,
        lowered_precedence_hash="e" * 64,
    )
    assert merged["coordination_edge_kind"] == "handoff,parallel"
    assert merged["handoff_id"] == "h1"
    assert merged["delegate_kind"] == "http"
    assert merged["lowered_precedence_hash"] == lowered


def test_coordination_audit_record_json_line() -> None:
    rec = CoordinationAuditRecord(
        record_version=1,
        timestamp_ms=1,
        event_id="e",
        event_type="runtime.action.completed",
        compile_run_id="run-1",
        runtime_run_id="rr",
        tenant_id="t",
        repo_id="r",
        coordination_spec_sha256="a" * 64,
        role_id="role-a",
        graph_step_id="step-1",
        action_id="act",
        idempotency_key="idem",
        policy_envelope_sha256="b" * 64,
        input_sha256="c" * 64,
        output_sha256="d" * 64,
        bundle_manifest_hash="f" * 64,
        otel_trace=otel_trace_json_from_akc_event(trace_id="t" * 32, event_id="e", parent_event_id=None),
    )
    line = rec.to_json_line()
    assert line.startswith("{")
    assert '"event_id":"e"' in line


def test_coordination_audit_record_from_action_event_phase5_fields() -> None:
    ctx = RuntimeContext(
        tenant_id="t",
        repo_id="r",
        run_id="run-1",
        runtime_run_id="rr-1",
        policy_mode="enforce",
        adapter_id="native",
    )
    ref = RuntimeBundleRef(
        bundle_path="/x/.akc/runtime/b.json",
        manifest_hash="f" * 64,
        created_at=0,
        source_compile_run_id="run-1",
    )
    bundle = RuntimeBundle(
        context=ctx,
        ref=ref,
        nodes=(),
        contract_ids=(),
        policy_envelope={},
        metadata={},
    )
    lowered = "a" * 64
    action = RuntimeAction(
        action_id="a1",
        action_type="coordination.step",
        node_ref=RuntimeNodeRef(node_id="n1", kind="service", contract_id="c"),
        inputs_fingerprint="b" * 64,
        idempotency_key="k1",
        policy_context={
            "coordination_step_id": "step-1",
            "coordination_role_id": "role-a",
            "coordination_spec_sha256": "c" * 64,
            "coordination_edge_kind": "delegate",
            "coordination_handoff_id": "h99",
            "coordination_delegate_kind": "opaque",
            "coordination_lowered_precedence_hash": lowered,
        },
    )
    rec = coordination_audit_record_from_action_event(
        context=ctx,
        bundle=bundle,
        event_id="e1",
        event_type="runtime.action.completed",
        timestamp_ms=0,
        action=action,
        result=None,
        parent_event_id=None,
        policy_envelope_sha256_digest="d" * 64,
        orchestration_spec_sha256=None,
        coordination_spec_version=2,
        sequence=0,
        trace_id="t" * 32,
    )
    assert rec is not None
    assert rec.coordination_edge_kind == "delegate"
    assert rec.handoff_id == "h99"
    assert rec.delegate_kind == "opaque"
    assert rec.lowered_precedence_hash == lowered
    obj = rec.to_json_obj()
    assert obj["coordination_edge_kind"] == "delegate"
    assert obj["handoff_id"] == "h99"
    assert obj["delegate_kind"] == "opaque"
    assert obj["lowered_precedence_hash"] == lowered


def test_coordination_audit_record_from_action_event_requires_coordination_context() -> None:
    ctx = RuntimeContext(
        tenant_id="t",
        repo_id="r",
        run_id="run-1",
        runtime_run_id="rr-1",
        policy_mode="enforce",
        adapter_id="native",
    )
    ref = RuntimeBundleRef(
        bundle_path="/x/.akc/runtime/b.json",
        manifest_hash="f" * 64,
        created_at=0,
        source_compile_run_id="run-1",
    )
    bundle = RuntimeBundle(
        context=ctx,
        ref=ref,
        nodes=(),
        contract_ids=(),
        policy_envelope={},
        metadata={},
    )
    action = RuntimeAction(
        action_id="a1",
        action_type="coordination.step",
        node_ref=RuntimeNodeRef(node_id="n1", kind="service", contract_id="c"),
        inputs_fingerprint="b" * 64,
        idempotency_key="k1",
        policy_context=None,
    )
    assert (
        coordination_audit_record_from_action_event(
            context=ctx,
            bundle=bundle,
            event_id="e1",
            event_type="runtime.action.completed",
            timestamp_ms=0,
            action=action,
            result=None,
            parent_event_id=None,
            policy_envelope_sha256_digest="c" * 64,
            orchestration_spec_sha256=None,
            coordination_spec_version=1,
            sequence=0,
            trace_id="t" * 32,
        )
        is None
    )
