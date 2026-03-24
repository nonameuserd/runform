from __future__ import annotations

from pathlib import Path

import pytest

from akc.compile.controller_policy_runtime import runtime_policy_actions
from akc.control.tracing import TraceSpan
from akc.ir.schema import IRDocument, IRNode
from akc.runtime.models import (
    RuntimeAction,
    RuntimeCheckpoint,
    RuntimeContext,
    RuntimeEvent,
    RuntimeNodeRef,
)
from akc.runtime.policy import (
    RUNTIME_POLICY_ACTIONS,
    RuntimePolicyRuntime,
    RuntimeScopeMismatchError,
    derive_scoped_runtime_environment,
    ensure_event_scope,
    merge_runtime_evidence_expectations,
    resolve_reconcile_desired_state_source,
    runtime_evidence_expectation_violations,
)
from akc.runtime.scheduler import RuntimeQueueSnapshot, ScheduledRuntimeAction
from akc.runtime.state_store import FileSystemRuntimeStateStore


def _context(runtime_run_id: str = "runtime-1") -> RuntimeContext:
    return RuntimeContext(
        tenant_id="tenant-a",
        repo_id="repo-a",
        run_id="compile-1",
        runtime_run_id=runtime_run_id,
        policy_mode="enforce",
        adapter_id="native",
    )


def test_runtime_policy_actions_are_exported_for_compile_and_runtime() -> None:
    assert runtime_policy_actions() == RUNTIME_POLICY_ACTIONS


def test_runtime_policy_runtime_authorizes_expected_actions() -> None:
    context = _context()
    policy = RuntimePolicyRuntime.default(context=context)

    decision = policy.authorize(action="runtime.action.dispatch", context=context)

    assert decision.allowed is True
    assert policy.decision_log[-1]["action"] == "runtime.action.dispatch"


def test_runtime_policy_runtime_consumes_intent_projection_deny_rules() -> None:
    context = _context()

    generic = RuntimePolicyRuntime.default(context=context)
    generic_decision = generic.authorize(action="service.reconcile.apply", context=context)
    assert generic_decision.allowed is True

    policy = RuntimePolicyRuntime.from_bundle(
        context=context,
        policy_envelope={},
        intent_projection={
            "policies": [
                {
                    "id": "policy.runtime.reconcile_guardrail",
                    "metadata": {
                        "runtime_deny_actions": ["service.reconcile.apply"],
                    },
                }
            ]
        },
    )

    decision = policy.authorize(action="service.reconcile.apply", context=context)

    assert decision.allowed is False
    assert decision.reason == "policy.default_deny.action_not_allowlisted"
    assert "service.reconcile.apply" in policy.effective_deny_actions


def test_runtime_policy_runtime_fail_closes_unknown_policy_ids_in_enforce_mode() -> None:
    context = _context()
    policy = RuntimePolicyRuntime.from_bundle(
        context=context,
        policy_envelope={},
        intent_projection={"policies": [{"id": "policy.unknown.runtime_guardrail"}]},
    )

    decision = policy.authorize(action="runtime.event.consume", context=context)

    assert decision.allowed is False
    assert policy.unresolved_policy_ids == ("policy.unknown.runtime_guardrail",)


def test_runtime_policy_knowledge_network_forbidden_denies_reconcile() -> None:
    context = _context()
    policy = RuntimePolicyRuntime.from_bundle(
        context=context,
        policy_envelope={"knowledge_network_egress_forbidden": True},
        intent_projection={},
    )

    apply_decision = policy.authorize(action="service.reconcile.apply", context=context)
    assert apply_decision.allowed is False
    assert "service.reconcile.apply" in policy.effective_deny_actions


def test_runtime_policy_ir_policy_node_applies_runtime_deny_actions() -> None:
    context = _context()
    ir = IRDocument(
        tenant_id="tenant-a",
        repo_id="repo-a",
        nodes=(
            IRNode(
                id="pol-1",
                tenant_id="tenant-a",
                kind="policy",
                name="guard",
                properties={
                    "metadata": {
                        "runtime_deny_actions": ["service.reconcile.apply"],
                    },
                },
            ),
        ),
    )
    policy = RuntimePolicyRuntime.from_bundle(
        context=context,
        policy_envelope={},
        intent_projection={},
        ir_document=ir,
    )
    decision = policy.authorize(action="service.reconcile.apply", context=context)
    assert decision.allowed is False


def test_merge_runtime_evidence_expectations_unions_when_widen_allowed() -> None:
    merged = merge_runtime_evidence_expectations(
        derived=("reconciler.health_check",),
        bundle_declared=["custom.signal", "reconciler.health_check"],
        allow_widen=True,
    )
    assert merged == ("custom.signal", "reconciler.health_check")


def test_merge_runtime_evidence_expectations_ignores_bundle_widen_by_default() -> None:
    merged = merge_runtime_evidence_expectations(
        derived=("reconciler.health_check",),
        bundle_declared=["custom.signal", "reconciler.health_check"],
    )
    assert merged == ("reconciler.health_check",)


def test_runtime_policy_runtime_ignores_bundle_evidence_widen_without_envelope_flag() -> None:
    context = _context()
    policy = RuntimePolicyRuntime.from_bundle(
        context=context,
        policy_envelope={},
        intent_projection={
            "success_criteria_summary": {
                "evaluation_modes": ["operational_spec"],
            }
        },
        bundle_evidence_expectations=["custom.widen", "operational_spec"],
    )
    assert "custom.widen" not in policy.evidence_expectations
    assert "operational_spec" in policy.evidence_expectations


def test_runtime_policy_runtime_allows_bundle_evidence_widen_when_envelope_permits() -> None:
    context = _context()
    policy = RuntimePolicyRuntime.from_bundle(
        context=context,
        policy_envelope={"allow_runtime_evidence_expectation_widening": True},
        intent_projection={
            "success_criteria_summary": {
                "evaluation_modes": ["operational_spec"],
            }
        },
        bundle_evidence_expectations=["custom.widen"],
    )
    assert "custom.widen" in policy.evidence_expectations


def test_resolve_reconcile_desired_state_source_strict_ir_missing_ir_raises() -> None:
    with pytest.raises(ValueError, match="reconcile_desired_state_source=ir"):
        resolve_reconcile_desired_state_source(
            ir_document=None,
            payload={"reconcile_desired_state_source": "ir"},
        )


def test_resolve_reconcile_desired_state_source_system_ir_ref_missing_load_raises() -> None:
    with pytest.raises(ValueError, match="references system IR"):
        resolve_reconcile_desired_state_source(
            ir_document=None,
            payload={
                "system_ir_ref": {
                    "path": ".akc/ir/missing.json",
                    "fingerprint": "a" * 64,
                    "format_version": "1",
                    "schema_version": 1,
                }
            },
        )


def test_resolve_reconcile_desired_state_source_explicit_deployment_intents_skips_handoff_check() -> None:
    src = resolve_reconcile_desired_state_source(
        ir_document=None,
        payload={
            "reconcile_desired_state_source": "deployment_intents",
            "system_ir_ref": {"path": ".akc/ir/missing.json"},
        },
    )
    assert src == "deployment_intents"


def test_runtime_evidence_expectation_violations_detects_operational_spec_gap() -> None:
    types = {"terminal_health", "action_decision"}
    violations = runtime_evidence_expectation_violations(
        expectations=["operational_spec"],
        evidence_types_present=types,
    )
    assert "operational_spec" in violations


def test_runtime_policy_runtime_marks_operational_spec_evidence_expectations() -> None:
    context = _context()
    policy = RuntimePolicyRuntime.from_bundle(
        context=context,
        policy_envelope={},
        intent_projection={
            "success_criteria_summary": {
                "evaluation_modes": ["operational_spec", "tests"],
            }
        },
    )

    assert policy.evidence_expectations == ("operational_spec", "reconciler.health_check")


def test_runtime_evidence_expectation_violations_detects_metric_threshold_gap() -> None:
    types = {"terminal_health", "action_decision"}
    violations = runtime_evidence_expectation_violations(
        expectations=["metric_threshold"],
        evidence_types_present=types,
    )
    assert "metric_threshold" in violations


def test_runtime_policy_runtime_marks_metric_threshold_evidence_expectations() -> None:
    context = _context()
    policy = RuntimePolicyRuntime.from_bundle(
        context=context,
        policy_envelope={},
        intent_projection={
            "success_criteria_summary": {
                "evaluation_modes": ["metric_threshold", "tests"],
            }
        },
    )

    assert policy.evidence_expectations == ("metric_threshold", "reconciler.health_check")


def test_event_scope_mismatch_is_rejected() -> None:
    context = _context()
    event = RuntimeEvent(
        event_id="evt-1",
        event_type="runtime.action.completed",
        timestamp=1,
        context=_context("runtime-2"),
        payload={},
    )

    with pytest.raises(RuntimeScopeMismatchError, match="scope mismatch"):
        ensure_event_scope(expected=context, event=event)


def test_scoped_runtime_environment_derives_tenant_repo_rooted_workdir() -> None:
    env = derive_scoped_runtime_environment(context=_context(), policy_envelope={})

    assert env.working_directory.endswith(".akc/runtime/tenant-a/repo-a/compile-1/runtime-1")
    assert env.secret_keys == ()


def test_filesystem_state_store_uses_tenant_repo_scoped_paths(tmp_path: Path) -> None:
    context = _context()
    store = FileSystemRuntimeStateStore(root=tmp_path)
    action = RuntimeAction(
        action_id="action-1",
        action_type="workflow.execute",
        node_ref=RuntimeNodeRef(node_id="node-1", kind="workflow", contract_id="contract-1"),
        inputs_fingerprint="fp-1",
        idempotency_key="idem-1",
    )
    checkpoint = RuntimeCheckpoint(
        checkpoint_id="cp-1",
        cursor="event:0",
        pending_queue=(action,),
        node_states={"node-1": {"state": "ready"}},
    )
    event = RuntimeEvent(
        event_id="evt-1",
        event_type="runtime.kernel.started",
        timestamp=1,
        context=context,
        payload={},
    )
    snapshot = RuntimeQueueSnapshot(
        queued=(
            ScheduledRuntimeAction(
                action=action,
                priority=0,
                enqueue_ts=1,
                node_class="workflow",
                attempt=0,
                available_at=1,
            ),
        ),
        in_flight=(),
        dead_letters=(),
    )

    store.save_checkpoint(context=context, checkpoint=checkpoint)
    store.append_event(context=context, event=event)
    store.save_queue_snapshot(context=context, snapshot=snapshot)
    store.append_trace_span(
        context=context,
        span=TraceSpan(
            trace_id="trace-1",
            span_id="span-1",
            parent_span_id=None,
            name="runtime.kernel.run",
            kind="internal",
            start_time_unix_nano=1,
            end_time_unix_nano=2,
            attributes={"status": "terminal"},
        ),
    )

    scoped_dir = tmp_path / "tenant-a" / "repo-a" / ".akc" / "runtime" / "compile-1" / "runtime-1"
    assert (scoped_dir / "checkpoint.json").exists()
    assert (scoped_dir / "events.json").exists()
    assert (scoped_dir / "queue_snapshot.json").exists()
    assert (scoped_dir / "runtime_trace_spans.json").exists()
    loaded_snapshot = store.load_queue_snapshot(context=context)
    assert loaded_snapshot is not None
    assert loaded_snapshot.queued[0].action.action_id == "action-1"
    loaded_spans = store.list_trace_spans(context=context)
    assert len(loaded_spans) == 1
    assert loaded_spans[0].name == "runtime.kernel.run"
