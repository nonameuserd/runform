from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.ir import ContractTrigger, IOContract, OperationalContract
from akc.runtime.adapters.native import NativeRuntimeAdapter
from akc.runtime.events import RuntimeEventBus
from akc.runtime.init import create_local_depth_runtime
from akc.runtime.kernel import RuntimeKernel
from akc.runtime.models import (
    RuntimeBundle,
    RuntimeBundleRef,
    RuntimeCheckpoint,
    RuntimeContext,
    RuntimeNodeRef,
)
from akc.runtime.scheduler import InMemoryRuntimeScheduler
from akc.runtime.state_store import InMemoryRuntimeStateStore
from akc.utils.fingerprint import stable_json_fingerprint


def _runtime_bundle() -> RuntimeBundle:
    context = RuntimeContext(
        tenant_id="tenant-a",
        repo_id="repo-a",
        run_id="compile-1",
        runtime_run_id="runtime-1",
        policy_mode="enforce",
        adapter_id="native",
    )
    return RuntimeBundle(
        context=context,
        ref=RuntimeBundleRef(
            bundle_path=".akc/runtime/compile-1.runtime_bundle.json",
            manifest_hash="a" * 64,
            created_at=1,
            source_compile_run_id="compile-1",
        ),
        nodes=(RuntimeNodeRef(node_id="node-1", kind="workflow", contract_id="contract-1"),),
        contract_ids=("contract-1",),
        metadata={
            "referenced_ir_nodes": [
                {
                    "id": "node-1",
                    "tenant_id": "tenant-a",
                    "kind": "workflow",
                    "name": "Workflow 1",
                    "properties": {"order_idx": 0},
                    "depends_on": [],
                    "contract": {
                        "contract_id": "contract-1",
                        "contract_category": "runtime",
                        "triggers": [
                            {
                                "trigger_id": "kernel_started",
                                "source": "runtime.kernel.started",
                                "details": {"event_type": "runtime.kernel.started"},
                            }
                        ],
                        "io_contract": {
                            "input_keys": ("runtime_run_id",),
                            "output_keys": ("action_id", "action_type", "adapter_id"),
                        },
                    },
                }
            ],
            "referenced_contracts": [
                OperationalContract(
                    contract_id="contract-1",
                    contract_category="runtime",
                    triggers=(
                        ContractTrigger(
                            trigger_id="kernel_started",
                            source="runtime.kernel.started",
                            details={"event_type": "runtime.kernel.started"},
                        ),
                    ),
                    io_contract=IOContract(
                        input_keys=("runtime_run_id",),
                        output_keys=("action_id", "action_type", "adapter_id"),
                    ),
                ).to_json_obj()
            ],
        },
    )


def _kernel(bundle: RuntimeBundle) -> RuntimeKernel:
    return RuntimeKernel(
        context=bundle.context,
        bundle=bundle,
        adapter=NativeRuntimeAdapter(),
        scheduler=InMemoryRuntimeScheduler(),
        state_store=InMemoryRuntimeStateStore(),
        event_bus=RuntimeEventBus(),
    )


def test_kernel_runs_to_terminal_on_start_event() -> None:
    bundle = _runtime_bundle()
    kernel = _kernel(bundle)

    result = kernel.run_until_terminal(max_iterations=5)

    assert result.status == "terminal"
    assert result.last_checkpoint.node_states["node-1"]["state"] == "completed"
    event_types = [event.event_type for event in result.emitted_events]
    assert "runtime.action.completed" in event_types
    trace_spans = kernel.state_store.list_trace_spans(context=bundle.context)
    assert any(span.name == "runtime.kernel.run" for span in trace_spans)
    assert any(span.name == "runtime.action.execute" for span in trace_spans)


def test_subprocess_execution_requires_runtime_policy_allow(tmp_path: Path) -> None:
    import sys

    bundle = _runtime_bundle()
    exe_base = Path(sys.executable).name
    out_keys = ("action_id", "action_type", "adapter_id", "exit_code", "stdout", "stderr")
    contract = OperationalContract(
        contract_id="contract-1",
        contract_category="runtime",
        triggers=(
            ContractTrigger(
                trigger_id="kernel_started",
                source="runtime.kernel.started",
                details={"event_type": "runtime.kernel.started"},
            ),
        ),
        io_contract=IOContract(
            input_keys=("runtime_run_id",),
            output_keys=out_keys,
        ),
    )
    bundle = RuntimeBundle(
        context=bundle.context,
        ref=bundle.ref,
        nodes=bundle.nodes,
        contract_ids=bundle.contract_ids,
        policy_envelope={"deny_runtime_actions": ["runtime.action.execute.subprocess"]},
        metadata={
            **dict(bundle.metadata),
            "runtime_execution": {"allow_subprocess": True, "subprocess_allowlist": [exe_base]},
            "referenced_ir_nodes": [
                {
                    "id": "node-1",
                    "tenant_id": bundle.context.tenant_id,
                    "kind": "workflow",
                    "name": "Workflow 1",
                    "properties": {
                        "runtime_execution": {
                            "route": "subprocess",
                            "subprocess": {"argv": [sys.executable, "-c", "0"], "timeout_ms": 1000},
                        }
                    },
                    "depends_on": [],
                    "contract": contract.to_json_obj(),
                }
            ],
            "referenced_contracts": [contract.to_json_obj()],
        },
    )
    kernel = create_local_depth_runtime(bundle, outputs_root=tmp_path)

    with pytest.raises(PermissionError, match="runtime.action.execute.subprocess"):
        kernel.run_until_terminal(max_iterations=5)


def test_http_execution_requires_runtime_policy_allow(tmp_path: Path) -> None:
    bundle = _runtime_bundle()
    out_keys = (
        "action_id",
        "action_type",
        "adapter_id",
        "route",
        "http_status_code",
        "http_latency_ms",
        "http_url_redacted",
        "http_response_snippet",
    )
    contract = OperationalContract(
        contract_id="contract-1",
        contract_category="runtime",
        triggers=(
            ContractTrigger(
                trigger_id="kernel_started",
                source="runtime.kernel.started",
                details={"event_type": "runtime.kernel.started"},
            ),
        ),
        io_contract=IOContract(input_keys=("runtime_run_id",), output_keys=out_keys),
    )
    bundle = RuntimeBundle(
        context=bundle.context,
        ref=bundle.ref,
        nodes=bundle.nodes,
        contract_ids=bundle.contract_ids,
        policy_envelope={"deny_runtime_actions": ["runtime.action.execute.http"]},
        metadata={
            **dict(bundle.metadata),
            "runtime_execution": {
                "allow_http": True,
                "http_allowlist": ["http://127.0.0.1"],
                "http_method_allowlist": ["GET"],
            },
            "referenced_ir_nodes": [
                {
                    "id": "node-1",
                    "tenant_id": bundle.context.tenant_id,
                    "kind": "workflow",
                    "name": "Workflow 1",
                    "properties": {
                        "runtime_execution": {
                            "route": "http",
                            "http": {"url": "http://127.0.0.1:9/", "method": "GET", "timeout_ms": 200},
                        }
                    },
                    "depends_on": [],
                    "contract": contract.to_json_obj(),
                }
            ],
            "referenced_contracts": [contract.to_json_obj()],
        },
    )
    kernel = create_local_depth_runtime(bundle, outputs_root=tmp_path)

    with pytest.raises(PermissionError, match="runtime.action.execute.http"):
        kernel.run_until_terminal(max_iterations=5)


def test_kernel_replay_token_skips_duplicate_action_execution() -> None:
    bundle = _runtime_bundle()
    kernel = _kernel(bundle)
    action = kernel._action_from_event(  # noqa: SLF001
        event=kernel.emit_event(
            event_type="runtime.kernel.started",
            payload={"runtime_run_id": bundle.context.runtime_run_id},
        ),
        node=kernel.build_runtime_graph().nodes["node-1"],
    )
    checkpoint = RuntimeCheckpoint(
        checkpoint_id="cp-1",
        cursor="event:0",
        pending_queue=(),
        node_states={"node-1": {"state": "ready"}},
        replay_token=action.idempotency_key,
    )

    replay_event = kernel.run_action(action=action, checkpoint=checkpoint)

    assert replay_event.event_type == "runtime.action.replayed"


def test_load_bundle_validates_hash_and_schema(tmp_path: Path) -> None:
    payload = {
        "schema_version": 1,
        "schema_id": "akc:runtime_bundle:v1",
        "run_id": "compile-1",
        "tenant_id": "tenant-a",
        "repo_id": "repo-a",
        "intent_ref": {
            "intent_id": "intent-1",
            "stable_intent_sha256": "c" * 64,
            "semantic_fingerprint": "d" * 16,
            "goal_text_fingerprint": "e" * 16,
        },
        "intent_policy_projection": {
            "policies": [{"id": "policy.unknown.runtime_guardrail"}],
            "success_criteria_summary": {
                "evaluation_modes": ["metric_threshold"],
            },
        },
        "referenced_ir_nodes": [],
        "referenced_contracts": [],
        "deployment_intents": [],
        "spec_hashes": {},
        "runtime_policy_envelope": {},
    }
    bundle_path = tmp_path / "runtime_bundle.json"
    bundle_path.write_text(json.dumps(payload), encoding="utf-8")
    bundle_ref = RuntimeBundleRef(
        bundle_path=str(bundle_path),
        manifest_hash=stable_json_fingerprint(payload),
        created_at=1,
        source_compile_run_id="compile-1",
    )
    kernel = _kernel(_runtime_bundle())

    loaded = kernel.load_bundle(bundle_ref)

    assert loaded.ref.bundle_path == str(bundle_path)
    assert loaded.metadata["intent_ref"]["intent_id"] == "intent-1"
    assert loaded.metadata["intent_policy_projection"]["policies"][0]["id"] == ("policy.unknown.runtime_guardrail")
    assert loaded.metadata["runtime_evidence_expectations"] == [
        "metric_threshold",
        "reconciler.health_check",
    ]
    assert loaded.policy_envelope["unresolved_intent_policy_ids"] == ["policy.unknown.runtime_guardrail"]

    bad_ref = RuntimeBundleRef(
        bundle_path=str(bundle_path),
        manifest_hash="b" * 64,
        created_at=1,
        source_compile_run_id="compile-1",
    )
    with pytest.raises(ValueError, match="hash mismatch"):
        kernel.load_bundle(bad_ref)


def test_load_bundle_system_ir_ref_without_loaded_ir_raises(tmp_path: Path) -> None:
    payload = {
        "schema_version": 1,
        "schema_id": "akc:runtime_bundle:v1",
        "run_id": "compile-1",
        "tenant_id": "tenant-a",
        "repo_id": "repo-a",
        "referenced_ir_nodes": [],
        "referenced_contracts": [],
        "deployment_intents": [],
        "spec_hashes": {},
        "runtime_policy_envelope": {},
        "system_ir_ref": {
            "path": ".akc/ir/compile-1.json",
            "fingerprint": "f" * 64,
            "format_version": "1",
            "schema_version": 1,
        },
    }
    bundle_path = tmp_path / "runtime_bundle.json"
    bundle_path.write_text(json.dumps(payload), encoding="utf-8")
    bundle_ref = RuntimeBundleRef(
        bundle_path=str(bundle_path),
        manifest_hash=stable_json_fingerprint(payload),
        created_at=1,
        source_compile_run_id="compile-1",
    )
    kernel = _kernel(_runtime_bundle())
    with pytest.raises(ValueError, match="references system IR"):
        kernel.load_bundle(bundle_ref)


def test_load_bundle_strict_ir_requires_loaded_system_ir(tmp_path: Path) -> None:
    payload = {
        "schema_version": 1,
        "schema_id": "akc:runtime_bundle:v1",
        "run_id": "compile-1",
        "tenant_id": "tenant-a",
        "repo_id": "repo-a",
        "intent_ref": {
            "intent_id": "intent-1",
            "stable_intent_sha256": "c" * 64,
            "semantic_fingerprint": "d" * 16,
            "goal_text_fingerprint": "e" * 16,
        },
        "intent_policy_projection": {"policies": []},
        "referenced_ir_nodes": [],
        "referenced_contracts": [],
        "deployment_intents": [],
        "spec_hashes": {
            "orchestration_spec_sha256": "a" * 64,
            "coordination_spec_sha256": "b" * 64,
        },
        "runtime_policy_envelope": {},
        "reconcile_desired_state_source": "ir",
        "system_ir_ref": {
            "path": ".akc/ir/compile-1.json",
            "fingerprint": "f" * 64,
            "format_version": "1",
            "schema_version": 1,
        },
    }
    bundle_path = tmp_path / "runtime_bundle.json"
    bundle_path.write_text(json.dumps(payload), encoding="utf-8")
    bundle_ref = RuntimeBundleRef(
        bundle_path=str(bundle_path),
        manifest_hash=stable_json_fingerprint(payload),
        created_at=1,
        source_compile_run_id="compile-1",
    )
    kernel = _kernel(_runtime_bundle())
    with pytest.raises(ValueError, match="reconcile_desired_state_source=ir"):
        kernel.load_bundle(bundle_ref)
