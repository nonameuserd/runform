"""Phase 2: handoff-aware fingerprints and coordination replay idempotency."""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any

from akc.ir import ContractTrigger, IOContract, OperationalContract
from akc.runtime.adapters.native import NativeRuntimeAdapter
from akc.runtime.coordination.worker import AgentWorkerTurnResult
from akc.runtime.events import RuntimeEventBus
from akc.runtime.kernel import RuntimeKernel
from akc.runtime.models import RuntimeBundle, RuntimeBundleRef, RuntimeCheckpoint, RuntimeContext
from akc.runtime.scheduler import InMemoryRuntimeScheduler
from akc.runtime.state_store import InMemoryRuntimeStateStore
from akc.utils.fingerprint import stable_json_fingerprint


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _now_ms() -> int:
    return int(time.time() * 1000)


class VariableBodyAgentWorker:
    """Deterministic output SHA per coordination step id (integration hook)."""

    def __init__(self, body_by_step: dict[str, str]) -> None:
        self._body_by_step = body_by_step

    def execute_role_turn(self, *, context: Any) -> AgentWorkerTurnResult:
        step = str(context.coordination_step_id).strip()
        body = self._body_by_step.get(step, f"default:{step}")
        started = time.perf_counter()
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        return AgentWorkerTurnResult(
            status="succeeded",
            output_text_sha256=_sha256_text(body),
            output_text_len=len(body),
            duration_ms=max(elapsed_ms, 0),
            usage_input_tokens=0,
            usage_output_tokens=0,
        )


def _contract() -> OperationalContract:
    return OperationalContract(
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
            output_keys=("action_id", "action_type", "adapter_id", "agent_worker_output_sha256"),
        ),
    )


def _workflow_node(*, node_id: str, order_idx: int, contract: OperationalContract) -> dict[str, Any]:
    return {
        "id": node_id,
        "tenant_id": "tenant-a",
        "kind": "workflow",
        "name": f"Workflow {node_id}",
        "properties": {"order_idx": order_idx},
        "depends_on": [],
        "contract": contract.to_json_obj(),
    }


def _handoff_coordination() -> dict[str, Any]:
    return {
        "spec_version": 2,
        "run_id": "compile-1",
        "tenant_id": "tenant-a",
        "repo_id": "repo-a",
        "coordination_graph": {
            "nodes": [
                {"node_id": "workflow_000", "kind": "step"},
                {"node_id": "workflow_001", "kind": "step"},
            ],
            "edges": [
                {
                    "edge_id": "h01",
                    "kind": "handoff",
                    "src_step_id": "workflow_000",
                    "dst_step_id": "workflow_001",
                    "metadata": {"handoff_id": "primary", "artifact_ref": "ctx.v1"},
                },
            ],
        },
        "orchestration_bindings": [
            {
                "role_name": "planner",
                "agent_name": "a1",
                "orchestration_step_ids": ["workflow_000"],
            },
            {
                "role_name": "writer",
                "agent_name": "a2",
                "orchestration_step_ids": ["workflow_001"],
            },
        ],
        "agent_roles": [{"name": "planner"}, {"name": "writer"}],
        "governance": {"max_steps": 4, "allowed_capabilities": [], "execution_allow_network": False},
    }


def _bundle_payload(
    *,
    coordination: dict[str, Any],
    orchestration: dict[str, Any],
    contract: OperationalContract,
    coordination_execution_contract: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": 4,
        "schema_id": "akc:runtime_bundle:v4",
        "run_id": "compile-1",
        "tenant_id": "tenant-a",
        "repo_id": "repo-a",
        "referenced_ir_nodes": [
            _workflow_node(node_id="node-1", order_idx=0, contract=contract),
            _workflow_node(node_id="node-2", order_idx=1, contract=contract),
        ],
        "referenced_contracts": [contract.to_json_obj()],
        "deployment_intents": [],
        "coordination_spec": coordination,
        "spec_hashes": {
            "orchestration_spec_sha256": stable_json_fingerprint(orchestration),
            "coordination_spec_sha256": stable_json_fingerprint(coordination),
        },
        "intent_ref": {
            "intent_id": "i1",
            "stable_intent_sha256": "a" * 64,
            "semantic_fingerprint": "b" * 16,
            "goal_text_fingerprint": "c" * 16,
        },
        "intent_policy_projection": {
            "intent_id": "i1",
            "stable_intent_sha256": "a" * 64,
            "intent_semantic_fingerprint": "b" * 16,
            "intent_goal_text_fingerprint": "c" * 16,
            "success_criteria_summary": {"count": 0},
        },
        "runtime_policy_envelope": {
            "tenant_id": "tenant-a",
            "repo_id": "repo-a",
            "run_id": "compile-1",
            "tenant_isolation_required": True,
        },
        "reconcile_desired_state_source": "deployment_intents",
    }
    if coordination_execution_contract is not None:
        payload["coordination_execution_contract"] = dict(coordination_execution_contract)
    return payload


def _orchestration() -> dict[str, Any]:
    return {
        "spec_version": 1,
        "tenant_id": "tenant-a",
        "repo_id": "repo-a",
        "run_id": "compile-1",
        "steps": [
            {
                "step_id": "workflow_000",
                "order_idx": 0,
                "agent_name": "a1",
                "role": "planner",
                "inputs": {"ir_node_id": "node-1"},
            },
            {
                "step_id": "workflow_001",
                "order_idx": 1,
                "agent_name": "a2",
                "role": "writer",
                "inputs": {"ir_node_id": "node-2"},
            },
        ],
    }


def _kernel_for_prefix(
    tmp_path: Path,
    *,
    runtime_run_id: str,
    first_step_body: str,
) -> RuntimeKernel:
    repo = tmp_path / "repo"
    orch_path = repo / ".akc" / "orchestration" / "compile-1.orchestration.json"
    orch_path.parent.mkdir(parents=True)
    orch = _orchestration()
    orch_path.write_text(json.dumps(orch), encoding="utf-8")

    contract = _contract()
    coord = _handoff_coordination()
    payload = _bundle_payload(coordination=coord, orchestration=orch, contract=contract)
    bundle_path = repo / ".akc" / "runtime" / "bundle.json"
    bundle_path.parent.mkdir(parents=True)
    bundle_path.write_text(json.dumps(payload), encoding="utf-8")
    bundle_hash = stable_json_fingerprint(json.loads(bundle_path.read_text(encoding="utf-8")))
    ref = RuntimeBundleRef(
        bundle_path=str(bundle_path),
        manifest_hash=bundle_hash,
        created_at=1,
        source_compile_run_id="compile-1",
    )
    ctx = RuntimeContext(
        tenant_id="tenant-a",
        repo_id="repo-a",
        run_id="compile-1",
        runtime_run_id=runtime_run_id,
        policy_mode="enforce",
        adapter_id="native",
    )
    worker = VariableBodyAgentWorker(
        {
            "workflow_000": first_step_body,
            "workflow_001": "second_step_fixed",
        }
    )
    adapter = NativeRuntimeAdapter(agent_worker=worker)
    return RuntimeKernel(
        context=ctx,
        bundle=RuntimeBundle(
            context=ctx,
            ref=ref,
            nodes=(),
            contract_ids=("contract-1",),
            metadata={},
        ),
        adapter=adapter,
        scheduler=InMemoryRuntimeScheduler(),
        state_store=InMemoryRuntimeStateStore(),
        event_bus=RuntimeEventBus(),
    )


def _second_step_fingerprint_from_events(events: list[Any]) -> str:
    for ev in reversed(events):
        if ev.event_type != "runtime.action.completed":
            continue
        action = ev.payload.get("action")
        if not isinstance(action, dict):
            continue
        if action.get("action_type") != "coordination.step":
            continue
        pc = action.get("policy_context")
        if not isinstance(pc, dict):
            continue
        if pc.get("coordination_step_id") != "workflow_001":
            continue
        fp = action.get("inputs_fingerprint")
        if isinstance(fp, str) and fp:
            return fp
    raise AssertionError("no completed coordination.step for workflow_001 found")


def test_handoff_second_step_inputs_fingerprint_changes_when_prior_output_changes(tmp_path: Path) -> None:
    fps: list[str] = []
    for body in ("first_body_alpha", "first_body_beta"):
        k = _kernel_for_prefix(tmp_path / body, runtime_run_id=f"rt-{body}", first_step_body=body)
        k.load_bundle(k.bundle.ref)
        k.build_runtime_graph()
        checkpoint = k.recover_or_init_checkpoint()
        for _ in range(40):
            emitted = k.dispatch_actions(checkpoint)
            if emitted:
                loaded = k.state_store.load_checkpoint(context=k.context)
                if loaded is not None:
                    checkpoint = loaded
            if not emitted and not k.scheduler.pending(context=k.context):
                break
        events = k.state_store.list_events(context=k.context)
        fps.append(_second_step_fingerprint_from_events(events))
    assert fps[0] != fps[1]


def test_coordination_step_replay_matches_replay_token_not_fingerprint(tmp_path: Path) -> None:
    k = _kernel_for_prefix(tmp_path / "replay", runtime_run_id="rt-replay", first_step_body="replay_first")
    k.load_bundle(k.bundle.ref)
    k.build_runtime_graph()
    checkpoint = k.recover_or_init_checkpoint()
    action0 = k.scheduler.dequeue(
        context=k.context,
        now_ms=_now_ms(),
        max_in_flight=1,
        max_in_flight_per_node_class=1,
    )
    assert action0 is not None and action0.action_type == "coordination.step"
    k.run_action(action=action0, checkpoint=checkpoint)
    replay_cp = RuntimeCheckpoint(
        checkpoint_id="replay-cp",
        cursor=checkpoint.cursor,
        pending_queue=(),
        node_states=dict(checkpoint.node_states),
        replay_token=action0.idempotency_key,
    )
    replay_event = k.run_action(action=action0, checkpoint=replay_cp)
    assert replay_event.event_type == "runtime.action.replayed"


def test_coordination_parallel_dispatch_reduces_layer_latency(tmp_path: Path) -> None:
    class SlowAgentWorker:
        def execute_role_turn(self, *, context: Any) -> AgentWorkerTurnResult:
            _ = context
            started = time.perf_counter()
            time.sleep(0.35)
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            return AgentWorkerTurnResult(
                status="succeeded",
                output_text_sha256=_sha256_text("slow"),
                output_text_len=4,
                duration_ms=max(elapsed_ms, 0),
                usage_input_tokens=0,
                usage_output_tokens=0,
            )

    repo = tmp_path / "parallel"
    orch = {
        "spec_version": 1,
        "tenant_id": "tenant-a",
        "repo_id": "repo-a",
        "run_id": "compile-1",
        "steps": [
            {
                "step_id": "workflow_000",
                "order_idx": 0,
                "agent_name": "a1",
                "role": "writer",
                "inputs": {"ir_node_id": "node-1"},
            },
            {
                "step_id": "workflow_001",
                "order_idx": 0,
                "agent_name": "a2",
                "role": "writer",
                "inputs": {"ir_node_id": "node-2"},
            },
        ],
    }
    orch_path = repo / ".akc" / "orchestration" / "compile-1.orchestration.json"
    orch_path.parent.mkdir(parents=True)
    orch_path.write_text(json.dumps(orch), encoding="utf-8")
    contract = _contract()
    coord = {
        "spec_version": 2,
        "run_id": "compile-1",
        "tenant_id": "tenant-a",
        "repo_id": "repo-a",
        "coordination_graph": {
            "nodes": [
                {"node_id": "workflow_000", "kind": "step"},
                {"node_id": "workflow_001", "kind": "step"},
            ],
            "edges": [],
        },
        "orchestration_bindings": [
            {"role_name": "writer", "agent_name": "a1", "orchestration_step_ids": ["workflow_000", "workflow_001"]}
        ],
        "agent_roles": [{"name": "writer"}],
        "governance": {"max_steps": 4, "allowed_capabilities": [], "execution_allow_network": False},
    }
    payload = _bundle_payload(
        coordination=coord,
        orchestration=orch,
        contract=contract,
        coordination_execution_contract={
            "parallel_dispatch_enabled": True,
            "max_in_flight_steps": 2,
            "max_in_flight_per_role": 2,
            "completion_fold_order": "coordination_step_id",
        },
    )
    payload["referenced_ir_nodes"] = [
        _workflow_node(node_id="node-1", order_idx=0, contract=contract),
        _workflow_node(node_id="node-2", order_idx=0, contract=contract),
    ]
    bundle_path = repo / ".akc" / "runtime" / "bundle.json"
    bundle_path.parent.mkdir(parents=True)
    bundle_path.write_text(json.dumps(payload), encoding="utf-8")
    bundle_hash = stable_json_fingerprint(json.loads(bundle_path.read_text(encoding="utf-8")))
    ref = RuntimeBundleRef(
        bundle_path=str(bundle_path),
        manifest_hash=bundle_hash,
        created_at=1,
        source_compile_run_id="compile-1",
    )
    ctx = RuntimeContext(
        tenant_id="tenant-a",
        repo_id="repo-a",
        run_id="compile-1",
        runtime_run_id="rt-parallel",
        policy_mode="enforce",
        adapter_id="native",
    )
    k = RuntimeKernel(
        context=ctx,
        bundle=RuntimeBundle(context=ctx, ref=ref, nodes=(), contract_ids=("contract-1",), metadata={}),
        adapter=NativeRuntimeAdapter(agent_worker=SlowAgentWorker()),
        scheduler=InMemoryRuntimeScheduler(),
        state_store=InMemoryRuntimeStateStore(),
        event_bus=RuntimeEventBus(),
    )
    k.load_bundle(ref)
    k.build_runtime_graph()
    checkpoint = k.recover_or_init_checkpoint()
    started = time.perf_counter()
    for _ in range(20):
        emitted = k.dispatch_actions(checkpoint)
        if emitted:
            loaded = k.state_store.load_checkpoint(context=k.context)
            if loaded is not None:
                checkpoint = loaded
        if not emitted and not k.scheduler.pending(context=k.context):
            break
    elapsed = time.perf_counter() - started
    assert elapsed < 0.75
