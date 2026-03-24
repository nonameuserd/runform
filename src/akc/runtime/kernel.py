from __future__ import annotations

import json
import time
from collections import defaultdict
from collections.abc import Mapping, Sequence
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final, Literal, cast

from akc.artifacts.contracts import is_runtime_bundle_schema_id
from akc.control.otel_export import (
    coordination_audit_record_to_export_obj,
    export_obj_to_json_line,
    stable_intent_sha256_from_mapping,
    trace_span_to_export_obj,
)
from akc.control.tracing import TraceSpan, new_span_id, new_trace_id
from akc.coordination.models import CoordinationScheduleLayer
from akc.ir import IRDocument, IRNode, OperationalContract
from akc.memory.models import JSONValue
from akc.runtime.action_routing import resolve_action_route
from akc.runtime.adapters.base import RuntimeAdapter
from akc.runtime.contracts import (
    RuntimeContractMapping,
    enforce_action_budget,
    is_allowed_transition,
    map_operational_contract,
    validate_action_outputs,
)
from akc.runtime.coordination.audit import (
    CoordinationAuditRecord,
    coordination_audit_record_from_action_event,
    lowered_precedence_edges_fingerprint,
    merge_coordination_telemetry_into_payload,
    orchestration_spec_sha256_from_bundle_metadata,
    policy_envelope_sha256,
)
from akc.runtime.coordination.load import LoadedCoordination
from akc.runtime.coordination.step_policy_extras import (
    coordination_step_policy_extras,
    normalize_agent_output_sha256_hex,
)
from akc.runtime.events import RuntimeEventBus
from akc.runtime.intent_authority import (
    resolve_intent_policy_projection_for_bundle,
    strict_intent_authority_enabled,
)
from akc.runtime.manifest_bridge import RuntimeEvidenceWriter
from akc.runtime.models import (
    RuntimeAction,
    RuntimeActionResult,
    RuntimeBundle,
    RuntimeBundleRef,
    RuntimeCheckpoint,
    RuntimeContext,
    RuntimeEvent,
    RuntimeNodeRef,
    RuntimeTransition,
    load_ir_document_from_bundle_payload,
)
from akc.runtime.policy import (
    RuntimePolicyRuntime,
    derive_scoped_runtime_environment,
    ensure_event_scope,
    ensure_runtime_context_match,
    resolve_reconcile_desired_state_source,
)
from akc.runtime.scheduler import RuntimeScheduler
from akc.runtime.state_store import RuntimeStateStore
from akc.utils.fingerprint import stable_json_fingerprint

# Reconcile / deployment knobs may appear as top-level runtime bundle JSON keys; copy into
# :attr:`RuntimeBundle.metadata` so CLI and reconciler see the same contract as unit tests
# that construct :class:`~akc.runtime.models.RuntimeBundle` directly.
_RUNTIME_BUNDLE_RECONCILE_METADATA_KEYS: Final[tuple[str, ...]] = (
    "layer_replacement_mode",
    "deployment_provider",
    "deployment_provider_contract",
    "workflow_execution_contract",
    "runtime_nonzero_exit_on_reconcile_divergence",
    "reconcile_resync_iterations",
    "reconcile_resync_interval_ms",
    "reconcile_resync_exponential_backoff",
    "reconcile_resync_base_interval_ms",
    "reconcile_resync_max_interval_ms",
    "reconcile_resync_jitter_ratio",
    "reconcile_resync_jitter_seed",
    "reconcile_health_gate",
    "reconcile_health_unknown_grace_ms",
    "coordination_execution_contract",
)

_COORDINATION_COMPLETION_FOLD_ORDER: Final[str] = "coordination_step_id"
_COORDINATION_EXECUTION_DEFAULTS: Final[tuple[bool, int, int, str]] = (
    False,
    1,
    1,
    _COORDINATION_COMPLETION_FOLD_ORDER,
)


@dataclass(frozen=True, slots=True)
class RuntimeGraphNode:
    ir_node: IRNode
    contract_mapping: RuntimeContractMapping | None
    depends_on: tuple[str, ...]
    initial_state: str
    terminal_states: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RuntimeGraph:
    bundle: RuntimeBundle
    nodes: Mapping[str, RuntimeGraphNode]
    contract_mappings: Mapping[str, RuntimeContractMapping]


@dataclass(frozen=True, slots=True)
class RuntimeLoopResult:
    status: str
    iterations: int
    last_checkpoint: RuntimeCheckpoint
    emitted_events: tuple[RuntimeEvent, ...]


@dataclass(frozen=True, slots=True)
class CoordinationExecutionContract:
    parallel_dispatch_enabled: bool
    max_in_flight_steps: int
    max_in_flight_per_role: int
    completion_fold_order: str

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "parallel_dispatch_enabled": bool(self.parallel_dispatch_enabled),
            "max_in_flight_steps": int(self.max_in_flight_steps),
            "max_in_flight_per_role": int(self.max_in_flight_per_role),
            "completion_fold_order": str(self.completion_fold_order),
        }


@dataclass(frozen=True, slots=True)
class _ParallelActionExecutionOutcome:
    status: Literal["ok", "value_error", "backend_error"]
    result: RuntimeActionResult | None = None
    error_reason: Literal["budget_exceeded", "contract_violation", "backend_error"] | None = None
    error: str | None = None


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass(slots=True)
class RuntimeKernel:
    context: RuntimeContext
    bundle: RuntimeBundle
    adapter: RuntimeAdapter
    scheduler: RuntimeScheduler
    state_store: RuntimeStateStore
    event_bus: RuntimeEventBus
    evidence_writer: RuntimeEvidenceWriter | None = None
    policy_runtime: RuntimePolicyRuntime | None = None
    max_in_flight_actions: int = 1
    max_in_flight_per_node_class: int = 1
    _runtime_graph: RuntimeGraph | None = field(default=None, init=False, repr=False)
    _pending_inputs: dict[str, dict[str, Any]] = field(default_factory=dict, init=False, repr=False)
    _node_classes: dict[str, str] = field(default_factory=dict, init=False, repr=False)
    _stop_requested: bool = field(default=False, init=False, repr=False)
    _seen_event_ids: set[str] = field(default_factory=set, init=False, repr=False)
    _runtime_trace_id: str = field(default_factory=new_trace_id, init=False, repr=False)
    _kernel_span_id: str | None = field(default=None, init=False, repr=False)
    _coordination_loaded: Any | None = field(default=None, init=False, repr=False)
    _policy_envelope_sha256_digest: str = field(default="", init=False, repr=False)
    _orchestration_spec_sha256: str | None = field(default=None, init=False, repr=False)
    _coordination_spec_version: int | None = field(default=None, init=False, repr=False)
    _coordination_lowered_precedence_hash: str | None = field(default=None, init=False, repr=False)
    _policy_allowed_event_by_action: dict[str, str] = field(default_factory=dict, init=False, repr=False)
    _coordination_audit_sequence: int = field(default=0, init=False, repr=False)
    _coordination_execution_contract: CoordinationExecutionContract = field(
        default_factory=lambda: CoordinationExecutionContract(
            parallel_dispatch_enabled=_COORDINATION_EXECUTION_DEFAULTS[0],
            max_in_flight_steps=_COORDINATION_EXECUTION_DEFAULTS[1],
            max_in_flight_per_role=_COORDINATION_EXECUTION_DEFAULTS[2],
            completion_fold_order=_COORDINATION_EXECUTION_DEFAULTS[3],
        ),
        init=False,
        repr=False,
    )

    def request_stop(self) -> None:
        self._stop_requested = True

    def _otel_stable_intent_hex(self) -> str | None:
        ref = self.bundle.metadata.get("intent_ref")
        return stable_intent_sha256_from_mapping(ref) if isinstance(ref, dict) else None

    def _intent_projection_for_otel(self) -> dict[str, Any] | None:
        raw = self.bundle.metadata.get("intent_policy_projection")
        return dict(raw) if isinstance(raw, Mapping) else None

    def _append_run_otel_export_line(self, line: str) -> None:
        append_fn = getattr(self.state_store, "append_run_otel_export_line", None)
        if callable(append_fn):
            append_fn(context=self.context, line=line)

    def _emit_runtime_trace_otel(self, span: TraceSpan) -> None:
        obj = trace_span_to_export_obj(
            span,
            tenant_id=self.context.tenant_id,
            repo_id=self.context.repo_id,
            run_id=self.context.run_id,
            source="runtime.trace_span",
            stable_intent_sha256=self._otel_stable_intent_hex(),
            runtime_run_id=self.context.runtime_run_id,
            intent_projection=self._intent_projection_for_otel(),
        )
        self._append_run_otel_export_line(export_obj_to_json_line(obj))

    def _emit_coordination_audit_otel(self, rec: CoordinationAuditRecord) -> None:
        obj = coordination_audit_record_to_export_obj(
            rec,
            stable_intent_sha256=self._otel_stable_intent_hex(),
            intent_projection=self._intent_projection_for_otel(),
        )
        if obj is None:
            return
        self._append_run_otel_export_line(export_obj_to_json_line(obj))

    def _persist_runtime_trace_span(self, span: TraceSpan) -> None:
        self.state_store.append_trace_span(context=self.context, span=span)
        self._emit_runtime_trace_otel(span)

    def load_bundle(
        self,
        bundle_ref: RuntimeBundleRef,
        *,
        strict_intent_authority: bool | None = None,
        coordination_execution_overrides: Mapping[str, Any] | None = None,
    ) -> RuntimeBundle:
        bundle_path = Path(bundle_ref.bundle_path).expanduser()
        raw_bytes = bundle_path.read_bytes()
        bundle_hash = stable_json_fingerprint(json.loads(raw_bytes.decode("utf-8")))
        if bundle_hash != bundle_ref.manifest_hash:
            raise ValueError("runtime bundle hash mismatch")
        payload = json.loads(raw_bytes.decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("runtime bundle must decode to an object")
        schema_kind = str(payload.get("schema_id", "")).strip()
        if not is_runtime_bundle_schema_id(schema_kind):
            raise ValueError("unsupported runtime bundle schema_id")
        self.context = RuntimeContext(
            tenant_id=str(payload.get("tenant_id", self.context.tenant_id)),
            repo_id=str(payload.get("repo_id", self.context.repo_id)),
            run_id=str(payload.get("run_id", self.context.run_id)),
            runtime_run_id=self.context.runtime_run_id,
            policy_mode=self.context.policy_mode,
            adapter_id=self.context.adapter_id,
        )
        if self.policy_runtime is not None:
            self.policy_runtime.context = self.context
        intent_ref = payload.get("intent_ref", {})
        if not isinstance(intent_ref, dict):
            intent_ref = {}
        policy_envelope = (
            payload.get("runtime_policy_envelope", {})
            if isinstance(payload.get("runtime_policy_envelope"), dict)
            else {}
        )
        strict = strict_intent_authority_enabled(
            policy_envelope=policy_envelope,
            cli_force_strict=strict_intent_authority,
        )
        intent_projection = resolve_intent_policy_projection_for_bundle(
            bundle_path=bundle_path,
            payload=payload,
            strict=strict,
        )
        ir_document: IRDocument | None = load_ir_document_from_bundle_payload(
            payload=payload,
            bundle_ref=bundle_ref,
            context=self.context,
        )
        system_ir_ref_raw = payload.get("system_ir_ref")
        system_ir_ref_meta: dict[str, JSONValue]
        if isinstance(system_ir_ref_raw, dict):
            system_ir_ref_meta = {str(k): v for k, v in system_ir_ref_raw.items()}
        else:
            system_ir_ref_meta = {}
        reconcile_source = resolve_reconcile_desired_state_source(ir_document=ir_document, payload=payload)
        raw_bundle_expectations = payload.get("runtime_evidence_expectations")
        bundle_expectations = (
            raw_bundle_expectations
            if isinstance(raw_bundle_expectations, Sequence) and not isinstance(raw_bundle_expectations, (str, bytes))
            else None
        )
        policy_runtime = RuntimePolicyRuntime.from_bundle(
            context=self.context,
            policy_envelope=policy_envelope,
            intent_projection=intent_projection,
            ir_document=ir_document,
            bundle_evidence_expectations=bundle_expectations,
        )
        self.policy_runtime = policy_runtime
        from akc.runtime.coordination.load import coordination_plan_metadata, load_coordination_for_bundle
        from akc.runtime.coordination.models import CoordinationScheduler

        loaded_coord = load_coordination_for_bundle(bundle_path=bundle_path, payload=payload)
        self._coordination_loaded = loaded_coord
        self._coordination_lowered_precedence_hash = None
        coord_meta: dict[str, JSONValue] = {}
        if loaded_coord is not None:
            sched = CoordinationScheduler(loaded_coord.parsed).schedule()
            self._coordination_lowered_precedence_hash = lowered_precedence_edges_fingerprint(
                sched.lowered_precedence_edges
            )
            coord_meta = {
                **coordination_plan_metadata(schedule_layers=tuple(layer.step_ids for layer in sched.layers)),
                "coordination_spec_fingerprint_sha256": loaded_coord.fingerprint_sha256,
            }
        coordination_ref_raw = payload.get("coordination_ref")
        raw_ir_only = payload.get("reconcile_deploy_targets_from_ir_only")
        if isinstance(raw_ir_only, bool):
            reconcile_deploy_targets_from_ir_only = bool(raw_ir_only)
        else:
            reconcile_deploy_targets_from_ir_only = str(raw_ir_only).strip().lower() in {"1", "true", "yes"}
        raw_align = payload.get("deployment_intents_ir_alignment", "off")
        deployment_intents_ir_alignment = str(raw_align).strip().lower() if raw_align is not None else "off"

        bundle_metadata: dict[str, JSONValue] = {
            "schema_version": int(payload.get("schema_version", 0)),
            "intent_ref": intent_ref,
            "intent_policy_projection": intent_projection,
            "referenced_ir_nodes": payload.get("referenced_ir_nodes", []),
            "referenced_contracts": payload.get("referenced_contracts", []),
            "deployment_intents": payload.get("deployment_intents", []),
            "spec_hashes": payload.get("spec_hashes", {}),
            "runtime_evidence_expectations": list(policy_runtime.evidence_expectations),
            "system_ir_ref": system_ir_ref_meta,
            "reconcile_desired_state_source": reconcile_source,
            "intent_authority_strict_applied": strict,
            "reconcile_deploy_targets_from_ir_only": reconcile_deploy_targets_from_ir_only,
            "deployment_intents_ir_alignment": deployment_intents_ir_alignment or "off",
        }
        if isinstance(coordination_ref_raw, dict) and coordination_ref_raw:
            bundle_metadata["coordination_ref"] = coordination_ref_raw
        if coord_meta:
            bundle_metadata["coordination_runtime"] = coord_meta
        for _meta_key in _RUNTIME_BUNDLE_RECONCILE_METADATA_KEYS:
            if _meta_key in payload:
                bundle_metadata[_meta_key] = payload[_meta_key]
        self._coordination_execution_contract = self._resolve_coordination_execution_contract(
            bundle_metadata=bundle_metadata,
            overrides=coordination_execution_overrides,
        )
        bundle_metadata["coordination_execution_contract_effective"] = (
            self._coordination_execution_contract.to_json_obj()
        )
        self.bundle = RuntimeBundle(
            context=self.context,
            ref=bundle_ref,
            nodes=tuple(
                RuntimeNodeRef(
                    node_id=str(node.get("id", "")).strip(),
                    kind=str(node.get("kind", "")).strip(),
                    contract_id=(
                        str(node.get("contract", {}).get("contract_id", "")).strip()
                        if isinstance(node.get("contract"), dict)
                        else "unknown"
                    ),
                )
                for node in payload.get("referenced_ir_nodes", [])
                if isinstance(node, dict) and isinstance(node.get("id"), str)
            ),
            contract_ids=tuple(
                str(contract.get("contract_id", "")).strip()
                for contract in payload.get("referenced_contracts", [])
                if isinstance(contract, dict) and str(contract.get("contract_id", "")).strip()
            ),
            policy_envelope={
                **policy_envelope,
                "effective_allow_actions": list(policy_runtime.effective_allow_actions),
                "effective_deny_actions": list(policy_runtime.effective_deny_actions),
                "unresolved_intent_policy_ids": list(policy_runtime.unresolved_policy_ids),
            },
            metadata=bundle_metadata,
            ir_document=ir_document,
        )
        self._policy_envelope_sha256_digest = policy_envelope_sha256(policy_envelope=dict(self.bundle.policy_envelope))
        self._orchestration_spec_sha256 = orchestration_spec_sha256_from_bundle_metadata(self.bundle.metadata)
        if self._coordination_loaded is not None:
            self._coordination_spec_version = int(self._coordination_loaded.parsed.spec_version)
        else:
            self._coordination_spec_version = None
        return self.bundle

    def coordination_execution_policy_snapshot(self) -> dict[str, JSONValue]:
        return dict(self._coordination_execution_contract.to_json_obj())

    def _resolve_coordination_execution_contract(
        self,
        *,
        bundle_metadata: Mapping[str, JSONValue],
        overrides: Mapping[str, Any] | None,
    ) -> CoordinationExecutionContract:
        raw = bundle_metadata.get("coordination_execution_contract")
        contract_raw = dict(raw) if isinstance(raw, Mapping) else {}
        over = dict(overrides) if isinstance(overrides, Mapping) else {}
        merged: dict[str, Any] = {**contract_raw, **over}
        enabled = bool(merged.get("parallel_dispatch_enabled", _COORDINATION_EXECUTION_DEFAULTS[0]))
        max_steps_raw = merged.get("max_in_flight_steps", _COORDINATION_EXECUTION_DEFAULTS[1])
        max_steps = (
            int(max_steps_raw)
            if isinstance(max_steps_raw, (int, float)) and not isinstance(max_steps_raw, bool)
            else _COORDINATION_EXECUTION_DEFAULTS[1]
        )
        if max_steps < 1:
            raise ValueError("coordination_execution_contract.max_in_flight_steps must be >= 1")
        max_role_raw = merged.get("max_in_flight_per_role", _COORDINATION_EXECUTION_DEFAULTS[2])
        max_role = (
            int(max_role_raw)
            if isinstance(max_role_raw, (int, float)) and not isinstance(max_role_raw, bool)
            else _COORDINATION_EXECUTION_DEFAULTS[2]
        )
        if max_role < 1:
            raise ValueError("coordination_execution_contract.max_in_flight_per_role must be >= 1")
        fold_order = str(merged.get("completion_fold_order", _COORDINATION_EXECUTION_DEFAULTS[3])).strip()
        fold_order = fold_order or _COORDINATION_EXECUTION_DEFAULTS[3]
        if fold_order != _COORDINATION_COMPLETION_FOLD_ORDER:
            raise ValueError("coordination_execution_contract.completion_fold_order must be 'coordination_step_id'")
        return CoordinationExecutionContract(
            parallel_dispatch_enabled=enabled,
            max_in_flight_steps=max_steps,
            max_in_flight_per_role=max_role,
            completion_fold_order=fold_order,
        )

    def build_runtime_graph(self) -> RuntimeGraph:
        referenced_nodes_raw = self.bundle.metadata.get("referenced_ir_nodes", [])
        referenced_contracts_raw = self.bundle.metadata.get("referenced_contracts", [])
        contracts_by_id: dict[str, RuntimeContractMapping] = {}
        if isinstance(referenced_contracts_raw, list):
            for raw_contract in referenced_contracts_raw:
                if not isinstance(raw_contract, dict):
                    continue
                contract = OperationalContract.from_json_obj(raw_contract)
                contracts_by_id[contract.contract_id] = map_operational_contract(contract)
        ir_by_id: dict[str, IRNode] | None = None
        if self.bundle.ir_document is not None:
            ir_by_id = {n.id: n for n in self.bundle.ir_document.nodes}
        nodes: dict[str, RuntimeGraphNode] = {}
        if isinstance(referenced_nodes_raw, list):
            for raw_node in referenced_nodes_raw:
                if not isinstance(raw_node, dict):
                    continue
                node_id = str(raw_node.get("id", "")).strip()
                if not node_id:
                    continue
                if ir_by_id is not None and node_id in ir_by_id:
                    ir_node = ir_by_id[node_id]
                else:
                    ir_node = IRNode.from_json_obj(raw_node)
                mapping = contracts_by_id.get(ir_node.contract.contract_id) if ir_node.contract is not None else None
                initial_state = (
                    mapping.source_contract.state_machine.initial_state
                    if mapping is not None and mapping.source_contract.state_machine is not None
                    else "ready"
                )
                terminal_states = (
                    tuple(
                        transition.to_state
                        for transitions in mapping.allowed_transitions.values()
                        for transition in transitions
                        if transition.to_state and transition.to_state not in mapping.allowed_transitions
                    )
                    if mapping is not None
                    else ("completed",)
                )
                nodes[ir_node.id] = RuntimeGraphNode(
                    ir_node=ir_node,
                    contract_mapping=mapping,
                    depends_on=tuple(ir_node.depends_on),
                    initial_state=initial_state,
                    terminal_states=terminal_states or ("completed",),
                )
        graph = RuntimeGraph(bundle=self.bundle, nodes=nodes, contract_mappings=contracts_by_id)
        self._runtime_graph = graph
        return graph

    def recover_or_init_checkpoint(self) -> RuntimeCheckpoint:
        self._validate_scoped_runtime_environment()
        self.adapter.prepare(context=self.context, bundle=self.bundle)
        queue_snapshot = self.state_store.load_queue_snapshot(context=self.context)
        if queue_snapshot is not None:
            self.scheduler.restore_snapshot(context=self.context, snapshot=queue_snapshot)
        checkpoint = self.state_store.load_checkpoint(context=self.context)
        if checkpoint is not None:
            if queue_snapshot is None:
                for action in checkpoint.pending_queue:
                    self.scheduler.enqueue(
                        context=self.context,
                        action=action,
                        priority=0,
                        enqueue_ts=self._event_timestamp_for_action(action),
                        node_class=self._node_class_for_action(action),
                    )
                self._persist_queue_snapshot()
            checkpoint = self._sync_coordination_layer_enqueue(checkpoint=checkpoint)
            self.state_store.save_checkpoint(context=self.context, checkpoint=checkpoint)
            self._persist_queue_snapshot()
            return checkpoint
        if self._runtime_graph is None:
            self.build_runtime_graph()
        initial_node_states: dict[str, Any] = {}
        if self._runtime_graph is not None:
            for node_id, node in self._runtime_graph.nodes.items():
                initial_node_states[node_id] = {"state": node.initial_state}
        checkpoint = RuntimeCheckpoint(
            checkpoint_id="init",
            cursor="start",
            pending_queue=(),
            node_states=initial_node_states,
        )
        checkpoint = self._enqueue_coordination_plan_if_needed(checkpoint=checkpoint)
        self.state_store.save_checkpoint(context=self.context, checkpoint=checkpoint)
        self._persist_queue_snapshot()
        return checkpoint

    def _make_coordination_step_action(
        self,
        *,
        loaded: LoadedCoordination,
        layer: CoordinationScheduleLayer,
        step_id: str,
        step_index: int,
        coordination_step_outputs: Mapping[str, str],
        ts_base: int,
    ) -> RuntimeAction:
        from akc.runtime.coordination.isolation import (
            build_coordination_policy_context,
            role_isolation_profile_for_role,
            tools_for_role_from_coordination_raw,
        )
        from akc.runtime.coordination.step_resolve import resolve_step_to_ir_node_id, resolve_step_to_role_name

        if self._runtime_graph is None:
            self.build_runtime_graph()
        ir_doc = self.bundle.ir_document
        orch = loaded.orchestration
        bundle_allow_net = bool(self.bundle.policy_envelope.get("allow_network"))
        gov = loaded.parsed.governance
        raw_coord = loaded.parsed.raw
        ir_node_id = resolve_step_to_ir_node_id(
            step_id=step_id,
            ir_document=ir_doc,
            orchestration_obj=orch,
        )
        graph_node = self._runtime_graph.nodes.get(ir_node_id) if self._runtime_graph is not None else None
        if graph_node is None:
            raise ValueError(f"coordination step {step_id!r} resolved to IR node {ir_node_id!r} not in runtime graph")
        node_ref = self._node_ref_for_graph_node(graph_node)
        role_name = resolve_step_to_role_name(step_id=step_id, orchestration_obj=orch)
        if not role_name:
            for binding in loaded.parsed.orchestration_bindings:
                if step_id in binding.orchestration_step_ids and str(binding.role_name).strip():
                    role_name = str(binding.role_name).strip()
                    break
        if not role_name:
            role_name = "coordination"
        fb_tools = tools_for_role_from_coordination_raw(
            coordination_raw=dict(raw_coord),
            role_name=role_name,
        )
        prof = role_isolation_profile_for_role(
            governance=gov,
            role_name=role_name,
            fallback_tools=fb_tools,
        )
        extras = coordination_step_policy_extras(
            parsed=loaded.parsed,
            step_id=step_id,
            coordination_step_outputs=coordination_step_outputs,
        )
        policy_ctx = {
            **build_coordination_policy_context(
                run_stage="coordination.step",
                coordination_step_id=step_id,
                role_name=role_name,
                role_profile=prof,
                bundle_allow_network=bundle_allow_net,
                governance=gov,
                coordination_spec_sha256=loaded.fingerprint_sha256,
            ),
            **extras,
        }
        if self._coordination_lowered_precedence_hash is not None:
            policy_ctx["coordination_lowered_precedence_hash"] = self._coordination_lowered_precedence_hash
        inputs_fp = stable_json_fingerprint(
            {
                "coordination_step_id": step_id,
                "layer": layer.layer_index,
                "coordination_spec_sha256": loaded.fingerprint_sha256,
                "coordination_role_id": role_name,
                "policy_context": policy_ctx,
            }
        )
        idem = stable_json_fingerprint(
            {
                "coordination": True,
                "runtime_run_id": self.context.runtime_run_id,
                "step_id": step_id,
                "layer": layer.layer_index,
                "spec_sha256": loaded.fingerprint_sha256,
            }
        )
        action = RuntimeAction(
            action_id=f"coordination:{step_id}:{layer.layer_index}",
            action_type="coordination.step",
            node_ref=node_ref,
            inputs_fingerprint=inputs_fp,
            idempotency_key=idem,
            policy_context=policy_ctx,
        )
        # Must match scheduler ``node_class`` so per-class in-flight caps release correctly on ack.
        self._node_classes[action.action_id] = "coordination"
        self.scheduler.enqueue(
            context=self.context,
            action=action,
            priority=layer.layer_index,
            enqueue_ts=ts_base + step_index,
            node_class="coordination",
        )
        return action

    def _sync_coordination_layer_enqueue(self, *, checkpoint: RuntimeCheckpoint) -> RuntimeCheckpoint:
        from akc.runtime.coordination.models import CoordinationScheduler

        loaded = self._coordination_loaded
        if loaded is None:
            return checkpoint
        co_raw = checkpoint.node_states.get("__coordination__")
        if not isinstance(co_raw, dict) or not co_raw.get("plan_enqueued"):
            return checkpoint
        if co_raw.get("coordination_next_layer_to_enqueue") is None:
            return checkpoint
        sched = CoordinationScheduler(loaded.parsed).schedule()
        layer_count = len(sched.layers)
        co_any = cast(dict[str, Any], co_raw)
        next_layer = int(co_any["coordination_next_layer_to_enqueue"])
        states = dict(checkpoint.node_states)
        co = dict(co_any)
        terminal: dict[str, str] = dict(co_any.get("coordination_step_terminal") or {})
        outputs: dict[str, str] = dict(co_any.get("coordination_step_outputs") or {})
        changed = False
        while next_layer < layer_count:
            if next_layer > 0:
                prev_ids = sched.layers[next_layer - 1].step_ids
                if not all(str(sid) in terminal for sid in prev_ids):
                    break
            ts_base = _now_ms()
            layer = sched.layers[next_layer]
            for i, step_id in enumerate(layer.step_ids):
                self._make_coordination_step_action(
                    loaded=loaded,
                    layer=layer,
                    step_id=str(step_id),
                    step_index=i,
                    coordination_step_outputs=outputs,
                    ts_base=ts_base,
                )
            next_layer += 1
            co["coordination_next_layer_to_enqueue"] = next_layer
            changed = True
        co["coordination_step_outputs"] = outputs
        co["coordination_step_terminal"] = terminal
        states["__coordination__"] = cast(JSONValue, co)
        if not changed:
            return checkpoint
        return RuntimeCheckpoint(
            checkpoint_id=checkpoint.checkpoint_id,
            cursor=checkpoint.cursor,
            pending_queue=self.scheduler.pending(context=self.context),
            node_states=states,
            replay_token=checkpoint.replay_token,
        )

    def _coordination_apply_terminal_to_states(
        self,
        *,
        states: dict[str, Any],
        action: RuntimeAction,
        terminal_status: str,
        output_sha256: str | None,
    ) -> None:
        if action.action_type != "coordination.step":
            return
        pc = action.policy_context or {}
        step_sid = pc.get("coordination_step_id")
        if not isinstance(step_sid, str) or not step_sid.strip():
            return
        co_raw = states.get("__coordination__")
        if not isinstance(co_raw, dict) or co_raw.get("coordination_next_layer_to_enqueue") is None:
            return
        co = dict(co_raw)
        term = dict(co.get("coordination_step_terminal") or {})
        sid_key = str(step_sid).strip()
        term[sid_key] = str(terminal_status).strip()
        co["coordination_step_terminal"] = term
        if output_sha256 is not None and terminal_status == "succeeded":
            outs = dict(co.get("coordination_step_outputs") or {})
            outs[sid_key] = normalize_agent_output_sha256_hex(output_sha256)
            co["coordination_step_outputs"] = outs
        states["__coordination__"] = co

    def _checkpoint_after_coordination_dead_letter(
        self,
        *,
        checkpoint: RuntimeCheckpoint,
        action: RuntimeAction,
    ) -> RuntimeCheckpoint | None:
        if action.action_type != "coordination.step":
            return None
        states = dict(checkpoint.node_states)
        self._coordination_apply_terminal_to_states(
            states=states,
            action=action,
            terminal_status="dead_lettered",
            output_sha256=None,
        )
        merged = RuntimeCheckpoint(
            checkpoint_id=checkpoint.checkpoint_id,
            cursor=checkpoint.cursor,
            pending_queue=self.scheduler.pending(context=self.context),
            node_states=states,
            replay_token=checkpoint.replay_token,
        )
        return self._sync_coordination_layer_enqueue(checkpoint=merged)

    def _enqueue_coordination_plan_if_needed(self, *, checkpoint: RuntimeCheckpoint) -> RuntimeCheckpoint:
        from akc.runtime.coordination.models import CoordinationScheduler

        loaded = self._coordination_loaded
        if loaded is None:
            return checkpoint
        prior = checkpoint.node_states.get("__coordination__")
        if isinstance(prior, dict) and prior.get("plan_enqueued"):
            if prior.get("coordination_next_layer_to_enqueue") is None:
                return checkpoint
            return self._sync_coordination_layer_enqueue(checkpoint=checkpoint)
        if self._runtime_graph is None:
            self.build_runtime_graph()
        sched = CoordinationScheduler(loaded.parsed).schedule()
        new_states = dict(checkpoint.node_states)
        new_states["__coordination__"] = {
            "plan_enqueued": True,
            "layer_count": len(sched.layers),
            "spec_fingerprint_sha256": loaded.fingerprint_sha256,
            "coordination_next_layer_to_enqueue": 0,
            "coordination_step_outputs": {},
            "coordination_step_terminal": {},
        }
        bootstrap = RuntimeCheckpoint(
            checkpoint_id=checkpoint.checkpoint_id,
            cursor=checkpoint.cursor,
            pending_queue=checkpoint.pending_queue,
            node_states=new_states,
            replay_token=checkpoint.replay_token,
        )
        self.emit_event(
            event_type="runtime.coordination.plan_enqueued",
            payload={
                "layer_count": len(sched.layers),
                "step_order": list(sched.step_order),
                "coordination_spec_sha256": loaded.fingerprint_sha256,
            },
        )
        return self._sync_coordination_layer_enqueue(checkpoint=bootstrap)

    def initialize(self) -> RuntimeCheckpoint:
        if self._runtime_graph is None:
            self.build_runtime_graph()
        return self.recover_or_init_checkpoint()

    def emit_event(
        self,
        *,
        event_type: str,
        payload: Mapping[str, JSONValue],
        action: RuntimeAction | None = None,
        result: RuntimeActionResult | None = None,
        parent_event_id: str | None = None,
    ) -> RuntimeEvent:
        """Publish a runtime event.

        Optional coordination / observability keys (backward compatible) are merged into ``payload``
        when the event is coordination-scoped or ``runtime.coordination.plan_enqueued``.
        See :data:`akc.runtime.coordination.audit.RUNTIME_EVENT_COORDINATION_PAYLOAD_KEYS`.
        """
        event_id = f"{self.context.runtime_run_id}:{event_type}:{_now_ms()}"
        resolved_parent = parent_event_id
        if (
            action is not None
            and event_type in ("runtime.action.completed", "runtime.action.dead_lettered")
            and resolved_parent is None
        ):
            resolved_parent = self._policy_allowed_event_by_action.pop(action.action_id, None)
        merged: dict[str, JSONValue] = dict(payload)
        merged.update(
            merge_coordination_telemetry_into_payload(
                event_type=event_type,
                policy_envelope_sha256_digest=self._policy_envelope_sha256_digest,
                orchestration_spec_sha256=self._orchestration_spec_sha256,
                coordination_spec_version=self._coordination_spec_version,
                trace_id=self._runtime_trace_id,
                event_id=event_id,
                action=action,
                result=result,
                parent_event_id=resolved_parent,
                lowered_precedence_hash=self._coordination_lowered_precedence_hash,
            )
        )
        event = RuntimeEvent(
            event_id=event_id,
            event_type=event_type,
            timestamp=_now_ms(),
            context=self.context,
            payload=merged,
        )
        if event_type == "runtime.action.policy_allowed" and action is not None:
            self._policy_allowed_event_by_action[action.action_id] = event.event_id
        self.event_bus.publish(event)
        self.state_store.append_event(context=self.context, event=event)
        if self.evidence_writer is not None:
            self.evidence_writer.write_event(event=event)
        self._maybe_append_coordination_audit(
            event=event,
            event_type=event_type,
            action=action,
            result=result,
            parent_event_id=resolved_parent,
        )
        return event

    def _maybe_append_coordination_audit(
        self,
        *,
        event: RuntimeEvent,
        event_type: str,
        action: RuntimeAction | None,
        result: RuntimeActionResult | None,
        parent_event_id: str | None,
    ) -> None:
        if event_type not in {"runtime.action.completed", "runtime.action.dead_lettered", "runtime.action.replayed"}:
            return
        if action is None:
            return
        rec = coordination_audit_record_from_action_event(
            context=self.context,
            bundle=self.bundle,
            event_id=event.event_id,
            event_type=event_type,
            timestamp_ms=event.timestamp,
            action=action,
            result=result,
            parent_event_id=parent_event_id,
            policy_envelope_sha256_digest=self._policy_envelope_sha256_digest,
            orchestration_spec_sha256=self._orchestration_spec_sha256,
            coordination_spec_version=self._coordination_spec_version,
            sequence=self._coordination_audit_sequence,
            trace_id=self._runtime_trace_id,
        )
        if rec is None:
            return
        self._coordination_audit_sequence += 1
        append_fn = getattr(self.state_store, "append_coordination_audit_line", None)
        if callable(append_fn):
            append_fn(context=self.context, line=rec.to_json_line())
        self._emit_coordination_audit_otel(rec)

    def _append_runtime_trace_span(
        self,
        *,
        name: str,
        start_ns: int,
        end_ns: int,
        parent_span_id: str | None = None,
        attributes: Mapping[str, Any] | None = None,
        status: str = "ok",
    ) -> None:
        span = TraceSpan(
            trace_id=self._runtime_trace_id,
            span_id=new_span_id(),
            parent_span_id=parent_span_id,
            name=name,
            kind="internal",
            start_time_unix_nano=int(start_ns),
            end_time_unix_nano=max(int(end_ns), int(start_ns)),
            attributes=({str(key): value for key, value in attributes.items()} if attributes is not None else None),
            status=status,
        )
        self._persist_runtime_trace_span(span)

    def poll_events(self, checkpoint: RuntimeCheckpoint) -> tuple[tuple[RuntimeEvent, ...], RuntimeCheckpoint]:
        events = self.state_store.list_events(context=self.context)
        cursor_index = 0
        if checkpoint.cursor.startswith("event:"):
            cursor_index = int(checkpoint.cursor.split(":", 1)[1]) + 1
        new_events = tuple(events[cursor_index:])
        if self._runtime_graph is None:
            self.build_runtime_graph()
        for event in new_events:
            self._authorize_runtime_action(
                action="runtime.event.consume",
                extra_context={"event_id": event.event_id, "event_type": event.event_type},
            )
            ensure_event_scope(expected=self.context, event=event)
            if event.event_id in self._seen_event_ids:
                continue
            self._seen_event_ids.add(event.event_id)
            if self._runtime_graph is None:
                continue
            skip_workflow_events = self._coordination_loaded is not None
            for _, node in self._runtime_graph.nodes.items():
                if skip_workflow_events and node.ir_node.kind == "workflow":
                    continue
                mapping = node.contract_mapping
                if mapping is None:
                    continue
                if any(predicate(event) for predicate in mapping.event_match_predicates.values()):
                    action = self._action_from_event(event=event, node=node)
                    self._pending_inputs[action.action_id] = {
                        **dict(event.payload),
                        "__event_type": event.event_type,
                        "__timestamp": event.timestamp,
                    }
                    self._node_classes[action.action_id] = node.ir_node.kind
                    self.scheduler.enqueue(
                        context=self.context,
                        action=action,
                        priority=0,
                        enqueue_ts=event.timestamp,
                        node_class=node.ir_node.kind,
                    )
        self._persist_queue_snapshot()
        updated_checkpoint = RuntimeCheckpoint(
            checkpoint_id=checkpoint.checkpoint_id,
            cursor=f"event:{len(events) - 1}" if events else checkpoint.cursor,
            pending_queue=self.scheduler.pending(context=self.context),
            node_states=checkpoint.node_states,
            replay_token=checkpoint.replay_token,
        )
        self.state_store.save_checkpoint(context=self.context, checkpoint=updated_checkpoint)
        return new_events, updated_checkpoint

    def dispatch_actions(self, checkpoint: RuntimeCheckpoint) -> tuple[RuntimeEvent, ...]:
        emitted: list[RuntimeEvent] = []
        max_in_flight = int(self.max_in_flight_actions)
        max_in_flight_per_class = int(self.max_in_flight_per_node_class)
        if self._coordination_execution_contract.parallel_dispatch_enabled:
            max_in_flight = max(max_in_flight, int(self._coordination_execution_contract.max_in_flight_steps))
            max_in_flight_per_class = max(
                max_in_flight_per_class,
                int(self._coordination_execution_contract.max_in_flight_steps),
            )
        while True:
            action = self.scheduler.dequeue(
                context=self.context,
                now_ms=_now_ms(),
                max_in_flight=max_in_flight,
                max_in_flight_per_node_class=max_in_flight_per_class,
            )
            if action is None:
                break
            self._persist_queue_snapshot()
            if (
                self._coordination_execution_contract.parallel_dispatch_enabled
                and action.action_type == "coordination.step"
            ):
                emitted.extend(
                    self._dispatch_coordination_parallel_batch(
                        first_action=action,
                        checkpoint=checkpoint,
                        max_in_flight=max_in_flight,
                        max_in_flight_per_class=max_in_flight_per_class,
                    )
                )
            else:
                emitted.append(self.run_action(action=action, checkpoint=checkpoint))
        return tuple(emitted)

    def _dispatch_coordination_parallel_batch(
        self,
        *,
        first_action: RuntimeAction,
        checkpoint: RuntimeCheckpoint,
        max_in_flight: int,
        max_in_flight_per_class: int,
    ) -> tuple[RuntimeEvent, ...]:
        if self._runtime_graph is None:
            self.build_runtime_graph()
        max_steps = int(self._coordination_execution_contract.max_in_flight_steps)
        max_per_role = int(self._coordination_execution_contract.max_in_flight_per_role)
        harvested: list[RuntimeAction] = [first_action]
        spill: list[RuntimeAction] = []
        while len(harvested) < max_steps:
            nxt = self.scheduler.dequeue(
                context=self.context,
                now_ms=_now_ms(),
                max_in_flight=max_in_flight,
                max_in_flight_per_node_class=max_in_flight_per_class,
            )
            if nxt is None:
                break
            self._persist_queue_snapshot()
            if nxt.action_type != "coordination.step":
                spill.append(nxt)
                break
            harvested.append(nxt)

        selected: list[RuntimeAction] = []
        overflow: list[RuntimeAction] = []
        by_role: dict[str, int] = defaultdict(int)
        for action in harvested:
            rid = self._coordination_role_id_from_action(action)
            if by_role[rid] < max_per_role:
                selected.append(action)
                by_role[rid] += 1
            else:
                overflow.append(action)

        fallback_sequential: list[RuntimeAction] = []
        submitted: list[tuple[RuntimeAction, RuntimeGraphNode, int]] = []
        results_by_action_id: dict[str, _ParallelActionExecutionOutcome] = {}
        with ThreadPoolExecutor(max_workers=max(1, min(max_steps, len(selected)))) as pool:
            futures: dict[Future[_ParallelActionExecutionOutcome], str] = {}
            for action in selected:
                graph_node = self._runtime_graph.nodes.get(action.node_ref.node_id) if self._runtime_graph else None
                if graph_node is None or graph_node.contract_mapping is None:
                    fallback_sequential.append(action)
                    continue
                if checkpoint.replay_token == action.idempotency_key:
                    fallback_sequential.append(action)
                    continue
                if not self._policy_allows(action):
                    fallback_sequential.append(action)
                    continue
                action_started_ns = time.time_ns()
                dispatch_ctx: dict[str, JSONValue] = {
                    "action_id": action.action_id,
                    "action_type": action.action_type,
                }
                if action.policy_context is not None:
                    dispatch_ctx["policy_context"] = dict(action.policy_context)
                self._authorize_runtime_action(
                    action="runtime.action.dispatch",
                    extra_context=dispatch_ctx,
                )
                self.emit_event(
                    event_type="runtime.action.policy_allowed",
                    payload={"action_id": action.action_id, "node_id": action.node_ref.node_id},
                    action=action,
                )
                route = resolve_action_route(
                    action=action,
                    graph_node=graph_node,
                    bundle_metadata=self.bundle.metadata,
                )
                if route.kind == "subprocess" and bool(getattr(self.adapter, "respects_runtime_action_routing", False)):
                    sp_ctx: dict[str, JSONValue] = {
                        "action_id": action.action_id,
                        "action_type": action.action_type,
                        "node_id": action.node_ref.node_id,
                    }
                    if action.policy_context is not None:
                        sp_ctx["policy_context"] = dict(action.policy_context)
                    self._authorize_runtime_action(
                        action="runtime.action.execute.subprocess",
                        extra_context=sp_ctx,
                    )
                if route.kind == "http" and bool(getattr(self.adapter, "respects_runtime_action_routing", False)):
                    http_ctx: dict[str, JSONValue] = {
                        "action_id": action.action_id,
                        "action_type": action.action_type,
                        "node_id": action.node_ref.node_id,
                    }
                    if action.policy_context is not None:
                        http_ctx["policy_context"] = dict(action.policy_context)
                    self._authorize_runtime_action(
                        action="runtime.action.execute.http",
                        extra_context=http_ctx,
                    )
                fut = pool.submit(self._execute_action_backend_only, action=action, graph_node=graph_node)
                futures[fut] = action.action_id
                submitted.append((action, graph_node, action_started_ns))
            for fut in as_completed(futures):
                aid = futures[fut]
                results_by_action_id[aid] = fut.result()

        emitted: list[RuntimeEvent] = []
        working_checkpoint = self.state_store.load_checkpoint(context=self.context) or checkpoint
        submitted_sorted = sorted(
            submitted,
            key=lambda item: (self._coordination_step_id_from_action(item[0]), item[0].action_id),
        )
        for action, graph_node, started_ns in submitted_sorted:
            outcome = results_by_action_id.get(action.action_id)
            if outcome is None:
                fallback_sequential.append(action)
                continue
            ev, working_checkpoint = self._finalize_parallel_action_outcome(
                action=action,
                graph_node=graph_node,
                outcome=outcome,
                checkpoint=working_checkpoint,
                action_started_ns=started_ns,
            )
            emitted.append(ev)

        for action in tuple(overflow) + tuple(fallback_sequential) + tuple(spill):
            current_checkpoint = self.state_store.load_checkpoint(context=self.context) or checkpoint
            emitted.append(self.run_action(action=action, checkpoint=current_checkpoint))
        return tuple(emitted)

    def _execute_action_backend_only(
        self,
        *,
        action: RuntimeAction,
        graph_node: RuntimeGraphNode,
    ) -> _ParallelActionExecutionOutcome:
        try:
            graph_executor = getattr(self.adapter, "execute_action_with_graph_node", None)
            if callable(graph_executor):
                result = graph_executor(
                    context=self.context,
                    action=action,
                    bundle=self.bundle,
                    graph_node=graph_node,
                )
            else:
                result = self.adapter.execute_action(context=self.context, action=action)
            contract_mapping = graph_node.contract_mapping
            if contract_mapping is None:
                raise ValueError("runtime graph node missing contract_mapping for backend execution")
            validate_action_outputs(
                mapping=contract_mapping,
                payload=dict(result.outputs),
            )
            enforce_action_budget(mapping=contract_mapping, result=result)
            return _ParallelActionExecutionOutcome(status="ok", result=result)
        except ValueError as exc:
            reason: Literal["budget_exceeded", "contract_violation"] = (
                "budget_exceeded" if "budget" in str(exc).lower() else "contract_violation"
            )
            return _ParallelActionExecutionOutcome(
                status="value_error",
                error_reason=reason,
                error=str(exc),
            )
        except Exception as exc:  # pragma: no cover - defensive parity with sequential path
            return _ParallelActionExecutionOutcome(
                status="backend_error",
                error_reason="backend_error",
                error=str(exc),
            )

    def _finalize_parallel_action_outcome(
        self,
        *,
        action: RuntimeAction,
        graph_node: RuntimeGraphNode,
        outcome: _ParallelActionExecutionOutcome,
        checkpoint: RuntimeCheckpoint,
        action_started_ns: int,
    ) -> tuple[RuntimeEvent, RuntimeCheckpoint]:
        if outcome.status == "ok":
            assert outcome.result is not None
            result = outcome.result
            transition = self.apply_transition(
                action=action,
                result=result,
                checkpoint=checkpoint,
                graph_node=graph_node,
            )
            self.scheduler.ack(
                context=self.context,
                action=action,
                node_class=self._node_class_for_action(action),
            )
            updated_states = dict(checkpoint.node_states)
            prior_state_obj = updated_states.get(action.node_ref.node_id, {})
            prior_state = (
                str(prior_state_obj.get("state")).strip()
                if isinstance(prior_state_obj, dict) and prior_state_obj.get("state") is not None
                else "completed"
            )
            updated_states[action.node_ref.node_id] = {
                "state": transition.to_state if transition is not None else prior_state,
                "last_action_id": action.action_id,
                "last_result": result.to_json_obj(),
            }
            if action.action_type == "coordination.step":
                out_raw = result.outputs.get("agent_worker_output_sha256")
                sha_str = str(out_raw).strip() if isinstance(out_raw, str) else None
                term = "succeeded" if result.status == "succeeded" else "failed"
                self._coordination_apply_terminal_to_states(
                    states=updated_states,
                    action=action,
                    terminal_status=term,
                    output_sha256=sha_str,
                )
            merged_checkpoint = RuntimeCheckpoint(
                checkpoint_id=action.action_id,
                cursor=checkpoint.cursor,
                pending_queue=self.scheduler.pending(context=self.context),
                node_states=updated_states,
                replay_token=action.idempotency_key,
            )
            new_checkpoint = self._sync_coordination_layer_enqueue(checkpoint=merged_checkpoint)
            self.state_store.save_checkpoint(context=self.context, checkpoint=new_checkpoint)
            self._persist_queue_snapshot()
            payload: dict[str, Any] = {
                "action": action.to_json_obj(),
                "result": result.to_json_obj(),
            }
            if transition is not None:
                payload["transition"] = transition.to_json_obj()
            self._append_runtime_trace_span(
                name="runtime.action.execute",
                start_ns=action_started_ns,
                end_ns=time.time_ns(),
                parent_span_id=self._kernel_span_id,
                attributes={
                    "action_id": action.action_id,
                    "action_type": action.action_type,
                    "node_id": action.node_ref.node_id,
                    "status": result.status,
                    "transition_to_state": transition.to_state if transition is not None else None,
                    "runtime_run_id": self.context.runtime_run_id,
                },
                status="ok" if result.status == "succeeded" else "error",
            )
            return (
                self.emit_event(
                    event_type="runtime.action.completed",
                    payload=payload,
                    action=action,
                    result=result,
                ),
                new_checkpoint,
            )
        reason = outcome.error_reason if outcome.error_reason is not None else "backend_error"
        error = str(outcome.error or "runtime action execution failed")
        if reason != "backend_error":
            self._authorize_runtime_action(
                action="runtime.action.retry",
                extra_context={"action_id": action.action_id, "reason": reason},
            )
        retried = self.scheduler.retry(
            context=self.context,
            action=action,
            node_class=self._node_class_for_action(action),
            reason=reason if reason != "backend_error" else "backend_error",
            error=error,
            now_ms=_now_ms(),
        )
        self._persist_queue_snapshot()
        self._append_runtime_trace_span(
            name="runtime.action.retry",
            start_ns=action_started_ns,
            end_ns=time.time_ns(),
            parent_span_id=self._kernel_span_id,
            attributes={
                "action_id": action.action_id,
                "action_type": action.action_type,
                "node_id": action.node_ref.node_id,
                "reason": reason,
                "runtime_run_id": self.context.runtime_run_id,
            },
            status="error",
        )
        ev = self.emit_event(
            event_type="runtime.action.retried" if retried else "runtime.action.dead_lettered",
            payload={"action": action.to_json_obj(), "reason": reason, "error": error},
            action=action,
        )
        if not retried:
            dl_cp = self._checkpoint_after_coordination_dead_letter(checkpoint=checkpoint, action=action)
            if dl_cp is not None:
                self.state_store.save_checkpoint(context=self.context, checkpoint=dl_cp)
                self._persist_queue_snapshot()
                return ev, dl_cp
        return ev, checkpoint

    def run_action(self, *, action: RuntimeAction, checkpoint: RuntimeCheckpoint) -> RuntimeEvent:
        action_started_ns = time.time_ns()
        self._validate_scoped_runtime_environment()
        dispatch_ctx: dict[str, JSONValue] = {
            "action_id": action.action_id,
            "action_type": action.action_type,
        }
        if action.policy_context is not None:
            dispatch_ctx["policy_context"] = dict(action.policy_context)
        self._authorize_runtime_action(
            action="runtime.action.dispatch",
            extra_context=dispatch_ctx,
        )
        if checkpoint.replay_token == action.idempotency_key:
            self.scheduler.ack(
                context=self.context,
                action=action,
                node_class=self._node_class_for_action(action),
            )
            self._persist_queue_snapshot()
            self._append_runtime_trace_span(
                name="runtime.action.replayed",
                start_ns=action_started_ns,
                end_ns=time.time_ns(),
                parent_span_id=self._kernel_span_id,
                attributes={
                    "action_id": action.action_id,
                    "action_type": action.action_type,
                    "node_id": action.node_ref.node_id,
                    "runtime_run_id": self.context.runtime_run_id,
                },
            )
            return self.emit_event(
                event_type="runtime.action.replayed",
                payload={"action": action.to_json_obj(), "replay_token": checkpoint.replay_token},
                action=action,
            )
        if self._runtime_graph is None:
            self.build_runtime_graph()
        graph_node = self._runtime_graph.nodes.get(action.node_ref.node_id) if self._runtime_graph is not None else None
        if graph_node is None or graph_node.contract_mapping is None:
            raise ValueError("runtime action references unknown graph node or contract")
        if not self._policy_allows(action):
            self.scheduler.dead_letter(
                context=self.context,
                action=action,
                node_class=self._node_class_for_action(action),
                reason="policy_denied",
                error="policy denied runtime action dispatch",
                now_ms=_now_ms(),
            )
            self._persist_queue_snapshot()
            self._append_runtime_trace_span(
                name="runtime.action.dead_lettered",
                start_ns=action_started_ns,
                end_ns=time.time_ns(),
                parent_span_id=self._kernel_span_id,
                attributes={
                    "action_id": action.action_id,
                    "action_type": action.action_type,
                    "node_id": action.node_ref.node_id,
                    "reason": "policy_denied",
                    "runtime_run_id": self.context.runtime_run_id,
                },
                status="error",
            )
            dl_checkpoint = self._checkpoint_after_coordination_dead_letter(checkpoint=checkpoint, action=action)
            if dl_checkpoint is not None:
                self.state_store.save_checkpoint(context=self.context, checkpoint=dl_checkpoint)
                self._persist_queue_snapshot()
            return self.emit_event(
                event_type="runtime.action.dead_lettered",
                payload={"action": action.to_json_obj(), "reason": "policy_denied"},
                action=action,
            )
        self.emit_event(
            event_type="runtime.action.policy_allowed",
            payload={"action_id": action.action_id, "node_id": action.node_ref.node_id},
            action=action,
        )
        route = resolve_action_route(
            action=action,
            graph_node=graph_node,
            bundle_metadata=self.bundle.metadata,
        )
        if route.kind == "subprocess" and bool(getattr(self.adapter, "respects_runtime_action_routing", False)):
            # Authorize before the broad execute try/except so policy denials are not retried as backend errors.
            sp_ctx: dict[str, JSONValue] = {
                "action_id": action.action_id,
                "action_type": action.action_type,
                "node_id": action.node_ref.node_id,
            }
            if action.policy_context is not None:
                sp_ctx["policy_context"] = dict(action.policy_context)
            self._authorize_runtime_action(
                action="runtime.action.execute.subprocess",
                extra_context=sp_ctx,
            )
        if route.kind == "http" and bool(getattr(self.adapter, "respects_runtime_action_routing", False)):
            http_ctx: dict[str, JSONValue] = {
                "action_id": action.action_id,
                "action_type": action.action_type,
                "node_id": action.node_ref.node_id,
            }
            if action.policy_context is not None:
                http_ctx["policy_context"] = dict(action.policy_context)
            self._authorize_runtime_action(
                action="runtime.action.execute.http",
                extra_context=http_ctx,
            )
        try:
            graph_executor = getattr(self.adapter, "execute_action_with_graph_node", None)
            if callable(graph_executor):
                result = graph_executor(
                    context=self.context,
                    action=action,
                    bundle=self.bundle,
                    graph_node=graph_node,
                )
            else:
                result = self.adapter.execute_action(context=self.context, action=action)
            contract_mapping = graph_node.contract_mapping
            if contract_mapping is None:
                raise ValueError("runtime graph node missing contract_mapping for backend execution")
            validate_action_outputs(
                mapping=contract_mapping,
                payload=dict(result.outputs),
            )
            enforce_action_budget(mapping=contract_mapping, result=result)
        except ValueError as exc:
            reason: Literal["budget_exceeded", "contract_violation"] = (
                "budget_exceeded" if "budget" in str(exc).lower() else "contract_violation"
            )
            self._authorize_runtime_action(
                action="runtime.action.retry",
                extra_context={"action_id": action.action_id, "reason": reason},
            )
            retried = self.scheduler.retry(
                context=self.context,
                action=action,
                node_class=self._node_class_for_action(action),
                reason=reason,
                error=str(exc),
                now_ms=_now_ms(),
            )
            self._persist_queue_snapshot()
            self._append_runtime_trace_span(
                name="runtime.action.retry",
                start_ns=action_started_ns,
                end_ns=time.time_ns(),
                parent_span_id=self._kernel_span_id,
                attributes={
                    "action_id": action.action_id,
                    "action_type": action.action_type,
                    "node_id": action.node_ref.node_id,
                    "reason": reason,
                    "runtime_run_id": self.context.runtime_run_id,
                },
                status="error",
            )
            out = self.emit_event(
                event_type="runtime.action.retried" if retried else "runtime.action.dead_lettered",
                payload={"action": action.to_json_obj(), "reason": reason, "error": str(exc)},
                action=action,
            )
            if not retried:
                dl_cp = self._checkpoint_after_coordination_dead_letter(checkpoint=checkpoint, action=action)
                if dl_cp is not None:
                    self.state_store.save_checkpoint(context=self.context, checkpoint=dl_cp)
                    self._persist_queue_snapshot()
            return out
        except Exception as exc:
            retried = self.scheduler.retry(
                context=self.context,
                action=action,
                node_class=self._node_class_for_action(action),
                reason="backend_error",
                error=str(exc),
                now_ms=_now_ms(),
            )
            self._persist_queue_snapshot()
            self._append_runtime_trace_span(
                name="runtime.action.retry",
                start_ns=action_started_ns,
                end_ns=time.time_ns(),
                parent_span_id=self._kernel_span_id,
                attributes={
                    "action_id": action.action_id,
                    "action_type": action.action_type,
                    "node_id": action.node_ref.node_id,
                    "reason": "backend_error",
                    "runtime_run_id": self.context.runtime_run_id,
                },
                status="error",
            )
            out = self.emit_event(
                event_type="runtime.action.retried" if retried else "runtime.action.dead_lettered",
                payload={"action": action.to_json_obj(), "reason": "backend_error", "error": str(exc)},
                action=action,
            )
            if not retried:
                dl_cp = self._checkpoint_after_coordination_dead_letter(checkpoint=checkpoint, action=action)
                if dl_cp is not None:
                    self.state_store.save_checkpoint(context=self.context, checkpoint=dl_cp)
                    self._persist_queue_snapshot()
            return out
        transition = self.apply_transition(
            action=action,
            result=result,
            checkpoint=checkpoint,
            graph_node=graph_node,
        )
        self.scheduler.ack(
            context=self.context,
            action=action,
            node_class=self._node_class_for_action(action),
        )
        updated_states = dict(checkpoint.node_states)
        prior_state_obj = updated_states.get(action.node_ref.node_id, {})
        prior_state = (
            str(prior_state_obj.get("state")).strip()
            if isinstance(prior_state_obj, dict) and prior_state_obj.get("state") is not None
            else "completed"
        )
        updated_states[action.node_ref.node_id] = {
            "state": transition.to_state if transition is not None else prior_state,
            "last_action_id": action.action_id,
            "last_result": result.to_json_obj(),
        }
        if action.action_type == "coordination.step":
            out_raw = result.outputs.get("agent_worker_output_sha256")
            sha_str = str(out_raw).strip() if isinstance(out_raw, str) else None
            term = "succeeded" if result.status == "succeeded" else "failed"
            self._coordination_apply_terminal_to_states(
                states=updated_states,
                action=action,
                terminal_status=term,
                output_sha256=sha_str,
            )
        merged_checkpoint = RuntimeCheckpoint(
            checkpoint_id=action.action_id,
            cursor=checkpoint.cursor,
            pending_queue=self.scheduler.pending(context=self.context),
            node_states=updated_states,
            replay_token=action.idempotency_key,
        )
        new_checkpoint = self._sync_coordination_layer_enqueue(checkpoint=merged_checkpoint)
        self.state_store.save_checkpoint(context=self.context, checkpoint=new_checkpoint)
        self._persist_queue_snapshot()
        payload: dict[str, Any] = {
            "action": action.to_json_obj(),
            "result": result.to_json_obj(),
        }
        if transition is not None:
            payload["transition"] = transition.to_json_obj()
        self._append_runtime_trace_span(
            name="runtime.action.execute",
            start_ns=action_started_ns,
            end_ns=time.time_ns(),
            parent_span_id=self._kernel_span_id,
            attributes={
                "action_id": action.action_id,
                "action_type": action.action_type,
                "node_id": action.node_ref.node_id,
                "status": result.status,
                "transition_to_state": transition.to_state if transition is not None else None,
                "runtime_run_id": self.context.runtime_run_id,
            },
            status="ok" if result.status == "succeeded" else "error",
        )
        return self.emit_event(
            event_type="runtime.action.completed",
            payload=payload,
            action=action,
            result=result,
        )

    def apply_transition(
        self,
        *,
        action: RuntimeAction,
        result: RuntimeActionResult,
        checkpoint: RuntimeCheckpoint,
        graph_node: RuntimeGraphNode,
    ) -> RuntimeTransition | None:
        mapping = graph_node.contract_mapping
        current_state_obj = checkpoint.node_states.get(action.node_ref.node_id, {})
        current_state = (
            str(current_state_obj.get("state")).strip()
            if isinstance(current_state_obj, dict) and current_state_obj.get("state") is not None
            else graph_node.initial_state
        )
        if mapping is not None and mapping.allowed_transitions:
            for trigger_id, predicate in mapping.event_match_predicates.items():
                trigger_event_payload = self._pending_inputs.get(action.action_id, {})
                candidate_event = RuntimeEvent(
                    event_id=f"{action.action_id}:{trigger_id}",
                    event_type=str(trigger_event_payload.get("__event_type", "")) or str(trigger_id),
                    timestamp=self._event_timestamp_for_action(action),
                    context=self.context,
                    payload={
                        key: value for key, value in trigger_event_payload.items() if not str(key).startswith("__")
                    },
                )
                if not predicate(candidate_event):
                    continue
                for transition in mapping.allowed_transitions.get(current_state, ()):
                    if not is_allowed_transition(
                        mapping=mapping,
                        from_state=current_state,
                        to_state=transition.to_state,
                        trigger_id=trigger_id,
                    ):
                        continue
                    return RuntimeTransition(
                        from_state=current_state,
                        to_state=transition.to_state,
                        trigger_id=trigger_id,
                        transition_id=transition.transition_id,
                        occurred_at=_now_ms(),
                    )
        if result.status == "succeeded":
            return RuntimeTransition(
                from_state=current_state,
                to_state="completed",
                trigger_id="runtime.action.completed",
                transition_id=f"{action.action_id}:completed",
                occurred_at=_now_ms(),
            )
        return None

    def persist_checkpoint(self, checkpoint: RuntimeCheckpoint) -> RuntimeCheckpoint:
        persisted = RuntimeCheckpoint(
            checkpoint_id=checkpoint.checkpoint_id,
            cursor=checkpoint.cursor,
            pending_queue=self.scheduler.pending(context=self.context),
            node_states=checkpoint.node_states,
            replay_token=checkpoint.replay_token,
        )
        self.state_store.save_checkpoint(context=self.context, checkpoint=persisted)
        self._persist_queue_snapshot()
        return persisted

    def run_until_terminal(
        self,
        *,
        bundle_ref: RuntimeBundleRef | None = None,
        max_iterations: int = 100,
    ) -> RuntimeLoopResult:
        kernel_started_ns = time.time_ns()
        self._kernel_span_id = new_span_id()
        if bundle_ref is not None:
            self.load_bundle(bundle_ref)
        graph = self.build_runtime_graph()
        checkpoint = self.recover_or_init_checkpoint()
        emitted_events: list[RuntimeEvent] = []
        self.emit_event(
            event_type="runtime.kernel.started",
            payload={"runtime_run_id": self.context.runtime_run_id, "node_count": len(graph.nodes)},
        )
        iterations = 0
        while iterations < max_iterations:
            if self._stop_requested:
                checkpoint = self.persist_checkpoint(checkpoint)
                self._persist_runtime_trace_span(
                    TraceSpan(
                        trace_id=self._runtime_trace_id,
                        span_id=self._kernel_span_id,
                        parent_span_id=None,
                        name="runtime.kernel.run",
                        kind="internal",
                        start_time_unix_nano=kernel_started_ns,
                        end_time_unix_nano=time.time_ns(),
                        attributes={
                            "runtime_run_id": self.context.runtime_run_id,
                            "status": "stopped",
                            "iterations": iterations,
                            "node_count": len(graph.nodes),
                        },
                    ),
                )
                self.emit_event(
                    event_type="runtime.kernel.loop_finished",
                    payload={
                        "terminal_status": "stopped",
                        "iterations": iterations,
                        "runtime_run_id": self.context.runtime_run_id,
                    },
                )
                return RuntimeLoopResult(
                    status="stopped",
                    iterations=iterations,
                    last_checkpoint=checkpoint,
                    emitted_events=tuple(emitted_events),
                )
            new_events, checkpoint = self.poll_events(checkpoint)
            emitted = self.dispatch_actions(checkpoint)
            emitted_events.extend(emitted)
            if emitted:
                last_checkpoint = self.state_store.load_checkpoint(context=self.context)
                if last_checkpoint is not None:
                    checkpoint = last_checkpoint
            elif not new_events and self._is_terminal(checkpoint, graph):
                checkpoint = self.persist_checkpoint(checkpoint)
                self._persist_runtime_trace_span(
                    TraceSpan(
                        trace_id=self._runtime_trace_id,
                        span_id=self._kernel_span_id,
                        parent_span_id=None,
                        name="runtime.kernel.run",
                        kind="internal",
                        start_time_unix_nano=kernel_started_ns,
                        end_time_unix_nano=time.time_ns(),
                        attributes={
                            "runtime_run_id": self.context.runtime_run_id,
                            "status": "terminal",
                            "iterations": iterations,
                            "node_count": len(graph.nodes),
                        },
                    ),
                )
                self.emit_event(
                    event_type="runtime.kernel.loop_finished",
                    payload={
                        "terminal_status": "terminal",
                        "iterations": iterations,
                        "runtime_run_id": self.context.runtime_run_id,
                    },
                )
                return RuntimeLoopResult(
                    status="terminal",
                    iterations=iterations,
                    last_checkpoint=checkpoint,
                    emitted_events=tuple(emitted_events),
                )
            elif not new_events and not self.scheduler.pending(context=self.context):
                checkpoint = self.persist_checkpoint(checkpoint)
                self._persist_runtime_trace_span(
                    TraceSpan(
                        trace_id=self._runtime_trace_id,
                        span_id=self._kernel_span_id,
                        parent_span_id=None,
                        name="runtime.kernel.run",
                        kind="internal",
                        start_time_unix_nano=kernel_started_ns,
                        end_time_unix_nano=time.time_ns(),
                        attributes={
                            "runtime_run_id": self.context.runtime_run_id,
                            "status": "idle",
                            "iterations": iterations,
                            "node_count": len(graph.nodes),
                        },
                    ),
                )
                self.emit_event(
                    event_type="runtime.kernel.loop_finished",
                    payload={
                        "terminal_status": "idle",
                        "iterations": iterations,
                        "runtime_run_id": self.context.runtime_run_id,
                    },
                )
                return RuntimeLoopResult(
                    status="idle",
                    iterations=iterations,
                    last_checkpoint=checkpoint,
                    emitted_events=tuple(emitted_events),
                )
            iterations += 1
        checkpoint = self.persist_checkpoint(checkpoint)
        self._persist_runtime_trace_span(
            TraceSpan(
                trace_id=self._runtime_trace_id,
                span_id=self._kernel_span_id,
                parent_span_id=None,
                name="runtime.kernel.run",
                kind="internal",
                start_time_unix_nano=kernel_started_ns,
                end_time_unix_nano=time.time_ns(),
                attributes={
                    "runtime_run_id": self.context.runtime_run_id,
                    "status": "max_iterations_exceeded",
                    "iterations": iterations,
                    "node_count": len(graph.nodes),
                },
                status="error",
            ),
        )
        self.emit_event(
            event_type="runtime.kernel.loop_finished",
            payload={
                "terminal_status": "max_iterations_exceeded",
                "iterations": iterations,
                "runtime_run_id": self.context.runtime_run_id,
            },
        )
        return RuntimeLoopResult(
            status="max_iterations_exceeded",
            iterations=iterations,
            last_checkpoint=checkpoint,
            emitted_events=tuple(emitted_events),
        )

    def contract_matches(self, *, event: RuntimeEvent, contract: RuntimeContractMapping) -> bool:
        return any(predicate(event) for predicate in contract.event_match_predicates.values())

    def _action_from_event(self, *, event: RuntimeEvent, node: RuntimeGraphNode) -> RuntimeAction:
        trigger_key = stable_json_fingerprint({"event_id": event.event_id, "node_id": node.ir_node.id})
        return RuntimeAction(
            action_id=f"{node.ir_node.id}:{event.event_id}",
            action_type=f"{node.ir_node.kind}.execute",
            node_ref=self._node_ref_for_graph_node(node),
            inputs_fingerprint=stable_json_fingerprint({"payload": dict(event.payload)}),
            idempotency_key=trigger_key,
        )

    def _node_ref_for_graph_node(self, node: RuntimeGraphNode) -> Any:
        for ref in self.bundle.nodes:
            if ref.node_id == node.ir_node.id:
                return ref
        contract_id = node.contract_mapping.contract_id if node.contract_mapping is not None else "unknown"
        return RuntimeNodeRef(node_id=node.ir_node.id, kind=node.ir_node.kind, contract_id=contract_id)

    def _event_timestamp_for_action(self, action: RuntimeAction) -> int:
        payload = self._pending_inputs.get(action.action_id, {})
        timestamp_raw = payload.get("__timestamp")
        return int(timestamp_raw) if isinstance(timestamp_raw, int) else 0

    def _coordination_step_id_from_action(self, action: RuntimeAction) -> str:
        pc = action.policy_context
        if not isinstance(pc, Mapping):
            return ""
        raw = pc.get("coordination_step_id")
        return str(raw).strip() if isinstance(raw, str) else ""

    def _coordination_role_id_from_action(self, action: RuntimeAction) -> str:
        pc = action.policy_context
        if not isinstance(pc, Mapping):
            return ""
        raw = pc.get("coordination_role_id")
        return str(raw).strip() if isinstance(raw, str) else ""

    def _node_class_for_action(self, action: RuntimeAction) -> str:
        return self._node_classes.get(action.action_id, action.node_ref.kind)

    def _is_terminal(self, checkpoint: RuntimeCheckpoint, graph: RuntimeGraph) -> bool:
        if checkpoint.pending_queue:
            return False
        for node_id, node in graph.nodes.items():
            state_obj = checkpoint.node_states.get(node_id, {})
            current_state = (
                str(state_obj.get("state")).strip()
                if isinstance(state_obj, dict) and state_obj.get("state") is not None
                else node.initial_state
            )
            if current_state not in set(node.terminal_states):
                return False
        return True

    def _persist_queue_snapshot(self) -> None:
        self._authorize_runtime_action(
            action="runtime.state.checkpoint.write",
            extra_context={"runtime_run_id": self.context.runtime_run_id},
        )
        self.state_store.save_queue_snapshot(
            context=self.context,
            snapshot=self.scheduler.snapshot(context=self.context),
        )

    def _policy_allows(self, action: RuntimeAction) -> bool:
        deny = self.bundle.policy_envelope.get("deny_action_types")
        if isinstance(deny, list):
            return action.action_type not in {str(item).strip() for item in deny if str(item).strip()}
        return True

    def observe_adapter_fallback(
        self,
        context: RuntimeContext,
        primary_adapter_id: str,
        fallback_adapter_id: str,
        capability: str,
        reason: str,
    ) -> None:
        ensure_runtime_context_match(expected=self.context, actual=context)
        self.emit_event(
            event_type="runtime.adapter.fallback",
            payload={
                "tenant_id": context.tenant_id,
                "repo_id": context.repo_id,
                "run_id": context.run_id,
                "runtime_run_id": context.runtime_run_id,
                "primary_adapter_id": primary_adapter_id,
                "fallback_adapter_id": fallback_adapter_id,
                "capability": capability,
                "reason": reason,
            },
        )

    def _authorize_runtime_action(self, *, action: str, extra_context: Mapping[str, JSONValue] | None = None) -> None:
        if self.policy_runtime is None:
            return
        decision = self.policy_runtime.authorize(
            action=action,
            context=self.context,
            extra_context=extra_context,
        )
        if bool(decision.block):
            raise PermissionError(f"runtime policy blocked action={action!r} reason={decision.reason!r}")

    def _validate_scoped_runtime_environment(self) -> None:
        env = derive_scoped_runtime_environment(
            context=self.context,
            policy_envelope=self.bundle.policy_envelope,
        )
        self.bundle = RuntimeBundle(
            context=self.bundle.context,
            ref=self.bundle.ref,
            nodes=self.bundle.nodes,
            contract_ids=self.bundle.contract_ids,
            policy_envelope={
                **dict(self.bundle.policy_envelope),
                "scoped_workdir": env.working_directory,
                "scoped_secret_keys": list(env.secret_keys),
            },
            metadata=self.bundle.metadata,
            ir_document=self.bundle.ir_document,
        )
