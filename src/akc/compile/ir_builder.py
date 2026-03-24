from __future__ import annotations

from collections.abc import Mapping, Sequence
from hashlib import sha256
from typing import Any, Literal, cast

from akc.intent.models import IntentSpecV1
from akc.intent.plan_step_intent import INTENT_REF_INPUT_KEY
from akc.intent.resolve import (
    ResolvedIntentContext,
    intent_link_summaries_for_prompts,
    intent_reference_summaries_for_prompts,
    resolve_compile_intent_context,
)
from akc.intent.store import IntentStore
from akc.ir import (
    ContractTrigger,
    EffectAnnotation,
    IOContract,
    IRDocument,
    IRNode,
    OperationalContract,
    ProvenancePointer,
    stable_node_id,
)
from akc.knowledge.persistence import KNOWLEDGE_SNAPSHOT_RELPATH
from akc.memory.models import JSONValue, PlanState, require_non_empty

IntentContractShape = Literal["full", "reference_first"]


def _stable_runtime_contract_id(*, plan_id: str, step_id: str) -> str:
    require_non_empty(plan_id, name="plan_id")
    require_non_empty(step_id, name="step_id")
    raw = f"runtime::{plan_id.strip()}::{step_id.strip()}".encode()
    return f"opc_rt_{sha256(raw).hexdigest()[:24]}"


def _stable_acceptance_contract_id(*, tenant_id: str, intent_id: str) -> str:
    require_non_empty(tenant_id, name="tenant_id")
    require_non_empty(intent_id, name="intent_id")
    raw = f"accept::{tenant_id.strip()}::{intent_id.strip()}".encode()
    return f"opc_acc_{sha256(raw).hexdigest()[:24]}"


def _acceptance_criteria_projection_for_intent(
    *,
    props: Mapping[str, Any],
    loaded: IntentSpecV1 | None,
) -> list[dict[str, JSONValue]]:
    if loaded is not None and loaded.success_criteria:
        return [
            {"id": sc.id.strip(), "evaluation_mode": sc.evaluation_mode}
            for sc in sorted(loaded.success_criteria, key=lambda s: s.id)
        ]
    raw = props.get("active_success_criteria")
    if not isinstance(raw, list):
        return []
    out: list[dict[str, JSONValue]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        sid = item.get("success_criterion_id") or item.get("id")
        mode = item.get("evaluation_mode")
        if isinstance(sid, str) and sid.strip() and isinstance(mode, str) and mode.strip():
            out.append({"id": sid.strip(), "evaluation_mode": mode.strip()})
    out.sort(key=lambda x: str(x["id"]))
    return out


def _build_intent_acceptance_contract(
    *,
    tenant_id: str,
    intent_id: str,
    criteria: list[dict[str, JSONValue]],
) -> OperationalContract:
    acc: dict[str, JSONValue] = {"criteria": cast(JSONValue, criteria)}
    return OperationalContract(
        contract_id=_stable_acceptance_contract_id(tenant_id=tenant_id, intent_id=intent_id),
        contract_category="acceptance",
        triggers=(ContractTrigger(trigger_id="t_acceptance_eval", source="compile.acceptance.evaluate", details={}),),
        io_contract=IOContract(input_keys=("intent_context",), output_keys=("acceptance_status",)),
        acceptance=acc,
    )


def _build_workflow_runtime_contract(*, plan_id: str, step_id: str) -> OperationalContract:
    return OperationalContract(
        contract_id=_stable_runtime_contract_id(plan_id=plan_id, step_id=step_id),
        contract_category="runtime",
        triggers=(
            ContractTrigger(trigger_id="t_compile_runtime_start", source="compile.runtime.start", details={}),
            ContractTrigger(
                trigger_id="t_scheduler_dispatch",
                source="scheduler",
                details={"event_type": "runtime.action.dispatch"},
            ),
        ),
        io_contract=IOContract(
            input_keys=("objective", "step_inputs"),
            output_keys=("step_artifacts", "step_status", "retrieval_snapshot"),
        ),
    )


def _workflow_step_effects(
    *,
    step_outputs: Mapping[str, Any],
    node_properties: Mapping[str, Any],
    allow_network_base: bool,
) -> EffectAnnotation:
    network = bool(allow_network_base)
    snap = step_outputs.get("retrieval_snapshot")
    if isinstance(snap, dict) and snap:
        network = True
    fs_write: list[str] = []
    touched = node_properties.get("last_touched_paths")
    if isinstance(touched, list):
        fs_write = sorted({str(p).strip() for p in touched if str(p).strip()})
    return EffectAnnotation(network=network, fs_write=tuple(fs_write))


def _allow_network_from_intent_props(*, loaded: IntentSpecV1 | None, intent_props: Mapping[str, Any]) -> bool:
    if loaded is not None and loaded.operating_bounds is not None:
        return bool(loaded.operating_bounds.allow_network)
    ob = intent_props.get("operating_bounds")
    if isinstance(ob, dict):
        return bool(ob.get("allow_network", False))
    return False


def build_ir_document_from_plan(
    *,
    plan: PlanState,
    intent_node_properties: Mapping[str, Any] | None = None,
    intent_store: IntentStore | None = None,
    controller_intent_spec: IntentSpecV1 | None = None,
    resolved_intent_context: ResolvedIntentContext | None = None,
    allow_legacy_first_step_intent_merge: bool = False,
    warn_legacy_step_blobs_without_intent_ref_under_outputs_root: bool = False,
    intent_contract_shape: IntentContractShape = "full",
) -> IRDocument:
    """Build an IRDocument representing the current plan graph.

    This is the shared "PlanState -> IR" builder so compiler passes can consume IR
    instead of owning ad-hoc IR emission logic.
    """

    require_non_empty(plan.tenant_id, name="plan.tenant_id")
    require_non_empty(plan.repo_id, name="plan.repo_id")

    nodes: list[IRNode] = []
    workflow_nodes: list[IRNode] = []
    knowledge_nodes_by_id: dict[str, IRNode] = {}
    allow_network_for_steps = False

    # Phase 6/intent-layer: include a first-class intent node so IR prompts and
    # IR diffs become sensitive to compiler contract changes even when workflow
    # nodes remain structurally identical.
    #
    # We build the intent node from the caller-provided `intent_node_properties`.
    # When omitted, we best-effort infer `intent_id` from PlanStep inputs so older
    # call sites can still emit a minimal intent node.
    inferred_intent_id: str | None = None
    if intent_node_properties is None:
        for s in plan.steps:
            inputs = dict(s.inputs or {})
            raw_intent_id = inputs.get("intent_id")
            if not (isinstance(raw_intent_id, str) and raw_intent_id.strip()):
                ref = inputs.get(INTENT_REF_INPUT_KEY)
                if isinstance(ref, dict):
                    rid = ref.get("intent_id")
                    if isinstance(rid, str) and rid.strip():
                        raw_intent_id = rid
            if isinstance(raw_intent_id, str) and raw_intent_id.strip():
                inferred_intent_id = raw_intent_id.strip()
                break
    else:
        raw_intent_id2 = intent_node_properties.get("intent_id")
        if isinstance(raw_intent_id2, str) and raw_intent_id2.strip():
            inferred_intent_id = raw_intent_id2.strip()

    has_intent_node = inferred_intent_id is not None
    intent_node_id: str | None = None
    prev_id: str | None = None

    if has_intent_node and inferred_intent_id is not None:
        intent_node_id = stable_node_id(kind="intent", name=f"intent:{inferred_intent_id}")
        prev_id = intent_node_id

        props: dict[str, Any] = dict(intent_node_properties or {})
        props.setdefault("intent_id", inferred_intent_id)
        props.setdefault("goal_statement", plan.goal)

        first_inputs = dict(plan.steps[0].inputs or {}) if plan.steps else {}
        resolution_inputs: dict[str, Any] = dict(first_inputs)
        if intent_node_properties:
            for k in ("intent_id", "goal_statement", INTENT_REF_INPUT_KEY):
                if k not in resolution_inputs and k in intent_node_properties:
                    resolution_inputs[k] = intent_node_properties[k]

        resolved: ResolvedIntentContext | None = resolved_intent_context
        if resolved is None:
            resolved = resolve_compile_intent_context(
                tenant_id=plan.tenant_id,
                repo_id=plan.repo_id,
                inputs=resolution_inputs,
                intent_store=intent_store,
                controller_intent_spec=controller_intent_spec,
                fallback_goal_statement=plan.goal,
                warn_legacy_step_blobs_without_intent_ref_under_outputs_root=(
                    warn_legacy_step_blobs_without_intent_ref_under_outputs_root
                ),
            )

        loaded_intent_spec: IntentSpecV1 | None = resolved.spec
        if intent_contract_shape == "reference_first":
            ao, lc, asc = intent_reference_summaries_for_prompts(spec=resolved.spec)
        else:
            ao, lc, asc = intent_link_summaries_for_prompts(spec=resolved.spec)
        props["active_objectives"] = ao
        props["linked_constraints"] = lc
        props["active_success_criteria"] = asc
        props["spec_version"] = int(resolved.spec.spec_version)
        if resolved.spec.goal_statement is not None:
            props["goal_statement"] = resolved.spec.goal_statement
        elif "goal_statement" not in props:
            props["goal_statement"] = plan.goal
        bounds_obj = (
            resolved.spec.operating_bounds.to_json_obj() if resolved.spec.operating_bounds is not None else None
        )
        if bounds_obj is not None and ("operating_bounds" not in props or props.get("operating_bounds") is None):
            props["operating_bounds"] = bounds_obj

        if allow_legacy_first_step_intent_merge:
            for step in plan.steps:
                inputs = dict(step.inputs or {})
                if "active_objectives" in inputs and "active_objectives" not in props:
                    props["active_objectives"] = inputs.get("active_objectives")
                if "linked_constraints" in inputs and "linked_constraints" not in props:
                    props["linked_constraints"] = inputs.get("linked_constraints")
                if "active_success_criteria" in inputs and "active_success_criteria" not in props:
                    props["active_success_criteria"] = inputs.get("active_success_criteria")
                if "operating_bounds" in inputs and "operating_bounds" not in props:
                    props["operating_bounds"] = inputs.get("operating_bounds")
                break

        acceptance_criteria = _acceptance_criteria_projection_for_intent(
            props=props,
            loaded=loaded_intent_spec,
        )
        acceptance_contract = _build_intent_acceptance_contract(
            tenant_id=plan.tenant_id,
            intent_id=inferred_intent_id,
            criteria=acceptance_criteria,
        )
        allow_network_for_steps = _allow_network_from_intent_props(
            loaded=loaded_intent_spec,
            intent_props=props,
        )
        nodes.append(
            IRNode(
                id=intent_node_id,
                tenant_id=plan.tenant_id,
                kind="intent",
                name="intent_contract",
                properties=props,
                depends_on=(),
                contract=acceptance_contract,
            )
        )

    knowledge_hub_id: str | None = None
    sem_16, prov_16, assertion_ids = _scan_plan_knowledge_fingerprints(plan)
    knowledge_hub_node = _build_knowledge_hub_ir_node(
        plan=plan,
        tenant_id=plan.tenant_id,
        knowledge_semantic_fingerprint_16=sem_16,
        knowledge_provenance_fingerprint_16=prov_16,
        assertion_ids=assertion_ids,
        depends_on=(intent_node_id,) if intent_node_id is not None else (),
    )
    if knowledge_hub_node is not None:
        nodes.append(knowledge_hub_node)
        knowledge_hub_id = knowledge_hub_node.id
        prev_id = knowledge_hub_id

    # Deterministic node ordering by `order_idx`.
    for step in sorted(plan.steps, key=lambda s: int(s.order_idx)):
        # Knowledge assertion nodes are stable across steps; dedupe by IR node id.
        for k_node in _build_knowledge_ir_nodes_for_step(plan=plan, step=step, knowledge_hub_id=knowledge_hub_id):
            existing = knowledge_nodes_by_id.get(k_node.id)
            if existing is None:
                knowledge_nodes_by_id[k_node.id] = k_node
                continue

            merged_prov_set = set(existing.provenance).union(set(k_node.provenance))

            def _prov_sort_key(p: ProvenancePointer) -> tuple[str, str, str, str]:
                return (p.kind, p.source_id, str(p.locator or ""), str(p.sha256 or ""))

            merged_provenance = tuple(sorted(merged_prov_set, key=_prov_sort_key))

            def _merge_str_seq(a: Any, b: Any) -> list[str]:
                out: set[str] = set()
                for raw in (a, b):
                    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
                        for x in raw:
                            if isinstance(x, str) and x.strip():
                                out.add(x.strip())
                    elif isinstance(raw, str) and raw.strip():
                        out.add(raw.strip())
                return sorted(out)

            merged_properties: dict[str, Any] = dict(existing.properties)
            for k, v in k_node.properties.items():
                if k == "evidence_doc_ids":
                    merged_properties[k] = _merge_str_seq(existing.properties.get(k), v)
                else:
                    merged_properties[k] = v

            merged_dep = tuple(
                sorted({str(d).strip() for d in (*existing.depends_on, *k_node.depends_on) if str(d).strip()})
            )
            knowledge_nodes_by_id[k_node.id] = IRNode(
                id=existing.id,
                tenant_id=existing.tenant_id,
                kind=existing.kind,
                name=existing.name,
                properties=cast(Mapping[str, JSONValue], merged_properties),
                depends_on=merged_dep,
                effects=existing.effects,
                provenance=merged_provenance,
            )
        node_id = stable_node_id(kind="workflow", name=f"{plan.id}:{step.id}:{step.title}")

        provenance: tuple[ProvenancePointer, ...] = ()
        step_outputs: Mapping[str, Any] = dict(step.outputs or {})
        snap_raw = step_outputs.get("retrieval_snapshot")
        if isinstance(snap_raw, dict):
            prov_raw = snap_raw.get("provenance")
            if isinstance(prov_raw, Sequence) and not isinstance(prov_raw, (str, bytes)):
                parsed: list[ProvenancePointer] = []
                for p in prov_raw:
                    if isinstance(p, dict):
                        try:
                            parsed.append(ProvenancePointer.from_json_obj(p))
                        except Exception:
                            # Best-effort IR emission; drift mapping can still
                            # fall back to conservative recompilation.
                            continue
                provenance = tuple(parsed)

        wf_props = _build_ir_node_properties(
            plan_id=plan.id,
            step_id=step.id,
            status=step.status,
            order_idx=step.order_idx,
            step_outputs=step_outputs,
        )
        wf_contract = _build_workflow_runtime_contract(plan_id=plan.id, step_id=step.id)
        wf_effects = _workflow_step_effects(
            step_outputs=step_outputs,
            node_properties=wf_props,
            allow_network_base=allow_network_for_steps,
        )
        workflow_nodes.append(
            IRNode(
                id=node_id,
                tenant_id=plan.tenant_id,
                kind="workflow",
                name=step.title,
                properties=wf_props,
                depends_on=(prev_id,) if prev_id is not None else (),
                provenance=provenance,
                effects=wf_effects,
                contract=wf_contract,
            )
        )
        prev_id = node_id

    knowledge_nodes = sorted(knowledge_nodes_by_id.values(), key=lambda n: n.id)
    nodes.extend(knowledge_nodes)
    nodes.extend(workflow_nodes)
    return IRDocument(tenant_id=plan.tenant_id, repo_id=plan.repo_id, nodes=tuple(nodes))


def _scan_plan_knowledge_fingerprints(plan: PlanState) -> tuple[str | None, str | None, tuple[str, ...]]:
    """Collect latest non-empty knowledge fingerprints and assertion ids from plan steps."""

    sem: str | None = None
    prov: str | None = None
    aids: set[str] = set()
    for step in sorted(plan.steps, key=lambda s: int(s.order_idx)):
        outputs = dict(step.outputs or {})
        ks = outputs.get("knowledge_semantic_fingerprint")
        if isinstance(ks, str) and ks.strip():
            sem = ks.strip().lower()
        kp = outputs.get("knowledge_provenance_fingerprint")
        if isinstance(kp, str) and kp.strip():
            prov = kp.strip().lower()
        snap_any: Any = outputs.get("knowledge_snapshot")
        if snap_any is not None and hasattr(snap_any, "to_json_obj"):
            to_json = getattr(snap_any, "to_json_obj", None)
            if callable(to_json):
                try:
                    snap_any = to_json()
                except Exception:
                    snap_any = None
        if isinstance(snap_any, dict):
            for c in snap_any.get("canonical_constraints") or []:
                if not isinstance(c, dict):
                    continue
                aid = c.get("assertion_id")
                if isinstance(aid, str) and aid.strip():
                    aids.add(aid.strip())
    return sem, prov, tuple(sorted(aids))


def _build_knowledge_hub_ir_node(
    *,
    plan: PlanState,
    tenant_id: str,
    knowledge_semantic_fingerprint_16: str | None,
    knowledge_provenance_fingerprint_16: str | None,
    assertion_ids: tuple[str, ...],
    depends_on: tuple[str, ...],
) -> IRNode | None:
    """Single `kind=knowledge` hub so viewers/runtime can anchor the layer without parsing step blobs."""

    if not knowledge_semantic_fingerprint_16 and not knowledge_provenance_fingerprint_16 and len(assertion_ids) == 0:
        return None
    hub_id = stable_node_id(kind="knowledge", name=f"layer:{plan.id}")
    props: dict[str, Any] = {
        "plan_id": plan.id,
        "persisted_snapshot_relpath": KNOWLEDGE_SNAPSHOT_RELPATH,
        "knowledge_assertion_ids": list(assertion_ids),
    }
    if knowledge_semantic_fingerprint_16:
        props["knowledge_semantic_fingerprint_16"] = knowledge_semantic_fingerprint_16
    if knowledge_provenance_fingerprint_16:
        props["knowledge_provenance_fingerprint_16"] = knowledge_provenance_fingerprint_16
    return IRNode(
        id=hub_id,
        tenant_id=tenant_id,
        kind="knowledge",
        name="knowledge_layer",
        properties=props,
        depends_on=depends_on,
    )


def _build_knowledge_ir_nodes_for_step(
    *, plan: PlanState, step: Any, knowledge_hub_id: str | None = None
) -> list[IRNode]:
    """Emit IR nodes for canonical knowledge assertions (constraints/decisions).

    These nodes make IR/prompts sensitive to the knowledge layer snapshot, which
    is replay-invalidating when evidence/provenance changes.
    """

    step_outputs: Mapping[str, Any] = dict(step.outputs or {})
    snap_raw = step_outputs.get("knowledge_snapshot")
    if snap_raw is None:
        return []
    if hasattr(snap_raw, "to_json_obj") and callable(snap_raw.to_json_obj):
        # Best-effort: some call sites may store a KnowledgeSnapshot object directly.
        try:
            snap_raw = snap_raw.to_json_obj()
        except Exception:
            return []
    if not isinstance(snap_raw, dict):
        return []

    evidence_by_aid: dict[str, dict[str, Any]] = {}
    evidence_raw = snap_raw.get("evidence_by_assertion") or []
    if isinstance(evidence_raw, list):
        for entry in evidence_raw:
            if not isinstance(entry, dict):
                continue
            aid_raw = entry.get("assertion_id")
            aid = str(aid_raw).strip() if isinstance(aid_raw, str) else None
            ev = entry.get("evidence")
            if aid and isinstance(ev, dict):
                evidence_by_aid[aid] = ev

    nodes: list[IRNode] = []
    assertion_dep: tuple[str, ...] = (knowledge_hub_id,) if knowledge_hub_id else ()

    canonical_constraints_raw = snap_raw.get("canonical_constraints") or []
    if isinstance(canonical_constraints_raw, list):
        for c in canonical_constraints_raw:
            if not isinstance(c, dict):
                continue
            aid_raw = c.get("assertion_id")
            aid = str(aid_raw).strip() if isinstance(aid_raw, str) else ""
            if not aid:
                continue

            prov_raw = evidence_by_aid.get(aid, {}).get("resolved_provenance_pointers") or []
            provenance: list[ProvenancePointer] = []
            if isinstance(prov_raw, list):
                for p_raw in prov_raw:
                    if not isinstance(p_raw, dict):
                        continue
                    try:
                        ptr = ProvenancePointer.from_json_obj(p_raw)
                        # Tenant-safety: only attach provenance pointers for this tenant.
                        if ptr.tenant_id.strip() == plan.tenant_id.strip():
                            provenance.append(ptr)
                    except Exception:
                        continue

            node_id = stable_node_id(kind="entity", name=f"constraint:{aid}")
            evidence_doc_ids_raw = evidence_by_aid.get(aid, {}).get("evidence_doc_ids") or []
            evidence_doc_ids: list[str] = []
            if isinstance(evidence_doc_ids_raw, list):
                evidence_doc_ids = sorted({str(x).strip() for x in evidence_doc_ids_raw if str(x).strip()})
            properties = {
                "plan_id": plan.id,
                "assertion_id": aid,
                "subject": c.get("subject"),
                "predicate": c.get("predicate"),
                "object": c.get("object"),
                "polarity": c.get("polarity"),
                "scope": c.get("scope"),
                "kind": c.get("kind"),
                "summary": c.get("summary"),
                "semantic_fingerprint": c.get("semantic_fingerprint"),
                "evidence_doc_ids": evidence_doc_ids,
            }

            # Filter to JSON-serializable-ish scalars/lists via best-effort casting.
            nodes.append(
                IRNode(
                    id=node_id,
                    tenant_id=plan.tenant_id,
                    kind="entity",
                    name=f"knowledge_constraint:{aid}",
                    properties=properties,  # type: ignore[arg-type]
                    depends_on=assertion_dep,
                    provenance=tuple(provenance),
                )
            )

    canonical_decisions_raw = snap_raw.get("canonical_decisions") or []
    if isinstance(canonical_decisions_raw, list):
        for d in canonical_decisions_raw:
            if not isinstance(d, dict):
                continue
            aid_raw = d.get("assertion_id")
            aid = str(aid_raw).strip() if isinstance(aid_raw, str) else ""
            if not aid:
                continue

            prov_raw = evidence_by_aid.get(aid, {}).get("resolved_provenance_pointers") or []
            dec_provenance: list[ProvenancePointer] = []
            if isinstance(prov_raw, list):
                for p_raw in prov_raw:
                    if not isinstance(p_raw, dict):
                        continue
                    try:
                        ptr = ProvenancePointer.from_json_obj(p_raw)
                        # Tenant-safety: only attach provenance pointers for this tenant.
                        if ptr.tenant_id.strip() == plan.tenant_id.strip():
                            dec_provenance.append(ptr)
                    except Exception:
                        continue

            node_id = stable_node_id(kind="entity", name=f"decision:{aid}")
            evidence_doc_ids_raw = d.get("evidence_doc_ids", ())
            if isinstance(evidence_doc_ids_raw, list):
                evidence_doc_ids = sorted({str(x).strip() for x in evidence_doc_ids_raw if str(x).strip()})
            else:
                evidence_doc_ids = sorted({str(x).strip() for x in (evidence_doc_ids_raw or ()) if str(x).strip()})
            properties = {
                "plan_id": plan.id,
                "assertion_id": aid,
                "selected": bool(d.get("selected", False)),
                "resolved": bool(d.get("resolved", False)),
                "conflict_resolution_target_assertion_ids": d.get("conflict_resolution_target_assertion_ids", ()),
                "evidence_doc_ids": evidence_doc_ids,
            }

            nodes.append(
                IRNode(
                    id=node_id,
                    tenant_id=plan.tenant_id,
                    kind="entity",
                    name=f"knowledge_decision:{aid}",
                    properties=properties,  # type: ignore[arg-type]
                    depends_on=assertion_dep,
                    provenance=tuple(dec_provenance),
                )
            )

    return nodes


def _build_ir_node_properties(
    *,
    plan_id: str,
    step_id: str,
    status: Any,
    order_idx: int,
    step_outputs: Mapping[str, Any],
) -> dict[str, Any]:
    """Attach patch-layer provenance metadata into IR node properties."""

    props: dict[str, Any] = {
        "plan_id": plan_id,
        "step_id": step_id,
        "status": status,
        "order_idx": order_idx,
    }

    last_prompt_key = step_outputs.get("last_prompt_key")
    if isinstance(last_prompt_key, str) and last_prompt_key.strip():
        props["last_prompt_key"] = last_prompt_key.strip()

    last_patch_sha256 = step_outputs.get("last_patch_sha256")
    if isinstance(last_patch_sha256, str) and last_patch_sha256.strip():
        props["last_patch_sha256"] = last_patch_sha256.strip()

    best_candidate = step_outputs.get("best_candidate")
    if isinstance(best_candidate, dict):
        touched_paths_raw = best_candidate.get("touched_paths")
        if isinstance(touched_paths_raw, list):
            touched_paths = [str(x) for x in touched_paths_raw if str(x).strip()]
            if touched_paths:
                props["last_touched_paths"] = touched_paths

    return props
