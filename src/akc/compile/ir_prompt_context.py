"""Deterministic IR + plan-execution slices for LLM prompts.

Compile prompts should treat :class:`~akc.ir.schema.IRDocument` as the structural
semantic spine and :class:`~akc.memory.models.PlanState` as execution state only.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal, cast

from akc.intent.models import IntentSpecV1
from akc.intent.plan_step_intent import INTENT_REF_INPUT_KEY
from akc.intent.resolve import (
    ResolvedIntentContext,
    intent_link_summaries_for_prompts,
    intent_ref_requires_store_resolution,
    intent_reference_summaries_for_prompts,
)
from akc.intent.store import IntentStore
from akc.ir import IRDocument, IRNode
from akc.ir.schema import stable_node_id
from akc.ir.versioning import IR_SCHEMA_KIND
from akc.memory.models import JSONValue, PlanState, PlanStep

logger = logging.getLogger(__name__)

IntentContractShape = Literal["full", "reference_first"]
CompilePromptIntentContractPolicy = Literal["auto", "full", "reference_first"]


def _compact_ir_node(n: IRNode) -> dict[str, JSONValue]:
    """Structural summary aligned with :meth:`IRNode.to_json_obj` dependency ordering."""

    deps_str = sorted({str(d).strip() for d in n.depends_on if str(d).strip()})
    deps: list[JSONValue] = [cast(JSONValue, s) for s in deps_str]
    out: dict[str, JSONValue] = {
        "id": n.id.strip(),
        "kind": n.kind,
        "depends_on": deps,
    }
    if n.contract is not None:
        out["contract_id"] = n.contract.contract_id.strip()
    if n.effects is not None:
        out["effects"] = n.effects.to_json_obj()
    return out


def compact_ir_document_for_prompt(ir_doc: IRDocument) -> dict[str, JSONValue]:
    """Compact IR graph for prompts (same node order as :meth:`IRDocument.to_json_obj`)."""

    nodes_compact = [_compact_ir_node(n) for n in sorted(ir_doc.nodes, key=lambda x: x.id.strip())]
    nodes_value = cast(JSONValue, [cast(JSONValue, x) for x in nodes_compact])
    return {
        "schema_kind": IR_SCHEMA_KIND,
        "schema_version": int(ir_doc.schema_version),
        "format_version": ir_doc.format_version,
        "tenant_id": ir_doc.tenant_id.strip(),
        "repo_id": ir_doc.repo_id.strip(),
        "nodes": nodes_value,
    }


def ir_intent_knowledge_anchor_for_prompt(ir_doc: IRDocument) -> dict[str, JSONValue]:
    """Bounded intent/knowledge identity slice for prompts (LLM ↔ IR constraint id cross-checks).

    Includes stable node fingerprints plus intent ``linked_constraints`` ids and knowledge hub
    fingerprints / assertion id lists (capped) without embedding full IR node payloads.
    """

    intent_rows: list[dict[str, JSONValue]] = []
    knowledge_rows: list[dict[str, JSONValue]] = []
    for n in sorted(ir_doc.nodes, key=lambda x: x.id.strip()):
        if n.kind == "intent":
            cid_list: list[str] = []
            lc = n.properties.get("linked_constraints")
            if isinstance(lc, list):
                for item in lc[:64]:
                    if not isinstance(item, Mapping):
                        continue
                    rid = item.get("constraint_id")
                    if not isinstance(rid, str) or not rid.strip():
                        rid = item.get("id")
                    if isinstance(rid, str) and rid.strip():
                        cid_list.append(rid.strip())
            intent_rows.append(
                {
                    "id": cast(JSONValue, n.id.strip()),
                    "fingerprint": cast(JSONValue, n.fingerprint()),
                    "constraint_ids": cast(JSONValue, sorted(set(cid_list))[:64]),
                }
            )
        elif n.kind == "knowledge":
            props = dict(n.properties)
            aid_ids: list[str] = []
            raw_aids = props.get("knowledge_assertion_ids")
            if isinstance(raw_aids, list):
                for x in raw_aids[:64]:
                    if isinstance(x, str) and x.strip():
                        aid_ids.append(x.strip())
            knowledge_rows.append(
                {
                    "id": cast(JSONValue, n.id.strip()),
                    "fingerprint": cast(JSONValue, n.fingerprint()),
                    "knowledge_semantic_fingerprint_16": props.get("knowledge_semantic_fingerprint_16"),
                    "knowledge_provenance_fingerprint_16": props.get("knowledge_provenance_fingerprint_16"),
                    "knowledge_assertion_ids": cast(JSONValue, sorted(set(aid_ids))[:64]),
                }
            )
    return {
        "intent_nodes": cast(JSONValue, [cast(JSONValue, x) for x in intent_rows]),
        "knowledge_nodes": cast(JSONValue, [cast(JSONValue, x) for x in knowledge_rows]),
    }


def ir_structural_hints_for_retrieval_query(ir_doc: IRDocument) -> str:
    """Stable, compact text to augment index queries (IR spine; no step payloads).

    Uses sorted ``kind:id`` tokens so ordering matches other IR serializations.
    """

    parts: list[str] = []
    for n in sorted(ir_doc.nodes, key=lambda x: x.id.strip()):
        kid = str(n.kind).strip()
        nid = str(n.id).strip()
        if kid and nid:
            parts.append(f"{kid}:{nid}")
    return " ".join(parts)


def plan_execution_trace_for_prompt(plan: PlanState) -> dict[str, JSONValue]:
    """Ordered step ids/titles/status only (no inputs/outputs/notes/budgets)."""

    ordered: list[PlanStep] = sorted(plan.steps, key=lambda s: (int(s.order_idx), s.id))
    steps_out: list[JSONValue] = []
    for s in ordered:
        steps_out.append(
            cast(
                JSONValue,
                {"id": s.id, "title": s.title, "status": s.status},
            )
        )
    trace: dict[str, JSONValue] = {
        "steps": cast(JSONValue, steps_out),
        "next_step_id": plan.next_step_id,
    }
    return trace


def format_prompt_json_section(label: str, payload: Mapping[str, Any]) -> str:
    """Stable JSON text for embedding in prompts (sorted keys, UTF-8)."""

    return f"{label}\n{json.dumps(dict(payload), sort_keys=True, ensure_ascii=False)}\n\n"


def effective_intent_contract_shape_for_compile_prompts(
    *,
    policy: CompilePromptIntentContractPolicy,
    intent_store: IntentStore | None,
    first_step_inputs: Mapping[str, Any],
) -> IntentContractShape:
    """Resolve whether compile prompts + retrieval use reference-first intent slices.

    ``auto`` enables reference-first when a tenant IntentStore is present and the
    first plan step carries a structurally complete ``intent_ref`` (store-backed).
    """

    if policy == "full":
        return "full"
    if policy == "reference_first":
        return "reference_first"
    if intent_store is not None and intent_ref_requires_store_resolution(inputs=first_step_inputs):
        return "reference_first"
    return "full"


@dataclass(frozen=True, slots=True)
class IntentPromptContext:
    """Generate/repair prompt slices derived from resolved intent + IR presence."""

    active_objectives: list[Mapping[str, Any]]
    linked_constraints: list[Mapping[str, Any]]
    active_success_criteria: list[Mapping[str, Any]]


def intent_prompt_context_from_ir_and_resolve(
    *,
    ir_doc: IRDocument,
    resolved: ResolvedIntentContext,
    reference_first: bool,
) -> IntentPromptContext:
    """Build prompt list sections from resolved intent; prefer id+fingerprint rows when anchored to IR + store.

    When ``reference_first`` is set but the IR graph has no ``kind=="intent"`` node,
    falls back to full link summaries (defensive).
    """

    if reference_first and any(n.kind == "intent" for n in ir_doc.nodes):
        ao, lc, asc = intent_reference_summaries_for_prompts(spec=resolved.spec)
    else:
        if reference_first:
            logger.debug(
                "reference_first intent prompts requested but IR has no intent node; using full link summaries"
            )
        ao, lc, asc = intent_link_summaries_for_prompts(spec=resolved.spec)
    return IntentPromptContext(
        active_objectives=list(ao),
        linked_constraints=list(lc),
        active_success_criteria=list(asc),
    )


def build_reference_intent_contract_for_retrieval(
    *,
    intent_spec: IntentSpecV1,
    resolved: ResolvedIntentContext,
    intent_semantic_fingerprint: str,
    intent_goal_text_fingerprint: str,
    operating_bounds_effective: Mapping[str, Any],
    first_step_inputs: Mapping[str, Any],
) -> dict[str, Any]:
    """Reference-first ``intent_contract`` for retrieval context (ids + hashes, minimal duplication)."""

    ao, lc, asc = intent_reference_summaries_for_prompts(spec=resolved.spec)
    intent_node_id = stable_node_id(kind="intent", name=f"intent:{intent_spec.intent_id.strip()}")
    ref_raw = first_step_inputs.get(INTENT_REF_INPUT_KEY)
    intent_ref = dict(ref_raw) if isinstance(ref_raw, dict) else None
    ob_req = intent_spec.operating_bounds.to_json_obj() if intent_spec.operating_bounds is not None else None
    return {
        "intent_contract_shape": "reference_first",
        "intent_id": intent_spec.intent_id,
        "spec_version": int(intent_spec.spec_version),
        "goal_statement": intent_spec.goal_statement,
        "stable_intent_sha256": resolved.stable_intent_sha256,
        "intent_resolution_source": resolved.source,
        "intent_ir_node_id": intent_node_id,
        "intent_semantic_fingerprint": intent_semantic_fingerprint,
        "intent_goal_text_fingerprint": intent_goal_text_fingerprint,
        "intent_ref": intent_ref,
        "active_objectives": ao,
        "linked_constraints": lc,
        "active_success_criteria": asc,
        "operating_bounds_requested": ob_req,
        "operating_bounds": dict(operating_bounds_effective),
    }


def format_active_objectives_for_prompt(active_objectives: list[Mapping[str, Any]]) -> str:
    lines: list[str] = []
    for o in active_objectives:
        if not isinstance(o, Mapping):
            continue
        oid = str(o.get("id") or "").strip()
        fp = str(o.get("fingerprint") or "").strip()
        stmt = str(o.get("statement") or o.get("summary") or "").strip()
        target = str(o.get("target") or "").strip()
        if fp and not stmt:
            tail = f" (target: {target})" if target else ""
            lines.append(f"- {oid}: {fp}{tail}".strip())
        elif stmt or oid:
            tail = f" (target: {target})" if target else ""
            lines.append(f"- {oid}: {stmt}{tail}".strip())
    return "\n".join(lines) if lines else "- (none)"


def format_linked_constraints_for_prompt(linked_constraints: list[Mapping[str, Any]]) -> str:
    lines: list[str] = []
    for c in linked_constraints:
        if not isinstance(c, Mapping):
            continue
        cid = str(c.get("constraint_id") or c.get("id") or "").strip()
        fp = str(c.get("fingerprint") or "").strip()
        kind = str(c.get("kind") or "").strip()
        summ = str(c.get("summary") or c.get("statement") or "").strip()
        if fp and not summ:
            kind_part = f" ({kind})" if kind else ""
            lines.append(f"- {cid}{kind_part}: {fp}".strip())
        elif summ or cid:
            kind_part = f" ({kind})" if kind else ""
            lines.append(f"- {cid}{kind_part}: {summ}".strip())
    return "\n".join(lines) if lines else "- (none)"


def format_success_criteria_for_prompt(active_success_criteria: list[Mapping[str, Any]]) -> str:
    lines: list[str] = []
    for sc in active_success_criteria:
        if not isinstance(sc, Mapping):
            continue
        sid = str(sc.get("success_criterion_id") or sc.get("id") or "").strip()
        fp = str(sc.get("fingerprint") or "").strip()
        mode = str(sc.get("evaluation_mode") or "").strip()
        summ = str(sc.get("summary") or sc.get("description") or "").strip()
        if fp and not summ:
            mode_part = f" ({mode})" if mode else ""
            lines.append(f"- {sid}{mode_part}: {fp}".strip())
        elif summ or sid:
            mode_part = f" ({mode})" if mode else ""
            lines.append(f"- {sid}{mode_part}: {summ}".strip())
    return "\n".join(lines) if lines else "- (none)"
