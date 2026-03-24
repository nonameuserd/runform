from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from akc.compile.controller_config import KnowledgeConflictNormalization
from akc.compile.knowledge_extractor import (
    compute_assertion_conflict_resolution_metadata,
    evidence_scores_for_conflict_resolution,
)
from akc.control.policy import KnowledgeUnresolvedConflictPolicy
from akc.ir.provenance import ProvenancePointer
from akc.knowledge.models import (
    CanonicalDecision,
    EvidenceMapping,
    KnowledgeSnapshot,
)
from akc.memory.models import WhyEdge, WhyNode
from akc.memory.why_graph_store_base import WhyGraphStore


@dataclass(frozen=True, slots=True)
class _ResolutionInfo:
    resolved_constraint_id: str
    rejected_constraint_ids: tuple[str, ...]
    resolution_rule: str
    provenance: tuple[dict[str, Any], ...]
    evidence_doc_ids: tuple[str, ...]


def _ptrs_to_json(ptrs: Iterable[ProvenancePointer]) -> tuple[dict[str, Any], ...]:
    return tuple(p.to_json_obj() for p in ptrs)


def _build_resolution_info_by_aid(
    *,
    snapshot: KnowledgeSnapshot,
    knowledge_unresolved_conflict_policy: KnowledgeUnresolvedConflictPolicy,
    knowledge_conflict_normalization: KnowledgeConflictNormalization | None = None,
    knowledge_embedding_clustering_enabled: bool = False,
    knowledge_embedding_clustering_threshold: float = 0.92,
    documents: Sequence[Mapping[str, Any]] | None = None,
    intent_constraint_ids_by_assertion: Mapping[str, str] | None = None,
) -> dict[str, _ResolutionInfo]:
    canonical_decisions = snapshot.canonical_decisions
    evidence_by_assertion = snapshot.evidence_by_assertion

    decisions_by_aid: dict[str, CanonicalDecision] = {d.assertion_id: d for d in canonical_decisions}

    scores = evidence_scores_for_conflict_resolution(snapshot=snapshot)
    doc_counts: dict[str, int] = {}
    for c in snapshot.canonical_constraints:
        em = snapshot.evidence_by_assertion.get(c.assertion_id)
        doc_counts[c.assertion_id] = len(em.evidence_doc_ids) if em is not None else 0
    doc_map: dict[str, Mapping[str, Any]] = {}
    if documents is not None:
        for doc in documents:
            if not isinstance(doc, Mapping):
                continue
            raw = doc.get("doc_id")
            if isinstance(raw, str) and raw.strip():
                doc_map[raw.strip()] = doc

    meta = compute_assertion_conflict_resolution_metadata(
        constraints=snapshot.canonical_constraints,
        evidence_scores=scores,
        unresolved_policy=knowledge_unresolved_conflict_policy,
        evidence_doc_counts_by_assertion=doc_counts,
        mediation_events=None,
        normalization=knowledge_conflict_normalization,
        evidence_by_assertion=snapshot.evidence_by_assertion,
        documents_by_id=doc_map if doc_map else None,
        embedding_clustering_enabled=knowledge_embedding_clustering_enabled,
        embedding_cluster_threshold=knowledge_embedding_clustering_threshold,
        intent_constraint_ids_by_assertion=intent_constraint_ids_by_assertion,
    )

    resolution_by_aid: dict[str, _ResolutionInfo] = {}

    for aid, row in meta.items():
        winner_aid = row.winner_assertion_id
        rejected_ids = tuple(x for x in row.participant_assertion_ids if x != winner_aid)
        winner_evidence: EvidenceMapping | None = evidence_by_assertion.get(winner_aid)
        provenance = _ptrs_to_json(winner_evidence.resolved_provenance_pointers) if winner_evidence is not None else ()
        evidence_doc_ids = winner_evidence.evidence_doc_ids if winner_evidence is not None else ()

        resolution_by_aid[aid] = _ResolutionInfo(
            resolved_constraint_id=winner_aid,
            rejected_constraint_ids=rejected_ids,
            resolution_rule=row.resolution_rule,
            provenance=provenance,
            evidence_doc_ids=evidence_doc_ids,
        )

    # Best-effort fallback for decision rows that are not part of an automated group
    # (defensive; normal snapshots only emit decisions for grouped conflicts).
    for aid, d in decisions_by_aid.items():
        if aid in resolution_by_aid:
            continue
        winner = (
            d.assertion_id
            if d.selected
            else (sorted([x for x in decisions_by_aid if decisions_by_aid[x].selected]) or [d.assertion_id])[0]
        )
        winner_evidence = evidence_by_assertion.get(winner)
        resolution_by_aid[aid] = _ResolutionInfo(
            resolved_constraint_id=winner,
            rejected_constraint_ids=tuple(sorted([x for x in decisions_by_aid if x != winner])),
            resolution_rule="unknown_resolution_fallback",
            provenance=_ptrs_to_json(winner_evidence.resolved_provenance_pointers)
            if winner_evidence is not None
            else (),
            evidence_doc_ids=winner_evidence.evidence_doc_ids if winner_evidence is not None else (),
        )

    return resolution_by_aid


def upsert_knowledge_snapshot_into_why_graph(
    *,
    tenant_id: str,
    repo_id: str,
    why_graph: WhyGraphStore,
    snapshot: KnowledgeSnapshot,
    plan_goal: str,
    intent_id: str | None = None,
    knowledge_unresolved_conflict_policy: KnowledgeUnresolvedConflictPolicy = "warn_and_continue",
    knowledge_conflict_normalization: KnowledgeConflictNormalization | None = None,
    knowledge_embedding_clustering_enabled: bool = False,
    knowledge_embedding_clustering_threshold: float = 0.92,
    documents: Sequence[Mapping[str, Any]] | None = None,
    intent_constraint_ids_by_assertion: Mapping[str, str] | None = None,
) -> dict[str, int]:
    """Upsert knowledge-layer canonical constraints/decisions into why-graph.

    - `constraint` nodes get semantic payload + claim-level provenance pointers.
    - `decision` nodes get resolution payloads (resolved + rejected ids) and provenance.
    """

    if not snapshot.canonical_constraints and not snapshot.canonical_decisions:
        return {"nodes_written": 0, "edges_written": 0}

    resolution_by_aid = _build_resolution_info_by_aid(
        snapshot=snapshot,
        knowledge_unresolved_conflict_policy=knowledge_unresolved_conflict_policy,
        knowledge_conflict_normalization=knowledge_conflict_normalization,
        knowledge_embedding_clustering_enabled=knowledge_embedding_clustering_enabled,
        knowledge_embedding_clustering_threshold=knowledge_embedding_clustering_threshold,
        documents=documents,
        intent_constraint_ids_by_assertion=intent_constraint_ids_by_assertion,
    )

    nodes: list[WhyNode] = []
    edges: list[WhyEdge] = []

    # Constraint nodes (always emitted for canonical constraints).
    for c in snapshot.canonical_constraints:
        evidence: EvidenceMapping | None = snapshot.evidence_by_assertion.get(c.assertion_id)
        resolved_ptrs = evidence.resolved_provenance_pointers if evidence is not None else ()
        evidence_doc_ids = evidence.evidence_doc_ids if evidence is not None else ()

        payload: dict[str, Any] = {
            "subject": str(c.subject),
            "predicate": str(c.predicate),
            "object": c.object if c.object is None else str(c.object),
            "polarity": int(c.polarity),
            "scope": str(c.scope),
            # Claim-level provenance: later conflict reporters can reuse this.
            # Keep JSON shape stable for both in-memory and SQLite stores.
            # In-memory store preserves Python tuples; conflict detectors expect lists.
            "provenance": list(_ptrs_to_json(resolved_ptrs)),
            "evidence_doc_ids": list(evidence_doc_ids),
            # Drift detector expects a `source` object with goal/plan_goal.
            "source": {
                "goal": str(plan_goal),
                "intent_id": str(intent_id) if intent_id is not None else None,
            },
            # Extra fields are harmless; contradiction detector ignores them.
            "kind": str(c.kind),
            "summary": str(c.summary),
            "semantic_fingerprint": str(c.semantic_fingerprint),
        }
        # Remove null intent_id for cleaner payload.
        if payload["source"].get("intent_id") is None:
            payload["source"].pop("intent_id", None)

        nodes.append(
            WhyNode(
                id=str(c.assertion_id),
                type="constraint",
                payload=payload,
            )
        )

    # Decision nodes (resolution payloads).
    for d in snapshot.canonical_decisions:
        info = resolution_by_aid.get(d.assertion_id)
        if info is None:
            continue

        decision_node_id = f"decision_{d.assertion_id}"
        decision_payload: dict[str, Any] = {
            "resolved_constraint_id": str(info.resolved_constraint_id),
            "rejected_constraint_ids": list(info.rejected_constraint_ids),
            "resolution_rule": str(info.resolution_rule),
            "provenance": list(info.provenance),
            "evidence_doc_ids": list(info.evidence_doc_ids),
            # Preserve canonical decision flags.
            "selected": bool(d.selected),
            "resolved": bool(d.resolved),
            "assertion_id": str(d.assertion_id),
            "conflict_resolution_target_assertion_ids": list(d.conflict_resolution_target_assertion_ids),
        }

        nodes.append(
            WhyNode(
                id=decision_node_id,
                type="decision",
                payload=decision_payload,
            )
        )

        # Edges: decision supports resolved constraint and prevents rejected constraints.
        edges.append(
            WhyEdge(
                src=decision_node_id,
                dst=str(info.resolved_constraint_id),
                type="supports",
                payload={
                    "role": "resolved",
                    "resolution_rule": str(info.resolution_rule),
                },
            )
        )
        for rid in info.rejected_constraint_ids:
            edges.append(
                WhyEdge(
                    src=decision_node_id,
                    dst=str(rid),
                    type="prevents",
                    payload={
                        "role": "rejected",
                        "resolution_rule": str(info.resolution_rule),
                    },
                )
            )

    nodes_written = why_graph.upsert_nodes(tenant_id=tenant_id, repo_id=repo_id, nodes=nodes)
    edges_written = why_graph.add_edges(tenant_id=tenant_id, repo_id=repo_id, edges=edges)
    return {"nodes_written": int(nodes_written), "edges_written": int(edges_written)}
