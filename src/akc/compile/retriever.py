"""Retriever hooks (Phase 2).

Phase 2 retrieval is intentionally simple: pull recent code memory items and
optionally include why-graph constraints/decisions for the tenant+repo scope.

Phase 3: optional compact knowledge (prior snapshot) augments the index query
text; tenant/repo scope is unchanged. Post-retrieval, evidence-linked docs can
be score-boosted using the current compile snapshot (see
``boost_retrieved_documents_for_knowledge_evidence``).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from akc.compile.interfaces import Index, IndexQuery, TenantRepoScope
from akc.compile.ir_prompt_context import ir_structural_hints_for_retrieval_query
from akc.ir import IRDocument
from akc.knowledge.models import KnowledgeSnapshot
from akc.memory.code_memory import CodeMemoryStore
from akc.memory.models import PlanState, normalize_repo_id, require_non_empty
from akc.memory.why_graph import WhyGraphStore


def compact_knowledge_query_suffix(
    snapshot: KnowledgeSnapshot | None,
    *,
    max_chars: int = 1200,
) -> str:
    """Build a bounded text block of selected hard constraints for query augmentation.

    Uses the same tenant-scoped snapshot object only; does not widen retrieval.
    """

    if snapshot is None or max_chars <= 0:
        return ""
    decisions = {d.assertion_id: d for d in snapshot.canonical_decisions}
    lines: list[str] = []
    for c in sorted(snapshot.canonical_constraints, key=lambda x: x.assertion_id):
        if c.kind != "hard":
            continue
        dec = decisions.get(c.assertion_id)
        if dec is not None and not dec.selected:
            continue
        subj = str(c.subject).strip()
        summ = str(c.summary).strip()
        if subj and summ:
            lines.append(f"- {subj}: {summ}")
        elif summ:
            lines.append(f"- {summ}")
        elif subj:
            lines.append(f"- {subj}")
    if not lines:
        return ""
    header = "Knowledge constraints (compile-time, tenant-scoped):"
    body = "\n".join(lines)
    text = f"{header}\n{body}"
    if len(text) <= max_chars:
        return f"\n\n{text}"
    return f"\n\n{text[: max_chars - 3]}..."


def _build_query_text(
    plan: PlanState,
    *,
    ir_document: IRDocument | None = None,
    knowledge_query_suffix: str = "",
) -> str:
    goal = plan.goal.strip()
    title: str | None = None
    if plan.next_step_id is not None:
        for s in plan.steps:
            if s.id == plan.next_step_id:
                title = s.title.strip()
                break
    base = f"{goal}\n\nNext step: {title}" if title else goal
    if ir_document is None:
        out = base
    else:
        hints = ir_structural_hints_for_retrieval_query(ir_document)
        out = base if not hints.strip() else f"{base}\n\nIR structure: {hints}"
    if knowledge_query_suffix:
        return f"{out}{knowledge_query_suffix}"
    return out


def boost_retrieved_documents_for_knowledge_evidence(
    documents: Sequence[Mapping[str, Any]] | None,
    *,
    snapshot: KnowledgeSnapshot,
    boost_delta: float = 0.25,
) -> list[dict[str, Any]]:
    """Increase ``score`` for index rows whose ``doc_id`` appears in evidence mappings.

    Only boosts **selected** hard constraints' evidence doc ids. Deterministic:
    stable sort by (new_score desc, doc_id) after adjustment.
    """

    decisions = {d.assertion_id: d for d in snapshot.canonical_decisions}
    cited: set[str] = set()
    for c in snapshot.canonical_constraints:
        if c.kind != "hard":
            continue
        dec = decisions.get(c.assertion_id)
        if dec is not None and not dec.selected:
            continue
        ev = snapshot.evidence_by_assertion.get(c.assertion_id)
        if ev is None:
            continue
        cited.update(ev.evidence_doc_ids)

    if not cited:
        return [dict(d) for d in (documents or ())]

    out: list[dict[str, Any]] = []
    for raw in documents or ():
        row = dict(raw)
        did = str(row.get("doc_id", "")).strip()
        score_raw = row.get("score", 0.0)
        try:
            base_score = float(score_raw)
        except (TypeError, ValueError):
            base_score = 0.0
        if did and did in cited:
            row["score"] = base_score + float(boost_delta)
            row["knowledge_evidence_boost"] = True
        else:
            row["score"] = base_score
        out.append(row)

    out.sort(key=lambda r: (-float(r.get("score") or 0.0), str(r.get("doc_id", ""))))
    return out


def retrieve_context(
    *,
    tenant_id: str,
    repo_id: str,
    plan: PlanState,
    code_memory: CodeMemoryStore,
    why_graph: WhyGraphStore | None = None,
    index: Index | None = None,
    limit: int = 20,
    ir_document: IRDocument | None = None,
    knowledge_snapshot_for_query: KnowledgeSnapshot | None = None,
    knowledge_query_budget_chars: int = 1200,
) -> dict[str, Any]:
    """Retrieve context for the compile loop.

    When ``knowledge_snapshot_for_query`` is set (e.g. prior step outputs), a
    compact constraint summary is appended to the index query text. Scope stays
    ``tenant_id`` / ``repo_id``; the index implementation must not widen beyond
    the existing ``TenantRepoScope`` query.
    """

    require_non_empty(tenant_id, name="tenant_id")
    repo = normalize_repo_id(repo_id)
    if plan.tenant_id != tenant_id or normalize_repo_id(plan.repo_id) != repo:
        raise ValueError("tenant_id/repo_id mismatch between arguments and plan")
    if limit <= 0:
        raise ValueError("limit must be > 0")
    if ir_document is not None:
        if ir_document.tenant_id.strip() != tenant_id.strip():
            raise ValueError("IR tenant_id must match plan tenant_id for retrieval queries")
        if normalize_repo_id(ir_document.repo_id) != repo:
            raise ValueError("IR repo_id must match plan repo_id for retrieval queries")

    mem_items = code_memory.list_items(tenant_id=tenant_id, repo_id=repo, limit=int(limit))

    why: dict[str, Any] | None = None
    if why_graph is not None:
        constraints = why_graph.list_nodes_by_type(tenant_id=tenant_id, repo_id=repo, node_type="constraint")
        decisions = why_graph.list_nodes_by_type(tenant_id=tenant_id, repo_id=repo, node_type="decision")
        why = {
            "constraints": [dict(n.payload) for n in constraints],
            "decisions": [dict(n.payload) for n in decisions],
        }

    suffix = compact_knowledge_query_suffix(
        knowledge_snapshot_for_query,
        max_chars=max(0, int(knowledge_query_budget_chars)),
    )
    docs: list[Mapping[str, Any]] = []
    if index is not None:
        scope = TenantRepoScope(tenant_id=tenant_id, repo_id=repo)
        q = IndexQuery(
            text=_build_query_text(plan, ir_document=ir_document, knowledge_query_suffix=suffix),
            k=int(limit),
            filters={"repo_id": repo},
        )
        results = index.query(scope=scope, query=q)
        docs = [
            {
                "doc_id": d.doc_id,
                "title": d.title,
                "content": d.content,
                "score": d.score,
                "metadata": dict(d.metadata or {}),
            }
            for d in results
        ]

    return {
        "code_memory_items": [i.to_json_obj() for i in mem_items],
        "documents": docs,
        "why_graph": why,
    }
