"""Retriever hooks (Phase 2).

Phase 2 retrieval is intentionally simple: pull recent code memory items and
optionally include why-graph constraints/decisions for the tenant+repo scope.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from akc.compile.interfaces import Index, IndexQuery, TenantRepoScope
from akc.memory.code_memory import CodeMemoryStore
from akc.memory.models import PlanState, normalize_repo_id, require_non_empty
from akc.memory.why_graph import WhyGraphStore


def _build_query_text(plan: PlanState) -> str:
    goal = plan.goal.strip()
    title: str | None = None
    if plan.next_step_id is not None:
        for s in plan.steps:
            if s.id == plan.next_step_id:
                title = s.title.strip()
                break
    if title:
        return f"{goal}\n\nNext step: {title}"
    return goal


def retrieve_context(
    *,
    tenant_id: str,
    repo_id: str,
    plan: PlanState,
    code_memory: CodeMemoryStore,
    why_graph: WhyGraphStore | None = None,
    index: Index | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    """Retrieve context for the compile loop without implementing Phase 3 logic."""

    require_non_empty(tenant_id, name="tenant_id")
    repo = normalize_repo_id(repo_id)
    if plan.tenant_id != tenant_id or normalize_repo_id(plan.repo_id) != repo:
        raise ValueError("tenant_id/repo_id mismatch between arguments and plan")
    if limit <= 0:
        raise ValueError("limit must be > 0")

    mem_items = code_memory.list_items(tenant_id=tenant_id, repo_id=repo, limit=int(limit))

    why: dict[str, Any] | None = None
    if why_graph is not None:
        constraints = why_graph.list_nodes_by_type(
            tenant_id=tenant_id, repo_id=repo, node_type="constraint"
        )
        decisions = why_graph.list_nodes_by_type(
            tenant_id=tenant_id, repo_id=repo, node_type="decision"
        )
        why = {
            "constraints": [dict(n.payload) for n in constraints],
            "decisions": [dict(n.payload) for n in decisions],
        }

    docs: list[Mapping[str, Any]] = []
    if index is not None:
        scope = TenantRepoScope(tenant_id=tenant_id, repo_id=repo)
        q = IndexQuery(text=_build_query_text(plan), k=int(limit), filters={"repo_id": repo})
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
