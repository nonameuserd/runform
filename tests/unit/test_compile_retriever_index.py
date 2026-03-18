from __future__ import annotations

from dataclasses import dataclass

import pytest

from akc.compile.interfaces import Index, IndexDocument, IndexQuery, TenantRepoScope
from akc.compile.retriever import retrieve_context
from akc.memory.facade import build_memory


@dataclass(frozen=True)
class _SpyIndex(Index):
    docs: list[IndexDocument]
    last_scope: TenantRepoScope | None = None
    last_query: IndexQuery | None = None

    def query(self, *, scope: TenantRepoScope, query: IndexQuery):  # type: ignore[override]
        object.__setattr__(self, "last_scope", scope)
        object.__setattr__(self, "last_query", query)
        return list(self.docs)


def test_retrieve_context_includes_scored_documents_when_index_provided() -> None:
    mem = build_memory(backend="memory")
    plan = mem.plan_state.create_plan(
        tenant_id="t1",
        repo_id="repo1",
        goal="Do the thing",
        initial_steps=["first", "second"],
    )
    mem.plan_state.set_active_plan(tenant_id="t1", repo_id="repo1", plan_id=plan.id)

    idx = _SpyIndex(
        docs=[
            IndexDocument(
                doc_id="d1",
                title="Doc 1",
                content="hello",
                score=0.9,
                metadata={"repo_id": "repo1"},
            )
        ]
    )

    ctx = retrieve_context(
        tenant_id="t1",
        repo_id="repo1",
        plan=plan,
        code_memory=mem.code_memory,
        why_graph=mem.why_graph,
        index=idx,
        limit=5,
    )

    assert idx.last_scope == TenantRepoScope(tenant_id="t1", repo_id="repo1")
    assert idx.last_query is not None
    assert idx.last_query.k == 5
    assert idx.last_query.filters == {"repo_id": "repo1"}

    assert ctx["documents"] == [
        {
            "doc_id": "d1",
            "title": "Doc 1",
            "content": "hello",
            "score": 0.9,
            "metadata": {"repo_id": "repo1"},
        }
    ]


def test_retrieve_context_rejects_plan_scope_mismatch() -> None:
    mem = build_memory(backend="memory")
    plan = mem.plan_state.create_plan(
        tenant_id="t1",
        repo_id="repo1",
        goal="Do the thing",
        initial_steps=["first"],
    )

    with pytest.raises(ValueError, match="tenant_id/repo_id mismatch"):
        retrieve_context(
            tenant_id="t2",
            repo_id="repo1",
            plan=plan,
            code_memory=mem.code_memory,
            index=None,
            limit=5,
        )
