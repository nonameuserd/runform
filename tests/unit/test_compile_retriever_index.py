from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pytest

from akc.compile.interfaces import Index, IndexDocument, IndexQuery, TenantRepoScope
from akc.compile.ir_builder import build_ir_document_from_plan
from akc.compile.retriever import boost_retrieved_documents_for_knowledge_evidence, retrieve_context
from akc.ir import IRDocument
from akc.knowledge.models import CanonicalConstraint, CanonicalDecision, EvidenceMapping, KnowledgeSnapshot
from akc.memory.facade import build_memory
from akc.memory.models import CodeArtifactRef, CodeMemoryItem, PlanState, PlanStep, now_ms


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


def test_retrieve_context_ir_document_augmented_query_text() -> None:
    mem = build_memory(backend="memory")
    t = now_ms()
    plan = PlanState(
        id="plan_ir",
        tenant_id="t1",
        repo_id="repo1",
        goal="Goal text",
        status="active",
        created_at_ms=t,
        updated_at_ms=t,
        steps=(
            PlanStep(
                id="s1",
                title="step one",
                status="pending",
                order_idx=0,
                inputs={"intent_id": "i1"},
            ),
        ),
        next_step_id="s1",
    )
    ir = build_ir_document_from_plan(plan=plan, intent_node_properties=None)
    idx = _SpyIndex(docs=[])
    retrieve_context(
        tenant_id="t1",
        repo_id="repo1",
        plan=plan,
        code_memory=mem.code_memory,
        index=idx,
        limit=5,
        ir_document=ir,
    )
    assert idx.last_query is not None
    assert "IR structure:" in idx.last_query.text
    assert "intent:" in idx.last_query.text


def test_retrieve_context_rejects_ir_tenant_mismatch() -> None:
    mem = build_memory(backend="memory")
    plan = mem.plan_state.create_plan(
        tenant_id="t1",
        repo_id="repo1",
        goal="Do the thing",
        initial_steps=["first"],
    )
    bad_ir = IRDocument(tenant_id="other", repo_id="repo1", nodes=())
    with pytest.raises(ValueError, match="tenant_id must match"):
        retrieve_context(
            tenant_id="t1",
            repo_id="repo1",
            plan=plan,
            code_memory=mem.code_memory,
            index=None,
            limit=5,
            ir_document=bad_ir,
        )


def test_boost_retrieved_documents_prefers_evidence_doc_ids() -> None:
    c = CanonicalConstraint(
        subject="s",
        predicate="forbidden",
        object=None,
        polarity=1,
        scope="repo",
        kind="hard",
        summary="x",
    )
    d = CanonicalDecision(assertion_id=c.assertion_id, selected=True, resolved=True)
    ev = EvidenceMapping(evidence_doc_ids=("cite-me",), resolved_provenance_pointers=())
    snap = KnowledgeSnapshot(
        canonical_constraints=(c,),
        canonical_decisions=(d,),
        evidence_by_assertion={c.assertion_id: ev},
    )
    docs = [
        {"doc_id": "other", "title": "o", "content": "", "score": 0.9},
        {"doc_id": "cite-me", "title": "c", "content": "", "score": 0.5},
    ]
    boosted = boost_retrieved_documents_for_knowledge_evidence(docs, snapshot=snap, boost_delta=0.5)
    assert boosted[0]["doc_id"] == "cite-me"
    assert float(boosted[0]["score"]) >= float(boosted[1]["score"])


def test_retrieve_context_prior_knowledge_suffix_in_query() -> None:
    mem = build_memory(backend="memory")
    t = now_ms()
    c = CanonicalConstraint(
        subject="policy",
        predicate="forbidden",
        object=None,
        polarity=1,
        scope="repo",
        kind="hard",
        summary="no outbound calls",
    )
    d = CanonicalDecision(assertion_id=c.assertion_id, selected=True, resolved=True)
    ev = EvidenceMapping(evidence_doc_ids=(), resolved_provenance_pointers=())
    prior = KnowledgeSnapshot(
        canonical_constraints=(c,),
        canonical_decisions=(d,),
        evidence_by_assertion={c.assertion_id: ev},
    )
    plan = PlanState(
        id="pk",
        tenant_id="t1",
        repo_id="repo1",
        goal="Goal",
        status="active",
        created_at_ms=t,
        updated_at_ms=t,
        steps=(PlanStep(id="s1", title="one", status="pending", order_idx=0, inputs={}),),
        next_step_id="s1",
    )
    idx = _SpyIndex(docs=[])
    retrieve_context(
        tenant_id="t1",
        repo_id="repo1",
        plan=plan,
        code_memory=mem.code_memory,
        index=idx,
        limit=5,
        knowledge_snapshot_for_query=prior,
    )
    assert idx.last_query is not None
    assert "Knowledge constraints" in idx.last_query.text
    assert "no outbound" in idx.last_query.text


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


def test_retrieve_context_weighted_memory_ranking_and_trace(tmp_path: Path) -> None:
    mem = build_memory(backend="memory")
    plan = mem.plan_state.create_plan(
        tenant_id="t1",
        repo_id="repo1",
        goal="prefer pinned",
        initial_steps=["first"],
    )
    idx = _SpyIndex(
        docs=[
            IndexDocument(
                doc_id="d1",
                title="Low relevance",
                content="noise",
                score=0.2,
                metadata={"repo_id": "repo1"},
            ),
            IndexDocument(
                doc_id="d2",
                title="Pinned doc",
                content="prefer pinned",
                score=0.2,
                metadata={"repo_id": "repo1"},
            ),
        ]
    )
    (tmp_path / ".akc").mkdir(parents=True)
    (tmp_path / ".akc" / "memory_policy.json").write_text(
        json.dumps({"token_budget": {"compile": 32}}),
        encoding="utf-8",
    )
    ctx = retrieve_context(
        tenant_id="t1",
        repo_id="repo1",
        plan=plan,
        code_memory=mem.code_memory,
        index=idx,
        limit=5,
        weighted_memory_enabled=True,
        weighted_memory_policy_root=tmp_path,
        weighted_memory_pins=("document:d2",),
    )
    trace = ctx.get("memory_trace")
    assert isinstance(trace, dict)
    assert trace.get("score_version") == "salience-v1"
    selected_ids = trace.get("selected_ids")
    assert isinstance(selected_ids, list)
    assert "document:d2" in selected_ids


def test_retrieve_context_weighted_memory_filters_code_memory_by_scope() -> None:
    mem = build_memory(backend="memory")
    plan = mem.plan_state.create_plan(
        tenant_id="t1",
        repo_id="repo1",
        goal="memory scope",
        initial_steps=["first"],
    )
    mem.code_memory.upsert_items(
        tenant_id="t1",
        repo_id="repo1",
        artifact_id=None,
        items=[
            CodeMemoryItem(
                id="local-item",
                ref=CodeArtifactRef(tenant_id="t1", repo_id="repo1", artifact_id=None),
                kind="note",
                content="local tenant memory",
                metadata={},
                created_at_ms=1,
                updated_at_ms=1,
            )
        ],
    )
    mem.code_memory.upsert_items(
        tenant_id="t2",
        repo_id="repo1",
        artifact_id=None,
        items=[
            CodeMemoryItem(
                id="other-tenant-item",
                ref=CodeArtifactRef(tenant_id="t2", repo_id="repo1", artifact_id=None),
                kind="note",
                content="must not leak",
                metadata={},
                created_at_ms=1,
                updated_at_ms=1,
            )
        ],
    )

    ctx = retrieve_context(
        tenant_id="t1",
        repo_id="repo1",
        plan=plan,
        code_memory=mem.code_memory,
        index=None,
        limit=10,
        weighted_memory_enabled=True,
    )
    rows = ctx.get("code_memory_items")
    assert isinstance(rows, list)
    ids = {str(r.get("item_id")) for r in rows if isinstance(r, dict)}
    assert "local-item" in ids
    assert "other-tenant-item" not in ids
