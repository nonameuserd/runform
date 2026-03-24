"""03_doc_derived_surface: default ``doc_derived_assertions_mode=limited`` vs opt-out ``off``.

Uses corpus chunk ``kd-doc-derived-controls`` (``corpus_manifest.json``) to assert the
pipeline boundary aligned with ``ControllerConfig.doc_derived_assertions_mode`` default
``limited`` in ``akc.compile.controller_config``: ``merge_documents_into_assertion_index``
plus ``extract_knowledge_snapshot`` participates (assertion index rows + snapshot soft
constraints), and yields no constraints when mode is ``off`` with empty intent (early
return in ``extract_knowledge_snapshot``).
"""

from __future__ import annotations

from pathlib import Path

from akc.compile.assertion_index_store import merge_documents_into_assertion_index
from akc.compile.knowledge_extractor import extract_knowledge_snapshot
from tests.integration.knowledge_domain_coverage_registry import KD_DOC_IDS_DOC_DERIVED
from tests.integration.test_knowledge_ingest_index_extract_ir_policy import (
    _intent,
    _load_documents,
    _scope_root,
)


def test_doc_derived_fixture_merge_and_limited_extract_non_empty(tmp_path: Path) -> None:
    """Ingest merge + compile extract (default limited) yields doc-derived soft assertions."""

    tenant_id = "tenant_kd_doc_derived"
    repo_id = "repo_kd_doc_derived"
    docs = _load_documents(subset_doc_ids=KD_DOC_IDS_DOC_DERIVED)
    assert len(docs) == 1
    meta = docs[0].get("metadata") or {}
    assert meta.get("ingest_source_kind") == "docs"

    root = _scope_root(tmp_path, tenant_id=tenant_id, repo_id=repo_id)
    n_merged = merge_documents_into_assertion_index(
        scope_root=root,
        tenant_id=tenant_id,
        repo_id=repo_id,
        documents=docs,
        max_assertions_per_batch=128,
    )
    assert n_merged >= 1

    intent = _intent(tenant_id=tenant_id, repo_id=repo_id, constraints=())
    retrieved = {"documents": docs}
    retrieved_ids = {str(d["doc_id"]).strip() for d in docs}
    provenance = {
        did: {
            "kind": "doc_chunk",
            "source_id": did,
            "tenant_id": tenant_id,
            "locator": f"fixture:{did}",
        }
        for did in retrieved_ids
    }

    snap = extract_knowledge_snapshot(
        tenant_id=tenant_id,
        repo_id=repo_id,
        intent_spec=intent,
        retrieved_context=retrieved,
        retrieval_provenance_by_doc_id=provenance,
        llm=None,
        use_llm=False,
        knowledge_artifact_root=root,
        stored_assertion_index_mode="merge",
        stored_assertion_index_max_rows=128,
    )
    assert len(snap.canonical_constraints) >= 1
    assert all(c.kind == "soft" for c in snap.canonical_constraints)
    summaries = " ".join(str(c.summary) for c in snap.canonical_constraints).lower()
    assert "tls" in summaries or "jwt" in summaries
    assert any("kd-doc-derived-controls" in tuple(ev.evidence_doc_ids) for ev in snap.evidence_by_assertion.values())


def test_doc_derived_mode_off_empty_intent_returns_empty_snapshot(tmp_path: Path) -> None:
    """Opt-out ``off`` with no intent constraints skips doc-derived extraction (empty snapshot)."""

    tenant_id = "tenant_kd_doc_derived_off"
    repo_id = "repo_kd_doc_derived_off"
    docs = _load_documents(subset_doc_ids=KD_DOC_IDS_DOC_DERIVED)

    root = _scope_root(tmp_path, tenant_id=tenant_id, repo_id=repo_id)
    merge_documents_into_assertion_index(
        scope_root=root,
        tenant_id=tenant_id,
        repo_id=repo_id,
        documents=docs,
        max_assertions_per_batch=128,
    )

    intent = _intent(tenant_id=tenant_id, repo_id=repo_id, constraints=())
    retrieved = {"documents": docs}
    retrieved_ids = {str(d["doc_id"]).strip() for d in docs}
    provenance = {
        did: {
            "kind": "doc_chunk",
            "source_id": did,
            "tenant_id": tenant_id,
            "locator": f"fixture:{did}",
        }
        for did in retrieved_ids
    }

    snap = extract_knowledge_snapshot(
        tenant_id=tenant_id,
        repo_id=repo_id,
        intent_spec=intent,
        retrieved_context=retrieved,
        retrieval_provenance_by_doc_id=provenance,
        llm=None,
        use_llm=False,
        doc_derived_assertions_mode="off",
        knowledge_artifact_root=root,
        stored_assertion_index_mode="merge",
        stored_assertion_index_max_rows=128,
    )
    assert snap.canonical_constraints == ()
    assert snap.evidence_by_assertion == {}
