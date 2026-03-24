from __future__ import annotations

import sqlite3
from pathlib import Path

from akc.compile.assertion_index_store import (
    ASSERTION_INDEX_SCHEMA_VERSION,
    assertion_index_sqlite_path,
    load_assertions_for_doc_ids,
    merge_documents_into_assertion_index,
    merge_indexed_assertions_into_snapshot_state,
)
from akc.compile.controller_config import DocDerivedPatternOptions
from akc.compile.knowledge_extractor import _finalize_knowledge_snapshot_conflicts
from akc.knowledge.models import CanonicalConstraint, EvidenceMapping


def test_assertion_index_merge_and_load_roundtrip(tmp_path: Path) -> None:
    scope = tmp_path / "t1" / "r1"
    doc_id = "d1"
    docs = [{"doc_id": doc_id, "title": "Spec", "content": "The system MUST authenticate all requests."}]
    n = merge_documents_into_assertion_index(
        scope_root=scope,
        tenant_id="tenant_a",
        repo_id="r1",
        documents=docs,
        max_assertions_per_batch=8,
    )
    assert n >= 1
    assert assertion_index_sqlite_path(scope_root=scope).is_file()

    c = CanonicalConstraint(
        subject="svc",
        predicate="required",
        object=None,
        polarity=1,
        scope="r1",
        kind="hard",
        summary="intent constraint",
    )
    em = EvidenceMapping(evidence_doc_ids=(doc_id,), resolved_provenance_pointers=())
    scores = {c.assertion_id: 2.0}

    idx_c, idx_e, idx_s = load_assertions_for_doc_ids(
        scope_root=scope,
        tenant_id="tenant_a",
        repo_id="r1",
        doc_ids={doc_id},
        limit=32,
        provenance_map={},
    )
    assert idx_c
    merged_c, merged_e, merged_s = merge_indexed_assertions_into_snapshot_state(
        canonical_constraints=(c,),
        evidence_by_assertion={c.assertion_id: em},
        base_evidence_scores=scores,
        indexed_constraints=idx_c,
        indexed_evidence=idx_e,
        indexed_scores=idx_s,
    )
    snap = _finalize_knowledge_snapshot_conflicts(
        canonical_constraints=merged_c,
        evidence_by_assertion=merged_e,
        base_evidence_scores=merged_s,
        documents=docs,
        knowledge_evidence_weighting=None,
        knowledge_unresolved_conflict_policy="warn_and_continue",
        compile_now_ms=0,
        mediation_report_out=None,
    )
    assert len(snap.canonical_constraints) >= 2


def test_assertion_index_schema_v2_and_meta_sidecar(tmp_path: Path) -> None:
    scope = tmp_path / "t1" / "r1"
    doc_id = "d1"
    docs = [
        {
            "doc_id": doc_id,
            "title": "Spec",
            "content": "The system MUST authenticate all requests.",
            "metadata": {"ingest_source_kind": "docs"},
        }
    ]
    merge_documents_into_assertion_index(
        scope_root=scope,
        tenant_id="tenant_a",
        repo_id="r1",
        documents=docs,
        max_assertions_per_batch=8,
    )
    db_path = assertion_index_sqlite_path(scope_root=scope)
    with sqlite3.connect(str(db_path)) as conn:
        ver = conn.execute("SELECT value FROM akc_assertion_index_meta WHERE key='schema_version'").fetchone()
        assert ver is not None and int(ver[0]) == ASSERTION_INDEX_SCHEMA_VERSION
        row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='akc_assertion_meta'").fetchone()
        assert row


def test_merge_more_assertions_with_ingest_pattern_preset(tmp_path: Path) -> None:
    """Extended doc patterns (ingest default) index more rows than legacy-only under the same cap."""
    narrow_root = tmp_path / "narrow" / "t" / "r"
    wide_root = tmp_path / "wide" / "t" / "r"
    corpus = [
        {
            "doc_id": "d1",
            "title": "",
            "content": (
                "4.2.1 The component SHOULD NOT log credentials.\n"
                "| Step | Requirement |\n"
                "| 1 | The server MUST verify tokens |\n"
            ),
            "metadata": {"ingest_source_kind": "docs"},
        }
    ]
    n_narrow = merge_documents_into_assertion_index(
        scope_root=narrow_root,
        tenant_id="tenant_a",
        repo_id="r1",
        documents=corpus,
        max_assertions_per_batch=32,
        pattern_options=DocDerivedPatternOptions(),
    )
    n_wide = merge_documents_into_assertion_index(
        scope_root=wide_root,
        tenant_id="tenant_a",
        repo_id="r1",
        documents=corpus,
        max_assertions_per_batch=32,
    )
    assert n_wide >= n_narrow
    assert n_wide >= 1
