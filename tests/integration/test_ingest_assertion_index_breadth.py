"""Phase 1: ingest-time assertion index breadth and tenant isolation (A4+)."""

from __future__ import annotations

from pathlib import Path

from akc.compile.assertion_index_store import (
    assertion_index_sqlite_path,
    load_assertions_for_doc_ids,
    merge_documents_into_assertion_index,
)
from akc.memory.models import normalize_repo_id


def test_mixed_chunk_corpus_indexes_more_with_default_ingest_patterns(tmp_path: Path) -> None:
    """Same max_assertions cap: default ingest patterns yield >= legacy-only pattern rows."""
    legacy_root = tmp_path / "legacy" / "tenant_a" / normalize_repo_id("repo1")
    default_root = tmp_path / "default" / "tenant_a" / normalize_repo_id("repo1")
    corpus = [
        {
            "doc_id": "chunk-a",
            "title": "API",
            "content": (
                "3.1 Servers MUST use TLS.\n"
                "4.2.1 Clients SHOULD NOT send secrets in URLs.\n"
                "| Phase | Rule |\n"
                "| 1 | The gateway MUST validate JWTs |\n"
            ),
            "metadata": {"ingest_source_kind": "docs", "indexed_at_ms": 1_700_000_000_000},
        },
        {
            "doc_id": "chunk-b",
            "title": "Ops",
            "content": "SHOULD rotate keys monthly.",
            "metadata": {"ingest_source_kind": "docs"},
        },
    ]

    from akc.compile.controller_config import DocDerivedPatternOptions

    n_legacy = merge_documents_into_assertion_index(
        scope_root=legacy_root,
        tenant_id="tenant_a",
        repo_id="repo1",
        documents=corpus,
        max_assertions_per_batch=64,
        pattern_options=DocDerivedPatternOptions(),
    )
    n_default = merge_documents_into_assertion_index(
        scope_root=default_root,
        tenant_id="tenant_a",
        repo_id="repo1",
        documents=corpus,
        max_assertions_per_batch=64,
    )
    assert n_default >= n_legacy

    c_default, _, _ = load_assertions_for_doc_ids(
        scope_root=default_root,
        tenant_id="tenant_a",
        repo_id="repo1",
        doc_ids={"chunk-a", "chunk-b"},
        limit=128,
        provenance_map={},
    )
    assert c_default


def test_assertion_index_does_not_leak_across_tenants(tmp_path: Path) -> None:
    scope_a = tmp_path / "t_a" / "r1"
    scope_b = tmp_path / "t_b" / "r1"
    doc_id = "shared-id"
    docs = [{"doc_id": doc_id, "title": "", "content": "The system MUST deny by default."}]
    merge_documents_into_assertion_index(
        scope_root=scope_a,
        tenant_id="tenant_a",
        repo_id="r1",
        documents=docs,
        max_assertions_per_batch=16,
    )
    c_b, _, _ = load_assertions_for_doc_ids(
        scope_root=scope_b,
        tenant_id="tenant_b",
        repo_id="r1",
        doc_ids={doc_id},
        limit=32,
        provenance_map={},
    )
    assert c_b == []
    assert assertion_index_sqlite_path(scope_root=scope_a).is_file()
