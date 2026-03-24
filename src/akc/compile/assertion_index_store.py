"""A4: tenant/repo-scoped SQLite assertion index under `.akc/knowledge/assertions.sqlite`.

Ingest merges doc-derived soft assertions from chunked documents; compile retrieve merges
matching rows (by retrieved `doc_id`) before conflict finalization.
"""

from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from akc.compile.controller_config import DocDerivedPatternOptions
from akc.compile.knowledge_extractor import (
    DOC_DERIVED_SOFT_EXTRACTOR_VERSION,
    extract_doc_derived_soft_assertions_from_documents,
)
from akc.ir.provenance import ProvenancePointer
from akc.knowledge.models import CanonicalConstraint, EvidenceMapping
from akc.memory.models import normalize_repo_id, require_non_empty

ASSERTION_INDEX_SCHEMA_VERSION = 2

# Ingest defaults to broader patterns under the same cap; compile keeps flags off unless configured.
DEFAULT_INGEST_DOC_DERIVED_PATTERNS = DocDerivedPatternOptions(
    rfc2119_bcp14=True,
    numbered_requirements=True,
    table_normative_rows=True,
)


def assertion_index_sqlite_path(*, scope_root: str | Path) -> Path:
    """Path to the assertion index DB for a tenant/repo scope (same layout as snapshot.json)."""

    return Path(scope_root).expanduser().resolve() / ".akc" / "knowledge" / "assertions.sqlite"


def _connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _init_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS akc_assertions (
          tenant_id TEXT NOT NULL,
          repo_id TEXT NOT NULL,
          assertion_id TEXT NOT NULL,
          constraint_json TEXT NOT NULL,
          evidence_score REAL NOT NULL,
          updated_at_ms INTEGER NOT NULL,
          PRIMARY KEY (tenant_id, repo_id, assertion_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS akc_assertion_docs (
          tenant_id TEXT NOT NULL,
          repo_id TEXT NOT NULL,
          assertion_id TEXT NOT NULL,
          doc_id TEXT NOT NULL,
          PRIMARY KEY (tenant_id, repo_id, assertion_id, doc_id)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS akc_assertion_docs_by_doc
        ON akc_assertion_docs(tenant_id, repo_id, doc_id)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS akc_assertion_index_meta (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS akc_assertion_meta (
          tenant_id TEXT NOT NULL,
          repo_id TEXT NOT NULL,
          assertion_id TEXT NOT NULL,
          source_kind TEXT,
          extractor_version TEXT,
          first_seen_ms INTEGER,
          supersedes_doc_id TEXT,
          PRIMARY KEY (tenant_id, repo_id, assertion_id)
        )
        """
    )


def _ensure_schema(conn: sqlite3.Connection) -> None:
    """Create tables and migrate assertion index schema on read/write."""

    _init_schema(conn)
    row = conn.execute("SELECT value FROM akc_assertion_index_meta WHERE key='schema_version'").fetchone()
    ver = int(row[0]) if row else 0
    if ver >= ASSERTION_INDEX_SCHEMA_VERSION:
        return
    # v1 -> v2: add akc_assertion_meta (CREATE IF NOT EXISTS above); bump version.
    conn.execute(
        "INSERT OR REPLACE INTO akc_assertion_index_meta(key, value) VALUES(?, ?)",
        ("schema_version", str(int(ASSERTION_INDEX_SCHEMA_VERSION))),
    )


def _meta_by_doc_id(documents: Sequence[Any]) -> dict[str, Mapping[str, Any]]:
    out: dict[str, Mapping[str, Any]] = {}
    for doc in documents:
        if isinstance(doc, Mapping):
            did = doc.get("doc_id") or doc.get("id")
            if not isinstance(did, str) or not did.strip():
                continue
            meta = doc.get("metadata")
            if isinstance(meta, Mapping):
                out[did.strip()] = meta
            continue
        did = getattr(doc, "id", None)
        if not isinstance(did, str) or not did.strip():
            continue
        meta = getattr(doc, "metadata", None) or {}
        if isinstance(meta, Mapping):
            out[did.strip()] = meta
    return out


def _infer_source_kind(meta_by_doc: Mapping[str, Mapping[str, Any]], doc_ids: tuple[str, ...]) -> str | None:
    for did in doc_ids:
        m = meta_by_doc.get(did)
        if m is None:
            continue
        sk = m.get("ingest_source_kind")
        if isinstance(sk, str) and sk.strip():
            return sk.strip().lower()
        st = m.get("source_type")
        if isinstance(st, str) and st.strip():
            return st.strip().lower()
    return None


def _infer_supersedes_doc_id(meta_by_doc: Mapping[str, Mapping[str, Any]], doc_ids: tuple[str, ...]) -> str | None:
    for did in doc_ids:
        m = meta_by_doc.get(did)
        if m is None:
            continue
        for key in ("supersedes_doc_id", "replaces_doc_id"):
            v = m.get(key)
            if isinstance(v, str) and v.strip():
                return v.strip()
    return None


def documents_as_retrieval_mappings(documents: Sequence[Any]) -> list[dict[str, Any]]:
    """Map ingest `Document` objects (or dict-like) into knowledge-extractor document shape."""

    out: list[dict[str, Any]] = []
    for doc in documents:
        if isinstance(doc, Mapping):
            did = doc.get("doc_id") or doc.get("id")
            if not isinstance(did, str) or not did.strip():
                continue
            content = str(doc.get("content") or "")
            title = str(doc.get("title") or "")
            row: dict[str, Any] = {"doc_id": did.strip(), "content": content, "title": title}
            meta_m = doc.get("metadata")
            if isinstance(meta_m, Mapping):
                row["metadata"] = dict(meta_m)
            out.append(row)
            continue
        doc_id = getattr(doc, "id", None)
        if not isinstance(doc_id, str) or not doc_id.strip():
            continue
        content = str(getattr(doc, "content", "") or "")
        meta = getattr(doc, "metadata", None) or {}
        title = ""
        if isinstance(meta, Mapping):
            t = meta.get("title") or meta.get("path") or meta.get("source")
            if isinstance(t, str):
                title = t
        row2: dict[str, Any] = {"doc_id": doc_id.strip(), "content": content, "title": title}
        if isinstance(meta, Mapping):
            row2["metadata"] = dict(meta)
        out.append(row2)
    return out


def merge_documents_into_assertion_index(
    *,
    scope_root: str | Path,
    tenant_id: str,
    repo_id: str,
    documents: Sequence[Any],
    max_assertions_per_batch: int = 256,
    pattern_options: DocDerivedPatternOptions | None = None,
) -> int:
    """Extract doc-derived assertions from `documents` and upsert into the index.

    Returns number of assertion rows touched (upserts).
    """

    require_non_empty(tenant_id, name="tenant_id")
    repo = normalize_repo_id(repo_id)
    maps = documents_as_retrieval_mappings(documents)
    if not maps:
        return 0
    cap = max(0, int(max_assertions_per_batch))
    po = pattern_options if pattern_options is not None else DEFAULT_INGEST_DOC_DERIVED_PATTERNS
    rows = extract_doc_derived_soft_assertions_from_documents(
        repo_id=repo,
        documents=maps,
        max_assertions=cap,
        skip_assertion_ids=None,
        patterns=po,
    )
    if not rows:
        return 0

    path = assertion_index_sqlite_path(scope_root=scope_root)
    now_ms = int(time.time() * 1000)
    touched = 0
    meta_by_doc = _meta_by_doc_id(documents)
    with _connect(path) as conn:
        _ensure_schema(conn)
        for canonical, doc_ids, score in rows:
            aid = canonical.assertion_id
            cj = json.dumps(canonical.to_json_obj(), sort_keys=True, ensure_ascii=False)
            conn.execute(
                """
                INSERT INTO akc_assertions(
                  tenant_id, repo_id, assertion_id, constraint_json, evidence_score, updated_at_ms
                ) VALUES(?,?,?,?,?,?)
                ON CONFLICT(tenant_id, repo_id, assertion_id) DO UPDATE SET
                  constraint_json=excluded.constraint_json,
                  evidence_score=MAX(akc_assertions.evidence_score, excluded.evidence_score),
                  updated_at_ms=excluded.updated_at_ms
                """,
                (tenant_id, repo, aid, cj, float(score), now_ms),
            )
            for did in doc_ids:
                if not isinstance(did, str) or not did.strip():
                    continue
                conn.execute(
                    """
                    INSERT OR IGNORE INTO akc_assertion_docs(
                      tenant_id, repo_id, assertion_id, doc_id
                    ) VALUES(?,?,?,?)
                    """,
                    (tenant_id, repo, aid, did.strip()),
                )
            sk = _infer_source_kind(meta_by_doc, doc_ids)
            sup = _infer_supersedes_doc_id(meta_by_doc, doc_ids)
            conn.execute(
                """
                INSERT INTO akc_assertion_meta(
                  tenant_id, repo_id, assertion_id, source_kind, extractor_version, first_seen_ms, supersedes_doc_id
                ) VALUES(?,?,?,?,?,?,?)
                ON CONFLICT(tenant_id, repo_id, assertion_id) DO UPDATE SET
                  source_kind=COALESCE(excluded.source_kind, akc_assertion_meta.source_kind),
                  extractor_version=excluded.extractor_version,
                  first_seen_ms=CASE
                    WHEN akc_assertion_meta.first_seen_ms IS NULL THEN excluded.first_seen_ms
                    WHEN excluded.first_seen_ms < akc_assertion_meta.first_seen_ms THEN excluded.first_seen_ms
                    ELSE akc_assertion_meta.first_seen_ms
                  END,
                  supersedes_doc_id=COALESCE(excluded.supersedes_doc_id, akc_assertion_meta.supersedes_doc_id)
                """,
                (
                    tenant_id,
                    repo,
                    aid,
                    sk,
                    DOC_DERIVED_SOFT_EXTRACTOR_VERSION,
                    now_ms,
                    sup,
                ),
            )
            touched += 1
        conn.commit()
    return touched


def load_assertions_for_doc_ids(
    *,
    scope_root: str | Path,
    tenant_id: str,
    repo_id: str,
    doc_ids: set[str],
    limit: int = 256,
    provenance_map: Mapping[str, ProvenancePointer] | None = None,
) -> tuple[list[CanonicalConstraint], dict[str, EvidenceMapping], dict[str, float]]:
    """Load assertions that reference any of `doc_ids`, capped by `limit`.

    Returns parallel structures suitable for merging before `_finalize_knowledge_snapshot_conflicts`.
    """

    require_non_empty(tenant_id, name="tenant_id")
    repo = normalize_repo_id(repo_id)
    path = assertion_index_sqlite_path(scope_root=scope_root)
    if not path.is_file() or not doc_ids:
        return [], {}, {}
    clean_ids = {str(x).strip() for x in doc_ids if isinstance(x, str) and str(x).strip()}
    if not clean_ids:
        return [], {}, {}

    prov = provenance_map or {}
    lim = max(1, int(limit))

    with _connect(path) as conn:
        _ensure_schema(conn)
        found: list[tuple[str, str, float]] = []
        for did in sorted(clean_ids):
            cur = conn.execute(
                """
                SELECT a.assertion_id, a.constraint_json, a.evidence_score
                FROM akc_assertions a
                JOIN akc_assertion_docs d
                  ON d.tenant_id = a.tenant_id
                 AND d.repo_id = a.repo_id
                 AND d.assertion_id = a.assertion_id
                WHERE a.tenant_id = ? AND a.repo_id = ? AND d.doc_id = ?
                """,
                (tenant_id, repo, did),
            )
            for aid, cj_raw, sc in cur.fetchall():
                found.append((str(aid), str(cj_raw), float(sc)))

        # Dedupe assertion_id keeping max score
        by_aid: dict[str, tuple[str, float]] = {}
        for aid, cj_raw, sc in found:
            prev = by_aid.get(aid)
            if prev is None or sc > prev[1]:
                by_aid[aid] = (cj_raw, sc)

        picked = sorted(by_aid.items(), key=lambda x: (-x[1][1], x[0]))[:lim]

        constraints: list[CanonicalConstraint] = []
        evidence_by_assertion: dict[str, EvidenceMapping] = {}
        scores: dict[str, float] = {}

        for aid, (cj_raw, sc) in picked:
            try:
                c_obj = json.loads(cj_raw)
            except Exception:
                continue
            if not isinstance(c_obj, dict):
                continue
            try:
                c = CanonicalConstraint.from_json_obj(c_obj)
            except Exception:
                continue
            if c.assertion_id != aid:
                continue
            cur2 = conn.execute(
                """
                SELECT doc_id FROM akc_assertion_docs
                WHERE tenant_id = ? AND repo_id = ? AND assertion_id = ?
                ORDER BY doc_id
                """,
                (tenant_id, repo, aid),
            )
            eids = tuple(str(r[0]).strip() for r in cur2.fetchall() if str(r[0]).strip())
            ptrs = tuple(prov[d] for d in eids if d in prov)
            constraints.append(c)
            evidence_by_assertion[aid] = EvidenceMapping(evidence_doc_ids=eids, resolved_provenance_pointers=ptrs)
            scores[aid] = float(sc)

    return constraints, evidence_by_assertion, scores


def merge_indexed_assertions_into_snapshot_state(
    *,
    canonical_constraints: tuple[CanonicalConstraint, ...],
    evidence_by_assertion: dict[str, EvidenceMapping],
    base_evidence_scores: dict[str, float],
    indexed_constraints: Sequence[CanonicalConstraint],
    indexed_evidence: Mapping[str, EvidenceMapping],
    indexed_scores: Mapping[str, float],
) -> tuple[tuple[CanonicalConstraint, ...], dict[str, EvidenceMapping], dict[str, float]]:
    """Union compile-time extraction with A4 index rows (deterministic ordering)."""

    c_by_id: dict[str, CanonicalConstraint] = {c.assertion_id: c for c in canonical_constraints}
    ev = dict(evidence_by_assertion)
    sc = {k: float(v) for k, v in base_evidence_scores.items()}

    def _merge_doc_ids(a: tuple[str, ...], b: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(sorted(set(a) | set(b)))

    for c in indexed_constraints:
        aid = c.assertion_id
        idx_em = indexed_evidence.get(aid)
        if idx_em is None:
            continue
        if aid not in c_by_id:
            c_by_id[aid] = c
            ev[aid] = idx_em
            sc[aid] = float(indexed_scores.get(aid, 0.0))
            continue
        cur = ev.get(aid)
        if cur is None:
            ev[aid] = idx_em
        else:
            merged_ids = _merge_doc_ids(cur.evidence_doc_ids, idx_em.evidence_doc_ids)
            # Provenance pointers: keep union by doc_id (re-resolve below if needed)
            ptr_by_doc: dict[str, ProvenancePointer] = {}
            for p in cur.resolved_provenance_pointers + idx_em.resolved_provenance_pointers:
                ptr_by_doc[p.source_id] = p
            ev[aid] = EvidenceMapping(
                evidence_doc_ids=merged_ids,
                resolved_provenance_pointers=tuple(ptr_by_doc[d] for d in merged_ids if d in ptr_by_doc),
            )
        sc[aid] = max(float(sc.get(aid, 0.0)), float(indexed_scores.get(aid, 0.0)))

    ordered_ids = sorted(c_by_id.keys())
    new_constraints = tuple(c_by_id[i] for i in ordered_ids)
    new_ev = {i: ev[i] for i in ordered_ids if i in ev}
    new_sc = {i: sc[i] for i in ordered_ids if i in sc}
    return new_constraints, new_ev, new_sc
