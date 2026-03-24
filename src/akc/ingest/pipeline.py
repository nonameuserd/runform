"""Ingestion pipeline orchestration (Phase 1).

Wires: connector -> normalize -> chunk -> (optional embed) -> (optional index).

This module is intentionally dependency-light and production-oriented:
- Strict tenant isolation (tenant_id required everywhere)
- Incremental ingestion support via a small JSON state file
- Clear, typed results for observability and testing
"""

from __future__ import annotations

import json
import logging
import os
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from akc.compile.controller_config import DocDerivedPatternOptions
from akc.compile.rust_bridge import RustExecConfig
from akc.ingest.chunking import ChunkingConfig, chunk_documents, normalize_documents
from akc.ingest.connectors.base import Connector
from akc.ingest.connectors.docs import build_docs_connector
from akc.ingest.connectors.messaging.slack import build_slack_connector
from akc.ingest.connectors.openapi import build_openapi_connector
from akc.ingest.embedding import Embedder, embed_documents
from akc.ingest.exceptions import IngestionError
from akc.ingest.index import InMemoryVectorStore, PgVectorStore, SQLiteVectorStore, VectorStore
from akc.ingest.rust_port import ingest_docs_via_rust
from akc.memory.models import normalize_repo_id

logger = logging.getLogger(__name__)

ConnectorName = Literal["docs", "openapi", "slack"]
IndexBackend = Literal["memory", "sqlite", "pgvector"]


def _require_non_empty(value: str, *, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


@dataclass(frozen=True, slots=True)
class IngestStats:
    sources_seen: int
    sources_skipped: int
    documents_fetched: int
    documents_chunked: int
    documents_embedded: int
    documents_indexed: int
    elapsed_s: float


@dataclass(frozen=True, slots=True)
class IngestResult:
    connector: str
    tenant_id: str
    index_backend: str | None
    state_path: str | None
    stats: IngestStats


class IngestionStateStore:
    """Small JSON state file for incremental ingestion.

    Format (per tenant+connector+source_id) stores a fingerprint dict.
    The store is not meant to be a general DB; it's a pragmatic Phase 1 cache.
    """

    def __init__(self, path: Path) -> None:
        self._path = path

    @property
    def path(self) -> Path:
        return self._path

    def load(self) -> dict[str, Any]:
        try:
            raw = self._path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return {}
        except OSError as e:
            raise IngestionError(f"failed to read ingestion state: {self._path}") from e
        try:
            data = json.loads(raw)
        except Exception as e:  # pragma: no cover
            raise IngestionError(f"ingestion state is not valid JSON: {self._path}") from e
        if not isinstance(data, dict):
            raise IngestionError(f"ingestion state must be a JSON object: {self._path}")
        return data

    def save(self, state: Mapping[str, Any]) -> None:
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._path.with_suffix(self._path.suffix + ".tmp")
            tmp.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
            tmp.replace(self._path)
        except OSError as e:
            raise IngestionError(f"failed to write ingestion state: {self._path}") from e


def default_state_path(*, tenant_id: str, connector: str, base_dir: Path | None = None) -> Path:
    """Choose a deterministic per-tenant state file location."""

    _require_non_empty(tenant_id, name="tenant_id")
    _require_non_empty(connector, name="connector")
    base = base_dir or Path.cwd()
    # Keep it human-readable and avoid secrets.
    safe_tenant = tenant_id.replace(os.sep, "_").replace("..", "_")
    return base / ".akc" / "ingest" / safe_tenant / f"{connector}.state.json"


def _get_connector(
    connector: ConnectorName,
    *,
    tenant_id: str,
    input_value: str,
    connector_options: Mapping[str, str] | None = None,
) -> Connector:
    if connector == "docs":
        return build_docs_connector(tenant_id=tenant_id, root_path=input_value)
    if connector == "openapi":
        opts_o: dict[str, str] = dict(connector_options or {})
        emit_soft = opts_o.get("emit_soft_assertion_chunks", "false").lower() in {"1", "true", "yes"}
        return build_openapi_connector(
            tenant_id=tenant_id,
            spec=input_value,
            emit_soft_assertion_chunks=emit_soft,
        )
    if connector == "slack":
        opts: dict[str, str] = dict(connector_options or {})
        token = opts.get("token", "")
        oldest = opts.get("oldest")
        latest = opts.get("latest")
        history_limit = int(opts.get("history_limit", "200"))
        max_threads = int(opts.get("max_threads", "200"))
        max_answers = int(opts.get("max_answers", "3"))
        include_bot_answers = opts.get("include_bot_answers", "false").lower() in {
            "1",
            "true",
            "yes",
        }
        return build_slack_connector(
            tenant_id=tenant_id,
            channel_id=input_value,
            token=token,
            oldest=oldest,
            latest=latest,
            history_limit=history_limit,
            max_threads=max_threads,
            max_answers=max_answers,
            include_bot_answers=include_bot_answers,
        )
    raise ValueError(f"unknown connector: {connector}")


def build_vector_store(
    *,
    backend: IndexBackend,
    sqlite_path: str | None = None,
    pg_dsn: str | None = None,
    pg_dimension: int | None = None,
    pg_table: str = "akc_documents",
) -> VectorStore:
    """Build a VectorStore backend for ingestion."""

    if backend == "memory":
        return InMemoryVectorStore()
    if backend == "sqlite":
        _require_non_empty(sqlite_path or "", name="sqlite_path")
        return SQLiteVectorStore(path=str(sqlite_path))
    if backend == "pgvector":
        _require_non_empty(pg_dsn or "", name="pg_dsn")
        if pg_dimension is None:
            raise ValueError("pg_dimension is required for pgvector backend")
        return PgVectorStore(dsn=str(pg_dsn), dimension=int(pg_dimension), table=pg_table)
    raise ValueError(f"unknown index backend: {backend}")


def _fingerprint_source(connector: Connector, source_id: str) -> dict[str, Any]:
    """Compute a best-effort fingerprint for a source_id."""

    # Docs sources are local paths.
    if connector.source_type == "docs":
        try:
            p = Path(source_id).expanduser()
            st = p.stat()
        except OSError:
            # If we cannot stat it, force re-fetch and allow connector to raise a helpful error.
            return {"kind": "docs", "path": source_id, "stat_failed": True}
        return {
            "kind": "docs",
            "path": str(p),
            "mtime_ns": int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1e9))),
            "size": int(st.st_size),
        }

    # OpenAPI sources may be URL or local file.
    if connector.source_type == "openapi":
        if source_id.startswith("http://") or source_id.startswith("https://"):
            # No HEAD/ETag retrieval in Phase 1 (keep deps minimal). Use a time-based throttle:
            # if a user wants deterministic updates for URLs, they can disable incremental mode.
            return {"kind": "openapi_url", "url": source_id}
        try:
            p = Path(source_id).expanduser()
            st = p.stat()
        except OSError:
            return {"kind": "openapi_file", "path": source_id, "stat_failed": True}
        return {
            "kind": "openapi_file",
            "path": str(p),
            "mtime_ns": int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1e9))),
            "size": int(st.st_size),
        }

    return {"kind": connector.source_type, "source_id": source_id}


def _state_key(*, tenant_id: str, connector_name: str, source_id: str) -> str:
    # Avoid surprising collisions; keep it stable and readable.
    return f"{tenant_id}::{connector_name}::{source_id}"


def _should_skip(
    *,
    previous_state: Mapping[str, Any],
    key: str,
    fingerprint: Mapping[str, Any],
    allow_incremental: bool,
) -> bool:
    if not allow_incremental:
        return False
    prev = previous_state.get(key)
    if not isinstance(prev, dict):
        return False
    # Dict equality is stable for JSON-loaded objects.
    return dict(prev) == dict(fingerprint)


def run_ingest(
    *,
    connector_name: ConnectorName,
    tenant_id: str,
    input_value: str,
    connector_options: Mapping[str, str] | None = None,
    chunking: ChunkingConfig | None = None,
    disable_chunking: bool = False,
    embedder: Embedder | None = None,
    vector_store: VectorStore | None = None,
    index_backend: str | None = None,
    state_store: IngestionStateStore | None = None,
    incremental: bool = True,
    on_source_error: Literal["raise", "skip"] = "raise",
    use_rust_ingest_docs: bool = False,
    rust_ingest_min_bytes: int | None = None,
    rust_ingest_mode: Literal["cli", "pyo3"] = "cli",
    assertion_index_scope_root: str | Path | None = None,
    assertion_index_repo_id: str | None = None,
    assertion_index_max_per_batch: int = 256,
    assertion_index_pattern_options: DocDerivedPatternOptions | None = None,
) -> IngestResult:
    """Run ingestion end-to-end for one connector + input."""

    _require_non_empty(tenant_id, name="tenant_id")
    _require_non_empty(input_value, name="input")

    start = time.perf_counter()
    connector = _get_connector(
        connector_name,
        tenant_id=tenant_id,
        input_value=input_value,
        connector_options=connector_options,
    )

    if vector_store is not None and embedder is None:
        raise ValueError("embedder is required when vector_store is enabled")

    state: dict[str, Any] = state_store.load() if state_store is not None else {}
    new_state: dict[str, Any] = dict(state)

    sources_seen = 0
    sources_skipped = 0
    documents_fetched = 0
    documents_chunked_count = 0
    documents_embedded_count = 0
    documents_indexed = 0

    def _source_error(source_id: str, err: Exception) -> None:
        logger.warning("Skipping source due to error. source=%s error=%s", source_id, err)

    for source_id in connector.list_sources():
        sources_seen += 1
        fp = _fingerprint_source(connector, source_id)
        key = _state_key(tenant_id=tenant_id, connector_name=connector_name, source_id=source_id)
        if _should_skip(previous_state=state, key=key, fingerprint=fp, allow_incremental=incremental):
            sources_skipped += 1
            continue
        chunked: list[Any] | None = None
        rust_used = False
        if use_rust_ingest_docs and connector.source_type == "docs" and not disable_chunking:
            size_val = fp.get("size")
            size_ok = rust_ingest_min_bytes is None or (
                isinstance(size_val, int) and int(size_val) >= int(rust_ingest_min_bytes)
            )
            if size_ok:
                try:
                    rust_cfg = RustExecConfig(mode=rust_ingest_mode)
                    max_chunk_chars = (
                        chunking.chunk_size_chars if chunking is not None else ChunkingConfig().chunk_size_chars
                    )
                    chunked = ingest_docs_via_rust(
                        tenant_id=tenant_id,
                        input_paths=[source_id],
                        rust_cfg=rust_cfg,
                        max_chunk_chars=max_chunk_chars,
                    )
                    rust_used = True
                except Exception as e:
                    logger.warning(
                        "Rust docs ingest failed; falling back to Python. source=%s error=%s",
                        source_id,
                        e,
                    )

        if chunked is None:
            try:
                docs = list(connector.fetch(source_id))
            except Exception as e:
                if on_source_error == "skip":
                    _source_error(source_id, e)
                    continue
                raise

            documents_fetched += len(docs)

            # Normalize + optional chunk.
            normed = list(normalize_documents(docs))
            chunked = normed if disable_chunking else list(chunk_documents(normed, config=chunking))

        if rust_used:
            # Each Rust docs ingest request is invoked per `source_id`; treat
            # it as one fetched source.
            documents_fetched += 1

        documents_chunked_count += len(chunked)

        # Optional embed + index.
        if embedder is not None:
            embedded = list(embed_documents(embedder, chunked))
            documents_embedded_count += len(embedded)
        else:
            embedded = chunked

        if vector_store is not None:
            documents_indexed += vector_store.add(tenant_id=tenant_id, documents=embedded)

        if assertion_index_scope_root is not None:
            from akc.compile.assertion_index_store import merge_documents_into_assertion_index

            repo_for_idx = normalize_repo_id(str(assertion_index_repo_id or "default"))
            scope = Path(assertion_index_scope_root).expanduser() / tenant_id / repo_for_idx
            merge_documents_into_assertion_index(
                scope_root=scope,
                tenant_id=tenant_id,
                repo_id=repo_for_idx,
                documents=embedded,
                max_assertions_per_batch=int(assertion_index_max_per_batch),
                pattern_options=assertion_index_pattern_options,
            )

        # Mark state only after the source successfully completed all enabled steps.
        new_state[key] = dict(fp)

    if state_store is not None:
        state_store.save(new_state)

    elapsed = time.perf_counter() - start
    stats = IngestStats(
        sources_seen=sources_seen,
        sources_skipped=sources_skipped,
        documents_fetched=documents_fetched,
        documents_chunked=documents_chunked_count,
        documents_embedded=documents_embedded_count,
        documents_indexed=documents_indexed,
        elapsed_s=elapsed,
    )
    return IngestResult(
        connector=str(connector_name),
        tenant_id=tenant_id,
        index_backend=(None if vector_store is None else (index_backend or vector_store.__class__.__name__)),
        state_path=str(state_store.path) if state_store is not None else None,
        stats=stats,
    )
