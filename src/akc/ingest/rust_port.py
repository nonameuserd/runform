from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from akc.compile.rust_bridge import IngestRequest, RustExecConfig, run_ingest_with_rust
from akc.compile.interfaces import TenantRepoScope
from akc.ingest.chunking import normalize_text
from akc.ingest.models import Document


@dataclass(frozen=True, slots=True)
class RustIngestDocsPortConfig:
    """Configuration for optional Rust docs ingest."""

    enabled: bool = True
    rust_mode: str = "cli"  # `RustExecConfig.mode` (cli|pyo3)
    max_chunk_chars: int | None = None


def _records_to_documents(*, records: Iterable[dict[str, Any]], tenant_id: str) -> list[Document]:
    docs: list[Document] = []
    for rec in records:
        # Expected record keys (from `akc_protocol::ChunkRecord`):
        # tenant_id, source_id, chunk_id, content, metadata, fingerprint
        source_id = str(rec.get("source_id") or "")
        chunk_id = str(rec.get("chunk_id") or "")
        content = str(rec.get("content") or "")
        metadata_in = rec.get("metadata") or {}
        if not isinstance(metadata_in, dict):
            metadata_in = {}

        # Carry chunk index for observability/debugging (best-effort).
        chunk_index_val = metadata_in.get("chunk_index")
        chunk_index: int | None = None
        if isinstance(chunk_index_val, int):
            chunk_index = chunk_index_val
        elif isinstance(chunk_index_val, float) and chunk_index_val.is_integer():
            chunk_index = int(chunk_index_val)

        md_out: dict[str, Any] = {
            "tenant_id": tenant_id,
            "source": source_id,
            "source_type": "docs",
        }
        if "path" in metadata_in and isinstance(metadata_in["path"], str):
            md_out["path"] = metadata_in["path"]
        if chunk_index is not None:
            md_out["chunk_index"] = chunk_index

        # Rust content is already chunked; apply the same normalization used by
        # Python's `normalize_documents` so downstream hashing/indexing is consistent.
        content_norm = normalize_text(content)
        docs.append(Document(id=chunk_id, content=content_norm, metadata=md_out))
    return docs


def ingest_docs_via_rust(
    *,
    tenant_id: str,
    input_paths: list[str],
    rust_cfg: RustExecConfig,
    max_chunk_chars: int | None,
) -> list[Document]:
    """Ingest docs with `akc-ingest` and convert records into `Document`s.

    This is an *optional* correctness/throughput tradeoff path. Python's docs
    connector + chunker implement richer Markdown/HTML-to-text extraction and
    overlap-aware chunking; Rust currently performs deterministic raw text
    chunking from input paths.
    """

    # `TenantRepoScope` is used by the rust bridge, but ingest only requires
    # tenant scoping in the underlying protocol.
    scope = TenantRepoScope(tenant_id=tenant_id, repo_id="ingest")
    req = IngestRequest(
        docs=IngestRequest.Docs(
            input_paths=tuple(input_paths),
            max_chunk_chars=max_chunk_chars,
            source_root=None,
        )
    )
    res = run_ingest_with_rust(cfg=rust_cfg, scope=scope, request=req)
    if not res.ok or not res.records:
        raise RuntimeError(res.error or "akc-ingest failed")
    return _records_to_documents(records=res.records, tenant_id=tenant_id)

