from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from typing import Any

from akc.ir.provenance import ProvenanceKind, ProvenancePointer

ProvenancePointerJson = dict[str, Any]


def _pointer_for_retrieved_document(
    *,
    tenant_id: str,
    doc_id: str,
    content: str,
    title: Any,
    score_raw: Any,
    md: dict[str, Any],
) -> ProvenancePointer:
    """Map ingest/retrieve metadata into a tenant-scoped provenance pointer (A3).

    ``source_id`` stays equal to ``doc_id`` so evidence_doc_ids and pointers stay joinable.
    """

    pointer_metadata: dict[str, Any] = {"doc_id": doc_id}

    if isinstance(title, str) and title.strip():
        pointer_metadata["title"] = title.strip()
    if isinstance(score_raw, (int, float)):
        pointer_metadata["score"] = float(score_raw)
    if "chunk_index" in md and isinstance(md.get("chunk_index"), int):
        pointer_metadata["chunk_index"] = int(md["chunk_index"])

    source_type = md.get("source_type")
    source = md.get("source")
    if isinstance(source_type, str) and source_type.strip() and isinstance(source, str) and source.strip():
        pointer_metadata["source_type"] = source_type.strip()
        pointer_metadata["source"] = source.strip()

    cid = md.get("connector_id") or md.get("source_type")
    if isinstance(cid, str) and cid.strip():
        pointer_metadata["connector_id"] = cid.strip()

    source_uri = None
    for key in ("url", "path", "source"):
        v = md.get(key)
        if isinstance(v, str) and v.strip():
            source_uri = v.strip()
            break
    if source_uri is not None:
        pointer_metadata["source_uri"] = source_uri

    raw_idx = md.get("indexed_at_ms")
    if isinstance(raw_idx, (int, float)) and not isinstance(raw_idx, bool):
        pointer_metadata["indexed_at_ms"] = int(raw_idx)

    thread_raw = md.get("thread_id")
    if isinstance(thread_raw, str) and thread_raw.strip():
        pointer_metadata["message_thread_id"] = thread_raw.strip()
    channel_raw = md.get("channel")
    if isinstance(channel_raw, str) and channel_raw.strip():
        pointer_metadata["slack_channel_id"] = channel_raw.strip()

    op_raw = md.get("operation_id") or md.get("openapi_operation_id")
    op_id_str = str(op_raw).strip() if isinstance(op_raw, str) else ""
    if op_id_str:
        pointer_metadata["openapi_operation_id"] = op_id_str
    op_path = md.get("openapi_path")
    if isinstance(op_path, str) and op_path.strip():
        pointer_metadata["openapi_path"] = op_path.strip()
    op_meth = md.get("openapi_method")
    if isinstance(op_meth, str) and op_meth.strip():
        pointer_metadata["openapi_method"] = op_meth.strip()

    kind: ProvenanceKind = "doc_chunk"
    if op_id_str:
        kind = "openapi_operation"
    elif isinstance(thread_raw, str) and thread_raw.strip():
        kind = "message"

    locator_val = md.get("path") or md.get("url") or md.get("source")
    locator = str(locator_val).strip() if locator_val is not None else None

    sha256 = hashlib.sha256(content.encode("utf-8")).hexdigest()

    return ProvenancePointer(
        tenant_id=tenant_id,
        kind=kind,
        source_id=doc_id,
        locator=locator if locator else None,
        sha256=sha256,
        metadata=pointer_metadata,
    )


def build_doc_id_to_provenance_map(
    *,
    tenant_id: str,
    documents: Sequence[Any],
) -> dict[str, ProvenancePointerJson]:
    """Build a `doc_id`-keyed provenance mapping.

    This is intended for claim-level evidence where downstream structured
    outputs reference `doc_id` directly (instead of relying on parallel
    arrays that are hard to reconcile).

    Tenant isolation note:
    - All returned provenance pointers are constructed with the explicit
      `tenant_id` passed in by the compiler controller.
    - Pointer `source_id` is set to the `doc_id` so it can be used as the
      canonical evidence document identifier.
    """

    if not isinstance(tenant_id, str) or not tenant_id.strip():
        raise ValueError("tenant_id must be a non-empty string")

    out: dict[str, ProvenancePointerJson] = {}

    for d in documents:
        if not isinstance(d, Mapping):
            continue

        doc_id_raw = d.get("doc_id")
        doc_id = str(doc_id_raw).strip() if isinstance(doc_id_raw, str) else None
        if not doc_id:
            continue

        content_raw = d.get("content")
        if not (isinstance(content_raw, str) and content_raw.strip()):
            continue

        metadata_raw = d.get("metadata")
        md: dict[str, Any] = metadata_raw if isinstance(metadata_raw, dict) else {}

        pointer = _pointer_for_retrieved_document(
            tenant_id=tenant_id.strip(),
            doc_id=doc_id,
            content=content_raw,
            title=d.get("title"),
            score_raw=d.get("score"),
            md=md,
        )
        out[doc_id] = pointer.to_json_obj()

    return out


def build_retrieval_documents_item_ids_and_provenance(
    *,
    tenant_id: str,
    documents: Sequence[Any],
) -> tuple[list[str], list[dict[str, Any]]]:
    """Best-effort mapping from retrieved documents to provenance pointers.

    Tenant isolation note:
    - All returned provenance pointers are constructed with the explicit `tenant_id`
      passed in by the compiler controller.
    """

    if not isinstance(tenant_id, str) or not tenant_id.strip():
        raise ValueError("tenant_id must be a non-empty string")

    retrieval_item_ids: list[str] = []
    retrieval_provenance: list[dict[str, Any]] = []

    for d in documents:
        if not isinstance(d, Mapping):
            continue

        doc_id = d.get("doc_id")
        content = d.get("content")
        metadata_raw = d.get("metadata")
        title = d.get("title")
        score_raw = d.get("score")

        if isinstance(doc_id, str) and doc_id.strip():
            retrieval_item_ids.append(doc_id.strip())

        # Best-effort provenance pointers for living drift detection.
        if not (isinstance(doc_id, str) and doc_id.strip()):
            continue
        if not (isinstance(content, str) and content.strip()):
            continue

        md = metadata_raw if isinstance(metadata_raw, dict) else {}
        pointer = _pointer_for_retrieved_document(
            tenant_id=tenant_id.strip(),
            doc_id=doc_id.strip(),
            content=content,
            title=title,
            score_raw=score_raw,
            md=md,
        )
        retrieval_provenance.append(pointer.to_json_obj())

    return retrieval_item_ids, retrieval_provenance
