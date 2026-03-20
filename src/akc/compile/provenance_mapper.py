from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from typing import Any

from akc.ir.provenance import ProvenancePointer


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

        md: dict[str, Any] = metadata_raw if isinstance(metadata_raw, dict) else {}
        source_type = md.get("source_type")
        source = md.get("source")
        if (
            isinstance(source_type, str)
            and source_type.strip()
            and isinstance(source, str)
            and source.strip()
        ):
            source_id = f"{source_type.strip()}::{source.strip()}"
        else:
            # Fallback: still attach provenance to something stable-ish.
            source_id = f"other::{doc_id.strip()}"

        locator_val = md.get("path") or md.get("url") or md.get("source")
        locator = str(locator_val).strip() if locator_val is not None else None

        pointer_metadata: dict[str, Any] = {}
        if isinstance(title, str) and title.strip():
            pointer_metadata["title"] = title.strip()
        if isinstance(score_raw, (int, float)):
            pointer_metadata["score"] = float(score_raw)
        pointer_metadata["doc_id"] = doc_id.strip()
        if "chunk_index" in md and isinstance(md.get("chunk_index"), int):
            pointer_metadata["chunk_index"] = int(md["chunk_index"])

        pointer = ProvenancePointer(
            tenant_id=tenant_id,
            kind="doc_chunk",
            source_id=source_id,
            locator=locator if isinstance(locator, str) and locator.strip() else None,
            sha256=hashlib.sha256(content.encode("utf-8")).hexdigest(),
            metadata=pointer_metadata if pointer_metadata else None,
        )
        retrieval_provenance.append(pointer.to_json_obj())

    return retrieval_item_ids, retrieval_provenance
