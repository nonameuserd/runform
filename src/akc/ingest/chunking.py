"""Chunking and normalization utilities for ingestion.

Phase 1: dependency-light, character-based chunking with overlap.
"""

from __future__ import annotations

import unicodedata
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass

from akc.ingest.exceptions import ChunkingError
from akc.ingest.models import Document, stable_document_id


@dataclass(frozen=True, slots=True)
class ChunkingConfig:
    chunk_size_chars: int = 2000
    overlap_chars: int = 200
    # Natural boundary separators in priority order (largest -> smallest).
    separators: tuple[str, ...] = ("\n\n", "\n", ". ", " ", "")

    def __post_init__(self) -> None:
        if self.chunk_size_chars <= 0:
            raise ValueError("chunk_size_chars must be > 0")
        if self.overlap_chars < 0:
            raise ValueError("overlap_chars must be >= 0")
        if self.overlap_chars >= self.chunk_size_chars:
            raise ValueError("overlap_chars must be < chunk_size_chars")
        if not self.separators:
            raise ValueError("separators must not be empty")


def normalize_text(text: str) -> str:
    """Normalize text for consistent chunking and hashing.

    - Unicode normalization (NFC)
    - Normalize newlines to \\n
    - Trim trailing whitespace and collapse excessive blank lines
    """

    if not isinstance(text, str):
        raise TypeError("text must be a str")
    t = unicodedata.normalize("NFC", text)
    t = t.replace("\r\n", "\n").replace("\r", "\n")
    # Trim line trailing spaces to avoid noisy diffs/chunk boundaries.
    t = "\n".join(line.rstrip() for line in t.split("\n"))
    # Collapse excessive blank lines, but keep paragraph breaks.
    while "\n\n\n" in t:
        t = t.replace("\n\n\n", "\n\n")
    return t.strip()


def normalize_documents(documents: Iterable[Document]) -> Iterator[Document]:
    """Yield documents with normalized content."""

    for doc in documents:
        yield doc.with_updates(content=normalize_text(doc.content))


def chunk_documents(
    documents: Iterable[Document],
    *,
    config: ChunkingConfig | None = None,
) -> Iterator[Document]:
    """Chunk documents into tenant-scoped `Document` chunks.

    Output chunks preserve tenant and source metadata and add:
    - parent_id: original document id
    - chunk_index: 0..N-1
    - chunk_start: start char offset (in normalized text)
    - chunk_end: end char offset (in normalized text)
    """

    cfg = config or ChunkingConfig()
    for doc in documents:
        try:
            norm = normalize_text(doc.content)
            yield from _chunk_one(doc, norm, cfg)
        except ChunkingError:
            raise
        except Exception as e:  # pragma: no cover
            raise ChunkingError("chunking failed") from e


def _chunk_one(doc: Document, normalized_content: str, cfg: ChunkingConfig) -> Iterator[Document]:
    if not normalized_content:
        raise ChunkingError("document content is empty after normalization")

    pieces = _recursive_split(
        normalized_content,
        chunk_size=cfg.chunk_size_chars,
        separators=cfg.separators,
    )
    if not pieces:
        raise ChunkingError("chunker produced no chunks")

    source = doc.metadata.get("source")
    if not isinstance(source, str) or not source.strip():
        raise ChunkingError("document metadata.source must be a non-empty string")

    # Stitch overlap using character offsets in the normalized content.
    # We compute start/end by searching forward; this is deterministic given
    # that `pieces` are produced in order from the normalized content.
    cursor = 0
    chunk_index = 0
    for piece in pieces:
        if not piece.strip():
            continue
        start = normalized_content.find(piece, cursor)
        if start < 0:
            # Fallback: if not found (due to repeated segments), approximate.
            start = cursor
        end = start + len(piece)
        cursor = max(end, cursor)

        chunk_text = piece
        chunk_start = start
        if cfg.overlap_chars and chunk_index > 0:
            overlap_start = max(0, start - cfg.overlap_chars)
            prefix = normalized_content[overlap_start:start]
            # Avoid leading whitespace explosion when prefix ends mid-space/newline.
            chunk_text = (prefix + chunk_text).lstrip()
            chunk_start = overlap_start

        # Stable, tenant-scoped chunk IDs derived from the parent doc.
        new_id = stable_document_id(
            tenant_id=doc.tenant_id,
            source=source,
            logical_locator=doc.id,
            chunk_index=chunk_index,
        )
        yield doc.with_updates(
            new_id=new_id,
            content=chunk_text,
            metadata_updates={
                "parent_id": doc.id,
                "chunk_index": chunk_index,
                "chunk_start": chunk_start,
                "chunk_end": end,
            },
            embedding=None,  # embeddings must be recomputed per chunk
        )
        chunk_index += 1


def _recursive_split(text: str, *, chunk_size: int, separators: Sequence[str]) -> list[str]:
    """Recursively split `text` into chunks <= chunk_size using separators."""

    if len(text) <= chunk_size:
        return [text]

    # Pick the first separator that exists in the text (except "" which always works).
    sep = ""
    for s in separators:
        if s == "":
            sep = ""
            break
        if s in text:
            sep = s
            break

    if sep == "":
        # Hard split.
        return [text[i : i + chunk_size] for i in range(0, len(text), chunk_size)]

    parts = text.split(sep)
    # Put separators back (except after last part) so chunks remain readable.
    rebuilt: list[str] = []
    for idx, p in enumerate(parts):
        if not p:
            continue
        rebuilt.append(p if idx == len(parts) - 1 else p + sep)

    out: list[str] = []
    buf = ""
    for part in rebuilt:
        if not buf:
            buf = part
            continue
        if len(buf) + len(part) <= chunk_size:
            buf += part
            continue

        # Buffer too large to add next part; flush buffer (possibly recursively).
        if len(buf) > chunk_size:
            out.extend(_recursive_split(buf, chunk_size=chunk_size, separators=separators[1:]))
        else:
            out.append(buf)
        buf = part

    if buf:
        if len(buf) > chunk_size:
            out.extend(_recursive_split(buf, chunk_size=chunk_size, separators=separators[1:]))
        else:
            out.append(buf)

    # Final cleanup: trim and drop empty.
    cleaned = [c.strip() for c in out if c.strip()]
    return cleaned
