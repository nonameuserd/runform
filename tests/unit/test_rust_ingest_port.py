from __future__ import annotations

from typing import Any

import pytest

from akc.compile.rust_bridge import IngestResult, RustExecConfig
from akc.ingest.rust_port import ingest_docs_via_rust


def test_ingest_docs_via_rust_converts_records_to_documents(monkeypatch: pytest.MonkeyPatch) -> None:
    tenant_id = "t1"
    input_paths = ["/abs/path/doc1.md"]
    rust_cfg = RustExecConfig(mode="cli")

    fake_records: list[dict[str, Any]] = [
        {
            "tenant_id": tenant_id,
            "source_id": "/abs/path/doc1.md",
            "chunk_id": "chunk-1",
            "content": "Hello world.\n\n",
            "metadata": {"path": "/abs/path/doc1.md", "chunk_index": 0},
            "fingerprint": "fp-1",
        }
    ]

    import akc.ingest.rust_port as rust_port_mod

    def _fake_run_ingest_with_rust(*args: Any, **kwargs: Any) -> IngestResult:
        return IngestResult(ok=True, records=fake_records, error=None)

    monkeypatch.setattr(rust_port_mod, "run_ingest_with_rust", _fake_run_ingest_with_rust)

    docs = ingest_docs_via_rust(
        tenant_id=tenant_id,
        input_paths=input_paths,
        rust_cfg=rust_cfg,
        max_chunk_chars=100,
    )

    assert len(docs) == 1
    d = docs[0]
    assert d.id == "chunk-1"
    assert d.content.startswith("Hello world.")
    assert d.metadata["tenant_id"] == tenant_id
    assert d.metadata["source"] == "/abs/path/doc1.md"
    assert d.metadata["source_type"] == "docs"
    assert d.metadata["path"] == "/abs/path/doc1.md"
    assert d.metadata["chunk_index"] == 0

