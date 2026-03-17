from __future__ import annotations

from pathlib import Path

import pytest

from akc.ingest.connectors.base import iter_documents
from akc.ingest.connectors.docs import DocsConnector, DocsConnectorConfig
from akc.ingest.exceptions import ConnectorError


def test_docs_connector_lists_and_fetches_markdown_and_html(tmp_path: Path) -> None:
    md_path = tmp_path / "a.md"
    md_path.write_text(
        """---
title: Example
---

# Intro

Hello **world**. See [link](https://example.com).
""",
        encoding="utf-8",
    )
    html_path = tmp_path / "b.html"
    html_path.write_text(
        "<html><head><title>T</title><style>.x{}</style></head>"
        "<body><h1>Heading</h1><p>Hello <b>there</b></p><script>noop()</script></body></html>",
        encoding="utf-8",
    )

    conn = DocsConnector(tenant_id="tenant-1", config=DocsConnectorConfig(root_path=tmp_path))
    sources = set(conn.list_sources())
    assert str(md_path.resolve()) in sources
    assert str(html_path.resolve()) in sources

    md_docs = list(conn.fetch(str(md_path)))
    assert len(md_docs) == 1
    assert "Hello world." in md_docs[0].content
    assert md_docs[0].metadata["tenant_id"] == "tenant-1"
    assert md_docs[0].metadata["source_type"] == "docs"
    assert md_docs[0].metadata["path"] == str(md_path.resolve())

    html_docs = list(conn.fetch(str(html_path)))
    assert len(html_docs) == 1
    assert "Heading" in html_docs[0].content
    assert "Hello there" in html_docs[0].content
    assert "noop" not in html_docs[0].content


def test_docs_connector_rejects_outside_root_path(tmp_path: Path) -> None:
    other = tmp_path.parent / "outside.md"
    other.write_text("hi", encoding="utf-8")
    conn = DocsConnector(tenant_id="t", config=DocsConnectorConfig(root_path=tmp_path))
    with pytest.raises(ConnectorError, match=r"outside root_path"):
        list(conn.fetch(str(other)))


def test_docs_connector_exclude_globs_are_applied(tmp_path: Path) -> None:
    included = tmp_path / "keep.md"
    included.write_text("# Keep\n\nhi", encoding="utf-8")
    excluded_dir = tmp_path / "private"
    excluded_dir.mkdir()
    excluded = excluded_dir / "secret.md"
    excluded.write_text("# Secret\n\ndo not ingest", encoding="utf-8")

    conn = DocsConnector(
        tenant_id="t",
        config=DocsConnectorConfig(
            root_path=tmp_path,
            exclude_globs=("private/**",),
        ),
    )
    sources = set(conn.list_sources())
    assert str(included.resolve()) in sources
    assert str(excluded.resolve()) not in sources


def test_docs_connector_rejects_binary_files(tmp_path: Path) -> None:
    p = tmp_path / "binary.md"
    p.write_bytes(b"\x00\x01\x02not really markdown")
    conn = DocsConnector(tenant_id="t", config=DocsConnectorConfig(root_path=tmp_path))
    assert str(p.resolve()) in set(conn.list_sources())
    with pytest.raises(ConnectorError, match=r"appears to be binary"):
        list(conn.fetch(str(p)))


def test_docs_connector_enforces_max_bytes(tmp_path: Path) -> None:
    p = tmp_path / "big.md"
    p.write_text("a" * 50, encoding="utf-8")
    conn = DocsConnector(
        tenant_id="t",
        config=DocsConnectorConfig(root_path=tmp_path, max_bytes=10),
    )
    with pytest.raises(ConnectorError, match=r"exceeds max_bytes"):
        list(conn.fetch(str(p)))


def test_iter_documents_can_skip_bad_sources(tmp_path: Path) -> None:
    good = tmp_path / "good.md"
    good.write_text("# Good\n\nok", encoding="utf-8")
    binary = tmp_path / "binary.md"
    binary.write_bytes(b"\x00\x01nope")
    big = tmp_path / "big.md"
    big.write_text("a" * 50, encoding="utf-8")

    conn = DocsConnector(
        tenant_id="t",
        config=DocsConnectorConfig(root_path=tmp_path, max_bytes=10),
    )
    skipped: list[str] = []
    docs = list(
        iter_documents(
            conn,
            on_error="skip",
            on_skip=lambda source_id, _e: skipped.append(source_id),
        )
    )
    assert len(docs) == 1
    assert "ok" in docs[0].content
    assert any(str(binary.resolve()) == s or str(binary) == s for s in skipped)
    assert any(str(big.resolve()) == s or str(big) == s for s in skipped)
