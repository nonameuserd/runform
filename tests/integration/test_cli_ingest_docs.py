from __future__ import annotations

from pathlib import Path

import pytest

from akc import cli


def test_cli_ingest_docs_offline_embed_and_query(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    docs_dir = tmp_path / "docs"
    docs_dir.mkdir()
    (docs_dir / "a.md").write_text("# Welcome\n\nThis project explains how to get started.\n", encoding="utf-8")
    (docs_dir / "b.md").write_text("# Auth\n\nUser login uses a token-based flow.\n", encoding="utf-8")

    argv = [
        "ingest",
        "--tenant-id",
        "tenant-1",
        "--connector",
        "docs",
        "--input",
        str(docs_dir),
        "--embedder",
        "hash",
        "--index-backend",
        "memory",
        "--no-state",
        "--query",
        "login token",
        "-k",
        "2",
    ]

    with pytest.raises(SystemExit) as e:
        cli.main(argv)
    assert e.value.code == 0

    out = capsys.readouterr().out
    assert "Ingest complete." in out
    assert "Top" in out and "results for query" in out
