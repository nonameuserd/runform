from __future__ import annotations

from pathlib import Path

import pytest

from akc import cli


def test_cli_ingest_codebase_offline_embed_and_query(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "src").mkdir()
    (repo / "src" / "app.py").write_text(
        "def add(a: int, b: int) -> int:\n"
        "    return a + b\n"
        "\n"
        "class Greeter:\n"
        "    def hello(self) -> str:\n"
        "        return 'hello'\n",
        encoding="utf-8",
    )

    argv = [
        "ingest",
        "--tenant-id",
        "tenant-1",
        "--connector",
        "codebase",
        "--input",
        str(repo),
        "--embedder",
        "hash",
        "--index-backend",
        "memory",
        "--no-state",
        "--query",
        "class Greeter hello",
        "-k",
        "2",
    ]

    with pytest.raises(SystemExit) as e:
        cli.main(argv)
    assert e.value.code == 0

    out = capsys.readouterr().out
    assert "Ingest complete." in out
    assert "Top" in out and "results for query" in out
