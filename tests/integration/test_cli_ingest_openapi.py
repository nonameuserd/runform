from __future__ import annotations

from pathlib import Path

import pytest

from akc import cli


def test_cli_ingest_openapi_offline_embed_and_query(
    capsys: pytest.CaptureFixture[str],
) -> None:
    spec_path = Path(__file__).resolve().parents[2] / "examples" / "openapi" / "petstore.json"
    assert spec_path.is_file(), f"missing OpenAPI fixture: {spec_path}"

    argv = [
        "ingest",
        "--tenant-id",
        "tenant-1",
        "--connector",
        "openapi",
        "--input",
        str(spec_path),
        "--embedder",
        "hash",
        "--index-backend",
        "memory",
        "--no-state",
        "--query",
        "create pet",
        "-k",
        "3",
    ]

    with pytest.raises(SystemExit) as e:
        cli.main(argv)
    assert e.value.code == 0

    out = capsys.readouterr().out
    assert "Ingest complete." in out
    assert "Top" in out and "results for query" in out
