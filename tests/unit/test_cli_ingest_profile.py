from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.cli import main


def test_cli_ingest_emerging_profile_emits_decisions_sidecar(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    docs_dir = tmp_path / "docs"
    docs_dir.mkdir(parents=True, exist_ok=True)
    (docs_dir / "a.md").write_text("# hello\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "ingest",
                "--tenant-id",
                "t1",
                "--developer-role-profile",
                "emerging",
                "--connector",
                "docs",
                "--input",
                str(docs_dir),
                "--no-index",
            ]
        )
    assert excinfo.value.code == 0

    decisions_path = tmp_path / ".akc" / "ingest" / "t1" / "docs.developer_profile_decisions.json"
    assert decisions_path.exists()
    payload = json.loads(decisions_path.read_text(encoding="utf-8"))
    assert payload["developer_role_profile"] == "emerging"
    assert isinstance(payload.get("fingerprint_sha256"), str)
