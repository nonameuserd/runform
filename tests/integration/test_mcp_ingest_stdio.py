from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

pytest.importorskip("mcp")

from akc.ingest.pipeline import IngestionStateStore, run_ingest


def test_mcp_ingest_stdio_fetches_resources(tmp_path: Path) -> None:
    fixture = Path(__file__).resolve().parent / "fixtures" / "mcp_stdio_static_server.py"
    cfg = {
        "servers": {
            "fixture": {
                "transport": "stdio",
                "command": sys.executable,
                "args": [str(fixture)],
            }
        }
    }
    cfg_path = tmp_path / "mcp-ingest.json"
    cfg_path.write_text(json.dumps(cfg), encoding="utf-8")

    state = IngestionStateStore(tmp_path / "state.json")
    res = run_ingest(
        connector_name="mcp",
        tenant_id="tenant-mcp",
        input_value="fixture",
        connector_options={
            "mcp_config_path": str(cfg_path),
            "mcp_uri_prefix": "test://",
            "mcp_static_prompt": "static prompt line",
            "mcp_timeout_s": "60",
        },
        embedder=None,
        vector_store=None,
        state_store=state,
        incremental=False,
    )
    assert res.stats.sources_seen >= 3
    assert res.stats.documents_fetched >= 3
