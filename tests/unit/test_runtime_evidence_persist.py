"""Runtime evidence persistence semantics (full snapshot per write)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from akc.cli.runtime import _load_runtime_evidence, _persist_runtime_evidence
from akc.run.manifest import RuntimeEvidenceRecord


def test_persist_runtime_evidence_replaces_file_without_duplicating_rows(tmp_path: Path) -> None:
    ev_path = tmp_path / "runtime_evidence.json"
    record: dict[str, Any] = {"runtime_evidence_path": str(ev_path)}
    first = (
        RuntimeEvidenceRecord(
            evidence_type="terminal_health",
            timestamp=1,
            runtime_run_id="r1",
            payload={"resource_id": "a", "health_status": "healthy"},
        ),
    )
    second = (
        RuntimeEvidenceRecord(
            evidence_type="terminal_health",
            timestamp=2,
            runtime_run_id="r1",
            payload={"resource_id": "b", "health_status": "degraded"},
        ),
    )
    _persist_runtime_evidence(record, evidence=first)
    assert len(_load_runtime_evidence(record)) == 1
    _persist_runtime_evidence(record, evidence=second)
    loaded = _load_runtime_evidence(record)
    assert len(loaded) == 1
    assert loaded[0].payload["resource_id"] == "b"
