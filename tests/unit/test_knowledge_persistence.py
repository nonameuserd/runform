from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.knowledge import (
    CanonicalConstraint,
    CanonicalDecision,
    EvidenceMapping,
    KnowledgeSnapshot,
    knowledge_provenance_fingerprint,
    knowledge_semantic_fingerprint,
    load_knowledge_snapshot_envelope,
    write_knowledge_snapshot_artifacts,
)
from akc.knowledge.persistence import (
    KNOWLEDGE_MEDIATION_SCHEMA_KIND,
    KNOWLEDGE_SNAPSHOT_FINGERPRINT_KIND,
    KNOWLEDGE_SNAPSHOT_SCHEMA_KIND,
    build_knowledge_snapshot_envelope,
    write_knowledge_mediation_report_artifact,
)


def _minimal_snapshot() -> KnowledgeSnapshot:
    c = CanonicalConstraint(
        subject="svc",
        predicate="must",
        object="run",
        polarity=1,
        scope="tenant",
        kind="hard",
        summary="run the service",
    )
    d = CanonicalDecision(
        assertion_id=c.assertion_id,
        selected=True,
        resolved=True,
    )
    m = EvidenceMapping(evidence_doc_ids=("doc1",), resolved_provenance_pointers=())
    return KnowledgeSnapshot(
        canonical_constraints=(c,),
        canonical_decisions=(d,),
        evidence_by_assertion={c.assertion_id: m},
    )


def test_knowledge_snapshot_json_roundtrip() -> None:
    snap = _minimal_snapshot()
    back = KnowledgeSnapshot.from_json_obj(snap.to_json_obj())
    assert back.canonical_constraints[0].assertion_id == snap.canonical_constraints[0].assertion_id
    assert knowledge_semantic_fingerprint(snapshot=back) == knowledge_semantic_fingerprint(snapshot=snap)


def test_write_and_load_knowledge_snapshot_artifacts(tmp_path: Path) -> None:
    snap = _minimal_snapshot()
    snap_sha, fp_sha = write_knowledge_snapshot_artifacts(
        tmp_path,
        tenant_id="t1",
        repo_id="r1",
        snapshot=snap,
        run_id="plan_a",
    )
    assert len(snap_sha) == 64
    assert len(fp_sha) == 64
    raw, loaded = load_knowledge_snapshot_envelope(scope_root=tmp_path)
    assert raw["schema_kind"] == KNOWLEDGE_SNAPSHOT_SCHEMA_KIND
    assert raw["run_id"] == "plan_a"
    assert loaded.canonical_constraints[0].assertion_id == snap.canonical_constraints[0].assertion_id

    fp_raw = (tmp_path / ".akc" / "knowledge" / "snapshot.fingerprint.json").read_text(encoding="utf-8")
    fp_obj = json.loads(fp_raw)
    assert fp_obj["schema_kind"] == KNOWLEDGE_SNAPSHOT_FINGERPRINT_KIND
    assert fp_obj["content_sha256"] == snap_sha


def test_write_knowledge_mediation_report_artifact_roundtrip(tmp_path: Path) -> None:
    report = {"policy": "warn_and_continue", "status": "ok", "events": [{"kind": "test"}]}
    fp = write_knowledge_mediation_report_artifact(
        tmp_path,
        tenant_id="t1",
        repo_id="r1",
        mediation_report=report,
    )
    assert len(fp) == 64
    raw = json.loads((tmp_path / ".akc" / "knowledge" / "mediation.json").read_text(encoding="utf-8"))
    assert raw["schema_kind"] == KNOWLEDGE_MEDIATION_SCHEMA_KIND
    assert raw["tenant_id"] == "t1"
    assert raw["mediation_report"]["events"][0]["kind"] == "test"


def test_build_envelope_rejects_bad_fingerprint_width() -> None:
    snap = _minimal_snapshot()
    with pytest.raises(ValueError, match="64-char"):
        build_knowledge_snapshot_envelope(
            tenant_id="t",
            repo_id="r",
            snapshot=snap,
            knowledge_semantic_fingerprint_full="ab",
            knowledge_provenance_fingerprint_full=knowledge_provenance_fingerprint(snapshot=snap),
        )
