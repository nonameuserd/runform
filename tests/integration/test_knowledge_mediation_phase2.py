"""Phase 2 knowledge mediation: contradictions, artifacts, deterministic replay."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from akc.compile.controller_config import KnowledgeConflictNormalization
from akc.compile.knowledge_extractor import (
    build_intent_constraint_ids_by_assertion,
    compute_assertion_conflict_resolution_metadata,
    extract_knowledge_snapshot,
)
from akc.intent.models import Constraint, IntentSpecV1
from akc.knowledge.persistence import write_knowledge_mediation_report_artifact
from akc.memory.models import WhyNode
from akc.memory.why_graph import ConflictDetector
from tests.integration.knowledge_domain_coverage_registry import KD_DOC_IDS_CONFLICTING_NORMS

_FIXTURE_ROOT = Path(__file__).resolve().parent.parent / "fixtures" / "knowledge_domains"


def _load_corpus_documents(*, subset_doc_ids: frozenset[str]) -> list[dict[str, Any]]:
    """Same manifest-driven loader as ``test_knowledge_ingest_index_extract_ir_policy``."""

    manifest = json.loads((_FIXTURE_ROOT / "corpus_manifest.json").read_text(encoding="utf-8"))
    out: list[dict[str, Any]] = []
    for ch in manifest.get("chunks", []):
        did = str(ch.get("doc_id") or "").strip()
        if did not in subset_doc_ids:
            continue
        rel = ch.get("path")
        if not isinstance(rel, str):
            continue
        content = (_FIXTURE_ROOT / rel).read_text(encoding="utf-8")
        meta = ch.get("metadata")
        out.append(
            {
                "doc_id": did,
                "title": str(ch.get("title") or ""),
                "content": content,
                "metadata": meta if isinstance(meta, dict) else {},
            }
        )
    assert out, f"no chunks loaded for {subset_doc_ids!r} under {_FIXTURE_ROOT}"
    return out


def _intent(
    *,
    tenant_id: str,
    repo_id: str,
    constraints: list[Constraint],
) -> IntentSpecV1:
    return IntentSpecV1(
        tenant_id=tenant_id,
        repo_id=repo_id,
        spec_version=1,
        status="draft",
        title=None,
        goal_statement="g",
        summary=None,
        derived_from_goal_text=False,
        objectives=(),
        constraints=tuple(constraints),
        policies=(),
        success_criteria=(),
        operating_bounds=None,
        assumptions=(),
        risk_notes=(),
        tags=(),
        metadata=None,
    )


def test_contradictory_docs_yield_conflict_report_and_mediation_artifact(tmp_path: Path) -> None:
    tenant_id = "t_med"
    repo_id = "repo_med"
    intent = _intent(
        tenant_id=tenant_id,
        repo_id=repo_id,
        constraints=[
            Constraint(id="ic_req", kind="hard", statement="service is required"),
            Constraint(id="ic_forb", kind="hard", statement="service is forbidden"),
        ],
    )
    # Two docs: each supports one side; metadata makes newer doc win on temporal tie-break.
    retrieved: dict[str, Any] = {
        "documents": [
            {
                "doc_id": "d_old",
                "title": "old",
                "content": "service must be required for the platform to operate.",
                "metadata": {"indexed_at_ms": 1_700_000_000_000, "doc_version": 1},
            },
            {
                "doc_id": "d_new",
                "title": "new",
                "content": "service is forbidden in production deployments.",
                "metadata": {"indexed_at_ms": 1_800_000_000_000, "doc_version": 2},
            },
        ]
    }
    mediation: dict[str, Any] = {}
    snap = extract_knowledge_snapshot(
        tenant_id=tenant_id,
        repo_id=repo_id,
        intent_spec=intent,
        retrieved_context=retrieved,
        retrieval_provenance_by_doc_id={},
        llm=None,
        use_llm=False,
        mediation_report_out=mediation,
    )
    assert snap.canonical_decisions
    fp = write_knowledge_mediation_report_artifact(
        tmp_path,
        tenant_id=tenant_id,
        repo_id=repo_id,
        mediation_report=mediation,
    )
    assert len(fp) == 64
    path = tmp_path / ".akc" / "knowledge" / "mediation.json"
    assert path.is_file()
    loaded = json.loads(path.read_text(encoding="utf-8"))
    assert loaded["mediation_report"]["policy"] == "warn_and_continue"
    assert loaded["mediation_report"]["events"] is not None

    detector = ConflictDetector()
    nodes = [
        WhyNode(
            id=c.assertion_id,
            type="constraint",
            payload={
                "subject": c.subject,
                "predicate": c.predicate,
                "object": c.object,
                "polarity": c.polarity,
                "scope": c.scope,
            },
        )
        for c in snap.canonical_constraints
    ]
    reports = detector.detect_constraint_contradictions(
        tenant_id=tenant_id,
        repo_id=repo_id,
        nodes=nodes,
        plan_id="p1",
    )
    assert reports
    assert reports[0].participant_assertion_ids is not None

    meta = compute_assertion_conflict_resolution_metadata(
        constraints=snap.canonical_constraints,
        evidence_scores=snap.evidence_strength_by_assertion,
        evidence_by_assertion=snap.evidence_by_assertion,
        documents_by_id={d["doc_id"]: d for d in retrieved["documents"]},
    )
    for d in snap.canonical_decisions:
        assert d.conflict_resolution_target_assertion_ids == (meta[d.assertion_id].winner_assertion_id,)


def test_conflicting_norms_manifest_retention_policies_mediation_artifact(tmp_path: Path) -> None:
    """``02_conflicting_norms/`` fixtures + manifest metadata (indexed_at_ms, doc_version) feed phase-2 mediation."""

    tenant_id = "t_med_corpus"
    repo_id = "repo_med_corpus"
    norm = KnowledgeConflictNormalization(
        subject_synonyms={
            "seven years retention of customer billing records": "customer billing_records",
            "thirty days cold storage archived customer billing records": "customer billing_records",
        },
        lowercase_subjects=True,
    )
    intent = _intent(
        tenant_id=tenant_id,
        repo_id=repo_id,
        constraints=[
            Constraint(
                id="ic_req",
                kind="hard",
                statement="seven years retention of customer billing records is required",
            ),
            Constraint(
                id="ic_forb",
                kind="hard",
                statement="thirty days cold storage archived customer billing records is forbidden",
            ),
        ],
    )
    docs = _load_corpus_documents(subset_doc_ids=KD_DOC_IDS_CONFLICTING_NORMS)
    by_id = {d["doc_id"]: d for d in docs}
    m1 = by_id["kd-conflict-retention-v1"]["metadata"]
    m2 = by_id["kd-conflict-retention-v2"]["metadata"]
    assert int(m1["indexed_at_ms"]) < int(m2["indexed_at_ms"])
    assert int(m1["doc_version"]) < int(m2["doc_version"])

    mediation: dict[str, Any] = {}
    snap = extract_knowledge_snapshot(
        tenant_id=tenant_id,
        repo_id=repo_id,
        intent_spec=intent,
        retrieved_context={"documents": docs},
        retrieval_provenance_by_doc_id={},
        llm=None,
        use_llm=False,
        knowledge_conflict_normalization=norm,
        mediation_report_out=mediation,
        doc_derived_assertions_mode="off",
    )
    assert mediation.get("policy") == "warn_and_continue"
    assert isinstance(mediation.get("events"), list)

    fp = write_knowledge_mediation_report_artifact(
        tmp_path,
        tenant_id=tenant_id,
        repo_id=repo_id,
        mediation_report=mediation,
    )
    assert len(fp) == 64
    path = tmp_path / ".akc" / "knowledge" / "mediation.json"
    loaded = json.loads(path.read_text(encoding="utf-8"))
    assert loaded["mediation_report"]["policy"] == "warn_and_continue"

    assert len(snap.canonical_constraints) == 2
    ic_by_assertion = build_intent_constraint_ids_by_assertion(
        intent_spec=intent,
        repo_id=repo_id,
        documents=docs,
    )
    meta = compute_assertion_conflict_resolution_metadata(
        constraints=snap.canonical_constraints,
        evidence_scores=snap.evidence_strength_by_assertion,
        evidence_by_assertion=snap.evidence_by_assertion,
        documents_by_id=by_id,
        normalization=norm,
    )
    winner_ids = {m.winner_assertion_id for m in meta.values()}
    assert len(winner_ids) == 1
    winner_aid = next(iter(winner_ids))
    assert ic_by_assertion[winner_aid] == "ic_forb"

    for d in snap.canonical_decisions:
        assert d.conflict_resolution_target_assertion_ids == (meta[d.assertion_id].winner_assertion_id,)


def test_subject_synonym_groups_mutex_without_changing_assertion_ids() -> None:
    tenant_id = "t_syn"
    repo_id = "r_syn"
    intent = _intent(
        tenant_id=tenant_id,
        repo_id=repo_id,
        constraints=[
            Constraint(id="a", kind="hard", statement="Widget is required"),
            Constraint(id="b", kind="hard", statement="widget is forbidden"),
        ],
    )
    norm = KnowledgeConflictNormalization(
        subject_synonyms={"widget": "widget"},
        lowercase_subjects=True,
    )
    snap = extract_knowledge_snapshot(
        tenant_id=tenant_id,
        repo_id=repo_id,
        intent_spec=intent,
        retrieved_context={
            "documents": [
                {"doc_id": "d1", "title": "t", "content": "Widget required"},
                {"doc_id": "d2", "title": "t2", "content": "forbidden"},
            ]
        },
        retrieval_provenance_by_doc_id={},
        llm=None,
        use_llm=False,
        knowledge_conflict_normalization=norm,
    )
    assert len(snap.canonical_constraints) == 2
    ids = {c.assertion_id for c in snap.canonical_constraints}
    assert len(ids) == 2
    meta = compute_assertion_conflict_resolution_metadata(
        constraints=snap.canonical_constraints,
        evidence_scores=snap.evidence_strength_by_assertion,
        normalization=norm,
        evidence_by_assertion=snap.evidence_by_assertion,
    )
    assert meta, "expected mutex mediation after normalization"


def test_defer_to_intent_marks_decisions_unresolved() -> None:
    tenant_id = "t_def"
    repo_id = "r_def"
    intent = _intent(
        tenant_id=tenant_id,
        repo_id=repo_id,
        constraints=[
            Constraint(id="c1", kind="hard", statement="beta is required"),
            Constraint(id="c2", kind="hard", statement="beta is forbidden"),
        ],
    )
    # Identical token overlap so scores tie after enrichment; lexicographic winner under defer.
    retrieved = {
        "documents": [
            {
                "doc_id": "d1",
                "title": "t",
                "content": "beta required beta forbidden beta",
                "metadata": {"indexed_at_ms": 1_000, "doc_version": 1},
            }
        ]
    }
    mediation: dict[str, Any] = {}
    snap = extract_knowledge_snapshot(
        tenant_id=tenant_id,
        repo_id=repo_id,
        intent_spec=intent,
        retrieved_context=retrieved,
        retrieval_provenance_by_doc_id={},
        llm=None,
        use_llm=False,
        knowledge_unresolved_conflict_policy="defer_to_intent",
        mediation_report_out=mediation,
    )
    for d in snap.canonical_decisions:
        assert d.resolved is False
    evs = [e for e in (mediation.get("events") or []) if isinstance(e, dict)]
    amb = [e for e in evs if e.get("kind") == "ambiguous_conflict_resolution"]
    assert amb, "expected defer_to_intent ambiguous_conflict_resolution event"
    assert amb[0].get("defer_to_intent") is True
    ics = amb[0].get("intent_constraint_ids")
    assert isinstance(ics, list) and set(ics) == {"c1", "c2"}


def test_replay_same_winner_with_frozen_retrieval() -> None:
    """Same retrieved documents and policy should yield identical mediation decisions."""

    tenant_id = "t_rep"
    repo_id = "r_rep"
    intent = _intent(
        tenant_id=tenant_id,
        repo_id=repo_id,
        constraints=[
            Constraint(id="x", kind="hard", statement="gamma is required"),
            Constraint(id="y", kind="hard", statement="gamma is forbidden"),
        ],
    )
    ctx = {
        "documents": [
            {"doc_id": "d1", "title": "t", "content": "gamma required detail"},
            {"doc_id": "d2", "title": "t2", "content": "forbidden"},
        ]
    }
    s1 = extract_knowledge_snapshot(
        tenant_id=tenant_id,
        repo_id=repo_id,
        intent_spec=intent,
        retrieved_context=ctx,
        retrieval_provenance_by_doc_id={},
        llm=None,
        use_llm=False,
        compile_now_ms=1_000_000,
    )
    s2 = extract_knowledge_snapshot(
        tenant_id=tenant_id,
        repo_id=repo_id,
        intent_spec=intent,
        retrieved_context=ctx,
        retrieval_provenance_by_doc_id={},
        llm=None,
        use_llm=False,
        compile_now_ms=1_000_000,
    )
    w1 = {d.assertion_id: d for d in s1.canonical_decisions}
    w2 = {d.assertion_id: d for d in s2.canonical_decisions}
    assert w1 == w2


def test_document_order_does_not_change_mediation_winner() -> None:
    """Deterministic ordering: shuffled ``documents`` list yields the same decisions."""

    tenant_id = "t_rep2"
    repo_id = "r_rep2"
    intent = _intent(
        tenant_id=tenant_id,
        repo_id=repo_id,
        constraints=[
            Constraint(id="x", kind="hard", statement="gamma is required"),
            Constraint(id="y", kind="hard", statement="gamma is forbidden"),
        ],
    )
    docs = [
        {"doc_id": "d1", "title": "t", "content": "gamma required detail"},
        {"doc_id": "d2", "title": "t2", "content": "forbidden"},
    ]
    ctx_a = {"documents": list(docs)}
    ctx_b = {"documents": [docs[1], docs[0]]}
    sa = extract_knowledge_snapshot(
        tenant_id=tenant_id,
        repo_id=repo_id,
        intent_spec=intent,
        retrieved_context=ctx_a,
        retrieval_provenance_by_doc_id={},
        llm=None,
        use_llm=False,
        compile_now_ms=2_000_000,
    )
    sb = extract_knowledge_snapshot(
        tenant_id=tenant_id,
        repo_id=repo_id,
        intent_spec=intent,
        retrieved_context=ctx_b,
        retrieval_provenance_by_doc_id={},
        llm=None,
        use_llm=False,
        compile_now_ms=2_000_000,
    )
    assert {d.assertion_id: d for d in sa.canonical_decisions} == {d.assertion_id: d for d in sb.canonical_decisions}
