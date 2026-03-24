from __future__ import annotations

import json
from typing import Any

import pytest

from akc.compile.controller_config import DocDerivedPatternOptions, KnowledgeEvidenceWeighting
from akc.compile.interfaces import LLMBackend, LLMRequest, LLMResponse, TenantRepoScope
from akc.compile.knowledge_extractor import (
    KnowledgeExtractionError,
    _select_evidence_doc_ids,
    compute_assertion_conflict_resolution_metadata,
    evidence_scores_for_conflict_resolution,
    extract_doc_derived_soft_assertions_from_documents,
    extract_knowledge_snapshot,
)
from akc.intent.models import Constraint, IntentSpecV1
from akc.ir.provenance import ProvenancePointer
from akc.ir.schema import IRDocument, IRNode
from akc.knowledge.models import CanonicalConstraint


def _mk_intent_spec(
    *,
    tenant_id: str,
    repo_id: str,
    constraints: list[Constraint],
    goal_statement: str | None = None,
) -> IntentSpecV1:
    return IntentSpecV1(
        tenant_id=tenant_id,
        repo_id=repo_id,
        spec_version=1,
        status="draft",
        title=None,
        goal_statement=goal_statement,
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


def test_knowledge_extractor_deterministic_extracts_required_with_evidence() -> None:
    tenant_id = "t1"
    repo_id = "repo1"

    intent = _mk_intent_spec(
        tenant_id=tenant_id,
        repo_id=repo_id,
        constraints=[
            Constraint(id="c1", kind="hard", statement="x is required"),
        ],
    )

    retrieved_context: dict[str, Any] = {
        "documents": [
            {"doc_id": "d1", "title": "t", "content": "x required"},
            {"doc_id": "d2", "title": "t2", "content": "unrelated text"},
        ]
    }

    snap = extract_knowledge_snapshot(
        tenant_id=tenant_id,
        repo_id=repo_id,
        intent_spec=intent,
        retrieved_context=retrieved_context,
        retrieval_provenance_by_doc_id={},
        llm=None,
        use_llm=False,
    )

    assert len(snap.canonical_constraints) == 1
    c = snap.canonical_constraints[0]
    assert c.subject == "x"
    assert c.predicate == "required"
    assert c.polarity in {-1, 1}
    assert c.polarity == 1
    assert c.scope == repo_id
    assert c.kind == "hard"
    assert c.summary == "x is required"

    evidence = snap.evidence_by_assertion[c.assertion_id]
    assert evidence.evidence_doc_ids == ("d1",)


@pytest.mark.parametrize(
    ("name", "constraint_text", "documents", "top_k", "expected"),
    [
        (
            "orders_by_overlap_then_doc_id_tie_break",
            "all egress traffic must use tls and mTLS certificates",
            [
                {
                    "doc_id": "doc-z",
                    "title": "Egress policy",
                    "content": "Egress traffic must use TLS with certificate pinning.",
                },
                {
                    "doc_id": "doc-a",
                    "title": "mTLS requirement",
                    "content": "All egress traffic must use mTLS certificates for service identity.",
                },
                {
                    "doc_id": "doc-m",
                    "title": "Unrelated",
                    "content": "Team lunch and onboarding notes.",
                },
            ],
            3,
            ("doc-a", "doc-z"),
        ),
        (
            "stable_lexicographic_tie_break_for_equal_overlap",
            "service must authenticate requests with token verification",
            [
                {
                    "doc_id": "doc-b",
                    "title": "Auth",
                    "content": "Service authenticate requests token verification.",
                },
                {
                    "doc_id": "doc-a",
                    "title": "Auth duplicate",
                    "content": "Service authenticate requests token verification.",
                },
            ],
            2,
            ("doc-a", "doc-b"),
        ),
        (
            "filters_invalid_doc_ids_and_respects_top_k",
            "rotate signing keys every 30 days",
            [
                {
                    "doc_id": "  ",
                    "title": "invalid",
                    "content": "rotate signing keys every 30 days",
                },
                {
                    "doc_id": "doc-2",
                    "title": "crypto policy",
                    "content": "rotate signing keys every 30 days and archive old keys",
                },
                {
                    "doc_id": "doc-1",
                    "title": "minimum policy",
                    "content": "rotate signing keys every 30 days",
                },
                {
                    "doc_id": "doc-3",
                    "title": "partial policy",
                    "content": "rotate keys quarterly",
                },
            ],
            2,
            ("doc-1", "doc-2"),
        ),
    ],
    ids=lambda row: row if isinstance(row, str) else None,
)
def test_select_evidence_doc_ids_table_driven_ranking(
    name: str,
    constraint_text: str,
    documents: list[dict[str, Any]],
    top_k: int,
    expected: tuple[str, ...],
) -> None:
    _ = name
    assert (
        _select_evidence_doc_ids(
            constraint_text=constraint_text,
            documents=documents,
            top_k=top_k,
        )
        == expected
    )


def test_select_evidence_doc_ids_returns_empty_for_blank_or_non_token_constraint() -> None:
    docs: list[dict[str, Any]] = [
        {"doc_id": "doc-1", "title": "policy", "content": "any content"},
    ]
    assert _select_evidence_doc_ids(constraint_text="", documents=docs, top_k=3) == ()
    assert _select_evidence_doc_ids(constraint_text="the and or", documents=docs, top_k=3) == ()


def test_knowledge_extractor_deterministic_mutex_resolution_picks_evidence_best() -> None:
    tenant_id = "t1"
    repo_id = "repo1"
    intent = _mk_intent_spec(
        tenant_id=tenant_id,
        repo_id=repo_id,
        constraints=[
            # Use a multi-char subject token ("service") so overlap scores are not trivially tied.
            Constraint(id="c_req", kind="hard", statement="service is required"),
            Constraint(id="c_forb", kind="hard", statement="service is forbidden"),
        ],
    )

    retrieved_context: dict[str, Any] = {
        "documents": [
            {"doc_id": "d_req", "title": "t", "content": "service required mandatory documentation"},
            {"doc_id": "d_forb", "title": "t2", "content": "forbidden"},
        ]
    }

    snap = extract_knowledge_snapshot(
        tenant_id=tenant_id,
        repo_id=repo_id,
        intent_spec=intent,
        retrieved_context=retrieved_context,
        retrieval_provenance_by_doc_id={},
        llm=None,
        use_llm=False,
    )

    c_by_pred = {c.predicate: c for c in snap.canonical_constraints}
    assert "required" in c_by_pred
    assert "forbidden" in c_by_pred

    aid_req = c_by_pred["required"].assertion_id
    aid_forb = c_by_pred["forbidden"].assertion_id
    assert snap.evidence_strength_by_assertion[aid_req] > snap.evidence_strength_by_assertion[aid_forb]

    decisions = list(snap.canonical_decisions)
    assert decisions, "expected a resolved mutex decision"

    d_map = {d.assertion_id: d.selected for d in decisions}
    assert d_map[aid_req] is True
    assert d_map[aid_forb] is False


def test_knowledge_extractor_attaches_resolved_provenance_when_available() -> None:
    tenant_id = "t1"
    repo_id = "repo1"

    ptr = ProvenancePointer(
        tenant_id=tenant_id,
        kind="doc_chunk",
        source_id="d1",
        locator=None,
        sha256="a" * 64,
        metadata={"doc_id": "d1"},
    )

    intent = _mk_intent_spec(
        tenant_id=tenant_id,
        repo_id=repo_id,
        constraints=[Constraint(id="c1", kind="hard", statement="x is required")],
    )
    retrieved_context: dict[str, Any] = {
        "documents": [
            {"doc_id": "d1", "title": "t", "content": "x required"},
        ]
    }

    snap = extract_knowledge_snapshot(
        tenant_id=tenant_id,
        repo_id=repo_id,
        intent_spec=intent,
        retrieved_context=retrieved_context,
        retrieval_provenance_by_doc_id={"d1": ptr},
        llm=None,
        use_llm=False,
    )

    c = snap.canonical_constraints[0]
    evidence = snap.evidence_by_assertion[c.assertion_id]
    assert evidence.evidence_doc_ids == ("d1",)
    assert evidence.resolved_provenance_pointers
    assert evidence.resolved_provenance_pointers[0].tenant_id == tenant_id
    assert evidence.resolved_provenance_pointers[0].source_id == "d1"


def test_conflict_resolution_metadata_matches_snapshot_decisions() -> None:
    tenant_id = "t1"
    repo_id = "repo1"
    intent = _mk_intent_spec(
        tenant_id=tenant_id,
        repo_id=repo_id,
        constraints=[
            Constraint(id="c_req", kind="hard", statement="service is required"),
            Constraint(id="c_forb", kind="hard", statement="service is forbidden"),
        ],
    )
    retrieved_context: dict[str, Any] = {
        "documents": [
            {"doc_id": "d_req", "title": "t", "content": "service required detail"},
            {"doc_id": "d_forb", "title": "t2", "content": "forbidden"},
        ]
    }
    snap = extract_knowledge_snapshot(
        tenant_id=tenant_id,
        repo_id=repo_id,
        intent_spec=intent,
        retrieved_context=retrieved_context,
        retrieval_provenance_by_doc_id={},
        llm=None,
        use_llm=False,
    )
    scores = evidence_scores_for_conflict_resolution(snapshot=snap)
    meta = compute_assertion_conflict_resolution_metadata(
        constraints=snap.canonical_constraints,
        evidence_scores=scores,
    )
    for d in snap.canonical_decisions:
        row = meta[d.assertion_id]
        assert d.conflict_resolution_target_assertion_ids == (row.winner_assertion_id,)
        assert d.selected == (d.assertion_id == row.winner_assertion_id)
    assert snap.evidence_strength_by_assertion


class _StaticJsonLLM:
    """Minimal fake backend for knowledge extraction tests."""

    def __init__(self, payload: dict[str, Any]) -> None:
        self._text = json.dumps(payload, ensure_ascii=False)

    def complete(
        self,
        *,
        scope: TenantRepoScope,
        stage: str,
        request: LLMRequest,
    ) -> LLMResponse:
        _ = scope, stage, request
        return LLMResponse(text=self._text)


class _CaptureUserMessageLLM:
    """Captures the last user message (prompt) for shape assertions."""

    def __init__(self, payload: dict[str, Any]) -> None:
        self._text = json.dumps(payload, ensure_ascii=False)
        self.last_user_message: str | None = None

    def complete(
        self,
        *,
        scope: TenantRepoScope,
        stage: str,
        request: LLMRequest,
    ) -> LLMResponse:
        _ = scope, stage
        self.last_user_message = request.messages[-1].content
        return LLMResponse(text=self._text)


def test_knowledge_extractor_llm_path_recomputes_decisions_from_evidence() -> None:
    tenant_id = "t1"
    repo_id = "repo1"
    intent = _mk_intent_spec(
        tenant_id=tenant_id,
        repo_id=repo_id,
        constraints=[
            Constraint(id="c_req", kind="hard", statement="alpha is required"),
            Constraint(id="c_forb", kind="hard", statement="alpha is forbidden"),
        ],
    )
    # LLM fabricates canonical rows; conflict winner must follow code-side rules
    # (evidence cardinality), not contradictory LLM decision flags.
    llm_json: dict[str, Any] = {
        "canonical_constraints": [
            {
                "subject": "alpha",
                "predicate": "required",
                "object": None,
                "polarity": 1,
                "scope": repo_id,
                "kind": "hard",
                "summary": "alpha is required",
                "evidence_doc_ids": ["d_a", "d_b"],
            },
            {
                "subject": "alpha",
                "predicate": "forbidden",
                "object": None,
                "polarity": 1,
                "scope": repo_id,
                "kind": "hard",
                "summary": "alpha is forbidden",
                "evidence_doc_ids": ["d_x"],
            },
        ],
        "evidence_by_assertion": {},
    }

    retrieved_context: dict[str, Any] = {
        "documents": [
            {"doc_id": "d_a", "title": "", "content": ""},
            {"doc_id": "d_b", "title": "", "content": ""},
            {"doc_id": "d_x", "title": "", "content": ""},
        ]
    }
    fake_llm: LLMBackend = _StaticJsonLLM(llm_json)
    snap = extract_knowledge_snapshot(
        tenant_id=tenant_id,
        repo_id=repo_id,
        intent_spec=intent,
        retrieved_context=retrieved_context,
        retrieval_provenance_by_doc_id={},
        llm=fake_llm,
        use_llm=True,
    )
    c_by_pred = {c.predicate: c for c in snap.canonical_constraints}
    assert c_by_pred["required"].assertion_id != c_by_pred["forbidden"].assertion_id
    winners = {d.assertion_id for d in snap.canonical_decisions if d.selected}
    assert winners == {c_by_pred["required"].assertion_id}


def test_controller_config_knowledge_extraction_mode_validation() -> None:
    from akc.compile.controller_config import ControllerConfig, TierConfig

    tiers = {
        "small": TierConfig(name="small", llm_model="m"),
        "medium": TierConfig(name="medium", llm_model="m"),
        "large": TierConfig(name="large", llm_model="m"),
    }
    cfg = ControllerConfig(tiers=tiers, knowledge_extraction_mode="deterministic")
    assert cfg.knowledge_extraction_mode == "deterministic"
    assert cfg.doc_derived_assertions_mode == "limited"

    with pytest.raises(ValueError, match="knowledge_extraction_mode"):
        ControllerConfig(tiers=tiers, knowledge_extraction_mode="invalid_mode")  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="knowledge_unresolved_conflict_policy"):
        ControllerConfig(tiers=tiers, knowledge_unresolved_conflict_policy="nope")  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="doc_derived_assertions_mode"):
        ControllerConfig(tiers=tiers, doc_derived_assertions_mode="everything")  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="doc_derived_max_assertions"):
        ControllerConfig(tiers=tiers, doc_derived_max_assertions=-1)
    with pytest.raises(ValueError, match="ir_operational_structure_policy"):
        ControllerConfig(tiers=tiers, ir_operational_structure_policy="nope")  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="ir_graph_integrity_policy"):
        ControllerConfig(tiers=tiers, ir_graph_integrity_policy="nope")  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="artifact_consistency_policy"):
        ControllerConfig(tiers=tiers, artifact_consistency_policy="nope")  # type: ignore[arg-type]


def test_doc_derived_assertions_from_chunks_without_intent() -> None:
    tenant_id = "t1"
    repo_id = "repo1"
    intent = _mk_intent_spec(
        tenant_id=tenant_id,
        repo_id=repo_id,
        constraints=[],
        goal_statement="synthetic goal for doc-derived extraction",
    )
    ctx: dict[str, Any] = {
        "documents": [
            {
                "doc_id": "d1",
                "title": "API rules",
                "content": (
                    "All callers MUST authenticate requests.\nThe system SHALL NOT expose secrets over the network."
                ),
            }
        ]
    }
    snap = extract_knowledge_snapshot(
        tenant_id=tenant_id,
        repo_id=repo_id,
        intent_spec=intent,
        retrieved_context=ctx,
        retrieval_provenance_by_doc_id={},
        llm=None,
        use_llm=False,
        doc_derived_assertions_mode="limited",
        doc_derived_max_assertions=5,
    )
    assert len(snap.canonical_constraints) >= 1
    assert all(c.kind == "soft" for c in snap.canonical_constraints)


def test_doc_derived_off_with_empty_intent_returns_empty_snapshot() -> None:
    tenant_id = "t1"
    repo_id = "repo1"
    intent = _mk_intent_spec(
        tenant_id=tenant_id,
        repo_id=repo_id,
        constraints=[],
        goal_statement="synthetic goal for doc-derived extraction",
    )
    ctx: dict[str, Any] = {
        "documents": [
            {
                "doc_id": "d1",
                "title": "t",
                "content": "The service MUST use TLS.",
            }
        ]
    }
    snap = extract_knowledge_snapshot(
        tenant_id=tenant_id,
        repo_id=repo_id,
        intent_spec=intent,
        retrieved_context=ctx,
        retrieval_provenance_by_doc_id={},
        llm=None,
        use_llm=False,
        doc_derived_assertions_mode="off",
    )
    assert snap.canonical_constraints == ()


def test_enrich_evidence_scores_applies_max_doc_metadata_bonus_per_assertion() -> None:
    from akc.compile.knowledge_extractor import (
        _documents_by_doc_id,
        enrich_evidence_scores_with_doc_metadata,
    )
    from akc.knowledge.models import EvidenceMapping

    docs: list[dict[str, Any]] = [
        {"doc_id": "t1", "metadata": {"trust_tier": "trusted"}, "title": "", "content": ""},
        {"doc_id": "l1", "metadata": {"trust_tier": "low"}, "title": "", "content": ""},
    ]
    em_hi = EvidenceMapping(evidence_doc_ids=("t1",), resolved_provenance_pointers=())
    em_lo = EvidenceMapping(evidence_doc_ids=("l1",), resolved_provenance_pointers=())
    by_id = _documents_by_doc_id(docs)
    weighting = KnowledgeEvidenceWeighting()
    out = enrich_evidence_scores_with_doc_metadata(
        base_scores={"a": 1.0, "b": 1.0},
        evidence_by_assertion={"a": em_hi, "b": em_lo},
        documents_by_id=by_id,
        weighting=weighting,
        compile_now_ms=1_700_000_000_000,
    )
    assert out["a"] > out["b"]


def test_knowledge_fail_closed_on_ambiguous_mutex_tie() -> None:
    repo_id = "repo1"
    c1 = CanonicalConstraint(
        subject="s",
        predicate="required",
        object=None,
        polarity=1,
        scope=repo_id,
        kind="hard",
        summary="s is required",
    )
    c2 = CanonicalConstraint(
        subject="s",
        predicate="forbidden",
        object=None,
        polarity=1,
        scope=repo_id,
        kind="hard",
        summary="s is forbidden",
    )
    scores = {c1.assertion_id: 1.0, c2.assertion_id: 1.0}
    with pytest.raises(KnowledgeExtractionError, match="knowledge_conflict_ambiguous_tie"):
        compute_assertion_conflict_resolution_metadata(
            constraints=(c1, c2),
            evidence_scores=scores,
            unresolved_policy="fail_closed",
            evidence_doc_counts_by_assertion={c1.assertion_id: 1, c2.assertion_id: 1},
        )


def test_knowledge_defer_to_intent_mutex_prefers_fewer_evidence_docs() -> None:
    repo_id = "repo1"
    c1 = CanonicalConstraint(
        subject="s",
        predicate="required",
        object=None,
        polarity=1,
        scope=repo_id,
        kind="hard",
        summary="s is required",
    )
    c2 = CanonicalConstraint(
        subject="s",
        predicate="forbidden",
        object=None,
        polarity=1,
        scope=repo_id,
        kind="hard",
        summary="s is forbidden",
    )
    scores = {c1.assertion_id: 2.0, c2.assertion_id: 2.0}
    meta = compute_assertion_conflict_resolution_metadata(
        constraints=(c1, c2),
        evidence_scores=scores,
        unresolved_policy="defer_to_intent",
        evidence_doc_counts_by_assertion={c1.assertion_id: 3, c2.assertion_id: 1},
    )
    assert meta[c1.assertion_id].winner_assertion_id == c2.assertion_id


def test_knowledge_mediation_report_records_lexicographic_tie_break() -> None:
    tenant_id = "t1"
    repo_id = "repo1"
    intent = _mk_intent_spec(
        tenant_id=tenant_id,
        repo_id=repo_id,
        constraints=[
            Constraint(id="c_req", kind="hard", statement="foo is required"),
            Constraint(id="c_forb", kind="hard", statement="foo is forbidden"),
        ],
    )
    retrieved_context: dict[str, Any] = {
        "documents": [
            {"doc_id": "d_req", "title": "", "content": "foo required"},
            {"doc_id": "d_forb", "title": "", "content": "foo forbidden"},
        ]
    }
    mediation: dict[str, Any] = {}
    extract_knowledge_snapshot(
        tenant_id=tenant_id,
        repo_id=repo_id,
        intent_spec=intent,
        retrieved_context=retrieved_context,
        retrieval_provenance_by_doc_id={},
        mediation_report_out=mediation,
        knowledge_unresolved_conflict_policy="warn_and_continue",
    )
    assert mediation.get("policy") == "warn_and_continue"
    assert mediation.get("status") == "ok"
    assert mediation.get("events"), "expected a tie-break mediation event"
    assert mediation["events"][0].get("tie_break") == "lexicographic_assertion_id"


def test_doc_derived_extended_patterns_capture_table_and_numbered_lines() -> None:
    text = (
        "4.2.1 The component SHOULD NOT log credentials.\n\n"
        "| Step | Requirement |\n"
        "| 1 | The server MUST verify tokens |\n"
    )
    doc = {"doc_id": "d1", "title": "", "content": text}
    legacy = extract_doc_derived_soft_assertions_from_documents(
        repo_id="repo1",
        documents=[doc],
        max_assertions=50,
        patterns=DocDerivedPatternOptions(),
    )
    extended = extract_doc_derived_soft_assertions_from_documents(
        repo_id="repo1",
        documents=[doc],
        max_assertions=50,
        patterns=DocDerivedPatternOptions(
            rfc2119_bcp14=True,
            numbered_requirements=True,
            table_normative_rows=True,
        ),
    )
    assert len(extended) >= len(legacy)
    assert len(extended) >= 2


def test_knowledge_llm_prompt_includes_ir_anchor_when_ir_document_set() -> None:
    tenant_id = "t1"
    repo_id = "repo1"
    intent = _mk_intent_spec(
        tenant_id=tenant_id,
        repo_id=repo_id,
        constraints=[Constraint(id="c1", kind="hard", statement="x is required")],
    )
    ir = IRDocument(
        tenant_id=tenant_id,
        repo_id=repo_id,
        nodes=(
            IRNode(
                id="intent-n",
                tenant_id=tenant_id,
                kind="intent",
                name="i",
                properties={
                    "linked_constraints": [{"constraint_id": "c1", "kind": "hard", "summary": "x is required"}],
                },
            ),
        ),
    )
    llm_json: dict[str, Any] = {
        "canonical_constraints": [
            {
                "subject": "x",
                "predicate": "required",
                "object": None,
                "polarity": 1,
                "scope": repo_id,
                "kind": "hard",
                "summary": "x is required",
                "evidence_doc_ids": ["d1"],
            }
        ],
        "evidence_by_assertion": {},
    }
    cap = _CaptureUserMessageLLM(llm_json)
    extract_knowledge_snapshot(
        tenant_id=tenant_id,
        repo_id=repo_id,
        intent_spec=intent,
        retrieved_context={"documents": [{"doc_id": "d1", "title": "", "content": "x required"}]},
        retrieval_provenance_by_doc_id={},
        llm=cap,
        use_llm=True,
        ir_document=ir,
    )
    assert cap.last_user_message is not None
    assert '"ir_anchor"' in cap.last_user_message
    assert '"ir_compact"' in cap.last_user_message
    tail = cap.last_user_message.split("Input:\n", 1)[1].strip()
    payload = json.loads(tail)
    assert "ir_anchor" in payload and payload["ir_anchor"]["intent_nodes"]


def test_knowledge_snapshot_assertion_ids_stable_when_ir_mirrors_intent_constraints() -> None:
    tenant_id = "t1"
    repo_id = "repo1"
    intent = _mk_intent_spec(
        tenant_id=tenant_id,
        repo_id=repo_id,
        constraints=[Constraint(id="c1", kind="hard", statement="feature x is required")],
    )
    retrieved_context: dict[str, Any] = {
        "documents": [
            {"doc_id": "d1", "title": "", "content": "feature x required"},
        ]
    }
    snap_plain = extract_knowledge_snapshot(
        tenant_id=tenant_id,
        repo_id=repo_id,
        intent_spec=intent,
        retrieved_context=retrieved_context,
        retrieval_provenance_by_doc_id={},
        llm=None,
        use_llm=False,
    )
    aid = snap_plain.canonical_constraints[0].assertion_id
    ir = IRDocument(
        tenant_id=tenant_id,
        repo_id=repo_id,
        nodes=(
            IRNode(
                id="intent-n",
                tenant_id=tenant_id,
                kind="intent",
                name="i",
                properties={
                    "linked_constraints": [
                        {"constraint_id": "c1", "kind": "hard", "summary": "feature x is required"},
                    ],
                },
            ),
            IRNode(
                id="khub",
                tenant_id=tenant_id,
                kind="knowledge",
                name="hub",
                properties={"knowledge_assertion_ids": [aid]},
            ),
        ),
    )
    snap_with_ir = extract_knowledge_snapshot(
        tenant_id=tenant_id,
        repo_id=repo_id,
        intent_spec=intent,
        retrieved_context=retrieved_context,
        retrieval_provenance_by_doc_id={},
        llm=None,
        use_llm=False,
        ir_document=ir,
    )
    assert snap_plain.canonical_constraints[0].assertion_id == snap_with_ir.canonical_constraints[0].assertion_id
    assert (
        snap_plain.evidence_by_assertion[aid].evidence_doc_ids
        == snap_with_ir.evidence_by_assertion[aid].evidence_doc_ids
    )
