from __future__ import annotations

import json
from pathlib import Path

from akc.knowledge.models import CanonicalConstraint, CanonicalDecision, EvidenceMapping, KnowledgeSnapshot
from akc.knowledge.operator_decisions import (
    apply_operator_decisions_to_snapshot,
    load_operator_knowledge_decisions,
    operator_decisions_path,
)


def test_load_and_apply_operator_decisions(tmp_path: Path) -> None:
    scope = tmp_path / "t" / "repo1"
    (scope / ".akc" / "knowledge").mkdir(parents=True)
    c = CanonicalConstraint(
        subject="x",
        predicate="required",
        object=None,
        polarity=1,
        scope="repo1",
        kind="hard",
        summary="need x",
    )
    d_auto = CanonicalDecision(
        assertion_id=c.assertion_id,
        selected=False,
        resolved=True,
        conflict_resolution_target_assertion_ids=(c.assertion_id,),
        evidence_doc_ids=(),
    )
    snap = KnowledgeSnapshot(
        canonical_constraints=(c,),
        canonical_decisions=(d_auto,),
        evidence_by_assertion={c.assertion_id: EvidenceMapping(evidence_doc_ids=(), resolved_provenance_pointers=())},
    )
    d_op = CanonicalDecision(
        assertion_id=c.assertion_id,
        selected=True,
        resolved=True,
        conflict_resolution_target_assertion_ids=(c.assertion_id,),
        evidence_doc_ids=(),
        rationale="operator override",
    )
    payload = {
        "schema_kind": "akc_operator_knowledge_decisions",
        "schema_version": 1,
        "tenant_id": "tenant_x",
        "repo_id": "repo1",
        "decisions": [d_op.to_json_obj()],
    }
    path = operator_decisions_path(scope_root=scope)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    loaded = load_operator_knowledge_decisions(scope_root=scope, tenant_id="tenant_x", repo_id="repo1")
    assert c.assertion_id in loaded

    merged = apply_operator_decisions_to_snapshot(snap, loaded)
    assert merged.canonical_decisions[0].selected is True
    assert merged.canonical_decisions[0].rationale == "operator override"
