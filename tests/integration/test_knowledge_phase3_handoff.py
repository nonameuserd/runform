"""Phase 3: knowledge → runtime bundle envelope (non-network constraints)."""

from __future__ import annotations

from akc.compile.artifact_consistency import effective_allow_network_for_handoff
from akc.intent.models import IntentSpecV1
from akc.ir.schema import IRDocument, IRNode


def test_effective_allow_network_merges_knowledge_derived_denies_for_pii_ir() -> None:
    ir = IRDocument(
        tenant_id="t1",
        repo_id="r1",
        nodes=(
            IRNode(
                id="kc_pii",
                tenant_id="t1",
                kind="entity",
                name="knowledge_constraint:aid1",
                properties={
                    "assertion_id": "aid1",
                    "kind": "hard",
                    "predicate": "forbidden",
                    "summary": "Never store raw PII in logs.",
                    "subject": "privacy",
                },
            ),
        ),
    )
    intent = IntentSpecV1(
        intent_id="i1",
        tenant_id="t1",
        repo_id="r1",
        spec_version=1,
        goal_statement="g",
        objectives=(),
        constraints=(),
        success_criteria=(),
        policies=(),
        operating_bounds=None,
    )
    _allow, renv = effective_allow_network_for_handoff(ir_document=ir, intent_spec=intent)
    assert "runtime.action.execute.subprocess" in (renv.get("knowledge_derived_deny_actions") or [])
    assert isinstance(renv.get("knowledge_explanations"), dict)
