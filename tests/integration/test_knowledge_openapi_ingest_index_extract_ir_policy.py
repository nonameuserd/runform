"""OpenAPI-shaped corpus chunks use the same ingest → assertion-index → extract path as docs/messaging."""

from __future__ import annotations

from pathlib import Path

import pytest

from akc.compile.artifact_consistency import effective_allow_network_for_handoff
from akc.compile.assertion_index_store import (
    load_assertions_for_doc_ids,
    merge_documents_into_assertion_index,
)
from akc.compile.knowledge_extractor import extract_knowledge_snapshot
from akc.intent.models import Constraint
from akc.knowledge.runtime_projection import knowledge_runtime_envelope_from_ir
from tests.integration.knowledge_domain_coverage_registry import KD_DOC_IDS_SECURITY_NETWORK_OPENAPI
from tests.integration.test_knowledge_ingest_index_extract_ir_policy import (
    _intent,
    _ir_document_from_snapshot,
    _llm_for_use_flag,
    _load_documents,
    _scope_root,
)


@pytest.mark.parametrize("use_llm", [False, True])
def test_openapi_yaml_chunk_metadata_index_merge_extract_and_runtime_envelope(
    tmp_path: Path,
    use_llm: bool,
) -> None:
    """YAML OpenAPI fragment (same pipeline as markdown/chat) indexes RFC2119 lines and projects policy.

    Fixture: ``tests/fixtures/knowledge_domains/01_security_network/openapi/external_api_fragment.yaml``
    (HTTPS, MUST NOT PII in URLs). ``ingest_source_kind`` is ``openapi``; runtime envelope matches
    :mod:`akc.knowledge.runtime_projection` regex heuristics on hard constraint summaries.
    """

    tenant_id = "tenant_kd_openapi"
    repo_id = "repo_kd_openapi"
    docs = _load_documents(subset_doc_ids=KD_DOC_IDS_SECURITY_NETWORK_OPENAPI)
    assert len(docs) == 1
    meta = docs[0].get("metadata") or {}
    assert meta.get("ingest_source_kind") == "openapi"
    assert str(meta.get("openapi_version") or "").strip() == "3.0.3"

    root = _scope_root(tmp_path, tenant_id=tenant_id, repo_id=repo_id)
    n_merged = merge_documents_into_assertion_index(
        scope_root=root,
        tenant_id=tenant_id,
        repo_id=repo_id,
        documents=docs,
        max_assertions_per_batch=128,
    )
    assert n_merged >= 1

    openapi_doc_id = "kd-sec-net-openapi-callbacks"
    idx_c, _, _ = load_assertions_for_doc_ids(
        scope_root=root,
        tenant_id=tenant_id,
        repo_id=repo_id,
        doc_ids={openapi_doc_id},
        limit=128,
        provenance_map={},
    )
    assert len(idx_c) >= 2
    idx_summaries = [str(c.summary) for c in idx_c]
    assert any("MUST use HTTPS" in s for s in idx_summaries)
    assert any("MUST NOT" in s and "personally identifiable" in s.lower() for s in idx_summaries)

    intent = _intent(
        tenant_id=tenant_id,
        repo_id=repo_id,
        constraints=(
            Constraint(
                id="ic_https",
                kind="hard",
                statement=("Outbound callbacks MUST use HTTPS only; cleartext HTTP is forbidden."),
            ),
            Constraint(
                id="ic_pii",
                kind="hard",
                statement=("Embedding personally identifiable information in callback URLs is forbidden."),
            ),
        ),
    )

    retrieved = {"documents": docs}
    retrieved_ids = {str(d["doc_id"]).strip() for d in docs}
    provenance = {
        did: {
            "kind": "doc_chunk",
            "source_id": did,
            "tenant_id": tenant_id,
            "locator": f"fixture:{did}",
        }
        for did in retrieved_ids
    }

    snap = extract_knowledge_snapshot(
        tenant_id=tenant_id,
        repo_id=repo_id,
        intent_spec=intent,
        retrieved_context=retrieved,
        retrieval_provenance_by_doc_id=provenance,
        llm=_llm_for_use_flag(use_llm=use_llm, repo_id=repo_id, intent=intent, documents=docs),
        use_llm=use_llm,
        knowledge_artifact_root=root,
        stored_assertion_index_mode="merge",
        stored_assertion_index_max_rows=128,
    )
    # Two intent hard constraints + doc-derived soft rows from the YAML description block.
    assert len(snap.canonical_constraints) >= 2
    summaries = {str(c.summary) for c in snap.canonical_constraints}
    assert any("HTTPS" in s for s in summaries)
    assert any("personally identifiable" in s.lower() for s in summaries)

    ir = _ir_document_from_snapshot(tenant_id=tenant_id, repo_id=repo_id, snapshot=snap)
    env = knowledge_runtime_envelope_from_ir(ir)
    assert env.get("knowledge_network_egress_forbidden") is True
    deny = set(env.get("knowledge_derived_deny_actions") or ())
    assert "runtime.action.execute.http" in deny
    assert "runtime.action.execute.subprocess" in deny
    assert "service.reconcile.apply" in deny
    assert "service.reconcile.rollback" in deny

    rules = env.get("knowledge_policy_rules")
    assert isinstance(rules, list) and rules
    classes_seen: set[str] = set()
    deny_by_class: dict[str, set[str]] = {}
    for r in rules:
        if not isinstance(r, dict):
            continue
        cl = r.get("classes") if isinstance(r.get("classes"), list) else []
        for c in cl:
            cs = str(c)
            classes_seen.add(cs)
            deny_by_class.setdefault(cs, set()).update(r.get("deny_actions") or [])
    assert "network_egress" in classes_seen
    assert "secrets_pii" in classes_seen
    assert "runtime.action.execute.http" in deny_by_class.get("network_egress", set())
    assert "runtime.action.execute.subprocess" in deny_by_class.get("secrets_pii", set())

    allow_net, renv = effective_allow_network_for_handoff(ir_document=ir, intent_spec=intent)
    assert allow_net is False
    assert renv.get("knowledge_network_egress_forbidden") is True
