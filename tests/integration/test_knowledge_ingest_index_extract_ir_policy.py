"""Narrow e2e: ingest/index → extract_knowledge_snapshot → IR → runtime knowledge envelope.

Proves default policy projection (``knowledge_derived_deny_actions``,
``knowledge_network_egress_forbidden``) for multi-domain intent statements merged with
A4 assertion-index rows under a tenant-scoped temp repo.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from akc.compile.artifact_consistency import effective_allow_network_for_handoff
from akc.compile.assertion_index_store import merge_documents_into_assertion_index
from akc.compile.interfaces import LLMBackend, LLMRequest, LLMResponse, TenantRepoScope
from akc.compile.knowledge_extractor import _deterministic_parse_constraint, extract_knowledge_snapshot
from akc.intent.models import Constraint, IntentSpecV1
from akc.ir.schema import IRDocument, IRNode, stable_node_id
from akc.knowledge.models import KnowledgeSnapshot
from akc.knowledge.runtime_projection import knowledge_runtime_envelope_from_ir
from akc.memory.models import normalize_repo_id
from tests.integration.knowledge_domain_coverage_registry import (
    KD_DOC_IDS_SECURITY_NETWORK_BASE,
    KD_DOC_IDS_SECURITY_NETWORK_MESSAGING,
)

_FIXTURE_ROOT = Path(__file__).resolve().parent.parent / "fixtures" / "knowledge_domains"


class _StaticJsonKnowledgeLLM:
    """Returns fixed JSON for ``extract_knowledge_snapshot`` LLM path (matches deterministic semantics)."""

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


def _llm_payload_matching_deterministic_intent(
    *,
    repo_id: str,
    intent: IntentSpecV1,
    documents: list[dict[str, Any]],
) -> dict[str, Any]:
    """Shape expected by ``_build_snapshot_from_llm_json``; aligned with ``_deterministic_parse_constraint``."""

    canonical_constraints: list[dict[str, Any]] = []
    for c in intent.constraints:
        canon, eids, _score = _deterministic_parse_constraint(
            repo_id=repo_id,
            constraint=c,
            documents=documents,
        )
        canonical_constraints.append(
            {
                "subject": canon.subject,
                "predicate": canon.predicate,
                "object": canon.object,
                "polarity": canon.polarity,
                "scope": canon.scope,
                "kind": canon.kind,
                "summary": canon.summary,
                "evidence_doc_ids": list(eids),
            }
        )
    return {"canonical_constraints": canonical_constraints, "evidence_by_assertion": {}}


def _llm_for_use_flag(
    *,
    use_llm: bool,
    repo_id: str,
    intent: IntentSpecV1,
    documents: list[dict[str, Any]],
) -> LLMBackend | None:
    if not use_llm:
        return None
    payload = _llm_payload_matching_deterministic_intent(
        repo_id=repo_id,
        intent=intent,
        documents=documents,
    )
    return _StaticJsonKnowledgeLLM(payload)


def _load_documents(*, subset_doc_ids: frozenset[str]) -> list[dict[str, Any]]:
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


def _scope_root(tmp_path: Path, *, tenant_id: str, repo_id: str) -> Path:
    return tmp_path / tenant_id / normalize_repo_id(repo_id)


def _ir_document_from_snapshot(
    *,
    tenant_id: str,
    repo_id: str,
    snapshot: KnowledgeSnapshot,
) -> IRDocument:
    """Mirror ``ir_builder._build_knowledge_ir_nodes_for_step`` shape for handoff tests."""

    nodes: list[IRNode] = []
    for c in snapshot.canonical_constraints:
        ev = snapshot.evidence_by_assertion.get(c.assertion_id)
        evidence_doc_ids = list(ev.evidence_doc_ids) if ev is not None else []
        props: dict[str, Any] = {
            "assertion_id": c.assertion_id,
            "subject": c.subject,
            "predicate": c.predicate,
            "object": c.object,
            "polarity": c.polarity,
            "scope": c.scope,
            "kind": c.kind,
            "summary": c.summary,
            "semantic_fingerprint": c.semantic_fingerprint,
            "evidence_doc_ids": evidence_doc_ids,
        }
        nodes.append(
            IRNode(
                id=stable_node_id(kind="entity", name=f"constraint:{c.assertion_id}"),
                tenant_id=tenant_id,
                kind="entity",
                name=f"knowledge_constraint:{c.assertion_id}",
                properties=props,
            )
        )
    for d in snapshot.canonical_decisions:
        nodes.append(
            IRNode(
                id=stable_node_id(kind="entity", name=f"decision:{d.assertion_id}"),
                tenant_id=tenant_id,
                kind="entity",
                name=f"knowledge_decision:{d.assertion_id}",
                properties={
                    "assertion_id": d.assertion_id,
                    "selected": d.selected,
                    "resolved": d.resolved,
                    "conflict_resolution_target_assertion_ids": list(d.conflict_resolution_target_assertion_ids),
                    "evidence_doc_ids": list(d.evidence_doc_ids),
                },
            )
        )
    return IRDocument(tenant_id=tenant_id, repo_id=repo_id, nodes=tuple(nodes))


def _intent(
    *,
    tenant_id: str,
    repo_id: str,
    constraints: tuple[Constraint, ...],
) -> IntentSpecV1:
    return IntentSpecV1(
        tenant_id=tenant_id,
        repo_id=repo_id,
        spec_version=1,
        goal_statement="g",
        constraints=constraints,
    )


@pytest.mark.parametrize("use_llm", [False, True])
def test_ingest_index_extract_ir_envelope_network_destructive_and_merge(
    tmp_path: Path,
    use_llm: bool,
) -> None:
    """Deterministic path: ingest merges into SQLite; compile extracts and projects IR → policy."""

    tenant_id = "tenant_kd_e2e"
    repo_id = "repo_kd_e2e"
    # Fixture chunks: firewall/egress language + destructive ops language.
    docs = _load_documents(subset_doc_ids=KD_DOC_IDS_SECURITY_NETWORK_BASE)
    root = _scope_root(tmp_path, tenant_id=tenant_id, repo_id=repo_id)
    n_merged = merge_documents_into_assertion_index(
        scope_root=root,
        tenant_id=tenant_id,
        repo_id=repo_id,
        documents=docs,
        max_assertions_per_batch=128,
    )
    assert n_merged > 0

    intent = _intent(
        tenant_id=tenant_id,
        repo_id=repo_id,
        constraints=(
            Constraint(
                id="ic_egress",
                kind="hard",
                statement=("Public internet egress for outbound HTTPS is forbidden without explicit approval."),
            ),
            Constraint(
                id="ic_destructive",
                kind="hard",
                statement=("Destructive rm -rf purge of production volumes is forbidden."),
            ),
        ),
    )
    retrieved = {"documents": docs}
    retrieved_ids = {str(d["doc_id"]).strip() for d in docs}
    provenance: dict[str, Any] = {
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
    # Intent + merged index rows (soft assertions) should all appear.
    assert len(snap.canonical_constraints) >= 2
    assert snap.canonical_decisions

    ir = _ir_document_from_snapshot(tenant_id=tenant_id, repo_id=repo_id, snapshot=snap)
    env = knowledge_runtime_envelope_from_ir(ir)
    assert env.get("knowledge_network_egress_forbidden") is True
    deny = set(env.get("knowledge_derived_deny_actions") or ())
    assert "runtime.action.execute.http" in deny
    assert "service.reconcile.apply" in deny
    assert "service.reconcile.rollback" in deny

    rules = env.get("knowledge_policy_rules")
    assert isinstance(rules, list) and rules
    classes_seen: set[str] = set()
    for r in rules:
        cl = r.get("classes") if isinstance(r, dict) else None
        if isinstance(cl, list):
            classes_seen.update(str(x) for x in cl)
    assert "network_egress" in classes_seen
    assert "destructive" in classes_seen

    allow_net, renv = effective_allow_network_for_handoff(ir_document=ir, intent_spec=intent)
    assert allow_net is False
    assert renv.get("knowledge_network_egress_forbidden") is True
    assert "runtime.action.execute.http" in (renv.get("knowledge_derived_deny_actions") or [])


@pytest.mark.parametrize("use_llm", [False, True])
def test_ingest_index_extract_ir_envelope_secrets_pii_subprocess_denial(
    tmp_path: Path,
    use_llm: bool,
) -> None:
    """Secrets/PII wording maps to subprocess denial (extends single-PII case in phase3 handoff).

    The Slack incident fixture includes RFC2119-style MUST NOT lines so ingest indexes
    doc-derived assertions alongside the chat transcript.
    """

    tenant_id = "tenant_kd_pii"
    repo_id = "repo_kd_pii"
    docs = _load_documents(subset_doc_ids=KD_DOC_IDS_SECURITY_NETWORK_MESSAGING)
    root = _scope_root(tmp_path, tenant_id=tenant_id, repo_id=repo_id)
    assert (
        merge_documents_into_assertion_index(
            scope_root=root,
            tenant_id=tenant_id,
            repo_id=repo_id,
            documents=docs,
            max_assertions_per_batch=64,
        )
        > 0
    )

    intent = _intent(
        tenant_id=tenant_id,
        repo_id=repo_id,
        constraints=(
            Constraint(
                id="ic_secret",
                kind="hard",
                statement=("Exporting customer API keys or secrets to external logs is forbidden."),
            ),
        ),
    )
    retrieved = {"documents": docs}
    retrieved_ids = {str(d["doc_id"]).strip() for d in docs}
    provenance = {
        did: {
            "kind": "message",
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
        stored_assertion_index_max_rows=64,
    )
    assert snap.canonical_constraints
    ir = _ir_document_from_snapshot(tenant_id=tenant_id, repo_id=repo_id, snapshot=snap)
    env = knowledge_runtime_envelope_from_ir(ir)
    assert env.get("knowledge_network_egress_forbidden") is False
    assert "runtime.action.execute.subprocess" in (env.get("knowledge_derived_deny_actions") or [])
    rules = env.get("knowledge_policy_rules")
    assert isinstance(rules, list) and rules
    classes = {str(x) for r in rules if isinstance(r, dict) for x in (r.get("classes") or [])}
    assert "secrets_pii" in classes
    expl = env.get("knowledge_explanations") or {}
    assert expl

    _, renv = effective_allow_network_for_handoff(ir_document=ir, intent_spec=intent)
    assert "runtime.action.execute.subprocess" in (renv.get("knowledge_derived_deny_actions") or [])
