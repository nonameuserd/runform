"""Retriever hooks (Phase 2).

Phase 2 retrieval is intentionally simple: pull recent code memory items and
optionally include why-graph constraints/decisions for the tenant+repo scope.

Phase 3: optional compact knowledge (prior snapshot) augments the index query
text; tenant/repo scope is unchanged. Post-retrieval, evidence-linked docs can
be score-boosted using the current compile snapshot (see
``boost_retrieved_documents_for_knowledge_evidence``).
"""

from __future__ import annotations

import math
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from akc.compile.interfaces import Index, IndexQuery, TenantRepoScope
from akc.compile.ir_prompt_context import ir_structural_hints_for_retrieval_query
from akc.ir import IRDocument
from akc.knowledge.models import KnowledgeSnapshot
from akc.memory.code_memory import CodeMemoryStore
from akc.memory.models import JSONValue, PlanState, normalize_repo_id, require_non_empty
from akc.memory.salience import (
    SalienceCandidate,
    build_extractive_compaction,
    load_memory_policy,
    pack_by_token_budget,
    score_candidates,
)
from akc.memory.why_graph import WhyGraphStore


def compact_knowledge_query_suffix(
    snapshot: KnowledgeSnapshot | None,
    *,
    max_chars: int = 1200,
) -> str:
    """Build a bounded text block of selected hard constraints for query augmentation.

    Uses the same tenant-scoped snapshot object only; does not widen retrieval.
    """

    if snapshot is None or max_chars <= 0:
        return ""
    decisions = {d.assertion_id: d for d in snapshot.canonical_decisions}
    lines: list[str] = []
    for c in sorted(snapshot.canonical_constraints, key=lambda x: x.assertion_id):
        if c.kind != "hard":
            continue
        dec = decisions.get(c.assertion_id)
        if dec is not None and not dec.selected:
            continue
        subj = str(c.subject).strip()
        summ = str(c.summary).strip()
        if subj and summ:
            lines.append(f"- {subj}: {summ}")
        elif summ:
            lines.append(f"- {summ}")
        elif subj:
            lines.append(f"- {subj}")
    if not lines:
        return ""
    header = "Knowledge constraints (compile-time, tenant-scoped):"
    body = "\n".join(lines)
    text = f"{header}\n{body}"
    if len(text) <= max_chars:
        return f"\n\n{text}"
    return f"\n\n{text[: max_chars - 3]}..."


def _build_query_text(
    plan: PlanState,
    *,
    ir_document: IRDocument | None = None,
    knowledge_query_suffix: str = "",
) -> str:
    goal = plan.goal.strip()
    title: str | None = None
    if plan.next_step_id is not None:
        for s in plan.steps:
            if s.id == plan.next_step_id:
                title = s.title.strip()
                break
    base = f"{goal}\n\nNext step: {title}" if title else goal
    if ir_document is None:
        out = base
    else:
        hints = ir_structural_hints_for_retrieval_query(ir_document)
        out = base if not hints.strip() else f"{base}\n\nIR structure: {hints}"
    if knowledge_query_suffix:
        return f"{out}{knowledge_query_suffix}"
    return out


def boost_retrieved_documents_for_knowledge_evidence(
    documents: Sequence[Mapping[str, Any]] | None,
    *,
    snapshot: KnowledgeSnapshot,
    boost_delta: float = 0.25,
) -> list[dict[str, Any]]:
    """Increase ``score`` for index rows whose ``doc_id`` appears in evidence mappings.

    Only boosts **selected** hard constraints' evidence doc ids. Deterministic:
    stable sort by (new_score desc, doc_id) after adjustment.
    """

    decisions = {d.assertion_id: d for d in snapshot.canonical_decisions}
    cited: set[str] = set()
    for c in snapshot.canonical_constraints:
        if c.kind != "hard":
            continue
        dec = decisions.get(c.assertion_id)
        if dec is not None and not dec.selected:
            continue
        ev = snapshot.evidence_by_assertion.get(c.assertion_id)
        if ev is None:
            continue
        cited.update(ev.evidence_doc_ids)

    if not cited:
        return [dict(d) for d in (documents or ())]

    out: list[dict[str, Any]] = []
    for raw in documents or ():
        row = dict(raw)
        did = str(row.get("doc_id", "")).strip()
        score_raw = row.get("score", 0.0)
        try:
            base_score = float(score_raw)
        except (TypeError, ValueError):
            base_score = 0.0
        if did and did in cited:
            row["score"] = base_score + float(boost_delta)
            row["knowledge_evidence_boost"] = True
        else:
            row["score"] = base_score
        out.append(row)

    out.sort(key=lambda r: (-float(r.get("score") or 0.0), str(r.get("doc_id", ""))))
    return out


def _clamp01(v: float) -> float:
    return max(0.0, min(1.0, float(v)))


def _trust_reliability(meta: Mapping[str, Any]) -> float:
    tier = str(meta.get("trust_tier") or meta.get("connector_trust_tier") or "default").strip().lower()
    if tier in {"trusted", "high"}:
        return 1.0
    if tier in {"untrusted", "low"}:
        return 0.25
    return 0.65


def _score_hint_to_relevance(score: Any) -> float | None:
    if isinstance(score, bool):
        return None
    if not isinstance(score, (int, float)):
        return None
    s = float(score)
    if not math.isfinite(s):
        return None
    # Conservative normalization for cosine-style and additive score ranges.
    if s <= 0.0:
        return 0.0
    if s >= 1.0:
        return 1.0
    return _clamp01(s)


def _apply_weighted_memory_ranking(
    *,
    query_text: str,
    docs: Sequence[Mapping[str, Any]],
    code_memory_items: Sequence[Mapping[str, Any]],
    knowledge_constraints: Sequence[Mapping[str, Any]] | None,
    now_ms: int,
    root: Path,
    policy_path: str | None,
    budget_tokens: int | None,
    pins: Sequence[str] | None,
    boosts: Mapping[str, float] | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any], dict[str, JSONValue] | None]:
    policy = load_memory_policy(root=root, policy_path=policy_path)

    candidates: list[SalienceCandidate] = []
    by_id_doc: dict[str, dict[str, Any]] = {}
    by_id_mem: dict[str, dict[str, Any]] = {}
    for raw in docs:
        row = dict(raw)
        did = str(row.get("doc_id") or "").strip()
        if not did:
            continue
        sid = f"document:{did}"
        meta_raw = row.get("metadata")
        meta = meta_raw if isinstance(meta_raw, Mapping) else {}
        created_at_raw = meta.get("indexed_at_ms")
        created_at = (
            int(created_at_raw)
            if isinstance(created_at_raw, (int, float)) and not isinstance(created_at_raw, bool)
            else now_ms
        )
        rel_hint = _score_hint_to_relevance(row.get("score"))
        importance = meta.get("importance")
        importance_f = (
            float(importance) if isinstance(importance, (int, float)) and not isinstance(importance, bool) else 0.60
        )
        usage_raw = meta.get("usage_count")
        usage = int(usage_raw) if isinstance(usage_raw, (int, float)) and not isinstance(usage_raw, bool) else 0
        candidates.append(
            SalienceCandidate(
                stable_id=sid,
                source="document",
                text=f"{str(row.get('title') or '')}\n{str(row.get('content') or '')}".strip(),
                created_at_ms=created_at,
                use_count=usage,
                pinned=bool(meta.get("pinned") is True),
                relevance_hint=rel_hint,
                importance=_clamp01(importance_f),
                reliability=_trust_reliability(meta),
                explicit_boost=0.0,
                metadata=meta,
            )
        )
        by_id_doc[sid] = row

    for raw in code_memory_items:
        row = dict(raw)
        item_id = str(row.get("item_id") or row.get("id") or "").strip()
        if not item_id:
            continue
        row.setdefault("item_id", item_id)
        sid = f"code_memory:{item_id}"
        metadata_raw = row.get("metadata")
        md = metadata_raw if isinstance(metadata_raw, Mapping) else {}
        created_at_raw = row.get("updated_at_ms")
        created_at = (
            int(created_at_raw)
            if isinstance(created_at_raw, (int, float)) and not isinstance(created_at_raw, bool)
            else now_ms
        )
        usage_raw = md.get("usage_count")
        usage = int(usage_raw) if isinstance(usage_raw, (int, float)) and not isinstance(usage_raw, bool) else 0
        kind = str(row.get("kind") or "").strip().lower()
        importance = 0.75 if kind in {"patch", "test_result"} else 0.55
        candidates.append(
            SalienceCandidate(
                stable_id=sid,
                source="code_memory",
                text=str(row.get("content") or ""),
                created_at_ms=created_at,
                use_count=usage,
                pinned=False,
                relevance_hint=None,
                importance=importance,
                reliability=0.90,
                explicit_boost=0.0,
                metadata=md if isinstance(md, Mapping) else None,
            )
        )
        by_id_mem[sid] = row

    for i, raw in enumerate(knowledge_constraints or ()):
        row = dict(raw)
        text = str(row.get("summary") or row.get("statement") or "").strip()
        if not text:
            continue
        cid = str(row.get("constraint_id") or row.get("assertion_id") or f"idx_{i}").strip()
        sid = f"knowledge:{cid}"
        candidates.append(
            SalienceCandidate(
                stable_id=sid,
                source="knowledge",
                text=text,
                created_at_ms=now_ms,
                importance=0.70,
                reliability=0.80,
            )
        )

    scored = score_candidates(
        candidates=candidates,
        query=query_text,
        policy=policy,
        now_ms=now_ms,
        pins=pins,
        boosts=boosts,
    )
    selected, evicted = pack_by_token_budget(
        scored=scored,
        budget_tokens=policy.budget_tokens(surface="compile", runtime_override=budget_tokens),
    )

    selected_ids = {s.candidate.stable_id for s in selected}
    selected_docs = [by_id_doc[s.candidate.stable_id] for s in selected if s.candidate.stable_id in by_id_doc]
    selected_mem = [by_id_mem[s.candidate.stable_id] for s in selected if s.candidate.stable_id in by_id_mem]
    ranking_trace = {
        "score_version": str(policy.score_version),
        "policy_fingerprint": policy.fingerprint(),
        "policy_path": policy.path,
        "selected_ids": [s.candidate.stable_id for s in selected],
        "evicted_ids": [e.candidate.stable_id for e in evicted],
        "budget_tokens": int(policy.budget_tokens(surface="compile", runtime_override=budget_tokens)),
        "scores": [
            {
                "memory_id": s.candidate.stable_id,
                "source": s.candidate.source,
                "total_score": float(s.total_score),
                "token_estimate": int(s.token_estimate),
                "selected": s.candidate.stable_id in selected_ids,
                "breakdown": {str(k): float(v) for k, v in s.score_breakdown.items()},
            }
            for s in scored
        ],
    }
    compaction_obj = build_extractive_compaction(evicted=evicted) if evicted else None
    return selected_docs, selected_mem, ranking_trace, compaction_obj


def retrieve_context(
    *,
    tenant_id: str,
    repo_id: str,
    plan: PlanState,
    code_memory: CodeMemoryStore,
    why_graph: WhyGraphStore | None = None,
    index: Index | None = None,
    limit: int = 20,
    ir_document: IRDocument | None = None,
    knowledge_snapshot_for_query: KnowledgeSnapshot | None = None,
    knowledge_query_budget_chars: int = 1200,
    weighted_memory_enabled: bool = False,
    weighted_memory_policy_path: str | None = None,
    weighted_memory_budget_tokens: int | None = None,
    weighted_memory_pins: Sequence[str] | None = None,
    weighted_memory_boosts: Mapping[str, float] | None = None,
    weighted_memory_policy_root: str | Path | None = None,
) -> dict[str, Any]:
    """Retrieve context for the compile loop.

    When ``knowledge_snapshot_for_query`` is set (e.g. prior step outputs), a
    compact constraint summary is appended to the index query text. Scope stays
    ``tenant_id`` / ``repo_id``; the index implementation must not widen beyond
    the existing ``TenantRepoScope`` query.
    """

    require_non_empty(tenant_id, name="tenant_id")
    repo = normalize_repo_id(repo_id)
    if plan.tenant_id != tenant_id or normalize_repo_id(plan.repo_id) != repo:
        raise ValueError("tenant_id/repo_id mismatch between arguments and plan")
    if limit <= 0:
        raise ValueError("limit must be > 0")
    if ir_document is not None:
        if ir_document.tenant_id.strip() != tenant_id.strip():
            raise ValueError("IR tenant_id must match plan tenant_id for retrieval queries")
        if normalize_repo_id(ir_document.repo_id) != repo:
            raise ValueError("IR repo_id must match plan repo_id for retrieval queries")

    mem_items = code_memory.list_items(tenant_id=tenant_id, repo_id=repo, limit=int(limit))
    mem_items_json: list[dict[str, Any]] = []
    for item in mem_items:
        row = dict(item.to_json_obj())
        if "item_id" not in row and isinstance(row.get("id"), str):
            row["item_id"] = str(row["id"])
        mem_items_json.append(row)

    why: dict[str, Any] | None = None
    if why_graph is not None:
        constraints = why_graph.list_nodes_by_type(tenant_id=tenant_id, repo_id=repo, node_type="constraint")
        decisions = why_graph.list_nodes_by_type(tenant_id=tenant_id, repo_id=repo, node_type="decision")
        why = {
            "constraints": [dict(n.payload) for n in constraints],
            "decisions": [dict(n.payload) for n in decisions],
        }

    suffix = compact_knowledge_query_suffix(
        knowledge_snapshot_for_query,
        max_chars=max(0, int(knowledge_query_budget_chars)),
    )
    docs: list[dict[str, Any]] = []
    if index is not None:
        scope = TenantRepoScope(tenant_id=tenant_id, repo_id=repo)
        q = IndexQuery(
            text=_build_query_text(plan, ir_document=ir_document, knowledge_query_suffix=suffix),
            k=int(limit),
            filters={"repo_id": repo},
        )
        results = index.query(scope=scope, query=q)
        docs = [
            {
                "doc_id": d.doc_id,
                "title": d.title,
                "content": d.content,
                "score": d.score,
                "metadata": dict(d.metadata or {}),
            }
            for d in results
        ]

    ranking_trace: dict[str, Any] | None = None
    compaction_obj: dict[str, JSONValue] | None = None
    if weighted_memory_enabled:
        root = (
            Path(weighted_memory_policy_root).expanduser().resolve()
            if weighted_memory_policy_root is not None
            else Path.cwd()
        )
        docs, mem_items_json, ranking_trace, compaction_obj = _apply_weighted_memory_ranking(
            query_text=_build_query_text(plan, ir_document=ir_document, knowledge_query_suffix=suffix),
            docs=docs,
            code_memory_items=mem_items_json,
            knowledge_constraints=(why or {}).get("constraints") if isinstance(why, dict) else None,
            now_ms=int(time.time() * 1000),
            root=root,
            policy_path=weighted_memory_policy_path,
            budget_tokens=weighted_memory_budget_tokens,
            pins=weighted_memory_pins,
            boosts=weighted_memory_boosts,
        )
        mem_items_out = mem_items_json
    else:
        mem_items_out = mem_items_json

    return {
        "code_memory_items": mem_items_out,
        "documents": docs,
        "why_graph": why,
        "memory_trace": ranking_trace,
        "memory_compaction": compaction_obj,
    }
