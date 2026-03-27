from __future__ import annotations

import hashlib
import json
import math
import re
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

from akc.compile.controller_config import (
    DocDerivedAssertionsMode,
    DocDerivedPatternOptions,
    KnowledgeConflictNormalization,
    KnowledgeEvidenceWeighting,
)
from akc.compile.interfaces import LLMBackend, LLMMessage, LLMRequest, LLMResponse, TenantRepoScope
from akc.compile.ir_prompt_context import compact_ir_document_for_prompt, ir_intent_knowledge_anchor_for_prompt
from akc.control.policy import KnowledgeUnresolvedConflictPolicy
from akc.intent.models import Constraint, IntentSpec
from akc.ir import IRDocument
from akc.ir.provenance import ProvenancePointer
from akc.knowledge.models import (
    AssertionKind,
    CanonicalConstraint,
    CanonicalDecision,
    EvidenceMapping,
    KnowledgeSnapshot,
)
from akc.path_security import safe_resolve_path

Predicate = Literal["required", "forbidden", "must_use", "must_not_use", "allowed"]

# Narrow tie-break for mediation when IR knowledge hubs list assertion ids (bounded).
_IR_KNOWLEDGE_HUB_ASSERTION_SCORE_BIAS = 1e-6

_MUTEX: dict[Predicate, set[Predicate]] = {
    "required": {"forbidden"},
    "forbidden": {"required"},
    "allowed": {"must_not_use"},
    "must_use": {"must_not_use"},
    "must_not_use": {"must_use", "allowed"},
}


_STOPWORDS = {
    "a",
    "an",
    "the",
    "is",
    "are",
    "was",
    "were",
    "be",
    "to",
    "of",
    "and",
    "or",
    "in",
    "for",
    "on",
    "with",
    "that",
    "this",
    "as",
    "at",
    "by",
    "from",
    "not",
    "no",
    "must",
    "should",
    "may",
    "might",
    "can",
    "will",
}


class KnowledgeExtractionError(Exception):
    """Raised when knowledge extraction cannot complete."""


def _require_non_empty(value: Any, *, name: str) -> str:
    s = str(value).strip()
    if not s:
        raise ValueError(f"{name} must be a non-empty value")
    return s


def _collapse_ws(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


_TOKEN_RE = re.compile(r"[a-zA-Z0-9_]+")

# Float score equality for grouping tied winners (deterministic mediation).
_SCORE_TIE_ABS_TOL = 1e-12


def _tokenize_for_overlap(text: str) -> set[str]:
    t = text.lower()
    tokens = {m.group(0) for m in _TOKEN_RE.finditer(t)}
    return {x for x in tokens if x and x not in _STOPWORDS and len(x) > 1}


def _select_evidence_doc_ids(
    *,
    constraint_text: str,
    documents: Sequence[Mapping[str, Any]],
    top_k: int = 3,
) -> tuple[str, ...]:
    if top_k <= 0:
        return ()
    if not constraint_text.strip():
        return ()
    tokens = _tokenize_for_overlap(constraint_text)
    if not tokens:
        return ()

    scored: list[tuple[float, str]] = []
    for d in documents:
        doc_id_raw = d.get("doc_id")
        if not isinstance(doc_id_raw, str) or not doc_id_raw.strip():
            continue
        doc_id = doc_id_raw.strip()
        content = str(d.get("content") or "")
        title = str(d.get("title") or "")
        doc_tokens = _tokenize_for_overlap(title + "\n" + content)
        score = float(len(tokens.intersection(doc_tokens)))
        if score > 0.0:
            scored.append((score, doc_id))
    scored.sort(key=lambda x: (-x[0], x[1]))
    return tuple([doc_id for _, doc_id in scored[: int(top_k)]])


def _normalize_scope(scope: Any, *, repo_id: str) -> str:
    if isinstance(scope, str) and scope.strip():
        return scope.strip()
    return repo_id


def _normalize_predicate(label: Any) -> Predicate:
    """Normalize predicate strings into the exact set used by why_conflicts."""
    raw = str(label or "").strip().lower()
    if not raw:
        # Fail-soft default: treat unknown labels as "required".
        return "required"

    # Treat underscores/hyphens as separators so keyword matching works for
    # phrases like "x_is_forbidden".
    search = raw.replace("_", " ").replace("-", " ")
    # A second normalized token-ish string (mainly for exact label matching).
    norm = search.replace(" ", "_")

    # Canonical labels already.
    if norm in {"required", "forbidden", "must_use", "must_not_use", "allowed"}:
        return norm  # type: ignore[return-value]

    # Synonyms.
    # Order matters: "must_not_use" must be detected before generic "must".
    if re.search(r"\bmust\b.*\bnot\b.*\buse\b|\bdo\b.*\bnot\b.*\buse\b", search):
        return "must_not_use"
    if re.search(r"\bforbidden\b|\bdisallowed\b|\bprohibited\b", search):
        return "forbidden"
    if re.search(r"\ballowed\b|\bpermitted\b", search):
        return "allowed"
    if re.search(r"\bmust\b.*\buse\b|\bshall\b.*\buse\b|\buse\b.*\bmust\b", search):
        return "must_use"
    if re.search(r"\brequired\b", search) or re.search(r"\bmust\b", search):
        return "required"
    # Best-effort fallback.
    return "required"


def _extract_subject_from_statement(
    *,
    statement: str,
    predicate: Predicate,
) -> str:
    """
    Best-effort subject extraction so mutex/polarity grouping works.

    We use patterns like:
    - "<subject> is required"
    - "<subject> must not use <object>"
    """
    st = _collapse_ws(statement)
    if not st:
        return ""

    patterns: list[re.Pattern[str]] = []
    if predicate == "required":
        patterns = [
            re.compile(r"^(?P<subject>.+?)\s+(is\s+)?required\b", re.IGNORECASE),
            re.compile(r"^(?P<subject>.+?)\s+(must\s+)?(be\s+)?required\b", re.IGNORECASE),
        ]
    elif predicate == "forbidden":
        patterns = [
            re.compile(r"^(?P<subject>.+?)\s+(is\s+)?forbidden\b", re.IGNORECASE),
            re.compile(r"^(?P<subject>.+?)\s+(is\s+)?disallowed\b", re.IGNORECASE),
            re.compile(r"^(?P<subject>.+?)\s+(is\s+)?prohibited\b", re.IGNORECASE),
        ]
    elif predicate == "allowed":
        patterns = [
            re.compile(r"^(?P<subject>.+?)\s+(is\s+)?allowed\b", re.IGNORECASE),
            re.compile(r"^(?P<subject>.+?)\s+(is\s+)?permitted\b", re.IGNORECASE),
        ]
    elif predicate == "must_use":
        patterns = [
            re.compile(r"^(?P<subject>.+?)\s+(must\s+)?use\b", re.IGNORECASE),
            re.compile(r"^(?P<subject>.+?)\s+shall\s+use\b", re.IGNORECASE),
        ]
    elif predicate == "must_not_use":
        patterns = [
            re.compile(r"^(?P<subject>.+?)\s+(must\s+)?not\s+use\b", re.IGNORECASE),
            re.compile(r"^(?P<subject>.+?)\s+do\s+not\s+use\b", re.IGNORECASE),
            re.compile(r"^(?P<subject>.+?)\s+must\s+not\s+use\b", re.IGNORECASE),
        ]

    for p in patterns:
        m = p.search(st)
        if m is not None:
            subject = m.group("subject").strip()
            subject = _collapse_ws(subject)
            return subject

    # Fallback: attempt to take the prefix before the keyword-ish portion.
    # This keeps subject stable enough for contradiction grouping.
    return _collapse_ws(st[: max(1, min(len(st), 64))])


def build_intent_constraint_ids_by_assertion(
    *,
    intent_spec: IntentSpec,
    repo_id: str,
    documents: Sequence[Mapping[str, Any]],
) -> dict[str, str]:
    """Map ``assertion_id`` -> intent ``Constraint.id`` for mediation / why-graph linkage."""

    out: dict[str, str] = {}
    for c in tuple(getattr(intent_spec, "constraints", ()) or ()):
        row = _deterministic_parse_constraint(repo_id=repo_id, constraint=c, documents=documents)
        out[row[0].assertion_id] = str(c.id)
    return out


def _ir_knowledge_hub_assertion_allowlist(ir_document: IRDocument | None) -> frozenset[str]:
    """Primary IR knowledge hub's ``knowledge_assertion_ids`` (bounded, deterministic)."""

    if ir_document is None:
        return frozenset()
    hubs = [n for n in ir_document.nodes if n.kind == "knowledge"]
    if not hubs:
        return frozenset()
    hub = sorted(hubs, key=lambda h: h.id.strip())[0]
    raw = hub.properties.get("knowledge_assertion_ids")
    out: list[str] = []
    if isinstance(raw, list):
        for x in raw[:128]:
            if isinstance(x, str) and x.strip():
                out.append(x.strip())
    return frozenset(out)


def _deterministic_parse_constraint(
    *,
    repo_id: str,
    constraint: Constraint,
    documents: Sequence[Mapping[str, Any]],
) -> tuple[CanonicalConstraint, tuple[str, ...], float]:
    """
    Deterministic (no-LLM) constraint -> canonical assertion.

    Returns:
    - CanonicalConstraint
    - evidence_doc_ids (best-effort)
    - evidence_score (used for deterministic conflict resolution)
    """
    statement = str(constraint.statement or "").strip()
    if not statement:
        # Never produce an empty summary; it affects semantic hashing.
        statement = f"(empty constraint statement: {constraint.id})"

    # Predicate detection: use statement keywords and kind it into the expected set.
    pred = _normalize_predicate(statement)

    # Negation:
    # - For "not required"/"not allowed" we model polarity=-1 while keeping the predicate.
    # - For "forbidden"/"must not use" we model predicate directly with polarity=+1.
    polarity: int = 1
    st_l = statement.lower()
    if pred in {"required", "allowed"} and re.search(r"\bnot\s+(required|allowed)\b", st_l):
        polarity = -1

    subject = _extract_subject_from_statement(statement=statement, predicate=pred)
    if not subject:
        subject = str(constraint.id)

    # Optional object: treat common "X[y]" suffix as object when subject extraction
    # keeps it out; this is best-effort and not guaranteed.
    obj: str | None = None
    m_obj = re.search(r"(?P<obj>[a-zA-Z0-9_]+\[[^\]]+\])", statement)
    if m_obj is not None:
        obj = m_obj.group("obj").strip()

    scope = repo_id

    kind: AssertionKind
    kind = "soft" if str(constraint.kind).strip() == "soft" else "hard"

    summary = statement
    canonical = CanonicalConstraint(
        subject=subject,
        predicate=pred,
        object=obj,
        polarity=polarity,
        scope=scope,
        kind=kind,
        summary=summary,
    )

    evidence_doc_ids = _select_evidence_doc_ids(
        constraint_text=statement,
        documents=documents,
        top_k=3,
    )

    # Evidence score: how many tokens overlap with the best doc(s).
    evidence_score = float(len(_tokenize_for_overlap(statement))) * (1.0 if len(evidence_doc_ids) > 0 else 0.0)
    # If we can cheaply re-score by document overlap, prefer that.
    if evidence_doc_ids:
        best = 0.0
        tokens = _tokenize_for_overlap(statement)
        for d in documents:
            doc_id_raw = d.get("doc_id")
            if not isinstance(doc_id_raw, str) or not doc_id_raw.strip():
                continue
            if doc_id_raw.strip() not in evidence_doc_ids:
                continue
            content = str(d.get("content") or "")
            title = str(d.get("title") or "")
            doc_tokens = _tokenize_for_overlap(title + "\n" + content)
            best = max(best, float(len(tokens.intersection(doc_tokens))))
        evidence_score = best

    return canonical, evidence_doc_ids, evidence_score


_MD_HEADER = re.compile(r"^#{1,6}\s+(.+?)\s*$")

_DOC_NORMATIVE_NEG = re.compile(r"(?is)\b(MUST\s+NOT|SHALL\s+NOT)\b\s+([A-Za-z0-9_\"'`][^\n.!?]{4,220})")
_DOC_NORMATIVE_POS = re.compile(r"(?is)\b(MUST|SHALL|REQUIRED\s+TO)\b\s+([A-Za-z0-9_\"'`][^\n.!?]{4,220})")

# Version for A4 metadata; bump when doc-derived heuristics change materially.
DOC_DERIVED_SOFT_EXTRACTOR_VERSION = "2"

# Extended BCP14 (optional): SHOULD / SHOULD NOT with the same tail discipline as MUST.
_DOC_RFC_BCP14_NEG = re.compile(r"(?is)\b(SHOULD\s+NOT)\b\s+([A-Za-z0-9_\"'`][^\n.!?]{4,220})")
_DOC_RFC_BCP14_POS = re.compile(r"(?is)\b(SHOULD)\b\s+(?!NOT\b)([A-Za-z0-9_\"'`][^\n.!?]{4,220})")

# Numbered requirements like "4.2.1 The component MUST ..." (RFC / spec style).
_NUMERIC_REQ_LINE = re.compile(
    r"(?m)^\s*((?:\d+\.){1,3}\s+[^\n]{0,120}?\b(?:MUST NOT|SHALL NOT|SHOULD NOT|MUST|SHALL|SHOULD)\b[^\n]{4,240})$"
)

# Markdown / pipe tables: row must include a normative keyword (high precision).
_MD_TABLE_NORMATIVE_ROW = re.compile(r"(?m)^(\s*\|[^\n]*\|[^\n]*\|[^\n]*)\s*$")


def _nearest_markdown_section_prefix(*, text: str, match_start: int) -> str:
    prefix = text[: int(match_start)]
    lines = prefix.splitlines()
    header = ""
    for ln in reversed(lines[-48:]):
        m = _MD_HEADER.match(ln.strip())
        if m:
            header = m.group(1).strip()
            break
    return f"[{header}] " if header else ""


def _append_soft_constraint_statement(
    *,
    statement: str,
    sort_pos: int,
    doc_id: str,
    blob: str,
    repo_id: str,
    skip_assertion_ids: set[str],
    captured: list[tuple[str, int, CanonicalConstraint, tuple[str, ...], float]],
) -> None:
    st = statement.strip()
    if len(st) < 10:
        return
    pred = _normalize_predicate(st)
    polarity: int = 1
    st_l = st.lower()
    if pred in {"required", "allowed"} and re.search(r"\bnot\s+(required|allowed)\b", st_l):
        polarity = -1
    subject = _extract_subject_from_statement(statement=st, predicate=pred)
    if not subject:
        subject = doc_id[:16]

    obj: str | None = None
    m_obj = re.search(r"(?P<obj>[a-zA-Z0-9_]+\[[^\]]+\])", st)
    if m_obj is not None:
        obj = m_obj.group("obj").strip()

    canonical = CanonicalConstraint(
        subject=subject,
        predicate=pred,
        object=obj,
        polarity=polarity,
        scope=repo_id,
        kind="soft",
        summary=st,
    )
    if canonical.assertion_id in skip_assertion_ids:
        return

    tokens = _tokenize_for_overlap(st)
    doc_tokens = _tokenize_for_overlap(blob)
    score = float(len(tokens.intersection(doc_tokens))) if tokens else 0.0
    if score <= 0.0:
        score = 1.0
    captured.append((doc_id, int(sort_pos), canonical, (doc_id,), score))


def _append_normative_doc_match(
    *,
    m: re.Match[str],
    doc_id: str,
    blob: str,
    repo_id: str,
    skip_assertion_ids: set[str],
    captured: list[tuple[str, int, CanonicalConstraint, tuple[str, ...], float]],
) -> None:
    span = m.group(0).strip()
    if len(span) < 10:
        return
    section = _nearest_markdown_section_prefix(text=blob, match_start=m.start())
    statement = (section + span).strip()
    _append_soft_constraint_statement(
        statement=statement,
        sort_pos=int(m.start()),
        doc_id=doc_id,
        blob=blob,
        repo_id=repo_id,
        skip_assertion_ids=skip_assertion_ids,
        captured=captured,
    )


def _extract_doc_derived_rows(
    *,
    repo_id: str,
    documents: Sequence[Mapping[str, Any]],
    max_assertions: int,
    skip_assertion_ids: set[str],
    patterns: DocDerivedPatternOptions | None = None,
) -> list[tuple[CanonicalConstraint, tuple[str, ...], float]]:
    """A2: deterministic normative phrases from retrieved chunks only (soft assertions)."""

    if max_assertions <= 0:
        return []

    po = patterns or DocDerivedPatternOptions()
    captured: list[tuple[str, int, CanonicalConstraint, tuple[str, ...], float]] = []

    for d in documents:
        doc_id_raw = d.get("doc_id")
        if not isinstance(doc_id_raw, str) or not doc_id_raw.strip():
            continue
        doc_id = doc_id_raw.strip()
        content = str(d.get("content") or "")
        title = str(d.get("title") or "")
        if not content.strip() and not title.strip():
            continue
        blob = f"{title}\n{content}".strip()

        neg_ranges: list[tuple[int, int]] = []

        for m in _DOC_NORMATIVE_NEG.finditer(blob):
            neg_ranges.append((m.start(), m.end()))
            _append_normative_doc_match(
                m=m,
                doc_id=doc_id,
                blob=blob,
                repo_id=repo_id,
                skip_assertion_ids=skip_assertion_ids,
                captured=captured,
            )
        if po.rfc2119_bcp14:
            for m in _DOC_RFC_BCP14_NEG.finditer(blob):
                neg_ranges.append((m.start(), m.end()))
                _append_normative_doc_match(
                    m=m,
                    doc_id=doc_id,
                    blob=blob,
                    repo_id=repo_id,
                    skip_assertion_ids=skip_assertion_ids,
                    captured=captured,
                )
        for m in _DOC_NORMATIVE_POS.finditer(blob):
            if any(not (m.end() <= s or m.start() >= e) for s, e in neg_ranges):
                continue
            _append_normative_doc_match(
                m=m,
                doc_id=doc_id,
                blob=blob,
                repo_id=repo_id,
                skip_assertion_ids=skip_assertion_ids,
                captured=captured,
            )
        if po.rfc2119_bcp14:
            for m in _DOC_RFC_BCP14_POS.finditer(blob):
                if any(not (m.end() <= s or m.start() >= e) for s, e in neg_ranges):
                    continue
                _append_normative_doc_match(
                    m=m,
                    doc_id=doc_id,
                    blob=blob,
                    repo_id=repo_id,
                    skip_assertion_ids=skip_assertion_ids,
                    captured=captured,
                )

        if po.numbered_requirements:
            for m in _NUMERIC_REQ_LINE.finditer(blob):
                line = str(m.group(1)).strip()
                section = _nearest_markdown_section_prefix(text=blob, match_start=m.start())
                statement = (section + line).strip()
                _append_soft_constraint_statement(
                    statement=statement,
                    sort_pos=int(m.start()),
                    doc_id=doc_id,
                    blob=blob,
                    repo_id=repo_id,
                    skip_assertion_ids=skip_assertion_ids,
                    captured=captured,
                )

        if po.table_normative_rows:
            for m in _MD_TABLE_NORMATIVE_ROW.finditer(blob):
                row = str(m.group(1)).strip()
                if row.count("|") < 2:
                    continue
                if not re.search(
                    r"\b(?:MUST NOT|SHALL NOT|SHOULD NOT|MUST|SHALL|SHOULD)\b",
                    row,
                    re.IGNORECASE,
                ):
                    continue
                section = _nearest_markdown_section_prefix(text=blob, match_start=m.start())
                statement = (section + row).strip()
                _append_soft_constraint_statement(
                    statement=statement,
                    sort_pos=int(m.start()),
                    doc_id=doc_id,
                    blob=blob,
                    repo_id=repo_id,
                    skip_assertion_ids=skip_assertion_ids,
                    captured=captured,
                )

    captured.sort(key=lambda row: (row[0], row[1], row[2].assertion_id))
    out: list[tuple[CanonicalConstraint, tuple[str, ...], float]] = []
    seen: set[str] = set()
    for _doc_id, _pos, canonical, eids, score in captured:
        if len(out) >= max_assertions:
            break
        aid = canonical.assertion_id
        if aid in seen:
            continue
        seen.add(aid)
        out.append((canonical, eids, float(score)))
    return out


def extract_doc_derived_soft_assertions_from_documents(
    *,
    repo_id: str,
    documents: Sequence[Mapping[str, Any]],
    max_assertions: int,
    skip_assertion_ids: set[str] | None = None,
    patterns: DocDerivedPatternOptions | None = None,
) -> list[tuple[CanonicalConstraint, tuple[str, ...], float]]:
    """Public entry point for A4 ingest-time indexing (same semantics as A2 doc-derived)."""

    return _extract_doc_derived_rows(
        repo_id=repo_id,
        documents=documents,
        max_assertions=int(max_assertions),
        skip_assertion_ids=set(skip_assertion_ids or ()),
        patterns=patterns,
    )


def _retrieved_doc_ids(*, documents: Sequence[Mapping[str, Any]]) -> set[str]:
    out: set[str] = set()
    for d in documents:
        if not isinstance(d, Mapping):
            continue
        did = d.get("doc_id")
        if isinstance(did, str) and did.strip():
            out.add(did.strip())
    return out


def _refresh_evidence_provenance(
    *,
    evidence_by_assertion: dict[str, EvidenceMapping],
    provenance_map: Mapping[str, ProvenancePointer],
) -> dict[str, EvidenceMapping]:
    out: dict[str, EvidenceMapping] = {}
    for aid, em in evidence_by_assertion.items():
        ids = em.evidence_doc_ids
        ptrs = tuple(provenance_map[d] for d in ids if d in provenance_map)
        out[aid] = EvidenceMapping(evidence_doc_ids=ids, resolved_provenance_pointers=ptrs)
    return out


def _merge_assertion_index_before_finalize(
    *,
    tenant_id: str,
    repo_id: str,
    knowledge_artifact_root: str | Path | None,
    stored_assertion_index_mode: Literal["off", "merge"],
    stored_assertion_index_max_rows: int,
    retrieved_doc_ids: set[str],
    provenance_map: Mapping[str, ProvenancePointer],
    canonical_constraints: tuple[CanonicalConstraint, ...],
    evidence_by_assertion: dict[str, EvidenceMapping],
    base_evidence_scores: dict[str, float],
) -> tuple[tuple[CanonicalConstraint, ...], dict[str, EvidenceMapping], dict[str, float]]:
    if stored_assertion_index_mode != "merge" or knowledge_artifact_root is None:
        return canonical_constraints, evidence_by_assertion, base_evidence_scores
    from akc.compile.assertion_index_store import (
        load_assertions_for_doc_ids,
        merge_indexed_assertions_into_snapshot_state,
    )

    root = safe_resolve_path(knowledge_artifact_root)
    idx_c, idx_e, idx_s = load_assertions_for_doc_ids(
        scope_root=root,
        tenant_id=tenant_id,
        repo_id=repo_id,
        doc_ids=retrieved_doc_ids,
        limit=int(stored_assertion_index_max_rows),
        provenance_map=provenance_map,
    )
    if not idx_c:
        return canonical_constraints, evidence_by_assertion, base_evidence_scores
    merged_c, merged_e, merged_s = merge_indexed_assertions_into_snapshot_state(
        canonical_constraints=canonical_constraints,
        evidence_by_assertion=evidence_by_assertion,
        base_evidence_scores=base_evidence_scores,
        indexed_constraints=idx_c,
        indexed_evidence=idx_e,
        indexed_scores=idx_s,
    )
    refreshed = _refresh_evidence_provenance(evidence_by_assertion=merged_e, provenance_map=provenance_map)
    return merged_c, refreshed, merged_s


def _apply_operator_decisions_optional(
    snapshot: KnowledgeSnapshot,
    *,
    tenant_id: str,
    repo_id: str,
    knowledge_artifact_root: str | Path | None,
    apply_operator_knowledge_decisions: bool,
) -> KnowledgeSnapshot:
    if not apply_operator_knowledge_decisions or knowledge_artifact_root is None:
        return snapshot
    from akc.knowledge.operator_decisions import apply_operator_decisions_to_snapshot, load_operator_knowledge_decisions

    overlay = load_operator_knowledge_decisions(
        scope_root=safe_resolve_path(knowledge_artifact_root),
        tenant_id=tenant_id,
        repo_id=repo_id,
    )
    if not overlay:
        return snapshot
    return apply_operator_decisions_to_snapshot(snapshot, overlay)


def _parse_json_strict(raw_text: str) -> Any:
    """
    Parse strict-ish JSON:
    - Prefer full-string parse
    - If that fails, try to extract the first JSON object/array
    """
    try:
        parsed = json.loads(raw_text)
        return parsed
    except Exception:
        pass

    # Best-effort extraction: locate the outermost JSON object.
    start = raw_text.find("{")
    end = raw_text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise KnowledgeExtractionError("LLM output is not valid JSON")
    candidate = raw_text[start : end + 1]
    return json.loads(candidate)


def _merge_evidence_from_decisions(
    *,
    decisions_raw: Any,
    constraint_by_assertion_id: Mapping[str, CanonicalConstraint],
    evidence_by_assertion_id: dict[str, EvidenceMapping],
    provenance_map: Mapping[str, ProvenancePointer],
) -> None:
    """
    If an LLM decision object includes evidence_doc_ids for an authoritative
    constraint assertion, merge it into that constraint's EvidenceMapping.
    """
    if not isinstance(decisions_raw, list):
        return

    for d_raw in decisions_raw:
        if not isinstance(d_raw, dict):
            continue
        evidence_ids_raw = d_raw.get("evidence_doc_ids")
        if not isinstance(evidence_ids_raw, list):
            continue
        evidence_ids = [str(x).strip() for x in evidence_ids_raw if str(x).strip()]
        if not evidence_ids:
            continue

        # Common target fields (contract-preferred + older variants).
        targets: list[str] = []
        targets_raw = d_raw.get("conflict_resolution_target_assertion_ids")
        if isinstance(targets_raw, list):
            targets.extend([str(x).strip() for x in targets_raw if str(x).strip()])
        else:
            aid = d_raw.get("assertion_id")
            target_aid = d_raw.get("authoritative_constraint_id") or d_raw.get("target_assertion_id")
            if isinstance(target_aid, str) and target_aid.strip():
                targets.append(target_aid.strip())
            elif isinstance(aid, str) and aid.strip():
                targets.append(aid.strip())

        for target_aid in targets:
            if target_aid not in constraint_by_assertion_id:
                continue
            current = evidence_by_assertion_id.get(target_aid)
            resolved_ptrs_to_add: tuple[ProvenancePointer, ...] = tuple(
                provenance_map[eid] for eid in evidence_ids if eid in provenance_map
            )
            if current is None:
                evidence_by_assertion_id[target_aid] = EvidenceMapping(
                    evidence_doc_ids=tuple(evidence_ids),
                    resolved_provenance_pointers=resolved_ptrs_to_add,
                )
            else:
                merged_ids = tuple(list(current.evidence_doc_ids) + list(evidence_ids))

                merged_ptrs_set = set(current.resolved_provenance_pointers).union(set(resolved_ptrs_to_add))

                def _prov_key(p: ProvenancePointer) -> tuple[str, str, str, str]:
                    return (
                        str(p.kind),
                        str(p.source_id),
                        str(p.locator or ""),
                        str(p.sha256 or ""),
                    )

                merged_ptrs = tuple(sorted(merged_ptrs_set, key=_prov_key))
                evidence_by_assertion_id[target_aid] = EvidenceMapping(
                    evidence_doc_ids=merged_ids,
                    resolved_provenance_pointers=merged_ptrs,
                )


def _convert_provenance_mapping_values(
    *,
    retrieval_provenance_by_doc_id: Mapping[str, Any],
) -> dict[str, ProvenancePointer]:
    out: dict[str, ProvenancePointer] = {}
    for doc_id, v in retrieval_provenance_by_doc_id.items():
        if not isinstance(doc_id, str) or not doc_id.strip():
            continue
        doc_id = doc_id.strip()
        if isinstance(v, ProvenancePointer):
            out[doc_id] = v
            continue
        if isinstance(v, dict):
            try:
                out[doc_id] = ProvenancePointer.from_json_obj(v)
            except Exception:
                continue
    return out


@dataclass(frozen=True, slots=True)
class AssertionConflictResolutionMeta:
    """Single source of truth for knowledge-layer conflict grouping + winner selection."""

    winner_assertion_id: str
    resolution_rule: str
    participant_assertion_ids: tuple[str, ...]
    conflict_group_id: str | None = None
    mediation_resolved: bool = True
    intent_constraint_ids: tuple[str, ...] = ()


def _normalized_synonym_lookup(m: Mapping[str, str] | None) -> dict[str, str]:
    if m is None:
        return {}
    return {_collapse_ws(str(k)).lower(): _collapse_ws(str(v)).strip() for k, v in m.items()}


def _normalize_subject_for_grouping(subject: str, norm: KnowledgeConflictNormalization | None) -> str:
    s = _collapse_ws(subject.strip())
    if norm is not None and norm.lowercase_subjects:
        s = s.lower()
    sm = _normalized_synonym_lookup(norm.subject_synonyms if norm is not None else None)
    lk = _collapse_ws(subject.strip()).lower()
    if lk in sm:
        return sm[lk]
    return s


def _normalize_object_for_grouping(obj: str | None, norm: KnowledgeConflictNormalization | None) -> str | None:
    if obj is None:
        return None
    s = _collapse_ws(str(obj).strip())
    om = _normalized_synonym_lookup(norm.object_synonyms if norm is not None else None)
    lk = _collapse_ws(str(obj).strip()).lower()
    if lk in om:
        return om[lk]
    return s


def _polarity_group_key(
    c: CanonicalConstraint,
    *,
    norm: KnowledgeConflictNormalization | None,
    subject_for_grouping: Mapping[str, str],
) -> tuple[str, str, str | None, str]:
    subj = subject_for_grouping.get(c.assertion_id, _normalize_subject_for_grouping(c.subject, norm))
    obj: str | None = c.object
    if c.predicate in ("must_use", "must_not_use"):
        obj = _normalize_object_for_grouping(obj, norm)
    return (subj, str(c.predicate), obj, str(c.scope))


def _mutex_group_key(
    c: CanonicalConstraint,
    *,
    norm: KnowledgeConflictNormalization | None,
    subject_for_grouping: Mapping[str, str],
) -> tuple[str, str]:
    subj = subject_for_grouping.get(c.assertion_id, _normalize_subject_for_grouping(c.subject, norm))
    return (subj, str(c.scope))


def _deterministic_embedding_vector(text: str, *, dim: int = 64) -> tuple[float, ...]:
    """Tenant-local deterministic pseudo-embedding for optional clustering (no network)."""

    t = _collapse_ws(text).lower().encode("utf-8")
    out: list[float] = []
    for i in range(int(dim)):
        h = hashlib.sha256(f"akc.knowledge.embed:{i}:".encode() + t).digest()
        val = int.from_bytes(h[:4], "big") / float(2**32)
        out.append(val * 2.0 - 1.0)
    return tuple(out)


def _cosine_similarity(a: Sequence[float], b: Sequence[float]) -> float:
    if len(a) != len(b) or not a:
        return 0.0
    dot = sum(float(x) * float(y) for x, y in zip(a, b, strict=True))
    na = math.sqrt(sum(float(x) * float(x) for x in a))
    nb = math.sqrt(sum(float(y) * float(y) for y in b))
    if na <= 0.0 or nb <= 0.0:
        return 0.0
    return dot / (na * nb)


def _predicates_mutex_pair(p: str, q: str) -> bool:
    pp = cast(Predicate, p)
    qq = cast(Predicate, q)
    return qq in _MUTEX.get(pp, set()) or pp in _MUTEX.get(qq, set())


def _embedding_merge_eligible(a: CanonicalConstraint, b: CanonicalConstraint) -> bool:
    """Whether two rows might contradict if they refer to the same underlying subject."""

    if a.scope != b.scope:
        return False
    return _predicates_mutex_pair(str(a.predicate), str(b.predicate)) or (
        str(a.predicate) == str(b.predicate) and a.polarity != b.polarity
    )


def _embedding_subject_cluster_map(
    constraints: Sequence[CanonicalConstraint],
    *,
    norm: KnowledgeConflictNormalization | None,
    enabled: bool,
    threshold: float,
) -> dict[str, str]:
    """Map assertion_id -> effective subject string for mutex/polarity grouping."""

    base: dict[str, str] = {c.assertion_id: _normalize_subject_for_grouping(c.subject, norm) for c in constraints}
    if not enabled or len(constraints) < 2:
        return base

    aids = [c.assertion_id for c in constraints]
    vecs = {c.assertion_id: _deterministic_embedding_vector(c.summary) for c in constraints}
    parent: dict[str, str] = {a: a for a in aids}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x: str, y: str) -> None:
        rx, ry = find(x), find(y)
        if rx != ry:
            parent[ry] = rx

    for i, ci in enumerate(constraints):
        for cj in constraints[i + 1 :]:
            if not _embedding_merge_eligible(ci, cj):
                continue
            if _cosine_similarity(vecs[ci.assertion_id], vecs[cj.assertion_id]) < float(threshold):
                continue
            union(ci.assertion_id, cj.assertion_id)

    clusters: dict[str, set[str]] = {}
    for a in aids:
        r = find(a)
        clusters.setdefault(r, set()).add(a)

    rep_subject: dict[str, str] = {}
    for root, members in clusters.items():
        subs = sorted(
            {base[m] for m in members},
            key=lambda s: (s,),
        )
        rep_subject[root] = subs[0]

    out = dict(base)
    for root, members in clusters.items():
        canon = rep_subject[root]
        for m in members:
            out[m] = canon
    return out


def _stable_conflict_group_id(participant_assertion_ids: tuple[str, ...]) -> str:
    h = hashlib.sha256(json.dumps(list(participant_assertion_ids), sort_keys=True).encode("utf-8")).hexdigest()
    return f"cg_{h[:16]}"


def _temporal_tuple_from_doc(doc: Mapping[str, Any]) -> tuple[int, int, int, int]:
    """Higher tuple compares newer/stronger for supersession tie-breaks."""

    meta_raw = doc.get("metadata")
    meta: dict[str, Any] = meta_raw if isinstance(meta_raw, dict) else {}
    idx_ms = 0
    raw_idx = meta.get("indexed_at_ms")
    if isinstance(raw_idx, (int, float)) and not isinstance(raw_idx, bool):
        idx_ms = max(0, int(raw_idx))
    eff_ms = 0
    for k in ("effective_date_ms", "effective_at_ms"):
        raw_e = meta.get(k)
        if isinstance(raw_e, (int, float)) and not isinstance(raw_e, bool):
            eff_ms = max(eff_ms, int(raw_e))
            break
    raw_ed = meta.get("effective_date")
    if eff_ms == 0 and isinstance(raw_ed, str) and raw_ed.strip():
        digits = "".join(ch for ch in raw_ed if ch.isdigit())
        if len(digits) >= 8:
            eff_ms = int(digits[:8])  # YYYYMMDD as coarse key
    ver = 0
    for vk in ("doc_version", "version"):
        raw_v = meta.get(vk)
        if isinstance(raw_v, int):
            ver = max(ver, int(raw_v))
            break
        if isinstance(raw_v, str) and raw_v.strip().isdigit():
            ver = max(ver, int(raw_v.strip()))
            break
    pinned = 0
    p = meta.get("pinned")
    if p is True or (isinstance(p, str) and p.strip().lower() in {"1", "true", "yes"}):
        pinned = 1
    return (idx_ms, eff_ms, ver, pinned)


def _max_temporal_tuple_for_assertion(
    assertion_id: str,
    *,
    evidence_by_assertion: Mapping[str, EvidenceMapping],
    documents_by_id: Mapping[str, Mapping[str, Any]],
) -> tuple[int, int, int, int]:
    em = evidence_by_assertion.get(assertion_id)
    if em is None:
        return (0, 0, 0, 0)
    best = (0, 0, 0, 0)
    for did in em.evidence_doc_ids:
        doc = documents_by_id.get(did)
        if doc is None:
            continue
        best = max(best, _temporal_tuple_from_doc(doc))
    return best


def _documents_by_doc_id(documents: Sequence[Mapping[str, Any]]) -> dict[str, Mapping[str, Any]]:
    out: dict[str, Mapping[str, Any]] = {}
    for d in documents:
        if not isinstance(d, Mapping):
            continue
        raw = d.get("doc_id")
        if isinstance(raw, str) and raw.strip():
            out[raw.strip()] = d
    return out


def _doc_metadata_bonus(
    *,
    doc: Mapping[str, Any],
    weighting: KnowledgeEvidenceWeighting,
    compile_now_ms: int | None,
) -> float:
    """Single-document trust/recency/pinned/version boost (tenant-scoped chunk metadata only)."""

    meta_raw = doc.get("metadata")
    meta: dict[str, Any] = meta_raw if isinstance(meta_raw, dict) else {}
    tiers = weighting.resolved_trust_tier_bonus()
    tier_key = str(meta.get("trust_tier") or meta.get("connector_trust_tier") or "default").strip().lower()
    bonus = float(tiers.get(tier_key, tiers.get("default", 0.0)))

    pinned = meta.get("pinned")
    if pinned is True or (isinstance(pinned, str) and pinned.strip().lower() in {"1", "true", "yes"}):
        bonus += float(weighting.pinned_bonus)

    idx_ms: int | None = None
    raw_idx = meta.get("indexed_at_ms")
    if isinstance(raw_idx, bool):
        idx_ms = None
    elif isinstance(raw_idx, (int, float)):
        idx_ms = int(raw_idx)

    halflife = float(weighting.recency_halflife_days)
    if idx_ms is not None and halflife > 0.0 and compile_now_ms is not None:
        age_days = max(0.0, (float(compile_now_ms) - float(idx_ms)) / 86400000.0)
        bonus += float(weighting.recency_max_bonus) * math.exp(-age_days / halflife)

    for ver_key in ("doc_version", "version"):
        raw_v = meta.get(ver_key)
        if isinstance(raw_v, int):
            step = max(0, int(raw_v))
            bonus += float(weighting.doc_version_step_bonus) * float(min(step, 100))
            break
        if isinstance(raw_v, str) and raw_v.strip().isdigit():
            step = int(raw_v.strip())
            bonus += float(weighting.doc_version_step_bonus) * float(min(max(0, step), 100))
            break

    return bonus


def enrich_evidence_scores_with_doc_metadata(
    *,
    base_scores: Mapping[str, float],
    evidence_by_assertion: Mapping[str, EvidenceMapping],
    documents_by_id: Mapping[str, Mapping[str, Any]],
    weighting: KnowledgeEvidenceWeighting | None,
    compile_now_ms: int | None,
) -> dict[str, float]:
    """Add max per-evidence-doc metadata bonus to each assertion's baseline score (B1)."""

    if weighting is None:
        return {k: float(v) for k, v in base_scores.items()}
    out: dict[str, float] = {}
    for aid, base in base_scores.items():
        b = float(base)
        mapping = evidence_by_assertion.get(aid)
        if mapping is None or not mapping.evidence_doc_ids:
            out[aid] = b
            continue
        max_meta = 0.0
        for did in mapping.evidence_doc_ids:
            doc = documents_by_id.get(did)
            if doc is None:
                continue
            max_meta = max(max_meta, _doc_metadata_bonus(doc=doc, weighting=weighting, compile_now_ms=compile_now_ms))
        out[aid] = b + max_meta
    return out


def evidence_scores_for_conflict_resolution(*, snapshot: KnowledgeSnapshot) -> dict[str, float]:
    """Scores used by `compute_assertion_conflict_resolution_metadata` and `_compute_canonical_decisions`.

    When the snapshot carries explicit strengths (deterministic extraction), those win.
    Otherwise infer a deterministic proxy from evidence doc-id cardinality.
    """

    if snapshot.evidence_strength_by_assertion:
        return dict(snapshot.evidence_strength_by_assertion)
    out: dict[str, float] = {}
    for c in snapshot.canonical_constraints:
        m = snapshot.evidence_by_assertion.get(c.assertion_id)
        n = len(m.evidence_doc_ids) if m is not None else 0
        out[c.assertion_id] = float(n)
    return out


@dataclass(frozen=True, slots=True)
class _ConflictWinnerPick:
    winner_assertion_id: str
    mediation_resolved: bool
    tie_break: str | None


def _select_conflict_group_winner(
    cands: Sequence[CanonicalConstraint],
    *,
    evidence_scores: Mapping[str, float],
    evidence_doc_counts: Mapping[str, int],
    temporal_by_assertion: Mapping[str, tuple[int, int, int, int]],
    unresolved_policy: KnowledgeUnresolvedConflictPolicy,
    resolution_rule: str,
    participant_assertion_ids: tuple[str, ...],
    conflict_group_id: str,
    mediation_events: list[dict[str, Any]] | None,
    evidence_by_assertion: Mapping[str, EvidenceMapping] | None,
    documents_by_id: Mapping[str, Mapping[str, Any]] | None,
    intent_constraint_ids_by_assertion: Mapping[str, str] | None,
) -> _ConflictWinnerPick:
    """Highest composite score wins; temporal supersession; hard beats soft; policy handles residual ties (B2)."""

    if not cands:
        raise KnowledgeExtractionError("empty conflict candidate set")

    def score_of(c: CanonicalConstraint) -> float:
        return float(evidence_scores.get(c.assertion_id, 0.0))

    def temporal_of(c: CanonicalConstraint) -> tuple[int, int, int, int]:
        return temporal_by_assertion.get(
            c.assertion_id,
            _max_temporal_tuple_for_assertion(
                c.assertion_id,
                evidence_by_assertion=evidence_by_assertion or {},
                documents_by_id=documents_by_id or {},
            ),
        )

    max_s = max(score_of(c) for c in cands)
    tier1 = [c for c in cands if math.isclose(score_of(c), max_s, rel_tol=0.0, abs_tol=_SCORE_TIE_ABS_TOL)]
    max_t = max(temporal_of(c) for c in tier1)
    tier1b = [c for c in tier1 if temporal_of(c) == max_t]
    max_h = max(1 if c.kind == "hard" else 0 for c in tier1b)
    tier2 = [c for c in tier1b if (1 if c.kind == "hard" else 0) == max_h]

    if len(tier2) == 1:
        winner = tier2[0].assertion_id
        if mediation_events is not None and evidence_by_assertion is not None and documents_by_id is not None:
            wt = temporal_of(tier2[0])
            for c in cands:
                if c.assertion_id == winner:
                    continue
                lt = temporal_of(c)
                if lt < wt:
                    mediation_events.append(
                        {
                            "kind": "supersedes",
                            "conflict_group_id": conflict_group_id,
                            "resolution_rule": resolution_rule,
                            "winner_assertion_id": winner,
                            "loser_assertion_id": c.assertion_id,
                            "reason": "temporal_precedence",
                        }
                    )
        return _ConflictWinnerPick(winner_assertion_id=winner, mediation_resolved=True, tie_break=None)

    if unresolved_policy == "fail_closed":
        raise KnowledgeExtractionError(
            "knowledge_conflict_ambiguous_tie: cannot resolve contradiction group under policy "
            f"fail_closed (participants={sorted({c.assertion_id for c in tier2})})"
        )

    if unresolved_policy == "defer_to_intent":
        tier2_sorted = sorted(
            tier2,
            key=lambda c: (int(evidence_doc_counts.get(c.assertion_id, 0)), c.assertion_id),
        )
        winner = tier2_sorted[0].assertion_id
        tie_break = "defer_to_intent_min_evidence_doc_count"
        intent_ids: list[str] = []
        if intent_constraint_ids_by_assertion is not None:
            for aid in participant_assertion_ids:
                raw = intent_constraint_ids_by_assertion.get(aid)
                if isinstance(raw, str) and raw.strip():
                    intent_ids.append(raw.strip())
        if mediation_events is not None:
            ev: dict[str, Any] = {
                "kind": "ambiguous_conflict_resolution",
                "conflict_group_id": conflict_group_id,
                "resolution_rule": resolution_rule,
                "participant_assertion_ids": list(participant_assertion_ids),
                "chosen_assertion_id": winner,
                "tie_break": tie_break,
                "composite_score_max": max_s,
                "mediation_resolved": False,
                "defer_to_intent": True,
            }
            if intent_ids:
                ev["intent_constraint_ids"] = sorted(set(intent_ids))
            mediation_events.append(ev)
        return _ConflictWinnerPick(
            winner_assertion_id=winner,
            mediation_resolved=False,
            tie_break=tie_break,
        )

    tier2_sorted = sorted(tier2, key=lambda c: c.assertion_id)
    winner = tier2_sorted[0].assertion_id
    tie_break = "lexicographic_assertion_id"
    if mediation_events is not None:
        mediation_events.append(
            {
                "kind": "ambiguous_conflict_resolution",
                "conflict_group_id": conflict_group_id,
                "resolution_rule": resolution_rule,
                "participant_assertion_ids": list(participant_assertion_ids),
                "chosen_assertion_id": winner,
                "tie_break": tie_break,
                "composite_score_max": max_s,
                "mediation_resolved": True,
            }
        )
    return _ConflictWinnerPick(winner_assertion_id=winner, mediation_resolved=True, tie_break=tie_break)


def compute_assertion_conflict_resolution_metadata(
    *,
    constraints: Sequence[CanonicalConstraint],
    evidence_scores: Mapping[str, float],
    unresolved_policy: KnowledgeUnresolvedConflictPolicy = "warn_and_continue",
    evidence_doc_counts_by_assertion: Mapping[str, int] | None = None,
    mediation_events: list[dict[str, Any]] | None = None,
    normalization: KnowledgeConflictNormalization | None = None,
    evidence_by_assertion: Mapping[str, EvidenceMapping] | None = None,
    documents_by_id: Mapping[str, Mapping[str, Any]] | None = None,
    embedding_clustering_enabled: bool = False,
    embedding_cluster_threshold: float = 0.92,
    intent_constraint_ids_by_assertion: Mapping[str, str] | None = None,
) -> dict[str, AssertionConflictResolutionMeta]:
    """Enumerate polarity and mutex-predicate conflicts with a single winner rule.

    Shared by `_compute_canonical_decisions` and `why_graph_writer` so resolution edges
    cannot drift from snapshot decisions.
    """

    meta: dict[str, AssertionConflictResolutionMeta] = {}
    counts = evidence_doc_counts_by_assertion or {}
    doc_map = documents_by_id or {}
    ev_map = evidence_by_assertion or {}

    subject_for_grouping = _embedding_subject_cluster_map(
        constraints,
        norm=normalization,
        enabled=embedding_clustering_enabled,
        threshold=float(embedding_cluster_threshold),
    )

    temporal_by_assertion: dict[str, tuple[int, int, int, int]] = {}
    for c in constraints:
        temporal_by_assertion[c.assertion_id] = _max_temporal_tuple_for_assertion(
            c.assertion_id,
            evidence_by_assertion=ev_map,
            documents_by_id=doc_map,
        )

    groups: dict[tuple[str, str, str | None, str], list[CanonicalConstraint]] = {}
    for c in constraints:
        key = _polarity_group_key(c, norm=normalization, subject_for_grouping=subject_for_grouping)
        groups.setdefault(key, []).append(c)

    for _grp_key, cands in groups.items():
        polarities = {c.polarity for c in cands}
        if 1 not in polarities or -1 not in polarities:
            continue
        participant_ids = tuple(sorted({c.assertion_id for c in cands}))
        cg_id = _stable_conflict_group_id(participant_ids)
        pick = _select_conflict_group_winner(
            cands,
            evidence_scores=evidence_scores,
            evidence_doc_counts=counts,
            temporal_by_assertion=temporal_by_assertion,
            unresolved_policy=unresolved_policy,
            resolution_rule="polarity_contradiction_evidence_winner",
            participant_assertion_ids=participant_ids,
            conflict_group_id=cg_id,
            mediation_events=mediation_events,
            evidence_by_assertion=ev_map if ev_map else None,
            documents_by_id=doc_map if doc_map else None,
            intent_constraint_ids_by_assertion=intent_constraint_ids_by_assertion,
        )
        intent_ids: tuple[str, ...] = ()
        if intent_constraint_ids_by_assertion is not None:
            tmp: list[str] = []
            for aid in participant_ids:
                raw = intent_constraint_ids_by_assertion.get(aid)
                if isinstance(raw, str) and raw.strip():
                    tmp.append(raw.strip())
            intent_ids = tuple(sorted(set(tmp)))
        for c in cands:
            meta[c.assertion_id] = AssertionConflictResolutionMeta(
                winner_assertion_id=pick.winner_assertion_id,
                resolution_rule="polarity_contradiction_evidence_winner",
                participant_assertion_ids=participant_ids,
                conflict_group_id=cg_id,
                mediation_resolved=pick.mediation_resolved,
                intent_constraint_ids=intent_ids,
            )

    groups2: dict[tuple[str, str], list[CanonicalConstraint]] = {}
    for c in constraints:
        k = _mutex_group_key(c, norm=normalization, subject_for_grouping=subject_for_grouping)
        groups2[k] = groups2.get(k, []) + [c]

    for (_subject, _scope), cands in groups2.items():
        present_preds = {c.predicate for c in cands}
        conflicting_preds: set[str] = set()
        for p in present_preds:
            pred = cast(Predicate, p)
            if pred in _MUTEX:
                conflicting_preds.add(str(p))
                conflicting_preds |= {str(x) for x in _MUTEX[pred]}
        if len(conflicting_preds) < 2:
            continue
        participants = [c for c in cands if c.predicate in conflicting_preds]
        if not participants:
            continue
        unresolved = [c for c in participants if c.assertion_id not in meta]
        if not unresolved:
            continue
        participant_ids = tuple(sorted({c.assertion_id for c in participants}))
        cg_id = _stable_conflict_group_id(participant_ids)
        pick = _select_conflict_group_winner(
            unresolved,
            evidence_scores=evidence_scores,
            evidence_doc_counts=counts,
            temporal_by_assertion=temporal_by_assertion,
            unresolved_policy=unresolved_policy,
            resolution_rule="mutex_predicate_evidence_winner",
            participant_assertion_ids=participant_ids,
            conflict_group_id=cg_id,
            mediation_events=mediation_events,
            evidence_by_assertion=ev_map if ev_map else None,
            documents_by_id=doc_map if doc_map else None,
            intent_constraint_ids_by_assertion=intent_constraint_ids_by_assertion,
        )
        intent_ids_mutex: tuple[str, ...] = ()
        if intent_constraint_ids_by_assertion is not None:
            tmp2: list[str] = []
            for aid in participant_ids:
                raw = intent_constraint_ids_by_assertion.get(aid)
                if isinstance(raw, str) and raw.strip():
                    tmp2.append(raw.strip())
            intent_ids_mutex = tuple(sorted(set(tmp2)))
        for c in unresolved:
            meta[c.assertion_id] = AssertionConflictResolutionMeta(
                winner_assertion_id=pick.winner_assertion_id,
                resolution_rule="mutex_predicate_evidence_winner",
                participant_assertion_ids=participant_ids,
                conflict_group_id=cg_id,
                mediation_resolved=pick.mediation_resolved,
                intent_constraint_ids=intent_ids_mutex,
            )

    return meta


def _compute_canonical_decisions(
    *,
    constraints: Sequence[CanonicalConstraint],
    evidence_scores: Mapping[str, float],
    evidence_doc_ids_by_aid: Mapping[str, tuple[str, ...]],
    unresolved_policy: KnowledgeUnresolvedConflictPolicy = "warn_and_continue",
    evidence_doc_counts_by_assertion: Mapping[str, int] | None = None,
    mediation_events: list[dict[str, Any]] | None = None,
    normalization: KnowledgeConflictNormalization | None = None,
    evidence_by_assertion: Mapping[str, EvidenceMapping] | None = None,
    documents_by_id: Mapping[str, Mapping[str, Any]] | None = None,
    embedding_clustering_enabled: bool = False,
    embedding_cluster_threshold: float = 0.92,
    intent_constraint_ids_by_assertion: Mapping[str, str] | None = None,
) -> tuple[CanonicalDecision, ...]:
    """
    Deterministic conflict resolution:
    - If both polarity+predicate assertions exist for the same (subject,predicate,object,scope),
      choose the evidence-best one.
    - For mutex predicate conflicts, choose the evidence-best one.

    CanonicalDecision is currently per-assertion and encodes only selected/resolved.
    """
    meta = compute_assertion_conflict_resolution_metadata(
        constraints=constraints,
        evidence_scores=evidence_scores,
        unresolved_policy=unresolved_policy,
        evidence_doc_counts_by_assertion=evidence_doc_counts_by_assertion,
        mediation_events=mediation_events,
        normalization=normalization,
        evidence_by_assertion=evidence_by_assertion,
        documents_by_id=documents_by_id,
        embedding_clustering_enabled=embedding_clustering_enabled,
        embedding_cluster_threshold=embedding_cluster_threshold,
        intent_constraint_ids_by_assertion=intent_constraint_ids_by_assertion,
    )
    if not meta:
        return ()
    out: list[CanonicalDecision] = []
    for aid in sorted(meta.keys()):
        row = meta[aid]
        winner = row.winner_assertion_id
        out.append(
            CanonicalDecision(
                assertion_id=aid,
                selected=bool(aid == winner),
                resolved=bool(row.mediation_resolved),
                conflict_resolution_target_assertion_ids=(winner,),
                evidence_doc_ids=evidence_doc_ids_by_aid.get(aid, ()),
            )
        )
    return tuple(out)


def _finalize_knowledge_snapshot_conflicts(
    *,
    canonical_constraints: tuple[CanonicalConstraint, ...],
    evidence_by_assertion: dict[str, EvidenceMapping],
    base_evidence_scores: dict[str, float],
    documents: Sequence[Mapping[str, Any]],
    knowledge_evidence_weighting: KnowledgeEvidenceWeighting | None,
    knowledge_unresolved_conflict_policy: KnowledgeUnresolvedConflictPolicy,
    compile_now_ms: int | None,
    mediation_report_out: dict[str, Any] | None,
    knowledge_conflict_normalization: KnowledgeConflictNormalization | None = None,
    knowledge_embedding_clustering_enabled: bool = False,
    knowledge_embedding_clustering_threshold: float = 0.92,
    intent_constraint_ids_by_assertion: Mapping[str, str] | None = None,
    ir_knowledge_hub_assertion_ids: frozenset[str] | None = None,
) -> KnowledgeSnapshot:
    """Apply B1 score enrichment + B2 conflict mediation; optional mediation JSON for operators."""

    now_ms = int(time.time() * 1000) if compile_now_ms is None else int(compile_now_ms)
    doc_map = _documents_by_doc_id(documents)
    enriched = enrich_evidence_scores_with_doc_metadata(
        base_scores=base_evidence_scores,
        evidence_by_assertion=evidence_by_assertion,
        documents_by_id=doc_map,
        weighting=knowledge_evidence_weighting,
        compile_now_ms=now_ms,
    )
    if ir_knowledge_hub_assertion_ids:
        enriched = {k: float(v) for k, v in enriched.items()}
        for aid in ir_knowledge_hub_assertion_ids:
            if aid in enriched:
                enriched[aid] = float(enriched[aid]) + _IR_KNOWLEDGE_HUB_ASSERTION_SCORE_BIAS
    counts = {aid: len(evidence_by_assertion[aid].evidence_doc_ids) for aid in evidence_by_assertion}
    evidence_doc_ids_by_aid = {aid: evidence_by_assertion[aid].evidence_doc_ids for aid in evidence_by_assertion}
    events: list[dict[str, Any]] | None = [] if mediation_report_out is not None else None

    canonical_decisions = _compute_canonical_decisions(
        constraints=canonical_constraints,
        evidence_scores=enriched,
        evidence_doc_ids_by_aid=evidence_doc_ids_by_aid,
        unresolved_policy=knowledge_unresolved_conflict_policy,
        evidence_doc_counts_by_assertion=counts,
        mediation_events=events,
        normalization=knowledge_conflict_normalization,
        evidence_by_assertion=evidence_by_assertion,
        documents_by_id=doc_map,
        embedding_clustering_enabled=knowledge_embedding_clustering_enabled,
        embedding_cluster_threshold=knowledge_embedding_clustering_threshold,
        intent_constraint_ids_by_assertion=intent_constraint_ids_by_assertion,
    )
    snap = KnowledgeSnapshot(
        canonical_constraints=canonical_constraints,
        canonical_decisions=canonical_decisions,
        evidence_by_assertion=evidence_by_assertion,
        evidence_strength_by_assertion=enriched,
    )
    if mediation_report_out is not None:
        mediation_report_out.clear()
        mediation_report_out.update(
            {
                "policy": knowledge_unresolved_conflict_policy,
                "status": "ok",
                "events": list(events or ()),
                "normalization": knowledge_conflict_normalization.to_json_obj()
                if knowledge_conflict_normalization is not None
                else None,
                "embedding_clustering_enabled": bool(knowledge_embedding_clustering_enabled),
                "embedding_clustering_threshold": float(knowledge_embedding_clustering_threshold),
            }
        )
    return snap


def extract_knowledge_snapshot(
    *,
    tenant_id: str,
    repo_id: str,
    intent_spec: IntentSpec,
    retrieved_context: Mapping[str, Any],
    retrieval_provenance_by_doc_id: Mapping[str, Any],
    llm: LLMBackend | None = None,
    use_llm: bool = False,
    doc_derived_assertions_mode: DocDerivedAssertionsMode = "limited",
    doc_derived_max_assertions: int = 12,
    doc_derived_patterns: DocDerivedPatternOptions | None = None,
    knowledge_evidence_weighting: KnowledgeEvidenceWeighting | None = None,
    knowledge_unresolved_conflict_policy: KnowledgeUnresolvedConflictPolicy = "warn_and_continue",
    compile_now_ms: int | None = None,
    mediation_report_out: dict[str, Any] | None = None,
    knowledge_artifact_root: str | Path | None = None,
    knowledge_conflict_normalization: KnowledgeConflictNormalization | None = None,
    knowledge_embedding_clustering_enabled: bool = False,
    knowledge_embedding_clustering_threshold: float = 0.92,
    stored_assertion_index_mode: Literal["off", "merge"] = "off",
    stored_assertion_index_max_rows: int = 64,
    apply_operator_knowledge_decisions: bool = True,
    ir_document: IRDocument | None = None,
) -> KnowledgeSnapshot:
    """
    Stage-3 knowledge extraction and semantic unification.

    Fail-closed:
    - If LLM parsing/validation fails (when `use_llm=True`), we fall back to deterministic
      extraction (still evidence-referencing when possible).

    A2: When ``doc_derived_assertions_mode == "limited"``, merge up to
    ``doc_derived_max_assertions`` **soft** assertions parsed from retrieved chunks
    (in addition to intent constraints when present).
    """
    tenant = _require_non_empty(tenant_id, name="tenant_id")
    repo = _require_non_empty(repo_id, name="repo_id")
    documents_raw = retrieved_context.get("documents") or []
    documents: list[Mapping[str, Any]] = []
    if isinstance(documents_raw, Sequence) and not isinstance(documents_raw, (str, bytes)):
        for d in documents_raw:
            if isinstance(d, Mapping):
                documents.append(d)

    provenance_map = _convert_provenance_mapping_values(retrieval_provenance_by_doc_id=retrieval_provenance_by_doc_id)

    intent_constraints: tuple[Constraint, ...] = tuple(getattr(intent_spec, "constraints", ()) or ())
    use_doc_derived = doc_derived_assertions_mode == "limited" and int(doc_derived_max_assertions) > 0

    def _empty_mediation() -> None:
        if mediation_report_out is not None:
            mediation_report_out.clear()
            mediation_report_out.update(
                {
                    "policy": knowledge_unresolved_conflict_policy,
                    "status": "ok",
                    "events": [],
                    "normalization": knowledge_conflict_normalization.to_json_obj()
                    if knowledge_conflict_normalization is not None
                    else None,
                    "embedding_clustering_enabled": bool(knowledge_embedding_clustering_enabled),
                    "embedding_clustering_threshold": float(knowledge_embedding_clustering_threshold),
                }
            )

    # No intent and no doc-derived path => empty snapshot.
    if not intent_constraints and not use_doc_derived:
        _empty_mediation()
        return _apply_operator_decisions_optional(
            KnowledgeSnapshot(
                canonical_constraints=(),
                canonical_decisions=(),
                evidence_by_assertion={},
            ),
            tenant_id=tenant,
            repo_id=repo,
            knowledge_artifact_root=knowledge_artifact_root,
            apply_operator_knowledge_decisions=apply_operator_knowledge_decisions,
        )

    constraints_list = list(intent_constraints)
    try:
        if constraints_list and llm is not None and use_llm:
            snapshot = _extract_via_llm(
                tenant_id=tenant,
                repo_id=repo,
                intent_spec=intent_spec,
                retrieved_context=retrieved_context,
                retrieval_provenance_by_doc_id=provenance_map,
                llm=llm,
                documents=documents,
                knowledge_evidence_weighting=knowledge_evidence_weighting,
                knowledge_unresolved_conflict_policy=knowledge_unresolved_conflict_policy,
                compile_now_ms=compile_now_ms,
                mediation_report_out=mediation_report_out,
                knowledge_artifact_root=knowledge_artifact_root,
                knowledge_conflict_normalization=knowledge_conflict_normalization,
                knowledge_embedding_clustering_enabled=knowledge_embedding_clustering_enabled,
                knowledge_embedding_clustering_threshold=knowledge_embedding_clustering_threshold,
                stored_assertion_index_mode=stored_assertion_index_mode,
                stored_assertion_index_max_rows=int(stored_assertion_index_max_rows),
                ir_document=ir_document,
            )
            return _apply_operator_decisions_optional(
                snapshot,
                tenant_id=tenant,
                repo_id=repo,
                knowledge_artifact_root=knowledge_artifact_root,
                apply_operator_knowledge_decisions=apply_operator_knowledge_decisions,
            )
    except Exception:
        # Deterministic fail-closed fallback below.
        pass

    if not constraints_list:
        snap = _extract_deterministic(
            tenant_id=tenant,
            repo_id=repo,
            intent_constraints=(),
            documents=documents,
            provenance_map=provenance_map,
            doc_derived_assertions_mode=doc_derived_assertions_mode,
            doc_derived_max_assertions=int(doc_derived_max_assertions),
            doc_derived_patterns=doc_derived_patterns,
            knowledge_evidence_weighting=knowledge_evidence_weighting,
            knowledge_unresolved_conflict_policy=knowledge_unresolved_conflict_policy,
            compile_now_ms=compile_now_ms,
            mediation_report_out=mediation_report_out,
            knowledge_artifact_root=knowledge_artifact_root,
            knowledge_conflict_normalization=knowledge_conflict_normalization,
            knowledge_embedding_clustering_enabled=knowledge_embedding_clustering_enabled,
            knowledge_embedding_clustering_threshold=knowledge_embedding_clustering_threshold,
            stored_assertion_index_mode=stored_assertion_index_mode,
            stored_assertion_index_max_rows=int(stored_assertion_index_max_rows),
            ir_document=ir_document,
        )
        return _apply_operator_decisions_optional(
            snap,
            tenant_id=tenant,
            repo_id=repo,
            knowledge_artifact_root=knowledge_artifact_root,
            apply_operator_knowledge_decisions=apply_operator_knowledge_decisions,
        )

    snap = _extract_deterministic(
        tenant_id=tenant,
        repo_id=repo,
        intent_constraints=constraints_list,
        documents=documents,
        provenance_map=provenance_map,
        doc_derived_assertions_mode=doc_derived_assertions_mode,
        doc_derived_max_assertions=int(doc_derived_max_assertions),
        doc_derived_patterns=doc_derived_patterns,
        knowledge_evidence_weighting=knowledge_evidence_weighting,
        knowledge_unresolved_conflict_policy=knowledge_unresolved_conflict_policy,
        compile_now_ms=compile_now_ms,
        mediation_report_out=mediation_report_out,
        knowledge_artifact_root=knowledge_artifact_root,
        knowledge_conflict_normalization=knowledge_conflict_normalization,
        knowledge_embedding_clustering_enabled=knowledge_embedding_clustering_enabled,
        knowledge_embedding_clustering_threshold=knowledge_embedding_clustering_threshold,
        stored_assertion_index_mode=stored_assertion_index_mode,
        stored_assertion_index_max_rows=int(stored_assertion_index_max_rows),
        ir_document=ir_document,
    )
    return _apply_operator_decisions_optional(
        snap,
        tenant_id=tenant,
        repo_id=repo,
        knowledge_artifact_root=knowledge_artifact_root,
        apply_operator_knowledge_decisions=apply_operator_knowledge_decisions,
    )


def _extract_deterministic(
    *,
    tenant_id: str,
    repo_id: str,
    intent_constraints: Sequence[Constraint],
    documents: Sequence[Mapping[str, Any]],
    provenance_map: Mapping[str, ProvenancePointer],
    doc_derived_assertions_mode: DocDerivedAssertionsMode,
    doc_derived_max_assertions: int,
    doc_derived_patterns: DocDerivedPatternOptions | None,
    knowledge_evidence_weighting: KnowledgeEvidenceWeighting | None,
    knowledge_unresolved_conflict_policy: KnowledgeUnresolvedConflictPolicy,
    compile_now_ms: int | None,
    mediation_report_out: dict[str, Any] | None,
    knowledge_artifact_root: str | Path | None,
    knowledge_conflict_normalization: KnowledgeConflictNormalization | None,
    knowledge_embedding_clustering_enabled: bool,
    knowledge_embedding_clustering_threshold: float,
    stored_assertion_index_mode: Literal["off", "merge"],
    stored_assertion_index_max_rows: int,
    ir_document: IRDocument | None = None,
) -> KnowledgeSnapshot:
    extracted: list[tuple[CanonicalConstraint, tuple[str, ...], float]] = []
    skip_ids: set[str] = set()
    intent_constraint_ids_by_assertion: dict[str, str] = {}
    hub_allow = _ir_knowledge_hub_assertion_allowlist(ir_document)
    for c in intent_constraints:
        row = _deterministic_parse_constraint(repo_id=repo_id, constraint=c, documents=documents)
        extracted.append(row)
        skip_ids.add(row[0].assertion_id)
        intent_constraint_ids_by_assertion[row[0].assertion_id] = str(c.id)

    if doc_derived_assertions_mode == "limited" and doc_derived_max_assertions > 0:
        extracted.extend(
            _extract_doc_derived_rows(
                repo_id=repo_id,
                documents=documents,
                max_assertions=int(doc_derived_max_assertions),
                skip_assertion_ids=set(skip_ids),
                patterns=doc_derived_patterns or DocDerivedPatternOptions(),
            )
        )

    canonical_constraints = tuple([x[0] for x in extracted])
    evidence_by_assertion: dict[str, EvidenceMapping] = {}
    evidence_scores: dict[str, float] = {}

    for canonical, evidence_doc_ids, evidence_score in extracted:
        resolved_ptrs: tuple[ProvenancePointer, ...] = tuple(
            provenance_map[d] for d in evidence_doc_ids if d in provenance_map
        )
        evidence_by_assertion[canonical.assertion_id] = EvidenceMapping(
            evidence_doc_ids=evidence_doc_ids,
            resolved_provenance_pointers=resolved_ptrs,
        )
        evidence_scores[canonical.assertion_id] = float(evidence_score)

    retrieved_ids = _retrieved_doc_ids(documents=documents)
    canonical_constraints, evidence_by_assertion, evidence_scores = _merge_assertion_index_before_finalize(
        tenant_id=tenant_id,
        repo_id=repo_id,
        knowledge_artifact_root=knowledge_artifact_root,
        stored_assertion_index_mode=stored_assertion_index_mode,
        stored_assertion_index_max_rows=stored_assertion_index_max_rows,
        retrieved_doc_ids=retrieved_ids,
        provenance_map=provenance_map,
        canonical_constraints=canonical_constraints,
        evidence_by_assertion=evidence_by_assertion,
        base_evidence_scores=evidence_scores,
    )

    return _finalize_knowledge_snapshot_conflicts(
        canonical_constraints=canonical_constraints,
        evidence_by_assertion=evidence_by_assertion,
        base_evidence_scores=evidence_scores,
        documents=documents,
        knowledge_evidence_weighting=knowledge_evidence_weighting,
        knowledge_unresolved_conflict_policy=knowledge_unresolved_conflict_policy,
        compile_now_ms=compile_now_ms,
        mediation_report_out=mediation_report_out,
        knowledge_conflict_normalization=knowledge_conflict_normalization,
        knowledge_embedding_clustering_enabled=knowledge_embedding_clustering_enabled,
        knowledge_embedding_clustering_threshold=knowledge_embedding_clustering_threshold,
        intent_constraint_ids_by_assertion=intent_constraint_ids_by_assertion or None,
        ir_knowledge_hub_assertion_ids=hub_allow or None,
    )


def _extract_via_llm(
    *,
    tenant_id: str,
    repo_id: str,
    intent_spec: IntentSpec,
    retrieved_context: Mapping[str, Any],
    retrieval_provenance_by_doc_id: Mapping[str, ProvenancePointer],
    llm: LLMBackend,
    documents: Sequence[Mapping[str, Any]],
    knowledge_evidence_weighting: KnowledgeEvidenceWeighting | None,
    knowledge_unresolved_conflict_policy: KnowledgeUnresolvedConflictPolicy,
    compile_now_ms: int | None,
    mediation_report_out: dict[str, Any] | None,
    knowledge_artifact_root: str | Path | None,
    knowledge_conflict_normalization: KnowledgeConflictNormalization | None,
    knowledge_embedding_clustering_enabled: bool,
    knowledge_embedding_clustering_threshold: float,
    stored_assertion_index_mode: Literal["off", "merge"],
    stored_assertion_index_max_rows: int,
    ir_document: IRDocument | None = None,
) -> KnowledgeSnapshot:
    """
    LLM extraction step (best-effort):
    - Request strict JSON
    - Validate/unify semantics in code-side normalization
    - Fail-closed back to deterministic extraction if parsing fails
    """
    scope = TenantRepoScope(tenant_id=tenant_id, repo_id=repo_id)
    docs_raw = retrieved_context.get("documents") or []
    docs_for_prompt = list(docs_raw) if isinstance(docs_raw, Sequence) else []

    constraints_input: list[dict[str, Any]] = []
    for c in intent_spec.constraints:
        if isinstance(c.statement, str):
            constraints_input.append(
                {
                    "constraint_id": c.id,
                    "kind": c.kind,
                    "statement": c.statement,
                }
            )

    prompt_obj: dict[str, Any] = {
        "tenant_id": tenant_id,
        "repo_id": repo_id,
        "intent_constraints": constraints_input,
        "retrieved_context": {
            # Keep the prompt payload bounded (content is included because this stage
            # is evidence mapping). Downstream tiers can adjust.
            "documents": [
                {
                    "doc_id": str(d.get("doc_id") or ""),
                    "title": str(d.get("title") or ""),
                    "content": str(d.get("content") or "")[:4000],
                    "score": d.get("score"),
                    "metadata": d.get("metadata") if isinstance(d.get("metadata"), dict) else None,
                }
                for d in docs_for_prompt
                if isinstance(d, Mapping)
            ][:20]
        },
    }
    if ir_document is not None:
        prompt_obj["ir_compact"] = compact_ir_document_for_prompt(ir_document)
        prompt_obj["ir_anchor"] = ir_intent_knowledge_anchor_for_prompt(ir_document)

    system = (
        "You are the AKC knowledge extractor. Convert intent constraints + retrieved docs into "
        "canonical semantic assertions for contradiction detection. "
        "Return ONLY JSON."
    )

    user = (
        "When `ir_anchor` / `ir_compact` are present, align outputs with IR intent node "
        "constraint_ids and stable fingerprints; do not invent constraint ids that are not in "
        "intent_constraints or ir_anchor.intent_nodes.\n\n"
        "Return ONLY valid JSON with this shape:\n"
        "{\n"
        '  "canonical_constraints": [\n'
        "    {\n"
        '      "subject": string,\n'
        '      "predicate": one of ["required","forbidden","must_use","must_not_use","allowed"],\n'
        '      "object": string|null,\n'
        '      "polarity": -1|1,\n'
        '      "scope": string,\n'
        '      "kind": "hard"|"soft",\n'
        '      "summary": string,\n'
        '      "evidence_doc_ids": string[]\n'
        "    }\n"
        "  ],\n"
        '  "canonical_decisions": [\n'
        "    {\n"
        '      "assertion_id": string,\n'
        '      "selected": boolean,\n'
        '      "resolved": boolean,\n'
        '      "conflict_resolution_target_assertion_ids": string[],\n'
        '      "evidence_doc_ids": string[]\n'
        "    }\n"
        "  ],\n"
        '  "evidence_by_assertion": {\n'
        '    "ASSERTION_ID": { "evidence_doc_ids": string[] }\n'
        "  }\n"
        "}\n\n"
        "Input:\n"
        f"{json.dumps(prompt_obj, sort_keys=True, ensure_ascii=False)}"
    )

    req = LLMRequest(
        messages=[
            LLMMessage(role="system", content=system),
            LLMMessage(role="user", content=user),
        ],
        temperature=0.0,
        max_output_tokens=2000,
    )
    resp: LLMResponse = llm.complete(scope=scope, stage="retrieve", request=req)
    parsed = _parse_json_strict(resp.text)
    if not isinstance(parsed, dict):
        raise KnowledgeExtractionError("LLM returned non-object JSON")

    intent_constraint_ids_by_assertion: dict[str, str] = {}
    for c in getattr(intent_spec, "constraints", ()) or ():
        row = _deterministic_parse_constraint(repo_id=repo_id, constraint=c, documents=documents)
        intent_constraint_ids_by_assertion[row[0].assertion_id] = str(c.id)

    # Code-side normalization + validation.
    snapshot = _build_snapshot_from_llm_json(
        parsed=parsed,
        tenant_id=tenant_id,
        repo_id=repo_id,
        provenance_map=retrieval_provenance_by_doc_id,
        documents=documents,
        knowledge_evidence_weighting=knowledge_evidence_weighting,
        knowledge_unresolved_conflict_policy=knowledge_unresolved_conflict_policy,
        compile_now_ms=compile_now_ms,
        mediation_report_out=mediation_report_out,
        knowledge_artifact_root=knowledge_artifact_root,
        knowledge_conflict_normalization=knowledge_conflict_normalization,
        knowledge_embedding_clustering_enabled=knowledge_embedding_clustering_enabled,
        knowledge_embedding_clustering_threshold=knowledge_embedding_clustering_threshold,
        intent_constraint_ids_by_assertion=intent_constraint_ids_by_assertion,
        stored_assertion_index_mode=stored_assertion_index_mode,
        stored_assertion_index_max_rows=int(stored_assertion_index_max_rows),
        ir_knowledge_hub_assertion_ids=_ir_knowledge_hub_assertion_allowlist(ir_document),
    )
    return snapshot


def _build_snapshot_from_llm_json(
    *,
    parsed: Mapping[str, Any],
    tenant_id: str,
    repo_id: str,
    provenance_map: Mapping[str, ProvenancePointer],
    documents: Sequence[Mapping[str, Any]],
    knowledge_evidence_weighting: KnowledgeEvidenceWeighting | None,
    knowledge_unresolved_conflict_policy: KnowledgeUnresolvedConflictPolicy,
    compile_now_ms: int | None,
    mediation_report_out: dict[str, Any] | None,
    knowledge_artifact_root: str | Path | None,
    knowledge_conflict_normalization: KnowledgeConflictNormalization | None,
    knowledge_embedding_clustering_enabled: bool,
    knowledge_embedding_clustering_threshold: float,
    intent_constraint_ids_by_assertion: Mapping[str, str] | None,
    stored_assertion_index_mode: Literal["off", "merge"],
    stored_assertion_index_max_rows: int,
    ir_knowledge_hub_assertion_ids: frozenset[str] | None = None,
) -> KnowledgeSnapshot:
    constraints_raw = parsed.get("canonical_constraints") or []
    decisions_raw = parsed.get("canonical_decisions") or []
    if not isinstance(constraints_raw, list):
        raise KnowledgeExtractionError("canonical_constraints must be a list")

    canonical_constraints: list[CanonicalConstraint] = []
    evidence_doc_ids_by_aid: dict[str, tuple[str, ...]] = {}

    for c_raw in constraints_raw:
        if not isinstance(c_raw, Mapping):
            raise KnowledgeExtractionError("canonical_constraints[] must be objects")

        subject = str(c_raw.get("subject") or "").strip()
        if not subject:
            raise KnowledgeExtractionError("constraint.subject missing/empty")

        predicate = _normalize_predicate(c_raw.get("predicate"))
        obj_raw = c_raw.get("object")
        obj = None
        if obj_raw is not None:
            if not isinstance(obj_raw, str):
                raise KnowledgeExtractionError("constraint.object must be string or null")
            obj_val = obj_raw.strip()
            obj = obj_val if obj_val else None

        polarity_raw = c_raw.get("polarity")
        try:
            polarity = int(polarity_raw)  # type: ignore[arg-type]
        except Exception as e:
            raise KnowledgeExtractionError("constraint.polarity must be int") from e
        if polarity not in (-1, 1):
            raise KnowledgeExtractionError("constraint.polarity must be -1 or 1")

        scope = _normalize_scope(c_raw.get("scope"), repo_id=repo_id)

        kind_raw = str(c_raw.get("kind") or "").strip().lower()
        if kind_raw not in {"hard", "soft"}:
            kind: AssertionKind = "hard"
        else:
            kind = kind_raw  # type: ignore[assignment]

        summary = str(c_raw.get("summary") or "").strip()
        if not summary:
            raise KnowledgeExtractionError("constraint.summary missing/empty")

        canonical = CanonicalConstraint(
            subject=subject,
            predicate=predicate,
            object=obj,
            polarity=polarity,
            scope=scope,
            kind=kind,
            summary=summary,
        )

        evidence_doc_ids_raw = c_raw.get("evidence_doc_ids") or []
        if not isinstance(evidence_doc_ids_raw, list):
            raise KnowledgeExtractionError("constraint.evidence_doc_ids must be a list")
        evidence_doc_ids = tuple([str(x).strip() for x in evidence_doc_ids_raw if str(x).strip()])
        canonical_constraints.append(canonical)
        evidence_doc_ids_by_aid[canonical.assertion_id] = evidence_doc_ids

    # Build evidence mapping from constraint evidence_doc_ids (contract),
    # then optionally merge with evidence_by_assertion map + decisions evidence.
    evidence_by_assertion: dict[str, EvidenceMapping] = {}
    for c in canonical_constraints:
        ids = evidence_doc_ids_by_aid.get(c.assertion_id, ())
        resolved_ptrs = tuple(provenance_map[d] for d in ids if d in provenance_map)
        evidence_by_assertion[c.assertion_id] = EvidenceMapping(
            evidence_doc_ids=ids,
            resolved_provenance_pointers=resolved_ptrs,
        )

    evidence_by_assertion_raw = parsed.get("evidence_by_assertion") or {}
    if isinstance(evidence_by_assertion_raw, dict):
        for aid_raw, m_raw in evidence_by_assertion_raw.items():
            if not isinstance(aid_raw, str) or not aid_raw.strip():
                continue
            aid = aid_raw.strip()
            if aid not in evidence_by_assertion:
                continue
            if not isinstance(m_raw, Mapping):
                continue
            ids_raw = m_raw.get("evidence_doc_ids")
            if not isinstance(ids_raw, list):
                continue
            ids = tuple([str(x).strip() for x in ids_raw if str(x).strip()])
            current = evidence_by_assertion[aid]
            evidence_by_assertion[aid] = EvidenceMapping(
                evidence_doc_ids=tuple(list(current.evidence_doc_ids) + list(ids)),
                resolved_provenance_pointers=current.resolved_provenance_pointers,
            )

    # Optional LLM `canonical_decisions` entries may attach extra evidence_doc_ids only.
    constraint_by_aid = {c.assertion_id: c for c in canonical_constraints}
    _merge_evidence_from_decisions(
        decisions_raw=decisions_raw,
        constraint_by_assertion_id=constraint_by_aid,
        evidence_by_assertion_id=evidence_by_assertion,
        provenance_map=provenance_map,
    )

    # Refresh provenance pointers after merges (doc ids are authoritative).
    for c in canonical_constraints:
        em = evidence_by_assertion[c.assertion_id]
        ids = em.evidence_doc_ids
        ptrs = tuple(provenance_map[d] for d in ids if d in provenance_map)
        evidence_by_assertion[c.assertion_id] = EvidenceMapping(
            evidence_doc_ids=ids,
            resolved_provenance_pointers=ptrs,
        )

    strengths = {
        c.assertion_id: float(len(evidence_by_assertion[c.assertion_id].evidence_doc_ids))
        for c in canonical_constraints
    }
    retrieved_ids = _retrieved_doc_ids(documents=documents)
    merged_constraints, merged_evidence, merged_scores = _merge_assertion_index_before_finalize(
        tenant_id=tenant_id,
        repo_id=repo_id,
        knowledge_artifact_root=knowledge_artifact_root,
        stored_assertion_index_mode=stored_assertion_index_mode,
        stored_assertion_index_max_rows=int(stored_assertion_index_max_rows),
        retrieved_doc_ids=retrieved_ids,
        provenance_map=provenance_map,
        canonical_constraints=tuple(canonical_constraints),
        evidence_by_assertion=dict(evidence_by_assertion),
        base_evidence_scores=strengths,
    )
    return _finalize_knowledge_snapshot_conflicts(
        canonical_constraints=merged_constraints,
        evidence_by_assertion=merged_evidence,
        base_evidence_scores=merged_scores,
        documents=documents,
        knowledge_evidence_weighting=knowledge_evidence_weighting,
        knowledge_unresolved_conflict_policy=knowledge_unresolved_conflict_policy,
        compile_now_ms=compile_now_ms,
        mediation_report_out=mediation_report_out,
        knowledge_conflict_normalization=knowledge_conflict_normalization,
        knowledge_embedding_clustering_enabled=knowledge_embedding_clustering_enabled,
        knowledge_embedding_clustering_threshold=knowledge_embedding_clustering_threshold,
        intent_constraint_ids_by_assertion=intent_constraint_ids_by_assertion,
        ir_knowledge_hub_assertion_ids=ir_knowledge_hub_assertion_ids,
    )
