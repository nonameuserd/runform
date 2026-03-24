"""Knowledge-layer observability: mediation events, conflict groups, supersession hints."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from akc.knowledge.models import KnowledgeSnapshot

KNOWLEDGE_GOVERNANCE_STATS_SCHEMA_VERSION = 1

KNOWLEDGE_DIR_RELATIVE = ".akc/knowledge"
KNOWLEDGE_SNAPSHOT_RELATIVE = ".akc/knowledge/snapshot.json"
KNOWLEDGE_SNAPSHOT_FINGERPRINT_RELATIVE = ".akc/knowledge/snapshot.fingerprint.json"
KNOWLEDGE_MEDIATION_RELATIVE = ".akc/knowledge/mediation.json"


def knowledge_paths_map() -> dict[str, str]:
    """Stable repo-relative paths under a tenant/repo scope root."""

    return {
        "dir": KNOWLEDGE_DIR_RELATIVE,
        "snapshot": KNOWLEDGE_SNAPSHOT_RELATIVE,
        "snapshot_fingerprint": KNOWLEDGE_SNAPSHOT_FINGERPRINT_RELATIVE,
        "mediation": KNOWLEDGE_MEDIATION_RELATIVE,
    }


def mediation_events_from_envelope(
    knowledge_mediation_envelope: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if not knowledge_mediation_envelope:
        return []
    inner = knowledge_mediation_envelope.get("mediation_report")
    if not isinstance(inner, dict):
        return []
    ev = inner.get("events")
    if not isinstance(ev, list):
        return []
    return [e for e in ev if isinstance(e, dict)]


def group_mediation_events_by_conflict_group(
    events: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    buckets: dict[str, list[dict[str, Any]]] = {}
    for e in events:
        cg = e.get("conflict_group_id")
        key = cg.strip() if isinstance(cg, str) and cg.strip() else "__ungrouped__"
        buckets.setdefault(key, []).append(e)
    for items in buckets.values():
        items.sort(key=lambda x: json.dumps(x, sort_keys=True, ensure_ascii=False))
    return dict(sorted(buckets.items()))


def supersession_hints_from_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = [e for e in events if e.get("kind") == "supersedes"]
    out.sort(key=lambda x: json.dumps(x, sort_keys=True, ensure_ascii=False))
    return out


def compute_unresolved_knowledge_conflict_count(events: list[dict[str, Any]]) -> int:
    """Count distinct conflict groups (or ungrouped events) still marked unresolved by mediation."""

    by_group: set[str] = set()
    ungrouped = 0
    for e in events:
        if e.get("mediation_resolved") is not False:
            continue
        cg = e.get("conflict_group_id")
        if isinstance(cg, str) and cg.strip():
            by_group.add(cg.strip())
        else:
            ungrouped += 1
    return len(by_group) + ungrouped


def compute_knowledge_governance_counts(
    *,
    snapshot: KnowledgeSnapshot,
    intent_assertion_ids: frozenset[str] | None = None,
) -> dict[str, Any]:
    """Deterministic coverage-style counts for CI, exports, and operator dashboards.

    When ``intent_assertion_ids`` is omitted, intent- vs doc-derived split fields are null
    (not inferrable from the snapshot alone).
    """

    constraints = snapshot.canonical_constraints
    total = len(constraints)
    hard_n = sum(1 for c in constraints if c.kind == "hard")
    soft_n = sum(1 for c in constraints if c.kind == "soft")

    intent_set = intent_assertion_ids
    intent_backed: int | None
    doc_derived_soft: int | None
    if intent_set is None:
        intent_backed = None
        doc_derived_soft = None
    else:
        intent_backed = sum(1 for c in constraints if c.assertion_id in intent_set)
        doc_derived_soft = sum(1 for c in constraints if c.kind == "soft" and c.assertion_id not in intent_set)

    with_evidence = 0
    for c in constraints:
        em = snapshot.evidence_by_assertion.get(c.assertion_id)
        if em is not None and len(em.evidence_doc_ids) > 0:
            with_evidence += 1

    distinct_docs: set[str] = set()
    for em in snapshot.evidence_by_assertion.values():
        distinct_docs.update(em.evidence_doc_ids)

    coverage = (with_evidence / total) if total else 0.0

    return {
        "schema_version": KNOWLEDGE_GOVERNANCE_STATS_SCHEMA_VERSION,
        "canonical_assertions_total": total,
        "hard_assertions_count": hard_n,
        "soft_assertions_count": soft_n,
        "intent_backed_assertion_ids_count": intent_backed,
        "doc_derived_soft_assertions_count": doc_derived_soft,
        "assertions_with_evidence_doc_ids_count": with_evidence,
        "assertions_without_evidence_doc_ids_count": total - with_evidence,
        "distinct_evidence_doc_ids_count": len(distinct_docs),
        "evidence_doc_coverage_fraction": float(coverage),
    }


def build_knowledge_observation_payload(
    *,
    knowledge_envelope: dict[str, Any] | None,
    conflict_reports: tuple[dict[str, Any], ...],
    knowledge_mediation_envelope: dict[str, Any] | None,
) -> dict[str, Any]:
    """Shape embedded in ``knowledge_obs.json`` and static web viewer data."""

    events = mediation_events_from_envelope(knowledge_mediation_envelope)
    conflict_groups = group_mediation_events_by_conflict_group(events)
    supers = supersession_hints_from_events(events)
    unresolved = compute_unresolved_knowledge_conflict_count(events)
    out: dict[str, Any] = {
        "knowledge_envelope": knowledge_envelope,
        "conflict_reports": list(conflict_reports),
        "knowledge_mediation_envelope": knowledge_mediation_envelope,
        "mediation_events": events,
        "conflict_groups": conflict_groups,
        "supersession_hints": supers,
        "unresolved_knowledge_conflicts_count": unresolved,
        "knowledge_paths": knowledge_paths_map(),
    }
    if isinstance(knowledge_envelope, dict):
        kg = knowledge_envelope.get("knowledge_governance")
        if isinstance(kg, dict):
            out["knowledge_governance"] = dict(kg)
    return out


def summarize_knowledge_governance(*, scope_root: Path) -> dict[str, Any]:
    """Operator-facing summary: unresolved count and which ``.akc/knowledge/`` files exist."""

    root = Path(scope_root).expanduser().resolve()
    kd = root / ".akc" / "knowledge"
    paths = knowledge_paths_map()
    present = {
        "dir": kd.is_dir(),
        "snapshot": (kd / "snapshot.json").is_file(),
        "snapshot_fingerprint": (kd / "snapshot.fingerprint.json").is_file(),
        "mediation": (kd / "mediation.json").is_file(),
    }
    mediation_env: dict[str, Any] | None = None
    if present["mediation"]:
        try:
            raw = json.loads((kd / "mediation.json").read_text(encoding="utf-8"))
            mediation_env = raw if isinstance(raw, dict) else None
        except (OSError, json.JSONDecodeError, UnicodeError):
            mediation_env = None
    events = mediation_events_from_envelope(mediation_env)
    unresolved = compute_unresolved_knowledge_conflict_count(events)
    result: dict[str, Any] = {
        "unresolved_knowledge_conflicts_count": unresolved,
        "knowledge_paths": paths,
        "knowledge_paths_present": present,
    }
    if present["snapshot"]:
        snap_path = kd / "snapshot.json"
        try:
            raw_snap = json.loads(snap_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, UnicodeError):
            raw_snap = None
        if isinstance(raw_snap, dict):
            kg = raw_snap.get("knowledge_governance")
            if isinstance(kg, dict):
                result["knowledge_governance"] = dict(kg)
            else:
                inner = raw_snap.get("snapshot")
                if isinstance(inner, dict):
                    try:
                        snap = KnowledgeSnapshot.from_json_obj(inner)
                        result["knowledge_governance"] = compute_knowledge_governance_counts(
                            snapshot=snap, intent_assertion_ids=None
                        )
                    except (ValueError, TypeError, KeyError):
                        pass
    return result
