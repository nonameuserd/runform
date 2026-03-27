"""B3: operator overrides for knowledge-layer decisions (`.akc/knowledge/decisions.json`)."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import cast

from akc.memory.models import JSONValue, normalize_repo_id, require_non_empty
from akc.path_security import safe_resolve_path, safe_resolve_scoped_path

from .models import CanonicalDecision, KnowledgeSnapshot

OPERATOR_DECISIONS_SCHEMA_KIND = "akc_operator_knowledge_decisions"
OPERATOR_DECISIONS_SCHEMA_VERSION = 1
OPERATOR_DECISIONS_RELPATH = ".akc/knowledge/decisions.json"


def operator_decisions_path(*, scope_root: str | Path) -> Path:
    root = safe_resolve_path(scope_root)
    return safe_resolve_scoped_path(root, ".akc", "knowledge", "decisions.json")


def load_operator_knowledge_decisions(
    *,
    scope_root: str | Path,
    tenant_id: str,
    repo_id: str,
) -> dict[str, CanonicalDecision]:
    """Load and validate operator decisions for this tenant/repo scope."""

    require_non_empty(tenant_id, name="tenant_id")
    repo_n = normalize_repo_id(repo_id)
    path = operator_decisions_path(scope_root=scope_root)
    if not path.is_file():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(raw, dict):
        return {}
    if str(raw.get("schema_kind", "")) != OPERATOR_DECISIONS_SCHEMA_KIND:
        return {}
    if int(raw.get("schema_version", -1)) != OPERATOR_DECISIONS_SCHEMA_VERSION:
        return {}
    if str(raw.get("tenant_id", "")).strip() != tenant_id.strip():
        return {}
    if normalize_repo_id(str(raw.get("repo_id", ""))) != repo_n:
        return {}

    decisions_raw = raw.get("decisions")
    if not isinstance(decisions_raw, list):
        return {}

    out: dict[str, CanonicalDecision] = {}
    for _i, d in enumerate(decisions_raw):
        if not isinstance(d, dict):
            continue
        try:
            dec = CanonicalDecision.from_json_obj(d)
        except Exception:
            continue
        if not dec.assertion_id:
            continue
        out[dec.assertion_id] = dec
    return out


def apply_operator_decisions_to_snapshot(
    snapshot: KnowledgeSnapshot,
    overlay: Mapping[str, CanonicalDecision],
) -> KnowledgeSnapshot:
    """Replace `canonical_decisions` entries where the operator supplied an override."""

    if not overlay:
        return snapshot

    allowed = {c.assertion_id for c in snapshot.canonical_constraints}
    by_id: dict[str, CanonicalDecision] = {d.assertion_id: d for d in snapshot.canonical_decisions}

    for aid, od in overlay.items():
        if aid not in allowed:
            continue
        if od.assertion_id != aid:
            continue
        by_id[aid] = od

    merged = tuple(sorted(by_id.values(), key=lambda d: d.assertion_id))
    return KnowledgeSnapshot(
        canonical_constraints=snapshot.canonical_constraints,
        canonical_decisions=merged,
        evidence_by_assertion=dict(snapshot.evidence_by_assertion),
        evidence_strength_by_assertion=dict(snapshot.evidence_strength_by_assertion),
    )


def build_operator_decisions_envelope(
    *,
    tenant_id: str,
    repo_id: str,
    decisions: Sequence[CanonicalDecision],
) -> dict[str, JSONValue]:
    """Serialize an envelope suitable for writing `decisions.json` (documentation / tooling)."""

    require_non_empty(tenant_id, name="tenant_id")
    repo_n = normalize_repo_id(repo_id)
    return {
        "schema_kind": OPERATOR_DECISIONS_SCHEMA_KIND,
        "schema_version": int(OPERATOR_DECISIONS_SCHEMA_VERSION),
        "tenant_id": tenant_id.strip(),
        "repo_id": repo_n,
        "decisions": cast(JSONValue, [d.to_json_obj() for d in decisions]),
    }
