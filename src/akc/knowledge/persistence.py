"""Versioned persistence for `KnowledgeSnapshot` under tenant/repo outputs (`.akc/knowledge/`)."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

from akc.memory.models import JSONValue, normalize_repo_id, require_non_empty
from akc.path_security import safe_resolve_path
from akc.utils.fingerprint import stable_json_fingerprint

from .models import KnowledgeSnapshot, knowledge_provenance_fingerprint, knowledge_semantic_fingerprint
from .observability import compute_knowledge_governance_counts

KNOWLEDGE_SNAPSHOT_SCHEMA_KIND = "akc_knowledge_snapshot"
KNOWLEDGE_SNAPSHOT_FINGERPRINT_KIND = "akc_knowledge_snapshot_fingerprint"
KNOWLEDGE_SNAPSHOT_SCHEMA_VERSION = 1

# Relative to `<outputs_root>/<tenant>/<repo>/`
KNOWLEDGE_SNAPSHOT_RELPATH = ".akc/knowledge/snapshot.json"
KNOWLEDGE_SNAPSHOT_FINGERPRINT_RELPATH = ".akc/knowledge/snapshot.fingerprint.json"

KNOWLEDGE_MEDIATION_SCHEMA_KIND = "akc_knowledge_mediation_report"
KNOWLEDGE_MEDIATION_SCHEMA_VERSION = 1
KNOWLEDGE_MEDIATION_RELPATH = ".akc/knowledge/mediation.json"


def build_knowledge_snapshot_envelope(
    *,
    tenant_id: str,
    repo_id: str,
    snapshot: KnowledgeSnapshot,
    knowledge_semantic_fingerprint_full: str,
    knowledge_provenance_fingerprint_full: str,
    run_id: str | None = None,
    ingest_generation_id: str | None = None,
    knowledge_governance: Mapping[str, JSONValue] | None = None,
) -> dict[str, JSONValue]:
    """Full on-disk envelope for `snapshot.json` (versioned, tenant/repo keyed)."""

    require_non_empty(tenant_id, name="tenant_id")
    require_non_empty(repo_id, name="repo_id")
    sem = knowledge_semantic_fingerprint_full.strip().lower()
    prov = knowledge_provenance_fingerprint_full.strip().lower()
    if len(sem) != 64 or any(c not in "0123456789abcdef" for c in sem):
        raise ValueError("knowledge_semantic_fingerprint_full must be a 64-char hex sha256")
    if len(prov) != 64 or any(c not in "0123456789abcdef" for c in prov):
        raise ValueError("knowledge_provenance_fingerprint_full must be a 64-char hex sha256")

    out: dict[str, JSONValue] = {
        "schema_kind": KNOWLEDGE_SNAPSHOT_SCHEMA_KIND,
        "schema_version": int(KNOWLEDGE_SNAPSHOT_SCHEMA_VERSION),
        "tenant_id": tenant_id.strip(),
        "repo_id": normalize_repo_id(repo_id),
        "knowledge_semantic_fingerprint": sem,
        "knowledge_provenance_fingerprint": prov,
        "snapshot": cast(JSONValue, snapshot.to_json_obj()),
    }
    if run_id is not None and str(run_id).strip():
        out["run_id"] = str(run_id).strip()
    if ingest_generation_id is not None and str(ingest_generation_id).strip():
        out["ingest_generation_id"] = str(ingest_generation_id).strip()
    if knowledge_governance is not None and knowledge_governance:
        out["knowledge_governance"] = cast(JSONValue, dict(knowledge_governance))
    return out


def build_knowledge_snapshot_fingerprint_sidecar(
    *,
    tenant_id: str,
    repo_id: str,
    snapshot_content_sha256: str,
    knowledge_semantic_fingerprint_full: str,
    knowledge_provenance_fingerprint_full: str,
) -> dict[str, JSONValue]:
    require_non_empty(tenant_id, name="tenant_id")
    require_non_empty(repo_id, name="repo_id")
    digest = _validate_sha256_hex(snapshot_content_sha256, name="snapshot_content_sha256")
    sem = _validate_sha256_hex(knowledge_semantic_fingerprint_full, name="knowledge_semantic_fingerprint_full")
    prov = _validate_sha256_hex(knowledge_provenance_fingerprint_full, name="knowledge_provenance_fingerprint_full")
    return {
        "schema_kind": KNOWLEDGE_SNAPSHOT_FINGERPRINT_KIND,
        "schema_version": int(KNOWLEDGE_SNAPSHOT_SCHEMA_VERSION),
        "tenant_id": tenant_id.strip(),
        "repo_id": normalize_repo_id(repo_id),
        "snapshot_path": KNOWLEDGE_SNAPSHOT_RELPATH,
        "content_sha256": digest,
        "knowledge_semantic_fingerprint": sem,
        "knowledge_provenance_fingerprint": prov,
    }


def _validate_sha256_hex(value: str, *, name: str) -> str:
    s = str(value).strip().lower()
    if len(s) != 64 or any(ch not in "0123456789abcdef" for ch in s):
        raise ValueError(f"{name} must be a 64-char hex sha256 string")
    return s


def write_knowledge_snapshot_artifacts(
    scope_root: str | Path,
    *,
    tenant_id: str,
    repo_id: str,
    snapshot: KnowledgeSnapshot,
    run_id: str | None = None,
    ingest_generation_id: str | None = None,
    intent_assertion_ids: frozenset[str] | None = None,
) -> tuple[str, str]:
    """Write `snapshot.json` + `snapshot.fingerprint.json` under `scope_root/.akc/knowledge/`.

    Returns:
        (snapshot_file_sha256, fingerprint_file_sha256) using `stable_json_fingerprint`
        on each file's JSON object (same convention as IR and run manifest output hashes).
    """
    root = safe_resolve_path(scope_root)
    knowledge_dir = root / ".akc" / "knowledge"
    knowledge_dir.mkdir(parents=True, exist_ok=True)

    sem_full = knowledge_semantic_fingerprint(snapshot=snapshot)
    prov_full = knowledge_provenance_fingerprint(snapshot=snapshot)
    gov = compute_knowledge_governance_counts(snapshot=snapshot, intent_assertion_ids=intent_assertion_ids)
    envelope = build_knowledge_snapshot_envelope(
        tenant_id=tenant_id,
        repo_id=repo_id,
        snapshot=snapshot,
        knowledge_semantic_fingerprint_full=sem_full,
        knowledge_provenance_fingerprint_full=prov_full,
        run_id=run_id,
        ingest_generation_id=ingest_generation_id,
        knowledge_governance=cast(Mapping[str, JSONValue], gov),
    )
    snapshot_text = json.dumps(envelope, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    snapshot_path = knowledge_dir / "snapshot.json"
    snapshot_path.write_text(snapshot_text, encoding="utf-8")
    snapshot_sha = stable_json_fingerprint(cast(Mapping[str, Any], envelope))

    sidecar = build_knowledge_snapshot_fingerprint_sidecar(
        tenant_id=tenant_id,
        repo_id=repo_id,
        snapshot_content_sha256=snapshot_sha,
        knowledge_semantic_fingerprint_full=sem_full,
        knowledge_provenance_fingerprint_full=prov_full,
    )
    fp_text = json.dumps(sidecar, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    fp_path = knowledge_dir / "snapshot.fingerprint.json"
    fp_path.write_text(fp_text, encoding="utf-8")
    fp_sha = stable_json_fingerprint(cast(Mapping[str, Any], sidecar))
    return snapshot_sha, fp_sha


def build_knowledge_mediation_envelope(
    *,
    tenant_id: str,
    repo_id: str,
    mediation_report: Mapping[str, Any],
    run_id: str | None = None,
) -> dict[str, JSONValue]:
    require_non_empty(tenant_id, name="tenant_id")
    require_non_empty(repo_id, name="repo_id")
    out: dict[str, JSONValue] = {
        "schema_kind": KNOWLEDGE_MEDIATION_SCHEMA_KIND,
        "schema_version": int(KNOWLEDGE_MEDIATION_SCHEMA_VERSION),
        "tenant_id": tenant_id.strip(),
        "repo_id": normalize_repo_id(repo_id),
        "mediation_report": cast(JSONValue, dict(mediation_report)),
    }
    if run_id is not None and str(run_id).strip():
        out["run_id"] = str(run_id).strip()
    return out


def write_knowledge_mediation_report_artifact(
    scope_root: str | Path,
    *,
    tenant_id: str,
    repo_id: str,
    mediation_report: Mapping[str, Any],
    run_id: str | None = None,
) -> str:
    """Write structured mediation JSON under ``scope_root/.akc/knowledge/mediation.json``.

    Returns the ``stable_json_fingerprint`` (sha256) of the written JSON object.
    """

    root = safe_resolve_path(scope_root)
    knowledge_dir = root / ".akc" / "knowledge"
    knowledge_dir.mkdir(parents=True, exist_ok=True)
    envelope = build_knowledge_mediation_envelope(
        tenant_id=tenant_id,
        repo_id=repo_id,
        mediation_report=mediation_report,
        run_id=run_id,
    )
    text = json.dumps(envelope, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    path = knowledge_dir / "mediation.json"
    path.write_text(text, encoding="utf-8")
    return stable_json_fingerprint(cast(Mapping[str, Any], envelope))


def load_knowledge_snapshot_envelope(*, scope_root: str | Path) -> tuple[dict[str, Any], KnowledgeSnapshot]:
    """Load and validate a persisted `snapshot.json`; returns (raw_envelope, snapshot)."""

    path = safe_resolve_path(scope_root) / ".akc" / "knowledge" / "snapshot.json"
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("knowledge snapshot file must contain a JSON object")
    if str(raw.get("schema_kind", "")) != KNOWLEDGE_SNAPSHOT_SCHEMA_KIND:
        raise ValueError(f"snapshot.schema_kind must be {KNOWLEDGE_SNAPSHOT_SCHEMA_KIND!r}")
    if int(raw.get("schema_version", -1)) != KNOWLEDGE_SNAPSHOT_SCHEMA_VERSION:
        raise ValueError(
            f"unsupported knowledge snapshot schema_version (expected {KNOWLEDGE_SNAPSHOT_SCHEMA_VERSION})"
        )
    inner = raw.get("snapshot")
    if not isinstance(inner, dict):
        raise ValueError("snapshot.snapshot must be an object")
    snap = KnowledgeSnapshot.from_json_obj(inner)
    return raw, snap
