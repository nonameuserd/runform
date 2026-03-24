from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

from akc.artifacts.validate import validate_obj
from akc.memory.code_memory import SQLiteCodeMemoryStore
from akc.memory.models import MemoryModelError, PlanState, normalize_repo_id, require_non_empty
from akc.memory.plan_state import JsonFilePlanStateStore, SQLitePlanStateStore

from .control_panels import load_operator_panels_for_scope
from .models import EvidenceIndex, EvidenceKind, EvidenceRef, Manifest, ViewerInputs, ViewerSnapshot


class ViewerError(Exception):
    """Raised when viewer inputs cannot be loaded safely."""


def _scoped_outputs_dir(*, outputs_root: Path, tenant_id: str, repo_id: str) -> Path:
    require_non_empty(tenant_id, name="tenant_id")
    repo_n = normalize_repo_id(repo_id)
    return outputs_root / tenant_id / repo_n


def _read_json_object(p: Path, *, what: str) -> dict[str, Any]:
    try:
        raw = p.read_text(encoding="utf-8")
    except FileNotFoundError:
        raise ViewerError(f"missing {what}: {p}") from None
    except OSError as e:
        raise ViewerError(f"failed to read {what}: {p}") from e
    try:
        loaded = json.loads(raw)
    except Exception as e:  # pragma: no cover
        raise ViewerError(f"{what} is not valid JSON: {p}") from e
    if not isinstance(loaded, dict):
        raise ViewerError(f"{what} must be a JSON object: {p}")
    return cast(dict[str, Any], loaded)


def _load_plan_state(inputs: ViewerInputs) -> PlanState:
    repo_n = normalize_repo_id(inputs.repo_id)
    plan_base_dir = (inputs.plan_base_dir or Path.cwd()).expanduser()

    # Primary: filesystem plan store (.akc/plan/<tenant>/<repo>/active.json + <plan_id>.json)
    json_store = JsonFilePlanStateStore(base_dir=plan_base_dir)
    try:
        active_id = json_store.get_active_plan_id(tenant_id=inputs.tenant_id, repo_id=repo_n)
        if active_id is not None:
            plan = json_store.load_plan(tenant_id=inputs.tenant_id, repo_id=repo_n, plan_id=active_id)
            if plan is not None:
                return plan
    except Exception:
        # Treat plan store failures as non-fatal; fall back to sqlite.
        pass

    # Fallback: sqlite memory under scoped outputs dir (.akc/memory.sqlite)
    scoped = _scoped_outputs_dir(outputs_root=inputs.outputs_root, tenant_id=inputs.tenant_id, repo_id=repo_n)
    sqlite_path = scoped / ".akc" / "memory.sqlite"
    if sqlite_path.exists():
        try:
            sqlite_store = SQLitePlanStateStore(path=str(sqlite_path))
            active_id = sqlite_store.get_active_plan_id(tenant_id=inputs.tenant_id, repo_id=repo_n)
            if active_id is None:
                raise ViewerError(f"no active plan found in sqlite store: {sqlite_path}")
            plan = sqlite_store.load_plan(tenant_id=inputs.tenant_id, repo_id=repo_n, plan_id=active_id)
            if plan is None:
                raise ViewerError(f"active plan id not found in sqlite store: {active_id}")
            return plan
        except ViewerError:
            raise
        except Exception as e:  # pragma: no cover
            raise ViewerError(f"failed to load plan from sqlite store: {sqlite_path}") from e

    raise ViewerError("no plan state found: expected .akc/plan active pointer or scoped .akc/memory.sqlite")


def _load_manifest(*, scoped_outputs_dir: Path) -> Manifest | None:
    mpath = scoped_outputs_dir / "manifest.json"
    if not mpath.exists():
        return None
    obj = _read_json_object(mpath, what="manifest.json")
    issues = validate_obj(obj=obj, kind="manifest", version=int(obj.get("schema_version") or 1))
    if issues:
        # Viewer is tolerant: it can render unknown/new fields, but schema violations
        # are still surfaced to the user by embedding a synthetic metadata record.
        obj.setdefault("metadata", {})
        md = obj.get("metadata")
        if isinstance(md, dict):
            md["viewer_schema_issues"] = [{"path": i.path, "message": i.message} for i in issues]
            obj["metadata"] = md
    return cast(Manifest, obj)


def _infer_evidence_kind(relpath: str, *, media_type: str | None) -> EvidenceKind:
    p = relpath.lower()
    if p.endswith("manifest.json"):
        return "manifest"
    if p.endswith(".json") and "/verification/" in p.replace("\\", "/"):
        return "verifier_result"
    if p.endswith(".json") and "/tests/" in p.replace("\\", "/"):
        return "execution_stage"
    if media_type and media_type.startswith("text/"):
        return "text"
    return "text"


def _index_evidence(*, manifest: Manifest | None) -> EvidenceIndex:
    all_refs: list[EvidenceRef] = []
    by_step: dict[str, list[EvidenceRef]] = {}
    if manifest is None:
        return EvidenceIndex(by_step={}, all=[])

    for a in manifest.get("artifacts") or []:
        relpath = str(a.get("path") or "").strip()
        if not relpath:
            continue
        md = a.get("metadata")
        step_id = md.get("step_id") if isinstance(md, dict) else None
        media_type = a.get("media_type") if isinstance(a.get("media_type"), str) else None
        sha256 = a.get("sha256") if isinstance(a.get("sha256"), str) else None
        size_bytes = a.get("size_bytes") if isinstance(a.get("size_bytes"), int) else None
        ref = EvidenceRef(
            kind=_infer_evidence_kind(relpath, media_type=media_type),
            relpath=relpath,
            media_type=media_type,
            sha256=sha256,
            size_bytes=size_bytes,
            metadata=cast(dict[str, Any] | None, md) if isinstance(md, dict) else None,
        )
        all_refs.append(ref)
        if isinstance(step_id, str) and step_id.strip():
            by_step.setdefault(step_id, []).append(ref)

    # Stable ordering for deterministic rendering.
    all_refs.sort(key=lambda r: r.relpath)
    for k in list(by_step.keys()):
        by_step[k].sort(key=lambda r: r.relpath)
    return EvidenceIndex(by_step=by_step, all=all_refs)


def _load_knowledge_envelope(*, scoped_outputs_dir: Path) -> dict[str, Any] | None:
    kpath = scoped_outputs_dir / ".akc" / "knowledge" / "snapshot.json"
    if not kpath.is_file():
        return None
    try:
        obj = json.loads(kpath.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return obj if isinstance(obj, dict) else None


def _load_knowledge_mediation_envelope(*, scoped_outputs_dir: Path) -> dict[str, Any] | None:
    mpath = scoped_outputs_dir / ".akc" / "knowledge" / "mediation.json"
    if not mpath.is_file():
        return None
    try:
        obj = json.loads(mpath.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeError):
        return None
    return obj if isinstance(obj, dict) else None


def _load_conflict_reports_from_memory(
    *,
    scoped_outputs_dir: Path,
    tenant_id: str,
    repo_id: str,
) -> tuple[dict[str, Any], ...]:
    mem_path = scoped_outputs_dir / ".akc" / "memory.sqlite"
    if not mem_path.is_file():
        return ()
    try:
        store = SQLiteCodeMemoryStore(path=str(mem_path))
        items = store.list_items(
            tenant_id=tenant_id,
            repo_id=repo_id,
            artifact_id=None,
            kind_filter=("conflict_report",),
            limit=200,
        )
    except Exception:
        return ()
    reports: list[dict[str, Any]] = []
    for it in items:
        md = it.metadata or {}
        rep = md.get("report")
        if isinstance(rep, dict):
            reports.append(dict(rep))
    reports.sort(key=lambda r: str(r.get("conflict_id", "")))
    return tuple(reports)


def load_viewer_snapshot(inputs: ViewerInputs) -> ViewerSnapshot:
    """Load a viewer snapshot for a tenant/repo scope (read-only)."""

    repo_n = normalize_repo_id(inputs.repo_id)
    scoped = _scoped_outputs_dir(outputs_root=inputs.outputs_root, tenant_id=inputs.tenant_id, repo_id=repo_n)
    plan = _load_plan_state(inputs)

    # Validate plan object against frozen schema (tolerant: collect issues).
    try:
        plan_obj: dict[str, Any] = cast(dict[str, Any], plan.to_json_obj())
        issues = validate_obj(obj=plan_obj, kind="plan_state", version=int(inputs.schema_version))
    except MemoryModelError as e:  # pragma: no cover
        raise ViewerError("plan state could not be serialized for validation") from e
    if issues:
        # Rehydrate plan from JSON to ensure we're rendering a schema-aligned view where possible.
        # Unknown/additional fields remain tolerated and preserved by PlanState itself.
        plan_obj.setdefault("last_feedback", {})
        lf = plan_obj.get("last_feedback")
        if isinstance(lf, dict):
            lf["viewer_schema_issues"] = [{"path": i.path, "message": i.message} for i in issues]
            plan_obj["last_feedback"] = lf
        plan = PlanState.from_json_obj(plan_obj)

    manifest = _load_manifest(scoped_outputs_dir=scoped)
    evidence = _index_evidence(manifest=manifest)
    knowledge_envelope = _load_knowledge_envelope(scoped_outputs_dir=scoped)
    knowledge_mediation_envelope = _load_knowledge_mediation_envelope(scoped_outputs_dir=scoped)
    conflict_reports = _load_conflict_reports_from_memory(
        scoped_outputs_dir=scoped,
        tenant_id=inputs.tenant_id,
        repo_id=repo_n,
    )
    operator_panels = load_operator_panels_for_scope(
        outputs_root=inputs.outputs_root.expanduser().resolve(),
        scoped_outputs_dir=scoped,
        tenant_id=inputs.tenant_id,
        repo_id=repo_n,
        manifest=dict(manifest) if manifest is not None else None,
        plan_run_id=str(plan.id),
    )
    return ViewerSnapshot(
        inputs=inputs,
        plan=plan,
        manifest=manifest,
        scoped_outputs_dir=scoped,
        evidence=evidence,
        knowledge_envelope=knowledge_envelope,
        knowledge_mediation_envelope=knowledge_mediation_envelope,
        conflict_reports=conflict_reports,
        operator_panels=operator_panels,
    )
