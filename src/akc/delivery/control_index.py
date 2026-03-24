"""Sync delivery sessions into tenant :class:`akc.control.operations_index.OperationsIndex` rows."""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

from akc.cli.project_config import load_akc_project_config
from akc.control.control_audit import append_control_audit_event
from akc.control.operations_index import OperationsIndex, operations_sqlite_path
from akc.delivery.metrics import compute_delivery_metrics
from akc.memory.models import normalize_repo_id

logger = logging.getLogger(__name__)


def resolve_delivery_index_context(project_dir: Path) -> tuple[Path, str, str] | None:
    """Return ``(outputs_root, tenant_id, repo_id)`` when the repo scope matches ``project_dir``."""

    cfg = load_akc_project_config(project_dir.resolve())
    if cfg is None or not (cfg.outputs_root or "").strip():
        return None
    out = Path(str(cfg.outputs_root)).expanduser().resolve()
    t = (cfg.tenant_id or "local").strip() or "local"
    r = normalize_repo_id((cfg.repo_id or "local").strip() or "local")
    scope = out / t / r
    if project_dir.resolve() != scope:
        return None
    return out, t, r


def delivery_artifact_rel_prefix(*, tenant_id: str, repo_id: str, delivery_id: str) -> str:
    """Path prefix under ``outputs_root`` for this delivery's artifacts."""

    r = normalize_repo_id(repo_id)
    return f"{tenant_id.strip()}/{r}/.akc/delivery/{delivery_id.strip()}"


def sync_delivery_session_row(
    *,
    outputs_root: Path,
    tenant_id: str,
    repo_id: str,
    delivery_id: str,
    request: dict[str, Any],
    session: dict[str, Any],
    events: list[dict[str, Any]],
) -> None:
    """Upsert one ``delivery_sessions`` row; does not touch compile/run artifacts."""

    tid = tenant_id.strip()
    rid = normalize_repo_id(repo_id)
    did = delivery_id.strip()
    prefix = delivery_artifact_rel_prefix(tenant_id=tid, repo_id=rid, delivery_id=did)
    session_rel_path = f"{prefix}/session.json"
    events_rel_path = f"{prefix}/events.json"

    phase = str(session.get("session_phase") or "")
    compile_run = session.get("compile_run_id")
    compile_run_id = str(compile_run).strip() if compile_run is not None and str(compile_run).strip() else None

    release_mode = str(session.get("release_mode") or "")

    recips_raw = request.get("recipients")
    recipient_count = len(recips_raw) if isinstance(recips_raw, list) else 0

    plats_raw = session.get("platforms")
    platforms_json = json.dumps(list(plats_raw), sort_keys=True) if isinstance(plats_raw, list) else None

    metrics = compute_delivery_metrics(request=request, session=session, events=events)
    metrics_json = json.dumps(metrics, sort_keys=True)

    su_raw = session.get("updated_at_unix_ms")
    updated_ms = (
        int(su_raw) if isinstance(su_raw, (int, float)) and not isinstance(su_raw, bool) else int(time.time() * 1000)
    )

    sqlite_p = operations_sqlite_path(outputs_root=outputs_root, tenant_id=tid)
    idx = OperationsIndex(sqlite_path=sqlite_p)
    idx.upsert_delivery_session(
        tenant_id=tid,
        repo_id=rid,
        delivery_id=did,
        updated_at_ms=updated_ms,
        session_phase=phase,
        compile_run_id=compile_run_id,
        release_mode=release_mode or None,
        session_rel_path=session_rel_path,
        events_rel_path=events_rel_path,
        recipient_count=recipient_count,
        platforms_json=platforms_json,
        metrics_json=metrics_json,
    )


def try_sync_delivery_session_from_project(
    project_dir: Path,
    delivery_id: str,
    *,
    request: dict[str, Any],
    session: dict[str, Any],
    events: list[dict[str, Any]],
) -> None:
    """Fail-soft index sync for CLI/workflows."""

    ctx = resolve_delivery_index_context(project_dir)
    if ctx is None:
        return
    out_root, tid, rid = ctx
    try:
        sync_delivery_session_row(
            outputs_root=out_root,
            tenant_id=tid,
            repo_id=rid,
            delivery_id=delivery_id,
            request=request,
            session=session,
            events=events,
        )
    except Exception:
        logger.debug(
            "delivery index sync failed for delivery_id=%s project_dir=%s",
            delivery_id,
            project_dir,
            exc_info=True,
        )


def append_delivery_control_audit_event(
    project_dir: Path,
    *,
    action: str,
    details: dict[str, Any],
    actor: str | None = None,
    request_id: str | None = None,
) -> None:
    """Append to ``control_audit.jsonl`` when repo scope matches indexed outputs layout."""

    ctx = resolve_delivery_index_context(project_dir)
    if ctx is None:
        return
    out_root, tid, _rid = ctx
    try:
        append_control_audit_event(
            outputs_root=out_root,
            tenant_id=tid,
            action=str(action).strip(),
            details=details,
            actor=actor,
            request_id=request_id,
        )
    except Exception:
        logger.debug("delivery control audit append failed action=%s", action, exc_info=True)
