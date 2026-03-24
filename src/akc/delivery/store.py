"""Filesystem-backed delivery session storage (v1 scaffolding for ``akc deliver``).

Before a stable release there is **no** migration from older on-disk shapes (for example
``events.json`` without ``delivery_events`` v1 fields). Operators who hit validation
errors after a schema change should remove ``.akc/delivery/<delivery_id>/`` and run
``akc deliver`` again. Add explicit migration only when preserving real sessions in
the field becomes a requirement.
"""

from __future__ import annotations

import json
import re
import time
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Final, Literal, cast

from akc.artifacts.contracts import apply_schema_envelope
from akc.artifacts.validate import validate_artifact_json
from akc.delivery import adapters as distribution_adapters
from akc.delivery import ingest as delivery_ingest
from akc.delivery.activation import (
    recompute_delivery_activation,
    resolve_recipient_email_for_token,
)
from akc.delivery.control_index import (
    append_delivery_control_audit_event,
    try_sync_delivery_session_from_project,
)
from akc.delivery.event_types import (
    DELIVERY_ACTIVATION_FIRST_RUN,
    DELIVERY_HUMAN_GATE_PASSED,
    DELIVERY_INVITE_RESEND_REQUESTED,
    DELIVERY_PREFLIGHT_COMPLETED,
    DELIVERY_PROVIDER_INSTALL_DETECTED,
    DELIVERY_RECIPIENT_ACTIVE,
    DELIVERY_RECIPIENTS_AMENDED,
    DELIVERY_REQUEST_ACCEPTED,
    DELIVERY_REQUEST_PARSED,
    DELIVERY_STORE_LIVE,
    DELIVERY_STORE_PROMOTION_REQUESTED,
    DELIVERY_STORE_SUBMITTED,
)
from akc.delivery.invites import (
    new_invite_hmac_key,
    new_invite_token_id,
    verify_invite_query,
)

RELEASE_MODES: tuple[str, ...] = ("beta", "store", "both")
PLATFORMS_ALLOWED: frozenset[str] = frozenset({"web", "ios", "android"})
PROMOTION_LANES: frozenset[str] = frozenset({"beta", "store"})

SCHEMA_DELIVERY_EVENT_ENVELOPE_V1: Final[str] = "akc.delivery_event_envelope.v1"

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def normalize_email(raw: str) -> str:
    """Normalize and validate a single recipient email (tenant-safe identifier only)."""

    s = raw.strip().lower()
    if not _EMAIL_RE.match(s):
        raise ValueError(f"invalid recipient email: {raw!r}")
    return s


def normalize_recipients(raw_list: list[str]) -> list[str]:
    """Deduplicate recipients while preserving first-seen order."""

    seen: set[str] = set()
    out: list[str] = []
    for r in raw_list:
        n = normalize_email(r)
        if n in seen:
            continue
        seen.add(n)
        out.append(n)
    if not out:
        raise ValueError("at least one --recipient is required")
    return out


def parse_platforms_csv(platforms: str) -> list[str]:
    parts = [p.strip().lower() for p in platforms.split(",") if p.strip()]
    bad = sorted({p for p in parts if p not in PLATFORMS_ALLOWED})
    if bad:
        raise ValueError(
            f"unsupported platform(s): {bad}; allowed: {sorted(PLATFORMS_ALLOWED)}",
        )
    if not parts:
        raise ValueError("at least one platform is required (e.g. web,ios,android)")
    return parts


def assert_safe_delivery_id(delivery_id: str) -> None:
    if ".." in delivery_id or "/" in delivery_id or "\\" in delivery_id:
        raise ValueError("invalid delivery_id")
    if not re.fullmatch(r"[a-zA-Z0-9_.-]+", delivery_id):
        raise ValueError("invalid delivery_id")


def delivery_root(project_dir: Path) -> Path:
    return (project_dir.resolve()).joinpath(".akc", "delivery")


def delivery_paths(project_dir: Path, delivery_id: str) -> dict[str, Path]:
    assert_safe_delivery_id(delivery_id)
    base = delivery_root(project_dir) / delivery_id
    return {
        "dir": base,
        "request": base / "request.json",
        "session": base / "session.json",
        "recipients": base / "recipients.json",
        "events": base / "events.json",
        "provider_state": base / "provider_state.json",
        "activation_evidence": base / "activation_evidence.json",
    }


def new_delivery_id() -> str:
    return str(uuid.uuid4())


def _now_ms() -> int:
    return int(time.time() * 1000)


def _write_json(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _read_json(path: Path) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(path.read_text(encoding="utf-8")))


def _pipeline_stage_initial() -> dict[str, Any]:
    return {
        "status": "not_started",
        "started_at_unix_ms": None,
        "completed_at_unix_ms": None,
        "error": None,
        "run_id": None,
    }


def _channel_initial(*, applicable: bool) -> dict[str, Any]:
    return {
        "status": "not_started" if applicable else "not_applicable",
        "details": {},
    }


def _per_platform_channels(
    *,
    release_mode: Literal["beta", "store", "both"],
) -> dict[str, dict[str, Any]]:
    beta_ok = release_mode in ("beta", "both")
    store_ok = release_mode in ("store", "both")
    return {
        "channels": {
            "beta": _channel_initial(applicable=beta_ok),
            "store": _channel_initial(applicable=store_ok),
        }
    }


def _mobile_store_lane_status(platform: str, platforms: list[str]) -> dict[str, Any]:
    st = "not_started" if platform in platforms else "not_applicable"
    return {"status": st, "external_ref": None, "notes": None}


def _recipients_sidecar_row(*, email: str, platforms: list[str], invite_token_id: str) -> dict[str, Any]:
    return {
        "email": email,
        "platforms": list(platforms),
        "invite_token_id": invite_token_id,
        "status": "pending",
        "last_invite_sent_at_unix_ms": None,
        "resend_count": 0,
    }


def _initial_recipients_sidecar_doc(
    *,
    delivery_id: str,
    rec_norm: list[str],
    plat_norm: list[str],
    invite_by_email: dict[str, str],
    created_ms: int,
    updated_ms: int,
) -> dict[str, Any]:
    return {
        "delivery_id": delivery_id,
        "recipients": {
            e: _recipients_sidecar_row(
                email=e,
                platforms=plat_norm,
                invite_token_id=invite_by_email[e],
            )
            for e in rec_norm
        },
        "created_at_unix_ms": created_ms,
        "updated_at_unix_ms": updated_ms,
    }


def _provider_platform_row_initial() -> dict[str, Any]:
    return {
        "status": "not_started",
        "adapter_kind": None,
        "external_refs": {},
        "details": {},
        "last_error": None,
        "last_updated_at_unix_ms": None,
    }


def _initial_provider_state_doc(
    *,
    delivery_id: str,
    plat_norm: list[str],
    created_ms: int,
    updated_ms: int,
) -> dict[str, Any]:
    return {
        "delivery_id": delivery_id,
        "platforms": {p: _provider_platform_row_initial() for p in plat_norm},
        "created_at_unix_ms": created_ms,
        "updated_at_unix_ms": updated_ms,
    }


def _initial_activation_evidence_doc(
    *,
    delivery_id: str,
    created_ms: int,
    updated_ms: int,
) -> dict[str, Any]:
    return {
        "delivery_id": delivery_id,
        "records": [],
        "created_at_unix_ms": created_ms,
        "updated_at_unix_ms": updated_ms,
    }


def _initial_events_doc(*, delivery_id: str, created_ms: int) -> dict[str, Any]:
    return {
        "delivery_id": delivery_id,
        "events": [],
        "created_at_unix_ms": created_ms,
        "updated_at_unix_ms": created_ms,
    }


def mint_invite_ids_for_recipients(rec_norm: list[str]) -> dict[str, str]:
    """Return a fresh opaque ``invite_token_id`` for each normalized recipient email."""

    return {e: new_invite_token_id() for e in rec_norm}


def _initial_session_doc(
    *,
    delivery_id: str,
    rec_norm: list[str],
    plat_norm: list[str],
    release_mode: Literal["beta", "store", "both"],
    delivery_version: str,
    invite_by_email: dict[str, str],
    invite_hmac_key: str,
    created_ms: int,
    updated_ms: int,
) -> dict[str, Any]:
    doc: dict[str, Any] = {
        "delivery_id": delivery_id,
        "session_phase": "accepted",
        "release_mode": release_mode,
        "platforms": plat_norm,
        "compile_run_id": None,
        "delivery_version": delivery_version,
        "secrets": {"invite_hmac_key": invite_hmac_key},
        "pipeline": {
            "compile": _pipeline_stage_initial(),
            "build": _pipeline_stage_initial(),
            "package": _pipeline_stage_initial(),
            "distribution": _pipeline_stage_initial(),
            "release": _pipeline_stage_initial(),
        },
        "per_platform": {p: _per_platform_channels(release_mode=release_mode) for p in plat_norm},
        "per_recipient": {
            e: {
                "status": "pending",
                "invite_token_id": invite_by_email[e],
                "activation_proof": {
                    "status": "pending",
                    "provider_proof": "pending",
                    "app_proof": "pending",
                },
            }
            for e in rec_norm
        },
        "activation_proof": {
            "status": "pending",
            "recipients_total": len(rec_norm),
            "recipients_provider_satisfied": 0,
            "recipients_fully_satisfied": 0,
        },
        "store_release": {
            "status": "not_started",
            "active_promotion_lane": None,
            "last_promotion_requested_at_unix_ms": None,
            "ios": _mobile_store_lane_status("ios", plat_norm),
            "android": _mobile_store_lane_status("android", plat_norm),
        },
        "human_readiness_gate": {
            "status": "pending" if release_mode == "both" else "not_applicable",
            "passed_at_unix_ms": None,
            "operator_note": None,
        },
        "created_at_unix_ms": created_ms,
        "updated_at_unix_ms": updated_ms,
    }
    if release_mode == "both":
        doc["distribution_plan"] = {
            "sequence_phases": list(distribution_adapters.BOTH_MODE_DISTRIBUTION_PHASES),
            "current_phase": "beta_delivery",
            "beta_distribution_completed_at_unix_ms": None,
            "v1_excluded_channels": [
                "enterprise_distribution",
                "mdm_managed_install",
                "adb_sideload",
                "adhoc_device_lists",
            ],
            "notes": (
                "For release_mode=both, distribution jobs are ordered beta-before-store per "
                "iter_distribution_jobs; complete beta recipient delivery, run operator readiness "
                "(human gate), then promote store lanes (App Store / Play production)."
            ),
        }
    return doc


def _delivery_preflight_completed_payload(
    *,
    plat_norm: list[str],
    release_mode: Literal["beta", "store", "both"],
    issues: list[dict[str, Any]],
    enforced: bool,
) -> dict[str, Any]:
    """Structured per-platform / per-lane summary for :data:`DELIVERY_PREFLIGHT_COMPLETED`."""

    lanes_scheduled = distribution_adapters.release_lanes_for_mode(release_mode)
    lanes_set = set(lanes_scheduled)
    platforms_out: dict[str, Any] = {}
    for p in plat_norm:
        p_issues = [i for i in issues if str(i.get("platform")) == p]
        lanes_out: dict[str, Any] = {}
        for lane in ("beta", "store"):
            if lane not in lanes_set:
                lanes_out[lane] = {"applicable": False, "ok": True, "issue_count": 0, "reasons": []}
                continue
            li = [x for x in p_issues if str(x.get("lane")) == lane]
            lanes_out[lane] = {
                "applicable": True,
                "ok": len(li) == 0,
                "issue_count": len(li),
                "adapter_kinds": sorted({str(x.get("adapter_kind")) for x in li if x.get("adapter_kind")}),
                "reasons": [str(x.get("reason", "")) for x in li],
            }
        platforms_out[p] = {
            "ok": len(p_issues) == 0,
            "issue_count": len(p_issues),
            "lanes": lanes_out,
        }
    return {
        "ok": len(issues) == 0,
        "issue_count": len(issues),
        "enforced": enforced,
        "release_mode": release_mode,
        "platforms": platforms_out,
    }


def _merge_preflight_into_session_channels(
    *,
    session_obj: dict[str, Any],
    plat_norm: list[str],
    release_mode: Literal["beta", "store", "both"],
    by_platform: dict[str, list[dict[str, Any]]],
    enforced: bool,
) -> None:
    """Attach per-lane preflight issues under ``session.per_platform.*.channels.[beta|store].details``."""

    pp_all = session_obj.get("per_platform")
    if not isinstance(pp_all, dict):
        return
    lanes_scheduled = set(distribution_adapters.release_lanes_for_mode(release_mode))
    for p in plat_norm:
        p_entry_raw = pp_all.get(p)
        if not isinstance(p_entry_raw, dict):
            continue
        p_entry = dict(p_entry_raw)
        ch_raw = p_entry.get("channels")
        if not isinstance(ch_raw, dict):
            continue
        ch: dict[str, Any] = {}
        for lane_key in ("beta", "store"):
            row_raw = ch_raw.get(lane_key)
            ch[lane_key] = dict(cast(dict[str, Any], row_raw)) if isinstance(row_raw, dict) else {}
        for lane_name in ("beta", "store"):
            lane_row = ch.get(lane_name)
            if not isinstance(lane_row, dict):
                continue
            if lane_name not in lanes_scheduled:
                continue
            st = str(lane_row.get("status") or "")
            if st == "not_applicable":
                continue
            lane_issues = [row for row in by_platform.get(p, []) if str(row.get("lane")) == lane_name]
            det_raw = lane_row.get("details")
            det = dict(cast(dict[str, Any], det_raw)) if isinstance(det_raw, dict) else {}
            det["preflight"] = {
                "enforced": enforced,
                "ok": len(lane_issues) == 0,
                "issues": list(lane_issues),
            }
            lane_row["details"] = det
            if lane_issues:
                lane_row["status"] = "blocked"
            ch[lane_name] = lane_row
        p_entry["channels"] = ch
        pp_all[p] = p_entry


def _apply_distribution_preflight_to_new_session(
    *,
    project_dir: Path,
    session_obj: dict[str, Any],
    provider_state_obj: dict[str, Any],
    plat_norm: list[str],
    release_mode: Literal["beta", "store", "both"],
    delivery_id: str,
    tenant_id: str,
    repo_id: str,
    delivery_version: str,
    ts: int,
) -> list[dict[str, Any]]:
    """Mutate ``session_obj`` / provider sidecar doc with preflight results; return issue rows."""

    issues = distribution_adapters.collect_distribution_preflight_issues(
        project_dir=project_dir,
        tenant_id=tenant_id,
        repo_id=repo_id,
        delivery_id=delivery_id,
        delivery_version=delivery_version,
        platforms=plat_norm,
        release_mode=release_mode,
    )
    by_platform: dict[str, list[dict[str, Any]]] = {}
    for row in issues:
        by_platform.setdefault(str(row["platform"]), []).append(row)

    enforced = distribution_adapters.enforce_adapter_preflight()
    _merge_preflight_into_session_channels(
        session_obj=session_obj,
        plat_norm=plat_norm,
        release_mode=release_mode,
        by_platform=by_platform,
        enforced=enforced,
    )

    plat_state = provider_state_obj.get("platforms")
    if not isinstance(plat_state, dict):
        return issues

    for p in plat_norm:
        raw_row = plat_state.get(p)
        if not isinstance(raw_row, dict):
            continue
        prow = dict(raw_row)
        p_issues = by_platform.get(p, [])
        details_src = prow.get("details")
        details = dict(cast(dict[str, Any], details_src)) if isinstance(details_src, dict) else {}
        details["preflight"] = {
            "issues": list(p_issues),
            "enforced": enforced,
            "delivery_version": delivery_version,
        }
        prow["details"] = details
        prow["last_updated_at_unix_ms"] = ts
        jobs = list(
            distribution_adapters.iter_distribution_jobs(platforms=[p], release_mode=release_mode),
        )
        if p_issues:
            prow["status"] = "blocked"
            prow["last_error"] = "; ".join(str(x.get("reason", "")) for x in p_issues)
            prow["adapter_kind"] = str(p_issues[0]["adapter_kind"]) if len(p_issues) == 1 else "multi"
        else:
            prow["last_error"] = None
            if len(jobs) == 1:
                prow["adapter_kind"] = jobs[0][2].kind
            elif len(jobs) > 1:
                prow["adapter_kind"] = "multi"
            else:
                prow["adapter_kind"] = None
        plat_state[p] = prow

    if issues:
        session_obj["session_phase"] = "blocked"
        pipe = cast(dict[str, Any], session_obj.get("pipeline") or {})
        dist = dict(cast(dict[str, Any], pipe.get("distribution") or {}))
        dist["status"] = "blocked"
        dist["error"] = "; ".join(f"{i.get('platform')}/{i.get('lane')}: {i.get('reason')}" for i in issues)
        pipe["distribution"] = dist
        session_obj["pipeline"] = pipe

    return issues


def create_delivery_session(
    *,
    project_dir: Path,
    request_text: str,
    recipients: list[str],
    platforms: list[str],
    release_mode: Literal["beta", "store", "both"],
    app_stack: str = "react_expo_default",
    tenant_id: str | None = None,
    repo_id: str | None = None,
    delivery_version: str = "1.0.0",
    skip_distribution_preflight: bool = False,
) -> dict[str, Any]:
    """Create request, initial session, and empty event log; return summary including ``delivery_id``."""

    delivery_id = new_delivery_id()
    paths = delivery_paths(project_dir, delivery_id)
    paths["dir"].mkdir(parents=True, exist_ok=True)

    rec_norm = normalize_recipients(recipients)
    plat_norm = list(platforms)
    for p in plat_norm:
        if p not in PLATFORMS_ALLOWED:
            raise ValueError(f"unsupported platform: {p}")
    if release_mode not in RELEASE_MODES:
        raise ValueError(f"unsupported release_mode: {release_mode}")

    tenant_resolved = (tenant_id or "local").strip()
    repo_resolved = (repo_id or "local").strip()
    if not tenant_resolved or not repo_resolved:
        raise ValueError("tenant_id and repo_id must be non-empty when provided")

    ts = _now_ms()
    invite_by_email = mint_invite_ids_for_recipients(rec_norm)
    invite_hmac_key = new_invite_hmac_key()
    parsed = delivery_ingest.build_parsed_delivery_fields(
        request_text=request_text,
        cli_platforms=plat_norm,
        release_mode=release_mode,
        authoritative_recipients=rec_norm,
    )
    required_human_inputs = delivery_ingest.collect_prerequisite_human_inputs(
        project_dir=project_dir.resolve(),
        platforms=plat_norm,
        release_mode=release_mode,
    )
    required_accounts = delivery_ingest.infer_required_accounts_from_human_inputs(required_human_inputs)
    request_obj: dict[str, Any] = {
        "delivery_id": delivery_id,
        "request_text": request_text,
        "platforms": plat_norm,
        "recipients": rec_norm,
        "release_mode": release_mode,
        "app_stack": app_stack,
        "delivery_version": str(delivery_version),
        "derived_intent_ref": None,
        "required_accounts": required_accounts,
        "parsed": parsed,
        "required_human_inputs": required_human_inputs,
        "created_at_unix_ms": ts,
    }
    apply_schema_envelope(obj=request_obj, kind="delivery_request", version=1)
    validate_artifact_json(obj=request_obj, kind="delivery_request", version=1)

    session_obj = _initial_session_doc(
        delivery_id=delivery_id,
        rec_norm=rec_norm,
        plat_norm=plat_norm,
        release_mode=release_mode,
        delivery_version=str(delivery_version),
        invite_by_email=invite_by_email,
        invite_hmac_key=invite_hmac_key,
        created_ms=ts,
        updated_ms=ts,
    )
    apply_schema_envelope(obj=session_obj, kind="delivery_session", version=1)
    validate_artifact_json(obj=session_obj, kind="delivery_session", version=1)

    recipients_obj = _initial_recipients_sidecar_doc(
        delivery_id=delivery_id,
        rec_norm=rec_norm,
        plat_norm=plat_norm,
        invite_by_email=invite_by_email,
        created_ms=ts,
        updated_ms=ts,
    )
    apply_schema_envelope(obj=recipients_obj, kind="delivery_recipients", version=1)
    validate_artifact_json(obj=recipients_obj, kind="delivery_recipients", version=1)

    provider_state_obj = _initial_provider_state_doc(
        delivery_id=delivery_id,
        plat_norm=plat_norm,
        created_ms=ts,
        updated_ms=ts,
    )
    apply_schema_envelope(obj=provider_state_obj, kind="delivery_provider_state", version=1)
    validate_artifact_json(obj=provider_state_obj, kind="delivery_provider_state", version=1)

    preflight_issues: list[dict[str, Any]] = []
    if not skip_distribution_preflight:
        preflight_issues = _apply_distribution_preflight_to_new_session(
            project_dir=project_dir,
            session_obj=session_obj,
            provider_state_obj=provider_state_obj,
            plat_norm=plat_norm,
            release_mode=release_mode,
            delivery_id=delivery_id,
            tenant_id=tenant_resolved,
            repo_id=repo_resolved,
            delivery_version=str(delivery_version),
            ts=ts,
        )
        ts_preflight = _now_ms()
        session_obj["updated_at_unix_ms"] = ts_preflight
        provider_state_obj["updated_at_unix_ms"] = ts_preflight
        validate_artifact_json(obj=session_obj, kind="delivery_session", version=1)
        validate_artifact_json(obj=provider_state_obj, kind="delivery_provider_state", version=1)

    activation_obj = _initial_activation_evidence_doc(
        delivery_id=delivery_id,
        created_ms=ts,
        updated_ms=ts,
    )
    apply_schema_envelope(obj=activation_obj, kind="delivery_activation_evidence", version=1)
    validate_artifact_json(obj=activation_obj, kind="delivery_activation_evidence", version=1)

    events_obj = _initial_events_doc(delivery_id=delivery_id, created_ms=ts)
    apply_schema_envelope(obj=events_obj, kind="delivery_events", version=1)
    validate_artifact_json(obj=events_obj, kind="delivery_events", version=1)

    _write_json(paths["request"], request_obj)
    _write_json(paths["session"], session_obj)
    _write_json(paths["recipients"], recipients_obj)
    _write_json(paths["events"], events_obj)
    _write_json(paths["provider_state"], provider_state_obj)
    _write_json(paths["activation_evidence"], activation_obj)

    append_event(
        project_dir=project_dir,
        delivery_id=delivery_id,
        event_type=DELIVERY_REQUEST_ACCEPTED,
        payload={"recipients": rec_norm, "platforms": plat_norm, "release_mode": release_mode},
    )
    append_event(
        project_dir=project_dir,
        delivery_id=delivery_id,
        event_type=DELIVERY_REQUEST_PARSED,
        payload={
            "app_goal": parsed.get("app_goal"),
            "requested_platforms": parsed.get("requested_platforms"),
            "delivery_mode": parsed.get("delivery_mode"),
            "release_lanes": parsed.get("release_lanes"),
            "recipient_count": len(rec_norm),
            "warnings": parsed.get("warnings") or [],
            "required_human_inputs_count": len(required_human_inputs),
        },
    )
    if not skip_distribution_preflight:
        append_event(
            project_dir=project_dir,
            delivery_id=delivery_id,
            event_type=DELIVERY_PREFLIGHT_COMPLETED,
            payload=_delivery_preflight_completed_payload(
                plat_norm=plat_norm,
                release_mode=release_mode,
                issues=list(preflight_issues),
                enforced=distribution_adapters.enforce_adapter_preflight(),
            ),
        )
    append_delivery_control_audit_event(
        project_dir,
        action="delivery.recipients.initialized",
        details={"delivery_id": delivery_id, "recipients": list(rec_norm)},
    )
    append_delivery_control_audit_event(
        project_dir,
        action="delivery.preflight.decision",
        details={
            "delivery_id": delivery_id,
            "skipped": bool(skip_distribution_preflight),
            "blocked": len(preflight_issues) > 0,
            "issue_count": len(preflight_issues),
            "enforced": distribution_adapters.enforce_adapter_preflight(),
            "issues": list(preflight_issues)[:48],
        },
    )
    return {
        "delivery_id": delivery_id,
        "delivery_dir": str(paths["dir"]),
        "request": request_obj,
        "session": load_session(project_dir, delivery_id),
        "preflight_issues": list(preflight_issues),
        "required_human_inputs": list(required_human_inputs),
    }


def update_session_compile_stage(
    *,
    project_dir: Path,
    delivery_id: str,
    run_id: str | None,
    succeeded: bool,
    error: str | None = None,
) -> dict[str, Any]:
    """Mark the delivery session compile pipeline stage after an ``akc compile`` orchestration attempt."""

    assert_safe_delivery_id(delivery_id)
    paths = delivery_paths(project_dir, delivery_id)
    session_path = paths["session"]
    session = _read_json(session_path)
    validate_artifact_json(obj=session, kind="delivery_session", version=1)
    ts = _now_ms()
    session["compile_run_id"] = run_id
    session["updated_at_unix_ms"] = ts
    pipe_raw = session.get("pipeline")
    pipe = dict(cast(dict[str, Any], pipe_raw)) if isinstance(pipe_raw, dict) else {}
    compile_raw = pipe.get("compile")
    compile_stage = (
        dict(cast(dict[str, Any], compile_raw)) if isinstance(compile_raw, dict) else _pipeline_stage_initial()
    )
    compile_stage["status"] = "completed" if succeeded else "failed"
    compile_stage["completed_at_unix_ms"] = ts
    compile_stage["error"] = error
    compile_stage["run_id"] = run_id
    pipe["compile"] = compile_stage
    session["pipeline"] = pipe
    if succeeded:
        if session.get("session_phase") != "blocked":
            session["session_phase"] = "building"
    else:
        session["session_phase"] = "failed"
    _write_json(session_path, session)
    validate_artifact_json(obj=session, kind="delivery_session", version=1)
    _sync_delivery_control_index(project_dir, delivery_id)
    return session


def update_delivery_request_compile_handoff(
    *,
    project_dir: Path,
    delivery_id: str,
    handoff: Mapping[str, Any],
) -> dict[str, Any]:
    """Persist compile output pointers on ``request.json`` (delivery_plan / intent fingerprints)."""

    assert_safe_delivery_id(delivery_id)
    paths = delivery_paths(project_dir, delivery_id)
    req_path = paths["request"]
    req = _read_json(req_path)
    validate_artifact_json(obj=req, kind="delivery_request", version=1)
    slim: dict[str, Any] = {
        "compile_run_id": handoff.get("compile_run_id"),
        "manifest_present": bool(handoff.get("manifest_present")),
        "manifest_rel_path": handoff.get("manifest_rel_path"),
        "delivery_plan_rel_path": handoff.get("delivery_plan_rel_path"),
        "delivery_plan_loaded": bool(handoff.get("delivery_plan_loaded")),
        "delivery_plan_ref": handoff.get("delivery_plan_ref"),
        "promotion_readiness": handoff.get("promotion_readiness"),
        "runtime_bundle_rel_path": handoff.get("runtime_bundle_rel_path"),
    }
    err = handoff.get("error")
    if err:
        slim["error"] = err
    req["compile_outputs_ref"] = slim
    derived = handoff.get("derived_intent_ref")
    if isinstance(derived, dict) and derived:
        req["derived_intent_ref"] = dict(derived)
    _write_json(req_path, req)
    validate_artifact_json(obj=req, kind="delivery_request", version=1)
    return req


def update_session_compile_handoff(
    *,
    project_dir: Path,
    delivery_id: str,
    handoff: Mapping[str, Any],
) -> dict[str, Any]:
    """Mirror compile handoff on ``session.json`` for status/export (no duplication of plan payloads)."""

    assert_safe_delivery_id(delivery_id)
    paths = delivery_paths(project_dir, delivery_id)
    session_path = paths["session"]
    session = _read_json(session_path)
    validate_artifact_json(obj=session, kind="delivery_session", version=1)
    slim: dict[str, Any] = {
        "compile_run_id": handoff.get("compile_run_id"),
        "manifest_present": bool(handoff.get("manifest_present")),
        "manifest_rel_path": handoff.get("manifest_rel_path"),
        "delivery_plan_rel_path": handoff.get("delivery_plan_rel_path"),
        "delivery_plan_loaded": bool(handoff.get("delivery_plan_loaded")),
        "delivery_plan_ref": handoff.get("delivery_plan_ref"),
        "promotion_readiness": handoff.get("promotion_readiness"),
        "runtime_bundle_rel_path": handoff.get("runtime_bundle_rel_path"),
    }
    err = handoff.get("error")
    if err:
        slim["error"] = err
    session["compile_outputs_ref"] = slim
    session["updated_at_unix_ms"] = _now_ms()
    _write_json(session_path, session)
    validate_artifact_json(obj=session, kind="delivery_session", version=1)
    _sync_delivery_control_index(project_dir, delivery_id)
    return session


def update_session_pipeline_stage(
    *,
    project_dir: Path,
    delivery_id: str,
    stage_name: Literal["compile", "build", "package", "distribution", "release"],
    status: str,
    started_at_unix_ms: int | None = None,
    completed_at_unix_ms: int | None = None,
    error: str | None = None,
    run_id: str | None = None,
    outputs: dict[str, Any] | None = None,
    new_session_phase: str | None = None,
) -> dict[str, Any]:
    """Update one pipeline stage row (and optionally ``session_phase``) on ``session.json``."""

    assert_safe_delivery_id(delivery_id)
    paths = delivery_paths(project_dir, delivery_id)
    session_path = paths["session"]
    session = _read_json(session_path)
    validate_artifact_json(obj=session, kind="delivery_session", version=1)
    ts = _now_ms()
    session["updated_at_unix_ms"] = ts
    pipe_raw = session.get("pipeline")
    pipe = dict(cast(dict[str, Any], pipe_raw)) if isinstance(pipe_raw, dict) else {}
    stage_raw = pipe.get(stage_name)
    stage = dict(cast(dict[str, Any], stage_raw)) if isinstance(stage_raw, dict) else _pipeline_stage_initial()
    stage["status"] = status
    if started_at_unix_ms is not None:
        stage["started_at_unix_ms"] = started_at_unix_ms
    if completed_at_unix_ms is not None:
        stage["completed_at_unix_ms"] = completed_at_unix_ms
    if error is not None:
        stage["error"] = error
    if run_id is not None:
        stage["run_id"] = run_id
    if outputs is not None:
        stage["outputs"] = outputs
    pipe[stage_name] = stage
    session["pipeline"] = pipe
    if new_session_phase is not None:
        session["session_phase"] = new_session_phase
    _write_json(session_path, session)
    validate_artifact_json(obj=session, kind="delivery_session", version=1)
    _sync_delivery_control_index(project_dir, delivery_id)
    return session


def load_request(project_dir: Path, delivery_id: str) -> dict[str, Any]:
    p = delivery_paths(project_dir, delivery_id)["request"]
    if not p.is_file():
        raise FileNotFoundError(f"missing delivery request: {p}")
    doc = _read_json(p)
    validate_artifact_json(obj=doc, kind="delivery_request", version=1)
    return doc


def load_session(project_dir: Path, delivery_id: str) -> dict[str, Any]:
    p = delivery_paths(project_dir, delivery_id)["session"]
    if not p.is_file():
        raise FileNotFoundError(f"missing delivery session: {p}")
    doc = _read_json(p)
    validate_artifact_json(obj=doc, kind="delivery_session", version=1)
    return doc


def load_events(project_dir: Path, delivery_id: str) -> list[dict[str, Any]]:
    p = delivery_paths(project_dir, delivery_id)["events"]
    if not p.is_file():
        raise FileNotFoundError(f"missing delivery events: {p}")
    doc = _read_json(p)
    validate_artifact_json(obj=doc, kind="delivery_events", version=1)
    ev = doc.get("events")
    if not isinstance(ev, list):
        raise ValueError("invalid events.json shape")
    return [cast(dict[str, Any], x) for x in ev]


def _sync_delivery_control_index(project_dir: Path, delivery_id: str) -> None:
    """Refresh tenant operations index row when ``project_dir`` matches scoped outputs layout."""

    assert_safe_delivery_id(delivery_id)
    try:
        req = load_request(project_dir, delivery_id)
        sess = load_session(project_dir, delivery_id)
        ev = load_events(project_dir, delivery_id)
    except (FileNotFoundError, ValueError, OSError):
        return
    try_sync_delivery_session_from_project(
        project_dir,
        delivery_id,
        request=req,
        session=sess,
        events=ev,
    )


def load_recipients_sidecar(project_dir: Path, delivery_id: str) -> dict[str, Any]:
    p = delivery_paths(project_dir, delivery_id)["recipients"]
    if not p.is_file():
        raise FileNotFoundError(f"missing delivery recipients sidecar: {p}")
    doc = _read_json(p)
    validate_artifact_json(obj=doc, kind="delivery_recipients", version=1)
    return doc


def load_provider_state_sidecar(project_dir: Path, delivery_id: str) -> dict[str, Any]:
    p = delivery_paths(project_dir, delivery_id)["provider_state"]
    if not p.is_file():
        raise FileNotFoundError(f"missing delivery provider_state: {p}")
    doc = _read_json(p)
    validate_artifact_json(obj=doc, kind="delivery_provider_state", version=1)
    return doc


def load_activation_evidence_sidecar(project_dir: Path, delivery_id: str) -> dict[str, Any]:
    p = delivery_paths(project_dir, delivery_id)["activation_evidence"]
    if not p.is_file():
        raise FileNotFoundError(f"missing delivery activation_evidence: {p}")
    doc = _read_json(p)
    validate_artifact_json(obj=doc, kind="delivery_activation_evidence", version=1)
    return doc


def append_event(
    *,
    project_dir: Path,
    delivery_id: str,
    event_type: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    paths = delivery_paths(project_dir, delivery_id)
    events_path = paths["events"]
    if not events_path.is_file():
        raise FileNotFoundError(f"missing delivery events: {events_path}")
    doc = _read_json(events_path)
    validate_artifact_json(obj=doc, kind="delivery_events", version=1)
    ev_list = doc.get("events")
    if not isinstance(ev_list, list):
        raise ValueError("invalid events.json shape")
    row: dict[str, Any] = {
        "schema": SCHEMA_DELIVERY_EVENT_ENVELOPE_V1,
        "delivery_id": delivery_id,
        "event_type": event_type,
        "occurred_at_unix_ms": _now_ms(),
        "payload": payload,
    }
    ev_list = list(ev_list)
    ev_list.append(row)
    doc["events"] = ev_list
    doc["updated_at_unix_ms"] = _now_ms()
    _write_json(events_path, doc)
    validate_artifact_json(obj=doc, kind="delivery_events", version=1)

    sess_path = paths["session"]
    if sess_path.is_file():
        session = _read_json(sess_path)
        session["updated_at_unix_ms"] = _now_ms()
        _write_json(sess_path, session)
        validate_artifact_json(obj=session, kind="delivery_session", version=1)

    _sync_delivery_control_index(project_dir, delivery_id)

    return row


def record_resend(
    *,
    project_dir: Path,
    delivery_id: str,
    recipient: str,
) -> dict[str, Any]:
    rec = normalize_email(recipient)
    req = load_request(project_dir, delivery_id)
    allowed = set(cast(list[str], req.get("recipients", [])))
    if rec not in allowed:
        raise ValueError(f"recipient {rec!r} is not part of this delivery")
    rpath = delivery_paths(project_dir, delivery_id)["recipients"]
    if rpath.is_file():
        rdoc = _read_json(rpath)
        rmap = rdoc.get("recipients")
        if isinstance(rmap, dict) and rec in rmap and isinstance(rmap[rec], dict):
            prev = cast(dict[str, Any], rmap[rec])
            updated_row = dict(prev)
            updated_row["resend_count"] = int(prev.get("resend_count", 0)) + 1
            rdoc["recipients"] = {**cast(dict[str, Any], rmap), rec: updated_row}
            rdoc["updated_at_unix_ms"] = _now_ms()
            _write_json(rpath, rdoc)
            validate_artifact_json(obj=rdoc, kind="delivery_recipients", version=1)
    append_delivery_control_audit_event(
        project_dir,
        action="delivery.invite.resend",
        details={"delivery_id": delivery_id, "recipient": rec},
    )
    return append_event(
        project_dir=project_dir,
        delivery_id=delivery_id,
        event_type=DELIVERY_INVITE_RESEND_REQUESTED,
        payload={"recipient": rec},
    )


def update_session_channel_lane(
    *,
    project_dir: Path,
    delivery_id: str,
    platform: str,
    lane: Literal["beta", "store"],
    status: str,
    details_patch: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Update ``session.per_platform[platform].channels[lane]`` (merge ``details``)."""

    assert_safe_delivery_id(delivery_id)
    session_path = delivery_paths(project_dir, delivery_id)["session"]
    session = _read_json(session_path)
    validate_artifact_json(obj=session, kind="delivery_session", version=1)
    pp_raw = session.get("per_platform")
    if not isinstance(pp_raw, dict):
        pp_raw = {}
    pp = dict(cast(dict[str, Any], pp_raw))
    plat_raw = pp.get(platform)
    plat = dict(cast(dict[str, Any], plat_raw)) if isinstance(plat_raw, dict) else {}
    ch_raw = plat.get("channels")
    ch: dict[str, Any] = {}
    if isinstance(ch_raw, dict):
        for k in ("beta", "store"):
            row = ch_raw.get(k)
            ch[k] = dict(cast(dict[str, Any], row)) if isinstance(row, dict) else {}
    lane_row = dict(ch.get(lane) or {})
    lane_row["status"] = status
    det_raw = lane_row.get("details")
    det = dict(cast(dict[str, Any], det_raw)) if isinstance(det_raw, dict) else {}
    if details_patch:
        det.update(details_patch)
    lane_row["details"] = det
    ch[lane] = lane_row
    plat["channels"] = ch
    pp[platform] = plat
    session["per_platform"] = pp
    session["updated_at_unix_ms"] = _now_ms()
    _write_json(session_path, session)
    validate_artifact_json(obj=session, kind="delivery_session", version=1)
    _sync_delivery_control_index(project_dir, delivery_id)
    return session


def touch_provider_platform_row(
    *,
    project_dir: Path,
    delivery_id: str,
    platform: str,
    adapter_kind: str | None,
    status: str,
    last_error: str | None = None,
    external_refs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Update ``provider_state.json`` row for one platform (non-secret refs only)."""

    assert_safe_delivery_id(delivery_id)
    path = delivery_paths(project_dir, delivery_id)["provider_state"]
    doc = _read_json(path)
    validate_artifact_json(obj=doc, kind="delivery_provider_state", version=1)
    plats_raw = doc.get("platforms")
    if not isinstance(plats_raw, dict):
        plats_raw = {}
    plats = dict(cast(dict[str, Any], plats_raw))
    row_raw = plats.get(platform)
    row = dict(cast(dict[str, Any], row_raw)) if isinstance(row_raw, dict) else _provider_platform_row_initial()
    if adapter_kind is not None:
        row["adapter_kind"] = adapter_kind
    row["status"] = status
    row["last_error"] = last_error
    row["last_updated_at_unix_ms"] = _now_ms()
    refs_raw = row.get("external_refs")
    refs = dict(cast(dict[str, Any], refs_raw)) if isinstance(refs_raw, dict) else {}
    if external_refs:
        refs.update(external_refs)
    row["external_refs"] = refs
    plats[platform] = row
    doc["platforms"] = plats
    doc["updated_at_unix_ms"] = _now_ms()
    _write_json(path, doc)
    validate_artifact_json(obj=doc, kind="delivery_provider_state", version=1)
    return doc


def update_distribution_plan_phase(
    *,
    project_dir: Path,
    delivery_id: str,
    current_phase: str,
    beta_completed_at_unix_ms: int | None = None,
) -> dict[str, Any]:
    assert_safe_delivery_id(delivery_id)
    session_path = delivery_paths(project_dir, delivery_id)["session"]
    session = _read_json(session_path)
    validate_artifact_json(obj=session, kind="delivery_session", version=1)
    dp_raw = session.get("distribution_plan")
    dp = dict(cast(dict[str, Any], dp_raw)) if isinstance(dp_raw, dict) else {}
    dp["current_phase"] = current_phase
    if beta_completed_at_unix_ms is not None:
        dp["beta_distribution_completed_at_unix_ms"] = beta_completed_at_unix_ms
    session["distribution_plan"] = dp
    session["updated_at_unix_ms"] = _now_ms()
    _write_json(session_path, session)
    validate_artifact_json(obj=session, kind="delivery_session", version=1)
    _sync_delivery_control_index(project_dir, delivery_id)
    return session


def ensure_human_readiness_gate_pending(
    *,
    project_dir: Path,
    delivery_id: str,
) -> dict[str, Any]:
    """If ``release_mode`` is ``both``, ensure gate is ``pending`` after beta distribution."""

    assert_safe_delivery_id(delivery_id)
    session_path = delivery_paths(project_dir, delivery_id)["session"]
    session = _read_json(session_path)
    validate_artifact_json(obj=session, kind="delivery_session", version=1)
    if str(session.get("release_mode") or "") != "both":
        return session
    gate_raw = session.get("human_readiness_gate")
    gate = dict(cast(dict[str, Any], gate_raw)) if isinstance(gate_raw, dict) else {}
    if str(gate.get("status") or "") not in {"passed"}:
        gate["status"] = "pending"
    session["human_readiness_gate"] = gate
    session["updated_at_unix_ms"] = _now_ms()
    _write_json(session_path, session)
    validate_artifact_json(obj=session, kind="delivery_session", version=1)
    _sync_delivery_control_index(project_dir, delivery_id)
    return session


def record_human_readiness_gate_pass(
    *,
    project_dir: Path,
    delivery_id: str,
    operator_note: str | None = None,
) -> dict[str, Any]:
    assert_safe_delivery_id(delivery_id)
    session_path = delivery_paths(project_dir, delivery_id)["session"]
    session = _read_json(session_path)
    validate_artifact_json(obj=session, kind="delivery_session", version=1)
    if str(session.get("release_mode") or "") != "both":
        raise ValueError("human readiness gate applies only when release_mode is both")
    gate_raw = session.get("human_readiness_gate")
    gate = dict(cast(dict[str, Any], gate_raw)) if isinstance(gate_raw, dict) else {}
    gate["status"] = "passed"
    gate["passed_at_unix_ms"] = _now_ms()
    gate["operator_note"] = operator_note
    session["human_readiness_gate"] = gate
    dp_raw = session.get("distribution_plan")
    dp = dict(cast(dict[str, Any], dp_raw)) if isinstance(dp_raw, dict) else {}
    dp["current_phase"] = "store_promotion"
    session["distribution_plan"] = dp
    session["updated_at_unix_ms"] = _now_ms()
    _write_json(session_path, session)
    validate_artifact_json(obj=session, kind="delivery_session", version=1)
    append_delivery_control_audit_event(
        project_dir,
        action="delivery.human_gate.passed",
        details={"delivery_id": delivery_id},
    )
    return append_event(
        project_dir=project_dir,
        delivery_id=delivery_id,
        event_type=DELIVERY_HUMAN_GATE_PASSED,
        payload={"operator_note": operator_note},
    )


def record_promote(
    *,
    project_dir: Path,
    delivery_id: str,
    lane: Literal["beta", "store"],
) -> dict[str, Any]:
    if lane not in PROMOTION_LANES:
        raise ValueError(f"invalid lane: {lane!r}")
    req = load_request(project_dir, delivery_id)
    sess = load_session(project_dir, delivery_id)
    release_mode = str(req.get("release_mode") or "")
    if lane == "store" and release_mode == "both":
        gate = sess.get("human_readiness_gate")
        if not isinstance(gate, dict) or str(gate.get("status") or "") != "passed":
            raise ValueError(
                "release_mode=both blocks store promotion until the human readiness gate is passed; "
                "run `akc deliver gate pass --delivery-id <id>` first",
            )
    store_rel = sess.get("store_release")
    if not isinstance(store_rel, dict):
        store_rel = {}
    store_rel = dict(store_rel)
    store_rel["status"] = "promotion_requested"
    store_rel["active_promotion_lane"] = lane
    store_rel["last_promotion_requested_at_unix_ms"] = _now_ms()
    ios = store_rel.get("ios")
    if isinstance(ios, dict):
        store_rel["ios"] = dict(ios)
    android = store_rel.get("android")
    if isinstance(android, dict):
        store_rel["android"] = dict(android)
    sess["store_release"] = store_rel
    sess["updated_at_unix_ms"] = _now_ms()
    session_path = delivery_paths(project_dir, delivery_id)["session"]
    _write_json(session_path, sess)
    validate_artifact_json(obj=sess, kind="delivery_session", version=1)
    append_delivery_control_audit_event(
        project_dir,
        action="delivery.store.promote",
        details={"delivery_id": delivery_id, "lane": lane},
    )
    return append_event(
        project_dir=project_dir,
        delivery_id=delivery_id,
        event_type=DELIVERY_STORE_PROMOTION_REQUESTED,
        payload={"lane": lane},
    )


def record_store_submitted(
    *,
    project_dir: Path,
    delivery_id: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Record App Store / Play submission milestone (provider-specific details in ``payload``)."""

    return append_event(
        project_dir=project_dir,
        delivery_id=delivery_id,
        event_type=DELIVERY_STORE_SUBMITTED,
        payload=dict(payload or {}),
    )


def record_store_live(
    *,
    project_dir: Path,
    delivery_id: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Record store listing / track live availability."""

    return append_event(
        project_dir=project_dir,
        delivery_id=delivery_id,
        event_type=DELIVERY_STORE_LIVE,
        payload=dict(payload or {}),
    )


def amend_delivery_recipients(
    *,
    project_dir: Path,
    delivery_id: str,
    additional_recipients: list[str],
) -> dict[str, Any]:
    """Append new unique recipient emails; updates request, session, and recipients sidecar (tenant-scoped)."""

    assert_safe_delivery_id(delivery_id)
    if not additional_recipients:
        raise ValueError("additional_recipients must be non-empty")
    seen: set[str] = set()
    add_list: list[str] = []
    for raw in additional_recipients:
        n = normalize_email(str(raw))
        if n in seen:
            continue
        seen.add(n)
        add_list.append(n)
    if not add_list:
        raise ValueError("no valid recipient emails to add")

    req_path = delivery_paths(project_dir, delivery_id)["request"]
    session_path = delivery_paths(project_dir, delivery_id)["session"]
    rpath = delivery_paths(project_dir, delivery_id)["recipients"]
    req = _read_json(req_path)
    validate_artifact_json(obj=req, kind="delivery_request", version=1)
    session = _read_json(session_path)
    validate_artifact_json(obj=session, kind="delivery_session", version=1)
    rdoc = _read_json(rpath)
    validate_artifact_json(obj=rdoc, kind="delivery_recipients", version=1)

    cur_raw = req.get("recipients")
    existing_list = list(cast(list[str], cur_raw)) if isinstance(cur_raw, list) else []
    existing = set(existing_list)
    new_emails = [e for e in add_list if e not in existing]
    if not new_emails:
        raise ValueError("all additional recipients are already on this delivery")

    before_list = list(existing_list)
    merged = existing_list + new_emails
    req["recipients"] = merged

    invite_new = mint_invite_ids_for_recipients(new_emails)
    plat_norm = list(cast(list[str], session.get("platforms") or []))
    per = session.get("per_recipient")
    per_map = dict(cast(dict[str, Any], per)) if isinstance(per, dict) else {}
    for e in new_emails:
        per_map[e] = {
            "status": "pending",
            "invite_token_id": invite_new[e],
            "activation_proof": {
                "status": "pending",
                "provider_proof": "pending",
                "app_proof": "pending",
            },
        }
    session["per_recipient"] = per_map

    rollup = session.get("activation_proof")
    if isinstance(rollup, dict):
        rl = dict(cast(dict[str, Any], rollup))
        rl["recipients_total"] = len(merged)
        session["activation_proof"] = rl

    rmap = rdoc.get("recipients")
    rmap_d = dict(cast(dict[str, Any], rmap)) if isinstance(rmap, dict) else {}
    for e in new_emails:
        rmap_d[e] = _recipients_sidecar_row(email=e, platforms=plat_norm, invite_token_id=invite_new[e])
    rdoc["recipients"] = rmap_d
    rdoc["updated_at_unix_ms"] = _now_ms()
    session["updated_at_unix_ms"] = _now_ms()

    validate_artifact_json(obj=req, kind="delivery_request", version=1)
    validate_artifact_json(obj=session, kind="delivery_session", version=1)
    validate_artifact_json(obj=rdoc, kind="delivery_recipients", version=1)
    _write_json(req_path, req)
    _write_json(session_path, session)
    _write_json(rpath, rdoc)

    append_delivery_control_audit_event(
        project_dir,
        action="delivery.recipients.changed",
        details={
            "delivery_id": delivery_id,
            "before": before_list,
            "after": list(merged),
            "added": new_emails,
        },
    )
    return append_event(
        project_dir=project_dir,
        delivery_id=delivery_id,
        event_type=DELIVERY_RECIPIENTS_AMENDED,
        payload={"added": new_emails, "recipient_count": len(merged)},
    )


def _invite_hmac_key_from_session(session: dict[str, Any]) -> str:
    sec = session.get("secrets")
    if isinstance(sec, dict):
        raw = str(sec.get("invite_hmac_key") or "").strip()
        if raw:
            return raw
    raise ValueError("delivery session missing secrets.invite_hmac_key (re-run akc deliver for this delivery)")


def sync_session_activation_from_evidence(*, project_dir: Path, delivery_id: str) -> dict[str, Any]:
    """Recompute ``session.json`` activation fields from ``activation_evidence.json``."""

    paths = delivery_paths(project_dir, delivery_id)
    session = _read_json(paths["session"])
    validate_artifact_json(obj=session, kind="delivery_session", version=1)
    ev_doc = _read_json(paths["activation_evidence"])
    validate_artifact_json(obj=ev_doc, kind="delivery_activation_evidence", version=1)
    sidecar = _read_json(paths["recipients"])
    validate_artifact_json(obj=sidecar, kind="delivery_recipients", version=1)
    records_raw = ev_doc.get("records")
    records = [cast(dict[str, Any], x) for x in records_raw] if isinstance(records_raw, list) else []

    prev_status: dict[str, str] = {}
    pr = session.get("per_recipient")
    if isinstance(pr, dict):
        for em, row in pr.items():
            if isinstance(row, dict):
                prev_status[str(em)] = str(row.get("status") or "")

    recompute_delivery_activation(
        session,
        evidence_records=records,
        recipients_sidecar=sidecar,
    )
    ts = _now_ms()
    session["updated_at_unix_ms"] = ts
    _write_json(paths["session"], session)
    validate_artifact_json(obj=session, kind="delivery_session", version=1)

    pr2 = session.get("per_recipient")
    if isinstance(pr2, dict):
        for em, row in pr2.items():
            if not isinstance(row, dict):
                continue
            email = str(em)
            if str(row.get("status") or "") == "active" and prev_status.get(email) != "active":
                append_event(
                    project_dir=project_dir,
                    delivery_id=delivery_id,
                    event_type=DELIVERY_RECIPIENT_ACTIVE,
                    payload={"recipient": email},
                )
    _sync_delivery_control_index(project_dir, delivery_id)
    return session


def append_activation_evidence_records(
    *,
    project_dir: Path,
    delivery_id: str,
    records: list[dict[str, Any]],
) -> None:
    """Append evidence rows and recompute session activation once."""

    if not records:
        return
    paths = delivery_paths(project_dir, delivery_id)
    ev_path = paths["activation_evidence"]
    doc = _read_json(ev_path)
    validate_artifact_json(obj=doc, kind="delivery_activation_evidence", version=1)
    recs = doc.get("records")
    if not isinstance(recs, list):
        raise ValueError("activation_evidence records must be a list")
    recs = list(recs) + list(records)
    doc["records"] = recs
    doc["updated_at_unix_ms"] = _now_ms()
    _write_json(ev_path, doc)
    validate_artifact_json(obj=doc, kind="delivery_activation_evidence", version=1)
    sync_session_activation_from_evidence(project_dir=project_dir, delivery_id=delivery_id)


def append_activation_evidence_record(
    *,
    project_dir: Path,
    delivery_id: str,
    record: dict[str, Any],
) -> dict[str, Any]:
    """Append one validated evidence row and refresh activation state on the session."""

    append_activation_evidence_records(project_dir=project_dir, delivery_id=delivery_id, records=[record])
    return record


def ingest_client_activation_report(
    *,
    project_dir: Path,
    delivery_id: str,
    payload: dict[str, Any],
) -> list[dict[str, Any]]:
    """Append app-side first-run / heartbeat evidence; **invite token must match** the session.

    Required for invite-based sign-in: ``recipient_token_id`` must resolve to a session recipient.
    First-run proof requires ``first_run_at_unix_ms`` on at least one report per platform.
    """

    if str(payload.get("delivery_id") or "").strip() != delivery_id:
        raise ValueError("payload delivery_id does not match delivery")
    token = str(payload.get("recipient_token_id") or "").strip()
    if not token:
        raise ValueError("recipient_token_id is required for activation reporting")
    platform = str(payload.get("platform") or "").strip().lower()
    if platform not in PLATFORMS_ALLOWED:
        raise ValueError(f"invalid platform: {platform!r}")
    session = load_session(project_dir, delivery_id)
    per = session.get("per_recipient")
    if not isinstance(per, dict):
        raise ValueError("invalid session per_recipient")
    email = resolve_recipient_email_for_token(per, token)
    if email is None:
        raise ValueError("recipient_token_id does not match this delivery (tenant isolation)")
    rdoc = load_recipients_sidecar(project_dir, delivery_id)
    rmap = rdoc.get("recipients")
    if not isinstance(rmap, dict) or email not in rmap:
        raise ValueError("recipient is not part of this delivery")
    row = cast(dict[str, Any], rmap[email])
    plats = row.get("platforms")
    allowed = {str(p) for p in plats} if isinstance(plats, list) else set()
    if platform not in allowed:
        raise ValueError(f"platform {platform!r} not enabled for recipient {email!r}")

    app_version = str(payload.get("app_version") or "").strip()
    batch: list[dict[str, Any]] = []
    ts_wall = _now_ms()
    if "first_run_at_unix_ms" in payload and payload["first_run_at_unix_ms"] is not None:
        fr = int(payload["first_run_at_unix_ms"])
        batch.append(
            {
                "record_id": str(uuid.uuid4()),
                "recipient_email": email,
                "recipient_token_id": token,
                "platform": platform,
                "evidence_kind": "app_first_run",
                "occurred_at_unix_ms": fr,
                "payload": {
                    "app_version": app_version,
                    "ingested_at_unix_ms": ts_wall,
                    "source": "client_activation_report",
                },
            },
        )
    hb = payload.get("heartbeat_at_unix_ms")
    if hb is not None:
        batch.append(
            {
                "record_id": str(uuid.uuid4()),
                "recipient_email": email,
                "recipient_token_id": token,
                "platform": platform,
                "evidence_kind": "heartbeat",
                "occurred_at_unix_ms": int(hb),
                "payload": {
                    "app_version": app_version,
                    "active": bool(payload.get("active", True)),
                    "ingested_at_unix_ms": ts_wall,
                    "source": "client_activation_report",
                },
            },
        )

    if not batch:
        raise ValueError("expected first_run_at_unix_ms and/or heartbeat_at_unix_ms in payload")
    append_activation_evidence_records(project_dir=project_dir, delivery_id=delivery_id, records=batch)
    if any(r["evidence_kind"] == "app_first_run" for r in batch):
        append_event(
            project_dir=project_dir,
            delivery_id=delivery_id,
            event_type=DELIVERY_ACTIVATION_FIRST_RUN,
            payload={"recipient": email, "platform": platform, "recipient_token_id": token},
        )
    return batch


def record_provider_install_detected(
    *,
    project_dir: Path,
    delivery_id: str,
    recipient_email: str,
    platform: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Append ``provider_install`` evidence for a named beta recipient (TestFlight / Firebase, etc.)."""

    email = normalize_email(recipient_email)
    if platform not in PLATFORMS_ALLOWED:
        raise ValueError(f"invalid platform: {platform!r}")
    session = load_session(project_dir, delivery_id)
    per = session.get("per_recipient")
    if not isinstance(per, dict) or email not in per:
        raise ValueError(f"recipient {email!r} is not part of this delivery")
    row = cast(dict[str, Any], per[email])
    token = str(row.get("invite_token_id") or "").strip()
    if not token:
        raise ValueError("recipient missing invite_token_id")
    ts = _now_ms()
    rec = {
        "record_id": str(uuid.uuid4()),
        "recipient_email": email,
        "recipient_token_id": token,
        "platform": platform,
        "evidence_kind": "provider_install",
        "occurred_at_unix_ms": ts,
        "payload": dict(payload or {}),
    }
    append_activation_evidence_record(project_dir=project_dir, delivery_id=delivery_id, record=rec)
    append_event(
        project_dir=project_dir,
        delivery_id=delivery_id,
        event_type=DELIVERY_PROVIDER_INSTALL_DETECTED,
        payload={"recipient": email, "platform": platform, "kind": "provider_install"},
    )
    return rec


def record_web_invite_opened(
    *,
    project_dir: Path,
    delivery_id: str,
    invite_token_id: str,
    signature: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Record provider proof for web: signed invite link open (mandatory for web beta provider evidence)."""

    tok = str(invite_token_id).strip()
    sig = str(signature).strip()
    if not tok or not sig:
        raise ValueError("invite_token_id and signature are required")
    session = load_session(project_dir, delivery_id)
    key = _invite_hmac_key_from_session(session)
    if not verify_invite_query(delivery_id=delivery_id, invite_token_id=tok, key=key, signature=sig):
        raise ValueError("invite signature verification failed")
    per = session.get("per_recipient")
    if not isinstance(per, dict):
        raise ValueError("invalid session per_recipient")
    email = resolve_recipient_email_for_token(per, tok)
    if email is None:
        raise ValueError("invite_token_id does not match this delivery")
    ts = _now_ms()
    rec = {
        "record_id": str(uuid.uuid4()),
        "recipient_email": email,
        "recipient_token_id": tok,
        "platform": "web",
        "evidence_kind": "invite_opened",
        "occurred_at_unix_ms": ts,
        "payload": dict(payload or {}),
    }
    append_activation_evidence_record(project_dir=project_dir, delivery_id=delivery_id, record=rec)
    append_event(
        project_dir=project_dir,
        delivery_id=delivery_id,
        event_type=DELIVERY_PROVIDER_INSTALL_DETECTED,
        payload={"recipient": email, "kind": "web_invite_opened", "invite_token_id": tok},
    )
    return rec
