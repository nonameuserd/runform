"""Run provider distribution after packaging (TestFlight, Firebase App Distribution, Play, web invites).

Respects ``release_mode`` sequencing: for ``both``, callers should pass ``lanes=("beta",)``
for the post-package wave and ``lanes=("store",)`` only after human gate + ``promote``.

Adapter preflight already ran at session creation; execution failures are recorded on
``session.per_platform.*.channels`` and ``provider_state.json``.
"""

from __future__ import annotations

import os
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any, cast

from akc.delivery import adapters as distribution_adapters
from akc.delivery import provider_clients
from akc.delivery import store as delivery_store
from akc.delivery.control_index import append_delivery_control_audit_event
from akc.delivery.event_types import (
    DELIVERY_FAILED,
    DELIVERY_INVITE_SENT,
    DELIVERY_STORE_SUBMITTED,
)
from akc.delivery.ingest import load_operator_prereqs_manifest, probe_android_application_id
from akc.delivery.invites import build_signed_web_invite_url
from akc.delivery.types import PlatformBuildSpec, ReleaseLane, ReleaseMode


def _env_list(name: str) -> list[str]:
    raw = str(os.environ.get(name, "") or "").strip()
    if not raw:
        return []
    return [x.strip() for x in raw.replace(";", ",").split(",") if x.strip()]


def _firebase_group_aliases() -> list[str]:
    return _env_list("AKC_DELIVERY_FIREBASE_APP_DIST_GROUPS") or _env_list("FIREBASE_APP_DISTRIBUTION_GROUPS")


def lanes_for_post_package_wave(release_mode: ReleaseMode) -> tuple[ReleaseLane, ...]:
    """First automated distribution wave after packaging (beta-only when ``both``)."""

    if release_mode == "both":
        return ("beta",)
    return distribution_adapters.release_lanes_for_mode(release_mode)


def _packaging_outputs_per_platform(session: dict[str, Any]) -> dict[str, Any]:
    pipe = session.get("pipeline")
    if not isinstance(pipe, dict):
        return {}
    pkg = pipe.get("package")
    if not isinstance(pkg, dict):
        return {}
    out = pkg.get("outputs")
    if not isinstance(out, dict):
        return {}
    pp = out.get("per_platform")
    return dict(pp) if isinstance(pp, dict) else {}


def _web_invite_base(*, project_dir: Path, spec_meta: dict[str, Any]) -> str:
    raw = spec_meta.get("web_invite_base_url")
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    env = str(os.environ.get("AKC_DELIVERY_WEB_INVITE_BASE_URL", "") or "").strip()
    if env:
        return env
    op = load_operator_prereqs_manifest(project_dir)
    web = op.get("web")
    if isinstance(web, dict):
        u = web.get("invite_base_url") or web.get("hosting_endpoint")
        if isinstance(u, str) and u.strip():
            return u.strip()
    return ""


def _dispatch_web_invite(
    *,
    project_dir: Path,
    delivery_id: str,
    recipients: list[str],
    tenant_id: str,
    repo_id: str,
    spec: PlatformBuildSpec,
    invite_by_email: dict[str, str],
    invite_hmac_key: str,
    lane: ReleaseLane,
) -> dict[str, Any]:
    base = _web_invite_base(project_dir=project_dir, spec_meta=dict(spec.metadata))
    if not base:
        return {
            "ok": False,
            "error": "web_invite_base_url not configured (metadata, env, or operator_prereqs)",
        }
    if not invite_hmac_key:
        return {"ok": False, "error": "session invite_hmac_key missing"}

    urls: dict[str, str] = {}
    for email in recipients:
        token_id = invite_by_email.get(email.strip())
        if not token_id:
            continue
        urls[email] = build_signed_web_invite_url(
            invite_base_url=base,
            delivery_id=delivery_id,
            invite_token_id=token_id,
            key=invite_hmac_key,
        )
    if not urls:
        return {"ok": False, "error": "no recipient invite tokens resolved for web distribution"}
    outbound_note = (
        "Web invite URLs generated locally; outbound email is operator responsibility "
        "(SMTP/SendGrid/etc. per adapters preflight)."
    )
    return {
        "ok": True,
        "lane": lane,
        "invite_urls_by_email": urls,
        "note": outbound_note,
    }


def _dispatch_testflight(
    *,
    emails: list[str],
    tenant_id: str,
    repo_id: str,
) -> dict[str, Any]:
    gid = str(os.environ.get("AKC_DELIVERY_ASC_BETA_GROUP_ID", "") or "").strip()
    if not gid:
        return {
            "ok": False,
            "error": "AKC_DELIVERY_ASC_BETA_GROUP_ID is required for TestFlight invitations via API",
        }
    try:
        return provider_clients.asc_invite_emails_to_beta_group(
            emails=emails,
            beta_group_id=gid,
            tenant_id=tenant_id,
            repo_id=repo_id,
        )
    except RuntimeError as exc:
        return {"ok": False, "error": str(exc)}


def _dispatch_firebase(
    *,
    emails: list[str],
    project_dir: Path,
    tenant_id: str,
    repo_id: str,
) -> dict[str, Any]:
    release = str(os.environ.get("AKC_DELIVERY_FIREBASE_RELEASE_NAME", "") or "").strip()
    if not release:
        return {
            "ok": False,
            "error": (
                "AKC_DELIVERY_FIREBASE_RELEASE_NAME not set "
                "(full resource, e.g. projects/PROJECT_NUMBER/apps/APP_ID/releases/RELEASE_ID)"
            ),
        }
    groups = _firebase_group_aliases()
    if not groups:
        op = load_operator_prereqs_manifest(project_dir)
        andr = op.get("android")
        if isinstance(andr, dict) and andr.get("firebase_app_distribution_groups"):
            raw = andr.get("firebase_app_distribution_groups")
            if isinstance(raw, str):
                groups = [x.strip() for x in raw.split(",") if x.strip()]
            elif isinstance(raw, list):
                groups = [str(x).strip() for x in raw if str(x).strip()]
    if not groups:
        return {"ok": False, "error": "Firebase group aliases missing (env or operator_prereqs)"}
    try:
        return provider_clients.firebase_distribute_release(
            release_name=release,
            tester_emails=emails,
            group_aliases=groups,
            tenant_id=tenant_id,
            repo_id=repo_id,
        )
    except RuntimeError as exc:
        return {"ok": False, "error": str(exc)}


def _play_package_name(*, project_dir: Path) -> str:
    env = str(os.environ.get("AKC_DELIVERY_PLAY_PACKAGE_NAME", "") or "").strip()
    if env:
        return env
    op = load_operator_prereqs_manifest(project_dir)
    andr = op.get("android")
    if isinstance(andr, dict):
        p = andr.get("play_package")
        if isinstance(p, str) and p.strip():
            return p.strip()
    probed = probe_android_application_id(project_dir.resolve())
    return (probed or "").strip()


def _dispatch_google_play_store(
    *,
    project_dir: Path,
    packaging: dict[str, Any],
    tenant_id: str,
    repo_id: str,
) -> dict[str, Any]:
    pkg = _play_package_name(project_dir=project_dir)
    if not pkg:
        return {"ok": False, "error": "Play package name not resolved (env, Gradle, or operator_prereqs)"}
    raw_aab = packaging.get("aab_path")
    if not raw_aab:
        nested = packaging.get("outputs")
        if isinstance(nested, dict):
            raw_aab = nested.get("aab_path")
    aab = Path(str(raw_aab)).expanduser() if raw_aab else None
    try:
        if aab and aab.is_file():
            out = provider_clients.play_upload_aab_and_commit_production(
                package_name=pkg,
                aab_path=aab,
                tenant_id=tenant_id,
                repo_id=repo_id,
            )
            if bool(out.get("ok")):
                out = dict(out)
                out["submitted"] = True
            return out
        validation = provider_clients.play_validate_edits_session(
            package_name=pkg,
            tenant_id=tenant_id,
            repo_id=repo_id,
        )
        return {
            "ok": False,
            "blocked": True,
            "submitted": False,
            "error": (
                "Google Play store release requires a packaged .aab artifact at "
                "session.pipeline.package.outputs.per_platform.android.outputs.aab_path"
            ),
            "validation": validation,
        }
    except RuntimeError as exc:
        return {"ok": False, "submitted": False, "error": str(exc)}


def _dispatch_app_store_release(
    *,
    project_dir: Path,
    packaging: dict[str, Any],
    tenant_id: str,
    repo_id: str,
) -> dict[str, Any]:
    _ = project_dir
    try:
        verified = provider_clients.asc_verify_api_token(tenant_id=tenant_id, repo_id=repo_id)
        if not bool(verified.get("ok")):
            return {
                "ok": False,
                "submitted": False,
                "error": "App Store Connect API verification failed",
                "verification": verified,
            }
        raw_ipa = packaging.get("ipa_path")
        if not raw_ipa:
            nested = packaging.get("outputs")
            if isinstance(nested, dict):
                raw_ipa = nested.get("ipa_path")
        ipa = Path(str(raw_ipa)).expanduser() if raw_ipa else None
        if not ipa or not ipa.is_file():
            return {
                "ok": False,
                "blocked": True,
                "submitted": False,
                "error": (
                    "iOS App Store release requires a packaged .ipa at "
                    "session.pipeline.package.outputs.per_platform.ios.outputs.ipa_path"
                ),
                "verification": verified,
            }
        upload = provider_clients.asc_upload_ipa_to_app_store_connect(
            ipa_path=ipa,
            tenant_id=tenant_id,
            repo_id=repo_id,
        )
        if not bool(upload.get("ok")):
            return {
                **upload,
                "submitted": False,
                "verification": verified,
            }
        result = dict(upload)
        result["ok"] = True
        result["submitted"] = True
        result["verification"] = verified
        return result
    except RuntimeError as exc:
        return {"ok": False, "submitted": False, "error": str(exc)}


def _stub_result(*, adapter_kind: str, reason: str) -> dict[str, Any]:
    return {"ok": True, "stub": True, "adapter_kind": adapter_kind, "note": reason}


def _store_submission_confirmed(job: dict[str, Any]) -> bool:
    """True only for confirmed mobile store submissions (not stubs or dry-runs)."""

    if not bool(job.get("ok")):
        return False
    if bool(job.get("stub")) or bool(job.get("dry_run")):
        return False
    if str(job.get("adapter_kind") or "") not in {"app_store_release", "google_play_release"}:
        return False
    return bool(job.get("submitted"))


def run_delivery_distribution(
    *,
    project_dir: Path,
    delivery_id: str,
    tenant_id: str,
    repo_id: str,
    platforms: Sequence[str],
    release_mode: ReleaseMode,
    delivery_version: str,
    compile_run_id: str | None,
    lanes: Sequence[ReleaseLane] | None = None,
) -> dict[str, Any]:
    """Execute provider distribution for ``lanes`` (defaults to post-package wave)."""

    delivery_store.assert_safe_delivery_id(delivery_id)

    request = delivery_store.load_request(project_dir, delivery_id)
    session = delivery_store.load_session(project_dir, delivery_id)

    lane_tuple: tuple[ReleaseLane, ...]
    if lanes is not None:
        lane_tuple = tuple(dict.fromkeys(lanes))
    else:
        rm = cast(ReleaseMode, str(request.get("release_mode") or release_mode))
        lane_tuple = lanes_for_post_package_wave(rm)

    recipients_raw = request.get("recipients")
    recipients = [str(x).strip() for x in cast(list[Any], recipients_raw or []) if str(x).strip()]
    per_recipient = session.get("per_recipient")
    invite_by_email: dict[str, str] = {}
    if isinstance(per_recipient, dict):
        for email, row in per_recipient.items():
            if not isinstance(row, dict):
                continue
            tid = row.get("invite_token_id")
            if isinstance(tid, str) and tid.strip():
                invite_by_email[str(email).strip()] = tid.strip()

    secrets = session.get("secrets")
    hmac_key = ""
    if isinstance(secrets, dict):
        k = secrets.get("invite_hmac_key")
        if isinstance(k, str):
            hmac_key = k

    per_platform_pkg = _packaging_outputs_per_platform(session)
    platform_meta: dict[str, Any] = {}
    ch = request.get("compile_outputs_ref")
    if isinstance(ch, dict):
        platform_meta["compile_handoff"] = dict(ch)

    t_start_ms = int(time.time() * 1000)
    jobs_out: dict[str, Any] = {}
    ok_all = True

    use_explicit_stub = not provider_clients.execute_providers_requested()

    if set(lane_tuple) == {"beta"}:
        iter_mode: ReleaseMode = "beta"
    elif set(lane_tuple) == {"store"}:
        iter_mode = "store"
    else:
        iter_mode = "both"

    for platform, lane, adapter in distribution_adapters.iter_distribution_jobs(
        platforms=platforms,
        release_mode=iter_mode,
    ):
        if lane not in lane_tuple:
            continue

        spec = PlatformBuildSpec(
            tenant_id=tenant_id,
            repo_id=repo_id,
            delivery_id=delivery_id,
            platform=platform,
            delivery_version=delivery_version,
            release_lanes=(lane,),
            compile_run_id=compile_run_id,
            metadata=dict(platform_meta),
        )

        pre = adapter.preflight(
            project_dir=project_dir.resolve(),
            tenant_id=tenant_id,
            repo_id=repo_id,
            spec=spec,
        )
        if pre:
            jobs_out[f"{platform}:{lane}"] = {"ok": False, "preflight": pre}
            ok_all = False
            delivery_store.update_session_channel_lane(
                project_dir=project_dir,
                delivery_id=delivery_id,
                platform=platform,
                lane=lane,
                status="blocked",
                details_patch={"dispatch_preflight": pre},
            )
            continue

        dispatch: dict[str, Any]
        if use_explicit_stub or provider_clients.provider_dry_run():
            dispatch = _stub_result(
                adapter_kind=adapter.kind,
                reason="AKC_DELIVERY_EXECUTE_PROVIDERS off or AKC_DELIVERY_PROVIDER_DRY_RUN on",
            )
        elif adapter.kind == "web_invite":
            dispatch = _dispatch_web_invite(
                project_dir=project_dir,
                delivery_id=delivery_id,
                recipients=recipients,
                tenant_id=tenant_id,
                repo_id=repo_id,
                spec=spec,
                invite_by_email=invite_by_email,
                invite_hmac_key=hmac_key,
                lane=lane,
            )
        elif adapter.kind == "testflight":
            dispatch = _dispatch_testflight(emails=recipients, tenant_id=tenant_id, repo_id=repo_id)
        elif adapter.kind == "firebase_app_distribution":
            dispatch = _dispatch_firebase(
                emails=recipients,
                project_dir=project_dir.resolve(),
                tenant_id=tenant_id,
                repo_id=repo_id,
            )
        elif adapter.kind == "google_play_release":
            dispatch = _dispatch_google_play_store(
                project_dir=project_dir.resolve(),
                packaging=dict(cast(dict[str, Any], per_platform_pkg.get(platform) or {})),
                tenant_id=tenant_id,
                repo_id=repo_id,
            )
        elif adapter.kind == "app_store_release":
            dispatch = _dispatch_app_store_release(
                project_dir=project_dir.resolve(),
                packaging=dict(cast(dict[str, Any], per_platform_pkg.get(platform) or {})),
                tenant_id=tenant_id,
                repo_id=repo_id,
            )
        else:
            dispatch = {"ok": False, "error": f"unknown adapter {adapter.kind}"}

        if lane == "store" and "submitted" in dispatch and not bool(dispatch.get("submitted")):
            dispatch = dict(dispatch)
            dispatch["ok"] = False
            dispatch.setdefault("blocked", True)
            dispatch.setdefault(
                "error",
                "store lane completed without a confirmed store submission",
            )

        ok = bool(dispatch.get("ok"))
        if not ok:
            ok_all = False
        jobs_out[f"{platform}:{lane}"] = {"adapter_kind": adapter.kind, **dispatch}
        delivery_store.update_session_channel_lane(
            project_dir=project_dir,
            delivery_id=delivery_id,
            platform=platform,
            lane=lane,
            status="completed" if ok else "failed",
            details_patch={"last_dispatch": dispatch},
        )
        delivery_store.touch_provider_platform_row(
            project_dir=project_dir,
            delivery_id=delivery_id,
            platform=platform,
            adapter_kind=adapter.kind,
            status="completed" if ok else "failed",
            last_error=None if ok else str(dispatch.get("error")),
            external_refs={
                "lane": lane,
                "dry_run": bool(dispatch.get("dry_run")),
                "stub": bool(dispatch.get("stub")),
            },
        )

    t_done = int(time.time() * 1000)
    ms = max(0, t_done - t_start_ms)

    pipe_status = "completed" if ok_all else "failed"
    new_phase: str | None
    rm_actual = cast(ReleaseMode, str(request.get("release_mode") or release_mode))
    if set(lane_tuple) == {"beta"} and rm_actual == "both":
        new_phase = "distributing"
        delivery_store.update_distribution_plan_phase(
            project_dir=project_dir,
            delivery_id=delivery_id,
            current_phase="human_readiness_gate",
            beta_completed_at_unix_ms=t_done,
        )
        delivery_store.ensure_human_readiness_gate_pending(
            project_dir=project_dir,
            delivery_id=delivery_id,
        )
    elif ok_all and "store" in lane_tuple and rm_actual in ("store", "both"):
        new_phase = "releasing"
    elif ok_all:
        new_phase = "distributing"
    else:
        new_phase = "failed"

    delivery_store.update_session_pipeline_stage(
        project_dir=project_dir,
        delivery_id=delivery_id,
        stage_name="distribution",
        status=pipe_status,
        started_at_unix_ms=t_done - ms,
        completed_at_unix_ms=t_done,
        error=None if ok_all else "one or more distribution lanes failed",
        outputs={"jobs": jobs_out, "lanes": list(lane_tuple), "duration_ms": ms},
        new_session_phase=new_phase,
    )

    if ok_all:
        delivery_store.append_event(
            project_dir=project_dir,
            delivery_id=delivery_id,
            event_type=DELIVERY_INVITE_SENT,
            payload={
                "lanes": list(lane_tuple),
                "jobs": {k: {"ok": v.get("ok"), "adapter_kind": v.get("adapter_kind")} for k, v in jobs_out.items()},
            },
        )
        if "store" in lane_tuple:
            submitted_jobs = {k: v for k, v in jobs_out.items() if _store_submission_confirmed(cast(dict[str, Any], v))}
            if submitted_jobs:
                delivery_store.append_event(
                    project_dir=project_dir,
                    delivery_id=delivery_id,
                    event_type=DELIVERY_STORE_SUBMITTED,
                    payload={"lanes": ["store"], "jobs": submitted_jobs},
                )
    else:
        delivery_store.append_event(
            project_dir=project_dir,
            delivery_id=delivery_id,
            event_type=DELIVERY_FAILED,
            payload={"phase": "distribution", "jobs": jobs_out},
        )

    append_delivery_control_audit_event(
        project_dir,
        action="delivery.distribution.completed" if ok_all else "delivery.distribution.failed",
        details={"delivery_id": delivery_id, "lanes": list(lane_tuple), "ok": ok_all},
    )

    return {"ok": ok_all, "jobs": jobs_out, "lanes": list(lane_tuple)}
