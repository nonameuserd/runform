"""Run provider distribution after packaging (TestFlight, Firebase App Distribution, Play, web invites).

Respects ``release_mode`` sequencing: for ``both``, callers should pass ``lanes=("beta",)``
for the post-package wave and ``lanes=("store",)`` only after human gate + ``promote``.

Adapter preflight already ran at session creation; execution failures are recorded on
``session.per_platform.*.channels`` and ``provider_state.json``.
"""

from __future__ import annotations

import json
import os
import subprocess
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
from akc.delivery.ingest import (
    load_operator_prereqs_manifest,
    probe_android_application_id,
    probe_firebase_android_app_id,
)
from akc.delivery.invites import build_signed_web_invite_url
from akc.delivery.types import DeliveryPlatform, PlatformBuildSpec, ReleaseLane, ReleaseMode


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


def _string_or_none(raw: Any) -> str | None:
    if raw is None:
        return None
    s = str(raw).strip()
    return s or None


def _packaging_distribution_inputs(packaging: dict[str, Any]) -> dict[str, Any]:
    raw = packaging.get("distribution_inputs")
    return dict(cast(dict[str, Any], raw)) if isinstance(raw, dict) else {}


def _packaging_distribution_results(packaging: dict[str, Any]) -> dict[str, Any]:
    raw = packaging.get("distribution_results")
    if isinstance(raw, dict):
        return dict(cast(dict[str, Any], raw))
    submit = packaging.get("submit_result")
    if isinstance(submit, dict) and bool(submit.get("submitted")):
        return {
            "store": {
                "status": "completed",
                "provider_kind": None,
                "artifact_provenance": None,
                "submitted": True,
                "submission_id": submit.get("submission_id") or submit.get("id") or submit.get("submissionId"),
                "updated_at_unix_ms": None,
            }
        }
    return {}


def _legacy_distribution_input(*, platform: str, lane: ReleaseLane, packaging: dict[str, Any]) -> dict[str, Any]:
    artifact_paths = dict(cast(dict[str, Any], packaging.get("artifact_paths") or {}))
    artifact_urls = dict(cast(dict[str, Any], packaging.get("artifact_urls") or {}))
    build_result = dict(cast(dict[str, Any], packaging.get("build_result") or {}))
    row: dict[str, Any] = {
        "provider_kind": None,
        "ready": False,
        "execution_workspace_root": _string_or_none(artifact_paths.get("execution_workspace_root")),
        "build_profile": _string_or_none(packaging.get("build_profile")),
        "hosting_url": _string_or_none(packaging.get("hosting_url"))
        or _string_or_none(packaging.get("deployed_url"))
        or _string_or_none(artifact_urls.get("hosting_url")),
        "eas_build_id": _string_or_none(packaging.get("eas_build_id")) or _string_or_none(build_result.get("build_id")),
        "build_url": _string_or_none(artifact_urls.get("build_url"))
        or _string_or_none(build_result.get("artifact_url")),
        "ipa_path": _string_or_none(packaging.get("ipa_path")) or _string_or_none(artifact_paths.get("ipa_path")),
        "aab_path": _string_or_none(packaging.get("aab_path")) or _string_or_none(artifact_paths.get("aab_path")),
        "apk_path": _string_or_none(packaging.get("apk_path")) or _string_or_none(artifact_paths.get("apk_path")),
    }
    if platform == "web":
        row["provider_kind"] = "web_invite"
        row["ready"] = bool(row.get("hosting_url"))
    elif platform == "ios":
        row["provider_kind"] = "testflight" if lane == "beta" else "app_store_release"
        row["ready"] = (
            bool(row.get("eas_build_id") or row.get("build_url"))
            if lane == "beta"
            else bool(row.get("eas_build_id") or row.get("build_url") or row.get("ipa_path"))
        )
    else:
        row["provider_kind"] = "firebase_app_distribution" if lane == "beta" else "google_play_release"
        row["ready"] = (
            bool(row.get("apk_path") or row.get("aab_path"))
            if lane == "beta"
            else bool(row.get("eas_build_id") or row.get("build_url") or row.get("aab_path"))
        )
    return row


def _lane_binding(*, platform: str, lane: ReleaseLane, packaging: dict[str, Any]) -> dict[str, Any]:
    inputs = _packaging_distribution_inputs(packaging)
    raw = inputs.get(lane)
    if isinstance(raw, dict):
        return dict(cast(dict[str, Any], raw))
    return _legacy_distribution_input(platform=platform, lane=lane, packaging=packaging)


def _lane_result(*, packaging: dict[str, Any], lane: ReleaseLane) -> dict[str, Any]:
    results = _packaging_distribution_results(packaging)
    raw = results.get(lane)
    return dict(cast(dict[str, Any], raw)) if isinstance(raw, dict) else {}


def _result_submitted(result: dict[str, Any]) -> bool:
    return bool(result.get("submitted"))


def _web_invite_base(*, project_dir: Path, spec_meta: dict[str, Any]) -> str:
    _ = project_dir
    packaging = spec_meta.get("packaging")
    if isinstance(packaging, dict):
        direct = packaging.get("hosting_url")
        if isinstance(direct, str) and direct.strip():
            return direct.strip()
        artifact_urls = packaging.get("artifact_urls")
        if isinstance(artifact_urls, dict):
            hosted = artifact_urls.get("hosting_url")
            if isinstance(hosted, str) and hosted.strip():
                return hosted.strip()
    return ""


def _packaging_detail_row(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    out = raw.get("outputs")
    return dict(cast(dict[str, Any], out)) if isinstance(out, dict) else dict(cast(dict[str, Any], raw))


def _packaging_is_authoritative(packaging: dict[str, Any]) -> bool:
    authority = str(packaging.get("artifact_authority") or "").strip().lower()
    if authority:
        return authority == "authoritative"
    return bool(packaging)


def _packaging_gate_failure(*, platform: str, lane: ReleaseLane, packaging: dict[str, Any]) -> dict[str, Any] | None:
    if not packaging:
        return {
            "ok": False,
            "blocked": True,
            "error": (
                f"{platform}:{lane} distribution requires packaged outputs in "
                "session.pipeline.package.outputs.per_platform"
            ),
        }
    if not _packaging_is_authoritative(packaging):
        return {
            "ok": False,
            "blocked": True,
            "error": (
                f"{platform}:{lane} distribution requires authoritative packaging outputs "
                "(plan mode is not distributable)"
            ),
        }
    binding = _lane_binding(platform=platform, lane=lane, packaging=packaging)
    if not bool(binding.get("ready")):
        existing_result = _lane_result(packaging=packaging, lane=lane)
        if platform == "android" and lane == "beta":
            fallback_release = _string_or_none(existing_result.get("release_name")) or _string_or_none(
                os.environ.get("AKC_DELIVERY_FIREBASE_RELEASE_NAME")
            )
            if fallback_release:
                return None
        error = f"{platform}:{lane} packaging completed without distribution-ready outputs"
        if platform == "ios" and lane == "beta":
            error = "iOS beta distribution requires a packaged EAS build reference"
        elif platform == "ios" and lane == "store":
            error = "iOS App Store release requires a packaged .ipa or EAS build reference"
        elif platform == "android" and lane == "beta":
            error = "Android beta distribution requires a packaged apk/aab artifact or Firebase release fallback"
        elif platform == "android" and lane == "store":
            error = "Google Play store release requires a packaged .aab or EAS build reference"
        failure = {
            "ok": False,
            "blocked": True,
            "error": error,
            "distribution_input": binding,
        }
        if lane == "store":
            failure["submitted"] = False
        return failure
    if platform == "web" and not _string_or_none(binding.get("hosting_url")):
        return {
            "ok": False,
            "blocked": True,
            "error": "web distribution requires a deployed hosting_url from packaging outputs",
            "distribution_input": binding,
        }
    return None


def _last_json(stdout: str) -> dict[str, Any] | None:
    for line in reversed([ln.strip() for ln in stdout.splitlines() if ln.strip()]):
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return parsed
    try:
        parsed = json.loads(stdout)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _run_local_command(*, argv: list[str], cwd: Path) -> dict[str, Any]:
    proc = subprocess.run(
        argv,
        cwd=str(cwd),
        env=dict(os.environ),
        capture_output=True,
        text=True,
        check=False,
    )
    return {
        "argv": list(argv),
        "cwd": str(cwd),
        "exit_code": int(proc.returncode),
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }


def _store_submit_mode(packaging: dict[str, Any], lane: ReleaseLane) -> str:
    if lane != "store":
        return "manual"
    mode = str(packaging.get("store_submit_mode") or "").strip().lower()
    if mode in {"auto", "manual"}:
        return mode
    return "auto"


def _eas_submit_latest(*, platform: DeliveryPlatform, binding: dict[str, Any]) -> dict[str, Any]:
    workspace_root = binding.get("execution_workspace_root")
    if not isinstance(workspace_root, str) or not workspace_root.strip():
        return {"ok": False, "submitted": False, "error": "execution_workspace_root missing for EAS submit"}
    profile = str(binding.get("build_profile") or "production").strip() or "production"
    app_dir = Path(workspace_root).expanduser().resolve() / "apps" / "universal"
    argv = ["eas", "submit", "--platform", platform, "--profile", profile, "--latest", "--non-interactive", "--json"]
    log = _run_local_command(argv=argv, cwd=app_dir)
    if int(log.get("exit_code", 1)) != 0:
        return {
            "ok": False,
            "submitted": False,
            "error": f"EAS submit failed: {' '.join(argv)}",
            "submit_command": argv,
            "submit_log": log,
        }
    parsed = _last_json(str(log.get("stdout") or "")) or {}
    submission_id = None
    for key in ("id", "submissionId"):
        value = parsed.get(key)
        if isinstance(value, str) and value.strip():
            submission_id = value.strip()
            break
    return {
        "ok": True,
        "submitted": True,
        "submit_command": argv,
        "submit_log": log,
        "submit_result": parsed,
        "submission_id": submission_id,
    }


def _dispatch_web_invite(
    *,
    base_url: str,
    delivery_id: str,
    recipients: list[str],
    invite_by_email: dict[str, str],
    invite_hmac_key: str,
    lane: ReleaseLane,
) -> dict[str, Any]:
    if not base_url:
        return {
            "ok": False,
            "error": "web distribution requires a packaged hosting_url",
        }
    if not invite_hmac_key:
        return {"ok": False, "error": "session invite_hmac_key missing"}

    urls: dict[str, str] = {}
    for email in recipients:
        token_id = invite_by_email.get(email.strip())
        if not token_id:
            continue
        urls[email] = build_signed_web_invite_url(
            invite_base_url=base_url,
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
        "hosting_url": base_url,
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


def _firebase_distribution_groups(*, project_dir: Path) -> list[str]:
    groups = _firebase_group_aliases()
    if groups:
        return groups
    op = load_operator_prereqs_manifest(project_dir)
    andr = op.get("android")
    if isinstance(andr, dict) and andr.get("firebase_app_distribution_groups"):
        raw = andr.get("firebase_app_distribution_groups")
        if isinstance(raw, str):
            return [x.strip() for x in raw.split(",") if x.strip()]
        if isinstance(raw, list):
            return [str(x).strip() for x in raw if str(x).strip()]
    return []


def _firebase_app_id(*, project_dir: Path) -> str:
    env = str(os.environ.get("AKC_DELIVERY_FIREBASE_APP_ID", "") or "").strip()
    if env:
        return env
    op = load_operator_prereqs_manifest(project_dir)
    andr = op.get("android")
    if isinstance(andr, dict):
        raw = andr.get("firebase_app_id")
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
    return (probe_firebase_android_app_id(project_dir) or "").strip()


def _dispatch_firebase_release(
    *,
    emails: list[str],
    project_dir: Path,
    tenant_id: str,
    repo_id: str,
    release_name: str,
) -> dict[str, Any]:
    groups = _firebase_distribution_groups(project_dir=project_dir)
    if not groups:
        return {"ok": False, "error": "Firebase group aliases missing (env or operator_prereqs)"}
    try:
        return provider_clients.firebase_distribute_release(
            release_name=release_name,
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


def _dispatch_ios_beta(
    *,
    binding: dict[str, Any],
    existing_result: dict[str, Any],
    emails: list[str],
    tenant_id: str,
    repo_id: str,
) -> dict[str, Any]:
    if _result_submitted(existing_result):
        invited = _dispatch_testflight(emails=emails, tenant_id=tenant_id, repo_id=repo_id)
        return {
            **invited,
            "submitted": True,
            "submission_id": existing_result.get("submission_id"),
            "artifact_provenance": existing_result.get("artifact_provenance"),
            "reused_submission": True,
        }
    if not (_string_or_none(binding.get("eas_build_id")) or _string_or_none(binding.get("build_url"))):
        return {
            "ok": False,
            "blocked": True,
            "submitted": False,
            "error": "iOS beta distribution requires a packaged EAS build reference before TestFlight invites",
        }
    submitted = _eas_submit_latest(platform="ios", binding=binding)
    if not bool(submitted.get("ok")):
        return submitted
    invited = _dispatch_testflight(emails=emails, tenant_id=tenant_id, repo_id=repo_id)
    return {
        **invited,
        "submitted": True,
        "submission_id": submitted.get("submission_id"),
        "submit_result": submitted.get("submit_result"),
        "submit_command": submitted.get("submit_command"),
        "submit_log": submitted.get("submit_log"),
        "artifact_provenance": "packaged_eas_build",
    }


def _dispatch_android_beta(
    *,
    project_dir: Path,
    binding: dict[str, Any],
    existing_result: dict[str, Any],
    emails: list[str],
    tenant_id: str,
    repo_id: str,
) -> dict[str, Any]:
    existing_release = _string_or_none(existing_result.get("release_name"))
    if existing_release:
        dispatched = _dispatch_firebase_release(
            emails=emails,
            project_dir=project_dir,
            tenant_id=tenant_id,
            repo_id=repo_id,
            release_name=existing_release,
        )
        return {
            **dispatched,
            "release_name": existing_release,
            "artifact_url": existing_result.get("artifact_url"),
            "artifact_provenance": existing_result.get("artifact_provenance"),
            "reused_release": True,
        }

    binary_path = _string_or_none(binding.get("apk_path")) or _string_or_none(binding.get("aab_path"))
    if binary_path:
        app_id = _firebase_app_id(project_dir=project_dir)
        if not app_id:
            return {"ok": False, "blocked": True, "error": "Firebase app id missing for artifact-backed Android beta"}
        groups = _firebase_distribution_groups(project_dir=project_dir)
        if not groups:
            return {"ok": False, "blocked": True, "error": "Firebase group aliases missing (env or operator_prereqs)"}
        uploaded = provider_clients.firebase_upload_and_distribute_artifact(
            app_id=app_id,
            binary_path=Path(binary_path).expanduser(),
            tester_emails=emails,
            group_aliases=groups,
            tenant_id=tenant_id,
            repo_id=repo_id,
        )
        if bool(uploaded.get("ok")):
            uploaded = dict(uploaded)
            uploaded["artifact_provenance"] = (
                "packaged_apk" if _string_or_none(binding.get("apk_path")) else "packaged_aab"
            )
        return uploaded

    release = str(os.environ.get("AKC_DELIVERY_FIREBASE_RELEASE_NAME", "") or "").strip()
    if not release:
        return {
            "ok": False,
            "blocked": True,
            "error": (
                "Android beta distribution requires a packaged apk/aab artifact or "
                "AKC_DELIVERY_FIREBASE_RELEASE_NAME fallback"
            ),
        }
    dispatched = _dispatch_firebase_release(
        emails=emails,
        project_dir=project_dir,
        tenant_id=tenant_id,
        repo_id=repo_id,
        release_name=release,
    )
    return {
        **dispatched,
        "release_name": release,
        "artifact_provenance": "external_release_name_fallback",
    }


def _dispatch_google_play_store(
    *,
    project_dir: Path,
    binding: dict[str, Any],
    existing_result: dict[str, Any],
    packaging: dict[str, Any],
    tenant_id: str,
    repo_id: str,
) -> dict[str, Any]:
    if _result_submitted(existing_result):
        return {
            "ok": True,
            "submitted": True,
            "submission_id": existing_result.get("submission_id"),
            "edit_id": existing_result.get("edit_id"),
            "artifact_provenance": existing_result.get("artifact_provenance"),
            "reused_submission": True,
        }
    if _store_submit_mode(packaging, "store") == "manual":
        return {
            "ok": False,
            "submitted": False,
            "blocked": True,
            "error": "store submit mode is manual; rerun with --store-submit auto or submit externally",
        }
    if _string_or_none(binding.get("eas_build_id")) or _string_or_none(binding.get("build_url")):
        submitted = _eas_submit_latest(platform="android", binding=binding)
        if bool(submitted.get("ok")):
            submitted = dict(submitted)
            submitted["artifact_provenance"] = "packaged_eas_build"
            return submitted
    pkg = _play_package_name(project_dir=project_dir)
    if not pkg:
        return {"ok": False, "error": "Play package name not resolved (env, Gradle, or operator_prereqs)"}
    raw_aab = _string_or_none(binding.get("aab_path"))
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
                out["artifact_provenance"] = "packaged_aab"
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
    binding: dict[str, Any],
    existing_result: dict[str, Any],
    packaging: dict[str, Any],
    tenant_id: str,
    repo_id: str,
) -> dict[str, Any]:
    if _result_submitted(existing_result):
        return {
            "ok": True,
            "submitted": True,
            "submission_id": existing_result.get("submission_id"),
            "artifact_provenance": existing_result.get("artifact_provenance"),
            "reused_submission": True,
        }
    if _store_submit_mode(packaging, "store") == "manual":
        return {
            "ok": False,
            "submitted": False,
            "blocked": True,
            "error": "store submit mode is manual; rerun with --store-submit auto or submit externally",
        }
    if _string_or_none(binding.get("eas_build_id")) or _string_or_none(binding.get("build_url")):
        submitted = _eas_submit_latest(platform="ios", binding=binding)
        if bool(submitted.get("ok")):
            submitted = dict(submitted)
            submitted["artifact_provenance"] = "packaged_eas_build"
            return submitted
    try:
        verified = provider_clients.asc_verify_api_token(tenant_id=tenant_id, repo_id=repo_id)
        if not bool(verified.get("ok")):
            return {
                "ok": False,
                "submitted": False,
                "error": "App Store Connect API verification failed",
                "verification": verified,
            }
        raw_ipa = _string_or_none(binding.get("ipa_path"))
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
        result["artifact_provenance"] = "packaged_ipa"
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


def _distribution_result_patch(
    *,
    adapter_kind: str,
    binding: dict[str, Any],
    existing_result: dict[str, Any],
    dispatch: dict[str, Any],
    status: str,
    updated_at_unix_ms: int,
) -> dict[str, Any]:
    patch: dict[str, Any] = {
        "status": status,
        "provider_kind": adapter_kind,
        "artifact_provenance": dispatch.get("artifact_provenance", existing_result.get("artifact_provenance")),
        "submitted": bool(dispatch.get("submitted", existing_result.get("submitted"))),
        "submission_id": dispatch.get("submission_id", existing_result.get("submission_id")),
        "edit_id": dispatch.get("edit_id", existing_result.get("edit_id")),
        "release_name": dispatch.get("release_name", existing_result.get("release_name")),
        "hosting_url": dispatch.get("hosting_url", existing_result.get("hosting_url", binding.get("hosting_url"))),
        "artifact_url": dispatch.get("artifact_url", existing_result.get("artifact_url", binding.get("build_url"))),
        "updated_at_unix_ms": updated_at_unix_ms,
    }
    console_url = dispatch.get("console_url", existing_result.get("console_url"))
    if console_url is not None:
        patch["console_url"] = console_url
    eas_build_id = _string_or_none(binding.get("eas_build_id"))
    if eas_build_id is not None:
        patch["eas_build_id"] = eas_build_id
    build_url = _string_or_none(binding.get("build_url"))
    if build_url is not None:
        patch["build_url"] = build_url
    return patch


def _provider_external_refs(
    *,
    lane: ReleaseLane,
    binding: dict[str, Any],
    result_patch: dict[str, Any],
    dispatch: dict[str, Any],
) -> dict[str, Any]:
    refs: dict[str, Any] = {
        "lane": lane,
        "dry_run": bool(dispatch.get("dry_run")),
        "stub": bool(dispatch.get("stub")),
        "artifact_provenance": result_patch.get("artifact_provenance"),
        "hosting_url": result_patch.get("hosting_url"),
        "eas_build_id": result_patch.get("eas_build_id", binding.get("eas_build_id")),
        "build_url": result_patch.get("build_url", binding.get("build_url")),
        "submission_id": result_patch.get("submission_id"),
        "edit_id": result_patch.get("edit_id"),
        "release_name": result_patch.get("release_name"),
        "artifact_url": result_patch.get("artifact_url"),
    }
    console_url = result_patch.get("console_url")
    if console_url is not None:
        refs["console_url"] = console_url
    return refs


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
    any_blocked = False
    any_failed = False

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
        if isinstance(per_platform_pkg.get(platform), dict):
            packaging_wrapper = dict(cast(dict[str, Any], per_platform_pkg.get(platform) or {}))
        else:
            packaging_wrapper = {}
        packaging = _packaging_detail_row(packaging_wrapper)
        binding = _lane_binding(platform=platform, lane=lane, packaging=packaging)
        existing_result = _lane_result(packaging=packaging, lane=lane)
        if packaging:
            spec.metadata["packaging"] = dict(packaging)

        packaging_gate = _packaging_gate_failure(platform=platform, lane=lane, packaging=packaging)
        if packaging_gate is not None:
            jobs_out[f"{platform}:{lane}"] = packaging_gate
            ok_all = False
            any_blocked = True
            delivery_store.update_session_channel_lane(
                project_dir=project_dir,
                delivery_id=delivery_id,
                platform=platform,
                lane=lane,
                status="blocked",
                details_patch={"packaging_gate": packaging_gate},
            )
            continue

        pre = adapter.preflight(
            project_dir=project_dir.resolve(),
            tenant_id=tenant_id,
            repo_id=repo_id,
            spec=spec,
        )
        if pre:
            jobs_out[f"{platform}:{lane}"] = {"ok": False, "preflight": pre}
            ok_all = False
            any_blocked = True
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
                base_url=_string_or_none(binding.get("hosting_url")) or "",
                delivery_id=delivery_id,
                recipients=recipients,
                invite_by_email=invite_by_email,
                invite_hmac_key=hmac_key,
                lane=lane,
            )
        elif adapter.kind == "testflight":
            dispatch = _dispatch_ios_beta(
                binding=binding,
                existing_result=existing_result,
                emails=recipients,
                tenant_id=tenant_id,
                repo_id=repo_id,
            )
        elif adapter.kind == "firebase_app_distribution":
            dispatch = _dispatch_android_beta(
                project_dir=project_dir.resolve(),
                binding=binding,
                existing_result=existing_result,
                emails=recipients,
                tenant_id=tenant_id,
                repo_id=repo_id,
            )
        elif adapter.kind == "google_play_release":
            dispatch = _dispatch_google_play_store(
                project_dir=project_dir.resolve(),
                binding=binding,
                existing_result=existing_result,
                packaging=dict(packaging),
                tenant_id=tenant_id,
                repo_id=repo_id,
            )
        elif adapter.kind == "app_store_release":
            dispatch = _dispatch_app_store_release(
                binding=binding,
                existing_result=existing_result,
                packaging=dict(packaging),
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
        blocked = bool(dispatch.get("blocked"))
        if not ok:
            ok_all = False
            if blocked:
                any_blocked = True
            else:
                any_failed = True
        jobs_out[f"{platform}:{lane}"] = {"adapter_kind": adapter.kind, **dispatch}
        t_job_done = int(time.time() * 1000)
        job_status = "completed" if ok else "blocked" if blocked else "failed"
        result_patch = _distribution_result_patch(
            adapter_kind=adapter.kind,
            binding=binding,
            existing_result=existing_result,
            dispatch=dispatch,
            status=job_status,
            updated_at_unix_ms=t_job_done,
        )
        delivery_store.update_session_packaging_distribution_result(
            project_dir=project_dir,
            delivery_id=delivery_id,
            platform=platform,
            lane=lane,
            result_patch=result_patch,
        )
        delivery_store.update_session_channel_lane(
            project_dir=project_dir,
            delivery_id=delivery_id,
            platform=platform,
            lane=lane,
            status=job_status,
            details_patch={"last_dispatch": dispatch},
        )
        if ok and lane == "store":
            external_ref = None
            for key in ("submission_id", "edit_id", "upload_id"):
                value = dispatch.get(key)
                if isinstance(value, str) and value.strip():
                    external_ref = value.strip()
                    break
            delivery_store.update_store_release_platform(
                project_dir=project_dir,
                delivery_id=delivery_id,
                platform=platform,
                status="submitted",
                external_ref=external_ref,
                notes=str(dispatch.get("error") or "") or None,
            )
            submit_payload = dispatch.get("submit_result")
            if isinstance(submit_payload, dict):
                delivery_store.update_session_packaging_platform_outputs(
                    project_dir=project_dir,
                    delivery_id=delivery_id,
                    platform=platform,
                    outputs_patch={"submit_result": {"submitted": True, **submit_payload}},
                )
        elif lane == "store":
            delivery_store.update_store_release_platform(
                project_dir=project_dir,
                delivery_id=delivery_id,
                platform=platform,
                status="blocked" if blocked else "failed",
                notes=str(dispatch.get("error") or "") or None,
            )
        delivery_store.touch_provider_platform_row(
            project_dir=project_dir,
            delivery_id=delivery_id,
            platform=platform,
            adapter_kind=adapter.kind,
            status=job_status,
            last_error=None if ok else str(dispatch.get("error")),
            external_refs=_provider_external_refs(
                lane=lane,
                binding=binding,
                result_patch=result_patch,
                dispatch=dispatch,
            ),
        )

    t_done = int(time.time() * 1000)
    ms = max(0, t_done - t_start_ms)

    pipe_status = "completed" if ok_all else "blocked" if any_blocked and not any_failed else "failed"
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
    elif pipe_status == "blocked":
        new_phase = "blocked"
    else:
        new_phase = "failed"

    delivery_store.update_session_pipeline_stage(
        project_dir=project_dir,
        delivery_id=delivery_id,
        stage_name="distribution",
        status=pipe_status,
        started_at_unix_ms=t_done - ms,
        completed_at_unix_ms=t_done,
        error=(
            None
            if ok_all
            else "one or more distribution lanes blocked"
            if pipe_status == "blocked"
            else "one or more distribution lanes failed"
        ),
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
