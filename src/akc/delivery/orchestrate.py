"""Compile orchestration hooks for ``akc deliver`` (runs ``akc compile`` in project context)."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Literal, cast

from akc.delivery import adapters as distribution_adapters
from akc.delivery import store as delivery_store
from akc.delivery.activation_contract import write_activation_client_contract
from akc.delivery.compile_handoff import load_compile_handoff, platform_spec_metadata_from_handoff
from akc.delivery.control_index import append_delivery_control_audit_event
from akc.delivery.distribution_dispatch import lanes_for_post_package_wave, run_delivery_distribution
from akc.delivery.event_types import (
    DELIVERY_BUILD_PACKAGED,
    DELIVERY_FAILED,
)
from akc.delivery.packaging_adapters import (
    collect_packaging_preflight_issues,
    enforce_packaging_preflight,
    packaging_adapter_for,
)
from akc.delivery.types import DeliveryPlatform, PlatformBuildSpec
from akc.delivery.versioning import derive_platform_provider_versions


def read_manifest_run_id(*, project_dir: Path) -> str | None:
    """Best-effort read of the latest compile ``run_id`` from ``manifest.json`` (scoped outputs layout)."""

    from akc.cli.project_config import load_akc_project_config

    cfg = load_akc_project_config(project_dir.resolve())
    if cfg is None:
        return None
    tenant = (cfg.tenant_id or "").strip()
    repo = (cfg.repo_id or "").strip()
    out_root = (cfg.outputs_root or "").strip()
    if not tenant or not repo or not out_root:
        return None
    manifest = Path(out_root).expanduser().resolve() / tenant / repo / "manifest.json"
    if not manifest.is_file():
        return None
    try:
        data = json.loads(manifest.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    rid = data.get("run_id")
    if rid is None:
        return None
    s = str(rid).strip()
    return s or None


def run_delivery_compile(*, project_dir: Path, goal: str) -> tuple[int, str | None]:
    """Chdir to ``project_dir`` and run :func:`akc.cli.compile.cmd_compile` with the default compile CLI flags.

    Returns the process exit code and manifest ``run_id`` when compile succeeds and the manifest exists.
    """

    from akc.cli import _build_parser
    from akc.cli.compile import cmd_compile

    parser = _build_parser()
    compile_args = parser.parse_args(["compile", "--goal", goal])
    old = os.getcwd()
    try:
        os.chdir(project_dir.resolve())
        code = int(cmd_compile(compile_args))
    finally:
        os.chdir(old)
    if code != 0:
        return code, None
    return 0, read_manifest_run_id(project_dir=project_dir)


def _resolve_tenant_repo(project_dir: Path) -> tuple[str, str]:
    from akc.cli.project_config import load_akc_project_config

    cfg = load_akc_project_config(project_dir.resolve())
    if cfg is None:
        return "local", "local"
    t = (cfg.tenant_id or "").strip() or "local"
    r = (cfg.repo_id or "").strip() or "local"
    return t, r


def run_delivery_build_and_package(
    *,
    project_dir: Path,
    delivery_id: str,
    platforms: list[str],
    release_mode: Literal["beta", "store", "both"],
    delivery_version: str,
    compile_run_id: str | None,
    tenant_id: str | None = None,
    repo_id: str | None = None,
) -> dict[str, Any]:
    """Run build (shared React/Expo-style base checkpoint) and per-platform packaging lanes.

    Invoked after ``akc compile`` succeeds. Updates ``session.json`` pipeline stages ``build`` and
    ``package``, derives provider build numbers from ``delivery_version``, and appends events.
    """

    tr, rr = _resolve_tenant_repo(project_dir)
    tid = (tenant_id or tr).strip() or "local"
    rid = (repo_id or rr).strip() or "local"

    compile_handoff = load_compile_handoff(project_dir=project_dir.resolve(), compile_run_id=compile_run_id)
    platform_meta = platform_spec_metadata_from_handoff(compile_handoff)

    lanes = distribution_adapters.release_lanes_for_mode(release_mode)
    provider_versions = derive_platform_provider_versions(delivery_version)
    t_start = int(time.time() * 1000)

    pref_issues = collect_packaging_preflight_issues(
        project_dir=project_dir.resolve(),
        tenant_id=tid,
        repo_id=rid,
        delivery_id=delivery_id,
        delivery_version=delivery_version,
        platforms=platforms,
        release_mode=release_mode,
    )
    sess_check = delivery_store.load_session(project_dir, delivery_id)
    was_blocked = str(sess_check.get("session_phase")) == "blocked"

    if pref_issues and enforce_packaging_preflight(release_mode=release_mode):
        fail_phase = None if was_blocked else "failed"
        delivery_store.update_session_pipeline_stage(
            project_dir=project_dir,
            delivery_id=delivery_id,
            stage_name="build",
            status="blocked",
            completed_at_unix_ms=t_start,
            error="packaging preflight blocked",
            outputs={"preflight_issues": pref_issues},
            new_session_phase=fail_phase,
        )
        delivery_store.update_session_pipeline_stage(
            project_dir=project_dir,
            delivery_id=delivery_id,
            stage_name="package",
            status="blocked",
            completed_at_unix_ms=t_start,
            error="packaging preflight blocked",
            outputs={"preflight_issues": pref_issues},
        )
        delivery_store.append_event(
            project_dir=project_dir,
            delivery_id=delivery_id,
            event_type=DELIVERY_FAILED,
            payload={"phase": "packaging_preflight", "issues": pref_issues},
        )
        append_delivery_control_audit_event(
            project_dir,
            action="delivery.packaging_preflight.blocked",
            details={
                "delivery_id": delivery_id,
                "issue_count": len(pref_issues),
                "issues": list(pref_issues)[:48],
            },
        )
        return {"ok": False, "error": "packaging preflight blocked", "preflight_issues": pref_issues}

    delivery_store.update_session_pipeline_stage(
        project_dir=project_dir,
        delivery_id=delivery_id,
        stage_name="build",
        status="in_progress",
        started_at_unix_ms=t_start,
    )

    t_build_done = int(time.time() * 1000)
    build_outputs: dict[str, Any] = {
        "app_stack": "react_expo_default",
        "note": (
            "Shared app generation uses the compile phase; this stage is a control-plane checkpoint "
            "before platform-specific packaging. Packaging consumes compile-time delivery_plan / "
            "runtime bundle refs — not a parallel product runtime."
        ),
        "compile_outputs": {
            "delivery_plan_ref": compile_handoff.get("delivery_plan_ref"),
            "promotion_readiness": compile_handoff.get("promotion_readiness"),
            "web_distribution_hints": compile_handoff.get("web_distribution_hints"),
            "delivery_plan_loaded": compile_handoff.get("delivery_plan_loaded"),
            "manifest_present": compile_handoff.get("manifest_present"),
        },
    }
    delivery_store.update_session_pipeline_stage(
        project_dir=project_dir,
        delivery_id=delivery_id,
        stage_name="build",
        status="completed",
        completed_at_unix_ms=t_build_done,
        outputs=build_outputs,
        new_session_phase="packaging" if not was_blocked else None,
    )

    t_pkg_start = int(time.time() * 1000)
    delivery_store.update_session_pipeline_stage(
        project_dir=project_dir,
        delivery_id=delivery_id,
        stage_name="package",
        status="in_progress",
        started_at_unix_ms=t_pkg_start,
    )

    per_platform: dict[str, Any] = {}
    ok_all = True
    err: str | None = None
    for p in platforms:
        if p not in ("web", "ios", "android"):
            continue
        plat = cast(DeliveryPlatform, p)
        adapter = packaging_adapter_for(plat)
        spec = PlatformBuildSpec(
            tenant_id=tid,
            repo_id=rid,
            delivery_id=delivery_id,
            platform=plat,
            delivery_version=delivery_version,
            release_lanes=lanes,
            compile_run_id=compile_run_id,
            metadata=dict(platform_meta),
        )
        p_issues = adapter.preflight(
            project_dir=project_dir.resolve(),
            tenant_id=tid,
            repo_id=rid,
            spec=spec,
        )
        if p_issues and enforce_packaging_preflight(release_mode=release_mode):
            per_platform[p] = {"ok": False, "lane": adapter.lane, "preflight": p_issues}
            ok_all = False
            joined = "; ".join(p_issues)
            err = err or joined
            continue
        res = adapter.package(
            project_dir=project_dir.resolve(),
            tenant_id=tid,
            repo_id=rid,
            spec=spec,
            compile_run_id=compile_run_id,
            provider_versions=provider_versions,
        )
        per_platform[p] = {
            "ok": res.ok,
            "lane": res.lane,
            "outputs": res.outputs,
            "error": res.error,
        }
        if not res.ok:
            ok_all = False
            err = res.error or err

    t_pkg_done = int(time.time() * 1000)
    pkg_status = "completed" if ok_all else "failed"
    next_phase = None if was_blocked else ("distributing" if ok_all else "failed")
    dist_summary: dict[str, Any] | None = None

    delivery_store.update_session_pipeline_stage(
        project_dir=project_dir,
        delivery_id=delivery_id,
        stage_name="package",
        status=pkg_status,
        completed_at_unix_ms=t_pkg_done,
        error=None if ok_all else (err or "packaging failed"),
        outputs={
            "delivery_version": delivery_version,
            "provider_versions": {
                "delivery_version": provider_versions.delivery_version,
                "ios_marketing_version": provider_versions.ios_marketing_version,
                "ios_build_number": provider_versions.ios_build_number,
                "android_version_name": provider_versions.android_version_name,
                "android_version_code": provider_versions.android_version_code,
                "web_pwa_version": provider_versions.web_pwa_version,
            },
            "per_platform": per_platform,
        },
        new_session_phase=next_phase,
    )

    payload_base: dict[str, Any] = {
        "delivery_version": delivery_version,
        "per_platform": per_platform,
    }
    if ok_all:
        write_activation_client_contract(
            project_dir=project_dir.resolve(),
            delivery_id=delivery_id,
            tenant_id=tid,
            repo_id=rid,
        )
        delivery_store.append_event(
            project_dir=project_dir,
            delivery_id=delivery_id,
            event_type=DELIVERY_BUILD_PACKAGED,
            payload=payload_base,
        )
        dist_summary = run_delivery_distribution(
            project_dir=project_dir.resolve(),
            delivery_id=delivery_id,
            tenant_id=tid,
            repo_id=rid,
            platforms=platforms,
            release_mode=release_mode,
            delivery_version=delivery_version,
            compile_run_id=compile_run_id,
            lanes=lanes_for_post_package_wave(release_mode),
        )
        if not bool(dist_summary.get("ok")):
            ok_all = False
            err = err or "distribution failed"
    else:
        delivery_store.append_event(
            project_dir=project_dir,
            delivery_id=delivery_id,
            event_type=DELIVERY_FAILED,
            payload={"phase": "packaging", "error": err, **payload_base},
        )

    return {
        "ok": ok_all,
        "error": err,
        "preflight_issues": pref_issues,
        "per_platform": per_platform,
        "distribution": dist_summary,
        "provider_versions": {
            "delivery_version": provider_versions.delivery_version,
            "ios_marketing_version": provider_versions.ios_marketing_version,
            "ios_build_number": provider_versions.ios_build_number,
            "android_version_name": provider_versions.android_version_name,
            "android_version_code": provider_versions.android_version_code,
            "web_pwa_version": provider_versions.web_pwa_version,
        },
    }
