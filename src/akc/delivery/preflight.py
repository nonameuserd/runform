"""Operator-facing delivery preflight report for credentials, packaging, and distribution."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any, Final, Literal, cast

from akc.delivery import adapters as distribution_adapters
from akc.delivery import ingest as delivery_ingest
from akc.delivery.compile_handoff import load_compile_handoff, platform_spec_metadata_from_handoff
from akc.delivery.orchestrate import read_manifest_run_id
from akc.delivery.packaging_adapters import collect_packaging_preflight_issues, enforce_packaging_preflight

SurfaceName = Literal[
    "expo_eas_build_hosting",
    "eas_submit_store_submission",
    "firebase_play_upload",
    "app_store_connect_testflight_invites",
]


def _packaging_platform_metadata(*, compile_handoff: Mapping[str, Any], store_submit_mode: str) -> dict[str, Any]:
    meta = platform_spec_metadata_from_handoff(compile_handoff)
    meta["packaging_mode"] = "execute"
    meta["store_submit_mode"] = store_submit_mode
    return meta


def _preflight_platform_summary(
    *,
    platforms: Sequence[str],
    release_mode: str,
    issues: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    lanes = set(distribution_adapters.release_lanes_for_mode(cast(Literal["beta", "store", "both"], release_mode)))
    summary: dict[str, Any] = {}
    for platform in platforms:
        p_issues = [dict(row) for row in issues if str(row.get("platform") or "") == platform]
        lanes_out: dict[str, Any] = {}
        for lane in ("beta", "store"):
            if lane not in lanes:
                lanes_out[lane] = {"applicable": False, "ok": True, "issue_count": 0, "issues": []}
                continue
            lane_issues = [dict(row) for row in p_issues if str(row.get("lane") or "") == lane]
            lanes_out[lane] = {
                "applicable": True,
                "ok": len(lane_issues) == 0,
                "issue_count": len(lane_issues),
                "issues": lane_issues,
            }
        summary[platform] = {
            "ok": len(p_issues) == 0,
            "issue_count": len(p_issues),
            "lanes": lanes_out,
        }
    return summary


def _packaging_lane_summary(*, issues: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    per_platform: dict[str, list[dict[str, Any]]] = {}
    for row in issues:
        platform = str(row.get("platform") or "").strip()
        if not platform:
            continue
        per_platform.setdefault(platform, []).append(dict(row))
    summary: dict[str, Any] = {}
    for platform, rows in per_platform.items():
        summary[platform] = {
            "ok": len(rows) == 0,
            "issue_count": len(rows),
            "lanes": sorted({str(row.get("lane") or "").strip() for row in rows if str(row.get("lane") or "").strip()}),
            "issues": rows,
        }
    return summary


def _surface_human_inputs(rows: Sequence[Mapping[str, Any]], ids: set[str]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows if str(row.get("id") or "") in ids]


def _surface_issue_rows(
    rows: Sequence[Mapping[str, Any]],
    predicate: Callable[[Mapping[str, Any]], bool],
) -> list[dict[str, Any]]:
    return [dict(row) for row in rows if predicate(row)]


_SURFACE_DEFS: Final[dict[SurfaceName, dict[str, Any]]] = {
    "expo_eas_build_hosting": {
        "title": "Expo/EAS build + hosting",
        "human_input_ids": {
            "expo_project_id",
            "expo_access_token",
            "web_hosting_endpoint",
            "web_invite_email_transport",
        },
        "distribution_filter": lambda row: str(row.get("platform") or "") == "web",
        "packaging_filter": lambda row: str(row.get("platform") or "") in {"web", "ios", "android"},
    },
    "eas_submit_store_submission": {
        "title": "EAS Submit / store submission",
        "human_input_ids": {
            "ios_signing_assets",
            "apple_app_store_registration",
            "app_store_connect_api_credentials",
            "google_play_app_registration",
            "google_play_publisher_credentials",
            "android_signing_assets",
        },
        "distribution_filter": lambda row: (
            str(row.get("lane") or "") == "store" and str(row.get("platform") or "") in {"ios", "android"}
        ),
        "packaging_filter": lambda row: str(row.get("platform") or "") in {"ios", "android"},
    },
    "firebase_play_upload": {
        "title": "Firebase App Distribution / Play upload",
        "human_input_ids": {
            "firebase_android_app_registration",
            "firebase_distribution_credentials",
            "google_play_app_registration",
            "google_play_publisher_credentials",
            "android_signing_assets",
        },
        "distribution_filter": lambda row: str(row.get("platform") or "") == "android",
        "packaging_filter": lambda row: str(row.get("platform") or "") == "android",
    },
    "app_store_connect_testflight_invites": {
        "title": "App Store Connect / TestFlight invites",
        "human_input_ids": {
            "ios_bundle_id",
            "apple_team_registration",
            "ios_signing_assets",
            "apple_app_store_registration",
            "app_store_connect_api_credentials",
            "testflight_beta_group_id",
        },
        "distribution_filter": lambda row: str(row.get("platform") or "") == "ios",
        "packaging_filter": lambda row: str(row.get("platform") or "") == "ios",
    },
}


def _surface_summary(
    *,
    name: SurfaceName,
    required_human_inputs: Sequence[Mapping[str, Any]],
    distribution_issues: Sequence[Mapping[str, Any]],
    packaging_execute_issues: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    cfg = _SURFACE_DEFS[name]
    human_rows = _surface_human_inputs(required_human_inputs, set(cast(set[str], cfg["human_input_ids"])))
    dist_rows = _surface_issue_rows(
        distribution_issues,
        cast(Callable[[Mapping[str, Any]], bool], cfg["distribution_filter"]),
    )
    pkg_rows = _surface_issue_rows(
        packaging_execute_issues,
        cast(Callable[[Mapping[str, Any]], bool], cfg["packaging_filter"]),
    )
    return {
        "title": cfg["title"],
        "ok": not human_rows and not dist_rows and not pkg_rows,
        "required_human_inputs": human_rows,
        "distribution_issues": dist_rows,
        "packaging_execute_issues": pkg_rows,
    }


def collect_delivery_preflight_report(
    *,
    project_dir: Path,
    platforms: Sequence[str],
    release_mode: Literal["beta", "store", "both"],
    delivery_version: str,
    tenant_id: str,
    repo_id: str,
    store_submit_mode: Literal["auto", "manual"] = "auto",
    compile_run_id: str | None = None,
) -> dict[str, Any]:
    """Compute an operator-readable delivery preflight report without creating a session."""

    project_root = project_dir.resolve()
    compile_run_source = "explicit" if compile_run_id and str(compile_run_id).strip() else "none"
    resolved_compile_run_id = str(compile_run_id).strip() if compile_run_id and str(compile_run_id).strip() else None
    if resolved_compile_run_id is None:
        inferred = read_manifest_run_id(project_dir=project_root)
        if inferred:
            resolved_compile_run_id = inferred
            compile_run_source = "latest_manifest"
    compile_handoff = load_compile_handoff(project_dir=project_root, compile_run_id=resolved_compile_run_id)
    packaging_meta = _packaging_platform_metadata(
        compile_handoff=compile_handoff,
        store_submit_mode=store_submit_mode,
    )
    required_human_inputs = delivery_ingest.collect_prerequisite_human_inputs(
        project_dir=project_root,
        platforms=platforms,
        release_mode=release_mode,
    )
    required_accounts = delivery_ingest.infer_required_accounts_from_human_inputs(required_human_inputs)
    distribution_issues = distribution_adapters.collect_distribution_preflight_issues(
        project_dir=project_root,
        tenant_id=tenant_id,
        repo_id=repo_id,
        delivery_id="preflight",
        delivery_version=delivery_version,
        platforms=platforms,
        release_mode=release_mode,
    )
    packaging_execute_issues = collect_packaging_preflight_issues(
        project_dir=project_root,
        tenant_id=tenant_id,
        repo_id=repo_id,
        delivery_id="preflight",
        delivery_version=delivery_version,
        platforms=list(platforms),
        release_mode=release_mode,
        platform_metadata=packaging_meta,
    )
    surfaces: dict[str, Any] = {}
    for name in _SURFACE_DEFS:
        surfaces[name] = _surface_summary(
            name=name,
            required_human_inputs=required_human_inputs,
            distribution_issues=distribution_issues,
            packaging_execute_issues=packaging_execute_issues,
        )

    distribution_blocked = bool(distribution_issues) and distribution_adapters.enforce_adapter_preflight()
    packaging_blocked = bool(packaging_execute_issues) and enforce_packaging_preflight(release_mode=release_mode)
    return {
        "ok": not required_human_inputs and not distribution_blocked and not packaging_blocked,
        "project_dir": str(project_root),
        "tenant_id": tenant_id,
        "repo_id": repo_id,
        "delivery_version": delivery_version,
        "platforms": list(platforms),
        "release_mode": release_mode,
        "store_submit_mode": store_submit_mode,
        "operator_prereqs_path": str(project_root / ".akc" / "delivery" / "operator_prereqs.json"),
        "strict": {
            "distribution_enforced": distribution_adapters.enforce_adapter_preflight(),
            "packaging_execute_enforced": enforce_packaging_preflight(release_mode=release_mode),
        },
        "compile_handoff": {
            "source": compile_run_source,
            "compile_run_id": compile_handoff.get("compile_run_id"),
            "manifest_present": bool(compile_handoff.get("manifest_present")),
            "delivery_plan_loaded": bool(compile_handoff.get("delivery_plan_loaded")),
            "execution_workspace_loaded": bool(compile_handoff.get("execution_workspace_loaded")),
            "manifest_rel_path": compile_handoff.get("manifest_rel_path") or compile_handoff.get("manifest_path"),
            "execution_workspace_rel_path": compile_handoff.get("execution_workspace_rel_path"),
            "error": compile_handoff.get("error"),
        },
        "required_accounts": required_accounts,
        "required_human_inputs": list(required_human_inputs),
        "distribution": {
            "ok": len(distribution_issues) == 0,
            "issue_count": len(distribution_issues),
            "issues": list(distribution_issues),
            "platforms": _preflight_platform_summary(
                platforms=platforms,
                release_mode=release_mode,
                issues=distribution_issues,
            ),
        },
        "packaging": {
            "execute": {
                "ok": len(packaging_execute_issues) == 0,
                "issue_count": len(packaging_execute_issues),
                "issues": list(packaging_execute_issues),
                "platforms": _packaging_lane_summary(issues=packaging_execute_issues),
            }
        },
        "surfaces": surfaces,
    }
