"""Bridge ``akc deliver`` to compile outputs: run manifest, ``delivery_plan``, runtime bundle refs.

Packaging and distribution run **after** the compile controller finishes; they consume the same
artifacts the runtime bundle already references (``delivery_plan_ref``, deployment intents),
rather than re-deriving hosting or store targets inside the compile loop.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

from akc.memory.models import JSONValue
from akc.run.loader import load_run_manifest
from akc.run.manifest import RunManifest
from akc.utils.fingerprint import stable_json_fingerprint


def run_manifest_path(*, project_dir: Path, compile_run_id: str) -> Path:
    """Path to :class:`akc.run.manifest.RunManifest` for ``compile_run_id`` under ``project_dir``."""

    rid = str(compile_run_id).strip()
    return project_dir.resolve() / ".akc" / "run" / f"{rid}.manifest.json"


def _pass_metadata(manifest: RunManifest, pass_name: str) -> dict[str, JSONValue]:
    for rec in manifest.passes:
        if rec.name != pass_name:
            continue
        raw = rec.metadata
        return dict(raw) if isinstance(raw, dict) else {}
    return {}


def _read_json_mapping(path: Path) -> dict[str, Any] | None:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return raw if isinstance(raw, dict) else None


def extract_web_distribution_hints(plan: Mapping[str, Any]) -> dict[str, Any]:
    """Pull non-secret web hosting hints from a ``delivery_plan`` v1 ``targets`` list."""

    web_targets: list[dict[str, Any]] = []
    suggested_base_urls: list[str] = []
    for raw in plan.get("targets") or []:
        if not isinstance(raw, dict):
            continue
        if str(raw.get("target_class") or "").strip() != "web_app":
            continue
        row: dict[str, Any] = {
            "target_id": raw.get("target_id"),
            "name": raw.get("name"),
        }
        dom = raw.get("domain")
        if isinstance(dom, str) and dom.strip():
            d = dom.strip().lstrip("/")
            row["domain"] = d
            suggested_base_urls.append(f"https://{d}")
        web_targets.append(row)
    return {"web_targets": web_targets, "suggested_base_urls": suggested_base_urls}


def extract_execution_workspace_hints(manifest: Mapping[str, Any]) -> dict[str, Any]:
    expo = manifest.get("expo")
    build_profiles = manifest.get("build_profiles")
    practical = manifest.get("practical_backend_generation")
    return {
        "workspace_root": manifest.get("workspace_root"),
        "runtime_profile": manifest.get("runtime_profile"),
        "requested_runtime_plugin": manifest.get("requested_runtime_plugin"),
        "requested_runtime_source": manifest.get("requested_runtime_source"),
        "materialization_status": manifest.get("materialization_status"),
        "materializer_kind": manifest.get("materializer_kind"),
        "package_manager": manifest.get("package_manager"),
        "artifact_role": manifest.get("artifact_role"),
        "generation_mode": manifest.get("generation_mode"),
        "practical_generation_proof": bool(manifest.get("practical_generation_proof")),
        "toolchain": (
            dict(cast(dict[str, Any], manifest.get("toolchain")))
            if isinstance(manifest.get("toolchain"), Mapping)
            else {}
        ),
        "expo": dict(expo) if isinstance(expo, Mapping) else {},
        "build_profiles": dict(build_profiles) if isinstance(build_profiles, Mapping) else {},
        "client_codegen_summary": (
            dict(cast(dict[str, Any], manifest.get("client_codegen_summary")))
            if isinstance(manifest.get("client_codegen_summary"), Mapping)
            else {}
        ),
        "frontend_integration_summary": (
            dict(cast(dict[str, Any], manifest.get("frontend_integration_summary")))
            if isinstance(manifest.get("frontend_integration_summary"), Mapping)
            else {}
        ),
        "api_contract_refs": (
            list(cast(list[Any], practical.get("api_contract_refs", [])))
            if isinstance(practical, Mapping)
            else list(cast(list[Any], manifest.get("api_contract_refs", [])))
            if isinstance(manifest.get("api_contract_refs"), list)
            else []
        ),
    }


def _delivery_plan_rel_path(*, manifest: RunManifest, compile_run_id: str) -> str:
    meta = _pass_metadata(manifest, "delivery_plan")
    path_raw = meta.get("delivery_plan_path")
    if isinstance(path_raw, str) and path_raw.strip():
        return path_raw.strip()
    return f".akc/deployment/{compile_run_id.strip()}.delivery_plan.json"


def _runtime_bundle_rel_path(manifest: RunManifest) -> str | None:
    meta = _pass_metadata(manifest, "runtime_bundle")
    rb = meta.get("runtime_bundle_path")
    if isinstance(rb, str) and rb.strip():
        return rb.strip()
    if manifest.runtime_bundle is not None and str(manifest.runtime_bundle.path).strip():
        return str(manifest.runtime_bundle.path).strip()
    return None


def _execution_workspace_rel_path(*, manifest: RunManifest, compile_run_id: str) -> str:
    meta = _pass_metadata(manifest, "execution_workspace")
    path_raw = meta.get("execution_workspace_manifest_path")
    if isinstance(path_raw, str) and path_raw.strip():
        return path_raw.strip()
    return f".akc/execution/{compile_run_id.strip()}.execution_workspace_manifest.json"


def _infra_plan_rel_path(*, manifest: RunManifest, compile_run_id: str) -> str:
    meta = _pass_metadata(manifest, "infrastructure_synthesis")
    path_raw = meta.get("infra_plan_path")
    if isinstance(path_raw, str) and path_raw.strip():
        return path_raw.strip()
    return f".akc/infra/{compile_run_id.strip()}.infra_plan.json"


def _iac_manifest_rel_path(*, manifest: RunManifest, compile_run_id: str) -> str:
    meta = _pass_metadata(manifest, "infrastructure_synthesis")
    path_raw = meta.get("iac_manifest_path")
    if isinstance(path_raw, str) and path_raw.strip():
        return path_raw.strip()
    return f".akc/infra/{compile_run_id.strip()}.iac_manifest.json"


def _latest_provision_summary_ref(*, project_dir: Path, compile_run_id: str) -> dict[str, Any] | None:
    provision_root = project_dir.resolve() / ".akc" / "provision"
    if not provision_root.is_dir():
        return None
    latest: tuple[int, dict[str, Any], Path] | None = None
    for session_path in provision_root.glob("*/session.json"):
        payload = _read_json_mapping(session_path)
        if not isinstance(payload, dict):
            continue
        if str(payload.get("compile_run_id") or "").strip() != compile_run_id.strip():
            continue
        updated = payload.get("updated_at_ms")
        updated_ms = int(updated) if isinstance(updated, int) else 0
        if latest is None or updated_ms >= latest[0]:
            latest = (updated_ms, payload, session_path)
    if latest is None:
        return None
    _, payload, session_path = latest
    rel_path = str(session_path.relative_to(project_dir.resolve()))
    return {
        "path": rel_path,
        "fingerprint": stable_json_fingerprint(payload),
        "status": payload.get("status"),
        "backend": payload.get("backend"),
        "environment": payload.get("environment"),
        "provision_id": payload.get("provision_id"),
    }


def load_compile_handoff(*, project_dir: Path, compile_run_id: str | None) -> dict[str, Any]:
    """Load tenant-safe compile outputs for a finished ``akc compile`` run.

    Returns a dict suitable for ``PlatformBuildSpec.metadata[\"compile_handoff\"]`` and for
    persisting on ``delivery_session.compile_outputs_ref`` / ``delivery_request.compile_outputs_ref``.
    """

    root = project_dir.resolve()
    if compile_run_id is None or not str(compile_run_id).strip():
        return {
            "compile_run_id": None,
            "manifest_present": False,
            "delivery_plan_loaded": False,
            "derived_intent_ref": None,
            "delivery_plan_ref": None,
            "promotion_readiness": None,
            "web_distribution_hints": {"web_targets": [], "suggested_base_urls": []},
            "runtime_bundle_rel_path": None,
            "execution_workspace_loaded": False,
            "execution_workspace_ref": None,
            "execution_workspace_hints": {},
            "infra_plan_loaded": False,
            "infra_plan_ref": None,
            "iac_manifest_loaded": False,
            "iac_manifest_ref": None,
            "latest_provision_summary_ref": None,
        }

    rid = str(compile_run_id).strip()
    mpath = run_manifest_path(project_dir=root, compile_run_id=rid)
    if not mpath.is_file():
        return {
            "compile_run_id": rid,
            "manifest_present": False,
            "manifest_path": str(mpath.relative_to(root)),
            "delivery_plan_loaded": False,
            "derived_intent_ref": None,
            "delivery_plan_ref": None,
            "promotion_readiness": None,
            "web_distribution_hints": {"web_targets": [], "suggested_base_urls": []},
            "runtime_bundle_rel_path": None,
            "execution_workspace_loaded": False,
            "execution_workspace_ref": None,
            "execution_workspace_hints": {},
            "infra_plan_loaded": False,
            "infra_plan_ref": None,
            "iac_manifest_loaded": False,
            "iac_manifest_ref": None,
            "latest_provision_summary_ref": None,
            "error": "run_manifest_missing",
        }

    manifest = load_run_manifest(path=mpath)
    if manifest.run_id.strip() != rid:
        return {
            "compile_run_id": rid,
            "manifest_present": True,
            "manifest_path": str(mpath.relative_to(root)),
            "delivery_plan_loaded": False,
            "derived_intent_ref": None,
            "delivery_plan_ref": None,
            "promotion_readiness": None,
            "web_distribution_hints": {"web_targets": [], "suggested_base_urls": []},
            "runtime_bundle_rel_path": None,
            "execution_workspace_loaded": False,
            "execution_workspace_ref": None,
            "execution_workspace_hints": {},
            "infra_plan_loaded": False,
            "infra_plan_ref": None,
            "iac_manifest_loaded": False,
            "iac_manifest_ref": None,
            "latest_provision_summary_ref": None,
            "error": "run_manifest_run_id_mismatch",
        }

    derived: dict[str, str] | None = None
    if manifest.stable_intent_sha256 and manifest.intent_semantic_fingerprint and manifest.intent_goal_text_fingerprint:
        derived = {
            "intent_id": rid,
            "stable_intent_sha256": manifest.stable_intent_sha256,
            "semantic_fingerprint": manifest.intent_semantic_fingerprint,
            "goal_text_fingerprint": manifest.intent_goal_text_fingerprint,
        }

    dp_rel = _delivery_plan_rel_path(manifest=manifest, compile_run_id=rid)
    dp_abs = root / dp_rel
    plan = _read_json_mapping(dp_abs)
    delivery_plan_ref: dict[str, str] | None = None
    promotion_readiness: dict[str, Any] | None = None
    web_hints: dict[str, Any] = {"web_targets": [], "suggested_base_urls": []}
    if plan is not None:
        delivery_plan_ref = {"path": dp_rel, "fingerprint": stable_json_fingerprint(plan)}
        pr = plan.get("promotion_readiness")
        promotion_readiness = dict(pr) if isinstance(pr, dict) else None
        web_hints = extract_web_distribution_hints(plan)

    ew_rel = _execution_workspace_rel_path(manifest=manifest, compile_run_id=rid)
    ew_abs = root / ew_rel
    execution_workspace_obj = _read_json_mapping(ew_abs)
    execution_workspace_ref: dict[str, str] | None = None
    execution_workspace_hints: dict[str, Any] = {}
    if execution_workspace_obj is not None:
        execution_workspace_ref = {"path": ew_rel, "fingerprint": stable_json_fingerprint(execution_workspace_obj)}
        execution_workspace_hints = extract_execution_workspace_hints(execution_workspace_obj)
    infra_rel = _infra_plan_rel_path(manifest=manifest, compile_run_id=rid)
    infra_obj = _read_json_mapping(root / infra_rel)
    infra_plan_ref: dict[str, str] | None = None
    if infra_obj is not None:
        infra_plan_ref = {"path": infra_rel, "fingerprint": stable_json_fingerprint(infra_obj)}
    iac_rel = _iac_manifest_rel_path(manifest=manifest, compile_run_id=rid)
    iac_obj = _read_json_mapping(root / iac_rel)
    iac_manifest_ref: dict[str, str] | None = None
    if iac_obj is not None:
        iac_manifest_ref = {"path": iac_rel, "fingerprint": stable_json_fingerprint(iac_obj)}

    return {
        "compile_run_id": rid,
        "manifest_present": True,
        "manifest_rel_path": str(mpath.relative_to(root)),
        "delivery_plan_rel_path": dp_rel,
        "delivery_plan_loaded": plan is not None,
        "derived_intent_ref": derived,
        "delivery_plan_ref": delivery_plan_ref,
        "promotion_readiness": promotion_readiness,
        "web_distribution_hints": web_hints,
        "runtime_bundle_rel_path": _runtime_bundle_rel_path(manifest),
        "execution_workspace_rel_path": ew_rel,
        "execution_workspace_loaded": execution_workspace_obj is not None,
        "execution_workspace_ref": execution_workspace_ref,
        "execution_workspace_hints": execution_workspace_hints,
        "infra_plan_rel_path": infra_rel,
        "infra_plan_loaded": infra_obj is not None,
        "infra_plan_ref": infra_plan_ref,
        "iac_manifest_rel_path": iac_rel,
        "iac_manifest_loaded": iac_obj is not None,
        "iac_manifest_ref": iac_manifest_ref,
        "latest_provision_summary_ref": _latest_provision_summary_ref(project_dir=root, compile_run_id=rid),
    }


def platform_spec_metadata_from_handoff(handoff: Mapping[str, Any]) -> dict[str, Any]:
    """Non-secret metadata for :class:`akc.delivery.types.PlatformBuildSpec` (all platforms)."""

    meta: dict[str, Any] = {"compile_handoff": dict(handoff)}
    urls = handoff.get("web_distribution_hints") or {}
    if isinstance(urls, dict):
        sugg = urls.get("suggested_base_urls")
        if isinstance(sugg, list) and sugg:
            first = str(sugg[0]).strip()
            if first:
                meta["web_invite_base_url"] = first
    execution_hints = handoff.get("execution_workspace_hints") or {}
    if isinstance(execution_hints, dict):
        meta["execution_workspace_ref"] = handoff.get("execution_workspace_ref")
        for key in (
            "runtime_profile",
            "package_manager",
            "artifact_role",
            "generation_mode",
            "practical_generation_proof",
            "toolchain",
            "expo",
            "build_profiles",
            "client_codegen_summary",
            "frontend_integration_summary",
            "api_contract_refs",
            "workspace_root",
        ):
            value = execution_hints.get(key)
            if value is not None:
                meta[key if key != "workspace_root" else "execution_workspace_root"] = value
    if handoff.get("infra_plan_ref") is not None:
        meta["infra_plan_ref"] = handoff.get("infra_plan_ref")
    if handoff.get("iac_manifest_ref") is not None:
        meta["iac_manifest_ref"] = handoff.get("iac_manifest_ref")
    if handoff.get("latest_provision_summary_ref") is not None:
        meta["latest_provision_summary_ref"] = handoff.get("latest_provision_summary_ref")
    return meta
