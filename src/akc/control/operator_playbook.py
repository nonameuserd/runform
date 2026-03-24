"""Operator governance playbook: compose manifest diff, replay forensics, incident export."""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any, Literal, cast

from jsonschema import Draft202012Validator

from akc.compile.interfaces import TenantRepoScope
from akc.compile.operational_verify import verify_run_operational_coupling
from akc.control.operator_workflows import (
    build_replay_plan_document,
    compute_manifest_intent_diff,
    export_incident_bundle,
    replay_decisions_payload_to_forensics,
    resolve_outputs_and_scope_root_for_manifest,
    resolve_run_manifest_path,
    try_load_replay_decisions,
    validate_replay_plan_document,
)
from akc.control.policy_explain import build_policy_explain_payload, policy_provenance_from_control_plane
from akc.memory.models import normalize_repo_id, require_non_empty
from akc.run.manifest import RunManifest


def operator_playbook_report_schema() -> dict[str, Any]:
    """Load the frozen JSON Schema for :func:`validate_operator_playbook_report`."""

    path = Path(__file__).resolve().parent / "schemas" / "akc_operator_playbook_report.v1.schema.json"
    loaded = json.loads(path.read_text(encoding="utf-8"))
    return cast(dict[str, Any], loaded)


def validate_operator_playbook_report(obj: dict[str, Any]) -> list[str]:
    """Return human-readable validation messages (empty if valid)."""

    schema = operator_playbook_report_schema()
    v = Draft202012Validator(schema)
    return [f"{list(e.path)}: {e.message}" for e in v.iter_errors(obj)]


def _policy_decision_refs_row(manifest: RunManifest) -> dict[str, Any]:
    cp = manifest.control_plane or {}
    cp_d = cp if isinstance(cp, dict) else {}
    ref = cp_d.get("policy_decisions_ref")
    row: dict[str, Any] = {
        "run_id": manifest.run_id.strip(),
        "policy_provenance": policy_provenance_from_control_plane(cp_d if isinstance(cp, dict) else None),
    }
    if isinstance(ref, dict):
        rel = str(ref.get("path", "")).strip()
        sha = str(ref.get("sha256", "")).strip().lower()
        if rel:
            row["policy_decisions_ref"] = {
                "path": rel,
                "sha256": sha if len(sha) == 64 else None,
            }
    inline = cp_d.get("policy_decisions")
    if isinstance(inline, list):
        row["inline_policy_decision_count"] = len([x for x in inline if isinstance(x, dict)])
    else:
        row["inline_policy_decision_count"] = 0
    return row


def _policy_explain_summary(*, manifest: RunManifest, scope_root: Path) -> dict[str, Any]:
    payload = build_policy_explain_payload(manifest=manifest, scope_root=scope_root)
    decisions = payload.get("decisions")
    n = len(decisions) if isinstance(decisions, list) else 0
    return {
        "run_id": payload.get("run_id"),
        "policy_decisions_source": payload.get("policy_decisions_source"),
        "policy_provenance": payload.get("policy_provenance"),
        "policy_decision_count": n,
    }


def _intent_fingerprint_refs_from_diff(diff: dict[str, Any]) -> dict[str, Any]:
    return {
        "stable_intent_sha256": diff.get("stable_intent_sha256"),
        "fingerprints": diff.get("fingerprints"),
        "ir_sha256": diff.get("ir_sha256"),
    }


def run_operator_playbook(
    *,
    outputs_root: Path,
    tenant_id: str,
    repo_id: str,
    run_id_a: str,
    run_id_b: str,
    focus: Literal["a", "b"] = "b",
    evaluation_modes: tuple[str, ...] | None = None,
    include_policy_explain: bool = False,
    shard_ids: tuple[str, ...] = (),
    max_file_mb: int = 8,
    include_runtime_bundle_pointer: bool = False,
    timestamp_utc: str | None = None,
    operational_coupling: bool = True,
) -> tuple[dict[str, Any], Path]:
    """Execute read-only playbook steps and write ``playbooks/<ts>.json`` under tenant control dir.

    Returns ``(report, absolute_report_path)``. Replay and incident export target the focus run only.
    """

    require_non_empty(tenant_id, name="tenant_id")
    repo_norm = normalize_repo_id(repo_id)
    root = Path(outputs_root).expanduser().resolve()
    ra = str(run_id_a).strip()
    rb = str(run_id_b).strip()
    if not ra or not rb:
        raise ValueError("run_id_a and run_id_b must be non-empty")

    path_a = resolve_run_manifest_path(
        manifest_path=None,
        outputs_root=root,
        tenant_id=tenant_id.strip(),
        repo_id=repo_norm,
        run_id=ra,
    )
    path_b = resolve_run_manifest_path(
        manifest_path=None,
        outputs_root=root,
        tenant_id=tenant_id.strip(),
        repo_id=repo_norm,
        run_id=rb,
    )
    if not path_a.is_file():
        raise FileNotFoundError(f"manifest not found for run a: {path_a}")
    if not path_b.is_file():
        raise FileNotFoundError(f"manifest not found for run b: {path_b}")

    left = RunManifest.from_json_file(path_a)
    right = RunManifest.from_json_file(path_b)
    focus_manifest, focus_path = (left, path_a) if focus == "a" else (right, path_b)

    ts = timestamp_utc or time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    playbooks_dir = root / tenant_id.strip() / ".akc" / "control" / "playbooks"
    playbooks_dir.mkdir(parents=True, exist_ok=True)
    report_path = (playbooks_dir / f"{ts}.json").resolve()

    tenant_dir = root / tenant_id.strip()
    try:
        report_relpath = str(report_path.relative_to(tenant_dir)).replace("\\", "/")
    except ValueError:
        report_relpath = str(report_path)

    generated_ms = int(time.time() * 1000)
    steps_out: list[dict[str, Any]] = []
    manifest_diff: dict[str, Any] | None = None
    replay_plan_artifact: dict[str, Any] | None = None
    replay_summary: dict[str, Any] | None = None
    incident_bundle: dict[str, Any] | None = None
    policy_summary: dict[str, Any] | None = None
    operational_coupling_summary: dict[str, Any] | None = None

    # manifest_diff
    t0 = time.perf_counter()
    try:
        diff = compute_manifest_intent_diff(
            left=left,
            right=right,
            left_label="a",
            right_label="b",
            evaluation_modes=evaluation_modes,
        )
        diff["sources"] = {"manifest_a": str(path_a), "manifest_b": str(path_b)}
        manifest_diff = diff
        steps_out.append(
            {
                "name": "manifest_diff",
                "status": "ok",
                "duration_ms": int((time.perf_counter() - t0) * 1000),
                "details": {"manifest_a": str(path_a), "manifest_b": str(path_b)},
            }
        )
    except (OSError, ValueError, TypeError) as e:
        steps_out.append(
            {
                "name": "manifest_diff",
                "status": "failed",
                "duration_ms": int((time.perf_counter() - t0) * 1000),
                "error": str(e),
            }
        )

    # replay_plan (focus run — suggested compile flags, schema-valid JSON artifact)
    t0 = time.perf_counter()
    try:
        rp_doc = build_replay_plan_document(
            manifest=focus_manifest,
            manifest_source_path=focus_path,
            evaluation_modes=evaluation_modes,
            generated_at_ms=generated_ms,
        )
        rp_issues = validate_replay_plan_document(rp_doc)
        if rp_issues:
            raise ValueError("replay plan failed schema validation: " + "; ".join(rp_issues[:6]))
        rp_path = (playbooks_dir / f"{ts}.replay_plan.json").resolve()
        rp_path.write_text(
            json.dumps(rp_doc, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        rp_sha = hashlib.sha256(rp_path.read_bytes()).hexdigest()
        replay_plan_artifact = {
            "path": str(rp_path),
            "sha256": rp_sha,
            "effective_partial_replay_passes": list(rp_doc["intent_replay_context"]["effective_partial_replay_passes"]),
        }
        steps_out.append(
            {
                "name": "replay_plan",
                "status": "ok",
                "duration_ms": int((time.perf_counter() - t0) * 1000),
                "details": {
                    "focus_run_id": focus_manifest.run_id.strip(),
                    "replay_plan_path": str(rp_path),
                },
            }
        )
    except (OSError, ValueError, TypeError) as e:
        steps_out.append(
            {
                "name": "replay_plan",
                "status": "failed",
                "duration_ms": int((time.perf_counter() - t0) * 1000),
                "error": str(e),
            }
        )

    # replay_forensics (focus run)
    t0 = time.perf_counter()
    try:
        _oroot, scope_root = resolve_outputs_and_scope_root_for_manifest(focus_path, focus_manifest)
        rd_payload = try_load_replay_decisions(scope_root=scope_root, manifest=focus_manifest)
        if rd_payload is None:
            steps_out.append(
                {
                    "name": "replay_forensics",
                    "status": "skipped",
                    "duration_ms": int((time.perf_counter() - t0) * 1000),
                    "details": {"reason": "no_replay_decisions"},
                }
            )
        else:
            replay_summary = replay_decisions_payload_to_forensics(rd_payload)
            steps_out.append(
                {
                    "name": "replay_forensics",
                    "status": "ok",
                    "duration_ms": int((time.perf_counter() - t0) * 1000),
                    "details": {"focus_run_id": focus_manifest.run_id.strip()},
                }
            )
    except (OSError, ValueError, TypeError) as e:
        steps_out.append(
            {
                "name": "replay_forensics",
                "status": "failed",
                "duration_ms": int((time.perf_counter() - t0) * 1000),
                "error": str(e),
            }
        )

    # incident_export (focus run)
    t0 = time.perf_counter()
    try:
        _oroot, scope_root = resolve_outputs_and_scope_root_for_manifest(focus_path, focus_manifest)
        out_dir = (playbooks_dir / f"{ts}.incident").resolve()
        max_bytes = max(1, int(max_file_mb)) * 1024 * 1024
        bundle_result = export_incident_bundle(
            scope_root=scope_root,
            manifest=focus_manifest,
            manifest_source_path=focus_path,
            out_dir=out_dir,
            make_zip=True,
            max_file_bytes=max_bytes,
            include_runtime_bundle_pointer=include_runtime_bundle_pointer,
        )
        summary_path = out_dir / "SUMMARY.json"
        summary_sha256: str | None = None
        if summary_path.is_file():
            summary_sha256 = hashlib.sha256(summary_path.read_bytes()).hexdigest()
        zip_path = bundle_result.get("zip_path")
        incident_bundle = {
            "out_dir": str(out_dir),
            "zip_path": zip_path,
            "summary_relpath": str(summary_path.relative_to(out_dir)).replace("\\", "/")
            if summary_path.is_file()
            else None,
            "summary_sha256": summary_sha256,
        }
        steps_out.append(
            {
                "name": "incident_export",
                "status": "ok",
                "duration_ms": int((time.perf_counter() - t0) * 1000),
                "details": {"focus_run_id": focus_manifest.run_id.strip()},
            }
        )
    except (OSError, ValueError, TypeError) as e:
        steps_out.append(
            {
                "name": "incident_export",
                "status": "failed",
                "duration_ms": int((time.perf_counter() - t0) * 1000),
                "error": str(e),
            }
        )

    # operational_coupling (focus run; default-on)
    if operational_coupling:
        t0 = time.perf_counter()
        try:
            scope = TenantRepoScope(tenant_id=focus_manifest.tenant_id, repo_id=focus_manifest.repo_id)
            op = verify_run_operational_coupling(
                outputs_root=root,
                scope=scope,
                run_id=focus_manifest.run_id.strip(),
                strict_manifest=True,
            )
            operational_coupling_summary = {
                "run_id": op.run_id,
                "authority": op.authority,
                "enforcement_mode": op.enforcement_mode,
                "passed": op.passed,
                "blocking_passed": op.blocking_passed,
                "advisory_only": op.advisory_only,
                "finding_count": len(op.findings),
            }
            steps_out.append(
                {
                    "name": "operational_coupling",
                    "status": "ok" if op.blocking_passed else "failed",
                    "duration_ms": int((time.perf_counter() - t0) * 1000),
                    "details": {"focus_run_id": focus_manifest.run_id.strip(), **operational_coupling_summary},
                }
            )
        except (OSError, ValueError, TypeError) as e:
            steps_out.append(
                {
                    "name": "operational_coupling",
                    "status": "failed",
                    "duration_ms": int((time.perf_counter() - t0) * 1000),
                    "error": str(e),
                }
            )

    # policy_explain (focus run — references only in summary)
    if include_policy_explain:
        t0 = time.perf_counter()
        try:
            _oroot, scope_root = resolve_outputs_and_scope_root_for_manifest(focus_path, focus_manifest)
            policy_summary = _policy_explain_summary(manifest=focus_manifest, scope_root=scope_root)
            steps_out.append(
                {
                    "name": "policy_explain",
                    "status": "ok",
                    "duration_ms": int((time.perf_counter() - t0) * 1000),
                    "details": {"focus_run_id": focus_manifest.run_id.strip()},
                }
            )
        except (OSError, ValueError, TypeError) as e:
            steps_out.append(
                {
                    "name": "policy_explain",
                    "status": "failed",
                    "duration_ms": int((time.perf_counter() - t0) * 1000),
                    "error": str(e),
                }
            )

    modes_list = list(evaluation_modes) if evaluation_modes else []

    report: dict[str, Any] = {
        "schema_kind": "akc_operator_playbook_report",
        "version": 1,
        "generated_at_ms": generated_ms,
        "inputs": {
            "tenant_id": tenant_id.strip(),
            "repo_id": repo_norm,
            "outputs_root": str(root),
            "shard_ids": list(shard_ids),
            "run_ids": {"a": ra, "b": rb},
            "focus_run_label": focus,
            "evaluation_modes": modes_list,
        },
        "intent_fingerprint_refs": _intent_fingerprint_refs_from_diff(manifest_diff or {}),
        "policy_decision_refs": {
            "a": _policy_decision_refs_row(left),
            "b": _policy_decision_refs_row(right),
        },
        "manifest_diff": manifest_diff or {},
        "replay_plan_artifact": replay_plan_artifact,
        "replay_forensics_summary": replay_summary,
        "incident_bundle": incident_bundle,
        "operational_coupling_summary": operational_coupling_summary,
        "policy_explain_summary": policy_summary,
        "steps": steps_out,
        "report_relpath": report_relpath,
        "notes": (
            "Read-only governance workflow: replay_plan writes a suggested partial-replay compile argv "
            "(no execution); replay forensics and incident export reference the focus run. "
            "Policy bodies and Rego are not embedded; use policy_decisions_ref paths under repo scope."
        ),
    }

    issues = validate_operator_playbook_report(report)
    if issues:
        raise ValueError("playbook report failed schema validation: " + "; ".join(issues[:6]))

    report_path.write_text(
        json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return report, report_path
