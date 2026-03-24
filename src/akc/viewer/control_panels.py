"""Read-only operator control-plane summaries for viewer export / static web (Track 6 path A).

Discovers optional ``FORENSICS.json`` bundles under ``<scope>/.akc/viewer/forensics/*/`` and
playbook reports under ``<outputs_root>/<tenant>/.akc/control/playbooks/*.json``. Only
metadata summaries are embedded in viewer bundles (no execution).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from akc.memory.models import normalize_repo_id, require_non_empty


def _read_json_if_object(p: Path) -> dict[str, Any] | None:
    try:
        raw = p.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        return None
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return obj if isinstance(obj, dict) else None


def _slim_forensics_bundle(doc: dict[str, Any]) -> dict[str, Any]:
    replay_raw = doc.get("replay")
    replay: dict[str, Any] = replay_raw if isinstance(replay_raw, dict) else {}
    coord_raw = doc.get("coordination_audit")
    coord: dict[str, Any] = coord_raw if isinstance(coord_raw, dict) else {}
    otel_raw = doc.get("otel")
    otel: dict[str, Any] = otel_raw if isinstance(otel_raw, dict) else {}
    exports = otel.get("exports")
    n_otel = len(exports) if isinstance(exports, list) else 0
    omitted = doc.get("omitted")
    n_omit = len(omitted) if isinstance(omitted, list) else 0
    ops_raw = doc.get("operations_index")
    ops: dict[str, Any] = ops_raw if isinstance(ops_raw, dict) else {}
    return {
        "schema_kind": doc.get("schema_kind"),
        "version": doc.get("version"),
        "tenant_id": doc.get("tenant_id"),
        "repo_id": doc.get("repo_id"),
        "run_id": doc.get("run_id"),
        "replay_included": replay.get("included"),
        "replay_forensics_summary": replay.get("forensics_summary"),
        "coordination_audit_included": coord.get("included"),
        "coordination_tail_line_count": coord.get("tail_line_count"),
        "otel_export_entries": n_otel,
        "operations_index_row_found": ops.get("row_found"),
        "omitted_entry_count": n_omit,
    }


def _slim_playbook_report(doc: dict[str, Any]) -> dict[str, Any]:
    steps_raw = doc.get("steps")
    steps_out: list[dict[str, Any]] = []
    if isinstance(steps_raw, list):
        for s in steps_raw:
            if not isinstance(s, dict):
                continue
            steps_out.append(
                {
                    "name": s.get("name"),
                    "status": s.get("status"),
                    "duration_ms": s.get("duration_ms"),
                    "error": s.get("error"),
                }
            )
    return {
        "schema_kind": doc.get("schema_kind"),
        "version": doc.get("version"),
        "generated_at_ms": doc.get("generated_at_ms"),
        "inputs": doc.get("inputs"),
        "steps": steps_out,
        "replay_forensics_summary": doc.get("replay_forensics_summary"),
        "incident_bundle": doc.get("incident_bundle"),
        "replay_plan_artifact": doc.get("replay_plan_artifact"),
        "manifest_diff_present": doc.get("manifest_diff") is not None,
        "policy_explain_summary": doc.get("policy_explain_summary"),
    }


def _forensics_matches_scope(
    doc: dict[str, Any],
    *,
    tenant_id: str,
    repo_norm: str,
) -> bool:
    if str(doc.get("schema_kind") or "") != "akc_forensics_bundle":
        return False
    if str(doc.get("tenant_id") or "").strip() != tenant_id.strip():
        return False
    doc_repo = str(doc.get("repo_id") or "").strip()
    return normalize_repo_id(doc_repo) == repo_norm


def _playbook_matches_scope(
    doc: dict[str, Any],
    *,
    tenant_id: str,
    repo_norm: str,
) -> bool:
    if str(doc.get("schema_kind") or "") != "akc_operator_playbook_report":
        return False
    inputs = doc.get("inputs")
    if not isinstance(inputs, dict):
        return False
    if str(inputs.get("tenant_id") or "").strip() != tenant_id.strip():
        return False
    in_repo = str(inputs.get("repo_id") or "").strip()
    return normalize_repo_id(in_repo) == repo_norm


_CONTROL_PLANE_REF_KEYS: tuple[str, ...] = (
    "developer_profile_decisions_ref",
    "compile_scoped_apply_ref",
    "policy_decisions_ref",
    "promotion_packet_ref",
    "replay_decisions_ref",
    "recompile_triggers_ref",
    "runtime_evidence_ref",
    "operational_validity_report_ref",
    "governance_profile_ref",
)


def _slim_ref(ref: Any) -> dict[str, Any] | None:
    if not isinstance(ref, dict):
        return None
    path = ref.get("path")
    sha = ref.get("sha256")
    if isinstance(path, str) and path.strip():
        out: dict[str, Any] = {"path": path.strip()}
        if isinstance(sha, str) and len(sha) == 64:
            out["sha256"] = sha
        return out
    return None


def _manifest_run_id(manifest: dict[str, Any] | None) -> str | None:
    if manifest is None:
        return None
    md = manifest.get("metadata")
    if isinstance(md, dict):
        rid = md.get("run_id")
        if isinstance(rid, str) and rid.strip():
            return rid.strip()
    return None


def _scope_context_for_profile_panel(
    *,
    manifest: dict[str, Any] | None,
    tenant_id: str | None,
    repo_id: str | None,
    outputs_root: Path | None,
    plan_run_id: str | None,
) -> dict[str, Any]:
    """Stable identifiers + copy-paste ``akc control`` follow-up (developer debugging path)."""

    mt = str(manifest.get("tenant_id") or "").strip() if isinstance(manifest, dict) else ""
    mr = str(manifest.get("repo_id") or "").strip() if isinstance(manifest, dict) else ""
    eff_tenant = mt or (str(tenant_id or "").strip() or None)
    eff_repo = mr or (str(repo_id or "").strip() or None)
    rid = _manifest_run_id(manifest) if isinstance(manifest, dict) else None
    if not rid and plan_run_id is not None and str(plan_run_id).strip():
        rid = str(plan_run_id).strip()
    out_root: str | None = None
    if outputs_root is not None:
        try:
            out_root = str(outputs_root.expanduser().resolve())
        except OSError:
            out_root = str(outputs_root)
    control_cli: str | None = None
    if eff_tenant and eff_repo and rid and out_root:
        control_cli = (
            f"akc control runs show --tenant-id {eff_tenant} --repo-id {eff_repo} "
            f"--run-id {rid} --outputs-root {out_root}"
        )
    return {
        "tenant_id": eff_tenant,
        "repo_id": eff_repo,
        "run_id": rid,
        "outputs_root": out_root,
        "control_followup_cli": control_cli,
        "doc_anchors": {
            "emerging_role_golden_path": "docs/getting-started.md#emerging-role-golden-path-opt-in",
            "scoped_realization": "docs/getting-started.md#opt-in-scoped-realization",
        },
    }


def load_profile_decisions_panel(
    *,
    manifest: dict[str, Any] | None,
    scoped_outputs_dir: Path,
    tenant_id: str | None = None,
    repo_id: str | None = None,
    outputs_root: Path | None = None,
    plan_run_id: str | None = None,
) -> dict[str, Any]:
    """Summarize manifest control_plane refs and load ``developer_profile_decisions`` when present."""

    scoped = scoped_outputs_dir.expanduser().resolve()
    scope_ctx = _scope_context_for_profile_panel(
        manifest=manifest,
        tenant_id=tenant_id,
        repo_id=repo_id,
        outputs_root=outputs_root,
        plan_run_id=plan_run_id,
    )
    if manifest is None:
        return {
            "available": False,
            "reason": "no_manifest",
            "control_plane_links": {},
            "developer_profile_decisions": None,
            "scope_context": scope_ctx,
        }
    cp = manifest.get("control_plane")
    cp_d = dict(cp) if isinstance(cp, dict) else {}
    links: dict[str, Any] = {}
    for key in _CONTROL_PLANE_REF_KEYS:
        if key not in cp_d:
            continue
        slim = _slim_ref(cp_d.get(key))
        if slim is not None:
            links[key] = slim
    role_raw = cp_d.get("developer_role_profile")
    developer_role_profile = str(role_raw).strip() if isinstance(role_raw, str) and role_raw.strip() else None

    decisions_obj: dict[str, Any] | None = None
    decisions_src: str | None = None
    ref = cp_d.get("developer_profile_decisions_ref")
    ref_path: str | None = None
    if isinstance(ref, dict) and isinstance(ref.get("path"), str):
        ref_path = str(ref["path"]).strip().replace("\\", "/")
    candidates: list[Path] = []
    if ref_path:
        candidates.append((scoped / ref_path).resolve())
    candidates.append((scoped / "developer_profile_decisions.json").resolve())
    seen: set[str] = set()
    for cand in candidates:
        key = str(cand)
        if key in seen:
            continue
        seen.add(key)
        try:
            cand.relative_to(scoped)
        except ValueError:
            continue
        if not cand.is_file():
            continue
        doc = _read_json_if_object(cand)
        if doc is None:
            continue
        try:
            rel = str(cand.relative_to(scoped)).replace("\\", "/")
        except ValueError:
            rel = str(cand)
        decisions_obj = doc
        decisions_src = rel
        break

    return {
        "available": True,
        "developer_role_profile": developer_role_profile,
        "control_plane_links": links,
        "developer_profile_decisions": decisions_obj,
        "developer_profile_decisions_source_relpath": decisions_src,
        "scope_context": scope_ctx,
        "trust_note": ("Read-only profile/decisions snapshot; paths are relative to the tenant/repo outputs scope."),
    }


def load_autopilot_operator_summary(*, scoped_outputs_dir: Path) -> dict[str, Any]:
    """Summarize autopilot scope state + latest decision/escalation paths for control-plane listing."""

    scoped = scoped_outputs_dir.expanduser().resolve()
    ap_root = scoped / ".akc" / "autopilot"
    out: dict[str, Any] = {
        "available": ap_root.is_dir(),
        "paths": {
            "state": ".akc/autopilot/state.json",
            "decisions_glob": ".akc/autopilot/decisions/*.decision.json",
            "escalations_glob": ".akc/autopilot/escalations/*.json",
        },
        "scope_state": None,
        "schema_ids": {
            "decision": "akc:autopilot_decision:v1",
            "human_escalation": "akc:autopilot_human_escalation:v1",
        },
        "latest_decision_relpath": None,
        "latest_escalation_relpath": None,
    }
    st_path = ap_root / "state.json"
    if st_path.is_file():
        doc = _read_json_if_object(st_path)
        if doc:
            bs = doc.get("budget_state")
            if isinstance(bs, dict):
                out["scope_state"] = {
                    "human_escalation_required": bool(bs.get("human_escalation_required", False)),
                    "consecutive_failures": int(bs.get("consecutive_failures", 0) or 0),
                    "mutations_count": int(bs.get("mutations_count", 0) or 0),
                    "rollbacks_count": int(bs.get("rollbacks_count", 0) or 0),
                    "lease_denied_streak": int(doc.get("lease_denied_streak", 0) or 0),
                }
    decisions_dir = ap_root / "decisions"
    if decisions_dir.is_dir():
        candidates: list[tuple[Path, float]] = []
        try:
            for p in decisions_dir.glob("*.decision.json"):
                if not p.is_file():
                    continue
                try:
                    candidates.append((p, p.stat().st_mtime))
                except OSError:
                    continue
        except OSError:
            candidates = []
        if candidates:
            candidates.sort(key=lambda t: t[1], reverse=True)
            try:
                out["latest_decision_relpath"] = str(candidates[0][0].relative_to(scoped)).replace("\\", "/")
            except ValueError:
                out["latest_decision_relpath"] = str(candidates[0][0])
    esc_dir = ap_root / "escalations"
    if esc_dir.is_dir():
        candidates_e: list[tuple[Path, float]] = []
        try:
            for p in esc_dir.glob("*.json"):
                if not p.is_file():
                    continue
                try:
                    candidates_e.append((p, p.stat().st_mtime))
                except OSError:
                    continue
        except OSError:
            candidates_e = []
        if candidates_e:
            candidates_e.sort(key=lambda t: t[1], reverse=True)
            try:
                out["latest_escalation_relpath"] = str(candidates_e[0][0].relative_to(scoped)).replace("\\", "/")
            except ValueError:
                out["latest_escalation_relpath"] = str(candidates_e[0][0])
    return out


def load_operator_panels_for_scope(
    *,
    outputs_root: Path,
    scoped_outputs_dir: Path,
    tenant_id: str,
    repo_id: str,
    manifest: dict[str, Any] | None = None,
    plan_run_id: str | None = None,
) -> dict[str, Any]:
    """Return serializable forensics / playbook panel payloads for viewer export."""

    require_non_empty(tenant_id, name="tenant_id")
    require_non_empty(repo_id, name="repo_id")
    repo_norm = normalize_repo_id(repo_id)
    scoped = scoped_outputs_dir.expanduser().resolve()
    root_out = outputs_root.expanduser().resolve()

    autopilot_panel = load_autopilot_operator_summary(scoped_outputs_dir=scoped)

    profile_panel = load_profile_decisions_panel(
        manifest=manifest,
        scoped_outputs_dir=scoped,
        tenant_id=tenant_id,
        repo_id=repo_norm,
        outputs_root=root_out,
        plan_run_id=plan_run_id,
    )

    forensics_out: dict[str, Any] | None = None
    f_root = scoped / ".akc" / "viewer" / "forensics"
    if f_root.is_dir():
        candidates: list[tuple[Path, str, float]] = []
        try:
            for child in f_root.iterdir():
                if not child.is_dir():
                    continue
                fj = (child / "FORENSICS.json").resolve()
                try:
                    fj.relative_to(scoped)
                except ValueError:
                    continue
                if not fj.is_file():
                    continue
                try:
                    rel = str(fj.relative_to(scoped)).replace("\\", "/")
                except ValueError:
                    continue
                try:
                    mt = fj.stat().st_mtime
                except OSError:
                    continue
                candidates.append((fj, rel, mt))
        except OSError:
            candidates = []
        candidates.sort(key=lambda t: t[2], reverse=True)
        for fj, rel, _mt in candidates:
            doc = _read_json_if_object(fj)
            if doc is None:
                continue
            if not _forensics_matches_scope(doc, tenant_id=tenant_id, repo_norm=repo_norm):
                continue
            forensics_out = {"source_relpath": rel, "summary": _slim_forensics_bundle(doc)}
            break

    playbook_out: dict[str, Any] | None = None
    pb_dir = root_out / tenant_id.strip() / ".akc" / "control" / "playbooks"
    if pb_dir.is_dir():
        reports: list[tuple[Path, float]] = []
        try:
            tenant_anchor = (root_out / tenant_id.strip()).resolve()
            for p in pb_dir.glob("*.json"):
                if p.name.endswith(".replay_plan.json"):
                    continue
                try:
                    p.resolve().relative_to(tenant_anchor)
                except ValueError:
                    continue
                if not p.is_file():
                    continue
                try:
                    reports.append((p, p.stat().st_mtime))
                except OSError:
                    continue
        except OSError:
            reports = []
        reports.sort(key=lambda t: t[1], reverse=True)
        for path, _mt in reports:
            doc = _read_json_if_object(path)
            if doc is None:
                continue
            if not _playbook_matches_scope(doc, tenant_id=tenant_id, repo_norm=repo_norm):
                continue
            try:
                rel = str(path.relative_to(root_out)).replace("\\", "/")
            except ValueError:
                rel = str(path)
            playbook_out = {"source_relpath": rel, "summary": _slim_playbook_report(doc)}
            break

    return {
        "forensics": forensics_out,
        "playbook": playbook_out,
        "profile_panel": profile_panel,
        "autopilot": autopilot_panel,
        "trust_note": (
            "Read-only summaries of local control-plane artifacts; paths are relative to the "
            "tenant/repo outputs tree (forensics) or outputs_root (playbook). No compile/runtime."
        ),
    }
