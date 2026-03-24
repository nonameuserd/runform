from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from akc.control.control_audit import append_control_audit_event
from akc.control.operations_index import (
    OperationsIndex,
    operations_sqlite_path,
    validate_run_label_key_value,
)
from akc.control.operator_playbook import run_operator_playbook
from akc.control.operator_workflows import (
    build_forensics_bundle,
    build_replay_plan_document,
    compute_manifest_intent_diff,
    export_incident_bundle,
    format_manifest_diff_text,
    format_replay_forensics_markdown,
    replay_decisions_payload_to_forensics,
    repo_scope_root,
    resolve_outputs_and_scope_root_for_manifest,
    resolve_run_manifest_path,
    try_load_replay_decisions,
    validate_replay_plan_document,
)
from akc.control.policy_bundle import (
    default_policy_bundle_path_for_scope,
    fingerprint_policy_bundle_bytes,
    governance_profile_from_document,
    load_policy_bundle_json_bytes,
    resolve_governance_profile_for_scope,
    validate_policy_bundle_document,
    write_resolved_governance_profile_for_scope,
)
from akc.memory.models import normalize_repo_id
from akc.run.manifest import RunManifest

from .profile_defaults import normalize_developer_role_profile


def _developer_role_profile_from_manifest_file(*, manifest_path: Path) -> str | None:
    if not manifest_path.is_file():
        return None
    try:
        manifest = RunManifest.from_json_file(manifest_path)
    except (OSError, ValueError, TypeError):
        return None
    cp = manifest.control_plane if isinstance(manifest.control_plane, dict) else {}
    raw = cp.get("developer_role_profile")
    if raw is None or str(raw).strip() == "":
        return None
    return normalize_developer_role_profile(raw)


def _developer_role_profile_from_index_row(
    *,
    outputs_root: Path,
    manifest_rel_path: object | None,
) -> str | None:
    rel = manifest_rel_path
    if not isinstance(rel, str) or not rel.strip():
        return None
    return _developer_role_profile_from_manifest_file(manifest_path=outputs_root / rel)


def cmd_control_runs_list(args: argparse.Namespace) -> int:
    """List runs from the tenant operations index."""

    tenant_id = str(args.tenant_id or "").strip()
    repo_id_raw = getattr(args, "repo_id", None)
    repo_id = normalize_repo_id(str(repo_id_raw).strip()) if repo_id_raw else None
    outputs_root = Path(str(args.outputs_root)).expanduser()
    db_path = operations_sqlite_path(outputs_root=outputs_root, tenant_id=tenant_id)
    idx = OperationsIndex(sqlite_path=db_path)

    since_ms = getattr(args, "since_ms", None)
    until_ms = getattr(args, "until_ms", None)
    intent = getattr(args, "intent_sha256", None)
    intent_s = str(intent).strip().lower() if intent else None
    if intent_s is not None and len(intent_s) != 64:
        print("error: --intent-sha256 must be a 64-char hex string", file=sys.stderr)
        return 2

    has_trig = getattr(args, "has_recompile_triggers", None)
    if has_trig == "any":
        trig_filter: bool | None = None
    elif has_trig == "yes":
        trig_filter = True
    elif has_trig == "no":
        trig_filter = False
    else:
        trig_filter = None

    rt_ev = getattr(args, "runtime_evidence", None)
    if rt_ev == "any":
        ev_filter: bool | None = None
    elif rt_ev == "yes":
        ev_filter = True
    elif rt_ev == "no":
        ev_filter = False
    else:
        ev_filter = None

    runs = idx.list_runs(
        tenant_id=tenant_id,
        repo_id=repo_id,
        since_ms=int(since_ms) if since_ms is not None else None,
        until_ms=int(until_ms) if until_ms is not None else None,
        stable_intent_sha256=intent_s,
        has_recompile_triggers=trig_filter,
        runtime_evidence_present=ev_filter,
        limit=int(getattr(args, "limit", 50)),
    )
    payload: dict[str, object] = {
        "tenant_id": tenant_id,
        "repo_id": repo_id,
        "operations_db": str(db_path),
        "runs": runs,
    }

    if str(getattr(args, "format", "text")) == "json":
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    scope = f"{tenant_id}/{repo_id}" if repo_id is not None else tenant_id
    print(f"Operations scope: {scope}")
    print(f"Operations db: {db_path}")
    if runs:
        print("Runs:")
        for r in runs:
            intent_raw = r.get("stable_intent_sha256")
            intent_short = str(intent_raw)[:12] if intent_raw not in (None, "") else ""
            pol = r.get("policy_bundle_id") or r.get("rego_pack_version")
            pol_flag = f"pol={pol or '-'}"
            pba = r.get("policy_bundle_artifact")
            bundle_fp = ""
            if isinstance(pba, dict):
                fp = pba.get("fingerprint_sha256")
                if isinstance(fp, str) and len(fp) >= 12:
                    bundle_fp = f" bundle_artifact={fp[:12]}…"
            print(
                "- "
                f"{r.get('repo_id')}/{r.get('run_id')} "
                f"updated_ms={r.get('updated_at_ms')} "
                f"replay={r.get('replay_mode')} "
                f"passes=+{r.get('pass_succeeded', 0)}/-{r.get('pass_failed', 0)}/~{r.get('pass_skipped', 0)} "
                f"triggers={r.get('recompile_trigger_count', 0)} "
                f"rt_evidence={r.get('runtime_evidence_present')} "
                f"health={r.get('aggregate_health') or '-'} "
                f"intent_sha={intent_short or '-'}… "
                f"{pol_flag}{bundle_fp}"
            )
    else:
        print("Runs: (none)")
    return 0


def cmd_control_runs_show(args: argparse.Namespace) -> int:
    """Show one run row plus sidecars from the operations index."""

    tenant_id = str(args.tenant_id or "").strip()
    repo_id = normalize_repo_id(str(args.repo_id or "").strip())
    run_id = str(args.run_id or "").strip()
    outputs_root = Path(str(args.outputs_root)).expanduser()
    db_path = operations_sqlite_path(outputs_root=outputs_root, tenant_id=tenant_id)
    idx = OperationsIndex(sqlite_path=db_path)
    row = idx.get_run(tenant_id=tenant_id, repo_id=repo_id, run_id=run_id)
    drp: str | None = None
    if row is not None:
        drp = _developer_role_profile_from_index_row(
            outputs_root=outputs_root,
            manifest_rel_path=row.get("manifest_rel_path"),
        )
    payload = {
        "tenant_id": tenant_id,
        "repo_id": repo_id,
        "run_id": run_id,
        "operations_db": str(db_path),
        "run": row,
        "developer_role_profile": drp,
    }

    if str(getattr(args, "format", "text")) == "json":
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0 if row is not None else 1

    if row is None:
        print(f"No indexed run {tenant_id}/{repo_id}/{run_id}", file=sys.stderr)
        return 1
    print(f"Operations db: {db_path}")
    print(
        f"run_id={row.get('run_id')} repo={row.get('repo_id')} updated_ms={row.get('updated_at_ms')} "
        f"replay={row.get('replay_mode')} manifest={row.get('manifest_rel_path')}"
    )
    print(
        f"intent_sha256={row.get('stable_intent_sha256') or '-'} "
        f"passes=+{row.get('pass_succeeded', 0)}/-{row.get('pass_failed', 0)}/~{row.get('pass_skipped', 0)} "
        f"triggers={row.get('recompile_trigger_count', 0)} "
        f"runtime_evidence_present={row.get('runtime_evidence_present')} "
        f"health={row.get('aggregate_health') or '-'}"
    )
    if drp is not None:
        print(f"developer_role_profile (from manifest): {drp}")
    ov_pass = row.get("operational_validity_passed")
    ov_summary = row.get("operational_predicate_summary")
    if ov_pass is not None or isinstance(ov_summary, dict):
        failed_count = ov_summary.get("failed_count") if isinstance(ov_summary, dict) else None
        total_count = ov_summary.get("total_count") if isinstance(ov_summary, dict) else None
        print(
            "operational_validity: "
            f"passed={ov_pass if ov_pass is not None else '-'} "
            f"failed_predicates={failed_count if failed_count is not None else '-'} "
            f"total_predicates={total_count if total_count is not None else '-'}"
        )
        failing = ov_summary.get("failing") if isinstance(ov_summary, dict) else None
        if isinstance(failing, list) and failing:
            print("Operational predicate failures:")
            for item in failing[:8]:
                if not isinstance(item, dict):
                    continue
                print(
                    "  - "
                    f"{item.get('success_criterion_id') or '-'} "
                    f"{item.get('signal_key') or item.get('predicate_kind') or 'predicate'} "
                    f"message={item.get('message') or '-'}"
                )
    pb = row.get("policy_bundle_id")
    pg = row.get("policy_git_sha")
    rv = row.get("rego_pack_version")
    if pb or pg or rv:
        print(
            "policy_provenance: "
            f"bundle={pb or '-'} git_sha={(str(pg)[:12] + '…') if isinstance(pg, str) and len(pg) > 12 else pg or '-'} "
            f"rego_pack={rv or '-'}"
        )
    pba = row.get("policy_bundle_artifact")
    if isinstance(pba, dict) and pba.get("fingerprint_sha256"):
        print(
            "policy_bundle_artifact: "
            f"path={pba.get('rel_path')} "
            f"sha256={str(pba.get('fingerprint_sha256'))[:12]}… "
            f"rollout={pba.get('rollout_stage') or '-'} "
            f"revision={pba.get('revision_id') or '-'}"
        )
    kd = row.get("knowledge_decisions")
    if isinstance(kd, dict) and kd.get("fingerprint_sha256"):
        print(
            "knowledge_decisions: "
            f"path={kd.get('decisions_rel_path')} "
            f"sha256={str(kd.get('fingerprint_sha256'))[:12]}… "
            f"last_modified_ms={kd.get('last_modified_ms')}"
        )
    sidecars = row.get("sidecars") or []
    if isinstance(sidecars, list) and sidecars:
        print("Sidecars:")
        for sc in sidecars:
            if isinstance(sc, dict):
                print(f"  - {sc.get('kind')}: {sc.get('rel_path')} sha256={sc.get('sha256') or '-'}")
    else:
        print("Sidecars: (none)")
    labels = row.get("labels") or {}
    if isinstance(labels, dict) and labels:
        print("Labels:")
        for k, v in sorted(labels.items()):
            print(f"  {k}={v}")
    return 0


def cmd_control_manifest_diff(args: argparse.Namespace) -> int:
    """Compare two run manifests (paths or indexed run ids)."""

    eval_modes_raw = getattr(args, "evaluation_modes", None)
    modes: tuple[str, ...] | None = None
    if isinstance(eval_modes_raw, str) and eval_modes_raw.strip():
        modes = tuple(m.strip() for m in eval_modes_raw.split(",") if m.strip())

    ma = getattr(args, "manifest_a", None)
    mb = getattr(args, "manifest_b", None)
    run_a = getattr(args, "run_id_a", None)
    run_b = getattr(args, "run_id_b", None)

    if ma and mb:
        path_a = Path(str(ma)).expanduser().resolve()
        path_b = Path(str(mb)).expanduser().resolve()
    elif run_a and run_b:
        outputs_root = Path(str(args.outputs_root)).expanduser()
        tenant_id = str(args.tenant_id or "").strip()
        repo_id = str(args.repo_id or "").strip()
        path_a = resolve_run_manifest_path(
            manifest_path=None,
            outputs_root=outputs_root,
            tenant_id=tenant_id,
            repo_id=repo_id,
            run_id=str(run_a).strip(),
        )
        path_b = resolve_run_manifest_path(
            manifest_path=None,
            outputs_root=outputs_root,
            tenant_id=tenant_id,
            repo_id=repo_id,
            run_id=str(run_b).strip(),
        )
    else:
        print(
            "error: provide either (--manifest-a and --manifest-b) or "
            "(--run-id-a, --run-id-b, --outputs-root, --tenant-id, --repo-id)",
            file=sys.stderr,
        )
        return 2

    if not path_a.is_file():
        print(f"error: manifest a not found: {path_a}", file=sys.stderr)
        return 2
    if not path_b.is_file():
        print(f"error: manifest b not found: {path_b}", file=sys.stderr)
        return 2

    left = RunManifest.from_json_file(path_a)
    right = RunManifest.from_json_file(path_b)
    diff = compute_manifest_intent_diff(
        left=left,
        right=right,
        left_label="a",
        right_label="b",
        evaluation_modes=modes,
    )
    diff["sources"] = {"manifest_a": str(path_a), "manifest_b": str(path_b)}

    fmt = str(getattr(args, "format", "json"))
    if fmt == "json":
        print(json.dumps(diff, indent=2, sort_keys=True, ensure_ascii=False))
        return 0
    print(format_manifest_diff_text(diff))
    return 0


def cmd_control_replay_plan(args: argparse.Namespace) -> int:
    """Emit schema-valid replay_plan.json: effective partial passes + suggested compile argv (no execution)."""

    eval_modes_raw = getattr(args, "evaluation_modes", None)
    modes: tuple[str, ...] | None = None
    if isinstance(eval_modes_raw, str) and eval_modes_raw.strip():
        modes = tuple(m.strip() for m in eval_modes_raw.split(",") if m.strip())

    manifest_arg = getattr(args, "manifest", None)
    outputs_root = getattr(args, "outputs_root", None)
    tenant_id = str(getattr(args, "tenant_id", "") or "").strip()
    repo_id = str(getattr(args, "repo_id", "") or "").strip()
    run_id = str(getattr(args, "run_id", "") or "").strip()

    if manifest_arg:
        mpath = Path(str(manifest_arg)).expanduser().resolve()
    elif outputs_root and tenant_id and repo_id and run_id:
        mpath = resolve_run_manifest_path(
            manifest_path=None,
            outputs_root=Path(str(outputs_root)),
            tenant_id=tenant_id,
            repo_id=repo_id,
            run_id=run_id,
        )
    else:
        print(
            "error: provide --manifest PATH or (--outputs-root, --tenant-id, --repo-id, --run-id)",
            file=sys.stderr,
        )
        return 2

    if not mpath.is_file():
        print(f"error: manifest not found: {mpath}", file=sys.stderr)
        return 2

    manifest = RunManifest.from_json_file(mpath)
    drp_cli = getattr(args, "developer_role_profile", None)
    cli_profile_arg: str | None = str(drp_cli).strip() if isinstance(drp_cli, str) and str(drp_cli).strip() else None
    doc = build_replay_plan_document(
        manifest=manifest,
        manifest_source_path=mpath,
        evaluation_modes=modes,
        developer_role_profile_cli=cli_profile_arg,
    )
    issues = validate_replay_plan_document(doc)
    if issues:
        print("error: replay plan document failed schema validation: " + "; ".join(issues[:8]), file=sys.stderr)
        return 2

    out_path = getattr(args, "out", None)
    payload: dict[str, object] = dict(doc)
    if out_path:
        op = Path(str(out_path)).expanduser().resolve()
        op.parent.mkdir(parents=True, exist_ok=True)
        text = json.dumps(doc, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
        op.write_text(text, encoding="utf-8")
        payload["written_path"] = str(op)

    fmt = str(getattr(args, "format", "json"))
    if fmt == "json":
        print(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False))
        return 0

    eff = doc["intent_replay_context"]["effective_partial_replay_passes"]
    print("Effective partial replay passes:", ", ".join(eff) if eff else "(none)")
    mdoc = doc.get("manifest") if isinstance(doc.get("manifest"), dict) else {}
    if isinstance(mdoc, dict) and mdoc.get("developer_role_profile_resolution"):
        res = mdoc["developer_role_profile_resolution"]
        if isinstance(res, dict):
            print(
                "Developer role (replay plan): "
                f"effective={res.get('effective')} source={res.get('source')} "
                f"manifest={res.get('manifest')} cli={res.get('cli')}"
            )
    sc = doc["suggested_compile"]
    passes_s = repr(sc.get("partial_replay_passes"))
    argv_t = sc.get("argv_template")
    if isinstance(argv_t, list) and argv_t:
        print("Suggested argv:", " ".join(str(x) for x in argv_t))
    else:
        print("Suggested: akc compile --replay-mode partial_replay --partial-replay-passes", passes_s)
    if payload.get("written_path"):
        print("Wrote:", payload["written_path"])
    return 0


def cmd_control_replay_forensics(args: argparse.Namespace) -> int:
    """Human-readable replay decision forensics (read-only)."""

    rd_path = getattr(args, "replay_decisions", None)
    payload: dict[str, object] | None = None
    if rd_path:
        p = Path(str(rd_path)).expanduser()
        if not p.is_file():
            print(f"error: replay decisions file not found: {p}", file=sys.stderr)
            return 2
        try:
            loaded = json.loads(p.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as e:
            print(f"error: failed to read replay decisions JSON: {e}", file=sys.stderr)
            return 2
        payload = loaded if isinstance(loaded, dict) else None
    else:
        if (
            not getattr(args, "outputs_root", None)
            or not str(getattr(args, "tenant_id", "") or "").strip()
            or not str(getattr(args, "repo_id", "") or "").strip()
            or not str(getattr(args, "run_id", "") or "").strip()
        ):
            print(
                "error: provide --replay-decisions PATH or (--outputs-root, --tenant-id, --repo-id, --run-id)",
                file=sys.stderr,
            )
            return 2
        outputs_root = Path(str(args.outputs_root)).expanduser()
        tenant_id = str(args.tenant_id or "").strip()
        repo_id = str(args.repo_id or "").strip()
        run_id = str(args.run_id or "").strip()
        mpath = resolve_run_manifest_path(
            manifest_path=None,
            outputs_root=outputs_root,
            tenant_id=tenant_id,
            repo_id=repo_id,
            run_id=run_id,
        )
        if not mpath.is_file():
            print(f"error: manifest not found: {mpath}", file=sys.stderr)
            return 2
        manifest = RunManifest.from_json_file(mpath)
        scope_root = repo_scope_root(outputs_root=outputs_root, tenant_id=tenant_id, repo_id=repo_id)
        payload = try_load_replay_decisions(scope_root=scope_root, manifest=manifest)

    if payload is None:
        print("error: could not load replay_decisions payload", file=sys.stderr)
        return 2

    try:
        report = replay_decisions_payload_to_forensics(payload)
    except (ValueError, TypeError) as e:
        print(f"error: invalid replay_decisions payload: {e}", file=sys.stderr)
        return 2

    fmt = str(getattr(args, "format", "markdown"))
    if fmt == "json":
        print(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False))
        return 0
    print(format_replay_forensics_markdown(report))
    return 0


def cmd_control_incident_export(args: argparse.Namespace) -> int:
    """Slim read-only incident bundle (manifest, replay, costs, evidence, knowledge ref)."""

    manifest_arg = getattr(args, "manifest", None)
    outputs_root = getattr(args, "outputs_root", None)
    tenant_id = str(getattr(args, "tenant_id", "") or "").strip()
    repo_id = str(getattr(args, "repo_id", "") or "").strip()
    run_id = str(getattr(args, "run_id", "") or "").strip()

    if manifest_arg:
        mpath = Path(str(manifest_arg)).expanduser().resolve()
    elif outputs_root and tenant_id and repo_id and run_id:
        mpath = resolve_run_manifest_path(
            manifest_path=None,
            outputs_root=Path(str(outputs_root)),
            tenant_id=tenant_id,
            repo_id=repo_id,
            run_id=run_id,
        )
    else:
        print(
            "error: provide --manifest PATH or (--outputs-root, --tenant-id, --repo-id, --run-id)",
            file=sys.stderr,
        )
        return 2

    if not mpath.is_file():
        print(f"error: manifest not found: {mpath}", file=sys.stderr)
        return 2

    manifest = RunManifest.from_json_file(mpath)
    try:
        _outputs_root, scope_root = resolve_outputs_and_scope_root_for_manifest(mpath, manifest)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2

    out_dir_arg = getattr(args, "out_dir", None)
    if out_dir_arg:
        out_dir = Path(str(out_dir_arg)).expanduser()
    else:
        ts = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
        out_dir = scope_root / ".akc" / "viewer" / "incident" / ts

    max_mb = int(getattr(args, "max_file_mb", 8) or 8)
    max_bytes = max(1, max_mb) * 1024 * 1024
    include_rb = bool(getattr(args, "include_runtime_bundle_pointer", False))
    signer_identity_raw = str(getattr(args, "signer_identity", "") or "").strip()
    signer_identity = signer_identity_raw or None
    signature_raw = str(getattr(args, "signature", "") or "").strip()
    signature = signature_raw or None

    try:
        result = export_incident_bundle(
            scope_root=scope_root,
            manifest=manifest,
            manifest_source_path=mpath,
            out_dir=out_dir,
            make_zip=not bool(getattr(args, "no_zip", False)),
            max_file_bytes=max_bytes,
            include_runtime_bundle_pointer=include_rb,
            signer_identity=signer_identity,
            signature=signature,
        )
    except (OSError, ValueError) as e:
        print(f"error: incident export failed: {e}", file=sys.stderr)
        return 2

    if str(getattr(args, "format", "text")) == "json":
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=False))
        return 0

    print(f"Wrote incident bundle: {result.get('out_dir')}")
    if result.get("zip_path"):
        print(f"Wrote zip: {result.get('zip_path')}")
    return 0


def cmd_control_forensics_export(args: argparse.Namespace) -> int:
    """Cross-signal forensics bundle: replay, coordination audit tail, OTel, knowledge, operations index."""

    manifest_arg = getattr(args, "manifest", None)
    outputs_root_arg = getattr(args, "outputs_root", None)
    tenant_id = str(getattr(args, "tenant_id", "") or "").strip()
    repo_id = str(getattr(args, "repo_id", "") or "").strip()
    run_id = str(getattr(args, "run_id", "") or "").strip()

    if manifest_arg:
        mpath = Path(str(manifest_arg)).expanduser().resolve()
    elif outputs_root_arg and tenant_id and repo_id and run_id:
        mpath = resolve_run_manifest_path(
            manifest_path=None,
            outputs_root=Path(str(outputs_root_arg)),
            tenant_id=tenant_id,
            repo_id=repo_id,
            run_id=run_id,
        )
    else:
        print(
            "error: provide --manifest PATH or (--outputs-root, --tenant-id, --repo-id, --run-id)",
            file=sys.stderr,
        )
        return 2

    if not mpath.is_file():
        print(f"error: manifest not found: {mpath}", file=sys.stderr)
        return 2

    manifest = RunManifest.from_json_file(mpath)
    try:
        outputs_root, scope_root = resolve_outputs_and_scope_root_for_manifest(mpath, manifest)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2

    out_dir_arg = getattr(args, "out_dir", None)
    if out_dir_arg:
        out_dir = Path(str(out_dir_arg)).expanduser()
    else:
        ts = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
        out_dir = scope_root / ".akc" / "viewer" / "forensics" / ts

    max_mb = int(getattr(args, "max_file_mb", 8) or 8)
    max_bytes = max(1, max_mb) * 1024 * 1024
    tail_lines = int(getattr(args, "coordination_audit_tail_lines", 500) or 500)
    tail_lines = max(0, tail_lines)
    scan_mb = int(getattr(args, "coordination_audit_max_scan_mb", 4) or 4)
    scan_bytes = max(1, scan_mb) * 1024 * 1024
    signer_identity_raw = str(getattr(args, "signer_identity", "") or "").strip()
    signer_identity = signer_identity_raw or None
    signature_raw = str(getattr(args, "signature", "") or "").strip()
    signature = signature_raw or None

    try:
        result = build_forensics_bundle(
            outputs_root=outputs_root,
            scope_root=scope_root,
            manifest=manifest,
            manifest_source_path=mpath,
            out_dir=out_dir,
            make_zip=not bool(getattr(args, "no_zip", False)),
            max_file_bytes=max_bytes,
            coordination_audit_tail_lines=tail_lines,
            coordination_audit_max_scan_bytes=scan_bytes,
            signer_identity=signer_identity,
            signature=signature,
        )
    except (OSError, ValueError) as e:
        print(f"error: forensics export failed: {e}", file=sys.stderr)
        return 2

    if str(getattr(args, "format", "text")) == "json":
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=False))
        return 0

    print(f"Wrote forensics bundle: {result.get('out_dir')}")
    if result.get("zip_path"):
        print(f"Wrote zip: {result.get('zip_path')}")
    return 0


def cmd_control_runs_label_set(args: argparse.Namespace) -> int:
    """Set a single run label in the tenant operations index (does not edit the manifest)."""

    tenant_id = str(args.tenant_id or "").strip()
    repo_id = normalize_repo_id(str(args.repo_id or "").strip())
    run_id = str(args.run_id or "").strip()
    label_key = str(args.label_key or "").strip()
    label_value = str(args.label_value or "").strip()
    outputs_root = Path(str(args.outputs_root)).expanduser()
    try:
        lk, lv = validate_run_label_key_value(label_key=label_key, label_value=label_value)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    db_path = operations_sqlite_path(outputs_root=outputs_root, tenant_id=tenant_id)
    idx = OperationsIndex(sqlite_path=db_path)
    prior = idx.get_label_value(tenant_id=tenant_id, repo_id=repo_id, run_id=run_id, label_key=lk)
    idx.upsert_label(
        tenant_id=tenant_id,
        repo_id=repo_id,
        run_id=run_id,
        label_key=lk,
        label_value=lv,
    )
    actor_env = str(os.environ.get("AKC_AUDIT_ACTOR", "") or "").strip()
    actor = actor_env or str(os.environ.get("USER", "") or "").strip() or "akc-cli"
    append_control_audit_event(
        outputs_root=outputs_root,
        tenant_id=tenant_id,
        action="runs.label.set",
        actor=actor,
        request_id=str(uuid.uuid4()),
        details={
            "repo_id": repo_id,
            "run_id": run_id,
            "label_key": lk,
            "before": {"label_value": prior},
            "after": {"label_value": lv},
        },
    )
    payload = {
        "tenant_id": tenant_id,
        "repo_id": repo_id,
        "run_id": run_id,
        "label_key": label_key,
        "label_value": label_value,
        "operations_db": str(db_path),
    }
    if str(getattr(args, "format", "text")) == "json":
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"Set label {label_key}={label_value!r} on {tenant_id}/{repo_id}/{run_id}")
    return 0


def cmd_control_playbook_run(args: argparse.Namespace) -> int:
    """Compose manifest diff, replay forensics, and incident export into one playbook report."""

    outputs_root = Path(str(args.outputs_root)).expanduser()
    tenant_id = str(args.tenant_id or "").strip()
    repo_id = str(args.repo_id or "").strip()
    run_a = str(args.run_id_a or "").strip()
    run_b = str(args.run_id_b or "").strip()
    focus_raw = str(getattr(args, "focus_run", "b") or "b").strip().lower()
    focus: Literal["a", "b"] = "a" if focus_raw == "a" else "b"

    eval_modes_raw = getattr(args, "evaluation_modes", None)
    modes: tuple[str, ...] | None = None
    if isinstance(eval_modes_raw, str) and eval_modes_raw.strip():
        modes = tuple(m.strip() for m in eval_modes_raw.split(",") if m.strip())

    shard_raw = getattr(args, "shard_id", None) or []
    shard_ids: tuple[str, ...] = tuple(str(s).strip() for s in shard_raw if str(s).strip())

    include_policy = bool(getattr(args, "with_policy_explain", False))
    ts_override = str(getattr(args, "timestamp_utc", "") or "").strip() or None

    try:
        report, report_path = run_operator_playbook(
            outputs_root=outputs_root,
            tenant_id=tenant_id,
            repo_id=repo_id,
            run_id_a=run_a,
            run_id_b=run_b,
            focus=focus,
            evaluation_modes=modes,
            include_policy_explain=include_policy,
            shard_ids=shard_ids,
            max_file_mb=int(getattr(args, "max_file_mb", 8) or 8),
            include_runtime_bundle_pointer=bool(getattr(args, "include_runtime_bundle_pointer", False)),
            timestamp_utc=ts_override,
            operational_coupling=not bool(getattr(args, "no_operational_coupling", False)),
        )
    except (OSError, ValueError, FileNotFoundError) as e:
        print(f"error: playbook run failed: {e}", file=sys.stderr)
        return 2

    report_bytes = report_path.read_bytes()
    report_sha256 = hashlib.sha256(report_bytes).hexdigest()

    append_control_audit_event(
        outputs_root=outputs_root,
        tenant_id=tenant_id,
        action="playbook_run",
        details={
            "report_path": str(report_path),
            "report_relpath": report.get("report_relpath"),
            "report_sha256": report_sha256,
            "run_ids": report.get("inputs", {}).get("run_ids"),
            "focus_run_label": report.get("inputs", {}).get("focus_run_label"),
            "step_statuses": [
                f"{s.get('name')}:{s.get('status')}" for s in (report.get("steps") or []) if isinstance(s, dict)
            ],
        },
    )

    webhook_item = {
        "tenant_id": tenant_id,
        "repo_id": normalize_repo_id(repo_id),
        "run_ids": {"a": run_a, "b": run_b},
        "report_relpath": report.get("report_relpath"),
        "report_sha256": report_sha256,
        "playbook_generated_at_ms": report.get("generated_at_ms"),
    }

    dry_notify = bool(getattr(args, "webhook_dry_run", False))

    wh_url = str(getattr(args, "webhook_url", "") or "").strip()
    wh_secret = str(getattr(args, "webhook_secret", "") or "").strip()
    wh_id = str(getattr(args, "webhook_id", "playbook-cli") or "").strip()
    if wh_url and wh_secret:
        from akc.control.fleet_webhooks import post_signed_fleet_webhook

        res = post_signed_fleet_webhook(
            url=wh_url,
            secret=wh_secret,
            webhook_id=wh_id,
            event="operator_playbook_completed",
            items=[webhook_item],
            dry_run=dry_notify,
        )
        if res.error:
            print(f"warning: playbook webhook failed: {res.error}", file=sys.stderr)

    fleet_cfg_path = getattr(args, "fleet_config", None)
    if fleet_cfg_path:
        from akc.control.fleet_config import load_fleet_config
        from akc.control.fleet_webhooks import deliver_operator_playbook_completed_webhooks

        cfg = load_fleet_config(Path(str(fleet_cfg_path)))
        for res in deliver_operator_playbook_completed_webhooks(
            cfg,
            tenant_id=tenant_id,
            item=webhook_item,
            dry_run=dry_notify,
        ):
            if res.error:
                print(
                    f"warning: fleet playbook webhook {res.webhook_id!r} failed: {res.error}",
                    file=sys.stderr,
                )

    if str(getattr(args, "format", "text")) == "json":
        out = dict(report)
        out["report_path"] = str(report_path)
        out["report_sha256"] = report_sha256
        print(json.dumps(out, indent=2, sort_keys=True, ensure_ascii=False))
        return 0

    print(f"Wrote operator playbook report: {report_path}")
    print(f"Incident bundle (focus {focus}): {(report.get('incident_bundle') or {}).get('out_dir') or '-'}")
    return 0


def _resolve_policy_bundle_path(args: argparse.Namespace) -> Path | None:
    path_arg = getattr(args, "path", None)
    if path_arg:
        return Path(str(path_arg)).expanduser().resolve()
    outputs_root = Path(str(getattr(args, "outputs_root", "") or "")).expanduser()
    tenant_id = str(getattr(args, "tenant_id", "") or "").strip()
    repo_id = str(getattr(args, "repo_id", "") or "").strip()
    if not outputs_root.parts or not tenant_id or not repo_id:
        return None
    return default_policy_bundle_path_for_scope(
        repo_scope_root(outputs_root=outputs_root, tenant_id=tenant_id, repo_id=repo_id)
    )


def cmd_control_policy_bundle_validate(args: argparse.Namespace) -> int:
    """Validate ``policy_bundle.json`` against the frozen schema."""

    bp = _resolve_policy_bundle_path(args)
    if bp is None:
        print(
            "error: provide --path or (--outputs-root, --tenant-id, --repo-id)",
            file=sys.stderr,
        )
        return 2
    if not bp.is_file():
        print(f"error: policy bundle file not found: {bp}", file=sys.stderr)
        return 1
    try:
        data = bp.read_bytes()
        doc = load_policy_bundle_json_bytes(data)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as e:
        print(f"error: failed to read policy bundle: {e}", file=sys.stderr)
        return 2
    errs = validate_policy_bundle_document(doc)
    if errs:
        for line in errs:
            print(line, file=sys.stderr)
        return 1
    if str(getattr(args, "format", "text")) == "json":
        print(
            json.dumps(
                {"ok": True, "path": str(bp), "fingerprint_sha256": fingerprint_policy_bundle_bytes(data)},
                indent=2,
                sort_keys=True,
            )
        )
    else:
        print(f"OK: {bp}")
    return 0


def cmd_control_policy_bundle_show(args: argparse.Namespace) -> int:
    """Print policy bundle document plus content fingerprint."""

    bp = _resolve_policy_bundle_path(args)
    if bp is None:
        print(
            "error: provide --path or (--outputs-root, --tenant-id, --repo-id)",
            file=sys.stderr,
        )
        return 2
    if not bp.is_file():
        print(f"error: policy bundle file not found: {bp}", file=sys.stderr)
        return 1
    try:
        data = bp.read_bytes()
        doc = load_policy_bundle_json_bytes(data)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as e:
        print(f"error: failed to read policy bundle: {e}", file=sys.stderr)
        return 2
    errs = validate_policy_bundle_document(doc)
    fp = fingerprint_policy_bundle_bytes(data)
    if str(getattr(args, "format", "text")) == "json":
        out: dict[str, object] = {
            "path": str(bp),
            "fingerprint_sha256": fp,
            "document": doc,
            "validation_errors": errs,
        }
        print(json.dumps(out, indent=2, sort_keys=True, ensure_ascii=False))
        return 0 if not errs else 1

    print(f"path: {bp}")
    print(f"fingerprint_sha256: {fp}")
    if errs:
        print("validation errors:", file=sys.stderr)
        for line in errs:
            print(f"  {line}", file=sys.stderr)
        return 1
    print("document:")
    print(json.dumps(doc, indent=2, sort_keys=True, ensure_ascii=False))
    return 0


def cmd_control_policy_bundle_effective_profile(args: argparse.Namespace) -> int:
    """Resolve and print effective governance profile for a tenant/repo scope."""

    outputs_root = Path(str(getattr(args, "outputs_root", "") or "")).expanduser()
    tenant_id = str(getattr(args, "tenant_id", "") or "").strip()
    repo_id = str(getattr(args, "repo_id", "") or "").strip()
    if not outputs_root.parts or not tenant_id or not repo_id:
        print("error: --outputs-root, --tenant-id, and --repo-id are required", file=sys.stderr)
        return 2
    scope = repo_scope_root(outputs_root=outputs_root, tenant_id=tenant_id, repo_id=repo_id)
    profile = resolve_governance_profile_for_scope(scope)
    if profile is None:
        print("error: no valid policy bundle/governance profile found for scope", file=sys.stderr)
        return 1
    payload = {
        "tenant_id": tenant_id,
        "repo_id": normalize_repo_id(repo_id),
        "scope_root": str(scope),
        "effective_profile": profile.to_json_obj(),
    }
    if str(getattr(args, "format", "text")) == "json":
        print(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False))
    else:
        print(f"scope_root: {scope}")
        print("effective_profile:")
        print(json.dumps(profile.to_json_obj(), indent=2, sort_keys=True, ensure_ascii=False))
    return 0


def cmd_control_policy_bundle_write(args: argparse.Namespace) -> int:
    """Write a validated policy bundle to ``.akc/control/policy_bundle.json`` and refresh the index."""

    src = Path(str(getattr(args, "from_file", "") or "")).expanduser().resolve()
    outputs_root = Path(str(getattr(args, "outputs_root", "") or "")).expanduser()
    tenant_id = str(getattr(args, "tenant_id", "") or "").strip()
    repo_id = str(getattr(args, "repo_id", "") or "").strip()
    if not src.is_file():
        print(f"error: --from-file not found: {src}", file=sys.stderr)
        return 2
    if not outputs_root.parts or not tenant_id or not repo_id:
        print("error: --outputs-root, --tenant-id, and --repo-id are required", file=sys.stderr)
        return 2
    try:
        data = src.read_bytes()
        doc = load_policy_bundle_json_bytes(data)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as e:
        print(f"error: failed to read --from-file: {e}", file=sys.stderr)
        return 2
    errs = validate_policy_bundle_document(doc)
    if errs:
        for line in errs:
            print(line, file=sys.stderr)
        return 1

    scope = repo_scope_root(outputs_root=outputs_root, tenant_id=tenant_id, repo_id=repo_id)
    dest = default_policy_bundle_path_for_scope(scope)
    try:
        dest.relative_to(scope.resolve())
    except ValueError:
        print("error: refused path outside tenant/repo scope", file=sys.stderr)
        return 2

    # Canonical JSON bytes for stable fingerprint vs hand-edited whitespace.
    canonical = (json.dumps(doc, sort_keys=True, ensure_ascii=False) + "\n").encode("utf-8")
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".tmp")
    tmp.write_bytes(canonical)
    tmp.replace(dest)
    fp = fingerprint_policy_bundle_bytes(canonical)
    profile = governance_profile_from_document(doc)
    resolved_profile = write_resolved_governance_profile_for_scope(
        scope_root=scope,
        profile=profile,
        now_ms=int(datetime.now(UTC).timestamp() * 1000),
        source_bundle_fingerprint_sha256=fp,
    )

    OperationsIndex.sync_repo_policy_bundle_for_scope(
        outputs_root=outputs_root,
        tenant_id=tenant_id,
        repo_id=repo_id,
    )

    if not bool(getattr(args, "no_audit", False)):
        actor = str(getattr(args, "audit_actor", None) or os.environ.get("AKC_AUDIT_ACTOR") or "").strip() or None
        append_control_audit_event(
            outputs_root=outputs_root,
            tenant_id=tenant_id,
            action="policy_bundle.write",
            details={
                "path": str(dest),
                "rel_path": ".akc/control/policy_bundle.json",
                "fingerprint_sha256": fp,
                "source_path": str(src),
                "rollout_stage": doc.get("rollout_stage"),
                "revision_id": doc.get("revision_id"),
                "governance_profile_fingerprint_sha256": (
                    (resolved_profile.get("profile") or {}).get("fingerprint_sha256")
                    if isinstance(resolved_profile.get("profile"), dict)
                    else None
                ),
            },
            actor=actor,
        )

    if str(getattr(args, "format", "text")) == "json":
        print(
            json.dumps(
                {
                    "wrote": str(dest),
                    "fingerprint_sha256": fp,
                    "tenant_id": tenant_id,
                    "repo_id": normalize_repo_id(repo_id),
                    "governance_profile": resolved_profile.get("profile"),
                },
                indent=2,
                sort_keys=True,
            )
        )
    else:
        print(f"Wrote policy bundle: {dest}")
        print(f"fingerprint_sha256={fp}")
    return 0


def cmd_control_index_rebuild(args: argparse.Namespace) -> int:
    """Rebuild operations.sqlite by scanning manifest files on disk."""

    tenant_id = str(args.tenant_id or "").strip()
    outputs_root = Path(str(args.outputs_root)).expanduser()
    db_path = operations_sqlite_path(outputs_root=outputs_root, tenant_id=tenant_id)
    n = OperationsIndex.rebuild_for_tenant(outputs_root=outputs_root, tenant_id=tenant_id)
    payload = {
        "tenant_id": tenant_id,
        "operations_db": str(db_path),
        "manifests_indexed": n,
    }
    if str(getattr(args, "format", "text")) == "json":
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"Rebuilt operations index: {db_path}")
        print(f"Manifests indexed: {n}")
    return 0
