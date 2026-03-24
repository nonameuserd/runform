from __future__ import annotations

import argparse
import os
from pathlib import Path

from akc.compile.interfaces import TenantRepoScope
from akc.outputs.drift import drift_report, write_baseline, write_drift_artifacts
from akc.outputs.fingerprints import fingerprint_ingestion_state
from akc.outputs.watch import WatchConfig, watch_for_changes
from akc.run.loader import find_latest_run_manifest, load_run_manifest

from .common import configure_logging
from .profile_defaults import resolve_developer_role_profile


def cmd_drift(args: argparse.Namespace) -> int:
    configure_logging(verbose=args.verbose)

    scope = TenantRepoScope(tenant_id=args.tenant_id, repo_id=args.repo_id)
    outputs_root = Path(args.outputs_root).expanduser()
    dev_res = resolve_developer_role_profile(
        cli_value=getattr(args, "developer_role_profile", None),
        cwd=Path.cwd(),
        env=os.environ,
    )
    dev_ctx = {"developer_role_profile": dev_res.value, "resolution_source": dev_res.source}

    ingest_fp = None
    if args.ingest_state is not None:
        ingest_fp = fingerprint_ingestion_state(
            tenant_id=scope.tenant_id,
            state_path=args.ingest_state,
        )

    baseline_path = (
        Path(args.baseline_path).expanduser()
        if args.baseline_path is not None
        else outputs_root / scope.tenant_id / scope.repo_id / ".akc" / "living" / "baseline.json"
    )
    latest_run_manifest = find_latest_run_manifest(
        outputs_root=outputs_root,
        tenant_id=scope.tenant_id,
        repo_id=scope.repo_id,
    )
    check_id = "latest"
    if latest_run_manifest is not None:
        check_id = load_run_manifest(path=latest_run_manifest).run_id

    if args.update_baseline:
        write_baseline(
            scope=scope,
            outputs_root=outputs_root,
            ingest_fingerprint=ingest_fp,
            baseline_path=baseline_path,
        )
        print(f"Wrote baseline: {baseline_path}")
        print(
            f"Developer role context (read-only): profile={dev_res.value} source={dev_res.source}",
        )
        return 0

    report = drift_report(
        scope=scope,
        outputs_root=outputs_root,
        ingest_fingerprint=ingest_fp,
        baseline_path=baseline_path,
    )
    write_drift_artifacts(
        scope=scope,
        outputs_root=outputs_root,
        report=report,
        check_id=check_id,
        baseline_path=baseline_path if baseline_path.exists() else None,
        source="manual",
    )
    if args.format == "json":
        import json as _json

        payload = dict(report.to_json_obj())
        payload["developer_role_context"] = dev_ctx
        print(_json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(
            f"Developer role context (read-only): profile={dev_res.value} source={dev_res.source}",
        )
        print(report.render_text(), end="")
    return 2 if report.has_drift() else 0


def cmd_watch(args: argparse.Namespace) -> int:
    configure_logging(verbose=args.verbose)

    scope = TenantRepoScope(tenant_id=args.tenant_id, repo_id=args.repo_id)
    outputs_root = Path(args.outputs_root).expanduser()
    dev_res = resolve_developer_role_profile(
        cli_value=getattr(args, "developer_role_profile", None),
        cwd=Path.cwd(),
        env=os.environ,
    )
    baseline_path = (
        Path(args.baseline_path).expanduser()
        if args.baseline_path is not None
        else outputs_root / scope.tenant_id / scope.repo_id / ".akc" / "living" / "baseline.json"
    )
    ingest_state = args.ingest_state
    if ingest_state is None:
        raise SystemExit("--ingest-state is required for watch mode (source trigger)")

    cfg = WatchConfig(
        poll_interval_s=float(args.poll_interval_s),
        debounce_s=float(args.debounce_s),
    )
    watched = [Path(ingest_state).expanduser()]

    print(f"Watching for drift. scope={scope.tenant_id}/{scope.repo_id}")
    print(
        f"  developer_role_context (read-only): profile={dev_res.value} source={dev_res.source}",
    )
    print(f"  ingest_state: {watched[0]}")
    print(f"  outputs_root: {outputs_root}")
    print(f"  baseline: {baseline_path}")

    for _ in watch_for_changes(paths=watched, cfg=cfg):
        latest_run_manifest = find_latest_run_manifest(
            outputs_root=outputs_root,
            tenant_id=scope.tenant_id,
            repo_id=scope.repo_id,
        )
        check_id = "latest"
        if latest_run_manifest is not None:
            check_id = load_run_manifest(path=latest_run_manifest).run_id
        ingest_fp = fingerprint_ingestion_state(tenant_id=scope.tenant_id, state_path=ingest_state)
        report = drift_report(
            scope=scope,
            outputs_root=outputs_root,
            ingest_fingerprint=ingest_fp,
            baseline_path=baseline_path,
        )
        write_drift_artifacts(
            scope=scope,
            outputs_root=outputs_root,
            report=report,
            check_id=check_id,
            baseline_path=baseline_path if baseline_path.exists() else None,
            source="drift_watch",
        )
        if report.has_drift():
            print(report.render_text(), end="")
            if args.exit_on_drift:
                return 2
        else:
            print("OK: no drift detected.")

    return 0
