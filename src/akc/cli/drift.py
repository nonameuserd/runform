from __future__ import annotations

import argparse
from pathlib import Path

from akc.compile.interfaces import TenantRepoScope
from akc.outputs.drift import drift_report, write_baseline
from akc.outputs.fingerprints import fingerprint_ingestion_state
from akc.outputs.watch import WatchConfig, watch_for_changes

from .common import configure_logging


def cmd_drift(args: argparse.Namespace) -> int:
    configure_logging(verbose=args.verbose)

    scope = TenantRepoScope(tenant_id=args.tenant_id, repo_id=args.repo_id)
    outputs_root = Path(args.outputs_root).expanduser()

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

    if args.update_baseline:
        write_baseline(
            scope=scope,
            outputs_root=outputs_root,
            ingest_fingerprint=ingest_fp,
            baseline_path=baseline_path,
        )
        print(f"Wrote baseline: {baseline_path}")
        return 0

    report = drift_report(
        scope=scope,
        outputs_root=outputs_root,
        ingest_fingerprint=ingest_fp,
        baseline_path=baseline_path,
    )
    if args.format == "json":
        import json as _json

        print(_json.dumps(report.to_json_obj(), indent=2, sort_keys=True))
    else:
        print(report.render_text(), end="")
    return 2 if report.has_drift() else 0


def cmd_watch(args: argparse.Namespace) -> int:
    configure_logging(verbose=args.verbose)

    scope = TenantRepoScope(tenant_id=args.tenant_id, repo_id=args.repo_id)
    outputs_root = Path(args.outputs_root).expanduser()
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
    print(f"  ingest_state: {watched[0]}")
    print(f"  outputs_root: {outputs_root}")
    print(f"  baseline: {baseline_path}")

    for _ in watch_for_changes(paths=watched, cfg=cfg):
        ingest_fp = fingerprint_ingestion_state(tenant_id=scope.tenant_id, state_path=ingest_state)
        report = drift_report(
            scope=scope,
            outputs_root=outputs_root,
            ingest_fingerprint=ingest_fp,
            baseline_path=baseline_path,
        )
        if report.has_drift():
            print(report.render_text(), end="")
            if args.exit_on_drift:
                return 2
        else:
            print("OK: no drift detected.")

    return 0

