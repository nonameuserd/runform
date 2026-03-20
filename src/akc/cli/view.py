from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path

from akc.viewer import ViewerInputs, load_viewer_snapshot
from akc.viewer.export import export_bundle
from akc.viewer.web import build_static_viewer


def _default_out_dir(*, outputs_root: Path, tenant_id: str, repo_id: str, kind: str) -> Path:
    ts = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    return outputs_root / tenant_id / repo_id / ".akc" / "viewer" / kind / ts


def cmd_view(args: argparse.Namespace) -> int:
    outputs_root = Path(args.outputs_root).expanduser()
    plan_base = Path(args.plan_base_dir).expanduser() if args.plan_base_dir else None

    inputs = ViewerInputs(
        tenant_id=str(args.tenant_id),
        repo_id=str(args.repo_id),
        outputs_root=outputs_root,
        plan_base_dir=plan_base,
        schema_version=int(getattr(args, "schema_version", 1)),
    )
    snap = load_viewer_snapshot(inputs)

    sub = str(getattr(args, "view_command", "") or "")
    if sub == "tui":
        try:
            from akc.viewer.tui import TuiError, run_tui
        except ModuleNotFoundError as e:
            print(f"ERROR: TUI viewer is unavailable on this platform: {e}")
            return 2
        try:
            return int(run_tui(snap))
        except TuiError as e:
            print(f"ERROR: {e}")
            # Fallback: minimal text output
            plan = snap.plan
            print(f"{plan.tenant_id}/{plan.repo_id} — {plan.status}")
            print(plan.goal)
            for s in sorted(plan.steps, key=lambda st: st.order_idx):
                ev = snap.evidence.by_step.get(s.id, [])
                print(f"- {s.status:>11}  {s.title}  (evidence={len(ev)})")
            return 0

    if sub == "web":
        out_dir = (
            Path(args.out_dir).expanduser()
            if args.out_dir
            else _default_out_dir(
                outputs_root=outputs_root,
                tenant_id=str(args.tenant_id),
                repo_id=str(args.repo_id),
                kind="web",
            )
        )
        web_res = build_static_viewer(snapshot=snap, out_dir=out_dir)
        print(f"Wrote static viewer: {web_res.index_html}")
        print(f"Copied evidence files: {web_res.copied_files}")
        return 0

    if sub == "export":
        out_dir = (
            Path(args.out_dir).expanduser()
            if args.out_dir
            else _default_out_dir(
                outputs_root=outputs_root,
                tenant_id=str(args.tenant_id),
                repo_id=str(args.repo_id),
                kind="export",
            )
        )
        export_res = export_bundle(
            snapshot=snap,
            out_dir=out_dir,
            include_all_evidence=bool(getattr(args, "include_all_evidence", True)),
            make_zip=bool(getattr(args, "zip", True)),
        )
        print(f"Wrote export bundle dir: {export_res.root}")
        print(f"Copied evidence files: {export_res.copied_files}")
        if export_res.zip_path is not None:
            print(f"Wrote export zip: {export_res.zip_path}")
        return 0

    print("ERROR: unknown view subcommand")
    return 2
