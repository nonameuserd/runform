from __future__ import annotations

import argparse
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

from akc.path_security import safe_resolve_path, safe_resolve_scoped_path
from akc.viewer import ViewerInputs, load_viewer_snapshot
from akc.viewer.export import export_bundle
from akc.viewer.models import ViewerSnapshot
from akc.viewer.snapshot import ViewerError
from akc.viewer.web import build_static_viewer

from .profile_defaults import resolve_optional_project_string
from .project_config import load_akc_project_config


def _print_view_tui_text_fallback(snap: ViewerSnapshot) -> None:
    """Plain-text plan summary when the curses TUI cannot run."""

    plan = snap.plan
    print(f"{plan.tenant_id}/{plan.repo_id} — {plan.status}")
    print(plan.goal)
    for s in sorted(plan.steps, key=lambda st: st.order_idx):
        ev = snap.evidence.by_step.get(s.id, [])
        print(f"- {s.status:>11}  {s.title}  (evidence={len(ev)})")


def _default_out_dir(*, outputs_root: Path, tenant_id: str, repo_id: str, kind: str) -> Path:
    ts = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    return safe_resolve_scoped_path(
        safe_resolve_path(outputs_root),
        tenant_id,
        repo_id,
        ".akc",
        "viewer",
        kind,
        ts,
    )


def cmd_view(args: argparse.Namespace) -> int:
    cwd = Path.cwd()
    proj = load_akc_project_config(cwd)
    tenant_r = resolve_optional_project_string(
        cli_value=getattr(args, "tenant_id", None),
        env_key="AKC_TENANT_ID",
        file_value=proj.tenant_id if proj is not None else None,
        env=os.environ,
    )
    repo_r = resolve_optional_project_string(
        cli_value=getattr(args, "repo_id", None),
        env_key="AKC_REPO_ID",
        file_value=proj.repo_id if proj is not None else None,
        env=os.environ,
    )
    outputs_r = resolve_optional_project_string(
        cli_value=getattr(args, "outputs_root", None),
        env_key="AKC_OUTPUTS_ROOT",
        file_value=proj.outputs_root if proj is not None else None,
        env=os.environ,
    )
    if tenant_r.value is None:
        raise SystemExit(
            "Missing tenant id: provide --tenant-id, set AKC_TENANT_ID, or add tenant_id to .akc/project.json"
        )
    if repo_r.value is None:
        raise SystemExit("Missing repo id: provide --repo-id, set AKC_REPO_ID, or add repo_id to .akc/project.json")
    if outputs_r.value is None:
        raise SystemExit(
            "Missing outputs root: provide --outputs-root, set AKC_OUTPUTS_ROOT, "
            "or add outputs_root to .akc/project.json"
        )
    args.tenant_id = tenant_r.value
    args.repo_id = repo_r.value
    args.outputs_root = outputs_r.value

    outputs_root = safe_resolve_path(args.outputs_root)
    plan_base = Path(args.plan_base_dir).expanduser() if args.plan_base_dir else None

    inputs = ViewerInputs(
        tenant_id=str(args.tenant_id),
        repo_id=str(args.repo_id),
        outputs_root=outputs_root,
        plan_base_dir=plan_base,
        schema_version=int(getattr(args, "schema_version", 1)),
    )
    try:
        snap = load_viewer_snapshot(inputs)
    except ViewerError as e:
        scope = outputs_root / args.tenant_id / args.repo_id
        print(f"ERROR: {e}", file=sys.stderr)
        print(
            "The viewer needs plan state for this tenant/repo under the outputs root.\n"
            f"  Resolved scope: {scope}\n"
            "  Fix: use the same --tenant-id / --repo-id as your last `akc compile` "
            "(or update .akc/project.json), or run `akc compile` for this scope first.",
            file=sys.stderr,
        )
        return 2

    sub = str(getattr(args, "view_command", "") or "")
    if sub == "tui":
        try:
            from akc.viewer.tui import TuiError, run_tui, tui_environment_ok
        except ModuleNotFoundError as e:
            print(f"ERROR: TUI viewer is unavailable on this platform: {e}")
            return 2
        ok, reason = tui_environment_ok()
        if not ok:
            print(f"ERROR: TUI needs a capable terminal ({reason}). Showing text view instead.")
            _print_view_tui_text_fallback(snap)
            return 0
        try:
            return int(run_tui(snap))
        except TuiError as e:
            print(f"ERROR: {e}")
            _print_view_tui_text_fallback(snap)
            return 0

    if sub == "web":
        if getattr(args, "serve_port", None) is not None and not bool(getattr(args, "serve", False)):
            print("ERROR: --port requires --serve", file=sys.stderr)
            return 2
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
        if bool(getattr(args, "serve", False)):
            from akc.viewer.serve import serve_viewer_bundle

            return int(serve_viewer_bundle(web_res.root, port=getattr(args, "serve_port", None)))
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
