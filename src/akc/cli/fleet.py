"""Phase 5 fleet CLI: read-only HTTP catalog and webhook delivery."""

from __future__ import annotations

import argparse
import importlib.resources
import json
import sys
import threading
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, cast

from akc.control.automation_coordinator import run_fleet_automation_coordinator
from akc.control.fleet_catalog import fleet_get_run, fleet_list_runs_merged
from akc.control.fleet_config import load_fleet_config
from akc.control.fleet_http import serve_fleet_http
from akc.control.fleet_webhooks import deliver_fleet_webhooks
from akc.control.operations_index import OperationsIndex
from akc.control.policy_bundle import (
    distribute_policy_bundle_document,
    load_policy_bundle_json_bytes,
    policy_bundle_drift_report,
    validate_policy_bundle_document,
)


def _http_base_url(server_address: tuple[object, ...] | object) -> str:
    if isinstance(server_address, tuple) and len(server_address) >= 2:
        host_part, port_part = server_address[0], server_address[1]
        host_s = host_part.decode("utf-8") if isinstance(host_part, bytes) else str(host_part)
        return f"http://{host_s}:{int(cast(int | str, port_part))}"
    return f"http://{server_address!r}"


def cmd_fleet_serve(args: argparse.Namespace) -> int:
    cfg = load_fleet_config(Path(str(args.config)))
    host = str(args.host or "127.0.0.1").strip()
    port = int(args.port)
    httpd = serve_fleet_http(cfg, host=host, port=port)
    print(f"akc fleet listening on {_http_base_url(httpd.server_address)} (read-only API)", file=sys.stderr)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nshutdown", file=sys.stderr)
    finally:
        httpd.server_close()
    return 0


def cmd_fleet_runs_list(args: argparse.Namespace) -> int:
    cfg = load_fleet_config(Path(str(args.config)))
    tenant_id = str(args.tenant_id or "").strip()
    repo_raw = getattr(args, "repo_id", None)
    repo_id = str(repo_raw).strip() if repo_raw else None
    if repo_id == "":
        repo_id = None

    has_trig = getattr(args, "has_recompile_triggers", "any")
    trig_filter: bool | None = None
    if has_trig == "yes":
        trig_filter = True
    elif has_trig == "no":
        trig_filter = False

    rt_ev = getattr(args, "runtime_evidence", "any")
    ev_filter: bool | None = None
    if rt_ev == "yes":
        ev_filter = True
    elif rt_ev == "no":
        ev_filter = False

    intent = getattr(args, "intent_sha256", None)
    intent_s = str(intent).strip().lower() if intent else None
    if intent_s == "":
        intent_s = None

    runs = fleet_list_runs_merged(
        cfg.shards,
        tenant_id=tenant_id,
        repo_id=repo_id,
        since_ms=int(args.since_ms) if getattr(args, "since_ms", None) is not None else None,
        until_ms=int(args.until_ms) if getattr(args, "until_ms", None) is not None else None,
        stable_intent_sha256=intent_s,
        has_recompile_triggers=trig_filter,
        runtime_evidence_present=ev_filter,
        limit=int(getattr(args, "limit", 50)),
    )
    payload = {"tenant_id": tenant_id, "repo_id": repo_id, "runs": runs}
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def cmd_fleet_runs_show(args: argparse.Namespace) -> int:
    cfg = load_fleet_config(Path(str(args.config)))
    row = fleet_get_run(
        cfg.shards,
        tenant_id=str(args.tenant_id or "").strip(),
        repo_id=str(args.repo_id or "").strip(),
        run_id=str(args.run_id or "").strip(),
    )
    if row is None:
        print(json.dumps({"error": "not_found"}, indent=2, sort_keys=True))
        return 1
    print(json.dumps({"run": row}, indent=2, sort_keys=True))
    return 0


def cmd_fleet_webhooks_deliver(args: argparse.Namespace) -> int:
    cfg = load_fleet_config(Path(str(args.config)))
    tenants_arg = getattr(args, "tenants", None)
    tenants: list[str] | None = None
    if isinstance(tenants_arg, str) and tenants_arg.strip():
        tenants = [t.strip() for t in tenants_arg.split(",") if t.strip()]
    results = deliver_fleet_webhooks(cfg, tenants=tenants, dry_run=bool(getattr(args, "dry_run", False)))
    out = [
        {
            "webhook_id": r.webhook_id,
            "event": r.event,
            "http_status": r.http_status,
            "item_count": r.item_count,
            "error": r.error,
        }
        for r in results
    ]
    print(json.dumps({"deliveries": out}, indent=2, sort_keys=True))
    return 0


def cmd_fleet_automation_run(args: argparse.Namespace) -> int:
    cfg = load_fleet_config(Path(str(args.config)))
    tenants_arg = getattr(args, "tenants", None)
    tenants: list[str] | None = None
    if isinstance(tenants_arg, str) and tenants_arg.strip():
        tenants = [t.strip() for t in tenants_arg.split(",") if t.strip()]
    actions_arg = getattr(args, "actions", None)
    actions = tuple(a.strip() for a in str(actions_arg or "").split(",") if a.strip())
    if not actions:
        actions = ("metadata_tag_write", "incident_workflow_orchestration", "webhook_signal")
    outcomes = run_fleet_automation_coordinator(
        cfg,
        tenants=tenants,
        actions=actions,
        policy_version=str(getattr(args, "policy_version", "v1")),
        max_candidates=int(getattr(args, "max_candidates", 50)),
        max_actions=int(getattr(args, "max_actions", 100)),
        max_retries=int(getattr(args, "max_retries", 3)),
        base_backoff_ms=int(getattr(args, "backoff_ms", 1000)),
        dry_run=bool(getattr(args, "dry_run", False)),
    )
    out = [
        {
            "dedupe_key": o.dedupe_key,
            "shard_id": o.shard_id,
            "tenant_id": o.tenant_id,
            "repo_id": o.repo_id,
            "run_id": o.run_id,
            "action": o.action,
            "status": o.status,
            "attempts": o.attempts,
            "checkpoint_status": o.checkpoint_status,
            "error": o.error,
            "dead_letter_relpath": o.dead_letter_relpath,
        }
        for o in outcomes
    ]
    print(json.dumps({"outcomes": out}, indent=2, sort_keys=True))
    return 0


def cmd_fleet_policy_bundle_distribute(args: argparse.Namespace) -> int:
    cfg = load_fleet_config(Path(str(args.config)))
    src = Path(str(args.from_file)).expanduser().resolve()
    if not src.is_file():
        print(json.dumps({"error": "from_file_not_found", "path": str(src)}, indent=2, sort_keys=True))
        return 2
    try:
        doc = load_policy_bundle_json_bytes(src.read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        print(json.dumps({"error": "invalid_bundle", "details": str(exc)}, indent=2, sort_keys=True))
        return 2
    errs = validate_policy_bundle_document(doc)
    if errs:
        print(json.dumps({"error": "schema_validation_failed", "validation_errors": errs}, indent=2, sort_keys=True))
        return 1

    tenant_id = str(args.tenant_id or "").strip()
    repo_id = str(args.repo_id or "").strip()
    writes = distribute_policy_bundle_document(
        shards=cfg.shards,
        tenant_id=tenant_id,
        repo_id=repo_id,
        document=doc,
        actor=str(getattr(args, "actor", "") or "").strip() or None,
        activate=bool(getattr(args, "activate", False)),
    )
    for item in writes:
        OperationsIndex.sync_repo_policy_bundle_for_scope(
            outputs_root=Path(str(item["outputs_root"])),
            tenant_id=tenant_id,
            repo_id=repo_id,
        )
    print(json.dumps({"writes": writes, "write_count": len(writes)}, indent=2, sort_keys=True))
    return 0


def cmd_fleet_policy_bundle_drift(args: argparse.Namespace) -> int:
    cfg = load_fleet_config(Path(str(args.config)))
    tenant_id = str(args.tenant_id or "").strip()
    repo_id = str(args.repo_id or "").strip()
    report = policy_bundle_drift_report(
        shards=cfg.shards,
        tenant_id=tenant_id,
        repo_id=repo_id,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 2 if bool(report.get("drift_detected")) else 0


def cmd_fleet_dashboard_serve(args: argparse.Namespace) -> int:
    """Host the Track 6 static operator dashboard (GET-only client to the fleet API)."""

    host = str(args.host or "127.0.0.1").strip()
    port = int(args.port)
    static_files = importlib.resources.files("akc.control.operator_dashboard")
    with importlib.resources.as_file(static_files) as root:
        handler = partial(SimpleHTTPRequestHandler, directory=str(root))
        httpd = ThreadingHTTPServer((host, port), handler)
        print(
            f"operator dashboard (read-only) at http://{host}:{port}/ — "
            f"point UI at your fleet API; set AKC_FLEET_CORS_ALLOW_ORIGIN if origins differ",
            file=sys.stderr,
        )
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nshutdown", file=sys.stderr)
        finally:
            httpd.server_close()
    return 0


def cmd_fleet_serve_smoke(args: argparse.Namespace) -> int:
    """Internal: used by tests to bind an ephemeral port and exit after one /health check."""

    from urllib.request import urlopen

    cfg = load_fleet_config(Path(str(args.config)))
    httpd = serve_fleet_http(cfg, host="127.0.0.1", port=0)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        with urlopen(f"{_http_base_url(httpd.server_address)}/health", timeout=5.0) as resp:
            _ = resp.read()
    finally:
        httpd.shutdown()
        httpd.server_close()
    return 0


def register_fleet_parsers(sub: Any) -> None:
    fleet = sub.add_parser(
        "fleet",
        help=(
            "Phase 5 fleet control plane: aggregate operations indexes across many outputs_root "
            "trees (read-only HTTP, optional static operator dashboard, webhook helpers)"
        ),
    )
    fleet_sub = fleet.add_subparsers(dest="fleet_group", required=True)

    serve = fleet_sub.add_parser("serve", help="Run read-only HTTP query API (stdlib server)")
    serve.add_argument("--config", required=True, help="Path to fleet JSON config")
    serve.add_argument("--host", default="127.0.0.1", help="Bind address (default: 127.0.0.1)")
    serve.add_argument("--port", type=int, default=8765, help="TCP port (default: 8765)")
    serve.set_defaults(func=cmd_fleet_serve)

    dash = fleet_sub.add_parser(
        "dashboard-serve",
        help="Serve static read-only operator dashboard (GET-only to fleet API; see fleet_http module docs)",
    )
    dash.add_argument("--host", default="127.0.0.1", help="Bind address (default: 127.0.0.1)")
    dash.add_argument("--port", type=int, default=9090, help="TCP port (default: 9090)")
    dash.set_defaults(func=cmd_fleet_dashboard_serve)

    runs = fleet_sub.add_parser("runs", help="Fleet-wide run queries (same data as HTTP, local JSON)")
    runs_sub = runs.add_subparsers(dest="fleet_runs_action", required=True)

    rl = runs_sub.add_parser("list", help="List runs merged across shards")
    rl.add_argument("--config", required=True, help="Path to fleet JSON config")
    rl.add_argument("--tenant-id", required=True, help="Tenant identifier")
    rl.add_argument("--repo-id", help="Optional repo filter")
    rl.add_argument("--since-ms", type=int, help="updated_at_ms lower bound")
    rl.add_argument("--until-ms", type=int, help="updated_at_ms upper bound")
    rl.add_argument("--intent-sha256", help="Stable intent hash filter")
    rl.add_argument(
        "--has-recompile-triggers",
        choices=["any", "yes", "no"],
        default="any",
        help="Filter by indexed recompile_trigger_count (default: any)",
    )
    rl.add_argument(
        "--runtime-evidence",
        choices=["any", "yes", "no"],
        default="any",
        help="Filter by runtime evidence present (default: any)",
    )
    rl.add_argument("--limit", type=int, default=50, help="Max runs (default: 50, max 500)")
    rl.set_defaults(func=cmd_fleet_runs_list)

    rs = runs_sub.add_parser("show", help="Show one run across shards (first match)")
    rs.add_argument("--config", required=True, help="Path to fleet JSON config")
    rs.add_argument("--tenant-id", required=True, help="Tenant identifier")
    rs.add_argument("--repo-id", required=True, help="Repo identifier")
    rs.add_argument("--run-id", required=True, help="Compile run id")
    rs.set_defaults(func=cmd_fleet_runs_show)

    wh = fleet_sub.add_parser(
        "webhooks-deliver",
        help="POST paged webhook payloads for recompile_triggers / living_drift signals",
    )
    wh.add_argument("--config", required=True, help="Path to fleet JSON config")
    wh.add_argument(
        "--tenants",
        help="Comma-separated tenant ids to scan (default: all tenant directories under shards)",
    )
    wh.add_argument("--dry-run", action="store_true", help="Build pages but do not POST or advance watermarks")
    wh.set_defaults(func=cmd_fleet_webhooks_deliver)

    auto = fleet_sub.add_parser(
        "automation-run",
        help="Run bounded cross-shard automation coordinator (control-plane only)",
    )
    auto.add_argument("--config", required=True, help="Path to fleet JSON config")
    auto.add_argument(
        "--tenants",
        help="Comma-separated tenant ids (default: all tenant directories under shard outputs roots)",
    )
    auto.add_argument(
        "--actions",
        default="metadata_tag_write,incident_workflow_orchestration,webhook_signal",
        help="Comma-separated actions (metadata_tag_write, incident_workflow_orchestration, webhook_signal)",
    )
    auto.add_argument("--policy-version", default="v1", help="Policy/version namespace used in dedupe key")
    auto.add_argument("--max-candidates", type=int, default=50, help="Max candidate runs per tenant")
    auto.add_argument("--max-actions", type=int, default=100, help="Max actions in one bounded pass")
    auto.add_argument("--max-retries", type=int, default=3, help="Retries before dead-letter checkpoint state")
    auto.add_argument("--backoff-ms", type=int, default=1000, help="Base retry backoff in milliseconds")
    auto.add_argument("--dry-run", action="store_true", help="Do not mutate labels/playbooks/webhook delivery")
    auto.set_defaults(func=cmd_fleet_automation_run)

    policy_bundle = fleet_sub.add_parser(
        "policy-bundle",
        help="Cross-shard policy bundle lifecycle controls (distribution, activation markers, drift report)",
    )
    policy_bundle_sub = policy_bundle.add_subparsers(dest="fleet_policy_bundle_action", required=True)

    pb_dist = policy_bundle_sub.add_parser(
        "distribute",
        help="Distribute one validated policy bundle revision to all tenant-eligible shards",
    )
    pb_dist.add_argument("--config", required=True, help="Path to fleet JSON config")
    pb_dist.add_argument("--tenant-id", required=True, help="Tenant identifier")
    pb_dist.add_argument("--repo-id", required=True, help="Repo identifier")
    pb_dist.add_argument("--from-file", type=Path, required=True, help="Source policy bundle JSON")
    pb_dist.add_argument(
        "--activate",
        action="store_true",
        help="Also write policy activation marker and audit event (includes rollback marker when revision changes)",
    )
    pb_dist.add_argument("--actor", default=None, help="Audit actor override (default: $USER)")
    pb_dist.set_defaults(func=cmd_fleet_policy_bundle_distribute)

    pb_drift = policy_bundle_sub.add_parser(
        "drift",
        help="Report shard drift when policy revisions/fingerprints or activation markers diverge",
    )
    pb_drift.add_argument("--config", required=True, help="Path to fleet JSON config")
    pb_drift.add_argument("--tenant-id", required=True, help="Tenant identifier")
    pb_drift.add_argument("--repo-id", required=True, help="Repo identifier")
    pb_drift.set_defaults(func=cmd_fleet_policy_bundle_drift)

    smoke = fleet_sub.add_parser(
        "serve-smoke",
        help=argparse.SUPPRESS,
    )
    smoke.add_argument("--config", required=True)
    smoke.set_defaults(func=cmd_fleet_serve_smoke)
