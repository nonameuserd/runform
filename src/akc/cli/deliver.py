"""CLI: ``akc deliver`` — named-recipient delivery sessions (local control-plane stubs)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Literal, cast

from akc.delivery import ingest as delivery_ingest
from akc.delivery import orchestrate as delivery_orchestrate
from akc.delivery import store as delivery_store
from akc.delivery.compile_handoff import load_compile_handoff
from akc.delivery.control_index import append_delivery_control_audit_event
from akc.delivery.distribution_dispatch import run_delivery_distribution
from akc.delivery.event_types import (
    DELIVERY_COMPILE_COMPLETED,
    DELIVERY_COMPILE_OUTPUTS_BOUND,
    DELIVERY_FAILED,
)
from akc.delivery.metrics import compute_delivery_metrics


def _project_dir(args: argparse.Namespace) -> Path:
    raw = getattr(args, "project_dir", None)
    return Path(raw).resolve() if raw else Path.cwd()


def _tenant_repo_for_project(project_dir: Path) -> tuple[str, str]:
    from akc.cli.project_config import load_akc_project_config

    cfg = load_akc_project_config(project_dir.resolve())
    if cfg is None:
        return "local", "local"
    t = (cfg.tenant_id or "").strip() or "local"
    r = (cfg.repo_id or "").strip() or "local"
    return t, r


def cmd_deliver_submit(args: argparse.Namespace) -> int:
    project_dir = _project_dir(args)
    request_text = str(getattr(args, "request", "") or "").strip()
    recipients_cli = list(getattr(args, "recipients", None) or [])
    recipients_file_path = getattr(args, "recipients_file", None)
    platforms_raw = str(getattr(args, "platforms", "") or "").strip()
    release_mode = str(getattr(args, "release_mode", "beta"))
    do_compile = bool(getattr(args, "deliver_compile", False))
    delivery_version = str(getattr(args, "delivery_version", "1.0.0") or "1.0.0").strip() or "1.0.0"

    if not request_text:
        print("akc deliver: --request is required", file=sys.stderr)
        return 2
    recipients_from_file: list[str] = []
    if recipients_file_path is not None:
        try:
            recipients_from_file = delivery_ingest.load_recipients_from_file(Path(recipients_file_path))
        except ValueError as exc:
            print(f"akc deliver: {exc}", file=sys.stderr)
            return 2
    recipients_merged = list(recipients_from_file) + list(recipients_cli)
    if not recipients_merged:
        print(
            "akc deliver: recipients are required via --recipient and/or --recipients-file "
            "(free-text --request does not define the authoritative recipient list)",
            file=sys.stderr,
        )
        return 2

    try:
        normalized_recipients = delivery_store.normalize_recipients(recipients_merged)
        platforms = delivery_store.parse_platforms_csv(platforms_raw)
        if release_mode not in delivery_store.RELEASE_MODES:
            print(
                f"akc deliver: invalid --release-mode {release_mode!r}; "
                f"expected one of {list(delivery_store.RELEASE_MODES)}",
                file=sys.stderr,
            )
            return 2
        summary = delivery_store.create_delivery_session(
            project_dir=project_dir,
            request_text=request_text,
            recipients=normalized_recipients,
            platforms=platforms,
            release_mode=cast(Literal["beta", "store", "both"], release_mode),
            delivery_version=delivery_version,
        )
    except ValueError as exc:
        print(f"akc deliver: {exc}", file=sys.stderr)
        return 2

    compile_exit: int | None = None
    compile_run_id: str | None = None
    packaging_ok: bool = True
    packaging_summary: dict[str, Any] | None = None
    if do_compile:
        req = summary.get("request")
        parsed_goal = ""
        if isinstance(req, dict):
            parsed_block = req.get("parsed")
            if isinstance(parsed_block, dict):
                parsed_goal = str(parsed_block.get("app_goal") or "").strip()
        goal = parsed_goal or request_text
        compile_exit, compile_run_id = delivery_orchestrate.run_delivery_compile(
            project_dir=project_dir,
            goal=goal,
        )
        try:
            delivery_store.update_session_compile_stage(
                project_dir=project_dir,
                delivery_id=str(summary["delivery_id"]),
                run_id=compile_run_id,
                succeeded=compile_exit == 0,
                error=None if compile_exit == 0 else "compile exited with non-zero status",
            )
        except (OSError, ValueError) as exc:
            print(f"akc deliver: could not update session after compile: {exc}", file=sys.stderr)
            return 2
        delivery_store.append_event(
            project_dir=project_dir,
            delivery_id=str(summary["delivery_id"]),
            event_type=DELIVERY_COMPILE_COMPLETED if compile_exit == 0 else DELIVERY_FAILED,
            payload={
                "exit_code": compile_exit,
                "run_id": compile_run_id,
                "phase": "compile",
            },
        )
        if compile_exit == 0:
            handoff = load_compile_handoff(project_dir=project_dir, compile_run_id=compile_run_id)
            try:
                delivery_store.update_delivery_request_compile_handoff(
                    project_dir=project_dir,
                    delivery_id=str(summary["delivery_id"]),
                    handoff=handoff,
                )
                delivery_store.update_session_compile_handoff(
                    project_dir=project_dir,
                    delivery_id=str(summary["delivery_id"]),
                    handoff=handoff,
                )
            except (OSError, ValueError) as exc:
                print(f"akc deliver: could not bind compile outputs to delivery session: {exc}", file=sys.stderr)
                return 2
            append_delivery_control_audit_event(
                project_dir,
                action="delivery.compile.outputs_bound",
                details={
                    "delivery_id": str(summary["delivery_id"]),
                    "compile_run_id": compile_run_id,
                    "delivery_plan_loaded": bool(handoff.get("delivery_plan_loaded")),
                    "manifest_present": bool(handoff.get("manifest_present")),
                },
            )
            delivery_store.append_event(
                project_dir=project_dir,
                delivery_id=str(summary["delivery_id"]),
                event_type=DELIVERY_COMPILE_OUTPUTS_BOUND,
                payload={
                    "compile_run_id": compile_run_id,
                    "manifest_present": handoff.get("manifest_present"),
                    "delivery_plan_loaded": handoff.get("delivery_plan_loaded"),
                    "delivery_plan_ref": handoff.get("delivery_plan_ref"),
                },
            )
            packaging_summary = delivery_orchestrate.run_delivery_build_and_package(
                project_dir=project_dir,
                delivery_id=str(summary["delivery_id"]),
                platforms=platforms,
                release_mode=cast(Literal["beta", "store", "both"], release_mode),
                delivery_version=delivery_version,
                compile_run_id=compile_run_id,
            )
            packaging_ok = bool(packaging_summary.get("ok"))
        summary["session"] = delivery_store.load_session(project_dir, str(summary["delivery_id"]))

    sess = summary["session"]
    if not isinstance(sess, dict):
        sess = {}
    req_out = summary.get("request")
    parsed_out: dict[str, Any] = {}
    if isinstance(req_out, dict):
        p = req_out.get("parsed")
        if isinstance(p, dict):
            parsed_out = dict(p)
    out: dict[str, Any] = {
        "delivery_id": summary["delivery_id"],
        "delivery_dir": summary["delivery_dir"],
        "session_phase": sess.get("session_phase"),
        "preflight_ok": len(summary.get("preflight_issues") or []) == 0,
        "parsed": parsed_out,
        "required_human_inputs_count": len(summary.get("required_human_inputs") or []),
    }
    if do_compile:
        out["compile_exit_code"] = compile_exit
        out["compile_run_id"] = compile_run_id
        out["delivery_version"] = delivery_version
        if packaging_summary is not None:
            out["packaging_ok"] = packaging_ok
            out["packaging"] = {
                "provider_versions": packaging_summary.get("provider_versions"),
                "preflight_issues": packaging_summary.get("preflight_issues"),
            }
    print(json.dumps(out, indent=2, sort_keys=True))
    if not do_compile:
        return 0
    if compile_exit != 0:
        return 2
    return 0 if packaging_ok else 2


def cmd_deliver_status(args: argparse.Namespace) -> int:
    project_dir = _project_dir(args)
    delivery_id = str(getattr(args, "delivery_id", "") or "").strip()
    if not delivery_id:
        print("akc deliver status: --delivery-id is required", file=sys.stderr)
        return 2
    try:
        delivery_store.assert_safe_delivery_id(delivery_id)
        request = delivery_store.load_request(project_dir, delivery_id)
        session = delivery_store.load_session(project_dir, delivery_id)
        events = delivery_store.load_events(project_dir, delivery_id)
    except FileNotFoundError as exc:
        print(f"akc deliver status: {exc}", file=sys.stderr)
        return 2
    except ValueError as exc:
        print(f"akc deliver status: {exc}", file=sys.stderr)
        return 2

    metrics = compute_delivery_metrics(request=request, session=session, events=events)
    payload = {
        "delivery_id": delivery_id,
        "request": request,
        "session": session,
        "metrics": metrics,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def cmd_deliver_events(args: argparse.Namespace) -> int:
    project_dir = _project_dir(args)
    delivery_id = str(getattr(args, "delivery_id", "") or "").strip()
    if not delivery_id:
        print("akc deliver events: --delivery-id is required", file=sys.stderr)
        return 2
    try:
        delivery_store.assert_safe_delivery_id(delivery_id)
        events = delivery_store.load_events(project_dir, delivery_id)
    except FileNotFoundError as exc:
        print(f"akc deliver events: {exc}", file=sys.stderr)
        return 2
    except ValueError as exc:
        print(f"akc deliver events: {exc}", file=sys.stderr)
        return 2

    print(json.dumps({"delivery_id": delivery_id, "events": events}, indent=2, sort_keys=True))
    return 0


def cmd_deliver_resend(args: argparse.Namespace) -> int:
    project_dir = _project_dir(args)
    delivery_id = str(getattr(args, "delivery_id", "") or "").strip()
    recipient = str(getattr(args, "recipient", "") or "").strip()
    if not delivery_id or not recipient:
        print("akc deliver resend: --delivery-id and --recipient are required", file=sys.stderr)
        return 2
    try:
        delivery_store.assert_safe_delivery_id(delivery_id)
        row = delivery_store.record_resend(
            project_dir=project_dir,
            delivery_id=delivery_id,
            recipient=recipient,
        )
    except FileNotFoundError as exc:
        print(f"akc deliver resend: {exc}", file=sys.stderr)
        return 2
    except ValueError as exc:
        print(f"akc deliver resend: {exc}", file=sys.stderr)
        return 2

    print(json.dumps({"ok": True, "event": row}, indent=2, sort_keys=True))
    return 0


def cmd_deliver_activation_report(args: argparse.Namespace) -> int:
    project_dir = _project_dir(args)
    delivery_id = str(getattr(args, "delivery_id", "") or "").strip()
    if not delivery_id:
        print("akc deliver activation-report: --delivery-id is required", file=sys.stderr)
        return 2
    path = getattr(args, "json_file", None)
    try:
        raw = Path(path).read_text(encoding="utf-8") if path is not None else sys.stdin.read()
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise ValueError("JSON must be an object")
        delivery_store.ingest_client_activation_report(
            project_dir=project_dir,
            delivery_id=delivery_id,
            payload=payload,
        )
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        print(f"akc deliver activation-report: {exc}", file=sys.stderr)
        return 2
    print(json.dumps({"ok": True, "delivery_id": delivery_id}, indent=2, sort_keys=True))
    return 0


def cmd_deliver_web_invite_open(args: argparse.Namespace) -> int:
    project_dir = _project_dir(args)
    delivery_id = str(getattr(args, "delivery_id", "") or "").strip()
    token = str(getattr(args, "invite_token_id", "") or "").strip()
    signature = str(getattr(args, "signature", "") or "").strip()
    if not delivery_id or not token or not signature:
        print(
            "akc deliver web-invite-open: --delivery-id, --invite-token-id, --signature are required",
            file=sys.stderr,
        )
        return 2
    try:
        rec = delivery_store.record_web_invite_opened(
            project_dir=project_dir,
            delivery_id=delivery_id,
            invite_token_id=token,
            signature=signature,
            payload={"cli": True},
        )
    except (OSError, ValueError) as exc:
        print(f"akc deliver web-invite-open: {exc}", file=sys.stderr)
        return 2
    print(json.dumps({"ok": True, "record": rec}, indent=2, sort_keys=True))
    return 0


def cmd_deliver_promote(args: argparse.Namespace) -> int:
    project_dir = _project_dir(args)
    delivery_id = str(getattr(args, "delivery_id", "") or "").strip()
    lane = str(getattr(args, "lane", "") or "").strip().lower()
    if not delivery_id or not lane:
        print("akc deliver promote: --delivery-id and --lane are required", file=sys.stderr)
        return 2
    try:
        delivery_store.assert_safe_delivery_id(delivery_id)
        row = delivery_store.record_promote(
            project_dir=project_dir,
            delivery_id=delivery_id,
            lane=cast(Literal["beta", "store"], lane),
        )
        distribution: dict[str, Any] | None = None
        if lane == "store":
            req = delivery_store.load_request(project_dir, delivery_id)
            sess = delivery_store.load_session(project_dir, delivery_id)
            rm = cast(Literal["beta", "store", "both"], str(req.get("release_mode") or "beta"))
            plats = [str(p) for p in cast(list[Any], req.get("platforms") or []) if str(p).strip()]
            tid, rid = _tenant_repo_for_project(project_dir)
            cr_raw = sess.get("compile_run_id")
            compile_run_id = (
                str(cr_raw).strip()
                if cr_raw is not None and str(cr_raw).strip() and str(cr_raw).strip().lower() != "none"
                else None
            )
            dist = run_delivery_distribution(
                project_dir=project_dir,
                delivery_id=delivery_id,
                tenant_id=tid,
                repo_id=rid,
                platforms=plats,
                release_mode=rm,
                delivery_version=str(sess.get("delivery_version") or "1.0.0"),
                compile_run_id=compile_run_id,
                lanes=("store",),
            )
            distribution = dist
    except FileNotFoundError as exc:
        print(f"akc deliver promote: {exc}", file=sys.stderr)
        return 2
    except ValueError as exc:
        print(f"akc deliver promote: {exc}", file=sys.stderr)
        return 2

    out: dict[str, Any] = {"ok": True, "event": row}
    exit_code = 0
    if lane == "store" and distribution is not None:
        out["distribution"] = distribution
        if not bool(distribution.get("ok")):
            exit_code = 2
    print(json.dumps(out, indent=2, sort_keys=True))
    return exit_code


def cmd_deliver_gate_pass(args: argparse.Namespace) -> int:
    project_dir = _project_dir(args)
    delivery_id = str(getattr(args, "delivery_id", "") or "").strip()
    note = str(getattr(args, "operator_note", "") or "").strip() or None
    if not delivery_id:
        print("akc deliver gate-pass: --delivery-id is required", file=sys.stderr)
        return 2
    try:
        delivery_store.assert_safe_delivery_id(delivery_id)
        row = delivery_store.record_human_readiness_gate_pass(
            project_dir=project_dir,
            delivery_id=delivery_id,
            operator_note=note,
        )
    except FileNotFoundError as exc:
        print(f"akc deliver gate-pass: {exc}", file=sys.stderr)
        return 2
    except ValueError as exc:
        print(f"akc deliver gate-pass: {exc}", file=sys.stderr)
        return 2

    print(json.dumps({"ok": True, "event": row}, indent=2, sort_keys=True))
    return 0


def register_deliver_parsers(sub: Any) -> None:
    deliver = sub.add_parser(
        "deliver",
        help=(
            "Named-recipient delivery sessions: capture a plain-language request, recipients, "
            "and platform targets under .akc/delivery/<id>/. After compile, packaging and release "
            "lanes consume compile-time delivery_plan / runtime outputs — not the compile controller loop."
        ),
    )
    deliver.add_argument(
        "--project-dir",
        type=Path,
        default=None,
        help="Project root containing .akc/ (default: current working directory)",
    )

    deliver_sub = deliver.add_subparsers(dest="deliver_command", required=False)

    deliver.add_argument(
        "--request",
        default=None,
        help='Plain-language delivery goal (required for the default "submit" action)',
    )
    deliver.add_argument(
        "--recipient",
        action="append",
        dest="recipients",
        default=None,
        metavar="EMAIL",
        help="Recipient email (repeat per address). Authoritative list; not parsed from --request.",
    )
    deliver.add_argument(
        "--recipients-file",
        type=Path,
        default=None,
        help=(
            "JSON {\"recipients\": [...]} / {\"emails\": [...]} or one email per line "
            "(merged with --recipient; authoritative list)"
        ),
    )
    deliver.add_argument(
        "--compile",
        dest="deliver_compile",
        action="store_true",
        help=(
            "Run `akc compile` from --project-dir, bind delivery_plan/manifest refs on the session, "
            "then run packaging (web uses hosting hints from generated delivery_plan when present)"
        ),
    )
    deliver.add_argument(
        "--delivery-version",
        default="1.0.0",
        help="Logical semver-like version for this session (drives iOS/Android/Web build metadata; default: 1.0.0)",
    )
    deliver.add_argument(
        "--platforms",
        default="web,ios,android",
        help="Comma-separated platforms: web, ios, android (default: web,ios,android)",
    )
    deliver.add_argument(
        "--release-mode",
        choices=list(delivery_store.RELEASE_MODES),
        default="beta",
        help="beta | store | both (default: beta; store lanes require provider credentials in later phases)",
    )

    st = deliver_sub.add_parser("status", help="Show delivery request + session JSON for a delivery_id")
    st.add_argument("--delivery-id", required=True, help="Delivery session id (from submit output)")
    st.add_argument(
        "--project-dir",
        type=Path,
        default=None,
        help="Project root containing .akc/ (default: current working directory)",
    )
    st.set_defaults(func=cmd_deliver_status)

    ev = deliver_sub.add_parser("events", help="List delivery control-plane events for a delivery_id")
    ev.add_argument("--delivery-id", required=True)
    ev.add_argument(
        "--project-dir",
        type=Path,
        default=None,
        help="Project root containing .akc/ (default: current working directory)",
    )
    ev.set_defaults(func=cmd_deliver_events)

    rs = deliver_sub.add_parser(
        "resend",
        help="Record a resend request for one recipient (distribution adapters consume events later)",
    )
    rs.add_argument("--delivery-id", required=True)
    rs.add_argument("--recipient", required=True, help="Email address (must belong to the delivery)")
    rs.add_argument(
        "--project-dir",
        type=Path,
        default=None,
        help="Project root containing .akc/ (default: current working directory)",
    )
    rs.set_defaults(func=cmd_deliver_resend)

    pr = deliver_sub.add_parser(
        "promote",
        help="Request promotion to a release lane (e.g. store) after beta readiness",
    )
    pr.add_argument("--delivery-id", required=True)
    pr.add_argument(
        "--lane",
        required=True,
        choices=sorted(delivery_store.PROMOTION_LANES),
        help="Target lane: beta | store",
    )
    pr.add_argument(
        "--project-dir",
        type=Path,
        default=None,
        help="Project root containing .akc/ (default: current working directory)",
    )
    pr.set_defaults(func=cmd_deliver_promote)

    gp = deliver_sub.add_parser(
        "gate-pass",
        help="Record human readiness gate passed (required before store promotion when release_mode is both)",
    )
    gp.add_argument("--delivery-id", required=True)
    gp.add_argument(
        "--note",
        default=None,
        dest="operator_note",
        help="Optional operator note (audit only)",
    )
    gp.add_argument(
        "--project-dir",
        type=Path,
        default=None,
        help="Project root containing .akc/ (default: current working directory)",
    )
    gp.set_defaults(func=cmd_deliver_gate_pass)

    ar = deliver_sub.add_parser(
        "activation-report",
        help="Ingest app activation JSON (first_run_at_unix_ms / heartbeat_at_unix_ms) for a recipient token",
    )
    ar.add_argument("--delivery-id", required=True)
    ar.add_argument(
        "--json-file",
        type=Path,
        default=None,
        help="Path to JSON payload (default: read stdin from activation client)",
    )
    ar.add_argument(
        "--project-dir",
        type=Path,
        default=None,
        help="Project root containing .akc/ (default: current working directory)",
    )
    ar.set_defaults(func=cmd_deliver_activation_report)

    wi = deliver_sub.add_parser(
        "web-invite-open",
        help="Record a signed web invite link open (provider proof for web beta)",
    )
    wi.add_argument("--delivery-id", required=True)
    wi.add_argument("--invite-token-id", required=True, dest="invite_token_id")
    wi.add_argument("--signature", required=True, help="Hex HMAC from the invite URL (akc_sig)")
    wi.add_argument(
        "--project-dir",
        type=Path,
        default=None,
        help="Project root containing .akc/ (default: current working directory)",
    )
    wi.set_defaults(func=cmd_deliver_web_invite_open)

    deliver.set_defaults(func=cmd_deliver_submit, deliver_command=None)
