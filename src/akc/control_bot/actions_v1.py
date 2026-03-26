from __future__ import annotations

import argparse
import io
import json
from collections.abc import Callable, Mapping
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from akc.control.control_audit import append_control_audit_event
from akc.control.fleet_catalog import fleet_get_run, fleet_list_runs_merged, fleet_resolve_label_write_shard
from akc.control.fleet_config import FleetShardConfig
from akc.control.operations_index import OperationsIndex, operations_sqlite_path, validate_run_label_key_value
from akc.control_bot.approval_workflow import ApprovalError, ApprovalWorkflow
from akc.control_bot.command_engine import (
    ActionHandler,
    ActionId,
    ActionRegistry,
    CommandContext,
    CommandResult,
    JSONValue,
    PolicyDenied,
    UnknownAction,
)


def _require_str_arg(args: Mapping[str, JSONValue], key: str) -> str:
    v = args.get(key)
    if not isinstance(v, str) or not v.strip():
        raise ValueError(f"missing required arg: {key}")
    return v.strip()


def _optional_str_arg(args: Mapping[str, JSONValue], key: str) -> str | None:
    v = args.get(key)
    if isinstance(v, str) and v.strip():
        return v.strip()
    return None


def _optional_int_arg(args: Mapping[str, JSONValue], key: str) -> int | None:
    v = args.get(key)
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return int(v)
    if isinstance(v, float):
        return int(v)
    if isinstance(v, str) and v.strip().isdigit():
        return int(v.strip())
    return None


def _optional_bool_arg(args: Mapping[str, JSONValue], key: str) -> bool | None:
    v = args.get(key)
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        s = v.strip().lower()
        if s in {"true", "1", "yes", "y", "on"}:
            return True
        if s in {"false", "0", "no", "n", "off"}:
            return False
    return None


def _single_shard_for_outputs_root(outputs_root: str) -> tuple[FleetShardConfig, ...]:
    root = Path(str(outputs_root or "")).expanduser().resolve()
    return (
        FleetShardConfig(
            id="control-bot-local",
            outputs_root=root,
            tenant_allowlist=("*",),
        ),
    )


def _invoke_cli(
    fn: Callable[[argparse.Namespace], int],
    ns: argparse.Namespace,
) -> tuple[int, str, str]:
    out = io.StringIO()
    err = io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        code = int(fn(ns))
    return code, out.getvalue(), err.getvalue()


def _parse_kv_lines(text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in (text or "").splitlines():
        s = line.strip()
        if not s or ":" not in s:
            continue
        k, v = s.split(":", 1)
        kk = k.strip()
        vv = v.strip()
        if kk:
            out[kk] = vv
    return out


@dataclass(frozen=True, slots=True)
class V1ActionDeps:
    approvals: ApprovalWorkflow
    engine_execute: Callable[[CommandContext, ActionId, dict[str, JSONValue]], CommandResult]


def build_action_registry_v1(*, deps: V1ActionDeps) -> ActionRegistry:
    """Define the control-bot v1 action registry.

    This registry is intentionally deterministic: handlers do bounded parsing, enforce tenant isolation,
    and delegate to existing control-plane code where possible.
    """

    from akc.cli.control import (
        cmd_control_incident_export,
        cmd_control_playbook_run,
    )
    from akc.cli.runtime import cmd_runtime_reconcile, cmd_runtime_start, cmd_runtime_stop

    def _status_runtime(ctx: CommandContext, _args: Mapping[str, JSONValue]) -> CommandResult:
        return CommandResult(
            ok=True,
            action_id="status.runtime",
            message=f"runtime ok (tenant={ctx.principal.tenant_id})",
            data={"tenant_id": ctx.principal.tenant_id},
            status="executed",
        )

    def _status_runs_list(ctx: CommandContext, args: Mapping[str, JSONValue]) -> CommandResult:
        outputs_root = _require_str_arg(args, "outputs_root")
        repo_id = _optional_str_arg(args, "repo_id")
        limit = _optional_int_arg(args, "limit") or 50
        limit = max(1, min(int(limit), 500))
        has_triggers = _optional_str_arg(args, "has_recompile_triggers")
        runtime_evidence = _optional_str_arg(args, "runtime_evidence")
        has_triggers_bool = None
        runtime_evidence_bool = None
        if has_triggers and has_triggers.lower() != "any":
            has_triggers_bool = _optional_bool_arg({"v": has_triggers}, "v")
        if runtime_evidence and runtime_evidence.lower() != "any":
            runtime_evidence_bool = _optional_bool_arg({"v": runtime_evidence}, "v")

        rows = fleet_list_runs_merged(
            _single_shard_for_outputs_root(outputs_root),
            tenant_id=ctx.principal.tenant_id,
            repo_id=repo_id,
            since_ms=_optional_int_arg(args, "since_ms"),
            until_ms=_optional_int_arg(args, "until_ms"),
            stable_intent_sha256=_optional_str_arg(args, "intent_sha256"),
            has_recompile_triggers=has_triggers_bool,
            runtime_evidence_present=runtime_evidence_bool,
            limit=limit,
        )
        n = len(rows)
        return CommandResult(
            ok=True,
            action_id="status.runs.list",
            message=f"Found {n} runs.",
            data={
                "runs": cast(JSONValue, rows),
                "tenant_id": ctx.principal.tenant_id,
                "repo_id": repo_id,
                "limit": limit,
                "outputs_root": outputs_root,
            },
            status="executed",
        )

    def _status_runs_show(ctx: CommandContext, args: Mapping[str, JSONValue]) -> CommandResult:
        outputs_root = _require_str_arg(args, "outputs_root")
        repo_id = _require_str_arg(args, "repo_id")
        run_id = _require_str_arg(args, "run_id")
        row = fleet_get_run(
            _single_shard_for_outputs_root(outputs_root),
            tenant_id=ctx.principal.tenant_id,
            repo_id=repo_id,
            run_id=run_id,
        )
        if row is None:
            return CommandResult(
                ok=False,
                action_id="status.runs.show",
                message=f"Run not found: {repo_id}/{run_id}",
                data={"tenant_id": ctx.principal.tenant_id, "repo_id": repo_id, "run_id": run_id},
                status="error",
            )
        return CommandResult(
            ok=True,
            action_id="status.runs.show",
            message=f"Run {repo_id}/{run_id}: ok",
            data={
                "run": row,
                "tenant_id": ctx.principal.tenant_id,
                "repo_id": repo_id,
                "run_id": run_id,
                "outputs_root": outputs_root,
            },
            status="executed",
        )

    def _mutate_runs_label_set(ctx: CommandContext, args: Mapping[str, JSONValue]) -> CommandResult:
        outputs_root = _require_str_arg(args, "outputs_root")
        repo_id = _require_str_arg(args, "repo_id")
        run_id = _require_str_arg(args, "run_id")
        label_key = _require_str_arg(args, "label_key")
        label_value = _require_str_arg(args, "label_value")
        try:
            lk, lv = validate_run_label_key_value(label_key=label_key, label_value=label_value)
        except ValueError as e:
            return CommandResult(ok=False, action_id="mutate.runs.label.set", message=str(e), status="error")
        shards = _single_shard_for_outputs_root(outputs_root)
        shard = fleet_resolve_label_write_shard(
            shards,
            tenant_id=ctx.principal.tenant_id,
            repo_id=repo_id,
            run_id=run_id,
        )
        if shard is None:
            return CommandResult(
                ok=False,
                action_id="mutate.runs.label.set",
                message="no writable shard for tenant",
                status="error",
            )

        sqlite_p = operations_sqlite_path(outputs_root=shard.outputs_root, tenant_id=ctx.principal.tenant_id)
        idx = OperationsIndex(sqlite_path=sqlite_p)
        row = idx.get_run(tenant_id=ctx.principal.tenant_id, repo_id=repo_id, run_id=run_id)
        if row is None:
            return CommandResult(
                ok=False,
                action_id="mutate.runs.label.set",
                message=f"Run not found: {repo_id}/{run_id}",
                status="error",
            )
        prior = idx.get_label_value(tenant_id=ctx.principal.tenant_id, repo_id=repo_id, run_id=run_id, label_key=lk)
        idx.upsert_label(
            tenant_id=ctx.principal.tenant_id,
            repo_id=repo_id,
            run_id=run_id,
            label_key=lk,
            label_value=lv,
        )
        append_control_audit_event(
            outputs_root=shard.outputs_root,
            tenant_id=ctx.principal.tenant_id,
            action="runs.label.set",
            actor=ctx.principal.principal_id,
            request_id=str(ctx.event.event_id),
            details={
                "repo_id": repo_id,
                "run_id": run_id,
                "label_key": lk,
                "before": {"label_value": prior},
                "after": {"label_value": lv},
                "via": "control-bot",
            },
        )
        return CommandResult(
            ok=True,
            action_id="mutate.runs.label.set",
            message=f"Set label {lk}={lv!r} on {repo_id}/{run_id}.",
            data={
                "tenant_id": ctx.principal.tenant_id,
                "repo_id": repo_id,
                "run_id": run_id,
                "label_key": lk,
                "label_value": lv,
                "outputs_root": str(shard.outputs_root),
            },
            status="executed",
        )

    def _incident_export(ctx: CommandContext, args: Mapping[str, JSONValue]) -> CommandResult:
        outputs_root = _require_str_arg(args, "outputs_root")
        repo_id = _require_str_arg(args, "repo_id")
        run_id = _require_str_arg(args, "run_id")
        ns = argparse.Namespace(
            manifest=None,
            outputs_root=outputs_root,
            tenant_id=ctx.principal.tenant_id,
            repo_id=repo_id,
            run_id=run_id,
            out_dir=_optional_str_arg(args, "out_dir"),
            no_zip=bool(args.get("no_zip", False)),
            max_file_mb=_optional_int_arg(args, "max_file_mb") or 8,
            include_runtime_bundle_pointer=bool(args.get("include_runtime_bundle_pointer", False)),
            signer_identity=_optional_str_arg(args, "signer_identity"),
            signature=_optional_str_arg(args, "signature"),
            format="json",
        )
        code, stdout, stderr = _invoke_cli(cmd_control_incident_export, ns)
        payload = json.loads(stdout) if stdout.strip() else {}
        if code != 0:
            msg = (stderr or stdout or "incident export failed").strip()
            return CommandResult(ok=False, action_id="incident.export", message=msg, status="error")
        out_dir = payload.get("out_dir")
        zip_path = payload.get("zip_path")
        msg = f"Wrote incident bundle: {out_dir}" + (f"\nWrote zip: {zip_path}" if zip_path else "")
        return CommandResult(ok=True, action_id="incident.export", message=msg, data=payload, status="executed")

    def _incident_playbook_run(ctx: CommandContext, args: Mapping[str, JSONValue]) -> CommandResult:
        outputs_root = _require_str_arg(args, "outputs_root")
        repo_id = _require_str_arg(args, "repo_id")
        run_id_a = _require_str_arg(args, "run_id_a")
        run_id_b = _require_str_arg(args, "run_id_b")
        ns = argparse.Namespace(
            outputs_root=outputs_root,
            tenant_id=ctx.principal.tenant_id,
            repo_id=repo_id,
            run_id_a=run_id_a,
            run_id_b=run_id_b,
            focus_run=_optional_str_arg(args, "focus_run") or "b",
            evaluation_modes=_optional_str_arg(args, "evaluation_modes"),
            shard_id=[],
            with_policy_explain=bool(args.get("with_policy_explain", False)),
            no_operational_coupling=bool(args.get("no_operational_coupling", False)),
            max_file_mb=_optional_int_arg(args, "max_file_mb") or 8,
            include_runtime_bundle_pointer=bool(args.get("include_runtime_bundle_pointer", False)),
            timestamp_utc=_optional_str_arg(args, "timestamp_utc"),
            format="json",
            webhook_url=None,
            webhook_secret=None,
            webhook_id="control-bot",
            fleet_config=None,
            webhook_dry_run=True,
        )
        code, stdout, stderr = _invoke_cli(cmd_control_playbook_run, ns)
        payload = json.loads(stdout) if stdout.strip() else {}
        if code != 0:
            msg = (stderr or stdout or "playbook run failed").strip()
            return CommandResult(ok=False, action_id="incident.playbook.run", message=msg, status="error")
        report = payload.get("report")
        report_path = payload.get("report_path")
        msg = f"Playbook completed. report_path={report_path}" if report_path else "Playbook completed."
        return CommandResult(
            ok=True,
            action_id="incident.playbook.run",
            message=msg,
            data={"report": report},
            status="executed",
        )

    def _mutate_runtime_start(ctx: CommandContext, args: Mapping[str, JSONValue]) -> CommandResult:
        outputs_root = _require_str_arg(args, "outputs_root")
        bundle = _require_str_arg(args, "bundle")
        mode = _optional_str_arg(args, "mode")
        ns = argparse.Namespace(
            bundle=bundle,
            mode=mode,
            developer_role_profile=_optional_str_arg(args, "developer_role_profile"),
            outputs_root=outputs_root,
            strict_intent_authority=bool(args.get("strict_intent_authority", False)),
            coordination_parallel_dispatch=_optional_str_arg(args, "coordination_parallel_dispatch") or "inherit",
            coordination_max_in_flight_steps=_optional_int_arg(args, "coordination_max_in_flight_steps"),
            coordination_max_in_flight_per_role=_optional_int_arg(args, "coordination_max_in_flight_per_role"),
            format="text",
            delivery_target_lane=_optional_str_arg(args, "delivery_target_lane"),
            verbose=False,
        )
        code, stdout, stderr = _invoke_cli(cmd_runtime_start, ns)
        if code != 0:
            msg = (stderr or stdout or "runtime start failed").strip()
            return CommandResult(ok=False, action_id="mutate.runtime.start", message=msg, status="error")
        kv = _parse_kv_lines(stdout)
        return CommandResult(
            ok=True,
            action_id="mutate.runtime.start",
            message="Runtime started.",
            data={
                "runtime_run_id": kv.get("runtime_run_id"),
                "tenant_id": kv.get("tenant_id") or ctx.principal.tenant_id,
                "repo_id": kv.get("repo_id"),
                "run_id": kv.get("run_id"),
                "status": kv.get("status"),
                "bundle_path": kv.get("bundle_path"),
                "outputs_root": outputs_root,
            },
            status="executed",
        )

    def _mutate_runtime_stop(ctx: CommandContext, args: Mapping[str, JSONValue]) -> CommandResult:
        outputs_root = _require_str_arg(args, "outputs_root")
        runtime_run_id = _require_str_arg(args, "runtime_run_id")
        ns = argparse.Namespace(
            runtime_run_id=runtime_run_id,
            outputs_root=outputs_root,
            tenant_id=ctx.principal.tenant_id,
            repo_id=_optional_str_arg(args, "repo_id"),
            verbose=False,
        )
        code, stdout, stderr = _invoke_cli(cmd_runtime_stop, ns)
        if code != 0:
            msg = (stderr or stdout or "runtime stop failed").strip()
            return CommandResult(ok=False, action_id="mutate.runtime.stop", message=msg, status="error")
        kv = _parse_kv_lines(stdout)
        return CommandResult(
            ok=True,
            action_id="mutate.runtime.stop",
            message=f"Stop requested for runtime_run_id={runtime_run_id}.",
            data={"runtime_run_id": kv.get("runtime_run_id", runtime_run_id), "status": kv.get("status")},
            status="executed",
        )

    def _mutate_runtime_reconcile(ctx: CommandContext, args: Mapping[str, JSONValue]) -> CommandResult:
        outputs_root = _require_str_arg(args, "outputs_root")
        runtime_run_id = _require_str_arg(args, "runtime_run_id")
        dry_run = bool(args.get("dry_run", False))
        apply = bool(args.get("apply", False))
        if dry_run == apply:
            raise ValueError("exactly one of dry_run=true or apply=true is required")
        ns = argparse.Namespace(
            runtime_run_id=runtime_run_id,
            outputs_root=outputs_root,
            tenant_id=ctx.principal.tenant_id,
            repo_id=_optional_str_arg(args, "repo_id"),
            dry_run=dry_run,
            apply=apply,
            watch=bool(args.get("watch", False)),
            watch_interval_sec=float(_optional_int_arg(args, "watch_interval_sec") or 5),
            watch_max_iterations=int(_optional_int_arg(args, "watch_max_iterations") or 30),
            strict_intent_authority=bool(args.get("strict_intent_authority", False)),
            coordination_parallel_dispatch=_optional_str_arg(args, "coordination_parallel_dispatch") or "inherit",
            coordination_max_in_flight_steps=_optional_int_arg(args, "coordination_max_in_flight_steps"),
            coordination_max_in_flight_per_role=_optional_int_arg(args, "coordination_max_in_flight_per_role"),
            verbose=False,
        )
        code, stdout, stderr = _invoke_cli(cmd_runtime_reconcile, ns)
        if code != 0:
            msg = (stderr or stdout or "runtime reconcile failed").strip()
            return CommandResult(ok=False, action_id="mutate.runtime.reconcile", message=msg, status="error")
        return CommandResult(
            ok=True,
            action_id="mutate.runtime.reconcile",
            message="Reconcile completed.",
            data={"runtime_run_id": runtime_run_id, "outputs_root": outputs_root, "stdout": stdout.strip()},
            status="executed",
        )

    def _approval_approve(ctx: CommandContext, args: Mapping[str, JSONValue]) -> CommandResult:
        request_id = _require_str_arg(args, "request_id")
        try:
            req = deps.approvals.resolve(
                tenant_id=ctx.principal.tenant_id,
                request_id=request_id,
                resolver_principal_id=ctx.principal.principal_id,
                decision="approve",
                now_ms=ctx.now_ms,
            )
        except ApprovalError as e:
            return CommandResult(ok=False, action_id="approval.approve", message=str(e), status="error")
        if req.status != "approved":
            return CommandResult(
                ok=False,
                action_id="approval.approve",
                message=f"Request {request_id} is {req.status}.",
                data={"request_id": request_id, "status": req.status},
                request_id=request_id,
                status="error",
            )
        # Claim execution exactly once to prevent duplicate runs on repeated approve commands.
        try:
            claimed = deps.approvals.claim_execution(tenant_id=ctx.principal.tenant_id, request_id=request_id)
        except ApprovalError as e:
            return CommandResult(ok=False, action_id="approval.approve", message=str(e), status="error")
        if not claimed:
            return CommandResult(
                ok=True,
                action_id="approval.approve",
                message=f"Request {request_id} was already executed.",
                data={"request_id": request_id, "status": "executed"},
                request_id=request_id,
                status="executed",
            )

        # Execute the originally requested action using the stored args.
        try:
            res = deps.engine_execute(ctx, req.action_id, dict(req.args))
        except PolicyDenied as e:
            return CommandResult(
                ok=False,
                action_id="approval.approve",
                message=f"Approved, but policy denied execution: {e}",
                data={"request_id": request_id, "approved_action_id": req.action_id},
                request_id=request_id,
                status="denied",
            )
        except UnknownAction as e:
            return CommandResult(
                ok=False,
                action_id="approval.approve",
                message=f"Approved, but action handler missing: {e}",
                data={"request_id": request_id, "approved_action_id": req.action_id},
                request_id=request_id,
                status="error",
            )
        return CommandResult(
            ok=True,
            action_id="approval.approve",
            message=f"Approved request {request_id} and executed {req.action_id}.\n\n{res.message}",
            data={
                "request_id": request_id,
                "approved_action_id": req.action_id,
                "approved_args": dict(req.args),
                "executed": {"action_id": res.action_id, "ok": res.ok, "data": res.data},
            },
            request_id=request_id,
            status="executed",
        )

    def _approval_deny(ctx: CommandContext, args: Mapping[str, JSONValue]) -> CommandResult:
        request_id = _require_str_arg(args, "request_id")
        try:
            req = deps.approvals.resolve(
                tenant_id=ctx.principal.tenant_id,
                request_id=request_id,
                resolver_principal_id=ctx.principal.principal_id,
                decision="deny",
                now_ms=ctx.now_ms,
            )
        except ApprovalError as e:
            return CommandResult(ok=False, action_id="approval.deny", message=str(e), status="error")
        return CommandResult(
            ok=True,
            action_id="approval.deny",
            message=f"Denied request {request_id}.",
            data={"request_id": request_id, "status": req.status, "action_id": req.action_id},
            request_id=request_id,
            status="executed",
        )

    handlers: dict[ActionId, ActionHandler] = {
        "status.runtime": _status_runtime,
        "status.runs.list": _status_runs_list,
        "status.runs.show": _status_runs_show,
        "approval.approve": _approval_approve,
        "approval.deny": _approval_deny,
        "incident.playbook.run": _incident_playbook_run,
        "incident.export": _incident_export,
        "mutate.runs.label.set": _mutate_runs_label_set,
        "mutate.runtime.start": _mutate_runtime_start,
        "mutate.runtime.stop": _mutate_runtime_stop,
        "mutate.runtime.reconcile": _mutate_runtime_reconcile,
    }
    return ActionRegistry(handlers=handlers)
