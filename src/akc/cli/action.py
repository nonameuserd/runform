from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from akc.action import (
    ActionChannelAdapters,
    ActionStore,
    ProviderRegistry,
    approve_step,
    build_notification,
    build_plan,
    execute_plan,
    parse_intent,
)


def cmd_action_submit(args: argparse.Namespace) -> int:
    store = ActionStore()
    intent = parse_intent(
        text=str(getattr(args, "text", "")),
        tenant_id=str(getattr(args, "tenant_id", "")),
        repo_id=str(getattr(args, "repo_id", "")),
        channel=_opt_str(getattr(args, "channel", None)),
        actor_id=_opt_str(getattr(args, "actor_id", None)),
    )
    plan = build_plan(intent)
    store.write_intent(intent)
    store.write_plan(plan)
    dry_run = bool(getattr(args, "dry_run", False))
    simulate = bool(getattr(args, "simulate", False))
    if dry_run and simulate:
        raise SystemExit("--dry-run and --simulate cannot be used together")
    mode = "live"
    if dry_run:
        mode = "dry_run"
        result = {
            "intent_id": intent.intent_id,
            "status": "dry_run",
            "mode": "dry_run",
            "steps": [
                {
                    "step_id": step.step_id,
                    "action_type": step.action_type,
                    "provider": step.provider,
                    "risk_tier": step.risk_tier,
                    "requires_approval": step.requires_approval,
                }
                for step in plan.steps
            ],
        }
        store.write_result(intent_id=intent.intent_id, result=result)
    else:
        if simulate:
            mode = "simulate"
        result = execute_plan(
            intent=intent,
            plan=plan,
            store=store,
            providers=ProviderRegistry(base_dir=store.workspace_root()),
            mode=mode,
        )
    envelope = build_notification(
        intent=intent,
        status=str(result.get("status", "unknown")),
        summary=f"Action {intent.intent_id} {result.get('status', 'unknown')}",
    )
    print(
        json.dumps(
            {"intent_id": intent.intent_id, "result": result, "notification": envelope},
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def cmd_action_status(args: argparse.Namespace) -> int:
    intent_id = str(getattr(args, "intent_id", "")).strip()
    if not intent_id:
        raise SystemExit("--intent-id is required")
    store = ActionStore()
    result = store.read_result(intent_id=intent_id)
    if result is None:
        execution = store.read_execution(intent_id=intent_id)
        payload: dict[str, Any] = {"intent_id": intent_id, "status": "submitted", "execution_records": len(execution)}
    else:
        payload = dict(result)
        payload["execution_records"] = len(store.read_execution(intent_id=intent_id))
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def cmd_action_approve(args: argparse.Namespace) -> int:
    intent_id = str(getattr(args, "intent_id", "")).strip()
    step_id = str(getattr(args, "step_id", "")).strip()
    if not intent_id:
        raise SystemExit("--intent-id is required")
    if not step_id:
        raise SystemExit("--step-id is required")
    store = ActionStore()
    approve_step(action_dir=store.action_dir(intent_id=intent_id), step_id=step_id)
    intent = store.read_intent(intent_id=intent_id)
    plan = store.read_plan(intent_id=intent_id)
    result = execute_plan(
        intent=intent,
        plan=plan,
        store=store,
        providers=ProviderRegistry(base_dir=store.workspace_root()),
        mode="live",
    )
    print(json.dumps({"intent_id": intent_id, "approved_step_id": step_id, "result": result}, indent=2, sort_keys=True))
    return 0


def cmd_action_replay(args: argparse.Namespace) -> int:
    intent_id = str(getattr(args, "intent_id", "")).strip()
    mode = str(getattr(args, "mode", "simulate") or "simulate").strip()
    if not intent_id:
        raise SystemExit("--intent-id is required")
    if mode not in {"simulate", "live"}:
        raise SystemExit("--mode must be one of: simulate, live")
    store = ActionStore()
    intent = store.read_intent(intent_id=intent_id)
    plan = store.read_plan(intent_id=intent_id)
    result = execute_plan(
        intent=intent,
        plan=plan,
        store=store,
        providers=ProviderRegistry(base_dir=store.workspace_root()),
        mode=mode,
    )
    print(json.dumps({"intent_id": intent_id, "mode": mode, "result": result}, indent=2, sort_keys=True))
    return 0


def cmd_action_dispatch_channel(args: argparse.Namespace) -> int:
    channel = str(getattr(args, "channel", "")).strip()
    payload_file = Path(str(getattr(args, "payload_file", "") or "")).expanduser()
    if not channel:
        raise SystemExit("--channel is required")
    if not str(payload_file).strip():
        raise SystemExit("--payload-file is required")
    loaded = json.loads(payload_file.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise SystemExit("payload-file must contain a JSON object")
    envelope = ActionChannelAdapters().parse_inbound(channel=channel, payload=loaded)
    submit_args = argparse.Namespace(
        text=envelope.text,
        tenant_id=envelope.tenant_id,
        repo_id=envelope.repo_id,
        channel=envelope.channel,
        actor_id=envelope.actor_id,
    )
    return cmd_action_submit(submit_args)


def _opt_str(value: object) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None
