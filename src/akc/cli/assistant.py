from __future__ import annotations

import argparse
import json
import os
import time
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from typing import cast

from akc.assistant import AssistantScope, AssistantSessionStore, execute_cli_command, process_prompt
from akc.assistant.models import AssistantMode
from akc.control_bot.approval_workflow import ApprovalWorkflow, SqliteApprovalStore
from akc.control_bot.policy_gate import PolicyGate, build_role_allowlist
from akc.llm import build_llm_backend, resolve_llm_runtime_config
from akc.memory.salience import parse_memory_boost_overrides, parse_memory_pin_overrides
from akc.path_security import safe_resolve_path, safe_resolve_scoped_path

from .common import configure_logging
from .project_config import load_akc_project_config


def _assistant_policy_gate() -> PolicyGate:
    # Fail-closed by default; allow only status/mutate/approval action namespaces.
    role_allowlist = build_role_allowlist(
        {
            "operator": ["status.*", "mutate.*", "approval.*"],
        }
    )
    return PolicyGate(mode="enforce", role_allowlist=role_allowlist, opa=None)


def _resolve_format(*, cli_value: str | None, project_value: str | None) -> str:
    raw = str(cli_value or project_value or "text").strip().lower()
    return "json" if raw == "json" else "text"


def _resolve_retention_days(*, cli_value: int | None, project_value: int | None) -> int:
    if cli_value is not None:
        return max(1, int(cli_value))
    if project_value is not None:
        return max(1, int(project_value))
    return 14


def _principal_id() -> str:
    explicit = str(os.environ.get("AKC_ASSISTANT_PRINCIPAL_ID", "") or "").strip()
    if explicit:
        return explicit
    user = str(os.environ.get("USER", "") or "").strip()
    if user:
        return user
    return "local-assistant"


def _print_text_response(payload: Mapping[str, object]) -> None:
    print(f"[{payload.get('status')}] {payload.get('message')}")
    print(f"session_id: {payload.get('session_id')}  mode: {payload.get('mode')}")
    scope = payload.get("scope")
    if isinstance(scope, dict):
        print(
            "scope: "
            f"tenant_id={scope.get('tenant_id') or '-'} "
            f"repo_id={scope.get('repo_id') or '-'} "
            f"outputs_root={scope.get('outputs_root') or '-'}"
        )
    suggested = payload.get("suggested_command")
    if isinstance(suggested, list) and suggested:
        print("suggested_command: akc " + " ".join(str(x) for x in suggested))
    request_id = payload.get("request_id")
    if isinstance(request_id, str) and request_id.strip():
        print(f"request_id: {request_id}")
    exit_code = payload.get("command_exit_code")
    if isinstance(exit_code, int):
        print(f"command_exit_code: {exit_code}")
    stdout = payload.get("command_stdout")
    stderr = payload.get("command_stderr")
    if isinstance(stdout, str) and stdout.strip():
        print("--- command stdout ---")
        print(stdout.rstrip())
    if isinstance(stderr, str) and stderr.strip():
        print("--- command stderr ---")
        print(stderr.rstrip())
    mtrace = payload.get("memory_trace")
    if isinstance(mtrace, dict):
        print("--- memory trace ---")
        print(json.dumps(mtrace, indent=2, sort_keys=True))


def cmd_assistant(args: argparse.Namespace) -> int:
    configure_logging(verbose=bool(getattr(args, "verbose", False)))
    cwd = safe_resolve_path(Path.cwd())
    project = load_akc_project_config(cwd)
    out_format = _resolve_format(
        cli_value=getattr(args, "format", None),
        project_value=project.assistant_default_format if project is not None else None,
    )
    retention_days = _resolve_retention_days(
        cli_value=getattr(args, "session_retention_days", None),
        project_value=project.assistant_session_retention_days if project is not None else None,
    )
    if project is not None and project.assistant_model_hint is not None and getattr(args, "llm_model", None) is None:
        args.assistant_model_hint = project.assistant_model_hint
    try:
        llm_cfg = resolve_llm_runtime_config(
            args=args,
            env=os.environ,
            project=(project.llm if project is not None else None),
            surface="assistant",
        )
        llm_backend = build_llm_backend(config=llm_cfg)
    except ValueError as e:
        raise SystemExit(f"failed to resolve assistant llm backend: {e}") from e

    session_store = AssistantSessionStore(root=cwd, retention_days=retention_days)
    _ = session_store.prune_expired()

    project_scope = AssistantScope(
        tenant_id=project.tenant_id if project is not None else None,
        repo_id=project.repo_id if project is not None else None,
        outputs_root=project.outputs_root if project is not None else None,
    ).normalized()
    scope = AssistantScope(
        tenant_id=str(getattr(args, "tenant_id", "") or "").strip() or project_scope.tenant_id,
        repo_id=str(getattr(args, "repo_id", "") or "").strip() or project_scope.repo_id,
        outputs_root=str(getattr(args, "outputs_root", "") or "").strip() or project_scope.outputs_root,
    ).normalized()

    policy_path_raw = str(getattr(args, "memory_policy_path", "") or "").strip()
    if not policy_path_raw and project is not None and project.memory_policy_path is not None:
        policy_path_raw = str(project.memory_policy_path).strip()
    memory_policy_path: str | None = None
    if policy_path_raw:
        p = Path(policy_path_raw).expanduser()
        memory_policy_path = str((cwd / p).resolve()) if not p.is_absolute() else str(p.resolve())
    memory_pins = parse_memory_pin_overrides(
        tuple(project.memory_pins if project is not None else ()) + tuple(getattr(args, "memory_pin", []) or [])
    )
    memory_boosts = (
        dict(project.memory_boosts or {}) if project is not None and project.memory_boosts is not None else {}
    )
    memory_boosts.update(parse_memory_boost_overrides(list(getattr(args, "memory_boost", []) or [])))
    memory_budget_tokens: int | None = None
    cli_budget = getattr(args, "memory_budget_tokens", None)
    if cli_budget is not None:
        memory_budget_tokens = int(cli_budget)
    elif project is not None and project.assistant_memory_budget_tokens is not None:
        memory_budget_tokens = int(project.assistant_memory_budget_tokens)
    elif project is not None and project.memory_budget_tokens is not None:
        memory_budget_tokens = int(project.memory_budget_tokens)
    memory_enabled = bool(str(os.environ.get("AKC_WEIGHTED_MEMORY_ENABLED", "")).strip() == "1")
    if memory_policy_path is not None or memory_pins or memory_boosts or memory_budget_tokens is not None:
        memory_enabled = True

    resume = str(getattr(args, "resume", "") or "").strip()
    if resume:
        session = session_store.load(session_id=resume)
    else:
        mode: AssistantMode = "execute" if str(getattr(args, "mode", "plan")) == "execute" else "plan"
        session = session_store.create_session(mode=mode, scope=scope)

    if scope != AssistantScope():
        # CLI scope hints override only the provided fields for this invocation.
        merged_scope = AssistantScope(
            tenant_id=scope.tenant_id if scope.tenant_id is not None else session.scope.tenant_id,
            repo_id=scope.repo_id if scope.repo_id is not None else session.scope.repo_id,
            outputs_root=scope.outputs_root if scope.outputs_root is not None else session.scope.outputs_root,
        ).normalized()
        if merged_scope != session.scope:
            session = replace(session, scope=merged_scope, updated_at_ms=int(time.time() * 1000))

    approval_db = safe_resolve_scoped_path(cwd, ".akc", "assistant", "assistant.sqlite")
    approvals = ApprovalWorkflow(store=SqliteApprovalStore(sqlite_path=approval_db), allow_self_approval=True)
    gate = _assistant_policy_gate()
    principal_id = _principal_id()

    prompt = getattr(args, "prompt", None)
    if isinstance(prompt, str):
        response = process_prompt(
            session=session,
            prompt=prompt,
            approvals=approvals,
            policy_gate=gate,
            principal_id=principal_id,
            execute_command=execute_cli_command,
            memory_enabled=memory_enabled,
            memory_policy_root=cwd,
            memory_policy_path=memory_policy_path,
            memory_budget_tokens=memory_budget_tokens,
            memory_pins=memory_pins,
            memory_boosts=memory_boosts,
            llm_backend=llm_backend,
            llm_mode=llm_cfg.backend,
        )
        session_store.save(session=response.session)
        payload = response.to_json_obj()
        if out_format == "json":
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            _print_text_response(cast(Mapping[str, object], payload))
        exit_code = payload.get("command_exit_code")
        if isinstance(exit_code, int):
            return exit_code
        if payload.get("status") == "error":
            return 2
        return 0

    print(f"Assistant session: {session.session_id} (mode={session.mode})")
    print("Enter /help for commands. Ctrl+D or /exit to quit.")
    session_store.save(session=session)
    while True:
        try:
            line = input("assistant> ")
        except EOFError:
            print("")
            break
        response = process_prompt(
            session=session,
            prompt=line,
            approvals=approvals,
            policy_gate=gate,
            principal_id=principal_id,
            execute_command=execute_cli_command,
            memory_enabled=memory_enabled,
            memory_policy_root=cwd,
            memory_policy_path=memory_policy_path,
            memory_budget_tokens=memory_budget_tokens,
            memory_pins=memory_pins,
            memory_boosts=memory_boosts,
            llm_backend=llm_backend,
            llm_mode=llm_cfg.backend,
        )
        session = response.session
        session_store.save(session=session)
        payload = response.to_json_obj()
        if out_format == "json":
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            _print_text_response(cast(Mapping[str, object], payload))
        if payload.get("status") == "exit":
            break
    return 0
