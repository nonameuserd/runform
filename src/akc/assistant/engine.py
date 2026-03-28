from __future__ import annotations

import hashlib
import json
import os
import shlex
import subprocess
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Literal, cast

from akc.compile.interfaces import LLMBackend, LLMMessage, LLMRequest, TenantRepoScope
from akc.control_bot.approval_workflow import ApprovalWorkflow, stable_args_fingerprint
from akc.control_bot.command_engine import (
    Command,
    CommandClarificationRequired,
    CommandContext,
    InboundEvent,
    Principal,
    nl_fallback_parse,
)
from akc.control_bot.policy_gate import PolicyGate
from akc.memory.models import JSONValue
from akc.memory.salience import (
    SalienceCandidate,
    build_extractive_compaction,
    load_memory_policy,
    pack_by_token_budget,
    score_candidates,
)

from .models import (
    AssistantCompactedTurnRef,
    AssistantMemoryEntry,
    AssistantResponse,
    AssistantScope,
    AssistantSession,
    AssistantTurn,
    PendingAction,
)


@dataclass(frozen=True, slots=True)
class CommandExecutionResult:
    argv: tuple[str, ...]
    exit_code: int
    stdout: str
    stderr: str


CommandExecutor = Callable[[Sequence[str]], CommandExecutionResult]


@dataclass(frozen=True, slots=True)
class SuggestedAction:
    argv: tuple[str, ...] | None
    message: str


def execute_cli_command(argv: Sequence[str]) -> CommandExecutionResult:
    tokens = tuple(str(x).strip() for x in argv if str(x).strip())
    if not tokens:
        return CommandExecutionResult(argv=(), exit_code=2, stdout="", stderr="empty command")
    if tokens[0] == "assistant":
        return CommandExecutionResult(
            argv=tokens,
            exit_code=2,
            stdout="",
            stderr="assistant mode cannot invoke assistant recursively",
        )
    proc = subprocess.run(  # noqa: S603
        [sys.executable, "-m", "akc.cli", *tokens],
        capture_output=True,
        text=True,
        check=False,
    )
    return CommandExecutionResult(
        argv=tokens,
        exit_code=int(proc.returncode),
        stdout=str(proc.stdout or ""),
        stderr=str(proc.stderr or ""),
    )


def process_prompt(
    *,
    session: AssistantSession,
    prompt: str,
    approvals: ApprovalWorkflow,
    policy_gate: PolicyGate,
    principal_id: str,
    execute_command: CommandExecutor,
    now_ms: int | None = None,
    memory_enabled: bool | None = None,
    memory_policy_root: Path | None = None,
    memory_policy_path: str | None = None,
    memory_budget_tokens: int | None = None,
    memory_pins: Sequence[str] | None = None,
    memory_boosts: dict[str, float] | None = None,
    llm_backend: LLMBackend | None = None,
    llm_mode: str | None = None,
) -> AssistantResponse:
    ms = int(now_ms if now_ms is not None else time.time() * 1000)
    if memory_enabled is None:
        enabled = bool(str(os.environ.get("AKC_WEIGHTED_MEMORY_ENABLED", "")).strip() == "1")
    else:
        enabled = bool(memory_enabled)
    if (
        memory_policy_path is not None
        or memory_budget_tokens is not None
        or (memory_pins or ())
        or (memory_boosts or {})
    ):
        enabled = True
    text = str(prompt or "").strip()
    if not text:
        return _assistant_response(
            status="no_action",
            message="Empty prompt. Enter a command, a natural-language request, or /help.",
            session=session,
        )
    session_with_user_turn = _append_turn(session=session, role="user", text=text, now_ms=ms)
    session_with_user_turn = _record_turn_memory(session=session_with_user_turn, role="user", text=text, now_ms=ms)
    if text.startswith("/"):
        response = _handle_slash_prompt(
            session=session_with_user_turn,
            prompt=text,
            approvals=approvals,
            policy_gate=policy_gate,
            principal_id=principal_id,
            execute_command=execute_command,
            now_ms=ms,
            memory_enabled=enabled,
            memory_policy_root=memory_policy_root,
            memory_policy_path=memory_policy_path,
            memory_budget_tokens=memory_budget_tokens,
            memory_pins=memory_pins,
            memory_boosts=memory_boosts,
            llm_backend=llm_backend,
            llm_mode=llm_mode,
        )
    else:
        response = _handle_text_prompt(
            session=session_with_user_turn,
            prompt=text,
            approvals=approvals,
            policy_gate=policy_gate,
            principal_id=principal_id,
            execute_command=execute_command,
            now_ms=ms,
            memory_enabled=enabled,
            memory_policy_root=memory_policy_root,
            memory_policy_path=memory_policy_path,
            memory_budget_tokens=memory_budget_tokens,
            memory_pins=memory_pins,
            memory_boosts=memory_boosts,
            llm_backend=llm_backend,
            llm_mode=llm_mode,
        )
    session_with_assistant_turn = _append_turn(
        session=response.session,
        role="assistant",
        text=response.message,
        now_ms=ms,
    )
    session_with_assistant_turn = _record_turn_memory(
        session=session_with_assistant_turn,
        role="assistant",
        text=response.message,
        now_ms=ms,
    )
    memory_trace: dict[str, JSONValue] | None = None
    if enabled:
        compacted_session, trace = _compact_session_memory(
            session=session_with_assistant_turn,
            now_ms=ms,
            policy_root=memory_policy_root,
            policy_path=memory_policy_path,
            budget_tokens=memory_budget_tokens,
            pins=memory_pins,
            boosts=memory_boosts,
        )
        session_with_assistant_turn = compacted_session
        memory_trace = trace
    return replace(response, session=session_with_assistant_turn, memory_trace=memory_trace)


def _handle_text_prompt(
    *,
    session: AssistantSession,
    prompt: str,
    approvals: ApprovalWorkflow,
    policy_gate: PolicyGate,
    principal_id: str,
    execute_command: CommandExecutor,
    now_ms: int,
    memory_enabled: bool,
    memory_policy_root: Path | None,
    memory_policy_path: str | None,
    memory_budget_tokens: int | None,
    memory_pins: Sequence[str] | None,
    memory_boosts: dict[str, float] | None,
    llm_backend: LLMBackend | None,
    llm_mode: str | None,
) -> AssistantResponse:
    suggestion = _suggest_action(
        prompt=prompt,
        scope=session.scope,
        session=session,
        memory_enabled=memory_enabled,
        memory_policy_root=memory_policy_root,
        memory_policy_path=memory_policy_path,
        memory_budget_tokens=memory_budget_tokens,
        memory_pins=memory_pins,
        memory_boosts=memory_boosts,
        now_ms=now_ms,
        llm_backend=llm_backend,
        llm_mode=llm_mode,
    )
    if suggestion.argv is None:
        return _assistant_response(
            status="no_action",
            message=suggestion.message,
            session=session,
        )
    session2 = _set_last_suggested(session=session, argv=suggestion.argv, now_ms=now_ms)
    risk = _command_risk(suggestion.argv)
    session3 = _update_scope_from_argv(session=session2, argv=suggestion.argv, now_ms=now_ms)
    if session3.mode == "plan":
        risk_msg = "mutating action" if risk == "mutating" else "read-only action"
        return _assistant_response(
            status="planned",
            message=(
                f"{suggestion.message}\n"
                f"Suggested ({risk_msg}): akc {' '.join(suggestion.argv)}\n"
                "Use /run to execute in this session."
            ),
            session=session3,
            llm_mode=llm_mode,
            suggested_command=suggestion.argv,
        )
    session4 = _record_command_memory(session=session3, argv=suggestion.argv, now_ms=now_ms)
    return _execute_or_request_approval(
        session=session4,
        argv=suggestion.argv,
        approvals=approvals,
        policy_gate=policy_gate,
        principal_id=principal_id,
        execute_command=execute_command,
        now_ms=now_ms,
    )


def _handle_slash_prompt(
    *,
    session: AssistantSession,
    prompt: str,
    approvals: ApprovalWorkflow,
    policy_gate: PolicyGate,
    principal_id: str,
    execute_command: CommandExecutor,
    now_ms: int,
    memory_enabled: bool,
    memory_policy_root: Path | None,
    memory_policy_path: str | None,
    memory_budget_tokens: int | None,
    memory_pins: Sequence[str] | None,
    memory_boosts: dict[str, float] | None,
    llm_backend: LLMBackend | None,
    llm_mode: str | None,
) -> AssistantResponse:
    try:
        parts = tuple(str(x).strip() for x in shlex.split(prompt) if str(x).strip())
    except ValueError as e:
        return _assistant_response(
            status="error",
            message=f"Invalid slash command quoting: {e}",
            session=session,
        )
    if not parts:
        return _assistant_response(status="no_action", message="Empty slash command.", session=session)
    cmd = parts[0].lower()
    args = parts[1:]
    if cmd == "/help":
        return _assistant_response(
            status="session_updated",
            message=(
                "Assistant commands:\n"
                "/help\n"
                "/plan <text>\n"
                "/run [akc <command> | <subcommand...>]\n"
                "/approve <request_id>\n"
                "/deny <request_id>\n"
                "/pin <memory_id>\n"
                "/unpin <memory_id>\n"
                "/memory\n"
                "/mode <plan|execute>\n"
                "/resume\n"
                "/exit"
            ),
            session=session,
        )
    if cmd == "/exit":
        return _assistant_response(status="exit", message="Exiting assistant session.", session=session)
    if cmd == "/resume":
        pending_ids = ", ".join(sorted(session.pending_actions)) if session.pending_actions else "(none)"
        return _assistant_response(
            status="session_updated",
            message=(f"Resumed session {session.session_id}.\nMode: {session.mode}\nPending approvals: {pending_ids}"),
            session=session,
        )
    if cmd == "/mode":
        if len(args) != 1 or args[0] not in {"plan", "execute"}:
            return _assistant_response(
                status="error",
                message="Usage: /mode <plan|execute>",
                session=session,
            )
        updated = replace(session, mode=args[0], updated_at_ms=now_ms)  # type: ignore[arg-type]
        return _assistant_response(
            status="session_updated",
            message=f"Assistant mode set to {args[0]}.",
            session=updated,
        )
    if cmd == "/plan":
        if not args:
            return _assistant_response(
                status="error",
                message="Usage: /plan <request text>",
                session=session,
            )
        as_plan = replace(session, mode="plan", updated_at_ms=now_ms)
        return _handle_text_prompt(
            session=as_plan,
            prompt=" ".join(args),
            approvals=approvals,
            policy_gate=policy_gate,
            principal_id=principal_id,
            execute_command=execute_command,
            now_ms=now_ms,
            memory_enabled=memory_enabled,
            memory_policy_root=memory_policy_root,
            memory_policy_path=memory_policy_path,
            memory_budget_tokens=memory_budget_tokens,
            memory_pins=memory_pins,
            memory_boosts=memory_boosts,
            llm_backend=llm_backend,
            llm_mode=llm_mode,
        )
    if cmd == "/run":
        if args:
            provided = args
            if provided and provided[0] == "akc":
                provided = provided[1:]
            argv = tuple(provided)
            if not argv:
                return _assistant_response(status="error", message="Usage: /run <command>", session=session)
            session2 = _set_last_suggested(session=session, argv=argv, now_ms=now_ms)
            session2 = _record_command_memory(session=session2, argv=argv, now_ms=now_ms)
            session3 = _update_scope_from_argv(session=session2, argv=argv, now_ms=now_ms)
            return _execute_or_request_approval(
                session=session3,
                argv=argv,
                approvals=approvals,
                policy_gate=policy_gate,
                principal_id=principal_id,
                execute_command=execute_command,
                now_ms=now_ms,
            )
        if session.last_suggested_command is None:
            return _assistant_response(
                status="error",
                message="No suggested command in session. Use /plan <text> first.",
                session=session,
            )
        session_cmd = _record_command_memory(session=session, argv=session.last_suggested_command, now_ms=now_ms)
        return _execute_or_request_approval(
            session=session_cmd,
            argv=session.last_suggested_command,
            approvals=approvals,
            policy_gate=policy_gate,
            principal_id=principal_id,
            execute_command=execute_command,
            now_ms=now_ms,
        )
    if cmd == "/pin":
        if len(args) != 1:
            return _assistant_response(status="error", message="Usage: /pin <memory_id>", session=session)
        key = str(args[0]).strip()
        if not key:
            return _assistant_response(status="error", message="Usage: /pin <memory_id>", session=session)
        updated = _set_memory_pin(session=session, memory_id=key, pinned=True, now_ms=now_ms)
        return _assistant_response(
            status="session_updated",
            message=f"Pinned memory entry {key}.",
            session=updated,
        )
    if cmd == "/unpin":
        if len(args) != 1:
            return _assistant_response(status="error", message="Usage: /unpin <memory_id>", session=session)
        key = str(args[0]).strip()
        if not key:
            return _assistant_response(status="error", message="Usage: /unpin <memory_id>", session=session)
        updated = _set_memory_pin(session=session, memory_id=key, pinned=False, now_ms=now_ms)
        return _assistant_response(
            status="session_updated",
            message=f"Unpinned memory entry {key}.",
            session=updated,
        )
    if cmd == "/memory":
        msg = _format_memory_view(session=session)
        return _assistant_response(
            status="session_updated",
            message=msg,
            session=session,
        )
    if cmd == "/approve":
        if len(args) != 1:
            return _assistant_response(status="error", message="Usage: /approve <request_id>", session=session)
        request_id = args[0]
        try:
            req = approvals.resolve(
                tenant_id=_approval_tenant_id(session),
                request_id=request_id,
                resolver_principal_id=principal_id,
                decision="approve",
                now_ms=now_ms,
            )
        except Exception as e:
            return _assistant_response(status="error", message=f"Approval failed: {e}", session=session)
        pending = session.pending_actions.get(req.request_id)
        if pending is None:
            return _assistant_response(
                status="error",
                message=f"Approval {req.request_id} resolved, but no pending command is associated with it.",
                session=session,
                request_id=req.request_id,
            )
        claimed = approvals.claim_execution(tenant_id=_approval_tenant_id(session), request_id=req.request_id)
        if not claimed:
            updated = _update_pending_status(
                session=session,
                request_id=req.request_id,
                status="approved",
                now_ms=now_ms,
            )
            return _assistant_response(
                status="session_updated",
                message=f"Approval {req.request_id} is already claimed or no longer executable.",
                session=updated,
                request_id=req.request_id,
            )
        return _execute_approved_pending(
            session=session,
            pending=pending,
            request_id=req.request_id,
            execute_command=execute_command,
            now_ms=now_ms,
        )
    if cmd == "/deny":
        if len(args) != 1:
            return _assistant_response(status="error", message="Usage: /deny <request_id>", session=session)
        request_id = args[0]
        try:
            req = approvals.resolve(
                tenant_id=_approval_tenant_id(session),
                request_id=request_id,
                resolver_principal_id=principal_id,
                decision="deny",
                now_ms=now_ms,
            )
        except Exception as e:
            return _assistant_response(status="error", message=f"Deny failed: {e}", session=session)
        updated = _update_pending_status(session=session, request_id=req.request_id, status="denied", now_ms=now_ms)
        return _assistant_response(
            status="session_updated",
            message=f"Denied pending action for approval request {req.request_id}.",
            session=updated,
            request_id=req.request_id,
        )
    return _assistant_response(
        status="error",
        message=f"Unknown slash command: {cmd}. Use /help.",
        session=session,
    )


def _execute_or_request_approval(
    *,
    session: AssistantSession,
    argv: tuple[str, ...],
    approvals: ApprovalWorkflow,
    policy_gate: PolicyGate,
    principal_id: str,
    execute_command: CommandExecutor,
    now_ms: int,
) -> AssistantResponse:
    risk = _command_risk(argv)
    action_id = _assistant_action_id(argv=argv, risk=risk)
    decision = _policy_decision(
        session=session,
        action_id=action_id,
        argv=argv,
        principal_id=principal_id,
        policy_gate=policy_gate,
        now_ms=now_ms,
    )
    if not decision[0]:
        return _assistant_response(
            status="error",
            message=f"Policy denied action {action_id}: {decision[1]}",
            session=session,
        )
    if risk == "mutating":
        args: dict[str, JSONValue] = {"argv": list(argv)}
        request = approvals.create_request(
            tenant_id=_approval_tenant_id(session),
            action_id=action_id,
            args_hash=stable_args_fingerprint(args),
            args=args,
            requester_principal_id=principal_id,
            now_ms=now_ms,
        )
        pending = PendingAction(
            request_id=request.request_id,
            action_id=action_id,
            argv=argv,
            risk="mutating",
            status="pending",
            created_at_ms=now_ms,
            updated_at_ms=now_ms,
        )
        pending_map = dict(session.pending_actions)
        pending_map[request.request_id] = pending
        updated = replace(session, pending_actions=pending_map, updated_at_ms=now_ms)
        return _assistant_response(
            status="approval_required",
            message=(
                f"Approval required for mutating command: akc {' '.join(argv)}\n"
                f"Request id: {request.request_id}\n"
                "Use /approve <request_id> to execute or /deny <request_id> to cancel."
            ),
            session=updated,
            request_id=request.request_id,
            suggested_command=argv,
        )
    result = execute_command(argv)
    status: Literal["executed", "error"] = "executed" if result.exit_code == 0 else "error"
    msg = f"Executed read-only command: akc {' '.join(result.argv)} (exit_code={result.exit_code})"
    return _assistant_response(
        status=status,
        message=msg,
        session=session,
        suggested_command=result.argv,
        command_exit_code=result.exit_code,
        command_stdout=result.stdout,
        command_stderr=result.stderr,
    )


def _execute_approved_pending(
    *,
    session: AssistantSession,
    pending: PendingAction,
    request_id: str,
    execute_command: CommandExecutor,
    now_ms: int,
) -> AssistantResponse:
    executed = execute_command(pending.argv)
    status: Literal["executed", "error"] = "executed" if executed.exit_code == 0 else "error"
    pending_map = dict(session.pending_actions)
    pending_map[request_id] = PendingAction(
        request_id=pending.request_id,
        action_id=pending.action_id,
        argv=pending.argv,
        risk=pending.risk,
        status="executed" if executed.exit_code == 0 else "error",
        created_at_ms=pending.created_at_ms,
        updated_at_ms=now_ms,
        command_exit_code=executed.exit_code,
        command_stdout=executed.stdout,
        command_stderr=executed.stderr,
    )
    updated = replace(session, pending_actions=pending_map, updated_at_ms=now_ms)
    return _assistant_response(
        status=status,
        message=(
            f"Executed approved command for request {request_id}: "
            f"akc {' '.join(executed.argv)} (exit_code={executed.exit_code})"
        ),
        session=updated,
        request_id=request_id,
        suggested_command=executed.argv,
        command_exit_code=executed.exit_code,
        command_stdout=executed.stdout,
        command_stderr=executed.stderr,
    )


def _policy_decision(
    *,
    session: AssistantSession,
    action_id: str,
    argv: tuple[str, ...],
    principal_id: str,
    policy_gate: PolicyGate,
    now_ms: int,
) -> tuple[bool, str]:
    ev = InboundEvent(
        channel="unknown",
        event_id=f"assistant:{session.session_id}:{now_ms}",
        principal_id=principal_id,
        tenant_id=_approval_tenant_id(session),
        raw_text=f"akc {' '.join(argv)}",
        payload_hash=stable_args_fingerprint({"argv": list(argv)}),
        received_at_ms=now_ms,
    )
    principal = Principal(principal_id=principal_id, tenant_id=_approval_tenant_id(session), roles=("operator",))
    ctx = CommandContext(event=ev, principal=principal, now_ms=now_ms)
    cmd = Command(action_id=action_id, args={"argv": list(argv)}, raw_text=ev.raw_text, parser="strict")
    decision = policy_gate.decide(ctx=ctx, cmd=cmd)
    return bool(decision.allowed), str(decision.reason)


def _assistant_action_id(*, argv: tuple[str, ...], risk: Literal["read_only", "mutating"]) -> str:
    head = str(argv[0] if argv else "unknown").strip().lower().replace("-", "_")
    prefix = "status" if risk == "read_only" else "mutate"
    return f"{prefix}.{head}"


def _approval_tenant_id(session: AssistantSession) -> str:
    tid = str(session.scope.tenant_id or "").strip()
    return tid or "local"


def _update_pending_status(
    *,
    session: AssistantSession,
    request_id: str,
    status: Literal["pending", "approved", "denied", "executed", "error"],
    now_ms: int,
) -> AssistantSession:
    req = str(request_id or "").strip()
    if req not in session.pending_actions:
        return session
    old = session.pending_actions[req]
    pending_map = dict(session.pending_actions)
    pending_map[req] = PendingAction(
        request_id=old.request_id,
        action_id=old.action_id,
        argv=old.argv,
        risk=old.risk,
        status=status,
        created_at_ms=old.created_at_ms,
        updated_at_ms=now_ms,
        command_exit_code=old.command_exit_code,
        command_stdout=old.command_stdout,
        command_stderr=old.command_stderr,
    )
    return replace(session, pending_actions=pending_map, updated_at_ms=now_ms)


def _set_last_suggested(*, session: AssistantSession, argv: tuple[str, ...], now_ms: int) -> AssistantSession:
    return replace(session, last_suggested_command=tuple(argv), updated_at_ms=now_ms)


def _append_turn(
    *,
    session: AssistantSession,
    role: Literal["user", "assistant", "system"],
    text: str,
    now_ms: int,
) -> AssistantSession:
    turn = AssistantTurn(role=role, text=text, created_at_ms=now_ms)
    turns = tuple(list(session.turns) + [turn])
    return replace(session, turns=turns, updated_at_ms=now_ms)


def _assistant_response(
    *,
    status: Literal[
        "planned",
        "approval_required",
        "executed",
        "error",
        "no_action",
        "session_updated",
        "exit",
    ],
    message: str,
    session: AssistantSession,
    suggested_command: tuple[str, ...] | None = None,
    request_id: str | None = None,
    command_exit_code: int | None = None,
    command_stdout: str | None = None,
    command_stderr: str | None = None,
    memory_trace: dict[str, JSONValue] | None = None,
    llm_mode: str | None = None,
) -> AssistantResponse:
    return AssistantResponse(
        status=status,
        message=message,
        session=session,
        llm_mode=llm_mode,
        suggested_command=suggested_command,
        request_id=request_id,
        command_exit_code=command_exit_code,
        command_stdout=command_stdout,
        command_stderr=command_stderr,
        memory_trace=memory_trace,
    )


def _update_scope_from_argv(*, session: AssistantSession, argv: tuple[str, ...], now_ms: int) -> AssistantSession:
    tenant_id = _arg_value(argv, "--tenant-id")
    repo_id = _arg_value(argv, "--repo-id")
    outputs_root = _arg_value(argv, "--outputs-root")
    scope = session.scope.normalized()
    merged = AssistantScope(
        tenant_id=tenant_id if tenant_id is not None else scope.tenant_id,
        repo_id=repo_id if repo_id is not None else scope.repo_id,
        outputs_root=outputs_root if outputs_root is not None else scope.outputs_root,
    ).normalized()
    return replace(session, scope=merged, updated_at_ms=now_ms)


def _arg_value(argv: tuple[str, ...], flag: str) -> str | None:
    f = str(flag)
    for i, tok in enumerate(argv):
        if tok == f:
            if i + 1 < len(argv):
                val = str(argv[i + 1]).strip()
                if val:
                    return val
            return None
        if tok.startswith(f + "="):
            val = tok.split("=", 1)[1].strip()
            return val or None
    return None


def _command_risk(argv: tuple[str, ...]) -> Literal["read_only", "mutating"]:
    if not argv:
        return "mutating"
    head = argv[0].strip().lower()
    if head in {"slack", "watch"}:
        return "read_only"
    if head == "drift":
        # Drift is read-only unless explicitly updating baselines on disk.
        return "mutating" if "--update-baseline" in argv else "read_only"
    if head in {"metrics", "policy"}:
        return "read_only"
    if head == "view":
        sub = argv[1].strip().lower() if len(argv) > 1 else ""
        return "read_only" if sub == "tui" else "mutating"
    if head == "runtime":
        sub = argv[1].strip().lower() if len(argv) > 1 else ""
        if sub in {"status", "events", "checkpoint", "replay", "coordination-plan"}:
            return "read_only"
        return "mutating"
    if head == "control":
        group = argv[1].strip().lower() if len(argv) > 1 else ""
        action = argv[2].strip().lower() if len(argv) > 2 else ""
        action3 = argv[3].strip().lower() if len(argv) > 3 else ""
        if group == "runs" and action in {"list", "show"}:
            return "read_only"
        if group == "replay" and action in {"plan", "forensics"}:
            return "read_only"
        if group == "manifest" and action == "diff":
            return "read_only"
        if group == "policy-bundle" and action in {"show", "effective-profile", "validate"}:
            return "read_only"
        if group == "policy-bundle" and action == "label" and action3 == "list":
            return "read_only"
        return "mutating"
    if head in {"verify"}:
        return "read_only"
    return "mutating"


def _memory_id_for_text(*, source: str, text: str) -> str:
    digest = hashlib.sha1(f"{source}\n{text}".encode()).hexdigest()[:16]
    return f"{source}:{digest}"


def _record_turn_memory(
    *,
    session: AssistantSession,
    role: Literal["user", "assistant", "system"],
    text: str,
    now_ms: int,
) -> AssistantSession:
    source: Literal["assistant_turn", "assistant_command"] = "assistant_turn"
    sid = _memory_id_for_text(source=source, text=f"{role}:{text}")
    existing = session.memory_index.get(sid)
    if existing is None:
        entry = AssistantMemoryEntry(
            memory_id=sid,
            source=source,
            text=f"{role}: {text}",
            created_at_ms=now_ms,
            last_used_at_ms=now_ms,
            use_count=1,
            pinned=sid in set(session.pin_set),
            importance=0.70 if role == "user" else 0.55,
            reliability=0.70,
            metadata={"role": role},
        )
    else:
        entry = replace(
            existing,
            last_used_at_ms=now_ms,
            use_count=int(existing.use_count) + 1,
        )
    out = dict(session.memory_index)
    out[sid] = entry
    return replace(session, memory_index=out, updated_at_ms=now_ms)


def _record_command_memory(*, session: AssistantSession, argv: tuple[str, ...], now_ms: int) -> AssistantSession:
    cmd_text = "akc " + " ".join(argv)
    sid = _memory_id_for_text(source="assistant_command", text=cmd_text)
    existing = session.memory_index.get(sid)
    if existing is None:
        entry = AssistantMemoryEntry(
            memory_id=sid,
            source="assistant_command",
            text=cmd_text,
            created_at_ms=now_ms,
            last_used_at_ms=now_ms,
            use_count=1,
            pinned=sid in set(session.pin_set),
            importance=0.95,
            reliability=1.0,
            metadata={"argv": list(argv)},
        )
    else:
        entry = replace(existing, last_used_at_ms=now_ms, use_count=int(existing.use_count) + 1)
    out = dict(session.memory_index)
    out[sid] = entry
    return replace(session, memory_index=out, updated_at_ms=now_ms)


def _set_memory_pin(*, session: AssistantSession, memory_id: str, pinned: bool, now_ms: int) -> AssistantSession:
    key = str(memory_id).strip()
    if not key:
        return session
    pin_set = set(session.pin_set)
    if pinned:
        pin_set.add(key)
    else:
        pin_set.discard(key)
    idx = dict(session.memory_index)
    existing = idx.get(key)
    if existing is not None:
        idx[key] = replace(existing, pinned=bool(pinned), last_used_at_ms=now_ms)
    return replace(session, pin_set=tuple(sorted(pin_set)), memory_index=idx, updated_at_ms=now_ms)


def _rank_session_memory(
    *,
    session: AssistantSession,
    query: str,
    now_ms: int,
    policy_root: Path | None,
    policy_path: str | None,
    budget_tokens: int | None,
    pins: Sequence[str] | None,
    boosts: dict[str, float] | None,
) -> list[Any]:
    root = policy_root if policy_root is not None else Path.cwd()
    policy = load_memory_policy(root=root, policy_path=policy_path)
    cands: list[SalienceCandidate] = []
    for entry in session.memory_index.values():
        cands.append(
            SalienceCandidate(
                stable_id=entry.memory_id,
                source=entry.source,
                text=entry.text,
                created_at_ms=entry.created_at_ms,
                last_used_at_ms=entry.last_used_at_ms,
                use_count=entry.use_count,
                pinned=entry.pinned,
                importance=entry.importance,
                reliability=entry.reliability,
                explicit_boost=entry.explicit_boost,
                metadata=entry.metadata,
            )
        )
    merged_pins = tuple(sorted(set(session.pin_set).union(set(pins or ()))))
    merged_boosts = dict(boosts or {})
    scored = score_candidates(
        candidates=cands,
        query=query,
        policy=policy,
        now_ms=now_ms,
        pins=merged_pins,
        boosts=merged_boosts,
    )
    selected, _ = pack_by_token_budget(
        scored=scored,
        budget_tokens=policy.budget_tokens(surface="assistant", runtime_override=budget_tokens),
    )
    return selected


def _compact_session_memory(
    *,
    session: AssistantSession,
    now_ms: int,
    policy_root: Path | None,
    policy_path: str | None,
    budget_tokens: int | None,
    pins: Sequence[str] | None,
    boosts: dict[str, float] | None,
) -> tuple[AssistantSession, dict[str, JSONValue]]:
    root = policy_root if policy_root is not None else Path.cwd()
    policy = load_memory_policy(root=root, policy_path=policy_path)
    cands: list[SalienceCandidate] = []
    for entry in session.memory_index.values():
        cands.append(
            SalienceCandidate(
                stable_id=entry.memory_id,
                source=entry.source,
                text=entry.text,
                created_at_ms=entry.created_at_ms,
                last_used_at_ms=entry.last_used_at_ms,
                use_count=entry.use_count,
                pinned=entry.pinned or entry.memory_id in set(session.pin_set),
                importance=entry.importance,
                reliability=entry.reliability,
                explicit_boost=entry.explicit_boost,
                metadata=entry.metadata,
            )
        )
    merged_pins = tuple(sorted(set(session.pin_set).union(set(pins or ()))))
    merged_boosts = dict(boosts or {})
    scored = score_candidates(
        candidates=cands,
        query="",
        policy=policy,
        now_ms=now_ms,
        pins=merged_pins,
        boosts=merged_boosts,
    )
    selected, evicted = pack_by_token_budget(
        scored=scored,
        budget_tokens=policy.budget_tokens(surface="assistant", runtime_override=budget_tokens),
    )
    selected_ids = {x.candidate.stable_id for x in selected}
    next_index: dict[str, AssistantMemoryEntry] = {}
    for k, v in session.memory_index.items():
        if k in selected_ids:
            next_index[k] = replace(v, pinned=(k in set(merged_pins)))
    compacted = list(session.compacted_turn_refs)
    if evicted:
        comp = build_extractive_compaction(evicted=evicted, max_items=8, max_chars_per_item=180)
        entries_raw = comp.get("entries")
        if isinstance(entries_raw, list):
            for i, row in enumerate(entries_raw):
                if not isinstance(row, dict):
                    continue
                mid = str(row.get("memory_id") or "").strip()
                summ = str(row.get("summary") or "").strip()
                if not mid or not summ:
                    continue
                compacted.append(
                    AssistantCompactedTurnRef(
                        compact_id=f"compact:{now_ms}:{i}",
                        summary=summ,
                        citation_memory_ids=(mid,),
                        created_at_ms=now_ms,
                    )
                )
    trace_ref = f"assistant_memory_trace:{session.session_id}:{now_ms}"
    trace: dict[str, JSONValue] = {
        "score_version": str(policy.score_version),
        "policy_fingerprint": policy.fingerprint(),
        "selected_ids": cast(JSONValue, sorted(selected_ids)),
        "evicted_ids": cast(JSONValue, [x.candidate.stable_id for x in evicted]),
        "budget_tokens": int(policy.budget_tokens(surface="assistant", runtime_override=budget_tokens)),
        "trace_ref": trace_ref,
    }
    updated = replace(
        session,
        memory_index=next_index,
        compacted_turn_refs=tuple(compacted[-24:]),
        pin_set=tuple(sorted(set(merged_pins))),
        last_memory_trace_ref=trace_ref,
        updated_at_ms=now_ms,
    )
    return updated, trace


def _format_memory_view(*, session: AssistantSession) -> str:
    lines: list[str] = []
    lines.append(f"Memory entries: {len(session.memory_index)}")
    for entry in sorted(session.memory_index.values(), key=lambda e: (e.source, -int(e.use_count), e.memory_id))[:12]:
        preview = entry.text[:80]
        lines.append(
            f"- {entry.memory_id} source={entry.source} pinned={entry.pinned} uses={entry.use_count} text={preview}"
        )
    lines.append(f"Compacted refs: {len(session.compacted_turn_refs)}")
    for ref in session.compacted_turn_refs[-5:]:
        lines.append(f"- {ref.compact_id} cites={list(ref.citation_memory_ids)} summary={ref.summary[:80]}")
    if session.last_memory_trace_ref is not None:
        lines.append(f"Last trace: {session.last_memory_trace_ref}")
    return "\n".join(lines)


def _apply_mode_hint_to_command(*, argv: tuple[str, ...], prompt: str) -> tuple[str, ...]:
    low = prompt.lower()
    out = list(argv)
    if "--mode" in out:
        i = out.index("--mode")
        if i + 1 < len(out):
            if "thorough" in low:
                out[i + 1] = "thorough"
            elif "quick" in low:
                out[i + 1] = "quick"
    elif "thorough" in low:
        out.extend(["--mode", "thorough"])
    elif "quick" in low:
        out.extend(["--mode", "quick"])
    return tuple(out)


def _maybe_resolve_referential_command(
    *,
    session: AssistantSession,
    prompt: str,
    memory_enabled: bool,
    memory_policy_root: Path | None,
    memory_policy_path: str | None,
    memory_budget_tokens: int | None,
    memory_pins: Sequence[str] | None,
    memory_boosts: dict[str, float] | None,
    now_ms: int,
) -> tuple[str, ...] | None:
    low = prompt.lower()
    if not any(x in low for x in ("again", "same as before", "previous", "last command", "run that")):
        return None
    if session.last_suggested_command is None and not session.memory_index:
        return None
    if session.last_suggested_command is not None:
        return _apply_mode_hint_to_command(argv=session.last_suggested_command, prompt=prompt)
    if not memory_enabled:
        return None
    scored = _rank_session_memory(
        session=session,
        query=prompt,
        now_ms=now_ms,
        policy_root=memory_policy_root,
        policy_path=memory_policy_path,
        budget_tokens=memory_budget_tokens,
        pins=memory_pins,
        boosts=memory_boosts,
    )
    for row in scored:
        entry = row.candidate
        if entry.source != "assistant_command":
            continue
        txt = str(entry.text).strip()
        if not txt.startswith("akc "):
            continue
        try:
            parsed = tuple(shlex.split(txt))[1:]
        except ValueError:
            continue
        if parsed:
            return _apply_mode_hint_to_command(argv=tuple(parsed), prompt=prompt)
    return None


def _suggest_action(
    *,
    prompt: str,
    scope: AssistantScope,
    session: AssistantSession,
    memory_enabled: bool,
    memory_policy_root: Path | None,
    memory_policy_path: str | None,
    memory_budget_tokens: int | None,
    memory_pins: Sequence[str] | None,
    memory_boosts: dict[str, float] | None,
    now_ms: int,
    llm_backend: LLMBackend | None,
    llm_mode: str | None,
) -> SuggestedAction:
    raw = str(prompt or "").strip()
    if not raw:
        return SuggestedAction(argv=None, message="Empty prompt.")
    recalled = _maybe_resolve_referential_command(
        session=session,
        prompt=raw,
        memory_enabled=memory_enabled,
        memory_policy_root=memory_policy_root,
        memory_policy_path=memory_policy_path,
        memory_budget_tokens=memory_budget_tokens,
        memory_pins=memory_pins,
        memory_boosts=memory_boosts,
        now_ms=now_ms,
    )
    if recalled is not None:
        return SuggestedAction(
            argv=recalled,
            message="Resolved command from weighted session memory.",
        )
    if raw.startswith("akc "):
        try:
            parts = tuple(shlex.split(raw))
        except ValueError as e:
            return SuggestedAction(argv=None, message=f"Invalid command quoting: {e}")
        if len(parts) < 2:
            return SuggestedAction(argv=None, message="Expected `akc <subcommand>`.")
        return SuggestedAction(
            argv=tuple(parts[1:]),
            message="Parsed explicit AKC command from prompt.",
        )

    if llm_backend is not None and str(llm_mode or "").strip() not in {"", "offline"}:
        hosted = _suggest_action_via_llm(prompt=raw, scope=scope, llm_backend=llm_backend)
        if hosted is not None:
            return hosted

    try:
        mapped = nl_fallback_parse(raw)
    except CommandClarificationRequired as e:
        choices = "\n".join(f"- akc {a.replace('.', ' ')}" for a in e.candidates)
        return SuggestedAction(argv=None, message=f"{e.message}\n{choices}")
    if mapped is not None:
        if mapped.action_id == "status.runs.list":
            cmd = _suggest_runs_list(scope=scope, limit=mapped.args.get("limit"))
            return SuggestedAction(argv=cmd, message="Mapped request to control runs list.")
        if mapped.action_id == "status.runtime":
            return SuggestedAction(
                argv=(
                    "runtime",
                    "status",
                    "--runtime-run-id",
                    "<runtime_run_id>",
                    "--outputs-root",
                    _scope_outputs_root(scope),
                ),
                message="Mapped request to runtime status (requires runtime run id).",
            )

    low = raw.lower()
    if "list" in low and ("runs" in low or "run" in low):
        return SuggestedAction(
            argv=_suggest_runs_list(scope=scope, limit=None),
            message="Heuristic mapping to control runs list.",
        )
    if "runtime" in low and "status" in low:
        return SuggestedAction(
            argv=(
                "runtime",
                "status",
                "--runtime-run-id",
                "<runtime_run_id>",
                "--outputs-root",
                _scope_outputs_root(scope),
            ),
            message="Heuristic mapping to runtime status (requires runtime run id).",
        )
    if "compile" in low:
        return SuggestedAction(
            argv=(
                "compile",
                "--tenant-id",
                _scope_tenant(scope),
                "--repo-id",
                _scope_repo(scope),
                "--outputs-root",
                _scope_outputs_root(scope),
                "--mode",
                "quick",
            ),
            message="Heuristic mapping to compile command.",
        )

    return SuggestedAction(
        argv=None,
        message=(
            "Could not map request to a canonical AKC command.\n"
            "Try `/plan <request>` for guidance or provide an explicit `akc ...` command."
        ),
    )


def _suggest_action_via_llm(
    *,
    prompt: str,
    scope: AssistantScope,
    llm_backend: LLMBackend,
) -> SuggestedAction | None:
    req = LLMRequest(
        messages=(
            LLMMessage(
                role="system",
                content=(
                    "Map the user request to one AKC CLI command only. "
                    "Return strict JSON with keys `argv` (array of CLI tokens without leading akc) and `message` "
                    "(short explanation). Never include shell syntax."
                ),
            ),
            LLMMessage(
                role="user",
                content=(
                    f"tenant_id={scope.tenant_id or ''}\n"
                    f"repo_id={scope.repo_id or ''}\n"
                    f"outputs_root={scope.outputs_root or ''}\n"
                    f"request={prompt}"
                ),
            ),
        ),
        temperature=0.0,
        max_output_tokens=512,
    )
    try:
        resp = llm_backend.complete(
            scope=TenantRepoScope(
                tenant_id=str(scope.tenant_id or "assistant"),
                repo_id=str(scope.repo_id or "assistant"),
            ),
            stage="plan",
            request=req,
        )
    except Exception as e:
        return SuggestedAction(argv=None, message=f"Hosted planner failed: {e}")
    text = str(resp.text or "").strip()
    if not text:
        return None
    try:
        obj = json.loads(text)
    except json.JSONDecodeError:
        obj = None
    if isinstance(obj, dict):
        argv_raw = obj.get("argv")
        if isinstance(argv_raw, list):
            argv = tuple(str(x).strip() for x in argv_raw if str(x).strip())
            if argv:
                return SuggestedAction(
                    argv=argv,
                    message=str(obj.get("message") or "Mapped request via hosted assistant planner."),
                )
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    for line in lines:
        if line.startswith("akc "):
            try:
                parts = tuple(shlex.split(line))
            except ValueError:
                break
            if len(parts) >= 2:
                return SuggestedAction(argv=tuple(parts[1:]), message="Mapped request via hosted assistant planner.")
    return None


def _suggest_runs_list(*, scope: AssistantScope, limit: object | None) -> tuple[str, ...]:
    cmd: list[str] = [
        "control",
        "runs",
        "list",
        "--tenant-id",
        _scope_tenant(scope),
        "--outputs-root",
        _scope_outputs_root(scope),
    ]
    if scope.repo_id is not None:
        cmd.extend(["--repo-id", scope.repo_id])
    if isinstance(limit, (int, float)):
        cmd.extend(["--limit", str(int(limit))])
    return tuple(cmd)


def _scope_tenant(scope: AssistantScope) -> str:
    val = str(scope.tenant_id or "").strip()
    return val or "<tenant_id>"


def _scope_repo(scope: AssistantScope) -> str:
    val = str(scope.repo_id or "").strip()
    return val or "<repo_id>"


def _scope_outputs_root(scope: AssistantScope) -> str:
    val = str(scope.outputs_root or "").strip()
    return val or "<outputs_root>"
