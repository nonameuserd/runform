from __future__ import annotations

import json
import shlex
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Literal, TypeAlias

from akc.memory.models import JSONValue, require_non_empty

ActionId: TypeAlias = str
ChannelId: TypeAlias = Literal["slack", "discord", "telegram", "whatsapp", "unknown"]


class CommandEngineError(Exception):
    """Raised when an inbound event cannot be parsed or executed."""


class CommandClarificationRequired(CommandEngineError):
    """Raised when NL parsing is ambiguous and requires user clarification."""

    def __init__(self, *, message: str, candidates: tuple[str, ...]) -> None:
        super().__init__(message)
        self.message = message
        self.candidates = candidates


class PolicyDenied(CommandEngineError):
    """Raised when an action is denied by policy."""


class UnknownAction(CommandEngineError):
    """Raised when no action handler exists for the requested action."""


@dataclass(frozen=True, slots=True)
class InboundEvent:
    """Canonical inbound envelope for all channels."""

    channel: ChannelId
    event_id: str
    principal_id: str
    tenant_id: str
    raw_text: str
    payload_hash: str
    received_at_ms: int

    def normalized(self) -> InboundEvent:
        return InboundEvent(
            channel=self.channel,
            event_id=self.event_id.strip(),
            principal_id=self.principal_id.strip(),
            tenant_id=self.tenant_id.strip(),
            raw_text=self.raw_text.strip(),
            payload_hash=self.payload_hash.strip().lower(),
            received_at_ms=int(self.received_at_ms),
        )

    def validate(self) -> None:
        require_non_empty(self.event_id, name="event_id")
        require_non_empty(self.principal_id, name="principal_id")
        require_non_empty(self.tenant_id, name="tenant_id")
        require_non_empty(self.raw_text, name="raw_text")
        require_non_empty(self.payload_hash, name="payload_hash")


@dataclass(frozen=True, slots=True)
class Principal:
    principal_id: str
    tenant_id: str
    roles: tuple[str, ...] = ()

    def validate(self) -> None:
        require_non_empty(self.principal_id, name="principal_id")
        require_non_empty(self.tenant_id, name="tenant_id")


@dataclass(frozen=True, slots=True)
class Command:
    action_id: ActionId
    args: dict[str, JSONValue]
    raw_text: str
    parser: Literal["strict", "nl_fallback"]

    def validate(self) -> None:
        require_non_empty(self.action_id, name="action_id")
        require_non_empty(self.raw_text, name="raw_text")


@dataclass(frozen=True, slots=True)
class CommandContext:
    event: InboundEvent
    principal: Principal
    now_ms: int

    def validate(self) -> None:
        self.event.validate()
        self.principal.validate()
        if self.event.tenant_id.strip() != self.principal.tenant_id.strip():
            raise ValueError("tenant isolation violated: event.tenant_id != principal.tenant_id")


@dataclass(frozen=True, slots=True)
class CommandResult:
    ok: bool
    action_id: ActionId
    message: str
    data: dict[str, JSONValue] | None = None
    request_id: str | None = None
    status: Literal["executed", "denied", "approval_required", "clarification_required", "error"] = "executed"


ActionHandler: TypeAlias = Callable[[CommandContext, Mapping[str, JSONValue]], CommandResult]


@dataclass(slots=True)
class ActionRegistry:
    handlers: dict[ActionId, ActionHandler]

    @staticmethod
    def default_v1() -> ActionRegistry:
        def _status_runtime(ctx: CommandContext, _args: Mapping[str, JSONValue]) -> CommandResult:
            return CommandResult(
                ok=True,
                action_id="status.runtime",
                message=f"runtime ok (tenant={ctx.principal.tenant_id})",
                data={"tenant_id": ctx.principal.tenant_id},
                status="executed",
            )

        def _status_runs_list(ctx: CommandContext, args: Mapping[str, JSONValue]) -> CommandResult:
            lim_raw = args.get("limit")
            lim = 10
            if isinstance(lim_raw, (int, float)):
                lim = max(1, min(int(lim_raw), 50))
            return CommandResult(
                ok=True,
                action_id="status.runs.list",
                message=f"runs: (stub) returning up to {lim}",
                data={"limit": lim, "tenant_id": ctx.principal.tenant_id},
                status="executed",
            )

        return ActionRegistry(
            handlers={
                "status.runtime": _status_runtime,
                "status.runs.list": _status_runs_list,
            }
        )

    def get(self, action_id: ActionId) -> ActionHandler | None:
        return self.handlers.get(action_id)


@dataclass(frozen=True, slots=True)
class PolicyDecision:
    allowed: bool
    reason: str


def strict_parse_command(text: str) -> Command | None:
    """Parse strict grammar: `akc <group> <verb> [<subverb> ...] [args...]`.

    Returns None when the text doesn't match strict grammar.

    Deterministic rules:
    - Tokenization uses shell-style quoting (`shlex.split`) so channel adapters can pass raw text.
    - The action id is the dot-join of all contiguous "action words" after `akc` until args begin.
      - Example: `akc status runs list` -> action_id `status.runs.list`
      - Example: `akc mutate runtime stop` -> action_id `mutate.runtime.stop`
    - Args begin at the first token that looks like a key/value or flag:
      - `k=v`
      - `--k=v`
      - `--k v`
      - `--flag` (boolean True)
    - Unknown/extra positional tokens after args begin are rejected (strict, fail-closed).
    """
    raw = str(text or "").strip()
    if not raw:
        return None
    try:
        parts = shlex.split(raw, posix=True)
    except Exception:
        return None
    if len(parts) < 3:  # akc + group + verb
        return None
    if parts[0].lower() != "akc":
        return None
    rest = [p.strip() for p in parts[1:] if str(p or "").strip()]
    if len(rest) < 2:
        return None

    def _is_arg_token(tok: str) -> bool:
        t = str(tok or "").strip()
        if not t:
            return False
        if t.startswith("--"):
            return True
        return "=" in t

    def _normalize_key(k: str) -> str:
        kk = str(k or "").strip().lower().replace("-", "_")
        if not kk:
            return ""
        # strict key allowlist (simple, deterministic)
        for ch in kk:
            if not (ch.isalnum() or ch == "_"):
                return ""
        return kk

    def _parse_value(v: str) -> JSONValue:
        s = str(v or "").strip()
        if not s:
            return ""
        low = s.lower()
        if low in {"true", "false"}:
            return low == "true"
        if low in {"null", "none"}:
            return None
        # int / float (deterministic)
        try:
            if "." in s:
                return float(s)
            return int(s)
        except Exception:
            pass
        # JSON literals for structured args (must be full token)
        if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
            try:
                parsed = json.loads(s)
                if parsed is None or isinstance(parsed, (str, int, float, bool, list, dict)):
                    return parsed
            except Exception:
                pass
        return s

    action_words: list[str] = []
    i = 0
    while i < len(rest):
        if _is_arg_token(rest[i]):
            break
        w = rest[i].strip().lower()
        if not w:
            return None
        action_words.append(w)
        i += 1

    if len(action_words) < 2:
        return None
    action_id = ".".join(action_words)

    args: dict[str, JSONValue] = {}
    j = i
    while j < len(rest):
        tok = rest[j]
        if tok.startswith("--"):
            t = tok[2:]
            if "=" in t:
                k_raw, v_raw = t.split("=", 1)
                key = _normalize_key(k_raw)
                if not key:
                    return None
                args[key] = _parse_value(v_raw)
                j += 1
                continue
            key = _normalize_key(t)
            if not key:
                return None
            # `--flag` boolean true unless explicit value follows.
            if j + 1 < len(rest) and not _is_arg_token(rest[j + 1]):
                args[key] = _parse_value(rest[j + 1])
                j += 2
                continue
            args[key] = True
            j += 1
            continue
        if "=" in tok:
            k_raw, v_raw = tok.split("=", 1)
            key = _normalize_key(k_raw)
            if not key:
                return None
            args[key] = _parse_value(v_raw)
            j += 1
            continue
        # strict: once args begin, positional tokens are not allowed
        return None

    return Command(action_id=action_id, args=args, raw_text=raw, parser="strict")


def nl_fallback_parse(text: str) -> Command | None:
    """Deterministic NL intent mapping (Phase B).

    This parser is intentionally non-LLM and deterministic. It maps natural language
    operator intents to the same canonical `Command` objects produced by strict parsing.

    Ambiguous intents raise `CommandClarificationRequired` with a stable prompt and a
    short list of canonical candidates.
    """
    raw = str(text or "").strip()
    if not raw:
        return None

    import re

    s = raw.lower().strip()
    s = re.sub(r"[^\w\s=:-]+", " ", s)  # drop punctuation, keep k=v-ish tokens
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return None
    if s.startswith("akc "):
        s = s[4:].strip()

    tokens = tuple(t for t in s.split(" ") if t)
    tset = set(tokens)

    def _extract_limit() -> int | None:
        # Deterministic, bounded extraction: "top 5", "last 10", "limit=7", "limit 7"
        m = re.search(r"(?:\blimit\s*=\s*|\blimit\s+)(\d{1,3})\b", s)
        if not m:
            m = re.search(r"\b(?:top|last)\s+(\d{1,3})\b", s)
        if not m:
            return None
        try:
            n = int(m.group(1))
        except Exception:
            return None
        return max(1, min(n, 50))

    # Candidate scoring (simple, deterministic).
    # Higher score wins; ties trigger clarification.
    candidates: list[tuple[int, str, dict[str, JSONValue]]] = []

    # status.runtime intent
    score_runtime = 0
    if "runtime" in tset:
        score_runtime += 3
    if "status" in tset:
        score_runtime += 1
    if tokens in {("status",), ("runtime",), ("status", "runtime"), ("runtime", "status")}:
        score_runtime += 2
    if score_runtime > 0:
        candidates.append((score_runtime, "status.runtime", {}))

    # status.runs.list intent
    score_runs_list = 0
    if "runs" in tset or "run" in tset:
        score_runs_list += 2
    if "list" in tset or "show" in tset or "recent" in tset:
        score_runs_list += 1
    if ("list" in tset and ("runs" in tset or "run" in tset)) or ("runs" in tset and "recent" in tset):
        score_runs_list += 2
    lim = _extract_limit()
    args_runs_list: dict[str, JSONValue] = {}
    if lim is not None:
        args_runs_list["limit"] = lim
        score_runs_list += 1
    if score_runs_list > 0:
        candidates.append((score_runs_list, "status.runs.list", args_runs_list))

    if not candidates:
        return None

    candidates.sort(key=lambda x: (-int(x[0]), x[1]))
    best_score = candidates[0][0]
    best = [c for c in candidates if c[0] == best_score]
    if len(best) > 1:
        actions = tuple(c[1] for c in best)
        prompt = "Ambiguous command. Did you mean one of:\n" + "\n".join(
            f"- akc {a.replace('.', ' ')}" for a in actions
        )
        raise CommandClarificationRequired(message=prompt, candidates=actions)

    _score, action_id, args = best[0]
    return Command(action_id=action_id, args=args, raw_text=raw, parser="nl_fallback")


@dataclass(slots=True)
class CommandEngine:
    registry: ActionRegistry
    # Policy is evaluated for every action and should be fail-closed.
    policy_decide: Callable[[CommandContext, Command], PolicyDecision] | None = None

    def parse(self, text: str) -> Command:
        # Phase A: strict grammar always wins, and is fail-closed when the user invoked `akc ...`.
        cmd = strict_parse_command(text)
        if cmd is not None:
            cmd.validate()
            return cmd

        raw = str(text or "").strip()
        if raw:
            try:
                parts = shlex.split(raw, posix=True)
            except Exception:
                parts = []
            if parts and str(parts[0] or "").strip().lower() == "akc":
                raise CommandEngineError("invalid strict command syntax after `akc`")

        # Phase B: deterministic NL intent mapping (only when strict grammar did not apply).
        fallback = nl_fallback_parse(text)
        if fallback is not None:
            fallback.validate()
            return fallback
        raise CommandEngineError("could not parse command (strict + fallback)")

    def execute(self, *, ctx: CommandContext, cmd: Command) -> CommandResult:
        ctx.validate()
        cmd.validate()
        if self.policy_decide is not None:
            decision = self.policy_decide(ctx, cmd)
            if not decision.allowed:
                raise PolicyDenied(f"policy denied {cmd.action_id}: {decision.reason}")
        handler = self.registry.get(cmd.action_id)
        if handler is None:
            raise UnknownAction(cmd.action_id)
        return handler(ctx, cmd.args)


def default_now_ms() -> int:
    return int(time.time() * 1000)
