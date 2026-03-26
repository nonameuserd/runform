from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any

from akc.control_bot.command_engine import Command, CommandContext, PolicyDecision
from akc.memory.models import JSONValue


class PolicyGateError(Exception):
    """Raised when policy evaluation fails in enforce mode."""


@dataclass(frozen=True, slots=True)
class RoleAllowlistRule:
    role: str
    patterns: tuple[str, ...]

    def allows(self, *, action_id: str) -> bool:
        aid = str(action_id or "").strip().lower()
        for pat in self.patterns:
            p = str(pat or "").strip().lower()
            if not p:
                continue
            if p.endswith(".*"):
                pref = p.removesuffix(".*") + "."
                if aid.startswith(pref):
                    return True
                continue
            if aid == p:
                return True
        return False


def build_role_allowlist(rules: dict[str, Any] | None) -> tuple[RoleAllowlistRule, ...]:
    if not rules:
        return ()
    out: list[RoleAllowlistRule] = []
    if not isinstance(rules, dict):
        raise ValueError("policy.role_allowlist must be an object mapping role -> [action_patterns]")
    for role, patterns in rules.items():
        r = str(role or "").strip()
        if not r:
            raise ValueError("policy.role_allowlist has an empty role key")
        if not isinstance(patterns, list):
            raise ValueError(f"policy.role_allowlist[{r}] must be an array of strings")
        pats: list[str] = []
        for p in patterns:
            if not isinstance(p, str) or not p.strip():
                raise ValueError(f"policy.role_allowlist[{r}] contains an empty pattern")
            pats.append(p.strip())
        out.append(RoleAllowlistRule(role=r, patterns=tuple(pats)))
    return tuple(out)


def _extract_dot_path(obj: Any, dot_path: str) -> Any:
    cur: Any = obj
    for part in str(dot_path or "").strip().split(".") if str(dot_path or "").strip() else []:
        if part in {"", "$"}:
            continue
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


@dataclass(frozen=True, slots=True)
class OPAConfig:
    url: str
    decision_path: str = "data.akc.allow"
    timeout_ms: int = 1500


@dataclass(frozen=True, slots=True)
class OPAClient:
    cfg: OPAConfig

    def decide(self, *, ctx: CommandContext, cmd: Command) -> PolicyDecision:
        url = str(self.cfg.url or "").strip()
        if not url:
            raise PolicyGateError("OPA enabled but policy URL is empty")

        payload: dict[str, Any] = {
            "input": {
                "tenant_id": ctx.principal.tenant_id,
                "principal_id": ctx.principal.principal_id,
                "roles": list(ctx.principal.roles),
                "action_id": cmd.action_id,
                "args": cmd.args,
                "channel": ctx.event.channel,
                "event_id": ctx.event.event_id,
            }
        }
        data = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            url=url,
            method="POST",
            headers={"Content-Type": "application/json"},
            data=data,
        )
        timeout_s = max(0.1, float(int(self.cfg.timeout_ms)) / 1000.0)
        try:
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:  # noqa: S310
                raw = resp.read()
        except (urllib.error.URLError, TimeoutError) as e:
            raise PolicyGateError(f"OPA request failed: {e}") from e
        try:
            parsed = json.loads(raw.decode("utf-8") or "{}")
        except Exception as e:
            raise PolicyGateError("OPA returned invalid JSON") from e

        val = _extract_dot_path(parsed, self.cfg.decision_path)
        if isinstance(val, bool):
            return PolicyDecision(allowed=val, reason=f"opa:{self.cfg.decision_path}={val}")
        if isinstance(val, (int, float)) and val in (0, 1):
            return PolicyDecision(allowed=bool(int(val)), reason=f"opa:{self.cfg.decision_path}={bool(int(val))}")
        raise PolicyGateError(f"OPA decision_path did not resolve to boolean: {self.cfg.decision_path}")


@dataclass(frozen=True, slots=True)
class PolicyGate:
    mode: str  # "audit_only" | "enforce"
    role_allowlist: tuple[RoleAllowlistRule, ...] = ()
    opa: OPAClient | None = None

    def decide(self, *, ctx: CommandContext, cmd: Command) -> PolicyDecision:
        roles = {r.strip().lower() for r in (ctx.principal.roles or ()) if str(r or "").strip()}
        allowlisted = False
        allow_reason = "not_allowlisted"
        for rule in self.role_allowlist:
            if rule.role.strip().lower() not in roles:
                continue
            if rule.allows(action_id=cmd.action_id):
                allowlisted = True
                allow_reason = f"role_allowlist:{rule.role}"
                break

        if not allowlisted:
            if self.mode == "audit_only":
                return PolicyDecision(allowed=True, reason=f"audit_only would_deny:{allow_reason}")
            return PolicyDecision(allowed=False, reason=allow_reason)

        if self.opa is not None:
            try:
                opa_decision = self.opa.decide(ctx=ctx, cmd=cmd)
            except PolicyGateError as e:
                if self.mode == "audit_only":
                    return PolicyDecision(allowed=True, reason=f"audit_only opa_error:{e}")
                return PolicyDecision(allowed=False, reason=str(e))
            if not opa_decision.allowed:
                if self.mode == "audit_only":
                    return PolicyDecision(allowed=True, reason=f"audit_only would_deny:{opa_decision.reason}")
                return PolicyDecision(allowed=False, reason=opa_decision.reason)

        return PolicyDecision(allowed=True, reason=allow_reason)


def policy_input_tenant_id(ctx: CommandContext) -> str:
    # Small helper to keep tenant isolation explicit in policy call sites.
    return str(ctx.principal.tenant_id or "").strip()


def _json_sanitize_args(args: dict[str, JSONValue]) -> dict[str, JSONValue]:
    # v1: args are already JSONValue-typed; keep as-is but ensure dict copy.
    return dict(args or {})
