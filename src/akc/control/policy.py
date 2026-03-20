"""Default-deny tool authorization and capability-based execution."""

from __future__ import annotations

import json
import subprocess
import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, Protocol

from akc.memory.models import JSONValue, require_non_empty

if TYPE_CHECKING:
    from akc.compile.interfaces import TenantRepoScope
else:  # pragma: no cover - runtime type alias to avoid import cycles
    TenantRepoScope = Any

PolicyMode = Literal["audit_only", "enforce"]


@dataclass(frozen=True, slots=True)
class CapabilityToken:
    """Capability token granting one explicit tool action."""

    token_id: str
    tenant_id: str
    repo_id: str
    action: str
    issued_at_ms: int
    expires_at_ms: int | None = None
    constraints: dict[str, JSONValue] | None = None

    def __post_init__(self) -> None:
        require_non_empty(self.token_id, name="token_id")
        require_non_empty(self.tenant_id, name="tenant_id")
        require_non_empty(self.repo_id, name="repo_id")
        require_non_empty(self.action, name="action")
        if self.expires_at_ms is not None and int(self.expires_at_ms) <= int(self.issued_at_ms):
            raise ValueError("expires_at_ms must be > issued_at_ms when set")

    def is_expired(self, *, now_ms: int | None = None) -> bool:
        if self.expires_at_ms is None:
            return False
        effective_now = int(now_ms if now_ms is not None else time.time() * 1000)
        return effective_now >= int(self.expires_at_ms)


@dataclass(frozen=True, slots=True)
class CapabilityIssuer:
    """Issues capability tokens with no ambient authority."""

    default_ttl_ms: int | None = None

    def issue(
        self,
        *,
        scope: TenantRepoScope,
        action: str,
        constraints: dict[str, JSONValue] | None = None,
    ) -> CapabilityToken:
        require_non_empty(action, name="action")
        now_ms = int(time.time() * 1000)
        expires_at_ms = (
            now_ms + int(self.default_ttl_ms)
            if self.default_ttl_ms is not None and int(self.default_ttl_ms) > 0
            else None
        )
        return CapabilityToken(
            token_id=str(uuid.uuid4()),
            tenant_id=scope.tenant_id,
            repo_id=scope.repo_id,
            action=action.strip(),
            issued_at_ms=now_ms,
            expires_at_ms=expires_at_ms,
            constraints=constraints,
        )

    def validate(
        self,
        *,
        token: CapabilityToken,
        scope: TenantRepoScope,
        action: str,
    ) -> tuple[bool, str]:
        action2 = str(action or "").strip()
        if not action2:
            return False, "capability.action_empty"
        if token.is_expired():
            return False, "capability.expired"
        if token.tenant_id != scope.tenant_id or token.repo_id != scope.repo_id:
            return False, "capability.scope_mismatch"
        if token.action != action2:
            return False, "capability.action_mismatch"
        return True, "ok"


@dataclass(frozen=True, slots=True)
class OpaInput:
    mode: PolicyMode
    scope: TenantRepoScope
    action: str
    capability: CapabilityToken
    context: dict[str, JSONValue] | None = None


@dataclass(frozen=True, slots=True)
class PolicyDecision:
    allowed: bool
    reason: str
    mode: PolicyMode
    source: Literal["allowlist", "opa", "capability"]
    block: bool

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "allowed": bool(self.allowed),
            "reason": self.reason,
            "mode": self.mode,
            "source": self.source,
            "block": bool(self.block),
        }


class OpaEvaluator(Protocol):
    """OPA/Rego integration point.

    Implementations should evaluate Rego and return allow + reason.
    """

    def evaluate(self, *, opa_input: OpaInput) -> tuple[bool, str]: ...


@dataclass(frozen=True, slots=True)
class SubprocessOpaEvaluator:
    """Evaluate OPA decisions via the `opa` CLI.

    This is an integration point only: if OPA is unavailable or errors, we return deny.
    """

    binary: str = "opa"
    policy_path: str | None = None
    decision_path: str = "data.akc.allow"

    def _evaluate_path(self, *, payload: dict[str, Any], decision_path: str) -> tuple[bool, Any]:
        cmd = [self.binary, "eval", "--format", "json", "--fail", "--stdin-input"]
        if self.policy_path is not None and str(self.policy_path).strip():
            cmd += ["--data", str(self.policy_path).strip()]
        cmd.append(decision_path)
        cp = subprocess.run(
            cmd,
            input=json.dumps(payload),
            text=True,
            capture_output=True,
            check=False,
        )
        if int(cp.returncode) != 0:
            detail = str(cp.stderr or cp.stdout or "opa_eval_failed").strip()
            raise RuntimeError(detail)
        parsed = json.loads(cp.stdout or "{}")
        result = parsed.get("result", [{}])[0].get("expressions", [{}])[0].get("value")
        return True, result

    def _reason_path(self) -> str | None:
        if self.decision_path.endswith(".allow"):
            return f"{self.decision_path[: -len('.allow')]}.reason"
        return None

    def evaluate(self, *, opa_input: OpaInput) -> tuple[bool, str]:
        payload = {
            "mode": opa_input.mode,
            "scope": {
                "tenant_id": opa_input.scope.tenant_id,
                "repo_id": opa_input.scope.repo_id,
            },
            "action": opa_input.action,
            "capability": {
                "token_id": opa_input.capability.token_id,
                "tenant_id": opa_input.capability.tenant_id,
                "repo_id": opa_input.capability.repo_id,
                "action": opa_input.capability.action,
                "issued_at_ms": opa_input.capability.issued_at_ms,
                "expires_at_ms": opa_input.capability.expires_at_ms,
                "constraints": opa_input.capability.constraints or {},
            },
            "context": opa_input.context or {},
        }
        try:
            _, result = self._evaluate_path(payload=payload, decision_path=self.decision_path)
        except FileNotFoundError:
            return False, "policy.opa.unavailable"
        except RuntimeError as exc:
            detail = str(exc).strip()
            return False, f"policy.opa.error: {detail}" if detail else "policy.opa.error"
        try:
            if isinstance(result, dict):
                allowed = result.get("allow")
                reason = str(result.get("reason", "") or "").strip()
                if isinstance(allowed, bool):
                    if allowed:
                        return True, reason or "policy.opa.allow"
                    return False, reason or "policy.opa.deny"
            if isinstance(result, bool):
                if result:
                    return True, "policy.opa.allow"
                reason_path = self._reason_path()
                if reason_path is None:
                    return False, "policy.opa.deny"
                try:
                    _, reason_result = self._evaluate_path(
                        payload=payload,
                        decision_path=reason_path,
                    )
                except FileNotFoundError:
                    return False, "policy.opa.unavailable"
                except RuntimeError:
                    return False, "policy.opa.deny"
                if isinstance(reason_result, str) and reason_result.strip():
                    return False, reason_result.strip()
                return False, "policy.opa.deny"
        except Exception:
            return False, "policy.opa.parse_error"
        return False, "policy.opa.unknown"


@dataclass(frozen=True, slots=True)
class ToolAuthorizationPolicy:
    """Default-deny policy with explicit action allowlist."""

    mode: PolicyMode
    allow_actions: tuple[str, ...]
    opa: OpaEvaluator | None = None

    def __post_init__(self) -> None:
        if self.mode not in {"audit_only", "enforce"}:
            raise ValueError("mode must be one of: audit_only, enforce")

    def is_allowed_action(self, *, action: str) -> bool:
        action2 = str(action or "").strip()
        if not action2:
            return False
        allowed = {str(a).strip() for a in self.allow_actions if str(a).strip()}
        return action2 in allowed


@dataclass(frozen=True, slots=True)
class ToolAuthorizationRequest:
    scope: TenantRepoScope
    action: str
    capability: CapabilityToken
    context: dict[str, JSONValue] | None = None


class PolicyEngine(Protocol):
    """Policy engine interface (authorization decision + capability issuer)."""

    issuer: CapabilityIssuer

    def authorize(self, *, req: ToolAuthorizationRequest) -> PolicyDecision: ...


@dataclass(frozen=True, slots=True)
class DefaultDenyPolicyEngine(PolicyEngine):
    """Default-deny policy engine with capability validation and optional OPA."""

    issuer: CapabilityIssuer
    policy: ToolAuthorizationPolicy

    def authorize(self, *, req: ToolAuthorizationRequest) -> PolicyDecision:
        action2 = str(req.action or "").strip()
        if not action2:
            return PolicyDecision(
                allowed=False,
                reason="policy.action_empty",
                mode=self.policy.mode,
                source="allowlist",
                block=self.policy.mode == "enforce",
            )
        is_valid_cap, cap_reason = self.issuer.validate(
            token=req.capability,
            scope=req.scope,
            action=action2,
        )
        if not is_valid_cap:
            return PolicyDecision(
                allowed=False,
                reason=cap_reason,
                mode=self.policy.mode,
                source="capability",
                block=self.policy.mode == "enforce",
            )

        if not self.policy.is_allowed_action(action=action2):
            return PolicyDecision(
                allowed=False,
                reason="policy.default_deny.action_not_allowlisted",
                mode=self.policy.mode,
                source="allowlist",
                block=self.policy.mode == "enforce",
            )

        if self.policy.opa is None:
            return PolicyDecision(
                allowed=True,
                reason="policy.allowlist.allow",
                mode=self.policy.mode,
                source="allowlist",
                block=False,
            )

        opa_ok, opa_reason = self.policy.opa.evaluate(
            opa_input=OpaInput(
                mode=self.policy.mode,
                scope=req.scope,
                action=action2,
                capability=req.capability,
                context=req.context,
            )
        )
        if opa_ok:
            return PolicyDecision(
                allowed=True,
                reason=opa_reason or "policy.opa.allow",
                mode=self.policy.mode,
                source="opa",
                block=False,
            )
        return PolicyDecision(
            allowed=False,
            reason=opa_reason or "policy.opa.deny",
            mode=self.policy.mode,
            source="opa",
            block=self.policy.mode == "enforce",
        )


@dataclass(frozen=True, slots=True)
class ToolAuthorizationError(PermissionError):
    """Raised by tool wrappers when policy decisions block the requested action."""

    action: str
    decision: PolicyDecision

    def __str__(self) -> str:
        return f"tool authorization blocked action={self.action!r} reason={self.decision.reason!r}"


@dataclass(frozen=True, slots=True)
class CapabilityAttenuator:
    """Derive a narrower capability token without ambient authority.

    Attenuation is a deterministic way to:
    - add/override constraints (for policy-as-code checks), and
    - optionally shorten TTL to further reduce misuse risk.
    """

    def attenuate(
        self,
        *,
        token: CapabilityToken,
        additional_constraints: dict[str, JSONValue] | None = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
    ) -> CapabilityToken:
        effective_now = int(now_ms if now_ms is not None else time.time() * 1000)
        if token.is_expired(now_ms=effective_now):
            raise ValueError("cannot attenuate expired capability token")

        if ttl_ms is not None and int(ttl_ms) <= 0:
            raise ValueError("ttl_ms must be > 0 when set")

        base_expires = token.expires_at_ms
        new_expires = base_expires
        if ttl_ms is not None:
            candidate = effective_now + int(ttl_ms)
            new_expires = (
                candidate if base_expires is None else min(int(base_expires), int(candidate))
            )

        merged_constraints: dict[str, JSONValue] = dict(token.constraints or {})
        if additional_constraints:
            merged_constraints.update(dict(additional_constraints))
        constraints_out = merged_constraints if merged_constraints else None

        return CapabilityToken(
            token_id=str(uuid.uuid4()),
            tenant_id=token.tenant_id,
            repo_id=token.repo_id,
            action=token.action,
            issued_at_ms=effective_now,
            expires_at_ms=new_expires,
            constraints=constraints_out,
        )


def _merge_json_context(
    *,
    base: Mapping[str, JSONValue] | None,
    extra: dict[str, JSONValue] | None,
) -> dict[str, JSONValue]:
    out: dict[str, JSONValue] = {}
    if base:
        out.update({str(k): v for k, v in base.items()})
    if extra:
        out.update({str(k): v for k, v in extra.items()})
    return out


@dataclass(frozen=True, slots=True)
class PolicyWrappedLLMBackend:
    """LLM wrapper enforcing capability-based policy before `llm.complete`.

    In `audit_only` mode, a denied decision will not block the tool call.
    """

    backend: Any  # LLMBackend (kept Any to avoid import cycles/typing overhead)
    policy_engine: PolicyEngine
    issuer: CapabilityIssuer
    decision_observer: (
        Callable[[str, CapabilityToken, PolicyDecision, dict[str, JSONValue] | None], None] | None
    ) = None

    def complete(
        self,
        *,
        scope: TenantRepoScope,
        stage: Any,
        request: Any,
        token_constraints: Mapping[str, JSONValue] | None = None,
    ) -> Any:
        action = "llm.complete"
        # Context is intentionally minimal: stage + any backend-provided metadata.
        ctx: dict[str, JSONValue] = {"stage": str(stage)}
        if getattr(request, "metadata", None):
            md = request.metadata
            if isinstance(md, Mapping):
                ctx = _merge_json_context(base=md, extra=ctx)
        constraints: dict[str, JSONValue] = {"stage": str(stage)}
        if token_constraints:
            constraints.update({str(k): v for k, v in token_constraints.items()})

        token = self.issuer.issue(scope=scope, action=action, constraints=constraints)
        decision = self.policy_engine.authorize(
            req=ToolAuthorizationRequest(
                scope=scope,
                action=action,
                capability=token,
                context=ctx,
            )
        )
        if self.decision_observer is not None:
            self.decision_observer(action, token, decision, ctx)
        if bool(decision.block):
            raise ToolAuthorizationError(action=action, decision=decision)
        return self.backend.complete(scope=scope, stage=stage, request=request)


@dataclass(frozen=True, slots=True)
class PolicyWrappedExecutor:
    """Executor wrapper enforcing capability-based policy before `executor.run`.

    The underlying `Executor` protocol does not include a `stage` parameter, so this
    wrapper requires `stage` explicitly at call-time (`run_with_stage`).
    """

    executor: Any  # Executor
    policy_engine: PolicyEngine
    issuer: CapabilityIssuer
    attenuator: CapabilityAttenuator | None = None
    decision_observer: (
        Callable[[str, CapabilityToken, PolicyDecision, dict[str, JSONValue] | None], None] | None
    ) = None

    def run_with_stage(
        self,
        *,
        scope: TenantRepoScope,
        stage: str,
        request: Any,
        context: Mapping[str, JSONValue] | None = None,
        ttl_ms: int | None = None,
        base_capability: CapabilityToken | None = None,
        token_constraints: Mapping[str, JSONValue] | None = None,
    ) -> Any:
        action = "executor.run"
        stage_s = str(stage)
        ctx = _merge_json_context(base=context, extra={"stage": stage_s})
        constraints: dict[str, JSONValue] = dict(ctx)
        if token_constraints:
            constraints.update({str(k): v for k, v in token_constraints.items()})

        if base_capability is not None:
            # Derive a narrower token instead of issuing a new one.
            if base_capability.action != action:
                raise ValueError("base_capability.action must match executor.run")
            token = (
                self.attenuator.attenuate(
                    token=base_capability,
                    additional_constraints=constraints,
                    ttl_ms=ttl_ms,
                )
                if self.attenuator is not None
                else base_capability
            )
        else:
            token = self.issuer.issue(scope=scope, action=action, constraints=constraints)

        decision = self.policy_engine.authorize(
            req=ToolAuthorizationRequest(
                scope=scope,
                action=action,
                capability=token,
                context=ctx,
            )
        )
        if self.decision_observer is not None:
            self.decision_observer(action, token, decision, ctx)
        if bool(decision.block):
            raise ToolAuthorizationError(action=action, decision=decision)
        return self.executor.run(scope=scope, request=request)
