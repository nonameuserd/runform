from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal, Protocol

from akc.action.models import ActionPlanStepV1
from akc.memory.models import normalize_repo_id, normalize_tenant_id

ProviderErrorKind = Literal[
    "retriable_transport",
    "non_retriable_business",
    "non_retriable_provider",
]
CompensationSupport = Literal["reversal", "manual_only"]


@dataclass(frozen=True, slots=True)
class ActionProviderExecutionContext:
    intent_id: str
    tenant_id: str
    repo_id: str
    idempotency_key: str
    mode: Literal["live", "simulate"] = "live"


@dataclass(frozen=True, slots=True)
class ActionProviderCompensationContext:
    intent_id: str
    tenant_id: str
    repo_id: str
    failed_step_id: str


@dataclass(frozen=True, slots=True)
class ProviderExecutionResult:
    status: str
    payload: dict[str, object]
    external_id: str | None = None


class ActionProviderAdapter(Protocol):
    def preflight(self, scope: dict[str, str]) -> None: ...

    def execute(
        self,
        step: ActionPlanStepV1,
        context: ActionProviderExecutionContext,
    ) -> ProviderExecutionResult: ...

    def compensate(
        self,
        step: ActionPlanStepV1,
        context: ActionProviderCompensationContext,
    ) -> ProviderExecutionResult: ...

    def classify_error(self, error: Exception) -> ProviderErrorKind: ...

    def compensation_support(self, step: ActionPlanStepV1) -> CompensationSupport: ...


def _env_truthy(name: str, *, default: bool = False) -> bool:
    raw = str(os.environ.get(name, "") or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _normalize_scope(tenant_id: str, repo_id: str) -> tuple[str, str]:
    return normalize_tenant_id(tenant_id), normalize_repo_id(repo_id)


def _short_external_id(prefix: str, *parts: str) -> str:
    digest = sha256("::".join(parts).encode("utf-8")).hexdigest()[:16]
    return f"{prefix}_{digest}"


def _classify_exception(error: Exception) -> ProviderErrorKind:
    lowered = str(error).lower()
    if any(token in lowered for token in ("timeout", "temporar", "429", "502", "503", "504")):
        return "retriable_transport"
    if any(token in lowered for token in ("invalid", "closed", "conflict", "already", "business", "forbidden")):
        return "non_retriable_business"
    return "non_retriable_provider"


def _set_private_mode(path: Path, *, mode: int) -> None:
    if os.name != "posix":
        return
    try:
        path.chmod(mode)
    except OSError:
        return


class OAuthTokenCache:
    """Tenant/repo scoped token cache under `.akc/oauth` with restrictive file permissions."""

    def __init__(self, *, base_dir: Path | None = None) -> None:
        workspace_root = (base_dir or Path.cwd()).resolve()
        self._root = workspace_root / ".akc" / "oauth"

    def token_path(self, *, provider: str, context: ActionProviderExecutionContext) -> Path:
        tenant_norm, repo_norm = _normalize_scope(context.tenant_id, context.repo_id)
        provider_norm = provider.strip().lower()
        if not provider_norm:
            raise ValueError("provider must be non-empty")
        return self._root / tenant_norm / repo_norm / f"{provider_norm}.token.json"

    def load(self, *, provider: str, context: ActionProviderExecutionContext) -> dict[str, Any] | None:
        path = self.token_path(provider=provider, context=context)
        if not path.exists():
            return None
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        if not isinstance(loaded, dict):
            return None
        tenant_norm, repo_norm = _normalize_scope(context.tenant_id, context.repo_id)
        if str(loaded.get("tenant_id", "")) != tenant_norm:
            return None
        if str(loaded.get("repo_id", "")) != repo_norm:
            return None
        expires_raw = loaded.get("expires_at_ms")
        if isinstance(expires_raw, int) and expires_raw > 0 and expires_raw <= int(time.time() * 1000):
            return None
        return loaded

    def store(
        self,
        *,
        provider: str,
        context: ActionProviderExecutionContext,
        token_payload: dict[str, Any],
    ) -> Path:
        path = self.token_path(provider=provider, context=context)
        path.parent.mkdir(parents=True, exist_ok=True)
        _set_private_mode(path.parent, mode=0o700)
        payload = dict(token_payload)
        tenant_norm, repo_norm = _normalize_scope(context.tenant_id, context.repo_id)
        payload["tenant_id"] = tenant_norm
        payload["repo_id"] = repo_norm
        payload["stored_at_ms"] = int(time.time() * 1000)
        tmp = path.with_suffix(path.suffix + ".tmp")
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, sort_keys=True, ensure_ascii=False, indent=2)
        tmp.replace(path)
        _set_private_mode(path, mode=0o600)
        return path


class _ScopedProviderBase:
    def __init__(self, *, oauth_cache: OAuthTokenCache | None = None, require_env_secrets: bool | None = None) -> None:
        self._oauth_cache = oauth_cache or OAuthTokenCache()
        self._require_env_secrets = (
            _env_truthy("AKC_ACTION_REQUIRE_PROVIDER_SECRETS", default=False)
            if require_env_secrets is None
            else bool(require_env_secrets)
        )

    def preflight(self, scope: dict[str, str]) -> None:
        tenant_norm, repo_norm = _normalize_scope(
            str(scope.get("tenant_id", "")),
            str(scope.get("repo_id", "")),
        )
        scope["tenant_id"] = tenant_norm
        scope["repo_id"] = repo_norm

    def classify_error(self, error: Exception) -> ProviderErrorKind:
        return _classify_exception(error)

    def compensation_support(self, step: ActionPlanStepV1) -> CompensationSupport:
        _ = step
        return "manual_only"

    def _require_scoped_inputs(self, *, step: ActionPlanStepV1, context: ActionProviderExecutionContext) -> None:
        tenant_raw = step.inputs.get("tenant_id")
        repo_raw = step.inputs.get("repo_id")
        if tenant_raw is not None and str(tenant_raw).strip() and str(tenant_raw).strip() != context.tenant_id:
            raise ValueError("step input tenant_id does not match execution context tenant_id")
        if repo_raw is not None and str(repo_raw).strip() and str(repo_raw).strip() != context.repo_id:
            raise ValueError("step input repo_id does not match execution context repo_id")

    def _require_env(self, name: str) -> str | None:
        value = str(os.environ.get(name, "") or "").strip()
        if value:
            return value
        if self._require_env_secrets:
            raise RuntimeError(f"missing required provider secret: {name}")
        return None


class GoogleAdapter(_ScopedProviderBase):
    def _access_token(self, *, context: ActionProviderExecutionContext) -> str | None:
        from_env = self._require_env("AKC_GOOGLE_OAUTH_ACCESS_TOKEN")
        if from_env:
            self._oauth_cache.store(
                provider="google",
                context=context,
                token_payload={"access_token": from_env, "source": "env"},
            )
            return from_env
        cached = self._oauth_cache.load(provider="google", context=context)
        if isinstance(cached, dict):
            token = cached.get("access_token")
            if isinstance(token, str) and token.strip():
                return token.strip()
        return None

    def execute(self, step: ActionPlanStepV1, context: ActionProviderExecutionContext) -> ProviderExecutionResult:
        self._require_scoped_inputs(step=step, context=context)
        token = self._access_token(context=context)
        auth_mode = "env_or_cache_oauth" if token else "local_stub"
        query_hint = str(
            step.inputs.get("query") or step.inputs.get("contact_hint") or step.inputs.get("goal") or ""
        ).strip()
        if step.action_type == "action.contact.lookup":
            payload: dict[str, object] = {
                "operation": "people.connections.list",
                "query": query_hint,
                "matches": [{"display_name": query_hint or "unknown", "phone": "+14155550123"}],
                "auth_mode": auth_mode,
            }
            return ProviderExecutionResult(
                status="ok",
                payload=payload,
                external_id=_short_external_id(
                    "google_contact",
                    context.intent_id,
                    step.step_id,
                    context.idempotency_key,
                ),
            )
        if step.action_type == "action.calendar.read":
            payload = {
                "operation": "freeBusy.query",
                "time_range": step.inputs.get("time_range", {}),
                "busy": [],
                "auth_mode": auth_mode,
            }
            return ProviderExecutionResult(
                status="ok",
                payload=payload,
                external_id=_short_external_id(
                    "google_freebusy",
                    context.intent_id,
                    step.step_id,
                    context.idempotency_key,
                ),
            )
        if step.action_type == "action.calendar.write":
            payload = {
                "operation": "events.insert",
                "event": step.inputs.get("event", {}),
                "calendar_id": step.inputs.get("calendar_id", "primary"),
                "auth_mode": auth_mode,
            }
            return ProviderExecutionResult(
                status="ok",
                payload=payload,
                external_id=_short_external_id(
                    "google_event",
                    context.intent_id,
                    step.step_id,
                    context.idempotency_key,
                ),
            )
        raise ValueError(f"google adapter does not support action_type: {step.action_type}")

    def compensate(
        self,
        step: ActionPlanStepV1,
        context: ActionProviderCompensationContext,
    ) -> ProviderExecutionResult:
        if step.action_type == "action.calendar.write":
            payload: dict[str, object] = {
                "operation": "events.delete",
                "calendar_id": step.inputs.get("calendar_id", "primary"),
                "failed_step_id": context.failed_step_id,
            }
            return ProviderExecutionResult(status="ok", payload=payload)
        return ProviderExecutionResult(
            status="ok",
            payload={"operation": "manual", "reason": "no reversal available"},
        )

    def compensation_support(self, step: ActionPlanStepV1) -> CompensationSupport:
        if step.action_type == "action.calendar.write":
            return "reversal"
        return "manual_only"


class TwilioAdapter(_ScopedProviderBase):
    def execute(self, step: ActionPlanStepV1, context: ActionProviderExecutionContext) -> ProviderExecutionResult:
        self._require_scoped_inputs(step=step, context=context)
        account_sid = self._require_env("AKC_TWILIO_ACCOUNT_SID")
        auth_token = self._require_env("AKC_TWILIO_AUTH_TOKEN")
        from_number = self._require_env("AKC_TWILIO_FROM_NUMBER") or "+10000000000"
        auth_mode = "env" if account_sid and auth_token else "local_stub"
        if step.action_type == "action.call.place":
            payload: dict[str, object] = {
                "operation": "calls.create",
                "request": {
                    "to": step.inputs.get("to"),
                    "from": step.inputs.get("from") or from_number,
                    "message": step.inputs.get("message") or "Automated call initiated by AKC action plane.",
                },
                "account_sid_present": bool(account_sid),
                "auth_mode": auth_mode,
            }
            return ProviderExecutionResult(
                status="ok",
                payload=payload,
                external_id=_short_external_id("twilio_call", context.intent_id, step.step_id, context.idempotency_key),
            )
        if step.action_type == "action.message.send":
            payload = {
                "operation": "messages.create",
                "request": {
                    "to": step.inputs.get("to"),
                    "from": step.inputs.get("from") or from_number,
                    "body": step.inputs.get("body") or step.inputs.get("message") or "",
                },
                "account_sid_present": bool(account_sid),
                "auth_mode": auth_mode,
            }
            return ProviderExecutionResult(
                status="ok",
                payload=payload,
                external_id=_short_external_id("twilio_msg", context.intent_id, step.step_id, context.idempotency_key),
            )
        raise ValueError(f"twilio adapter does not support action_type: {step.action_type}")

    def compensate(
        self,
        step: ActionPlanStepV1,
        context: ActionProviderCompensationContext,
    ) -> ProviderExecutionResult:
        _ = (step, context)
        return ProviderExecutionResult(
            status="ok",
            payload={"operation": "manual", "note": "Twilio calls/messages are not reversible in v1"},
        )


class AmadeusAdapter(_ScopedProviderBase):
    def _access_token(self, *, context: ActionProviderExecutionContext) -> str | None:
        from_env = self._require_env("AKC_AMADEUS_ACCESS_TOKEN")
        if from_env:
            self._oauth_cache.store(
                provider="amadeus",
                context=context,
                token_payload={"access_token": from_env, "source": "env"},
            )
            return from_env
        cached = self._oauth_cache.load(provider="amadeus", context=context)
        if isinstance(cached, dict):
            token = cached.get("access_token")
            if isinstance(token, str) and token.strip():
                return token.strip()
        return None

    def execute(self, step: ActionPlanStepV1, context: ActionProviderExecutionContext) -> ProviderExecutionResult:
        self._require_scoped_inputs(step=step, context=context)
        token = self._access_token(context=context)
        auth_mode = "env_or_cache_oauth" if token else "local_stub"
        if step.action_type == "action.flight.search":
            payload: dict[str, object] = {
                "operation": "flight-offers-search",
                "origin": step.inputs.get("origin"),
                "destination": step.inputs.get("destination"),
                "departure_date": step.inputs.get("departure_date"),
                "offers": [
                    {"offer_id": "offer_demo_1", "price": "420.00", "currency": "USD"},
                ],
                "auth_mode": auth_mode,
            }
            return ProviderExecutionResult(
                status="ok",
                payload=payload,
                external_id=_short_external_id(
                    "amadeus_search",
                    context.intent_id,
                    step.step_id,
                    context.idempotency_key,
                ),
            )
        if step.action_type == "action.flight.book":
            payload = {
                "operation": "flight-create-order",
                "offer_id": step.inputs.get("offer_id", "offer_demo_1"),
                "traveler": step.inputs.get("traveler", {}),
                "booking_status": "confirmed",
                "auth_mode": auth_mode,
            }
            return ProviderExecutionResult(
                status="ok",
                payload=payload,
                external_id=_short_external_id(
                    "amadeus_order",
                    context.intent_id,
                    step.step_id,
                    context.idempotency_key,
                ),
            )
        raise ValueError(f"amadeus adapter does not support action_type: {step.action_type}")

    def compensate(
        self,
        step: ActionPlanStepV1,
        context: ActionProviderCompensationContext,
    ) -> ProviderExecutionResult:
        if step.action_type == "action.flight.book":
            payload: dict[str, object] = {
                "operation": "flight-order-cancel",
                "failed_step_id": context.failed_step_id,
            }
            return ProviderExecutionResult(status="ok", payload=payload)
        return ProviderExecutionResult(
            status="ok",
            payload={"operation": "manual", "reason": "no reversible booking available"},
        )

    def compensation_support(self, step: ActionPlanStepV1) -> CompensationSupport:
        if step.action_type == "action.flight.book":
            return "reversal"
        return "manual_only"


class NoopProviderAdapter:
    def preflight(self, scope: dict[str, str]) -> None:
        tenant_norm, repo_norm = _normalize_scope(
            str(scope.get("tenant_id", "")),
            str(scope.get("repo_id", "")),
        )
        scope["tenant_id"] = tenant_norm
        scope["repo_id"] = repo_norm

    def execute(self, step: ActionPlanStepV1, context: ActionProviderExecutionContext) -> ProviderExecutionResult:
        payload: dict[str, object] = {
            "message": "noop provider executed",
            "inputs": step.inputs,
            "context": {
                "intent_id": context.intent_id,
                "tenant_id": context.tenant_id,
                "repo_id": context.repo_id,
                "mode": context.mode,
            },
        }
        return ProviderExecutionResult(
            status="ok",
            payload=payload,
            external_id=_short_external_id("noop", step.step_id, step.idempotency_key),
        )

    def compensate(
        self,
        step: ActionPlanStepV1,
        context: ActionProviderCompensationContext,
    ) -> ProviderExecutionResult:
        payload: dict[str, object] = {
            "message": "noop compensation",
            "step_id": step.step_id,
            "context": {
                "intent_id": context.intent_id,
                "tenant_id": context.tenant_id,
                "repo_id": context.repo_id,
                "failed_step_id": context.failed_step_id,
            },
        }
        return ProviderExecutionResult(status="ok", payload=payload, external_id=None)

    def classify_error(self, error: Exception) -> ProviderErrorKind:
        return _classify_exception(error)

    def compensation_support(self, step: ActionPlanStepV1) -> CompensationSupport:
        _ = step
        return "manual_only"


class ProviderRegistry:
    def __init__(self, *, base_dir: Path | None = None) -> None:
        cache = OAuthTokenCache(base_dir=base_dir)
        self._providers: dict[str, ActionProviderAdapter] = {
            "noop": NoopProviderAdapter(),
            "google": GoogleAdapter(oauth_cache=cache),
            "twilio": TwilioAdapter(oauth_cache=cache),
            "amadeus": AmadeusAdapter(oauth_cache=cache),
        }

    def get(self, name: str) -> ActionProviderAdapter:
        provider = self._providers.get(name)
        if provider is None:
            raise ValueError(f"unknown provider: {name}")
        return provider

    def register(self, *, name: str, provider: ActionProviderAdapter) -> None:
        key = str(name).strip()
        if not key:
            raise ValueError("provider name must be non-empty")
        self._providers[key] = provider
