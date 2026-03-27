from __future__ import annotations

import os
import stat
import time
from pathlib import Path

import pytest

from akc.action.models import ActionPlanStepV1
from akc.action.provider_registry import (
    ActionProviderExecutionContext,
    OAuthTokenCache,
    ProviderRegistry,
)


def _ctx() -> ActionProviderExecutionContext:
    return ActionProviderExecutionContext(
        intent_id="intent_1",
        tenant_id="tenant_a",
        repo_id="repo_a",
        idempotency_key="idem_1",
        mode="live",
    )


def test_oauth_cache_scopes_tokens_per_tenant_repo_and_enforces_private_mode(tmp_path: Path) -> None:
    cache = OAuthTokenCache(base_dir=tmp_path)
    ctx = _ctx()
    token_path = cache.store(
        provider="google",
        context=ctx,
        token_payload={"access_token": "tok", "expires_at_ms": int(time.time() * 1000) + 60_000},
    )
    assert token_path == tmp_path / ".akc" / "oauth" / "tenant_a" / "repo_a" / "google.token.json"
    loaded = cache.load(provider="google", context=ctx)
    assert isinstance(loaded, dict)
    assert loaded["access_token"] == "tok"
    if os.name == "posix":
        mode = stat.S_IMODE(token_path.stat().st_mode)
        assert mode == 0o600


def test_oauth_cache_ignores_expired_token(tmp_path: Path) -> None:
    cache = OAuthTokenCache(base_dir=tmp_path)
    ctx = _ctx()
    cache.store(
        provider="amadeus",
        context=ctx,
        token_payload={"access_token": "tok", "expires_at_ms": int(time.time() * 1000) - 1},
    )
    assert cache.load(provider="amadeus", context=ctx) is None


def test_twilio_provider_builds_request_payload_without_network(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("AKC_TWILIO_ACCOUNT_SID", "AC123")
    monkeypatch.setenv("AKC_TWILIO_AUTH_TOKEN", "secret")
    monkeypatch.setenv("AKC_TWILIO_FROM_NUMBER", "+15005550006")
    reg = ProviderRegistry(base_dir=tmp_path)
    provider = reg.get("twilio")
    step = ActionPlanStepV1(
        step_id="step_1",
        action_type="action.call.place",
        provider="twilio",
        inputs={"to": "+14155550123"},
        idempotency_key="idem_1",
        risk_tier="medium",
        requires_approval=False,
    )
    res = provider.execute(step, _ctx())
    assert res.status == "ok"
    assert res.payload["operation"] == "calls.create"


def test_provider_rejects_scope_override_in_step_inputs(tmp_path: Path) -> None:
    reg = ProviderRegistry(base_dir=tmp_path)
    provider = reg.get("google")
    step = ActionPlanStepV1(
        step_id="step_1",
        action_type="action.contact.lookup",
        provider="google",
        inputs={"tenant_id": "tenant_b", "query": "dad"},
        idempotency_key="idem_1",
        risk_tier="low",
        requires_approval=False,
    )
    with pytest.raises(ValueError, match="tenant_id"):
        provider.execute(step, _ctx())
