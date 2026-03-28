from __future__ import annotations

import argparse
import io
import json
import urllib.error

import pytest

from akc.llm.http import LlmHttpError, post_json, redact_sensitive_http_detail
from akc.llm.providers import AnthropicLlmBackend, GeminiLlmBackend, OpenAiLlmBackend
from akc.llm.resolver import resolve_llm_runtime_config
from akc.llm.usage import usage_from_openai_response


def _args(**overrides: object) -> argparse.Namespace:
    base = {
        "llm_backend": None,
        "llm_model": None,
        "llm_base_url": None,
        "llm_api_key": None,
        "llm_timeout_s": None,
        "llm_max_retries": None,
        "llm_allow_network": None,
        "llm_backend_class": None,
        "llm_mode": None,
        "assistant_model_hint": None,
    }
    base.update(overrides)
    return argparse.Namespace(**base)


def test_resolve_llm_runtime_config_prefers_cli_over_env() -> None:
    cfg = resolve_llm_runtime_config(
        args=_args(llm_backend="openai", llm_model="gpt-cli", llm_api_key="cli-key", llm_allow_network=True),
        env={
            "AKC_LLM_BACKEND": "gemini",
            "AKC_LLM_MODEL": "env-model",
            "AKC_LLM_API_KEY": "env-key",
            "AKC_LLM_ALLOW_NETWORK": "0",
        },
        project=None,
        surface="compile",
    )
    assert cfg.backend == "openai"
    assert cfg.model == "gpt-cli"
    assert cfg.api_key == "cli-key"
    assert cfg.allow_network is True


def test_resolve_llm_runtime_config_accepts_provider_native_env_fallback() -> None:
    cfg = resolve_llm_runtime_config(
        args=_args(llm_backend="anthropic", llm_allow_network=True),
        env={"ANTHROPIC_API_KEY": "native-key"},
        project=None,
        surface="living",
    )
    assert cfg.backend == "anthropic"
    assert cfg.api_key == "native-key"


def test_resolve_llm_runtime_config_requires_explicit_network_for_hosted() -> None:
    with pytest.raises(ValueError, match="requires explicit network opt-in"):
        resolve_llm_runtime_config(
            args=_args(llm_backend="openai", llm_api_key="k"),
            env={},
            project=None,
            surface="compile",
        )


def test_openai_backend_parses_output_text(monkeypatch: pytest.MonkeyPatch) -> None:
    import akc.llm.providers as providers
    from akc.compile.interfaces import LLMMessage, LLMRequest, TenantRepoScope
    from akc.llm.config import LlmRuntimeConfig

    def _fake_post_json(**kwargs: object) -> dict[str, object]:
        assert kwargs["headers"] == {"Authorization": "Bearer k"}
        body = kwargs["body"]
        assert isinstance(body, dict)
        assert body["model"] == "gpt-test"
        return {"output_text": "akc control runs list", "usage": {"input_tokens": 4, "output_tokens": 7}}

    monkeypatch.setattr(providers, "post_json", _fake_post_json)
    backend = OpenAiLlmBackend(
        config=LlmRuntimeConfig(
            backend="openai",
            model="gpt-test",
            api_key="k",
            allow_network=True,
        )
    )
    resp = backend.complete(
        scope=TenantRepoScope(tenant_id="t1", repo_id="r1"),
        stage="generate",
        request=LLMRequest(messages=(LLMMessage(role="user", content="hi"),)),
    )
    assert resp.text == "akc control runs list"
    assert resp.usage == {"input_tokens": 4, "output_tokens": 7}


def test_anthropic_backend_parses_text_blocks(monkeypatch: pytest.MonkeyPatch) -> None:
    import akc.llm.providers as providers
    from akc.compile.interfaces import LLMMessage, LLMRequest, TenantRepoScope
    from akc.llm.config import LlmRuntimeConfig

    monkeypatch.setattr(
        providers,
        "post_json",
        lambda **_k: {
            "content": [{"type": "text", "text": "akc control runs list --limit 5"}],
            "usage": {"input_tokens": 3, "output_tokens": 9},
        },
    )
    backend = AnthropicLlmBackend(
        config=LlmRuntimeConfig(backend="anthropic", model="claude-test", api_key="k", allow_network=True)
    )
    resp = backend.complete(
        scope=TenantRepoScope(tenant_id="t1", repo_id="r1"),
        stage="repair",
        request=LLMRequest(messages=(LLMMessage(role="user", content="hi"),)),
    )
    assert resp.text == "akc control runs list --limit 5"
    assert resp.usage == {"input_tokens": 3, "output_tokens": 9}


def test_usage_from_openai_accepts_prompt_and_completion_token_aliases() -> None:
    raw = {"usage": {"prompt_tokens": 3, "completion_tokens": 5}}
    assert usage_from_openai_response(raw) == {"input_tokens": 3, "output_tokens": 5}


def test_post_json_retries_transient_http_500(monkeypatch: pytest.MonkeyPatch) -> None:
    import akc.llm.http as http_mod

    calls: list[int] = []

    class _Ok:
        def __enter__(self) -> _Ok:
            return self

        def __exit__(self, *a: object) -> None:
            return None

        def read(self) -> bytes:
            return b'{"ok":true,"n":1}'

    def _urlopen(*a: object, **k: object) -> object:
        calls.append(1)
        if len(calls) == 1:
            raise urllib.error.HTTPError("http://example.invalid/x", 500, "err", hdrs={}, fp=io.BytesIO(b"{}"))
        return _Ok()

    monkeypatch.setattr(http_mod.urllib.request, "urlopen", _urlopen)
    monkeypatch.setattr(http_mod.time, "sleep", lambda _s: None)
    out = post_json(url="http://example.invalid/x", body={}, headers={}, timeout_s=1.0, max_retries=2)
    assert out == {"ok": True, "n": 1}
    assert len(calls) == 2


def test_post_json_http_error_redacts_bearer_like_tokens(monkeypatch: pytest.MonkeyPatch) -> None:
    import akc.llm.http as http_mod

    body = json.dumps({"error": {"message": "bad Bearer sk-fixed-value-1234567890"}}).encode()

    def _urlopen(*a: object, **k: object) -> object:
        raise urllib.error.HTTPError("http://example.invalid/x", 401, "err", hdrs={}, fp=io.BytesIO(body))

    monkeypatch.setattr(http_mod.urllib.request, "urlopen", _urlopen)
    with pytest.raises(LlmHttpError) as ei:
        post_json(
            url="http://example.invalid/x",
            body={},
            headers={},
            timeout_s=1.0,
            max_retries=0,
        )
    msg = str(ei.value)
    assert "sk-fixed" not in msg
    assert "1234567890" not in msg
    assert "[REDACTED]" in msg


def test_redact_sensitive_http_detail_covers_common_patterns() -> None:
    raw = "err: Bearer eyJhbGciOiJIUzI1NiJ9.abcdef and sk-12345678901234567890"
    out = redact_sensitive_http_detail(raw)
    assert "eyJhbGciOi" not in out
    assert "sk-12345678901234567890" not in out


def test_resolve_llm_runtime_config_env_wins_over_project_model() -> None:
    from akc.llm.config import LlmProjectConfig

    cfg = resolve_llm_runtime_config(
        args=_args(),
        env={"AKC_LLM_MODEL": "from-env"},
        project=LlmProjectConfig(model="from-project", backend="offline"),
        surface="compile",
    )
    assert cfg.model == "from-env"


def test_llm_metadata_includes_planner_mode() -> None:
    from akc.llm.config import LlmRuntimeConfig
    from akc.llm.resolver import llm_metadata

    off = llm_metadata(config=LlmRuntimeConfig(backend="offline", model="x"), surface="compile")
    assert off["llm_mode"] == "offline"
    on = llm_metadata(config=LlmRuntimeConfig(backend="openai", model="m", allow_network=True), surface="assistant")
    assert on["llm_mode"] == "hosted"


def test_gemini_backend_parses_candidates(monkeypatch: pytest.MonkeyPatch) -> None:
    import akc.llm.providers as providers
    from akc.compile.interfaces import LLMMessage, LLMRequest, TenantRepoScope
    from akc.llm.config import LlmRuntimeConfig

    monkeypatch.setattr(
        providers,
        "post_json",
        lambda **_k: {
            "candidates": [{"content": {"parts": [{"text": "akc compile --mode quick"}]}}],
            "usageMetadata": {"promptTokenCount": 11, "candidatesTokenCount": 13},
        },
    )
    backend = GeminiLlmBackend(
        config=LlmRuntimeConfig(
            backend="gemini",
            model="gemini-test",
            api_key="k",
            allow_network=True,
        )
    )
    resp = backend.complete(
        scope=TenantRepoScope(tenant_id="t1", repo_id="r1"),
        stage="generate",
        request=LLMRequest(messages=(LLMMessage(role="user", content="hi"),)),
    )
    assert resp.text == "akc compile --mode quick"
    assert resp.usage == {"input_tokens": 11, "output_tokens": 13}
