from __future__ import annotations

import argparse
import os
from collections.abc import Mapping
from typing import cast

from akc.compile.interfaces import LLMBackend

from .config import LlmBackendName, LlmProjectConfig, LlmRuntimeConfig
from .providers import (
    AnthropicLlmBackend,
    GeminiLlmBackend,
    OfflineLlmBackend,
    OpenAiLlmBackend,
    load_custom_llm_backend,
)


def _env_bool(env: Mapping[str, str], key: str) -> bool | None:
    raw = str(env.get(key, "") or "").strip().lower()
    if not raw:
        return None
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return None


def _clean(value: object) -> str | None:
    s = str(value or "").strip()
    return s or None


def _float_value(value: object) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            return float(s)
        except ValueError:
            return None
    return None


def _int_value(value: object) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not value.is_integer():
            return None
        return int(value)
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            return int(s, 10)
        except ValueError:
            return None
    return None


def _default_model(*, backend: str, surface: str) -> str:
    if backend == "openai":
        return "gpt-4.1-mini" if surface == "assistant" else "gpt-4.1"
    if backend == "anthropic":
        return "claude-3-5-haiku-latest" if surface == "assistant" else "claude-3-7-sonnet-latest"
    if backend == "gemini":
        return "gemini-2.5-flash"
    return "offline-small"


def _provider_env_api_key(*, backend: str, env: Mapping[str, str]) -> str | None:
    if backend == "openai":
        return _clean(env.get("AKC_OPENAI_API_KEY")) or _clean(env.get("OPENAI_API_KEY"))
    if backend == "anthropic":
        return _clean(env.get("AKC_ANTHROPIC_API_KEY")) or _clean(env.get("ANTHROPIC_API_KEY"))
    if backend == "gemini":
        return _clean(env.get("AKC_GEMINI_API_KEY")) or _clean(env.get("GEMINI_API_KEY"))
    return None


def _provider_env_base_url(*, backend: str, env: Mapping[str, str]) -> str | None:
    if backend == "openai":
        return _clean(env.get("AKC_OPENAI_BASE_URL"))
    if backend == "anthropic":
        return _clean(env.get("AKC_ANTHROPIC_BASE_URL"))
    if backend == "gemini":
        return _clean(env.get("AKC_GEMINI_BASE_URL"))
    return None


def resolve_llm_runtime_config(
    *,
    args: argparse.Namespace,
    env: Mapping[str, str] | None = None,
    project: LlmProjectConfig | None = None,
    surface: str,
) -> LlmRuntimeConfig:
    effective_env = env if env is not None else os.environ
    legacy_mode = _clean(getattr(args, "llm_mode", None))
    backend = (
        _clean(getattr(args, "llm_backend", None))
        or _clean(effective_env.get("AKC_LLM_BACKEND"))
        or (_clean(project.backend) if project is not None else None)
        or ("custom" if legacy_mode == "custom" else None)
        or "offline"
    )
    if backend not in {"offline", "openai", "anthropic", "gemini", "custom"}:
        raise ValueError(f"unsupported llm backend: {backend}")
    backend_name = cast(LlmBackendName, backend)
    backend_class = (
        _clean(getattr(args, "llm_backend_class", None))
        or _clean(effective_env.get("AKC_LLM_BACKEND_CLASS"))
        or (_clean(project.backend_class) if project is not None else None)
    )
    model = (
        _clean(getattr(args, "llm_model", None))
        or _clean(effective_env.get("AKC_LLM_MODEL"))
        or (_clean(getattr(args, "assistant_model_hint", None)) if surface == "assistant" else None)
        or (_clean(project.model) if project is not None else None)
        or _default_model(backend=backend_name, surface=surface)
    )
    base_url = (
        _clean(getattr(args, "llm_base_url", None))
        or _clean(effective_env.get("AKC_LLM_BASE_URL"))
        or _provider_env_base_url(backend=backend_name, env=effective_env)
        or (_clean(project.base_url) if project is not None else None)
    )
    api_key = (
        _clean(getattr(args, "llm_api_key", None))
        or _clean(effective_env.get("AKC_LLM_API_KEY"))
        or _provider_env_api_key(backend=backend_name, env=effective_env)
    )
    timeout_s = (
        _float_value(getattr(args, "llm_timeout_s", None))
        or _float_value(effective_env.get("AKC_LLM_TIMEOUT_S"))
        or (_float_value(project.timeout_s) if project is not None else None)
        or 60.0
    )
    retries = (
        _int_value(getattr(args, "llm_max_retries", None))
        or _int_value(effective_env.get("AKC_LLM_MAX_RETRIES"))
        or (_int_value(project.max_retries) if project is not None else None)
        or 2
    )
    allow_network = (
        getattr(args, "llm_allow_network", None)
        if getattr(args, "llm_allow_network", None) is not None
        else _env_bool(effective_env, "AKC_LLM_ALLOW_NETWORK")
    )
    if allow_network is None:
        allow_network = (
            bool(project.allow_network) if project is not None and project.allow_network is not None else False
        )
    config = LlmRuntimeConfig(
        backend=backend_name,
        model=str(model),
        base_url=base_url,
        api_key=api_key,
        timeout_s=float(timeout_s),
        max_retries=max(0, int(retries)),
        allow_network=bool(allow_network),
        backend_class=backend_class,
    )
    if config.is_hosted:
        if not config.allow_network:
            raise ValueError(
                f"Hosted llm backend {config.backend} requires explicit network opt-in "
                "(use --llm-allow-network or AKC_LLM_ALLOW_NETWORK=1)"
            )
        if not config.api_key:
            raise ValueError(f"Hosted llm backend {config.backend} requires an API key")
    if config.backend == "custom" and not config.backend_class:
        raise ValueError("Custom llm backend requires --llm-backend-class or AKC_LLM_BACKEND_CLASS")
    return config


def build_llm_backend(*, config: LlmRuntimeConfig) -> LLMBackend:
    if config.backend == "offline":
        return OfflineLlmBackend(config=config)
    if config.backend == "openai":
        return OpenAiLlmBackend(config=config)
    if config.backend == "anthropic":
        return AnthropicLlmBackend(config=config)
    if config.backend == "gemini":
        return GeminiLlmBackend(config=config)
    return load_custom_llm_backend(class_path=str(config.backend_class))


def llm_metadata(*, config: LlmRuntimeConfig, surface: str) -> dict[str, object]:
    planner_mode = "hosted" if config.backend != "offline" else "offline"
    return {
        "llm_backend": config.backend,
        "llm_provider": config.provider,
        "llm_model": config.model,
        "llm_network_allowed": bool(config.allow_network),
        "llm_mode": planner_mode,
        "llm_surface": surface,
    }
