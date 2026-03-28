from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

LlmBackendName = Literal["offline", "openai", "anthropic", "gemini", "custom"]


@dataclass(frozen=True, slots=True)
class LlmRuntimeConfig:
    backend: LlmBackendName
    model: str
    base_url: str | None = None
    api_key: str | None = None
    timeout_s: float = 60.0
    max_retries: int = 2
    allow_network: bool = False
    backend_class: str | None = None

    @property
    def provider(self) -> str:
        return self.backend

    @property
    def is_hosted(self) -> bool:
        return self.backend in {"openai", "anthropic", "gemini"}


@dataclass(frozen=True, slots=True)
class LlmProjectConfig:
    backend: str | None = None
    model: str | None = None
    base_url: str | None = None
    timeout_s: float | None = None
    max_retries: int | None = None
    allow_network: bool | None = None
    backend_class: str | None = None
