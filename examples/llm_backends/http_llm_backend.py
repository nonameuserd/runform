from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.request
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from akc.compile.interfaces import LLMBackend, LLMMessage, LLMRequest, LLMResponse, TenantRepoScope

_TraversalLike = re.compile(r"(?:\.\./|\\\.\.\\|/\\\.\.|\\\.\./)")
_AbsolutePathLike = re.compile(r"(?m)^(--- |\+\+\+ )(?:/|~)")


@dataclass(frozen=True, slots=True)
class HttpLLMBackendConfig:
    """Configuration for the example HTTP-backed LLM.

    Environment variables:
    - `AKC_HTTP_LLM_URL` (required): endpoint that accepts POST JSON.
    - `AKC_HTTP_LLM_API_KEY` (optional): bearer token for Authorization header.
    - `AKC_HTTP_LLM_TIMEOUT_S` (optional, default: 30): request timeout.
    """

    url: str
    api_key: str | None
    timeout_s: float

    @staticmethod
    def from_env() -> HttpLLMBackendConfig:
        url = os.environ.get("AKC_HTTP_LLM_URL", "").strip()
        api_key = os.environ.get("AKC_HTTP_LLM_API_KEY")
        timeout_raw = os.environ.get("AKC_HTTP_LLM_TIMEOUT_S", "30").strip()
        try:
            timeout_s = float(timeout_raw)
        except ValueError:
            timeout_s = 30.0

        if not url:
            raise ValueError(
                "AKC_HTTP_LLM_URL must be set when using HttpLLMBackend (custom living backend)"
            )
        if timeout_s <= 0:
            timeout_s = 30.0

        api_key2 = api_key.strip() if isinstance(api_key, str) else None
        return HttpLLMBackendConfig(url=url, api_key=api_key2, timeout_s=timeout_s)


class HttpLLMBackend(LLMBackend):
    """Example LLM backend that calls an HTTP endpoint.

    This is intentionally dependency-free (standard library only).
    It is meant to show how to wire a real provider without baking any
    vendor-specific assumptions into AKC.

    Expected response shape (JSON):
    - `text`: string (required)
    - `usage`: optional object with integer-ish `input_tokens`/`output_tokens`
    - any other fields are included in `raw` for auditing/debugging.
    """

    def __init__(self) -> None:
        self._cfg = HttpLLMBackendConfig.from_env()

    def _render_messages(self, *, messages: Sequence[LLMMessage]) -> str:
        # Deterministic message rendering for stable cache keys/replay.
        parts: list[str] = []
        for m in messages:
            parts.append(f"[{m.role}] {m.content}")
        return "\n".join(parts)

    def _validate_patch_safety(self, *, text: str) -> None:
        # Best-effort safety guardrail for the example backend; production
        # should rely on verifier/policy enforcement as well.
        if _TraversalLike.search(text or ""):
            raise ValueError("refusing response with path traversal-like sequences")
        if _AbsolutePathLike.search(text or ""):
            raise ValueError("refusing response with absolute/tilde paths in diff headers")

    def complete(
        self,
        *,
        scope: TenantRepoScope,
        stage: str,
        request: LLMRequest,
    ) -> LLMResponse:
        rendered_prompt = self._render_messages(messages=request.messages)

        payload: dict[str, Any] = {
            "tenant_id": scope.tenant_id,
            "repo_id": scope.repo_id,
            "stage": stage,
            "temperature": request.temperature,
            "max_output_tokens": request.max_output_tokens,
            "stop": list(request.stop) if request.stop is not None else None,
            "prompt": rendered_prompt,
        }

        headers: dict[str, str] = {"Content-Type": "application/json"}
        if self._cfg.api_key:
            headers["Authorization"] = f"Bearer {self._cfg.api_key}"

        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        req = urllib.request.Request(
            url=self._cfg.url,
            data=body,
            headers=headers,
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, timeout=self._cfg.timeout_s) as resp:
                raw_bytes = resp.read()
        except urllib.error.URLError as e:
            raise RuntimeError(f"HTTP LLM request failed: {e.__class__.__name__}: {e}") from e

        try:
            resp_obj = json.loads(raw_bytes.decode("utf-8"))
        except Exception as e:  # pragma: no cover
            raise ValueError("HTTP LLM response was not valid JSON") from e

        if not isinstance(resp_obj, dict):
            raise ValueError("HTTP LLM response must be a JSON object")

        text_raw = resp_obj.get("text")
        if not isinstance(text_raw, str) or not text_raw.strip():
            raise ValueError("HTTP LLM response must include non-empty string field `text`")
        text = text_raw

        self._validate_patch_safety(text=text)

        usage_raw = resp_obj.get("usage")
        usage: Mapping[str, int] | None = None
        if isinstance(usage_raw, Mapping):
            maybe_in = usage_raw.get("input_tokens")
            maybe_out = usage_raw.get("output_tokens")
            usage_dict: dict[str, int] = {}
            if isinstance(maybe_in, (int, float)):
                usage_dict["input_tokens"] = int(maybe_in)
            if isinstance(maybe_out, (int, float)):
                usage_dict["output_tokens"] = int(maybe_out)
            usage = usage_dict if usage_dict else None

        # Preserve the raw response for audit/debugging in `RunManifest`.
        return LLMResponse(text=text, raw=resp_obj, usage=usage)
