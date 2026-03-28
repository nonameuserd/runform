"""Map provider-specific usage payloads to AKC token accounting fields."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def _pick_int(mapping: Mapping[str, Any], *keys: str) -> int | None:
    for key in keys:
        raw = mapping.get(key)
        if isinstance(raw, (int, float)):
            return int(raw)
    return None


def usage_from_openai_response(raw: Mapping[str, Any]) -> dict[str, int] | None:
    """Normalize OpenAI Responses (or compatible) ``usage`` objects."""
    usage = raw.get("usage")
    if not isinstance(usage, Mapping):
        return None
    out: dict[str, int] = {}
    inp = _pick_int(usage, "input_tokens", "prompt_tokens")
    if inp is not None:
        out["input_tokens"] = inp
    out_tok = _pick_int(usage, "output_tokens", "completion_tokens")
    if out_tok is not None:
        out["output_tokens"] = out_tok
    return out or None


def usage_from_anthropic_response(raw: Mapping[str, Any]) -> dict[str, int] | None:
    usage = raw.get("usage")
    if not isinstance(usage, Mapping):
        return None
    out: dict[str, int] = {}
    inp = _pick_int(usage, "input_tokens")
    if inp is not None:
        out["input_tokens"] = inp
    out_tok = _pick_int(usage, "output_tokens")
    if out_tok is not None:
        out["output_tokens"] = out_tok
    return out or None


def usage_from_gemini_response(raw: Mapping[str, Any]) -> dict[str, int] | None:
    usage_raw = raw.get("usageMetadata")
    if not isinstance(usage_raw, Mapping):
        return None
    out: dict[str, int] = {}
    inp = _pick_int(usage_raw, "promptTokenCount")
    if inp is not None:
        out["input_tokens"] = inp
    out_tok = _pick_int(usage_raw, "candidatesTokenCount")
    if out_tok is not None:
        out["output_tokens"] = out_tok
    return out or None
