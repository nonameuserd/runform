from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from akc.memory.models import JSONValue
from akc.utils.fingerprint import stable_json_fingerprint


def llm_vcr_prompt_key(
    *,
    messages: Sequence[Any],
    temperature: float,
    max_output_tokens: int | None,
    metadata: Mapping[str, JSONValue] | None,
) -> str:
    payload: dict[str, Any] = {
        "messages": [
            {"role": str(getattr(m, "role", "")), "content": str(getattr(m, "content", ""))}
            for m in messages
        ],
        "temperature": float(temperature),
        "max_output_tokens": int(max_output_tokens) if max_output_tokens is not None else None,
        "metadata": dict(metadata) if metadata else None,
    }
    return stable_json_fingerprint(payload)
