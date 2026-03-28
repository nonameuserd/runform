from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.request
from collections.abc import Mapping
from typing import Any


class LlmHttpError(RuntimeError):
    pass


_REDACT_BEARER = re.compile(r"(?i)(bearer\s+)[^\s\"']{8,}")
_REDACT_SK = re.compile(r"\bsk-[A-Za-z0-9]{12,}\b")
_REDACT_JSONISH_SECRET = re.compile(
    r"(?i)(\"?(?:api[_-]?key|x-api-key|password|secret|token|authorization)\"?\s*:\s*)"
    r"(\"?)([^\s\",}]{8,})"
)


def redact_sensitive_http_detail(text: str, *, max_len: int = 800) -> str:
    """Strip likely secrets from provider error bodies before surfacing to users or logs."""
    s = str(text or "")[:max_len]
    s = _REDACT_BEARER.sub(r"\1[REDACTED]", s)
    s = _REDACT_SK.sub("[REDACTED]", s)
    s = _REDACT_JSONISH_SECRET.sub(r"\1\2[REDACTED]", s)
    return s


def _transient_status(code: int) -> bool:
    return code == 429 or 500 <= code <= 599


def post_json(
    *,
    url: str,
    body: Mapping[str, Any],
    headers: Mapping[str, str],
    timeout_s: float,
    max_retries: int,
) -> dict[str, Any]:
    payload = json.dumps(dict(body), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    req = urllib.request.Request(
        url=url,
        data=payload,
        headers={"Content-Type": "application/json", **dict(headers)},
        method="POST",
    )
    attempts = max(1, int(max_retries) + 1)
    delay_s = 0.25
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(req, timeout=float(timeout_s)) as resp:
                raw = resp.read()
            obj = json.loads(raw.decode("utf-8"))
            if not isinstance(obj, dict):
                raise LlmHttpError("provider response must be a JSON object")
            return obj
        except urllib.error.HTTPError as e:
            code = int(e.code)
            if attempt + 1 < attempts and _transient_status(code):
                time.sleep(delay_s)
                delay_s *= 2.0
                continue
            try:
                detail = e.read().decode("utf-8", errors="replace").strip()
            except Exception:
                detail = ""
            safe_detail = redact_sensitive_http_detail(detail) if detail else ""
            detail_s = f": {safe_detail}" if safe_detail else ""
            raise LlmHttpError(f"provider HTTP error {code}{detail_s}") from e
        except (urllib.error.URLError, TimeoutError) as e:
            last_error = e
            if attempt + 1 < attempts:
                time.sleep(delay_s)
                delay_s *= 2.0
                continue
            raise LlmHttpError(f"provider request failed: {e.__class__.__name__}: {e}") from e
        except json.JSONDecodeError as e:
            raise LlmHttpError("provider response was not valid JSON") from e
    if last_error is not None:
        raise LlmHttpError(f"provider request failed: {last_error}")
    raise LlmHttpError("provider request failed")
