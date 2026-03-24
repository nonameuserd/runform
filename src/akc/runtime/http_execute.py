"""Policy-bounded outbound HTTP for :class:`LocalDepthRuntimeAdapter` (no ambient creds by default)."""

from __future__ import annotations

import time
import urllib.error
import urllib.request
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from fnmatch import fnmatch
from typing import Any
from urllib.parse import urlparse


@dataclass(frozen=True, slots=True)
class BoundedHttpResult:
    ok: bool
    status_code: int | None
    latency_ms: int
    url_redacted: str
    response_body_snippet: str
    error: str | None


def redact_url_for_evidence(url: str) -> str:
    """Strip query and fragment for operational evidence (PII/token leakage reduction)."""
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return "***"
    path = parsed.path or ""
    return f"{parsed.scheme}://{parsed.netloc}{path}"


def pattern_allows_url(pattern: str, url: str) -> bool:
    """Return True when ``url`` matches an allowlist entry (host, host glob, or URL prefix)."""
    p = str(pattern).strip()
    if not p:
        return False
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False
    host = (parsed.hostname or "").lower()
    if not host:
        return False
    if "://" in p:
        base = p.rstrip("/")
        return url == base or url.startswith(base + "/")
    needle = p.lower().lstrip(".")
    if "*" in needle or needle.startswith("."):
        return fnmatch(host, needle.removeprefix(".")) or fnmatch(host, needle)
    return host == needle or host.endswith("." + needle)


def url_allowed_by_lists(
    url: str,
    *,
    bundle_patterns: Sequence[str],
    envelope_patterns: Sequence[str],
) -> bool:
    """Fail closed: non-empty bundle list required; optional envelope list further constrains."""
    bp = tuple(str(x).strip() for x in bundle_patterns if str(x).strip())
    if not bp:
        return False
    if not any(pattern_allows_url(p, url) for p in bp):
        return False
    ep = tuple(str(x).strip() for x in envelope_patterns if str(x).strip())
    return not ep or any(pattern_allows_url(p, url) for p in ep)


class AllowlistRedirectHandler(urllib.request.HTTPRedirectHandler):
    def __init__(
        self,
        *,
        max_redirects: int,
        url_allowed: Callable[[str], bool],
    ) -> None:
        super().__init__()
        self._max_redirects = max(0, int(max_redirects))
        self._url_allowed = url_allowed
        self._count = 0

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> urllib.request.Request | None:
        self._count += 1
        if self._count > self._max_redirects:
            raise urllib.error.HTTPError(req.full_url, code, "too many redirects", headers, fp)
        if not self._url_allowed(newurl):
            raise urllib.error.HTTPError(req.full_url, code, "redirect target not allowlisted", headers, fp)
        return urllib.request.HTTPRedirectHandler.redirect_request(self, req, fp, code, msg, headers, newurl)


def execute_bounded_http(
    *,
    url: str,
    method: str,
    headers: tuple[tuple[str, str], ...],
    body: bytes | None,
    timeout_ms: int,
    max_response_bytes: int,
    allow_ambient_proxy_env: bool,
    url_allowed: Callable[[str], bool],
    max_redirects: int = 5,
) -> BoundedHttpResult:
    started = time.time_ns()
    redacted = redact_url_for_evidence(url)

    def _latency() -> int:
        return max(0, int((time.time_ns() - started) / 1_000_000))

    try:
        req = urllib.request.Request(url, data=body, method=method.upper())
        for hk, hv in headers:
            req.add_header(hk, hv)
        handlers: list[urllib.request.BaseHandler] = [
            AllowlistRedirectHandler(max_redirects=max_redirects, url_allowed=url_allowed),
        ]
        if not allow_ambient_proxy_env:
            handlers.insert(0, urllib.request.ProxyHandler({}))
        opener = urllib.request.build_opener(*handlers)
        timeout_s = max(timeout_ms / 1000.0, 0.001)
        with opener.open(req, timeout=timeout_s) as resp:  # noqa: S310 — bounded by allowlist + policy
            code = getattr(resp, "status", None)
            if code is None:
                code = resp.getcode()
            chunk = resp.read(max_response_bytes + 1)
    except urllib.error.HTTPError as exc:
        code = int(exc.code)
        chunk = exc.read(max_response_bytes + 1) if exc.fp is not None else b""
        snippet = _snippet(chunk, max_response_bytes)
        ok = 200 <= code < 400
        return BoundedHttpResult(
            ok=ok,
            status_code=code,
            latency_ms=_latency(),
            url_redacted=redacted,
            response_body_snippet=snippet,
            error=None if ok else f"http_error status={code}",
        )
    except Exception as exc:
        return BoundedHttpResult(
            ok=False,
            status_code=None,
            latency_ms=_latency(),
            url_redacted=redacted,
            response_body_snippet="",
            error=str(exc),
        )

    if len(chunk) > max_response_bytes:
        return BoundedHttpResult(
            ok=False,
            status_code=int(code) if code is not None else None,
            latency_ms=_latency(),
            url_redacted=redacted,
            response_body_snippet="",
            error="response exceeded max_response_bytes",
        )
    snippet = _snippet(chunk, max_response_bytes)
    code_int = int(code) if code is not None else None
    ok = code_int is not None and 200 <= code_int < 400
    return BoundedHttpResult(
        ok=ok,
        status_code=code_int,
        latency_ms=_latency(),
        url_redacted=redacted,
        response_body_snippet=snippet,
        error=None if ok else f"unexpected status={code_int}",
    )


def _snippet(raw: bytes, max_len: int) -> str:
    if not raw:
        return ""
    text = raw.decode("utf-8", errors="replace")
    if len(text) > max_len:
        return text[:max_len] + "…"
    return text
