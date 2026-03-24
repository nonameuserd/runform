"""Optional read-only HTTP/TCP probes for deployment observe providers.

Probes are declared under ``deployment_provider.observe_probes`` on the runtime bundle.
They add :class:`ObservedHealthCondition` rows (for example ``ProbeHttp`` / ``ProbeTcp``) and
do **not** affect the stable observe hash fingerprint (readiness vs identity).

**Security:** URLs and hosts come from bundle metadata only; callers must scope bundles to
trusted tenants. HTTP probes use a direct GET with no ambient proxy (no credential injection).
"""

from __future__ import annotations

import socket
import urllib.error
import urllib.request
from collections.abc import Mapping, Sequence
from typing import cast

from akc.memory.models import JSONValue
from akc.runtime.models import ObservedHealthCondition, ObservedHealthConditionStatus


def parse_observe_probe_specs(raw: object) -> tuple[dict[str, JSONValue], ...]:
    """Normalize ``observe_probes`` array from ``deployment_provider`` metadata."""
    if raw is None:
        return ()
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        return ()
    out: list[dict[str, JSONValue]] = []
    for item in raw:
        if isinstance(item, Mapping):
            row: dict[str, JSONValue] = {}
            for k, v in item.items():
                if isinstance(k, str) and k.strip() and (isinstance(v, (str, int, float, bool)) or v is None):
                    row[k.strip()] = cast(JSONValue, v)
            if row:
                out.append(row)
    return tuple(out)


def _status(ok: bool) -> ObservedHealthConditionStatus:
    return "true" if ok else "false"


def _tcp_probe(host: str, port: int, timeout_ms: int) -> tuple[bool, str | None]:
    if port < 1 or port > 65535:
        return False, "invalid_port"
    timeout_s = max(timeout_ms / 1000.0, 0.001)
    try:
        with socket.create_connection((host, port), timeout=timeout_s):
            return True, None
    except OSError as exc:
        return False, str(exc)


def _http_probe(
    url: str,
    *,
    timeout_ms: int,
    status_min: int,
    status_max: int,
) -> tuple[bool, str | None]:
    from urllib.parse import urlparse

    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return False, "url_must_be_http_or_https_with_host"
    timeout_s = max(timeout_ms / 1000.0, 0.001)
    req = urllib.request.Request(url, method="GET")
    try:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(req, timeout=timeout_s) as resp:  # noqa: S310 — bundle-scoped probe URL
            code = getattr(resp, "status", None) or resp.getcode()
    except urllib.error.HTTPError as exc:
        code = int(exc.code)
        if status_min <= code <= status_max:
            return True, None
        return False, f"http_status={code}"
    except Exception as exc:
        return False, str(exc)
    if not isinstance(code, int):
        return False, "no_status_code"
    if status_min <= code <= status_max:
        return True, None
    return False, f"http_status={code}"


def evaluate_observe_probes(specs: tuple[dict[str, JSONValue], ...]) -> tuple[ObservedHealthCondition, ...]:
    """Run probe specs and return condition rows (stable ordering)."""
    rows: list[ObservedHealthCondition] = []
    for idx, spec in enumerate(specs):
        kind = str(spec.get("kind", "")).strip().lower()
        timeout_raw = spec.get("timeout_ms", 3000)
        timeout_ms = int(timeout_raw) if isinstance(timeout_raw, int) and not isinstance(timeout_raw, bool) else 3000
        timeout_ms = max(1, min(timeout_ms, 120_000))
        if kind == "tcp":
            host = str(spec.get("host", "127.0.0.1")).strip() or "127.0.0.1"
            port_raw = spec.get("port", 0)
            port = int(port_raw) if isinstance(port_raw, int) and not isinstance(port_raw, bool) else 0
            ok, err = _tcp_probe(host, port, timeout_ms)
            rows.append(
                ObservedHealthCondition(
                    type="ProbeTcp",
                    status=_status(ok),
                    reason="tcp_connected" if ok else "tcp_failed",
                    message=None if ok else (err or "tcp_failed"),
                    last_transition_time=None,
                )
            )
        elif kind == "http":
            url = str(spec.get("url", "")).strip()
            smin_raw = spec.get("expected_status_min", 200)
            smax_raw = spec.get("expected_status_max", 299)
            smin = int(smin_raw) if isinstance(smin_raw, int) and not isinstance(smin_raw, bool) else 200
            smax = int(smax_raw) if isinstance(smax_raw, int) and not isinstance(smax_raw, bool) else 299
            if not url:
                rows.append(
                    ObservedHealthCondition(
                        type="ProbeHttp",
                        status="false",
                        reason="missing_url",
                        message=f"probe[{idx}]",
                        last_transition_time=None,
                    )
                )
                continue
            ok, err = _http_probe(url, timeout_ms=timeout_ms, status_min=smin, status_max=smax)
            rows.append(
                ObservedHealthCondition(
                    type="ProbeHttp",
                    status=_status(ok),
                    reason="http_ok" if ok else "http_failed",
                    message=None if ok else (err or "http_failed"),
                    last_transition_time=None,
                )
            )
    return tuple(rows)
