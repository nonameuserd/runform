from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from akc.ingest.exceptions import ConnectorError


def looks_like_url(value: str) -> bool:
    try:
        parsed = urllib.parse.urlparse(value)
    except Exception:  # pragma: no cover
        return False
    return parsed.scheme in {"http", "https"}


def load_spec_bytes(
    spec: str,
    *,
    allow_urls: bool,
    max_bytes: int,
    user_agent: str,
    timeout_seconds: float,
) -> tuple[bytes, str, Path | None]:
    if looks_like_url(spec):
        if not allow_urls:
            raise ConnectorError("URL specs are disabled (allow_urls=false)")
        url = spec
        req = urllib.request.Request(url, headers={"User-Agent": user_agent})
        try:
            with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:  # noqa: S310
                raw = resp.read(max_bytes + 1)
        except (urllib.error.URLError, TimeoutError) as e:
            raise ConnectorError(f"failed to fetch OpenAPI spec URL: {url}") from e
        if len(raw) > max_bytes:
            raise ConnectorError(f"OpenAPI spec exceeds max_bytes ({max_bytes}): {url}")
        return raw, url, None

    try:
        path = Path(spec).expanduser()
    except Exception as e:  # pragma: no cover
        raise ConnectorError("invalid spec path") from e
    try:
        resolved = path.resolve()
    except FileNotFoundError as e:
        raise ConnectorError(f"OpenAPI spec not found: {path}") from e
    if not resolved.is_file():
        raise ConnectorError("OpenAPI spec must be a file")
    try:
        raw = resolved.read_bytes()
    except OSError as e:
        raise ConnectorError(f"failed to read OpenAPI spec: {resolved}") from e
    if len(raw) > max_bytes:
        raise ConnectorError(f"OpenAPI spec exceeds max_bytes ({max_bytes}): {resolved}")
    return raw, str(resolved), resolved.parent


def parse_spec(raw: bytes, *, source_hint: str) -> dict[str, Any]:
    # Try JSON first (fast path).
    try:
        obj = json.loads(raw.decode("utf-8"))
    except Exception:
        obj = None
    if isinstance(obj, dict):
        return obj

    # YAML is optional.
    try:
        import yaml
    except Exception as e:
        raise ConnectorError(
            f"failed to parse OpenAPI spec as JSON; YAML support requires PyYAML: {source_hint}"
        ) from e
    try:
        loaded = yaml.safe_load(raw)
    except Exception as e:
        raise ConnectorError(f"failed to parse OpenAPI spec (YAML): {source_hint}") from e
    if not isinstance(loaded, dict):
        raise ConnectorError(f"OpenAPI spec must be a mapping object: {source_hint}")
    return loaded
