"""Authorization helpers for the read-only AKC MCP server."""

from __future__ import annotations

import hmac
from collections.abc import Collection


def normalize_allowed_tenants(raw: Collection[str] | None) -> frozenset[str] | None:
    """Return a non-empty frozenset of stripped tenant ids, or None if unconstrained."""

    if raw is None:
        return None
    out = frozenset(t.strip() for t in raw if isinstance(t, str) and t.strip())
    return out or None


def ensure_tenant_allowed(*, allowed: frozenset[str] | None, tenant_id: str) -> None:
    if allowed is None:
        return
    tid = tenant_id.strip()
    if tid not in allowed:
        raise PermissionError("tenant_id is not in the configured allowlist (AKC_MCP_ALLOWED_TENANTS / --allow-tenant)")


def ensure_tool_token(*, expected: str | None, provided: str | None) -> None:
    """When ``expected`` is set (stdio shared-secret), require a matching ``tool_token``."""

    if expected is None:
        return
    if provided is None or not str(provided).strip():
        raise PermissionError("tool_token is required (set AKC_MCP_TOOL_TOKEN on the server and pass tool_token)")
    a = str(provided).strip()
    b = str(expected).strip()
    if len(a) != len(b) or not hmac.compare_digest(a, b):
        raise PermissionError("invalid tool_token")
