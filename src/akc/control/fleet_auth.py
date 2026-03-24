"""Minimal RBAC for fleet read API (Bearer tokens + tenant allowlists)."""

from __future__ import annotations

from dataclasses import dataclass

from akc.control.fleet_config import FleetConfig


@dataclass(frozen=True, slots=True)
class FleetAuthContext:
    """Resolved caller for a fleet HTTP request."""

    role: str
    tenant_allowlist: tuple[str, ...]
    scopes: tuple[str, ...]
    token_id: str | None


def resolve_bearer_token(*, cfg: FleetConfig, authorization_header: str | None) -> FleetAuthContext | None:
    """Return auth context or None if missing / non-matching token."""

    if not authorization_header or not authorization_header.strip():
        return None
    parts = authorization_header.strip().split(None, 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    token = parts[1].strip()
    if not token:
        return None
    for entry in cfg.api_tokens:
        if entry.token == token:
            return FleetAuthContext(
                role=entry.role,
                tenant_allowlist=entry.tenant_allowlist,
                scopes=entry.scopes,
                token_id=entry.id,
            )
    return None


def auth_has_scope(ctx: FleetAuthContext | None, scope: str) -> bool:
    if ctx is None:
        return False
    if scope == "runs:metadata:write" and "runs:label" in ctx.scopes:
        return True
    return scope in ctx.scopes


def auth_allows_tenant(
    ctx: FleetAuthContext | None,
    *,
    tenant_id: str,
    cfg: FleetConfig,
    enforce_allowlist: bool = False,
) -> bool:
    """Whether the caller may access data for ``tenant_id``.

    When ``cfg.allow_anonymous_read`` is true, unauthenticated reads bypass the allowlist.
    Mutating routes must pass ``enforce_allowlist=True`` so bearer tokens are always
    constrained by their tenant allowlist even if anonymous read is enabled.
    """

    if cfg.allow_anonymous_read and not enforce_allowlist:
        return True
    if ctx is None:
        return False
    t = tenant_id.strip()
    if "*" in ctx.tenant_allowlist:
        return True
    return t in ctx.tenant_allowlist


def fleet_read_auth_result(
    cfg: FleetConfig,
    authorization_header: str | None,
) -> tuple[int | None, FleetAuthContext | None]:
    """Return ``(http_status, ctx)``. ``http_status`` None means authentication succeeded.

    * ``allow_anonymous_read`` → ``(None, None)``.
    * No tokens configured → ``403`` (fleet API disabled).
    * Missing/invalid Bearer → ``401``.
    * Authenticated token without ``runs:read`` → ``403``.
    """

    if cfg.allow_anonymous_read:
        return None, None
    if not cfg.api_tokens:
        return 403, None
    ctx = resolve_bearer_token(cfg=cfg, authorization_header=authorization_header)
    if ctx is None:
        return 401, None
    if not auth_has_scope(ctx, "runs:read"):
        return 403, None
    return None, ctx


def fleet_write_auth_result(
    cfg: FleetConfig,
    authorization_header: str | None,
    *,
    required_scope: str | tuple[str, ...],
) -> tuple[int | None, FleetAuthContext | None]:
    """Bearer-only auth for mutating routes (never ``allow_anonymous_read``).

    * No tokens configured → ``403``.
    * Missing/invalid Bearer → ``401``.
    * Missing required scope → ``403``.
    """

    if not cfg.api_tokens:
        return 403, None
    ctx = resolve_bearer_token(cfg=cfg, authorization_header=authorization_header)
    if ctx is None:
        return 401, None
    required_scopes = (required_scope,) if isinstance(required_scope, str) else required_scope
    if not any(auth_has_scope(ctx, scope) for scope in required_scopes):
        return 403, None
    return None, ctx
