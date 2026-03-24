"""Unit tests for MCP server authorization guards."""

from __future__ import annotations

import pytest

from akc.mcp_serve.guards import ensure_tenant_allowed, ensure_tool_token, normalize_allowed_tenants


def test_normalize_allowed_tenants_empty() -> None:
    assert normalize_allowed_tenants([]) is None
    assert normalize_allowed_tenants(None) is None
    assert normalize_allowed_tenants(["", "  "]) is None


def test_normalize_allowed_tenants_strips() -> None:
    assert normalize_allowed_tenants([" a ", "b"]) == frozenset({"a", "b"})


def test_ensure_tenant_allowed_unconstrained() -> None:
    ensure_tenant_allowed(allowed=None, tenant_id="any-tenant")


def test_ensure_tenant_allowed_denied() -> None:
    with pytest.raises(PermissionError):
        ensure_tenant_allowed(allowed=frozenset({"a"}), tenant_id="b")


def test_ensure_tool_token_optional() -> None:
    ensure_tool_token(expected=None, provided=None)
    ensure_tool_token(expected=None, provided="x")


def test_ensure_tool_token_required() -> None:
    with pytest.raises(PermissionError):
        ensure_tool_token(expected="secret", provided=None)
    with pytest.raises(PermissionError):
        ensure_tool_token(expected="secret", provided="wrong")


def test_ensure_tool_token_ok() -> None:
    ensure_tool_token(expected="secret", provided="secret")
