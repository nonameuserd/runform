from __future__ import annotations

import pytest

from akc.ingest.connectors.mcp.config import expand_env_refs, validate_mcp_http_url
from akc.ingest.connectors.mcp.connector import (
    mcp_incremental_can_skip,
    mcp_listing_revision_from_resource,
    mcp_placeholder_text,
    mcp_source_id_for_uri,
)
from akc.ingest.exceptions import ConnectorError


def test_expand_env_refs_substitutes() -> None:
    assert expand_env_refs("a${X}b", env={"X": "Z"}) == "aZb"
    assert expand_env_refs("no refs", env={}) == "no refs"


def test_validate_mcp_http_url_https_remote() -> None:
    validate_mcp_http_url("https://example.com/mcp")


def test_validate_mcp_http_url_http_localhost_ok() -> None:
    validate_mcp_http_url("http://127.0.0.1:8787/mcp")


def test_validate_mcp_http_url_http_remote_rejected() -> None:
    with pytest.raises(ConnectorError, match="HTTPS"):
        validate_mcp_http_url("http://example.com/mcp")


def test_mcp_source_id_for_uri_inline_vs_hashed() -> None:
    short = "test://a"
    assert mcp_source_id_for_uri(server_name="s", uri=short) == short
    long = "x" * 600
    sid = mcp_source_id_for_uri(server_name="s", uri=long)
    assert sid.startswith("mcp:sha256:")
    assert len(sid) < len(long)


def test_mcp_listing_revision_from_meta() -> None:
    class R:
        meta = {"etag": "abc"}

    assert mcp_listing_revision_from_resource(R()) == "etag:abc"

    class R2:
        meta = {}

    assert mcp_listing_revision_from_resource(R2()) is None


def test_mcp_incremental_skip_requires_revision() -> None:
    prev = {
        "kind": "mcp",
        "server_id": "srv",
        "uri": "u1",
        "listing_revision": "etag:v1",
    }
    fp_ok = {"kind": "mcp", "server_id": "srv", "uri": "u1", "listing_revision": "etag:v1"}
    assert mcp_incremental_can_skip(prev, fp_ok) is True
    fp_bad = {**fp_ok, "listing_revision": "etag:v2"}
    assert mcp_incremental_can_skip(prev, fp_bad) is False
    fp_no_rev = {"kind": "mcp", "server_id": "srv", "uri": "u1"}
    assert mcp_incremental_can_skip(prev, fp_no_rev) is False


def test_mcp_placeholder_non_empty() -> None:
    t = mcp_placeholder_text(uri="u://x", mime="application/octet-stream", note="binary")
    assert "u://x" in t
    assert t.strip()
