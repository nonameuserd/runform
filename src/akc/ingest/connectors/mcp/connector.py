"""MCP resources → :class:`~akc.ingest.models.Document`."""

from __future__ import annotations

import base64
import hashlib
import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pydantic import AnyUrl

from akc.ingest.connectors.base import BaseConnector
from akc.ingest.connectors.mcp.client import (
    list_all_resources,
    read_resource_contents_to_parts,
    run_mcp_session_sync,
)
from akc.ingest.connectors.mcp.config import ResolvedMcpServer, resolve_mcp_server
from akc.ingest.exceptions import ConnectorError
from akc.ingest.models import Document, content_hash

logger = logging.getLogger(__name__)

STATIC_PROMPT_SOURCE_ID = "akc://static-prompt"
MAX_INLINE_URI_LEN = 512


def mcp_source_id_for_uri(*, server_name: str, uri: str) -> str:
    """Use the resource URI as ``source_id`` when short enough; otherwise a deterministic hash id."""

    if len(uri) <= MAX_INLINE_URI_LEN:
        return uri
    digest = hashlib.sha256(f"{server_name}\n{uri}".encode()).hexdigest()
    return f"mcp:sha256:{digest}"


def mcp_listing_revision_from_resource(resource: object) -> str | None:
    """Return a stable revision string when the server exposes hints in ``meta``."""

    meta = getattr(resource, "meta", None)
    if isinstance(meta, dict):
        for key in ("etag", "lastModified", "rev", "revision", "version", "hash"):
            val = meta.get(key)
            if isinstance(val, str) and val.strip():
                return f"{key}:{val.strip()}"
            if isinstance(val, (int, float)):
                return f"{key}:{val}"
    return None


def mcp_placeholder_text(*, uri: str, mime: str | None, note: str) -> str:
    body = (
        f"[AKC MCP ingest placeholder: {note}]\n"
        f"resource_uri={uri}\n"
        f"mime_type={mime or ''}\n"
        "This resource was not decoded as UTF-8 text; metadata is still indexed.\n"
    )
    if not body.strip():
        raise ConnectorError("internal: placeholder must be non-empty")
    return body


def _mime_is_probably_text(mime: str | None) -> bool:
    if mime is None or not str(mime).strip():
        return True
    base = str(mime).lower().split(";", 1)[0].strip()
    if base.startswith("text/"):
        return True
    return base in {
        "application/json",
        "application/javascript",
        "application/xml",
        "application/xhtml+xml",
        "image/svg+xml",
    }


def _decode_blob_if_text(*, blob_b64: str, mime: str | None) -> tuple[str, bool]:
    try:
        raw = base64.b64decode(blob_b64, validate=True)
    except Exception:
        return "", False
    if not _mime_is_probably_text(mime):
        return "", False
    try:
        return raw.decode("utf-8"), True
    except UnicodeDecodeError:
        try:
            return raw.decode("utf-8", errors="replace"), True
        except Exception:
            return "", False


@dataclass(frozen=True, slots=True)
class McpConnectorOptions:
    uri_prefix: str | None = None
    static_prompt: str | None = None


class McpConnector(BaseConnector):
    """Ingest MCP ``resources/list`` + ``resources/read`` into documents."""

    def __init__(
        self,
        *,
        tenant_id: str,
        server: ResolvedMcpServer,
        options: McpConnectorOptions | None = None,
        timeout_s: float | None = 120.0,
    ) -> None:
        super().__init__(tenant_id=tenant_id, source_type="mcp")
        self._server = server
        self._options = options or McpConnectorOptions()
        self._timeout_s = timeout_s
        self._source_to_uri: dict[str, str] = {}
        self._resources_by_uri: dict[str, object] = {}
        self._listed = False

    @property
    def server_id(self) -> str:
        return self._server.name

    def _uri_allowed(self, uri: str) -> bool:
        p = self._options.uri_prefix
        if not p:
            return True
        return uri.startswith(p)

    def _ensure_listing(self) -> None:
        if self._listed:
            return

        async def _list(session: Any) -> list[object]:
            return await list_all_resources(session)

        resources = run_mcp_session_sync(self._server, _list, timeout_s=self._timeout_s)
        self._source_to_uri.clear()
        self._resources_by_uri.clear()

        for r in resources:
            uri_val = getattr(r, "uri", None)
            uri_str = str(uri_val) if uri_val is not None else ""
            if not uri_str.strip():
                logger.warning("Skipping MCP resource with empty uri")
                continue
            if not self._uri_allowed(uri_str):
                continue
            sid = mcp_source_id_for_uri(server_name=self._server.name, uri=uri_str)
            self._resources_by_uri[uri_str] = r
            self._source_to_uri[sid] = uri_str

        if self._options.static_prompt is not None and str(self._options.static_prompt).strip():
            self._source_to_uri[STATIC_PROMPT_SOURCE_ID] = STATIC_PROMPT_SOURCE_ID

        self._listed = True

    def list_sources(self) -> Iterable[str]:
        self._ensure_listing()
        return sorted(self._source_to_uri.keys())

    def listing_fingerprint(self, source_id: str) -> dict[str, Any]:
        """Fingerprint from ``resources/list`` (before ``resources/read``)."""

        self._ensure_listing()
        uri = self._source_to_uri.get(source_id)
        if uri is None:
            return {"kind": "mcp", "server_id": self._server.name, "uri": source_id, "missing": True}
        if source_id == STATIC_PROMPT_SOURCE_ID:
            h = content_hash(str(self._options.static_prompt or ""))
            return {
                "kind": "mcp",
                "server_id": self._server.name,
                "uri": STATIC_PROMPT_SOURCE_ID,
                "listing_revision": f"static_prompt:{h}",
            }
        resource = self._resources_by_uri.get(uri)
        revision = mcp_listing_revision_from_resource(resource) if resource is not None else None
        mime = getattr(resource, "mimeType", None) if resource is not None else None
        size = getattr(resource, "size", None) if resource is not None else None
        fp: dict[str, Any] = {
            "kind": "mcp",
            "server_id": self._server.name,
            "uri": uri,
            "source_id": source_id,
        }
        if isinstance(mime, str) and mime.strip():
            fp["mimeType"] = mime
        if isinstance(size, int) and size >= 0:
            fp["size"] = size
        if revision is not None:
            fp["listing_revision"] = revision
        return fp

    def finalize_fingerprint_after_fetch(self, fp: Mapping[str, Any], documents: list[Document]) -> dict[str, Any]:
        """Attach content hash after a successful read (for drift / future use)."""

        out = dict(fp)
        if not documents:
            return out
        meta = documents[0].metadata
        ch = meta.get("mcp_content_sha256")
        if isinstance(ch, str) and ch.strip():
            out["content_sha256"] = ch.strip()
        return out

    def fetch(self, source_id: str) -> Iterable[Document]:
        self._ensure_listing()
        if source_id == STATIC_PROMPT_SOURCE_ID:
            text = str(self._options.static_prompt or "").strip()
            if not text:
                raise ConnectorError("static prompt is empty")
            loc = f"mcp://{self._server.name}/{STATIC_PROMPT_SOURCE_ID}"
            doc = self._make_document(
                source=source_id,
                logical_locator=loc,
                content=text,
                metadata={
                    "connector_id": "mcp",
                    "mcp_server_id": self._server.name,
                    "mcp_uri": STATIC_PROMPT_SOURCE_ID,
                    "mime_type": "text/plain",
                    "mcp_content_sha256": content_hash(text),
                },
            )
            return [doc]

        uri = self._source_to_uri.get(source_id)
        if uri is None:
            raise ConnectorError(f"unknown MCP source_id: {source_id!r}")

        resource = self._resources_by_uri.get(uri)
        list_mime = getattr(resource, "mimeType", None) if resource is not None else None

        async def _read(session: Any) -> object:
            return await session.read_resource(AnyUrl(uri))

        read_result = run_mcp_session_sync(self._server, _read, timeout_s=self._timeout_s)
        parts = read_resource_contents_to_parts(read_result)

        try:
            from mcp.types import BlobResourceContents, TextResourceContents
        except ImportError as e:  # pragma: no cover
            raise ConnectorError("mcp package is required for MCP ingestion") from e

        texts: list[str] = []
        used_mime: str | None = list_mime if isinstance(list_mime, str) and list_mime.strip() else None

        for part in parts:
            if isinstance(part, TextResourceContents):
                t = part.text
                if isinstance(t, str) and t.strip():
                    texts.append(t)
                pm = getattr(part, "mimeType", None)
                if isinstance(pm, str) and pm.strip():
                    used_mime = used_mime or pm
            elif isinstance(part, BlobResourceContents):
                b64 = part.blob
                pm = getattr(part, "mimeType", None)
                mime_part = pm if isinstance(pm, str) and pm.strip() else used_mime
                decoded, ok = _decode_blob_if_text(blob_b64=b64, mime=mime_part)
                if ok and decoded.strip():
                    texts.append(decoded)
                elif not ok:
                    logger.warning(
                        "Skipping non-text MCP blob resource part uri=%s mime=%s",
                        uri,
                        mime_part,
                    )
                    texts.append(
                        mcp_placeholder_text(
                            uri=uri,
                            mime=mime_part,
                            note="binary or non-text blob part",
                        )
                    )

        if not texts:
            content = mcp_placeholder_text(uri=uri, mime=used_mime, note="empty or unreadable resource")
        else:
            content = "\n\n".join(texts).strip()
            if not content:
                content = mcp_placeholder_text(uri=uri, mime=used_mime, note="empty decoded content")

        combined_hash = content_hash(content)
        title = getattr(resource, "name", None) if resource is not None else None
        description = getattr(resource, "description", None) if resource is not None else None

        logical = f"mcp://{self._server.name}/{uri}"
        meta: dict[str, object] = {
            "connector_id": "mcp",
            "mcp_server_id": self._server.name,
            "mcp_uri": uri,
            "mcp_source_id": source_id,
            "mcp_content_sha256": combined_hash,
        }
        if used_mime is not None:
            meta["mime_type"] = used_mime
        if isinstance(title, str) and title.strip():
            meta["mcp_resource_name"] = title.strip()
        if isinstance(description, str) and description.strip():
            meta["mcp_resource_description"] = description.strip()

        doc = self._make_document(
            source=source_id,
            logical_locator=logical,
            content=content,
            metadata=meta,
        )
        return [doc]


def build_mcp_connector(
    *,
    tenant_id: str,
    input_value: str,
    config_path: str | Path,
    uri_prefix: str | None = None,
    static_prompt: str | None = None,
    timeout_s: float | None = 120.0,
) -> McpConnector:
    """Build a :class:`McpConnector` from CLI-style *input_value* (server name or JSON path)."""

    path = Path(config_path).expanduser()
    server = resolve_mcp_server(input_value=input_value, config_path=path)
    opts = McpConnectorOptions(
        uri_prefix=str(uri_prefix).strip() if isinstance(uri_prefix, str) and uri_prefix.strip() else None,
        static_prompt=str(static_prompt).strip() if isinstance(static_prompt, str) and static_prompt.strip() else None,
    )
    return McpConnector(tenant_id=tenant_id, server=server, options=opts, timeout_s=timeout_s)


def mcp_incremental_can_skip(prev: Mapping[str, Any], listing_fp: Mapping[str, Any]) -> bool:
    """Return True when listing metadata indicates the resource has not changed."""

    if prev.get("kind") != "mcp" or listing_fp.get("kind") != "mcp":
        return False
    if prev.get("server_id") != listing_fp.get("server_id"):
        return False
    if listing_fp.get("missing"):
        return False
    rev = listing_fp.get("listing_revision")
    if not isinstance(rev, str) or not rev.strip():
        return False
    return prev.get("listing_revision") == rev and prev.get("uri") == listing_fp.get("uri")


__all__ = [
    "McpConnector",
    "McpConnectorOptions",
    "STATIC_PROMPT_SOURCE_ID",
    "build_mcp_connector",
    "mcp_incremental_can_skip",
    "mcp_listing_revision_from_resource",
    "mcp_placeholder_text",
    "mcp_source_id_for_uri",
]
