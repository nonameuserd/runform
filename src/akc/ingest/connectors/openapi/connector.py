"""OpenAPI connector for ingesting OpenAPI 3.x specs."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

from akc.ingest.connectors.base import BaseConnector
from akc.ingest.connectors.openapi.loader import load_spec_bytes, looks_like_url, parse_spec
from akc.ingest.connectors.openapi.refs import RefResolver
from akc.ingest.connectors.openapi.render import (
    render_component_schema,
    render_endpoint_index,
    render_operation,
)
from akc.ingest.exceptions import ConnectorError
from akc.ingest.models import Document

_HTTP_METHODS: Final[set[str]] = {
    "get",
    "put",
    "post",
    "delete",
    "options",
    "head",
    "patch",
    "trace",
}


@dataclass(frozen=True, slots=True)
class OpenAPIConnectorConfig:
    """Configuration for OpenAPI ingestion."""

    spec: str
    allow_urls: bool = True
    max_bytes: int = 5_000_000
    resolve_external_refs: bool = False
    max_ref_depth: int = 8
    max_resolved_chars: int = 12_000
    user_agent: str = "akc-openapi-connector/0.1"
    timeout_seconds: float = 20.0


class OpenAPIConnector(BaseConnector):
    """Ingest OpenAPI 3.x specs from local path or URL."""

    def __init__(self, *, tenant_id: str, config: OpenAPIConnectorConfig) -> None:
        super().__init__(tenant_id=tenant_id, source_type="openapi")
        if not isinstance(config.spec, str) or not config.spec.strip():
            raise TypeError("config.spec must be a non-empty string")
        if config.max_bytes <= 0:
            raise ValueError("config.max_bytes must be > 0")
        if config.max_ref_depth < 0:
            raise ValueError("config.max_ref_depth must be >= 0")
        if config.max_resolved_chars <= 0:
            raise ValueError("config.max_resolved_chars must be > 0")
        if config.timeout_seconds <= 0:
            raise ValueError("config.timeout_seconds must be > 0")
        self._config = config

    def list_sources(self) -> Iterable[str]:
        return [self._config.spec]

    def fetch(self, source_id: str) -> Iterable[Document]:
        if source_id != self._config.spec:
            raise ConnectorError("unknown source_id (expected the configured spec)")

        raw, canonical_source, base_path = load_spec_bytes(
            self._config.spec,
            allow_urls=self._config.allow_urls,
            max_bytes=self._config.max_bytes,
            user_agent=self._config.user_agent,
            timeout_seconds=self._config.timeout_seconds,
        )
        spec = parse_spec(raw, source_hint=canonical_source)
        validate_minimal_openapi(spec, source=canonical_source)

        resolver = RefResolver(
            root=spec,
            base_path=base_path,
            resolve_external=self._config.resolve_external_refs,
            max_bytes=self._config.max_bytes,
            allow_urls=self._config.allow_urls,
            user_agent=self._config.user_agent,
            timeout_seconds=self._config.timeout_seconds,
            max_depth=self._config.max_ref_depth,
        )

        docs: list[Document] = []
        ops_ctx = list(_iter_operations_with_context(spec, resolver=resolver))
        ops = [(p, m, o) for (p, m, o, _path_item) in ops_ctx]
        top_servers = spec.get("servers")
        top_security = spec.get("security")
        security_schemes: Mapping[str, Any] | None = None
        comps = spec.get("components")
        if isinstance(comps, dict):
            schemes = comps.get("securitySchemes")
            if isinstance(schemes, dict):
                security_schemes = schemes
        docs.append(
            self._make_document(
                source=canonical_source,
                logical_locator=f"{canonical_source}#endpoints",
                content=render_endpoint_index(spec, ops),
                metadata=(
                    {"url": canonical_source}
                    if looks_like_url(canonical_source)
                    else {"path": canonical_source}
                ),
            )
        )

        for path, method, op, path_item in ops_ctx:
            method_up = method.upper()
            operation_id = op.get("operationId")
            logical_locator = (
                f"{canonical_source}#/paths/{path}/{method}"
                if not operation_id
                else f"{canonical_source}#/operationId/{operation_id}"
            )
            resolved_op = resolver.resolve(op)
            op_servers = None
            if isinstance(resolved_op, dict):
                op_servers = resolved_op.get("servers")
            path_item_servers = None
            if isinstance(path_item, dict):
                path_item_servers = path_item.get("servers")
            op_security = None
            if isinstance(resolved_op, dict):
                op_security = resolved_op.get("security")
            content = render_operation(
                path=path,
                method=method_up,
                op=resolved_op,
                servers=(
                    op_servers
                    if isinstance(op_servers, list)
                    else (path_item_servers if isinstance(path_item_servers, list) else top_servers)
                ),
                security=op_security if isinstance(op_security, list) else top_security,
                security_schemes=security_schemes,
                max_chars=self._config.max_resolved_chars,
            )
            metadata: dict[str, object] = {
                "openapi_path": path,
                "openapi_method": method_up,
            }
            if isinstance(operation_id, str) and operation_id.strip():
                metadata["operation_id"] = operation_id
            tags = op.get("tags")
            if isinstance(tags, list) and all(isinstance(t, str) for t in tags):
                metadata["tags"] = list(tags)

            docs.append(
                self._make_document(
                    source=canonical_source,
                    logical_locator=logical_locator,
                    content=content,
                    metadata=metadata
                    | (
                        {"url": canonical_source}
                        if looks_like_url(canonical_source)
                        else {"path": canonical_source}
                    ),
                )
            )

        for name, schema in iter_component_schemas(spec):
            logical_locator = f"{canonical_source}#/components/schemas/{name}"
            resolved = resolver.resolve(schema)
            rendered = render_component_schema(
                name=name, schema=resolved, max_chars=self._config.max_resolved_chars
            )
            docs.append(
                self._make_document(
                    source=canonical_source,
                    logical_locator=logical_locator,
                    content=rendered,
                    metadata={
                        "component": "schema",
                        "component_name": name,
                        **(
                            {"url": canonical_source}
                            if looks_like_url(canonical_source)
                            else {"path": canonical_source}
                        ),
                    },
                )
            )

        return docs


def build_openapi_connector(
    *,
    tenant_id: str,
    spec: str | Path,
    allow_urls: bool = True,
    max_bytes: int = 5_000_000,
    resolve_external_refs: bool = False,
    max_ref_depth: int = 8,
    max_resolved_chars: int = 12_000,
    user_agent: str = "akc-openapi-connector/0.1",
    timeout_seconds: float = 20.0,
) -> OpenAPIConnector:
    spec_str = str(spec) if isinstance(spec, Path) else spec
    return OpenAPIConnector(
        tenant_id=tenant_id,
        config=OpenAPIConnectorConfig(
            spec=spec_str,
            allow_urls=allow_urls,
            max_bytes=max_bytes,
            resolve_external_refs=resolve_external_refs,
            max_ref_depth=max_ref_depth,
            max_resolved_chars=max_resolved_chars,
            user_agent=user_agent,
            timeout_seconds=timeout_seconds,
        ),
    )


def validate_minimal_openapi(spec: Mapping[str, Any], *, source: str) -> None:
    openapi = spec.get("openapi")
    if not isinstance(openapi, str) or not openapi.startswith("3."):
        raise ConnectorError(f"OpenAPI 'openapi' field must be a 3.x version string: {source}")
    paths = spec.get("paths")
    if not isinstance(paths, dict):
        raise ConnectorError(f"OpenAPI spec missing 'paths' object: {source}")


def iter_operations(spec: Mapping[str, Any]) -> Iterator[tuple[str, str, Mapping[str, Any]]]:
    return _iter_operations(spec, resolver=None)


def _iter_operations(
    spec: Mapping[str, Any], *, resolver: RefResolver | None
) -> Iterator[tuple[str, str, Mapping[str, Any]]]:
    for path, method, op, _path_item in _iter_operations_with_context(spec, resolver=resolver):
        yield path, method, op


def _iter_operations_with_context(
    spec: Mapping[str, Any], *, resolver: RefResolver | None
) -> Iterator[tuple[str, str, Mapping[str, Any], Mapping[str, Any]]]:
    paths = spec.get("paths")
    if not isinstance(paths, dict):
        return
    for path, path_item in paths.items():
        if not isinstance(path, str) or not isinstance(path_item, dict):
            continue
        if resolver is not None and "$ref" in path_item:
            # OpenAPI allows $ref on path items; resolve so we don't drop operations.
            resolved_path_item = resolver.resolve(path_item)
            if isinstance(resolved_path_item, dict):
                path_item = resolved_path_item
        for method, op in path_item.items():
            if not isinstance(method, str):
                continue
            m = method.lower()
            if m not in _HTTP_METHODS:
                continue
            if not isinstance(op, dict):
                continue
            yield path, m, op, path_item


def iter_component_schemas(spec: Mapping[str, Any]) -> Iterator[tuple[str, Mapping[str, Any]]]:
    comps = spec.get("components")
    if not isinstance(comps, dict):
        return
    schemas = comps.get("schemas")
    if not isinstance(schemas, dict):
        return
    for name, schema in schemas.items():
        if isinstance(name, str) and isinstance(schema, dict):
            yield name, schema
