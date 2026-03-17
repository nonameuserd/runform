from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.ingest.connectors.openapi import OpenAPIConnector, OpenAPIConnectorConfig
from akc.ingest.exceptions import ConnectorError


def test_openapi_connector_emits_index_and_operation_documents(tmp_path: Path) -> None:
    spec = {
        "openapi": "3.0.3",
        "info": {"title": "Example API", "version": "1.0.0"},
        "servers": [{"url": "https://api.example.com"}],
        "security": [{"ApiKeyAuth": []}],
        "paths": {
            "/users": {
                "servers": [{"url": "https://users.api.example.com"}],
                "get": {
                    "operationId": "listUsers",
                    "summary": "List users",
                    "responses": {"200": {"description": "ok"}},
                },
                "post": {
                    "summary": "Create user",
                    "servers": [{"url": "https://write.api.example.com"}],
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/UserCreate"}
                            }
                        },
                    },
                    "responses": {"201": {"description": "created"}},
                },
            }
        },
        "components": {
            "securitySchemes": {
                "ApiKeyAuth": {"type": "apiKey", "in": "header", "name": "X-API-Key"}
            },
            "schemas": {
                "UserCreate": {
                    "type": "object",
                    "properties": {"email": {"type": "string", "format": "email"}},
                    "required": ["email"],
                }
            },
        },
    }

    p = tmp_path / "openapi.json"
    p.write_text(json.dumps(spec), encoding="utf-8")

    conn = OpenAPIConnector(tenant_id="tenant-1", config=OpenAPIConnectorConfig(spec=str(p)))
    sources = list(conn.list_sources())
    assert sources == [str(p)]

    docs = list(conn.fetch(str(p)))
    assert len(docs) >= 1 + 2 + 1  # index + 2 ops + component schema

    # Discovery index
    index_docs = [d for d in docs if d.content.startswith("OpenAPI: Example API\nEndpoints:\n")]
    assert index_docs, "expected an endpoint index document"

    # Operation docs
    op_docs = [d for d in docs if "openapi_method" in d.metadata]
    assert {d.metadata["openapi_method"] for d in op_docs} == {"GET", "POST"}
    assert any("GET /users" in d.content for d in op_docs)
    assert any("POST /users" in d.content for d in op_docs)
    # Operation-level servers override path-item/top-level; path-item overrides top-level.
    assert any(
        d.metadata["openapi_method"] == "GET"
        and "servers:\n  - https://users.api.example.com" in d.content
        for d in op_docs
    )
    assert any(
        d.metadata["openapi_method"] == "POST"
        and "servers:\n  - https://write.api.example.com" in d.content
        for d in op_docs
    )
    assert any("auth:\n  - ApiKeyAuth: apiKey (header)" in d.content for d in op_docs)
    assert all(d.metadata["tenant_id"] == "tenant-1" for d in docs)
    assert all(d.metadata["source_type"] == "openapi" for d in docs)

    # Component schema doc should include resolved email field.
    comp_docs = [d for d in docs if d.metadata.get("component") == "schema"]
    assert comp_docs
    assert any("components.schemas.UserCreate" in d.content for d in comp_docs)
    assert any('"email"' in d.content for d in comp_docs)


def test_openapi_connector_rejects_non_3x_specs(tmp_path: Path) -> None:
    p = tmp_path / "bad.json"
    p.write_text(json.dumps({"openapi": "2.0", "paths": {}}), encoding="utf-8")
    conn = OpenAPIConnector(tenant_id="t", config=OpenAPIConnectorConfig(spec=str(p)))
    with pytest.raises(ConnectorError, match=r"3\.x"):
        list(conn.fetch(str(p)))


def test_openapi_connector_rejects_unknown_source_id(tmp_path: Path) -> None:
    p = tmp_path / "ok.json"
    p.write_text(json.dumps({"openapi": "3.0.0", "paths": {}}), encoding="utf-8")
    conn = OpenAPIConnector(tenant_id="t", config=OpenAPIConnectorConfig(spec=str(p)))
    with pytest.raises(ConnectorError, match=r"unknown source_id"):
        list(conn.fetch(str(tmp_path / "other.json")))
