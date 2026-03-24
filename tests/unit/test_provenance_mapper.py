from __future__ import annotations

from akc.compile.provenance_mapper import build_doc_id_to_provenance_map


def test_provenance_mapper_openapi_operation_kind_and_metadata() -> None:
    out = build_doc_id_to_provenance_map(
        tenant_id="tenant-a",
        documents=[
            {
                "doc_id": "doc-openapi-1",
                "title": "https://example.com/openapi.json",
                "content": "operation body",
                "metadata": {
                    "source_type": "openapi",
                    "source": "https://example.com/openapi.json",
                    "connector_id": "openapi",
                    "operation_id": "listWidgets",
                    "openapi_path": "/widgets",
                    "openapi_method": "GET",
                    "url": "https://example.com/openapi.json",
                },
            }
        ],
    )
    ptr = out["doc-openapi-1"]
    assert ptr["kind"] == "openapi_operation"
    assert ptr["source_id"] == "doc-openapi-1"
    assert ptr["metadata"]["openapi_operation_id"] == "listWidgets"
    assert ptr["metadata"]["connector_id"] == "openapi"
    assert "source_uri" in ptr["metadata"]


def test_provenance_mapper_slack_thread_uses_message_kind() -> None:
    out = build_doc_id_to_provenance_map(
        tenant_id="tenant-a",
        documents=[
            {
                "doc_id": "doc-slack-1",
                "title": "slack",
                "content": "thread text",
                "metadata": {
                    "source_type": "slack",
                    "source": "slack:C123",
                    "connector_id": "slack",
                    "channel": "C123",
                    "thread_id": "1234567890.123456",
                },
            }
        ],
    )
    ptr = out["doc-slack-1"]
    assert ptr["kind"] == "message"
    assert ptr["metadata"]["message_thread_id"] == "1234567890.123456"
    assert ptr["metadata"]["slack_channel_id"] == "C123"
