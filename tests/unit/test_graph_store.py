from __future__ import annotations

import pytest

from akc.ingest import (
    Edge,
    GraphStoreError,
    InMemoryGraphStore,
    Node,
    SQLiteGraphStore,
)


def test_in_memory_graph_store_is_tenant_isolated_for_nodes_and_edges() -> None:
    g = InMemoryGraphStore()
    g.upsert_nodes(
        tenant_id="tenant-a",
        nodes=[Node(id="n1", type="doc", payload={"x": 1})],
    )
    g.upsert_nodes(
        tenant_id="tenant-b",
        nodes=[Node(id="n1", type="doc", payload={"x": 2})],
    )
    g.add_edges(
        tenant_id="tenant-a",
        edges=[Edge(src="n1", dst="n2", type="ref", payload={"why": "a"})],
    )
    g.add_edges(
        tenant_id="tenant-b",
        edges=[Edge(src="n1", dst="n3", type="ref", payload={"why": "b"})],
    )

    na = g.get_node(tenant_id="tenant-a", node_id="n1")
    nb = g.get_node(tenant_id="tenant-b", node_id="n1")
    assert na is not None and na.payload["x"] == 1
    assert nb is not None and nb.payload["x"] == 2

    out_a = list(g.iter_out_edges(tenant_id="tenant-a", src="n1"))
    out_b = list(g.iter_out_edges(tenant_id="tenant-b", src="n1"))
    assert [(e.dst, e.payload) for e in out_a] == [("n2", {"why": "a"})]
    assert [(e.dst, e.payload) for e in out_b] == [("n3", {"why": "b"})]


def test_graph_store_rejects_non_json_payloads() -> None:
    g = InMemoryGraphStore()
    with pytest.raises(GraphStoreError, match=r"JSON-serializable"):
        g.upsert_nodes(
            tenant_id="t",
            nodes=[Node(id="n1", type="doc", payload={"bad": object()})],
        )

    with pytest.raises(GraphStoreError, match=r"JSON-serializable"):
        g.add_edges(
            tenant_id="t",
            edges=[Edge(src="n1", dst="n2", type="ref", payload={"bad": object()})],
        )


def test_sqlite_graph_store_persists_and_is_tenant_isolated(tmp_path) -> None:  # type: ignore[no-untyped-def]
    db_path = tmp_path / "graph.sqlite3"

    g1 = SQLiteGraphStore(path=str(db_path))
    g1.upsert_nodes(tenant_id="tenant-a", nodes=[Node(id="n1", type="doc", payload={"x": 1})])
    g1.add_edges(tenant_id="tenant-a", edges=[Edge(src="n1", dst="n2", type="ref")])

    # Same node_id in different tenant must not collide.
    g1.upsert_nodes(tenant_id="tenant-b", nodes=[Node(id="n1", type="doc", payload={"x": 2})])
    g1.add_edges(tenant_id="tenant-b", edges=[Edge(src="n1", dst="n3", type="ref")])

    # Re-open to verify persistence.
    g2 = SQLiteGraphStore(path=str(db_path))

    na = g2.get_node(tenant_id="tenant-a", node_id="n1")
    nb = g2.get_node(tenant_id="tenant-b", node_id="n1")
    assert na is not None and na.payload["x"] == 1
    assert nb is not None and nb.payload["x"] == 2

    out_a = list(g2.iter_out_edges(tenant_id="tenant-a", src="n1"))
    out_b = list(g2.iter_out_edges(tenant_id="tenant-b", src="n1"))
    assert [(e.dst, e.type) for e in out_a] == [("n2", "ref")]
    assert [(e.dst, e.type) for e in out_b] == [("n3", "ref")]
