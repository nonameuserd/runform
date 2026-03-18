from __future__ import annotations

import pytest

from akc.memory.code_memory import InMemoryCodeMemoryStore, SQLiteCodeMemoryStore
from akc.memory.models import CodeArtifactRef, CodeMemoryItem, now_ms


def _item(
    *,
    tenant_id: str,
    repo_id: str,
    artifact_id: str | None,
    item_id: str,
    kind: str,
) -> CodeMemoryItem:
    t = now_ms()
    return CodeMemoryItem(
        id=item_id,
        ref=CodeArtifactRef(tenant_id=tenant_id, repo_id=repo_id, artifact_id=artifact_id),
        kind=kind,  # type: ignore[arg-type]
        content=f"content-{item_id}",
        metadata={"k": item_id},
        created_at_ms=t,
        updated_at_ms=t,
    )


def test_in_memory_code_memory_is_tenant_and_repo_isolated() -> None:
    s = InMemoryCodeMemoryStore()
    s.upsert_items(
        tenant_id="tenant-a",
        repo_id="repo",
        artifact_id=None,
        items=[
            _item(
                tenant_id="tenant-a",
                repo_id="repo",
                artifact_id=None,
                item_id="i1",
                kind="snippet",
            )
        ],
    )
    s.upsert_items(
        tenant_id="tenant-b",
        repo_id="repo",
        artifact_id=None,
        items=[
            _item(
                tenant_id="tenant-b",
                repo_id="repo",
                artifact_id=None,
                item_id="i1",
                kind="snippet",
            )
        ],
    )
    s.upsert_items(
        tenant_id="tenant-a",
        repo_id="repo-2",
        artifact_id=None,
        items=[
            _item(
                tenant_id="tenant-a",
                repo_id="repo-2",
                artifact_id=None,
                item_id="i1",
                kind="snippet",
            )
        ],
    )

    assert [i.id for i in s.list_items(tenant_id="tenant-a", repo_id="repo")] == ["i1"]
    assert [i.id for i in s.list_items(tenant_id="tenant-b", repo_id="repo")] == ["i1"]
    assert [i.id for i in s.list_items(tenant_id="tenant-a", repo_id="repo-2")] == ["i1"]


def test_code_memory_rejects_cross_tenant_or_repo_item() -> None:
    s = InMemoryCodeMemoryStore()
    bad_tenant = _item(
        tenant_id="tenant-a",
        repo_id="repo",
        artifact_id=None,
        item_id="i1",
        kind="snippet",
    )
    with pytest.raises(Exception, match=r"tenant_id mismatch"):
        s.upsert_items(tenant_id="tenant-b", repo_id="repo", artifact_id=None, items=[bad_tenant])

    bad_repo = _item(
        tenant_id="tenant-a",
        repo_id="repo",
        artifact_id=None,
        item_id="i2",
        kind="snippet",
    )
    with pytest.raises(Exception, match=r"repo_id mismatch"):
        s.upsert_items(tenant_id="tenant-a", repo_id="other", artifact_id=None, items=[bad_repo])


def test_sqlite_code_memory_persists_and_filters(tmp_path) -> None:  # type: ignore[no-untyped-def]
    db_path = tmp_path / "mem.sqlite3"
    s1 = SQLiteCodeMemoryStore(path=str(db_path))

    s1.upsert_items(
        tenant_id="t",
        repo_id="repo",
        artifact_id="a1",
        items=[
            _item(tenant_id="t", repo_id="repo", artifact_id="a1", item_id="i1", kind="snippet"),
            _item(
                tenant_id="t",
                repo_id="repo",
                artifact_id="a1",
                item_id="i2",
                kind="test_result",
            ),
        ],
    )
    s1.upsert_items(
        tenant_id="t",
        repo_id="repo",
        artifact_id=None,
        items=[
            _item(
                tenant_id="t",
                repo_id="repo",
                artifact_id=None,
                item_id="i3",
                kind="note",
            )
        ],
    )

    s2 = SQLiteCodeMemoryStore(path=str(db_path))
    all_ids = [i.id for i in s2.list_items(tenant_id="t", repo_id="repo", limit=10)]
    assert set(all_ids) == {"i1", "i2", "i3"}

    a1_ids = [
        i.id for i in s2.list_items(tenant_id="t", repo_id="repo", artifact_id="a1", limit=10)
    ]
    assert set(a1_ids) == {"i1", "i2"}

    kind_ids = [
        i.id
        for i in s2.list_items(
            tenant_id="t",
            repo_id="repo",
            kind_filter=("test_result",),
            limit=10,  # type: ignore[arg-type]
        )
    ]
    assert kind_ids == ["i2"]
