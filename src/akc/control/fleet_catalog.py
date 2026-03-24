"""Cross-shard read-only aggregation of :class:`OperationsIndex` catalogs."""

from __future__ import annotations

import heapq
from collections.abc import Iterator, Sequence
from typing import Any, cast

from akc.control.fleet_config import FleetShardConfig
from akc.control.operations_index import OperationsIndex, operations_sqlite_path
from akc.memory.models import JSONValue, normalize_repo_id, require_non_empty

_MERGE_PAGE = 64


def shard_accepts_tenant(shard: FleetShardConfig, tenant_id: str) -> bool:
    t = tenant_id.strip()
    if "*" in shard.tenant_allowlist:
        return True
    return t in shard.tenant_allowlist


def _iter_shard_run_keys(
    shard: FleetShardConfig,
    *,
    tenant_id: str,
    repo_id: str | None = None,
    since_ms: int | None = None,
    until_ms: int | None = None,
    stable_intent_sha256: str | None = None,
    has_recompile_triggers: bool | None = None,
    runtime_evidence_present: bool | None = None,
) -> Iterator[tuple[int, str, str, str, FleetShardConfig, dict[str, JSONValue]]]:
    if not shard_accepts_tenant(shard, tenant_id):
        return
    db_path = operations_sqlite_path(outputs_root=shard.outputs_root, tenant_id=tenant_id)
    if not db_path.is_file():
        return
    idx = OperationsIndex(sqlite_path=db_path)
    offset = 0
    while True:
        batch = idx.list_runs(
            tenant_id=tenant_id,
            repo_id=repo_id,
            since_ms=since_ms,
            until_ms=until_ms,
            stable_intent_sha256=stable_intent_sha256,
            has_recompile_triggers=has_recompile_triggers,
            runtime_evidence_present=runtime_evidence_present,
            limit=_MERGE_PAGE,
            offset=offset,
        )
        if not batch:
            break
        for row in batch:
            ms = int(cast(int, row["updated_at_ms"]))
            rid = str(row["repo_id"])
            run_id = str(row["run_id"])
            yield (-ms, shard.id, rid, run_id, shard, row)
        offset += _MERGE_PAGE
        if len(batch) < _MERGE_PAGE:
            break


def fleet_list_runs_merged(
    shards: Sequence[FleetShardConfig],
    *,
    tenant_id: str,
    repo_id: str | None = None,
    since_ms: int | None = None,
    until_ms: int | None = None,
    stable_intent_sha256: str | None = None,
    has_recompile_triggers: bool | None = None,
    runtime_evidence_present: bool | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Merge per-shard operations indexes (descending ``updated_at_ms``) with k-way merge."""

    require_non_empty(tenant_id, name="tenant_id")
    lim = max(1, min(int(limit), 500))
    iters = [
        _iter_shard_run_keys(
            s,
            tenant_id=tenant_id,
            repo_id=repo_id,
            since_ms=since_ms,
            until_ms=until_ms,
            stable_intent_sha256=stable_intent_sha256,
            has_recompile_triggers=has_recompile_triggers,
            runtime_evidence_present=runtime_evidence_present,
        )
        for s in shards
    ]
    merged = heapq.merge(*iters)
    out: list[dict[str, Any]] = []
    for _tup in merged:
        _neg_ms, _sid, _rid, _run_id, shard, row = _tup
        item = dict(row)
        item["shard_id"] = shard.id
        item["outputs_root"] = str(shard.outputs_root)
        out.append(item)
        if len(out) >= lim:
            break
    return out


def fleet_resolve_label_write_shard(
    shards: Sequence[FleetShardConfig],
    *,
    tenant_id: str,
    repo_id: str,
    run_id: str,
) -> FleetShardConfig | None:
    """Pick one shard to write ``run_labels`` for this run.

    Prefer a shard that already indexes the run (declaration order). If the run is not
    indexed yet, use the first shard whose tenant allowlist includes ``tenant_id``.
    """

    require_non_empty(tenant_id, name="tenant_id")
    rnorm = normalize_repo_id(repo_id)
    rid = str(run_id).strip()
    candidates = [s for s in shards if shard_accepts_tenant(s, tenant_id)]
    if not candidates:
        return None
    for shard in candidates:
        db_path = operations_sqlite_path(outputs_root=shard.outputs_root, tenant_id=tenant_id)
        if not db_path.is_file():
            continue
        idx = OperationsIndex(sqlite_path=db_path)
        if idx.get_run(tenant_id=tenant_id.strip(), repo_id=rnorm, run_id=rid) is not None:
            return shard
    return candidates[0]


def fleet_get_run(
    shards: Sequence[FleetShardConfig],
    *,
    tenant_id: str,
    repo_id: str,
    run_id: str,
) -> dict[str, Any] | None:
    """Return first matching indexed run across shards (deterministic: shard declaration order)."""

    require_non_empty(tenant_id, name="tenant_id")
    rnorm = normalize_repo_id(repo_id)
    rid = str(run_id).strip()
    for shard in shards:
        if not shard_accepts_tenant(shard, tenant_id):
            continue
        db_path = operations_sqlite_path(outputs_root=shard.outputs_root, tenant_id=tenant_id)
        if not db_path.is_file():
            continue
        idx = OperationsIndex(sqlite_path=db_path)
        row = idx.get_run(tenant_id=tenant_id, repo_id=rnorm, run_id=rid)
        if row is not None:
            out = dict(row)
            out["shard_id"] = shard.id
            out["outputs_root"] = str(shard.outputs_root)
            return out
    return None


def _iter_shard_delivery_keys(
    shard: FleetShardConfig,
    *,
    tenant_id: str,
    repo_id: str | None = None,
    since_ms: int | None = None,
    until_ms: int | None = None,
) -> Iterator[tuple[int, str, str, str, FleetShardConfig, dict[str, JSONValue]]]:
    if not shard_accepts_tenant(shard, tenant_id):
        return
    db_path = operations_sqlite_path(outputs_root=shard.outputs_root, tenant_id=tenant_id)
    if not db_path.is_file():
        return
    idx = OperationsIndex(sqlite_path=db_path)
    offset = 0
    while True:
        batch = idx.list_deliveries(
            tenant_id=tenant_id,
            repo_id=repo_id,
            since_ms=since_ms,
            until_ms=until_ms,
            limit=_MERGE_PAGE,
            offset=offset,
        )
        if not batch:
            break
        for row in batch:
            ms = int(cast(int, row["updated_at_ms"]))
            rid = str(row["repo_id"])
            did = str(row["delivery_id"])
            yield (-ms, shard.id, rid, did, shard, row)
        offset += _MERGE_PAGE
        if len(batch) < _MERGE_PAGE:
            break


def fleet_list_deliveries_merged(
    shards: Sequence[FleetShardConfig],
    *,
    tenant_id: str,
    repo_id: str | None = None,
    since_ms: int | None = None,
    until_ms: int | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Merge per-shard delivery session indexes (descending ``updated_at_ms``)."""

    require_non_empty(tenant_id, name="tenant_id")
    lim = max(1, min(int(limit), 500))
    iters = [
        _iter_shard_delivery_keys(
            s,
            tenant_id=tenant_id,
            repo_id=repo_id,
            since_ms=since_ms,
            until_ms=until_ms,
        )
        for s in shards
    ]
    merged = heapq.merge(*iters)
    out: list[dict[str, Any]] = []
    for _tup in merged:
        _neg_ms, _sid, _rid, _did, shard, row = _tup
        item = dict(row)
        item["shard_id"] = shard.id
        item["outputs_root"] = str(shard.outputs_root)
        out.append(item)
        if len(out) >= lim:
            break
    return out


def fleet_get_delivery(
    shards: Sequence[FleetShardConfig],
    *,
    tenant_id: str,
    repo_id: str,
    delivery_id: str,
) -> dict[str, Any] | None:
    """Return first matching indexed delivery across shards (shard declaration order)."""

    require_non_empty(tenant_id, name="tenant_id")
    rnorm = normalize_repo_id(repo_id)
    did = str(delivery_id).strip()
    for shard in shards:
        if not shard_accepts_tenant(shard, tenant_id):
            continue
        db_path = operations_sqlite_path(outputs_root=shard.outputs_root, tenant_id=tenant_id)
        if not db_path.is_file():
            continue
        idx = OperationsIndex(sqlite_path=db_path)
        row = idx.get_delivery(tenant_id=tenant_id, repo_id=rnorm, delivery_id=did)
        if row is not None:
            out = dict(row)
            out["shard_id"] = shard.id
            out["outputs_root"] = str(shard.outputs_root)
            return out
    return None
