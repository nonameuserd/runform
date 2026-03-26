from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol

from akc.control_bot.command_engine import InboundEvent
from akc.memory.models import require_non_empty


class EventStoreError(Exception):
    """Raised when inbound event persistence fails."""


DedupReason = Literal["new", "duplicate.event_id", "duplicate.payload_hash"]


@dataclass(frozen=True, slots=True)
class PersistResult:
    is_duplicate: bool
    reason: DedupReason


class InboundEventStore(Protocol):
    def persist(self, ev: InboundEvent) -> PersistResult: ...


_SCHEMA = """
CREATE TABLE IF NOT EXISTS inbound_events (
  channel TEXT NOT NULL,
  event_id TEXT NOT NULL,
  principal_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  raw_text TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  received_at_ms INTEGER NOT NULL,
  persisted_at_ms INTEGER NOT NULL,
  PRIMARY KEY (channel, event_id)
);
CREATE INDEX IF NOT EXISTS idx_inbound_events_tenant_received
  ON inbound_events(tenant_id, received_at_ms DESC);

-- Dedupe key for channels that retry with a different event_id.
-- Tenant isolation boundary is enforced in the key.
CREATE TABLE IF NOT EXISTS inbound_event_payload_dedupe (
  tenant_id TEXT NOT NULL,
  channel TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  first_event_id TEXT NOT NULL,
  first_seen_at_ms INTEGER NOT NULL,
  last_seen_at_ms INTEGER NOT NULL,
  seen_count INTEGER NOT NULL,
  PRIMARY KEY (tenant_id, channel, payload_hash)
);
CREATE INDEX IF NOT EXISTS idx_inbound_event_payload_dedupe_last_seen
  ON inbound_event_payload_dedupe(tenant_id, channel, last_seen_at_ms DESC);
"""


@dataclass(slots=True)
class SqliteInboundEventStore:
    """Durable inbound event persistence.

    This store is intentionally append-only (idempotent) to support retry safety:
    duplicate `(channel, event_id)` inserts are ignored.
    """

    sqlite_path: Path

    def __post_init__(self) -> None:
        p = Path(self.sqlite_path).expanduser()
        self.sqlite_path = p
        parent = p.parent
        if str(parent).strip():
            parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.sqlite_path), timeout=5.0, isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def _init_db(self) -> None:
        try:
            with self._connect() as conn:
                conn.executescript(_SCHEMA)
        except Exception as e:  # pragma: no cover
            raise EventStoreError(f"failed to init sqlite event store: {e}") from e

    def persist(self, ev: InboundEvent) -> PersistResult:
        evn = ev.normalized()
        evn.validate()
        require_non_empty(str(self.sqlite_path), name="sqlite_path")
        now_ms = int(time.time() * 1000)
        try:
            with self._connect() as conn:
                conn.execute("BEGIN IMMEDIATE;")
                # Safety: if (channel,event_id) already exists, payload_hash must match.
                row = conn.execute(
                    "SELECT payload_hash FROM inbound_events WHERE channel=? AND event_id=?",
                    (evn.channel, evn.event_id),
                ).fetchone()
                if row and row[0] and str(row[0]).strip().lower() != evn.payload_hash.strip().lower():
                    conn.execute("ROLLBACK;")
                    raise EventStoreError("inbound event_id replayed with different payload_hash")

                # Dedupe by (tenant, channel, payload_hash) first.
                # This handles channel retries that do not preserve a stable event_id.
                cur_dedupe = conn.execute(
                    """
                    INSERT OR IGNORE INTO inbound_event_payload_dedupe(
                      tenant_id,channel,payload_hash,first_event_id,first_seen_at_ms,last_seen_at_ms,seen_count
                    ) VALUES (?,?,?,?,?,?,?)
                    """,
                    (
                        evn.tenant_id,
                        str(evn.channel),
                        evn.payload_hash.strip().lower(),
                        evn.event_id,
                        int(evn.received_at_ms),
                        now_ms,
                        1,
                    ),
                )
                is_payload_dup = int(cur_dedupe.rowcount or 0) == 0
                if is_payload_dup:
                    conn.execute(
                        """
                        UPDATE inbound_event_payload_dedupe
                           SET last_seen_at_ms=?,
                               seen_count=seen_count+1
                         WHERE tenant_id=? AND channel=? AND payload_hash=?
                        """,
                        (now_ms, evn.tenant_id, str(evn.channel), evn.payload_hash.strip().lower()),
                    )
                    conn.execute("COMMIT;")
                    # If the payload has already been seen for this tenant+channel, treat as duplicate,
                    # regardless of whether event_id differs.
                    return PersistResult(is_duplicate=True, reason="duplicate.payload_hash")

                # First time we've seen this payload for this tenant+channel: persist full event row.
                cur = conn.execute(
                    """
                    INSERT OR IGNORE INTO inbound_events(
                      channel,event_id,principal_id,tenant_id,raw_text,payload_hash,received_at_ms,persisted_at_ms
                    ) VALUES (?,?,?,?,?,?,?,?)
                    """,
                    (
                        evn.channel,
                        evn.event_id,
                        evn.principal_id,
                        evn.tenant_id,
                        evn.raw_text,
                        evn.payload_hash,
                        int(evn.received_at_ms),
                        now_ms,
                    ),
                )
                conn.execute("COMMIT;")
                if int(cur.rowcount or 0) == 0:
                    # Same (channel,event_id) replay with same payload_hash.
                    return PersistResult(is_duplicate=True, reason="duplicate.event_id")
                return PersistResult(is_duplicate=False, reason="new")
        except Exception as e:
            raise EventStoreError(f"failed to persist inbound event: {e}") from e
