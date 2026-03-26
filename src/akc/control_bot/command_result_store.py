from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol

from akc.control_bot.approval_workflow import stable_args_fingerprint
from akc.control_bot.command_engine import CommandResult, InboundEvent
from akc.memory.models import JSONValue, require_non_empty


class CommandResultStoreError(Exception):
    """Raised when command result persistence or idempotency checks fail."""


ExecutionStatus = Literal["started", "finished"]


@dataclass(frozen=True, slots=True)
class StoredCommandResult:
    """Durable record for retry-safe command execution."""

    tenant_id: str
    request_hash: str
    channel: str
    event_id: str
    payload_hash: str
    principal_id: str
    action_id: str
    args_hash: str
    started_at_ms: int
    finished_at_ms: int | None = None
    status: ExecutionStatus = "started"
    # Result is stored as JSON to be replayable.
    result_json: str | None = None
    outcome_hash: str | None = None

    def validate(self) -> None:
        require_non_empty(self.tenant_id, name="tenant_id")
        require_non_empty(self.request_hash, name="request_hash")
        require_non_empty(self.channel, name="channel")
        require_non_empty(self.event_id, name="event_id")
        require_non_empty(self.payload_hash, name="payload_hash")
        require_non_empty(self.principal_id, name="principal_id")
        require_non_empty(self.action_id, name="action_id")
        require_non_empty(self.args_hash, name="args_hash")
        if self.status not in ("started", "finished"):
            raise ValueError("invalid status")
        if self.status == "finished":
            require_non_empty(self.result_json or "", name="result_json")
            require_non_empty(self.outcome_hash or "", name="outcome_hash")


class CommandResultStore(Protocol):
    def try_begin(self, *, ev: InboundEvent, action_id: str, args: dict[str, JSONValue], request_hash: str) -> bool: ...

    def finish(self, *, request_hash: str, result: CommandResult, finished_at_ms: int | None = None) -> None: ...

    def get_finished(self, *, request_hash: str) -> CommandResult | None: ...

    def ensure_event_payload_hash(self, *, channel: str, event_id: str, payload_hash: str) -> None: ...


def stable_command_request_hash(*, ev: InboundEvent, action_id: str, args: dict[str, JSONValue]) -> str:
    """Compute deterministic request hash for retry safety.

    Intentionally includes:
    - tenant_id (tenant isolation boundary)
    - channel + event_id (channel retry key)
    - payload_hash (detect event_id reuse with different content)
    - action_id + args (dedupe within same payload)
    """
    import hashlib

    evn = ev.normalized()
    evn.validate()
    aid = str(action_id or "").strip()
    require_non_empty(aid, name="action_id")
    args_hash = stable_args_fingerprint(args)
    base = "|".join(
        [
            "v1",
            evn.tenant_id.strip(),
            evn.channel.strip(),
            evn.event_id.strip(),
            evn.payload_hash.strip().lower(),
            aid.lower(),
            args_hash,
        ]
    ).encode("utf-8")
    return hashlib.sha256(base).hexdigest()


def stable_command_outcome_hash(result: CommandResult) -> str:
    import hashlib

    obj: dict[str, object] = {
        "ok": bool(result.ok),
        "action_id": str(result.action_id or "").strip(),
        "message": str(result.message or "").strip(),
        "data": result.data or {},
        "request_id": str(result.request_id) if result.request_id is not None else None,
        "status": str(result.status),
    }
    b = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(b).hexdigest()


def _result_to_json(result: CommandResult) -> str:
    obj: dict[str, object] = {
        "ok": bool(result.ok),
        "action_id": str(result.action_id or "").strip(),
        "message": str(result.message or "").strip(),
        "data": result.data or {},
        "request_id": str(result.request_id) if result.request_id is not None else None,
        "status": str(result.status),
    }
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _json_to_result(s: str) -> CommandResult:
    try:
        obj = json.loads(str(s or "") or "{}")
    except Exception as e:
        raise CommandResultStoreError(f"invalid stored result json: {e}") from e
    if not isinstance(obj, dict):
        raise CommandResultStoreError("stored result json must be an object")
    ok = bool(obj.get("ok"))
    action_id = str(obj.get("action_id") or "").strip()
    message = str(obj.get("message") or "").strip()
    data_raw = obj.get("data")
    data: dict[str, JSONValue] | None = data_raw if isinstance(data_raw, dict) else None
    request_id = obj.get("request_id")
    rid = str(request_id).strip() if request_id is not None and str(request_id).strip() else None
    status = str(obj.get("status") or "executed")
    return CommandResult(ok=ok, action_id=action_id, message=message, data=data, request_id=rid, status=status)  # type: ignore[arg-type]


_SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS command_results (
  tenant_id TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  channel TEXT NOT NULL,
  event_id TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  principal_id TEXT NOT NULL,
  action_id TEXT NOT NULL,
  args_hash TEXT NOT NULL,
  started_at_ms INTEGER NOT NULL,
  finished_at_ms INTEGER,
  status TEXT NOT NULL,
  result_json TEXT,
  outcome_hash TEXT,
  PRIMARY KEY (tenant_id, request_hash)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_command_results_event
  ON command_results(tenant_id, channel, event_id);
"""


@dataclass(slots=True)
class SqliteCommandResultStore:
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
                conn.executescript(_SQLITE_SCHEMA)
        except Exception as e:  # pragma: no cover
            raise CommandResultStoreError(f"failed to init sqlite command result store: {e}") from e

    def ensure_event_payload_hash(self, *, channel: str, event_id: str, payload_hash: str) -> None:
        # We enforce this by checking any existing row for (tenant_id,channel,event_id) during try_begin.
        # This method exists for symmetry/testing and future ingress hardening.
        require_non_empty(channel, name="channel")
        require_non_empty(event_id, name="event_id")
        require_non_empty(payload_hash, name="payload_hash")

    def try_begin(self, *, ev: InboundEvent, action_id: str, args: dict[str, JSONValue], request_hash: str) -> bool:
        evn = ev.normalized()
        evn.validate()
        require_non_empty(request_hash, name="request_hash")
        aid = str(action_id or "").strip()
        require_non_empty(aid, name="action_id")
        args_hash = stable_args_fingerprint(args).strip()
        t = evn.tenant_id.strip()
        now_ms = int(time.time() * 1000)
        try:
            with self._connect() as conn:
                conn.execute("BEGIN IMMEDIATE;")
                # Safety check: if event_id already used for this tenant+channel, payload_hash must match.
                row = conn.execute(
                    """
                    SELECT payload_hash
                      FROM command_results
                     WHERE tenant_id=? AND channel=? AND event_id=?
                     LIMIT 1
                    """,
                    (t, str(evn.channel), evn.event_id),
                ).fetchone()
                if row and row[0] and str(row[0]).strip().lower() != evn.payload_hash.strip().lower():
                    conn.execute("ROLLBACK;")
                    raise CommandResultStoreError("inbound event_id replayed with different payload_hash")
                cur = conn.execute(
                    """
                    INSERT OR IGNORE INTO command_results(
                      tenant_id,request_hash,channel,event_id,payload_hash,principal_id,action_id,args_hash,
                      started_at_ms,finished_at_ms,status,result_json,outcome_hash
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        t,
                        request_hash.strip().lower(),
                        str(evn.channel),
                        evn.event_id,
                        evn.payload_hash.strip().lower(),
                        evn.principal_id,
                        aid,
                        args_hash,
                        now_ms,
                        None,
                        "started",
                        None,
                        None,
                    ),
                )
                conn.execute("COMMIT;")
                return int(cur.rowcount or 0) == 1
        except CommandResultStoreError:
            raise
        except Exception as e:
            raise CommandResultStoreError(f"failed to begin command execution: {e}") from e

    def finish(self, *, request_hash: str, result: CommandResult, finished_at_ms: int | None = None) -> None:
        require_non_empty(request_hash, name="request_hash")
        ms = int(finished_at_ms if finished_at_ms is not None else time.time() * 1000)
        rjson = _result_to_json(result)
        oh = stable_command_outcome_hash(result)
        try:
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE command_results
                       SET finished_at_ms=?,
                           status='finished',
                           result_json=?,
                           outcome_hash=?
                     WHERE request_hash=?
                    """,
                    (ms, rjson, oh, request_hash.strip().lower()),
                )
        except Exception as e:
            raise CommandResultStoreError(f"failed to finish command execution: {e}") from e

    def get_finished(self, *, request_hash: str) -> CommandResult | None:
        require_non_empty(request_hash, name="request_hash")
        try:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT result_json
                      FROM command_results
                     WHERE request_hash=? AND status='finished'
                    """,
                    (request_hash.strip().lower(),),
                ).fetchone()
        except Exception as e:
            raise CommandResultStoreError(f"failed to load command result: {e}") from e
        if not row or not row[0]:
            return None
        return _json_to_result(str(row[0]))


@dataclass(slots=True)
class InMemoryCommandResultStore:
    _rows: dict[str, StoredCommandResult]
    _event_payload: dict[tuple[str, str, str], str]

    def __init__(self) -> None:
        self._rows = {}
        self._event_payload = {}

    def ensure_event_payload_hash(self, *, channel: str, event_id: str, payload_hash: str) -> None:
        require_non_empty(channel, name="channel")
        require_non_empty(event_id, name="event_id")
        require_non_empty(payload_hash, name="payload_hash")

    def try_begin(self, *, ev: InboundEvent, action_id: str, args: dict[str, JSONValue], request_hash: str) -> bool:
        evn = ev.normalized()
        evn.validate()
        require_non_empty(request_hash, name="request_hash")
        aid = str(action_id or "").strip()
        require_non_empty(aid, name="action_id")
        args_hash = stable_args_fingerprint(args).strip()
        key = (evn.tenant_id.strip(), str(evn.channel), evn.event_id.strip())
        existing_ph = self._event_payload.get(key)
        if existing_ph is not None and existing_ph.strip().lower() != evn.payload_hash.strip().lower():
            raise CommandResultStoreError("inbound event_id replayed with different payload_hash")
        self._event_payload[key] = evn.payload_hash.strip().lower()
        rh = request_hash.strip().lower()
        if rh in self._rows:
            return False
        now_ms = int(time.time() * 1000)
        rec = StoredCommandResult(
            tenant_id=evn.tenant_id.strip(),
            request_hash=rh,
            channel=str(evn.channel),
            event_id=evn.event_id.strip(),
            payload_hash=evn.payload_hash.strip().lower(),
            principal_id=evn.principal_id.strip(),
            action_id=aid,
            args_hash=args_hash,
            started_at_ms=now_ms,
            status="started",
        )
        self._rows[rh] = rec
        return True

    def finish(self, *, request_hash: str, result: CommandResult, finished_at_ms: int | None = None) -> None:
        require_non_empty(request_hash, name="request_hash")
        rh = request_hash.strip().lower()
        rec = self._rows.get(rh)
        if rec is None:
            raise CommandResultStoreError("cannot finish unknown request_hash")
        ms = int(finished_at_ms if finished_at_ms is not None else time.time() * 1000)
        rjson = _result_to_json(result)
        oh = stable_command_outcome_hash(result)
        self._rows[rh] = StoredCommandResult(
            tenant_id=rec.tenant_id,
            request_hash=rec.request_hash,
            channel=rec.channel,
            event_id=rec.event_id,
            payload_hash=rec.payload_hash,
            principal_id=rec.principal_id,
            action_id=rec.action_id,
            args_hash=rec.args_hash,
            started_at_ms=rec.started_at_ms,
            finished_at_ms=ms,
            status="finished",
            result_json=rjson,
            outcome_hash=oh,
        )

    def get_finished(self, *, request_hash: str) -> CommandResult | None:
        require_non_empty(request_hash, name="request_hash")
        rh = request_hash.strip().lower()
        rec = self._rows.get(rh)
        if rec is None or rec.status != "finished" or not rec.result_json:
            return None
        return _json_to_result(rec.result_json)
