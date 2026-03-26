from __future__ import annotations

import json
import sqlite3
import time
import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol

from akc.memory.models import JSONValue, require_non_empty


class ApprovalError(Exception):
    """Raised when an approval request cannot be created or resolved."""


ApprovalStatus = Literal["pending", "approved", "denied", "expired", "executed"]


@dataclass(frozen=True, slots=True)
class ApprovalRequest:
    request_id: str
    tenant_id: str
    action_id: str
    args_hash: str
    args: dict[str, JSONValue]
    requester_principal_id: str
    created_at_ms: int
    ttl_ms: int
    expires_at_ms: int
    status: ApprovalStatus = "pending"
    resolved_by_principal_id: str | None = None
    resolved_at_ms: int | None = None
    idempotency_key: str | None = None

    def validate(self) -> None:
        require_non_empty(self.request_id, name="request_id")
        require_non_empty(self.tenant_id, name="tenant_id")
        require_non_empty(self.action_id, name="action_id")
        require_non_empty(self.args_hash, name="args_hash")
        require_non_empty(self.requester_principal_id, name="requester_principal_id")
        if not isinstance(self.args, dict):
            raise TypeError("args must be a dict")
        if int(self.ttl_ms) < 1_000:
            raise ValueError("ttl_ms must be >= 1000")
        if self.idempotency_key is not None:
            require_non_empty(self.idempotency_key, name="idempotency_key")

    def is_expired(self, *, now_ms: int) -> bool:
        return int(now_ms) >= int(self.expires_at_ms)


class ApprovalStore(Protocol):
    def put(self, req: ApprovalRequest) -> None: ...

    def get(self, *, tenant_id: str, request_id: str) -> ApprovalRequest | None: ...

    def get_by_idempotency_key(self, *, tenant_id: str, idempotency_key: str) -> ApprovalRequest | None: ...

    def list_pending(self, *, tenant_id: str) -> Iterable[ApprovalRequest]: ...

    def update(self, req: ApprovalRequest) -> None: ...

    def try_resolve_pending(
        self,
        *,
        tenant_id: str,
        request_id: str,
        decision_status: ApprovalStatus,
        resolved_by_principal_id: str,
        resolved_at_ms: int,
        now_ms: int,
        allow_self_approval: bool,
    ) -> ApprovalRequest: ...

    def try_mark_executed(
        self,
        *,
        tenant_id: str,
        request_id: str,
    ) -> bool: ...


@dataclass(slots=True)
class InMemoryApprovalStore:
    _rows: dict[tuple[str, str], ApprovalRequest]
    _idempotency: dict[tuple[str, str], str]

    def __init__(self) -> None:
        self._rows = {}
        self._idempotency = {}

    def put(self, req: ApprovalRequest) -> None:
        req.validate()
        key = (req.tenant_id.strip(), req.request_id.strip())
        if req.idempotency_key is not None:
            ik = (req.tenant_id.strip(), req.idempotency_key.strip())
            existing_request_id = self._idempotency.get(ik)
            if existing_request_id:
                existing = self._rows.get((req.tenant_id.strip(), existing_request_id))
                if existing is not None:
                    return
            self._idempotency[ik] = req.request_id.strip()
        self._rows[key] = req

    def get(self, *, tenant_id: str, request_id: str) -> ApprovalRequest | None:
        require_non_empty(tenant_id, name="tenant_id")
        require_non_empty(request_id, name="request_id")
        return self._rows.get((tenant_id.strip(), request_id.strip()))

    def get_by_idempotency_key(self, *, tenant_id: str, idempotency_key: str) -> ApprovalRequest | None:
        require_non_empty(tenant_id, name="tenant_id")
        require_non_empty(idempotency_key, name="idempotency_key")
        rid = self._idempotency.get((tenant_id.strip(), idempotency_key.strip()))
        if not rid:
            return None
        return self._rows.get((tenant_id.strip(), rid))

    def list_pending(self, *, tenant_id: str) -> Iterable[ApprovalRequest]:
        require_non_empty(tenant_id, name="tenant_id")
        t = tenant_id.strip()
        for (tid, _rid), req in self._rows.items():
            if tid != t:
                continue
            if req.status == "pending":
                yield req

    def update(self, req: ApprovalRequest) -> None:
        self.put(req)

    def try_resolve_pending(
        self,
        *,
        tenant_id: str,
        request_id: str,
        decision_status: ApprovalStatus,
        resolved_by_principal_id: str,
        resolved_at_ms: int,
        now_ms: int,
        allow_self_approval: bool,
    ) -> ApprovalRequest:
        require_non_empty(tenant_id, name="tenant_id")
        require_non_empty(request_id, name="request_id")
        require_non_empty(resolved_by_principal_id, name="resolver_principal_id")
        current = self.get(tenant_id=tenant_id, request_id=request_id)
        if current is None:
            raise ApprovalError("approval request not found")
        current.validate()
        ms = int(now_ms)
        if current.is_expired(now_ms=ms):
            expired = ApprovalRequest(
                **{
                    **current.__dict__,
                    "status": "expired",
                    "resolved_by_principal_id": resolved_by_principal_id.strip(),
                    "resolved_at_ms": int(resolved_at_ms),
                }
            )
            self.update(expired)
            return expired
        if current.status != "pending":
            return current
        if (not allow_self_approval) and resolved_by_principal_id.strip() == current.requester_principal_id.strip():
            raise ApprovalError("self-approval is not allowed")
        resolved = ApprovalRequest(
            **{
                **current.__dict__,
                "status": decision_status,
                "resolved_by_principal_id": resolved_by_principal_id.strip(),
                "resolved_at_ms": int(resolved_at_ms),
            }
        )
        self.update(resolved)
        return resolved

    def try_mark_executed(
        self,
        *,
        tenant_id: str,
        request_id: str,
    ) -> bool:
        require_non_empty(tenant_id, name="tenant_id")
        require_non_empty(request_id, name="request_id")
        current = self.get(tenant_id=tenant_id, request_id=request_id)
        if current is None:
            raise ApprovalError("approval request not found")
        current.validate()
        if current.status == "executed":
            return False
        if current.status != "approved":
            return False
        updated = ApprovalRequest(**{**current.__dict__, "status": "executed"})
        self.update(updated)
        return True


_SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS approval_requests (
  tenant_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  action_id TEXT NOT NULL,
  args_hash TEXT NOT NULL,
  args_json TEXT NOT NULL,
  requester_principal_id TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL,
  ttl_ms INTEGER NOT NULL,
  expires_at_ms INTEGER NOT NULL,
  status TEXT NOT NULL,
  resolved_by_principal_id TEXT,
  resolved_at_ms INTEGER,
  idempotency_key TEXT,
  PRIMARY KEY (tenant_id, request_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_approval_requests_idempotency
  ON approval_requests(tenant_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_approval_requests_tenant_status_created
  ON approval_requests(tenant_id, status, created_at_ms DESC);
"""


@dataclass(slots=True)
class SqliteApprovalStore:
    """Durable approval request store (sqlite).

    Idempotency:
    - If `idempotency_key` is provided, inserts are de-duped by `(tenant_id, idempotency_key)`.
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
                conn.executescript(_SQLITE_SCHEMA)
        except Exception as e:  # pragma: no cover
            raise ApprovalError(f"failed to init sqlite approval store: {e}") from e

    def put(self, req: ApprovalRequest) -> None:
        req.validate()
        t = req.tenant_id.strip()
        rid = req.request_id.strip()
        ik = req.idempotency_key.strip() if req.idempotency_key is not None else None
        args_json = json.dumps(req.args or {}, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        try:
            with self._connect() as conn:
                conn.execute("BEGIN IMMEDIATE;")
                if ik is not None:
                    row = conn.execute(
                        "SELECT request_id FROM approval_requests WHERE tenant_id=? AND idempotency_key=?",
                        (t, ik),
                    ).fetchone()
                    if row and row[0]:
                        conn.execute("COMMIT;")
                        return
                conn.execute(
                    """
                    INSERT INTO approval_requests(
                      tenant_id,request_id,action_id,args_hash,args_json,requester_principal_id,
                      created_at_ms,ttl_ms,expires_at_ms,status,resolved_by_principal_id,resolved_at_ms,idempotency_key
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        t,
                        rid,
                        req.action_id.strip(),
                        req.args_hash.strip().lower(),
                        args_json,
                        req.requester_principal_id.strip(),
                        int(req.created_at_ms),
                        int(req.ttl_ms),
                        int(req.expires_at_ms),
                        str(req.status),
                        req.resolved_by_principal_id.strip() if req.resolved_by_principal_id else None,
                        int(req.resolved_at_ms) if req.resolved_at_ms is not None else None,
                        ik,
                    ),
                )
                conn.execute("COMMIT;")
        except sqlite3.IntegrityError:
            # Request already exists (by primary key or idempotency unique index). Treat as idempotent.
            return
        except Exception as e:
            raise ApprovalError(f"failed to persist approval request: {e}") from e

    def _row_to_req(self, row: tuple[object, ...]) -> ApprovalRequest:
        def _int_ms(v: object) -> int:
            if v is None:
                return 0
            if isinstance(v, bool):
                return 1 if v else 0
            if isinstance(v, int):
                return int(v)
            if isinstance(v, float):
                return int(v)
            if isinstance(v, str):
                s = v.strip()
                if not s:
                    return 0
                try:
                    return int(float(s))
                except Exception:
                    return 0
            return 0

        (
            tenant_id,
            request_id,
            action_id,
            args_hash,
            args_json,
            requester_principal_id,
            created_at_ms,
            ttl_ms,
            expires_at_ms,
            status,
            resolved_by_principal_id,
            resolved_at_ms,
            idempotency_key,
        ) = row
        args_obj: dict[str, JSONValue]
        try:
            parsed = json.loads(str(args_json or "") or "{}")
            args_obj = parsed if isinstance(parsed, dict) else {}
        except Exception:
            args_obj = {}
        return ApprovalRequest(
            tenant_id=str(tenant_id or ""),
            request_id=str(request_id or ""),
            action_id=str(action_id or ""),
            args_hash=str(args_hash or ""),
            args=args_obj,
            requester_principal_id=str(requester_principal_id or ""),
            created_at_ms=_int_ms(created_at_ms),
            ttl_ms=max(1_000, _int_ms(ttl_ms)),
            expires_at_ms=_int_ms(expires_at_ms),
            status=str(status or "pending"),  # type: ignore[arg-type]
            resolved_by_principal_id=str(resolved_by_principal_id) if resolved_by_principal_id is not None else None,
            resolved_at_ms=_int_ms(resolved_at_ms) if resolved_at_ms is not None else None,
            idempotency_key=str(idempotency_key) if idempotency_key is not None else None,
        )

    def get(self, *, tenant_id: str, request_id: str) -> ApprovalRequest | None:
        require_non_empty(tenant_id, name="tenant_id")
        require_non_empty(request_id, name="request_id")
        t = tenant_id.strip()
        rid = request_id.strip()
        try:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT tenant_id,request_id,action_id,args_hash,args_json,requester_principal_id,
                           created_at_ms,ttl_ms,expires_at_ms,status,resolved_by_principal_id,resolved_at_ms,idempotency_key
                      FROM approval_requests
                     WHERE tenant_id=? AND request_id=?
                    """,
                    (t, rid),
                ).fetchone()
        except Exception as e:
            raise ApprovalError(f"failed to load approval request: {e}") from e
        if not row:
            return None
        return self._row_to_req(row)

    def get_by_idempotency_key(self, *, tenant_id: str, idempotency_key: str) -> ApprovalRequest | None:
        require_non_empty(tenant_id, name="tenant_id")
        require_non_empty(idempotency_key, name="idempotency_key")
        t = tenant_id.strip()
        ik = idempotency_key.strip()
        try:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT tenant_id,request_id,action_id,args_hash,args_json,requester_principal_id,
                           created_at_ms,ttl_ms,expires_at_ms,status,resolved_by_principal_id,resolved_at_ms,idempotency_key
                      FROM approval_requests
                     WHERE tenant_id=? AND idempotency_key=?
                    """,
                    (t, ik),
                ).fetchone()
        except Exception as e:
            raise ApprovalError(f"failed to load approval request by idempotency_key: {e}") from e
        if not row:
            return None
        return self._row_to_req(row)

    def list_pending(self, *, tenant_id: str) -> Iterable[ApprovalRequest]:
        require_non_empty(tenant_id, name="tenant_id")
        t = tenant_id.strip()
        try:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT tenant_id,request_id,action_id,args_hash,args_json,requester_principal_id,
                           created_at_ms,ttl_ms,expires_at_ms,status,resolved_by_principal_id,resolved_at_ms,idempotency_key
                      FROM approval_requests
                     WHERE tenant_id=? AND status='pending'
                     ORDER BY created_at_ms DESC
                    """,
                    (t,),
                ).fetchall()
        except Exception as e:
            raise ApprovalError(f"failed to list pending approvals: {e}") from e
        for row in rows or ():
            yield self._row_to_req(row)

    def update(self, req: ApprovalRequest) -> None:
        # v1: update by full row replacement.
        req.validate()
        t = req.tenant_id.strip()
        rid = req.request_id.strip()
        ik = req.idempotency_key.strip() if req.idempotency_key is not None else None
        args_json = json.dumps(req.args or {}, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        try:
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE approval_requests
                       SET action_id=?,
                           args_hash=?,
                           args_json=?,
                           requester_principal_id=?,
                           created_at_ms=?,
                           ttl_ms=?,
                           expires_at_ms=?,
                           status=?,
                           resolved_by_principal_id=?,
                           resolved_at_ms=?,
                           idempotency_key=?
                     WHERE tenant_id=? AND request_id=?
                    """,
                    (
                        req.action_id.strip(),
                        req.args_hash.strip().lower(),
                        args_json,
                        req.requester_principal_id.strip(),
                        int(req.created_at_ms),
                        int(req.ttl_ms),
                        int(req.expires_at_ms),
                        str(req.status),
                        req.resolved_by_principal_id.strip() if req.resolved_by_principal_id else None,
                        int(req.resolved_at_ms) if req.resolved_at_ms is not None else None,
                        ik,
                        t,
                        rid,
                    ),
                )
        except Exception as e:
            raise ApprovalError(f"failed to update approval request: {e}") from e

    def try_resolve_pending(
        self,
        *,
        tenant_id: str,
        request_id: str,
        decision_status: ApprovalStatus,
        resolved_by_principal_id: str,
        resolved_at_ms: int,
        now_ms: int,
        allow_self_approval: bool,
    ) -> ApprovalRequest:
        require_non_empty(tenant_id, name="tenant_id")
        require_non_empty(request_id, name="request_id")
        require_non_empty(resolved_by_principal_id, name="resolver_principal_id")
        if decision_status not in ("approved", "denied"):
            raise ValueError("decision_status must be approved or denied")
        t = tenant_id.strip()
        rid = request_id.strip()
        resolver = resolved_by_principal_id.strip()
        ms = int(now_ms)
        try:
            with self._connect() as conn:
                conn.execute("BEGIN IMMEDIATE;")
                row = conn.execute(
                    """
                    SELECT tenant_id,request_id,action_id,args_hash,args_json,requester_principal_id,
                           created_at_ms,ttl_ms,expires_at_ms,status,resolved_by_principal_id,resolved_at_ms,idempotency_key
                      FROM approval_requests
                     WHERE tenant_id=? AND request_id=?
                    """,
                    (t, rid),
                ).fetchone()
                if not row:
                    conn.execute("ROLLBACK;")
                    raise ApprovalError("approval request not found")
                current = self._row_to_req(row)
                current.validate()

                if current.is_expired(now_ms=ms):
                    # Expire only if still pending (avoid rewriting resolved rows).
                    conn.execute(
                        """
                        UPDATE approval_requests
                           SET status='expired',
                               resolved_by_principal_id=?,
                               resolved_at_ms=?
                         WHERE tenant_id=? AND request_id=? AND status='pending'
                        """,
                        (resolver, int(resolved_at_ms), t, rid),
                    )
                    conn.execute("COMMIT;")
                    return self.get(tenant_id=t, request_id=rid) or current

                if current.status != "pending":
                    conn.execute("COMMIT;")
                    return current

                if (not allow_self_approval) and resolver == current.requester_principal_id.strip():
                    conn.execute("ROLLBACK;")
                    raise ApprovalError("self-approval is not allowed")

                cur = conn.execute(
                    """
                    UPDATE approval_requests
                       SET status=?,
                           resolved_by_principal_id=?,
                           resolved_at_ms=?
                     WHERE tenant_id=? AND request_id=? AND status='pending'
                    """,
                    (str(decision_status), resolver, int(resolved_at_ms), t, rid),
                )
                # If another resolver won, rowcount will be 0; return the current persisted state.
                conn.execute("COMMIT;")
                if int(cur.rowcount or 0) == 0:
                    latest = self.get(tenant_id=t, request_id=rid)
                    if latest is None:
                        raise ApprovalError("approval request not found")
                    return latest
                latest2 = self.get(tenant_id=t, request_id=rid)
                if latest2 is None:
                    raise ApprovalError("approval request not found")
                return latest2
        except ApprovalError:
            raise
        except Exception as e:
            raise ApprovalError(f"failed to resolve approval request atomically: {e}") from e

    def try_mark_executed(
        self,
        *,
        tenant_id: str,
        request_id: str,
    ) -> bool:
        require_non_empty(tenant_id, name="tenant_id")
        require_non_empty(request_id, name="request_id")
        t = tenant_id.strip()
        rid = request_id.strip()
        try:
            with self._connect() as conn:
                conn.execute("BEGIN IMMEDIATE;")
                cur = conn.execute(
                    """
                    UPDATE approval_requests
                       SET status='executed'
                     WHERE tenant_id=? AND request_id=? AND status='approved'
                    """,
                    (t, rid),
                )
                conn.execute("COMMIT;")
                return int(cur.rowcount or 0) == 1
        except Exception as e:
            raise ApprovalError(f"failed to mark approval executed: {e}") from e


def default_requires_approval(action_id: str) -> bool:
    aid = str(action_id or "").strip().lower()
    return aid.startswith("incident.") or aid.startswith("mutate.")


@dataclass(slots=True)
class ApprovalWorkflow:
    store: ApprovalStore
    requires_approval: Callable[[str], bool] = default_requires_approval
    default_ttl_ms: int = 10 * 60 * 1000
    allow_self_approval: bool = False

    def create_request(
        self,
        *,
        tenant_id: str,
        action_id: str,
        args_hash: str,
        args: dict[str, JSONValue],
        requester_principal_id: str,
        idempotency_key: str | None = None,
        now_ms: int | None = None,
        ttl_ms: int | None = None,
    ) -> ApprovalRequest:
        require_non_empty(tenant_id, name="tenant_id")
        require_non_empty(action_id, name="action_id")
        require_non_empty(args_hash, name="args_hash")
        require_non_empty(requester_principal_id, name="requester_principal_id")
        if not isinstance(args, dict):
            raise TypeError("args must be a dict")
        if idempotency_key is not None:
            require_non_empty(idempotency_key, name="idempotency_key")
            existing = self.store.get_by_idempotency_key(
                tenant_id=tenant_id.strip(),
                idempotency_key=idempotency_key.strip(),
            )
            if existing is not None:
                return existing
        ms = int(now_ms if now_ms is not None else time.time() * 1000)
        ttl = int(ttl_ms if ttl_ms is not None else self.default_ttl_ms)
        ttl = max(1_000, min(ttl, 7 * 24 * 60 * 60 * 1000))
        req = ApprovalRequest(
            request_id=str(uuid.uuid4()),
            tenant_id=tenant_id.strip(),
            action_id=action_id.strip(),
            args_hash=args_hash.strip().lower(),
            args=dict(args),
            requester_principal_id=requester_principal_id.strip(),
            created_at_ms=ms,
            ttl_ms=ttl,
            expires_at_ms=ms + ttl,
            status="pending",
            idempotency_key=idempotency_key.strip() if idempotency_key is not None else None,
        )
        self.store.put(req)
        return req

    def resolve(
        self,
        *,
        tenant_id: str,
        request_id: str,
        resolver_principal_id: str,
        decision: Literal["approve", "deny"],
        now_ms: int | None = None,
    ) -> ApprovalRequest:
        require_non_empty(tenant_id, name="tenant_id")
        require_non_empty(request_id, name="request_id")
        require_non_empty(resolver_principal_id, name="resolver_principal_id")
        ms = int(now_ms if now_ms is not None else time.time() * 1000)
        new_status: ApprovalStatus = "approved" if decision == "approve" else "denied"
        return self.store.try_resolve_pending(
            tenant_id=tenant_id.strip(),
            request_id=request_id.strip(),
            decision_status=new_status,
            resolved_by_principal_id=resolver_principal_id.strip(),
            resolved_at_ms=ms,
            now_ms=ms,
            allow_self_approval=bool(self.allow_self_approval),
        )

    def claim_execution(
        self,
        *,
        tenant_id: str,
        request_id: str,
    ) -> bool:
        """Atomically claim execution for an approved request.

        Returns True exactly once per request, transitioning `approved` -> `executed`.
        """
        return self.store.try_mark_executed(tenant_id=tenant_id.strip(), request_id=request_id.strip())


def stable_args_fingerprint(args: dict[str, JSONValue]) -> str:
    """Low-fi stable args fingerprint for approvals/persistence wiring.

    This is intentionally simple (no canonical JSON yet); replace with repo's stable
    JSON fingerprint helper when the persistence layer is wired in.
    """
    items = sorted((str(k), str(v)) for k, v in (args or {}).items())
    if not items:
        return "∅"
    return "|".join(f"{k}={v}" for k, v in items)
