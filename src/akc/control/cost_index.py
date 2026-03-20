"""SQLite-backed cost index for per-run and per-tenant attribution."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path

from akc.memory.models import JSONValue, require_non_empty


@dataclass(frozen=True, slots=True)
class RunCostRecord:
    """Stored run cost metrics scoped by tenant+repo+run."""

    tenant_id: str
    repo_id: str
    run_id: str
    llm_calls: int
    tool_calls: int
    input_tokens: int
    output_tokens: int
    total_tokens: int
    wall_time_ms: int
    estimated_cost_usd: float

    def __post_init__(self) -> None:
        require_non_empty(self.tenant_id, name="run_cost_record.tenant_id")
        require_non_empty(self.repo_id, name="run_cost_record.repo_id")
        require_non_empty(self.run_id, name="run_cost_record.run_id")


@dataclass(frozen=True, slots=True)
class CostIndex:
    """Control-plane cost index optimized for tenant aggregate queries."""

    sqlite_path: str | Path

    def _db_path(self) -> Path:
        return Path(self.sqlite_path).expanduser()

    def _connect(self) -> sqlite3.Connection:
        p = self._db_path()
        p.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(p))
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS run_costs (
                tenant_id TEXT NOT NULL,
                repo_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                llm_calls INTEGER NOT NULL,
                tool_calls INTEGER NOT NULL,
                input_tokens INTEGER NOT NULL,
                output_tokens INTEGER NOT NULL,
                total_tokens INTEGER NOT NULL,
                wall_time_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                estimated_cost_usd REAL NOT NULL,
                PRIMARY KEY (tenant_id, repo_id, run_id)
            )
            """
        )
        # Backward-compat for older DBs created before `updated_at_ms` existed.
        cols = {row[1] for row in conn.execute("PRAGMA table_info(run_costs)").fetchall()}
        if "updated_at_ms" not in cols:
            conn.execute(
                "ALTER TABLE run_costs ADD COLUMN updated_at_ms INTEGER NOT NULL DEFAULT 0"
            )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_run_costs_updated_at ON run_costs(updated_at_ms)"
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_run_costs_tenant ON run_costs(tenant_id)")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_run_costs_tenant_repo ON run_costs(tenant_id, repo_id)"
        )
        return conn

    def upsert_run_cost(self, *, record: RunCostRecord) -> None:
        updated_at_ms = int(time.time() * 1000)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO run_costs (
                    tenant_id, repo_id, run_id, llm_calls, tool_calls,
                    input_tokens, output_tokens, total_tokens, wall_time_ms,
                    updated_at_ms, estimated_cost_usd
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(tenant_id, repo_id, run_id) DO UPDATE SET
                    llm_calls=excluded.llm_calls,
                    tool_calls=excluded.tool_calls,
                    input_tokens=excluded.input_tokens,
                    output_tokens=excluded.output_tokens,
                    total_tokens=excluded.total_tokens,
                    wall_time_ms=excluded.wall_time_ms,
                    updated_at_ms=excluded.updated_at_ms,
                    estimated_cost_usd=excluded.estimated_cost_usd
                """,
                (
                    record.tenant_id,
                    record.repo_id,
                    record.run_id,
                    int(record.llm_calls),
                    int(record.tool_calls),
                    int(record.input_tokens),
                    int(record.output_tokens),
                    int(record.total_tokens),
                    int(record.wall_time_ms),
                    int(updated_at_ms),
                    float(record.estimated_cost_usd),
                ),
            )

    def tenant_totals(self, *, tenant_id: str) -> dict[str, JSONValue]:
        require_non_empty(tenant_id, name="tenant_id")
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    COUNT(*) AS runs_count,
                    COALESCE(SUM(llm_calls), 0) AS llm_calls,
                    COALESCE(SUM(tool_calls), 0) AS tool_calls,
                    COALESCE(SUM(input_tokens), 0) AS input_tokens,
                    COALESCE(SUM(output_tokens), 0) AS output_tokens,
                    COALESCE(SUM(total_tokens), 0) AS total_tokens,
                    COALESCE(SUM(wall_time_ms), 0) AS wall_time_ms,
                    COALESCE(SUM(estimated_cost_usd), 0.0) AS estimated_cost_usd
                FROM run_costs
                WHERE tenant_id = ?
                """,
                (tenant_id,),
            ).fetchone()
        if row is None:
            return {
                "runs_count": 0,
                "llm_calls": 0,
                "tool_calls": 0,
                "input_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0,
                "wall_time_ms": 0,
                "estimated_cost_usd": 0.0,
            }
        return {
            "runs_count": int(row[0]),
            "llm_calls": int(row[1]),
            "tool_calls": int(row[2]),
            "input_tokens": int(row[3]),
            "output_tokens": int(row[4]),
            "total_tokens": int(row[5]),
            "wall_time_ms": int(row[6]),
            "estimated_cost_usd": float(row[7]),
        }

    def repo_totals(self, *, tenant_id: str, repo_id: str) -> dict[str, JSONValue]:
        require_non_empty(tenant_id, name="tenant_id")
        require_non_empty(repo_id, name="repo_id")
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    COUNT(*) AS runs_count,
                    COALESCE(SUM(llm_calls), 0) AS llm_calls,
                    COALESCE(SUM(tool_calls), 0) AS tool_calls,
                    COALESCE(SUM(input_tokens), 0) AS input_tokens,
                    COALESCE(SUM(output_tokens), 0) AS output_tokens,
                    COALESCE(SUM(total_tokens), 0) AS total_tokens,
                    COALESCE(SUM(wall_time_ms), 0) AS wall_time_ms,
                    COALESCE(SUM(estimated_cost_usd), 0.0) AS estimated_cost_usd
                FROM run_costs
                WHERE tenant_id = ? AND repo_id = ?
                """,
                (tenant_id, repo_id),
            ).fetchone()
        if row is None:
            return {
                "runs_count": 0,
                "llm_calls": 0,
                "tool_calls": 0,
                "input_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0,
                "wall_time_ms": 0,
                "estimated_cost_usd": 0.0,
            }
        return {
            "runs_count": int(row[0]),
            "llm_calls": int(row[1]),
            "tool_calls": int(row[2]),
            "input_tokens": int(row[3]),
            "output_tokens": int(row[4]),
            "total_tokens": int(row[5]),
            "wall_time_ms": int(row[6]),
            "estimated_cost_usd": float(row[7]),
        }

    def list_runs(
        self,
        *,
        tenant_id: str,
        repo_id: str | None = None,
        limit: int = 20,
    ) -> list[dict[str, JSONValue]]:
        require_non_empty(tenant_id, name="tenant_id")
        n = max(1, int(limit))
        with self._connect() as conn:
            if repo_id is not None and str(repo_id).strip():
                rows = conn.execute(
                    """
                    SELECT
                        tenant_id, repo_id, run_id, llm_calls, tool_calls,
                        input_tokens, output_tokens, total_tokens, wall_time_ms, estimated_cost_usd
                    FROM run_costs
                    WHERE tenant_id = ? AND repo_id = ?
                    ORDER BY updated_at_ms DESC, rowid DESC
                    LIMIT ?
                    """,
                    (tenant_id, repo_id, n),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT
                        tenant_id, repo_id, run_id, llm_calls, tool_calls,
                        input_tokens, output_tokens, total_tokens, wall_time_ms, estimated_cost_usd
                    FROM run_costs
                    WHERE tenant_id = ?
                    ORDER BY updated_at_ms DESC, rowid DESC
                    LIMIT ?
                    """,
                    (tenant_id, n),
                ).fetchall()
        out: list[dict[str, JSONValue]] = []
        for row in rows:
            out.append(
                {
                    "tenant_id": str(row[0]),
                    "repo_id": str(row[1]),
                    "run_id": str(row[2]),
                    "llm_calls": int(row[3]),
                    "tool_calls": int(row[4]),
                    "input_tokens": int(row[5]),
                    "output_tokens": int(row[6]),
                    "total_tokens": int(row[7]),
                    "wall_time_ms": int(row[8]),
                    "estimated_cost_usd": float(row[9]),
                }
            )
        return out
