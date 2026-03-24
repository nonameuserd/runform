"""Plan state store (Phase 2).

Provides resumable, tenant+repo-scoped plan tracking for the compile loop.
Backends:
- JsonFilePlanStateStore: simple local persistence (dependency-light)
- SQLitePlanStateStore: robust local persistence with queryable steps
"""

from __future__ import annotations

import json
import os
import sqlite3
from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from dataclasses import replace
from pathlib import Path
from typing import Any

from akc.memory.models import (
    MemoryModelError,
    PlanState,
    PlanStep,
    PlanStepStatus,
    goal_fingerprint,
    json_dumps,
    json_loads_object,
    new_uuid,
    normalize_repo_id,
    now_ms,
    require_non_empty,
)


class PlanStateError(Exception):
    """Raised when plan state operations fail."""


class PlanStateStore(ABC):
    @abstractmethod
    def create_plan(
        self,
        *,
        tenant_id: str,
        repo_id: str,
        goal: str,
        initial_steps: Iterable[str] | None = None,
        budgets: Mapping[str, Any] | None = None,
    ) -> PlanState: ...

    @abstractmethod
    def load_plan(self, *, tenant_id: str, repo_id: str, plan_id: str) -> PlanState | None: ...

    @abstractmethod
    def save_plan(self, *, tenant_id: str, repo_id: str, plan: PlanState) -> None: ...

    @abstractmethod
    def set_active_plan(self, *, tenant_id: str, repo_id: str, plan_id: str) -> None: ...

    @abstractmethod
    def get_active_plan_id(self, *, tenant_id: str, repo_id: str) -> str | None: ...

    @abstractmethod
    def append_step(
        self,
        *,
        tenant_id: str,
        repo_id: str,
        plan_id: str,
        title: str,
    ) -> PlanState: ...

    @abstractmethod
    def mark_step(
        self,
        *,
        tenant_id: str,
        repo_id: str,
        plan_id: str,
        step_id: str,
        status: PlanStepStatus,
        notes: str | None = None,
    ) -> PlanState: ...

    @abstractmethod
    def set_next_step(
        self,
        *,
        tenant_id: str,
        repo_id: str,
        plan_id: str,
        step_id: str | None,
    ) -> PlanState: ...


def _safe_id_for_path(value: str) -> str:
    # Match Phase 1 pattern: human-readable, avoid traversal.
    return value.replace(os.sep, "_").replace("..", "_")


def default_plan_dir(*, base_dir: Path | None = None) -> Path:
    base = base_dir or Path.cwd()
    return base / ".akc" / "plan"


def _plan_path(*, base_dir: Path, tenant_id: str, repo_id: str, plan_id: str) -> Path:
    safe_tenant = _safe_id_for_path(tenant_id)
    safe_repo = _safe_id_for_path(normalize_repo_id(repo_id))
    require_non_empty(plan_id, name="plan_id")
    return base_dir / safe_tenant / safe_repo / f"{plan_id}.json"


def _new_plan(
    *,
    tenant_id: str,
    repo_id: str,
    goal: str,
    initial_steps: Iterable[str] | None,
    budgets: Mapping[str, Any] | None,
) -> PlanState:
    require_non_empty(tenant_id, name="tenant_id")
    repo = normalize_repo_id(repo_id)
    require_non_empty(goal, name="goal")
    t = now_ms()
    fp = goal_fingerprint(goal)
    steps: list[PlanStep] = []
    for idx, title in enumerate(list(initial_steps or [])):
        steps.append(
            PlanStep(
                id=new_uuid(),
                title=str(title),
                status="pending",
                order_idx=int(idx),
                inputs={
                    "intent_id": "",
                    "active_objectives": [],
                    "linked_constraints": [],
                    "active_success_criteria": [],
                    "goal_fingerprint": fp,
                },
                outputs={},
            )
        )
    next_step_id = steps[0].id if steps else None
    return PlanState(
        id=new_uuid(),
        tenant_id=tenant_id,
        repo_id=repo,
        goal=goal,
        status="active",
        created_at_ms=t,
        updated_at_ms=t,
        steps=tuple(steps),
        next_step_id=next_step_id,
        budgets=dict(budgets or {}),
        last_feedback=None,
    )


class JsonFilePlanStateStore(PlanStateStore):
    def __init__(self, *, base_dir: Path | None = None) -> None:
        self._base_dir = default_plan_dir(base_dir=base_dir)

    @property
    def base_dir(self) -> Path:
        return self._base_dir

    def create_plan(
        self,
        *,
        tenant_id: str,
        repo_id: str,
        goal: str,
        initial_steps: Iterable[str] | None = None,
        budgets: Mapping[str, Any] | None = None,
    ) -> PlanState:
        plan = _new_plan(
            tenant_id=tenant_id,
            repo_id=repo_id,
            goal=goal,
            initial_steps=initial_steps,
            budgets=budgets,
        )
        self.save_plan(tenant_id=tenant_id, repo_id=repo_id, plan=plan)
        self.set_active_plan(tenant_id=tenant_id, repo_id=repo_id, plan_id=plan.id)
        return plan

    def _active_pointer_path(self, *, tenant_id: str, repo_id: str) -> Path:
        safe_tenant = _safe_id_for_path(tenant_id)
        safe_repo = _safe_id_for_path(normalize_repo_id(repo_id))
        return self._base_dir / safe_tenant / safe_repo / "active.json"

    def set_active_plan(self, *, tenant_id: str, repo_id: str, plan_id: str) -> None:
        require_non_empty(tenant_id, name="tenant_id")
        repo = normalize_repo_id(repo_id)
        require_non_empty(plan_id, name="plan_id")
        p = self._active_pointer_path(tenant_id=tenant_id, repo_id=repo)
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            tmp = p.with_suffix(p.suffix + ".tmp")
            tmp.write_text(
                json.dumps({"plan_id": plan_id}, indent=2, sort_keys=True),
                encoding="utf-8",
            )
            tmp.replace(p)
        except OSError as e:
            raise PlanStateError(f"failed to write active plan pointer: {p}") from e

    def get_active_plan_id(self, *, tenant_id: str, repo_id: str) -> str | None:
        require_non_empty(tenant_id, name="tenant_id")
        repo = normalize_repo_id(repo_id)
        p = self._active_pointer_path(tenant_id=tenant_id, repo_id=repo)
        try:
            raw = p.read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        except OSError as e:
            raise PlanStateError(f"failed to read active plan pointer: {p}") from e
        try:
            data = json.loads(raw)
        except Exception as e:  # pragma: no cover
            raise PlanStateError(f"active plan pointer is not valid JSON: {p}") from e
        if not isinstance(data, dict):
            raise PlanStateError(f"active plan pointer must be a JSON object: {p}")
        plan_id = data.get("plan_id")
        if not isinstance(plan_id, str) or not plan_id.strip():
            return None
        return plan_id

    def load_plan(self, *, tenant_id: str, repo_id: str, plan_id: str) -> PlanState | None:
        require_non_empty(tenant_id, name="tenant_id")
        repo = normalize_repo_id(repo_id)
        require_non_empty(plan_id, name="plan_id")
        p = _plan_path(base_dir=self._base_dir, tenant_id=tenant_id, repo_id=repo, plan_id=plan_id)
        try:
            raw = p.read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        except OSError as e:
            raise PlanStateError(f"failed to read plan state: {p}") from e
        try:
            data = json.loads(raw)
        except Exception as e:  # pragma: no cover
            raise PlanStateError(f"plan state is not valid JSON: {p}") from e
        if not isinstance(data, dict):
            raise PlanStateError(f"plan state must be a JSON object: {p}")
        plan = PlanState.from_json_obj(data)
        if plan.tenant_id != tenant_id or normalize_repo_id(plan.repo_id) != repo:
            raise PlanStateError("tenant_id/repo_id mismatch when loading plan")
        return plan

    def save_plan(self, *, tenant_id: str, repo_id: str, plan: PlanState) -> None:
        require_non_empty(tenant_id, name="tenant_id")
        repo = normalize_repo_id(repo_id)
        plan_n = plan.normalized()
        if plan_n.tenant_id != tenant_id:
            raise PlanStateError("tenant_id mismatch between argument and plan.tenant_id")
        if normalize_repo_id(plan_n.repo_id) != repo:
            raise PlanStateError("repo_id mismatch between argument and plan.repo_id")
        p = _plan_path(base_dir=self._base_dir, tenant_id=tenant_id, repo_id=repo, plan_id=plan_n.id)
        obj = plan_n.to_json_obj()
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            tmp = p.with_suffix(p.suffix + ".tmp")
            tmp.write_text(json.dumps(obj, indent=2, sort_keys=True), encoding="utf-8")
            tmp.replace(p)
        except OSError as e:
            raise PlanStateError(f"failed to write plan state: {p}") from e

    def append_step(
        self,
        *,
        tenant_id: str,
        repo_id: str,
        plan_id: str,
        title: str,
    ) -> PlanState:
        plan = self.load_plan(tenant_id=tenant_id, repo_id=repo_id, plan_id=plan_id)
        if plan is None:
            raise PlanStateError("plan not found")
        order_idx = len(plan.steps)
        fp = goal_fingerprint(plan.goal)
        step = PlanStep(
            id=new_uuid(),
            title=title,
            status="pending",
            order_idx=order_idx,
            inputs={
                "intent_id": "",
                "active_objectives": [],
                "linked_constraints": [],
                "active_success_criteria": [],
                "goal_fingerprint": fp,
            },
            outputs={},
        )
        steps = tuple(list(plan.steps) + [step])
        updated = replace(plan, steps=steps, updated_at_ms=now_ms())
        if updated.next_step_id is None:
            updated = replace(updated, next_step_id=step.id, updated_at_ms=now_ms())
        self.save_plan(tenant_id=tenant_id, repo_id=repo_id, plan=updated)
        return updated

    def mark_step(
        self,
        *,
        tenant_id: str,
        repo_id: str,
        plan_id: str,
        step_id: str,
        status: PlanStepStatus,
        notes: str | None = None,
    ) -> PlanState:
        plan = self.load_plan(tenant_id=tenant_id, repo_id=repo_id, plan_id=plan_id)
        if plan is None:
            raise PlanStateError("plan not found")
        require_non_empty(step_id, name="step_id")
        t = now_ms()
        new_steps: list[PlanStep] = []
        found = False
        for s in plan.steps:
            if s.id != step_id:
                new_steps.append(s)
                continue
            found = True
            started = s.started_at_ms
            finished = s.finished_at_ms
            if status == "in_progress" and started is None:
                started = t
            if status in {"done", "failed", "skipped"} and finished is None:
                finished = t
            new_steps.append(
                PlanStep(
                    id=s.id,
                    title=s.title,
                    status=status,
                    order_idx=s.order_idx,
                    started_at_ms=started,
                    finished_at_ms=finished,
                    notes=notes if notes is not None else s.notes,
                    inputs=s.inputs,
                    outputs=s.outputs,
                )
            )
        if not found:
            raise PlanStateError("step not found")
        updated = replace(plan, steps=tuple(new_steps), updated_at_ms=t)
        self.save_plan(tenant_id=tenant_id, repo_id=repo_id, plan=updated)
        return updated

    def set_next_step(
        self,
        *,
        tenant_id: str,
        repo_id: str,
        plan_id: str,
        step_id: str | None,
    ) -> PlanState:
        plan = self.load_plan(tenant_id=tenant_id, repo_id=repo_id, plan_id=plan_id)
        if plan is None:
            raise PlanStateError("plan not found")
        if step_id is not None:
            require_non_empty(step_id, name="step_id")
            if step_id not in {s.id for s in plan.steps}:
                raise PlanStateError("step_id not present in plan")
        updated = replace(plan, next_step_id=step_id, updated_at_ms=now_ms())
        self.save_plan(tenant_id=tenant_id, repo_id=repo_id, plan=updated)
        return updated


class SQLitePlanStateStore(PlanStateStore):
    def __init__(self, *, path: str) -> None:
        require_non_empty(path, name="path")
        self._path = path
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS plans (
                  tenant_id     TEXT NOT NULL,
                  repo_id       TEXT NOT NULL,
                  plan_id       TEXT NOT NULL,
                  goal          TEXT NOT NULL,
                  status        TEXT NOT NULL,
                  created_at_ms INTEGER NOT NULL,
                  updated_at_ms INTEGER NOT NULL,
                  next_step_id  TEXT NULL,
                  budgets       TEXT NOT NULL,
                  last_feedback TEXT NULL,
                  PRIMARY KEY (tenant_id, repo_id, plan_id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS plan_steps (
                  tenant_id      TEXT NOT NULL,
                  repo_id        TEXT NOT NULL,
                  plan_id        TEXT NOT NULL,
                  step_id        TEXT NOT NULL,
                  title          TEXT NOT NULL,
                  status         TEXT NOT NULL,
                  order_idx      INTEGER NOT NULL,
                  started_at_ms  INTEGER NULL,
                  finished_at_ms INTEGER NULL,
                  notes          TEXT NULL,
                  inputs         TEXT NOT NULL,
                  outputs        TEXT NOT NULL,
                  PRIMARY KEY (tenant_id, repo_id, plan_id, step_id),
                  FOREIGN KEY (tenant_id, repo_id, plan_id)
                    REFERENCES plans(tenant_id, repo_id, plan_id)
                    ON DELETE CASCADE
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS active_plans (
                  tenant_id     TEXT NOT NULL,
                  repo_id       TEXT NOT NULL,
                  plan_id       TEXT NOT NULL,
                  updated_at_ms INTEGER NOT NULL,
                  PRIMARY KEY (tenant_id, repo_id)
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS plans_by_repo_updated
                ON plans(tenant_id, repo_id, updated_at_ms DESC)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS plan_steps_by_plan_order
                ON plan_steps(tenant_id, repo_id, plan_id, order_idx)
                """
            )

    def create_plan(
        self,
        *,
        tenant_id: str,
        repo_id: str,
        goal: str,
        initial_steps: Iterable[str] | None = None,
        budgets: Mapping[str, Any] | None = None,
    ) -> PlanState:
        plan = _new_plan(
            tenant_id=tenant_id,
            repo_id=repo_id,
            goal=goal,
            initial_steps=initial_steps,
            budgets=budgets,
        )
        self.save_plan(tenant_id=tenant_id, repo_id=repo_id, plan=plan)
        self.set_active_plan(tenant_id=tenant_id, repo_id=repo_id, plan_id=plan.id)
        return plan

    def set_active_plan(self, *, tenant_id: str, repo_id: str, plan_id: str) -> None:
        require_non_empty(tenant_id, name="tenant_id")
        repo = normalize_repo_id(repo_id)
        require_non_empty(plan_id, name="plan_id")
        t = now_ms()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO active_plans (tenant_id, repo_id, plan_id, updated_at_ms)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(tenant_id, repo_id) DO UPDATE SET
                  plan_id=excluded.plan_id,
                  updated_at_ms=excluded.updated_at_ms
                """,
                (tenant_id, repo, plan_id, t),
            )

    def get_active_plan_id(self, *, tenant_id: str, repo_id: str) -> str | None:
        require_non_empty(tenant_id, name="tenant_id")
        repo = normalize_repo_id(repo_id)
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT plan_id FROM active_plans WHERE tenant_id=? AND repo_id=?",
                (tenant_id, repo),
            )
            row = cur.fetchone()
        if row is None:
            return None
        plan_id = row[0]
        return str(plan_id) if isinstance(plan_id, str) and plan_id.strip() else None

    def load_plan(self, *, tenant_id: str, repo_id: str, plan_id: str) -> PlanState | None:
        require_non_empty(tenant_id, name="tenant_id")
        repo = normalize_repo_id(repo_id)
        require_non_empty(plan_id, name="plan_id")
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT
                  goal,
                  status,
                  created_at_ms,
                  updated_at_ms,
                  next_step_id,
                  budgets,
                  last_feedback
                FROM plans
                WHERE tenant_id=? AND repo_id=? AND plan_id=?
                """,
                (tenant_id, repo, plan_id),
            )
            row = cur.fetchone()
            if row is None:
                return None
            (
                goal,
                status,
                created_at_ms,
                updated_at_ms,
                next_step_id,
                budgets_raw,
                last_feedback_raw,
            ) = row

            cur2 = conn.execute(
                """
                SELECT
                  step_id,
                  title,
                  status,
                  order_idx,
                  started_at_ms,
                  finished_at_ms,
                  notes,
                  inputs,
                  outputs
                FROM plan_steps
                WHERE tenant_id=? AND repo_id=? AND plan_id=?
                ORDER BY order_idx ASC
                """,
                (tenant_id, repo, plan_id),
            )
            step_rows = cur2.fetchall()

        budgets = json_loads_object(str(budgets_raw), what="plan budgets")
        last_feedback: dict[str, Any] | None = None
        if last_feedback_raw is not None:
            last_feedback = json_loads_object(str(last_feedback_raw), what="plan last_feedback")

        steps: list[PlanStep] = []
        for (
            step_id,
            title,
            st,
            order_idx,
            started_at_ms,
            finished_at_ms,
            notes,
            inputs_raw,
            outputs_raw,
        ) in step_rows:
            inputs = json_loads_object(str(inputs_raw), what="plan step inputs")
            outputs = json_loads_object(str(outputs_raw), what="plan step outputs")
            steps.append(
                PlanStep(
                    id=str(step_id),
                    title=str(title),
                    status=str(st),  # type: ignore[arg-type]
                    order_idx=int(order_idx),
                    started_at_ms=int(started_at_ms) if started_at_ms is not None else None,
                    finished_at_ms=int(finished_at_ms) if finished_at_ms is not None else None,
                    notes=str(notes) if notes is not None else None,
                    inputs=inputs,
                    outputs=outputs,
                )
            )

        try:
            plan = PlanState(
                id=plan_id,
                tenant_id=tenant_id,
                repo_id=repo,
                goal=str(goal),
                status=str(status),  # type: ignore[arg-type]
                created_at_ms=int(created_at_ms),
                updated_at_ms=int(updated_at_ms),
                steps=tuple(steps),
                next_step_id=str(next_step_id) if next_step_id is not None else None,
                budgets=budgets,
                last_feedback=last_feedback,
            )
        except MemoryModelError as e:  # pragma: no cover
            raise PlanStateError("stored PlanState was invalid") from e
        return plan

    def save_plan(self, *, tenant_id: str, repo_id: str, plan: PlanState) -> None:
        require_non_empty(tenant_id, name="tenant_id")
        repo = normalize_repo_id(repo_id)
        plan_n = plan.normalized()
        if plan_n.tenant_id != tenant_id:
            raise PlanStateError("tenant_id mismatch between argument and plan.tenant_id")
        if normalize_repo_id(plan_n.repo_id) != repo:
            raise PlanStateError("repo_id mismatch between argument and plan.repo_id")

        budgets_obj = dict(plan_n.budgets or {})
        json_dumps(budgets_obj)  # validate
        last_feedback_raw: str | None = None
        if plan_n.last_feedback is not None:
            last_feedback_obj = dict(plan_n.last_feedback)
            json_dumps(last_feedback_obj)  # validate
            last_feedback_raw = json.dumps(last_feedback_obj, sort_keys=True, ensure_ascii=False)

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO plans (
                  tenant_id, repo_id, plan_id, goal, status, created_at_ms, updated_at_ms,
                  next_step_id, budgets, last_feedback
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(tenant_id, repo_id, plan_id) DO UPDATE SET
                  goal=excluded.goal,
                  status=excluded.status,
                  created_at_ms=excluded.created_at_ms,
                  updated_at_ms=excluded.updated_at_ms,
                  next_step_id=excluded.next_step_id,
                  budgets=excluded.budgets,
                  last_feedback=excluded.last_feedback
                """,
                (
                    tenant_id,
                    repo,
                    plan_n.id,
                    plan_n.goal,
                    str(plan_n.status),
                    int(plan_n.created_at_ms),
                    int(plan_n.updated_at_ms),
                    plan_n.next_step_id,
                    json.dumps(budgets_obj, sort_keys=True, ensure_ascii=False),
                    last_feedback_raw,
                ),
            )

            # Replace steps (Phase 2 simplicity). Still safe via transaction.
            conn.execute(
                "DELETE FROM plan_steps WHERE tenant_id=? AND repo_id=? AND plan_id=?",
                (tenant_id, repo, plan_n.id),
            )
            step_rows: list[tuple[object, ...]] = []
            for s in plan_n.steps:
                step_rows.append(
                    (
                        tenant_id,
                        repo,
                        plan_n.id,
                        s.id,
                        s.title,
                        str(s.status),
                        int(s.order_idx),
                        s.started_at_ms,
                        s.finished_at_ms,
                        s.notes,
                        json.dumps(dict(s.inputs or {}), sort_keys=True, ensure_ascii=False),
                        json.dumps(dict(s.outputs or {}), sort_keys=True, ensure_ascii=False),
                    )
                )
            if step_rows:
                conn.executemany(
                    """
                    INSERT INTO plan_steps (
                      tenant_id, repo_id, plan_id, step_id, title, status, order_idx,
                      started_at_ms, finished_at_ms, notes, inputs, outputs
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    step_rows,
                )

    def append_step(
        self,
        *,
        tenant_id: str,
        repo_id: str,
        plan_id: str,
        title: str,
    ) -> PlanState:
        plan = self.load_plan(tenant_id=tenant_id, repo_id=repo_id, plan_id=plan_id)
        if plan is None:
            raise PlanStateError("plan not found")
        order_idx = len(plan.steps)
        fp = goal_fingerprint(plan.goal)
        step = PlanStep(
            id=new_uuid(),
            title=title,
            status="pending",
            order_idx=order_idx,
            inputs={
                "intent_id": "",
                "active_objectives": [],
                "linked_constraints": [],
                "active_success_criteria": [],
                "goal_fingerprint": fp,
            },
            outputs={},
        )
        updated = replace(plan, steps=tuple(list(plan.steps) + [step]), updated_at_ms=now_ms())
        if updated.next_step_id is None:
            updated = replace(updated, next_step_id=step.id, updated_at_ms=now_ms())
        self.save_plan(tenant_id=tenant_id, repo_id=repo_id, plan=updated)
        return updated

    def mark_step(
        self,
        *,
        tenant_id: str,
        repo_id: str,
        plan_id: str,
        step_id: str,
        status: PlanStepStatus,
        notes: str | None = None,
    ) -> PlanState:
        # Reuse JsonFile semantics: load -> transform -> save.
        plan = self.load_plan(tenant_id=tenant_id, repo_id=repo_id, plan_id=plan_id)
        if plan is None:
            raise PlanStateError("plan not found")
        require_non_empty(step_id, name="step_id")
        t = now_ms()
        new_steps: list[PlanStep] = []
        found = False
        for s in plan.steps:
            if s.id != step_id:
                new_steps.append(s)
                continue
            found = True
            started = s.started_at_ms
            finished = s.finished_at_ms
            if status == "in_progress" and started is None:
                started = t
            if status in {"done", "failed", "skipped"} and finished is None:
                finished = t
            new_steps.append(
                PlanStep(
                    id=s.id,
                    title=s.title,
                    status=status,
                    order_idx=s.order_idx,
                    started_at_ms=started,
                    finished_at_ms=finished,
                    notes=notes if notes is not None else s.notes,
                    inputs=s.inputs,
                    outputs=s.outputs,
                )
            )
        if not found:
            raise PlanStateError("step not found")
        updated = replace(plan, steps=tuple(new_steps), updated_at_ms=t)
        self.save_plan(tenant_id=tenant_id, repo_id=repo_id, plan=updated)
        return updated

    def set_next_step(
        self,
        *,
        tenant_id: str,
        repo_id: str,
        plan_id: str,
        step_id: str | None,
    ) -> PlanState:
        plan = self.load_plan(tenant_id=tenant_id, repo_id=repo_id, plan_id=plan_id)
        if plan is None:
            raise PlanStateError("plan not found")
        if step_id is not None:
            require_non_empty(step_id, name="step_id")
            if step_id not in {s.id for s in plan.steps}:
                raise PlanStateError("step_id not present in plan")
        updated = replace(plan, next_step_id=step_id, updated_at_ms=now_ms())
        self.save_plan(tenant_id=tenant_id, repo_id=repo_id, plan=updated)
        return updated
