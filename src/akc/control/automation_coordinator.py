"""Bounded cross-shard automation coordinator (control-plane only)."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from akc.control.control_audit import append_control_audit_event
from akc.control.fleet_catalog import fleet_list_runs_merged, shard_accepts_tenant
from akc.control.fleet_config import FleetConfig
from akc.control.fleet_webhooks import deliver_operator_playbook_completed_webhooks
from akc.control.operations_index import OperationsIndex, operations_sqlite_path
from akc.control.operator_playbook import run_operator_playbook
from akc.memory.models import json_value_as_int, normalize_repo_id
from akc.path_security import safe_resolve_scoped_path

ALLOWED_AUTOMATION_ACTIONS: tuple[str, ...] = (
    "metadata_tag_write",
    "incident_workflow_orchestration",
    "webhook_signal",
)


@dataclass(frozen=True, slots=True)
class AutomationActionOutcome:
    dedupe_key: str
    shard_id: str
    tenant_id: str
    repo_id: str
    run_id: str
    action: str
    status: str
    attempts: int
    checkpoint_status: str
    error: str | None = None
    dead_letter_relpath: str | None = None


def _tenant_ids(cfg: FleetConfig, explicit: list[str] | None) -> list[str]:
    if explicit:
        return sorted({str(t).strip() for t in explicit if str(t).strip()})
    out: set[str] = set()
    for shard in cfg.shards:
        if not shard.outputs_root.is_dir():
            continue
        for child in shard.outputs_root.iterdir():
            if child.is_dir() and not child.name.startswith("."):
                out.add(child.name)
    return sorted(out)


def _dedupe_key(*, shard_id: str, tenant_id: str, repo_id: str, run_id: str, action: str, policy_version: str) -> str:
    key = "|".join(
        (
            shard_id.strip(),
            tenant_id.strip(),
            normalize_repo_id(repo_id),
            run_id.strip(),
            action.strip(),
            policy_version.strip(),
        )
    )
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def _backoff_ms(*, attempts: int, base_backoff_ms: int) -> int:
    # Bounded exponential growth: x1, x2, x4, x8, x16
    mul = min(16, 2 ** max(0, attempts - 1))
    return int(base_backoff_ms) * int(mul)


def _candidate_allows_action(row: dict[str, Any], action: str) -> bool:
    if action == "metadata_tag_write":
        return True
    if action == "incident_workflow_orchestration":
        return bool(row.get("runtime_evidence_present")) and int(row.get("pass_failed", 0) or 0) > 0
    if action == "webhook_signal":
        return int(row.get("recompile_trigger_count", 0) or 0) > 0
    return False


def _dead_letter_path(*, outputs_root: Path, tenant_id: str, dedupe_key: str) -> Path:
    ts = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    safe_key = "".join(ch for ch in str(dedupe_key).strip().lower() if ch in "0123456789abcdef")[:32] or "dedupe"
    return safe_resolve_scoped_path(
        outputs_root,
        tenant_id.strip(),
        ".akc",
        "control",
        "automation",
        "dead_letter",
        f"{ts}.{safe_key}.json",
    )


def _write_dead_letter(
    *,
    outputs_root: Path,
    tenant_id: str,
    payload: dict[str, Any],
    dedupe_key: str,
) -> str:
    target = _dead_letter_path(outputs_root=outputs_root, tenant_id=tenant_id, dedupe_key=dedupe_key)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    try:
        scope_root = safe_resolve_scoped_path(outputs_root, tenant_id.strip())
        return str(target.relative_to(scope_root)).replace("\\", "/")
    except ValueError:
        return str(target)


def _apply_action(
    *,
    cfg: FleetConfig,
    shard_id: str,
    outputs_root: Path,
    row: dict[str, Any],
    action: str,
    dry_run: bool,
) -> dict[str, Any]:
    tenant_id = str(row["tenant_id"]).strip()
    repo_id = normalize_repo_id(str(row["repo_id"]))
    run_id = str(row["run_id"]).strip()
    idx = OperationsIndex(operations_sqlite_path(outputs_root=outputs_root, tenant_id=tenant_id))

    if action == "metadata_tag_write":
        lk = "fleet.automated"
        lv = "true"
        if not dry_run:
            idx.upsert_label(tenant_id=tenant_id, repo_id=repo_id, run_id=run_id, label_key=lk, label_value=lv)
        return {"label_key": lk, "label_value": lv}

    if action == "webhook_signal":
        result = deliver_operator_playbook_completed_webhooks(
            cfg,
            tenant_id=tenant_id,
            item={
                "tenant_id": tenant_id,
                "repo_id": repo_id,
                "run_ids": {"a": run_id, "b": run_id},
                "report_relpath": None,
                "report_sha256": None,
                "playbook_generated_at_ms": int(time.time() * 1000),
                "source": "fleet_automation_coordinator",
            },
            dry_run=dry_run,
        )
        return {"webhook_deliveries": [r.__dict__ for r in result]}

    if action == "incident_workflow_orchestration":
        prior_rows = fleet_list_runs_merged(
            cfg.shards,
            tenant_id=tenant_id,
            repo_id=repo_id,
            limit=20,
        )
        previous_run_id: str | None = None
        for r in prior_rows:
            rid = str(r.get("run_id", "")).strip()
            if rid and rid != run_id:
                previous_run_id = rid
                break
        if not previous_run_id:
            raise ValueError("no prior run available for playbook comparison")
        if dry_run:
            return {"playbook_report_relpath": None, "run_ids": {"a": previous_run_id, "b": run_id}}
        report, report_path = run_operator_playbook(
            outputs_root=outputs_root,
            tenant_id=tenant_id,
            repo_id=repo_id,
            run_id_a=previous_run_id,
            run_id_b=run_id,
            focus="b",
            include_policy_explain=False,
        )
        report_relpath = report.get("report_relpath")
        if not isinstance(report_relpath, str) or not report_relpath:
            report_relpath = str(report_path)
        return {"playbook_report_relpath": report_relpath, "run_ids": {"a": previous_run_id, "b": run_id}}

    raise ValueError(f"unsupported action: {action}")


def run_fleet_automation_coordinator(
    cfg: FleetConfig,
    *,
    tenants: list[str] | None = None,
    actions: tuple[str, ...] = ALLOWED_AUTOMATION_ACTIONS,
    policy_version: str = "v1",
    max_candidates: int = 50,
    max_actions: int = 100,
    max_retries: int = 3,
    base_backoff_ms: int = 1000,
    dry_run: bool = False,
    now_ms: int | None = None,
) -> list[AutomationActionOutcome]:
    """Execute a bounded coordinator pass with artifact-backed checkpoint semantics."""

    now = int(time.time() * 1000) if now_ms is None else int(now_ms)
    allowed_actions = tuple(a for a in actions if a in ALLOWED_AUTOMATION_ACTIONS)
    tenant_ids = _tenant_ids(cfg, tenants)
    outcomes: list[AutomationActionOutcome] = []
    budget = max(1, int(max_actions))

    for tenant_id in tenant_ids:
        if budget <= 0:
            break
        runs = fleet_list_runs_merged(cfg.shards, tenant_id=tenant_id, limit=max(1, int(max_candidates)))
        for row in runs:
            if budget <= 0:
                break
            shard_id = str(row.get("shard_id", "")).strip()
            if not shard_id:
                continue
            shard = next((s for s in cfg.shards if s.id == shard_id), None)
            if shard is None or not shard_accepts_tenant(shard, tenant_id):
                continue
            repo_id = normalize_repo_id(str(row.get("repo_id", "")))
            run_id = str(row.get("run_id", "")).strip()
            if not repo_id or not run_id:
                continue
            idx = OperationsIndex(operations_sqlite_path(outputs_root=shard.outputs_root, tenant_id=tenant_id))
            for action in allowed_actions:
                if budget <= 0:
                    break
                if not _candidate_allows_action(row, action):
                    continue

                dedupe = _dedupe_key(
                    shard_id=shard_id,
                    tenant_id=tenant_id,
                    repo_id=repo_id,
                    run_id=run_id,
                    action=action,
                    policy_version=policy_version,
                )
                checkpoint = idx.get_automation_checkpoint(dedupe_key=dedupe)
                attempts = json_value_as_int((checkpoint or {}).get("attempts", 0), default=0)
                checkpoint_status = str((checkpoint or {}).get("status", "pending"))
                next_attempt_at_ms = json_value_as_int((checkpoint or {}).get("next_attempt_at_ms", 0), default=0)

                if checkpoint_status == "succeeded":
                    outcomes.append(
                        AutomationActionOutcome(
                            dedupe_key=dedupe,
                            shard_id=shard_id,
                            tenant_id=tenant_id,
                            repo_id=repo_id,
                            run_id=run_id,
                            action=action,
                            status="skipped",
                            attempts=attempts,
                            checkpoint_status=checkpoint_status,
                        )
                    )
                    continue
                if now < next_attempt_at_ms:
                    continue

                try:
                    result = _apply_action(
                        cfg=cfg,
                        shard_id=shard_id,
                        outputs_root=shard.outputs_root,
                        row=row,
                        action=action,
                        dry_run=dry_run,
                    )
                    idx.upsert_automation_checkpoint(
                        dedupe_key=dedupe,
                        tenant_id=tenant_id,
                        repo_id=repo_id,
                        run_id=run_id,
                        action=action,
                        policy_version=policy_version,
                        shard_id=shard_id,
                        status="succeeded",
                        attempts=attempts + 1,
                        next_attempt_at_ms=now,
                        last_result=result,
                        updated_at_ms=now,
                    )
                    append_control_audit_event(
                        outputs_root=shard.outputs_root,
                        tenant_id=tenant_id,
                        action="fleet.automation.action.succeeded",
                        actor="fleet-automation-coordinator",
                        details={
                            "dedupe_key": dedupe,
                            "policy_version": policy_version,
                            "repo_id": repo_id,
                            "run_id": run_id,
                            "action": action,
                            "result": result,
                        },
                    )
                    outcomes.append(
                        AutomationActionOutcome(
                            dedupe_key=dedupe,
                            shard_id=shard_id,
                            tenant_id=tenant_id,
                            repo_id=repo_id,
                            run_id=run_id,
                            action=action,
                            status="executed",
                            attempts=attempts + 1,
                            checkpoint_status="succeeded",
                        )
                    )
                except (OSError, ValueError, TypeError) as exc:
                    err = str(exc)
                    next_attempt = now + _backoff_ms(attempts=attempts + 1, base_backoff_ms=base_backoff_ms)
                    checkpoint_next_status = "pending"
                    dead_letter_relpath: str | None = None
                    if attempts + 1 >= max(1, int(max_retries)):
                        checkpoint_next_status = "dead_letter"
                        dead_letter_payload = {
                            "schema_kind": "akc_automation_dead_letter",
                            "version": 1,
                            "generated_at_ms": now,
                            "dedupe_key": dedupe,
                            "policy_version": policy_version,
                            "shard_id": shard_id,
                            "tenant_id": tenant_id,
                            "repo_id": repo_id,
                            "run_id": run_id,
                            "action": action,
                            "attempts": attempts + 1,
                            "error": err,
                        }
                        dead_letter_relpath = _write_dead_letter(
                            outputs_root=shard.outputs_root,
                            tenant_id=tenant_id,
                            payload=dead_letter_payload,
                            dedupe_key=dedupe,
                        )
                    idx.upsert_automation_checkpoint(
                        dedupe_key=dedupe,
                        tenant_id=tenant_id,
                        repo_id=repo_id,
                        run_id=run_id,
                        action=action,
                        policy_version=policy_version,
                        shard_id=shard_id,
                        status=checkpoint_next_status,
                        attempts=attempts + 1,
                        next_attempt_at_ms=next_attempt,
                        last_error=err,
                        last_result={"dead_letter_relpath": dead_letter_relpath} if dead_letter_relpath else None,
                        updated_at_ms=now,
                    )
                    append_control_audit_event(
                        outputs_root=shard.outputs_root,
                        tenant_id=tenant_id,
                        action="fleet.automation.action.failed",
                        actor="fleet-automation-coordinator",
                        details={
                            "dedupe_key": dedupe,
                            "policy_version": policy_version,
                            "repo_id": repo_id,
                            "run_id": run_id,
                            "action": action,
                            "attempts": attempts + 1,
                            "error": err,
                            "checkpoint_status": checkpoint_next_status,
                            "dead_letter_relpath": dead_letter_relpath,
                        },
                    )
                    outcomes.append(
                        AutomationActionOutcome(
                            dedupe_key=dedupe,
                            shard_id=shard_id,
                            tenant_id=tenant_id,
                            repo_id=repo_id,
                            run_id=run_id,
                            action=action,
                            status="failed",
                            attempts=attempts + 1,
                            checkpoint_status=checkpoint_next_status,
                            error=err,
                            dead_letter_relpath=dead_letter_relpath,
                        )
                    )
                budget -= 1
    return outcomes
