from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path
from typing import Any

from akc.action.models import ActionExecutionRecordV1, ActionIntentV1, ActionPlanV1
from akc.memory.models import normalize_repo_id, normalize_tenant_id, require_non_empty


class ActionStore:
    def __init__(self, *, base_dir: Path | None = None) -> None:
        self._workspace_root = (base_dir or Path.cwd()).resolve()
        self._root = self._workspace_root / ".akc" / "actions"
        self._scope_index_path = self._root / "intent_scope_index.json"

    def workspace_root(self) -> Path:
        return self._workspace_root

    def bind_intent_scope(self, *, intent_id: str, tenant_id: str, repo_id: str) -> Path:
        require_non_empty(intent_id, name="intent_id")
        tenant_norm, repo_norm = self._normalize_scope(tenant_id=tenant_id, repo_id=repo_id)
        scoped_dir = self._root / tenant_norm / repo_norm / intent_id
        index = self._read_scope_index()
        index[intent_id] = {"tenant_id": tenant_norm, "repo_id": repo_norm}
        self._write_json(self._scope_index_path, index)
        return scoped_dir

    def action_dir(self, *, intent_id: str, tenant_id: str | None = None, repo_id: str | None = None) -> Path:
        if isinstance(tenant_id, str) and isinstance(repo_id, str) and tenant_id.strip() and repo_id.strip():
            tenant_norm, repo_norm = self._normalize_scope(tenant_id=tenant_id, repo_id=repo_id)
            return self._root / tenant_norm / repo_norm / intent_id
        scoped = self._resolve_scoped_dir(intent_id=intent_id)
        if scoped is not None:
            return scoped
        return self._root / intent_id

    def write_intent(self, intent: ActionIntentV1) -> Path:
        path = (
            self.bind_intent_scope(
                intent_id=intent.intent_id,
                tenant_id=intent.tenant_id,
                repo_id=intent.repo_id,
            )
            / "intent.json"
        )
        self._write_json(path, intent.to_json_obj())
        return path

    def write_plan(self, plan: ActionPlanV1) -> Path:
        path = self.action_dir(intent_id=plan.intent_id) / "plan.json"
        self._write_json(path, plan.to_json_obj())
        return path

    def append_execution(self, record: ActionExecutionRecordV1) -> Path:
        path = self.action_dir(intent_id=record.intent_id) / "execution.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record.to_json_obj(), ensure_ascii=False, sort_keys=True) + "\n")
        return path

    def write_result(self, *, intent_id: str, result: dict[str, Any]) -> Path:
        path = self.action_dir(intent_id=intent_id) / "result.json"
        self._write_json(path, result)
        refs = self.action_artifact_refs(intent_id=intent_id)
        if refs:
            enriched = dict(result)
            enriched["artifact_refs"] = refs
            enriched["run_catalog_ref"] = {"path": ".akc/run/action_runs.jsonl"}
            enriched["control_catalog_ref"] = {"path": ".akc/control/action_runs.jsonl"}
            self._write_json(path, enriched)
        self._append_action_catalog(intent_id=intent_id)
        return path

    def write_policy_decisions(self, *, intent_id: str, decisions: list[dict[str, Any]]) -> Path:
        path = self.action_dir(intent_id=intent_id) / "policy_decisions.json"
        self._write_json(
            path,
            {
                "schema_kind": "action_policy_decisions",
                "schema_version": 1,
                "intent_id": intent_id,
                "decisions": decisions,
            },
        )
        return path

    def consent_root(self, *, tenant_id: str | None = None, repo_id: str | None = None) -> Path:
        if isinstance(tenant_id, str) and isinstance(repo_id, str) and tenant_id.strip() and repo_id.strip():
            tenant_norm, repo_norm = self._normalize_scope(tenant_id=tenant_id, repo_id=repo_id)
            return self._root / tenant_norm / repo_norm / "consents"
        return self._root / "consents"

    def read_intent(self, *, intent_id: str) -> ActionIntentV1:
        raw = self._read_json(self.action_dir(intent_id=intent_id) / "intent.json")
        return ActionIntentV1.from_json_obj(raw)

    def read_plan(self, *, intent_id: str) -> ActionPlanV1:
        raw = self._read_json(self.action_dir(intent_id=intent_id) / "plan.json")
        return ActionPlanV1.from_json_obj(raw)

    def read_result(self, *, intent_id: str) -> dict[str, Any] | None:
        path = self.action_dir(intent_id=intent_id) / "result.json"
        if not path.exists():
            return None
        return self._read_json(path)

    def read_execution(self, *, intent_id: str) -> list[dict[str, Any]]:
        path = self.action_dir(intent_id=intent_id) / "execution.jsonl"
        if not path.exists():
            return []
        rows: list[dict[str, Any]] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                loaded = json.loads(line)
                if isinstance(loaded, dict):
                    rows.append(loaded)
        return rows

    def next_attempt(self, *, intent_id: str, step_id: str) -> int:
        current = 0
        for row in self.read_execution(intent_id=intent_id):
            if str(row.get("step_id", "")) != step_id:
                continue
            raw = row.get("attempt")
            if isinstance(raw, int) and raw > current:
                current = raw
        return current + 1

    def action_artifact_refs(self, *, intent_id: str) -> dict[str, dict[str, str | None]]:
        action_dir = self.action_dir(intent_id=intent_id)
        refs: dict[str, dict[str, str | None]] = {}
        for name in ("intent.json", "plan.json", "execution.jsonl", "result.json", "policy_decisions.json"):
            p = action_dir / name
            if p.exists():
                refs[name.replace(".", "_")] = {"path": self._repo_relative(p), "sha256": self._sha256_file(p)}
        return refs

    def _append_action_catalog(self, *, intent_id: str) -> None:
        refs = self.action_artifact_refs(intent_id=intent_id)
        if not refs:
            return
        row = json.dumps(
            {
                "schema_kind": "action_run_pointer",
                "schema_version": 1,
                "intent_id": intent_id,
                "refs": refs,
            },
            sort_keys=True,
        )
        self._append_jsonl(self._root.parent / "run" / "action_runs.jsonl", row=row)
        self._append_jsonl(self._root.parent / "control" / "action_runs.jsonl", row=row)

    def _append_jsonl(self, path: Path, *, row: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(row + "\n")

    def _write_json(self, path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False), encoding="utf-8")
        tmp.replace(path)

    def _read_json(self, path: Path) -> dict[str, Any]:
        loaded = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(loaded, dict):
            raise ValueError(f"expected JSON object at {path}")
        return loaded

    def _sha256_file(self, path: Path) -> str:
        return sha256(path.read_bytes()).hexdigest()

    def _repo_relative(self, path: Path) -> str:
        try:
            return str(path.resolve().relative_to(Path.cwd().resolve()))
        except ValueError:
            return str(path.resolve())

    def _normalize_scope(self, *, tenant_id: str, repo_id: str) -> tuple[str, str]:
        return normalize_tenant_id(tenant_id), normalize_repo_id(repo_id)

    def _read_scope_index(self) -> dict[str, dict[str, str]]:
        if not self._scope_index_path.exists():
            return {}
        loaded = self._read_json(self._scope_index_path)
        out: dict[str, dict[str, str]] = {}
        for intent_id, scope in loaded.items():
            if not isinstance(intent_id, str) or not isinstance(scope, dict):
                continue
            tenant_raw = scope.get("tenant_id")
            repo_raw = scope.get("repo_id")
            if not isinstance(tenant_raw, str) or not isinstance(repo_raw, str):
                continue
            try:
                tenant_norm, repo_norm = self._normalize_scope(tenant_id=tenant_raw, repo_id=repo_raw)
            except ValueError:
                continue
            out[intent_id] = {"tenant_id": tenant_norm, "repo_id": repo_norm}
        return out

    def _resolve_scoped_dir(self, *, intent_id: str) -> Path | None:
        require_non_empty(intent_id, name="intent_id")
        indexed = self._read_scope_index().get(intent_id)
        if isinstance(indexed, dict):
            tenant = indexed.get("tenant_id")
            repo = indexed.get("repo_id")
            if isinstance(tenant, str) and isinstance(repo, str):
                return self._root / tenant / repo / intent_id
        legacy = self._root / intent_id
        if legacy.exists():
            return legacy
        matches = sorted(self._root.glob(f"*/*/{intent_id}"))
        for candidate in matches:
            if candidate.is_dir():
                return candidate
        return None
