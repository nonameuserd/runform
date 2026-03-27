"""Append-only local audit log under ``<outputs_root>/<tenant>/.akc/control/``."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

from akc.memory.models import require_non_empty
from akc.path_security import safe_resolve_path, safe_resolve_scoped_path


def control_audit_jsonl_path(*, outputs_root: str | Path, tenant_id: str) -> Path:
    require_non_empty(tenant_id, name="tenant_id")
    return safe_resolve_scoped_path(
        safe_resolve_path(outputs_root),
        tenant_id.strip(),
        ".akc",
        "control",
        "control_audit.jsonl",
    )


def append_control_audit_event(
    *,
    outputs_root: str | Path,
    tenant_id: str,
    action: str,
    details: dict[str, Any],
    actor: str | None = None,
    request_id: str | None = None,
) -> Path:
    """Append one JSON object line; creates parent dirs. Returns path written."""

    require_non_empty(action, name="action")
    path = control_audit_jsonl_path(outputs_root=outputs_root, tenant_id=tenant_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    who = str(actor).strip() if actor is not None else str(os.environ.get("USER", "") or "").strip() or "unknown"
    record: dict[str, Any] = {
        "ts_ms": int(time.time() * 1000),
        "actor": who,
        "action": action.strip(),
        "tenant_id": tenant_id.strip(),
        "details": dict(details),
    }
    rid = str(request_id).strip() if request_id is not None else ""
    if rid:
        record["request_id"] = rid
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, sort_keys=True) + "\n")
    return path
