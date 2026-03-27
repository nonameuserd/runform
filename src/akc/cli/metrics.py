from __future__ import annotations

import argparse
import json

from akc.control.cost_index import CostIndex
from akc.memory.models import normalize_repo_id
from akc.path_security import safe_resolve_path, safe_resolve_scoped_path


def cmd_metrics(args: argparse.Namespace) -> int:
    """Read tenant/repo cost rollups from the control-plane metrics index."""

    tenant_id = str(args.tenant_id or "").strip()
    repo_id_raw = getattr(args, "repo_id", None)
    repo_id = normalize_repo_id(str(repo_id_raw).strip()) if repo_id_raw else None
    outputs_root = safe_resolve_path(str(args.outputs_root))
    metrics_db = safe_resolve_scoped_path(outputs_root, tenant_id, ".akc", "control", "metrics.sqlite")

    idx = CostIndex(sqlite_path=metrics_db)
    totals = (
        idx.repo_totals(tenant_id=tenant_id, repo_id=repo_id)
        if repo_id is not None
        else idx.tenant_totals(tenant_id=tenant_id)
    )
    runs = idx.list_runs(
        tenant_id=tenant_id,
        repo_id=repo_id,
        limit=int(getattr(args, "limit", 20)),
    )
    payload = {
        "tenant_id": tenant_id,
        "repo_id": repo_id,
        "metrics_db": str(metrics_db),
        "totals": totals,
        "runs": runs,
    }

    if str(getattr(args, "format", "text")) == "json":
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    scope = f"{tenant_id}/{repo_id}" if repo_id is not None else tenant_id
    print(f"Metrics scope: {scope}")
    print(f"Metrics db: {metrics_db}")
    print(
        "Totals: "
        f"runs={totals.get('runs_count', 0)} "
        f"llm_calls={totals.get('llm_calls', 0)} "
        f"tool_calls={totals.get('tool_calls', 0)} "
        f"total_tokens={totals.get('total_tokens', 0)} "
        f"estimated_cost_usd={totals.get('estimated_cost_usd', 0.0)} "
        f"wall_time_ms={totals.get('wall_time_ms', 0)}"
    )
    if runs:
        print("Recent runs:")
        for r in runs:
            print(
                "- "
                f"{r.get('repo_id')}/{r.get('run_id')} "
                f"tokens={r.get('total_tokens', 0)} "
                f"cost={r.get('estimated_cost_usd', 0.0)} "
                f"llm_calls={r.get('llm_calls', 0)} "
                f"tool_calls={r.get('tool_calls', 0)}"
            )
    else:
        print("Recent runs: (none)")
    return 0
