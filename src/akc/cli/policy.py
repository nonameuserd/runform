from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from akc.control.control_audit import append_control_audit_event
from akc.control.operations_index import infer_outputs_root_from_run_manifest_path
from akc.control.operator_workflows import resolve_run_manifest_path
from akc.control.policy_explain import build_policy_explain_payload
from akc.memory.models import normalize_repo_id
from akc.run.manifest import RunManifest


def cmd_policy_explain(args: argparse.Namespace) -> int:
    """Print policy provenance + decisions for a run (inline or policy_decisions_ref)."""

    manifest_path = getattr(args, "manifest", None)
    run_id = str(getattr(args, "run_id", "") or "").strip()
    tenant_id = str(getattr(args, "tenant_id", "") or "").strip()
    repo_id_raw = str(getattr(args, "repo_id", "") or "").strip()
    repo_id = normalize_repo_id(repo_id_raw) if repo_id_raw else ""
    outputs_root = Path(str(getattr(args, "outputs_root", "") or "")).expanduser()

    if manifest_path:
        mp = Path(str(manifest_path)).expanduser().resolve()
    elif run_id and tenant_id and repo_id and outputs_root.parts:
        mp = resolve_run_manifest_path(
            manifest_path=None,
            outputs_root=outputs_root,
            tenant_id=tenant_id,
            repo_id=repo_id,
            run_id=run_id,
        )
    else:
        print(
            "error: provide --manifest or (--run-id, --tenant-id, --repo-id, --outputs-root)",
            file=sys.stderr,
        )
        return 2

    if not mp.is_file():
        print(f"error: manifest not found: {mp}", file=sys.stderr)
        return 1

    manifest = RunManifest.from_json_file(mp)
    inferred_tenant = str(manifest.tenant_id or "").strip()
    inferred_repo = normalize_repo_id(str(manifest.repo_id or ""))
    if tenant_id and tenant_id != inferred_tenant:
        print(
            f"error: --tenant-id {tenant_id!r} does not match manifest tenant {inferred_tenant!r}",
            file=sys.stderr,
        )
        return 2
    if repo_id_raw and inferred_repo != repo_id:
        print(
            f"error: --repo-id {repo_id_raw!r} does not match manifest repo {manifest.repo_id!r}",
            file=sys.stderr,
        )
        return 2

    scope_root = mp.resolve().parent.parent.parent
    payload = build_policy_explain_payload(manifest=manifest, scope_root=scope_root)

    if bool(getattr(args, "record_audit", False)):
        root = infer_outputs_root_from_run_manifest_path(mp)
        if root is None:
            root = outputs_root if outputs_root.parts else scope_root.parent.parent
        append_control_audit_event(
            outputs_root=root,
            tenant_id=inferred_tenant,
            action="policy.explain",
            details={
                "run_id": manifest.run_id,
                "repo_id": inferred_repo,
                "manifest_path": str(mp),
                "policy_decisions_source": payload.get("policy_decisions_source"),
            },
            actor=str(getattr(args, "audit_actor", None) or os.environ.get("AKC_AUDIT_ACTOR") or "").strip() or None,
        )

    fmt = str(getattr(args, "format", "text"))
    if fmt == "json":
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    prov = payload.get("policy_provenance") or {}
    print(f"Run: {manifest.run_id} ({inferred_tenant}/{inferred_repo})")
    print(f"Policy decisions source: {payload.get('policy_decisions_source')}")
    print("Policy provenance (control_plane):")
    print(f"  policy_bundle_id: {prov.get('policy_bundle_id') or '-'}")
    print(f"  policy_git_sha: {prov.get('policy_git_sha') or '-'}")
    print(f"  rego_pack_version: {prov.get('rego_pack_version') or '-'}")
    explained = payload.get("decisions_explained") or []
    if not explained:
        print("Decisions: (none recorded on manifest)")
        return 0
    print("Decisions:")
    for item in explained:
        if not isinstance(item, dict):
            continue
        idx = item.get("index")
        action = item.get("action")
        allowed = item.get("allowed")
        reason = item.get("reason")
        detail = item.get("reason_detail")
        src = item.get("source")
        mode = item.get("mode")
        print(f"  [{idx}] action={action!r} allowed={allowed} source={src!r} mode={mode!r}")
        print(f"       reason: {reason!r}")
        print(f"       → {detail}")
    return 0
