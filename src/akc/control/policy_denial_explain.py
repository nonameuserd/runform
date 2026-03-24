"""Structured policy denial context for CLI stderr / JSON (OPA-style explainability, no OPA server)."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

from akc.compile.controller_policy_runtime import COMPILE_PATCH_APPLY_ACTION
from akc.compile.controller_types import ControllerResult
from akc.control.policy_bundle import default_policy_bundle_path_for_scope, fingerprint_policy_bundle_bytes
from akc.control.policy_provenance import _ENV_KEYS
from akc.memory.models import PlanState

SCHEMA_ID = "akc:policy_denial_explain:v1"

# Doc anchors in docs/getting-started.md (stable headings).
DOC_ANCHOR_SCOPED_APPLY = "docs/getting-started.md#opt-in-scoped-realization"
DOC_ANCHOR_EMERGING_PATH = "docs/getting-started.md#emerging-role-golden-path-opt-in"


def policy_provenance_from_env() -> dict[str, str | None]:
    """Mirror control_plane stamp keys from environment (see :mod:`akc.control.policy_provenance`)."""

    out: dict[str, str | None] = {}
    for json_key, env_name in _ENV_KEYS:
        raw = str(os.environ.get(env_name, "") or "").strip()
        out[json_key] = raw or None
    return out


def _try_policy_bundle_fingerprint(scope_root: Path) -> str | None:
    p = default_policy_bundle_path_for_scope(scope_root)
    if not p.is_file():
        return None
    try:
        data = p.read_bytes()
    except OSError:
        return None
    try:
        return fingerprint_policy_bundle_bytes(data)
    except ValueError:
        return None


def _last_denied_from_accounting(accounting: dict[str, Any]) -> dict[str, Any] | None:
    raw = accounting.get("policy_decisions", [])
    if not isinstance(raw, list):
        return None
    for item in reversed(raw):
        if isinstance(item, dict) and not bool(item.get("allowed", False)):
            return dict(item)
    return None


def _last_policy_failure_from_plan(plan: PlanState) -> dict[str, Any] | None:
    for step in plan.steps:
        out = step.outputs or {}
        if not isinstance(out, dict):
            continue
        lp = out.get("last_policy_failure")
        if isinstance(lp, dict):
            return dict(lp)
    return None


def compile_extract_policy_denial(
    result: ControllerResult,
    *,
    scope_root: Path,
    tenant_id: str,
    repo_id: str,
    outputs_root: str,
    opa_policy_path: str | None,
    opa_decision_path: str,
) -> dict[str, Any] | None:
    """Return a structured denial payload when compile failed due to policy/OPA, else None."""

    accounting = result.accounting if isinstance(result.accounting, dict) else {}
    run_id = str(result.plan.id)

    lp = _last_policy_failure_from_plan(result.plan)
    csa_raw = accounting.get("compile_scoped_apply")
    denied = _last_denied_from_accounting(accounting)

    action: str | None = None
    reason: str | None = None
    source: str | None = None
    decision_code: str | None = None
    doc_anchor = DOC_ANCHOR_EMERGING_PATH

    if lp is not None:
        decision_code = str(lp.get("code") or "") or None
        action = str(lp.get("action") or "") or None
        reason = str(lp.get("reason") or lp.get("message") or "") or None
        if decision_code == "policy.missing_tests":
            doc_anchor = DOC_ANCHOR_EMERGING_PATH
    elif isinstance(csa_raw, dict) and bool(csa_raw.get("policy_blocked")):
        action = COMPILE_PATCH_APPLY_ACTION
        source = "opa"
        reason = str(csa_raw.get("reject_reason") or csa_raw.get("deny_reason") or "") or None
        doc_anchor = DOC_ANCHOR_SCOPED_APPLY
        decision_code = "policy.compile.patch.apply_denied"
    elif denied is not None:
        action = str(denied.get("action") or "") or None
        reason = str(denied.get("reason") or "") or None
        source = str(denied.get("source") or "") or None
        if action == COMPILE_PATCH_APPLY_ACTION:
            doc_anchor = DOC_ANCHOR_SCOPED_APPLY
            decision_code = "policy.compile.patch.apply_denied"
    else:
        return None

    if not reason and not action and not decision_code:
        return None

    prov_env = policy_provenance_from_env()
    bundle_fp = _try_policy_bundle_fingerprint(scope_root)

    control_cli = (
        f"akc control runs show --tenant-id {tenant_id} --repo-id {repo_id} "
        f"--run-id {run_id} --outputs-root {outputs_root}"
    )
    return build_policy_denial_explain_v1(
        run_id=run_id,
        run_kind="compile",
        decision_path=str(opa_decision_path),
        opa_policy_path=str(opa_policy_path).strip() if opa_policy_path else None,
        opa_decision_path=str(opa_decision_path),
        reason=reason,
        action=action,
        source=source,
        decision_code=decision_code,
        policy_bundle_id=prov_env.get("policy_bundle_id"),
        policy_git_sha=prov_env.get("policy_git_sha"),
        rego_pack_version=prov_env.get("rego_pack_version"),
        policy_bundle_fingerprint_sha256=bundle_fp,
        doc_anchor_suggestion=doc_anchor,
        control_followup_cli=control_cli,
    )


def build_policy_denial_explain_v1(
    *,
    run_id: str,
    run_kind: str,
    decision_path: str,
    opa_policy_path: str | None,
    opa_decision_path: str,
    reason: str | None,
    action: str | None,
    source: str | None,
    decision_code: str | None = None,
    policy_bundle_id: str | None = None,
    policy_git_sha: str | None = None,
    rego_pack_version: str | None = None,
    policy_bundle_fingerprint_sha256: str | None = None,
    runtime_run_id: str | None = None,
    doc_anchor_suggestion: str,
    control_followup_cli: str,
) -> dict[str, Any]:
    """Single JSON object for stderr (text) or stdout (json mode)."""

    payload: dict[str, Any] = {
        "schema_id": SCHEMA_ID,
        "run_kind": run_kind,
        "run_id": run_id,
        "decision_path": decision_path,
        "opa_decision_path": opa_decision_path,
        "opa_policy_path": opa_policy_path,
        "reason": reason,
        "action": action,
        "source": source,
        "decision_code": decision_code,
        "policy_bundle_id": policy_bundle_id,
        "policy_git_sha": policy_git_sha,
        "rego_pack_version": rego_pack_version,
        "policy_bundle_fingerprint_sha256": policy_bundle_fingerprint_sha256,
        "doc_anchor_suggestion": doc_anchor_suggestion,
        "control_followup_cli": control_followup_cli,
    }
    if runtime_run_id:
        payload["runtime_run_id"] = runtime_run_id
    return payload


def runtime_policy_denial_from_denied_decision(
    *,
    denied: dict[str, Any],
    scope_root: Path,
    opa_policy_path: str | None,
    opa_decision_path: str,
    record: dict[str, Any],
) -> dict[str, Any]:
    """Build payload from runtime policy_decisions entry (allowed=false)."""

    prov_env = policy_provenance_from_env()
    bundle_fp = _try_policy_bundle_fingerprint(scope_root)
    action = str(denied.get("action") or "") or None
    reason = str(denied.get("reason") or "") or None
    source = str(denied.get("source") or "") or None
    doc_anchor = DOC_ANCHOR_SCOPED_APPLY if action == COMPILE_PATCH_APPLY_ACTION else DOC_ANCHOR_EMERGING_PATH
    runtime_run_id = str(record.get("runtime_run_id") or "").strip()
    compile_run_id = str(record.get("run_id") or "").strip()
    rid = compile_run_id or runtime_run_id
    tenant_id = str(record.get("tenant_id") or "").strip()
    repo_id = str(record.get("repo_id") or "").strip()
    outputs_root = str(record.get("outputs_root") or "").strip()
    control_cli = (
        f"akc control runs show --tenant-id {tenant_id} --repo-id {repo_id} "
        f"--run-id {rid} --outputs-root {outputs_root}"
    )
    return build_policy_denial_explain_v1(
        run_id=rid,
        run_kind="runtime",
        decision_path=str(opa_decision_path),
        opa_policy_path=str(opa_policy_path).strip() if opa_policy_path else None,
        opa_decision_path=str(opa_decision_path),
        reason=reason,
        action=action,
        source=source,
        policy_bundle_id=prov_env.get("policy_bundle_id"),
        policy_git_sha=prov_env.get("policy_git_sha"),
        rego_pack_version=prov_env.get("rego_pack_version"),
        policy_bundle_fingerprint_sha256=bundle_fp,
        runtime_run_id=runtime_run_id or None,
        doc_anchor_suggestion=doc_anchor,
        control_followup_cli=control_cli,
    )


def runtime_policy_denial_from_permission_error(
    *,
    record: dict[str, Any] | None,
    scope_root: Path,
    opa_policy_path: str | None,
    opa_decision_path: str,
    message: str,
) -> dict[str, Any]:
    """PermissionError from runtime kernel policy (no structured decision row)."""

    prov_env = policy_provenance_from_env()
    bundle_fp = _try_policy_bundle_fingerprint(scope_root)
    runtime_run_id = str(record.get("runtime_run_id") or "").strip() if record else ""
    compile_run_id = str(record.get("run_id") or "").strip() if record else ""
    tenant_id = str(record.get("tenant_id") or "").strip() if record else ""
    repo_id = str(record.get("repo_id") or "").strip() if record else ""
    outputs_root = str(record.get("outputs_root") or "").strip() if record else ""
    rid = compile_run_id or runtime_run_id or "unknown"
    control_cli = (
        f"akc control runs show --tenant-id {tenant_id} --repo-id {repo_id} "
        f"--run-id {rid} --outputs-root {outputs_root}"
        if tenant_id and repo_id and outputs_root
        else f"(set scope) akc control runs show --run-id {rid}"
    )
    return build_policy_denial_explain_v1(
        run_id=rid,
        run_kind="runtime",
        decision_path=str(opa_decision_path),
        opa_policy_path=str(opa_policy_path).strip() if opa_policy_path else None,
        opa_decision_path=str(opa_decision_path),
        reason=message,
        action=None,
        source="opa",
        decision_code="runtime.permission_denied",
        policy_bundle_id=prov_env.get("policy_bundle_id"),
        policy_git_sha=prov_env.get("policy_git_sha"),
        rego_pack_version=prov_env.get("rego_pack_version"),
        policy_bundle_fingerprint_sha256=bundle_fp,
        runtime_run_id=runtime_run_id or None,
        doc_anchor_suggestion=DOC_ANCHOR_EMERGING_PATH,
        control_followup_cli=control_cli,
    )


def format_policy_denial_for_text_stderr(payload: dict[str, Any]) -> str:
    """Human-readable multi-line stderr block."""

    lines = [
        "policy_denial_explain:",
        f"  schema_id: {payload.get('schema_id', '')}",
        f"  run_kind: {payload.get('run_kind', '')}",
        f"  run_id: {payload.get('run_id', '')}",
    ]
    if payload.get("runtime_run_id"):
        lines.append(f"  runtime_run_id: {payload.get('runtime_run_id')}")
    lines.extend(
        [
            f"  decision_path: {payload.get('decision_path', '')}",
            f"  opa_decision_path: {payload.get('opa_decision_path', '')}",
            f"  opa_policy_path: {payload.get('opa_policy_path')}",
            f"  decision_code: {payload.get('decision_code')}",
            f"  reason: {payload.get('reason')}",
            f"  action: {payload.get('action')}",
            f"  source: {payload.get('source')}",
            f"  rego_pack_version: {payload.get('rego_pack_version')}",
            f"  policy_bundle_id: {payload.get('policy_bundle_id')}",
            f"  policy_git_sha: {payload.get('policy_git_sha')}",
            f"  policy_bundle_fingerprint_sha256: {payload.get('policy_bundle_fingerprint_sha256')}",
            f"  doc_anchor_suggestion: {payload.get('doc_anchor_suggestion')}",
            f"  control_followup_cli: {payload.get('control_followup_cli')}",
        ]
    )
    return "\n".join(lines) + "\n"


def print_policy_denial(*, payload: dict[str, Any], format_mode: str) -> None:
    """Emit structured policy denial to stderr (text) or stdout (json)."""

    if format_mode == "json":
        print(json.dumps(payload, sort_keys=True, ensure_ascii=False), flush=True)
    else:
        print(format_policy_denial_for_text_stderr(payload), file=sys.stderr, flush=True)
