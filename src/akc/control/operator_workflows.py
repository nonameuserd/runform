"""Read-only operator workflows: manifest diff, replay forensics, incident bundles."""

from __future__ import annotations

import hashlib
import json
import time
import zipfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

from jsonschema import Draft202012Validator

from akc.control.operations_index import (
    OperationsIndex,
    _validate_manifest_path_matches_record,
    infer_outputs_root_from_run_manifest_path,
    operations_sqlite_path,
)
from akc.control.policy import BundleRedactionPolicy
from akc.knowledge.observability import summarize_knowledge_governance
from akc.memory.models import JSONValue, normalize_repo_id, require_non_empty
from akc.run.intent_replay_mandates import mandatory_partial_replay_passes_for_evaluation_modes
from akc.run.manifest import RunManifest
from akc.run.replay_decisions import PassReplayDecisionRecord


def _normalize_developer_role_profile(value: object) -> str:
    """Match :func:`akc.cli.profile_defaults.normalize_developer_role_profile` without importing ``akc.cli``."""

    raw = str(value or "classic").strip().lower()
    if raw == "emerging":
        return "emerging"
    return "classic"


_CONTROL_PLANE_REF_KEYS: tuple[str, ...] = (
    "action_run_ref",
    "runtime_evidence_ref",
    "policy_decisions_ref",
    "coordination_audit_ref",
    "replay_decisions_ref",
    "recompile_triggers_ref",
    "operational_assurance_ref",
    "governance_profile_ref",
)


def repo_scope_root(*, outputs_root: Path, tenant_id: str, repo_id: str) -> Path:
    require_non_empty(tenant_id, name="tenant_id")
    root = Path(outputs_root).expanduser().resolve()
    return root / tenant_id.strip() / normalize_repo_id(repo_id)


def resolve_outputs_and_scope_root_for_manifest(
    manifest_path: Path,
    manifest: RunManifest,
) -> tuple[Path, Path]:
    """Infer ``outputs_root`` and tenant/repo scope from a standard run manifest path."""

    mp = manifest_path.expanduser().resolve()
    outputs_root = infer_outputs_root_from_run_manifest_path(mp)
    if outputs_root is None:
        raise ValueError(
            "cannot infer outputs_root from manifest path "
            "(expected …/<tenant>/<repo>/.akc/run/<run_id>.manifest.json): "
            f"{mp}"
        )
    _validate_manifest_path_matches_record(manifest_path=mp, outputs_root=outputs_root, manifest=manifest)
    scope = repo_scope_root(outputs_root=outputs_root, tenant_id=manifest.tenant_id, repo_id=manifest.repo_id)
    return outputs_root.resolve(), scope.resolve()


def resolve_run_manifest_path(
    *,
    manifest_path: Path | None,
    outputs_root: Path | None,
    tenant_id: str | None,
    repo_id: str | None,
    run_id: str | None,
) -> Path:
    if manifest_path is not None:
        return Path(manifest_path).expanduser().resolve()
    require_non_empty(str(outputs_root or ""), name="outputs_root")
    require_non_empty(str(tenant_id or ""), name="tenant_id")
    require_non_empty(str(repo_id or ""), name="repo_id")
    require_non_empty(str(run_id or ""), name="run_id")
    root = cast(Path, outputs_root).expanduser().resolve()
    tenant_s = cast(str, tenant_id).strip()
    repo_s = cast(str, repo_id).strip()
    run_s = cast(str, run_id).strip()
    scope = repo_scope_root(outputs_root=root, tenant_id=tenant_s, repo_id=repo_s)
    return (scope / ".akc" / "run" / f"{run_s}.manifest.json").resolve()


def _ensure_under_scope(*, scope_root: Path, target: Path) -> None:
    scope_r = scope_root.resolve()
    target_r = target.resolve()
    try:
        target_r.relative_to(scope_r)
    except ValueError as e:
        raise ValueError(f"path escapes repo scope ({scope_root}): {target}") from e


def read_repo_relative_file(*, scope_root: Path, rel_path: str) -> bytes:
    require_non_empty(rel_path, name="rel_path")
    raw = rel_path.strip().replace("\\", "/")
    if raw.startswith("/") or raw.startswith(".."):
        raise ValueError(f"unsafe artifact path: {rel_path!r}")
    p = (scope_root / raw).resolve()
    _ensure_under_scope(scope_root=scope_root, target=p)
    return p.read_bytes()


def load_json_under_scope(*, scope_root: Path, rel_path: str) -> dict[str, Any]:
    raw = read_repo_relative_file(scope_root=scope_root, rel_path=rel_path)
    loaded = json.loads(raw.decode("utf-8"))
    if not isinstance(loaded, dict):
        raise ValueError(f"expected JSON object at {rel_path}")
    return loaded


def _pass_status_map(manifest: RunManifest) -> dict[str, str]:
    return {p.name.strip(): p.status for p in manifest.passes}


def _summarize_control_plane_ref(cp: dict[str, JSONValue] | None, key: str) -> dict[str, Any] | None:
    if not cp:
        return None
    ref = cp.get(key)
    if not isinstance(ref, dict):
        return None
    return {
        "path": ref.get("path"),
        "sha256": ref.get("sha256"),
    }


def partial_replay_effective_for_manifest(
    manifest: RunManifest,
    *,
    evaluation_modes_override: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    """Partial, mandatory, and effective replay pass sets for one manifest.

    Shared by :func:`compute_manifest_intent_diff` and :func:`build_replay_plan_document`.
    Mandate resolution delegates to :func:`mandatory_partial_replay_passes_for_evaluation_modes`
    (do not duplicate mapping tables here).
    """

    partial = frozenset(manifest.partial_replay_passes)
    if evaluation_modes_override:
        mandatory = mandatory_partial_replay_passes_for_evaluation_modes(modes=evaluation_modes_override)
        return {
            "partial": partial,
            "mandatory": mandatory,
            "effective": partial | mandatory,
            "modes_source": "cli",
        }
    modes = tuple(manifest.success_criteria_evaluation_modes) if manifest.success_criteria_evaluation_modes else ()
    mandatory = mandatory_partial_replay_passes_for_evaluation_modes(modes=modes) if modes else frozenset()
    return {
        "partial": partial,
        "mandatory": mandatory,
        "effective": partial | mandatory,
        "modes_source": "manifest",
        "manifest_modes": list(modes),
        "schema_version": manifest.success_criteria_evaluation_modes_schema_version,
        "intent_acceptance_fingerprint": manifest.intent_acceptance_fingerprint,
    }


def replay_plan_schema() -> dict[str, Any]:
    """Load the frozen JSON Schema for :func:`validate_replay_plan_document`."""

    path = Path(__file__).resolve().parent / "schemas" / "akc_replay_plan.v1.schema.json"
    loaded = json.loads(path.read_text(encoding="utf-8"))
    return cast(dict[str, Any], loaded)


def validate_replay_plan_document(obj: dict[str, Any]) -> list[str]:
    """Return human-readable validation messages (empty if valid)."""

    schema = replay_plan_schema()
    v = Draft202012Validator(schema)
    return [f"{list(e.path)}: {e.message}" for e in v.iter_errors(obj)]


def build_replay_plan_document(
    *,
    manifest: RunManifest,
    manifest_source_path: Path,
    evaluation_modes: tuple[str, ...] | None = None,
    generated_at_ms: int | None = None,
    developer_role_profile_cli: str | None = None,
) -> dict[str, Any]:
    """Machine-readable partial replay plan aligned with manifest-diff mandatory union semantics."""

    eff = partial_replay_effective_for_manifest(manifest, evaluation_modes_override=evaluation_modes)
    effective_sorted = sorted(eff["effective"])
    passes_csv = ",".join(effective_sorted)
    cp = manifest.control_plane if isinstance(manifest.control_plane, dict) else {}
    raw_manifest_profile = cp.get("developer_role_profile")
    manifest_profile: str | None
    if raw_manifest_profile is None or str(raw_manifest_profile).strip() == "":
        manifest_profile = None
    else:
        manifest_profile = _normalize_developer_role_profile(raw_manifest_profile)
    cli_profile: str | None = None
    if developer_role_profile_cli is not None and str(developer_role_profile_cli).strip() != "":
        cli_profile = _normalize_developer_role_profile(developer_role_profile_cli)
    if cli_profile is not None:
        effective_role = cli_profile
        role_source = "cli"
    elif manifest_profile is not None:
        effective_role = manifest_profile
        role_source = "manifest"
    else:
        effective_role = _normalize_developer_role_profile(None)
        role_source = "default"
    argv_template: list[str] = ["akc"]
    if effective_role == "emerging":
        argv_template.extend(["--developer-role-profile", "emerging"])
    argv_template.extend(
        [
            "compile",
            "--replay-mode",
            "partial_replay",
            "--partial-replay-passes",
            passes_csv,
            "<INPUT_OR_REPO_PATH>",
        ]
    )
    warnings: list[str] = []
    if not effective_sorted:
        warnings.append(
            "effective_partial_replay_passes is empty: set intent success_criteria_evaluation_modes on the "
            "manifest, pass --evaluation-modes to replay plan, declare partial_replay_passes, or consider "
            "full_replay when appropriate."
        )

    cli_modes = list(evaluation_modes) if evaluation_modes else None
    doc: dict[str, Any] = {
        "schema_kind": "akc_replay_plan",
        "version": 1,
        "generated_at_ms": int(time.time() * 1000) if generated_at_ms is None else int(generated_at_ms),
        "documentation": (
            "Suggested `akc compile` flags only (operator/CI contract). Do not auto-execute from the viewer; "
            "run compile in a governed environment with tenant-isolated credentials."
        ),
        "manifest": {
            "path": str(manifest_source_path.expanduser().resolve()),
            "run_id": manifest.run_id.strip(),
            "tenant_id": manifest.tenant_id.strip(),
            "repo_id": manifest.repo_id.strip(),
            "replay_mode": manifest.replay_mode,
            "developer_role_profile": effective_role,
            "developer_role_profile_resolution": {
                "manifest": manifest_profile,
                "cli": cli_profile,
                "effective": effective_role,
                "source": role_source,
            },
        },
        "intent_replay_context": {
            "partial_replay_passes_declared": sorted(eff["partial"]),
            "mandatory_partial_replay_passes": sorted(eff["mandatory"]),
            "effective_partial_replay_passes": effective_sorted,
            "evaluation_modes_source": eff["modes_source"],
            "cli_evaluation_modes": cli_modes,
            "manifest_success_criteria_evaluation_modes": eff.get("manifest_modes"),
            "manifest_success_criteria_evaluation_modes_schema_version": eff.get("schema_version"),
            "intent_acceptance_fingerprint": eff.get("intent_acceptance_fingerprint"),
        },
        "suggested_compile": {
            "replay_mode": "partial_replay",
            "partial_replay_passes": passes_csv,
            "argv_template": argv_template,
        },
        "warnings": warnings,
    }
    return doc


def compute_manifest_intent_diff(
    *,
    left: RunManifest,
    right: RunManifest,
    left_label: str = "a",
    right_label: str = "b",
    evaluation_modes: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    """Structured delta between two run manifests (intent / replay operator view)."""

    lcp = left.control_plane or {}
    rcp = right.control_plane or {}
    cp_delta: dict[str, Any] = {}
    all_cp_keys = set(lcp.keys()) | set(rcp.keys())
    for k in sorted(all_cp_keys):
        if k in _CONTROL_PLANE_REF_KEYS:
            ref_left = _summarize_control_plane_ref(dict(lcp) if lcp else None, k)
            ref_right = _summarize_control_plane_ref(dict(rcp) if rcp else None, k)
            if ref_left != ref_right:
                cp_delta[k] = {left_label: ref_left, right_label: ref_right}
        else:
            lv, rv = lcp.get(k), rcp.get(k)
            if lv != rv:
                cp_delta[k] = {left_label: lv, right_label: rv}

    l_pass = _pass_status_map(left)
    r_pass = _pass_status_map(right)
    pass_names = sorted(set(l_pass) | set(r_pass))
    pass_changes: list[dict[str, Any]] = []
    for name in pass_names:
        st_left, st_right = l_pass.get(name), r_pass.get(name)
        if st_left != st_right:
            pass_changes.append({"pass": name, left_label: st_left, right_label: st_right})

    left_eff = partial_replay_effective_for_manifest(left, evaluation_modes_override=evaluation_modes)
    right_eff = partial_replay_effective_for_manifest(right, evaluation_modes_override=evaluation_modes)
    l_partial = left_eff["partial"]
    r_partial = right_eff["partial"]
    mandatory_l = left_eff["mandatory"]
    mandatory_r = right_eff["mandatory"]
    eff_l = left_eff["effective"]
    eff_r = right_eff["effective"]

    if evaluation_modes:
        mandatory_partial_block: dict[str, Any] = {
            "evaluation_modes_source": left_eff["modes_source"],
            "evaluation_modes": list(evaluation_modes),
            "union_for_modes": sorted(mandatory_l),
            "effective_with_partial": {
                left_label: sorted(eff_l),
                right_label: sorted(eff_r),
            },
            "effective_symmetric_diff": sorted(eff_l.symmetric_difference(eff_r)),
        }
    else:
        mandatory_partial_block = {
            "evaluation_modes_source": left_eff["modes_source"],
            "evaluation_modes": [],
            "manifest_success_criteria_evaluation_modes": {
                left_label: list(left_eff.get("manifest_modes", [])),
                right_label: list(right_eff.get("manifest_modes", [])),
            },
            "manifest_success_criteria_evaluation_modes_schema_version": {
                left_label: left.success_criteria_evaluation_modes_schema_version,
                right_label: right.success_criteria_evaluation_modes_schema_version,
            },
            "intent_acceptance_fingerprint": {
                left_label: left.intent_acceptance_fingerprint,
                right_label: right.intent_acceptance_fingerprint,
            },
            "union_for_modes_per_side": {
                left_label: sorted(mandatory_l),
                right_label: sorted(mandatory_r),
            },
            "effective_with_partial": {
                left_label: sorted(eff_l),
                right_label: sorted(eff_r),
            },
            "effective_symmetric_diff": sorted(eff_l.symmetric_difference(eff_r)),
            "note": (
                "When manifests omit success_criteria_evaluation_modes, mandatory unions fall back to "
                "partial_replay_passes only. Pass --evaluation-modes to override with explicit modes."
            ),
        }

    out: dict[str, Any] = {
        "left": {
            "run_id": left.run_id.strip(),
            "tenant_id": left.tenant_id.strip(),
            "repo_id": left.repo_id.strip(),
        },
        "right": {
            "run_id": right.run_id.strip(),
            "tenant_id": right.tenant_id.strip(),
            "repo_id": right.repo_id.strip(),
        },
        "stable_intent_sha256": {
            left_label: left.stable_intent_sha256,
            right_label: right.stable_intent_sha256,
            "match": left.stable_intent_sha256 == right.stable_intent_sha256,
        },
        "fingerprints": {
            "intent_semantic": {
                left_label: left.intent_semantic_fingerprint,
                right_label: right.intent_semantic_fingerprint,
            },
            "knowledge_semantic": {
                left_label: left.knowledge_semantic_fingerprint,
                right_label: right.knowledge_semantic_fingerprint,
            },
            "knowledge_provenance": {
                left_label: left.knowledge_provenance_fingerprint,
                right_label: right.knowledge_provenance_fingerprint,
            },
        },
        "replay_mode": {left_label: left.replay_mode, right_label: right.replay_mode},
        "ir_sha256": {
            left_label: left.ir_sha256,
            right_label: right.ir_sha256,
            "match": left.ir_sha256 == right.ir_sha256,
        },
        "control_plane_delta": cp_delta,
        "pass_status_changes": pass_changes,
        "partial_replay_passes": {
            left_label: sorted(l_partial),
            right_label: sorted(r_partial),
            "only_left": sorted(l_partial - r_partial),
            "only_right": sorted(r_partial - l_partial),
        },
        "mandatory_partial_replay_passes": mandatory_partial_block,
    }
    return out


def replay_decisions_payload_to_forensics(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Normalize replay_decisions JSON into a structured forensics report."""

    decisions_raw = payload.get("decisions")
    if not isinstance(decisions_raw, list):
        raise ValueError("replay_decisions.decisions must be a list")
    rows: list[dict[str, Any]] = []
    triggers_seen: dict[str, int] = {}
    for item in decisions_raw:
        if not isinstance(item, dict):
            continue
        rec = PassReplayDecisionRecord.from_json_obj(item)
        trig = rec.trigger
        kind = str(rec.trigger_reason or "")
        triggers_seen[kind] = triggers_seen.get(kind, 0) + 1
        rows.append(
            {
                "pass_name": rec.pass_name,
                "replay_mode": rec.replay_mode,
                "should_call_model": rec.should_call_model,
                "should_call_tools": rec.should_call_tools,
                "trigger_reason": rec.trigger_reason,
                "trigger_kind": trig.kind if trig is not None else None,
                "trigger_details": dict(trig.details) if trig is not None else None,
                "inputs_snapshot": dict(rec.inputs_snapshot),
            }
        )
    return {
        "schema_kind": "akc_replay_forensics",
        "version": 1,
        "run_id": payload.get("run_id"),
        "tenant_id": payload.get("tenant_id"),
        "repo_id": payload.get("repo_id"),
        "replay_source_run_id": payload.get("replay_source_run_id"),
        "replay_mode": payload.get("replay_mode"),
        "trigger_reason_histogram": dict(sorted(triggers_seen.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))),
        "decisions": rows,
        "alignment_notes": {
            "intent_stable_changed": (
                "Hard replay invalidation when manifest vs current stable_intent_sha256 differ "
                "(see docs/akc-alignment.md)."
            ),
            "intent_semantic_changed": (
                "Hard recompilation / replay invalidation across passes when semantic intent fingerprint drifts."
            ),
            "knowledge_semantic_changed": (
                "May force model passes (generate/repair) to refresh prompts when knowledge semantics drift."
            ),
            "knowledge_provenance_changed": ("May force model passes when knowledge provenance fingerprint drifts."),
        },
    }


def format_replay_forensics_markdown(report: Mapping[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Replay forensics")
    lines.append("")
    lines.append(
        f"- **run_id:** `{report.get('run_id')}`  "
        f"**tenant:** `{report.get('tenant_id')}`  "
        f"**repo:** `{report.get('repo_id')}`"
    )
    lines.append(f"- **replay_mode:** `{report.get('replay_mode')}`")
    lines.append(f"- **replay_source_run_id:** `{report.get('replay_source_run_id')}`")
    hist = report.get("trigger_reason_histogram") or {}
    if isinstance(hist, dict) and hist:
        lines.append("")
        lines.append("## Trigger reasons (counts)")
        for k, v in sorted(hist.items(), key=lambda kv: (-int(kv[1]), str(kv[0]))):
            lines.append(f"- `{k}`: {v}")
    notes = report.get("alignment_notes")
    if isinstance(notes, dict):
        lines.append("")
        lines.append("## Invalidation triggers (alignment)")
        for nk, nv in notes.items():
            lines.append(f"- **{nk}:** {nv}")
    decisions = report.get("decisions") or []
    lines.append("")
    lines.append("## Per-pass decisions")
    if isinstance(decisions, list):
        for d in decisions:
            if not isinstance(d, dict):
                continue
            pname = d.get("pass_name", "")
            lines.append(f"### `{pname}`")
            lines.append(f"- replay_mode: `{d.get('replay_mode')}`")
            lines.append(
                f"- should_call_model: `{d.get('should_call_model')}`  "
                f"should_call_tools: `{d.get('should_call_tools')}`"
            )
            lines.append(f"- trigger_reason: `{d.get('trigger_reason')}`")
            if d.get("trigger_kind"):
                lines.append(f"- trigger_kind: `{d.get('trigger_kind')}`")
            td = d.get("trigger_details")
            if td:
                lines.append(f"- trigger_details: `{json.dumps(td, sort_keys=True)}`")
            snap = d.get("inputs_snapshot")
            if isinstance(snap, dict) and snap:
                lines.append("- inputs_snapshot:")
                for sk, sv in sorted(snap.items()):
                    lines.append(f"  - `{sk}`: `{json.dumps(sv, sort_keys=True, ensure_ascii=False)}`")
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def try_load_replay_decisions(*, scope_root: Path, manifest: RunManifest) -> dict[str, Any] | None:
    cp = manifest.control_plane or {}
    ref = cp.get("replay_decisions_ref")
    if isinstance(ref, dict):
        rel = str(ref.get("path", "")).strip()
        if rel:
            try:
                return load_json_under_scope(scope_root=scope_root, rel_path=rel)
            except (OSError, ValueError, json.JSONDecodeError):
                pass
    fallback = scope_root / ".akc" / "run" / f"{manifest.run_id.strip()}.replay_decisions.json"
    if fallback.is_file():
        try:
            loaded = json.loads(fallback.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        return loaded if isinstance(loaded, dict) else None
    return None


def _costs_rel_path(manifest: RunManifest) -> str | None:
    prefix = f".akc/run/{manifest.run_id.strip()}"
    if not manifest.output_hashes:
        return None
    for rel in manifest.output_hashes:
        rs = str(rel).strip().replace("\\", "/")
        if rs.startswith(prefix) and rs.endswith(".costs.json"):
            return rs
    return None


def _maybe_copy_bytes(
    *,
    scope_root: Path,
    rel: str | None,
    dst: Path,
    max_bytes: int,
) -> tuple[bool, int | None, str | None]:
    if not rel:
        return False, None, "no_pointer"
    try:
        data = read_repo_relative_file(scope_root=scope_root, rel_path=rel)
    except (OSError, ValueError):
        return False, None, "unreadable"
    if len(data) > max_bytes:
        return False, len(data), "too_large"
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_bytes(data)
    return True, len(data), None


def _json_path_matches(pattern: str, path: tuple[str, ...]) -> bool:
    p_parts = tuple(seg for seg in pattern.split(".") if seg)
    if len(p_parts) != len(path):
        return False
    for want, got in zip(p_parts, path, strict=False):
        if want == "*":
            continue
        if want != got:
            return False
    return True


def _redact_summary_value(
    value: JSONValue,
    *,
    policy: BundleRedactionPolicy,
    block_name: str,
) -> tuple[JSONValue, list[dict[str, Any]]]:
    findings: list[dict[str, Any]] = []
    denylist = policy.compiled_denylist()
    sensitive_keys = {k.strip().lower() for k in policy.sensitive_field_names if str(k).strip()}
    field_rules = policy.effective_schema_field_redactions()
    block_rules = tuple(field_rules.get(block_name, ()))

    def _walk(node: JSONValue, path: tuple[str, ...]) -> JSONValue:
        if isinstance(node, dict):
            out: dict[str, JSONValue] = {}
            for k, v in node.items():
                ks = str(k)
                child_path = path + (ks,)
                forced_redaction = any(_json_path_matches(rule, child_path) for rule in block_rules)
                if forced_redaction:
                    findings.append(
                        {
                            "path": ".".join(child_path),
                            "reason": "schema_field",
                            "rule_id": f"{block_name}.field_rule",
                        }
                    )
                    out[ks] = policy.redaction_placeholder
                    continue
                if ks.strip().lower() in sensitive_keys:
                    findings.append(
                        {
                            "path": ".".join(child_path),
                            "reason": "sensitive_key",
                            "rule_id": "default.sensitive_field_name",
                        }
                    )
                    out[ks] = policy.redaction_placeholder
                    continue
                out[ks] = _walk(v, child_path)
            return out
        if isinstance(node, list):
            out_list: list[JSONValue] = []
            for i, item in enumerate(node):
                out_list.append(_walk(item, path + (str(i),)))
            return out_list
        if isinstance(node, str):
            for idx, rx in enumerate(denylist):
                if rx.search(node):
                    findings.append(
                        {
                            "path": ".".join(path),
                            "reason": "denylist_pattern",
                            "rule_id": f"default.denylist.{idx}",
                        }
                    )
                    return policy.redaction_placeholder
        return node

    redacted = _walk(value, ())
    return redacted, findings


def _redaction_manifest(
    *,
    policy: BundleRedactionPolicy,
    findings: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "policy_version": policy.policy_version,
        "denylist_patterns_count": len(policy.denylist_patterns),
        "schema_field_rule_count": sum(len(v) for v in policy.effective_schema_field_redactions().values()),
        "total_redactions": len(findings),
        "entries": findings,
    }


def _hash_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _policy_refs(
    *,
    manifest: RunManifest,
    operations_index_run: dict[str, Any] | None,
) -> dict[str, Any]:
    cp = manifest.control_plane or {}
    out: dict[str, Any] = {
        "policy_bundle_id": None,
        "policy_git_sha": None,
        "rego_pack_version": None,
        "policy_bundle_artifact": None,
    }
    if operations_index_run:
        out["policy_bundle_id"] = operations_index_run.get("policy_bundle_id")
        out["policy_git_sha"] = operations_index_run.get("policy_git_sha")
        out["rego_pack_version"] = operations_index_run.get("rego_pack_version")
        out["policy_bundle_artifact"] = operations_index_run.get("policy_bundle_artifact")
    if isinstance(cp, dict):
        out["policy_bundle_id"] = out["policy_bundle_id"] or cp.get("policy_bundle_id")
        out["policy_git_sha"] = out["policy_git_sha"] or cp.get("policy_git_sha")
        out["rego_pack_version"] = out["rego_pack_version"] or cp.get("rego_pack_version")
    return out


def forensics_bundle_schema() -> dict[str, Any]:
    """Load the frozen JSON Schema for :func:`validate_forensics_bundle`."""

    path = Path(__file__).resolve().parent / "schemas" / "akc_forensics_bundle.v1.schema.json"
    loaded = json.loads(path.read_text(encoding="utf-8"))
    return cast(dict[str, Any], loaded)


def validate_forensics_bundle(obj: dict[str, Any]) -> list[str]:
    """Return human-readable validation messages (empty if valid)."""

    schema = forensics_bundle_schema()
    v = Draft202012Validator(schema)
    return [f"{list(e.path)}: {e.message}" for e in v.iter_errors(obj)]


def _tail_text_lines_from_path(
    path: Path,
    *,
    line_limit: int,
    max_scan_bytes: int,
) -> tuple[list[str], dict[str, Any]]:
    """Return up to the last ``line_limit`` lines from a text file, scanning at most ``max_scan_bytes`` from EOF."""

    stats: dict[str, Any] = {"path": str(path)}
    lim = max(0, int(line_limit))
    cap = max(1, int(max_scan_bytes))
    try:
        size = path.stat().st_size
    except OSError as e:
        stats["error"] = str(e)
        return [], stats
    stats["file_bytes"] = int(size)
    if size == 0 or lim == 0:
        return [], stats
    read_size = min(size, cap)
    try:
        with path.open("rb") as f:
            if size > read_size:
                f.seek(size - read_size)
                stats["scanned_from_eof_bytes"] = read_size
                stats["prefix_truncated"] = True
                chunk = f.read()
                # First line may be partial when we did not start at a line boundary.
                lines = chunk.splitlines()
                if lines:
                    lines = lines[1:]
            else:
                chunk = f.read()
                stats["scanned_from_eof_bytes"] = len(chunk)
                lines = chunk.splitlines()
    except OSError as e:
        stats["error"] = str(e)
        return [], stats
    text_lines = [ln.decode("utf-8", errors="replace") for ln in lines]
    stats["lines_in_scan"] = len(text_lines)
    out = text_lines[-lim:] if len(text_lines) > lim else text_lines
    stats["tail_line_count"] = len(out)
    return out, stats


def _otel_export_rel_paths(*, scope_root: Path, manifest: RunManifest) -> list[str]:
    """Resolve candidate ``*.otel.jsonl`` paths for this compile run (canonical + manifest output_hashes)."""

    run_id = manifest.run_id.strip()
    run_prefix = f".akc/run/{run_id}"
    seen: set[str] = set()
    ordered: list[str] = []
    default = f"{run_prefix}.otel.jsonl"
    try:
        p = (scope_root / default).resolve()
        _ensure_under_scope(scope_root=scope_root, target=p)
        if p.is_file():
            seen.add(default)
            ordered.append(default)
    except ValueError:
        pass
    if manifest.output_hashes:
        for rel in manifest.output_hashes:
            rs = str(rel).strip().replace("\\", "/")
            if not rs.startswith(run_prefix) or not rs.endswith(".otel.jsonl"):
                continue
            if rs in seen:
                continue
            try:
                p2 = (scope_root / rs).resolve()
                _ensure_under_scope(scope_root=scope_root, target=p2)
            except ValueError:
                continue
            if p2.is_file():
                seen.add(rs)
                ordered.append(rs)
    return ordered


def build_forensics_bundle(
    *,
    outputs_root: Path,
    scope_root: Path,
    manifest: RunManifest,
    manifest_source_path: Path,
    out_dir: Path,
    make_zip: bool = True,
    max_file_bytes: int = 8 * 1024 * 1024,
    coordination_audit_tail_lines: int = 500,
    coordination_audit_max_scan_bytes: int = 4 * 1024 * 1024,
    redaction_policy: BundleRedactionPolicy | None = None,
    signer_identity: str | None = None,
    signature: str | None = None,
    generated_at_ms: int | None = None,
) -> dict[str, Any]:
    """Build a single-folder “why” bundle: replay, coordination tail, OTel pointers/copies, knowledge, ops index.

    All reads are confined under ``scope_root`` via :func:`read_repo_relative_file` / :func:`_ensure_under_scope`.
    """

    out_dir = out_dir.expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    data_dir = (out_dir / "data").resolve()
    data_dir.mkdir(parents=True, exist_ok=True)
    files_dir = (out_dir / "files").resolve()
    files_dir.mkdir(parents=True, exist_ok=True)

    root_out = Path(outputs_root).expanduser().resolve()
    scope_r = scope_root.resolve()
    tenant_s = manifest.tenant_id.strip()
    run_s = manifest.run_id.strip()
    repo_norm = normalize_repo_id(manifest.repo_id)

    omitted: list[dict[str, Any]] = []
    included_files: list[dict[str, Any]] = []

    sqlite_p = operations_sqlite_path(outputs_root=root_out, tenant_id=tenant_s)
    row_found = False
    run_slim: dict[str, Any] | None = None
    if sqlite_p.is_file():
        idx = OperationsIndex(sqlite_p)
        full = idx.get_run(tenant_id=tenant_s, repo_id=manifest.repo_id, run_id=run_s)
        if full is not None:
            row_found = True
            run_slim = {
                "stable_intent_sha256": full.get("stable_intent_sha256"),
                "replay_mode": full.get("replay_mode"),
                "manifest_rel_path": full.get("manifest_rel_path"),
                "updated_at_ms": full.get("updated_at_ms"),
                "sidecars": full.get("sidecars"),
                "knowledge_decisions": full.get("knowledge_decisions"),
                "aggregate_health": full.get("aggregate_health"),
                "recompile_trigger_count": full.get("recompile_trigger_count"),
                "runtime_evidence_present": full.get("runtime_evidence_present"),
                "labels": full.get("labels"),
                "policy_bundle_id": full.get("policy_bundle_id"),
                "policy_git_sha": full.get("policy_git_sha"),
                "rego_pack_version": full.get("rego_pack_version"),
                "policy_bundle_artifact": full.get("policy_bundle_artifact"),
                "operational_validity_passed": full.get("operational_validity_passed"),
            }

    operations_index_block: dict[str, Any] = {
        "sqlite_path": str(sqlite_p.resolve()),
        "row_found": row_found,
        "run": run_slim,
    }

    cp = manifest.control_plane if isinstance(manifest.control_plane, dict) else {}

    # Replay decisions + structured forensics summary
    rd_payload = try_load_replay_decisions(scope_root=scope_r, manifest=manifest)
    replay_cp_ref = cp.get("replay_decisions_ref") if isinstance(cp.get("replay_decisions_ref"), dict) else None
    replay_source_relpath: str | None = None
    if isinstance(replay_cp_ref, dict):
        replay_source_relpath = str(replay_cp_ref.get("path", "")).strip() or None
    if replay_source_relpath is None and rd_payload is not None:
        replay_source_relpath = f".akc/run/{run_s}.replay_decisions.json"

    replay_forensics: dict[str, Any] | None = None
    if rd_payload is not None:
        try:
            replay_forensics = replay_decisions_payload_to_forensics(rd_payload)
        except (TypeError, ValueError):
            replay_forensics = None
            omitted.append({"kind": "replay_forensics_summary", "reason": "normalize_failed"})

    replay_block: dict[str, Any]
    if rd_payload is None:
        replay_block = {
            "included": False,
            "control_plane_ref": replay_cp_ref,
            "source_relpath": replay_source_relpath,
            "bundle_relpath": None,
            "forensics_summary": None,
            "omitted_reason": "missing",
        }
        omitted.append({"kind": "replay_decisions", "reason": "missing"})
    else:
        rd_dst = data_dir / "replay_decisions.json"
        rd_dst.write_text(
            json.dumps(rd_payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        rel_bf = str(rd_dst.relative_to(out_dir))
        included_files.append(
            {"bundle_relpath": rel_bf, "source_relpath": replay_source_relpath, "bytes": rd_dst.stat().st_size}
        )
        replay_block = {
            "included": True,
            "control_plane_ref": replay_cp_ref,
            "source_relpath": replay_source_relpath,
            "bundle_relpath": rel_bf,
            "forensics_summary": replay_forensics,
            "omitted_reason": None,
        }

    # Coordination audit JSONL tail
    ca_ref = cp.get("coordination_audit_ref") if isinstance(cp.get("coordination_audit_ref"), dict) else None
    ca_rel: str | None = None
    if isinstance(ca_ref, dict):
        ca_rel = str(ca_ref.get("path", "")).strip() or None

    coord_block: dict[str, Any]
    if not ca_rel:
        coord_block = {
            "included": False,
            "control_plane_ref": ca_ref,
            "source_relpath": None,
            "bundle_relpath": None,
            "tail_line_count": None,
            "tail_stats": None,
            "omitted_reason": "no_manifest_ref",
        }
        omitted.append({"kind": "coordination_audit", "reason": "no_manifest_ref"})
    else:
        try:
            ca_path = (scope_r / ca_rel).resolve()
            _ensure_under_scope(scope_root=scope_r, target=ca_path)
        except ValueError as e:
            coord_block = {
                "included": False,
                "control_plane_ref": ca_ref,
                "source_relpath": ca_rel,
                "bundle_relpath": None,
                "tail_line_count": None,
                "tail_stats": None,
                "omitted_reason": "path_escape",
            }
            omitted.append({"kind": "coordination_audit", "reason": f"path_escape:{e}"})
        else:
            if not ca_path.is_file():
                coord_block = {
                    "included": False,
                    "control_plane_ref": ca_ref,
                    "source_relpath": ca_rel,
                    "bundle_relpath": None,
                    "tail_line_count": None,
                    "tail_stats": None,
                    "omitted_reason": "missing_file",
                }
                omitted.append({"kind": "coordination_audit", "reason": "missing_file", "source_relpath": ca_rel})
            else:
                lines, tstats = _tail_text_lines_from_path(
                    ca_path,
                    line_limit=coordination_audit_tail_lines,
                    max_scan_bytes=coordination_audit_max_scan_bytes,
                )
                ca_dst = data_dir / "coordination_audit.tail.jsonl"
                ca_dst.write_text("".join(f"{ln}\n" for ln in lines), encoding="utf-8")
                rel_ca = str(ca_dst.relative_to(out_dir))
                included_files.append(
                    {
                        "bundle_relpath": rel_ca,
                        "source_relpath": ca_rel,
                        "bytes": ca_dst.stat().st_size,
                    }
                )
                coord_block = {
                    "included": True,
                    "control_plane_ref": ca_ref,
                    "source_relpath": ca_rel,
                    "bundle_relpath": rel_ca,
                    "tail_line_count": len(lines),
                    "tail_stats": tstats,
                    "omitted_reason": None,
                }

    # OTel NDJSON exports
    otel_exports: list[dict[str, Any]] = []
    used_dst_names: set[str] = set()
    for otel_rel in _otel_export_rel_paths(scope_root=scope_r, manifest=manifest):
        digest = None
        if manifest.output_hashes:
            raw_d = manifest.output_hashes.get(otel_rel)
            if raw_d is not None:
                digest = str(raw_d).strip().lower() if str(raw_d).strip() else None
                if digest is not None and (len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest)):
                    digest = None
        base_name = Path(otel_rel).name
        dst_name = base_name
        n = 0
        while dst_name in used_dst_names:
            n += 1
            dst_name = f"{Path(otel_rel).stem}_{n}{Path(otel_rel).suffix}"
        used_dst_names.add(dst_name)
        dst = data_dir / dst_name
        ok_o, sz_o, reason_o = _maybe_copy_bytes(scope_root=scope_r, rel=otel_rel, dst=dst, max_bytes=max_file_bytes)
        if ok_o:
            rel_o = str(dst.relative_to(out_dir))
            included_files.append({"bundle_relpath": rel_o, "source_relpath": otel_rel, "bytes": int(sz_o or 0)})
            otel_exports.append(
                {
                    "source_relpath": otel_rel,
                    "bundle_relpath": rel_o,
                    "bytes_copied": int(sz_o or 0),
                    "sha256_expected": digest,
                    "omitted_reason": None,
                }
            )
        else:
            otel_exports.append(
                {
                    "source_relpath": otel_rel,
                    "bundle_relpath": None,
                    "bytes_copied": None,
                    "sha256_expected": digest,
                    "omitted_reason": reason_o or "not_copied",
                }
            )
            omitted.append(
                {
                    "kind": "otel_jsonl",
                    "reason": reason_o or "not_copied",
                    "source_relpath": otel_rel,
                    "bytes": sz_o,
                }
            )

    if not otel_exports:
        omitted.append({"kind": "otel_jsonl", "reason": "no_candidate_files"})

    # Knowledge snapshot (manifest ref + optional copy)
    ks = manifest.knowledge_snapshot
    knowledge_block: dict[str, Any]
    if ks is None or not str(ks.path).strip():
        knowledge_block = {
            "included": False,
            "manifest_ref": None,
            "source_relpath": None,
            "bundle_relpath": None,
            "omitted_reason": "not_on_manifest",
        }
        omitted.append({"kind": "knowledge_snapshot", "reason": "not_on_manifest"})
    else:
        ks_rel = str(ks.path).strip().replace("\\", "/")
        ks_ref_obj = {"path": ks.path, "sha256": ks.sha256}
        ks_dst = files_dir / ks_rel
        ok_k, sz_k, reason_k = _maybe_copy_bytes(scope_root=scope_r, rel=ks_rel, dst=ks_dst, max_bytes=max_file_bytes)
        if ok_k:
            rel_k = str(ks_dst.relative_to(out_dir))
            included_files.append({"bundle_relpath": rel_k, "source_relpath": ks_rel, "bytes": int(sz_k or 0)})
            knowledge_block = {
                "included": True,
                "manifest_ref": ks_ref_obj,
                "source_relpath": ks_rel,
                "bundle_relpath": rel_k,
                "omitted_reason": None,
            }
        else:
            knowledge_block = {
                "included": False,
                "manifest_ref": ks_ref_obj,
                "source_relpath": ks_rel,
                "bundle_relpath": None,
                "omitted_reason": reason_k or "not_copied",
            }
            omitted.append(
                {
                    "kind": "knowledge_snapshot",
                    "reason": reason_k or "not_copied",
                    "source_relpath": ks_rel,
                    "bytes": sz_k,
                }
            )

    policy_refs = _policy_refs(manifest=manifest, operations_index_run=run_slim)
    generated = int(time.time() * 1000) if generated_at_ms is None else int(generated_at_ms)
    bundle_doc: dict[str, Any] = {
        "schema_kind": "akc_forensics_bundle",
        "version": 1,
        "tenant_id": tenant_s,
        "repo_id": repo_norm,
        "run_id": run_s,
        "scope_root": str(scope_r),
        "outputs_root": str(root_out),
        "manifest_source_path": str(manifest_source_path.resolve()),
        "operations_index": operations_index_block,
        "replay": replay_block,
        "coordination_audit": coord_block,
        "otel": {"exports": otel_exports},
        "knowledge_snapshot": knowledge_block,
        "included_files": included_files,
        "omitted": omitted,
    }
    redaction_findings: list[dict[str, Any]] = []
    redactor = redaction_policy or BundleRedactionPolicy()
    redacted_doc, redaction_findings = _redact_summary_value(
        cast(JSONValue, bundle_doc),
        policy=redactor,
        block_name="forensics.replay",
    )
    redacted_doc2, more_findings = _redact_summary_value(
        redacted_doc,
        policy=redactor,
        block_name="forensics.operations_index",
    )
    redaction_findings.extend(more_findings)
    if not isinstance(redacted_doc2, dict):
        raise ValueError("forensics summary redaction produced non-object")
    bundle_doc = cast(dict[str, Any], redacted_doc2)
    bundle_doc["redaction_applied"] = _redaction_manifest(policy=redactor, findings=redaction_findings)
    hash_manifest: list[dict[str, Any]] = []
    for item in sorted(bundle_doc.get("included_files", []), key=lambda x: str(x.get("bundle_relpath", ""))):
        if not isinstance(item, dict):
            continue
        rel = str(item.get("bundle_relpath", "")).strip()
        if not rel:
            continue
        p = (out_dir / rel).resolve()
        _ensure_under_scope(scope_root=out_dir, target=p)
        if not p.is_file():
            continue
        hash_manifest.append(
            {
                "bundle_relpath": rel,
                "source_relpath": item.get("source_relpath"),
                "bytes": int(p.stat().st_size),
                "sha256": _hash_file(p),
            }
        )
    bundle_doc["export_metadata"] = {
        "schema_kind": "akc_bundle_export_metadata",
        "version": 1,
        "generated_at_ms": generated,
        "deterministic_serialization": {"json_sort_keys": True, "path_separator": "/"},
        "hash_manifest": hash_manifest,
        "policy_refs": policy_refs,
        "signature": (
            {
                "identity": signer_identity.strip(),
                "signature": signature.strip() if isinstance(signature, str) and signature.strip() else None,
            }
            if isinstance(signer_identity, str) and signer_identity.strip()
            else None
        ),
    }
    forensics_path = out_dir / "FORENSICS.json"
    forensics_path.write_text(
        json.dumps(bundle_doc, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    zip_path: str | None = None
    if make_zip:
        zp = out_dir.with_suffix(".zip")
        with zipfile.ZipFile(zp, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for p in sorted(out_dir.rglob("*")):
                if p.is_dir() or p == zp:
                    continue
                zf.write(p, arcname=str(p.relative_to(out_dir)))
        zip_path = str(zp)

    return {
        "summary": bundle_doc,
        "out_dir": str(out_dir),
        "zip_path": zip_path,
        "forensics_relpath": str(forensics_path.relative_to(out_dir)),
    }


def export_incident_bundle(
    *,
    scope_root: Path,
    manifest: RunManifest,
    manifest_source_path: Path,
    out_dir: Path,
    make_zip: bool = True,
    max_file_bytes: int = 8 * 1024 * 1024,
    include_runtime_bundle_pointer: bool = False,
    redaction_policy: BundleRedactionPolicy | None = None,
    signer_identity: str | None = None,
    signature: str | None = None,
    generated_at_ms: int | None = None,
) -> dict[str, Any]:
    """Copy a slim evidence set for incidents; read-only reads under ``scope_root``."""

    out_dir = out_dir.expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    data_dir = (out_dir / "data").resolve()
    data_dir.mkdir(parents=True, exist_ok=True)

    summary_included: dict[str, dict[str, Any]] = {}
    summary_omitted: list[dict[str, Any]] = []

    # Manifest
    man_dst = data_dir / "run.manifest.json"
    man_dst.write_text(
        json.dumps(manifest.to_json_obj(), indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    summary_included["manifest"] = {
        "bundle_relpath": str(man_dst.relative_to(out_dir)),
        "source_abs": str(manifest_source_path.resolve()),
        "bytes": man_dst.stat().st_size,
    }

    # Replay decisions
    rd = try_load_replay_decisions(scope_root=scope_root, manifest=manifest)
    if rd is not None:
        rd_dst = data_dir / "replay_decisions.json"
        rd_dst.write_text(json.dumps(rd, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
        summary_included["replay_decisions"] = {
            "bundle_relpath": str(rd_dst.relative_to(out_dir)),
            "bytes": rd_dst.stat().st_size,
        }
    else:
        summary_omitted.append({"kind": "replay_decisions", "reason": "missing"})

    # Costs
    costs_rel = _costs_rel_path(manifest)
    costs_dst = data_dir / "costs.json"
    ok_c, sz_c, reason_c = _maybe_copy_bytes(
        scope_root=scope_root, rel=costs_rel, dst=costs_dst, max_bytes=max_file_bytes
    )
    if ok_c:
        summary_included["costs"] = {
            "bundle_relpath": str(costs_dst.relative_to(out_dir)),
            "source_relpath": costs_rel,
            "bytes": sz_c,
        }
    else:
        summary_omitted.append(
            {"kind": "costs", "reason": reason_c or "missing", "source_relpath": costs_rel, "bytes": sz_c}
        )

    # Runtime evidence: prefer sidecar ref; else inline slice
    cp = manifest.control_plane or {}
    rev_ref = cp.get("runtime_evidence_ref") if isinstance(cp, dict) else None
    rev_dst = data_dir / "runtime_evidence.json"
    rev_rel: str | None = None
    if isinstance(rev_ref, dict):
        rev_rel = str(rev_ref.get("path", "")).strip() or None
    ok_r, sz_r, reason_r = _maybe_copy_bytes(scope_root=scope_root, rel=rev_rel, dst=rev_dst, max_bytes=max_file_bytes)
    if ok_r:
        summary_included["runtime_evidence"] = {
            "bundle_relpath": str(rev_dst.relative_to(out_dir)),
            "source_relpath": rev_rel,
            "bytes": sz_r,
        }
    elif manifest.runtime_evidence:
        inline_obj = [r.to_json_obj() for r in manifest.runtime_evidence]
        raw = (json.dumps(inline_obj, ensure_ascii=False) + "\n").encode("utf-8")
        if len(raw) <= max_file_bytes:
            rev_dst.write_bytes(raw)
            summary_included["runtime_evidence"] = {
                "bundle_relpath": str(rev_dst.relative_to(out_dir)),
                "source": "manifest_inline",
                "bytes": len(raw),
            }
        else:
            summary_omitted.append({"kind": "runtime_evidence", "reason": "inline_too_large", "bytes": len(raw)})
    else:
        summary_omitted.append({"kind": "runtime_evidence", "reason": reason_r or "missing", "source_relpath": rev_rel})

    # Knowledge snapshot ref (small JSON)
    files_dir = (out_dir / "files").resolve()
    ks = manifest.knowledge_snapshot
    if ks is not None and ks.path.strip():
        ks_rel = ks.path.strip().replace("\\", "/")
        ks_dst = files_dir / ks_rel
        ok_k, sz_k, reason_k = _maybe_copy_bytes(
            scope_root=scope_root, rel=ks_rel, dst=ks_dst, max_bytes=max_file_bytes
        )
        if ok_k:
            summary_included["knowledge_snapshot"] = {
                "bundle_relpath": str(ks_dst.relative_to(out_dir)),
                "source_relpath": ks_rel,
                "bytes": sz_k,
            }
        else:
            summary_omitted.append(
                {
                    "kind": "knowledge_snapshot",
                    "reason": reason_k or "missing",
                    "source_relpath": ks_rel,
                    "bytes": sz_k,
                }
            )
    else:
        summary_omitted.append({"kind": "knowledge_snapshot", "reason": "not_on_manifest"})

    heavy: list[dict[str, Any]] = []
    rb = manifest.runtime_bundle
    if rb is not None and rb.path.strip():
        heavy.append(
            {
                "kind": "runtime_bundle",
                "path": rb.path.strip(),
                "sha256": rb.sha256,
                "note": "Omitted by default; use include_runtime_bundle_pointer only.",
            }
        )
    if include_runtime_bundle_pointer and rb is not None:
        summary_included["runtime_bundle_pointer"] = {
            "path": rb.path.strip(),
            "sha256": rb.sha256,
        }

    policy_refs = _policy_refs(manifest=manifest, operations_index_run=None)
    generated = int(time.time() * 1000) if generated_at_ms is None else int(generated_at_ms)
    summary: dict[str, Any] = {
        "schema_kind": "akc_incident_bundle_summary",
        "version": 1,
        "tenant_id": manifest.tenant_id.strip(),
        "repo_id": normalize_repo_id(manifest.repo_id),
        "run_id": manifest.run_id.strip(),
        "scope_root": str(scope_root.resolve()),
        "included": summary_included,
        "omitted": summary_omitted,
        "heavy_artifacts_not_copied": heavy,
        "knowledge_governance": summarize_knowledge_governance(scope_root=scope_root),
    }
    redactor = redaction_policy or BundleRedactionPolicy()
    redacted_summary, redaction_findings = _redact_summary_value(
        cast(JSONValue, summary),
        policy=redactor,
        block_name="incident.runtime_evidence",
    )
    if not isinstance(redacted_summary, dict):
        raise ValueError("incident summary redaction produced non-object")
    summary = cast(dict[str, Any], redacted_summary)
    summary["redaction_applied"] = _redaction_manifest(policy=redactor, findings=redaction_findings)

    hash_manifest: list[dict[str, Any]] = []
    for _kind, item in sorted(summary_included.items(), key=lambda kv: kv[0]):
        rel = str(item.get("bundle_relpath", "")).strip()
        if not rel:
            continue
        p = (out_dir / rel).resolve()
        _ensure_under_scope(scope_root=out_dir, target=p)
        if not p.is_file():
            continue
        hash_manifest.append(
            {
                "bundle_relpath": rel,
                "source_relpath": item.get("source_relpath"),
                "bytes": int(p.stat().st_size),
                "sha256": _hash_file(p),
            }
        )
    summary["export_metadata"] = {
        "schema_kind": "akc_bundle_export_metadata",
        "version": 1,
        "generated_at_ms": generated,
        "deterministic_serialization": {"json_sort_keys": True, "path_separator": "/"},
        "hash_manifest": hash_manifest,
        "policy_refs": policy_refs,
        "signature": (
            {
                "identity": signer_identity.strip(),
                "signature": signature.strip() if isinstance(signature, str) and signature.strip() else None,
            }
            if isinstance(signer_identity, str) and signer_identity.strip()
            else None
        ),
    }
    (out_dir / "SUMMARY.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    zip_path: str | None = None
    if make_zip:
        zp = out_dir.with_suffix(".zip")
        with zipfile.ZipFile(zp, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for p in sorted(out_dir.rglob("*")):
                if p.is_dir() or p == zp:
                    continue
                zf.write(p, arcname=str(p.relative_to(out_dir)))
        zip_path = str(zp)

    return {"summary": summary, "out_dir": str(out_dir), "zip_path": zip_path}


def format_manifest_diff_text(diff: Mapping[str, Any]) -> str:
    lines: list[str] = []
    lines.append("Manifest / intent diff")
    lines.append("")
    si = diff.get("stable_intent_sha256")
    if isinstance(si, dict):
        lines.append(f"stable_intent_sha256: match={si.get('match')}")
        for k, v in si.items():
            if k != "match":
                lines.append(f"  {k}: {v}")
    lines.append("")
    rm = diff.get("replay_mode")
    if isinstance(rm, dict):
        lines.append(f"replay_mode: {rm}")
    ir = diff.get("ir_sha256")
    if isinstance(ir, dict):
        lines.append(f"ir_sha256 match: {ir.get('match')}")
    lines.append("")
    lines.append("Control plane deltas (refs and other changed keys):")
    cpd = diff.get("control_plane_delta")
    if isinstance(cpd, dict) and cpd:
        for k, v in sorted(cpd.items()):
            lines.append(f"  {k}: {json.dumps(v, sort_keys=True)}")
    else:
        lines.append("  (none)")
    lines.append("")
    lines.append("Pass status changes:")
    pc = diff.get("pass_status_changes")
    if isinstance(pc, list) and pc:
        for row in pc:
            if isinstance(row, dict):
                lines.append(f"  {row.get('pass')}: {row}")
    else:
        lines.append("  (none)")
    lines.append("")
    pr = diff.get("partial_replay_passes")
    if isinstance(pr, dict):
        lines.append(f"partial_replay_passes only_left: {pr.get('only_left')}")
        lines.append(f"partial_replay_passes only_right: {pr.get('only_right')}")
    lines.append("")
    mp = diff.get("mandatory_partial_replay_passes")
    lines.append(f"mandatory_partial_replay_passes: {json.dumps(mp, indent=2, sort_keys=True)}")
    return "\n".join(lines) + "\n"
