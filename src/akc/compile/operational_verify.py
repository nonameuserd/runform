from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

from akc.artifacts.contracts import apply_schema_envelope
from akc.artifacts.validate import validate_artifact_json
from akc.compile.interfaces import TenantRepoScope
from akc.control.operator_workflows import resolve_outputs_and_scope_root_for_manifest, resolve_run_manifest_path
from akc.control.policy_bundle import GOVERNANCE_PROFILE_RESOLVED_REL_PATH, resolve_governance_profile_for_scope
from akc.intent.models import (
    IntentSpecV1,
    OperationalValidityParams,
    parse_operational_validity_params,
    stable_intent_sha256,
)
from akc.intent.operational_eval import (
    OperationalEvidenceRollupMeta,
    OperationalPathScopeError,
    OperationalRollupLoadError,
    OperationalVerdict,
    combined_operational_fingerprint,
    ensure_path_under_repo_outputs,
    evaluate_operational_spec,
    load_merged_runtime_evidence_from_rollup_path,
    parse_otel_metric_ndjson_slice,
    parse_otel_ndjson_slice,
)
from akc.intent.store import JsonFileIntentStore
from akc.memory.models import JSONValue
from akc.run.manifest import RunManifest, RuntimeEvidenceRecord
from akc.utils.fingerprint import stable_json_fingerprint

OperationalVerificationAuthority = Literal["recomputed", "none"]
OperationalEnforcementMode = Literal["advisory", "blocking"]


@dataclass(frozen=True, slots=True)
class OperationalVerificationFinding:
    code: str
    message: str
    severity: Literal["error", "warning"] = "error"
    evidence: dict[str, JSONValue] | None = None

    def to_json_obj(self) -> dict[str, JSONValue]:
        out: dict[str, JSONValue] = {
            "code": self.code,
            "message": self.message,
            "severity": self.severity,
        }
        if self.evidence:
            out["evidence"] = dict(self.evidence)
        return out


@dataclass(frozen=True, slots=True)
class OperationalVerificationResult:
    scope: TenantRepoScope
    run_id: str
    authority: OperationalVerificationAuthority
    passed: bool
    checked_manifest_strict: bool
    had_report: bool
    recomputed_attestation_fingerprint_sha256: str | None
    stored_attestation_fingerprint_sha256: str | None
    enforcement_mode: OperationalEnforcementMode
    blocking_passed: bool
    advisory_only: bool
    assurance_result: dict[str, JSONValue] | None
    assurance_rel_path: str | None
    findings: tuple[OperationalVerificationFinding, ...]


def _safe_json_obj(value: Any) -> dict[str, JSONValue]:
    if not isinstance(value, Mapping):
        return {}
    out: dict[str, JSONValue] = {}
    for k, v in value.items():
        if isinstance(k, str):
            out[k] = cast(JSONValue, v)
    return out


def _json_sha256(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    import hashlib

    return hashlib.sha256(raw).hexdigest()


def _read_json(path: Path, *, what: str) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"{what} must be a JSON object")
    return raw


def _pointer_json_sha256(path: Path) -> str | None:
    if not path.is_file():
        return None
    text = path.read_text(encoding="utf-8")
    try:
        raw = json.loads(text)
    except json.JSONDecodeError:
        return _json_sha256(text)
    if isinstance(raw, (dict, list)):
        return _json_sha256(raw)
    return _json_sha256(text)


def _artifact_path_from_rel(*, scope_root: Path, rel_path: str, kind: str) -> Path:
    raw = str(rel_path).strip()
    if not raw:
        raise ValueError(f"{kind} path must be non-empty")
    candidate = (scope_root / raw).resolve()
    return ensure_path_under_repo_outputs(candidate, repo_outputs_root=scope_root)


def _normalize_intent_ref(value: Any) -> dict[str, str] | None:
    if not isinstance(value, Mapping):
        return None
    intent_id = str(value.get("intent_id", "")).strip()
    stable_sha = str(value.get("stable_intent_sha256", "")).strip().lower()
    semantic_fp = str(value.get("semantic_fingerprint", "")).strip().lower()
    goal_fp = str(value.get("goal_text_fingerprint", "")).strip().lower()
    if not intent_id:
        return None
    out = {
        "intent_id": intent_id,
        "stable_intent_sha256": stable_sha,
        "semantic_fingerprint": semantic_fp,
        "goal_text_fingerprint": goal_fp,
    }
    return out


def _intent_ref_diff(
    left: Mapping[str, str] | None,
    right: Mapping[str, str] | None,
) -> dict[str, dict[str, str]]:
    if left is None or right is None:
        return {}
    out: dict[str, dict[str, str]] = {}
    for key in ("intent_id", "stable_intent_sha256", "semantic_fingerprint", "goal_text_fingerprint"):
        lv = str(left.get(key, ""))
        rv = str(right.get(key, ""))
        if lv != rv:
            out[key] = {"left": lv, "right": rv}
    return out


def _post_runtime_operational_specs(intent: IntentSpecV1) -> list[tuple[str, OperationalValidityParams]]:
    out: list[tuple[str, OperationalValidityParams]] = []
    for sc in sorted(intent.success_criteria, key=lambda item: item.id):
        if sc.evaluation_mode != "operational_spec":
            continue
        try:
            params = parse_operational_validity_params(sc.params)
        except Exception:
            continue
        if params is None:
            continue
        if params.evaluation_phase == "post_runtime":
            out.append((sc.id, params))
    return out


def _predicate_rows_from_verdict(per_criterion: Sequence[Mapping[str, JSONValue]]) -> list[dict[str, JSONValue]]:
    rows: list[dict[str, JSONValue]] = []
    for row in per_criterion:
        ck = str(row.get("check_name", "")).strip()
        pk = "threshold" if "threshold_signal" in ck else "presence"
        detail = row.get("evidence")
        item: dict[str, JSONValue] = {
            "predicate_kind": pk,
            "signal_key": ck or "check",
            "passed": bool(row.get("passed")),
        }
        if isinstance(detail, Mapping):
            item["details"] = dict(cast(Mapping[str, JSONValue], detail))
        rows.append(item)
    return rows


def _otel_run_sidecar_path(*, scope_root: Path, run_id: str, metrics: bool) -> Path:
    suffix = "otel_metrics.jsonl" if metrics else "otel.jsonl"
    return ensure_path_under_repo_outputs(
        scope_root / ".akc" / "run" / f"{run_id}.{suffix}",
        repo_outputs_root=scope_root,
    )


def _runtime_run_id_from(
    *,
    report_obj: Mapping[str, Any] | None,
    manifest: RunManifest,
    evidence: Sequence[RuntimeEvidenceRecord],
) -> str | None:
    if isinstance(report_obj, Mapping):
        raw = report_obj.get("runtime_run_id")
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
    cp = manifest.control_plane or {}
    raw = cp.get("runtime_run_id")
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    ids = sorted({rec.runtime_run_id.strip() for rec in evidence if rec.runtime_run_id.strip()})
    return ids[0] if len(ids) == 1 else None


def _load_runtime_evidence(
    *,
    scope_root: Path,
    manifest: RunManifest,
    report_obj: Mapping[str, Any] | None,
) -> tuple[tuple[RuntimeEvidenceRecord, ...], Path | None, str | None]:
    pointer: Mapping[str, Any] | None = None
    if isinstance(report_obj, Mapping):
        ref = report_obj.get("runtime_evidence_ref")
        if isinstance(ref, Mapping):
            pointer = cast(Mapping[str, Any], ref)
    if pointer is None:
        cp = manifest.control_plane or {}
        ref = cp.get("runtime_evidence_ref")
        if isinstance(ref, Mapping):
            pointer = cast(Mapping[str, Any], ref)

    path: Path | None = None
    sha: str | None = None
    if pointer is not None:
        rel = str(pointer.get("path", "")).strip()
        if rel:
            path = _artifact_path_from_rel(scope_root=scope_root, rel_path=rel, kind="runtime evidence")
            raw_sha = str(pointer.get("sha256", "")).strip().lower()
            if raw_sha:
                sha = raw_sha
    if path is None:
        return manifest.runtime_evidence, None, None
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError("runtime evidence file must be a JSON array")
    validate_artifact_json(obj=raw, kind="runtime_evidence_stream")
    return tuple(RuntimeEvidenceRecord.from_json_obj(item) for item in raw if isinstance(item, dict)), path, sha


def _load_authoritative_intent(
    *,
    scope_root: Path,
    intent_store_root: Path,
    scope: TenantRepoScope,
    runtime_bundle_obj: Mapping[str, Any],
    report_obj: Mapping[str, Any] | None,
) -> tuple[IntentSpecV1, dict[str, str], list[OperationalVerificationFinding]]:
    from akc.intent.policy_projection import build_handoff_intent_ref, project_runtime_intent_projection

    findings: list[OperationalVerificationFinding] = []
    bundle_ref = _normalize_intent_ref(runtime_bundle_obj.get("intent_ref"))
    report_ref = _normalize_intent_ref(report_obj.get("intent_ref")) if isinstance(report_obj, Mapping) else None
    if bundle_ref is None:
        raise ValueError(
            f"tenant/repo trust boundary violated for {scope.tenant_id}/{scope.repo_id}: "
            "runtime bundle is missing intent_ref"
        )
    if report_ref is not None:
        diff = _intent_ref_diff(bundle_ref, report_ref)
        if diff:
            raise ValueError(
                f"tenant/repo trust boundary violated for {scope.tenant_id}/{scope.repo_id}: "
                "runtime bundle intent_ref diverges from operational_validity_report intent_ref"
            )

    # Match compile session: JsonFileIntentStore(base_dir=<outputs_root>) →
    # <outputs_root>/.akc/intent/<tenant>/<repo>/<intent_id>.json
    store = JsonFileIntentStore(base_dir=intent_store_root)
    loaded = store.load_intent(
        tenant_id=scope.tenant_id,
        repo_id=scope.repo_id,
        intent_id=bundle_ref["intent_id"],
    )
    if loaded is None:
        raise ValueError(
            f"tenant/repo trust boundary violated for {scope.tenant_id}/{scope.repo_id}: "
            f"intent_id {bundle_ref['intent_id']!r} not found under .akc/intent"
        )
    normalized = loaded.normalized()
    disk_sha = stable_intent_sha256(intent=normalized)
    if disk_sha != bundle_ref["stable_intent_sha256"]:
        raise ValueError(
            f"tenant/repo trust boundary violated for {scope.tenant_id}/{scope.repo_id}: "
            "runtime bundle intent_ref stable_intent_sha256 does not match intent store bytes"
        )
    canonical_ref = {
        str(k): str(v)
        for k, v in build_handoff_intent_ref(intent=normalized).items()
        if isinstance(k, str) and isinstance(v, str)
    }
    diff = _intent_ref_diff(bundle_ref, canonical_ref)
    if diff:
        raise ValueError(
            f"tenant/repo trust boundary violated for {scope.tenant_id}/{scope.repo_id}: "
            "runtime bundle intent_ref does not match normalized intent authority"
        )
    if report_ref is not None and report_ref != canonical_ref:
        raise ValueError(
            f"tenant/repo trust boundary violated for {scope.tenant_id}/{scope.repo_id}: "
            "operational_validity_report intent_ref does not match normalized intent authority"
        )

    bundle_projection = runtime_bundle_obj.get("intent_policy_projection")
    if isinstance(bundle_projection, Mapping):
        recomputed_projection = project_runtime_intent_projection(intent=normalized).to_json_obj()
        if stable_json_fingerprint(bundle_projection) != stable_json_fingerprint(recomputed_projection):
            findings.append(
                OperationalVerificationFinding(
                    code="intent.projection_mismatch",
                    message="runtime bundle intent_policy_projection does not match normalized intent projection",
                    severity="error",
                )
            )
    return normalized, canonical_ref, findings


def _enforcement_mode_from_profile(
    *,
    rollout_stage: str | None,
    verifier_enforcement: str | None,
) -> OperationalEnforcementMode:
    raw = str(verifier_enforcement or "auto").strip().lower()
    if raw == "advisory":
        return "advisory"
    if raw == "blocking":
        return "blocking"
    stage = str(rollout_stage or "").strip().lower()
    if stage == "observe":
        return "advisory"
    return "blocking"


def _collect_stub_ids(specs: Sequence[tuple[str, OperationalValidityParams]]) -> tuple[str, ...]:
    out: set[str] = set()
    for _sc_id, params in specs:
        for sig in params.signals:
            raw = str(sig.otel_query_stub or "").strip()
            if raw:
                out.add(raw)
    return tuple(sorted(out))


def _load_telemetry_bindings(*, scope_root: Path) -> dict[str, dict[str, Any]]:
    path = (scope_root / ".akc" / "control" / "telemetry_bindings.json").resolve()
    ensure_path_under_repo_outputs(path, repo_outputs_root=scope_root)
    if not path.is_file():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        return {}
    providers = raw.get("providers")
    providers_by_id = providers if isinstance(providers, dict) else {}
    bindings_obj = raw.get("bindings")
    if not isinstance(bindings_obj, dict):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for stub_id, cfg in bindings_obj.items():
        sid = str(stub_id).strip()
        if not sid or not isinstance(cfg, dict):
            continue
        provider_id = str(cfg.get("provider", "")).strip()
        if provider_id and provider_id in providers_by_id and isinstance(providers_by_id.get(provider_id), dict):
            merged = dict(cast(dict[str, Any], providers_by_id[provider_id]))
            merged.update(dict(cfg))
            merged["provider_id"] = provider_id
            out[sid] = merged
            continue
        out[sid] = dict(cfg)
    return out


def _fetch_http_text(url: str, *, timeout_ms: int) -> str:
    req = urllib.request.Request(url=url, method="GET")
    with urllib.request.urlopen(req, timeout=max(0.1, float(timeout_ms) / 1000.0)) as resp:
        raw = resp.read()
    return cast(str, raw.decode("utf-8"))


def _load_stub_provider_data(
    *,
    scope_root: Path,
    stub_id: str,
    binding: Mapping[str, Any],
) -> tuple[
    tuple[RuntimeEvidenceRecord, ...],
    tuple[dict[str, JSONValue], ...],
    tuple[dict[str, JSONValue], ...],
    dict[str, JSONValue],
]:
    kind = str(binding.get("kind", "artifact_ndjson")).strip() or "artifact_ndjson"
    record_kind = str(binding.get("record_kind", "runtime_evidence")).strip() or "runtime_evidence"
    details: dict[str, JSONValue] = {
        "stub_id": stub_id,
        "provider": kind,
        "record_kind": record_kind,
        "status": "ok",
    }
    text: str | None = None
    if kind == "artifact_ndjson":
        rel = str(binding.get("path", "")).strip()
        if not rel:
            raise ValueError("artifact_ndjson binding requires path")
        p = ensure_path_under_repo_outputs((scope_root / rel).resolve(), repo_outputs_root=scope_root)
        text = p.read_text(encoding="utf-8")
        details["path"] = rel
    elif kind == "http_json":
        url = str(binding.get("url", "")).strip()
        if not (url.startswith("http://") or url.startswith("https://")):
            raise ValueError("http_json binding requires http(s) url")
        timeout_ms_raw = binding.get("timeout_ms", 5000)
        timeout_ms = (
            int(timeout_ms_raw)
            if isinstance(timeout_ms_raw, (int, float)) and not isinstance(timeout_ms_raw, bool)
            else 5000
        )
        text = _fetch_http_text(url, timeout_ms=timeout_ms)
        details["url"] = url
    else:
        raise ValueError(f"unsupported telemetry provider kind: {kind!r}")

    if text is None:
        return (), (), (), details
    if record_kind == "runtime_evidence":
        loaded = json.loads(text)
        if not isinstance(loaded, list):
            raise ValueError("runtime_evidence provider payload must be a JSON array")
        validate_artifact_json(obj=loaded, kind="runtime_evidence_stream")
        ev = tuple(RuntimeEvidenceRecord.from_json_obj(x) for x in loaded if isinstance(x, dict))
        details["record_count"] = len(ev)
        return ev, (), (), details
    if record_kind == "otel":
        otel = parse_otel_ndjson_slice(text)
        details["record_count"] = len(otel)
        return (), otel, (), details
    if record_kind == "otel_metrics":
        metric_parse = parse_otel_metric_ndjson_slice(text)
        details["record_count"] = len(metric_parse.records)
        if metric_parse.rejected_reason:
            details["parse_rejected_reason"] = metric_parse.rejected_reason
        return (), (), metric_parse.records, details
    raise ValueError(f"unsupported telemetry provider record_kind: {record_kind!r}")


def verify_run_operational_coupling(
    *,
    outputs_root: Path,
    scope: TenantRepoScope,
    run_id: str,
    strict_manifest: bool,
) -> OperationalVerificationResult:
    manifest_path = resolve_run_manifest_path(
        manifest_path=None,
        outputs_root=outputs_root,
        tenant_id=scope.tenant_id,
        repo_id=scope.repo_id,
        run_id=run_id,
    )
    manifest = RunManifest.from_json_file(manifest_path)
    outputs_root_resolved, scope_root = resolve_outputs_and_scope_root_for_manifest(manifest_path, manifest)

    findings: list[OperationalVerificationFinding] = []
    cp = manifest.control_plane or {}
    governance_profile = resolve_governance_profile_for_scope(scope_root)
    enforcement_mode: OperationalEnforcementMode = _enforcement_mode_from_profile(
        rollout_stage=(governance_profile.rollout_stage if governance_profile is not None else None),
        verifier_enforcement=(governance_profile.verifier_enforcement if governance_profile is not None else None),
    )
    provider_allowlist = (
        set(governance_profile.provider_allowlist)
        if governance_profile is not None and governance_profile.provider_allowlist
        else set()
    )

    bundle_rel = (
        manifest.runtime_bundle.path
        if manifest.runtime_bundle is not None
        else f".akc/runtime/{run_id}.runtime_bundle.json"
    )
    bundle_path = _artifact_path_from_rel(scope_root=scope_root, rel_path=bundle_rel, kind="runtime bundle")
    runtime_bundle_obj = _read_json(bundle_path, what="runtime bundle")

    report_obj: dict[str, Any] | None = None
    report_path: Path | None = None
    report_pointer = cp.get("operational_validity_report_ref")
    if isinstance(report_pointer, Mapping):
        report_rel = str(report_pointer.get("path", "")).strip()
        if report_rel:
            report_path = _artifact_path_from_rel(scope_root=scope_root, rel_path=report_rel, kind="operational report")
            if report_path.is_file():
                report_obj = _read_json(report_path, what="operational validity report")
                validate_artifact_json(obj=report_obj, kind="operational_validity_report")

    intent, canonical_ref, authority_findings = _load_authoritative_intent(
        scope_root=scope_root,
        intent_store_root=outputs_root_resolved,
        scope=scope,
        runtime_bundle_obj=runtime_bundle_obj,
        report_obj=report_obj,
    )
    findings.extend(authority_findings)

    specs = _post_runtime_operational_specs(intent)
    provider_result_rows: list[dict[str, JSONValue]] = []
    all_stub_ids = _collect_stub_ids(specs)
    binding_map = _load_telemetry_bindings(scope_root=scope_root) if all_stub_ids else {}
    for stub in all_stub_ids:
        if stub not in binding_map:
            provider_result_rows.append({"stub_id": stub, "provider": "none", "status": "missing", "details": None})
            findings.append(
                OperationalVerificationFinding(
                    code="operational.stub_binding_missing",
                    message=f"otel_query_stub={stub!r} has no operator telemetry binding",
                    severity="warning",
                )
            )
    if not specs:
        assurance_result_early: dict[str, JSONValue] = apply_schema_envelope(
            obj={
                "run_id": run_id,
                "tenant_id": scope.tenant_id,
                "repo_id": scope.repo_id,
                "checked_at_ms": int(time.time() * 1000),
                "enforcement_mode": enforcement_mode,
                "passed": not any(f.severity == "error" for f in findings),
                "advisory_only": enforcement_mode == "advisory",
                "findings": [f.to_json_obj() for f in findings],
                "provider_results": provider_result_rows,
                "attestation_fingerprint_sha256": stable_json_fingerprint(
                    {
                        "run_id": run_id,
                        "findings": [f.to_json_obj() for f in findings],
                        "provider_results": provider_result_rows,
                    }
                ),
            },
            kind="operational_assurance_result",
        )
        return OperationalVerificationResult(
            scope=scope,
            run_id=run_id,
            authority="none",
            passed=not any(f.severity == "error" for f in findings),
            checked_manifest_strict=False,
            had_report=report_obj is not None,
            recomputed_attestation_fingerprint_sha256=None,
            stored_attestation_fingerprint_sha256=(
                str(report_obj.get("attestation_fingerprint_sha256", "")).strip().lower()
                if isinstance(report_obj, Mapping)
                else None
            ),
            enforcement_mode=enforcement_mode,
            blocking_passed=not any(f.severity == "error" for f in findings) or enforcement_mode == "advisory",
            advisory_only=enforcement_mode == "advisory",
            assurance_result=assurance_result_early,
            assurance_rel_path=f".akc/verification/{run_id}.operational_assurance.json",
            findings=tuple(findings),
        )

    evidence, evidence_path, evidence_pointer_sha = _load_runtime_evidence(
        scope_root=scope_root,
        manifest=manifest,
        report_obj=report_obj,
    )
    otel_records: tuple[dict[str, JSONValue], ...] = ()
    otel_metric_records: tuple[dict[str, JSONValue], ...] | None = None
    otel_metric_parse_rejected_reason: str | None = None
    otel_path = _otel_run_sidecar_path(scope_root=scope_root, run_id=run_id, metrics=False)
    if otel_path.is_file():
        otel_records = parse_otel_ndjson_slice(otel_path.read_text(encoding="utf-8"))
    otel_metric_path = _otel_run_sidecar_path(scope_root=scope_root, run_id=run_id, metrics=True)
    if otel_metric_path.is_file():
        metric_parse = parse_otel_metric_ndjson_slice(otel_metric_path.read_text(encoding="utf-8"))
        otel_metric_records = metric_parse.records
        otel_metric_parse_rejected_reason = metric_parse.rejected_reason
    # Merge external telemetry providers configured by operator bindings.
    for stub in all_stub_ids:
        binding = binding_map.get(stub)
        if binding is None:
            continue
        provider_kind = str(binding.get("kind", "artifact_ndjson")).strip() or "artifact_ndjson"
        if provider_allowlist and provider_kind not in provider_allowlist:
            provider_result_rows.append(
                {
                    "stub_id": stub,
                    "provider": provider_kind,
                    "status": "error",
                    "details": {"reason": "provider_not_allowlisted"},
                }
            )
            findings.append(
                OperationalVerificationFinding(
                    code="operational.provider_not_allowlisted",
                    message=(
                        f"otel_query_stub={stub!r} provider={provider_kind!r} is outside governance provider_allowlist"
                    ),
                )
            )
            continue
        try:
            extra_ev, extra_otel, extra_metrics, details = _load_stub_provider_data(
                scope_root=scope_root,
                stub_id=stub,
                binding=binding,
            )
        except (OSError, ValueError, json.JSONDecodeError, UnicodeDecodeError, urllib.error.URLError) as exc:
            provider_result_rows.append(
                {
                    "stub_id": stub,
                    "provider": provider_kind,
                    "status": "error",
                    "details": {"error": str(exc)},
                }
            )
            findings.append(
                OperationalVerificationFinding(
                    code="operational.provider_load_failed",
                    message=f"failed to load telemetry binding for stub={stub!r}: {exc}",
                    severity="warning" if enforcement_mode == "advisory" else "error",
                )
            )
            continue
        if extra_ev:
            evidence = tuple(list(evidence) + list(extra_ev))
        if extra_otel:
            otel_records = tuple(list(otel_records) + list(extra_otel))
        if extra_metrics:
            otel_metric_records = tuple(list(otel_metric_records or ()) + list(extra_metrics))
            otel_metric_parse_rejected_reason = None
        provider_result_rows.append(
            {
                "stub_id": stub,
                "provider": provider_kind,
                "status": "ok",
                "details": _safe_json_obj(details),
            }
        )

    bundle_schema_version: int | None = None
    raw_sv = runtime_bundle_obj.get("schema_version")
    if isinstance(raw_sv, int) and not isinstance(raw_sv, bool):
        bundle_schema_version = int(raw_sv)

    verdicts: list[
        tuple[
            str,
            OperationalValidityParams,
            OperationalVerdict,
            tuple[RuntimeEvidenceRecord, ...],
            OperationalEvidenceRollupMeta | None,
        ]
    ] = []
    for sc_id, params in specs:
        evidence_use = evidence
        rolling_meta: OperationalEvidenceRollupMeta | None = None
        if params.window == "rolling_ms":
            rel = str(params.evidence_rollup_rel_path or "").strip()
            if not rel:
                findings.append(
                    OperationalVerificationFinding(
                        code="operational.rollup_missing",
                        message=f"success criterion {sc_id!r} requires evidence_rollup_rel_path",
                    )
                )
                continue
            try:
                evidence_use, rolling_meta = load_merged_runtime_evidence_from_rollup_path(
                    rollup_path=scope_root / rel,
                    repo_outputs_root=scope_root,
                )
            except (OperationalRollupLoadError, OperationalPathScopeError) as exc:
                findings.append(
                    OperationalVerificationFinding(
                        code="operational.rollup_load_failed",
                        message=f"success criterion {sc_id!r} rollup load failed: {exc}",
                    )
                )
                continue
        verdict = evaluate_operational_spec(
            params=params,
            evidence=evidence_use,
            otel_records=otel_records,
            otel_contract=None,
            runtime_bundle_schema_version=bundle_schema_version,
            success_criterion_id=sc_id,
            rolling_rollup_meta=rolling_meta,
            otel_metric_records=otel_metric_records,
            otel_metric_parse_rejected_reason=otel_metric_parse_rejected_reason,
        )
        verdicts.append((sc_id, params, verdict, evidence_use, rolling_meta))
        if not verdict.passed:
            findings.append(
                OperationalVerificationFinding(
                    code="operational.verdict_failed",
                    message=f"operational_spec failed for success criterion {sc_id!r}",
                    evidence={
                        "success_criterion_id": sc_id,
                        "failures": list(verdict.failures),
                        "passed": False,
                    },
                )
            )

    fp_parts: list[str] = []
    predicate_rows: list[dict[str, JSONValue]] = []
    for sc_id, params, verdict, evidence_use, roll_meta in verdicts:
        _ = sc_id
        fp_parts.append(
            combined_operational_fingerprint(
                params=params,
                evidence=evidence_use,
                verdict=verdict,
                otel_records=otel_records,
                otel_contract=None,
                runtime_bundle_schema_version=bundle_schema_version,
                rolling_rollup_meta=roll_meta,
                otel_metric_records=otel_metric_records,
                otel_metric_parse_rejected_reason=otel_metric_parse_rejected_reason,
            )
        )
        predicate_rows.extend(_predicate_rows_from_verdict(verdict.per_criterion))

    recomputed_attestation = stable_json_fingerprint(
        {"criteria": [{"id": sc_id, "sha256": fp_parts[i]} for i, (sc_id, _, _, _, _) in enumerate(verdicts)]}
    )
    stored_attestation = None
    if isinstance(report_obj, Mapping):
        raw = report_obj.get("attestation_fingerprint_sha256")
        if isinstance(raw, str) and raw.strip():
            stored_attestation = raw.strip().lower()
        if _normalize_intent_ref(report_obj.get("intent_ref")) != canonical_ref:
            findings.append(
                OperationalVerificationFinding(
                    code="intent.report_authority_mismatch",
                    message="operational_validity_report intent_ref does not match authoritative intent",
                )
            )
        report_passed = report_obj.get("passed")
        recomputed_passed = all(verdict.passed for _, _, verdict, _, _ in verdicts)
        if isinstance(report_passed, bool) and report_passed != recomputed_passed:
            findings.append(
                OperationalVerificationFinding(
                    code="operational.report_passed_mismatch",
                    message="operational_validity_report passed flag does not match recomputed verdict",
                )
            )
        if stored_attestation and stored_attestation != recomputed_attestation:
            findings.append(
                OperationalVerificationFinding(
                    code="operational.report_fingerprint_mismatch",
                    message="operational_validity_report attestation fingerprint does not match recomputed result",
                    evidence={
                        "stored": stored_attestation,
                        "recomputed": recomputed_attestation,
                    },
                )
            )
        if report_path is not None and isinstance(report_pointer, Mapping):
            pointer_sha = str(report_pointer.get("sha256", "")).strip().lower()
            current_sha = _pointer_json_sha256(report_path)
            if pointer_sha and current_sha and pointer_sha != current_sha:
                findings.append(
                    OperationalVerificationFinding(
                        code="operational.report_pointer_sha_mismatch",
                        message=(
                            "manifest control_plane operational_validity_report_ref sha256 "
                            "does not match current report bytes"
                        ),
                    )
                )

    checked_manifest_strict = False
    manifest_fp = str(cp.get("operational_validity_fingerprint_sha256", "")).strip().lower()
    current_evidence_sha = _pointer_json_sha256(evidence_path) if evidence_path is not None else None
    evidence_bytes_unchanged = bool(
        evidence_path is not None
        and evidence_pointer_sha
        and current_evidence_sha
        and evidence_pointer_sha == current_evidence_sha
    )
    if strict_manifest and evidence_bytes_unchanged and bool(manifest_fp):
        checked_manifest_strict = True
        if manifest_fp != recomputed_attestation:
            findings.append(
                OperationalVerificationFinding(
                    code="operational.manifest_fingerprint_mismatch",
                    message=(
                        "manifest control_plane operational_validity_fingerprint_sha256 "
                        "does not match recomputed attestation"
                    ),
                    evidence={
                        "stored": manifest_fp,
                        "recomputed": recomputed_attestation,
                    },
                )
            )

    if predicate_rows and report_obj is None:
        findings.append(
            OperationalVerificationFinding(
                code="operational.report_missing_recomputed",
                message="operational validity report is absent; replay authority fell back to recomputation",
                severity="warning",
            )
        )

    if governance_profile is not None:
        max_errors = int(governance_profile.max_errors_before_block)
        if max_errors >= 0:
            err_count = len([f for f in findings if f.severity == "error"])
            if err_count > max_errors and enforcement_mode == "blocking":
                findings.append(
                    OperationalVerificationFinding(
                        code="operational.governance_error_threshold_exceeded",
                        message=(
                            f"governance max_errors_before_block={max_errors} exceeded by {err_count} error finding(s)"
                        ),
                        severity="error",
                        evidence={"max_errors_before_block": max_errors, "error_count": err_count},
                    )
                )

    passed = not any(f.severity == "error" for f in findings)
    assurance_result: dict[str, JSONValue] = apply_schema_envelope(
        obj={
            "run_id": run_id,
            "tenant_id": scope.tenant_id,
            "repo_id": scope.repo_id,
            "checked_at_ms": int(time.time() * 1000),
            "enforcement_mode": enforcement_mode,
            "passed": bool(passed),
            "advisory_only": enforcement_mode == "advisory",
            "findings": [f.to_json_obj() for f in findings],
            "provider_results": provider_result_rows,
            "attestation_fingerprint_sha256": stable_json_fingerprint(
                {
                    "run_id": run_id,
                    "enforcement_mode": enforcement_mode,
                    "passed": bool(passed),
                    "findings": [f.to_json_obj() for f in findings],
                    "provider_results": provider_result_rows,
                    "recomputed_attestation_fingerprint_sha256": recomputed_attestation,
                    "governance_profile_rel_path": GOVERNANCE_PROFILE_RESOLVED_REL_PATH,
                }
            ),
        },
        kind="operational_assurance_result",
    )

    return OperationalVerificationResult(
        scope=scope,
        run_id=run_id,
        authority="recomputed",
        passed=passed,
        checked_manifest_strict=checked_manifest_strict,
        had_report=report_obj is not None,
        recomputed_attestation_fingerprint_sha256=recomputed_attestation,
        stored_attestation_fingerprint_sha256=stored_attestation or manifest_fp or None,
        enforcement_mode=enforcement_mode,
        blocking_passed=passed or enforcement_mode == "advisory",
        advisory_only=enforcement_mode == "advisory",
        assurance_result=assurance_result,
        assurance_rel_path=f".akc/verification/{run_id}.operational_assurance.json",
        findings=tuple(findings),
    )
