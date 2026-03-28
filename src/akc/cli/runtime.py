from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

from akc.artifacts.contracts import apply_schema_envelope, is_runtime_bundle_schema_id
from akc.artifacts.validate import validate_artifact_json
from akc.control.operations_index import try_upsert_operations_index_from_manifest
from akc.control.otel_export import otel_export_extra_callbacks_from_env
from akc.control.policy_denial_explain import (
    format_policy_denial_for_text_stderr,
    runtime_policy_denial_from_denied_decision,
    runtime_policy_denial_from_permission_error,
)
from akc.control.policy_provenance import apply_env_policy_provenance
from akc.coordination.protocol import coordination_schedule_to_jsonable
from akc.intent.models import (
    OperationalValidityParams,
    OperationalValidityParamsError,
    parse_operational_validity_params,
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
    operational_verdict_for_rollup_load_failure,
    parse_otel_metric_ndjson_slice,
    parse_otel_ndjson_slice,
)
from akc.memory.models import JSONValue, normalize_repo_id
from akc.promotion import normalize_promotion_mode, verify_signed_packet
from akc.run import ArtifactPointer, RunManifest, RuntimeEvidenceRecord, replay_runtime_execution
from akc.run.delivery_lifecycle import project_delivery_run_projection, resolve_delivery_target_lane
from akc.run.time_compression import derive_time_compression_metrics
from akc.runtime.adapters.local_depth import LocalDepthRuntimeAdapter
from akc.runtime.adapters.native import NativeRuntimeAdapter
from akc.runtime.bundle_delivery import build_delivery_handoff_context
from akc.runtime.compile_apply_attestation import verify_compile_apply_attestation_for_rollout
from akc.runtime.coordination.load import load_coordination_for_bundle
from akc.runtime.coordination.models import CoordinationScheduler
from akc.runtime.events import RuntimeEventBus
from akc.runtime.kernel import RuntimeKernel
from akc.runtime.models import (
    ReconcileStatus,
    RuntimeBundle,
    RuntimeBundleRef,
    RuntimeContext,
    RuntimeEvent,
    RuntimePolicyMode,
)
from akc.runtime.policy import (
    RuntimePolicyRuntime,
    RuntimeScopeMismatchError,
    runtime_evidence_expectation_violations,
)
from akc.runtime.providers import create_deployment_provider
from akc.runtime.providers.factory import full_layer_replacement_enabled
from akc.runtime.reconciler import DeploymentReconciler, ReconcileEvidence, ReconcileMode
from akc.runtime.resync_backoff import compute_resync_sleep_ms, parse_resync_backoff_config
from akc.runtime.scheduler import InMemoryRuntimeScheduler
from akc.runtime.state_store import FileSystemRuntimeStateStore
from akc.utils.fingerprint import stable_json_fingerprint
from akc.validation import execute_validator_bindings, merge_validator_evidence, resolve_validator_bindings_path

from .common import configure_logging
from .profile_defaults import (
    resolve_developer_role_profile,
    resolve_optional_project_string,
    resolve_runtime_start_profile_defaults,
)
from .project_config import AkcProjectConfig, load_akc_project_config


@dataclass(frozen=True, slots=True)
class _ReconcileCLIResult:
    statuses: tuple[ReconcileStatus, ...]
    evidence: tuple[ReconcileEvidence, ...]
    completed_attempts: int
    configured_max_attempts: int
    interval_ms: int
    total_resync_wait_ms: int


def _stable_intent_sha256_from_bundle_metadata(metadata: Mapping[str, JSONValue]) -> str | None:
    intent_ref = metadata.get("intent_ref")
    if isinstance(intent_ref, Mapping):
        sha = str(intent_ref.get("stable_intent_sha256", "")).strip().lower()
        if len(sha) == 64:
            return sha
    return None


def _stable_intent_sha256_from_bundle_file(bundle_path: Path) -> str | None:
    try:
        raw = json.loads(bundle_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(raw, dict):
        return None
    meta = raw.get("metadata")
    if isinstance(meta, dict):
        hit = _stable_intent_sha256_from_bundle_metadata(cast(Mapping[str, JSONValue], meta))
        if hit is not None:
            return hit
    intent_ref = raw.get("intent_ref")
    if isinstance(intent_ref, dict):
        sha = str(intent_ref.get("stable_intent_sha256", "")).strip().lower()
        if len(sha) == 64:
            return sha
    return None


def _aggregate_terminal_health_from_evidence(evidence: tuple[RuntimeEvidenceRecord, ...]) -> str | None:
    for rec in reversed(evidence):
        if rec.evidence_type != "terminal_health":
            continue
        if rec.payload.get("aggregate") is True:
            hs = rec.payload.get("health_status")
            if isinstance(hs, str) and hs.strip():
                return hs.strip()
    for rec in reversed(evidence):
        if rec.evidence_type == "terminal_health":
            hs = rec.payload.get("health_status")
            if isinstance(hs, str) and hs.strip():
                return hs.strip()
    return None


def _health_rank(value: str) -> int:
    return {
        "failed": 4,
        "degraded": 3,
        "unknown": 2,
        "healthy": 1,
    }.get(value.strip().lower(), 2)


def _worst_health_status(*values: str | None) -> str:
    worst = "healthy"
    worst_r = 1
    for raw in values:
        if raw is None or not str(raw).strip():
            continue
        candidate = str(raw).strip().lower()
        r = _health_rank(candidate)
        if r > worst_r:
            worst_r = r
            worst = candidate if candidate in {"failed", "degraded", "unknown", "healthy"} else "unknown"
    return worst


def _kernel_loop_health(terminal_status: str) -> str:
    ts = terminal_status.strip()
    if ts == "max_iterations_exceeded":
        return "degraded"
    if ts in {"terminal", "idle", "stopped"}:
        return "healthy"
    return "unknown"


def _evidence_correlation(*, context: RuntimeContext, stable_intent_sha256: str | None) -> dict[str, JSONValue]:
    out: dict[str, JSONValue] = {
        "tenant_id": context.tenant_id.strip(),
        "repo_id": context.repo_id.strip(),
        "compile_run_id": context.run_id.strip(),
        "runtime_run_id": context.runtime_run_id.strip(),
    }
    if stable_intent_sha256 is not None and str(stable_intent_sha256).strip():
        out["stable_intent_sha256"] = str(stable_intent_sha256).strip().lower()
    return out


def _read_json_object(path: Path, *, what: str) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"{what} must be a JSON object")
    return raw


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass(frozen=True, slots=True)
class OperationalValidityReportSummary:
    """Outcome of optional post-runtime operational validity evaluation + report write."""

    ran_evaluation: bool
    passed_all: bool
    failed_success_criterion_ids: tuple[str, ...]


def _outputs_root_for_record(record: dict[str, Any]) -> Path:
    raw = record.get("outputs_root")
    if isinstance(raw, str) and raw.strip():
        return Path(raw).expanduser().resolve()
    scope_dir = Path(str(record["scope_dir"])).resolve()
    # Expected: <outputs_root>/<tenant>/<repo>/.akc/runtime/<run_id>/<runtime_run_id>
    if len(scope_dir.parts) < 7:
        raise ValueError("runtime record scope_dir is too shallow to infer outputs_root")
    return scope_dir.parents[5]


def _runtime_context_from_record(record: dict[str, Any]) -> RuntimeContext:
    mode_raw = str(record.get("mode", "enforce")).strip() or "enforce"
    return RuntimeContext(
        tenant_id=str(record["tenant_id"]).strip(),
        repo_id=str(record["repo_id"]).strip(),
        run_id=str(record["run_id"]).strip(),
        runtime_run_id=str(record["runtime_run_id"]).strip(),
        policy_mode=cast(RuntimePolicyMode, mode_raw),
        adapter_id=str(record.get("adapter_id", "native")).strip() or "native",
    )


def _append_operational_validity_attested_failure_event(
    record: dict[str, Any],
    *,
    failed_success_criterion_ids: tuple[str, ...],
) -> None:
    ctx = _runtime_context_from_record(record)
    payload: dict[str, JSONValue] = {"passed": False}
    norm_ids = tuple(str(x).strip() for x in failed_success_criterion_ids if str(x).strip())
    if norm_ids:
        payload["success_criterion_ids"] = list(norm_ids)
        if len(norm_ids) == 1:
            payload["success_criterion_id"] = norm_ids[0]
    event = RuntimeEvent(
        event_id=f"{ctx.runtime_run_id}:runtime.operational_validity.attested:{_now_ms()}",
        event_type="runtime.operational_validity.attested",
        timestamp=_now_ms(),
        context=ctx,
        payload=payload,
    )
    store = FileSystemRuntimeStateStore(
        root=_outputs_root_for_record(record),
        otel_export_extra_callbacks=otel_export_extra_callbacks_from_env(),
    )
    store.append_event(context=ctx, event=event)


def _json_sha256(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    import hashlib

    return hashlib.sha256(raw).hexdigest()


def _infer_outputs_root_from_bundle(bundle_path: Path) -> Path | None:
    parts = bundle_path.resolve().parts
    try:
        akc_index = parts.index(".akc")
    except ValueError:
        return None
    if akc_index < 2:
        return None
    return Path(*parts[: akc_index - 2])


def _find_latest_runtime_bundle(outputs_root: Path) -> Path | None:
    candidates = sorted(
        outputs_root.expanduser().rglob("*.runtime_bundle.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[0].resolve() if candidates else None


def _repo_outputs_root_from_bundle_path(bundle_path: Path) -> Path | None:
    """Return ``<outputs_root>/<tenant_id>/<repo_id>`` when ``bundle_path`` contains ``.akc``."""

    parts = bundle_path.resolve().parts
    try:
        akc_index = parts.index(".akc")
    except ValueError:
        return None
    if akc_index < 2:
        return None
    return Path(*parts[:akc_index])


def _repo_outputs_root_for_operational_eval(record: dict[str, Any], *, bundle_path: Path) -> Path:
    """Canonical tenant/repo output directory; bundle path must agree with ``scope_dir`` when both apply."""

    from_bundle = _repo_outputs_root_from_bundle_path(bundle_path)
    from_scope = _scope_root(record)
    if from_bundle is not None:
        if from_bundle.resolve() != from_scope.resolve():
            raise OperationalPathScopeError(
                "runtime bundle path and scope_dir resolve to different tenant/repo output roots"
            )
        return from_bundle
    return from_scope


def _scope_dir(*, outputs_root: Path, tenant_id: str, repo_id: str, run_id: str, runtime_run_id: str) -> Path:
    return (
        outputs_root.expanduser()
        / tenant_id.strip()
        / normalize_repo_id(repo_id)
        / ".akc"
        / "runtime"
        / run_id.strip()
        / runtime_run_id.strip()
    )


def _runtime_record_path(*, outputs_root: Path, tenant_id: str, repo_id: str, run_id: str, runtime_run_id: str) -> Path:
    return (
        _scope_dir(
            outputs_root=outputs_root,
            tenant_id=tenant_id,
            repo_id=repo_id,
            run_id=run_id,
            runtime_run_id=runtime_run_id,
        )
        / "runtime_run.json"
    )


def _control_path(record: dict[str, Any]) -> Path:
    return Path(str(record["control_path"]))


def _events_path(record: dict[str, Any]) -> Path:
    return Path(str(record["events_path"]))


def _checkpoint_path(record: dict[str, Any]) -> Path:
    return Path(str(record["checkpoint_path"]))


def _evidence_path(record: dict[str, Any]) -> Path:
    return Path(str(record["runtime_evidence_path"]))


def _coordination_audit_path(record: dict[str, Any]) -> Path:
    return Path(str(record["coordination_audit_path"]))


def _policy_log_path(record: dict[str, Any]) -> Path:
    return Path(str(record["policy_decisions_path"]))


def _queue_snapshot_path(record: dict[str, Any]) -> Path:
    return Path(str(record["queue_snapshot_path"]))


def _scope_root(record: dict[str, Any]) -> Path:
    return Path(str(record["scope_dir"])).resolve().parents[3]


def _compile_run_manifest_path(record: dict[str, Any]) -> Path:
    return _scope_root(record) / ".akc" / "run" / f"{str(record['run_id']).strip()}.manifest.json"


def _rel_artifact_path(path: Path, *, record: dict[str, Any]) -> str:
    resolved = path.resolve()
    try:
        return str(resolved.relative_to(_scope_root(record)))
    except ValueError:
        return str(resolved)


def _last_policy_decision_id(policy_runtime: RuntimePolicyRuntime | None) -> str | None:
    if policy_runtime is None or not policy_runtime.decision_log:
        return None
    token_id = policy_runtime.decision_log[-1].get("token_id")
    if isinstance(token_id, str) and token_id.strip():
        return token_id.strip()
    return None


def _emit_runtime_error(
    summary: str,
    *,
    hint: str | None = None,
    policy_decision_id: str | None = None,
    policy_denial: dict[str, Any] | None = None,
    format_mode: str = "text",
) -> int:
    if format_mode == "json":
        payload: dict[str, Any] = {"error": summary}
        if hint:
            payload["hint"] = hint
        if policy_decision_id:
            payload["policy_decision_id"] = policy_decision_id
        if policy_denial is not None:
            payload["policy_denial_explain"] = policy_denial
        print(json.dumps(payload, sort_keys=True, ensure_ascii=False), flush=True)
        return 2
    print(f"Runtime CLI error: {summary}", file=sys.stderr)
    if hint:
        print(f"hint: {hint}", file=sys.stderr)
    if policy_decision_id:
        print(f"policy_decision_id: {policy_decision_id}", file=sys.stderr)
    if policy_denial is not None:
        print(format_policy_denial_for_text_stderr(policy_denial), file=sys.stderr, end="", flush=True)
    return 2


def _last_denied_policy_decision_from_record(record: dict[str, Any]) -> dict[str, Any] | None:
    raw = record.get("policy_decisions", [])
    if not isinstance(raw, list):
        return None
    for item in reversed(raw):
        if isinstance(item, dict) and not bool(item.get("allowed", False)):
            return dict(item)
    return None


def _policy_decision_id_from_record(record: dict[str, Any]) -> str | None:
    raw = record.get("policy_decisions", [])
    if not isinstance(raw, list) or not raw:
        return None
    last = raw[-1]
    if not isinstance(last, dict):
        return None
    token_id = last.get("token_id")
    if isinstance(token_id, str) and token_id.strip():
        return token_id.strip()
    return None


def _load_runtime_record(record_path: Path) -> dict[str, Any]:
    return _read_json_object(record_path, what="runtime run record")


def _resolve_runtime_run_record(
    *,
    outputs_root: Path,
    runtime_run_id: str,
    tenant_id: str | None = None,
    repo_id: str | None = None,
) -> dict[str, Any]:
    tenant_hint = tenant_id.strip() if isinstance(tenant_id, str) and tenant_id.strip() else None
    repo_hint = normalize_repo_id(repo_id) if isinstance(repo_id, str) and repo_id.strip() else None
    matches: list[dict[str, Any]] = []
    scope_mismatches: list[dict[str, Any]] = []
    for record_path in outputs_root.expanduser().rglob("runtime_run.json"):
        try:
            record = _load_runtime_record(record_path)
        except Exception:
            continue
        candidate_id = str(record.get("runtime_run_id", "")).strip()
        if candidate_id != runtime_run_id.strip():
            continue
        candidate_tenant = str(record.get("tenant_id", "")).strip()
        candidate_repo = normalize_repo_id(str(record.get("repo_id", "")).strip())
        if tenant_hint is not None and tenant_hint != candidate_tenant:
            scope_mismatches.append(record)
            continue
        if repo_hint is not None and repo_hint != candidate_repo:
            scope_mismatches.append(record)
            continue
        matches.append(record)
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise ValueError("multiple runtime runs matched this runtime_run_id; rerun with --tenant-id and --repo-id")
    if scope_mismatches:
        mismatch = scope_mismatches[0]
        raise RuntimeScopeMismatchError(
            "runtime scope mismatch; this runtime_run_id exists under "
            f"tenant_id={mismatch.get('tenant_id')} repo_id={mismatch.get('repo_id')}. "
            "Retry with matching scope hints."
        )
    raise FileNotFoundError(
        "runtime run not found; verify --outputs-root and runtime_run_id, "
        "or rerun start to create the scoped runtime record"
    )


def _bundle_ref_from_path(bundle_path: Path, *, source_compile_run_id: str) -> RuntimeBundleRef:
    payload = _read_json_object(bundle_path, what="runtime bundle")
    return RuntimeBundleRef(
        bundle_path=str(bundle_path),
        manifest_hash=stable_json_fingerprint(payload),
        created_at=int(bundle_path.stat().st_mtime * 1000),
        source_compile_run_id=source_compile_run_id,
    )


def _placeholder_bundle(context: RuntimeContext, bundle_ref: RuntimeBundleRef) -> RuntimeBundle:
    return RuntimeBundle(
        context=context,
        ref=bundle_ref,
        nodes=(),
        contract_ids=(),
        policy_envelope={},
        metadata={},
    )


def _load_bundle_context(
    *, bundle_path: Path, runtime_run_id: str, runtime_mode: str
) -> tuple[dict[str, Any], RuntimeContext]:
    payload = _read_json_object(bundle_path, what="runtime bundle")
    if not is_runtime_bundle_schema_id(str(payload.get("schema_id", ""))):
        raise ValueError("bundle schema mismatch; expected a runtime_bundle artifact")
    tenant_id = str(payload.get("tenant_id", "")).strip()
    repo_id = str(payload.get("repo_id", "")).strip()
    run_id = str(payload.get("run_id", "")).strip()
    if not tenant_id or not repo_id or not run_id:
        raise ValueError("runtime bundle must include tenant_id, repo_id, and run_id")
    context = RuntimeContext(
        tenant_id=tenant_id,
        repo_id=repo_id,
        run_id=run_id,
        runtime_run_id=runtime_run_id,
        policy_mode=runtime_mode,  # type: ignore[arg-type]
        adapter_id="native",
    )
    return payload, context


def _build_runtime_kernel(
    *,
    bundle_path: Path,
    outputs_root: Path,
    runtime_run_id: str,
    runtime_mode: str,
    strict_intent_authority: bool | None = None,
    coordination_execution_overrides: Mapping[str, Any] | None = None,
) -> tuple[RuntimeKernel, RuntimeContext]:
    payload, context = _load_bundle_context(
        bundle_path=bundle_path,
        runtime_run_id=runtime_run_id,
        runtime_mode=runtime_mode,
    )
    bundle_ref = RuntimeBundleRef(
        bundle_path=str(bundle_path),
        manifest_hash=stable_json_fingerprint(payload),
        created_at=_now_ms(),
        source_compile_run_id=context.run_id,
    )
    policy_runtime = RuntimePolicyRuntime.default(context=context, policy_mode="enforce")
    adapter = (
        LocalDepthRuntimeAdapter(outputs_root=outputs_root, delegate=NativeRuntimeAdapter())
        if full_layer_replacement_enabled(
            RuntimeBundle(
                context=context,
                ref=bundle_ref,
                nodes=(),
                contract_ids=(),
                policy_envelope=payload.get("runtime_policy_envelope", {})
                if isinstance(payload.get("runtime_policy_envelope"), Mapping)
                else {},
                metadata=payload,
            )
        )
        else NativeRuntimeAdapter()
    )
    runtime_context = RuntimeContext(
        tenant_id=context.tenant_id,
        repo_id=context.repo_id,
        run_id=context.run_id,
        runtime_run_id=context.runtime_run_id,
        policy_mode=context.policy_mode,
        adapter_id=adapter.adapter_id,
    )
    kernel = RuntimeKernel(
        context=runtime_context,
        bundle=_placeholder_bundle(runtime_context, bundle_ref),
        adapter=adapter,
        scheduler=InMemoryRuntimeScheduler(),
        state_store=FileSystemRuntimeStateStore(
            root=outputs_root,
            otel_export_extra_callbacks=otel_export_extra_callbacks_from_env(),
        ),
        event_bus=RuntimeEventBus(),
        evidence_writer=None,
        policy_runtime=policy_runtime,
    )
    kernel.load_bundle(
        bundle_ref,
        strict_intent_authority=strict_intent_authority,
        coordination_execution_overrides=coordination_execution_overrides,
    )
    return kernel, runtime_context


def _record_template(
    *,
    outputs_root: Path,
    context: RuntimeContext,
    bundle_path: Path,
    mode: str,
    status: str,
) -> dict[str, Any]:
    scoped_dir = _scope_dir(
        outputs_root=outputs_root,
        tenant_id=context.tenant_id,
        repo_id=context.repo_id,
        run_id=context.run_id,
        runtime_run_id=context.runtime_run_id,
    )
    return {
        "tenant_id": context.tenant_id,
        "repo_id": context.repo_id,
        "run_id": context.run_id,
        "runtime_run_id": context.runtime_run_id,
        "adapter_id": context.adapter_id,
        "outputs_root": str(outputs_root.expanduser().resolve()),
        "mode": mode,
        "status": status,
        "bundle_path": str(bundle_path),
        "scope_dir": str(scoped_dir),
        "checkpoint_path": str(scoped_dir / "checkpoint.json"),
        "events_path": str(scoped_dir / "events.json"),
        "queue_snapshot_path": str(scoped_dir / "queue_snapshot.json"),
        "runtime_evidence_path": str(scoped_dir / "runtime_evidence.json"),
        "coordination_audit_path": str(scoped_dir / "evidence" / "coordination_audit.jsonl"),
        "policy_decisions_path": str(scoped_dir / "policy_decisions.json"),
        "control_path": str(scoped_dir / "control.json"),
        "started_at_ms": _now_ms(),
        "finished_at_ms": None,
        "policy_decisions": [],
        "last_error": None,
    }


def _save_runtime_record(record: dict[str, Any]) -> None:
    record_path = Path(str(record["scope_dir"])) / "runtime_run.json"
    _write_json(record_path, record)


def _load_runtime_events(record: dict[str, Any]) -> list[dict[str, Any]]:
    path = _events_path(record)
    if not path.exists():
        return []
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError("runtime events file must be an array")
    return [dict(item) for item in raw if isinstance(item, dict)]


def _load_runtime_evidence(record: dict[str, Any]) -> tuple[RuntimeEvidenceRecord, ...]:
    path = _evidence_path(record)
    if not path.exists():
        return ()
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError("runtime evidence file must be an array")
    validate_artifact_json(obj=raw, kind="runtime_evidence_stream")
    return tuple(RuntimeEvidenceRecord.from_json_obj(item) for item in raw if isinstance(item, dict))


def _persist_policy_log(record: dict[str, Any], *, policy_runtime: RuntimePolicyRuntime | None) -> None:
    decisions = list(policy_runtime.decision_log) if policy_runtime is not None else []
    _write_json(_policy_log_path(record), decisions)
    record["policy_decisions"] = decisions


def _reconcile_resync_from_bundle_metadata(metadata: Mapping[str, JSONValue]) -> tuple[int, int]:
    iterations = 1
    raw = metadata.get("reconcile_resync_iterations")
    if isinstance(raw, int) and not isinstance(raw, bool) and raw >= 1:
        iterations = min(int(raw), 10_000)
    interval_ms = 0
    raw_i = metadata.get("reconcile_resync_interval_ms")
    if isinstance(raw_i, int) and not isinstance(raw_i, bool) and raw_i >= 0:
        interval_ms = int(raw_i)
    return iterations, interval_ms


def _deployment_provider_id_from_bundle(bundle: RuntimeBundle) -> str:
    raw = bundle.metadata.get("deployment_provider")
    if isinstance(raw, dict):
        kind = str(raw.get("kind", "")).strip()
        if kind:
            return kind
    return "in_memory"


def _mutating_provider_kind_from_bundle(bundle: RuntimeBundle) -> str | None:
    raw = bundle.metadata.get("deployment_provider")
    if not isinstance(raw, Mapping):
        return None
    kind = str(raw.get("kind", "")).strip()
    if kind in {"docker_compose_apply", "kubernetes_apply"}:
        return kind
    return None


def _coordination_execution_overrides_from_args(args: argparse.Namespace) -> dict[str, JSONValue]:
    out: dict[str, JSONValue] = {}
    mode_raw = str(getattr(args, "coordination_parallel_dispatch", "inherit")).strip().lower()
    if mode_raw == "enabled":
        out["parallel_dispatch_enabled"] = True
    elif mode_raw == "disabled":
        out["parallel_dispatch_enabled"] = False
    max_steps = getattr(args, "coordination_max_in_flight_steps", None)
    if isinstance(max_steps, int):
        if max_steps < 1:
            raise ValueError("--coordination-max-in-flight-steps must be >= 1")
        out["max_in_flight_steps"] = int(max_steps)
    max_per_role = getattr(args, "coordination_max_in_flight_per_role", None)
    if isinstance(max_per_role, int):
        if max_per_role < 1:
            raise ValueError("--coordination-max-in-flight-per-role must be >= 1")
        out["max_in_flight_per_role"] = int(max_per_role)
    return out


def _deployment_provider_contract_from_bundle(bundle: RuntimeBundle) -> Mapping[str, Any] | None:
    raw = bundle.metadata.get("deployment_provider_contract")
    return raw if isinstance(raw, Mapping) else None


def _validate_deployment_provider_preflight_contract(
    *,
    bundle: RuntimeBundle,
    mode: str,
    policy_runtime: RuntimePolicyRuntime | None,
) -> None:
    if str(mode).strip() not in {"enforce", "canary"}:
        return
    contract = _deployment_provider_contract_from_bundle(bundle)
    if contract is None:
        return
    required_env = contract.get("required_env_flags")
    if isinstance(required_env, Sequence) and not isinstance(required_env, (str, bytes)):
        missing_env: list[str] = []
        for item in required_env:
            name = str(item).strip()
            if not name:
                continue
            if os.environ.get(name, "").strip() != "1":
                missing_env.append(name)
        if missing_env:
            raise ValueError(
                "deployment provider preflight failed: missing required env flags set to 1: "
                + ", ".join(sorted(set(missing_env)))
            )
    required_actions = contract.get("required_policy_actions")
    if isinstance(required_actions, Sequence) and not isinstance(required_actions, (str, bytes)):
        allow = set(policy_runtime.effective_allow_actions) if policy_runtime is not None else set()
        missing_actions: list[str] = []
        for item in required_actions:
            action = str(item).strip()
            if action and action not in allow:
                missing_actions.append(action)
        if missing_actions:
            raise ValueError(
                "deployment provider preflight failed: runtime policy does not allow required actions: "
                + ", ".join(sorted(set(missing_actions)))
            )


def _load_promotion_packet_for_record(record: dict[str, Any]) -> dict[str, Any] | None:
    manifest = _load_compile_run_manifest(record)
    if manifest is None or not isinstance(manifest.control_plane, dict):
        return None
    ref = manifest.control_plane.get("promotion_packet_ref")
    if not isinstance(ref, dict):
        return None
    rel = str(ref.get("path", "")).strip()
    if not rel:
        return None
    packet_path = (_scope_root(record) / rel).resolve()
    payload = _read_json_object(packet_path, what="promotion packet")
    validate_artifact_json(obj=payload, kind="promotion_packet", version=1)
    expected = str(ref.get("sha256", "")).strip().lower()
    if expected and _json_sha256(payload) != expected:
        raise ValueError("promotion packet hash mismatch against compile manifest control_plane ref")
    return payload


def _require_promotion_packet_for_live_mutation(*, record: dict[str, Any], bundle: RuntimeBundle, mode: str) -> None:
    if str(mode).strip() not in {"enforce", "canary"}:
        return
    provider_kind = _mutating_provider_kind_from_bundle(bundle)
    if provider_kind is None:
        return
    packet = _load_promotion_packet_for_record(record)
    if packet is None:
        raise ValueError(
            "live mutation requires a signed promotion packet in compile manifest control_plane.promotion_packet_ref"
        )
    ok, err = verify_signed_packet(packet)
    if not ok:
        raise ValueError(err or "invalid promotion packet signature")
    promotion_mode = normalize_promotion_mode(str(packet.get("promotion_mode", "")))
    if promotion_mode != "live_apply":
        raise ValueError(
            f"live mutation denied: promotion_mode={packet.get('promotion_mode')} (requires live_apply packet)"
        )
    allow = packet.get("policy_allow_decision")
    if not isinstance(allow, Mapping) or not bool(allow.get("allowed", False)):
        raise ValueError("live mutation denied: promotion packet policy_allow_decision.allowed is not true")
    manifest = _load_compile_run_manifest(record)
    manifest_cp = manifest.control_plane if manifest is not None else None
    verify_compile_apply_attestation_for_rollout(
        packet=packet,
        manifest_control_plane=manifest_cp if isinstance(manifest_cp, dict) else None,
    )
    target = packet.get("apply_target_metadata")
    if isinstance(target, Mapping):
        pp = target.get("deployment_provider")
        if isinstance(pp, Mapping):
            packet_kind = str(pp.get("kind", "")).strip()
            if packet_kind and packet_kind != provider_kind:
                raise ValueError(
                    "live mutation denied: promotion packet deployment provider does not match runtime bundle provider"
                )
    if full_layer_replacement_enabled(bundle):
        raw_provider = bundle.metadata.get("deployment_provider")
        provider = raw_provider if isinstance(raw_provider, Mapping) else None
        rollback_map = provider.get("rollback_apply_manifest_by_desired_hash") if provider is not None else None
        if not isinstance(rollback_map, Mapping) or not rollback_map:
            raise ValueError(
                "full layer replacement requires deterministic rollback snapshots: "
                "deployment_provider.rollback_apply_manifest_by_desired_hash must be non-empty"
            )


def _runtime_nonzero_exit_on_reconcile_divergence(metadata: Mapping[str, JSONValue]) -> bool:
    return metadata.get("runtime_nonzero_exit_on_reconcile_divergence") is True


def _reconcile_divergence_exit_code(
    *,
    metadata: Mapping[str, JSONValue],
    statuses: tuple[ReconcileStatus, ...],
    terminal_status: str,
) -> int | None:
    """Return 3 when bundle opts in and reconcile did not fully succeed."""

    if not _runtime_nonzero_exit_on_reconcile_divergence(metadata):
        return None
    if not statuses:
        return None
    if not all(s.converged for s in statuses):
        return 3
    agg = _worst_health_status(
        *(s.health_status for s in statuses),
        _kernel_loop_health(terminal_status),
    )
    if agg == "failed":
        return 3
    return None


def _run_deployment_reconcile(
    *,
    bundle: RuntimeBundle,
    mode: ReconcileMode,
    policy_runtime: RuntimePolicyRuntime | None,
    iterations: int,
    interval_ms: int,
    exit_early_if_converged: bool,
    runtime_run_id: str,
) -> _ReconcileCLIResult:
    _validate_deployment_provider_preflight_contract(
        bundle=bundle,
        mode=mode,
        policy_runtime=policy_runtime,
    )
    reconciler = DeploymentReconciler(
        mode=mode,
        provider=create_deployment_provider(bundle),
        policy_runtime=policy_runtime,
    )
    if DeploymentReconciler.provider_is_observe_only(reconciler.provider) and mode in {"enforce", "canary"}:
        print(
            "runtime: observe-only deployment provider — external apply/rollback are not invoked; "
            "reconcile still observes real infrastructure state",
            file=sys.stderr,
        )
    backoff_cfg = parse_resync_backoff_config(bundle.metadata, reconcile_resync_interval_ms=int(interval_ms))
    last_statuses: tuple[ReconcileStatus, ...] = ()
    all_evidence: list[ReconcileEvidence] = []
    count = max(1, int(iterations))
    completed = 0
    elapsed_wait_ms = 0
    cumulative_scheduled_wait_ms = 0
    for i in range(count):
        next_sleep = compute_resync_sleep_ms(
            sleep_after_attempt_index=i,
            config=backoff_cfg,
            fixed_interval_ms=int(interval_ms),
            runtime_run_id=str(runtime_run_id).strip(),
        )
        interval_for_evidence = int(next_sleep) if i + 1 < count else 0
        statuses, ev = reconciler.reconcile_with_evidence(
            bundle=bundle,
            resync_attempt=i + 1,
            resync_max_attempts=count,
            resync_interval_ms=interval_for_evidence,
            resync_elapsed_wait_ms=int(elapsed_wait_ms),
        )
        last_statuses = statuses
        all_evidence.extend(ev)
        completed = i + 1
        if exit_early_if_converged and statuses and all(s.converged for s in statuses):
            break
        if i + 1 < count and next_sleep > 0:
            time.sleep(next_sleep / 1000.0)
            cumulative_scheduled_wait_ms += int(next_sleep)
            elapsed_wait_ms += int(next_sleep)
    return _ReconcileCLIResult(
        statuses=last_statuses,
        evidence=tuple(all_evidence),
        completed_attempts=completed,
        configured_max_attempts=count,
        interval_ms=int(interval_ms),
        total_resync_wait_ms=int(cumulative_scheduled_wait_ms),
    )


def _build_runtime_evidence(
    *,
    context: RuntimeContext,
    runtime_events: list[dict[str, Any]],
    policy_decisions: list[dict[str, Any]],
    reconcile_evidence: tuple[ReconcileEvidence, ...],
    reconcile_statuses: tuple[ReconcileStatus, ...],
    terminal_status: str,
    stable_intent_sha256: str | None = None,
    resync_completed_attempts: int = 1,
    resync_configured_max_attempts: int = 1,
    resync_interval_ms: int = 0,
    provider_id: str = "in_memory",
    policy_mode: str = "enforce",
    total_resync_wait_ms: int = 0,
    provider_contract: Mapping[str, Any] | None = None,
    bundle_metadata: Mapping[str, JSONValue] | None = None,
) -> tuple[RuntimeEvidenceRecord, ...]:
    evidence: list[RuntimeEvidenceRecord] = []
    correlation = _evidence_correlation(context=context, stable_intent_sha256=stable_intent_sha256)
    retry_counts: dict[str, int] = {}
    for decision in policy_decisions:
        if not isinstance(decision, dict):
            continue
        ctx = decision.get("context")
        action_id = None
        if isinstance(ctx, dict):
            raw_action_id = ctx.get("action_id")
            action_id = str(raw_action_id).strip() if raw_action_id is not None else None
        evidence.append(
            RuntimeEvidenceRecord(
                evidence_type="action_decision",
                timestamp=_now_ms(),
                runtime_run_id=context.runtime_run_id,
                payload={
                    "action": str(decision.get("action", "")),
                    "action_id": action_id or "",
                    "decision": "allowed" if bool(decision.get("allowed", False)) else "denied",
                    "reason": str(decision.get("reason", "")),
                    "token_id": str(decision.get("token_id", "")),
                },
            )
        )
    for event in runtime_events:
        payload = event.get("payload", {})
        if not isinstance(payload, dict):
            continue
        event_type = str(event.get("event_type", "")).strip()
        timestamp = int(event.get("timestamp", 0))
        action_payload = payload.get("action")
        action_id = (
            str(action_payload.get("action_id", "")).strip()
            if isinstance(action_payload, dict)
            else str(payload.get("action_id", "")).strip()
        )
        if event_type == "runtime.action.completed" and isinstance(payload.get("transition"), dict):
            evidence.append(
                RuntimeEvidenceRecord(
                    evidence_type="transition_application",
                    timestamp=timestamp,
                    runtime_run_id=context.runtime_run_id,
                    payload={
                        "action_id": action_id,
                        "transition": dict(payload["transition"]),
                    },
                )
            )
        if event_type == "runtime.action.retried":
            retry_counts[action_id] = retry_counts.get(action_id, 0) + 1
            evidence.append(
                RuntimeEvidenceRecord(
                    evidence_type="retry_budget",
                    timestamp=timestamp,
                    runtime_run_id=context.runtime_run_id,
                    payload={
                        "action_id": action_id,
                        "retry_count": retry_counts[action_id],
                        "budget_burn": {
                            "retries": retry_counts[action_id],
                            "reason": str(payload.get("reason", "")),
                        },
                    },
                )
            )
    contract_payload: dict[str, JSONValue] = {
        "provider_id": provider_id,
        "mutation_mode": "observe_only",
        "rollback_mode": "none",
        "rollback_determinism": "deterministic",
        **correlation,
    }
    if provider_contract is not None:
        for key in (
            "kind",
            "mutation_mode",
            "rollback_mode",
            "rollback_determinism",
            "required_env_flags",
            "required_policy_actions",
        ):
            if key in provider_contract:
                v = provider_contract.get(key)
                if isinstance(v, (str, bool, int, float)) or v is None:
                    contract_payload[key] = cast(JSONValue, v)
                elif isinstance(v, Sequence) and not isinstance(v, (str, bytes)):
                    contract_payload[key] = cast(JSONValue, [str(x) for x in v])
                elif isinstance(v, Mapping):
                    contract_payload[key] = cast(JSONValue, {str(k): cast(JSONValue, val) for k, val in v.items()})
    handoff = build_delivery_handoff_context(bundle_metadata) if isinstance(bundle_metadata, Mapping) else {}
    if handoff:
        contract_payload["delivery_handoff"] = cast(JSONValue, dict(handoff))
    evidence.append(
        RuntimeEvidenceRecord(
            evidence_type="provider_capability_snapshot",
            timestamp=_now_ms(),
            runtime_run_id=context.runtime_run_id,
            payload=contract_payload,
        )
    )
    for item in reconcile_evidence:
        resync_meta: dict[str, JSONValue] = {
            "resync_attempt": int(item.resync_attempt),
            "resync_max_attempts": int(item.resync_max_attempts),
            "resync_interval_ms": int(item.resync_interval_ms),
        }
        for op in item.operations:
            evidence.append(
                RuntimeEvidenceRecord(
                    evidence_type="reconcile_outcome",
                    timestamp=_now_ms(),
                    runtime_run_id=context.runtime_run_id,
                    payload={
                        "resource_id": item.resource_id,
                        "operation_type": op.operation.operation_type,
                        "applied": bool(op.applied),
                        "observed_hash": op.observed_hash,
                        "health_status": op.health_status,
                        "error": op.error,
                        **resync_meta,
                        **correlation,
                    },
                )
            )
        if item.rollback_triggered:
            evidence.append(
                RuntimeEvidenceRecord(
                    evidence_type="rollback_attempt",
                    timestamp=_now_ms(),
                    runtime_run_id=context.runtime_run_id,
                    payload={
                        "resource_id": item.resource_id,
                        "rollback_target_hash": str(item.rollback_target_hash or ""),
                        **resync_meta,
                        **correlation,
                    },
                )
            )
            chain: list[str] = []
            if item.rollback_target_hash is not None:
                chain.append(item.rollback_target_hash)
            evidence.append(
                RuntimeEvidenceRecord(
                    evidence_type="rollback_chain",
                    timestamp=_now_ms(),
                    runtime_run_id=context.runtime_run_id,
                    payload={
                        "resource_id": item.resource_id,
                        "chain": cast(JSONValue, chain),
                        **resync_meta,
                        **correlation,
                    },
                )
            )
            evidence.append(
                RuntimeEvidenceRecord(
                    evidence_type="rollback_result",
                    timestamp=_now_ms(),
                    runtime_run_id=context.runtime_run_id,
                    payload={
                        "resource_id": item.resource_id,
                        "rollback_outcome": str(item.rollback_outcome or "rollback_failed"),
                        "rollback_target_hash": str(item.rollback_target_hash or ""),
                        **resync_meta,
                        **correlation,
                    },
                )
            )
        evidence.append(
            RuntimeEvidenceRecord(
                evidence_type="terminal_health",
                timestamp=_now_ms(),
                runtime_run_id=context.runtime_run_id,
                payload={
                    "resource_id": item.resource_id,
                    "health_status": item.health_status,
                    "runtime_status": terminal_status,
                    **resync_meta,
                    **correlation,
                },
            )
        )
    for status in reconcile_statuses:
        rs_payload: dict[str, JSONValue] = {
            "resource_id": status.resource_id,
            "converged": status.converged,
            "conditions": cast(JSONValue, [c.to_json_obj() for c in status.conditions]),
            "observed_health_conditions": cast(JSONValue, [c.to_json_obj() for c in status.observed_health_conditions]),
            "reconcile_health_gate": status.reconcile_health_gate,
            "observed_hash": status.observed_hash,
            "health_status": status.health_status,
            "desired_hash": status.desired_hash,
            "hash_matched": bool(status.hash_matched),
            "health_gate_passed": bool(status.health_gate_passed),
            "resync_completed_attempts": int(resync_completed_attempts),
            "resync_configured_max_attempts": int(resync_configured_max_attempts),
            "resync_interval_ms": int(resync_interval_ms),
            **correlation,
        }
        if status.last_error is not None:
            rs_payload["last_error"] = status.last_error
        evidence.append(
            RuntimeEvidenceRecord(
                evidence_type="reconcile_resource_status",
                timestamp=_now_ms(),
                runtime_run_id=context.runtime_run_id,
                payload=rs_payload,
            )
        )
    if reconcile_statuses:
        pm = str(policy_mode).strip() or "enforce"
        prov = str(provider_id).strip() or "in_memory"
        win_ms = int(total_resync_wait_ms)
        attempts_used = int(resync_completed_attempts)
        for status in reconcile_statuses:
            hs = str(status.health_status).strip().lower()
            if hs not in {"healthy", "degraded", "unknown", "failed"}:
                hs = "unknown"
            evidence.append(
                RuntimeEvidenceRecord(
                    evidence_type="convergence_certificate",
                    timestamp=_now_ms(),
                    runtime_run_id=context.runtime_run_id,
                    payload={
                        "certificate_schema_version": 1,
                        "resource_id": status.resource_id,
                        "desired_hash": status.desired_hash,
                        "observed_hash": status.observed_hash,
                        "health": hs,
                        "attempts": attempts_used,
                        "window_ms": win_ms,
                        "provider_id": prov,
                        "policy_mode": pm,
                        "converged": bool(status.converged),
                        **correlation,
                    },
                )
            )
        ordered = sorted(reconcile_statuses, key=lambda s: s.resource_id)
        desired_map = {s.resource_id: s.desired_hash for s in ordered}
        observed_map = {s.resource_id: s.observed_hash for s in ordered}
        agg_h = _worst_health_status(*(s.health_status for s in reconcile_statuses))
        agg_norm = str(agg_h).strip().lower()
        if agg_norm not in {"healthy", "degraded", "unknown", "failed"}:
            agg_norm = "unknown"
        evidence.append(
            RuntimeEvidenceRecord(
                evidence_type="convergence_certificate",
                timestamp=_now_ms(),
                runtime_run_id=context.runtime_run_id,
                payload={
                    "certificate_schema_version": 1,
                    "resource_id": "__runtime_aggregate__",
                    "aggregate": True,
                    "desired_hash": stable_json_fingerprint(desired_map),
                    "observed_hash": stable_json_fingerprint(observed_map),
                    "health": agg_norm,
                    "attempts": attempts_used,
                    "window_ms": win_ms,
                    "provider_id": prov,
                    "policy_mode": pm,
                    "converged": bool(all(s.converged for s in reconcile_statuses)),
                    **correlation,
                },
            )
        )
    if not reconcile_evidence:
        evidence.append(
            RuntimeEvidenceRecord(
                evidence_type="terminal_health",
                timestamp=_now_ms(),
                runtime_run_id=context.runtime_run_id,
                payload={
                    "resource_id": context.run_id,
                    "health_status": "unknown",
                    "runtime_status": terminal_status,
                    **correlation,
                },
            )
        )
    reconcile_healths = [s.health_status for s in reconcile_statuses]
    aggregate_health = _worst_health_status(
        *reconcile_healths,
        _kernel_loop_health(terminal_status),
    )
    evidence.append(
        RuntimeEvidenceRecord(
            evidence_type="terminal_health",
            timestamp=_now_ms(),
            runtime_run_id=context.runtime_run_id,
            payload={
                "resource_id": "__runtime_aggregate__",
                "health_status": aggregate_health,
                "runtime_status": terminal_status,
                "aggregate": True,
                "kernel_terminal_status": terminal_status,
                **correlation,
            },
        )
    )
    return tuple(evidence)


def _persist_runtime_evidence(record: dict[str, Any], *, evidence: tuple[RuntimeEvidenceRecord, ...]) -> None:
    # `evidence` is always the full in-memory snapshot for this persist (build + optional validator augment).
    # Merging with the previous file would duplicate rows that do not carry `binding_id` (e.g. terminal_health).
    payload = [item.to_json_obj() for item in evidence]
    validate_artifact_json(obj=payload, kind="runtime_evidence_stream")
    _write_json(_evidence_path(record), payload)


def _augment_evidence_with_validators(
    record: dict[str, Any],
    *,
    evidence: tuple[RuntimeEvidenceRecord, ...],
    specs: Sequence[tuple[str, OperationalValidityParams]],
    cwd: Path,
    project: AkcProjectConfig | None,
) -> tuple[RuntimeEvidenceRecord, ...]:
    if not specs:
        return evidence
    bindings_path = resolve_validator_bindings_path(cwd=cwd, project=project, cli_value=None)
    result = execute_validator_bindings(
        scope_root=_scope_root(record),
        run_id=str(record["run_id"]).strip(),
        runtime_run_id=str(record["runtime_run_id"]).strip(),
        specs=specs,
        bindings_path=bindings_path,
        adapter_id=str(record.get("adapter_id", "native")).strip() or "native",
    )
    return merge_validator_evidence(existing=evidence, updates=result.evidence)


def _pointer_for_json_file(path: Path, *, record: dict[str, Any] | None = None) -> ArtifactPointer:
    pointer_path = _rel_artifact_path(path, record=record) if record is not None else str(path)
    if not path.exists():
        return ArtifactPointer(path=pointer_path)
    text = path.read_text(encoding="utf-8")
    try:
        raw = json.loads(text)
        if isinstance(raw, (dict, list)):
            return ArtifactPointer(path=pointer_path, sha256=_json_sha256(raw))
    except json.JSONDecodeError:
        pass
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return ArtifactPointer(path=pointer_path, sha256=digest)


def _operational_validity_report_file(record: dict[str, Any]) -> Path:
    return Path(str(record["scope_dir"])) / "operational_validity_report.json"


def _otel_ndjson_path_for_run(record: dict[str, Any]) -> Path:
    return _scope_root(record) / ".akc" / "run" / f"{str(record['run_id']).strip()}.otel.jsonl"


def _otel_metric_ndjson_path_for_run(record: dict[str, Any]) -> Path:
    return _scope_root(record) / ".akc" / "run" / f"{str(record['run_id']).strip()}.otel_metrics.jsonl"


def _post_runtime_operational_criteria_from_bundle(
    bundle_path: Path,
) -> list[tuple[str, OperationalValidityParams]]:
    """Return (success_criterion_id, OperationalValidityParams) for post-runtime operational evaluation."""

    try:
        payload = _read_json_object(bundle_path, what="runtime bundle")
    except (OSError, ValueError):
        return []
    ipp = payload.get("intent_policy_projection")
    if not isinstance(ipp, dict):
        return []
    summary = ipp.get("success_criteria_summary")
    if not isinstance(summary, dict):
        return []
    criteria = summary.get("criteria")
    if not isinstance(criteria, list):
        return []
    out: list[tuple[str, OperationalValidityParams]] = []
    for c in criteria:
        if not isinstance(c, dict):
            continue
        if str(c.get("evaluation_mode", "")).strip() != "operational_spec":
            continue
        sc_id = str(c.get("id", "")).strip() or "operational_spec"
        params_raw = c.get("params")
        if not isinstance(params_raw, dict):
            continue
        try:
            parsed = parse_operational_validity_params(params_raw)
        except OperationalValidityParamsError:
            continue
        if parsed is None:
            continue
        if parsed.evaluation_phase != "post_runtime":
            continue
        out.append((sc_id, parsed))
    return sorted(out, key=lambda x: x[0])


def _predicate_rows_from_verdict(per_criterion: tuple[dict[str, JSONValue], ...]) -> list[dict[str, JSONValue]]:
    rows: list[dict[str, JSONValue]] = []
    for row in per_criterion:
        ck = str(row.get("check_name", "")).strip()
        pk = "threshold" if "threshold_signal" in ck else "presence"
        detail = row.get("evidence")
        r: dict[str, JSONValue] = {
            "predicate_kind": pk,
            "signal_key": ck or "check",
            "passed": bool(row.get("passed")),
        }
        sc_id = str(row.get("success_criterion_id", "")).strip()
        if sc_id:
            r["success_criterion_id"] = sc_id
        msg = str(row.get("message", "")).strip()
        if msg:
            r["message"] = msg
        if isinstance(detail, dict):
            r["details"] = dict(detail)
        rows.append(r)
    return rows


def _maybe_write_operational_validity_report(
    record: dict[str, Any], *, evidence: tuple[RuntimeEvidenceRecord, ...]
) -> OperationalValidityReportSummary:
    """When the bundle requests post-runtime operational evaluation, emit a validity report + record fields."""

    bundle_path = Path(str(record["bundle_path"])).expanduser().resolve()
    repo_root = _repo_outputs_root_for_operational_eval(record, bundle_path=bundle_path)
    ensure_path_under_repo_outputs(bundle_path, repo_outputs_root=repo_root)
    ensure_path_under_repo_outputs(_evidence_path(record), repo_outputs_root=repo_root)
    ensure_path_under_repo_outputs(_otel_ndjson_path_for_run(record), repo_outputs_root=repo_root)
    ensure_path_under_repo_outputs(_otel_metric_ndjson_path_for_run(record), repo_outputs_root=repo_root)
    ensure_path_under_repo_outputs(Path(str(record["scope_dir"])), repo_outputs_root=repo_root)
    ensure_path_under_repo_outputs(_operational_validity_report_file(record), repo_outputs_root=repo_root)

    specs = _post_runtime_operational_criteria_from_bundle(bundle_path)
    if not specs:
        return OperationalValidityReportSummary(
            ran_evaluation=False,
            passed_all=True,
            failed_success_criterion_ids=(),
        )
    try:
        bundle_payload = _read_json_object(bundle_path, what="runtime bundle")
    except (OSError, ValueError):
        return OperationalValidityReportSummary(
            ran_evaluation=False,
            passed_all=True,
            failed_success_criterion_ids=(),
        )
    bundle_schema_version: int | None = None
    raw_sv = bundle_payload.get("schema_version")
    if isinstance(raw_sv, int) and not isinstance(raw_sv, bool):
        bundle_schema_version = int(raw_sv)

    otel_text = ""
    otp = _otel_ndjson_path_for_run(record)
    if otp.is_file():
        try:
            otel_text = otp.read_text(encoding="utf-8")
        except OSError:
            otel_text = ""
    otel_records = parse_otel_ndjson_slice(otel_text)

    otel_metric_records: tuple[dict[str, JSONValue], ...] | None = None
    otel_metric_parse_rejected_reason: str | None = None
    omp = _otel_metric_ndjson_path_for_run(record)
    if omp.is_file():
        try:
            mtext = omp.read_text(encoding="utf-8")
        except OSError:
            mtext = ""
        m_out = parse_otel_metric_ndjson_slice(mtext)
        otel_metric_records = m_out.records
        otel_metric_parse_rejected_reason = m_out.rejected_reason

    intent_ref_raw = bundle_payload.get("intent_ref")
    intent_ref: dict[str, JSONValue] | None = None
    if isinstance(intent_ref_raw, dict) and str(intent_ref_raw.get("intent_id", "")).strip():
        intent_ref = dict(cast(Mapping[str, JSONValue], intent_ref_raw))

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
        rolling_meta: OperationalEvidenceRollupMeta | None = None
        evidence_use: tuple[RuntimeEvidenceRecord, ...] = evidence
        if params.window == "rolling_ms":
            rel = (params.evidence_rollup_rel_path or "").strip()
            if not rel:
                verdicts.append(
                    (
                        sc_id,
                        params,
                        operational_verdict_for_rollup_load_failure(
                            success_criterion_id=sc_id,
                            message="rolling_ms requires evidence_rollup_rel_path",
                        ),
                        evidence,
                        None,
                    )
                )
                continue
            rollup_abs = (repo_root / rel).expanduser()
            try:
                evidence_use, rolling_meta = load_merged_runtime_evidence_from_rollup_path(
                    rollup_path=rollup_abs,
                    repo_outputs_root=repo_root,
                )
            except (OperationalRollupLoadError, OperationalPathScopeError) as exc:
                verdicts.append(
                    (
                        sc_id,
                        params,
                        operational_verdict_for_rollup_load_failure(
                            success_criterion_id=sc_id,
                            message=str(exc),
                        ),
                        evidence,
                        None,
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

    passed_all = all(v[2].passed for v in verdicts)
    max_spec_ver = max(int(p.spec_version) for _, p, _, _, _ in verdicts)

    fp_parts: list[str] = []
    pred_rows: list[dict[str, JSONValue]] = []
    for _sc_id, params, verdict, ev_fp, roll_meta in verdicts:
        fp_parts.append(
            combined_operational_fingerprint(
                params=params,
                evidence=ev_fp,
                verdict=verdict,
                otel_records=otel_records,
                otel_contract=None,
                runtime_bundle_schema_version=bundle_schema_version,
                rolling_rollup_meta=roll_meta,
                otel_metric_records=otel_metric_records,
                otel_metric_parse_rejected_reason=otel_metric_parse_rejected_reason,
            )
        )
        pred_rows.extend(_predicate_rows_from_verdict(verdict.per_criterion))

    attestation_fp = stable_json_fingerprint(
        {"criteria": [{"id": sc_id, "sha256": fp_parts[i]} for i, (sc_id, _, _, _, _) in enumerate(verdicts)]}
    )

    report_path = _operational_validity_report_file(record)
    ev_ref = _pointer_for_json_file(_evidence_path(record), record=record).to_json_obj()
    bundle_ref_obj: dict[str, JSONValue] = {
        **_pointer_for_json_file(bundle_path, record=record).to_json_obj(),
        "schema_version": int(bundle_schema_version or 1),
    }

    ev_types: set[str] = set()
    for _, p, _, _, _ in verdicts:
        ev_types.update(str(x).strip() for x in p.expected_evidence_types if str(x).strip())

    report_obj: dict[str, Any] = {
        "tenant_id": str(record["tenant_id"]),
        "repo_id": str(record["repo_id"]),
        "run_id": str(record["run_id"]),
        "runtime_run_id": str(record["runtime_run_id"]),
        "evaluated_at_ms": _now_ms(),
        "passed": passed_all,
        "operational_spec_version": int(max_spec_ver),
        "success_criterion_id": (str(verdicts[0][0]) if len(verdicts) == 1 else None),
        "intent_ref": intent_ref,
        "runtime_bundle_ref": bundle_ref_obj,
        "runtime_evidence_ref": ev_ref,
        "predicate_results": pred_rows,
        "expected_evidence_types": sorted(ev_types),
        "attestation_fingerprint_sha256": attestation_fp,
    }
    report_obj = {k: v for k, v in report_obj.items() if v is not None}
    report_obj = apply_schema_envelope(obj=report_obj, kind="operational_validity_report")
    validate_artifact_json(obj=report_obj, kind="operational_validity_report")
    _write_json(report_path, report_obj)

    record["operational_validity_passed"] = passed_all
    record["operational_validity_fingerprint_sha256"] = attestation_fp

    failed_ids = tuple(sorted(sc_id for sc_id, _, v, _, _ in verdicts if not v.passed))
    return OperationalValidityReportSummary(
        ran_evaluation=True,
        passed_all=passed_all,
        failed_success_criterion_ids=failed_ids,
    )


def _runtime_control_plane_links(record: dict[str, Any]) -> dict[str, JSONValue]:
    links: dict[str, JSONValue] = {
        "runtime_run_id": str(record["runtime_run_id"]),
        "runtime_evidence_ref": _pointer_for_json_file(_evidence_path(record), record=record).to_json_obj(),
        "policy_decisions_ref": _pointer_for_json_file(_policy_log_path(record), record=record).to_json_obj(),
    }
    cap = _coordination_audit_path(record)
    if cap.exists() and cap.stat().st_size > 0:
        links["coordination_audit_ref"] = _pointer_for_json_file(cap, record=record).to_json_obj()
    ovr = _operational_validity_report_file(record)
    if ovr.exists():
        links["operational_validity_report_ref"] = _pointer_for_json_file(ovr, record=record).to_json_obj()
    if record.get("operational_validity_passed") is not None:
        links["operational_validity_passed"] = bool(record["operational_validity_passed"])
    ov_fp = record.get("operational_validity_fingerprint_sha256")
    if isinstance(ov_fp, str) and ov_fp.strip():
        links["operational_validity_fingerprint_sha256"] = ov_fp.strip()
    cep = record.get("coordination_execution_policy")
    if isinstance(cep, Mapping):
        links["coordination_execution_policy"] = cast(JSONValue, dict(cep))
    role_profile = record.get("developer_role_profile")
    if isinstance(role_profile, str) and role_profile.strip():
        links["developer_role_profile"] = role_profile.strip()
    decisions_ref = record.get("developer_profile_decisions_ref")
    if isinstance(decisions_ref, Mapping):
        links["developer_profile_decisions_ref"] = cast(JSONValue, dict(decisions_ref))
    return links


def _runtime_manifest_with_links(
    *,
    manifest: RunManifest | None,
    record: dict[str, Any],
    replay_mode: str,
    runtime_evidence: tuple[RuntimeEvidenceRecord, ...],
) -> RunManifest:
    syn_stable_intent = (
        _stable_intent_sha256_from_bundle_file(Path(str(record["bundle_path"])).expanduser().resolve())
        if manifest is None
        else None
    )
    control_plane = dict(manifest.control_plane) if manifest is not None and manifest.control_plane else {}
    control_plane.update(_runtime_control_plane_links(record))
    lifecycle_raw = control_plane.get("lifecycle_timestamps")
    lifecycle = dict(lifecycle_raw) if isinstance(lifecycle_raw, Mapping) else {}
    if "intent_received_at" not in lifecycle and "compile_started_at" in lifecycle:
        lifecycle["intent_received_at"] = lifecycle.get("compile_started_at")
    th = str(record.get("terminal_health_status", "")).strip().lower()
    if "runtime_healthy_at" not in lifecycle and th == "healthy":
        lifecycle["runtime_healthy_at"] = int(record.get("finished_at_ms", 0) or 0)
    dproj = record.get("delivery_lifecycle_projection")
    manual_touch: int | None = None
    if isinstance(dproj, Mapping):
        ts_merge = dproj.get("timestamps")
        if isinstance(ts_merge, Mapping):
            for key, val in ts_merge.items():
                if isinstance(key, str) and isinstance(val, int) and not isinstance(val, bool) and val >= 0:
                    lifecycle[key] = val
        mc_raw = dproj.get("manual_touch_count")
        if isinstance(mc_raw, int) and not isinstance(mc_raw, bool) and mc_raw >= 0:
            manual_touch = int(mc_raw)
    if lifecycle:
        control_plane["lifecycle_timestamps"] = lifecycle
        baseline_hours_raw = control_plane.get("baseline_duration_hours")
        baseline_hours = None
        if isinstance(baseline_hours_raw, (int, float)) and not isinstance(baseline_hours_raw, bool):
            baseline_hours = float(baseline_hours_raw)
        control_plane["time_compression_metrics"] = cast(
            JSONValue,
            derive_time_compression_metrics(
                lifecycle_timestamps=lifecycle,
                baseline_duration_hours=baseline_hours,
                manual_touch_count=manual_touch,
            ),
        )
    if syn_stable_intent is not None:
        control_plane["stable_intent_sha256"] = syn_stable_intent
    apply_env_policy_provenance(control_plane)
    control_plane = apply_schema_envelope(obj=control_plane, kind="control_plane_envelope")
    validate_artifact_json(obj=control_plane, kind="control_plane_envelope")
    base = manifest
    return RunManifest(
        run_id=str(record["run_id"]),
        tenant_id=str(record["tenant_id"]),
        repo_id=str(record["repo_id"]),
        ir_sha256=base.ir_sha256 if base is not None else "0" * 64,
        replay_mode=replay_mode,  # type: ignore[arg-type]
        intent_semantic_fingerprint=base.intent_semantic_fingerprint if base is not None else None,
        intent_goal_text_fingerprint=base.intent_goal_text_fingerprint if base is not None else None,
        stable_intent_sha256=base.stable_intent_sha256 if base is not None else syn_stable_intent,
        knowledge_semantic_fingerprint=(base.knowledge_semantic_fingerprint if base is not None else None),
        knowledge_provenance_fingerprint=(base.knowledge_provenance_fingerprint if base is not None else None),
        retrieval_snapshots=base.retrieval_snapshots if base is not None else (),
        passes=base.passes if base is not None else (),
        model=base.model if base is not None else None,
        model_params=dict(base.model_params) if base is not None and base.model_params else None,
        tool_params=dict(base.tool_params) if base is not None and base.tool_params else None,
        partial_replay_passes=base.partial_replay_passes if base is not None else (),
        llm_vcr=dict(base.llm_vcr) if base is not None and base.llm_vcr else None,
        budgets=dict(base.budgets) if base is not None and base.budgets else None,
        output_hashes=dict(base.output_hashes) if base is not None and base.output_hashes else None,
        runtime_bundle=_pointer_for_json_file(Path(str(record["bundle_path"])), record=record),
        runtime_event_transcript=_pointer_for_json_file(_events_path(record), record=record),
        runtime_evidence=runtime_evidence,
        trace_spans=base.trace_spans if base is not None else (),
        control_plane=control_plane,
        cost_attribution=dict(base.cost_attribution) if base is not None and base.cost_attribution else None,
        manifest_version=base.manifest_version if base is not None else 1,
    )


def _load_compile_run_manifest(record: dict[str, Any]) -> RunManifest | None:
    manifest_path = _compile_run_manifest_path(record)
    if not manifest_path.exists():
        return None
    return RunManifest.from_json_file(manifest_path)


def _persist_compile_run_manifest_with_runtime_links(
    record: dict[str, Any], *, runtime_evidence: tuple[RuntimeEvidenceRecord, ...]
) -> None:
    existing = _load_compile_run_manifest(record)
    if existing is None:
        return
    updated = _runtime_manifest_with_links(
        manifest=existing,
        record=record,
        replay_mode=existing.replay_mode,
        runtime_evidence=runtime_evidence,
    )
    mp = _compile_run_manifest_path(record)
    mp.write_text(json.dumps(updated.to_json_obj(), indent=2, sort_keys=True), encoding="utf-8")
    scope_root = _scope_root(record)
    outputs_root = scope_root.parent.parent
    try_upsert_operations_index_from_manifest(mp, outputs_root=outputs_root)


def _run_manifest_for_replay(
    *,
    record: dict[str, Any],
    replay_mode: str,
    runtime_evidence: tuple[RuntimeEvidenceRecord, ...],
    runtime_events: list[dict[str, Any]],
) -> RunManifest:
    del runtime_events
    return _runtime_manifest_with_links(
        manifest=_load_compile_run_manifest(record),
        record=record,
        replay_mode=replay_mode,
        runtime_evidence=runtime_evidence,
    )


def _format_scope_hint(record: dict[str, Any]) -> str:
    return (
        f"tenant_id={record.get('tenant_id')} repo_id={record.get('repo_id')} "
        f"runtime_run_id={record.get('runtime_run_id')}"
    )


def cmd_runtime_coordination_plan(args: argparse.Namespace) -> int:
    """Print deterministic coordination schedule layers from a runtime bundle (no kernel execution)."""
    configure_logging(verbose=bool(getattr(args, "verbose", False)))
    bundle_path = Path(str(args.bundle)).expanduser().resolve()
    if not bundle_path.exists():
        return _emit_runtime_error("bundle path does not exist", hint="provide a valid --bundle path")
    try:
        payload = _read_json_object(bundle_path, what="runtime bundle")
        loaded = load_coordination_for_bundle(bundle_path=bundle_path, payload=payload)
        if loaded is None:
            print(json.dumps({"coordination": None}, indent=2, sort_keys=True))
            return 0
        sched = CoordinationScheduler(loaded.parsed).schedule()
        out = {
            "coordination_spec_sha256": loaded.fingerprint_sha256,
            **coordination_schedule_to_jsonable(sched),
        }
        print(json.dumps(out, indent=2, sort_keys=True))
        return 0
    except Exception as exc:
        return _emit_runtime_error(str(exc), hint="verify coordination_ref / coordination_spec and spec_hashes")


def cmd_runtime_start(args: argparse.Namespace) -> int:
    configure_logging(verbose=bool(getattr(args, "verbose", False)))
    fmt = str(getattr(args, "format", "text"))
    cwd = Path.cwd()
    proj = load_akc_project_config(cwd)
    outputs_r = resolve_optional_project_string(
        cli_value=getattr(args, "outputs_root", None),
        env_key="AKC_OUTPUTS_ROOT",
        file_value=proj.outputs_root if proj is not None else None,
        env=os.environ,
    )
    if outputs_r.value is not None:
        args.outputs_root = outputs_r.value
    developer_role_profile = resolve_developer_role_profile(
        cli_value=getattr(args, "developer_role_profile", None),
        cwd=cwd,
        env=os.environ,
        project=proj,
    ).value
    start_profile = resolve_runtime_start_profile_defaults(
        profile=developer_role_profile,
        mode=cast(str | None, getattr(args, "mode", None)),
        bundle=cast(str | None, getattr(args, "bundle", None)),
    )
    mode_value = cast(str | None, start_profile["mode"].value)
    bundle_value = cast(str | None, start_profile["bundle"].value)
    if mode_value is None:
        return _emit_runtime_error(
            "missing required --mode", hint="set --mode simulate|enforce|canary", format_mode=fmt
        )
    args.mode = mode_value
    inferred_outputs_root = (
        Path(str(args.outputs_root)).expanduser().resolve() if getattr(args, "outputs_root", None) else None
    )
    if bundle_value is None:
        if developer_role_profile != "emerging":
            return _emit_runtime_error(
                "missing required --bundle", hint="provide --bundle path to runtime bundle", format_mode=fmt
            )
        if inferred_outputs_root is None:
            return _emit_runtime_error(
                "emerging profile bundle auto-resolution requires an outputs root",
                hint=(
                    "pass --outputs-root, set AKC_OUTPUTS_ROOT, add outputs_root to .akc/project.json, "
                    "or provide --bundle"
                ),
                format_mode=fmt,
            )
        latest = _find_latest_runtime_bundle(inferred_outputs_root)
        if latest is None:
            return _emit_runtime_error(
                "no runtime bundle found under outputs root",
                hint="run compile first or provide --bundle",
                format_mode=fmt,
            )
        bundle_value = str(latest)
    bundle_path = Path(str(bundle_value)).expanduser().resolve()
    if not bundle_path.exists():
        return _emit_runtime_error("bundle path does not exist", hint="provide a valid --bundle path", format_mode=fmt)
    inferred_root = _infer_outputs_root_from_bundle(bundle_path)
    outputs_root = (
        Path(str(args.outputs_root)).expanduser().resolve()
        if getattr(args, "outputs_root", None)
        else (inferred_root.resolve() if inferred_root is not None else bundle_path.parent)
    )
    if outputs_r.value is not None:
        outputs_root_source: str = outputs_r.source
    elif inferred_root is not None:
        outputs_root_source = "bundle_inference"
    else:
        outputs_root_source = "bundle_parent"
    runtime_run_id = str(uuid.uuid4())
    kernel: RuntimeKernel | None = None
    record: dict[str, Any] | None = None
    try:
        if (
            developer_role_profile == "emerging"
            and str(getattr(args, "coordination_parallel_dispatch", "inherit")).strip() == "inherit"
        ):
            args.coordination_parallel_dispatch = str(start_profile["coordination_parallel_dispatch"].value)
            if getattr(args, "coordination_max_in_flight_steps", None) is None:
                args.coordination_max_in_flight_steps = int(start_profile["coordination_max_in_flight_steps"].value)
            if getattr(args, "coordination_max_in_flight_per_role", None) is None:
                args.coordination_max_in_flight_per_role = int(
                    start_profile["coordination_max_in_flight_per_role"].value
                )
        coord_overrides = _coordination_execution_overrides_from_args(args)
        kernel, context = _build_runtime_kernel(
            bundle_path=bundle_path,
            outputs_root=outputs_root,
            runtime_run_id=runtime_run_id,
            runtime_mode=str(args.mode),
            strict_intent_authority=True if bool(getattr(args, "strict_intent_authority", False)) else None,
            coordination_execution_overrides=coord_overrides,
        )
        record = _record_template(
            outputs_root=outputs_root,
            context=context,
            bundle_path=bundle_path,
            mode=str(args.mode),
            status="running",
        )
        runtime_profile_decisions = {
            "developer_role_profile": developer_role_profile,
            "resolved": {
                "mode": {"value": str(args.mode), "source": start_profile["mode"].source},
                "bundle": {"value": str(bundle_path), "source": start_profile["bundle"].source},
                "outputs_root": {"value": str(outputs_root.resolve()), "source": outputs_root_source},
                "coordination_parallel_dispatch": {
                    "value": str(getattr(args, "coordination_parallel_dispatch", "inherit")),
                    "source": start_profile["coordination_parallel_dispatch"].source,
                },
                "coordination_max_in_flight_steps": {
                    "value": getattr(args, "coordination_max_in_flight_steps", None),
                    "source": start_profile["coordination_max_in_flight_steps"].source,
                },
                "coordination_max_in_flight_per_role": {
                    "value": getattr(args, "coordination_max_in_flight_per_role", None),
                    "source": start_profile["coordination_max_in_flight_per_role"].source,
                },
            },
        }
        runtime_profile_decisions["fingerprint_sha256"] = stable_json_fingerprint(runtime_profile_decisions)
        profile_decisions_path = Path(str(record["scope_dir"])) / "developer_profile_decisions.json"
        _write_json(profile_decisions_path, runtime_profile_decisions)
        record["developer_role_profile"] = developer_role_profile
        record["developer_profile_decisions_ref"] = _pointer_for_json_file(
            profile_decisions_path,
            record=record,
        ).to_json_obj()
        record["coordination_execution_policy"] = kernel.coordination_execution_policy_snapshot()
        _save_runtime_record(record)
        loop_result = kernel.run_until_terminal()
        _require_promotion_packet_for_live_mutation(
            record=record,
            bundle=kernel.bundle,
            mode=str(args.mode),
        )
        r_iters, r_interval = _reconcile_resync_from_bundle_metadata(kernel.bundle.metadata)
        r_loop = _run_deployment_reconcile(
            bundle=kernel.bundle,
            mode=cast(ReconcileMode, str(args.mode)),
            policy_runtime=kernel.policy_runtime,
            iterations=r_iters,
            interval_ms=r_interval,
            exit_early_if_converged=False,
            runtime_run_id=context.runtime_run_id,
        )
        runtime_events = _load_runtime_events(record)
        _persist_policy_log(record, policy_runtime=kernel.policy_runtime)
        intent_sha = _stable_intent_sha256_from_bundle_metadata(kernel.bundle.metadata)
        lane = resolve_delivery_target_lane(
            cli_value=getattr(args, "delivery_target_lane", None),
            env_value=os.environ.get("AKC_DELIVERY_TARGET_LANE"),
        )
        evidence = _build_runtime_evidence(
            context=context,
            runtime_events=runtime_events,
            policy_decisions=list(record.get("policy_decisions", [])),
            reconcile_evidence=r_loop.evidence,
            reconcile_statuses=r_loop.statuses,
            terminal_status=loop_result.status,
            stable_intent_sha256=intent_sha,
            resync_completed_attempts=r_loop.completed_attempts,
            resync_configured_max_attempts=r_loop.configured_max_attempts,
            resync_interval_ms=r_loop.interval_ms,
            provider_id=_deployment_provider_id_from_bundle(kernel.bundle),
            policy_mode=str(context.policy_mode),
            total_resync_wait_ms=r_loop.total_resync_wait_ms,
            provider_contract=_deployment_provider_contract_from_bundle(kernel.bundle),
            bundle_metadata=kernel.bundle.metadata,
        )
        evidence = _augment_evidence_with_validators(
            record,
            evidence=evidence,
            specs=_post_runtime_operational_criteria_from_bundle(bundle_path),
            cwd=cwd,
            project=proj,
        )
        expectations_raw = kernel.bundle.metadata.get("runtime_evidence_expectations") or ()
        expectations_list: list[str] = []
        if isinstance(expectations_raw, Sequence) and not isinstance(expectations_raw, (str, bytes)):
            expectations_list = [str(x).strip() for x in expectations_raw if str(x).strip()]
        violations = runtime_evidence_expectation_violations(
            expectations=expectations_list,
            evidence_types_present={r.evidence_type for r in evidence},
        )
        _persist_runtime_evidence(record, evidence=evidence)
        ov_summary = _maybe_write_operational_validity_report(record, evidence=evidence)
        if ov_summary.ran_evaluation and not ov_summary.passed_all:
            _append_operational_validity_attested_failure_event(
                record,
                failed_success_criterion_ids=ov_summary.failed_success_criterion_ids,
            )
        record["status"] = loop_result.status
        record["finished_at_ms"] = _now_ms()
        record["event_count"] = len(runtime_events)
        record["last_checkpoint_id"] = loop_result.last_checkpoint.checkpoint_id
        record["terminal_health_status"] = _aggregate_terminal_health_from_evidence(evidence)
        record["reconcile_all_converged"] = bool(r_loop.statuses) and all(s.converged for s in r_loop.statuses)
        record["reconcile_resync_wait_ms_scheduled"] = int(r_loop.total_resync_wait_ms)
        healthy_at: int | None = None
        if str(record.get("terminal_health_status", "")).strip().lower() == "healthy":
            raw_fin = record.get("finished_at_ms")
            if isinstance(raw_fin, int) and not isinstance(raw_fin, bool) and raw_fin >= 0:
                healthy_at = int(raw_fin)
        record["delivery_lifecycle_projection"] = project_delivery_run_projection(
            evidence=evidence,
            delivery_lane=lane,
            record_started_at_ms=int(record["started_at_ms"]),
            terminal_health_status=str(record.get("terminal_health_status", "")),
            runtime_healthy_at=healthy_at,
        )
        if violations:
            record["status"] = "failed"
            joined = ", ".join(violations)
            record["evidence_expectation_violations"] = list(violations)
            record["last_error"] = f"runtime evidence expectations not satisfied: {joined}"
        _save_runtime_record(record)
        _persist_compile_run_manifest_with_runtime_links(record, runtime_evidence=_load_runtime_evidence(record))
        print(f"runtime_run_id: {context.runtime_run_id}")
        print(f"tenant_id: {context.tenant_id}")
        print(f"repo_id: {context.repo_id}")
        print(f"run_id: {context.run_id}")
        print(f"status: {record['status']}")
        print(f"bundle_path: {bundle_path}")
        print(f"events_path: {_events_path(record)}")
        if violations:
            return _emit_runtime_error(
                str(record.get("last_error", "runtime evidence expectations not satisfied")),
                hint="compare bundle runtime_evidence_expectations to runtime_evidence.json evidence_type values",
                format_mode=fmt,
            )
        div_exit = _reconcile_divergence_exit_code(
            metadata=kernel.bundle.metadata,
            statuses=r_loop.statuses,
            terminal_status=str(loop_result.status),
        )
        if div_exit is not None:
            record["status"] = "failed"
            record["last_error"] = (
                "reconcile divergence or failed aggregate health (runtime_nonzero_exit_on_reconcile_divergence)"
            )
            _save_runtime_record(record)
            _persist_compile_run_manifest_with_runtime_links(record, runtime_evidence=_load_runtime_evidence(record))
            print(
                "Runtime CLI error: reconcile did not satisfy convergence contract for this bundle "
                "(see convergence_certificate and reconcile_resource_status in runtime_evidence.json)",
                file=sys.stderr,
            )
            return int(div_exit)
        return 0
    except RuntimeScopeMismatchError as exc:
        return _emit_runtime_error(
            str(exc),
            hint="verify tenant/repo scope and rerun with matching outputs root",
            policy_decision_id=_last_policy_decision_id(kernel.policy_runtime) if kernel is not None else None,
            format_mode=fmt,
        )
    except PermissionError as exc:
        scope_root = Path(str(record["scope_dir"])) if record is not None else cwd
        denied = _last_denied_policy_decision_from_record(record) if record is not None else None
        denial: dict[str, Any] | None
        if denied is not None and record is not None:
            denial = runtime_policy_denial_from_denied_decision(
                denied=denied,
                scope_root=scope_root,
                opa_policy_path=None,
                opa_decision_path="data.akc.allow",
                record=record,
            )
        else:
            denial = runtime_policy_denial_from_permission_error(
                record=record,
                scope_root=scope_root,
                opa_policy_path=None,
                opa_decision_path="data.akc.allow",
                message=str(exc),
            )
        return _emit_runtime_error(
            str(exc),
            hint="check the runtime policy decision and scope configuration",
            policy_decision_id=_last_policy_decision_id(kernel.policy_runtime) if kernel is not None else None,
            policy_denial=denial,
            format_mode=fmt,
        )
    except Exception as exc:
        if record is not None:
            record["status"] = "failed"
            record["finished_at_ms"] = _now_ms()
            record["last_error"] = str(exc)
            if kernel is not None:
                _persist_policy_log(record, policy_runtime=kernel.policy_runtime)
            _save_runtime_record(record)
        return _emit_runtime_error(str(exc), hint="inspect the runtime bundle and scoped output paths", format_mode=fmt)


def cmd_runtime_stop(args: argparse.Namespace) -> int:
    configure_logging(verbose=bool(getattr(args, "verbose", False)))
    try:
        record = _resolve_runtime_run_record(
            outputs_root=Path(str(args.outputs_root)).expanduser().resolve(),
            runtime_run_id=str(args.runtime_run_id),
            tenant_id=getattr(args, "tenant_id", None),
            repo_id=getattr(args, "repo_id", None),
        )
    except (FileNotFoundError, RuntimeScopeMismatchError, ValueError) as exc:
        return _emit_runtime_error(
            str(exc),
            hint="use --outputs-root plus matching --tenant-id/--repo-id when scope is ambiguous",
        )
    status = str(record.get("status", "")).strip()
    if status in {"terminal", "idle", "stopped", "failed", "max_iterations_exceeded"}:
        print(f"runtime_run_id: {record['runtime_run_id']}")
        print(f"status: {status}")
        print("stop_request: no-op (runtime already terminal)")
        return 0
    control_payload = {
        "stop_requested": True,
        "requested_at_ms": _now_ms(),
        "runtime_run_id": record["runtime_run_id"],
    }
    _write_json(_control_path(record), control_payload)
    record["status"] = "stop_requested"
    _save_runtime_record(record)
    print(f"runtime_run_id: {record['runtime_run_id']}")
    print("status: stop_requested")
    return 0


def cmd_runtime_status(args: argparse.Namespace) -> int:
    configure_logging(verbose=bool(getattr(args, "verbose", False)))
    try:
        record = _resolve_runtime_run_record(
            outputs_root=Path(str(args.outputs_root)).expanduser().resolve(),
            runtime_run_id=str(args.runtime_run_id),
            tenant_id=getattr(args, "tenant_id", None),
            repo_id=getattr(args, "repo_id", None),
        )
    except (FileNotFoundError, RuntimeScopeMismatchError, ValueError) as exc:
        return _emit_runtime_error(str(exc), hint="rerun with matching scope hints if needed")
    events = _load_runtime_events(record)
    checkpoint_exists = _checkpoint_path(record).exists()
    queue_exists = _queue_snapshot_path(record).exists()
    evidence = _load_runtime_evidence(record)
    print(f"runtime_run_id: {record['runtime_run_id']}")
    print(f"scope: {_format_scope_hint(record)}")
    print(f"status: {record.get('status', 'unknown')}")
    print(f"mode: {record.get('mode', 'unknown')}")
    print(f"bundle_path: {record.get('bundle_path')}")
    print(f"event_count: {len(events)}")
    print(f"checkpoint_present: {'yes' if checkpoint_exists else 'no'}")
    print(f"queue_snapshot_present: {'yes' if queue_exists else 'no'}")
    print(f"runtime_evidence_count: {len(evidence)}")
    if events:
        last = events[-1]
        print(f"last_event_type: {last.get('event_type', 'unknown')}")
        print(f"last_event_timestamp: {last.get('timestamp', 0)}")
    denied_decision = next(
        (
            item
            for item in reversed(list(record.get("policy_decisions", [])))
            if isinstance(item, dict) and not bool(item.get("allowed", False))
        ),
        None,
    )
    if isinstance(denied_decision, dict):
        print(f"last_denied_policy_decision_id: {denied_decision.get('token_id', '')}")
        print(f"last_denied_reason: {denied_decision.get('reason', '')}")
    return 0


def cmd_runtime_events(args: argparse.Namespace) -> int:
    configure_logging(verbose=bool(getattr(args, "verbose", False)))
    try:
        record = _resolve_runtime_run_record(
            outputs_root=Path(str(args.outputs_root)).expanduser().resolve(),
            runtime_run_id=str(args.runtime_run_id),
            tenant_id=getattr(args, "tenant_id", None),
            repo_id=getattr(args, "repo_id", None),
        )
    except (FileNotFoundError, RuntimeScopeMismatchError, ValueError) as exc:
        return _emit_runtime_error(str(exc), hint="runtime events are scoped under the runtime run record")
    last_index = 0
    while True:
        events = _load_runtime_events(record)
        for event in events[last_index:]:
            print(json.dumps(event, sort_keys=True))
        last_index = len(events)
        if not bool(getattr(args, "follow", False)):
            return 0
        time.sleep(0.5)


def cmd_runtime_reconcile(args: argparse.Namespace) -> int:
    configure_logging(verbose=bool(getattr(args, "verbose", False)))
    try:
        record = _resolve_runtime_run_record(
            outputs_root=Path(str(args.outputs_root)).expanduser().resolve(),
            runtime_run_id=str(args.runtime_run_id),
            tenant_id=getattr(args, "tenant_id", None),
            repo_id=getattr(args, "repo_id", None),
        )
    except (FileNotFoundError, RuntimeScopeMismatchError, ValueError) as exc:
        return _emit_runtime_error(str(exc), hint="reconcile requires a valid scoped runtime run")
    bundle_path = Path(str(record["bundle_path"])).expanduser().resolve()
    mode: ReconcileMode = "simulate" if bool(getattr(args, "dry_run", False)) else "enforce"
    iterations = 1
    interval_ms = 0
    exit_early = False
    if bool(getattr(args, "watch", False)):
        iterations = max(1, int(getattr(args, "watch_max_iterations", 30)))
        interval_ms = int(max(0.0, float(getattr(args, "watch_interval_sec", 5.0))) * 1000)
        exit_early = True
    try:
        coord_overrides = _coordination_execution_overrides_from_args(args)
        kernel, context = _build_runtime_kernel(
            bundle_path=bundle_path,
            outputs_root=Path(str(args.outputs_root)).expanduser().resolve(),
            runtime_run_id=str(record["runtime_run_id"]),
            runtime_mode=str(record.get("mode", "enforce")),
            strict_intent_authority=True if bool(getattr(args, "strict_intent_authority", False)) else None,
            coordination_execution_overrides=coord_overrides,
        )
        record["coordination_execution_policy"] = kernel.coordination_execution_policy_snapshot()
        _require_promotion_packet_for_live_mutation(
            record=record,
            bundle=kernel.bundle,
            mode=mode,
        )
        r_loop = _run_deployment_reconcile(
            bundle=kernel.bundle,
            mode=mode,
            policy_runtime=kernel.policy_runtime,
            iterations=iterations,
            interval_ms=interval_ms,
            exit_early_if_converged=exit_early,
            runtime_run_id=context.runtime_run_id,
        )
        _persist_policy_log(record, policy_runtime=kernel.policy_runtime)
        intent_sha = _stable_intent_sha256_from_bundle_file(bundle_path)
        built = _build_runtime_evidence(
            context=context,
            runtime_events=_load_runtime_events(record),
            policy_decisions=list(record.get("policy_decisions", [])),
            reconcile_evidence=r_loop.evidence,
            reconcile_statuses=r_loop.statuses,
            terminal_status=str(record.get("status", "unknown")),
            stable_intent_sha256=intent_sha,
            resync_completed_attempts=r_loop.completed_attempts,
            resync_configured_max_attempts=r_loop.configured_max_attempts,
            resync_interval_ms=r_loop.interval_ms,
            provider_id=_deployment_provider_id_from_bundle(kernel.bundle),
            policy_mode=str(context.policy_mode),
            total_resync_wait_ms=r_loop.total_resync_wait_ms,
            provider_contract=_deployment_provider_contract_from_bundle(kernel.bundle),
            bundle_metadata=kernel.bundle.metadata,
        )
        cwd = Path.cwd()
        proj = load_akc_project_config(cwd)
        built = _augment_evidence_with_validators(
            record,
            evidence=built,
            specs=_post_runtime_operational_criteria_from_bundle(bundle_path),
            cwd=cwd,
            project=proj,
        )
        _persist_runtime_evidence(record, evidence=built)
        ov_summary = _maybe_write_operational_validity_report(record, evidence=built)
        if ov_summary.ran_evaluation and not ov_summary.passed_all:
            _append_operational_validity_attested_failure_event(
                record,
                failed_success_criterion_ids=ov_summary.failed_success_criterion_ids,
            )
        record["last_reconcile_mode"] = mode
        record["last_reconcile_at_ms"] = _now_ms()
        record["reconcile_all_converged"] = bool(r_loop.statuses) and all(s.converged for s in r_loop.statuses)
        record["reconcile_resync_wait_ms_scheduled"] = int(r_loop.total_resync_wait_ms)
        _save_runtime_record(record)
        _persist_compile_run_manifest_with_runtime_links(record, runtime_evidence=_load_runtime_evidence(record))
        print(f"runtime_run_id: {record['runtime_run_id']}")
        print(f"reconcile_mode: {mode}")
        print(f"resource_count: {len(r_loop.statuses)}")
        if bool(getattr(args, "watch", False)):
            print(f"reconcile_iterations: {iterations} interval_ms: {interval_ms}")
        for status in r_loop.statuses:
            print(json.dumps(status.to_json_obj(), sort_keys=True))
        div_exit = _reconcile_divergence_exit_code(
            metadata=kernel.bundle.metadata,
            statuses=r_loop.statuses,
            terminal_status=str(record.get("status", "unknown")),
        )
        if div_exit is not None:
            record["last_error"] = (
                "reconcile divergence or failed aggregate health (runtime_nonzero_exit_on_reconcile_divergence)"
            )
            _save_runtime_record(record)
            _persist_compile_run_manifest_with_runtime_links(record, runtime_evidence=_load_runtime_evidence(record))
            print(
                "Runtime CLI error: reconcile did not satisfy convergence contract for this bundle "
                "(see convergence_certificate and reconcile_resource_status in runtime_evidence.json)",
                file=sys.stderr,
            )
            return int(div_exit)
        return 0
    except PermissionError as exc:
        return _emit_runtime_error(
            str(exc),
            hint="reconcile was blocked by policy; inspect the decision id below",
            policy_decision_id=_policy_decision_id_from_record(record),
        )
    except Exception as exc:
        return _emit_runtime_error(str(exc), hint="verify the runtime bundle and run scope")


def cmd_runtime_checkpoint(args: argparse.Namespace) -> int:
    configure_logging(verbose=bool(getattr(args, "verbose", False)))
    try:
        record = _resolve_runtime_run_record(
            outputs_root=Path(str(args.outputs_root)).expanduser().resolve(),
            runtime_run_id=str(args.runtime_run_id),
            tenant_id=getattr(args, "tenant_id", None),
            repo_id=getattr(args, "repo_id", None),
        )
    except (FileNotFoundError, RuntimeScopeMismatchError, ValueError) as exc:
        return _emit_runtime_error(str(exc), hint="checkpoint reads are tenant/repo scoped")
    path = _checkpoint_path(record)
    if not path.exists():
        return _emit_runtime_error(
            "runtime checkpoint not found",
            hint="run `akc runtime start` first or verify the runtime run scope",
        )
    print(path.read_text(encoding="utf-8"))
    return 0


def cmd_runtime_replay(args: argparse.Namespace) -> int:
    configure_logging(verbose=bool(getattr(args, "verbose", False)))
    try:
        record = _resolve_runtime_run_record(
            outputs_root=Path(str(args.outputs_root)).expanduser().resolve(),
            runtime_run_id=str(args.runtime_run_id),
            tenant_id=getattr(args, "tenant_id", None),
            repo_id=getattr(args, "repo_id", None),
        )
    except (FileNotFoundError, RuntimeScopeMismatchError, ValueError) as exc:
        return _emit_runtime_error(str(exc), hint="runtime replay requires the scoped runtime record")
    runtime_events = _load_runtime_events(record)
    runtime_evidence = _load_runtime_evidence(record)
    if not runtime_evidence:
        return _emit_runtime_error(
            "no runtime evidence recorded for this runtime run",
            hint="run `akc runtime start` or `akc runtime reconcile` first",
        )
    manifest = _run_manifest_for_replay(
        record=record,
        replay_mode=str(args.mode),
        runtime_evidence=runtime_evidence,
        runtime_events=runtime_events,
    )
    replay = replay_runtime_execution(manifest=manifest, transcript=runtime_events)
    payload = {
        "runtime_run_id": replay.runtime_run_id,
        "mode": replay.mode,
        "transition_count": len(replay.transitions),
        "reconcile_decision_count": len(replay.reconcile_decisions),
        "terminal_health_status": replay.terminal_health_status,
        "transitions": [
            {
                "event_id": item.event.get("event_id"),
                "event_type": item.event.get("event_type"),
                "transition": dict(item.transition) if item.transition is not None else None,
                "action_decision": item.action_decision,
                "retry_count": item.retry_count,
                "budget_burn": dict(item.budget_burn) if item.budget_burn is not None else None,
            }
            for item in replay.transitions
        ],
        "reconcile_decisions": [
            {
                "resource_id": item.resource_id,
                "operation_type": item.operation_type,
                "applied": item.applied,
                "rollback_chain": list(item.rollback_chain),
                "health_status": item.health_status,
                "payload": dict(item.payload),
            }
            for item in replay.reconcile_decisions
        ],
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def cmd_runtime_autopilot(args: argparse.Namespace) -> int:
    """Run always-on runtime autopilot controller loop."""
    configure_logging(verbose=bool(getattr(args, "verbose", False)))

    from akc.adopt.trust_ladder import adoption_level_index, parse_adoption_level
    from akc.cli.project_config import load_akc_project_config
    from akc.living.automation_profile import resolve_living_automation_profile
    from akc.runtime.autopilot import AutonomyBudgetConfig, ReliabilitySLOGateConfig, run_runtime_autopilot

    proj = load_akc_project_config(Path.cwd())
    adoption = parse_adoption_level(proj.adoption_level) if proj is not None else None
    living_profile = resolve_living_automation_profile(
        cli_value=getattr(args, "living_automation_profile", None),
        env=os.environ,
        project_value=proj.living_automation_profile if proj is not None else None,
    )

    # Progressive takeover (Level 4): if adoption is set to full autonomy and living automation
    # was not configured via CLI/env/project, default to unattended living loop.
    if adoption is not None and adoption_level_index(adoption) >= 4:
        cli_lp = getattr(args, "living_automation_profile", None)
        env_lp = os.environ.get("AKC_LIVING_AUTOMATION_PROFILE")
        proj_lp = proj.living_automation_profile if proj is not None else None
        if (
            (cli_lp is None or str(cli_lp).strip() == "")
            and (env_lp is None or str(env_lp).strip() == "")
            and (proj_lp is None or str(proj_lp).strip() == "")
        ):
            from akc.living.automation_profile import PROFILE_LIVING_LOOP_UNATTENDED_V1

            living_profile = PROFILE_LIVING_LOOP_UNATTENDED_V1

    use_unattended_defaults = bool(getattr(args, "unattended_defaults", False))
    if use_unattended_defaults and living_profile.id != "living_loop_unattended_v1":
        raise SystemExit(
            "--unattended-defaults requires living_loop_unattended_v1 "
            "(set --living-automation-profile or AKC_LIVING_AUTOMATION_PROFILE / .akc/project.json)."
        )

    living_interval_s = float(
        getattr(args, "living_check_interval_sec", getattr(args, "living_check_interval_s", 3600.0))
    )
    lease_ns_effective: str | None = getattr(args, "lease_namespace", None)

    # Progressive takeover (Level 4): if budgets were not provided explicitly, prefer unattended defaults
    # (still requires unattended living automation to be enabled, i.e. living_loop_unattended_v1).
    if adoption is not None and adoption_level_index(adoption) >= 4 and not use_unattended_defaults:
        missing_budget_flag = False
        for _label, val in (
            ("--max-mutations-per-day", getattr(args, "max_mutations_per_day", None)),
            ("--max-concurrent-rollouts", getattr(args, "max_concurrent_rollouts", None)),
            ("--rollback-budget-per-day", getattr(args, "rollback_budget_per_day", None)),
            ("--max-consecutive-rollout-failures", getattr(args, "max_consecutive_rollout_failures", None)),
            (
                "--max-rollbacks-per-day-before-escalation",
                getattr(args, "max_rollbacks_per_day_before_escalation", None),
            ),
            ("--cooldown-after-failure-ms", getattr(args, "cooldown_after_failure_ms", None)),
            ("--cooldown-after-policy-deny-ms", getattr(args, "cooldown_after_policy_deny_ms", None)),
        ):
            if val is None:
                missing_budget_flag = True
                break
        if missing_budget_flag:
            use_unattended_defaults = True

    if use_unattended_defaults and living_profile.id != "living_loop_unattended_v1":
        raise SystemExit(
            "--unattended-defaults requires living_loop_unattended_v1 "
            "(set --living-automation-profile or AKC_LIVING_AUTOMATION_PROFILE / .akc/project.json)."
        )

    if use_unattended_defaults:
        from akc.living.unattended_defaults import unattended_autopilot_defaults_for_env

        ud = unattended_autopilot_defaults_for_env(str(getattr(args, "env_profile", "staging")))
        budgets = ud.budgets
        living_interval_s = float(ud.living_check_interval_s)
        if lease_ns_effective is None and ud.lease_namespace is not None:
            lease_ns_effective = ud.lease_namespace
    else:
        missing: list[str] = []
        for label, val in (
            ("--max-mutations-per-day", getattr(args, "max_mutations_per_day", None)),
            ("--max-concurrent-rollouts", getattr(args, "max_concurrent_rollouts", None)),
            ("--rollback-budget-per-day", getattr(args, "rollback_budget_per_day", None)),
            ("--max-consecutive-rollout-failures", getattr(args, "max_consecutive_rollout_failures", None)),
            (
                "--max-rollbacks-per-day-before-escalation",
                getattr(args, "max_rollbacks_per_day_before_escalation", None),
            ),
            ("--cooldown-after-failure-ms", getattr(args, "cooldown_after_failure_ms", None)),
            ("--cooldown-after-policy-deny-ms", getattr(args, "cooldown_after_policy_deny_ms", None)),
        ):
            if val is None:
                missing.append(label)
        if missing:
            raise SystemExit(
                "Missing autonomy budget flag(s): "
                + ", ".join(missing)
                + " (or pass --unattended-defaults with living_loop_unattended_v1)."
            )
        budgets = AutonomyBudgetConfig(
            max_mutations_per_day=int(args.max_mutations_per_day),
            max_concurrent_rollouts=int(args.max_concurrent_rollouts),
            rollback_budget_per_day=int(args.rollback_budget_per_day),
            max_consecutive_rollout_failures=int(args.max_consecutive_rollout_failures),
            max_rollbacks_per_day_before_escalation=int(args.max_rollbacks_per_day_before_escalation),
            cooldown_after_failure_ms=int(args.cooldown_after_failure_ms),
            cooldown_after_policy_deny_ms=int(args.cooldown_after_policy_deny_ms),
        )

    slo_gate_cfg: ReliabilitySLOGateConfig | None = None
    # Progressive takeover (Level 4): default to SLO gating when configured for full autonomy.
    use_slo_gate = bool(getattr(args, "slo_gate", False)) or (
        adoption is not None and adoption_level_index(adoption) >= 4
    )
    if use_slo_gate:
        slo_gate_cfg = ReliabilitySLOGateConfig(
            min_rollouts_total=int(getattr(args, "slo_min_rollouts", 5)),
            min_policy_compliance_rate=float(getattr(args, "slo_min_policy_compliance_rate", 0.98)),
            min_rollback_success_rate=float(getattr(args, "slo_min_rollback_success_rate", 0.95)),
            max_delivery_change_instability_proxy=float(getattr(args, "slo_max_change_instability_proxy", 0.25)),
        )

    return run_runtime_autopilot(
        outputs_root=args.outputs_root,
        ingest_state_path=args.ingest_state_path,
        tenant_id=getattr(args, "tenant_id", None),
        repo_id=getattr(args, "repo_id", None),
        eval_suite_path=getattr(args, "eval_suite_path", "configs/evals/intent_system_v1.json"),
        policy_mode=cast(Literal["audit_only", "enforce"], str(args.policy_mode)),
        canary_mode=cast(Literal["quick", "thorough"], str(args.canary_mode)),
        accept_mode=cast(Literal["quick", "thorough"], str(args.accept_mode)),
        living_check_interval_s=living_interval_s,
        scoreboard_window_ms=int(args.scoreboard_window_ms),
        budgets=budgets,
        max_iterations=getattr(args, "max_iterations", None),
        goal=str(getattr(args, "goal", "Compile repository")),
        verbose=bool(getattr(args, "verbose", False)),
        controller_id=getattr(args, "controller_id", None),
        lease_backend=cast(Literal["filesystem", "k8s"], str(getattr(args, "lease_backend", "filesystem"))),
        lease_name=getattr(args, "lease_name", None),
        lease_namespace=lease_ns_effective,
        scope_registry_path=getattr(args, "scope_registry_path", None),
        env_profile=cast(Literal["dev", "staging", "prod"], str(getattr(args, "env_profile", "staging"))),
        living_automation_profile=living_profile,
        reliability_slo_gate=slo_gate_cfg,
    )
