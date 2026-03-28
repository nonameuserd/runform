from __future__ import annotations

import json
import os
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from akc.compile import (
    Budget,
    CompileSession,
    ControllerConfig,
    CostRates,
    SubprocessExecutor,
    TierConfig,
)
from akc.compile.controller import ControllerResult
from akc.compile.interfaces import Executor, Index, LLMBackend, TenantRepoScope
from akc.compile.knowledge_extractor import extract_knowledge_snapshot
from akc.compile.provenance_mapper import build_doc_id_to_provenance_map
from akc.control.policy import (
    CapabilityIssuer,
    DefaultDenyPolicyEngine,
    SubprocessOpaEvaluator,
    ToolAuthorizationPolicy,
)
from akc.evals import run_eval_suite
from akc.intent import compile_intent_spec, compute_intent_fingerprint
from akc.intent.models import stable_intent_sha256
from akc.ir import IRDocument
from akc.knowledge import knowledge_provenance_fingerprint, knowledge_semantic_fingerprint
from akc.living.automation_profile import LivingAutomationProfile, resolve_living_automation_profile
from akc.memory.models import PlanState, normalize_repo_id, normalize_tenant_id, require_non_empty
from akc.outputs.drift import DriftFinding, drift_report, extend_drift_report, write_baseline, write_drift_artifacts
from akc.outputs.fingerprints import IngestStateFingerprint, stable_json_fingerprint
from akc.path_security import expanduser_resolve_trusted_invoker, safe_resolve_scoped_path
from akc.run.loader import find_latest_run_manifest, load_run_manifest
from akc.run.recompile_triggers import (
    evaluate_recompile_triggers,
    normalized_success_criterion_ids_from_runtime_payload,
)
from akc.runtime.models import RuntimeEvent
from akc.runtime.policy import RuntimePolicyRuntime

_TenantKeySep = "::"
RuntimeImpactClass = Literal["service_degradation", "agent_degradation", "workflow_degradation"]

_SENSITIVE_KEY_MARKERS: frozenset[str] = frozenset(
    {
        "secret",
        "secrets",
        "token",
        "password",
        "passphrase",
        "private_key",
        "client_secret",
        "api_key",
        "apikey",
        "access_key",
        "secret_key",
        "authorization",
        "cookie",
        "set_cookie",
    }
)


def _looks_sensitive_key(key: str) -> bool:
    k = "".join(ch for ch in str(key).strip().lower() if ch.isalnum() or ch in {"_", "-"})
    if not k:
        return False
    if k in _SENSITIVE_KEY_MARKERS:
        return True
    for marker in _SENSITIVE_KEY_MARKERS:
        if (
            k.endswith(f"_{marker}")
            or k.endswith(f"-{marker}")
            or k.startswith(f"{marker}_")
            or k.startswith(f"{marker}-")
        ):
            return True
    return False


def _redact_sensitive_payload(obj: Any) -> Any:
    """Redact secret-like fields before writing JSON artifacts to disk."""
    if isinstance(obj, Mapping):
        out: dict[str, Any] = {}
        for k, v in obj.items():
            ks = str(k)
            if _looks_sensitive_key(ks):
                out[ks] = "<redacted>"
            else:
                out[ks] = _redact_sensitive_payload(v)
        return out
    if isinstance(obj, list):
        return [_redact_sensitive_payload(v) for v in obj]
    return obj


def _sanitize_regression_thresholds_for_disk(raw: Any) -> dict[str, float]:
    """Allowlist + coerce numeric regression thresholds for safe persistence."""
    if not isinstance(raw, Mapping):
        return {}
    allowed: tuple[str, ...] = (
        "min_success_rate",
        "max_avg_repair_iterations",
        "max_success_rate_drop",
        "max_avg_wall_time_regression_pct",
    )
    out: dict[str, float] = {}
    for k in allowed:
        if k not in raw:
            continue
        v = raw.get(k)
        if isinstance(v, bool) or v is None:
            continue
        if isinstance(v, (int, float)):
            out[k] = float(v)
        else:
            try:
                out[k] = float(str(v).strip())
            except Exception:
                continue
    return out


def _mk_canary_eval_suite_for_disk(
    *,
    tenant_id: str,
    repo_id: str,
    manifest_path: str,
    regression_thresholds: Any,
) -> dict[str, Any]:
    """Build an allowlisted eval suite object safe to write to disk."""
    return {
        "suite_version": "living-canary-v1",
        "tasks": [
            {
                "id": "living-canary",
                "tenant_id": tenant_id,
                "repo_id": repo_id,
                "manifest_path": manifest_path,
                "checks": {
                    "require_success": True,
                    "required_passes": ["plan", "retrieve", "generate", "execute"],
                    "max_repair_iterations": 2,
                    "max_total_tokens": 100000,
                    "max_wall_time_ms": 60000,
                    "require_trace_spans": True,
                    "require_runtime_replay_determinism": True,
                    "runtime_mode": "simulate",
                    "runtime_reliability_kpis": {
                        "max_rollbacks_total": 0,
                        "max_convergence_latency_ms_avg": 60000,
                        "max_mttr_like_repair_latency_ms_avg": 60000,
                        "require_terminal_health_in": ["healthy", "unknown", "degraded"],
                    },
                },
                "judge": {"enabled": False},
            }
        ],
        "regression_thresholds": _sanitize_regression_thresholds_for_disk(regression_thresholds),
    }


@dataclass(frozen=True, slots=True)
class RuntimeImpact:
    event: RuntimeEvent
    impact_class: RuntimeImpactClass
    severity: Literal["med", "high"]
    impacted_step_ids: tuple[str, ...]
    resource_id: str | None = None


def _read_json_object(path: Path, *, what: str) -> dict[str, Any]:
    raw = path.read_text(encoding="utf-8")
    loaded = json.loads(raw)
    if not isinstance(loaded, dict):
        raise ValueError(f"{what} must be a JSON object: {path}")
    return loaded


def _payload_str(payload: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _coerce_runtime_event(raw: RuntimeEvent | Mapping[str, Any]) -> RuntimeEvent:
    if isinstance(raw, RuntimeEvent):
        return raw
    if isinstance(raw, Mapping):
        return RuntimeEvent.from_json_obj(raw)
    raise ValueError("runtime event must be RuntimeEvent or JSON object")


def _is_operational_validity_attestation_failure(event: RuntimeEvent) -> bool:
    """True when the runtime recorded a failed post-runtime operational validity attestation."""

    if event.event_type.strip() != "runtime.operational_validity.attested":
        return False
    return event.payload.get("passed") is False


def _operational_validity_drift_findings_from_impacts(
    impacts: tuple[RuntimeImpact, ...],
) -> tuple[DriftFinding, ...]:
    """Drift rows for authorized operational attestation failures (policy-gated upstream)."""

    seen: set[str] = set()
    out: list[DriftFinding] = []
    for imp in impacts:
        if not _is_operational_validity_attestation_failure(imp.event):
            continue
        eid = imp.event.event_id
        if eid in seen:
            continue
        seen.add(eid)
        details: dict[str, Any] = {
            "event_id": eid,
            "event_type": imp.event.event_type,
            "runtime_run_id": imp.event.context.runtime_run_id,
        }
        sc_ids = normalized_success_criterion_ids_from_runtime_payload(dict(imp.event.payload))
        if sc_ids:
            details["success_criterion_ids"] = list(sc_ids)
            if len(sc_ids) == 1:
                details["success_criterion_id"] = sc_ids[0]
        out.append(
            DriftFinding(
                kind="operational_validity_failed",
                severity="high",
                details=details,
            )
        )
    return tuple(out)


def _collect_operational_validity_success_criterion_ids_from_impacts(
    impacts: tuple[RuntimeImpact, ...],
) -> tuple[str, ...]:
    """Union of success criterion ids from failed operational attestation events (deterministic order)."""

    collected: set[str] = set()
    for imp in impacts:
        if not _is_operational_validity_attestation_failure(imp.event):
            continue
        for cid in normalized_success_criterion_ids_from_runtime_payload(dict(imp.event.payload)):
            collected.add(cid)
    return tuple(sorted(collected))


def _is_runtime_health_or_reconcile_failure(event: RuntimeEvent) -> bool:
    if _is_operational_validity_attestation_failure(event):
        return True
    payload = event.payload
    health_status = _payload_str(payload, "health_status")
    if health_status in {"degraded", "failed"}:
        return True
    event_type = event.event_type.strip().lower()
    if event_type.endswith(".failed") or ".failure" in event_type:
        return True
    if "reconcile" not in event_type:
        return False
    if payload.get("converged") is False:
        return True
    if isinstance(payload.get("rollback_triggered"), bool) and bool(payload.get("rollback_triggered")):
        return True
    error = _payload_str(payload, "error", "reason", "last_error")
    return error is not None and error.lower() not in {
        "simulate",
        "simulation",
        "dry-run",
        "dry_run",
    }


def _runtime_impact_class(event: RuntimeEvent) -> RuntimeImpactClass:
    payload = event.payload
    kind_hint = (
        _payload_str(payload, "resource_class", "node_kind", "kind") or event.event_type.strip().lower()
    ).lower()
    if "service" in kind_hint or "reconcile" in event.event_type.strip().lower():
        return "service_degradation"
    if "agent" in kind_hint:
        return "agent_degradation"
    return "workflow_degradation"


def _runtime_impact_severity(event: RuntimeEvent) -> Literal["med", "high"]:
    if _is_operational_validity_attestation_failure(event):
        return "high"
    health_status = (_payload_str(event.payload, "health_status") or event.event_type.strip().lower()).lower()
    if "failed" in health_status or "rollback" in health_status:
        return "high"
    return "med"


def _runtime_impacted_step_ids(*, ir: IRDocument | None, event: RuntimeEvent) -> tuple[str, ...]:
    if ir is None:
        return ()
    resource_id = _payload_str(event.payload, "resource_id", "node_id", "target")
    if resource_id is None:
        return ()
    step_ids: set[str] = set()
    for node in ir.nodes:
        if resource_id not in {
            node.id,
            node.name,
            str(node.properties.get("resource_id", "")).strip(),
            str(node.properties.get("deployment_target", "")).strip(),
        }:
            continue
        step_id = node.properties.get("step_id")
        if isinstance(step_id, str) and step_id.strip():
            step_ids.add(step_id.strip())
    return tuple(sorted(step_ids))


def _mk_runtime_policy_runtime(
    *,
    event: RuntimeEvent,
    policy_mode: Literal["audit_only", "enforce"],
    opa_policy_path: str | None,
    opa_decision_path: str,
) -> RuntimePolicyRuntime:
    engine = DefaultDenyPolicyEngine(
        issuer=CapabilityIssuer(),
        policy=ToolAuthorizationPolicy(
            mode=policy_mode,
            allow_actions=("runtime.event.consume",),
            opa=(
                SubprocessOpaEvaluator(
                    policy_path=opa_policy_path,
                    decision_path=opa_decision_path,
                )
                if opa_policy_path is not None
                else None
            ),
        ),
    )
    return RuntimePolicyRuntime(
        context=event.context,
        policy_engine=engine,
        issuer=engine.issuer,
        decision_log=[],
    )


def _collect_runtime_impacts(
    *,
    runtime_events: tuple[RuntimeEvent | Mapping[str, Any], ...] | None,
    ir: IRDocument | None,
    policy_mode: Literal["audit_only", "enforce"],
    opa_policy_path: str | None,
    opa_decision_path: str,
    runtime_policy_runtime: RuntimePolicyRuntime | None,
) -> tuple[RuntimeImpact, ...]:
    if not runtime_events:
        return ()
    impacts: list[RuntimeImpact] = []
    for raw_event in runtime_events:
        event = _coerce_runtime_event(raw_event)
        if not _is_runtime_health_or_reconcile_failure(event):
            continue
        policy_runtime = runtime_policy_runtime or _mk_runtime_policy_runtime(
            event=event,
            policy_mode=policy_mode,
            opa_policy_path=opa_policy_path,
            opa_decision_path=opa_decision_path,
        )
        decision = policy_runtime.authorize(
            action="runtime.event.consume",
            context=event.context,
            extra_context={
                "event_id": event.event_id,
                "event_type": event.event_type,
                "health_status": _payload_str(event.payload, "health_status") or "unknown",
                "resource_id": _payload_str(event.payload, "resource_id", "node_id", "target") or "",
                "impact_class": _runtime_impact_class(event),
            },
        )
        if not decision.allowed and decision.block:
            continue
        impacts.append(
            RuntimeImpact(
                event=event,
                impact_class=_runtime_impact_class(event),
                severity=_runtime_impact_severity(event),
                impacted_step_ids=_runtime_impacted_step_ids(ir=ir, event=event),
                resource_id=_payload_str(event.payload, "resource_id", "node_id", "target"),
            )
        )
    return tuple(impacts)


def _runtime_canary_thresholds_allow(
    *,
    impacts: tuple[RuntimeImpact, ...],
    thresholds: Mapping[str, Any] | None,
) -> bool:
    if not impacts:
        return True
    limits = dict(thresholds or {})
    if not limits:
        return True
    total = len(impacts)
    failed = sum(1 for impact in impacts if impact.severity == "high")
    degraded = total - failed

    max_total = limits.get("max_total_impacts")
    if isinstance(max_total, (int, float)) and total > int(max_total):
        return False
    max_failed = limits.get("max_failed_impacts")
    if isinstance(max_failed, (int, float)) and failed > int(max_failed):
        return False
    max_degraded = limits.get("max_degraded_impacts")
    return not (isinstance(max_degraded, (int, float)) and degraded > int(max_degraded))


def parse_ingest_state_key(
    *,
    tenant_id: str,
    key: str,
) -> tuple[str, str] | None:
    """Parse an ingestion state key into (connector, source_id).

    Ingestion state keys are expected to follow:
    `<tenant_id>::<connector_name>::<source_id>`.
    """

    if not isinstance(key, str) or not key.startswith(f"{tenant_id}{_TenantKeySep}"):
        return None
    # Preserve any `::` inside source_id by limiting splits.
    parts = key.split(_TenantKeySep, 2)
    if len(parts) != 3:
        return None
    _, connector, source_id = parts
    connector = str(connector).strip()
    source_id = str(source_id).strip()
    if not connector or not source_id:
        return None
    return connector, source_id


def fq_source_id(connector: str, source_id: str) -> str:
    return f"{connector.strip()}::{source_id.strip()}"


def compute_changed_source_ids(
    *,
    tenant_id: str,
    baseline_sources_by_key: Mapping[str, Any] | None,
    current_state_by_key: Mapping[str, Any],
) -> set[str]:
    """Compute changed ingestion source ids by per-key fingerprint equality."""

    require_non_empty(tenant_id, name="tenant_id")
    changed: set[str] = set()
    baseline = dict(baseline_sources_by_key or {})

    def _fp(value: Any) -> str:
        if isinstance(value, Mapping):
            return stable_json_fingerprint(dict(value))
        # Fall back to a stable fingerprint of a wrapped scalar/string representation.
        return stable_json_fingerprint({"value": str(value)})

    # Union over keys so removals count as changes.
    keys = set(baseline.keys()) | set(current_state_by_key.keys())
    for k in keys:
        parsed = parse_ingest_state_key(tenant_id=tenant_id, key=str(k))
        if parsed is None:
            continue
        connector, source_id = parsed
        prev = baseline.get(k)
        curr = current_state_by_key.get(k, None)
        # Backward compatible:
        # - old baselines stored raw per-key objects (dict/JSON)
        # - new baselines store per-key stable fingerprints (hex strings)
        if isinstance(prev, str) and len(prev) >= 16:
            curr_fp = _fp(curr)
            if prev.strip().lower() != curr_fp.strip().lower():
                changed.add(fq_source_id(connector, source_id))
            continue
        if isinstance(prev, dict) and isinstance(curr, dict):
            if dict(prev) != dict(curr):
                changed.add(fq_source_id(connector, source_id))
            continue
        if prev != curr:
            changed.add(fq_source_id(connector, source_id))
    return changed


def compute_impacted_workflow_step_ids(
    *,
    ir: IRDocument,
    changed_source_ids: set[str],
) -> set[str]:
    """Compute impacted workflow step ids from the IR provenance + dependency graph."""

    if not changed_source_ids:
        return set()

    nodes_by_id = {n.id: n for n in ir.nodes}
    dependents: dict[str, set[str]] = {}
    for n in ir.nodes:
        for dep in n.depends_on:
            dependents.setdefault(dep, set()).add(n.id)

    start_node_ids: set[str] = set()
    for n in ir.nodes:
        if n.kind != "workflow":
            continue
        for p in n.provenance:
            if p.source_id in changed_source_ids:
                start_node_ids.add(n.id)
                break

    if not start_node_ids:
        # No provenance hit -> conservative caller should recompile everything.
        return set()

    queue = list(start_node_ids)
    visited: set[str] = set(start_node_ids)
    while queue:
        cur = queue.pop(0)
        for nxt in dependents.get(cur, set()):
            if nxt in visited:
                continue
            visited.add(nxt)
            queue.append(nxt)

    impacted_steps: set[str] = set()
    for nid in visited:
        node = nodes_by_id.get(nid)
        if node is None or node.kind != "workflow":
            continue
        step_id = node.properties.get("step_id")
        if isinstance(step_id, str) and step_id.strip():
            impacted_steps.add(step_id.strip())
    return impacted_steps


def _latest_ir_document_path(
    *,
    outputs_root: Path,
    tenant_id: str,
    repo_id: str,
) -> Path | None:
    from akc.path_security import safe_resolve_scoped_path

    base = safe_resolve_scoped_path(outputs_root, tenant_id, normalize_repo_id(repo_id))
    ir_dir = safe_resolve_scoped_path(base, ".akc", "ir")
    if not ir_dir.exists():
        return None
    candidates = [
        p
        for p in ir_dir.iterdir()
        if p.is_file() and p.suffix == ".json" and not p.name.endswith(".diff.json") and p.name != "diff.json"
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _reset_steps_for_recompile(
    *,
    plan_store: Any,
    tenant_id: str,
    repo_id: str,
    plan_id: str,
    impacted_step_ids: set[str],
    skip_other_pending: bool,
) -> None:
    plan: PlanState | None = plan_store.load_plan(tenant_id=tenant_id, repo_id=repo_id, plan_id=plan_id)
    if plan is None:
        raise ValueError("active plan not found for recompile")

    new_steps = []
    for s in plan.steps:
        if s.id in impacted_step_ids:
            new_steps.append(
                type(s)(
                    id=s.id,
                    title=s.title,
                    status="pending",
                    order_idx=s.order_idx,
                    started_at_ms=None,
                    finished_at_ms=None,
                    notes=None,
                    inputs=s.inputs,
                    outputs={},
                )
            )
        elif skip_other_pending and s.status in {"pending", "in_progress"}:
            new_steps.append(
                type(s)(
                    id=s.id,
                    title=s.title,
                    status="skipped",
                    order_idx=s.order_idx,
                    started_at_ms=s.started_at_ms,
                    finished_at_ms=s.finished_at_ms,
                    notes=s.notes,
                    inputs=s.inputs,
                    outputs=s.outputs,
                )
            )
        else:
            new_steps.append(s)

    plan2 = type(plan)(
        id=plan.id,
        tenant_id=plan.tenant_id,
        repo_id=plan.repo_id,
        goal=plan.goal,
        status=plan.status,
        created_at_ms=plan.created_at_ms,
        updated_at_ms=int(time.time() * 1000),
        steps=tuple(new_steps),
        next_step_id=plan.next_step_id,
        budgets=plan.budgets,
        last_feedback=plan.last_feedback,
    )
    plan_store.save_plan(tenant_id=tenant_id, repo_id=repo_id, plan=plan2)


class _OfflineCompileLLM(LLMBackend):
    """Deterministic offline backend for safe recompile canaries."""

    def complete(
        self,
        *,
        scope: Any,
        stage: str,
        request: Any,
    ) -> Any:
        # Ignore request content; generate a patch that touches both code and tests.
        _ = (scope, request)
        text = "\n".join(
            [
                "--- a/src/akc_compiled.py",
                "+++ b/src/akc_compiled.py",
                "@@",
                f"+# compiled stage={stage}",
                "",
                "--- a/tests/test_akc_compiled.py",
                "+++ b/tests/test_akc_compiled.py",
                "@@",
                "+def test_compiled_smoke():",
                "+    assert True",
                "",
            ]
        )
        # Minimal compatibility with controller expectations (LLMResponse shape).
        from akc.compile.interfaces import LLMResponse  # local import to avoid cycles

        return LLMResponse(text=text, raw=None, usage=None)


def _mk_compile_config(
    *,
    mode: Literal["quick", "thorough"],
    test_mode: Literal["smoke", "full"],
    policy_mode: Literal["audit_only", "enforce"],
    promotion_mode: Literal["artifact_only", "staged_apply", "live_apply"] | None = None,
    opa_policy_path: str | None = None,
    opa_decision_path: str = "data.akc.allow",
    cost_rates: CostRates | None = None,
    living_automation_profile: LivingAutomationProfile | None = None,
    llm_backend_name: str = "offline",
    llm_model: str | None = None,
) -> ControllerConfig:
    from akc.compile.controller_config import ControllerConfig as CC

    if llm_backend_name == "offline":
        tiers: dict[Literal["small", "medium", "large"], TierConfig] = {
            "small": TierConfig(name="small", llm_model="offline-small", temperature=0.0),
            "medium": TierConfig(name="medium", llm_model="offline-medium", temperature=0.2),
            "large": TierConfig(name="large", llm_model="offline-large", temperature=0.3),
        }
    else:
        selected_model = str(llm_model or "hosted-model").strip()
        tiers = {
            "small": TierConfig(name="small", llm_model=selected_model, temperature=0.0),
            "medium": TierConfig(name="medium", llm_model=selected_model, temperature=0.2),
            "large": TierConfig(name="large", llm_model=selected_model, temperature=0.3),
        }

    if mode == "thorough":
        budget = Budget(max_llm_calls=8, max_repairs_per_step=3, max_iterations_total=6)
        full_every: int | None = None
    else:
        budget = Budget(max_llm_calls=4, max_repairs_per_step=2, max_iterations_total=4)
        full_every = 2

    meta: dict[str, Any] = {
        "mode": mode,
        **({"promotion_mode": promotion_mode} if promotion_mode is not None else {}),
    }
    if living_automation_profile is not None and living_automation_profile.id in (
        "living_loop_v1",
        "living_loop_unattended_v1",
    ):
        meta["living_automation_profile_id"] = living_automation_profile.id
        if living_automation_profile.baseline_duration_hours is not None:
            meta["baseline_duration_hours"] = float(living_automation_profile.baseline_duration_hours)
    meta["llm_backend"] = llm_backend_name
    if llm_model is not None:
        meta["llm_model"] = str(llm_model)

    return CC(
        tiers=tiers,
        stage_tiers={"generate": "small", "repair": "small"},
        budget=budget,
        test_mode=test_mode,
        full_test_every_n_iterations=full_every if test_mode == "smoke" else None,
        policy_mode=policy_mode,
        tool_allowlist=("llm.complete", "executor.run"),
        opa_policy_path=opa_policy_path,
        opa_decision_path=opa_decision_path,
        cost_rates=cost_rates or CostRates(),
        # `promotion_mode` drives whether compile emits a signed promotion packet
        # that runtime is allowed to consume for actual live mutation.
        metadata=meta,
    )


def safe_recompile_on_drift(
    *,
    tenant_id: str,
    repo_id: str,
    outputs_root: str | Path,
    ingest_state_path: str | Path,
    baseline_path: str | Path | None = None,
    eval_suite_path: str | Path,
    goal: str = "Compile repository",
    policy_mode: Literal["audit_only", "enforce"] = "enforce",
    canary_mode: Literal["quick", "thorough"] = "quick",
    accept_mode: Literal["quick", "thorough"] = "thorough",
    canary_test_mode: Literal["smoke", "full"] = "smoke",
    allow_network: bool = False,
    llm_backend: LLMBackend | None = None,
    index: Index | None = None,
    update_baseline_on_accept: bool = True,
    skip_other_pending: bool = True,
    opa_policy_path: str | None = None,
    opa_decision_path: str = "data.akc.allow",
    runtime_events: tuple[RuntimeEvent | Mapping[str, Any], ...] | None = None,
    runtime_canary_thresholds: Mapping[str, Any] | None = None,
    runtime_policy_runtime: RuntimePolicyRuntime | None = None,
    granular_acceptance_recompile_triggers: bool | None = None,
    operational_validity_failed_trigger_severity: Literal["block", "advisory"] | None = None,
    living_automation_profile: LivingAutomationProfile | None = None,
    project_living_automation_profile: str | None = None,
) -> int:
    """Living systems safe recompile entrypoint.

    - Detect changed ingestion sources (from baseline + current ingest state).
    - Compute impacted IR subgraph using IR provenance.
    - Canary compile only impacted plan steps, then run an eval subset on canary manifests.
    - If canary passes, accept changes by running full compile for impacted steps.
    - Uses a caller-provided LLM backend when supplied, otherwise falls back to
      a deterministic offline backend.

    ``living_automation_profile`` (or env ``AKC_LIVING_AUTOMATION_PROFILE`` / project file) selects
    Phase E defaults: ``living_loop_v1`` / ``living_loop_unattended_v1`` enable granular
    acceptance triggers by default and record time-compression baselines on emitted manifests.
    """

    profile = (
        living_automation_profile
        if living_automation_profile is not None
        else resolve_living_automation_profile(
            cli_value=None,
            env=os.environ,
            project_value=project_living_automation_profile,
        )
    )
    tenant_id = normalize_tenant_id(str(tenant_id))
    repo_id = normalize_repo_id(str(repo_id))
    try:
        outputs_root_p = expanduser_resolve_trusted_invoker(outputs_root)
    except OSError as e:
        raise ValueError("invalid outputs_root") from e
    ingest_state_p = expanduser_resolve_trusted_invoker(ingest_state_path)
    if baseline_path is None:
        baseline_path_p = safe_resolve_scoped_path(
            outputs_root_p,
            tenant_id,
            repo_id,
            ".akc",
            "living",
            "baseline.json",
        )
    else:
        try:
            baseline_path_p = expanduser_resolve_trusted_invoker(baseline_path)
        except OSError as e:
            raise ValueError("invalid baseline_path") from e
        if not baseline_path_p.is_relative_to(outputs_root_p):
            raise ValueError("baseline_path must be under outputs_root")

    standard_base = safe_resolve_scoped_path(outputs_root_p, tenant_id, repo_id)
    memory_db = safe_resolve_scoped_path(standard_base, ".akc", "memory.sqlite")
    memory_db.parent.mkdir(parents=True, exist_ok=True)

    baseline_exists = baseline_path_p.exists()
    baseline_sources_by_key: Mapping[str, Any] | None = None
    baseline_loaded: dict[str, Any] = {}
    if baseline_exists:
        try:
            baseline_loaded = _read_json_object(baseline_path_p, what="baseline")
            maybe_fps = baseline_loaded.get("sources_by_key_fps")
            if isinstance(maybe_fps, dict):
                baseline_sources_by_key = maybe_fps
            else:
                maybe = baseline_loaded.get("sources_by_key")
                baseline_sources_by_key = maybe if isinstance(maybe, dict) else None
        except Exception:
            baseline_sources_by_key = None
            baseline_loaded = {}

    # Load current ingestion state and filter to tenant.
    current_state = _read_json_object(ingest_state_p, what="ingest_state")
    current_tenant_by_key: dict[str, Any] = {}
    tenant_prefix = f"{tenant_id}::"
    for k, v in current_state.items():
        if isinstance(k, str) and k.startswith(tenant_prefix):
            current_tenant_by_key[k] = v

    scope = TenantRepoScope(tenant_id=tenant_id, repo_id=repo_id)
    latest_manifest_path = find_latest_run_manifest(
        outputs_root=outputs_root_p,
        tenant_id=tenant_id,
        repo_id=repo_id,
    )
    living_check_id = "latest"
    if latest_manifest_path is not None:
        try:
            living_check_id = load_run_manifest(
                path=latest_manifest_path,
                expected_tenant_id=tenant_id,
                expected_repo_id=repo_id,
            ).run_id
        except Exception:
            living_check_id = "latest"
    ingest_fp = IngestStateFingerprint(
        tenant_id=tenant_id,
        state_path=str(ingest_state_p),
        sha256=stable_json_fingerprint(current_tenant_by_key),
        keys_included=len(current_tenant_by_key),
    )

    regression_thresholds: dict[str, Any] = {}
    runtime_thresholds_effective: Mapping[str, Any] | None = runtime_canary_thresholds
    granular_triggers_effective = profile.granular_acceptance_default
    if granular_acceptance_recompile_triggers is not None:
        granular_triggers_effective = bool(granular_acceptance_recompile_triggers)
    operational_validity_trigger_severity_effective: Literal["block", "advisory"] = (
        operational_validity_failed_trigger_severity
        if operational_validity_failed_trigger_severity in {"block", "advisory"}
        else profile.operational_validity_failed_trigger_severity_default
    )
    try:
        eval_suite_obj = _read_json_object(
            expanduser_resolve_trusted_invoker(eval_suite_path),
            what="eval_suite",
        )
        maybe = eval_suite_obj.get("regression_thresholds")
        if isinstance(maybe, dict):
            regression_thresholds = maybe
        if runtime_thresholds_effective is None:
            maybe_runtime_thresholds = eval_suite_obj.get("runtime_canary_thresholds")
            if isinstance(maybe_runtime_thresholds, dict):
                runtime_thresholds_effective = maybe_runtime_thresholds
        if granular_acceptance_recompile_triggers is None:
            pol = eval_suite_obj.get("living_recompile_policy")
            if isinstance(pol, dict):
                if "granular_acceptance_triggers" in pol:
                    granular_triggers_effective = bool(pol.get("granular_acceptance_triggers", False))
                sev_raw = str(pol.get("operational_validity_failed_trigger_severity", "")).strip().lower()
                if operational_validity_failed_trigger_severity is None and sev_raw in {"block", "advisory"}:
                    operational_validity_trigger_severity_effective = sev_raw  # type: ignore[assignment]
    except Exception:
        regression_thresholds = {}
        runtime_thresholds_effective = runtime_canary_thresholds

    # Phase 6: intent fingerprint is part of the living recompile contract.
    # Use the acceptance budget because that is what ultimately gets emitted.
    llm_runtime_cfg = getattr(llm_backend, "runtime_config", None) if llm_backend is not None else None
    llm_backend_name = str(getattr(llm_runtime_cfg, "backend", "offline"))
    llm_model_name = str(getattr(llm_runtime_cfg, "model", "")).strip() or None
    accept_config = _mk_compile_config(
        mode=accept_mode,
        test_mode="full",
        policy_mode=policy_mode,
        promotion_mode="live_apply",
        opa_policy_path=opa_policy_path,
        opa_decision_path=opa_decision_path,
        living_automation_profile=profile,
        llm_backend_name=llm_backend_name,
        llm_model=llm_model_name,
    )
    intent_spec = compile_intent_spec(
        tenant_id=tenant_id,
        repo_id=repo_id,
        goal_statement=goal,
        controller_budget=accept_config.budget,
    )
    intent_fingerprint = compute_intent_fingerprint(intent=intent_spec)
    current_stable_intent_sha256 = stable_intent_sha256(intent=intent_spec.normalized())

    # Phase 6 knowledge-layer fingerprints (evidence-aware when `index` is supplied).
    # This uses the same retrieval context shape the controller's knowledge extraction expects.
    session = CompileSession.from_sqlite(
        tenant_id=tenant_id,
        repo_id=repo_id,
        sqlite_path=str(memory_db),
        index=index,
    )
    plan = session.plan(goal=goal)
    session.memory.plan_state.set_active_plan(tenant_id=tenant_id, repo_id=repo_id, plan_id=plan.id)
    loaded_plan = session.memory.plan_state.load_plan(tenant_id=tenant_id, repo_id=repo_id, plan_id=plan.id)
    if loaded_plan is None:
        raise ValueError("active plan missing after planning")

    retrieved_context_for_fp = session.retrieve(plan=plan, limit=20)
    retrieved_documents = retrieved_context_for_fp.get("documents") or []
    doc_id_to_provenance = build_doc_id_to_provenance_map(tenant_id=tenant_id, documents=retrieved_documents)
    knowledge_snapshot = extract_knowledge_snapshot(
        tenant_id=tenant_id,
        repo_id=repo_id,
        intent_spec=intent_spec,
        retrieved_context=retrieved_context_for_fp,
        retrieval_provenance_by_doc_id=doc_id_to_provenance,
        llm=None,
        use_llm=False,
        knowledge_artifact_root=standard_base,
        stored_assertion_index_mode=accept_config.stored_assertion_index_mode,
        stored_assertion_index_max_rows=int(accept_config.stored_assertion_index_max_rows),
        apply_operator_knowledge_decisions=bool(accept_config.apply_operator_knowledge_decisions),
    )
    knowledge_semantic_fp_full = knowledge_semantic_fingerprint(snapshot=knowledge_snapshot)
    knowledge_provenance_fp_full = knowledge_provenance_fingerprint(snapshot=knowledge_snapshot)
    knowledge_semantic_fp_16 = knowledge_semantic_fp_full[:16]
    knowledge_provenance_fp_16 = knowledge_provenance_fp_full[:16]

    # Find latest accepted IR doc to compute an impacted workflow subgraph + map runtime events.
    latest_ir_path = _latest_ir_document_path(outputs_root=outputs_root_p, tenant_id=tenant_id, repo_id=repo_id)
    latest_ir: IRDocument | None = None
    if latest_ir_path is not None:
        latest_ir = IRDocument.from_json_obj(_read_json_object(latest_ir_path, what="ir"))

    runtime_impacts = _collect_runtime_impacts(
        runtime_events=runtime_events,
        ir=latest_ir,
        policy_mode=policy_mode,
        opa_policy_path=opa_policy_path,
        opa_decision_path=opa_decision_path,
        runtime_policy_runtime=runtime_policy_runtime,
    )
    operational_validity_extra = _operational_validity_drift_findings_from_impacts(runtime_impacts)
    operational_validity_failed_for_triggers = bool(operational_validity_extra)
    operational_validity_sc_ids = _collect_operational_validity_success_criterion_ids_from_impacts(runtime_impacts)

    drift = extend_drift_report(
        drift_report(
            scope=scope,
            outputs_root=outputs_root_p,
            ingest_fingerprint=ingest_fp,
            baseline_path=baseline_path_p if baseline_exists else None,
            intent_semantic_fingerprint=intent_fingerprint.semantic,
            intent_goal_text_fingerprint=intent_fingerprint.goal_text,
            knowledge_semantic_fingerprint=knowledge_semantic_fp_16,
            knowledge_provenance_fingerprint=knowledge_provenance_fp_16,
        ),
        operational_validity_extra,
    )
    trigger_records = [
        trigger.to_json_obj()
        for trigger in evaluate_recompile_triggers(
            manifest_intent_semantic_fingerprint=(
                str(baseline_loaded.get("intent_semantic_fingerprint")).strip().lower()
                if baseline_loaded.get("intent_semantic_fingerprint")
                else None
            ),
            current_intent_semantic_fingerprint=intent_fingerprint.semantic,
            manifest_knowledge_semantic_fingerprint=(
                str(baseline_loaded.get("knowledge_semantic_fingerprint")).strip().lower()
                if baseline_loaded.get("knowledge_semantic_fingerprint")
                else None
            ),
            current_knowledge_semantic_fingerprint=knowledge_semantic_fp_16,
            manifest_knowledge_provenance_fingerprint=(
                str(baseline_loaded.get("knowledge_provenance_fingerprint")).strip().lower()
                if baseline_loaded.get("knowledge_provenance_fingerprint")
                else None
            ),
            current_knowledge_provenance_fingerprint=knowledge_provenance_fp_16,
            manifest_stable_intent_sha256=(
                str(baseline_loaded.get("stable_intent_sha256")).strip().lower()
                if baseline_loaded.get("stable_intent_sha256")
                else None
            ),
            current_stable_intent_sha256=current_stable_intent_sha256,
            operational_validity_failed=operational_validity_failed_for_triggers,
            operational_validity_failed_trigger_severity=operational_validity_trigger_severity_effective,
            operational_validity_success_criterion_ids=operational_validity_sc_ids or None,
            enable_granular_acceptance_triggers=granular_triggers_effective,
        )
    ]
    write_drift_artifacts(
        scope=scope,
        outputs_root=outputs_root_p,
        report=drift,
        check_id=living_check_id,
        triggers=trigger_records,
        baseline_path=baseline_path_p if baseline_exists else None,
        source="drift_check",
    )

    # Fast-path: when we have an existing baseline contract and it matches
    # current sources+outputs, no safe action is required.
    if baseline_exists and not drift.has_drift() and not runtime_impacts:
        return 0

    # If baseline_sources_by_key is present we can compute a precise changed
    # source set; otherwise we conservatively recompile everything.
    changed_source_ids: set[str] = set()
    if baseline_sources_by_key is not None:
        changed_source_ids = compute_changed_source_ids(
            tenant_id=tenant_id,
            baseline_sources_by_key=baseline_sources_by_key,
            current_state_by_key=current_tenant_by_key,
        )

    source_plan_step_ids_to_reset: set[str] = set()
    if latest_ir is not None and changed_source_ids:
        source_plan_step_ids_to_reset = compute_impacted_workflow_step_ids(
            ir=latest_ir, changed_source_ids=changed_source_ids
        )
    plan_step_ids_to_reset = set(source_plan_step_ids_to_reset)
    runtime_plan_step_ids = {step_id for impact in runtime_impacts for step_id in impact.impacted_step_ids}
    plan_step_ids_to_reset |= runtime_plan_step_ids

    # Decide how conservative we need to be when mapping changed sources into
    # a recompile subgraph. When provenance mapping is missing/unreliable, we
    # recompile all steps (not just non-done) to restore safe output state.
    sources_drifted = any(f.kind == "changed_sources" for f in drift.findings)
    outputs_drifted = any(f.kind in {"missing_manifest", "changed_outputs"} for f in drift.findings)
    intent_drifted = any(f.kind == "changed_intent" for f in drift.findings)
    knowledge_semantic_drifted = any(f.kind == "changed_knowledge_semantic" for f in drift.findings)
    knowledge_provenance_drifted = any(f.kind == "changed_knowledge_provenance" for f in drift.findings)
    knowledge_drifted = knowledge_semantic_drifted or knowledge_provenance_drifted
    runtime_drifted = bool(runtime_impacts)
    provenance_failed_to_map = bool(changed_source_ids) and latest_ir is not None and not source_plan_step_ids_to_reset
    runtime_failed_to_map = runtime_drifted and not runtime_plan_step_ids

    # If we don't have a baseline, we can't safely narrow to impacted outputs.
    need_all_steps = (
        (not baseline_exists)
        or outputs_drifted
        or provenance_failed_to_map
        or intent_drifted
        or knowledge_drifted
        or runtime_failed_to_map
    )
    no_ir_mapping = sources_drifted and bool(changed_source_ids) and latest_ir is None
    if no_ir_mapping:
        need_all_steps = True
    if sources_drifted and baseline_sources_by_key is None:
        need_all_steps = True
    if sources_drifted and baseline_sources_by_key is not None and not changed_source_ids:
        need_all_steps = True
    if (
        runtime_drifted
        and not _runtime_canary_thresholds_allow(
            impacts=runtime_impacts,
            thresholds=runtime_thresholds_effective,
        )
        and not drift.has_drift()
    ):
        return 0

    if not plan_step_ids_to_reset:
        if need_all_steps:
            plan_step_ids_to_reset = {s.id for s in loaded_plan.steps}
        else:
            # Conservative default when we can map drift but only need to
            # re-run steps that aren't already finalized.
            plan_step_ids_to_reset = {s.id for s in loaded_plan.steps if s.status != "done"}

    if not plan_step_ids_to_reset:
        # Nothing to do: plan is already done for all steps.
        return 0

    _reset_steps_for_recompile(
        plan_store=session.memory.plan_state,
        tenant_id=tenant_id,
        repo_id=repo_id,
        plan_id=plan.id,
        impacted_step_ids=plan_step_ids_to_reset,
        skip_other_pending=skip_other_pending,
    )

    llm: LLMBackend = llm_backend if llm_backend is not None else _OfflineCompileLLM()
    work_root = standard_base
    executor: Executor = SubprocessExecutor(
        work_root=work_root,
        disable_network=not allow_network,
    )

    canary_config = _mk_compile_config(
        mode=canary_mode,
        test_mode=canary_test_mode,
        policy_mode=policy_mode,
        promotion_mode="artifact_only",
        opa_policy_path=opa_policy_path,
        opa_decision_path=opa_decision_path,
        living_automation_profile=profile,
        llm_backend_name=llm_backend_name,
        llm_model=llm_model_name,
    )

    # Canary outputs root must be a top-level outputs root so the compile session can
    # apply its standard tenant/repo scoping underneath it (tests and manifests expect
    # `<outputs_root>/.akc/living/canary/<tenant>/<repo>/...`).
    canary_outputs_root = safe_resolve_scoped_path(outputs_root_p, ".akc", "living", "canary")
    canary_outputs_root.mkdir(parents=True, exist_ok=True)

    # Canary compile: iteratively run compile loop until impacted steps are done.
    while True:
        active = session.memory.plan_state.load_plan(tenant_id=tenant_id, repo_id=repo_id, plan_id=plan.id)
        if active is None:
            raise ValueError("plan disappeared during canary compile")
        remaining = [s.id for s in active.steps if s.id in plan_step_ids_to_reset and s.status != "done"]
        if not remaining:
            break
        cres: ControllerResult = session.run(
            goal=goal,
            llm=llm,
            executor=executor,
            config=canary_config,
            outputs_root=canary_outputs_root,
            replay_mode="live",
        )
        if cres.status != "succeeded":
            return 2

    # Canary eval subset: deterministic checks against the emitted canary manifest.
    canary_manifest_path = find_latest_run_manifest(
        outputs_root=canary_outputs_root,
        tenant_id=tenant_id,
        repo_id=repo_id,
    )
    if canary_manifest_path is None:
        return 2

    canary_manifest_abs = expanduser_resolve_trusted_invoker(canary_manifest_path)
    canary_suite_for_disk = _mk_canary_eval_suite_for_disk(
        tenant_id=tenant_id,
        repo_id=repo_id,
        manifest_path=str(canary_manifest_abs),
        regression_thresholds=regression_thresholds,
    )
    # Persist the suite under a fixed file name within the already-scoped canary directory.
    canary_suite_path = safe_resolve_scoped_path(canary_outputs_root, "canary_eval_suite.json")
    canary_suite_path.parent.mkdir(parents=True, exist_ok=True)
    # Backward compatible: ensure per-tenant living dir exists under canary scope.
    safe_resolve_scoped_path(
        canary_outputs_root,
        tenant_id,
        repo_id,
        ".akc",
        "living",
    ).mkdir(parents=True, exist_ok=True)
    canary_suite_path.write_text(
        json.dumps(canary_suite_for_disk, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    eval_report = run_eval_suite(
        suite_path=canary_suite_path,
        outputs_root=canary_outputs_root,
        baseline_report_path=None,
    )
    if not eval_report.passed:
        return 2

    # Acceptance compile: reset impacted steps and run full compilation (live).
    _reset_steps_for_recompile(
        plan_store=session.memory.plan_state,
        tenant_id=tenant_id,
        repo_id=repo_id,
        plan_id=plan.id,
        impacted_step_ids=plan_step_ids_to_reset,
        skip_other_pending=False,
    )

    while True:
        active2 = session.memory.plan_state.load_plan(tenant_id=tenant_id, repo_id=repo_id, plan_id=plan.id)
        if active2 is None:
            raise ValueError("plan disappeared during acceptance compile")
        remaining2 = [s.id for s in active2.steps if s.id in plan_step_ids_to_reset and s.status != "done"]
        if not remaining2:
            break
        cres2: ControllerResult = session.run(
            goal=goal,
            llm=llm,
            executor=executor,
            config=accept_config,
            outputs_root=outputs_root_p,
            replay_mode="live",
        )
        if cres2.status != "succeeded":
            return 2

    # Acceptance succeeded: update baseline contract after the new outputs exist.
    if update_baseline_on_accept:
        # Prefer the knowledge fingerprints emitted by the latest *acceptance* run manifest.
        # This keeps the baseline contract aligned with how controller knowledge extraction
        # actually ran for the accepted step set.
        latest_manifest_path = find_latest_run_manifest(
            outputs_root=outputs_root_p,
            tenant_id=tenant_id,
            repo_id=repo_id,
        )
        knowledge_sem_fp_final = knowledge_semantic_fp_16
        knowledge_prov_fp_final = knowledge_provenance_fp_16
        if latest_manifest_path is not None:
            try:
                latest_manifest = load_run_manifest(
                    path=latest_manifest_path,
                    expected_tenant_id=tenant_id,
                    expected_repo_id=repo_id,
                )
                if isinstance(latest_manifest.knowledge_semantic_fingerprint, str):
                    knowledge_sem_fp_final = latest_manifest.knowledge_semantic_fingerprint
                if isinstance(latest_manifest.knowledge_provenance_fingerprint, str):
                    knowledge_prov_fp_final = latest_manifest.knowledge_provenance_fingerprint
            except Exception:
                # Best-effort fallback to precomputed fingerprints.
                pass
        write_baseline(
            scope=TenantRepoScope(tenant_id=tenant_id, repo_id=repo_id),
            outputs_root=outputs_root_p,
            ingest_fingerprint=ingest_fp,
            baseline_path=baseline_path_p,
            intent_semantic_fingerprint=intent_fingerprint.semantic,
            intent_goal_text_fingerprint=intent_fingerprint.goal_text,
            knowledge_semantic_fingerprint=knowledge_sem_fp_final,
            knowledge_provenance_fingerprint=knowledge_prov_fp_final,
        )

    return 0
