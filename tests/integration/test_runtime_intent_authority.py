"""Phase C: strict intent reload at runtime and intent-derived evidence expectations."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.artifacts.contracts import schema_id_for
from akc.intent.models import IntentSpecV1, SuccessCriterion, stable_intent_sha256
from akc.intent.policy_projection import project_runtime_intent_projection
from akc.intent.store import JsonFileIntentStore
from akc.runtime.adapters.native import NativeRuntimeAdapter
from akc.runtime.events import RuntimeEventBus
from akc.runtime.kernel import RuntimeKernel
from akc.runtime.models import RuntimeBundle, RuntimeBundleRef, RuntimeContext
from akc.runtime.policy import RuntimePolicyRuntime, derive_runtime_evidence_expectations
from akc.runtime.scheduler import InMemoryRuntimeScheduler
from akc.runtime.state_store import InMemoryRuntimeStateStore
from akc.utils.fingerprint import stable_json_fingerprint


def _minimal_runtime_bundle_payload(
    *,
    intent_projection: dict[str, object],
    intent_ref: dict[str, object],
    policy_envelope: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "schema_id": schema_id_for(kind="runtime_bundle"),
        "run_id": "compile-intent-auth",
        "tenant_id": "tenant-a",
        "repo_id": "repo-a",
        "intent_ref": intent_ref,
        "intent_policy_projection": intent_projection,
        "referenced_ir_nodes": [],
        "referenced_contracts": [],
        "deployment_intents": [],
        "spec_hashes": {
            "orchestration_spec_sha256": "a" * 64,
            "coordination_spec_sha256": "b" * 64,
        },
        "runtime_policy_envelope": dict(policy_envelope or {}),
    }


def test_strict_load_rejects_tampered_intent_policy_projection(tmp_path: Path) -> None:
    repo = tmp_path / "tenant-a" / "repo-a"
    intent_dir = repo / ".akc" / "intent" / "tenant-a" / "repo-a"
    bundle_dir = repo / ".akc" / "runtime"
    intent_dir.mkdir(parents=True)
    bundle_dir.mkdir(parents=True)

    intent = IntentSpecV1(
        intent_id="intent-strict-1",
        tenant_id="tenant-a",
        repo_id="repo-a",
        goal_statement="Strict runtime authority",
        success_criteria=(
            SuccessCriterion(
                id="sc_metric",
                evaluation_mode="metric_threshold",
                description="Latency SLO",
            ),
        ),
    )
    store = JsonFileIntentStore(base_dir=repo)
    store.save_intent(tenant_id="tenant-a", repo_id="repo-a", intent=intent)
    sha = stable_intent_sha256(intent=intent.normalized())
    canonical = project_runtime_intent_projection(intent=intent).to_json_obj()
    tampered = json.loads(json.dumps(canonical))
    tampered.setdefault("success_criteria_summary", {})
    if isinstance(tampered["success_criteria_summary"], dict):
        tampered["success_criteria_summary"]["count"] = 999

    payload = _minimal_runtime_bundle_payload(
        intent_projection=tampered,
        intent_ref={
            "intent_id": "intent-strict-1",
            "stable_intent_sha256": sha,
            "semantic_fingerprint": "1" * 16,
            "goal_text_fingerprint": "2" * 16,
        },
        policy_envelope={"intent_authority_strict": True},
    )
    bundle_path = bundle_dir / "bundle.json"
    bundle_path.write_text(json.dumps(payload), encoding="utf-8")
    bundle_ref = RuntimeBundleRef(
        bundle_path=str(bundle_path),
        manifest_hash=stable_json_fingerprint(payload),
        created_at=1,
        source_compile_run_id="compile-intent-auth",
    )
    ctx = RuntimeContext(
        tenant_id="tenant-a",
        repo_id="repo-a",
        run_id="compile-intent-auth",
        runtime_run_id="runtime-1",
        policy_mode="enforce",
        adapter_id="native",
    )
    kernel = RuntimeKernel(
        context=ctx,
        bundle=RuntimeBundle(
            context=ctx,
            ref=bundle_ref,
            nodes=(),
            contract_ids=(),
            policy_envelope={},
            metadata={},
        ),
        adapter=NativeRuntimeAdapter(),
        scheduler=InMemoryRuntimeScheduler(),
        state_store=InMemoryRuntimeStateStore(),
        event_bus=RuntimeEventBus(),
        policy_runtime=RuntimePolicyRuntime.default(context=ctx),
    )
    with pytest.raises(ValueError, match="intent_policy_projection does not match"):
        kernel.load_bundle(bundle_ref, strict_intent_authority=True)


def test_derive_runtime_evidence_expectations_matches_intent_metric_mode() -> None:
    intent = IntentSpecV1(
        intent_id="intent-exp",
        tenant_id="tenant-a",
        repo_id="repo-a",
        goal_statement="Expect metric evidence",
        success_criteria=(
            SuccessCriterion(
                id="m1",
                evaluation_mode="metric_threshold",
                description="p99",
            ),
        ),
    )
    proj = project_runtime_intent_projection(intent=intent).to_json_obj()
    got = derive_runtime_evidence_expectations(projection=proj, policy_envelope={})
    assert got == ("metric_threshold", "reconciler.health_check")
