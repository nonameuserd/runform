"""Unit tests for IR → runtime knowledge envelope projection (Phase 3)."""

from __future__ import annotations

from akc.ir.schema import IRDocument, IRNode
from akc.knowledge.runtime_projection import knowledge_runtime_envelope_from_ir
from akc.runtime.models import RuntimeContext
from akc.runtime.policy import RuntimePolicyRuntime


def test_knowledge_runtime_envelope_pii_denies_subprocess_not_network() -> None:
    """Non-network hard constraint (PII) maps to subprocess denial with explanation."""

    ir = IRDocument(
        tenant_id="t1",
        repo_id="r1",
        nodes=(
            IRNode(
                id="kc1",
                tenant_id="t1",
                kind="entity",
                name="knowledge_constraint:assertion_x",
                properties={
                    "assertion_id": "assertion_x",
                    "kind": "hard",
                    "predicate": "forbidden",
                    "summary": "Do not log or export customer PII outside the tenant boundary.",
                    "subject": "data",
                },
            ),
        ),
    )
    env = knowledge_runtime_envelope_from_ir(ir)
    assert env.get("knowledge_network_egress_forbidden") is False
    assert "runtime.action.execute.subprocess" in (env.get("knowledge_derived_deny_actions") or [])
    expl = env.get("knowledge_explanations") or {}
    assert "assertion_x" in expl
    assert "PII" in expl["assertion_x"] or "pii" in expl["assertion_x"].lower()

    ctx = RuntimeContext(
        tenant_id="t1",
        repo_id="r1",
        run_id="run-1",
        runtime_run_id="rr-1",
        policy_mode="enforce",
        adapter_id="native",
    )
    rt = RuntimePolicyRuntime.from_bundle(
        context=ctx,
        policy_envelope=dict(env),
        intent_projection={},
        ir_document=ir,
    )
    assert "runtime.action.execute.subprocess" in rt.effective_deny_actions
    sub_auth = rt.authorize(context=ctx, action="runtime.action.execute.subprocess")
    assert sub_auth.allowed is False


def test_knowledge_runtime_envelope_network_still_tightens_reconcile() -> None:
    ir = IRDocument(
        tenant_id="t1",
        repo_id="r1",
        nodes=(
            IRNode(
                id="kc1",
                tenant_id="t1",
                kind="entity",
                name="knowledge_constraint:assertion_n",
                properties={
                    "assertion_id": "assertion_n",
                    "kind": "hard",
                    "predicate": "forbidden",
                    "summary": "No outbound HTTPS to the public internet.",
                    "subject": "network",
                },
            ),
        ),
    )
    env = knowledge_runtime_envelope_from_ir(ir)
    assert env.get("knowledge_network_egress_forbidden") is True
    assert "service.reconcile.apply" in (env.get("knowledge_derived_deny_actions") or [])
    assert "runtime.action.execute.http" in (env.get("knowledge_derived_deny_actions") or [])

    ctx = RuntimeContext(
        tenant_id="t1",
        repo_id="r1",
        run_id="run-1",
        runtime_run_id="rr-1",
        policy_mode="enforce",
        adapter_id="native",
    )
    rt = RuntimePolicyRuntime.from_bundle(
        context=ctx,
        policy_envelope=dict(env),
        intent_projection={},
        ir_document=ir,
    )
    http_auth = rt.authorize(context=ctx, action="runtime.action.execute.http")
    assert http_auth.allowed is False
