from __future__ import annotations

import json

import pytest

from akc.control_bot.approval_workflow import (
    ApprovalWorkflow,
    InMemoryApprovalStore,
    SqliteApprovalStore,
    stable_args_fingerprint,
)
from akc.control_bot.audit import ControlBotAuditWriter
from akc.control_bot.command_engine import (
    ActionRegistry,
    CommandClarificationRequired,
    CommandContext,
    CommandEngine,
    CommandResult,
    InboundEvent,
    PolicyDenied,
    Principal,
)
from akc.control_bot.command_result_store import InMemoryCommandResultStore
from akc.control_bot.outbound_response_adapters import TextOutboundAdapter
from akc.control_bot.policy_gate import PolicyGate, build_role_allowlist
from akc.control_bot.server import process_inbound_event


def _allow_status_policy() -> callable:
    gate = PolicyGate(
        mode="enforce",
        role_allowlist=build_role_allowlist({"viewer": ["status.*"]}),
        opa=None,
    )

    def _decide(ctx: CommandContext, cmd: object) -> object:
        # cmd is Command, but keep signature flexible in tests
        return gate.decide(ctx=ctx, cmd=cmd)  # type: ignore[arg-type]

    return _decide


def _allow_all_policy() -> callable:
    gate = PolicyGate(
        mode="enforce",
        role_allowlist=build_role_allowlist({"operator": ["status.*", "approval.*", "incident.*", "mutate.*"]}),
        opa=None,
    )

    def _decide(ctx: CommandContext, cmd: object) -> object:
        return gate.decide(ctx=ctx, cmd=cmd)  # type: ignore[arg-type]

    return _decide


def test_engine_strict_parse_and_execute_status_runtime() -> None:
    eng = CommandEngine(registry=ActionRegistry.default_v1(), policy_decide=_allow_status_policy())
    ev = InboundEvent(
        channel="slack",
        event_id="e1",
        principal_id="u1",
        tenant_id="t1",
        raw_text="akc status runtime",
        payload_hash="abc",
        received_at_ms=1,
    )
    ctx = CommandContext(event=ev, principal=Principal(principal_id="u1", tenant_id="t1", roles=("viewer",)), now_ms=1)
    cmd = eng.parse("akc status runtime")
    assert cmd.action_id == "status.runtime"
    res = eng.execute(ctx=ctx, cmd=cmd)
    assert res.ok is True
    assert res.action_id == "status.runtime"


def test_engine_policy_default_deny_non_status() -> None:
    eng = CommandEngine(registry=ActionRegistry.default_v1(), policy_decide=_allow_status_policy())
    ev = InboundEvent(
        channel="slack",
        event_id="e1",
        principal_id="u1",
        tenant_id="t1",
        raw_text="akc mutate runtime stop",
        payload_hash="abc",
        received_at_ms=1,
    )
    ctx = CommandContext(event=ev, principal=Principal(principal_id="u1", tenant_id="t1", roles=("viewer",)), now_ms=1)
    cmd = eng.parse("akc mutate runtime stop")
    assert cmd.action_id == "mutate.runtime.stop"
    with pytest.raises(PolicyDenied):
        eng.execute(ctx=ctx, cmd=cmd)


def test_engine_strict_parse_multi_segment_action_and_kv_args() -> None:
    eng = CommandEngine(registry=ActionRegistry.default_v1(), policy_decide=_allow_status_policy())
    cmd = eng.parse("akc status runs list limit=5")
    assert cmd.action_id == "status.runs.list"
    assert cmd.args["limit"] == 5


def test_engine_strict_parse_long_flag_args() -> None:
    eng = CommandEngine(registry=ActionRegistry.default_v1(), policy_decide=_allow_status_policy())
    cmd = eng.parse("akc status runs list --limit 7")
    assert cmd.action_id == "status.runs.list"
    assert cmd.args["limit"] == 7


def test_engine_strict_parse_rejects_positional_after_args_begin() -> None:
    eng = CommandEngine(registry=ActionRegistry.default_v1(), policy_decide=_allow_status_policy())
    with pytest.raises(Exception, match="invalid strict command syntax"):
        eng.parse("akc status runs list --limit 7 extra")


def test_engine_nl_fallback_maps_runs_list_with_limit_extraction() -> None:
    eng = CommandEngine(registry=ActionRegistry.default_v1(), policy_decide=_allow_status_policy())
    cmd = eng.parse("list runs top 5")
    assert cmd.parser == "nl_fallback"
    assert cmd.action_id == "status.runs.list"
    assert cmd.args["limit"] == 5


def test_engine_nl_fallback_ambiguous_returns_clarification_prompt() -> None:
    eng = CommandEngine(registry=ActionRegistry.default_v1(), policy_decide=_allow_status_policy())
    with pytest.raises(CommandClarificationRequired) as ei:
        eng.parse("status list")
    err = ei.value
    assert "Ambiguous command" in err.message
    assert set(err.candidates) == {"status.runtime", "status.runs.list"}


def test_engine_rejects_tenant_mismatch() -> None:
    eng = CommandEngine(registry=ActionRegistry.default_v1(), policy_decide=_allow_status_policy())
    ev = InboundEvent(
        channel="slack",
        event_id="e1",
        principal_id="u1",
        tenant_id="t1",
        raw_text="akc status runtime",
        payload_hash="abc",
        received_at_ms=1,
    )
    ctx = CommandContext(event=ev, principal=Principal(principal_id="u1", tenant_id="t2", roles=()), now_ms=1)
    cmd = eng.parse("akc status runtime")
    with pytest.raises(ValueError, match="tenant isolation violated"):
        eng.execute(ctx=ctx, cmd=cmd)


def test_approval_workflow_self_approval_forbidden() -> None:
    store = InMemoryApprovalStore()
    wf = ApprovalWorkflow(store=store)
    req = wf.create_request(
        tenant_id="t1",
        action_id="mutate.runtime.stop",
        args_hash=stable_args_fingerprint({"a": "b"}),
        args={"a": "b"},
        requester_principal_id="u1",
        now_ms=1,
        ttl_ms=60_000,
    )
    with pytest.raises(Exception, match="self-approval"):
        wf.resolve(
            tenant_id="t1",
            request_id=req.request_id,
            resolver_principal_id="u1",
            decision="approve",
            now_ms=2,
        )


def test_sqlite_approval_resolve_is_atomic_and_execution_claim_is_once(tmp_path) -> None:
    store = SqliteApprovalStore(sqlite_path=tmp_path / "approvals.sqlite")
    wf = ApprovalWorkflow(store=store)
    req = wf.create_request(
        tenant_id="t1",
        action_id="mutate.runtime.stop",
        args_hash=stable_args_fingerprint({"a": "b"}),
        args={"a": "b"},
        requester_principal_id="requester",
        now_ms=10,
        ttl_ms=60_000,
    )

    # First resolver wins.
    r1 = wf.resolve(
        tenant_id="t1",
        request_id=req.request_id,
        resolver_principal_id="approver-1",
        decision="approve",
        now_ms=20,
    )
    assert r1.status == "approved"
    assert r1.resolved_by_principal_id == "approver-1"

    # Second resolver cannot overwrite atomically.
    r2 = wf.resolve(
        tenant_id="t1",
        request_id=req.request_id,
        resolver_principal_id="approver-2",
        decision="deny",
        now_ms=21,
    )
    assert r2.status == "approved"
    assert r2.resolved_by_principal_id == "approver-1"

    # Claim execution exactly once.
    assert wf.claim_execution(tenant_id="t1", request_id=req.request_id) is True
    assert wf.claim_execution(tenant_id="t1", request_id=req.request_id) is False


def test_process_inbound_event_persists_even_when_principal_unknown() -> None:
    eng = CommandEngine(registry=ActionRegistry.default_v1(), policy_decide=_allow_status_policy())
    ev = InboundEvent(
        channel="slack",
        event_id="e1",
        principal_id="u1",
        tenant_id="t1",
        raw_text="akc status runtime",
        payload_hash="abc",
        received_at_ms=1,
    )

    seen: list[InboundEvent] = []

    class _Store:
        def persist(self, ev: InboundEvent) -> object:
            seen.append(ev)
            return type("_Persist", (), {"is_duplicate": False, "reason": "new"})()

    with pytest.raises(PolicyDenied, match="unknown principal_id"):
        process_inbound_event(
            event=ev,
            principals={},
            allowed_tenants={"t1"},
            engine=eng,
            approvals=ApprovalWorkflow(store=InMemoryApprovalStore()),
            outbound=TextOutboundAdapter(channel="slack"),
            event_store=_Store(),
            result_store=InMemoryCommandResultStore(),
            now_ms=1,
        )

    assert len(seen) == 1
    assert seen[0].event_id == "e1"
    assert seen[0].tenant_id == "t1"


def test_process_inbound_event_persists_before_execution() -> None:
    persisted = {"ok": False}

    def _handler(_ctx: CommandContext, _args: object) -> CommandResult:
        assert persisted["ok"] is True
        return CommandResult(ok=True, action_id="status.runtime", message="ok")

    reg = ActionRegistry(handlers={"status.runtime": _handler})
    eng = CommandEngine(registry=reg, policy_decide=_allow_status_policy())

    ev = InboundEvent(
        channel="slack",
        event_id="e2",
        principal_id="u1",
        tenant_id="t1",
        raw_text="akc status runtime",
        payload_hash="abc",
        received_at_ms=1,
    )

    class _Ident:
        tenant_id = "t1"
        roles = ("viewer",)

    class _Store:
        def persist(self, _ev: InboundEvent) -> object:
            persisted["ok"] = True
            return type("_Persist", (), {"is_duplicate": False, "reason": "new"})()

    process_inbound_event(
        event=ev,
        principals={"u1": _Ident()},
        allowed_tenants={"t1"},
        engine=eng,
        approvals=ApprovalWorkflow(store=InMemoryApprovalStore()),
        outbound=TextOutboundAdapter(channel="slack"),
        event_store=_Store(),
        result_store=InMemoryCommandResultStore(),
        now_ms=1,
    )


def test_process_inbound_event_returns_approval_required_message() -> None:
    eng = CommandEngine(registry=ActionRegistry.default_v1(), policy_decide=_allow_status_policy())
    ev = InboundEvent(
        channel="slack",
        event_id="e3",
        principal_id="u1",
        tenant_id="t1",
        raw_text="akc mutate runtime stop",
        payload_hash="abc",
        received_at_ms=1,
    )

    class _Ident:
        tenant_id = "t1"
        roles = ("operator",)

    class _Store:
        def persist(self, _ev: InboundEvent) -> object:
            return type("_Persist", (), {"is_duplicate": False, "reason": "new"})()

    approvals_store = InMemoryApprovalStore()
    wf = ApprovalWorkflow(store=approvals_store)
    msg = process_inbound_event(
        event=ev,
        principals={"u1": _Ident()},
        allowed_tenants={"t1"},
        engine=eng,
        approvals=wf,
        outbound=TextOutboundAdapter(channel="slack"),
        event_store=_Store(),
        result_store=InMemoryCommandResultStore(),
        now_ms=1,
    )
    assert msg is not None
    assert "Approval required" in msg.text
    pending = list(approvals_store.list_pending(tenant_id="t1"))
    assert len(pending) == 1
    assert pending[0].status == "pending"
    assert pending[0].action_id == "mutate.runtime.stop"
    assert pending[0].ttl_ms >= 1000


def test_command_execution_records_persist_approval_required_outcome_and_replay() -> None:
    eng = CommandEngine(registry=ActionRegistry.default_v1(), policy_decide=_allow_all_policy())
    ev = InboundEvent(
        channel="slack",
        event_id="e-approval-1",
        principal_id="u1",
        tenant_id="t1",
        raw_text="akc mutate runtime stop",
        payload_hash="abc",
        received_at_ms=1,
    )

    class _Ident:
        tenant_id = "t1"
        roles = ("operator",)

    class _Store:
        def persist(self, _ev: InboundEvent) -> object:
            return type("_Persist", (), {"is_duplicate": False, "reason": "new"})()

    rs = InMemoryCommandResultStore()
    wf = ApprovalWorkflow(store=InMemoryApprovalStore())
    m1 = process_inbound_event(
        event=ev,
        principals={"u1": _Ident()},
        allowed_tenants={"t1"},
        engine=eng,
        approvals=wf,
        outbound=TextOutboundAdapter(channel="slack"),
        event_store=_Store(),
        result_store=rs,
        now_ms=10,
    )
    assert m1 is not None
    assert "Approval required" in m1.text

    # Same inbound event should not execute again; if it makes it past event dedupe,
    # the finished command result should be replayable by request hash.
    from akc.control_bot.command_result_store import stable_command_request_hash

    request_hash = stable_command_request_hash(ev=ev, action_id="mutate.runtime.stop", args={})
    replay = rs.get_finished(request_hash=request_hash)
    assert replay is not None
    assert replay.status == "approval_required"


def test_command_execution_records_persist_policy_denial_outcome() -> None:
    # Policy denies status.runtime; process should return a deterministic denied result and finish the record.
    def _deny_all(_ctx: CommandContext, _cmd: object) -> object:
        from akc.control_bot.command_engine import PolicyDecision

        return PolicyDecision(allowed=False, reason="test deny")

    eng = CommandEngine(registry=ActionRegistry.default_v1(), policy_decide=_deny_all)
    ev = InboundEvent(
        channel="slack",
        event_id="e-deny-1",
        principal_id="u1",
        tenant_id="t1",
        raw_text="akc status runtime",
        payload_hash="abc",
        received_at_ms=1,
    )

    class _Ident:
        tenant_id = "t1"
        roles = ("viewer",)

    class _Store:
        def persist(self, _ev: InboundEvent) -> object:
            return type("_Persist", (), {"is_duplicate": False, "reason": "new"})()

    rs = InMemoryCommandResultStore()
    wf = ApprovalWorkflow(store=InMemoryApprovalStore())
    msg = process_inbound_event(
        event=ev,
        principals={"u1": _Ident()},
        allowed_tenants={"t1"},
        engine=eng,
        approvals=wf,
        outbound=TextOutboundAdapter(channel="slack"),
        event_store=_Store(),
        result_store=rs,
        now_ms=10,
    )
    assert msg is not None
    assert "policy denied" in msg.text.lower()

    from akc.control_bot.command_result_store import stable_command_request_hash

    request_hash = stable_command_request_hash(ev=ev, action_id="status.runtime", args={})
    replay = rs.get_finished(request_hash=request_hash)
    assert replay is not None
    assert replay.status == "denied"


def test_default_approval_rule_incident_and_mutate_require_approval_status_does_not() -> None:
    store = InMemoryApprovalStore()
    wf = ApprovalWorkflow(store=store)
    assert wf.requires_approval("incident.playbook.run") is True
    assert wf.requires_approval("mutate.runtime.stop") is True
    assert wf.requires_approval("status.runtime") is False


def test_process_inbound_event_status_does_not_create_approval_request() -> None:
    eng = CommandEngine(registry=ActionRegistry.default_v1(), policy_decide=_allow_status_policy())
    ev = InboundEvent(
        channel="slack",
        event_id="e4",
        principal_id="u1",
        tenant_id="t1",
        raw_text="akc status runtime",
        payload_hash="abc",
        received_at_ms=1,
    )

    class _Ident:
        tenant_id = "t1"
        roles = ("viewer",)

    class _Store:
        def persist(self, _ev: InboundEvent) -> object:
            return type("_Persist", (), {"is_duplicate": False, "reason": "new"})()

    approvals_store = InMemoryApprovalStore()
    wf = ApprovalWorkflow(store=approvals_store)
    msg = process_inbound_event(
        event=ev,
        principals={"u1": _Ident()},
        allowed_tenants={"t1"},
        engine=eng,
        approvals=wf,
        outbound=TextOutboundAdapter(channel="slack"),
        event_store=_Store(),
        result_store=InMemoryCommandResultStore(),
        now_ms=1,
    )
    assert msg is not None
    assert "runtime ok" in msg.text
    pending = list(approvals_store.list_pending(tenant_id="t1"))
    assert pending == []


def test_policy_gate_role_allowlist_default_deny() -> None:
    # No allowlist configured => default deny all.
    gate = PolicyGate(mode="enforce", role_allowlist=build_role_allowlist({}), opa=None)
    ev = InboundEvent(
        channel="slack",
        event_id="e1",
        principal_id="u1",
        tenant_id="t1",
        raw_text="akc status runtime",
        payload_hash="abc",
        received_at_ms=1,
    )
    ctx = CommandContext(event=ev, principal=Principal(principal_id="u1", tenant_id="t1", roles=("viewer",)), now_ms=1)
    cmd = CommandEngine(registry=ActionRegistry.default_v1()).parse("akc status runtime")
    dec = gate.decide(ctx=ctx, cmd=cmd)
    assert dec.allowed is False


def test_policy_gate_opa_denies_even_if_allowlisted(monkeypatch: pytest.MonkeyPatch) -> None:
    # If OPA is enabled, allowlist is necessary but not sufficient.
    from akc.control_bot.policy_gate import OPAClient, OPAConfig

    def _fake_urlopen(req: object, timeout: float) -> object:
        class _Resp:
            def __enter__(self) -> _Resp:
                return self

            def __exit__(self, *args: object) -> None:
                return None

            def read(self) -> bytes:
                return b'{"data":{"akc":{"allow":false}}}'

        return _Resp()

    monkeypatch.setattr("urllib.request.urlopen", _fake_urlopen)
    gate = PolicyGate(
        mode="enforce",
        role_allowlist=build_role_allowlist({"viewer": ["status.*"]}),
        opa=OPAClient(
            cfg=OPAConfig(url="http://opa/v1/data/akc/allow", decision_path="data.akc.allow", timeout_ms=200)
        ),
    )
    ev = InboundEvent(
        channel="slack",
        event_id="e1",
        principal_id="u1",
        tenant_id="t1",
        raw_text="akc status runtime",
        payload_hash="abc",
        received_at_ms=1,
    )
    ctx = CommandContext(event=ev, principal=Principal(principal_id="u1", tenant_id="t1", roles=("viewer",)), now_ms=1)
    cmd = CommandEngine(registry=ActionRegistry.default_v1()).parse("akc status runtime")
    dec = gate.decide(ctx=ctx, cmd=cmd)
    assert dec.allowed is False


def test_idempotency_duplicate_inbound_event_does_not_execute_twice() -> None:
    calls = {"n": 0}

    def _handler(ctx: CommandContext, _args: object) -> CommandResult:
        calls["n"] += 1
        return CommandResult(ok=True, action_id="status.runtime", message=f"ok:{calls['n']}:{ctx.event.event_id}")

    reg = ActionRegistry(handlers={"status.runtime": _handler})
    eng = CommandEngine(registry=reg, policy_decide=_allow_status_policy())

    ev = InboundEvent(
        channel="slack",
        event_id="dup-1",
        principal_id="u1",
        tenant_id="t1",
        raw_text="akc status runtime",
        payload_hash="abc",
        received_at_ms=1,
    )

    class _Ident:
        tenant_id = "t1"
        roles = ("viewer",)

    class _Store:
        def persist(self, _ev: InboundEvent) -> object:
            return type("_Persist", (), {"is_duplicate": False, "reason": "new"})()

    rs = InMemoryCommandResultStore()
    m1 = process_inbound_event(
        event=ev,
        principals={"u1": _Ident()},
        allowed_tenants={"t1"},
        engine=eng,
        approvals=ApprovalWorkflow(store=InMemoryApprovalStore()),
        outbound=TextOutboundAdapter(channel="slack"),
        event_store=_Store(),
        result_store=rs,
        now_ms=10,
    )
    m2 = process_inbound_event(
        event=ev,
        principals={"u1": _Ident()},
        allowed_tenants={"t1"},
        engine=eng,
        approvals=ApprovalWorkflow(store=InMemoryApprovalStore()),
        outbound=TextOutboundAdapter(channel="slack"),
        event_store=_Store(),
        result_store=rs,
        now_ms=11,
    )
    assert calls["n"] == 1
    assert m1 is not None
    assert m2 is None or m2.text == m1.text


def test_idempotency_rejects_event_id_reuse_with_different_payload_hash() -> None:
    eng = CommandEngine(registry=ActionRegistry.default_v1(), policy_decide=_allow_status_policy())

    class _Ident:
        tenant_id = "t1"
        roles = ("viewer",)

    class _Store:
        def persist(self, _ev: InboundEvent) -> object:
            return type("_Persist", (), {"is_duplicate": False, "reason": "new"})()

    rs = InMemoryCommandResultStore()
    ev1 = InboundEvent(
        channel="slack",
        event_id="e-reuse",
        principal_id="u1",
        tenant_id="t1",
        raw_text="akc status runtime",
        payload_hash="abc",
        received_at_ms=1,
    )
    ev2 = InboundEvent(
        channel="slack",
        event_id="e-reuse",
        principal_id="u1",
        tenant_id="t1",
        raw_text="akc status runtime",
        payload_hash="def",
        received_at_ms=2,
    )
    _ = process_inbound_event(
        event=ev1,
        principals={"u1": _Ident()},
        allowed_tenants={"t1"},
        engine=eng,
        approvals=ApprovalWorkflow(store=InMemoryApprovalStore()),
        outbound=TextOutboundAdapter(channel="slack"),
        event_store=_Store(),
        result_store=rs,
        now_ms=10,
    )
    with pytest.raises(Exception, match="payload_hash"):
        _ = process_inbound_event(
            event=ev2,
            principals={"u1": _Ident()},
            allowed_tenants={"t1"},
            engine=eng,
            approvals=ApprovalWorkflow(store=InMemoryApprovalStore()),
            outbound=TextOutboundAdapter(channel="slack"),
            event_store=_Store(),
            result_store=rs,
            now_ms=11,
        )


def test_idempotency_duplicate_payload_hash_with_different_event_id_does_not_execute_twice() -> None:
    calls = {"n": 0}

    def _handler(ctx: CommandContext, _args: object) -> CommandResult:
        calls["n"] += 1
        return CommandResult(ok=True, action_id="status.runtime", message=f"ok:{calls['n']}:{ctx.event.event_id}")

    reg = ActionRegistry(handlers={"status.runtime": _handler})
    eng = CommandEngine(registry=reg, policy_decide=_allow_status_policy())

    class _Ident:
        tenant_id = "t1"
        roles = ("viewer",)

    # Simulate a store that dedupes by payload_hash across event_ids.
    class _Store:
        seen: set[tuple[str, str, str]] = set()

        def persist(self, ev: InboundEvent) -> object:
            key = (ev.tenant_id.strip(), str(ev.channel), ev.payload_hash.strip().lower())
            dup = key in self.seen
            if not dup:
                self.seen.add(key)
            return type("_Persist", (), {"is_duplicate": dup, "reason": "duplicate.payload_hash" if dup else "new"})()

    store = _Store()
    rs = InMemoryCommandResultStore()
    ev1 = InboundEvent(
        channel="slack",
        event_id="pdup-1",
        principal_id="u1",
        tenant_id="t1",
        raw_text="akc status runtime",
        payload_hash="same",
        received_at_ms=1,
    )
    ev2 = InboundEvent(
        channel="slack",
        event_id="pdup-2",
        principal_id="u1",
        tenant_id="t1",
        raw_text="akc status runtime",
        payload_hash="same",
        received_at_ms=2,
    )

    m1 = process_inbound_event(
        event=ev1,
        principals={"u1": _Ident()},
        allowed_tenants={"t1"},
        engine=eng,
        approvals=ApprovalWorkflow(store=InMemoryApprovalStore()),
        outbound=TextOutboundAdapter(channel="slack"),
        event_store=store,
        result_store=rs,
        now_ms=10,
    )
    m2 = process_inbound_event(
        event=ev2,
        principals={"u1": _Ident()},
        allowed_tenants={"t1"},
        engine=eng,
        approvals=ApprovalWorkflow(store=InMemoryApprovalStore()),
        outbound=TextOutboundAdapter(channel="slack"),
        event_store=store,
        result_store=rs,
        now_ms=11,
    )
    assert calls["n"] == 1
    assert m1 is not None
    assert m2 is None


def test_process_inbound_event_writes_structured_audit_events(tmp_path) -> None:
    eng = CommandEngine(registry=ActionRegistry.default_v1(), policy_decide=_allow_status_policy())
    ev = InboundEvent(
        channel="slack",
        event_id="audit-1",
        principal_id="u1",
        tenant_id="t1",
        raw_text="akc status runtime",
        payload_hash="abc",
        received_at_ms=1,
    )

    class _Ident:
        tenant_id = "t1"
        roles = ("viewer",)

    class _Store:
        def persist(self, _ev: InboundEvent) -> object:
            return type("_Persist", (), {"is_duplicate": False, "reason": "new"})()

    audit_path = tmp_path / "control_bot.audit.jsonl"
    writer = ControlBotAuditWriter(audit_log_path=audit_path)
    msg = process_inbound_event(
        event=ev,
        principals={"u1": _Ident()},
        allowed_tenants={"t1"},
        engine=eng,
        approvals=ApprovalWorkflow(store=InMemoryApprovalStore()),
        outbound=TextOutboundAdapter(channel="slack"),
        event_store=_Store(),
        result_store=InMemoryCommandResultStore(),
        now_ms=10,
        audit_writer=writer,
    )
    assert msg is not None
    rows = [json.loads(line) for line in audit_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    types = [str(r.get("event_type")) for r in rows]
    assert "control.bot.command.received" in types
    assert "control.bot.command.executed" in types
