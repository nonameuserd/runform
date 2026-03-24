"""Compile IR → runtime handoff fields for knowledge-backed enforcement (C3)."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from akc.ir.schema import IRDocument

# Mirror ``akc.runtime.policy.RUNTIME_POLICY_ACTIONS`` without importing runtime (avoid cycles
# with compile ↔ artifact_consistency ↔ this module).
_RUNTIME_ACTION_ALLOWLIST: frozenset[str] = frozenset(
    (
        "runtime.event.consume",
        "runtime.action.dispatch",
        "runtime.action.retry",
        "runtime.action.execute.subprocess",
        "runtime.action.execute.http",
        "runtime.state.checkpoint.write",
        "service.reconcile.apply",
        "service.reconcile.rollback",
    )
)

_NETWORK_HINTS = re.compile(r"(?i)\b(network|egress|internet|outbound|http|https|external\s+api|public\s+internet)\b")
_SECRETS_PII_HINTS = re.compile(
    r"(?i)\b(pii|personally\s+identifiable|secret|secrets?|credential|password|api\s*key|auth\s+token|ssn|token\s+leak)\b"
)
_DESTRUCTIVE_HINTS = re.compile(r"(?i)\b(delete|destroy|drop|purge|wipe|remove\s+all|rm\s+-rf|destructive)\b")


def knowledge_runtime_envelope_from_ir(ir_document: IRDocument) -> dict[str, Any]:
    """Derive runtime_policy_envelope knowledge fields from IR knowledge entity nodes.

    Tenant isolation: only reads node payloads already scoped to ``ir_document``; no
    cross-tenant fetches.

    Emits structured ``knowledge_policy_rules`` and ``knowledge_explanations`` (keyed by
    ``assertion_id``) plus ``knowledge_derived_deny_actions`` aligned with
    :mod:`akc.runtime.policy` default-deny semantics.
    """

    constraints: list[Mapping[str, Any]] = []
    decisions_by_aid: dict[str, Mapping[str, Any]] = {}

    for node in ir_document.nodes:
        name = str(node.name or "")
        props = node.properties or {}
        if not isinstance(props, Mapping):
            continue
        if node.kind == "entity" and name.startswith("knowledge_constraint:"):
            constraints.append(props)
        elif node.kind == "entity" and name.startswith("knowledge_decision:"):
            aid = str(props.get("assertion_id", "")).strip()
            if aid:
                decisions_by_aid[aid] = props

    hard_selected: list[dict[str, Any]] = []
    network_forbidden = False
    policy_rules: dict[str, dict[str, Any]] = {}
    explanations: dict[str, str] = {}
    derived_deny: set[str] = set()

    for c in constraints:
        aid = str(c.get("assertion_id", "")).strip()
        if not aid:
            continue
        dec = decisions_by_aid.get(aid)
        selected = True if dec is None else bool(dec.get("selected", False))
        if not selected:
            continue
        kind = str(c.get("kind", "hard")).strip().lower()
        pred = str(c.get("predicate", "")).strip().lower()
        summary = str(c.get("summary", "")).strip()
        hard_selected.append(
            {
                "assertion_id": aid,
                "predicate": pred,
                "kind": kind,
                "summary": summary,
                "selected": True,
            }
        )
        if kind != "hard":
            continue
        if pred not in {"forbidden", "must_not_use"}:
            continue

        rule_classes: list[str] = []
        deny_for_rule: set[str] = set()
        expl_parts: list[str] = []

        if _NETWORK_HINTS.search(summary):
            network_forbidden = True
            rule_classes.append("network_egress")
            deny_for_rule.update(
                {
                    "service.reconcile.apply",
                    "service.reconcile.rollback",
                    "runtime.action.execute.http",
                }
            )
            expl_parts.append("Knowledge constraint forbids network egress; reconcile and bounded http actions denied.")
        if _SECRETS_PII_HINTS.search(summary):
            rule_classes.append("secrets_pii")
            deny_for_rule.add("runtime.action.execute.subprocess")
            expl_parts.append("Knowledge constraint limits secret/PII exposure; subprocess execution denied.")
        if _DESTRUCTIVE_HINTS.search(summary):
            rule_classes.append("destructive")
            deny_for_rule.update({"service.reconcile.apply", "service.reconcile.rollback"})
            expl_parts.append("Knowledge constraint forbids destructive operations; reconcile actions denied.")

        deny_for_rule &= _RUNTIME_ACTION_ALLOWLIST

        if rule_classes:
            policy_rules[aid] = {
                "assertion_id": aid,
                "classes": sorted(rule_classes),
                "deny_actions": sorted(deny_for_rule),
            }
            derived_deny |= deny_for_rule
            explanations[aid] = " ".join(expl_parts) if expl_parts else f"Hard constraint: {summary[:200]}"

    out: dict[str, Any] = {
        "knowledge_hard_constraints": hard_selected,
        "knowledge_network_egress_forbidden": network_forbidden,
    }
    if policy_rules:
        out["knowledge_policy_rules"] = [policy_rules[k] for k in sorted(policy_rules.keys())]
    if explanations:
        out["knowledge_explanations"] = dict(sorted(explanations.items()))
    if derived_deny:
        out["knowledge_derived_deny_actions"] = sorted(derived_deny)
    return out
