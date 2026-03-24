"""Human-readable lines for policy decision reason codes (allowlist, capability, OPA/Rego)."""

from __future__ import annotations

# Known codes from DefaultDenyPolicyEngine, SubprocessOpaEvaluator, and prod/base Rego packs.
_POLICY_REASON_NARRATIVES: dict[str, str] = {
    "ok": "Capability token matched the requested action and tenant/repo scope.",
    "policy.action_empty": "The requested action was empty; default-deny applies.",
    "capability.action_empty": "The capability token did not name an action.",
    "capability.expired": "The capability token TTL expired before authorization.",
    "capability.scope_mismatch": "Token tenant/repo did not match the request scope.",
    "capability.action_mismatch": "Token action did not match the requested action.",
    "policy.default_deny.action_not_allowlisted": (
        "Action is not on the configured tool allowlist (default deny before OPA)."
    ),
    "policy.allowlist.allow": "Action is allowlisted and no OPA evaluator was configured.",
    "policy.opa.allow": "OPA/Rego evaluated to allow for this input.",
    "policy.opa.deny": "OPA/Rego evaluated to deny without a specific reason string.",
    "policy.opa.unavailable": "OPA binary or policy inputs were unavailable; treated as deny.",
    "policy.opa.parse_error": "OPA returned a value shape the evaluator could not interpret.",
    "policy.opa.unknown": "OPA evaluation did not yield a recognized allow/deny result.",
}


def describe_policy_reason(reason: str) -> str:
    """Return a short operator-facing sentence, or a generic fallback."""

    key = str(reason or "").strip()
    if not key:
        return "No reason code was recorded on this decision."
    if key.startswith("policy.opa.error"):
        return "OPA exited with an error while evaluating this decision; see stderr details in logs."
    mapped = _POLICY_REASON_NARRATIVES.get(key)
    if mapped is not None:
        return mapped
    if key.startswith("policy.prod.") or key.startswith("policy.executor."):
        return f"Rego prod/base pack rule fired; see configs/policy/*.rego for the exact guard ({key})."
    return f"Policy reason code `{key}` — see OPA/Rego bundles and allowlist configuration."
