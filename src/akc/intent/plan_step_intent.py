"""Plan step inputs: intent reference vs duplicated contract blobs.

When a compile run uses a tenant-scoped :class:`~akc.intent.store.IntentStore`,
:class:`~akc.compile.controller.run_compile_loop` persists the normalized intent and
stores an ``intent_ref`` block on each :class:`~akc.memory.models.PlanStep` instead
of copying ``active_objectives`` / ``linked_constraints`` /
``active_success_criteria`` into every step.

Callers that still read those keys from step inputs can resolve the authoritative
:class:`~akc.intent.models.IntentSpecV1` via :func:`load_intent_verified_from_plan_step_inputs`.
Compile-time orchestration should prefer :mod:`akc.intent.resolve`
(:func:`~akc.intent.resolve.resolve_compile_intent_context`).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from akc.intent.models import IntentSpecV1, stable_intent_sha256
from akc.intent.store import IntentStore
from akc.memory.models import normalize_repo_id, require_non_empty

INTENT_REF_INPUT_KEY = "intent_ref"


def build_plan_step_intent_ref(
    *,
    intent: IntentSpecV1,
    semantic_fingerprint: str,
    goal_text_fingerprint: str,
) -> dict[str, Any]:
    """JSON-serializable correlation block for :class:`~akc.memory.models.PlanStep`."""

    n = intent.normalized()
    return {
        "intent_id": n.intent_id,
        "spec_version": int(n.spec_version),
        "stable_intent_sha256": stable_intent_sha256(intent=n),
        "semantic_fingerprint": str(semantic_fingerprint),
        "goal_text_fingerprint": str(goal_text_fingerprint),
    }


def _normalize_stable_sha(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    s = value.strip().lower()
    if len(s) != 64 or any(c not in "0123456789abcdef" for c in s):
        return None
    return s


def load_intent_verified_from_plan_step_inputs(
    *,
    tenant_id: str,
    repo_id: str,
    inputs: Mapping[str, Any],
    intent_store: IntentStore,
) -> IntentSpecV1 | None:
    """Load intent from the store when ``intent_ref`` matches stored bytes.

    Returns ``None`` when inputs lack a valid ref, the artifact is missing, or the
    stable hash does not match (tamper / stale plan).
    """

    require_non_empty(tenant_id, name="tenant_id")
    repo = normalize_repo_id(repo_id)
    ref = inputs.get(INTENT_REF_INPUT_KEY)
    if not isinstance(ref, dict):
        return None
    intent_id_raw = ref.get("intent_id")
    if not isinstance(intent_id_raw, str) or not intent_id_raw.strip():
        return None
    expected = _normalize_stable_sha(ref.get("stable_intent_sha256"))
    if expected is None:
        return None
    loaded = intent_store.load_intent(
        tenant_id=tenant_id.strip(),
        repo_id=repo,
        intent_id=intent_id_raw.strip(),
    )
    if loaded is None:
        return None
    normalized = loaded.normalized()
    if stable_intent_sha256(intent=normalized) != expected:
        return None
    if normalized.tenant_id.strip() != tenant_id.strip() or normalize_repo_id(normalized.repo_id) != repo:
        raise ValueError("tenant_id/repo_id mismatch when resolving intent_ref")
    return normalized
