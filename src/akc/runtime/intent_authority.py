"""Strict runtime verification that ``intent_policy_projection`` matches normalized intent on disk."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from akc.intent.models import stable_intent_sha256
from akc.intent.policy_projection import project_runtime_intent_projection
from akc.intent.store import IntentStoreError, JsonFileIntentStore
from akc.memory.models import normalize_repo_id
from akc.path_security import safe_resolve_path
from akc.utils.fingerprint import stable_json_fingerprint


def repo_root_from_bundle_path(bundle_path: Path) -> Path | None:
    """Return the directory that contains ``.akc`` (repo / outputs root) when ``bundle_path`` includes ``.akc``."""

    parts = safe_resolve_path(bundle_path).parts
    try:
        akc_index = parts.index(".akc")
    except ValueError:
        return None
    if akc_index < 1:
        return None
    return Path(*parts[:akc_index])


def strict_intent_authority_enabled(
    *,
    policy_envelope: Mapping[str, Any],
    cli_force_strict: bool | None,
) -> bool:
    """True when strict intent reload/verify should run.

    ``cli_force_strict``:
    - ``True`` — always strict
    - ``False`` — never strict (programmatic opt-out)
    - ``None`` — defer to ``runtime_policy_envelope.intent_authority_strict``
    """

    if cli_force_strict is True:
        return True
    if cli_force_strict is False:
        return False
    return bool(policy_envelope.get("intent_authority_strict"))


def resolve_intent_policy_projection_for_bundle(
    *,
    bundle_path: Path,
    payload: Mapping[str, Any],
    strict: bool,
) -> dict[str, Any]:
    """Return the ``intent_policy_projection`` object used for policy derivation.

    In strict mode, load ``.akc/intent/...``, verify ``stable_intent_sha256``, recompute the projection,
    and require fingerprint parity with the bundle (fail closed on mismatch). The returned mapping is
    always the effective projection (recomputed when strict).
    """

    raw = payload.get("intent_policy_projection")
    bundle_projection: dict[str, Any] = dict(raw) if isinstance(raw, Mapping) else {}
    if not strict:
        return bundle_projection

    intent_ref = payload.get("intent_ref")
    if not isinstance(intent_ref, Mapping):
        raise ValueError("strict intent authority requires a bundle intent_ref object")
    intent_id = str(intent_ref.get("intent_id", "")).strip()
    ref_sha = str(intent_ref.get("stable_intent_sha256", "")).strip().lower()
    if not intent_id or len(ref_sha) != 64 or any(c not in "0123456789abcdef" for c in ref_sha):
        raise ValueError("strict intent authority requires intent_ref.intent_id and stable_intent_sha256 (64 hex)")

    tenant_id = str(payload.get("tenant_id", "")).strip()
    repo_id = str(payload.get("repo_id", "")).strip()
    if not tenant_id or not repo_id:
        raise ValueError("strict intent authority requires bundle tenant_id and repo_id")

    root = repo_root_from_bundle_path(bundle_path)
    if root is None:
        raise ValueError(
            "strict intent authority requires the bundle path to include a `.akc` segment "
            "so the repo root (intent store base) can be resolved"
        )

    store = JsonFileIntentStore(base_dir=root)
    try:
        loaded = store.load_intent(tenant_id=tenant_id, repo_id=repo_id, intent_id=intent_id)
    except IntentStoreError as e:
        raise ValueError(f"strict intent authority: failed to load intent from store: {e}") from e
    if loaded is None:
        raise ValueError(
            f"strict intent authority: no normalized intent at .akc/intent for "
            f"tenant_id={tenant_id!r} repo_id={normalize_repo_id(repo_id)!r} intent_id={intent_id!r}"
        )

    normalized = loaded.normalized()
    disk_sha = stable_intent_sha256(intent=normalized).strip().lower()
    if disk_sha != ref_sha:
        raise ValueError(
            "strict intent authority: stable_intent_sha256 mismatch between intent store and bundle intent_ref"
        )

    recomputed = project_runtime_intent_projection(intent=normalized).to_json_obj()
    bundle_fp = stable_json_fingerprint(bundle_projection)
    recomputed_fp = stable_json_fingerprint(recomputed)
    if bundle_fp != recomputed_fp:
        raise ValueError(
            "strict intent authority: bundle intent_policy_projection does not match recomputed projection "
            f"(bundle_fp={bundle_fp} recomputed_fp={recomputed_fp})"
        )
    return recomputed
