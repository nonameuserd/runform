"""Exponential backoff + jitter between bounded reconcile resync attempts (CLI).

Controller-runtime-style spacing reduces thundering herds when many bundles resync.
When ``reconcile_resync_jitter_seed`` is set, delays are reproducible for tests;
otherwise delays derive from a hash of ``runtime_run_id`` so a single run is stable
across attempts but different runs are decorrelated.
"""

from __future__ import annotations

import hashlib
import random
from collections.abc import Mapping
from dataclasses import dataclass

from akc.memory.models import JSONValue


@dataclass(frozen=True, slots=True)
class ResyncBackoffConfig:
    """Parsed from runtime bundle ``metadata`` alongside ``reconcile_resync_*``."""

    exponential: bool
    base_interval_ms: int
    max_interval_ms: int
    jitter_ratio: float
    explicit_jitter_seed: int | None


def _coerce_positive_int(raw: object, *, default: int, cap: int) -> int:
    if isinstance(raw, int) and not isinstance(raw, bool) and raw >= 0:
        return min(int(raw), cap)
    return default


def parse_resync_backoff_config(
    metadata: Mapping[str, JSONValue],
    *,
    reconcile_resync_interval_ms: int,
) -> ResyncBackoffConfig:
    """Read backoff knobs from bundle metadata."""

    exponential = metadata.get("reconcile_resync_exponential_backoff") is True
    default_base = int(reconcile_resync_interval_ms) if int(reconcile_resync_interval_ms) > 0 else 1_000
    base = _coerce_positive_int(metadata.get("reconcile_resync_base_interval_ms"), default=default_base, cap=86_400_000)
    if base <= 0:
        base = 1
    max_iv = _coerce_positive_int(
        metadata.get("reconcile_resync_max_interval_ms"),
        default=max(base, 60_000),
        cap=86_400_000,
    )
    if max_iv < base:
        max_iv = base
    jr_raw = metadata.get("reconcile_resync_jitter_ratio")
    jitter_ratio = 0.2
    if isinstance(jr_raw, (int, float)) and not isinstance(jr_raw, bool):
        jitter_ratio = max(0.0, min(1.0, float(jr_raw)))
    seed_raw = metadata.get("reconcile_resync_jitter_seed")
    explicit: int | None = None
    if isinstance(seed_raw, int) and not isinstance(seed_raw, bool):
        explicit = int(seed_raw)
    return ResyncBackoffConfig(
        exponential=exponential,
        base_interval_ms=base,
        max_interval_ms=max_iv,
        jitter_ratio=jitter_ratio,
        explicit_jitter_seed=explicit,
    )


def _rng_for_resync_attempt(*, runtime_run_id: str, attempt_index: int, explicit_seed: int | None) -> random.Random:
    if explicit_seed is not None:
        return random.Random((int(explicit_seed) * 1_000_003) ^ attempt_index)
    digest = hashlib.sha256(f"{runtime_run_id}\n{attempt_index}".encode()).digest()
    seed = int.from_bytes(digest[:8], "big", signed=False)
    return random.Random(seed)


def compute_resync_sleep_ms(
    *,
    sleep_after_attempt_index: int,
    config: ResyncBackoffConfig,
    fixed_interval_ms: int,
    runtime_run_id: str,
) -> int:
    """Milliseconds to sleep after attempt ``sleep_after_attempt_index`` (0-based)."""

    if not config.exponential:
        return int(fixed_interval_ms) if int(fixed_interval_ms) > 0 else 0
    raw = min(int(config.max_interval_ms), int(config.base_interval_ms) * (2 ** int(sleep_after_attempt_index)))
    if raw <= 0:
        return 0
    if config.jitter_ratio <= 0:
        return int(raw)
    rng = _rng_for_resync_attempt(
        runtime_run_id=runtime_run_id,
        attempt_index=sleep_after_attempt_index,
        explicit_seed=config.explicit_jitter_seed,
    )
    lo = float(raw) * (1.0 - float(config.jitter_ratio))
    hi = float(raw)
    return int(rng.uniform(lo, hi))
