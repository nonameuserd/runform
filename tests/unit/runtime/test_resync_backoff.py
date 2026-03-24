from __future__ import annotations

from akc.memory.models import JSONValue
from akc.runtime.resync_backoff import compute_resync_sleep_ms, parse_resync_backoff_config


def test_parse_resync_backoff_uses_interval_as_base_when_exponential() -> None:
    meta: dict[str, JSONValue] = {"reconcile_resync_exponential_backoff": True}
    cfg = parse_resync_backoff_config(meta, reconcile_resync_interval_ms=500)
    assert cfg.exponential is True
    assert cfg.base_interval_ms == 500


def test_exponential_backoff_deterministic_with_explicit_seed() -> None:
    meta: dict[str, JSONValue] = {
        "reconcile_resync_exponential_backoff": True,
        "reconcile_resync_base_interval_ms": 1000,
        "reconcile_resync_max_interval_ms": 10_000,
        "reconcile_resync_jitter_ratio": 0.5,
        "reconcile_resync_jitter_seed": 42,
    }
    cfg = parse_resync_backoff_config(meta, reconcile_resync_interval_ms=0)
    a = compute_resync_sleep_ms(
        sleep_after_attempt_index=0,
        config=cfg,
        fixed_interval_ms=0,
        runtime_run_id="run-a",
    )
    b = compute_resync_sleep_ms(
        sleep_after_attempt_index=0,
        config=cfg,
        fixed_interval_ms=0,
        runtime_run_id="run-b",
    )
    assert a == b
    c = compute_resync_sleep_ms(
        sleep_after_attempt_index=1,
        config=cfg,
        fixed_interval_ms=0,
        runtime_run_id="run-a",
    )
    assert c >= 1000
