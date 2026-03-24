from __future__ import annotations

from akc.run.time_compression import derive_time_compression_metrics


def test_derive_time_compression_ignores_non_monotonic_intent_to_healthy() -> None:
    out = derive_time_compression_metrics(
        lifecycle_timestamps={
            "intent_received_at": 200,
            "runtime_healthy_at": 100,
        },
        baseline_duration_hours=8.0,
    )
    assert "intent_to_healthy_runtime_ms" not in out
    assert "compression_factor_vs_baseline" not in out


def test_derive_time_compression_includes_compression_when_monotonic() -> None:
    out = derive_time_compression_metrics(
        lifecycle_timestamps={
            "intent_received_at": 0,
            "runtime_healthy_at": 3_600_000,
        },
        baseline_duration_hours=8.0,
    )
    assert out.get("intent_to_healthy_runtime_ms") == 3_600_000.0
    assert out.get("compression_factor_vs_baseline") == 8.0
