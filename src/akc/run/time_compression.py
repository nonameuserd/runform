from __future__ import annotations

from typing import Any


def _to_int_ms(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, float):
        iv = int(value)
        return iv if iv >= 0 else None
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            iv = int(float(s))
        except ValueError:
            return None
        return iv if iv >= 0 else None
    return None


def derive_time_compression_metrics(
    *,
    lifecycle_timestamps: dict[str, Any],
    baseline_duration_hours: float | None = None,
) -> dict[str, float]:
    """Compute canonical time-compression metrics from lifecycle timestamps.

    Non-monotonic timestamp pairs are ignored so a single bad clock skew does not
    emit misleading duration or compression values.
    """

    intent_received_at = _to_int_ms(lifecycle_timestamps.get("intent_received_at"))
    compile_started_at = _to_int_ms(lifecycle_timestamps.get("compile_started_at"))
    compile_completed_at = _to_int_ms(lifecycle_timestamps.get("compile_completed_at"))
    runtime_healthy_at = _to_int_ms(lifecycle_timestamps.get("runtime_healthy_at"))

    out: dict[str, float] = {}

    if intent_received_at is not None and runtime_healthy_at is not None:
        delta = int(runtime_healthy_at) - int(intent_received_at)
        if delta >= 0:
            out["intent_to_healthy_runtime_ms"] = float(delta)

    if compile_started_at is not None and runtime_healthy_at is not None:
        delta_c = int(runtime_healthy_at) - int(compile_started_at)
        if delta_c >= 0:
            out["compile_to_healthy_runtime_ms"] = float(delta_c)

    if baseline_duration_hours is not None and baseline_duration_hours > 0.0 and "intent_to_healthy_runtime_ms" in out:
        observed_hours = out["intent_to_healthy_runtime_ms"] / 3_600_000.0
        if observed_hours > 0.0:
            out["compression_factor_vs_baseline"] = float(baseline_duration_hours) / observed_hours
    if compile_completed_at is not None and compile_started_at is not None:
        cdelta = int(compile_completed_at) - int(compile_started_at)
        if cdelta >= 0:
            out["compile_duration_ms"] = float(cdelta)
    return out
