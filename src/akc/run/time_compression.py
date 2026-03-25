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
    manual_touch_count: int | None = None,
) -> dict[str, float]:
    """Compute canonical time-compression metrics from lifecycle timestamps.

    Non-monotonic timestamp pairs are ignored so a single bad clock skew does not
    emit misleading duration or compression values.
    """

    intent_received_at = _to_int_ms(lifecycle_timestamps.get("intent_received_at"))
    compile_started_at = _to_int_ms(lifecycle_timestamps.get("compile_started_at"))
    compile_completed_at = _to_int_ms(lifecycle_timestamps.get("compile_completed_at"))
    runtime_healthy_at = _to_int_ms(lifecycle_timestamps.get("runtime_healthy_at"))
    staging_healthy_at = _to_int_ms(lifecycle_timestamps.get("staging_healthy_at"))
    prod_healthy_at = _to_int_ms(lifecycle_timestamps.get("prod_healthy_at"))
    approval_started = _to_int_ms(lifecycle_timestamps.get("approval_wait_started_at"))
    approval_completed = _to_int_ms(lifecycle_timestamps.get("approval_wait_completed_at"))

    out: dict[str, float] = {}

    if intent_received_at is not None and runtime_healthy_at is not None:
        delta = int(runtime_healthy_at) - int(intent_received_at)
        if delta >= 0:
            out["intent_to_healthy_runtime_ms"] = float(delta)

    if compile_started_at is not None and runtime_healthy_at is not None:
        delta_c = int(runtime_healthy_at) - int(compile_started_at)
        if delta_c >= 0:
            out["compile_to_healthy_runtime_ms"] = float(delta_c)

    if intent_received_at is not None and staging_healthy_at is not None:
        d_st = int(staging_healthy_at) - int(intent_received_at)
        if d_st >= 0:
            out["intent_to_staging_ms"] = float(d_st)

    if intent_received_at is not None and prod_healthy_at is not None:
        d_pr = int(prod_healthy_at) - int(intent_received_at)
        if d_pr >= 0:
            out["intent_to_prod_ms"] = float(d_pr)

    if staging_healthy_at is not None and prod_healthy_at is not None:
        d_sp = int(prod_healthy_at) - int(staging_healthy_at)
        if d_sp >= 0:
            out["staging_to_prod_ms"] = float(d_sp)

    if approval_started is not None and approval_completed is not None:
        d_ap = int(approval_completed) - int(approval_started)
        if d_ap >= 0:
            out["approval_wait_ms"] = float(d_ap)

    if (
        manual_touch_count is not None
        and isinstance(manual_touch_count, int)
        and not isinstance(manual_touch_count, bool)
        and manual_touch_count >= 0
    ):
        out["manual_touch_count"] = float(int(manual_touch_count))

    if baseline_duration_hours is not None and baseline_duration_hours > 0.0 and "intent_to_healthy_runtime_ms" in out:
        observed_hours = out["intent_to_healthy_runtime_ms"] / 3_600_000.0
        if observed_hours > 0.0:
            out["compression_factor_vs_baseline"] = float(baseline_duration_hours) / observed_hours
    if compile_completed_at is not None and compile_started_at is not None:
        cdelta = int(compile_completed_at) - int(compile_started_at)
        if cdelta >= 0:
            out["compile_duration_ms"] = float(cdelta)
    return out
