"""Compile/run pass name ordering (no imports from `compile` or `run` — avoids import cycles).

Lowering DAG semantics and per-pass I/O live in `akc.compile.artifact_passes` (module docstring).
"""

from __future__ import annotations

# Controller-loop pass names in manifest order (prefix of `REPLAYABLE_PASSES` in `akc.run.manifest`).
# NOTE: Keep in sync with `CompileSession._build_pass_records()`.
CONTROLLER_LOOP_PASS_ORDER: tuple[str, ...] = (
    "plan",
    "retrieve",
    "generate",
    "execute",
    "repair",
    "verify",
    "intent_acceptance",
)

# Deterministic artifact lowering passes (suffix of `REPLAYABLE_PASSES`).
ARTIFACT_PASS_ORDER: tuple[str, ...] = (
    "system_design",
    "orchestration_spec",
    "agent_coordination",
    "delivery_plan",
    "runtime_bundle",
    "deployment_config",
)


def assert_expected_artifact_pass_order(*, actual: list[str]) -> None:
    """Raise if `actual` does not match `ARTIFACT_PASS_ORDER` (session invariant)."""

    if tuple(actual) != ARTIFACT_PASS_ORDER:
        msg = f"artifact pass order mismatch: got {actual!r}, expected {list(ARTIFACT_PASS_ORDER)!r}"
        raise RuntimeError(msg)
