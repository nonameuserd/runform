from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from akc.compile.controller_config import ControllerConfig, TierConfig
from akc.compile.controller_types import Candidate
from akc.compile.interfaces import ExecutionResult


def _tier_order(name: str) -> int:
    if name == "small":
        return 0
    if name == "medium":
        return 1
    if name == "large":
        return 2
    return 99


def _escalate_tier(*, current: TierConfig, config: ControllerConfig) -> TierConfig:
    """Escalate tier conservatively: small→medium→large (if available)."""

    tiers = sorted(config.tiers.values(), key=lambda t: _tier_order(t.name))
    for idx, t in enumerate(tiers):
        if t.name == current.name:
            if idx + 1 < len(tiers):
                return tiers[idx + 1]
            return current
    return current


def _best_of(a: Candidate | None, b: Candidate) -> Candidate:
    if a is None:
        return b
    # Higher score wins; tie-break deterministically on attempt_idx.
    if b.score > a.score:
        return b
    if b.score < a.score:
        return a
    return b if b.attempt_idx >= a.attempt_idx else a


def _extract_patch_paths(patch_text: str) -> list[str]:
    """Extract touched file paths from a unified diff.

    Best-effort and deterministic: returns stable sorted unique paths.
    """

    paths: set[str] = set()
    for raw in (patch_text or "").splitlines():
        line = raw.strip()
        # Common forms:
        # --- a/foo.py
        # +++ b/foo.py
        if line.startswith("+++ "):
            p = line[4:].strip()
            if p.startswith("b/"):
                p = p[2:]
            if p and p != "/dev/null":
                paths.add(p)
        elif line.startswith("--- "):
            p = line[4:].strip()
            if p.startswith("a/"):
                p = p[2:]
            if p and p != "/dev/null":
                paths.add(p)
    return sorted(paths)


def _is_test_path(p: str) -> bool:
    p2 = str(p or "").replace("\\", "/")
    parts = [seg for seg in p2.split("/") if seg]
    if not parts:
        return False
    if parts[0] in {"test", "tests"}:
        return True
    leaf = parts[-1]
    if leaf.startswith("test_") and leaf.endswith(".py"):
        return True
    return bool(leaf.endswith("_test.py"))


def _policy_requires_tests(
    *,
    touched_paths: list[str],
    require_tests_for_non_test_changes: bool,
) -> tuple[bool, dict[str, Any]]:
    """Return (ok, evidence) for the tests-generated-by-default heuristic."""

    tests = [p for p in touched_paths if _is_test_path(p)]
    non_tests = [p for p in touched_paths if not _is_test_path(p)]
    if not require_tests_for_non_test_changes:
        return True, {
            "touched_paths": touched_paths,
            "test_paths": tests,
            "non_test_paths": non_tests,
        }
    # Only require tests if the patch touches at least one non-test path.
    if non_tests and not tests:
        return False, {
            "touched_paths": touched_paths,
            "test_paths": tests,
            "non_test_paths": non_tests,
        }
    return True, {
        "touched_paths": touched_paths,
        "test_paths": tests,
        "non_test_paths": non_tests,
    }


def _score_execution(result: ExecutionResult | None) -> int:
    if result is None:
        return 0
    # Simple monotone scoring: pass >> fail. Keep it stable for tests.
    return 1000 if int(result.exit_code) == 0 else 10


def _score_candidate(
    *,
    execution: ExecutionResult | None,
    ok_tests_policy: bool,
    promotable: bool,
    verifier_passed: bool | None,
) -> int:
    """Deterministic scoring for monotonic repair progress.

    We intentionally incorporate policy + verifier outcomes so that a repair
    candidate can be considered "improving" even when the test exit code
    stays the same (e.g. verifier vetoes due to patch safety issues).
    """

    base = _score_execution(execution)
    if base != 1000:
        return base

    # Differentiate smoke-only pass vs full promotable pass.
    if not promotable:
        return 950

    if not ok_tests_policy:
        return 900

    # verifier_passed is only meaningful when promotable and ok_tests_policy.
    if verifier_passed is False:
        return 800

    return 1000


def _estimate_token_count(text: str) -> int:
    # Deterministic heuristic fallback when provider usage isn't available.
    s = str(text or "")
    if not s:
        return 0
    return max(1, len(s) // 4)


def _estimate_cost_usd(
    *,
    input_tokens: int,
    output_tokens: int,
    tool_calls: int,
    mcp_calls: int = 0,
    input_per_1k_tokens_usd: float,
    output_per_1k_tokens_usd: float,
    tool_call_usd: float,
    mcp_call_usd: float = 0.0,
) -> float:
    """Estimate run cost from explicit rate configuration."""

    return (
        (float(max(0, int(input_tokens))) / 1000.0) * float(input_per_1k_tokens_usd)
        + (float(max(0, int(output_tokens))) / 1000.0) * float(output_per_1k_tokens_usd)
        + float(max(0, int(tool_calls))) * float(tool_call_usd)
        + float(max(0, int(mcp_calls))) * float(mcp_call_usd)
    )


def combined_tool_like_calls(accounting: Mapping[str, Any]) -> int:
    """Executor tool runs plus compile-time MCP operations (shared ``max_tool_calls`` budget)."""

    return int(accounting.get("tool_calls", 0)) + int(accounting.get("mcp_calls", 0))


def refresh_controller_estimated_cost_usd(*, accounting: dict[str, Any], config: ControllerConfig) -> None:
    """Recompute ``accounting['estimated_cost_usd']`` from rates + current counters."""

    md = dict(getattr(config, "metadata", None) or {})
    in_rate = float(config.cost_rates.input_per_1k_tokens_usd)
    out_rate = float(config.cost_rates.output_per_1k_tokens_usd)
    tool_rate = float(config.cost_rates.tool_call_usd)
    mcp_rate = float(config.cost_rates.mcp_call_usd)
    if in_rate == 0.0:
        in_rate = float(md.get("cost_input_per_1k_tokens_usd", 0.0) or 0.0)
    if out_rate == 0.0:
        out_rate = float(md.get("cost_output_per_1k_tokens_usd", 0.0) or 0.0)
    if tool_rate == 0.0:
        tool_rate = float(md.get("cost_tool_call_usd", 0.0) or 0.0)
    if mcp_rate == 0.0:
        mcp_rate = float(md.get("cost_mcp_call_usd", 0.0) or 0.0)
    accounting["estimated_cost_usd"] = float(
        _estimate_cost_usd(
            input_tokens=int(accounting["input_tokens"]),
            output_tokens=int(accounting["output_tokens"]),
            tool_calls=int(accounting["tool_calls"]),
            mcp_calls=int(accounting.get("mcp_calls", 0)),
            input_per_1k_tokens_usd=in_rate,
            output_per_1k_tokens_usd=out_rate,
            tool_call_usd=tool_rate,
            mcp_call_usd=mcp_rate,
        )
    )


def _derive_full_test_command(smoke_command: list[str]) -> list[str]:
    """Derive a 'full' test command from a smoke command deterministically.

    Policy:
    - If '-q' is present, drop it (common 'smoke' speed + noiseless mode).
    - Otherwise keep the command as-is.
    """

    if not smoke_command:
        raise ValueError("smoke_command must be non-empty")
    return [c for c in smoke_command if c != "-q"]
