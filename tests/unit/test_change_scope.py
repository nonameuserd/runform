from __future__ import annotations

import pytest

from akc.compile.change_scope import categorize_touched_paths
from akc.compile.controller_config import (
    ControllerConfig,
    TierConfig,
    controller_test_promotion_is_smoke_first,
    controller_uses_native_toolchain_resolved_commands,
)


def _tiers() -> dict[str, TierConfig]:
    return {"small": TierConfig(name="small", llm_model="m")}


def test_categorize_touched_paths_dependency_and_code() -> None:
    s = categorize_touched_paths(["package.json", "src/foo.py", ".github/workflows/ci.yml"])
    assert s.counts_by_category.get("dependency") == 1
    assert s.counts_by_category.get("code") == 1
    assert s.counts_by_category.get("ci") == 1


def test_categorize_unknown_extension_is_other() -> None:
    s = categorize_touched_paths(["renovate.json"])
    assert s.counts_by_category.get("other") == 1


def test_controller_native_test_mode_uses_toolchain_for_smoke_full() -> None:
    base = ControllerConfig(tiers=_tiers(), test_mode="smoke", native_test_mode=True)
    assert controller_uses_native_toolchain_resolved_commands(base) is True
    assert controller_test_promotion_is_smoke_first(base) is True
    full_native = ControllerConfig(tiers=_tiers(), test_mode="full", native_test_mode=True)
    assert controller_uses_native_toolchain_resolved_commands(full_native) is True
    assert controller_test_promotion_is_smoke_first(full_native) is False


def test_controller_native_full_explicit_without_flag() -> None:
    cfg = ControllerConfig(tiers=_tiers(), test_mode="native_full")
    assert controller_uses_native_toolchain_resolved_commands(cfg) is True
    assert controller_test_promotion_is_smoke_first(cfg) is False


def test_change_scope_deny_categories_validation() -> None:
    ControllerConfig(tiers=_tiers(), change_scope_deny_categories=("ci", "infra"))
    with pytest.raises(ValueError, match="unknown category"):
        ControllerConfig(tiers=_tiers(), change_scope_deny_categories=("ci", "bogus"))  # type: ignore[arg-type]
