from __future__ import annotations

from akc.run.recompile_triggers import RECOMPILE_TRIGGER_EVAL_ORDER, is_known_recompile_trigger_kind


def test_recompile_trigger_kind_registry_covers_literal() -> None:
    assert is_known_recompile_trigger_kind("intent_semantic_changed")
    assert is_known_recompile_trigger_kind("acceptance_criterion_failed")
    assert not is_known_recompile_trigger_kind("unknown_kind")


def test_eval_order_is_stable_tuple() -> None:
    assert len(RECOMPILE_TRIGGER_EVAL_ORDER) == 6
    assert RECOMPILE_TRIGGER_EVAL_ORDER[0] == "intent_semantic_changed"
