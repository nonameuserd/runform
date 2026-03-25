"""Pure TUI helper tests (no curses / TTY)."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from akc.memory.models import PlanStep
from akc.viewer.models import EvidenceRef
from akc.viewer.tui import (
    _build_evidence_list_lines,
    _evidence_split_heights,
    _status_badge,
    clamp_scroll_offset,
    ensure_line_visible,
    move_in_filtered_list,
    next_filtered_step_index,
    status_attr_key,
    step_indices_matching_filter,
    tui_environment_ok,
)


def _step(i: int, title: str, status: str = "done") -> PlanStep:
    return PlanStep(id=f"s{i}", title=title, status=status, order_idx=i)


def test_status_badge_and_attr_key() -> None:
    assert _status_badge("done") == "[done]"
    assert _status_badge("in_progress") == "[... ]"
    assert _status_badge("failed") == "[FAIL]"
    assert _status_badge("skipped") == "[skip]"
    assert _status_badge("unknown") == "[    ]"
    assert status_attr_key("done") == "done"
    assert status_attr_key("weird") == "other"


@pytest.mark.parametrize(
    ("offset", "lines", "vh", "expected"),
    [
        (0, 0, 5, 0),
        (5, 10, 5, 5),
        (99, 10, 5, 5),
        (3, 10, 5, 3),
        (0, 10, 0, 0),
    ],
)
def test_clamp_scroll_offset(offset: int, lines: int, vh: int, expected: int) -> None:
    assert clamp_scroll_offset(offset, line_count=lines, viewport_height=vh) == expected


def test_ensure_line_visible() -> None:
    assert ensure_line_visible(0, line_index=3, viewport_height=4, line_count=10) == 0
    assert ensure_line_visible(0, line_index=7, viewport_height=4, line_count=10) == 4
    assert ensure_line_visible(5, line_index=2, viewport_height=4, line_count=10) == 2


def test_step_indices_matching_filter() -> None:
    steps = [_step(0, "Alpha build"), _step(1, "Beta test"), _step(2, "alpha docs")]
    assert step_indices_matching_filter(steps, "") == [0, 1, 2]
    assert step_indices_matching_filter(steps, "alpha") == [0, 2]
    assert step_indices_matching_filter(steps, "  BETA ") == [1]


def test_move_in_filtered_list() -> None:
    assert move_in_filtered_list(1, [1, 3, 5], -1) == 5
    assert move_in_filtered_list(1, [1, 3, 5], 1) == 3
    assert move_in_filtered_list(9, [1, 3], 1) == 1


def test_next_filtered_step_index() -> None:
    assert next_filtered_step_index(2, [1, 3, 5], direction=1) == 3
    assert next_filtered_step_index(5, [1, 3, 5], direction=1) == 1
    assert next_filtered_step_index(3, [1, 3, 5], direction=-1) == 1
    assert next_filtered_step_index(1, [1, 3, 5], direction=-1) == 5


def test_evidence_split_heights() -> None:
    lh, sep, ph = _evidence_split_heights(20)
    assert lh + sep + ph == 20
    assert lh >= 2 and ph >= 2


def test_build_evidence_list_lines_empty() -> None:
    lines = _build_evidence_list_lines([], selected_idx=0, scoped_root=Path("/out"))
    assert len(lines) == 3
    assert "no evidence" in lines[2]


def test_build_evidence_list_lines_with_items() -> None:
    ev = [
        EvidenceRef(kind="text", relpath="a.txt"),
        EvidenceRef(kind="text", relpath="b.txt"),
    ]
    root = Path("/scoped")
    lines = _build_evidence_list_lines(ev, selected_idx=1, scoped_root=root)
    assert any("> b.txt" in ln for ln in lines)
    assert str(root / "b.txt") in "".join(lines)


def test_tui_environment_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("akc.viewer.tui.sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("akc.viewer.tui.sys.stdout.isatty", lambda: True)
    monkeypatch.setenv("TERM", "xterm-256color")
    monkeypatch.setattr(
        "akc.viewer.tui.shutil.get_terminal_size",
        lambda: SimpleNamespace(lines=24, columns=100),
    )
    ok, reason = tui_environment_ok()
    assert ok is True
    assert reason == ""


def test_tui_environment_not_tty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("akc.viewer.tui.sys.stdin.isatty", lambda: False)
    monkeypatch.setattr("akc.viewer.tui.sys.stdout.isatty", lambda: True)
    ok, reason = tui_environment_ok()
    assert ok is False
    assert "TTY" in reason


def test_tui_environment_dumb_term(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("akc.viewer.tui.sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("akc.viewer.tui.sys.stdout.isatty", lambda: True)
    monkeypatch.setenv("TERM", "dumb")
    ok, reason = tui_environment_ok()
    assert ok is False
    assert "dumb" in reason.lower()


def test_tui_environment_short_terminal(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("akc.viewer.tui.sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("akc.viewer.tui.sys.stdout.isatty", lambda: True)
    monkeypatch.setenv("TERM", "xterm")
    monkeypatch.setattr(
        "akc.viewer.tui.shutil.get_terminal_size",
        lambda: SimpleNamespace(lines=4, columns=80),
    )
    ok, reason = tui_environment_ok()
    assert ok is False
    assert "height" in reason.lower()
