from __future__ import annotations

import curses
from dataclasses import dataclass
from pathlib import Path

from akc.memory.models import PlanStep

from .models import EvidenceRef, ViewerSnapshot


class TuiError(Exception):
    """Raised when the TUI cannot be started."""


@dataclass(slots=True)
class _TuiState:
    snap: ViewerSnapshot
    selected_step_idx: int = 0
    mode: str = "steps"  # steps | evidence
    selected_evidence_idx: int = 0


def _status_badge(status: str) -> str:
    s = status.strip()
    if s == "done":
        return "[done]"
    if s == "in_progress":
        return "[... ]"
    if s == "failed":
        return "[FAIL]"
    if s == "skipped":
        return "[skip]"
    return "[    ]"


def _step_line(step: PlanStep, *, is_selected: bool) -> str:
    prefix = ">" if is_selected else " "
    return f"{prefix} {_status_badge(step.status)} {step.title}"


def _render_lines(stdscr: curses.window, *, lines: list[str]) -> None:
    stdscr.erase()
    h, w = stdscr.getmaxyx()
    usable = max(0, h - 1)
    for i, line in enumerate(lines[:usable]):
        # Defensive clipping for terminal width.
        stdscr.addnstr(i, 0, line, max(0, w - 1))
    stdscr.refresh()


def _read_text_preview(path: Path, *, max_bytes: int = 32_768) -> str:
    try:
        data = path.read_bytes()
    except OSError:
        return "(unable to read file)"
    if len(data) > max_bytes:
        data = data[:max_bytes] + b"\n\n(truncated)\n"
    try:
        return data.decode("utf-8", errors="replace")
    except Exception:
        return "(binary file)"


def run_tui(snapshot: ViewerSnapshot) -> int:
    """Run a curses TUI for the snapshot (read-only)."""

    state = _TuiState(snap=snapshot)

    def _loop(stdscr: curses.window) -> int:
        curses.curs_set(0)
        stdscr.keypad(True)

        while True:
            plan = state.snap.plan
            steps = list(plan.steps)
            header = [
                f"AKC viewer (read-only) — {plan.tenant_id}/{plan.repo_id}",
                f"Goal: {plan.goal}",
                f"Plan: {plan.status}  Updated: {plan.updated_at_ms}  Steps: {len(steps)}",
                "Keys: ↑/↓ move  Enter open  b back  v view file  q quit",
                "",
            ]

            if state.mode == "steps":
                body: list[str] = []
                if not steps:
                    body = ["(no steps in plan)"]
                else:
                    sel = max(0, min(state.selected_step_idx, len(steps) - 1))
                    state.selected_step_idx = sel
                    for i, s in enumerate(steps):
                        body.append(_step_line(s, is_selected=(i == sel)))
                    body.append("")
                    step = steps[sel]
                    ev = state.snap.evidence.by_step.get(step.id, [])
                    body.append(f"Evidence files linked to this step: {len(ev)}")
                _render_lines(stdscr, lines=header + body)
            else:
                # evidence mode
                step_sel: PlanStep | None = steps[state.selected_step_idx] if steps else None
                ev = state.snap.evidence.by_step.get(step_sel.id, []) if step_sel else []
                sel = max(0, min(state.selected_evidence_idx, max(0, len(ev) - 1)))
                state.selected_evidence_idx = sel

                body = ["Evidence (relative paths):", ""]
                if not ev:
                    body.append("(no evidence files for selected step)")
                else:
                    for i, r in enumerate(ev):
                        prefix = ">" if i == sel else " "
                        body.append(f"{prefix} {r.relpath} ({r.kind})")
                    body.append("")
                    ref_sel = ev[sel]
                    body.append(f"Selected: {ref_sel.relpath}")
                    body.append("Tip: press 'v' to preview text files.")
                _render_lines(stdscr, lines=header + body)

            ch = stdscr.getch()
            if ch in (ord("q"), 27):  # q or ESC
                return 0
            if state.mode == "steps":
                if ch == curses.KEY_UP:
                    state.selected_step_idx = max(0, state.selected_step_idx - 1)
                elif ch == curses.KEY_DOWN:
                    state.selected_step_idx = min(len(steps) - 1, state.selected_step_idx + 1)
                elif ch in (curses.KEY_ENTER, 10, 13):
                    state.mode = "evidence"
                    state.selected_evidence_idx = 0
            else:
                step_sel2: PlanStep | None = steps[state.selected_step_idx] if steps else None
                ev = state.snap.evidence.by_step.get(step_sel2.id, []) if step_sel2 else []
                if ch == ord("b"):
                    state.mode = "steps"
                elif ch == curses.KEY_UP:
                    state.selected_evidence_idx = max(0, state.selected_evidence_idx - 1)
                elif ch == curses.KEY_DOWN:
                    state.selected_evidence_idx = min(
                        max(0, len(ev) - 1), state.selected_evidence_idx + 1
                    )
                elif ch == ord("v") and ev:
                    ref: EvidenceRef = ev[state.selected_evidence_idx]
                    fp = state.snap.scoped_outputs_dir / ref.relpath
                    preview = _read_text_preview(fp)
                    lines = (
                        header
                        + [
                            f"Preview: {ref.relpath}",
                            "-" * 40,
                        ]
                        + preview.splitlines()
                    )
                    _render_lines(stdscr, lines=lines)
                    stdscr.getch()

        return 0

    try:
        return int(curses.wrapper(_loop))
    except curses.error as e:
        raise TuiError("curses TUI could not be initialized (not a TTY?)") from e
