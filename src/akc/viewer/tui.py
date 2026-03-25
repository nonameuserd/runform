from __future__ import annotations

import contextlib
import curses
import json
import os
import shutil
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from akc.knowledge.observability import build_knowledge_observation_payload
from akc.memory.models import PlanStep

from .models import EvidenceRef, ViewerSnapshot


class TuiError(Exception):
    """Raised when the TUI cannot be started."""


# Minimum rows for header + body + footer (see _MIN_TUI_ROWS in view preflight — keep in sync).
_MIN_TUI_ROWS = 10

_MODE_ORDER = ("steps", "evidence", "knowledge", "profile")


def tui_environment_ok() -> tuple[bool, str]:
    """
    Return whether the curses TUI is likely usable without initializing curses.

    Call from the CLI before ``curses.wrapper`` so dumb terminals fail fast
    with a clear reason.
    """

    if not sys.stdin.isatty() or not sys.stdout.isatty():
        return False, "stdin and stdout must be TTYs"
    term = (os.environ.get("TERM") or "").strip().lower()
    if term in ("dumb", ""):
        return False, "TERM is missing or set to dumb"
    try:
        sz = shutil.get_terminal_size()
    except OSError:
        return False, "terminal size is unavailable"
    if sz.lines < _MIN_TUI_ROWS:
        return (
            False,
            f"terminal height is {sz.lines} rows; need at least {_MIN_TUI_ROWS}",
        )
    return True, ""


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


def status_attr_key(status: str) -> str:
    """Map plan step status to a key in the color map from ``_init_status_attrs``."""

    s = status.strip()
    if s in ("done", "in_progress", "failed", "skipped"):
        return s
    return "other"


def clamp_scroll_offset(scroll_offset: int, *, line_count: int, viewport_height: int) -> int:
    if viewport_height <= 0 or line_count <= 0:
        return 0
    max_offset = max(0, line_count - viewport_height)
    return max(0, min(scroll_offset, max_offset))


def ensure_line_visible(
    scroll_offset: int,
    *,
    line_index: int,
    viewport_height: int,
    line_count: int,
) -> int:
    """Adjust scroll so line ``line_index`` lies inside the viewport."""

    if viewport_height <= 0 or line_count <= 0:
        return 0
    new = scroll_offset
    if line_index < scroll_offset:
        new = line_index
    elif line_index >= scroll_offset + viewport_height:
        new = line_index - viewport_height + 1
    return clamp_scroll_offset(new, line_count=line_count, viewport_height=viewport_height)


def step_indices_matching_filter(steps: Sequence[PlanStep], query: str) -> list[int]:
    q = query.strip().lower()
    if not q:
        return list(range(len(steps)))
    return [i for i, s in enumerate(steps) if q in (s.title or "").lower()]


def move_in_filtered_list(current: int, matches: Sequence[int], delta: int) -> int:
    """Move selection by ``delta`` (+1 / -1) within ``matches`` (wrapping)."""

    mlist = list(matches)
    if not mlist:
        return current
    if current not in mlist:
        return mlist[0]
    pos = mlist.index(current)
    new_pos = (pos + delta) % len(mlist)
    return mlist[new_pos]


def next_filtered_step_index(current: int, matches: Sequence[int], *, direction: int) -> int:
    """
    Next (direction > 0) or previous (direction < 0) match in sorted order, wrapping.
    Used for ``n`` / ``N`` when a substring filter is active.
    """

    mlist = sorted(matches)
    if not mlist:
        return current
    if direction > 0:
        for x in mlist:
            if x > current:
                return x
        return mlist[0]
    for x in reversed(mlist):
        if x < current:
            return x
    return mlist[-1]


def _init_status_attrs() -> dict[str, int] | None:
    if not curses.has_colors():
        return None
    try:
        curses.start_color()
    except curses.error:
        return None
    with contextlib.suppress(curses.error):
        curses.use_default_colors()
    try:
        curses.init_pair(1, curses.COLOR_GREEN, -1)
        curses.init_pair(2, curses.COLOR_YELLOW, -1)
        curses.init_pair(3, curses.COLOR_RED, -1)
        curses.init_pair(4, curses.COLOR_BLUE, -1)
        curses.init_pair(5, curses.COLOR_WHITE, -1)
    except curses.error:
        return None
    return {
        "done": curses.color_pair(1) | curses.A_BOLD,
        "in_progress": curses.color_pair(2) | curses.A_BOLD,
        "failed": curses.color_pair(3) | curses.A_BOLD,
        "skipped": curses.color_pair(4),
        "other": curses.color_pair(5),
    }


Segment = tuple[str, int]


def _step_line_segments(
    step: PlanStep,
    *,
    is_selected: bool,
    status_attrs: dict[str, int] | None,
) -> list[Segment]:
    prefix = ">" if is_selected else " "
    mark = curses.A_REVERSE if is_selected else curses.A_NORMAL
    badge = _status_badge(step.status)
    key = status_attr_key(step.status)
    badge_attr = 0
    if status_attrs is not None:
        badge_attr = status_attrs.get(key, status_attrs["other"])
    return [
        (prefix, mark),
        (" ", mark),
        (badge, badge_attr | mark),
        (f" {step.title}", mark),
    ]


def _add_segments(stdscr: curses.window, row: int, col: int, segments: Sequence[Segment], width: int) -> None:
    x = col
    for text, attr in segments:
        if x >= width - 1:
            break
        chunk = text
        room = max(0, width - 1 - x)
        if len(chunk) > room:
            chunk = chunk[:room]
        if chunk:
            stdscr.addstr(row, x, chunk, attr)
            x += len(chunk)


def _add_plain(stdscr: curses.window, row: int, text: str, width: int, attr: int = 0) -> None:
    clip = text[: max(0, width - 1)]
    if clip:
        stdscr.addnstr(row, 0, clip, max(0, width - 1), attr)


BodyLine = str | list[Segment]


def _render_frame(
    stdscr: curses.window,
    *,
    header: list[str],
    body_lines: Sequence[BodyLine],
    footer: list[str],
    body_scroll: int,
) -> None:
    stdscr.erase()
    h, w = stdscr.getmaxyx()
    header_h = len(header)
    footer_h = len(footer)
    body_h = max(1, h - header_h - footer_h)

    for i, line in enumerate(header):
        if i >= h:
            break
        _add_plain(stdscr, i, line, w, 0)

    start = clamp_scroll_offset(body_scroll, line_count=len(body_lines), viewport_height=body_h)
    for j in range(body_h):
        row = header_h + j
        if row >= h - footer_h:
            break
        idx = start + j
        if idx >= len(body_lines):
            continue
        item = body_lines[idx]
        if isinstance(item, str):
            _add_plain(stdscr, row, item, w, 0)
        else:
            _add_segments(stdscr, row, 0, item, w)

    for k, line in enumerate(footer):
        row = h - footer_h + k
        if row < 0 or row >= h:
            continue
        _add_plain(stdscr, row, line, w, curses.A_DIM)

    stdscr.refresh()


def _build_evidence_list_lines(
    ev: list[EvidenceRef],
    *,
    selected_idx: int,
    scoped_root: Path,
) -> list[str]:
    list_lines: list[str] = ["Evidence (relative paths):", ""]
    if not ev:
        list_lines.append("(no evidence files for selected step)")
    else:
        for i, ev_ref in enumerate(ev):
            prefix = ">" if i == selected_idx else " "
            list_lines.append(f"{prefix} {ev_ref.relpath} ({ev_ref.kind})")
        list_lines.append("")
        ref_sel = ev[selected_idx]
        list_lines.append(f"Selected: {ref_sel.relpath}")
        list_lines.append(str(scoped_root / ref_sel.relpath))
        list_lines.append("Keys: , . preview scroll  v fullscreen  PgUp/PgDn")
    return list_lines


def _evidence_split_heights(body_h: int) -> tuple[int, int, int]:
    """Returns (list_h, separator_h, preview_h)."""

    sep = 1 if body_h > 4 else 0
    list_h = max(2, int(body_h * 0.4))
    preview_h = body_h - list_h - sep
    if preview_h < 2:
        preview_h = 2
        list_h = max(2, body_h - preview_h - sep)
    return list_h, sep, preview_h


def _render_evidence_split(
    stdscr: curses.window,
    *,
    header: list[str],
    footer: list[str],
    ev: list[EvidenceRef],
    selected_idx: int,
    list_scroll: int,
    preview_scroll: int,
    preview_text: str,
    scoped_root: Path,
) -> None:
    stdscr.erase()
    h, w = stdscr.getmaxyx()
    header_h = len(header)
    footer_h = len(footer)
    body_h = max(1, h - header_h - footer_h)
    list_h, sep_h, preview_h = _evidence_split_heights(body_h)

    for i, line in enumerate(header):
        if i >= h:
            break
        _add_plain(stdscr, i, line, w, 0)

    body_top = header_h
    preview_lines = preview_text.splitlines()
    list_lines = _build_evidence_list_lines(ev, selected_idx=selected_idx, scoped_root=scoped_root)

    list_scroll = clamp_scroll_offset(list_scroll, line_count=len(list_lines), viewport_height=list_h)
    for j in range(list_h):
        r = body_top + j
        if r >= h - footer_h:
            break
        idx = list_scroll + j
        if idx < len(list_lines):
            _add_plain(stdscr, r, list_lines[idx], w, 0)

    sep_row = body_top + list_h
    if sep_h and sep_row < h - footer_h:
        _add_plain(stdscr, sep_row, "─" * max(0, w - 1), w, curses.A_DIM)

    preview_top = sep_row + sep_h
    prev_head = ["Preview (read-only):", ""]
    prev_body = preview_lines
    combined = prev_head + prev_body
    preview_scroll = clamp_scroll_offset(preview_scroll, line_count=len(combined), viewport_height=preview_h)
    for j in range(preview_h):
        r = preview_top + j
        if r >= h - footer_h:
            break
        idx = preview_scroll + j
        if idx < len(combined):
            _add_plain(stdscr, r, combined[idx], w, 0)

    for k, line in enumerate(footer):
        row = h - footer_h + k
        if 0 <= row < h:
            _add_plain(stdscr, row, line, w, curses.A_DIM)

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


def _build_knowledge_body(state: ViewerSnapshot) -> list[str]:
    body = ["Knowledge layer (read-only)", ""]
    env = state.knowledge_envelope
    if env is None:
        body.append("No `.akc/knowledge/snapshot.json` in this scope.")
    else:
        inner = env.get("snapshot")
        n_c = 0
        if isinstance(inner, dict):
            cc = inner.get("canonical_constraints")
            if isinstance(cc, list):
                n_c = len(cc)
        body.append(f"Persisted knowledge envelope: yes (constraints≈{n_c})")
    body.append(f"Conflict reports (code memory): {len(state.conflict_reports)}")
    obs = build_knowledge_observation_payload(
        knowledge_envelope=state.knowledge_envelope,
        conflict_reports=state.conflict_reports,
        knowledge_mediation_envelope=state.knowledge_mediation_envelope,
    )
    body.append(
        f"Mediation events: {len(obs['mediation_events'])}  "
        f"Unresolved (distinct groups): {obs['unresolved_knowledge_conflicts_count']}  "
        f"Supersession hints: {len(obs['supersession_hints'])}"
    )
    body.append("Knowledge paths: " + ", ".join(f"{k}={v}" for k, v in sorted(obs["knowledge_paths"].items())))
    if state.conflict_reports:
        body.append("")
        body.append("Latest conflict summaries:")
        for cr in state.conflict_reports[:12]:
            summ = str(cr.get("summary", "")).strip()
            cid = str(cr.get("conflict_id", ""))[:10]
            body.append(f"- {cid}… {summ[:120]}")
    return body


def _build_profile_body(state: ViewerSnapshot) -> list[str]:
    body = ["Profile / developer decisions (read-only)", ""]
    op = state.operator_panels or {}
    pp = op.get("profile_panel") if isinstance(op, dict) else None
    sc = pp.get("scope_context") if isinstance(pp, dict) else None
    has_scope = isinstance(sc, dict) and (sc.get("run_id") or sc.get("control_followup_cli") or sc.get("tenant_id"))
    if not isinstance(pp, dict) or (not pp.get("available") and not has_scope):
        body.append("No manifest control_plane / developer_profile_decisions in this scope.")
    else:
        try:
            dumped = json.dumps(pp, indent=2, sort_keys=True, ensure_ascii=False)
        except (TypeError, ValueError):
            dumped = str(pp)
        body.extend(dumped.splitlines()[:400])
    return body


def _footer_lines(
    *,
    mode: str,
    filter_editing: bool,
    filter_buffer: str,
    step_filter: str,
) -> list[str]:
    keys = (
        "q quit  h/l [] mode  / filter  n/N match  Enter evidence  "
        "b back  o knowledge  p profile  v preview  PgUp/Dn scroll"
    )
    if filter_editing:
        return [keys, f"Filter (Enter apply, Esc cancel): {filter_buffer}_"]
    if step_filter and mode == "steps":
        return [keys, f"Active filter: {step_filter!r}"]
    return [keys, f"Mode: {mode}"]


def _cycle_mode(current: str, delta: int) -> str:
    idx = _MODE_ORDER.index(current) if current in _MODE_ORDER else 0
    return _MODE_ORDER[(idx + delta) % len(_MODE_ORDER)]


def run_tui(snapshot: ViewerSnapshot) -> int:
    """Run a curses TUI for the snapshot (read-only)."""

    @dataclass(slots=True)
    class _TuiState:
        snap: ViewerSnapshot
        selected_step_idx: int = 0
        mode: str = "steps"
        selected_evidence_idx: int = 0
        body_scroll: int = 0
        evidence_list_scroll: int = 0
        evidence_preview_scroll: int = 0
        step_filter: str = ""
        filter_editing: bool = False
        filter_buffer: str = ""

    state = _TuiState(snap=snapshot)

    def _loop(stdscr: curses.window) -> int:
        curses.curs_set(0)
        stdscr.keypad(True)
        status_attrs = _init_status_attrs()

        while True:
            plan = state.snap.plan
            steps = list(plan.steps)
            header = [
                f"AKC viewer (read-only) — {plan.tenant_id}/{plan.repo_id}",
                f"Goal: {plan.goal}",
                f"Plan: {plan.status}  Updated: {plan.updated_at_ms}  Steps: {len(steps)}",
            ]
            footer = _footer_lines(
                mode=state.mode,
                filter_editing=state.filter_editing,
                filter_buffer=state.filter_buffer,
                step_filter=state.step_filter,
            )

            if state.mode == "knowledge":
                body = _build_knowledge_body(state.snap)
                h, w = stdscr.getmaxyx()
                body_h = max(1, h - len(header) - len(footer))
                state.body_scroll = clamp_scroll_offset(state.body_scroll, line_count=len(body), viewport_height=body_h)
                _render_frame(
                    stdscr,
                    header=header,
                    body_lines=body,
                    footer=footer,
                    body_scroll=state.body_scroll,
                )
            elif state.mode == "profile":
                body = _build_profile_body(state.snap)
                h, w = stdscr.getmaxyx()
                body_h = max(1, h - len(header) - len(footer))
                state.body_scroll = clamp_scroll_offset(state.body_scroll, line_count=len(body), viewport_height=body_h)
                _render_frame(
                    stdscr,
                    header=header,
                    body_lines=body,
                    footer=footer,
                    body_scroll=state.body_scroll,
                )
            elif state.mode == "steps":
                matches = step_indices_matching_filter(steps, state.step_filter)
                if steps:
                    if state.selected_step_idx not in matches and matches:
                        state.selected_step_idx = matches[0]
                    elif not matches:
                        pass
                    else:
                        state.selected_step_idx = max(0, min(state.selected_step_idx, len(steps) - 1))
                steps_body: list[BodyLine] = []
                if not steps:
                    steps_body.append("(no steps in plan)")
                elif not matches:
                    steps_body.append("(no steps match filter)")
                    steps_body.append("Press / to edit filter")
                else:
                    sel = state.selected_step_idx
                    for i in matches:
                        s = steps[i]
                        steps_body.append(
                            _step_line_segments(
                                s,
                                is_selected=(i == sel),
                                status_attrs=status_attrs,
                            )
                        )
                    steps_body.append("")
                    step = steps[sel] if sel < len(steps) else None
                    if step is not None:
                        ev = state.snap.evidence.by_step.get(step.id, [])
                        steps_body.append(f"Evidence files linked to this step: {len(ev)}")
                h, w = stdscr.getmaxyx()
                body_h = max(1, h - len(header) - len(footer))
                vis_count = len(steps_body)
                if matches and steps:
                    try:
                        pos = matches.index(state.selected_step_idx)
                    except ValueError:
                        pos = 0
                    state.body_scroll = ensure_line_visible(
                        state.body_scroll,
                        line_index=pos,
                        viewport_height=body_h,
                        line_count=vis_count,
                    )
                else:
                    state.body_scroll = clamp_scroll_offset(
                        state.body_scroll, line_count=vis_count, viewport_height=body_h
                    )
                _render_frame(
                    stdscr,
                    header=header,
                    body_lines=steps_body,
                    footer=footer,
                    body_scroll=state.body_scroll,
                )
            else:
                step_sel: PlanStep | None = steps[state.selected_step_idx] if steps else None
                ev = state.snap.evidence.by_step.get(step_sel.id, []) if step_sel else []
                sel = max(0, min(state.selected_evidence_idx, max(0, len(ev) - 1)))
                state.selected_evidence_idx = sel
                ref = ev[sel] if ev else None
                fp = state.snap.scoped_outputs_dir / ref.relpath if ref is not None else None
                preview = _read_text_preview(fp) if fp is not None else ""
                h, w = stdscr.getmaxyx()
                body_h = max(1, h - len(header) - len(footer))
                list_h, _sep_h, preview_h = _evidence_split_heights(body_h)
                list_lines = _build_evidence_list_lines(ev, selected_idx=sel, scoped_root=state.snap.scoped_outputs_dir)
                list_line_for_sel = 2 + sel if ev else 2
                state.evidence_list_scroll = clamp_scroll_offset(
                    state.evidence_list_scroll,
                    line_count=len(list_lines),
                    viewport_height=list_h,
                )
                state.evidence_list_scroll = ensure_line_visible(
                    state.evidence_list_scroll,
                    line_index=list_line_for_sel,
                    viewport_height=list_h,
                    line_count=max(1, len(list_lines)),
                )
                prev_lines = 2 + max(1, len(preview.splitlines()))
                state.evidence_preview_scroll = clamp_scroll_offset(
                    state.evidence_preview_scroll,
                    line_count=prev_lines,
                    viewport_height=preview_h,
                )
                _render_evidence_split(
                    stdscr,
                    header=header,
                    footer=footer,
                    ev=ev,
                    selected_idx=sel,
                    list_scroll=state.evidence_list_scroll,
                    preview_scroll=state.evidence_preview_scroll,
                    preview_text=preview,
                    scoped_root=state.snap.scoped_outputs_dir,
                )

            ch = stdscr.getch()
            if ch in (ord("q"), 27) and not state.filter_editing:
                return 0
            if state.filter_editing:
                if ch in (27,):
                    state.filter_editing = False
                    state.filter_buffer = state.step_filter
                elif ch in (curses.KEY_ENTER, 10, 13):
                    state.step_filter = state.filter_buffer
                    state.filter_editing = False
                    applied = step_indices_matching_filter(steps, state.step_filter)
                    state.selected_step_idx = applied[0] if applied else 0
                elif ch in (curses.KEY_BACKSPACE, 127, 8):
                    state.filter_buffer = state.filter_buffer[:-1]
                elif 32 <= ch < 127:
                    state.filter_buffer += chr(ch)
                continue

            if ch in (ord("h"), ord("[")):
                state.mode = _cycle_mode(state.mode, -1)
                state.body_scroll = 0
                continue
            if ch in (ord("l"), ord("]")):
                state.mode = _cycle_mode(state.mode, 1)
                state.body_scroll = 0
                continue

            if ch == ord("/") and state.mode == "steps":
                state.filter_editing = True
                state.filter_buffer = state.step_filter
                continue

            matches = step_indices_matching_filter(steps, state.step_filter)
            if ch == ord("n") and state.mode == "steps" and state.step_filter.strip():
                state.selected_step_idx = next_filtered_step_index(state.selected_step_idx, matches, direction=1)
                continue
            if ch == ord("N") and state.mode == "steps" and state.step_filter.strip():
                state.selected_step_idx = next_filtered_step_index(state.selected_step_idx, matches, direction=-1)
                continue

            if state.mode == "knowledge":
                if ch == ord("b"):
                    state.mode = "steps"
                elif ch == ord("p"):
                    state.mode = "profile"
                elif ch == curses.KEY_UP:
                    state.body_scroll = max(0, state.body_scroll - 1)
                elif ch == curses.KEY_DOWN:
                    state.body_scroll += 1
                elif ch == curses.KEY_PPAGE:
                    h, _w = stdscr.getmaxyx()
                    fh = len(
                        _footer_lines(
                            mode="knowledge",
                            filter_editing=False,
                            filter_buffer="",
                            step_filter=state.step_filter,
                        )
                    )
                    body_h = max(1, h - len(header) - fh)
                    state.body_scroll = max(0, state.body_scroll - body_h)
                elif ch == curses.KEY_NPAGE:
                    h, _w = stdscr.getmaxyx()
                    fh = len(
                        _footer_lines(
                            mode="knowledge",
                            filter_editing=False,
                            filter_buffer="",
                            step_filter=state.step_filter,
                        )
                    )
                    body_h = max(1, h - len(header) - fh)
                    state.body_scroll += max(1, body_h)
                continue

            if state.mode == "profile":
                if ch == ord("b"):
                    state.mode = "steps"
                elif ch == ord("o"):
                    state.mode = "knowledge"
                elif ch == curses.KEY_UP:
                    state.body_scroll = max(0, state.body_scroll - 1)
                elif ch == curses.KEY_DOWN:
                    state.body_scroll += 1
                elif ch == curses.KEY_PPAGE:
                    h, _w = stdscr.getmaxyx()
                    fh = len(
                        _footer_lines(
                            mode="profile",
                            filter_editing=False,
                            filter_buffer="",
                            step_filter=state.step_filter,
                        )
                    )
                    body_h = max(1, h - len(header) - fh)
                    state.body_scroll = max(0, state.body_scroll - body_h)
                elif ch == curses.KEY_NPAGE:
                    h, _w = stdscr.getmaxyx()
                    fh = len(
                        _footer_lines(
                            mode="profile",
                            filter_editing=False,
                            filter_buffer="",
                            step_filter=state.step_filter,
                        )
                    )
                    body_h = max(1, h - len(header) - fh)
                    state.body_scroll += max(1, body_h)
                continue

            if state.mode == "steps":
                if ch == curses.KEY_UP:
                    if matches:
                        state.selected_step_idx = move_in_filtered_list(state.selected_step_idx, matches, -1)
                elif ch == curses.KEY_DOWN:
                    if matches:
                        state.selected_step_idx = move_in_filtered_list(state.selected_step_idx, matches, 1)
                elif ch in (curses.KEY_ENTER, 10, 13):
                    state.mode = "evidence"
                    state.selected_evidence_idx = 0
                    state.evidence_list_scroll = 0
                    state.evidence_preview_scroll = 0
                elif ch == ord("o"):
                    state.mode = "knowledge"
                    state.body_scroll = 0
                elif ch == ord("p"):
                    state.mode = "profile"
                    state.body_scroll = 0
                continue

            step_sel2: PlanStep | None = steps[state.selected_step_idx] if steps else None
            ev = state.snap.evidence.by_step.get(step_sel2.id, []) if step_sel2 else []
            if ch == ord("b"):
                state.mode = "steps"
            elif ch == ord("o"):
                state.mode = "knowledge"
                state.body_scroll = 0
            elif ch == ord("p"):
                state.mode = "profile"
                state.body_scroll = 0
            elif ch == curses.KEY_UP and ev:
                state.selected_evidence_idx = max(0, state.selected_evidence_idx - 1)
                state.evidence_preview_scroll = 0
            elif ch == curses.KEY_DOWN and ev:
                state.selected_evidence_idx = min(len(ev) - 1, state.selected_evidence_idx + 1)
                state.evidence_preview_scroll = 0
            elif ch == ord(",") and ev:
                state.evidence_preview_scroll = max(0, state.evidence_preview_scroll - 1)
            elif ch == ord(".") and ev:
                state.evidence_preview_scroll += 1
            elif ch == curses.KEY_PPAGE and ev:
                h, _w = stdscr.getmaxyx()
                fh = len(
                    _footer_lines(
                        mode="evidence",
                        filter_editing=False,
                        filter_buffer="",
                        step_filter=state.step_filter,
                    )
                )
                body_h = max(1, h - len(header) - fh)
                _list_h, _sep, prev_h = _evidence_split_heights(body_h)
                state.evidence_preview_scroll = max(0, state.evidence_preview_scroll - max(1, prev_h - 1))
            elif ch == curses.KEY_NPAGE and ev:
                h, _w = stdscr.getmaxyx()
                fh = len(
                    _footer_lines(
                        mode="evidence",
                        filter_editing=False,
                        filter_buffer="",
                        step_filter=state.step_filter,
                    )
                )
                body_h = max(1, h - len(header) - fh)
                _list_h, _sep, prev_h = _evidence_split_heights(body_h)
                state.evidence_preview_scroll += max(1, prev_h - 1)
            elif ch == ord("v") and ev:
                ref_fs: EvidenceRef = ev[state.selected_evidence_idx]
                fp2 = state.snap.scoped_outputs_dir / ref_fs.relpath
                preview_full = _read_text_preview(fp2)
                full_header = header + [
                    f"Preview (fullscreen): {ref_fs.relpath}",
                    "─" * 40,
                ]
                h2, w2 = stdscr.getmaxyx()
                foot2 = ["Any key to return"]
                body_h2 = max(1, h2 - len(full_header) - len(foot2))
                scroll_fs = 0
                plines = preview_full.splitlines()
                while True:
                    scroll_fs = clamp_scroll_offset(
                        scroll_fs,
                        line_count=len(plines),
                        viewport_height=body_h2,
                    )
                    _render_frame(
                        stdscr,
                        header=full_header,
                        body_lines=plines,
                        footer=foot2,
                        body_scroll=scroll_fs,
                    )
                    ch2 = stdscr.getch()
                    if ch2 == curses.KEY_UP:
                        scroll_fs = max(0, scroll_fs - 1)
                    elif ch2 == curses.KEY_DOWN:
                        scroll_fs += 1
                    elif ch2 == curses.KEY_PPAGE:
                        scroll_fs = max(0, scroll_fs - body_h2)
                    elif ch2 == curses.KEY_NPAGE:
                        scroll_fs += body_h2
                    else:
                        break

        return 0  # pragma: no cover

    try:
        return int(curses.wrapper(_loop))
    except curses.error as e:
        raise TuiError("curses TUI could not be initialized (not a TTY?)") from e
