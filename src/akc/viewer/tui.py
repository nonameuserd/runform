from __future__ import annotations

import curses
import json
from dataclasses import dataclass
from pathlib import Path

from akc.knowledge.observability import build_knowledge_observation_payload
from akc.memory.models import PlanStep

from .models import EvidenceRef, ViewerSnapshot


class TuiError(Exception):
    """Raised when the TUI cannot be started."""


@dataclass(slots=True)
class _TuiState:
    snap: ViewerSnapshot
    selected_step_idx: int = 0
    mode: str = "steps"  # steps | evidence | knowledge | profile
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
                "Keys: ↑/↓ move  Enter open  b back  v view file  o knowledge  p profile  q quit",
                "",
            ]

            if state.mode == "knowledge":
                body = ["Knowledge layer (read-only)", ""]
                env = state.snap.knowledge_envelope
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
                body.append(f"Conflict reports (code memory): {len(state.snap.conflict_reports)}")
                obs = build_knowledge_observation_payload(
                    knowledge_envelope=state.snap.knowledge_envelope,
                    conflict_reports=state.snap.conflict_reports,
                    knowledge_mediation_envelope=state.snap.knowledge_mediation_envelope,
                )
                body.append(
                    f"Mediation events: {len(obs['mediation_events'])}  "
                    f"Unresolved (distinct groups): {obs['unresolved_knowledge_conflicts_count']}  "
                    f"Supersession hints: {len(obs['supersession_hints'])}"
                )
                body.append(
                    "Knowledge paths: " + ", ".join(f"{k}={v}" for k, v in sorted(obs["knowledge_paths"].items()))
                )
                if state.snap.conflict_reports:
                    body.append("")
                    body.append("Latest conflict summaries:")
                    for cr in state.snap.conflict_reports[:12]:
                        summ = str(cr.get("summary", "")).strip()
                        cid = str(cr.get("conflict_id", ""))[:10]
                        line = f"- {cid}… {summ[:120]}"
                        body.append(line)
                body.append("")
                body.append("Press b to return.")
                _render_lines(stdscr, lines=header + body)
            elif state.mode == "profile":
                body = ["Profile / developer decisions (read-only)", ""]
                op = state.snap.operator_panels or {}
                pp = op.get("profile_panel") if isinstance(op, dict) else None
                sc = pp.get("scope_context") if isinstance(pp, dict) else None
                has_scope = isinstance(sc, dict) and (
                    sc.get("run_id") or sc.get("control_followup_cli") or sc.get("tenant_id")
                )
                if not isinstance(pp, dict) or (not pp.get("available") and not has_scope):
                    body.append("No manifest control_plane / developer_profile_decisions in this scope.")
                else:
                    try:
                        dumped = json.dumps(pp, indent=2, sort_keys=True, ensure_ascii=False)
                    except (TypeError, ValueError):
                        dumped = str(pp)
                    body.extend(dumped.splitlines()[:400])
                body.append("")
                body.append("Press b to return.")
                _render_lines(stdscr, lines=header + body)
            elif state.mode == "steps":
                steps_body: list[str] = []
                if not steps:
                    steps_body = ["(no steps in plan)"]
                else:
                    sel = max(0, min(state.selected_step_idx, len(steps) - 1))
                    state.selected_step_idx = sel
                    for i, s in enumerate(steps):
                        steps_body.append(_step_line(s, is_selected=(i == sel)))
                    steps_body.append("")
                    step = steps[sel]
                    ev = state.snap.evidence.by_step.get(step.id, [])
                    steps_body.append(f"Evidence files linked to this step: {len(ev)}")
                _render_lines(stdscr, lines=header + steps_body)
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
                    for i, ev_ref in enumerate(ev):
                        prefix = ">" if i == sel else " "
                        body.append(f"{prefix} {ev_ref.relpath} ({ev_ref.kind})")
                    body.append("")
                    ref_sel = ev[sel]
                    body.append(f"Selected: {ref_sel.relpath}")
                    body.append("Tip: press 'v' to preview text files.")
                _render_lines(stdscr, lines=header + body)

            ch = stdscr.getch()
            if ch in (ord("q"), 27):  # q or ESC
                return 0
            if state.mode == "knowledge":
                if ch == ord("b"):
                    state.mode = "steps"
                elif ch == ord("p"):
                    state.mode = "profile"
            elif state.mode == "profile":
                if ch == ord("b"):
                    state.mode = "steps"
                elif ch == ord("o"):
                    state.mode = "knowledge"
            elif state.mode == "steps":
                if ch == curses.KEY_UP:
                    state.selected_step_idx = max(0, state.selected_step_idx - 1)
                elif ch == curses.KEY_DOWN:
                    state.selected_step_idx = min(len(steps) - 1, state.selected_step_idx + 1)
                elif ch in (curses.KEY_ENTER, 10, 13):
                    state.mode = "evidence"
                    state.selected_evidence_idx = 0
                elif ch == ord("o"):
                    state.mode = "knowledge"
                elif ch == ord("p"):
                    state.mode = "profile"
            else:
                step_sel2: PlanStep | None = steps[state.selected_step_idx] if steps else None
                ev = state.snap.evidence.by_step.get(step_sel2.id, []) if step_sel2 else []
                if ch == ord("b"):
                    state.mode = "steps"
                elif ch == ord("o"):
                    state.mode = "knowledge"
                elif ch == ord("p"):
                    state.mode = "profile"
                elif ch == curses.KEY_UP:
                    state.selected_evidence_idx = max(0, state.selected_evidence_idx - 1)
                elif ch == curses.KEY_DOWN:
                    state.selected_evidence_idx = min(max(0, len(ev) - 1), state.selected_evidence_idx + 1)
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
