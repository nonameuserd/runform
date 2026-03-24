from __future__ import annotations

# ruff: noqa: E501
import contextlib
import json
from dataclasses import dataclass
from pathlib import Path

from akc.knowledge.observability import build_knowledge_observation_payload

from .export import _safe_copy
from .models import ViewerSnapshot
from .snapshot import ViewerError


@dataclass(frozen=True, slots=True)
class WebBuildResult:
    root: Path
    index_html: Path
    copied_files: int


_INDEX_HTML = """<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <title>AKC viewer (read-only)</title>
    <style>
      :root {
        --bg: #0b0e14;
        --panel: #111726;
        --muted: #93a4c7;
        --text: #e6eefc;
        --ok: #2dd4bf;
        --warn: #fbbf24;
        --bad: #fb7185;
        --line: rgba(255,255,255,.08);
        --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono",
          "Courier New", monospace;
        --sans: ui-sans-serif, system-ui, -apple-system, Segoe UI, sans-serif;
      }
      body { margin: 0; background: var(--bg); color: var(--text); font-family: var(--sans); }
      header { padding: 16px 20px; border-bottom: 1px solid var(--line); background: rgba(255,255,255,.02); }
      header h1 { margin: 0 0 6px; font-size: 16px; letter-spacing: .02em; }
      header .meta { color: var(--muted); font-size: 13px; }
      main { display: grid; grid-template-columns: 360px 1fr; gap: 14px; padding: 14px; }
      .card { background: var(--panel); border: 1px solid var(--line); border-radius: 12px; overflow: hidden; }
      .card h2 { margin: 0; padding: 12px 14px; font-size: 13px; color: var(--muted); border-bottom: 1px solid var(--line); }
      .list { max-height: calc(100vh - 130px); overflow: auto; }
      .item { padding: 12px 14px; border-bottom: 1px solid var(--line); cursor: pointer; }
      .item:hover { background: rgba(255,255,255,.03); }
      .item.sel { background: rgba(45, 212, 191, .10); }
      .badge { display: inline-block; font-family: var(--mono); font-size: 11px; padding: 2px 8px; border-radius: 999px; border: 1px solid var(--line); margin-right: 8px; }
      .badge.ok { color: var(--ok); }
      .badge.bad { color: var(--bad); }
      .badge.warn { color: var(--warn); }
      .title { font-size: 13px; }
      .detail { padding: 14px; }
      .detail pre { background: rgba(0,0,0,.25); border: 1px solid var(--line); border-radius: 10px; padding: 10px; overflow: auto; font-family: var(--mono); font-size: 12px; }
      .detail pre.compact { max-height: 200px; }
      .actions a { display: inline-block; margin-right: 10px; margin-top: 8px; color: var(--text); text-decoration: none; border: 1px solid var(--line); padding: 8px 10px; border-radius: 10px; font-size: 12px; }
      .actions a:hover { background: rgba(255,255,255,.04); }
      .muted { color: var(--muted); font-size: 12px; }
    </style>
  </head>
  <body>
    <header>
      <h1>AKC viewer (read-only)</h1>
      <div class="meta" id="meta"></div>
    </header>
    <main>
      <section class="card">
        <h2>Plan steps</h2>
        <div class="list" id="steps"></div>
      </section>
      <section class="card">
        <h2>Details</h2>
        <div class="detail">
          <div class="actions" id="downloads"></div>
          <div class="muted" id="hint"></div>
          <h3 style="margin: 14px 0 8px; font-size: 13px; color: var(--muted);">Knowledge (compile/runtime debug)</h3>
          <pre class="compact" id="knowledge_block">(loading…)</pre>
          <h3 style="margin: 14px 0 8px; font-size: 13px; color: var(--muted);">Mediation events</h3>
          <pre class="compact" id="mediation_block">(loading…)</pre>
          <h3 style="margin: 14px 0 8px; font-size: 13px; color: var(--muted);">Conflict groups</h3>
          <pre class="compact" id="groups_block">(loading…)</pre>
          <h3 style="margin: 14px 0 8px; font-size: 13px; color: var(--muted);">Supersession hints</h3>
          <pre class="compact" id="supersession_block">(loading…)</pre>
          <h3 style="margin: 14px 0 8px; font-size: 13px; color: var(--muted);">Conflict reports</h3>
          <pre class="compact" id="conflict_block">(loading…)</pre>
          <h3 style="margin: 14px 0 8px; font-size: 13px; color: var(--muted);">Control plane — forensics summary (read-only)</h3>
          <pre class="compact" id="forensics_panel">(loading…)</pre>
          <h3 style="margin: 14px 0 8px; font-size: 13px; color: var(--muted);">Control plane — playbook report (read-only)</h3>
          <pre class="compact" id="playbook_panel">(loading…)</pre>
          <h3 style="margin: 14px 0 8px; font-size: 13px; color: var(--muted);">Autopilot scope (read-only)</h3>
          <pre class="compact" id="autopilot_panel">(loading…)</pre>
          <h3 style="margin: 14px 0 8px; font-size: 13px; color: var(--muted);">Profile / developer decisions (read-only)</h3>
          <pre class="compact" id="profile_panel">(loading…)</pre>
          <h3 style="margin: 14px 0 8px; font-size: 13px; color: var(--muted);">Evidence files</h3>
          <div class="list" id="evidence"></div>
          <h3 style="margin: 14px 0 8px; font-size: 13px; color: var(--muted);">Step notes</h3>
          <pre id="notes">(select a step)</pre>
        </div>
      </section>
    </main>
    <script>
      const loadJson = async (p) => (await fetch(p)).json();
      const badgeClass = (st) => st === "done" ? "ok" : (st === "failed" ? "bad" : (st === "in_progress" ? "warn" : ""));
      const esc = (s) => (s ?? "").toString();

      const main = async () => {
        const plan = await loadJson("./data/plan.json");
        let manifest = null;
        try { manifest = await loadJson("./data/manifest.json"); } catch (e) {}

        const meta = document.getElementById("meta");
        meta.textContent = `${plan.tenant_id}/${plan.repo_id} — ${plan.status} — steps=${(plan.steps||[]).length}`;

        const stepsEl = document.getElementById("steps");
        const evEl = document.getElementById("evidence");
        const notesEl = document.getElementById("notes");
        const downloadsEl = document.getElementById("downloads");
        const hintEl = document.getElementById("hint");
        const knowEl = document.getElementById("knowledge_block");
        const medEl = document.getElementById("mediation_block");
        const grpEl = document.getElementById("groups_block");
        const supEl = document.getElementById("supersession_block");
        const confEl = document.getElementById("conflict_block");
        const forensicsEl = document.getElementById("forensics_panel");
        const playbookEl = document.getElementById("playbook_panel");
        const profileEl = document.getElementById("profile_panel");
        const autopilotEl = document.getElementById("autopilot_panel");

        let kobs = null;
        try { kobs = await loadJson("./data/knowledge_obs.json"); } catch (e) {}
        if (kobs && kobs.knowledge_envelope) {
          knowEl.textContent = JSON.stringify(kobs.knowledge_envelope, null, 2);
        } else {
          knowEl.textContent = "(no persisted knowledge snapshot in this export)";
        }
        if (kobs && Array.isArray(kobs.mediation_events) && kobs.mediation_events.length) {
          medEl.textContent = JSON.stringify(kobs.mediation_events, null, 2);
        } else {
          medEl.textContent = "(no mediation events — missing or empty .akc/knowledge/mediation.json)";
        }
        if (kobs && kobs.conflict_groups && Object.keys(kobs.conflict_groups).length) {
          grpEl.textContent = JSON.stringify(kobs.conflict_groups, null, 2);
        } else {
          grpEl.textContent = "(no grouped mediation events)";
        }
        if (kobs && Array.isArray(kobs.supersession_hints) && kobs.supersession_hints.length) {
          supEl.textContent = JSON.stringify(kobs.supersession_hints, null, 2);
        } else {
          supEl.textContent = "(no supersession hints in mediation events)";
        }
        let hintBase = "Evidence items open a local copied file (no execution).";
        if (kobs && typeof kobs.unresolved_knowledge_conflicts_count === "number") {
          hintBase = `Unresolved knowledge conflicts (distinct groups): ${kobs.unresolved_knowledge_conflicts_count} — ${hintBase}`;
        }
        if (kobs && Array.isArray(kobs.conflict_reports) && kobs.conflict_reports.length) {
          confEl.textContent = JSON.stringify(kobs.conflict_reports, null, 2);
        } else {
          confEl.textContent = "(no conflict_report items in scoped code memory)";
        }

        let panels = null;
        try { panels = await loadJson("./data/operator_panels.json"); } catch (e) {}
        if (panels && panels.forensics && panels.forensics.summary) {
          forensicsEl.textContent = JSON.stringify(panels.forensics, null, 2);
        } else {
          forensicsEl.textContent = "(no forensics bundle under .akc/viewer/forensics for this tenant/repo)";
        }
        if (panels && panels.playbook && panels.playbook.summary) {
          playbookEl.textContent = JSON.stringify(panels.playbook, null, 2);
        } else {
          playbookEl.textContent = "(no playbook report under .akc/control/playbooks for this tenant/repo)";
        }
        if (panels && panels.autopilot && (panels.autopilot.available || panels.autopilot.scope_state)) {
          autopilotEl.textContent = JSON.stringify(panels.autopilot, null, 2);
        } else {
          autopilotEl.textContent = "(no .akc/autopilot state in this export)";
        }
        if (
          panels &&
          panels.profile_panel &&
          (panels.profile_panel.available ||
            (panels.profile_panel.scope_context &&
              (panels.profile_panel.scope_context.run_id || panels.profile_panel.scope_context.control_followup_cli)))
        ) {
          profileEl.textContent = JSON.stringify(panels.profile_panel, null, 2);
        } else {
          profileEl.textContent = "(no profile / scope context in this export)";
        }

        const byStep = new Map();
        if (manifest && Array.isArray(manifest.artifacts)) {
          for (const a of manifest.artifacts) {
            const md = (a && a.metadata && typeof a.metadata === "object") ? a.metadata : null;
            const sid = md && typeof md.step_id === "string" ? md.step_id : null;
            if (!sid) continue;
            if (!byStep.has(sid)) byStep.set(sid, []);
            byStep.get(sid).push(a);
          }
          for (const [k,v] of byStep.entries()) {
            v.sort((x,y)=> (x.path||"").localeCompare(y.path||""));
          }
        }

        const selectStep = (step) => {
          const ev = byStep.get(step.id) || [];
          evEl.innerHTML = "";
          for (const a of ev) {
            const div = document.createElement("div");
            div.className = "item";
            const p = esc(a.path);
            const href = "./files/" + p;
            div.innerHTML = `<span class="badge ${badgeClass(step.status)}">${esc(step.status)}</span><span class="title">${p}</span><div class="muted">${esc(a.media_type||"")}</div>`;
            div.onclick = () => window.open(href, "_blank");
            evEl.appendChild(div);
          }
          notesEl.textContent = esc(step.notes) || "(no notes)";

          downloadsEl.innerHTML = "";
          const a1 = document.createElement("a");
          a1.href = "./data/plan.json";
          a1.textContent = "Download plan.json";
          a1.setAttribute("download", "plan.json");
          downloadsEl.appendChild(a1);
          if (manifest) {
            const a2 = document.createElement("a");
            a2.href = "./data/manifest.json";
            a2.textContent = "Download manifest.json";
            a2.setAttribute("download", "manifest.json");
            downloadsEl.appendChild(a2);
          }
          hintEl.textContent = hintBase;
        };

        const steps = Array.isArray(plan.steps) ? plan.steps.slice().sort((a,b)=> (a.order_idx||0)-(b.order_idx||0)) : [];
        stepsEl.innerHTML = "";
        let selectedId = null;
        for (const s of steps) {
          const div = document.createElement("div");
          div.className = "item";
          div.innerHTML = `<span class="badge ${badgeClass(s.status)}">${esc(s.status)}</span><span class="title">${esc(s.title)}</span>`;
          div.onclick = () => {
            selectedId = s.id;
            for (const child of stepsEl.children) child.classList.remove("sel");
            div.classList.add("sel");
            selectStep(s);
          };
          stepsEl.appendChild(div);
        }
        if (steps.length) {
          stepsEl.children[0].classList.add("sel");
          selectStep(steps[0]);
        }
      };
      main();
    </script>
  </body>
</html>
"""


def build_static_viewer(*, snapshot: ViewerSnapshot, out_dir: Path) -> WebBuildResult:
    """Build a local, static HTML viewer bundle (read-only)."""

    out_dir = out_dir.expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    data_dir = (out_dir / "data").resolve()
    data_dir.mkdir(parents=True, exist_ok=True)
    files_dir = (out_dir / "files").resolve()
    files_dir.mkdir(parents=True, exist_ok=True)

    (data_dir / "plan.json").write_text(
        json.dumps(snapshot.plan.to_json_obj(), indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    if snapshot.manifest is not None:
        (data_dir / "manifest.json").write_text(
            json.dumps(dict(snapshot.manifest), indent=2, sort_keys=True, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
    else:
        # Keep fetch() failure predictable: no manifest file.
        with contextlib.suppress(FileNotFoundError):
            (data_dir / "manifest.json").unlink()

    kobs_obj = build_knowledge_observation_payload(
        knowledge_envelope=snapshot.knowledge_envelope,
        conflict_reports=snapshot.conflict_reports,
        knowledge_mediation_envelope=snapshot.knowledge_mediation_envelope,
    )
    (data_dir / "knowledge_obs.json").write_text(
        json.dumps(kobs_obj, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    panels_obj = snapshot.operator_panels
    if panels_obj is None:
        panels_obj = {"forensics": None, "playbook": None, "profile_panel": None, "autopilot": None}
    (data_dir / "operator_panels.json").write_text(
        json.dumps(panels_obj, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    # Copy all referenced artifact files for local opening/downloading.
    copied = 0
    if snapshot.manifest is not None:
        for a in snapshot.manifest.get("artifacts") or []:
            relpath = str(a.get("path") or "").strip()
            if not relpath:
                continue
            copied += _safe_copy(src_root=snapshot.scoped_outputs_dir, relpath=relpath, dst_root=files_dir)
    for rel in (
        ".akc/knowledge/snapshot.json",
        ".akc/knowledge/snapshot.fingerprint.json",
        ".akc/knowledge/mediation.json",
    ):
        copied += _safe_copy(src_root=snapshot.scoped_outputs_dir, relpath=rel, dst_root=files_dir)

    # Write HTML last for atomic-ish success.
    index = (out_dir / "index.html").resolve()
    try:
        index.write_text(_INDEX_HTML, encoding="utf-8")
    except OSError as e:  # pragma: no cover
        raise ViewerError(f"failed to write static viewer: {index}") from e

    return WebBuildResult(root=out_dir, index_html=index, copied_files=copied)
