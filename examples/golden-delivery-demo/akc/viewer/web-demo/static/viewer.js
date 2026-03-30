const JSON_TREE_CHUNK = 64;

const loadJson = async (p) => (await fetch(p)).json();
const esc = (s) => (s ?? "").toString();

const badgeClass = (st) =>
  st === "done" ? "ok" : st === "failed" ? "bad" : st === "in_progress" ? "warn" : "";

function mountJsonTree(container, data) {
  container.textContent = "";
  if (data === undefined) {
    container.textContent = "(missing)";
    return;
  }
  container.appendChild(renderJsonNode(data, "", 0));
}

function renderJsonNode(value, keyLabel, depth) {
  const wrap = document.createElement("div");
  wrap.className = "jnode";
  if (value !== null && typeof value === "object") {
    const isArr = Array.isArray(value);
    const count = isArr ? value.length : Object.keys(value).length;
    const summary = document.createElement("button");
    summary.type = "button";
    summary.className = "jtoggle";
    summary.setAttribute("aria-expanded", "false");
    const head = keyLabel ? keyLabel + ": " : "";
    summary.textContent = head + (isArr ? "Array(" + count + ")" : "Object(" + count + ")");
    const children = document.createElement("div");
    children.className = "jchildren";
    children.hidden = true;
    let shown = 0;
    const keys = isArr ? [...Array(value.length).keys()] : Object.keys(value).sort();
    const renderChunk = () => {
      const end = Math.min(shown + JSON_TREE_CHUNK, keys.length);
      for (let i = shown; i < end; i++) {
        const k = keys[i];
        const childVal = isArr ? value[k] : value[k];
        const label = isArr ? String(k) : JSON.stringify(k);
        children.appendChild(renderJsonNode(childVal, label, depth + 1));
      }
      shown = end;
      const oldMore = children.querySelector(":scope > .jmore");
      if (oldMore) oldMore.remove();
      if (shown < keys.length) {
        const more = document.createElement("button");
        more.type = "button";
        more.className = "jmore muted";
        more.textContent = "Load more (" + (keys.length - shown) + " remaining)";
        more.onclick = () => renderChunk();
        children.appendChild(more);
      }
    };
    summary.onclick = () => {
      const open = children.hidden;
      children.hidden = !open;
      summary.setAttribute("aria-expanded", String(open));
      if (open && !children.dataset.hydrated) {
        children.dataset.hydrated = "1";
        shown = 0;
        renderChunk();
      }
    };
    wrap.appendChild(summary);
    wrap.appendChild(children);
  } else {
    const row = document.createElement("div");
    row.className = "jleaf";
    const prefix = keyLabel ? keyLabel + ": " : "";
    row.textContent = prefix + JSON.stringify(value);
    wrap.appendChild(row);
  }
  return wrap;
}

const main = async () => {
  const plan = await loadJson("./data/plan.json");
  let manifest = null;
  try {
    manifest = await loadJson("./data/manifest.json");
  } catch (e) {}

  let kobs = null;
  try {
    kobs = await loadJson("./data/knowledge_obs.json");
  } catch (e) {}

  let panels = null;
  try {
    panels = await loadJson("./data/operator_panels.json");
  } catch (e) {}

  const meta = document.getElementById("meta");
  meta.textContent =
    plan.tenant_id +
    "/" +
    plan.repo_id +
    " — " +
    plan.status +
    " — steps=" +
    (plan.steps || []).length;

  const stepsEl = document.getElementById("steps");
  const tablist = document.getElementById("tablist");
  const subTitle = document.getElementById("step_subheader_title");
  const subMeta = document.getElementById("step_subheader_meta");

  const tabIds = ["summary", "evidence", "knowledge", "control", "profile"];
  const tabLabels = {
    summary: "Summary",
    evidence: "Evidence",
    knowledge: "Knowledge",
    control: "Control plane",
    profile: "Profile",
  };

  const panelsById = {};
  for (const id of tabIds) {
    panelsById[id] = document.getElementById("panel-" + id);
  }

  const built = new Set();
  let hintBase = "Evidence items open a local copied file (no execution).";
  if (kobs && typeof kobs.unresolved_knowledge_conflicts_count === "number") {
    hintBase =
      "Unresolved knowledge conflicts (distinct groups): " +
      kobs.unresolved_knowledge_conflicts_count +
      " — " +
      hintBase;
  }

  const byStep = new Map();
  if (manifest && Array.isArray(manifest.artifacts)) {
    for (const a of manifest.artifacts) {
      const md = a && a.metadata && typeof a.metadata === "object" ? a.metadata : null;
      const sid = md && typeof md.step_id === "string" ? md.step_id : null;
      if (!sid) continue;
      if (!byStep.has(sid)) byStep.set(sid, []);
      byStep.get(sid).push(a);
    }
    for (const v of byStep.values()) {
      v.sort((x, y) => (x.path || "").localeCompare(y.path || ""));
    }
  }

  let selectedStep = null;
  const tabButtons = [];

  function syncStepUrl(stepId) {
    try {
      const u = new URL(window.location.href);
      if (stepId) u.searchParams.set("step", stepId);
      else u.searchParams.delete("step");
      history.replaceState(null, "", u);
    } catch (e) {}
  }

  function updateSubheader(step) {
    if (!step) {
      subTitle.textContent = "Select a step";
      subMeta.textContent = "";
      return;
    }
    subTitle.textContent = esc(step.title) || esc(step.id);
    subMeta.textContent = esc(step.id) + " · " + esc(step.status);
  }

  function refreshSummary() {
    const notesEl = document.getElementById("summary_notes");
    const hintEl = document.getElementById("summary_hint");
    const downloadsEl = document.getElementById("summary_downloads");
    if (!notesEl || !hintEl || !downloadsEl) return;
    notesEl.textContent = selectedStep
      ? esc(selectedStep.notes) || "(no notes)"
      : "(select a step)";
    hintEl.textContent = hintBase;
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
  }

  function refreshEvidence() {
    const host = document.getElementById("evidence_host");
    if (!host) return;
    host.innerHTML = "";
    if (!selectedStep) return;
    const ev = byStep.get(selectedStep.id) || [];
    for (const a of ev) {
      const div = document.createElement("div");
      div.className = "item";
      const p = esc(a.path);
      const href = "./files/" + p;
      div.innerHTML =
        '<span class="badge ' +
        badgeClass(selectedStep.status) +
        '">' +
        esc(selectedStep.status) +
        '</span><span class="title">' +
        p +
        '</span><div class="muted">' +
        esc(a.media_type || "") +
        "</div>";
      div.onclick = () => window.open(href, "_blank");
      host.appendChild(div);
    }
    if (!ev.length) {
      const empty = document.createElement("div");
      empty.className = "muted";
      empty.style.padding = "12px 0";
      empty.textContent = "(no evidence artifacts for this step)";
      host.appendChild(empty);
    }
  }

  function buildSummaryPanel() {
    const el = panelsById.summary;
    el.innerHTML =
      '<div class="panel-pad"><div class="actions" id="summary_downloads"></div>' +
      '<p class="muted" id="summary_hint"></p>' +
      '<h3>Step notes</h3><pre class="notes-pre" id="summary_notes"></pre></div>';
  }

  function buildEvidencePanel() {
    const el = panelsById.evidence;
    el.innerHTML =
      '<div class="panel-pad"><h3>Evidence files</h3><div id="evidence_host"></div></div>';
  }

  function addJsonSection(root, title, condition, data) {
    const h = document.createElement("h3");
    h.textContent = title;
    root.appendChild(h);
    if (!condition) {
      const p = document.createElement("p");
      p.className = "muted";
      p.textContent = data;
      root.appendChild(p);
      return;
    }
    const host = document.createElement("div");
    host.className = "json-tree-host";
    mountJsonTree(host, data);
    root.appendChild(host);
  }

  function buildKnowledgePanel() {
    const el = panelsById.knowledge;
    const pad = document.createElement("div");
    pad.className = "panel-pad";
    addJsonSection(
      pad,
      "Knowledge (compile/runtime debug)",
      !!(kobs && kobs.knowledge_envelope),
      kobs && kobs.knowledge_envelope ? kobs.knowledge_envelope : "(no persisted knowledge snapshot in this export)"
    );
    addJsonSection(
      pad,
      "Mediation events",
      !!(kobs && Array.isArray(kobs.mediation_events) && kobs.mediation_events.length),
      kobs && Array.isArray(kobs.mediation_events) && kobs.mediation_events.length
        ? kobs.mediation_events
        : "(no mediation events — missing or empty .akc/knowledge/mediation.json)"
    );
    addJsonSection(
      pad,
      "Conflict groups",
      !!(kobs && kobs.conflict_groups && Object.keys(kobs.conflict_groups).length),
      kobs && kobs.conflict_groups && Object.keys(kobs.conflict_groups).length
        ? kobs.conflict_groups
        : "(no grouped mediation events)"
    );
    addJsonSection(
      pad,
      "Supersession hints",
      !!(kobs && Array.isArray(kobs.supersession_hints) && kobs.supersession_hints.length),
      kobs && Array.isArray(kobs.supersession_hints) && kobs.supersession_hints.length
        ? kobs.supersession_hints
        : "(no supersession hints in mediation events)"
    );
    addJsonSection(
      pad,
      "Conflict reports",
      !!(kobs && Array.isArray(kobs.conflict_reports) && kobs.conflict_reports.length),
      kobs && Array.isArray(kobs.conflict_reports) && kobs.conflict_reports.length
        ? kobs.conflict_reports
        : "(no conflict_report items in scoped code memory)"
    );
    el.appendChild(pad);
  }

  function buildControlPanel() {
    const el = panelsById.control;
    const pad = document.createElement("div");
    pad.className = "panel-pad";
    addJsonSection(
      pad,
      "Forensics summary (read-only)",
      !!(panels && panels.forensics && panels.forensics.summary),
      panels && panels.forensics && panels.forensics.summary
        ? panels.forensics
        : "(no forensics bundle under .akc/viewer/forensics for this tenant/repo)"
    );
    addJsonSection(
      pad,
      "Playbook report (read-only)",
      !!(panels && panels.playbook && panels.playbook.summary),
      panels && panels.playbook && panels.playbook.summary
        ? panels.playbook
        : "(no playbook report under .akc/control/playbooks for this tenant/repo)"
    );
    addJsonSection(
      pad,
      "Autopilot scope (read-only)",
      !!(
        panels &&
        panels.autopilot &&
        (panels.autopilot.available || panels.autopilot.scope_state)
      ),
      panels && panels.autopilot && (panels.autopilot.available || panels.autopilot.scope_state)
        ? panels.autopilot
        : "(no .akc/autopilot state in this export)"
    );
    el.appendChild(pad);
  }

  function buildProfilePanel() {
    const el = panelsById.profile;
    const pad = document.createElement("div");
    pad.className = "panel-pad";
    const prof =
      panels &&
      panels.profile_panel &&
      (panels.profile_panel.available ||
        (panels.profile_panel.scope_context &&
          (panels.profile_panel.scope_context.run_id ||
            panels.profile_panel.scope_context.control_followup_cli)));
    addJsonSection(
      pad,
      "Profile / developer decisions (read-only)",
      !!prof,
      prof
        ? panels.profile_panel
        : "(no profile / scope context in this export)"
    );
    el.appendChild(pad);
  }

  const builders = {
    summary: buildSummaryPanel,
    evidence: buildEvidencePanel,
    knowledge: buildKnowledgePanel,
    control: buildControlPanel,
    profile: buildProfilePanel,
  };

  let activeTab = "summary";

  function activateTab(name) {
    activeTab = name;
    for (const id of tabIds) {
      panelsById[id].hidden = id !== name;
    }
    for (let i = 0; i < tabButtons.length; i++) {
      const b = tabButtons[i];
      const on = b.dataset.tab === name;
      b.setAttribute("aria-selected", on ? "true" : "false");
      b.tabIndex = on ? 0 : -1;
    }
    if (!built.has(name)) {
      builders[name]();
      built.add(name);
    }
    if (name === "summary") refreshSummary();
    if (name === "evidence") refreshEvidence();
  }

  for (let i = 0; i < tabIds.length; i++) {
    const id = tabIds[i];
    const btn = document.createElement("button");
    btn.type = "button";
    btn.id = "tab-" + id;
    btn.setAttribute("role", "tab");
    btn.setAttribute("aria-controls", "panel-" + id);
    btn.dataset.tab = id;
    btn.textContent = tabLabels[id];
    btn.addEventListener("click", () => activateTab(id));
    btn.addEventListener("keydown", (e) => {
      const ix = tabButtons.indexOf(btn);
      if (e.key === "ArrowRight" || e.key === "ArrowDown") {
        e.preventDefault();
        const n = tabButtons[(ix + 1) % tabButtons.length];
        n.focus();
        activateTab(n.dataset.tab);
      } else if (e.key === "ArrowLeft" || e.key === "ArrowUp") {
        e.preventDefault();
        const n = tabButtons[(ix - 1 + tabButtons.length) % tabButtons.length];
        n.focus();
        activateTab(n.dataset.tab);
      } else if (e.key === "Home") {
        e.preventDefault();
        tabButtons[0].focus();
        activateTab(tabButtons[0].dataset.tab);
      } else if (e.key === "End") {
        e.preventDefault();
        const last = tabButtons[tabButtons.length - 1];
        last.focus();
        activateTab(last.dataset.tab);
      }
    });
    tablist.appendChild(btn);
    tabButtons.push(btn);
  }

  function selectStep(step) {
    selectedStep = step;
    syncStepUrl(step ? step.id : null);
    updateSubheader(step);
    if (built.has("summary")) refreshSummary();
    if (built.has("evidence")) refreshEvidence();
  }

  const steps = Array.isArray(plan.steps)
    ? plan.steps.slice().sort((a, b) => (a.order_idx || 0) - (b.order_idx || 0))
    : [];
  stepsEl.innerHTML = "";
  const params = new URLSearchParams(window.location.search);
  const wantId = params.get("step");
  let initial = null;
  for (const s of steps) {
    const div = document.createElement("div");
    div.className = "item";
    div.innerHTML =
      '<span class="badge ' +
      badgeClass(s.status) +
      '">' +
      esc(s.status) +
      '</span><span class="title">' +
      esc(s.title) +
      "</span>";
    div.onclick = () => {
      for (const child of stepsEl.children) child.classList.remove("sel");
      div.classList.add("sel");
      selectStep(s);
    };
    stepsEl.appendChild(div);
    if (wantId && s.id === wantId) initial = { div, step: s };
  }
  if (steps.length) {
    const pick = initial || { div: stepsEl.children[0], step: steps[0] };
    pick.div.classList.add("sel");
    selectStep(pick.step);
  } else {
    updateSubheader(null);
  }

  activateTab("summary");
};
main();
