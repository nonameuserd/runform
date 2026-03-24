/**
 * Read-only fleet dashboard: GET /health, GET /v1/runs, GET /v1/runs/{tenant}/{repo}/{run_id}.
 * No writes, no compile/runtime.
 */
(function () {
  "use strict";

  const STORAGE_KEY = "akc_operator_dashboard_v1";

  /** @type {HTMLInputElement} */
  const fleetBaseEl = document.getElementById("fleetBase");
  /** @type {HTMLInputElement} */
  const tokenEl = document.getElementById("token");
  /** @type {HTMLInputElement} */
  const tenantEl = document.getElementById("tenantId");
  /** @type {HTMLInputElement} */
  const repoEl = document.getElementById("repoId");
  /** @type {HTMLInputElement} */
  const limitEl = document.getElementById("limit");
  const statusEl = document.getElementById("status");
  const runsBody = document.getElementById("runsBody");
  const detailJson = document.getElementById("detailJson");
  const detailHint = document.getElementById("detailHint");

  function loadSettings() {
    try {
      const raw = sessionStorage.getItem(STORAGE_KEY);
      if (!raw) {
        return;
      }
      const o = JSON.parse(raw);
      if (typeof o.fleetBase === "string") {
        fleetBaseEl.value = o.fleetBase;
      }
      if (typeof o.token === "string") {
        tokenEl.value = o.token;
      }
      if (typeof o.tenantId === "string") {
        tenantEl.value = o.tenantId;
      }
      if (typeof o.repoId === "string") {
        repoEl.value = o.repoId;
      }
      if (typeof o.limit === "string") {
        limitEl.value = o.limit;
      }
    } catch {
      /* ignore */
    }
  }

  function saveSettings() {
    sessionStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({
        fleetBase: fleetBaseEl.value.trim(),
        token: tokenEl.value,
        tenantId: tenantEl.value.trim(),
        repoId: repoEl.value.trim(),
        limit: limitEl.value.trim(),
      }),
    );
    setStatus("Saved to sessionStorage for this tab.", "ok");
  }

  function baseUrl() {
    const u = fleetBaseEl.value.trim().replace(/\/+$/, "");
    if (!u) {
      throw new Error("Fleet API base URL is required.");
    }
    return u;
  }

  function authHeaders() {
    const t = tokenEl.value.trim();
    const h = {};
    if (t) {
      h.Authorization = `Bearer ${t}`;
    }
    return h;
  }

  function setStatus(msg, cls) {
    statusEl.textContent = msg || "";
    statusEl.className = cls || "";
  }

  /**
   * @param {string} pathWithQuery
   * @returns {Promise<any>}
   */
  async function apiGet(pathWithQuery) {
    const url = `${baseUrl()}${pathWithQuery}`;
    const res = await fetch(url, {
      method: "GET",
      headers: authHeaders(),
    });
    const text = await res.text();
    let data;
    try {
      data = text ? JSON.parse(text) : null;
    } catch {
      throw new Error(`Non-JSON response (${res.status}): ${text.slice(0, 200)}`);
    }
    if (!res.ok) {
      const err = data && data.error ? String(data.error) : res.statusText;
      throw new Error(`${res.status} ${err}`);
    }
    return data;
  }

  function joinPath(root, rel) {
    const a = root.replace(/\/+$/, "");
    const b = String(rel).replace(/^\/+/, "");
    return `${a}/${b}`;
  }

  function pathHints(run) {
    if (!run || typeof run !== "object") {
      return "";
    }
    const root = run.outputs_root;
    const lines = [];
    if (root && run.manifest_rel_path) {
      lines.push(`Manifest (local): ${joinPath(root, run.manifest_rel_path)}`);
    }
    if (Array.isArray(run.sidecars)) {
      for (const sc of run.sidecars) {
        if (sc && sc.rel_path) {
          lines.push(`Sidecar [${sc.kind || "?"}]: ${joinPath(root, sc.rel_path)}`);
        }
      }
    }
    lines.push(
      "Forensics / playbooks (if present on disk): check .akc/control/ under the same tenant/repo tree.",
    );
    lines.push(
      "Portable bundle: run akc view export from a machine that can read outputs_root (not from this page).",
    );
    return lines.join("\n");
  }

  function renderRuns(runs) {
    runsBody.textContent = "";
    if (!runs.length) {
      const tr = document.createElement("tr");
      const td = document.createElement("td");
      td.colSpan = 5;
      td.style.color = "var(--muted)";
      td.textContent = "No runs returned.";
      tr.appendChild(td);
      runsBody.appendChild(tr);
      return;
    }
    for (const r of runs) {
      const tr = document.createElement("tr");
      tr.dataset.selectable = "1";
      tr.addEventListener("click", () => selectRun(r, tr));
      const cells = [r.shard_id, r.repo_id, r.run_id, r.updated_at_ms, r.aggregate_health || "—"];
      for (const c of cells) {
        const td = document.createElement("td");
        td.textContent = c === undefined || c === null ? "—" : String(c);
        tr.appendChild(td);
      }
      runsBody.appendChild(tr);
    }
  }

  async function selectRun(r, trEl) {
    document.querySelectorAll("tr.selected").forEach((el) => el.classList.remove("selected"));
    trEl.classList.add("selected");
    detailJson.hidden = false;
    detailHint.textContent = "Loading run detail…";
    try {
      const tenant = encodeURIComponent(r.tenant_id);
      const repo = encodeURIComponent(r.repo_id);
      const run = encodeURIComponent(r.run_id);
      const full = await apiGet(`/v1/runs/${tenant}/${repo}/${run}`);
      const block = {
        api: full.run,
        local_path_hints: pathHints(full.run),
      };
      detailJson.textContent = JSON.stringify(block, null, 2);
      detailHint.textContent = "API JSON plus local path hints (filesystem paths on the shard; not fetched over HTTP).";
    } catch (e) {
      detailHint.textContent = "Failed to load run detail.";
      detailJson.textContent = String(e);
      setStatus(String(e), "err");
    }
  }

  document.getElementById("btnSave").addEventListener("click", saveSettings);

  document.getElementById("btnHealth").addEventListener("click", async () => {
    setStatus("");
    try {
      const h = await apiGet("/health");
      setStatus(`Health: ${h.status} — ${JSON.stringify(h.fleet || {})}`, "ok");
    } catch (e) {
      setStatus(String(e), "err");
    }
  });

  document.getElementById("btnRuns").addEventListener("click", async () => {
    setStatus("");
    detailJson.hidden = true;
    detailJson.textContent = "";
    detailHint.textContent = "Select a run row for GET /v1/runs/… and path hints.";
    document.querySelectorAll("tr.selected").forEach((el) => el.classList.remove("selected"));
    const tenant = tenantEl.value.trim();
    if (!tenant) {
      setStatus("Tenant id is required.", "err");
      return;
    }
    let lim = parseInt(limitEl.value.trim(), 10);
    if (Number.isNaN(lim) || lim < 1) {
      lim = 50;
    }
    const params = new URLSearchParams({ tenant_id: tenant, limit: String(lim) });
    const repo = repoEl.value.trim();
    if (repo) {
      params.set("repo_id", repo);
    }
    try {
      const data = await apiGet(`/v1/runs?${params.toString()}`);
      renderRuns(data.runs || []);
      setStatus(`Loaded ${(data.runs || []).length} run(s).`, "ok");
    } catch (e) {
      runsBody.textContent = "";
      const tr = document.createElement("tr");
      const td = document.createElement("td");
      td.colSpan = 5;
      td.style.color = "var(--muted)";
      td.textContent = "Error — see status line.";
      tr.appendChild(td);
      runsBody.appendChild(tr);
      setStatus(String(e), "err");
    }
  });

  loadSettings();
})();
