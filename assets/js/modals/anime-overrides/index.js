/* assets/js/modals/anime-overrides/index.js */
/* Anime mapping overrides modal. */

const I18N = () => globalThis.window?.CW?.I18n || globalThis.window?.__cwI18n;
const T = (key, vars) => I18N()?.t?.(key, vars) ?? key;
const TH = (key, vars) => I18N()?.html?.(key, vars) ?? key;
const REQUEST_TIMEOUT_MS = 20_000;
const API = "/api/anime-mapping/overrides";

const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

const PROVIDER_LABELS = {
  tvdb: "TVDB", tmdb: "TMDB", imdb: "IMDb",
  anidb: "AniDB", mal: "MyAnimeList", anilist: "AniList", simkl: "SIMKL", kitsu: "Kitsu",
};

const label = (key) => PROVIDER_LABELS[String(key || "").toLowerCase()] || String(key || "").toUpperCase();

function esc(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

async function apiJson(url, opts = {}) {
  const ctrl = new AbortController();
  const timeout = window.setTimeout(() => ctrl.abort(), REQUEST_TIMEOUT_MS);
  try {
    const r = await fetch(url, { cache: "no-store", credentials: "same-origin", ...opts, signal: ctrl.signal });
    const data = await r.json().catch(() => ({}));
    if (!r.ok || data.ok === false) throw new Error(data.message || data.error || `${r.status} ${r.statusText || ""}`.trim());
    if (opts.method && opts.method !== "GET" && (url === API || url.startsWith(`${API}/`))) window.dispatchEvent(new CustomEvent("cw:anime-mappings-changed"));
    return data;
  } catch (e) {
    if (e?.name === "AbortError") throw new Error(T("modals.animeOverrides.timedOut"));
    throw e;
  } finally {
    window.clearTimeout(timeout);
  }
}

function injectCss() {
  const existing = document.getElementById("cw-anime-overrides-css");
  if (existing?.tagName === "LINK") return Promise.resolve();
  existing?.remove();
  const link = document.createElement("link");
  const cssUrl = new URL("./styles.css", import.meta.url);
  const version = new URL(import.meta.url).searchParams.get("v") || window.__CW_VERSION__;
  if (version) cssUrl.searchParams.set("v", version);
  link.id = "cw-anime-overrides-css";
  link.rel = "stylesheet";
  link.href = cssUrl.href;
  return new Promise((resolve) => {
    link.addEventListener("load", resolve, { once: true });
    link.addEventListener("error", resolve, { once: true });
    document.head.appendChild(link);
  });
}

function ruleSummary(row) {
  const from = `${label(row.match_provider)} ${esc(row.match_id)}`;
  const to = `${label(row.target_namespace)} ${esc(row.target_id)}`;
  if (row.episode_start_at == null) return `${from} &rarr; ${to}`;
  const last = row.episode_to == null ? "&hellip;" : esc(row.episode_to);
  const span = row.episode_to == null
    ? `${esc(row.episode_start_at)}&hellip;`
    : `${esc(row.episode_start_at)}-${Number(row.episode_start_at) + (Number(row.episode_to) - Number(row.episode_from))}`;
  return `${from} S${esc(row.match_season)} E${esc(row.episode_from)}-${last} &rarr; ${to} ${TH("modals.animeOverrides.episodeWord")} ${span}`;
}

export default {
  async mount(root) {
    await injectCss();

    const shell = root.closest(".cx-modal-shell");
    if (shell) {
      shell.classList.add("cw-anime-overrides-shell");
      shell.style.setProperty("--cxModalW", "980px");
      shell.style.setProperty("--cxModalMaxW", "980px");
      shell.style.setProperty("--cxModalMaxH", "780px");
    }

    let rows = [];
    let schema = { match_providers: [], target_namespaces: [], media_types: ["show", "movie"] };
    let editingId = "";
    let busy = false;
    let simklReady = false;
    let findResults = [];

    root.innerHTML = `
      <div class="cw-anime-overrides">
        <div class="cx-head">
          <div class="ao-head-left">
            <div class="ao-head-icon"><span class="material-symbols-rounded" aria-hidden="true">rule_settings</span></div>
            <div>
              <div class="ao-title">${TH("modals.animeOverrides.title")}</div>
              <div class="ao-sub">${TH("modals.animeOverrides.sub")}</div>
            </div>
          </div>
          <button type="button" class="ao-close" id="ao-close" title="${TH("modals.animeOverrides.close")}" aria-label="${TH("modals.animeOverrides.close")}"><span class="material-symbols-rounded" aria-hidden="true">close</span></button>
        </div>

        <div class="ao-body cw-scrollbars">
          <section class="ao-card">
            <div class="ao-card-head">
              <div>
                <h3 id="ao-form-title">${TH("modals.animeOverrides.addRule")}</h3>
                <p>${TH("modals.animeOverrides.addRuleHelp")}</p>
              </div>
            </div>

            <div class="ao-grid">
              <label><span>${TH("modals.animeOverrides.thisIsA")}</span>
                <select id="ao-media-type">
                  <option value="show">${TH("modals.animeOverrides.tvShow")}</option>
                  <option value="movie">${TH("modals.animeOverrides.movie")}</option>
                </select>
              </label>
              <label><span>${TH("modals.animeOverrides.nameOptional")}</span>
                <input id="ao-title" type="text" placeholder="Dragon Ball Z" maxlength="200">
              </label>
            </div>

            <div class="ao-rule-row">
              <div class="ao-rule-side">
                <div class="ao-rule-legend">${TH("modals.animeOverrides.whenItem")}</div>
                <div class="ao-inline">
                  <select id="ao-match-provider"></select>
                  <input id="ao-match-id" type="text" placeholder="ID" maxlength="64">
                </div>
                <label class="ao-season" id="ao-season-wrap"><span>${TH("modals.animeOverrides.season")}</span>
                  <input id="ao-match-season" type="number" min="0" step="1" placeholder="${TH("modals.animeOverrides.any")}">
                </label>
              </div>
              <span class="material-symbols-rounded ao-arrow" aria-hidden="true">arrow_forward</span>
              <div class="ao-rule-side">
                <div class="ao-rule-legend">${TH("modals.animeOverrides.treatAs")}</div>
                <div class="ao-inline">
                  <select id="ao-target-namespace"></select>
                  <div class="ao-target-id">
                    <input id="ao-target-id" type="text" placeholder="ID" maxlength="64">
                    <button type="button" class="ao-find" id="ao-find" hidden>
                      <span class="material-symbols-rounded" aria-hidden="true">search</span>
                    </button>
                  </div>
                </div>
                <p class="ao-find-note" id="ao-find-note" hidden></p>
              </div>
            </div>

            <section class="ao-finder" id="ao-finder" hidden>
              <div class="ao-finder-head">
                <span class="material-symbols-rounded" aria-hidden="true">travel_explore</span>
                <div>
                  <strong>${TH("modals.animeOverrides.lookUpSimkl")}</strong>
                  <p>${TH("modals.animeOverrides.lookUpHelp")}</p>
                </div>
                <button type="button" class="ao-icon" id="ao-finder-close" title="${TH("modals.animeOverrides.closeLookup")}"><span class="material-symbols-rounded" aria-hidden="true">close</span></button>
              </div>
              <div class="ao-finder-search">
                <input id="ao-finder-q" type="text" placeholder="${TH("modals.animeOverrides.searchSimkl")}" maxlength="200">
                <button type="button" class="ao-btn" id="ao-finder-go">${TH("modals.animeOverrides.search")}</button>
              </div>
              <p class="ao-finder-status" id="ao-finder-status"></p>
              <div class="ao-finder-list" id="ao-finder-list"></div>
            </section>

            <div class="ao-episodes" id="ao-episodes">
              <div class="ao-rule-legend">${TH("modals.animeOverrides.episodeNumbers")}</div>
              <div class="ao-grid ao-grid-3">
                <label><span>${TH("modals.animeOverrides.firstEpisode")}</span><input id="ao-ep-from" type="number" min="1" step="1" placeholder="1"></label>
                <label><span>${TH("modals.animeOverrides.lastEpisode")}</span><input id="ao-ep-to" type="number" min="1" step="1" placeholder="${TH("modals.animeOverrides.openEnded")}"></label>
                <label><span>${TH("modals.animeOverrides.countsAs")}</span><input id="ao-ep-start" type="number" min="1" step="1" placeholder="40"></label>
              </div>
              <p class="ao-hint" id="ao-preview"></p>
            </div>

            <label class="ao-note"><span>${TH("modals.animeOverrides.noteOptional")}</span>
              <input id="ao-note" type="text" placeholder="${TH("modals.animeOverrides.whyRule")}" maxlength="500">
            </label>

            <div class="ao-form-actions">
              <span class="ao-status" id="ao-status"></span>
              <button type="button" class="ao-btn" id="ao-reset" hidden>${TH("modals.animeOverrides.cancelEdit")}</button>
              <button type="button" class="ao-btn primary" id="ao-save">${TH("modals.animeOverrides.addRuleBtn")}</button>
            </div>
          </section>

          <section class="ao-card">
            <div class="ao-card-head compact has-tools">
              <div>
                <h3>${TH("modals.animeOverrides.yourRules")} <span class="ao-count" id="ao-count"></span></h3>
                <p>${TH("modals.animeOverrides.rulesHelp")}</p>
              </div>
              <div class="ao-tools">
                <select id="ao-import-mode" title="${TH("modals.animeOverrides.importMode")}">
                  <option value="merge">${TH("modals.animeOverrides.merge")}</option>
                  <option value="replace">${TH("modals.animeOverrides.replace")}</option>
                </select>
                <button type="button" class="ao-btn ao-btn-icon" id="ao-import">
                  <span class="material-symbols-rounded" aria-hidden="true">upload</span>${TH("modals.animeOverrides.import")}
                </button>
                <button type="button" class="ao-btn ao-btn-icon" id="ao-export">
                  <span class="material-symbols-rounded" aria-hidden="true">download</span>${TH("modals.animeOverrides.export")}
                </button>
                <input type="file" id="ao-file" accept="application/json,.json" hidden>
              </div>
            </div>
            <p class="ao-io-status" id="ao-io-status"></p>
            <div id="ao-list" class="ao-list"></div>
          </section>
        </div>
      </div>
    `;

    const el = (id) => $(`#${id}`, root);
    const statusEl = el("ao-status");
    const ioStatusEl = el("ao-io-status");

    function setStatus(message, tone = "") {
      statusEl.textContent = message || "";
      statusEl.dataset.tone = tone;
    }

    function setIoStatus(message, tone = "") {
      ioStatusEl.textContent = message || "";
      ioStatusEl.dataset.tone = tone;
    }

    function setBusy(on) {
      busy = !!on;
      $$("button, input, select", root).forEach((n) => { n.disabled = busy; });
      if (!busy) syncMediaType();
    }

    async function refreshSimklStatus() {
      try {
        const data = await apiJson("/api/anime-mapping/simkl/status");
        simklReady = !!data.configured;
      } catch {
        simklReady = false;
      }
      syncTargetTools();
    }

    function fillSelect(node, values, current) {
      node.innerHTML = [...values].sort((a, b) => label(a).localeCompare(label(b), undefined, { sensitivity: "base", numeric: true })).map((v) => `<option value="${esc(v)}">${esc(label(v))}</option>`).join("");
      if (current) node.value = current;
    }

    function syncTargetTools() {
      const find = el("ao-find");
      const note = el("ao-find-note");
      if (!simklReady) {
        find.hidden = true;
        note.hidden = true;
        el("ao-finder").hidden = true;
        return;
      }
      const isSimkl = el("ao-target-namespace").value === "simkl";
      find.hidden = false;
      find.disabled = busy || !isSimkl;
      find.title = isSimkl ? T("modals.animeOverrides.lookThisUp") : T("modals.animeOverrides.simklOnly");
      note.hidden = isSimkl;
      note.textContent = isSimkl ? "" : T("modals.animeOverrides.simklOnlyDot");
      if (!isSimkl) el("ao-finder").hidden = true;
    }

    function refreshSelects() {
      ["ao-media-type", "ao-match-provider", "ao-target-namespace"].forEach((id) => {
        const node = el(id);
        try { window.CW?.IconSelect?.enhance?.(node, node.__cwIconSelectCfg || { className: "cw-plain-select" }); } catch {}
      });
    }

    function syncMediaType() {
      const isMovie = el("ao-media-type").value === "movie";
      el("ao-episodes").hidden = isMovie;
      el("ao-season-wrap").hidden = isMovie;
      syncTargetTools();
      renderPreview();
    }

    function readForm() {
      const isMovie = el("ao-media-type").value === "movie";
      const num = (id) => {
        const raw = String(el(id).value || "").trim();
        return raw === "" ? null : Number(raw);
      };
      return {
        id: editingId || undefined,
        media_type: el("ao-media-type").value,
        title: el("ao-title").value,
        note: el("ao-note").value,
        match_provider: el("ao-match-provider").value,
        match_id: el("ao-match-id").value,
        match_season: isMovie ? null : num("ao-match-season"),
        target_namespace: el("ao-target-namespace").value,
        target_id: el("ao-target-id").value,
        episode_from: isMovie ? null : num("ao-ep-from"),
        episode_to: isMovie ? null : num("ao-ep-to"),
        episode_start_at: isMovie ? null : num("ao-ep-start"),
      };
    }

    function renderPreview() {
      const d = readForm();
      if (d.media_type === "movie" || d.episode_start_at == null || d.episode_from == null) {
        el("ao-preview").textContent = "";
        return;
      }
      const last = d.episode_to == null ? T("modals.animeOverrides.onwards") : `E${d.episode_to}`;
      const endsAt = d.episode_to == null ? T("modals.animeOverrides.onwards") : String(d.episode_start_at + (d.episode_to - d.episode_from));
      el("ao-preview").textContent = T("modals.animeOverrides.preview", {from: `S${d.match_season ?? "?"}E${d.episode_from}`, last, start: String(d.episode_start_at), end: endsAt});
    }

    function resetForm() {
      editingId = "";
      el("ao-form-title").textContent = T("modals.animeOverrides.addRule");
      el("ao-save").textContent = T("modals.animeOverrides.addRuleBtn");
      el("ao-reset").hidden = true;
      ["ao-title", "ao-match-id", "ao-target-id", "ao-note", "ao-match-season", "ao-ep-from", "ao-ep-to", "ao-ep-start"]
        .forEach((id) => { el(id).value = ""; });
      el("ao-media-type").value = "show";
      refreshSelects();
      syncMediaType();
      setStatus("");
    }

    function editRow(row) {
      editingId = String(row.id || "");
      el("ao-form-title").textContent = T("modals.animeOverrides.editRule");
      el("ao-save").textContent = T("modals.animeOverrides.saveChanges");
      el("ao-reset").hidden = false;
      el("ao-media-type").value = row.media_type || "show";
      el("ao-title").value = row.title || "";
      el("ao-note").value = row.note || "";
      el("ao-match-provider").value = row.match_provider || "";
      el("ao-match-id").value = row.match_id || "";
      el("ao-match-season").value = row.match_season ?? "";
      el("ao-target-namespace").value = row.target_namespace || "";
      el("ao-target-id").value = row.target_id || "";
      el("ao-ep-from").value = row.episode_from ?? "";
      el("ao-ep-to").value = row.episode_to ?? "";
      el("ao-ep-start").value = row.episode_start_at ?? "";
      refreshSelects();
      syncMediaType();
      setStatus(T("modals.animeOverrides.editing"));
      el("ao-match-id").focus();
    }

    function renderList() {
      el("ao-count").textContent = rows.length ? `(${rows.length})` : "";
      if (!rows.length) {
        el("ao-list").innerHTML = `
          <div class="ao-empty">
            <span class="material-symbols-rounded" aria-hidden="true">inbox</span>
            <p>${TH("modals.animeOverrides.noRules")}</p>
          </div>`;
        return;
      }
      el("ao-list").innerHTML = rows.map((row) => `
        <div class="ao-row${row.enabled ? "" : " is-off"}" data-id="${esc(row.id)}">
          <div class="ao-row-main">
            <div class="ao-row-title">
              <span class="ao-chip">${esc(row.media_type === "movie" ? T("modals.animeOverrides.movie") : T("modals.animeOverrides.show"))}</span>
              ${esc(row.title || T("modals.animeOverrides.untitled"))}
            </div>
            <div class="ao-row-rule">${ruleSummary(row)}</div>
            ${row.note ? `<div class="ao-row-note">${esc(row.note)}</div>` : ""}
          </div>
          <div class="ao-row-actions">
            <button type="button" class="ao-icon" data-act="toggle" title="${row.enabled ? T("modals.animeOverrides.disable") : T("modals.animeOverrides.enable")}">
              <span class="material-symbols-rounded" aria-hidden="true">${row.enabled ? "toggle_on" : "toggle_off"}</span>
            </button>
            <button type="button" class="ao-icon" data-act="edit" title="${TH("modals.animeOverrides.edit")}"><span class="material-symbols-rounded" aria-hidden="true">edit</span></button>
            <button type="button" class="ao-icon danger" data-act="delete" title="${TH("modals.animeOverrides.delete")}"><span class="material-symbols-rounded" aria-hidden="true">delete</span></button>
          </div>
        </div>
      `).join("");
    }

    async function refresh() {
      const data = await apiJson(API);
      rows = Array.isArray(data.overrides) ? data.overrides : [];
      if (data.schema) schema = data.schema;
      fillSelect(el("ao-match-provider"), schema.match_providers || [], el("ao-match-provider").value);
      fillSelect(el("ao-target-namespace"), schema.target_namespaces || [], el("ao-target-namespace").value);
      renderList();
      syncTargetTools();
    }

    function setFindStatus(message, tone = "") {
      el("ao-finder-status").textContent = message || "";
      el("ao-finder-status").dataset.tone = tone;
    }

    function renderFindResults() {
      if (!findResults.length) {
        el("ao-finder-list").innerHTML = "";
        return;
      }
      el("ao-finder-list").innerHTML = findResults.map((row, i) => `
        <div class="ao-finder-row" data-i="${i}">
          <div class="ao-finder-main">
            <div class="ao-finder-title">${esc(row.label || row.title)}</div>
            <div class="ao-finder-meta">
              SIMKL ${esc(row.simkl)}${row.year ? ` &middot; ${esc(row.year)}` : ""}${row.tmdb ? ` &middot; TMDB ${esc(row.tmdb)}` : ` &middot; ${TH("modals.animeOverrides.noTmdb")}`}
            </div>
          </div>
          <div class="ao-finder-actions">
            <button type="button" class="ao-btn" data-act="use">${TH("modals.animeOverrides.useId")}</button>
            <button type="button" class="ao-btn primary" data-act="plan">${TH("modals.animeOverrides.buildSeasons")}</button>
          </div>
        </div>
      `).join("");
    }

    async function runFind() {
      const term = String(el("ao-finder-q").value || "").trim();
      if (!term) { setFindStatus(T("modals.animeOverrides.typeTitle"), "err"); return; }
      setBusy(true);
      setFindStatus(T("modals.animeOverrides.searchingSimkl"));
      try {
        const data = await apiJson(`/api/anime-mapping/simkl/search?q=${encodeURIComponent(term)}`);
        findResults = Array.isArray(data.results) ? data.results : [];
        renderFindResults();
        setFindStatus(findResults.length ? T("modals.animeOverrides.results", {count: findResults.length}) : T("modals.animeOverrides.nothingFound"), findResults.length ? "ok" : "err");
      } catch (e) {
        findResults = [];
        renderFindResults();
        setFindStatus(String(e.message || e), "err");
      } finally {
        setBusy(false);
        syncTargetTools();
      }
    }

    async function buildSeasonRules(row) {
      const form = readForm();
      if (!form.match_id) { setFindStatus(T("modals.animeOverrides.fillSource"), "err"); return; }
      setBusy(true);
      setFindStatus(T("modals.animeOverrides.building"));
      try {
        const plan = await apiJson("/api/anime-mapping/simkl/plan", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            simkl: row.simkl,
            match_provider: form.match_provider,
            match_id: form.match_id,
            match_season: form.match_season == null ? 1 : form.match_season,
            title: form.title || row.title,
          }),
        });
        const planned = Array.isArray(plan.rules) ? plan.rules : [];
        if (!planned.length) { setFindStatus(T("modals.animeOverrides.noAired"), "err"); return; }
        const summary = planned
          .map((r) => `  ${form.match_provider.toUpperCase()} S${r.match_season} E${r.episode_from}-${r.episode_to}  ->  SIMKL ${r.target_id}  (${r.note})`)
          .join("\n");
        const skipped = (plan.skipped || []).length ? `\n\n${T("modals.animeOverrides.skippedUnaired", {count: plan.skipped.length})}` : "";
        if (!(await (window.CW?.MobileUI?.ask || window.confirm)(`${T("modals.animeOverrides.confirmPlan", {count: planned.length, episodes: String(plan.total_episodes)})}\n\n${summary}${skipped}`))) {
          setFindStatus(T("modals.animeOverrides.nothingAdded"));
          return;
        }
        let added = 0;
        for (const rule of planned) {
          await apiJson(API, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(rule) });
          added += 1;
        }
        await refresh();
        resetForm();
        el("ao-finder").hidden = true;
        setIoStatus(T("modals.animeOverrides.addedSeasons", {count: added}), "ok");
        window.CW?.DOM?.showToast?.(T("modals.animeOverrides.addedToast", {count: added}), true);
      } catch (e) {
        setFindStatus(String(e.message || e), "err");
      } finally {
        setBusy(false);
        syncTargetTools();
      }
    }

    el("ao-find").addEventListener("click", () => {
      if (busy || !simklReady || el("ao-target-namespace").value !== "simkl") return;
      const panel = el("ao-finder");
      panel.hidden = !panel.hidden;
      if (panel.hidden) return;
      if (!el("ao-finder-q").value) el("ao-finder-q").value = el("ao-title").value || "";
      el("ao-finder-q").focus();
      if (el("ao-finder-q").value) runFind();
    });

    el("ao-finder-close").addEventListener("click", () => { el("ao-finder").hidden = true; });
    el("ao-finder-go").addEventListener("click", runFind);
    el("ao-finder-q").addEventListener("keydown", (ev) => { if (ev.key === "Enter") { ev.preventDefault(); runFind(); } });

    el("ao-finder-list").addEventListener("click", async (ev) => {
      const btn = ev.target.closest("button[data-act]");
      if (!btn || busy) return;
      const row = findResults[Number(btn.closest(".ao-finder-row")?.dataset.i)];
      if (!row) return;
      if (btn.dataset.act === "use") {
        el("ao-target-id").value = row.simkl;
        if (!el("ao-title").value) el("ao-title").value = row.title || "";
        setFindStatus(T("modals.animeOverrides.usingSimkl", {id: String(row.simkl)}), "ok");
        renderPreview();
        return;
      }
      await buildSeasonRules(row);
    });

    el("ao-target-namespace").addEventListener("change", syncTargetTools);
    el("ao-close").addEventListener("click", () => window.cxCloseModal?.());
    el("ao-media-type").addEventListener("change", syncMediaType);
    ["ao-ep-from", "ao-ep-to", "ao-ep-start", "ao-match-season"].forEach((id) => {
      el(id).addEventListener("input", renderPreview);
    });
    el("ao-reset").addEventListener("click", resetForm);

    el("ao-save").addEventListener("click", async () => {
      if (busy) return;
      setBusy(true);
      setStatus(T("modals.animeOverrides.saving"));
      try {
        await apiJson(API, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(readForm()),
        });
        await refresh();
        resetForm();
        setStatus(T("modals.animeOverrides.saved"), "ok");
        window.CW?.DOM?.showToast?.(T("modals.animeOverrides.savedToast"), true);
      } catch (e) {
        setStatus(String(e.message || e), "err");
      } finally {
        setBusy(false);
      }
    });

    el("ao-export").addEventListener("click", async () => {
      if (busy) return;
      if (!rows.length) { setIoStatus(T("modals.animeOverrides.nothingExport"), "err"); return; }
      setBusy(true);
      setIoStatus(T("modals.animeOverrides.preparing"));
      let url = "";
      try {
        const r = await fetch(`${API}/export`, { cache: "no-store", credentials: "same-origin" });
        if (!r.ok) throw new Error(`${r.status} ${r.statusText || ""}`.trim());
        const blob = await r.blob();
        const stamp = new Date().toISOString().replace(/[-:]/g, "").replace(/\..+$/, "Z");
        url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = `crosswatch-anime-mappings-${stamp}.json`;
        document.body.appendChild(a);
        a.click();
        a.remove();
        setIoStatus(T("modals.animeOverrides.exported", {count: rows.length}), "ok");
      } catch (e) {
        setIoStatus(T("modals.animeOverrides.exportFailed", {error: String(e.message || e)}), "err");
      } finally {
        if (url) window.setTimeout(() => URL.revokeObjectURL(url), 10_000);
        setBusy(false);
      }
    });

    el("ao-import").addEventListener("click", () => {
      if (busy) return;
      el("ao-file").value = "";
      el("ao-file").click();
    });

    el("ao-file").addEventListener("change", async (ev) => {
      const file = ev.target.files?.[0];
      if (!file) return;
      const mode = el("ao-import-mode").value;
      if (mode === "replace" && rows.length && !(await (window.CW?.MobileUI?.ask || window.confirm)(T("modals.animeOverrides.confirmReplace", {count: rows.length, file: file.name})))) {
        el("ao-file").value = "";
        return;
      }
      setBusy(true);
      setIoStatus(T("modals.animeOverrides.importing", {file: file.name}));
      try {
        let parsed;
        try {
          parsed = JSON.parse(await file.text());
        } catch {
          throw new Error(T("modals.animeOverrides.invalidJson"));
        }
        const data = await apiJson(`${API}/import`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ mode, payload: parsed }),
        });
        await refresh();
        const res = data.result || {};
        const parts = [T("modals.animeOverrides.added", {count: res.added || 0}), T("modals.animeOverrides.updated", {count: res.updated || 0})];
        if (res.skipped_count) parts.push(T("modals.animeOverrides.skipped", {count: res.skipped_count}));
        setIoStatus(T("modals.animeOverrides.imported", {file: file.name, parts: parts.join(", ")}), res.skipped_count ? "" : "ok");
        window.CW?.DOM?.showToast?.(T("modals.animeOverrides.importedToast", {count: res.total || 0}), true);
        const first = (res.skipped || [])[0];
        if (first) setIoStatus(`${parts.join(", ")}. First skipped rule: ${first.title || `#${first.index}`} - ${first.reason}`, "err");
      } catch (e) {
        setIoStatus(T("modals.animeOverrides.importFailed", {error: String(e.message || e)}), "err");
      } finally {
        el("ao-file").value = "";
        setBusy(false);
      }
    });

    el("ao-list").addEventListener("click", async (ev) => {
      const btn = ev.target.closest("button[data-act]");
      if (!btn || busy) return;
      const id = btn.closest(".ao-row")?.dataset.id;
      const row = rows.find((r) => String(r.id) === String(id));
      if (!row) return;

      if (btn.dataset.act === "edit") { editRow(row); return; }

      setBusy(true);
      try {
        if (btn.dataset.act === "delete") {
          if (!(await (window.CW?.MobileUI?.ask || window.confirm)(T("modals.animeOverrides.confirmDelete", {name: row.title || row.match_id})))) return;
          await apiJson(`${API}/${encodeURIComponent(row.id)}`, { method: "DELETE" });
          if (editingId === row.id) resetForm();
          setStatus(T("modals.animeOverrides.deleted"), "ok");
        } else {
          await apiJson(API, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ ...row, enabled: !row.enabled }),
          });
          setStatus(row.enabled ? T("modals.animeOverrides.disabled") : T("modals.animeOverrides.enabled"), "ok");
        }
        await refresh();
      } catch (e) {
        setStatus(String(e.message || e), "err");
      } finally {
        setBusy(false);
      }
    });

    syncMediaType();
    try {
      await refresh();
    } catch (e) {
      setStatus(T("modals.animeOverrides.loadFailed", {error: String(e.message || e)}), "err");
    }
    await refreshSimklStatus();
  },
};
