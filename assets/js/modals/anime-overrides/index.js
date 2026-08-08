/* assets/js/modals/anime-overrides/index.js */
/* Anime mapping overrides modal. */

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
    return data;
  } catch (e) {
    if (e?.name === "AbortError") throw new Error("Request timed out");
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
  return `${from} S${esc(row.match_season)} E${esc(row.episode_from)}-${last} &rarr; ${to} episode ${span}`;
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

    root.innerHTML = `
      <div class="cw-anime-overrides">
        <div class="cx-head">
          <div class="ao-head-left">
            <div class="ao-head-icon"><span class="material-symbols-rounded" aria-hidden="true">rule_settings</span></div>
            <div>
              <div class="ao-title">Custom anime mappings</div>
              <div class="ao-sub">Your rules always win over the downloaded AniBridge dataset.</div>
            </div>
          </div>
          <button type="button" class="ao-close" id="ao-close" title="Close" aria-label="Close"><span class="material-symbols-rounded" aria-hidden="true">close</span></button>
        </div>

        <div class="ao-body cw-scrollbars">
          <section class="ao-card">
            <div class="ao-card-head">
              <span class="material-symbols-rounded" aria-hidden="true">edit_note</span>
              <div>
                <h3 id="ao-form-title">Add a rule</h3>
                <p>Point a show or movie at the anime entry it really is. Leave the episode fields empty to only correct the ID.</p>
              </div>
            </div>

            <div class="ao-grid">
              <label><span>This is a</span>
                <select id="ao-media-type">
                  <option value="show">TV show</option>
                  <option value="movie">Movie</option>
                </select>
              </label>
              <label><span>Name (optional)</span>
                <input id="ao-title" type="text" placeholder="Dragon Ball Z" maxlength="200">
              </label>
            </div>

            <div class="ao-rule-row">
              <div class="ao-rule-side">
                <div class="ao-rule-legend">When the item is</div>
                <div class="ao-inline">
                  <select id="ao-match-provider"></select>
                  <input id="ao-match-id" type="text" placeholder="ID" maxlength="64">
                </div>
                <label class="ao-season" id="ao-season-wrap"><span>Season</span>
                  <input id="ao-match-season" type="number" min="0" step="1" placeholder="any">
                </label>
              </div>
              <span class="material-symbols-rounded ao-arrow" aria-hidden="true">arrow_forward</span>
              <div class="ao-rule-side">
                <div class="ao-rule-legend">Treat it as</div>
                <div class="ao-inline">
                  <select id="ao-target-namespace"></select>
                  <input id="ao-target-id" type="text" placeholder="ID" maxlength="64">
                </div>
              </div>
            </div>

            <div class="ao-episodes" id="ao-episodes">
              <div class="ao-rule-legend">Episode numbers (optional)</div>
              <div class="ao-grid ao-grid-3">
                <label><span>First episode</span><input id="ao-ep-from" type="number" min="1" step="1" placeholder="1"></label>
                <label><span>Last episode</span><input id="ao-ep-to" type="number" min="1" step="1" placeholder="leave empty for open ended"></label>
                <label><span>Counts as</span><input id="ao-ep-start" type="number" min="1" step="1" placeholder="40"></label>
              </div>
              <p class="ao-hint" id="ao-preview"></p>
            </div>

            <label class="ao-note"><span>Note (optional)</span>
              <input id="ao-note" type="text" placeholder="Why this rule exists" maxlength="500">
            </label>

            <div class="ao-form-actions">
              <span class="ao-status" id="ao-status"></span>
              <button type="button" class="ao-btn" id="ao-reset" hidden>Cancel edit</button>
              <button type="button" class="ao-btn primary" id="ao-save">Add rule</button>
            </div>
          </section>

          <section class="ao-card">
            <div class="ao-card-head compact">
              <span class="material-symbols-rounded" aria-hidden="true">list</span>
              <div>
                <h3>Your rules <span class="ao-count" id="ao-count"></span></h3>
                <p>Rules are checked in order, top first. Disabled rules are ignored.</p>
              </div>
            </div>
            <div id="ao-list" class="ao-list"></div>
          </section>
        </div>
      </div>
    `;

    const el = (id) => $(`#${id}`, root);
    const statusEl = el("ao-status");

    function setStatus(message, tone = "") {
      statusEl.textContent = message || "";
      statusEl.dataset.tone = tone;
    }

    function setBusy(on) {
      busy = !!on;
      $$("button, input, select", root).forEach((n) => { n.disabled = busy; });
      if (!busy) syncMediaType();
    }

    function fillSelect(node, values, current) {
      node.innerHTML = values.map((v) => `<option value="${esc(v)}">${esc(label(v))}</option>`).join("");
      if (current) node.value = current;
    }

    function syncMediaType() {
      const isMovie = el("ao-media-type").value === "movie";
      el("ao-episodes").hidden = isMovie;
      el("ao-season-wrap").hidden = isMovie;
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
      const last = d.episode_to == null ? "onwards" : `E${d.episode_to}`;
      const endsAt = d.episode_to == null ? "onwards" : String(d.episode_start_at + (d.episode_to - d.episode_from));
      el("ao-preview").textContent =
        `S${d.match_season ?? "?"}E${d.episode_from} to ${last} will be written as episode ${d.episode_start_at} to ${endsAt}.`;
    }

    function resetForm() {
      editingId = "";
      el("ao-form-title").textContent = "Add a rule";
      el("ao-save").textContent = "Add rule";
      el("ao-reset").hidden = true;
      ["ao-title", "ao-match-id", "ao-target-id", "ao-note", "ao-match-season", "ao-ep-from", "ao-ep-to", "ao-ep-start"]
        .forEach((id) => { el(id).value = ""; });
      el("ao-media-type").value = "show";
      syncMediaType();
      setStatus("");
    }

    function editRow(row) {
      editingId = String(row.id || "");
      el("ao-form-title").textContent = "Edit rule";
      el("ao-save").textContent = "Save changes";
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
      syncMediaType();
      setStatus("Editing an existing rule.");
      el("ao-match-id").focus();
    }

    function renderList() {
      el("ao-count").textContent = rows.length ? `(${rows.length})` : "";
      if (!rows.length) {
        el("ao-list").innerHTML = `
          <div class="ao-empty">
            <span class="material-symbols-rounded" aria-hidden="true">inbox</span>
            <p>No custom rules yet. CrossWatch is using the downloaded dataset for every anime.</p>
          </div>`;
        return;
      }
      el("ao-list").innerHTML = rows.map((row) => `
        <div class="ao-row${row.enabled ? "" : " is-off"}" data-id="${esc(row.id)}">
          <div class="ao-row-main">
            <div class="ao-row-title">
              <span class="ao-chip">${esc(row.media_type === "movie" ? "Movie" : "Show")}</span>
              ${esc(row.title || "Untitled rule")}
            </div>
            <div class="ao-row-rule">${ruleSummary(row)}</div>
            ${row.note ? `<div class="ao-row-note">${esc(row.note)}</div>` : ""}
          </div>
          <div class="ao-row-actions">
            <button type="button" class="ao-icon" data-act="toggle" title="${row.enabled ? "Disable" : "Enable"}">
              <span class="material-symbols-rounded" aria-hidden="true">${row.enabled ? "toggle_on" : "toggle_off"}</span>
            </button>
            <button type="button" class="ao-icon" data-act="edit" title="Edit"><span class="material-symbols-rounded" aria-hidden="true">edit</span></button>
            <button type="button" class="ao-icon danger" data-act="delete" title="Delete"><span class="material-symbols-rounded" aria-hidden="true">delete</span></button>
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
    }

    el("ao-close").addEventListener("click", () => window.cxCloseModal?.());
    el("ao-media-type").addEventListener("change", syncMediaType);
    ["ao-ep-from", "ao-ep-to", "ao-ep-start", "ao-match-season"].forEach((id) => {
      el(id).addEventListener("input", renderPreview);
    });
    el("ao-reset").addEventListener("click", resetForm);

    el("ao-save").addEventListener("click", async () => {
      if (busy) return;
      setBusy(true);
      setStatus("Saving...");
      try {
        await apiJson(API, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(readForm()),
        });
        await refresh();
        resetForm();
        setStatus("Rule saved.", "ok");
        window.CW?.DOM?.showToast?.("Anime mapping rule saved", true);
      } catch (e) {
        setStatus(String(e.message || e), "err");
      } finally {
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
          if (!window.confirm(`Delete the rule for ${row.title || row.match_id}?`)) return;
          await apiJson(`${API}/${encodeURIComponent(row.id)}`, { method: "DELETE" });
          if (editingId === row.id) resetForm();
          setStatus("Rule deleted.", "ok");
        } else {
          await apiJson(API, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ ...row, enabled: !row.enabled }),
          });
          setStatus(row.enabled ? "Rule disabled." : "Rule enabled.", "ok");
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
      setStatus(`Could not load rules: ${e.message || e}`, "err");
    }
  },
};
