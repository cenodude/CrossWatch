/* assets/js/artwork-picker.js */
/* CrossWatch - per-title artwork picker for posters, logos, backgrounds and discs */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const KINDS = [["poster", "Posters"], ["logo", "Logos"], ["backdrop", "Backgrounds"], ["disc", "Discs"]];
  const DISC_TYPES = { bluray: "Blu-ray", dvd: "DVD", "3d": "3D" };
  const PAGE = 18;
  const esc = (value) => String(value ?? "").replace(/[&<>"']/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch]));
  const icon = (name) => `<span class="material-symbols-rounded" aria-hidden="true">${esc(name)}</span>`;
  const state = { node: null, type: "", id: "", title: "", kind: "poster", source: "all", shown: PAGE, data: new Map(), busy: false, seq: 0, lastFocus: null };

  const sourceLabel = (source) => (source === "fanart" ? "fanart.tv" : "TMDb");
  const langLabel = (lang) => (!lang || lang === "00" || lang === "xx" ? "No text" : lang.toUpperCase());

  function endpoint(kind) {
    const base = `/api/artwork/${encodeURIComponent(state.type)}/${encodeURIComponent(state.id)}`;
    return kind ? `${base}?kind=${encodeURIComponent(kind)}` : base;
  }

  function current() {
    return state.data.get(state.kind) || null;
  }

  function filtered(data) {
    const rows = Array.isArray(data?.options) ? data.options : [];
    return state.source === "all" || state.kind === "disc" ? rows : rows.filter((row) => row.source === state.source);
  }

  function isCurrent(data, row) {
    return !!data?.current && data.current.source === row.source && data.current.ref === row.ref;
  }

  function optionHtml(data, row, index) {
    const meta = row.disc_type ? [DISC_TYPES[row.disc_type] || "Disc", sourceLabel(row.source)] : [sourceLabel(row.source), langLabel(row.lang)];
    if (row.source === "fanart" && row.likes) meta.push(`${row.likes} likes`);
    if (row.source === "tmdb" && row.width) meta.push(`${row.width}×${row.height}`);
    const active = isCurrent(data, row);
    return `<button type="button" class="cw-mm-art-opt${active ? " is-current" : ""}" data-art-pick="${index}" aria-pressed="${active ? "true" : "false"}" title="${esc(meta.join(" · "))}">
      <img src="${esc(row.thumb)}" alt="" loading="lazy" decoding="async">
      <span class="cw-mm-art-tag">${esc(meta.slice(0, 2).join(" · "))}</span>
      ${active ? `<span class="cw-mm-art-check">${icon("check")}</span>` : ""}
    </button>`;
  }

  function bodyHtml() {
    const data = current();
    if (!data || data.loading) return `<p class="cw-mm-art-note">Loading artwork...</p>`;
    if (data.error) return `<p class="cw-mm-art-note is-error">${esc(data.error)}</p>`;
    const rows = filtered(data);
    const notes = (data.errors || []).map((text) => `<p class="cw-mm-art-note is-error">${esc(text)}</p>`).join("");
    const missing = data.current?.missing ? `<p class="cw-mm-art-note is-error">The chosen image is no longer available, so automatic artwork is shown.</p>` : "";
    if (!rows.length) return `${notes}${missing}<p class="cw-mm-art-note">No ${esc(KINDS.find(([kind]) => kind === state.kind)?.[1].toLowerCase() || "artwork")} found for this title.</p>`;
    const all = Array.isArray(data.options) ? data.options : [];
    const shown = rows.slice(0, state.shown);
    const grid = shown.map((row) => optionHtml(data, row, all.indexOf(row))).join("");
    const more = rows.length > shown.length ? `<button type="button" class="cw-mm-btn cw-mm-art-more" data-art-more>Show more (${rows.length - shown.length})</button>` : "";
    return `${notes}${missing}<div class="cw-mm-art-grid" data-kind="${esc(state.kind)}">${grid}</div>${more}`;
  }

  function render() {
    if (!state.node) return;
    const data = current();
    const hasFanart = !!data?.fanart;
    const fanartOn = [...state.data.values()].some((row) => row?.fanart);
    state.node.querySelectorAll("[data-art-kind]").forEach((btn) => {
      if (btn.dataset.artKind === "disc") btn.hidden = state.type !== "movie" || !fanartOn;
      const on = btn.dataset.artKind === state.kind;
      btn.classList.toggle("is-active", on);
      btn.setAttribute("aria-selected", on ? "true" : "false");
    });
    const sources = state.node.querySelector(".cw-mm-art-sources");
    if (sources) {
      sources.hidden = !hasFanart || state.kind === "disc";
      sources.querySelectorAll("[data-art-source]").forEach((btn) => btn.classList.toggle("is-active", btn.dataset.artSource === state.source));
    }
    const reset = state.node.querySelector("[data-art-reset]");
    if (reset) reset.disabled = state.busy || !data?.current;
    state.node.querySelector(".cw-mm-art-body").innerHTML = bodyHtml();
  }

  async function load(kind) {
    const seq = ++state.seq;
    if (state.data.get(kind) && !state.data.get(kind).error) {
      render();
      return;
    }
    state.data.set(kind, { loading: true });
    render();
    try {
      const response = await fetch(endpoint(kind), { cache: "no-store", credentials: "same-origin" });
      const data = await response.json().catch(() => ({}));
      if (!response.ok || !data?.ok) throw new Error(data?.error || "Artwork could not be loaded.");
      state.data.set(kind, data);
    } catch (error) {
      state.data.set(kind, { error: error?.message || "Artwork could not be loaded." });
    }
    if (seq === state.seq) render();
  }

  function applyRev(rev) {
    state.data.forEach((data) => { if (data && !data.loading && !data.error) data.rev = rev; });
    window.CW?.ArtChoice?.set?.(state.type, state.id, rev);
  }

  async function pick(index) {
    const data = current();
    const row = data?.options?.[index];
    if (!row || state.busy || isCurrent(data, row)) return;
    state.busy = true;
    const button = state.node.querySelector(`[data-art-pick="${index}"]`);
    button?.classList.add("is-saving");
    try {
      const response = await fetch(endpoint(""), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        cache: "no-store",
        credentials: "same-origin",
        body: JSON.stringify({ kind: state.kind, source: row.source, ref: row.ref, lang: row.lang || "" }),
      });
      const result = await response.json().catch(() => ({}));
      if (!response.ok || !result?.ok) throw new Error(result?.error || "Saving failed.");
      data.current = { source: row.source, ref: row.ref };
      applyRev(result.rev || "");
    } catch (error) {
      data.errors = [error?.message || "Saving failed."];
    } finally {
      state.busy = false;
      render();
    }
  }

  async function reset() {
    const data = current();
    if (!data?.current || state.busy) return;
    state.busy = true;
    render();
    try {
      const response = await fetch(endpoint(state.kind), { method: "DELETE", cache: "no-store", credentials: "same-origin" });
      const result = await response.json().catch(() => ({}));
      if (!response.ok || !result?.ok) throw new Error(result?.error || "Reset failed.");
      data.current = null;
      applyRev(result.rev || "");
    } catch (error) {
      data.errors = [error?.message || "Reset failed."];
    } finally {
      state.busy = false;
      render();
    }
  }

  function onClick(event) {
    const target = event.target;
    if (target === state.node || target?.closest?.("[data-art-close]")) { close(); return; }
    const kind = target?.closest?.("[data-art-kind]");
    if (kind) {
      state.kind = kind.dataset.artKind;
      state.shown = PAGE;
      void load(state.kind);
      return;
    }
    const source = target?.closest?.("[data-art-source]");
    if (source) {
      state.source = source.dataset.artSource;
      state.shown = PAGE;
      render();
      return;
    }
    if (target?.closest?.("[data-art-more]")) {
      state.shown += PAGE;
      render();
      return;
    }
    if (target?.closest?.("[data-art-reset]")) { void reset(); return; }
    const option = target?.closest?.("[data-art-pick]");
    if (option) void pick(Number(option.dataset.artPick));
  }

  function onKey(event) {
    if (event.key !== "Escape" || !state.node) return;
    event.preventDefault();
    event.stopImmediatePropagation();
    close();
  }

  function open({ type, id, title = "", host = document.body } = {}) {
    if (!type || !id) return false;
    close();
    Object.assign(state, { type: type === "movie" ? "movie" : "tv", id: String(id), title: String(title || ""), kind: "poster", source: "all", shown: PAGE, data: new Map(), busy: false, lastFocus: document.activeElement });
    const node = document.createElement("div");
    node.className = "cw-mm-art";
    node.innerHTML = `<section class="cw-mm-art-panel" role="dialog" aria-modal="true" aria-labelledby="cw-mm-art-title" tabindex="-1">
        <header class="cw-mm-art-head">
          <div><h3 id="cw-mm-art-title">Change artwork</h3><p>${esc(state.title)}</p></div>
          <button type="button" class="cw-mm-art-close" data-art-close aria-label="Close">${icon("close")}</button>
        </header>
        <div class="cw-mm-art-bar">
          <div class="cw-mm-art-kinds" role="tablist">${KINDS.map(([kind, label]) => `<button type="button" class="cw-mm-art-pill" role="tab" data-art-kind="${kind}"${kind === "disc" ? " hidden" : ""}>${label}</button>`).join("")}</div>
          <div class="cw-mm-art-sources" hidden>${[["all", "All"], ["tmdb", "TMDb"], ["fanart", "fanart.tv"]].map(([source, label]) => `<button type="button" class="cw-mm-art-pill" data-art-source="${source}">${label}</button>`).join("")}</div>
        </div>
        <div class="cw-mm-art-body"></div>
        <footer class="cw-mm-art-foot">
          <small>Your choice is used everywhere in CrossWatch and kept when the cache is cleared.</small>
          <button type="button" class="cw-mm-btn" data-art-reset>${icon("restart_alt")}Use automatic</button>
        </footer>
      </section>`;
    node.addEventListener("click", onClick);
    node.addEventListener("keydown", onKey);
    host.appendChild(node);
    state.node = node;
    window.CW?.DialogFocus?.activate(node, close);
    node.querySelector(".cw-mm-art-panel")?.focus({ preventScroll: true });
    void load("poster");
    return true;
  }

  function close() {
    if (!state.node) return;
    window.CW?.DialogFocus?.deactivate(state.node);
    state.node.remove();
    state.node = null;
    state.seq += 1;
    try { state.lastFocus?.focus?.({ preventScroll: true }); } catch {}
  }

  (window.CW ||= {}).ArtworkPicker = { open, close, isOpen: () => !!state.node };
})();
