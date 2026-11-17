/* assets/js/shelf-editor.js */
/* CrossWatch - shelf status and copies editor for a single title */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const CURRENCIES = ["EUR", "USD", "GBP", "CAD", "AUD", "NZD", "CHF", "SEK", "NOK", "DKK", "PLN", "CZK", "HUF", "JPY", "BRL", "MXN", "INR", "ZAR"];
  const PACKAGING = { keep_case: "Keep case", steelbook: "Steelbook", digipak: "Digipak", mediabook: "Mediabook", slipcover: "Slipcover", box_set: "Box set", other: "Other" };
  const CONDITIONS = { mint: "Mint", like_new: "Like new", good: "Good", fair: "Fair", poor: "Poor" };
  const EDITIONS = ["Standard", "Steelbook", "Collector's Edition", "Limited Edition", "Special Edition", "Director's Cut", "Extended Edition", "Anniversary Edition", "Criterion Collection", "3D"];
  const RESOLUTIONS = { uhd_4k: "4K", hd_1080p: "1080p", hd_1080i: "1080i", hd_720p: "720p", sd_576p: "576p", sd_576i: "576i", sd_480p: "480p", sd_480i: "480i" };
  const HDRS = { dolby_vision: "Dolby Vision", hdr10_plus: "HDR10+", hdr10: "HDR10", hlg: "HLG" };
  const AUDIOS = { dolby_atmos: "Dolby Atmos", dolby_digital_plus_atmos: "Dolby Atmos", dolby_truehd: "Dolby TrueHD", dolby_digital_plus: "Dolby Digital Plus", dolby_digital: "Dolby Digital", dts_x: "DTS:X", dts_ma: "DTS-HD MA", dts_hr: "DTS-HD HRA", dts: "DTS", lpcm: "LPCM", flac: "FLAC", aac: "AAC" };
  const esc = (value) => String(value ?? "").replace(/[&<>"']/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch]));
  const icon = (name) => `<span class="material-symbols-rounded" aria-hidden="true">${esc(name)}</span>`;
  const shelf = () => window.CW?.Shelf || {};
  const formats = () => shelf().formats || {};
  const statuses = () => shelf().statuses || { owned: "Owned", ordered: "Ordered", wishlist: "Wishlist" };
  const state = { node: null, type: "", id: "", title: "", year: "", ids: {}, entry: null, synced: null, services: null, servicesLoading: false, flashId: "", canEdit: false, loading: true, error: "", editing: null, busy: false, lastFocus: null, seq: 0 };

  const endpoint = () => `/api/profile/shelf/${encodeURIComponent(state.type)}/${encodeURIComponent(state.id)}`;
  const options = (map, value, blank = "") => `${blank ? `<option value="">${esc(blank)}</option>` : ""}${Object.entries(map).map(([key, label]) => `<option value="${esc(key)}"${key === value ? " selected" : ""}>${esc(label)}</option>`).join("")}`;

  function dateLabel(value) {
    const parts = String(value || "").split("-").map(Number);
    if (parts.length !== 3 || parts.some((n) => !Number.isFinite(n))) return "";
    try {
      return new Date(parts[0], parts[1] - 1, parts[2]).toLocaleDateString(window.__CW_LOCALE || undefined, { day: "numeric", month: "short", year: "numeric" });
    } catch {
      return value;
    }
  }

  function copyHtml(row, index) {
    const head = [formats()[row.format] || row.format, row.format === "digital" ? row.service?.name : "", row.edition].filter(Boolean).join(" · ");
    const scope = state.type === "tv" ? [row.season != null ? `Season ${row.season}` : "", row.volume || ""] : [];
    const lineOne = [PACKAGING[row.packaging], row.region ? `Region ${row.region}` : "", row.discs ? `${row.discs} ${row.discs === 1 ? "disc" : "discs"}` : "", ...scope].filter(Boolean).join(" · ");
    const bought = [row.purchased ? `Bought ${dateLabel(row.purchased)}` : "", row.price != null ? shelf().money?.(row.price) || "" : "", row.store ? `at ${row.store}` : ""].filter(Boolean).join(" ");
    const lineTwo = [CONDITIONS[row.condition], row.shelf ? `Shelf ${row.shelf}` : "", bought].filter(Boolean).join(" · ");
    const tools = state.canEdit ? `<div class="cw-shelf-copy-tools">
        <button type="button" class="cw-mm-btn" data-shelf-edit="${index}">${icon("edit")}Edit</button>
        <button type="button" class="cw-mm-btn is-danger" data-shelf-delete="${index}">${icon("delete")}Remove</button>
      </div>` : "";
    const logo = row.format === "digital" ? shelf().logoUrl?.(row.service?.logo) || "" : "";
    const disc = logo ? "" : shelf().discUrl?.(state.type, state.id, row.format) || "";
    const art = logo ? `<img class="is-logo" src="${esc(logo)}" alt="" loading="lazy" decoding="async" onerror="this.remove()">` : disc ? `<img class="is-disc" src="${esc(disc)}" alt="" loading="lazy" decoding="async" onerror="this.remove()">` : "";
    const service = row.format === "digital" && row.service?.name ? row.service.name : "";
    return `<article class="cw-shelf-copy${row.id && row.id === state.flashId ? " is-new" : ""}" data-shelf-copy="${index}">
      <span class="cw-shelf-copy-mark${art ? " has-art" : ""}" data-format="${esc(row.format)}">${icon(row.format === "digital" ? "cloud" : "album")}${art}</span>
      <div class="cw-shelf-copy-text">
        <strong>${esc(head)}</strong>
        ${lineOne ? `<span>${esc(lineOne)}</span>` : ""}
        ${lineTwo ? `<span>${esc(lineTwo)}</span>` : ""}
        ${row.lent_to ? `<span class="is-lent">${icon("handshake")}Lent to ${esc(row.lent_to)}</span>` : ""}
        ${row.notes ? `<p>${esc(row.notes)}</p>` : ""}
      </div>
      ${tools}
    </article>`;
  }

  function syncedHtml() {
    const synced = state.synced;
    if (!synced) return "";
    const media = synced.media_info || {};
    const parts = [formats()[synced.format], RESOLUTIONS[media.resolution], HDRS[media.hdr], [AUDIOS[media.audio], media.audio_channels].filter(Boolean).join(" ")].filter(Boolean);
    if (!parts.length) return "";
    const names = (synced.providers || []).map((key) => window.CW?.ProviderMeta?.label?.(key) || String(key).toUpperCase()).join(", ");
    return `<p class="cw-shelf-hint">${icon("info")}<span>Synced media info: <strong>${esc(parts.join(" · "))}</strong>${names ? ` from ${esc(names)}` : ""}</span></p>`;
  }

  function serviceOptions(row) {
    const current = row.service?.id ? String(row.service.id) : "";
    const list = Array.isArray(state.services) ? state.services : [];
    const known = list.some((item) => String(item.id) === current);
    const extra = current && !known ? `<option value="${esc(current)}" selected>${esc(row.service?.name || current)}</option>` : "";
    const loading = !Array.isArray(state.services) ? `<option value="" disabled>${state.servicesLoading ? "Loading services..." : "Services unavailable"}</option>` : "";
    return `<option value="">Not set</option>${extra}${loading}${list.map((item) => `<option value="${esc(item.id)}"${String(item.id) === current ? " selected" : ""}>${esc(item.name)}</option>`).join("")}`;
  }

  async function loadServices() {
    if (Array.isArray(state.services) || state.servicesLoading) return;
    state.servicesLoading = true;
    try {
      const response = await fetch(`/api/metadata/watch-providers?type=${encodeURIComponent(state.type)}`, { cache: "no-store", credentials: "same-origin" });
      const data = await response.json().catch(() => ({}));
      state.services = response.ok && data?.ok && Array.isArray(data.providers) ? data.providers : null;
    } catch {
      state.services = null;
    } finally {
      state.servicesLoading = false;
    }
    const select = state.node?.querySelector("[data-shelf-form] select[name=service_id]");
    if (select) {
      const copies = state.entry?.copies || [];
      const row = state.editing === "new" || state.editing === null ? {} : copies[state.editing] || {};
      const value = select.value;
      select.innerHTML = serviceOptions(value ? { service: { id: value, name: select.selectedOptions[0]?.textContent || value } } : row);
    }
  }

  function field(label, control, wide = false) {
    return `<label class="cw-shelf-field${wide ? " is-wide" : ""}"><span>${esc(label)}</span>${control}</label>`;
  }

  function formHtml() {
    const copies = state.entry?.copies || [];
    const prefill = !copies.length && formats()[state.synced?.format] ? state.synced.format : "";
    const row = state.editing === "new" ? { format: prefill, discs: 1 } : copies[state.editing] || {};
    const tv = state.type === "tv";
    return `<form class="cw-shelf-form" data-shelf-form novalidate>
      ${syncedHtml()}
      <div class="cw-shelf-grid">
        ${field("Format", `<select name="format" required>${options(formats(), row.format, "Choose a format")}</select>`)}
        <label class="cw-shelf-field" data-shelf-service-field${row.format === "digital" ? "" : " hidden"}><span>Service</span><select name="service_id">${serviceOptions(row)}</select></label>
        ${field("Edition", `<input name="edition" list="cw-shelf-editions" maxlength="80" value="${esc(row.edition || "")}" placeholder="Standard">`)}
        ${field("Packaging", `<select name="packaging">${options(PACKAGING, row.packaging, "Not set")}</select>`)}
        ${field("Condition", `<select name="condition">${options(CONDITIONS, row.condition, "Not set")}</select>`)}
        ${field("Region", `<input name="region" maxlength="12" value="${esc(row.region || "")}" placeholder="A, B, 2, Free">`)}
        ${field("Discs", `<input name="discs" type="number" min="1" max="99" value="${esc(row.discs ?? "")}">`)}
        ${tv ? field("Season", `<input name="season" type="number" min="0" max="999" value="${esc(row.season ?? "")}" placeholder="All seasons">`) : ""}
        ${tv ? field("Volume or part", `<input name="volume" maxlength="40" value="${esc(row.volume || "")}" placeholder="Part 1">`) : ""}
        ${field("Purchased", `<input name="purchased" type="date" value="${esc(row.purchased || "")}">`)}
        ${field("Store", `<input name="store" maxlength="80" value="${esc(row.store || "")}" placeholder="Where you bought it">`)}
        ${field("Price", `<span class="cw-shelf-price"><input name="price" type="number" min="0" step="0.01" inputmode="decimal" value="${esc(row.price ?? "")}"><select data-shelf-currency aria-label="Currency for all prices">${[shelf().currency?.() || "", ...CURRENCIES].filter((code, i, all) => all.indexOf(code) === i).map((code) => `<option value="${esc(code)}"${code === (shelf().currency?.() || "") ? " selected" : ""}>${esc(code || "No currency")}</option>`).join("")}</select></span>`)}
        ${field("Shelf", `<input name="shelf" maxlength="60" value="${esc(row.shelf || "")}" placeholder="Living room A3">`)}
        ${field("Lent to", `<input name="lent_to" maxlength="80" value="${esc(row.lent_to || "")}" placeholder="Nobody">`)}
        ${field("Notes", `<textarea name="notes" maxlength="500" rows="3">${esc(row.notes || "")}</textarea>`, true)}
      </div>
      <datalist id="cw-shelf-editions">${EDITIONS.map((name) => `<option value="${esc(name)}"></option>`).join("")}</datalist>
      <p class="cw-mm-art-note is-error" data-shelf-form-error hidden></p>
      <div class="cw-shelf-form-actions">
        <button type="button" class="cw-mm-btn" data-shelf-cancel>Cancel</button>
        <button type="submit" class="cw-mm-btn is-primary">${icon("check")}${state.editing === "new" ? "Add copy" : "Save copy"}</button>
      </div>
    </form>`;
  }

  function bodyHtml() {
    if (state.loading) return `<p class="cw-mm-art-note">Loading shelf...</p>`;
    if (state.error && !state.entry && state.editing === null) return `<p class="cw-mm-art-note is-error">${esc(state.error)}</p>`;
    if (state.editing !== null) return formHtml();
    const copies = state.entry?.copies || [];
    const error = state.error ? `<p class="cw-mm-art-note is-error">${esc(state.error)}</p>` : "";
    if (!copies.length) {
      const hint = !state.entry ? "This title is not on your shelf yet." : state.entry.status === "wishlist" ? "On your wishlist. Add a copy once you have it." : "No copies added yet.";
      return `${error}${syncedHtml()}<p class="cw-mm-art-note">${esc(hint)}${state.canEdit ? " Choose a status above or add a copy." : ""}</p>`;
    }
    return `${error}${syncedHtml()}<div class="cw-shelf-copies">${copies.map(copyHtml).join("")}</div>`;
  }

  function render() {
    if (!state.node) return;
    const current = state.entry?.status || "";
    state.node.querySelectorAll("[data-shelf-status]").forEach((btn) => {
      const on = btn.dataset.shelfStatus === current;
      btn.classList.toggle("is-active", on);
      btn.setAttribute("aria-checked", on ? "true" : "false");
      btn.disabled = state.busy || state.loading || !state.canEdit;
    });
    const add = state.node.querySelector("[data-shelf-add]");
    if (add) add.hidden = !state.canEdit || state.editing !== null || state.loading;
    const remove = state.node.querySelector("[data-shelf-remove]");
    if (remove) remove.hidden = !state.canEdit || !state.entry || state.editing !== null;
    state.node.querySelectorAll("button[data-shelf-add],button[data-shelf-remove]").forEach((btn) => { btn.disabled = state.busy; });
    state.node.querySelector(".cw-mm-art-body").innerHTML = bodyHtml();
    state.flashId = "";
    const formatSelect = state.node.querySelector("[data-shelf-form] select[name=format]");
    formatSelect?.focus({ preventScroll: true });
    if (formatSelect?.value === "digital") void loadServices();
  }

  async function load() {
    const seq = ++state.seq;
    try {
      const response = await fetch(endpoint(), { cache: "no-store", credentials: "same-origin" });
      const data = await response.json().catch(() => ({}));
      if (!response.ok || !data?.ok) throw new Error(data?.error || "Shelf could not be loaded.");
      if (seq !== state.seq) return;
      state.entry = data.entry || null;
      state.synced = data.synced && typeof data.synced === "object" ? data.synced : null;
      state.canEdit = data.can_edit === true && state.canEdit;
    } catch (error) {
      if (seq !== state.seq) return;
      state.error = error?.message || "Shelf could not be loaded.";
    }
    state.loading = false;
    render();
  }

  async function save(next) {
    if (state.busy) return false;
    state.busy = true;
    state.error = "";
    render();
    try {
      const body = { status: next.status, title: state.title, year: state.year || undefined, ids: state.ids, copies: next.copies };
      const response = await fetch(endpoint(), { method: "PUT", headers: { "Content-Type": "application/json" }, cache: "no-store", credentials: "same-origin", body: JSON.stringify(body) });
      const data = await response.json().catch(() => ({}));
      if (!response.ok || !data?.ok) throw new Error(data?.error || "Saving failed.");
      state.entry = data.entry;
      shelf().set?.(state.type, state.id, data.summary || null);
      return true;
    } catch (error) {
      state.error = error?.message || "Saving failed.";
      return false;
    } finally {
      state.busy = false;
      render();
    }
  }

  async function removeTitle() {
    if (state.busy || !state.entry) return;
    state.busy = true;
    render();
    try {
      const response = await fetch(endpoint(), { method: "DELETE", cache: "no-store", credentials: "same-origin" });
      const data = await response.json().catch(() => ({}));
      if (!response.ok || !data?.ok) throw new Error(data?.error || "Removing failed.");
      state.entry = null;
      shelf().set?.(state.type, state.id, null);
    } catch (error) {
      state.error = error?.message || "Removing failed.";
    } finally {
      state.busy = false;
      render();
    }
  }

  function readForm(form) {
    const data = new FormData(form);
    const row = {};
    for (const [key, value] of data.entries()) {
      const text = String(value || "").trim();
      if (text) row[key] = text;
    }
    if (row.discs) row.discs = Number(row.discs);
    if (row.price) row.price = Number(row.price);
    const serviceId = row.service_id;
    delete row.service_id;
    if (row.format === "digital" && serviceId) {
      const found = (state.services || []).find((item) => String(item.id) === String(serviceId));
      const previous = state.editing !== "new" ? (state.entry?.copies || [])[state.editing]?.service : null;
      const service = found || (previous && String(previous.id) === String(serviceId) ? previous : null);
      if (service) row.service = { id: Number(service.id), name: service.name, ...(service.logo ? { logo: service.logo } : {}) };
    }
    if (row.season) row.season = Number(row.season);
    return row;
  }

  async function submitForm(form) {
    const row = readForm(form);
    const error = form.querySelector("[data-shelf-form-error]");
    if (!row.format) {
      if (error) {
        error.textContent = "Choose a format.";
        error.hidden = false;
      }
      form.querySelector("select[name=format]")?.focus();
      return;
    }
    const copies = [...(state.entry?.copies || [])];
    if (state.editing === "new") copies.push(row);
    else {
      const before = { ...copies[state.editing] };
      if (row.format !== "digital" || !row.service) delete before.service;
      copies[state.editing] = { ...before, ...row, id: before.id, added_at: before.added_at };
    }
    const status = state.entry?.status === "wishlist" && state.editing === "new" ? "owned" : state.entry?.status || "owned";
    const editing = state.editing;
    state.editing = null;
    if (await save({ status, copies })) {
      const saved = state.entry?.copies || [];
      state.flashId = (editing === "new" ? saved[saved.length - 1] : saved[editing])?.id || "";
    } else {
      state.editing = editing;
    }
    render();
  }

  function onClick(event) {
    const target = event.target;
    if (target === state.node || target?.closest?.("[data-shelf-close]")) { close(); return; }
    const status = target?.closest?.("[data-shelf-status]");
    if (status && state.canEdit) {
      if (status.dataset.shelfStatus !== state.entry?.status) void save({ status: status.dataset.shelfStatus, copies: state.entry?.copies || [] });
      return;
    }
    if (target?.closest?.("[data-shelf-add]")) { state.editing = "new"; state.error = ""; render(); return; }
    if (target?.closest?.("[data-shelf-cancel]")) { state.editing = null; render(); return; }
    if (target?.closest?.("[data-shelf-remove]")) { void removeTitle(); return; }
    const edit = target?.closest?.("[data-shelf-edit]");
    if (edit) { state.editing = Number(edit.dataset.shelfEdit); state.error = ""; render(); return; }
    const del = target?.closest?.("[data-shelf-delete]");
    if (del && !state.busy) {
      const copies = [...(state.entry?.copies || [])];
      copies.splice(Number(del.dataset.shelfDelete), 1);
      del.closest("[data-shelf-copy]")?.classList.add("is-leaving");
      const quick = window.matchMedia?.("(prefers-reduced-motion: reduce)")?.matches;
      setTimeout(() => { void save({ status: state.entry?.status || "owned", copies }); }, quick ? 0 : 180);
    }
  }

  async function saveCurrency(select) {
    const value = select.value;
    try {
      const response = await fetch("/api/profile/shelf/settings", { method: "PUT", headers: { "Content-Type": "application/json" }, cache: "no-store", credentials: "same-origin", body: JSON.stringify({ currency: value }) });
      const data = await response.json().catch(() => ({}));
      if (!response.ok || !data?.ok) throw new Error(data?.error || "Saving the currency failed.");
      shelf().setCurrency?.(data.currency || "");
    } catch (error) {
      select.value = shelf().currency?.() || "";
      const note = state.node?.querySelector("[data-shelf-form-error]");
      if (note) {
        note.textContent = error?.message || "Saving the currency failed.";
        note.hidden = false;
      }
    }
  }

  function onChange(event) {
    const select = event.target?.closest?.("[data-shelf-currency]");
    if (select && state.canEdit) void saveCurrency(select);
    const format = event.target?.closest?.("[data-shelf-form] select[name=format]");
    if (format) {
      const field = state.node?.querySelector("[data-shelf-service-field]");
      if (field) field.hidden = format.value !== "digital";
      if (format.value === "digital") void loadServices();
    }
  }

  function onSubmit(event) {
    const form = event.target?.closest?.("[data-shelf-form]");
    if (!form) return;
    event.preventDefault();
    void submitForm(form);
  }

  function onKey(event) {
    if (event.key !== "Escape" || !state.node) return;
    event.preventDefault();
    event.stopImmediatePropagation();
    if (state.editing !== null) {
      state.editing = null;
      render();
      return;
    }
    close();
  }

  function open({ type, id, title = "", year = "", ids = {}, canEdit = false, host = document.body } = {}) {
    if (!type || !id) return false;
    close();
    Object.assign(state, { type: type === "movie" ? "movie" : "tv", id: String(id), title: String(title || ""), year: year || "", ids: ids && typeof ids === "object" ? ids : {}, entry: null, synced: null, canEdit: !!canEdit, loading: true, error: "", editing: null, busy: false, lastFocus: document.activeElement });
    const node = document.createElement("div");
    node.className = "cw-mm-art cw-shelf";
    node.innerHTML = `<section class="cw-mm-art-panel" role="dialog" aria-modal="true" aria-labelledby="cw-shelf-title" tabindex="-1">
        <header class="cw-mm-art-head">
          <div><h3 id="cw-shelf-title">My shelf</h3><p>${esc(state.title)}</p></div>
          <button type="button" class="cw-mm-art-close" data-shelf-close aria-label="Close">${icon("close")}</button>
        </header>
        <div class="cw-mm-art-bar">
          <div class="cw-mm-art-kinds" role="radiogroup" aria-label="Shelf status">${Object.entries(statuses()).map(([key, label]) => `<button type="button" class="cw-mm-art-pill" role="radio" aria-checked="false" data-shelf-status="${esc(key)}">${esc(label)}</button>`).join("")}</div>
        </div>
        <div class="cw-mm-art-body"></div>
        <footer class="cw-mm-art-foot">
          <button type="button" class="cw-mm-btn is-danger" data-shelf-remove hidden>${icon("delete")}Remove from shelf</button>
          <button type="button" class="cw-mm-btn is-primary" data-shelf-add hidden>${icon("add")}Add copy</button>
        </footer>
      </section>`;
    node.addEventListener("click", onClick);
    node.addEventListener("submit", onSubmit);
    node.addEventListener("change", onChange);
    node.addEventListener("keydown", onKey);
    host.appendChild(node);
    state.node = node;
    render();
    node.querySelector(".cw-mm-art-panel")?.focus({ preventScroll: true });
    void load();
    return true;
  }

  function close() {
    if (!state.node) return;
    state.node.remove();
    state.node = null;
    state.seq += 1;
    try { state.lastFocus?.focus?.({ preventScroll: true }); } catch {}
  }

  (window.CW ||= {}).ShelfEditor = { open, close, isOpen: () => !!state.node };
})();
