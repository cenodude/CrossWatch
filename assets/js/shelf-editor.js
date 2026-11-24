/* assets/js/shelf-editor.js */
/* CrossWatch - shelf status and copies editor for a single title */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;
  const CURRENCIES = ["EUR", "USD", "GBP", "CAD", "AUD", "NZD", "CHF", "SEK", "NOK", "DKK", "PLN", "CZK", "HUF", "JPY", "BRL", "MXN", "INR", "ZAR"];
  const QUICK = ["4k_bluray", "bluray", "dvd", "digital"];
  const PACKAGING = Object.defineProperties({}, Object.fromEntries(["keep_case", "steelbook", "digipak", "mediabook", "slipcover", "box_set", "other"].map(key => [key, { enumerable: true, get: () => T(`library.shelfEditor.packaging.${key}`) }])));
  const CONDITIONS = Object.defineProperties({}, Object.fromEntries(["mint", "like_new", "good", "fair", "poor"].map(key => [key, { enumerable: true, get: () => T(`library.shelfEditor.condition.${key}`) }])));
  const EDITIONS = ["Standard", "Steelbook", "Collector's Edition", "Limited Edition", "Special Edition", "Director's Cut", "Extended Edition", "Anniversary Edition", "Criterion Collection", "3D"];
  const RESOLUTIONS = { uhd_4k: "4K", hd_1080p: "1080p", hd_1080i: "1080i", hd_720p: "720p", sd_576p: "576p", sd_576i: "576i", sd_480p: "480p", sd_480i: "480i" };
  const HDRS = { dolby_vision: "Dolby Vision", hdr10_plus: "HDR10+", hdr10: "HDR10", hlg: "HLG" };
  const AUDIOS = { dolby_atmos: "Dolby Atmos", dolby_digital_plus_atmos: "Dolby Atmos", dolby_truehd: "Dolby TrueHD", dolby_digital_plus: "Dolby Digital Plus", dolby_digital: "Dolby Digital", dts_x: "DTS:X", dts_ma: "DTS-HD MA", dts_hr: "DTS-HD HRA", dts: "DTS", lpcm: "LPCM", flac: "FLAC", aac: "AAC" };
  const AUDIO_CHOICES = { dolby_atmos: "Dolby Atmos (TrueHD)", dolby_digital_plus_atmos: "Dolby Atmos (DD+)", dolby_truehd: "Dolby TrueHD", dolby_digital_plus: "Dolby Digital Plus", dolby_digital: "Dolby Digital", dolby_prologic: "Dolby Pro Logic", dts_x: "DTS:X", dts_ma: "DTS-HD MA", dts_hr: "DTS-HD HRA", dts: "DTS", auro_3d: "Auro-3D", lpcm: "LPCM", flac: "FLAC", aac: "AAC", mp3: "MP3", mp2: "MP2", ogg: "Ogg Vorbis", ogg_opus: "Opus", wma: "WMA" };
  const CHANNELS = Object.fromEntries(["1.0", "2.0", "2.1", "3.0", "3.1", "4.0", "4.1", "5.0", "5.1", "5.1.2", "5.1.4", "6.1", "7.1", "7.1.2", "7.1.4", "9.1", "10.1"].map((value) => [value, value]));
  const esc = (value) => String(value ?? "").replace(/[&<>"']/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch]));
  const icon = (name) => `<span class="material-symbols-rounded" aria-hidden="true">${esc(name)}</span>`;
  const shelf = () => window.CW?.Shelf || {};
  const formats = () => shelf().formats || {};
  const statuses = () => shelf().statuses || { owned: T("library.shelf.status.owned"), ordered: T("library.shelf.status.ordered"), wishlist: T("library.shelf.status.wishlist") };
  const state = { node: null, type: "", id: "", title: "", year: "", ids: {}, entry: null, synced: null, services: null, servicesLoading: false, draft: null, groups: {}, confirmRemove: false, confirmDelete: null, canEdit: false, loading: true, error: "", editing: null, busy: false, lastFocus: null, seq: 0, fallbackSynced: null, start: null, onSaved: null };

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

  function syncedText() {
    const synced = state.synced;
    if (!synced) return "";
    const media = synced.media_info || {};
    return [formats()[synced.format], RESOLUTIONS[media.resolution], HDRS[media.hdr], [AUDIOS[media.audio], media.audio_channels].filter(Boolean).join(" ")].filter(Boolean).join(" · ");
  }

  function copyMedia(row) {
    return [RESOLUTIONS[row.resolution], HDRS[row.hdr], [AUDIOS[row.audio], row.audio_channels].filter(Boolean).join(" "), row.is_3d ? "3D" : ""].filter(Boolean).join(" · ");
  }

  function copySummary(row) {
    const parts = [
      copyMedia(row),
      row.format === "digital" ? row.service?.name : "",
      row.edition || PACKAGING[row.packaging],
      row.shelf ? T("library.shelfEditor.shelfN", {name: row.shelf}) : "",
      row.purchased ? dateLabel(row.purchased) : "",
      row.price != null ? shelf().money?.(row.price) || "" : "",
      row.lent_to ? T("library.shelfEditor.lentTo", {name: row.lent_to}) : "",
    ].filter(Boolean);
    return parts.length ? parts.join(" · ") : T("library.shelfEditor.noDetails");
  }

  function serviceOptions(row) {
    const current = row.service?.id ? String(row.service.id) : "";
    const list = Array.isArray(state.services) ? state.services : [];
    const known = list.some((item) => String(item.id) === current);
    const extra = current && !known ? `<option value="${esc(current)}" selected>${esc(row.service?.name || current)}</option>` : "";
    const loading = !Array.isArray(state.services) ? `<option value="" disabled>${TH(state.servicesLoading ? "library.shelfEditor.loadingServices" : "library.shelfEditor.servicesUnavailable")}</option>` : "";
    return `<option value="">${TH("library.shelfEditor.notSet")}</option>${extra}${loading}${list.map((item) => `<option value="${esc(item.id)}"${String(item.id) === current ? " selected" : ""}>${esc(item.name)}</option>`).join("")}`;
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
      const row = (state.entry?.copies || [])[state.editing] || {};
      const value = select.value;
      select.innerHTML = serviceOptions(value ? { service: { id: value, name: select.selectedOptions[0]?.textContent || value } } : row);
    }
  }

  function field(label, control, wide = false) {
    return `<label class="cw-shelf-field${wide ? " is-wide" : ""}"><span>${esc(label)}</span>${control}</label>`;
  }

  function foldGroup(key, label, values) {
    const summary = values.filter(Boolean).join(" · ");
    return `<details class="cw-shelf-group cw-shelf-fold" data-shelf-group="${esc(key)}"${state.groups[key] ? " open" : ""}><summary><span class="cw-shelf-fold-label">${esc(label)}</span>${summary ? `<span class="cw-shelf-fold-sum">${esc(summary)}</span>` : ""}${icon("expand_more")}</summary>`;
  }

  function detailsForm(row) {
    const digital = row.format === "digital";
    const tv = state.type === "tv";
    const currency = shelf().currency?.() || "";
    const currencies = [currency, ...CURRENCIES].filter((code, i, all) => all.indexOf(code) === i);
    const editionInput = field(T("library.shelfEditor.edition"), `<input name="edition" list="cw-shelf-editions" maxlength="80" value="${esc(row.edition || "")}" placeholder="${TH("library.shelfEditor.standard")}">`);
    const edition = digital
      ? field(T("library.shelfEditor.service"), `<select name="service_id">${serviceOptions(row)}</select>`) + editionInput
      : editionInput
        + field(T("library.shelfEditor.packagingLabel"), `<select name="packaging">${options(PACKAGING, row.packaging, T("library.shelfEditor.notSet"))}</select>`)
        + field(T("library.shelfEditor.region"), `<input name="region" maxlength="12" value="${esc(row.region || "")}" placeholder="B">`)
        + field(T("library.shelfEditor.discs"), `<input name="discs" type="number" min="1" max="99" value="${esc(row.discs ?? "")}" placeholder="1">`);
    const scope = tv ? field(T("library.shelfEditor.season"), `<input name="season" type="number" min="0" max="999" value="${esc(row.season ?? "")}" placeholder="${TH("library.shelfEditor.all")}">`) + field(T("library.shelfEditor.volume"), `<input name="volume" maxlength="40" value="${esc(row.volume || "")}" placeholder="${TH("library.shelfEditor.part1")}">`) : "";
    return `<form class="cw-shelf-form" data-shelf-form novalidate>
      <input type="hidden" name="format" value="${esc(row.format || "")}">
      <fieldset class="cw-shelf-group"><legend>${TH("library.shelfEditor.edition")}</legend><div class="cw-shelf-grid">${edition}${scope}</div></fieldset>
      <fieldset class="cw-shelf-group"><legend>${TH("library.shelfEditor.video")}</legend><div class="cw-shelf-grid">
        ${field(T("library.shelfEditor.resolution"), `<select name="resolution">${options(RESOLUTIONS, row.resolution, T("library.shelfEditor.notSet"))}</select>`)}
        ${field("HDR", `<select name="hdr">${options(HDRS, row.hdr, T("library.shelfEditor.none"))}</select>`)}
        ${field(T("library.shelfEditor.audio"), `<select name="audio">${options(AUDIO_CHOICES, row.audio, T("library.shelfEditor.notSet"))}</select>`)}
        ${field(T("library.shelfEditor.channels"), `<select name="audio_channels">${options(CHANNELS, row.audio_channels, T("library.shelfEditor.notSet"))}</select>`)}
        ${field("3D", `<select name="is_3d"><option value="">${TH("library.shelfEditor.no")}</option><option value="true"${row.is_3d ? " selected" : ""}>${TH("library.shelf.yes")}</option></select>`)}
      </div></fieldset>
      ${foldGroup("purchase", T("library.shelfEditor.purchase"), [shelf().date?.(row.purchased) || row.purchased, row.price != null && row.price !== "" ? shelf().money?.(row.price) || String(row.price) : "", row.store])}<div class="cw-shelf-grid">
        ${field(T("library.shelfEditor.date"), `<input name="purchased" type="date" value="${esc(row.purchased || "")}">`)}
        ${field(T("library.shelfEditor.price"), `<span class="cw-shelf-price"><input name="price" type="number" min="0" step="0.01" inputmode="decimal" value="${esc(row.price ?? "")}"><select data-shelf-currency aria-label="${TH("library.shelfEditor.currency")}">${currencies.map((code) => `<option value="${esc(code)}"${code === currency ? " selected" : ""}>${esc(code || "–")}</option>`).join("")}</select></span>`)}
        ${field(T("library.shelfEditor.store"), `<input name="store" maxlength="80" value="${esc(row.store || "")}" placeholder="Amazon">`)}
      </div></details>
      ${foldGroup("where", T("library.shelfEditor.where"), [row.shelf, CONDITIONS[row.condition] || "", row.lent_to ? T("library.shelfEditor.lentTo", {name: row.lent_to}) : ""])}<div class="cw-shelf-grid">
        ${field(T("library.shelfEditor.shelf"), `<input name="shelf" maxlength="60" value="${esc(row.shelf || "")}" placeholder="${TH("library.shelfEditor.livingRoom")}">`)}
        ${field(T("library.shelfEditor.conditionLabel"), `<select name="condition">${options(CONDITIONS, row.condition, T("library.shelfEditor.notSet"))}</select>`)}
        ${field(T("library.shelfEditor.lentToLabel"), `<input name="lent_to" maxlength="80" value="${esc(row.lent_to || "")}" placeholder="${TH("library.shelfEditor.nobody")}">`)}
      </div></details>
      ${field(T("library.shelfEditor.notes"), `<textarea name="notes" maxlength="500" rows="2">${esc(row.notes || "")}</textarea>`, true)}
      <datalist id="cw-shelf-editions">${EDITIONS.map((name) => `<option value="${esc(name)}"></option>`).join("")}</datalist>
      <p class="cw-mm-art-note is-error" data-shelf-form-error hidden></p>
      <div class="cw-shelf-form-actions">
        <button type="button" class="cw-shelf-link is-danger" data-shelf-form-delete>${icon("delete")}${TH("library.shelfEditor.removeCopyBtn")}</button>
        <button type="button" class="cw-mm-btn" data-shelf-cancel>${TH("library.shelfEditor.cancel")}</button>
        <button type="submit" class="cw-mm-btn is-primary">${TH("library.shelfEditor.save")}</button>
      </div>
    </form>`;
  }

  function copyHtml(row, index) {
    const open = state.editing === index;
    const logo = row.format === "digital" ? shelf().logoUrl?.(row.service?.logo) || "" : "";
    const disc = logo ? "" : shelf().discUrl?.(state.type, state.id, row.format) || "";
    const art = logo ? `<img class="is-logo" src="${esc(logo)}" alt="" loading="lazy" decoding="async" onerror="this.parentNode.classList.remove('has-art');this.remove()">` : disc ? `<img class="is-disc" src="${esc(disc)}" alt="" loading="lazy" decoding="async" onerror="this.parentNode.classList.remove('has-art');this.remove()">` : "";
    const tools = state.canEdit && !open ? `<div class="cw-shelf-copy-tools">
        <button type="button" class="cw-shelf-link" data-shelf-edit="${index}">${TH("library.shelfEditor.details")}</button>
        <button type="button" class="cw-shelf-icon${state.confirmDelete === index ? " is-confirming" : ""}" data-shelf-delete="${index}" aria-label="${TH(state.confirmDelete === index ? "library.shelfEditor.clickRemoveCopy" : "library.shelfEditor.removeCopy")}" title="${TH(state.confirmDelete === index ? "library.shelfEditor.clickRemoveCopy" : "library.shelfEditor.removeCopy")}">${icon(state.confirmDelete === index ? "delete_forever" : "delete")}</button>
      </div>` : "";
    return `<article class="cw-shelf-copy${open ? " is-open" : ""}" data-shelf-copy="${index}">
      <div class="cw-shelf-copy-row">
        <span class="cw-shelf-copy-mark${art ? " has-art" : ""}" data-format="${esc(row.format)}">${icon(row.format === "digital" ? "cloud" : "album")}${art}</span>
        <div class="cw-shelf-copy-text"><strong>${esc(formats()[row.format] || row.format)}</strong><span>${esc(copySummary(row))}</span></div>
        ${tools}
      </div>
      ${open ? detailsForm(state.draft || row) : ""}
    </article>`;
  }

  function quickHtml(hasCopies) {
    if (!state.canEdit) return "";
    const suggested = state.synced?.format || "";
    const more = Object.keys(formats()).filter((key) => !QUICK.includes(key));
    const buttons = QUICK.map((fmt) => `<button type="button" class="cw-shelf-quick-btn${fmt === suggested ? " is-suggested" : ""}" data-shelf-quick="${esc(fmt)}"${state.busy ? " disabled" : ""}>${icon(fmt === "digital" ? "cloud" : "add")}${esc(formats()[fmt] || fmt)}</button>`).join("");
    const select = more.length ? `<select class="cw-shelf-quick-more" data-shelf-quick-more aria-label="${TH("library.shelfEditor.otherFormats")}"${state.busy ? " disabled" : ""}><option value="">${TH("library.shelfEditor.more")}</option>${more.map((key) => `<option value="${esc(key)}">${esc(formats()[key] || key)}</option>`).join("")}</select>` : "";
    const hint = syncedText();
    return `<div class="cw-shelf-quick">
      <span class="cw-shelf-quick-label">${TH(hasCopies ? "library.shelfEditor.addAnother" : "library.shelfEditor.whatHave")}</span>
      <div class="cw-shelf-quick-row">${buttons}${select}</div>
      ${hint && !hasCopies ? `<small class="cw-shelf-quick-hint">Your providers: ${esc(hint)}</small>` : ""}
    </div>`;
  }

  function bodyHtml() {
    if (state.loading) return `<p class="cw-mm-art-note">${TH("library.shelfEditor.loading")}</p>`;
    if (state.error && !state.entry) return `<p class="cw-mm-art-note is-error">${esc(state.error)}</p>`;
    const copies = state.entry?.copies || [];
    const error = state.error ? `<p class="cw-mm-art-note is-error">${esc(state.error)}</p>` : "";
    const list = copies.length ? `<div class="cw-shelf-copies">${copies.map(copyHtml).join("")}</div>` : "";
    const empty = !copies.length && !state.canEdit ? `<p class="cw-mm-art-note">${TH("library.shelfEditor.noCopies")}</p>` : "";
    return `${error}${list}${empty}${state.editing === null ? quickHtml(copies.length > 0) : ""}`;
  }

  function render() {
    if (!state.node) return;
    const current = state.entry?.status || "";
    state.node.querySelectorAll("[data-shelf-status]").forEach((btn) => {
      const on = btn.dataset.shelfStatus === current;
      btn.classList.toggle("is-active", on);
      btn.setAttribute("aria-checked", on ? "true" : "false");
      btn.disabled = state.busy || state.loading || !state.canEdit || state.editing !== null;
    });
    const foot = state.node.querySelector(".cw-mm-art-foot");
    if (foot) foot.hidden = state.editing !== null;
    const remove = state.node.querySelector("[data-shelf-remove]");
    if (remove) {
      remove.hidden = !state.canEdit || !state.entry || state.editing !== null;
      remove.textContent = state.confirmRemove ? T("library.shelfEditor.clickRemove") : T("library.shelfEditor.removeFromShelf");
      remove.classList.toggle("is-confirming", state.confirmRemove);
      remove.disabled = state.busy;
    }
    state.node.querySelector(".cw-mm-art-body").innerHTML = bodyHtml();
    const form = state.node.querySelector("[data-shelf-form]");
    if (form) {
      form.querySelector("input:not([type=hidden]),select")?.focus({ preventScroll: true });
      if (form.querySelector("select[name=service_id]")) void loadServices();
    }
  }

  function applyStart() {
    if (Number.isInteger(state.start) && state.canEdit && (state.entry?.copies || [])[state.start]) state.editing = state.start;
    state.start = null;
  }

  async function load() {
    const seq = ++state.seq;
    try {
      const response = await fetch(endpoint(), { cache: "no-store", credentials: "same-origin" });
      const data = await response.json().catch(() => ({}));
      if (!response.ok || !data?.ok) throw new Error(data?.error || T("library.shelfEditor.loadFailed"));
      if (seq !== state.seq) return;
      state.entry = data.entry || null;
      state.synced = data.synced && typeof data.synced === "object" ? data.synced : state.fallbackSynced;
      state.canEdit = data.can_edit === true && state.canEdit;
    } catch (error) {
      if (seq !== state.seq) return;
      state.error = error?.message || T("library.shelfEditor.loadFailed");
    }
    state.loading = false;
    applyStart();
    render();
  }

  async function save(next) {
    if (state.busy) return false;
    state.confirmRemove = false;
    state.busy = true;
    state.error = "";
    render();
    try {
      const body = { status: next.status, title: state.title, year: state.year || undefined, ids: state.ids, copies: next.copies };
      const response = await fetch(endpoint(), { method: "PUT", headers: { "Content-Type": "application/json" }, cache: "no-store", credentials: "same-origin", body: JSON.stringify(body) });
      const data = await response.json().catch(() => ({}));
      if (!response.ok || !data?.ok) throw new Error(data?.error || T("library.shelfEditor.saveFailed"));
      state.entry = data.entry;
      shelf().set?.(state.type, state.id, data.summary || null);
      try { state.onSaved?.(data); } catch {}
      return true;
    } catch (error) {
      state.error = error?.message || T("library.shelfEditor.saveFailed");
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
      if (!response.ok || !data?.ok) throw new Error(data?.error || T("library.shelfEditor.removeFailed"));
      state.entry = null;
      shelf().set?.(state.type, state.id, null);
    } catch (error) {
      state.error = error?.message || T("library.shelfEditor.removeFailed");
    } finally {
      state.busy = false;
      render();
    }
  }

  function deleteCopy(index) {
    const copies = [...(state.entry?.copies || [])];
    copies.splice(index, 1);
    state.editing = null;
    state.draft = null;
    const status = state.entry?.status || "owned";
    if (!copies.length && status === "owned") { void removeTitle(); return; }
    void save({ status, copies });
  }

  function addCopy(format) {
    if (!format || state.busy) return;
    const copies = [...(state.entry?.copies || []), { format }];
    const status = !state.entry || state.entry.status === "wishlist" ? "owned" : state.entry.status;
    void save({ status, copies });
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
    if (row.season) row.season = Number(row.season);
    if (row.is_3d) row.is_3d = true;
    const serviceId = row.service_id;
    delete row.service_id;
    if (row.format === "digital" && serviceId) {
      const found = (state.services || []).find((item) => String(item.id) === String(serviceId));
      const previous = (state.entry?.copies || [])[state.editing]?.service;
      const service = found || (previous && String(previous.id) === String(serviceId) ? previous : null);
      if (service) row.service = { id: Number(service.id), name: service.name, ...(service.logo ? { logo: service.logo } : {}) };
    }
    return row;
  }

  async function submitForm(form) {
    const row = readForm(form);
    const index = state.editing;
    const copies = [...(state.entry?.copies || [])];
    const before = copies[index] || {};
    copies[index] = { ...row, format: row.format || before.format, id: before.id, added_at: before.added_at, ...(before.source ? { source: before.source } : {}), ...(before.ref ? { ref: before.ref } : {}) };
    state.editing = null;
    if (await save({ status: state.entry?.status || "owned", copies })) {
      state.draft = null;
    } else {
      state.editing = index;
      state.draft = row;
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
    const quick = target?.closest?.("[data-shelf-quick]");
    if (quick) { addCopy(quick.dataset.shelfQuick); return; }
    if (target?.closest?.("[data-shelf-cancel]")) { state.editing = null; state.draft = null; render(); return; }
    const formDelete = target?.closest?.("[data-shelf-form-delete]");
    if (formDelete && !state.busy) {
      if (!formDelete.classList.contains("is-confirming")) {
        formDelete.classList.add("is-confirming");
        formDelete.lastChild.textContent = T("library.shelfEditor.clickRemove");
        return;
      }
      deleteCopy(state.editing);
      return;
    }
    if (target?.closest?.("[data-shelf-remove]")) {
      if (!state.confirmRemove) {
        state.confirmRemove = true;
        render();
        return;
      }
      state.confirmRemove = false;
      void removeTitle();
      return;
    }
    const del = target?.closest?.("[data-shelf-delete]");
    if (del && !state.busy) {
      const index = Number(del.dataset.shelfDelete);
      if (state.confirmDelete !== index) {
        state.confirmDelete = index;
        state.confirmRemove = false;
        render();
        return;
      }
      state.confirmDelete = null;
      deleteCopy(index);
      return;
    }
    if (state.confirmRemove || state.confirmDelete !== null) {
      state.confirmRemove = false;
      state.confirmDelete = null;
      render();
    }
    const edit = target?.closest?.("[data-shelf-edit]");
    if (edit) { state.editing = Number(edit.dataset.shelfEdit); state.draft = null; state.error = ""; render(); }
  }

  async function saveCurrency(select) {
    const value = select.value;
    try {
      const response = await fetch("/api/profile/shelf/settings", { method: "PUT", headers: { "Content-Type": "application/json" }, cache: "no-store", credentials: "same-origin", body: JSON.stringify({ currency: value }) });
      const data = await response.json().catch(() => ({}));
      if (!response.ok || !data?.ok) throw new Error(data?.error || T("library.shelfEditor.currencyFailed"));
      shelf().setCurrency?.(data.currency || "");
    } catch (error) {
      select.value = shelf().currency?.() || "";
      const note = state.node?.querySelector("[data-shelf-form-error]");
      if (note) {
        note.textContent = error?.message || T("library.shelfEditor.currencyFailed");
        note.hidden = false;
      }
    }
  }

  function onChange(event) {
    const currency = event.target?.closest?.("[data-shelf-currency]");
    if (currency && state.canEdit) { void saveCurrency(currency); return; }
    const more = event.target?.closest?.("[data-shelf-quick-more]");
    if (more && more.value) addCopy(more.value);
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
      state.draft = null;
      render();
      return;
    }
    close();
  }

  function open({ type, id, title = "", year = "", ids = {}, canEdit = false, host = document.body, start = null, synced = null, onSaved = null } = {}) {
    if (!type || !id) return false;
    close();
    const fallbackSynced = synced && (synced.format || synced.media_info) ? { format: synced.format || "", media_info: synced.media_info || {}, providers: [] } : null;
    Object.assign(state, {
      type: type === "movie" ? "movie" : "tv", id: String(id), title: String(title || ""), year: year || "", ids: ids && typeof ids === "object" ? ids : {},
      entry: null, synced: null, fallbackSynced,
      draft: null, groups: {}, confirmRemove: false, confirmDelete: null, canEdit: !!canEdit, loading: true, error: "", editing: null, busy: false, lastFocus: document.activeElement,
      start, onSaved: typeof onSaved === "function" ? onSaved : null,
    });
    void shelf().load?.();
    const node = document.createElement("div");
    node.className = `cw-mm-art cw-shelf${host === document.body ? " is-floating" : ""}`;
    const statusBar = `<div class="cw-shelf-status" role="radiogroup" aria-label="${TH("library.shelfEditor.status")}">${Object.entries(statuses()).map(([key, label]) => `<button type="button" role="radio" aria-checked="false" data-shelf-status="${esc(key)}">${esc(label)}</button>`).join("")}</div>`;
    node.innerHTML = `<section class="cw-mm-art-panel cw-shelf-dialog" role="dialog" aria-modal="true" aria-labelledby="cw-shelf-title" tabindex="-1">
        <header class="cw-mm-art-head">
          <div><h3 id="cw-shelf-title">${TH("library.shelfEditor.title")}</h3><p>${esc(state.title)}</p></div>
          ${statusBar}
          <button type="button" class="cw-mm-art-close" data-shelf-close aria-label="${TH("library.shelfEditor.close")}">${icon("close")}</button>
        </header>
        <div class="cw-mm-art-body"></div>
        <footer class="cw-mm-art-foot">
          <button type="button" class="cw-shelf-link is-danger" data-shelf-remove hidden>${TH("library.shelfEditor.removeFromShelf")}</button>
          <button type="button" class="cw-mm-btn is-primary" data-shelf-close>${TH("library.shelfEditor.done")}</button>
        </footer>
      </section>`;
    node.addEventListener("click", onClick);
    node.addEventListener("submit", onSubmit);
    node.addEventListener("change", onChange);
    node.addEventListener("keydown", onKey);
    node.addEventListener("toggle", (event) => {
      const key = event.target?.dataset?.shelfGroup;
      if (key) state.groups[key] = event.target.open;
    }, true);
    host.appendChild(node);
    state.node = node;
    window.CW?.DialogFocus?.activate(node, close);
    render();
    node.querySelector(".cw-mm-art-panel")?.focus({ preventScroll: true });
    void load();
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

  (window.CW ||= {}).ShelfEditor = { open, close, isOpen: () => !!state.node };
})();
