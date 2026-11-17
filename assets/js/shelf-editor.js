/* assets/js/shelf-editor.js */
/* CrossWatch - shelf status and copies editor for a single title */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(function () {
  const CURRENCIES = ["EUR", "USD", "GBP", "CAD", "AUD", "NZD", "CHF", "SEK", "NOK", "DKK", "PLN", "CZK", "HUF", "JPY", "BRL", "MXN", "INR", "ZAR"];
  const QUICK = ["4k_bluray", "bluray", "dvd", "digital"];
  const PACKAGING = { keep_case: "Keep case", steelbook: "Steelbook", digipak: "Digipak", mediabook: "Mediabook", slipcover: "Slipcover", box_set: "Box set", other: "Other" };
  const CONDITIONS = { mint: "Mint", like_new: "Like new", good: "Good", fair: "Fair", poor: "Poor" };
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
  const statuses = () => shelf().statuses || { owned: "Owned", ordered: "Ordered", wishlist: "Wishlist" };
  const state = { node: null, type: "", id: "", title: "", year: "", ids: {}, entry: null, synced: null, services: null, servicesLoading: false, draft: null, confirmRemove: false, confirmDelete: null, canEdit: false, loading: true, error: "", editing: null, busy: false, lastFocus: null, seq: 0, tracker: null, start: null, onSaved: null };

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
      row.shelf ? `Shelf ${row.shelf}` : "",
      row.purchased ? dateLabel(row.purchased) : "",
      row.price != null ? shelf().money?.(row.price) || "" : "",
      row.lent_to ? `Lent to ${row.lent_to}` : "",
    ].filter(Boolean);
    return parts.length ? parts.join(" · ") : "No details yet";
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
      const row = (state.entry?.copies || [])[state.editing] || {};
      const value = select.value;
      select.innerHTML = serviceOptions(value ? { service: { id: value, name: select.selectedOptions[0]?.textContent || value } } : row);
    }
  }

  function field(label, control, wide = false) {
    return `<label class="cw-shelf-field${wide ? " is-wide" : ""}"><span>${esc(label)}</span>${control}</label>`;
  }

  function detailsForm(row) {
    const digital = row.format === "digital";
    const tv = state.type === "tv";
    const currency = shelf().currency?.() || "";
    const currencies = [currency, ...CURRENCIES].filter((code, i, all) => all.indexOf(code) === i);
    const editionInput = field("Edition", `<input name="edition" list="cw-shelf-editions" maxlength="80" value="${esc(row.edition || "")}" placeholder="Standard">`);
    const edition = digital
      ? field("Service", `<select name="service_id">${serviceOptions(row)}</select>`) + editionInput
      : editionInput
        + field("Packaging", `<select name="packaging">${options(PACKAGING, row.packaging, "Not set")}</select>`)
        + field("Region", `<input name="region" maxlength="12" value="${esc(row.region || "")}" placeholder="B">`)
        + field("Discs", `<input name="discs" type="number" min="1" max="99" value="${esc(row.discs ?? "")}" placeholder="1">`);
    const scope = tv ? field("Season", `<input name="season" type="number" min="0" max="999" value="${esc(row.season ?? "")}" placeholder="All">`) + field("Volume", `<input name="volume" maxlength="40" value="${esc(row.volume || "")}" placeholder="Part 1">`) : "";
    return `<form class="cw-shelf-form" data-shelf-form novalidate>
      <input type="hidden" name="format" value="${esc(row.format || "")}">
      <fieldset class="cw-shelf-group"><legend>Edition</legend><div class="cw-shelf-grid">${edition}${scope}</div></fieldset>
      <fieldset class="cw-shelf-group"><legend>Video and audio</legend><div class="cw-shelf-grid">
        ${field("Resolution", `<select name="resolution">${options(RESOLUTIONS, row.resolution, "Not set")}</select>`)}
        ${field("HDR", `<select name="hdr">${options(HDRS, row.hdr, "None")}</select>`)}
        ${field("Audio", `<select name="audio">${options(AUDIO_CHOICES, row.audio, "Not set")}</select>`)}
        ${field("Channels", `<select name="audio_channels">${options(CHANNELS, row.audio_channels, "Not set")}</select>`)}
        ${field("3D", `<select name="is_3d"><option value="">No</option><option value="true"${row.is_3d ? " selected" : ""}>Yes</option></select>`)}
      </div></fieldset>
      <fieldset class="cw-shelf-group"><legend>Purchase</legend><div class="cw-shelf-grid">
        ${field("Date", `<input name="purchased" type="date" value="${esc(row.purchased || "")}">`)}
        ${field("Price", `<span class="cw-shelf-price"><input name="price" type="number" min="0" step="0.01" inputmode="decimal" value="${esc(row.price ?? "")}"><select data-shelf-currency aria-label="Currency for all prices">${currencies.map((code) => `<option value="${esc(code)}"${code === currency ? " selected" : ""}>${esc(code || "–")}</option>`).join("")}</select></span>`)}
        ${field("Store", `<input name="store" maxlength="80" value="${esc(row.store || "")}" placeholder="Amazon">`)}
      </div></fieldset>
      <fieldset class="cw-shelf-group"><legend>Where</legend><div class="cw-shelf-grid">
        ${field("Shelf", `<input name="shelf" maxlength="60" value="${esc(row.shelf || "")}" placeholder="Living room A">`)}
        ${field("Condition", `<select name="condition">${options(CONDITIONS, row.condition, "Not set")}</select>`)}
        ${field("Lent to", `<input name="lent_to" maxlength="80" value="${esc(row.lent_to || "")}" placeholder="Nobody">`)}
      </div></fieldset>
      ${field("Notes", `<textarea name="notes" maxlength="500" rows="2">${esc(row.notes || "")}</textarea>`, true)}
      <datalist id="cw-shelf-editions">${EDITIONS.map((name) => `<option value="${esc(name)}"></option>`).join("")}</datalist>
      <p class="cw-mm-art-note is-error" data-shelf-form-error hidden></p>
      <div class="cw-shelf-form-actions">
        <button type="button" class="cw-shelf-link is-danger" data-shelf-form-delete>${icon("delete")}Remove copy</button>
        <button type="button" class="cw-mm-btn" data-shelf-cancel>Cancel</button>
        <button type="submit" class="cw-mm-btn is-primary">Save</button>
      </div>
    </form>`;
  }

  function copyHtml(row, index) {
    const open = state.editing === index;
    const logo = row.format === "digital" ? shelf().logoUrl?.(row.service?.logo) || "" : "";
    const disc = logo ? "" : shelf().discUrl?.(state.type, state.id, row.format) || "";
    const art = logo ? `<img class="is-logo" src="${esc(logo)}" alt="" loading="lazy" decoding="async" onerror="this.parentNode.classList.remove('has-art');this.remove()">` : disc ? `<img class="is-disc" src="${esc(disc)}" alt="" loading="lazy" decoding="async" onerror="this.parentNode.classList.remove('has-art');this.remove()">` : "";
    const tools = state.canEdit && !open ? `<div class="cw-shelf-copy-tools">
        <button type="button" class="cw-shelf-link" data-shelf-edit="${index}">Details</button>
        <button type="button" class="cw-shelf-icon${state.confirmDelete === index ? " is-confirming" : ""}" data-shelf-delete="${index}" aria-label="${state.confirmDelete === index ? "Click again to remove this copy" : "Remove this copy"}" title="${state.confirmDelete === index ? "Click again to remove this copy" : "Remove this copy"}">${icon(state.confirmDelete === index ? "delete_forever" : "delete")}</button>
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
    const select = more.length ? `<select class="cw-shelf-quick-more" data-shelf-quick-more aria-label="Other formats"${state.busy ? " disabled" : ""}><option value="">More…</option>${more.map((key) => `<option value="${esc(key)}">${esc(formats()[key] || key)}</option>`).join("")}</select>` : "";
    const hint = syncedText();
    return `<div class="cw-shelf-quick">
      <span class="cw-shelf-quick-label">${hasCopies ? "Add another copy" : "What do you have?"}</span>
      <div class="cw-shelf-quick-row">${buttons}${select}</div>
      ${hint && !hasCopies ? `<small class="cw-shelf-quick-hint">${state.tracker ? "Tracker" : "Your providers"}: ${esc(hint)}</small>` : ""}
    </div>`;
  }

  function bodyHtml() {
    if (state.loading) return `<p class="cw-mm-art-note">Loading...</p>`;
    if (state.error && !state.entry && !state.tracker) return `<p class="cw-mm-art-note is-error">${esc(state.error)}</p>`;
    const copies = state.entry?.copies || [];
    const error = state.error ? `<p class="cw-mm-art-note is-error">${esc(state.error)}</p>` : "";
    const list = copies.length ? `<div class="cw-shelf-copies">${copies.map(copyHtml).join("")}</div>` : "";
    const empty = !copies.length && !state.canEdit ? `<p class="cw-mm-art-note">No copies yet.</p>` : "";
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
      remove.hidden = !!state.tracker || !state.canEdit || !state.entry || state.editing !== null;
      remove.textContent = state.confirmRemove ? "Click again to remove" : "Remove from shelf";
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
    if (state.tracker) {
      state.loading = false;
      applyStart();
      render();
      return;
    }
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
      if (state.tracker) {
        const body = { feature: "collection", provider_instance: state.tracker.instance, key: state.tracker.key, changes: { copies: next.copies } };
        const response = await fetch("/api/tracker/edit", { method: "POST", headers: { "Content-Type": "application/json" }, cache: "no-store", credentials: "same-origin", body: JSON.stringify(body) });
        const data = await response.json().catch(() => ({}));
        if (!response.ok || !data?.ok) throw new Error(data?.error === "no_changes" ? "Nothing changed." : data?.error === "invalid_copies" ? "Check the copy details." : "Saving failed.");
        if (data.key) state.tracker.key = data.key;
        state.entry = { status: "owned", copies: Array.isArray(data.item?.copies) ? data.item.copies : [] };
        try { state.onSaved?.(data); } catch {}
        return true;
      }
      const body = { status: next.status, title: state.title, year: state.year || undefined, ids: state.ids, copies: next.copies };
      const response = await fetch(endpoint(), { method: "PUT", headers: { "Content-Type": "application/json" }, cache: "no-store", credentials: "same-origin", body: JSON.stringify(body) });
      const data = await response.json().catch(() => ({}));
      if (!response.ok || !data?.ok) throw new Error(data?.error || "Saving failed.");
      state.entry = data.entry;
      shelf().set?.(state.type, state.id, data.summary || null);
      try { state.onSaved?.(data); } catch {}
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
        formDelete.lastChild.textContent = "Click again to remove";
        return;
      }
      const copies = [...(state.entry?.copies || [])];
      copies.splice(state.editing, 1);
      state.editing = null;
      state.draft = null;
      void save({ status: state.entry?.status || "owned", copies });
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
      const copies = [...(state.entry?.copies || [])];
      copies.splice(index, 1);
      void save({ status: state.entry?.status || "owned", copies });
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

  function open({ type, id, title = "", year = "", ids = {}, canEdit = false, host = document.body, start = null, tracker = null, onSaved = null } = {}) {
    if (!type || !id) return false;
    close();
    const trackerMode = tracker && tracker.key ? { instance: String(tracker.instance || "default"), key: String(tracker.key) } : null;
    const trackerItem = trackerMode ? tracker.item || {} : null;
    Object.assign(state, {
      type: type === "movie" ? "movie" : "tv", id: String(id), title: String(title || ""), year: year || "", ids: ids && typeof ids === "object" ? ids : {},
      entry: trackerMode ? { status: "owned", copies: Array.isArray(trackerItem.copies) ? trackerItem.copies : [] } : null,
      synced: trackerMode && (trackerItem.format || trackerItem.media_info) ? { format: trackerItem.format || "", media_info: trackerItem.media_info || {}, providers: [] } : null,
      draft: null, confirmRemove: false, confirmDelete: null, canEdit: !!canEdit, loading: true, error: "", editing: null, busy: false, lastFocus: document.activeElement,
      tracker: trackerMode, start, onSaved: typeof onSaved === "function" ? onSaved : null,
    });
    void shelf().load?.();
    const node = document.createElement("div");
    node.className = `cw-mm-art cw-shelf${host === document.body ? " is-floating" : ""}`;
    const statusBar = trackerMode ? "" : `<div class="cw-shelf-status" role="radiogroup" aria-label="Status">${Object.entries(statuses()).map(([key, label]) => `<button type="button" role="radio" aria-checked="false" data-shelf-status="${esc(key)}">${esc(label)}</button>`).join("")}</div>`;
    node.innerHTML = `<section class="cw-mm-art-panel cw-shelf-dialog" role="dialog" aria-modal="true" aria-labelledby="cw-shelf-title" tabindex="-1">
        <header class="cw-mm-art-head">
          <div><h3 id="cw-shelf-title">${trackerMode ? "Copies in this tracker" : "My shelf"}</h3><p>${esc(state.title)}</p></div>
          ${statusBar}
          <button type="button" class="cw-mm-art-close" data-shelf-close aria-label="Close">${icon("close")}</button>
        </header>
        <div class="cw-mm-art-body"></div>
        <footer class="cw-mm-art-foot">
          <button type="button" class="cw-shelf-link is-danger" data-shelf-remove hidden>Remove from shelf</button>
          <button type="button" class="cw-mm-btn is-primary" data-shelf-close>Done</button>
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
