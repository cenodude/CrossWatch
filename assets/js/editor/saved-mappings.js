/* assets/js/editor/saved-mappings.js */
/* CrossWatch - saved mapping comparisons in the Editor */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
const esc = value => String(value ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const icon = name => `<span class="material-symbols-rounded" aria-hidden="true">${name}</span>`;
let dialog;

export async function navigateToMapping(mapping, ctx) {
  const state = ctx.state;
  if (state.loading || state.saving) throw new Error("Wait for the Editor to finish loading or saving, then try again.");
  if (state.hasChanges && !window.confirm("Discard your unsaved Editor changes and open this mapping?")) return false;
  const previous = {...state, typeFilter:{...state.typeFilter}};
  try {
    state.source = "state";
    state.kind = mapping.feature;
    state.snapshot = mapping.provider;
    state.instance = mapping.instance;
    state.mappingPair = mapping.pair_id || "";
    state.page = 0;
    state.blockedOnly = false;
    for (const type of Object.keys(state.typeFilter)) state.typeFilter[type] = true;
    ctx.clearFilter();
    ctx.syncSourceUI();
    await ctx.loadSnapshots();
    if (state.snapshot !== mapping.provider || state.instance !== mapping.instance) {
      throw new Error("This mapping’s provider or profile is no longer available in the Editor.");
    }
    await ctx.loadState();
    if (state.loadError) throw new Error("Could not open this mapping. Refresh and try again.");
    const row = state.rows.find(row => row.key === mapping.key && row._origin !== "baseline");
    if (!row) throw new Error("This mapping is no longer saved. Refresh the list to see the current mappings.");
    const index = ctx.sortRows(ctx.applyFilter ? ctx.applyFilter(state.rows) : state.rows).indexOf(row);
    state.page = Math.floor(index / state.pageSize);
    ctx.syncTypeFilterUI();
    ctx.persistUIState();
    ctx.renderRows();
    return () => ctx.focusMapping(row);
  } catch (error) {
    Object.assign(state, previous);
    ctx.restoreUI();
    throw error;
  }
}

function itemDetails(item, key) {
  if (!item) return '<span class="sm-muted">Original details were not recorded.</span>';
  const title = item.series_title || item.show_title || item.title || item.name || "Untitled item";
  const coordinates = item.season != null ? `S${String(item.season).padStart(2,"0")}${item.episode != null ? `E${String(item.episode).padStart(2,"0")}` : ""}` : "";
  const ids = {...item.ids, ...item.show_ids};
  return `<strong>${esc(title)}</strong><span class="sm-muted">${esc([item.year, coordinates, item.type].filter(Boolean).join(" · "))}</span><div class="sm-ids">${Object.entries(ids).filter(([,value])=>value).map(([name,value])=>`<span>${esc(name.toUpperCase())}: ${esc(value)}</span>`).join("")}</div>${key ? `<small class="sm-key">${esc(key)}</small>` : ""}`;
}

function blockedRows(rows) {
  return `<table><thead><tr><th>Blocked item</th><th>Applies to</th><th>Action</th></tr></thead><tbody>${rows.map((row,index) => `<tr><td><span class="sm-block-label">${icon("block")}Blocked</span>${row.corrected ? itemDetails(row.corrected,row.key) : `<strong>${esc(row.key)}</strong><small>No saved item details.</small>`}</td><td><strong>${esc(row.scope_label || "All pairs")}</strong><span>${esc(row.provider)}</span><span>${esc(row.instance === "default" ? "Default" : row.instance)} · ${esc(row.feature)}</span></td><td><button type="button" data-unblock="${index}">${icon("lock_open")}Unblock</button></td></tr>`).join("")}</tbody></table>`;
}

export function open(trigger, editor) {
  if (dialog?.open) { dialog.focus(); return; }
  const current = editor?.state;
  const scoped = current && ["state", "manual"].includes(current.source);
  const pairId = scoped ? String(current.mappingPair || "shared") : "";
  const sourceValue = scoped && current.snapshot ? JSON.stringify([current.snapshot, current.instance || "default"]) : "";
  const scopeLabel = scoped ? (document.getElementById("cw-mapping-scope")?.selectedOptions[0]?.textContent
    || (current.mappingPair ? `Pair ${current.mappingPair}` : "All pairs using this provider instance")) : "";
  if (!document.getElementById("saved-mappings-style")) {
    const style = document.createElement("link");
    style.id = "saved-mappings-style";
    style.rel = "stylesheet";
    style.href = `/assets/css/saved-mappings.css?v=${encodeURIComponent(window.APP_VERSION || "1")}`;
    document.head.append(style);
  }
  dialog = document.createElement("dialog");
  const root = dialog;
  root.className = "sm-dialog";
  root.setAttribute("aria-labelledby", "sm-title");
  root.innerHTML = `<div class="sm-head"><div><h2 id="sm-title">Mappings & blocks</h2><p>Manage saved corrections and blocked items.</p></div><button type="button" data-close aria-label="Close mappings and blocks">${icon("close")}</button></div>
    <div class="sm-tabs" role="group" aria-label="Saved rules"><button type="button" data-view="mapping" aria-pressed="true">${icon("link")}Mappings</button><button type="button" data-view="block" aria-pressed="false">${icon("block")}Blocked items</button></div>
    <div class="sm-filters"><label>Search<input type="search" placeholder="Title, ID, provider or episode…" data-search></label><label>Source and profile<select data-source aria-label="Source and profile"><option value="">All sources</option></select></label><label>Feature<select data-feature aria-label="Feature"><option value="">All features</option>${["watchlist","history","ratings","progress","collection"].map(f=>`<option value="${f}">${f[0].toUpperCase()+f.slice(1)}</option>`).join("")}</select></label><button type="button" data-refresh>${icon("refresh")}Refresh</button></div>
    <p class="sm-note">Pair corrections override shared mappings for that pair. All pairs means every sync using that source instance. Use the pencil to edit a mapping in the Editor, then save your changes.</p>
    <form class="sm-add-block" hidden><label>Item key<input data-block-key required maxlength="1024" placeholder="For example: tmdb:123 or tmdb:123#s01e02"></label><button type="submit">${icon("block")}Block item</button><small>Select a source, profile and feature above. This rule applies to the displayed scope.</small></form>
    <p class="sm-edit-error" role="alert"></p><p class="sm-action-status" role="status"></p><div class="sm-results" aria-live="polite"></div><footer><span data-count></span><div><button type="button" data-prev>Previous</button><button type="button" data-next>Next</button></div></footer>`;
  document.body.append(root);
  const $ = selector => root.querySelector(selector);
  if (scoped) {
    $(".sm-head p").textContent = `Mapping scope: ${scopeLabel}`;
    $("[data-feature]").value = current.kind;
    if (sourceValue) {
      const option = document.createElement("option");
      option.value = sourceValue;
      option.textContent = `${current.snapshot} · ${current.instance === "default" || !current.instance ? "Default" : current.instance}`;
      $("[data-source]").append(option);
      $("[data-source]").value = sourceValue;
    }
  }
  let offset = 0, total = 0, controller, timer, editing = false, afterClose;
  let entryType = "mapping";
  const mappingNote = $(".sm-note").textContent;
  let currentRows = [];
  const scope = () => String(window.CW?.OverviewProfile?.id || "");

  async function load() {
    controller?.abort();
    const request = controller = new AbortController();
    $(".sm-results").innerHTML = '<p class="sm-empty" role="status">Loading saved mappings…</p>';
    $("[data-prev]").disabled = $("[data-next]").disabled = true;
    $("[data-count]").textContent = "";
    await window.CW?.OverviewProfile?.ready;
    if (request.signal.aborted || !root.open) return;
    const params = new URLSearchParams({q:$("[data-search]").value,feature:$("[data-feature]").value,offset:String(offset),limit:"50",user_profile:scope()});
    params.set("entry_type", entryType);
    if (pairId) params.set("pair_id", pairId);
    if ($("[data-source]").value) {
      const [provider,instance] = JSON.parse($("[data-source]").value);
      params.set("provider",provider); params.set("instance",instance);
    }
    try {
      const response = await fetch(`/api/editor/mappings?${params}`, {credentials:"same-origin",cache:"no-store",signal:request.signal});
      if (response.status === 404) throw new Error("Saved mappings is not available on this server. Update and restart CrossWatch, then reload this page.");
      if (response.status === 401) throw new Error("Your session has expired. Sign in again to view saved mappings.");
      if (response.status === 403) throw new Error("Your account does not have permission to view saved mappings.");
      if (!response.ok) throw new Error(`Could not load saved mappings (HTTP ${response.status}). Try refreshing; if it continues, check the server log.`);
      const data = await response.json();
      if (request.signal.aborted || !root.open) return;
      const source = $("[data-source]"), selected = source.value;
      const sources = [...data.sources];
      if (selected) {
        const [provider, instance] = JSON.parse(selected);
        if (!sources.some(s => s.provider === provider && s.instance === instance)) sources.unshift({provider, instance});
      }
      const options = '<option value="">All sources</option>' + sources.map(s=>`<option value="${esc(JSON.stringify([s.provider,s.instance]))}">${esc(`${s.provider} · ${s.instance === "default" ? "Default" : s.instance}`)}</option>`).join("");
      if (source.innerHTML !== options) { source.innerHTML = options; source.value = selected; }
      total = data.total;
      currentRows = data.items;
      $(".sm-results").innerHTML = entryType === "block" ? (data.items.length ? blockedRows(data.items) : '<div class="sm-empty">No blocked items match this scope.</div>') : data.items.length ? `<table><thead><tr><th>Original</th><th>Saved correction</th><th>Applies to</th></tr></thead><tbody>${data.items.map((row,index)=>`<tr><td>${itemDetails(row.original,row.original_key)}</td><td>${itemDetails(row.corrected,row.key)}</td><td><strong>${esc(row.scope_label || "All pairs")}</strong><span>${esc(row.provider)}</span><span>${esc(row.instance === "default" ? "Default" : row.instance)} · ${esc(row.feature)}</span><small>${row.origin === "interactive_sync" ? "Saved in Interactive Sync" : row.origin === "editor" ? "Saved in Editor" : row.origin === "analyzer" ? "Saved in Analyzer" : "Saved override"}</small>${row.saved_at ? `<small>${esc(new Date(row.saved_at*1000).toLocaleString())}</small>` : ""}${row.excluded ? '<span class="sm-muted">Also excluded by a block rule</span>' : ""}${editor ? `<button type="button" class="sm-edit" data-edit-mapping="${index}" title="Edit mapping" aria-label="Edit mapping: ${esc(row.corrected.title || row.corrected.series_title || row.key)}">${icon("edit")}</button>` : ""}</td></tr>`).join("")}</tbody></table>` : '<div class="sm-empty">No saved mappings match this view.<p>Save a correction in Interactive Sync or the Editor to see it here.</p></div>';
      const noun = entryType === "block" ? "blocked items" : "saved corrections";
      $("[data-count]").textContent = total ? `${offset+1}–${Math.min(offset+50,total)} of ${total} ${noun}` : `0 ${noun}`;
      $("[data-prev]").disabled = offset === 0;
      $("[data-next]").disabled = offset+50 >= total;
      window.CW?.IconSelect?.enhancePlain(root);
    } catch(error) {
      if (!request.signal.aborted) $(".sm-results").innerHTML = `<p class="sm-empty" role="alert">${esc(error.message)}</p>`;
    }
  }
  const resetScope = () => { if (scoped) root.close(); else { offset=0; $("[data-source]").value=""; load(); } };
  const closeOnNavigation = () => root.close();
  window.addEventListener("cw:overview-profile-changed", resetScope);
  window.addEventListener("auth-changed", closeOnNavigation);
  window.addEventListener("cw:auth-state-changed", closeOnNavigation);
  document.addEventListener("tab-changed", closeOnNavigation);
  root.addEventListener("close", () => {
    controller?.abort(); clearTimeout(timer);
    window.removeEventListener("cw:overview-profile-changed", resetScope);
    window.removeEventListener("auth-changed", closeOnNavigation);
    window.removeEventListener("cw:auth-state-changed", closeOnNavigation);
    document.removeEventListener("tab-changed", closeOnNavigation);
    root.remove();
    if (afterClose) afterClose();
    else trigger?.focus();
  }, {once:true});
  $("[data-close]").onclick = () => root.close();
  root.addEventListener("cancel", event => { if (editing) event.preventDefault(); });
  async function changeBlock(rule, blocked) {
    if (editing) return;
    $(".sm-edit-error").textContent = "";
    $(".sm-action-status").textContent = "";
    if (editor?.state?.hasChanges || editor?.state?.saving || editor?.state?.loading || editor?.state?.mappingEditing) {
      $(".sm-edit-error").textContent = "Save or discard your Editor changes before changing block rules.";
      return;
    }
    editing = true;
    const activeProfile = scope();
    const controls = [...root.querySelectorAll("button,input,select")];
    const disabled = controls.map(control => control.disabled);
    controls.forEach(control => { control.disabled = true; });
    try {
      const response = await fetch("/api/editor/mapping-block", {method:"POST", credentials:"same-origin",
        headers:{"Content-Type":"application/json"}, body:JSON.stringify({...rule, blocked})});
      const data = await response.json();
      if (!response.ok) throw new Error(typeof data.detail === "string" ? data.detail : "Could not update this block rule.");
      if (!root.open || scope() !== activeProfile) return;
      if (editor && !editor.state.hasChanges) await editor.loadState();
      $("[data-block-key]").value = "";
      offset = 0;
      $(".sm-action-status").textContent = blocked ? "Item blocked for future syncs." : "Block removed. The item can participate in future syncs.";
    } catch (error) { $(".sm-edit-error").textContent = error.message; }
    finally {
      editing = false;
      controls.forEach((control,index) => { control.disabled = disabled[index]; });
      if (root.open) await load();
    }
  }
  $(".sm-add-block").onsubmit = event => {
    event.preventDefault();
    const source = $("[data-source]").value, feature = $("[data-feature]").value;
    if (!source || !feature) { $(".sm-edit-error").textContent = "Select a source, profile and feature for this block."; return; }
    const [provider,instance] = JSON.parse(source);
    changeBlock({provider, instance, feature, key:$("[data-block-key]").value, pair_id:pairId === "shared" ? "" : pairId}, true);
  };
  $(".sm-results").addEventListener("click", async event => {
    const unblock = event.target.closest("[data-unblock]");
    if (unblock && !editing) {
      const rule = currentRows[Number(unblock.dataset.unblock)];
      if (rule) await changeBlock(rule, false);
      return;
    }
    const button = event.target.closest("[data-edit-mapping]");
    if (!button || !editor || editing) return;
    const mapping = currentRows[Number(button.dataset.editMapping)];
    if (!mapping) return;
    editing = true;
    button.disabled = true;
    $("[data-close]").disabled = true;
    $(".sm-edit-error").textContent = "";
    try {
      const focus = await navigateToMapping(mapping, editor);
      if (focus) { afterClose = focus; root.close(); }
    } catch (error) { $(".sm-edit-error").textContent = error.message; }
    finally { editing = false; button.disabled = false; $("[data-close]").disabled = false; }
  });
  $("[data-refresh]").onclick = load;
  for (const button of root.querySelectorAll("[data-view]")) button.onclick = () => {
    if (editing) return;
    entryType = button.dataset.view;
    offset = 0;
    for (const tab of root.querySelectorAll("[data-view]")) tab.setAttribute("aria-pressed", String(tab === button));
    $(".sm-add-block").hidden = entryType !== "block";
    $(".sm-note").textContent = entryType === "block"
      ? `Blocks exclude items from future syncs. ${scoped ? `Scope: ${scopeLabel}.` : "New blocks apply to all pairs using the selected provider instance."} Blocks belonging to a correction are managed with that mapping.`
      : mappingNote;
    $(".sm-edit-error").textContent = $(".sm-action-status").textContent = "";
    load();
  };
  $("[data-search]").oninput = () => { offset=0; clearTimeout(timer); timer=setTimeout(load,200); };
  for(const field of ["[data-source]","[data-feature]"]) $(field).onchange = () => {offset=0; load();};
  $("[data-prev]").onclick = () => {offset=Math.max(0,offset-50);load();};
  $("[data-next]").onclick = () => {offset+=50;load();};
  root.showModal();
  load();
}
