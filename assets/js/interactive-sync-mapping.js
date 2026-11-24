/* assets/js/interactive-sync-mapping.js */
/* CrossWatch - Batch mapping workspace for Interactive Sync */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
const I18N = () => globalThis.window?.CW?.I18n || globalThis.window?.__cwI18n;
const T = (key, vars) => I18N()?.t?.(key, vars) ?? key;
const TH = (key, vars) => I18N()?.html?.(key, vars) ?? key;
const ID_FIELDS = ["tmdb", "imdb", "tvdb", "trakt", "simkl", "mdblist", "wetrakr", "anilist", "mal", "anidb"];
const icon = name => `<span class="material-symbols-rounded" aria-hidden="true">${name}</span>`;
const esc = value => String(value ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"})[c]);
export const seriesTitle = item => String(item.series_title || item.show_title || item.title || "").replace(/\s*[·-]?\s*S\d+E\d+.*$/i, "").trim();
export function sameSeason(left, right) {
  return left.item.type === "episode" && right.item.type === "episode"
    && ["source", "source_instance", "provider", "instance", "feature"].every(key => left[key] === right[key])
    && seriesTitle(left.item).toLocaleLowerCase() === seriesTitle(right.item).toLocaleLowerCase()
    && Number(left.item.season) === Number(right.item.season);
}
export function correctedItem(original, {ids, title, season, offset = 0}) {
  const item = structuredClone(original);
  if (ids) {
    item.ids = {...ids};
    if (["episode", "season"].includes(item.type)) item.show_ids = {...ids};
  }
  if (title) {
    item.title = title;
    if (["episode", "season"].includes(item.type)) {
      item.series_title = title;
      item.show_title = title;
    }
  }
  if (["episode", "season"].includes(item.type) && season !== "" && season != null) item.season = Number(season);
  if (item.type === "episode") item.episode = Number(item.episode) + Number(offset);
  return item;
}

export function suggestedPart(original, parts) {
  const text = value => String(value || "").replace(/\W+/g, "").toLocaleLowerCase();
  const title = text(original.title);
  const named = parts.filter(part => text(part.title) && title.includes(text(part.title)));
  if (named.length === 1) return {number:named[0].number, sure:true};
  const dated = parts.filter(part => part.year && Number(part.year) === Number(original.year));
  if (dated.length === 1) return {number:dated[0].number, sure:true};
  return {number:parts[0].number, sure:false};
}

export function matchedIds(current, next) {
  const shared = Object.keys(next || {}).filter(key => current?.[key]);
  if (!shared.length || shared.some(key => String(current[key]) !== String(next[key]))) return null;
  return {...current, ...next};
}

export function candidateConflict(original, candidate) {
  const current = original.show_ids || original.ids || {};
  const conflicts = ID_FIELDS.filter(key => current[key] && candidate.ids?.[key] && String(current[key]) !== String(candidate.ids[key]));
  return conflicts.length ? `${conflicts.map(key => key.toUpperCase()).join(", ")} IDs differ. Confirm this is the same title before saving.` : "";
}

export function changeSummary(original, item) {
  const before = original.show_ids || original.ids || {}, after = item.show_ids || item.ids || {};
  const parts = [];
  if (original.type !== item.type) parts.push(T("sync.mapping.change.type", {from: original.type, to: item.type}));
  if (seriesTitle(original) !== seriesTitle(item)) parts.push(T("sync.mapping.change.title"));
  const yearOf = value => String((["episode", "season"].includes(value.type) ? value.series_year : value.year) ?? "");
  if (yearOf(original) !== yearOf(item)) parts.push(T("sync.mapping.change.year"));
  if (["season", "episode", "part"].some(key => (original[key] ?? "") !== (item[key] ?? ""))) parts.push(T("sync.mapping.change.numbering"));
  for (const key of new Set([...Object.keys(before), ...Object.keys(after)])) {
    if (String(before[key] ?? "") === String(after[key] ?? "")) continue;
    parts.push(!after[key] ? T("sync.mapping.change.idRemoved", {id: key.toUpperCase()}) : before[key] ? `${key.toUpperCase()} ${before[key]} → ${after[key]}` : T("sync.mapping.change.idAdded", {id: key.toUpperCase(), value: String(after[key])}));
  }
  return parts.slice(0, 4).join(", ");
}

export function mappingGroups(rows) {
  const groups = [];
  rows.forEach((row, index) => {
    const group = groups.find(indices => sameSeason(rows[indices[0]], row));
    if (group) group.push(index); else groups.push([index]);
  });
  return groups;
}

export function correctedEpisode(original, match) {
  const ids = original.show_ids || original.ids || {};
  const sameShow = ids.tmdb && String(ids.tmdb) === String(match.ids?.tmdb);
  return {...correctedItem(original, {...match, ids: sameShow ? {...ids, ...match.ids} : match.ids}), episode:match.episode};
}

export function movieDraft(original) {
  const item = structuredClone(original);
  item.type = "movie";
  item.title = seriesTitle(original);
  item.year = original.series_year || original.year;
  item.ids = {};
  for (const key of ["season", "episode", "show_ids", "series_title", "series_year", "show_title", "part", "simkl_bucket", "anime_type"]) delete item[key];
  return item;
}

export function mappingAppliesToRow(rule, row, pairId) {
  return rule.provider === row.source && (rule.instance || "default") === (row.source_instance || "default")
    && rule.feature === row.feature && (!rule.pair_id || rule.pair_id === pairId)
    && [rule.key, rule.original_key].includes(row.key);
}

export function openMappingWorkspace({rows, session = {}, json, post, onSaved, onClose, total, mappingApi, standalone = false, scope = "pair", scopes = [{id:"pair", label:T("sync.mapping.scopePair")}, {id:"shared", label:T("sync.mapping.scopeShared")}], scopeNote = null, staged = false, direct = false, recovery = false, allowMovieConversion = false, onEpisodeGroup, pairId = session.pair_id, onRemoved}) {
  const api = mappingApi || {
    catalogs: row => json(`/api/interactive-sync/${session.id}/mapping-catalogs?${new URLSearchParams({revision:session.revision, row_id:row.id})}`),
    search: (row, q, catalog, options) => json(`/api/interactive-sync/${session.id}/mapping-search?${new URLSearchParams({revision:session.revision, row_id:row.id, q, catalog})}`, options),
    parts: (row, simklId, options) => json(`/api/interactive-sync/${session.id}/mapping-parts?${new URLSearchParams({revision:session.revision, row_id:row.id, simkl_id:simklId})}`, options),
    episodes: (edits, options) => json(`/api/interactive-sync/${session.id}/mapping-episodes`, {...options, method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify({revision:session.revision, selection_version:session.selection_version, edits})}),
    save: (edits, scope) => post(`/api/interactive-sync/${session.id}/mappings`, {revision:session.revision, selection_version:session.selection_version, edits, scope}),
  };
  const dialog = document.createElement("dialog");
  dialog.className = "is-page is-mapping-dialog";
  dialog.setAttribute("aria-label", recovery ? T("sync.mapping.recoverTitle") : T("sync.mapping.fixTitle"));
  const drafts = rows.map(row => ({row, item: structuredClone(row.item), checked: true, dirty: false}));
  const canConvert = allowMovieConversion && direct && standalone && !recovery && rows.length === 1
    && rows[0].source === "CROSSWATCH" && rows[0].feature === "history" && ["episode", "show"].includes(rows[0].item.type);
  const converting = () => canConvert && drafts[0].item.type === "movie";
  const saveable = draft => draft.dirty && (!recovery || draft.checked)
    && (!recovery || draft.item.type === draft.row.item.type || !!draft.matchSearch)
    && (!converting() || !!draft.item.ids?.tmdb);
  const cache = new Map();
  const catalogs = new Map();
  const episodeMatching = new Map();
  const routeKey = row => JSON.stringify([row.provider, row.instance || "default"]);
  let saving = false, searching = false, closed = false, searchController = null;
  let background = false, backgroundTimer = null, backgroundRun = null;
  const groups = mappingGroups(rows);
  const idFields = recovery ? ["tmdb", "tvdb", "imdb"] : ID_FIELDS;
  const first = rows[0];
  const single = !recovery && rows.length === 1;
  const endpoint = row => [row.provider, row.instance !== "default" ? row.instance : ""].filter(Boolean).join(" · ");
  const input = (name, value, type = "text", extra = "") => `<input data-field="${name}" type="${type}" value="${esc(value)}" ${extra}>`;
  const episodeLabel = item => item.type === "episode" ? `S${String(item.season).padStart(2,"0")}E${String(item.episode).padStart(2,"0")}` : item.type;
  const itemLabel = item => [seriesTitle(item), item.type === "episode" ? episodeLabel(item) : item.year, item.part ? T("sync.mapping.partN", {n: String(item.part)}) : ""].filter(Boolean).join(" · ");
  const renderDraft = index => {
    const {row, item} = drafts[index], episodic = ["episode", "season"].includes(item.type), ids = item.show_ids || item.ids || {};
    return `<article class="is-map-row" data-draft="${index}">
      <div class="is-map-row-preview"><input type="checkbox" data-edit checked aria-label="${TH("sync.review.selectItem", {title: itemLabel(item)})}">
        <div class="is-map-before"><strong>${esc(itemLabel(item))}</strong><small>${recovery ? `${TH("sync.mapping.itemCount", {count: Number(row.recovery_count || 0)})} · ${TH(row.item.type === "show" ? "sync.mapping.series" : "sync.mapping.movie")}` : `${esc(endpoint(row))} · ${esc(row.feature)}`}</small></div>
        ${icon("arrow_forward")}<div class="is-map-after"><strong data-after>${esc(itemLabel(item))}</strong><span data-draft-status class="is-map-status">${TH("sync.mapping.noChangeYet")}</span></div>
        <div class="is-map-actions"><button class="is-btn is-map-action" data-edit-row aria-expanded="false" aria-controls="mapping-edit-${index}" aria-label="${TH("sync.mapping.editItem", {title: itemLabel(item)})}" title="${TH("sync.mapping.edit")}">${icon("edit")}</button><button class="is-btn is-map-action" data-reset hidden aria-label="${TH("sync.mapping.undoItem", {title: itemLabel(item)})}" title="${TH("sync.mapping.undo")}">${icon("undo")}</button></div>
      </div>
      <div class="is-map-row-editor" id="mapping-edit-${index}" hidden><p data-review-reason hidden></p>
        <div class="is-map-edit-fields"><label>${TH("sync.mapping.title")}${input("title", seriesTitle(item))}</label>${episodic ? `<label>${TH("sync.mapping.season")}${input("season", item.season, "number", 'min="0"')}</label>` : ""}${item.type === "episode" ? `<label>${TH("sync.mapping.episode")}${input("episode", item.episode, "number", 'min="1"')}</label>` : ""}${item.type === "movie" ? `<label data-part-wrap hidden>${TH("sync.mapping.part")}<select data-field="part"></select></label>` : ""}</div>
        <button class="is-btn is-map-action" data-find-row hidden>${icon("search")}${TH("sync.mapping.findAnother")}</button>
        <details class="is-map-advanced"><summary>${TH("sync.mapping.advancedIds")}</summary><div class="is-map-id-inputs">${idFields.map(key => `<label>${key.toUpperCase()}${input(key, ids[key] || "")}</label>`).join("")}</div></details>
      </div></article>`;
  };
  dialog.innerHTML = `<header class="is-mapping-head"><div><div class="is-eyebrow">${TH("sync.mapping.kicker")}</div><h2>${recovery ? T("sync.mapping.recoverTitle") : T("sync.mapping.fixTitle")}</h2><p>${recovery ? T("sync.mapping.recoverIntro") : T("sync.mapping.fixIntro")}</p></div><button class="is-btn is-map-close" data-close aria-label="${TH("sync.mapping.close")}" title="${TH("sync.mapping.close")}">${icon("close")}</button></header>
    <div class="is-mapping-body">
    <div class="is-map-intro"><span>${TH(recovery ? "sync.mapping.titleCount" : "sync.mapping.itemCount", {count: rows.length})}${total > rows.length ? ` ${TH("sync.mapping.fromResults", {count: total})}` : ""}</span><details class="is-map-help" ${recovery ? "hidden" : ""}><summary aria-label="${TH("sync.mapping.aboutLabel")}" title="${TH("sync.mapping.aboutLabel")}">${icon("info")}<span>${TH("sync.mapping.about")}</span></summary><p>${TH("sync.mapping.aboutBody")} ${standalone ? T("sync.mapping.runAgain") : T("sync.mapping.updatesReview")}</p></details></div>
    ${recovery ? `<p>${TH("sync.mapping.recoveryIntro")}</p><p><strong>${TH("sync.mapping.trafficWarning")}</strong> ${TH("sync.mapping.trafficBody")}</p>` : ""}
    <section class="is-map-bulk" aria-label="${TH("sync.mapping.chooseTitle")}"><div class="is-map-search-heading"><h3>${recovery ? T("sync.mapping.chooseTitle") : `<span class="is-map-step">1</span>${TH("sync.mapping.findRight")}`}</h3><div class="is-map-actions"><button class="is-btn" data-suggest hidden>${icon("auto_fix_high")}${TH("sync.mapping.autoMatch")}</button><button class="is-btn" data-stop-search hidden>${TH("sync.mapping.stopSearch")}</button></div></div>
      ${canConvert ? `<label class="is-map-scope">${TH("sync.mapping.matchAs")}<select data-media-type><option value="${esc(first.item.type)}">${TH(first.item.type === "episode" ? "sync.mapping.episodeShow" : "sync.mapping.show")}</option><option value="movie">${TH("sync.mapping.movie")}</option></select><small>${TH("sync.mapping.convertHint")}</small></label>` : ""}
      ${recovery ? `<label class="is-map-scope" data-recovery-type hidden>${TH("sync.mapping.matchAs")}<select data-media-type><option value="show">${TH("sync.mapping.episodeShow")}</option><option value="movie">${TH("sync.mapping.movie")}</option></select><small>${TH("sync.mapping.recoverTypeHint")}</small></label>` : ""}
      <div data-chosen hidden class="is-map-chosen"><span>${icon("check_circle")}<strong data-match></strong></span><button class="is-btn is-small" data-change-match>${TH("sync.mapping.changeMatch")}</button></div>
      <div data-search-panel><div class="is-map-search-bar" hidden><label>${TH("sync.mapping.searchTitle")}<input data-search-query value="${esc(seriesTitle(first.item))}" maxlength="200"></label><label>${TH("sync.mapping.searchIn")}<select data-catalog></select></label>${recovery ? `<label data-language-wrap hidden title="${TH("sync.mapping.languageHint")}">${TH("sync.mapping.metaLanguage")}<select data-language>${Object.entries({"en-US":T("sync.mapping.lang.en"),"nl-NL":T("sync.mapping.lang.nl"),"de-DE":T("sync.mapping.lang.de"),"fr-FR":T("sync.mapping.lang.fr"),"es-ES":T("sync.mapping.lang.es"),"it-IT":T("sync.mapping.lang.it"),"pt-PT":T("sync.mapping.lang.pt"),"pt-BR":T("sync.mapping.lang.ptBr"),"pl-PL":T("sync.mapping.lang.pl"),"da-DK":T("sync.mapping.lang.da"),"sv-SE":T("sync.mapping.lang.sv"),"nb-NO":T("sync.mapping.lang.nb"),"fi-FI":T("sync.mapping.lang.fi"),"cs-CZ":T("sync.mapping.lang.cs"),"tr-TR":T("sync.mapping.lang.tr"),"ja-JP":T("sync.mapping.lang.ja"),"ko-KR":T("sync.mapping.lang.ko"),"zh-CN":T("sync.mapping.lang.zhCn"),"zh-TW":T("sync.mapping.lang.zhTw")}).map(([value,label])=>`<option value="${value}">${label}</option>`).join("")}</select></label>` : ""}<button class="is-btn" data-search>${TH("sync.mapping.findMatches")}</button></div><div class="is-map-candidates"></div></div>
      <p data-search-status role="status">${TH("sync.mapping.chooseOrAuto")}</p>
    </section>
    <div class="is-map-row-tools">${recovery ? "" : `<h3><span class="is-map-step">2</span>${TH("sync.mapping.checkResult")}</h3>`}<strong data-selection-count ${single ? "hidden" : ""}></strong><button class="is-btn is-small" data-check-all ${single ? "hidden" : ""}>${TH("sync.mapping.selectAll")}</button><button class="is-btn is-small" data-check-none ${single ? "hidden" : ""}>${TH("sync.mapping.clearSelection")}</button></div>
    <details class="is-map-numbering" hidden><summary>${TH("sync.mapping.adjustNumbering")}</summary><div class="is-map-numbering-fields"><label>${TH("sync.mapping.season")}<input data-season type="number" min="0" placeholder="${TH("sync.mapping.keepCurrent")}"></label><label>${TH("sync.mapping.shiftBy")}<input data-offset type="number" value="0"></label><button class="is-btn" data-bulk>${TH("sync.mapping.applyNumbering")}</button></div><p>${TH("sync.mapping.shiftHint")}</p></details>
    <div class="is-map-review">${groups.map((indices, groupIndex) => indices.length === 1 ? renderDraft(indices[0]) : `<details class="is-map-group" data-group="${groupIndex}"><summary><span><strong>${esc(seriesTitle(rows[indices[0]].item))} · ${TH("sync.mapping.seasonN", {n: String(rows[indices[0]].item.season)})}</strong><small>${TH("sync.mapping.episodeCount", {count: indices.length})} · ${esc(endpoint(rows[indices[0]]))}</small></span><span data-group-after>${TH("sync.mapping.reviewEpisodes")}</span></summary><div class="is-map-group-tools"><button class="is-btn is-small" data-select-group="${groupIndex}">${TH("sync.mapping.selectSeason")}</button><span data-group-count></span></div>${indices.map(renderDraft).join("")}</details>`).join("")}</div>
    <label class="is-map-scope" ${recovery ? "hidden" : ""}>${TH("sync.mapping.useFixFor")}<select data-mapping-scope ${scopes.length < 2 ? "disabled hidden" : ""}>${scopes.map(option => `<option value="${esc(option.id)}" ${option.id === scope ? "selected" : ""}>${esc(option.label)}</option>`).join("")}</select>${scopes.length < 2 ? `<strong>${esc(scopes[0]?.label || "")}</strong>` : ""}<small data-scope-note></small></label>
    </div><footer class="is-map-save"><div><strong data-count>${TH("sync.mapping.noChanges")}</strong><p data-error role="alert"></p></div><button class="is-btn is-primary" data-save disabled>${TH(recovery ? "sync.mapping.saveChecked" : staged ? (direct ? "sync.mapping.saveFix" : "sync.mapping.useCorrection") : "sync.mapping.saveMappings")}</button></footer>`;
  document.body.append(dialog);
  window.CW?.MobileUI?.openDialog(dialog) || dialog.showModal();
  const $ = selector => dialog.querySelector(selector);
  function refreshSearchSelects() {
    if (!recovery) return;
    for (const [selector,label] of [["[data-catalog]",T("sync.mapping.searchIn")],["[data-language]",T("sync.mapping.metaLanguage")]]) {
      const select=$(selector);
      select.dataset.cwNativeSelect="true";
      const wrap=window.CW?.IconSelect?.enhance(select,{className:"ie-select",menuClassName:"ie-select-menu",menuMinWidth:240,sortAlphabetically:true});
      wrap?.querySelector("button")?.setAttribute("aria-label",label);
      wrap?.__cwMenu?.setAttribute("aria-label",label);
    }
  }
  refreshSearchSelects();
  function metadataLanguage() {
    return recovery && !$("[data-language-wrap]").hidden ? $("[data-language]").value : "en-US";
  }
  function updateLanguage() {
    if (!recovery) return;
    const row = drafts.find(d => d.checked)?.row || first;
    const choice = (catalogs.get(routeKey(row)) || []).find(option => option.id === $("[data-catalog]").value);
    $("[data-language-wrap]").hidden = !choice?.supports_language;
  }
  if (recovery) {
    const searchOptionsChanged = () => {
      updateLanguage();
      showSearch();
      $(".is-map-candidates").replaceChildren();
      status(T("sync.mapping.searchAgain"));
    };
    $("[data-catalog]").onchange = searchOptionsChanged;
    $("[data-language]").onchange = searchOptionsChanged;
  }
  function updateScopeNote() {
    if (converting()) {
      $("[data-scope-note]").textContent = T("sync.mapping.convertsOnly");
      return;
    }
    if (typeof scopeNote === "function") {
      $("[data-scope-note]").textContent = scopeNote($("[data-mapping-scope]").value);
      return;
    }
    const shared = $("[data-mapping-scope]").value === "shared";
    $("[data-scope-note]").textContent = (shared
      ? T("sync.mapping.sharedNote")
      : T("sync.mapping.pairNote"))
      + (!staged ? ` ${T("sync.mapping.futureSyncs")}` : direct ? ` ${T("sync.mapping.savedNow")}` : ` ${T("sync.mapping.editorUnsaved")}`);
  }
  $("[data-mapping-scope]").onchange = updateScopeNote;
  updateScopeNote();
  function updateCatalogs() {
    const row = drafts.find(d => d.checked)?.row || first;
    if (recovery) {
      const selected = drafts.filter(d => d.checked);
      $("[data-recovery-type]").hidden = selected.length !== 1 || selected[0].row.item.type !== "show";
      $("[data-media-type]").value = selected[0]?.item.type || "show";
    }
    const options = catalogs.get(routeKey(row)) || [];
    const select = $("[data-catalog]"), previous = select.value;
    select.innerHTML = options.map(option => `<option value="${esc(option.id)}">${esc(option.label)}</option>`).join("");
    if (options.some(option => option.id === previous)) select.value = previous;
    updateLanguage();
    $(".is-map-search-bar").hidden = !options.length;
    $("[data-suggest]").hidden = !options.length || converting();
    $(".is-map-numbering").hidden = !drafts.some(d => d.checked && d.item.type === "episode");
    $("[data-selection-count]").textContent = T("sync.mapping.selectedCount", {count: drafts.filter(d => d.checked).length});
    dialog.querySelectorAll("[data-find-row]").forEach(button => {
      const index = Number(button.closest("[data-draft]").dataset.draft);
      button.hidden = !(catalogs.get(routeKey(drafts[index].row)) || []).length;
    });
    updateGroups();
  }
  async function loadCatalogs() {
    const unique = new Map(rows.map(row => [routeKey(row), row]));
    let recoveryState;
    const results = await Promise.allSettled([...unique].map(async ([key, row]) => {
      const data = await api.catalogs(row);
      catalogs.set(key, data.catalogs || []);
      episodeMatching.set(key, !!data.episode_matching);
      if (recovery && api.autoStatus) recoveryState=data;
    }));
    if (closed) return;
    updateCatalogs();
    if (results.some(result => result.status === "rejected")) status(T("sync.mapping.catalogsFailed"));
    else if (![...catalogs.values()].some(options => options.length)) status(T("sync.mapping.noCatalog"));
    if(recoveryState) syncBackground(recoveryState);
    if (!recovery && groups.length === 1 && (catalogs.get(routeKey(first)) || []).length) find(first);
  }
  loadCatalogs();
  function status(message) { $("[data-search-status]").textContent = message; }
  function showSearch() {
    $("[data-search-panel]").hidden = false;
    $("[data-chosen]").hidden = true;
  }
  function selectionChanged() {
    dialog.querySelectorAll("[data-edit]").forEach(el => { el.checked = drafts[Number(el.closest("[data-draft]").dataset.draft)].checked; });
    showSearch();
    if (recovery) $(".is-map-candidates").replaceChildren();
    updateCatalogs();
    if (recovery) drafts.forEach(changed);
  }
  function updateGroups() {
    groups.forEach((indices, groupIndex) => {
      const group = $(`[data-group="${groupIndex}"]`);
      if (!group) return;
      const items = indices.map(index => drafts[index]);
      const changed = items.filter(d => d.dirty).length;
      const review = items.filter(d => d.review).length;
      const targets = [...new Set(items.filter(d => d.dirty).map(d => `${seriesTitle(d.item)} · ${T("sync.mapping.seasonN", {n: String(d.item.season)})}`))];
      group.querySelector("[data-group-after]").textContent = [targets.length === 1 ? `→ ${targets[0]}` : targets.length ? T("sync.mapping.corrected", {count: targets.length}) : T("sync.mapping.reviewEpisodes"), review ? T("sync.mapping.needReview", {count: review}) : changed ? T("sync.mapping.changedCount", {count: changed}) : ""].filter(Boolean).join(" · ");
      group.querySelector("[data-group-count]").textContent = T("sync.mapping.ofSelected", {selected: items.filter(d => d.checked).length, total: items.length});
    });
  }
  function draftStatus(draft, reason = "") {
    const cell = dialog.querySelector(`[data-draft="${drafts.indexOf(draft)}"] [data-draft-status]`);
    cell.className = `is-map-status${draft.review ? " is-review" : draft.dirty ? " is-edited" : ""}`;
    const summary = draft.dirty ? changeSummary(draft.row.item, draft.item) : "";
    cell.textContent = draft.review ? T("sync.mapping.needsReview") : draft.dirty ? (summary ? T("sync.mapping.changedSummary", {summary}) : T("sync.mapping.changed")) : T("sync.mapping.noChangeYet");
    cell.title = reason || draft.review || "";
    const explanation = $(`[data-draft="${drafts.indexOf(draft)}"] [data-review-reason]`);
    explanation.textContent = draft.review || "";
    explanation.hidden = !draft.review;
    updateGroups();
  }
  function changed(draft) {
    draft.dirty = JSON.stringify(draft.item) !== JSON.stringify(draft.row.item);
    draftStatus(draft);
    const count = drafts.filter(saveable).length;
    $("[data-count]").textContent = count ? T("sync.mapping.readyToSave", {count}) : T("sync.mapping.noChanges");
    const row = $(`[data-draft="${drafts.indexOf(draft)}"]`);
    row.querySelector("[data-after]").textContent = itemLabel(draft.item);
    row.querySelector("[data-reset]").hidden = !draft.dirty;
    $("[data-save]").disabled = !count || saving || searching;
  }
  function applyParts(draft, match, parts) {
    delete draft.item.part;
    for (const key of ["simkl_bucket", "anime_type"]) {
      if (match[key]) draft.item[key] = match[key];
      else if (key in draft.row.item) draft.item[key] = draft.row.item[key];
      else delete draft.item[key];
    }
    const row = dialog.querySelector(`[data-draft="${drafts.indexOf(draft)}"]`);
    const wrap = row.querySelector("[data-part-wrap]");
    if (!wrap) return;
    wrap.hidden = !parts.length;
    wrap.querySelector("select").innerHTML = parts.map(part => `<option value="${esc(part.number)}">${esc([T("sync.mapping.partN", {n: String(part.number)}), part.title, part.year].filter(Boolean).join(" · "))}</option>`).join("");
    if (!parts.length) return;
    const suggestion = suggestedPart(draft.row.item, parts);
    draft.item.ids = {simkl:String(match.ids.simkl)};
    draft.item.part = suggestion.number;
    if (!suggestion.sure) draft.review = T("sync.mapping.choosePart");
    const editor = row.querySelector(".is-map-row-editor");
    editor.hidden = false;
    row.querySelector("[data-edit-row]").setAttribute("aria-expanded", "true");
  }
  function paint(draft) {
    const el = dialog.querySelector(`[data-draft="${drafts.indexOf(draft)}"]`);
    el.querySelectorAll("[data-field]").forEach(field => {
      const key = field.dataset.field;
      field.value = ID_FIELDS.includes(key) ? (draft.item.show_ids || draft.item.ids || {})[key] || "" : key === "title" ? seriesTitle(draft.item) : draft.item[key] ?? "";
      if (canConvert && ["season", "episode"].includes(key)) field.closest("label").hidden = converting();
    });
    if (canConvert) {
      $("[data-media-type]").value = draft.item.type;
      if (converting()) $("[data-mapping-scope]").value = "record";
      $("[data-mapping-scope]").disabled = converting() || scopes.length < 2;
      updateScopeNote();
      updateCatalogs();
    }
    changed(draft);
  }
  function lock(value) {
    if (recovery && value) window.CW?.IconSelect?.closeAll();
    dialog.querySelectorAll("button,input,select").forEach(el => { el.disabled = value || el.hasAttribute("data-match-unavailable") || (el.hasAttribute("data-mapping-scope") && (scopes.length < 2 || converting())); });
    $("[data-stop-search]").hidden = !searching;
    $("[data-stop-search]").disabled = !searching;
    if(background) $("[data-close]").disabled=false;
    if (!value) $("[data-save]").disabled = !drafts.some(saveable) || saving || searching;
  }
  async function search(row, term) {
    const catalog = $("[data-catalog]").value;
    if (!(catalogs.get(routeKey(row)) || []).some(option => option.id === catalog)) return null;
    const language = metadataLanguage();
    const key = JSON.stringify([row.provider, row.instance, row.item.type === "movie" ? "movie" : "show", term, catalog, language]);
    if (!cache.has(key)) {
      const data = await api.search(row, term, catalog, {signal:searchController?.signal, language});
      if (data.results.some(match => match.mapping_unavailable)) return data;
      cache.set(key, data);
    }
    return cache.get(key);
  }
  async function find(row) {
    if (canConvert) row = {...row, item: drafts[0].item};
    if (recovery) row = {...row, item: drafts.find(d => d.row.id === row.id)?.item || row.item};
    const term = $("[data-search-query]").value.trim();
    const matchSearch = {catalog:$("[data-catalog]").value, match_query:term, language:metadataLanguage()};
    if (term.length < 2) { status(T("sync.mapping.twoChars")); return; }
    showSearch();
    searchController = new AbortController(); searching = true; lock(true); status(T("sync.mapping.searching", {target: endpoint(row)}));
    try {
      const data = await search(row, term);
      if (closed) return;
      if (!data) { status(T("sync.mapping.searchUnavailable")); return; }
      const matches = data.results.slice(0, 10);
      status(`${data.catalog}: ${T(data.results.length > matches.length ? "sync.mapping.matchesShown" : "sync.mapping.matchesChoose", {count: matches.length})}`);
      $(".is-map-candidates").replaceChildren();
      matches.forEach(match => {
        const button = document.createElement("button");
        button.type = "button"; button.className = "is-btn is-map-candidate";
        if (match.mapping_unavailable) {
          button.setAttribute("data-match-unavailable", "");
          button.disabled = true;
        }
        const conflict = candidateConflict(row.item, match);
        button.title = [match.title, match.year, match.mapping_unavailable, conflict].filter(Boolean).join(" · ");
        button.innerHTML = `${match.ids?.tmdb ? `<img class="is-map-candidate-art" src="/art/tmdb/${row.item.type === "movie" ? "movie" : "tv"}/${encodeURIComponent(match.ids.tmdb)}?fallback=2&size=w92${window.CW?.ArtChoice?.param?.(row.item.type, match.ids.tmdb) || ""}" alt="" loading="lazy">` : ""}<span class="is-map-candidate-copy"><strong>${esc(match.title)}</strong>${match.year ? `<span class="is-map-candidate-year">${esc(match.year)}</span>` : ""}</span>`;
        if (conflict) {
          const warning = document.createElement("span");
          warning.className = "is-map-candidate-warning";
          warning.textContent = T("sync.mapping.differentIds");
          warning.title = conflict;
          button.querySelector(".is-map-candidate-copy").append(warning);
        }
        button.querySelector("img")?.addEventListener("error", event => event.target.remove());
        button.onclick = async () => {
          if (match.mapping_unavailable) { status(match.mapping_unavailable); return; }
          const selected = drafts.filter(d => d.checked);
          if (!selected.length) { status(T("sync.mapping.checkFirst")); return; }
          if (recovery && selected.length !== 1) {
            status(T("sync.mapping.selectOne"));
            return;
          }
          if (recovery && selected[0].row.id !== row.id) {
            status(T("sync.mapping.searchSelected"));
            return;
          }
          if (selected.some(d => routeKey(d.row) !== routeKey(row)
            || (d.item.type === "movie" ? "movie" : "show") !== (row.item.type === "movie" ? "movie" : "show"))) {
            status(T("sync.mapping.sameDestination"));
            return;
          }
          let parts = [];
          if (!recovery && api.parts && match.simkl_bucket === "anime" && match.anime_type === "movie" && match.ids?.simkl) {
            searchController = new AbortController(); searching = true; lock(true);
            status(T("sync.mapping.loadingParts"));
            try { parts = (await api.parts(row, String(match.ids.simkl), {signal:searchController.signal})).parts || []; }
            catch (error) { if (!closed) status(error.name === "AbortError" ? T("sync.mapping.stoppedParts") : T("sync.mapping.partsFailed", {error: error.message})); return; }
            finally { searching = false; if (!closed) lock(false); }
            if (closed) return;
          }
          const yearField = draft => ["episode", "season"].includes(draft.item.type) ? "series_year" : "year";
          const fillsYear = draft => Boolean(match.year) && !draft.item[yearField(draft)];
          const pending = recovery || parts.length ? selected : selected.filter(draft => {
            const current = draft.item.show_ids || draft.item.ids || {};
            const merged = matchedIds(current, match.ids);
            return fillsYear(draft) || !merged || Object.keys(merged).length !== Object.keys(current).length;
          });
          if (!pending.length) {
            status(T("sync.mapping.alreadyMatched"));
            return;
          }
          pending.forEach(draft => {
            draft.review = "";
            const current = draft.item.show_ids || draft.item.ids || {};
            const missingYear = fillsYear(draft);
            draft.item = correctedItem(draft.item, {ids:(!recovery && !parts.length && matchedIds(current, match.ids)) || match.ids, title:match.title});
            if (recovery) draft.matchSearch = matchSearch;
            if (converting() && match.year) draft.item.year = match.year;
            if (recovery && match.year) draft.item.year = match.year;
            if (missingYear) draft.item[yearField(draft)] = match.year;
            applyParts(draft, match, parts);
            paint(draft);
          });
          $("[data-match]").textContent = [match.title, match.year].filter(Boolean).join(" · ");
          $("[data-chosen]").hidden = false;
          $("[data-search-panel]").hidden = true;
          $("[data-change-match]").focus();
          $(".is-map-candidates").querySelectorAll(".is-map-candidate").forEach(b => b.setAttribute("aria-pressed", String(b === button)));
          searchController = new AbortController(); searching = true; lock(true);
          status(T("sync.mapping.checkingNumbering"));
          try {
            const review = await matchEpisodes(pending);
            if (!closed) status(`${T("sync.mapping.itemsUpdated", {count: pending.length})} ${review ? T("sync.mapping.needEpisodeReview", {count: review}) : T("sync.mapping.reviewChanges")}`);
          } catch (error) { if (!closed) status(error.name === "AbortError" ? T("sync.mapping.stoppedCheck") : T("sync.mapping.episodeCheckFailed", {error: error.message})); }
          finally { searching = false; if (!closed) lock(false); }
        };
        const card = window.CW?.ProfileMediaModal;
        const preview = match.ids?.tmdb ? {type: row.item.type === "movie" ? "movie" : "show", title: match.title || "", year: match.year || "", ids: {tmdb: String(match.ids.tmdb)}} : null;
        if (preview && card?.canOpen?.(preview)) {
          const wrap = document.createElement("div");
          wrap.className = "is-map-candidate-wrap";
          const info = document.createElement("button");
          info.type = "button"; info.className = "is-btn is-map-candidate-info";
          info.title = T("sync.mapping.detailsFor", {title: match.title});
          info.setAttribute("aria-label", info.title);
          info.innerHTML = icon("info");
          info.onclick = () => card.open(preview, {host: dialog});
          wrap.append(button, info);
          $(".is-map-candidates").append(wrap);
        } else {
          $(".is-map-candidates").append(button);
        }
      });
    } catch (error) { status(error.name === "AbortError" ? T("sync.mapping.searchStopped") : error.message); }
    finally { searching = false; lock(false); }
  }
  dialog.addEventListener("input", event => {
    const field = event.target, tr = field.closest("[data-draft]");
    if (!tr || !field.dataset.field) return;
    const draft = drafts[Number(tr.dataset.draft)], key = field.dataset.field;
    draft.review = "";
    if (ID_FIELDS.includes(key)) {
      const ids = {...(draft.item.show_ids || draft.item.ids || {})};
      if (field.value.trim()) ids[key] = field.value.trim(); else delete ids[key];
      draft.item = correctedItem(draft.item, {ids});
    } else if (key === "title") draft.item = correctedItem(draft.item, {title:field.value});
    else draft.item[key] = field.value === "" ? null : Number(field.value);
    changed(draft);
  });
  dialog.addEventListener("change", event => {
    if ((canConvert || recovery) && event.target.hasAttribute("data-media-type") && !saving && !searching) {
      const selected = drafts.filter(d => d.checked);
      if (recovery && (selected.length !== 1 || selected[0].row.item.type !== "show")) return;
      const draft = recovery ? selected[0] : drafts[0];
      draft.item = event.target.value === "movie" ? movieDraft(draft.row.item) : structuredClone(draft.row.item);
      delete draft.matchSearch;
      draft.review = "";
      paint(draft);
      $("[data-search-query]").value = seriesTitle(draft.item);
      showSearch();
      $(".is-map-candidates").replaceChildren();
      status(T("sync.mapping.searchCorrect"));
      return;
    }
    if (event.target.hasAttribute("data-edit")) {
      drafts[Number(event.target.closest("[data-draft]").dataset.draft)].checked = event.target.checked;
      selectionChanged();
    }
  });
  dialog.addEventListener("click", event => {
    const button = event.target.closest("button");
    if (!button) return;
    if (button.hasAttribute("data-select-group")) {
      const indices = groups[Number(button.dataset.selectGroup)];
      drafts.forEach((d, index) => { d.checked = indices.includes(index); });
      selectionChanged();
      $("[data-search-query]").value = seriesTitle(drafts[indices[0]].item);
      $(".is-map-bulk").scrollIntoView({block:"nearest"});
      return;
    }
    const row = button.closest("[data-draft]");
    if (!row) return;
    const draft = drafts[Number(row.dataset.draft)];
    if (button.hasAttribute("data-edit-row")) {
      const editor = row.querySelector(".is-map-row-editor");
      editor.hidden = !editor.hidden;
      button.setAttribute("aria-expanded", String(!editor.hidden));
      if (!editor.hidden) editor.querySelector("input").focus();
    }
    if (button.hasAttribute("data-reset")) {
      draft.item = structuredClone(draft.row.item); draft.review = "";
      delete draft.matchSearch;
      if (recovery) selectionChanged();
      const partWrap = row.querySelector("[data-part-wrap]");
      if (partWrap) partWrap.hidden = true;
      paint(draft);
      showSearch();
      row.querySelector("[data-edit-row]").focus();
    }
    if (button.hasAttribute("data-find-row")) {
      drafts.forEach(d => { d.checked = d === draft; });
      selectionChanged();
      $("[data-search-query]").value = seriesTitle(draft.item);
      $(".is-map-bulk").scrollIntoView({block:"nearest"});
      find(draft.row);
    }
  });
  $("[data-change-match]").onclick = () => { showSearch(); $("[data-search-query]").focus(); };
  $("[data-stop-search]").onclick = async () => {
    if(!background) {searchController?.abort();return;}
    try {await api.autoStop();status(T("sync.mapping.stopping"));}
    catch(error) {status(error.message);}
  };
  $("[data-search]").onclick = () => {
    const selected = drafts.find(d => d.checked);
    if (!selected) { status(T("sync.mapping.selectItem")); return; }
    find(selected.row);
  };
  $("[data-search-query]").onkeydown = event => { if (event.key === "Enter") { event.preventDefault(); $("[data-search]").click(); } };
  async function matchEpisodes(items) {
    const episodes = items.filter(d => d.item.type === "episode");
    const selected = episodes.filter(d => episodeMatching.get(routeKey(d.row)));
    for (const draft of episodes.filter(d => !selected.includes(d))) {
      draft.review = T("sync.mapping.autoEpisodeUnavailable");
      draftStatus(draft);
    }
    if (!selected.length) return episodes.length;
    try {
      const data = await api.episodes(selected.map(d => ({row_id:d.row.id, item:d.item})), {signal:searchController.signal});
      if (closed) return 0;
      for (const draft of selected) {
        const result = data.results.find(result => result.row_id === draft.row.id);
        if (result?.match) {
          draft.item = correctedEpisode(draft.item, result.match);
          draft.review = "";
          paint(draft);
          draftStatus(draft, result.reason);
        } else {
          draft.review = result?.reason || T("sync.mapping.noReliableEpisode");
          draftStatus(draft);
        }
      }
      return episodes.filter(d => d.review).length;
    } catch (error) {
      for (const draft of selected) {
        draft.review = T("sync.mapping.numberingUnchecked");
        draftStatus(draft);
      }
      throw error;
    }
  }
  function syncBackground(data) {
    if(closed)return;
    const state=data.auto_match || {};
    if (state.id && state.id !== backgroundRun) {
      backgroundRun=state.id;
      $("[data-catalog]").value=state.catalog || $("[data-catalog]").value;
      if (state.language) $("[data-language]").value=state.language;
      updateLanguage();
      refreshSearchSelects();
    }
    background=state.status === "running";searching=background;
    for(const draft of drafts) {
      const suggestion=data.suggestions?.[draft.row.id];
      if(!suggestion || (draft.dirty && JSON.stringify(draft.item) !== draft.persisted))continue;
      if(suggestion.match) {
        draft.item=correctedItem(draft.row.item,suggestion.match);
        if(suggestion.match.year)draft.item.year=suggestion.match.year;
        draft.review="";
        draft.persisted=JSON.stringify(draft.item);
        paint(draft);
      } else {draft.review=suggestion.reason;draftStatus(draft);}
    }
    lock(background);
    if(state.id) status(`${state.message} ${T("sync.mapping.autoProgress", {done: state.done, total: state.total, matched: state.matched})}${state.current_title && background ? ` ${T("sync.mapping.currentTitle", {title: state.current_title})}` : ""}`);
    clearTimeout(backgroundTimer);
    if(background)backgroundTimer=setTimeout(pollBackground,1500);
  }
  async function pollBackground() {
    try {syncBackground(await api.autoStatus());}
    catch(error) {
      if(closed)return;
      status(T("sync.mapping.autoRefreshFailed", {error: error.message}));
      backgroundTimer=setTimeout(pollBackground,5000);
    }
  }
  $("[data-suggest]").onclick = async () => {
    const selected = drafts.filter(d => d.checked && (!recovery || !d.dirty || d.review));
    if (!selected.length) { status(recovery ? T("sync.mapping.allSuggested") : T("sync.mapping.selectToMatch")); return; }
    if(recovery && api.autoStart) {
      if(!(await (window.CW?.MobileUI?.ask || window.confirm)(T("sync.mapping.autoConfirm", {count: selected.length}))))return;
      background=true;searching=true;lock(true);
      try {syncBackground(await api.autoStart(selected.map(d=>d.row.id),$("[data-catalog]").value,metadataLanguage()));}
      catch(error) {status(error.message);backgroundTimer=setTimeout(pollBackground,1500);}
      return;
    }
    searchController = new AbortController(); searching = true; lock(true);
    const matched = [];
    try {
      for (const draft of selected) {
        if (searchController.signal.aborted) break;
        status(T("sync.mapping.finding", {n: selected.indexOf(draft) + 1, total: selected.length}));
        const data = await search(draft.row, seriesTitle(draft.item));
        if (closed) return;
        const year = ["episode", "season"].includes(draft.item.type) ? draft.item.series_year : draft.item.year;
        const exact = (data?.results || []).filter(row => row.exact_title && (!year || !row.year || Number(year) === Number(row.year)));
        if (exact.length === 1 && !exact[0].mapping_unavailable) {
          draft.review = draft.item.type === "episode" ? T("sync.mapping.numberingPending") : "";
          draft.item = correctedItem(draft.item, {ids:exact[0].ids, title:exact[0].title});
          if (recovery && exact[0].year) draft.item.year = exact[0].year;
          paint(draft); matched.push(draft);
        } else {
          draft.review = T("sync.mapping.chooseManually");
          draftStatus(draft);
        }
      }
      if (searchController.signal.aborted) {
        status(T("sync.mapping.stoppedKept"));
        return;
      }
      status(T("sync.mapping.checkingNumbering"));
      const episodeReview = await matchEpisodes(matched);
      if (!closed) {
        const review = selected.length - matched.length + episodeReview;
        status(`${T("sync.mapping.matchedCount", {count: selected.length - review})}${review ? ` · ${T("sync.mapping.needReview", {count: review})}` : ""}. ${T("sync.mapping.reviewBelow")}`);
        if (review) showSearch();
        else {
          $("[data-match]").textContent = T("sync.mapping.itemsMatched", {count: matched.length});
          $("[data-search-panel]").hidden = true;
          $("[data-chosen]").hidden = false;
        }
      }
    } catch (error) { if (!closed) status(error.name === "AbortError" ? T("sync.mapping.stoppedKept") : T("sync.mapping.keptError", {error: error.message})); }
    finally { searching = false; if (!closed) lock(false); }
  };
  $("[data-bulk]").onclick = () => {
    const selected = drafts.filter(d => d.checked && d.item.type === "episode");
    const season = $("[data-season]"), offset = $("[data-offset]");
    if (!season.reportValidity() || !offset.reportValidity()) return;
    const corrected = selected.map(d => correctedItem(d.item, {season:season.value, offset:offset.value}));
    if (corrected.some(item => !Number.isInteger(item.episode) || item.episode < 1)) {
      status(T("sync.mapping.invalidShift")); return;
    }
    selected.forEach((draft,i) => { draft.item = corrected[i]; draft.review = ""; paint(draft); });
    status(T("sync.mapping.numberingUpdated", {count: selected.length}));
    offset.value = "0";
    $(".is-map-numbering").open = false;
  };
  for (const [selector, checked] of [["[data-check-all]",true],["[data-check-none]",false]]) $(selector).onclick = () => {
    drafts.forEach(d => { d.checked = checked; });
    selectionChanged();
  };
  async function close() {
    if (saving || (searching && !background)) return;
    if (drafts.some(d => d.dirty && JSON.stringify(d.item) !== d.persisted) && !(await (window.CW?.MobileUI?.ask || window.confirm)(T("sync.mapping.discard")))) return;
    dismiss();
  }
  function dismiss() {
    if (closed) return;
    clearTimeout(backgroundTimer);
    closed = true; dialog.close(); dialog.remove(); onClose();
  }
  const warnUnsaved = event => { if (drafts.some(d => d.dirty && JSON.stringify(d.item) !== d.persisted) && !closed) { event.preventDefault(); event.returnValue = ""; } };
  window.addEventListener("beforeunload", warnUnsaved);
  dialog.addEventListener("close", () => {
    window.removeEventListener("beforeunload", warnUnsaved);
    if (recovery) {
      window.CW?.IconSelect?.closeAll();
      dialog.querySelectorAll("select").forEach(select=>select.__cwOptionsObserver?.disconnect());
    }
  });
  dialog.addEventListener("cancel", event => { event.preventDefault(); close(); });
  $("[data-close]").onclick = close;
  if (single && pairId && onRemoved && first.source && first.key) {
    const section = document.createElement("div");
    section.className = "is-map-intro";
    section.textContent = T("sync.mapping.checkingSaved");
    $(".is-mapping-body").prepend(section);
    const requestMapping = async (url, options = {}) => {
      const response = await fetch(url, {credentials:"same-origin", cache:"no-store", ...options});
      const data = await response.json();
      if (!response.ok || data.ok === false) throw new Error(typeof data.detail === "string" ? data.detail : T("sync.mapping.updateSavedFailed"));
      return data;
    };
    (async () => {
      try {
        const rules = [];
        for (const scopeId of [pairId, "shared"]) {
          let offset = 0;
          while (!closed) {
            const params = new URLSearchParams({provider:first.source, instance:first.source_instance || "default", feature:first.feature, pair_id:scopeId, offset, limit:100});
            const data = await requestMapping("/api/editor/mappings?" + params);
            rules.push(...data.items.filter(rule => mappingAppliesToRow(rule, first, pairId)));
            offset += data.items.length;
            if (!data.items.length || offset >= data.total) break;
          }
        }
        if (closed) return;
        section.replaceChildren();
        if (!rules.length) { section.remove(); return; }
        for (const rule of rules) {
          const label = rule.corrected?.title || rule.corrected?.series_title || rule.key;
          const scopeLabel = rule.pair_id ? T("sync.mapping.thisPair") : T("sync.mapping.allPairsSource");
          const button = document.createElement("button");
          button.type = "button"; button.className = "is-btn";
          button.textContent = T("sync.mapping.removeSaved", {label, scope: scopeLabel});
          button.onclick = async () => {
            if (saving || searching || closed) return;
            if (!(await (window.CW?.MobileUI?.ask || window.confirm)(T("sync.mapping.removeConfirm", {label, scope: scopeLabel})))) return;
            saving = true; lock(true); button.disabled = true;
            try {
              const identity = {...Object.fromEntries(["provider", "instance", "feature", "key", "pair_id"].map(key => [key, rule[key]])), entry_type:"mapping"};
              await requestMapping("/api/editor/mappings/delete", {method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify(identity)});
              closed = true; dialog.close(); dialog.remove(); onRemoved();
            } catch (error) { $("[data-error]").textContent = error.message; }
            finally { saving = false; if (!closed) { lock(false); button.disabled = false; } }
          };
          section.append(button);
        }
      } catch (error) { if (!closed) section.textContent = T("sync.mapping.checkSavedFailed", {error: error.message}); }
    })();
  }
  if (onEpisodeGroup) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "is-btn";
    button.textContent = T("sync.mapping.mapSplit");
    button.dataset.episodeGroups = "";
    button.onclick = async () => {
      if (saving || searching || closed) return;
      button.disabled = true;
      try { await onEpisodeGroup(button); }
      catch (error) { $("[data-error]").textContent = error.message; }
      finally { button.disabled = false; }
    };
    $(".is-map-intro").append(button);
  }
  $("[data-save]").onclick = async () => {
    for (const draft of drafts.filter(saveable)) {
      const row = $(`[data-draft="${drafts.indexOf(draft)}"]`);
      const invalid = Array.from(row.querySelectorAll("[data-field]")).find(el => !el.checkValidity());
      if (!invalid) continue;
      const group = row.closest(".is-map-group");
      if (group) group.open = true;
      row.querySelector(".is-map-row-editor").hidden = false;
      row.querySelector("[data-edit-row]").setAttribute("aria-expanded", "true");
      if (invalid.closest("details")) invalid.closest("details").open = true;
      invalid.reportValidity();
      return;
    }
    const edits = drafts.filter(saveable).map(d => ({row_id:d.row.id, item:d.item, selected:true,
      ...(recovery && d.item.type !== d.row.item.type ? {matchSearch:d.matchSearch} : {})}));
    if (!edits.length) return;
    saving = true; lock(true); $("[data-error]").textContent = "";
    try {
      const data = await api.save(edits, $("[data-mapping-scope]").value);
      closed = true; dialog.close(); dialog.remove(); onSaved(data);
    } catch (error) { $("[data-error]").textContent = error.message; }
    finally { saving = false; if (!closed) lock(false); }
  };
  return {close, destroy() { searchController?.abort(); dismiss(); }};
}
