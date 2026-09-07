/* assets/js/interactive-sync-mapping.js */
/* CrossWatch - Batch mapping workspace for Interactive Sync */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
const ID_FIELDS = ["tmdb", "imdb", "tvdb", "trakt", "simkl", "mdblist", "anilist", "mal", "anidb"];
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

export function openMappingWorkspace({rows, session = {}, json, post, onSaved, onClose, total, mappingApi, standalone = false}) {
  const api = mappingApi || {
    catalogs: row => json(`/api/interactive-sync/${session.id}/mapping-catalogs?${new URLSearchParams({revision:session.revision, row_id:row.id})}`),
    search: (row, q, catalog, options) => json(`/api/interactive-sync/${session.id}/mapping-search?${new URLSearchParams({revision:session.revision, row_id:row.id, q, catalog})}`, options),
    episodes: (edits, options) => json(`/api/interactive-sync/${session.id}/mapping-episodes`, {...options, method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify({revision:session.revision, selection_version:session.selection_version, edits})}),
    save: edits => post(`/api/interactive-sync/${session.id}/mappings`, {revision:session.revision, selection_version:session.selection_version, edits}),
  };
  const dialog = document.createElement("dialog");
  dialog.className = "is-page is-mapping-dialog";
  dialog.setAttribute("aria-label", "Edit mappings");
  const drafts = rows.map(row => ({row, item: structuredClone(row.item), checked: true, dirty: false}));
  const cache = new Map();
  const catalogs = new Map();
  const episodeMatching = new Map();
  const routeKey = row => JSON.stringify([row.provider, row.instance || "default"]);
  let saving = false, searching = false, closed = false, searchController = null;
  const groups = mappingGroups(rows);
  const first = rows[0];
  const endpoint = row => [row.provider, row.instance !== "default" ? row.instance : ""].filter(Boolean).join(" · ");
  const input = (name, value, type = "text", extra = "") => `<input data-field="${name}" type="${type}" value="${esc(value)}" ${extra}>`;
  const episodeLabel = item => item.type === "episode" ? `S${String(item.season).padStart(2,"0")}E${String(item.episode).padStart(2,"0")}` : item.type;
  const itemLabel = item => [seriesTitle(item), item.type === "episode" ? episodeLabel(item) : item.year].filter(Boolean).join(" · ");
  const renderDraft = index => {
    const {row, item} = drafts[index], episodic = ["episode", "season"].includes(item.type), ids = item.show_ids || item.ids || {};
    return `<article class="is-map-row" data-draft="${index}">
      <div class="is-map-row-preview"><input type="checkbox" data-edit checked aria-label="Select ${esc(itemLabel(item))}">
        <div class="is-map-before"><strong>${esc(itemLabel(item))}</strong><small>${esc(endpoint(row))} · ${esc(row.feature)}</small></div>
        ${icon("arrow_forward")}<div class="is-map-after"><strong data-after>${esc(itemLabel(item))}</strong><span data-draft-status class="is-map-status">Unchanged</span></div>
        <div class="is-map-actions"><button class="is-btn is-map-action" data-edit-row aria-expanded="false" aria-controls="mapping-edit-${index}" aria-label="Edit ${esc(itemLabel(item))}" title="Edit">${icon("edit")}</button><button class="is-btn is-map-action" data-reset hidden aria-label="Undo changes to ${esc(itemLabel(item))}" title="Undo">${icon("undo")}</button></div>
      </div>
      <div class="is-map-row-editor" id="mapping-edit-${index}" hidden><p data-review-reason hidden></p>
        <div class="is-map-edit-fields"><label>Title${input("title", seriesTitle(item))}</label>${episodic ? `<label>Season${input("season", item.season, "number", 'min="0"')}</label>` : ""}${item.type === "episode" ? `<label>Episode${input("episode", item.episode, "number", 'min="1"')}</label>` : ""}</div>
        <button class="is-btn is-map-action" data-find-row hidden>${icon("search")}Find another title</button>
        <details class="is-map-advanced"><summary>Advanced · Manual IDs</summary><div class="is-map-id-inputs">${ID_FIELDS.map(key => `<label>${key.toUpperCase()}${input(key, ids[key] || "")}</label>`).join("")}</div></details>
      </div></article>`;
  };
  dialog.innerHTML = `<header class="is-mapping-head"><div><div class="is-eyebrow">MAPPING</div><h2>Edit mappings</h2><p>Choose the correct title and review your changes.</p></div><button class="is-btn is-map-close" data-close aria-label="Close" title="Close">${icon("close")}</button></header>
    <div class="is-mapping-body">
    <div class="is-map-intro"><span>${rows.length} item${rows.length === 1 ? "" : "s"}${total > rows.length ? ` from ${total} results` : ""}</span><details class="is-map-help"><summary aria-label="About saved mappings" title="About saved mappings">${icon("info")}</summary><p>Saved mappings also apply to future syncs using the same source and profile. Watched dates and ratings are kept. ${standalone ? "Run the pair again to retry with your correction. Saving does not start a sync." : "Saving updates your sync review; it does not start a sync."}</p></details></div>
    <section class="is-map-bulk" aria-label="Choose a title"><div class="is-map-search-heading"><h3>Choose a title</h3><div class="is-map-actions"><button class="is-btn" data-suggest hidden>${icon("auto_fix_high")}Auto match</button><button class="is-btn" data-stop-search hidden>Stop search</button></div></div>
      <div data-chosen hidden class="is-map-chosen"><span>${icon("check_circle")}<strong data-match></strong></span><button class="is-btn is-small" data-change-match>Change match</button></div>
      <div data-search-panel><div class="is-map-search-bar" hidden><label>Search title<input data-search-query value="${esc(seriesTitle(first.item))}" maxlength="200"></label><label>Search in<select data-catalog></select></label><button class="is-btn" data-search>Find matches</button></div><div class="is-map-candidates"></div></div>
      <p data-search-status role="status">Choose a match or try Auto match.</p>
    </section>
    <div class="is-map-row-tools"><strong data-selection-count></strong><button class="is-btn is-small" data-check-all>Select all</button><button class="is-btn is-small" data-check-none>Clear selection</button></div>
    <details class="is-map-numbering" hidden><summary>Adjust episode numbering</summary><div class="is-map-numbering-fields"><label>Season<input data-season type="number" min="0" placeholder="Keep current"></label><label>Shift episode numbers by<input data-offset type="number" value="0"></label><button class="is-btn" data-bulk>Apply numbering</button></div><p>Use 0 to keep episode numbers, or a shift such as −10 to change episode 11 to 1. Applies to selected episodes.</p></details>
    <div class="is-map-review">${groups.map((indices, groupIndex) => indices.length === 1 ? renderDraft(indices[0]) : `<details class="is-map-group" data-group="${groupIndex}"><summary><span><strong>${esc(seriesTitle(rows[indices[0]].item))} · Season ${esc(rows[indices[0]].item.season)}</strong><small>${indices.length} episodes · ${esc(endpoint(rows[indices[0]]))}</small></span><span data-group-after>Review episodes</span></summary><div class="is-map-group-tools"><button class="is-btn is-small" data-select-group="${groupIndex}">Select this season</button><span data-group-count></span></div>${indices.map(renderDraft).join("")}</details>`).join("")}</div>
    </div><footer class="is-map-save"><div><strong data-count>No changes yet</strong><p data-error role="alert"></p></div><button class="is-btn is-primary" data-save disabled>Save mappings</button></footer>`;
  document.body.append(dialog);
  dialog.showModal();
  const $ = selector => dialog.querySelector(selector);
  function updateCatalogs() {
    const row = drafts.find(d => d.checked)?.row || first;
    const options = catalogs.get(routeKey(row)) || [];
    const select = $("[data-catalog]"), previous = select.value;
    select.innerHTML = options.map(option => `<option value="${esc(option.id)}">${esc(option.label)}</option>`).join("");
    if (options.some(option => option.id === previous)) select.value = previous;
    $(".is-map-search-bar").hidden = !options.length;
    $("[data-suggest]").hidden = !options.length;
    $(".is-map-numbering").hidden = !drafts.some(d => d.checked && d.item.type === "episode");
    $("[data-selection-count]").textContent = `${drafts.filter(d => d.checked).length} selected`;
    dialog.querySelectorAll("[data-find-row]").forEach(button => {
      const index = Number(button.closest("[data-draft]").dataset.draft);
      button.hidden = !(catalogs.get(routeKey(drafts[index].row)) || []).length;
    });
    updateGroups();
  }
  async function loadCatalogs() {
    const unique = new Map(rows.map(row => [routeKey(row), row]));
    const results = await Promise.allSettled([...unique].map(async ([key, row]) => {
      const data = await api.catalogs(row);
      catalogs.set(key, data.catalogs || []);
      episodeMatching.set(key, !!data.episode_matching);
    }));
    if (closed) return;
    updateCatalogs();
    if (results.some(result => result.status === "rejected")) status("Could not check available catalogs. Reopen mapping to retry. Manual editing is available.");
    else if (![...catalogs.values()].some(options => options.length)) status("No configured search catalog is available. You can still edit IDs and episode numbers below.");
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
    updateCatalogs();
  }
  function updateGroups() {
    groups.forEach((indices, groupIndex) => {
      const group = $(`[data-group="${groupIndex}"]`);
      if (!group) return;
      const items = indices.map(index => drafts[index]);
      const changed = items.filter(d => d.dirty).length;
      const review = items.filter(d => d.review).length;
      const targets = [...new Set(items.filter(d => d.dirty).map(d => `${seriesTitle(d.item)} · Season ${d.item.season}`))];
      group.querySelector("[data-group-after]").textContent = [targets.length === 1 ? `→ ${targets[0]}` : targets.length ? `${targets.length} corrected titles / seasons` : "Review episodes", review ? `${review} need review` : changed ? `${changed} changed` : ""].filter(Boolean).join(" · ");
      group.querySelector("[data-group-count]").textContent = `${items.filter(d => d.checked).length} of ${items.length} selected`;
    });
  }
  function draftStatus(draft, reason = "") {
    const cell = dialog.querySelector(`[data-draft="${drafts.indexOf(draft)}"] [data-draft-status]`);
    cell.className = `is-map-status${draft.review ? " is-review" : draft.dirty ? " is-edited" : ""}`;
    cell.textContent = draft.review ? "Needs review" : draft.dirty ? "Changed" : "Unchanged";
    cell.title = reason || draft.review || "";
    const explanation = $(`[data-draft="${drafts.indexOf(draft)}"] [data-review-reason]`);
    explanation.textContent = draft.review || "";
    explanation.hidden = !draft.review;
    updateGroups();
  }
  function changed(draft) {
    draft.dirty = JSON.stringify(draft.item) !== JSON.stringify(draft.row.item);
    draftStatus(draft);
    const count = drafts.filter(d => d.dirty).length;
    $("[data-count]").textContent = count ? `${count} change${count === 1 ? "" : "s"} ready to save` : "No changes yet";
    const row = $(`[data-draft="${drafts.indexOf(draft)}"]`);
    row.querySelector("[data-after]").textContent = itemLabel(draft.item);
    row.querySelector("[data-reset]").hidden = !draft.dirty;
    $("[data-save]").disabled = !count || saving || searching;
  }
  function paint(draft) {
    const el = dialog.querySelector(`[data-draft="${drafts.indexOf(draft)}"]`);
    el.querySelectorAll("[data-field]").forEach(field => {
      const key = field.dataset.field;
      field.value = ID_FIELDS.includes(key) ? (draft.item.show_ids || draft.item.ids || {})[key] || "" : key === "title" ? seriesTitle(draft.item) : draft.item[key] ?? "";
    });
    changed(draft);
  }
  function lock(value) {
    dialog.querySelectorAll("button,input,select").forEach(el => { el.disabled = value || el.hasAttribute("data-match-unavailable"); });
    $("[data-stop-search]").hidden = !searching;
    $("[data-stop-search]").disabled = !searching;
    if (!value) $("[data-save]").disabled = !drafts.some(d => d.dirty) || saving || searching;
  }
  async function search(row, term) {
    const catalog = $("[data-catalog]").value;
    if (!(catalogs.get(routeKey(row)) || []).some(option => option.id === catalog)) return null;
    const key = JSON.stringify([row.provider, row.instance, row.item.type === "movie" ? "movie" : "show", term, catalog]);
    if (!cache.has(key)) {
      const data = await api.search(row, term, catalog, {signal:searchController?.signal});
      if (data.results.some(match => match.mapping_unavailable)) return data;
      cache.set(key, data);
    }
    return cache.get(key);
  }
  async function find(row) {
    const term = $("[data-search-query]").value.trim();
    if (term.length < 2) { status("Enter at least two characters."); return; }
    showSearch();
    searchController = new AbortController(); searching = true; lock(true); status(`Searching ${endpoint(row)}...`);
    try {
      const data = await search(row, term);
      if (closed) return;
      if (!data) { status("Search is not available for this destination. Choose another configured catalog."); return; }
      const matches = data.results.slice(0, 10);
      status(`${data.catalog}: ${matches.length} match${matches.length === 1 ? "" : "es"}${data.results.length > matches.length ? " shown. Refine your search for more specific results." : ". Choose a title to update the checked items."}`);
      $(".is-map-candidates").replaceChildren();
      matches.forEach(match => {
        const button = document.createElement("button");
        button.type = "button"; button.className = "is-btn is-map-candidate";
        if (match.mapping_unavailable) {
          button.setAttribute("data-match-unavailable", "");
          button.disabled = true;
        }
        button.title = [match.title, match.year, match.mapping_unavailable].filter(Boolean).join(" · ");
        button.innerHTML = `<strong>${esc(match.title)}</strong>${match.year ? `<span class="is-map-candidate-year">${esc(match.year)}</span>` : ""}`;
        button.onclick = async () => {
          if (match.mapping_unavailable) { status(match.mapping_unavailable); return; }
          const selected = drafts.filter(d => d.checked);
          if (!selected.length) { status("Check the items you want to update first."); return; }
          if (selected.some(d => routeKey(d.row) !== routeKey(row)
            || (d.item.type === "movie" ? "movie" : "show") !== (row.item.type === "movie" ? "movie" : "show"))) {
            status("Choose rows with the same destination and media type for this match.");
            return;
          }
          selected.forEach(draft => {
            draft.review = "";
            draft.item = correctedItem(draft.item, {ids:match.ids, title:match.title});
            paint(draft);
          });
          $("[data-match]").textContent = [match.title, match.year].filter(Boolean).join(" · ");
          $("[data-chosen]").hidden = false;
          $("[data-search-panel]").hidden = true;
          $("[data-change-match]").focus();
          $(".is-map-candidates").querySelectorAll("button").forEach(b => b.setAttribute("aria-pressed", String(b === button)));
          searchController = new AbortController(); searching = true; lock(true);
          status("Checking episode numbering…");
          try {
            const review = await matchEpisodes(selected);
            if (!closed) status(`${selected.length} item${selected.length === 1 ? "" : "s"} updated. ${review ? `${review} need episode review.` : "Review your changes below."}`);
          } catch (error) { if (!closed) status(error.name === "AbortError" ? "Stopped. Your changes are kept; check episode numbers before saving." : `Title updated. Could not check episode numbers: ${error.message}`); }
          finally { searching = false; if (!closed) lock(false); }
        };
        $(".is-map-candidates").append(button);
      });
    } catch (error) { status(error.name === "AbortError" ? "Search stopped. Your drafts are kept." : error.message); }
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
      draft.item = structuredClone(draft.row.item); draft.review = ""; paint(draft);
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
  $("[data-stop-search]").onclick = () => searchController?.abort();
  $("[data-search]").onclick = () => {
    const selected = drafts.find(d => d.checked);
    if (!selected) { status("Select an item to find a match for."); return; }
    find(selected.row);
  };
  $("[data-search-query]").onkeydown = event => { if (event.key === "Enter") { event.preventDefault(); $("[data-search]").click(); } };
  async function matchEpisodes(items) {
    const episodes = items.filter(d => d.item.type === "episode");
    const selected = episodes.filter(d => episodeMatching.get(routeKey(d.row)));
    for (const draft of episodes.filter(d => !selected.includes(d))) {
      draft.review = "Automatic episode matching is unavailable. Check the season and episode.";
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
          draft.review = result?.reason || "No reliable episode match. Check the season and episode.";
          draftStatus(draft);
        }
      }
      return episodes.filter(d => d.review).length;
    } catch (error) {
      for (const draft of selected) {
        draft.review = "Episode numbering could not be checked. Review it before saving.";
        draftStatus(draft);
      }
      throw error;
    }
  }
  $("[data-suggest]").onclick = async () => {
    const selected = drafts.filter(d => d.checked);
    if (!selected.length) { status("Select the items you want to match first."); return; }
    searchController = new AbortController(); searching = true; lock(true);
    const matched = [];
    try {
      for (const draft of selected) {
        if (searchController.signal.aborted) break;
        status(`Finding matches: ${selected.indexOf(draft) + 1} of ${selected.length}…`);
        const data = await search(draft.row, seriesTitle(draft.item));
        if (closed) return;
        const year = ["episode", "season"].includes(draft.item.type) ? draft.item.series_year : draft.item.year;
        const exact = (data?.results || []).filter(row => row.exact_title && (!year || !row.year || Number(year) === Number(row.year)));
        if (exact.length === 1 && !exact[0].mapping_unavailable) {
          draft.review = draft.item.type === "episode" ? "Episode numbering still needs to be checked." : "";
          draft.item = correctedItem(draft.item, {ids:exact[0].ids, title:exact[0].title});
          paint(draft); matched.push(draft);
        } else {
          draft.review = "Choose a title manually; no single reliable match was found.";
          draftStatus(draft);
        }
      }
      if (searchController.signal.aborted) {
        status("Stopped. Your changes are kept; review them before saving.");
        return;
      }
      status("Checking episode numbering…");
      const episodeReview = await matchEpisodes(matched);
      if (!closed) {
        const review = selected.length - matched.length + episodeReview;
        status(`${selected.length - review} matched${review ? ` · ${review} need review` : ""}. Review your changes below.`);
        if (review) showSearch();
        else {
          $("[data-match]").textContent = `${matched.length} item${matched.length === 1 ? "" : "s"} matched`;
          $("[data-search-panel]").hidden = true;
          $("[data-chosen]").hidden = false;
        }
      }
    } catch (error) { if (!closed) status(error.name === "AbortError" ? "Stopped. Your changes are kept; review them before saving." : `Your changes are kept. ${error.message}`); }
    finally { searching = false; if (!closed) lock(false); }
  };
  $("[data-bulk]").onclick = () => {
    const selected = drafts.filter(d => d.checked && d.item.type === "episode");
    const season = $("[data-season]"), offset = $("[data-offset]");
    if (!season.reportValidity() || !offset.reportValidity()) return;
    const corrected = selected.map(d => correctedItem(d.item, {season:season.value, offset:offset.value}));
    if (corrected.some(item => !Number.isInteger(item.episode) || item.episode < 1)) {
      status("This shift would produce an invalid episode number. No items were changed."); return;
    }
    selected.forEach((draft,i) => { draft.item = corrected[i]; draft.review = ""; paint(draft); });
    status(`Episode numbering updated for ${selected.length} items.`);
    offset.value = "0";
    $(".is-map-numbering").open = false;
  };
  for (const [selector, checked] of [["[data-check-all]",true],["[data-check-none]",false]]) $(selector).onclick = () => {
    drafts.forEach(d => { d.checked = checked; });
    selectionChanged();
  };
  function close() {
    if (saving || searching) return;
    if (drafts.some(d => d.dirty) && !window.confirm("Discard unsaved mapping corrections?")) return;
    dismiss();
  }
  function dismiss() {
    if (closed) return;
    closed = true; dialog.close(); dialog.remove(); onClose();
  }
  const warnUnsaved = event => { if (drafts.some(d => d.dirty) && !closed) { event.preventDefault(); event.returnValue = ""; } };
  window.addEventListener("beforeunload", warnUnsaved);
  dialog.addEventListener("close", () => window.removeEventListener("beforeunload", warnUnsaved));
  dialog.addEventListener("cancel", event => { event.preventDefault(); close(); });
  $("[data-close]").onclick = close;
  $("[data-save]").onclick = async () => {
    for (const draft of drafts.filter(d => d.dirty)) {
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
    const edits = drafts.filter(d => d.dirty).map(d => ({row_id:d.row.id, item:d.item, selected:true}));
    if (!edits.length) return;
    saving = true; lock(true); $("[data-error]").textContent = "";
    try {
      const data = await api.save(edits);
      closed = true; dialog.close(); dialog.remove(); onSaved(data);
    } catch (error) { $("[data-error]").textContent = error.message; }
    finally { saving = false; if (!closed) lock(false); }
  };
  return {close, destroy() { searchController?.abort(); dismiss(); }};
}
