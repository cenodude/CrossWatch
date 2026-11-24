/* assets/js/editor/episode-alignment.js */
/* CrossWatch - Episode matching review for one show in a History sync pair */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
const I18N = () => globalThis.window?.CW?.I18n || globalThis.window?.__cwI18n;
const T = (key, vars) => I18N()?.t?.(key, vars) ?? key;
const TH = (key, vars) => I18N()?.html?.(key, vars) ?? key;
const esc = value => String(value ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const code = item => `S${String(item.season).padStart(2,"0")}E${String(item.episode).padStart(2,"0")}`;

export function selection(proposal, picks = {}, unchecked = new Set(), confirmed = new Set()) {
  const groups = proposal.groups.map((group, index) => ({group, option:group.options[picks[index] ?? group.suggested]}))
    .filter(entry => entry.option).map(({group, option}) => ({name:group.name, source:option.source, target:option.target}));
  const covered = new Set(groups.flatMap(group => group.source));
  const corrections = proposal.corrections.filter(item => !covered.has(item.key) && !unchecked.has(item.key)
    && (!item.doubt || confirmed.has(item.key))).map(({key, season, episode, show_ids}) => show_ids ? {key, season, episode, show_ids} : {key, season, episode});
  return {groups, corrections, covered};
}

export async function openEpisodeAlignment(trigger, {pairId, showIds, onSaved} = {}) {
  const dialog = document.createElement("dialog");
  dialog.className = "eg-dialog cw-page-panel";
  dialog.setAttribute("aria-labelledby", "episode-alignment-title");
  dialog.innerHTML = `<div class="eg-head"><div><h2 id="episode-alignment-title">${TH("editor.align.title")}</h2><p>${TH("editor.align.sub")}</p></div><button type="button" class="eg-close" data-close aria-label="${TH("editor.align.close")}"><span class="material-symbols-rounded" aria-hidden="true">close</span></button></div>
    <div class="eg-body"><p class="eg-status" role="status">${TH("editor.align.comparing")}</p><p class="eg-error" role="alert"></p><div data-review></div></div>
    <footer class="eg-footer"><div><button type="button" data-close>${TH("editor.align.cancel")}</button><button type="button" class="eg-primary" data-save disabled>${TH("editor.align.saveFixes")}</button></div></footer>`;
  document.body.append(dialog);
  const $ = selector => dialog.querySelector(selector);
  const controller = new AbortController();
  const picks = {}, unchecked = new Set(), confirmed = new Set();
  let proposal = null, busy = false, changed = false;
  const request = async (path, body) => {
    const response = await fetch(`/api/editor/episode-alignment${path}`, {method:"POST", credentials:"same-origin", cache:"no-store",
      signal:controller.signal, headers:{"Content-Type":"application/json"}, body:JSON.stringify(body)});
    const data = await response.json();
    if (!response.ok) throw new Error(typeof data.detail === "string" ? data.detail : T("editor.align.failed"));
    return data;
  };
  const name = side => window.CW?.ProviderMeta?.label?.(proposal[side].provider) || proposal[side].provider;
  const render = () => {
    const sides = {source:new Map(), target:new Map()};
    for (const row of proposal.rows) for (const side of ["source", "target"]) if (row[side]) sides[side].set(row[side].key, row[side]);
    const list = (side, keys) => keys.map(key => code(sides[side].get(key))).join(" + ");
    const chosen = selection(proposal, picks, unchecked, confirmed);
    const matches = new Map(proposal.rows.filter(row => row.source && row.target).map(row => [row.source.key, row.target]));
    const note = {missing_key:T("editor.align.missingKey"),
      no_tmdb_id:T("editor.align.noTmdb"),
      unavailable:T("editor.align.unavailable")}[proposal.metadata] || "";
    const season = number => T("editor.align.season", {n: String(number)});
    const finding = proposal.diagnosis === "layout"
      ? T("editor.align.layout", {parts: proposal.layout.map(part => T("editor.align.layoutPart", {source: name("source"), sourceSeason: season(part.source_season), target: name("target"), targetSeason: season(part.target_season), count: part.episodes})).join("; ")})
      : proposal.diagnosis === "numbering" ? T("editor.align.numbering") : "";
    const checked = (proposal.sources || []).map(source => source.ok
      ? `${source.label}: ${source.title || T("editor.align.found")}${source.seasons.length ? `, ${T("editor.align.seasons", {count: source.seasons.length, episodes: source.seasons.map(part => part[1]).join(", ")})}` : ""}`
      : `${source.label}: ${T("editor.align.notFound")}`).join(" · ");
    const groups = proposal.groups.map((group, index) => {
      const pick = picks[index] ?? group.suggested ?? "none";
      const other = group.split === "source" ? "target" : "source";
      const orphan = group.orphans.map(key => code(sides[group.split].get(key))).join(", ");
      const option = (value, title, detail) => `<label class="eg-scrobble"><input type="radio" name="alignment-group-${index}" value="${value}" data-group="${index}" ${pick === value ? "checked" : ""}><span><strong>${esc(title)}</strong><small>${esc(detail)}</small></span></label>`;
      return `<section class="eg-saved"><div class="eg-saved-head"><h3>${TH("editor.align.noMatch", {name: name(group.split), episodes: orphan, other: name(other)})}</h3></div>
        ${Object.entries(group.options).map(([where, value]) => option(where,
          T("editor.align.combine", {source: name("source"), sourceList: list("source", value.source), target: name("target"), targetList: list("target", value.target)}),
          [...value[group.split].map(key => sides[group.split].get(key).title), where === group.suggested ? group.reason : ""].filter(Boolean).join(" · "))).join("")}
        ${option("none", T("editor.align.leave"), T("editor.align.noGroup"))}</section>`;
    }).join("");
    const corrections = proposal.corrections.length ? `<section class="eg-saved"><div class="eg-saved-head"><h3>${TH("editor.align.shifted")}</h3><span class="eg-muted">${TH("editor.align.selected", {count: chosen.corrections.length})}</span></div>${proposal.corrections.map(item => {
      const from = sides.source.get(item.key), covered = chosen.covered.has(item.key);
      const ticked = item.doubt ? confirmed.has(item.key) : !unchecked.has(item.key);
      const detail = covered ? T("editor.align.handled") : item.reason ? `${item.reason} ${T("editor.align.sameOnly")}` : item.doubt
        ? T("editor.align.titlesDiffer", {from: from.title, source: name("source"), to: matches.get(item.key)?.title || "", target: name("target")})
        : from.title;
      return `<label class="eg-scrobble"><input type="checkbox" data-correction="${esc(item.key)}" ${item.doubt ? "data-doubt" : ""} ${covered ? "disabled" : ticked ? "checked" : ""}><span><strong>${esc(name("source"))} ${esc(code(from))} → ${esc(name("target"))} ${esc(code(item))}${item.doubt ? ` · ${TH("editor.align.checkThis")}` : ""}</strong><small>${esc(detail)}</small></span></label>`;
    }).join("")}</section>` : "";
    const all = `<details class="eg-help"><summary>${TH("editor.align.allWatched", {count: proposal.rows.length})}</summary><ul>${proposal.rows.map(row =>
      `<li>${esc(row.source ? `${code(row.source)} ${row.source.title}` : "—")} ↔ ${esc(row.target ? `${code(row.target)} ${row.target.title}` : "—")}${row.status === "same" ? "" : ` (${esc(row.status)})`}</li>`).join("")}</ul></details>`;
    $("[data-review]").innerHTML = `${checked ? `<p class="eg-hint">${TH("editor.align.checked", {what: checked})}</p>` : ""}${finding ? `<p class="eg-empty">${esc(finding)}</p>` : ""}${note && proposal.diagnosis !== "layout" ? `<p class="eg-hint">${esc(note)}</p>` : ""}${groups}${corrections}${groups || corrections || finding ? "" : '<p class="eg-empty">' + TH("editor.align.lineUp") + '</p>'}${all}`;
    $("[data-save]").disabled = busy || !(chosen.groups.length || chosen.corrections.length);
    $("[data-save]").textContent = chosen.groups.length || chosen.corrections.length ? T("editor.align.saveN", {count: chosen.groups.length + chosen.corrections.length}) : T("editor.align.saveFixes");
  };
  dialog.addEventListener("change", event => {
    const input = event.target;
    if (input.dataset.group != null) picks[input.dataset.group] = input.value;
    else if (input.dataset.correction && input.dataset.doubt != null) input.checked ? confirmed.add(input.dataset.correction) : confirmed.delete(input.dataset.correction);
    else if (input.dataset.correction) input.checked ? unchecked.delete(input.dataset.correction) : unchecked.add(input.dataset.correction);
    else return;
    render();
  });
  $("[data-save]").onclick = async () => {
    if (busy || !proposal) return;
    const chosen = selection(proposal, picks, unchecked, confirmed);
    busy = true; $("[data-save]").disabled = true; $(".eg-error").textContent = "";
    try {
      await request("/apply", {pair_id:pairId, show_ids:showIds, groups:chosen.groups, corrections:chosen.corrections});
      changed = true; busy = false; dialog.close();
    } catch (error) {
      busy = false;
      if (!controller.signal.aborted) { $(".eg-error").textContent = error.message; render(); }
    }
  };
  for (const button of dialog.querySelectorAll("[data-close]")) button.onclick = () => { if (!busy) dialog.close(); };
  dialog.addEventListener("cancel", event => { if (busy) event.preventDefault(); });
  dialog.addEventListener("close", () => {
    controller.abort(); dialog.remove(); trigger?.focus();
    if (changed) onSaved?.();
  }, {once:true});
  window.CW?.MobileUI?.openDialog(dialog) || dialog.showModal();
  try {
    proposal = await request("", {pair_id:pairId, show_ids:showIds});
    $(".eg-status").textContent = "";
    render();
  } catch (error) {
    if (!controller.signal.aborted) { $(".eg-status").textContent = ""; $(".eg-error").textContent = error.message; }
  }
}
