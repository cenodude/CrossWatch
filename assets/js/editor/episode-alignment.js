/* assets/js/editor/episode-alignment.js */
/* CrossWatch - Episode numbering review for one show in a History sync pair */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
const esc = value => String(value ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const code = item => `S${String(item.season).padStart(2,"0")}E${String(item.episode).padStart(2,"0")}`;

export function selection(proposal, picks = {}, unchecked = new Set()) {
  const groups = proposal.groups.map((group, index) => ({group, option:group.options[picks[index] ?? group.suggested]}))
    .filter(entry => entry.option).map(({group, option}) => ({name:group.name, source:option.source, target:option.target}));
  const covered = new Set(groups.flatMap(group => group.source));
  const corrections = proposal.corrections.filter(item => !covered.has(item.key) && !unchecked.has(item.key));
  return {groups, corrections, covered};
}

export async function openEpisodeAlignment(trigger, {pairId, showIds, onSaved} = {}) {
  const dialog = document.createElement("dialog");
  dialog.className = "eg-dialog cw-page-panel";
  dialog.setAttribute("aria-labelledby", "episode-alignment-title");
  dialog.innerHTML = `<div class="eg-head"><div><h2 id="episode-alignment-title">Fix episode numbering</h2><p>Compare how both providers number this show and confirm the proposed fixes.</p></div><button type="button" class="eg-close" data-close aria-label="Close numbering review"><span class="material-symbols-rounded" aria-hidden="true">close</span></button></div>
    <div class="eg-body"><p class="eg-status" role="status">Comparing watched episodes…</p><p class="eg-error" role="alert"></p><div data-review></div></div>
    <footer class="eg-footer"><span class="eg-muted">Saving does not start a sync.</span><div><button type="button" data-close>Cancel</button><button type="button" class="eg-primary" data-save disabled>Save fixes</button></div></footer>`;
  document.body.append(dialog);
  const $ = selector => dialog.querySelector(selector);
  const controller = new AbortController();
  const picks = {}, unchecked = new Set();
  let proposal = null, busy = false, changed = false;
  const request = async (path, body) => {
    const response = await fetch(`/api/editor/episode-alignment${path}`, {method:"POST", credentials:"same-origin", cache:"no-store",
      signal:controller.signal, headers:{"Content-Type":"application/json"}, body:JSON.stringify(body)});
    const data = await response.json();
    if (!response.ok) throw new Error(typeof data.detail === "string" ? data.detail : "The numbering review could not be completed.");
    return data;
  };
  const name = side => window.CW?.ProviderMeta?.label?.(proposal[side].provider) || proposal[side].provider;
  const render = () => {
    const sides = {source:new Map(), target:new Map()};
    for (const row of proposal.rows) for (const side of ["source", "target"]) if (row[side]) sides[side].set(row[side].key, row[side]);
    const list = (side, keys) => keys.map(key => code(sides[side].get(key))).join(" + ");
    const chosen = selection(proposal, picks, unchecked);
    const note = {missing_key:"Add a TMDb metadata key in Settings to get suggestions for split episodes, including season finales.",
      no_tmdb_id:"This show has no TMDb ID, so split episodes cannot be suggested.",
      unavailable:"TMDb could not be reached, so split episode suggestions are limited."}[proposal.metadata] || "";
    const groups = proposal.groups.map((group, index) => {
      const pick = picks[index] ?? group.suggested ?? "none";
      const other = group.split === "source" ? "target" : "source";
      const orphan = group.orphans.map(key => code(sides[group.split].get(key))).join(", ");
      const option = (value, title, detail) => `<label class="eg-scrobble"><input type="radio" name="alignment-group-${index}" value="${value}" data-group="${index}" ${pick === value ? "checked" : ""}><span><strong>${esc(title)}</strong><small>${esc(detail)}</small></span></label>`;
      return `<section class="eg-saved"><div class="eg-saved-head"><h3>${esc(name(group.split))} ${esc(orphan)} has no match on ${esc(name(other))}</h3></div>
        ${Object.entries(group.options).map(([where, value]) => option(where,
          `Combine: ${name("source")} ${list("source", value.source)} ↔ ${name("target")} ${list("target", value.target)}`,
          [...value[group.split].map(key => sides[group.split].get(key).title), where === group.suggested ? group.reason : ""].filter(Boolean).join(" · "))).join("")}
        ${option("none", "Leave it alone", "Do not create an episode group here.")}</section>`;
    }).join("");
    const corrections = proposal.corrections.length ? `<section class="eg-saved"><div class="eg-saved-head"><h3>Shifted episodes</h3><span class="eg-muted">${chosen.corrections.length} selected</span></div>${proposal.corrections.map(item => {
      const from = sides.source.get(item.key), covered = chosen.covered.has(item.key);
      return `<label class="eg-scrobble"><input type="checkbox" data-correction="${esc(item.key)}" ${covered ? "disabled" : unchecked.has(item.key) ? "" : "checked"}><span><strong>${esc(name("source"))} ${esc(code(from))} → ${esc(name("target"))} ${esc(code(item))}</strong><small>${esc(covered ? "Handled by the episode group above." : from.title)}</small></span></label>`;
    }).join("")}</section>` : "";
    const all = `<details class="eg-help"><summary>All watched episodes (${proposal.rows.length})</summary><ul>${proposal.rows.map(row =>
      `<li>${esc(row.source ? `${code(row.source)} ${row.source.title}` : "—")} ↔ ${esc(row.target ? `${code(row.target)} ${row.target.title}` : "—")}${row.status === "same" ? "" : ` (${esc(row.status)})`}</li>`).join("")}</ul></details>`;
    $("[data-review]").innerHTML = `${note ? `<p class="eg-hint">${esc(note)}</p>` : ""}${groups}${corrections}${groups || corrections ? "" : '<p class="eg-empty">The watched episodes of this show already line up between both providers.</p>'}${all}`;
    $("[data-save]").disabled = busy || !(chosen.groups.length || chosen.corrections.length);
    $("[data-save]").textContent = chosen.groups.length || chosen.corrections.length ? `Save ${chosen.groups.length + chosen.corrections.length} fixes` : "Save fixes";
  };
  dialog.addEventListener("change", event => {
    const input = event.target;
    if (input.dataset.group != null) picks[input.dataset.group] = input.value;
    else if (input.dataset.correction) input.checked ? unchecked.delete(input.dataset.correction) : unchecked.add(input.dataset.correction);
    else return;
    render();
  });
  $("[data-save]").onclick = async () => {
    if (busy || !proposal) return;
    const chosen = selection(proposal, picks, unchecked);
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
  dialog.showModal();
  try {
    proposal = await request("", {pair_id:pairId, show_ids:showIds});
    $(".eg-status").textContent = "";
    render();
  } catch (error) {
    if (!controller.signal.aborted) { $(".eg-status").textContent = ""; $(".eg-error").textContent = error.message; }
  }
}
