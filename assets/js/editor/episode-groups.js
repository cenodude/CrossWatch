/* assets/js/editor/episode-groups.js */
/* CrossWatch - Pair-specific History episode group editor */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
const esc = value => String(value ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const coordinates = items => items.map(item => `S${String(item.season).padStart(2,"0")}E${String(item.episode).padStart(2,"0")}`).join(", ");
const namespaces = ["tmdb", "tvdb", "imdb", "trakt", "simkl"];

export function parseEpisodes(value, showIds, title) {
  const parts = value.trim().split(/[\s,;]+/).filter(Boolean);
  if (!parts.length || parts.length > 20) throw new Error("Enter between 1 and 20 episodes per side.");
  return parts.map(part => {
    const match = /^S(\d{1,3})E(\d{1,5})$/i.exec(part);
    if (!match || Number(match[2]) < 1) throw new Error("Use episode coordinates such as S04E23, S04E24.");
    return {type:"episode", title, show_ids:{...showIds}, season:Number(match[1]), episode:Number(match[2])};
  });
}

export async function openEpisodeGroups(trigger, {pairId = "", groupId = "", original, provider, instance = "default", onSaved} = {}) {
  if (!document.getElementById("saved-mappings-style")) {
    const style = document.createElement("link");
    style.id = "saved-mappings-style"; style.rel = "stylesheet";
    style.href = `/assets/css/saved-mappings.css?v=${encodeURIComponent(window.APP_VERSION || "1")}`;
    document.head.append(style);
  }
  const dialog = document.createElement("dialog");
  dialog.className = "sm-dialog";
  dialog.setAttribute("aria-labelledby", "episode-groups-title");
  dialog.innerHTML = `<div class="sm-head"><div><h2 id="episode-groups-title">History episode groups</h2><p>Link a combined episode to its separate parts.</p></div><button type="button" data-close aria-label="Close episode groups">Close</button></div>
    <div class="sm-results"><div class="sm-filters"><label>Sync pair<select data-pair aria-label="History sync pair"></select></label><button type="button" data-new>New group</button></div>
    <p class="sm-note">All separate parts must be watched before the combined episode is marked watched. In a two-way pair, a watched combined episode marks all parts watched. Saving does not start a sync.</p>
    <p class="sm-note">Grouped rewatches are unsupported. Unwatched History changes pause the group until you resolve its watched states on the providers.</p>
    <form data-form><div class="sm-filters"><label>Group name<input data-name required maxlength="200" placeholder="Friends — Season 4 finale"></label></div>
    ${["source","target"].map(side => `<section data-side="${side}"><div class="sm-filters"><strong data-endpoint></strong><label>Show ID type<select data-namespace aria-label="${side} show ID type">${namespaces.map(ns=>`<option value="${ns}">${ns.toUpperCase()}</option>`).join("")}</select></label><label>Show ID<input data-show-id required maxlength="128" aria-label="${side} show ID"></label><label>Episodes<input data-episodes required placeholder="S04E23, S04E24" aria-label="${side} episodes"></label></div></section>`).join("")}
    <p class="sm-note">Use show IDs, not episode IDs. Enter one episode on one side and two or more on the other. Later numbering shifts still use ordinary episode corrections.</p>
    <p class="sm-note"><label><input type="checkbox" data-scrobble> Also use this group for completed Watcher and webhook scrobbles</label></p>
    <p class="sm-note">Applies to matching provider instances and profile. Grouped start/pause updates stay local. All parts must complete after enabling the group; earlier watches are handled by History sync. Each destination episode is sent once. Ordinary corrections still apply only to sync.</p>
    <div class="sm-filters"><button type="submit" data-save>Save group</button><button type="button" data-cancel-edit>Clear form</button></div></form>
    <p class="sm-edit-error" role="alert"></p><p class="sm-action-status" role="status"></p><div data-groups></div></div>
    <footer><span data-count>Loading…</span></footer>`;
  document.body.append(dialog);
  const $ = selector => dialog.querySelector(selector);
  let pairs = [], editingId = "", busy = false, changed = false;
  const controller = new AbortController();
  const request = async body => {
    const response = await fetch("/api/editor/episode-groups", {credentials:"same-origin", cache:"no-store", signal:controller.signal,
      ...(body ? {method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify(body)} : {})});
    const data = await response.json();
    if (!response.ok) throw new Error(typeof data.detail === "string" ? data.detail : "Check the group identifiers and episode coordinates.");
    return data;
  };
  const selectedPair = () => pairs.find(pair => pair.id === $("[data-pair]").value);
  const reset = group => {
    editingId = group?.id || "";
    $("[data-name]").value = group?.name || "";
    $("[data-scrobble]").checked = group?.scrobble === true;
    $("[data-save]").textContent = group ? "Save changes" : "Save group";
    for (const side of ["source", "target"]) {
      const section = $(`[data-side="${side}"]`), endpoint = selectedPair()?.[side];
      const episode = group?.[side]?.episodes?.[0];
      const ids = episode?.show_ids || {};
      section.querySelector("[data-endpoint]").textContent = endpoint ? `${endpoint.provider} · ${endpoint.instance}` : side;
      const ns = namespaces.find(key => ids[key]) || "tmdb";
      section.querySelector("[data-namespace]").value = ns;
      section.querySelector("[data-show-id]").value = ids[ns] || "";
      section.querySelector("[data-episodes]").value = group ? coordinates(group[side].episodes) : "";
    }
  };
  const render = () => {
    const pair = selectedPair(), groups = pair?.groups || [];
    $("[data-form]").hidden = !pair;
    $("[data-new]").disabled = !pair;
    $("[data-count]").textContent = pair ? `${groups.length} episode groups` : "No accessible History sync pairs. Configure a pair with History enabled first.";
    $("[data-groups]").innerHTML = groups.length ? groups.map((group,index)=>`<article class="sm-note"><strong>${esc(group.name)}</strong><p>Completed scrobbles: ${group.scrobble ? "On" : "Off"}</p>${["source","target"].map(side=>`<p>${esc(group[side].provider)}: ${esc(coordinates(group[side].episodes))}</p>`).join("")}<button type="button" data-edit="${index}">Edit</button> <button type="button" data-delete="${index}">Delete</button></article>`).join("") : '<p class="sm-note">No episode groups saved for this pair.</p>';
  };
  const load = async () => {
    const selected = $("[data-pair]").value || pairId;
    pairs = (await request()).pairs;
    $("[data-pair]").innerHTML = pairs.map(pair=>`<option value="${esc(pair.id)}">${esc(pair.name)}</option>`).join("");
    if (pairs.some(pair=>pair.id === selected)) $("[data-pair]").value = selected;
    render(); reset();
  };
  const mutate = async body => {
    busy = true;
    $(".sm-edit-error").textContent = "";
    const controls = [...dialog.querySelectorAll("button,input,select")];
    controls.forEach(control => { control.disabled = true; });
    try {
      await request(body); changed = true;
      await load();
      $(".sm-action-status").textContent = body.action === "delete" ? "Group deleted." : "Group saved. Refresh open History reviews. Enabled scrobble rules apply to new playback events.";
    } catch (error) { if (!controller.signal.aborted) $(".sm-edit-error").textContent = error.message; }
    finally { busy = false; controls.forEach(control => { control.disabled = false; }); }
  };
  $("[data-form]").onsubmit = event => {
    event.preventDefault();
    if (busy) return;
    try {
      const pair = selectedPair(), name = $("[data-name]").value.trim();
      const group = {id:editingId || Array.from(crypto.getRandomValues(new Uint8Array(16)), byte=>byte.toString(16).padStart(2,"0")).join(""), name, scrobble:$("[data-scrobble]").checked};
      for (const side of ["source","target"]) {
        const section = $(`[data-side="${side}"]`);
        const ns = section.querySelector("[data-namespace]").value, id = section.querySelector("[data-show-id]").value.trim();
        const previous = pair.groups.find(saved=>saved.id === editingId)?.[side].episodes[0].show_ids || {};
        const ids = previous[ns] === id ? {...previous} : {[ns]:id};
        group[side] = {...pair[side], episodes:parseEpisodes(section.querySelector("[data-episodes]").value, ids, name)};
      }
      if (Math.min(group.source.episodes.length, group.target.episodes.length) !== 1 || Math.max(group.source.episodes.length, group.target.episodes.length) < 2) {
        throw new Error("Enter one combined episode on one side and two or more separate episodes on the other.");
      }
      mutate({pair_id:pair.id, group});
    } catch (error) { $(".sm-edit-error").textContent = error.message; }
  };
  $("[data-pair]").onchange = () => { render(); reset(); };
  $("[data-new]").onclick = $("[data-cancel-edit]").onclick = () => reset();
  $("[data-groups]").onclick = event => {
    if (busy) return;
    const edit = event.target.closest("[data-edit]"), remove = event.target.closest("[data-delete]");
    if (edit) reset(selectedPair().groups[Number(edit.dataset.edit)]);
    if (remove) {
      const group = selectedPair().groups[Number(remove.dataset.delete)];
      if (window.confirm(`Delete episode group “${group.name}”? These episodes will use ordinary matching again.`)) {
        mutate({pair_id:selectedPair().id, action:"delete", group_id:group.id});
      }
    }
  };
  const close = () => { if (!busy) dialog.close(); };
  $("[data-close]").onclick = close;
  dialog.addEventListener("cancel", event => { if (busy) event.preventDefault(); });
  const navigation = () => { controller.abort(); dialog.close(); };
  for (const name of ["cw:overview-profile-changed", "auth-changed", "cw:auth-state-changed"]) window.addEventListener(name, navigation);
  document.addEventListener("tab-changed", navigation);
  dialog.addEventListener("close", () => {
    controller.abort(); dialog.remove(); trigger?.focus();
    for (const name of ["cw:overview-profile-changed", "auth-changed", "cw:auth-state-changed"]) window.removeEventListener(name, navigation);
    document.removeEventListener("tab-changed", navigation);
    if (changed) onSaved?.();
  }, {once:true});
  dialog.showModal();
  try {
    await load();
    if (groupId) reset(selectedPair()?.groups.find(group => group.id === groupId));
    if (original?.type === "episode") {
      const pair = selectedPair(), side = ["source","target"].find(key=>pair?.[key].provider === provider && pair[key].instance === instance);
      if (side) {
        $("[data-name]").value = original.series_title || original.title || "";
        const section = $(`[data-side="${side}"]`), ids = original.show_ids || original.ids || {};
        const ns = namespaces.find(key=>ids[key]);
        if (ns) { section.querySelector("[data-namespace]").value = ns; section.querySelector("[data-show-id]").value = ids[ns]; }
        section.querySelector("[data-episodes]").value = coordinates([original]);
      }
    }
  } catch (error) { if (!controller.signal.aborted) $(".sm-edit-error").textContent = error.message; }
}
