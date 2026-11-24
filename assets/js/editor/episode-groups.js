/* assets/js/editor/episode-groups.js */
/* CrossWatch - Pair-specific History episode group editor */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
const I18N = () => globalThis.window?.CW?.I18n || globalThis.window?.__cwI18n;
const T = (key, vars) => I18N()?.t?.(key, vars) ?? key;
const TH = (key, vars) => I18N()?.html?.(key, vars) ?? key;
const esc = value => String(value ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const coordinates = items => items.map(item => `S${String(item.season).padStart(2,"0")}E${String(item.episode).padStart(2,"0")}`).join(", ");
const namespaces = ["tmdb", "tvdb", "imdb", "trakt", "simkl"];

export function parseEpisodes(value, showIds, title) {
  const parts = value.trim().split(/[\s,;]+/).filter(Boolean);
  if (!parts.length || parts.length > 20) throw new Error(T("editor.groups.between"));
  return parts.map(part => {
    const match = /^S(\d{1,3})E(\d{1,5})$/i.exec(part);
    if (!match || Number(match[2]) < 1) throw new Error(T("editor.groups.coords"));
    return {type:"episode", title, show_ids:{...showIds}, season:Number(match[1]), episode:Number(match[2])};
  });
}

export function episodeGroupAction(row, pairId, onSaved) {
  if (row?.feature !== "history" || row?.item?.type !== "episode" || !pairId) return undefined;
  return trigger => openEpisodeGroups(trigger, {pairId, original:row.item, provider:String(row.source || "").toUpperCase(),
    instance:row.source_instance || "default", returnToMapping:true, onSaved});
}

export async function openEpisodeGroups(trigger, {pairId = "", groupId = "", original, provider, instance = "default", onSaved, returnToMapping = false} = {}) {
  const dialog = document.createElement("dialog");
  dialog.className = "eg-dialog cw-page-panel";
  dialog.setAttribute("aria-labelledby", "episode-groups-title");
  dialog.innerHTML = `<div class="eg-head"><div><div class="eg-eyebrow">${TH("editor.groups.eyebrow")}</div><h2 id="episode-groups-title">${TH("editor.groups.title")}</h2><p>${TH("editor.groups.sub")}</p></div><button type="button" class="eg-close" data-close aria-label="${TH("editor.groups.close")}"><span class="material-symbols-rounded" aria-hidden="true">close</span></button></div>
    <div class="eg-body">${returnToMapping ? '<button type="button" class="eg-back" data-back><span class="material-symbols-rounded" aria-hidden="true">arrow_back</span>' + TH("editor.groups.back") + '</button>' : ""}<div class="eg-toolbar"><label class="eg-field">${TH("editor.groups.pair")}<select data-pair aria-label="${TH("editor.groups.pair")}"></select></label><button type="button" data-align title="${TH("editor.groups.compareTitle")}"><span class="material-symbols-rounded" aria-hidden="true">compare</span>${TH("editor.groups.check")}</button><button type="button" data-new><span class="material-symbols-rounded" aria-hidden="true">add</span>${TH("editor.groups.new")}</button></div>
    <form id="episode-group-form" data-form><label class="eg-field">${TH("editor.groups.name")}<input data-name required maxlength="200" placeholder="${TH("editor.groups.namePlaceholder")}"></label>
    <div class="eg-sides">${["source","target"].map(side => `<section class="eg-provider" data-side="${side}" aria-label="${TH(`editor.groups.side.${side}.episodes`)}"><div class="eg-provider-head"><span class="eg-provider-logo" data-logo aria-hidden="true"></span><div><strong data-endpoint></strong><span class="eg-muted" data-instance></span></div></div><div class="eg-identity"><label class="eg-field">${TH("editor.groups.idType")}<select data-namespace aria-label="${TH(`editor.groups.side.${side}.idType`)}">${namespaces.map(ns=>`<option value="${ns}">${ns.toUpperCase()}</option>`).join("")}</select></label><label class="eg-field">${TH("editor.groups.showId")}<input data-show-id required maxlength="128" placeholder="${TH("editor.groups.showIdPlaceholder")}" aria-label="${TH(`editor.groups.side.${side}.showId`)}"></label></div><label class="eg-field">${TH("editor.groups.episodes")}<input data-episodes required placeholder="${side === "source" ? "S04E23, S04E24" : "S04E23"}" aria-label="${TH(`editor.groups.side.${side}.episodes`)}"></label></section>`).join('<span class="material-symbols-rounded eg-link" aria-hidden="true">compare_arrows</span>')}</div>
    <p class="eg-hint">${TH("editor.groups.hint")}</p>
    <label class="eg-scrobble"><input type="checkbox" data-scrobble><span><strong>${TH("editor.groups.alsoScrobble")}</strong><small>${TH("editor.groups.alsoScrobbleSub")}</small></span></label>
    <details class="eg-help"><summary>${TH("editor.groups.how")}</summary><ul><li>${TH("editor.groups.help1")}</li><li>${TH("editor.groups.help2")}</li><li>${TH("editor.groups.help3")}</li><li>${TH("editor.groups.help4")}</li></ul></details></form>
    <p class="eg-error" role="alert"></p><p class="eg-status" role="status"></p>
    <section class="eg-saved"><div class="eg-saved-head"><h3>${TH("editor.groups.saved")}</h3><span class="eg-muted" data-count>${TH("editor.groups.loading")}</span></div><div data-groups></div></section></div>
    <footer class="eg-footer"><div><button type="button" data-cancel-edit>${TH("editor.groups.reset")}</button><button type="submit" form="episode-group-form" class="eg-primary" data-save>${TH("editor.groups.saveGroup")}</button></div></footer>`;
  document.body.append(dialog);
  const $ = selector => dialog.querySelector(selector);
  let pairs = [], editingId = "", busy = false, changed = false;
  const controller = new AbortController();
  const request = async body => {
    const response = await fetch("/api/editor/episode-groups", {credentials:"same-origin", cache:"no-store", signal:controller.signal,
      ...(body ? {method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify(body)} : {})});
    const data = await response.json();
    if (!response.ok) throw new Error(typeof data.detail === "string" ? data.detail : T("editor.groups.checkIds"));
    return data;
  };
  const selectedPair = () => pairs.find(pair => pair.id === $("[data-pair]").value);
  const pairOption = value => {
    const pair = pairs.find(pair => pair.id === value);
    return pair ? window.CW?.syncPairOptionData?.({source:pair.source.provider, source_instance:pair.source.instance,
      target:pair.target.provider, target_instance:pair.target.instance, mode:pair.mode}) || null : null;
  };
  const reset = group => {
    editingId = group?.id || "";
    $("[data-name]").value = group?.name || "";
    $("[data-scrobble]").checked = group?.scrobble === true;
    $("[data-save]").textContent = group ? T("editor.groups.saveChanges") : T("editor.groups.saveGroup");
    for (const side of ["source", "target"]) {
      const section = $(`[data-side="${side}"]`), endpoint = selectedPair()?.[side];
      const episode = group?.[side]?.episodes?.[0];
      const ids = episode?.show_ids || {};
      section.querySelector("[data-endpoint]").textContent = window.CW?.ProviderMeta?.label?.(endpoint?.provider) || endpoint?.provider || side;
      section.querySelector("[data-instance]").textContent = endpoint?.instance === "default" ? T("editor.groups.defaultInstance") : endpoint?.instance || "";
      const logo = section.querySelector("[data-logo]");
      logo.replaceChildren();
      const src = endpoint && window.CW?.ProviderMeta?.logoPath?.(endpoint.provider);
      if (src) {
        const image = document.createElement("img");
        image.src = src; image.alt = "";
        logo.append(image);
      } else {
        logo.innerHTML = '<span class="material-symbols-rounded">tv</span>';
      }
      const ns = namespaces.find(key => ids[key]) || "tmdb";
      section.querySelector("[data-namespace]").value = ns;
      section.querySelector("[data-show-id]").value = ids[ns] || "";
      section.querySelector("[data-episodes]").value = group ? coordinates(group[side].episodes) : "";
    }
  };
  const render = () => {
    const pair = selectedPair(), groups = pair?.groups || [];
    $("[data-form]").hidden = !pair;
    $("[data-new]").disabled = $("[data-align]").disabled = !pair;
    $("[data-pair]").disabled = !pairs.length;
    $("[data-count]").textContent = pair ? T("editor.groups.count", {count: groups.length}) : "";
    $("[data-save]").disabled = $("[data-cancel-edit]").disabled = !pair;
    $("[data-groups]").innerHTML = groups.length ? groups.map((group,index)=>`<article class="eg-saved-group"><div class="eg-saved-heading"><strong>${esc(group.name)}</strong><span class="eg-scrobble-state">${TH(group.scrobble ? "editor.groups.scrobblesOn" : "editor.groups.scrobblesOff")}</span></div><div class="eg-saved-mapping">${["source","target"].map(side=>`<span><b>${esc(window.CW?.ProviderMeta?.label?.(group[side].provider) || group[side].provider)}</b> ${esc(coordinates(group[side].episodes))}</span>`).join('<span aria-hidden="true">↔</span>')}</div><div class="eg-saved-actions"><button type="button" data-edit="${index}" aria-label="${TH("editor.groups.editLabel", {name: group.name})}">${TH("editor.groups.edit")}</button><button type="button" class="eg-delete" data-delete="${index}" aria-label="${TH("editor.groups.deleteLabel", {name: group.name})}"><span class="material-symbols-rounded" aria-hidden="true">delete</span></button></div></article>`).join("") : `<p class="eg-empty">${pair ? T("editor.groups.none") : provider ? T("editor.groups.noPairsProvider") : T("editor.groups.noPairs")}</p>`;
  };
  const load = async () => {
    const selected = $("[data-pair]").value || pairId;
    pairs = (await request()).pairs.filter(pair => !provider || ["source", "target"].some(side =>
      pair[side].provider === provider && pair[side].instance === instance));
    $("[data-pair]").innerHTML = pairs.map(pair=>`<option value="${esc(pair.id)}">${esc(pair.name)}</option>`).join("");
    if (pairs.some(pair=>pair.id === selected)) $("[data-pair]").value = selected;
    window.CW?.IconSelect?.enhance?.($("[data-pair]"), {className:"cw-plain-select", getOptionData:pairOption});
    render(); reset();
  };
  const mutate = async body => {
    busy = true;
    $(".eg-error").textContent = "";
    const controls = [...dialog.querySelectorAll("button,input,select")];
    controls.forEach(control => { control.disabled = true; });
    try {
      const warnings = (await request(body)).warnings || []; changed = true;
      await load();
      $(".eg-status").textContent = body.action === "delete" ? T("editor.groups.deleted") : warnings.length ? T("editor.groups.savedCheck") : T("editor.groups.savedOk");
      $(".eg-error").textContent = warnings.join(" ");
    } catch (error) { if (!controller.signal.aborted) $(".eg-error").textContent = error.message; }
    finally { busy = false; controls.forEach(control => { control.disabled = false; }); render(); }
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
        throw new Error(T("editor.groups.oneCombined"));
      }
      mutate({pair_id:pair.id, group});
    } catch (error) { $(".eg-error").textContent = error.message; }
  };
  $("[data-align]").onclick = async () => {
    if (busy) return;
    const pair = selectedPair();
    const section = ["source","target"].map(side=>$(`[data-side="${side}"]`)).find(part=>part.querySelector("[data-show-id]").value.trim());
    if (!pair || !section) { $(".eg-error").textContent = T("editor.groups.showIdFirst"); return; }
    $(".eg-error").textContent = "";
    try {
      const {openEpisodeAlignment} = await import(`/assets/js/editor/episode-alignment.js?v=${encodeURIComponent(window.APP_VERSION || "1")}`);
      await openEpisodeAlignment($("[data-align]"), {pairId:pair.id,
        showIds:{[section.querySelector("[data-namespace]").value]:section.querySelector("[data-show-id]").value.trim()},
        onSaved:async () => { changed = true; await load(); $(".eg-status").textContent = T("editor.groups.fixesSaved"); }});
    } catch (error) { $(".eg-error").textContent = error.message; }
  };
  $("[data-pair]").onchange = () => { render(); reset(); };
  $("[data-new]").onclick = $("[data-cancel-edit]").onclick = () => {
    reset(); $("[data-name]").focus();
  };
  $("[data-groups]").onclick = async event => {
    if (busy) return;
    const edit = event.target.closest("[data-edit]"), remove = event.target.closest("[data-delete]");
    if (edit) { reset(selectedPair().groups[Number(edit.dataset.edit)]); $("[data-name]").focus(); }
    if (remove) {
      const group = selectedPair().groups[Number(remove.dataset.delete)];
      if ((await (window.CW?.MobileUI?.ask || window.confirm)(T("editor.groups.confirmDelete", {name: group.name})))) {
        mutate({pair_id:selectedPair().id, action:"delete", group_id:group.id});
      }
    }
  };
  const close = () => { if (!busy) dialog.close(); };
  $("[data-close]").onclick = close;
  if (returnToMapping) $("[data-back]").onclick = close;
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
  window.CW?.MobileUI?.openDialog(dialog) || dialog.showModal();
  try {
    await load();
    if (groupId) reset(selectedPair()?.groups.find(group => group.id === groupId));
    if (original?.type === "episode") {
      const pair = selectedPair(), side = ["source","target"].find(key=>pair?.[key].provider === provider && pair[key].instance === instance);
      if (side) {
        $("[data-name]").value = original.series_title || original.title || "";
        const section = $(`[data-side="${side}"]`), ids = original.show_ids || original.ids || {};
        const ns = namespaces.find(key=>ids[key]);
        if (ns) {
          for (const part of ["source","target"].map(key=>$(`[data-side="${key}"]`))) {
            part.querySelector("[data-namespace]").value = ns; part.querySelector("[data-show-id]").value = ids[ns];
          }
        }
        section.querySelector("[data-episodes]").value = coordinates([original]);
      }
    }
  } catch (error) { if (!controller.signal.aborted) $(".eg-error").textContent = error.message; }
}
