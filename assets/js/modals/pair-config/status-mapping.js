/* assets/js/modals/pair-config/status-mapping.js */
/* CrossWatch - Inline pair status mapping draft controls. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const esc = value => String(value ?? "").replace(/[&<>"']/g, char => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[char]));
const clone = value => JSON.parse(JSON.stringify(value));
const meta = (state, provider) => state.providers?.find(row => row.name.toLowerCase() === String(provider).toLowerCase());
const endpoint = (state, side) => ({provider:String(state[side] || "").toLowerCase(), instance:String(state[`${side}_instance`] || "default")});
const twoWay = state => state.mode === "two-way" || state.twoWay === true;

export function hasStatusMapping(state) {
  return [state.src, state.dst].every(provider => !!meta(state, provider)?.capabilities?.status_mapping);
}

export function statusMappingPayload(state) {
  const saved = state.statusMapping;
  const matches = (value, expected) => value?.provider === expected.provider && value?.instance === expected.instance;
  if (!saved || !matches(saved.a, endpoint(state, "src")) || !matches(saved.b, endpoint(state, "dst"))) return null;
  const out = clone(saved);
  if (!twoWay(state)) {
    delete out.directions.b_to_a;
    if (out.conflict === "prefer_b") out.conflict = "leave_unchanged";
  }
  return out;
}

function title(state, side) {
  const provider = state[side];
  const instance = String(state[`${side}_instance`] || "default");
  const list = state.instanceMap?.[String(provider).toUpperCase()] || state.instanceMap?.[String(provider).toLowerCase()] || [];
  const row = list.find(item => item.id === instance);
  return `${meta(state, provider)?.label || provider} · ${row?.display_label || row?.label || instance}`;
}

export function mappingDefaults(state, direction, media) {
  const reverse = direction === "b_to_a";
  const source = meta(state, reverse ? state.dst : state.src)?.capabilities?.status_mapping?.media?.[media];
  const target = meta(state, reverse ? state.src : state.dst)?.capabilities?.status_mapping?.media?.[media];
  const writable = target?.writable || [];
  return Object.fromEntries((source?.readable || []).filter(status => status !== "planned" && writable.length).map(status => [status, {
    include: target?.defaults?.[status]?.include === true && writable.includes(status),
    target: writable.includes(status) ? status : writable[0],
  }]));
}

export function mountStatusMapping(host, state) {
  if (!host || !hasStatusMapping(state)) return;
  let direction = twoWay(state) ? state.statusDirection || "a_to_b" : "a_to_b";
  let media = state.statusMedia || "shows";
  const persist = change => {
    const draft = statusMappingPayload(state) || {version:1, a:endpoint(state,"src"), b:endpoint(state,"dst"), conflict:"leave_unchanged", directions:{}};
    if (!Object.keys(draft.directions).length) {
      for (const dir of twoWay(state) ? ["a_to_b", "b_to_a"] : ["a_to_b"])
        draft.directions[dir] = Object.fromEntries(["shows", "movies"].map(kind => [kind, mappingDefaults(state, dir, kind)]));
    }
    draft.directions[direction] ||= {};
    const defaults = mappingDefaults(state, direction, media);
    const stored = draft.directions[direction][media];
    draft.directions[direction][media] = stored ? Object.fromEntries(Object.entries(defaults).map(([key, value]) => [key, stored[key] || {...value, include:false}])) : defaults;
    change(draft);
    state.statusMapping = draft;
  };
  const render = focus => {
    const draft = statusMappingPayload(state);
    const reverse = direction === "b_to_a";
    const source = meta(state, reverse ? state.dst : state.src).capabilities.status_mapping;
    const target = meta(state, reverse ? state.src : state.dst).capabilities.status_mapping;
    const defaults = mappingDefaults(state, direction, media);
    const stored = draft?.directions?.[direction]?.[media];
    const rows = draft ? Object.fromEntries(Object.entries(defaults).map(([key, value]) => [key, stored?.[key] || {...value, include:false}])) : defaults;
    const labels = source.labels;
    const writable = target.media?.[media]?.writable || [];
    const history = state.options.history || {};
    host.innerHTML = `<section class="cw-status-mapping" aria-labelledby="cx-status-title">
      <div class="opt-row cw-status-optin"><h3 id="cx-status-title">Status mapping</h3><label class="switch"><input type="checkbox" data-status-optin aria-label="Enable status syncing" aria-describedby="cx-status-optin-help" ${history.use_source_status ? "checked" : ""}><span class="slider"></span></label></div>
      <p>Choose which title statuses to transfer. Planned stays managed by Watchlist.</p>
      <p id="cx-status-optin-help">${history.use_source_status ? "Enabled" : "Disabled"} — shared with History’s “Use source watch status”.</p>
      <p role="status">${!history.enable ? "History is off. These rules will not run until History and its status opt-in are enabled." : !history.use_source_status ? "Status syncing is off." : draft ? "Custom status rules apply when this pair syncs." : "Existing status behavior is active. Edit a rule to use custom mappings."}</p>
      <div class="cw-status-selectors">
        <label>Direction<select data-status-direction aria-label="Status direction"><option value="a_to_b" ${!reverse ? "selected" : ""}>${esc(title(state,"src"))} → ${esc(title(state,"dst"))}</option>${twoWay(state) ? `<option value="b_to_a" ${reverse ? "selected" : ""}>${esc(title(state,"dst"))} → ${esc(title(state,"src"))}</option>` : ""}</select></label>
        <label>Titles<select data-status-media aria-label="Status media"><option value="shows" ${media === "shows" ? "selected" : ""}>Shows</option><option value="movies" ${media === "movies" ? "selected" : ""}>Movies</option></select></label>
      </div>
      <div class="cw-status-map-rows">${Object.entries(rows).map(([status, rule]) => `<div class="cw-status-map-row">
        <label><input type="checkbox" data-status-include="${esc(status)}" ${rule.include ? "checked" : ""}> ${esc(labels[status] || status)}</label>
        <div><select data-status-target="${esc(status)}" aria-label="Map ${esc(labels[status] || status)} to" ${rule.include ? "" : "disabled"}>${writable.map(value => `<option value="${esc(value)}" ${rule.target === value ? "selected" : ""}>${esc(target.labels[value] || value)}</option>`).join("")}</select>
        ${rule.include && target.media?.[media]?.effects?.[rule.target] ? `<small>${esc(target.media[media].effects[rule.target])}</small>` : ""}</div>
      </div>`).join("") || "<p>No supported mappings for these titles.</p>"}</div>
      ${!writable.includes("completed") && target.media?.[media]?.effects?.completed ? `<p>${esc(target.media[media].effects.completed)}</p>` : ""}
      <p>Excluded statuses are left unchanged. No status mapping deletes a title or creates watched plays.</p>
      <label class="cw-status-conflict">When statuses conflict<select data-status-conflict aria-label="Status conflict policy"><option value="leave_unchanged">Leave unchanged</option><option value="prefer_a">Prefer ${esc(title(state,"src"))}</option>${twoWay(state) ? `<option value="prefer_b">Prefer ${esc(title(state,"dst"))}</option>` : ""}</select></label>
      <div class="cw-status-actions"><button type="button" data-status-reset>Restore these defaults</button>${draft ? '<button type="button" data-status-legacy>Use existing behavior</button>' : ""}</div>
    </section>`;
    host.querySelector("[data-status-conflict]").value = draft?.conflict || "leave_unchanged";
    host.querySelector("[data-status-optin]").onchange = event => {state.options.history = {...state.options.history, use_source_status:event.target.checked}; render("[data-status-optin]");};
    host.querySelector("[data-status-direction]").onchange = event => {direction = state.statusDirection = event.target.value; render("[data-status-direction]");};
    host.querySelector("[data-status-media]").onchange = event => {media = state.statusMedia = event.target.value; render("[data-status-media]");};
    host.querySelectorAll("[data-status-include]").forEach(input => input.onchange = () => {persist(value => {value.directions[direction][media][input.dataset.statusInclude].include = input.checked;}); render(`[data-status-include="${input.dataset.statusInclude}"]`);});
    host.querySelectorAll("[data-status-target]").forEach(select => select.onchange = () => {persist(value => {value.directions[direction][media][select.dataset.statusTarget].target = select.value;}); render(`[data-status-target="${select.dataset.statusTarget}"]`);});
    host.querySelector("[data-status-conflict]").onchange = event => {persist(value => {value.conflict = event.target.value;});};
    host.querySelector("[data-status-reset]").onclick = () => {persist(value => {value.directions[direction][media] = mappingDefaults(state, direction, media); value.conflict = "leave_unchanged";}); render("[data-status-reset]");};
    host.querySelector("[data-status-legacy]")?.addEventListener("click", () => {state.statusMapping = null; render("[data-status-optin]");});
    if (focus) host.querySelector(focus)?.focus();
  };
  render();
}
