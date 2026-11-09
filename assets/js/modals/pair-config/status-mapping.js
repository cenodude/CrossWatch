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

function defaultMapping(state) {
  return {version:1, a:endpoint(state, "src"), b:endpoint(state, "dst"), conflict:"leave_unchanged",
    directions:Object.fromEntries((twoWay(state) ? ["a_to_b", "b_to_a"] : ["a_to_b"]).map(dir => [dir, Object.fromEntries(["shows", "movies"].map(kind => [kind, mappingDefaults(state, dir, kind)]))]))};
}

export function statusMappingPayload(state) {
  const saved = state.statusMapping;
  const matches = (value, expected) => value?.provider === expected.provider && value?.instance === expected.instance;
  const enabled = state.statusEnabled ?? state.options?.history?.use_source_status;
  const current = saved && matches(saved.a, endpoint(state, "src")) && matches(saved.b, endpoint(state, "dst")) ? saved
    : enabled && hasStatusMapping(state) ? defaultMapping(state) : null;
  if (!current) return null;
  const out = clone(current);
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
    const draft = statusMappingPayload(state) || defaultMapping(state);
    if (!Object.keys(draft.directions || {}).length) draft.directions = defaultMapping(state).directions;
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
    const active = !!history.use_source_status;
    const expanded = active || state.statusExpanded === true;
    const targetName = meta(state, reverse ? state.src : state.dst).label || (reverse ? state.src : state.dst);
    host.innerHTML = `<section class="cw-status-mapping cw-page-status-mapping" data-enabled="${active}" aria-labelledby="cx-status-title">
      <div class="cw-status-heading">
        <h3 id="cx-status-title"><span class="material-symbols-rounded" aria-hidden="true">tune</span>Status mapping</h3>
        <label class="cw-status-toggle"><span>${active ? "On" : "Off"}</span><input type="checkbox" class="cw-checkbox" data-status-optin aria-label="Enable status syncing" ${active ? "checked" : ""}><span class="cw-status-track" aria-hidden="true"></span></label>
      </div>
      <details class="cw-status-editor" ${expanded ? "open" : ""}>
        <summary>Configure mappings<span class="material-symbols-rounded" aria-hidden="true">expand_more</span></summary>
        <div class="cw-status-body">
          ${!history.enable ? '<div class="cw-status-notice"><span>History is off</span><button type="button" data-status-history>History settings<span aria-hidden="true"> &rarr;</span></button></div>' : ""}
          ${[...new Set([state.src, state.dst].map(provider => meta(state, provider)?.capabilities?.status_mapping?.notice).filter(Boolean))].map(text => `<div class="cw-status-notice"><span>${esc(text)}</span></div>`).join("")}
          <div class="cw-status-selectors">
            <select data-status-direction aria-label="Status direction"><option value="a_to_b" ${!reverse ? "selected" : ""}>${esc(title(state,"src"))} &rarr; ${esc(title(state,"dst"))}</option>${twoWay(state) ? `<option value="b_to_a" ${reverse ? "selected" : ""}>${esc(title(state,"dst"))} &rarr; ${esc(title(state,"src"))}</option>` : ""}</select>
            <div class="cw-status-media" role="group" aria-label="Title type">${["shows", "movies"].map(kind => `<button type="button" data-status-media="${kind}" aria-pressed="${media === kind}">${kind === "shows" ? "Shows" : "Movies"}</button>`).join("")}</div>
          </div>
          <div class="cw-status-map-rows">
            <div class="cw-status-columns" aria-hidden="true"><span>Source status</span><span></span><span>${esc(targetName)} status</span></div>
            ${Object.entries(rows).map(([status, rule]) => `<div class="cw-status-map-row" data-included="${rule.include}">
              <label><input type="checkbox" data-status-include="${esc(status)}" ${rule.include ? "checked" : ""}>${esc(labels[status] || status)}</label>
              <span class="cw-status-arrow" aria-hidden="true">&rarr;</span>
              <div class="cw-status-target"><select data-status-target="${esc(status)}" aria-label="Map ${esc(labels[status] || status)} to" ${rule.include ? "" : "disabled"}>${writable.map(value => `<option value="${esc(value)}" ${rule.target === value ? "selected" : ""}>${esc(target.labels[value] || value)}</option>`).join("")}</select>
              ${rule.include && target.media?.[media]?.effects?.[rule.target] ? `<details class="cw-status-info"><summary aria-label="Details for ${esc(labels[status] || status)}"><span class="material-symbols-rounded" aria-hidden="true">info</span></summary><p>${esc(target.media[media].effects[rule.target])}</p></details>` : ""}</div>
            </div>`).join("") || '<p class="cw-status-empty">No supported mappings for these titles.</p>'}
          </div>
          <footer class="cw-status-footer">
            <label class="cw-status-conflict">Conflicts<select data-status-conflict aria-label="Status conflict policy"><option value="leave_unchanged">Leave unchanged</option><option value="prefer_a">Prefer ${esc(title(state,"src"))}</option>${twoWay(state) ? `<option value="prefer_b">Prefer ${esc(title(state,"dst"))}</option>` : ""}</select></label>
            <div class="cw-status-actions"><button type="button" data-status-reset><span class="material-symbols-rounded" aria-hidden="true">restart_alt</span>Reset defaults</button></div>
          </footer>
          <details class="cw-status-help"><summary>How it works</summary><div>
            <p>This is the same switch as Status mapping in History options. History must be enabled for these rules to run.</p>
            <p>Planned stays managed by Watchlist. Excluded statuses stay unchanged. Mappings never delete titles or create watched plays.</p>
            ${!writable.includes("completed") && target.media?.[media]?.effects?.completed ? `<p>${esc(target.media[media].effects.completed)}</p>` : ""}
            <p>Rules apply to this pair only. Reset defaults applies to the selected direction and title type, and resets the conflict preference.</p>
          </div></details>
        </div>
      </details>
    </section>`;
    host.querySelector("[data-status-conflict]").value = draft?.conflict || "leave_unchanged";
    host.querySelector("[data-status-optin]").onchange = event => {state.options.history = {...state.options.history, use_source_status:event.target.checked}; state.statusExpanded = false; render("[data-status-optin]");};
    host.querySelector("[data-status-direction]").onchange = event => {direction = state.statusDirection = event.target.value; render("[data-status-direction]");};
    host.querySelectorAll("[data-status-media]").forEach(button => button.onclick = () => {media = state.statusMedia = button.dataset.statusMedia; render(`[data-status-media="${media}"]`);});
    const editor = host.querySelector(".cw-status-editor");
    editor.ontoggle = () => {if (editor.isConnected && !state.options.history?.use_source_status) state.statusExpanded = editor.open;};
    host.querySelector("[data-status-history]")?.addEventListener("click", () => {
      host.ownerDocument.getElementById("cx-tab-history")?.click();
      queueMicrotask(() => host.ownerDocument.getElementById("cx-hs-enable")?.focus());
    });
    host.querySelectorAll("[data-status-include]").forEach(input => input.onchange = () => {persist(value => {value.directions[direction][media][input.dataset.statusInclude].include = input.checked;}); render(`[data-status-include="${input.dataset.statusInclude}"]`);});
    host.querySelectorAll("[data-status-target]").forEach(select => select.onchange = () => {persist(value => {value.directions[direction][media][select.dataset.statusTarget].target = select.value;}); render(`[data-status-target="${select.dataset.statusTarget}"]`);});
    host.querySelector("[data-status-conflict]").onchange = event => {persist(value => {value.conflict = event.target.value;}); render("[data-status-conflict]");};
    host.querySelector("[data-status-reset]").onclick = () => {persist(value => {value.directions[direction][media] = mappingDefaults(state, direction, media); value.conflict = "leave_unchanged";}); render("[data-status-reset]");};
    if (focus) host.querySelector(focus)?.focus();
  };
  render();
}
