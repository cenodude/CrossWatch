/* assets/js/logs-format.js */
/* CrossWatch - Readable diagnostic log formatting */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const humanize = value => String(value).replace(/[_.:]+/g, ' ').trim().replace(/\b\w/, c => c.toUpperCase());
const titles = new Map(Object.entries({
  health:'Provider health checked', 'api:hit':'API request', 'api:totals':'API usage summary',
  'run:start':'Sync started', 'run:pair':'Syncing pair', 'run:done':'Sync finished',
  'run:error':'Sync failed', 'feature:start':'Feature sync started', 'feature:done':'Feature sync finished',
  'two:start':'Two-way sync started', 'two:plan':'Sync changes planned', 'two:done':'Two-way sync finished',
  'snapshot:start':'Reading provider items', 'snapshot:progress':'Reading progress', 'snapshot:done':'Provider items loaded',
  'drop_guard:skipped':'Removal protection skipped', 'stats:overview':'Sync overview', 'http:overview':'HTTP activity summary',
  'state.persisted':'Sync state saved', 'run.kwargs.ignored':'Unused sync options ignored',
  'observed.deletions':'Deletions checked', 'tombstones.marked':'Removal records updated', 'hidefile.cleared':'Hidden-item records cleared',
}));

export function describeLog(row) {
  const raw = String(row.text ?? '');
  let text = raw.trim().replace(/^\d{4}-\d{2}-\d{2}T\S+\s+/, '').replace(/^\[\d{4}-\d{2}-\d{2}[^\]]*\]\s*/, '');
  for (let i = 0; i < 6; i++) {
    const next = text.replace(/^\[(?:[A-Z][A-Z0-9_-]*(?::[^\[\]]*)?|i|!)\]\s*/, '').replace(/^(?:INFO|DEBUG|WARN(?:ING)?|ERROR|CRITICAL|SUCCESS)\s+/, '');
    if (next === text) break;
    text = next;
  }
  const start = text.match(/^SYNC start:\s+orchestrator pairs run_id=(\S+)$/);
  if (start) return {summary:'Sync started', pills:[{label:'Run',value:start[1],tone:''}], raw, structured:true, start:true};
  const pair = text.match(/^Running single pair:\s+(.+?)\s+→\s+(.+?)\s+\(([^()\s]+)\)$/);
  if (pair) return {
    summary:'Sync pair',
    pills:[{label:'From',value:pair[1],tone:'provider'},{label:'To',value:pair[2],tone:'provider'},{label:'Pair',value:pair[3],tone:''}],
    raw, structured:true,
  };
  const completion = text.match(/^(Done|Cancelled)\.\s+(.+)$/i);
  if (completion) {
    const counts = completion[2].split(/,\s*/).map(part => part.match(/^Total (added|removed|updated|skipped|unresolved|errors|blocked):\s*(\d+)\s*$/i));
    if (counts.length === 7 && counts.every(Boolean) && new Set(counts.map(count => count[1].toLowerCase())).size === 7) {
      const cancelled = completion[1].toLowerCase() === 'cancelled';
      const issues = counts.some(count => ['unresolved','errors','blocked'].includes(count[1].toLowerCase()) && Number(count[2]) > 0);
      const failed = counts.some(count => count[1].toLowerCase() === 'errors' && Number(count[2]) > 0);
      return {
        summary:cancelled ? 'Sync cancelled' : issues ? 'Sync finished with issues' : 'Sync completed',
        pills:counts.map(count => {
          const name = count[1].toLowerCase(), positive = Number(count[2]) > 0;
          const tone = !positive ? '' : name === 'errors' ? 'error' : ['unresolved','blocked','skipped'].includes(name) ? 'warning' : name === 'added' ? 'success' : 'feature';
          return {label:humanize(name), value:count[2], tone};
        }),
        raw, structured:true, completion:failed ? 'error' : cancelled || issues ? 'warning' : 'success',
      };
    }
  }
  let data;
  if (text.startsWith('{')) {
    try { const value = JSON.parse(text); if (value && typeof value === 'object' && !Array.isArray(value)) data = value; } catch {}
  }
  if (!data) return {summary:text || raw, pills:[], raw, structured:false};
  const event = typeof data.event === 'string' ? data.event : '';
  const message = typeof data.msg === 'string' ? data.msg : typeof data.message === 'string' ? data.message : '';
  const key = event === 'debug' && message ? message : event;
  let summary = titles.get(key) || (message ? humanize(message) : key ? humanize(key) : 'Log details');
  if (event === 'run:done') {
    if (data.cancelled === true) summary = 'Sync cancelled';
    else if (['errors','unresolved','blocked'].some(k => Number(data[k]) > 0)) summary = 'Sync finished with issues';
  }
  const pills = [];
  const add = (label, value, tone = '') => {
    if (value === null || value === undefined || value === '') return;
    if (Array.isArray(value)) { if (!value.length || value.some(v => v && typeof v === 'object')) return; value = value.join(', '); }
    if (typeof value === 'object') return;
    pills.push({label, value:typeof value === 'boolean' ? value ? 'On' : 'Off' : String(value), tone});
  };
  const src = typeof data.src === 'string' ? data.src : typeof data.a === 'string' ? data.a : '';
  const dst = typeof data.dst === 'string' ? data.dst : typeof data.b === 'string' ? data.b : '';
  if (src) add('From', src, 'provider');
  if (dst) add('To', dst, 'provider');
  if (data.provider && data.provider !== row.provider && data.provider !== src && data.provider !== dst) add('Provider', data.provider, 'provider');
  if (data.instance && data.instance !== 'default') add('Instance', data.instance);
  for (const side of ['src','dst']) if (data[`${side}_instance`] && data[`${side}_instance`] !== 'default') add(side === 'src' ? 'Source instance' : 'Target instance', data[`${side}_instance`]);
  add('Feature', data.feature, 'feature');
  if (Array.isArray(data.features)) add('Features', data.features, 'feature');
  if (data.status != null) {
    const status = String(data.status), code = Number(status);
    add('Status', status, ['ok','success','completed'].includes(status) || (code >= 200 && code < 300) ? 'success' : ['error','failed'].includes(status) || code >= 400 ? 'error' : '');
  }
  add('Endpoint', data.endpoint);
  if (typeof data.latency_ms === 'number') add('Latency', `${data.latency_ms} ms`);
  if (data.done != null && data.total != null) add('Items', `${data.done} / ${data.total}`);
  else add('Items', data.count ?? data.total);
  for (const name of ['added','updated','removed','skipped','unresolved','blocked','errors','warnings','pairs']) {
    add(humanize(name), data[name], Number(data[name]) > 0 ? name === 'errors' ? 'error' : ['warnings','unresolved','blocked'].includes(name) ? 'warning' : '' : '');
  }
  for (const [field, label] of Object.entries({add_to_A:'Add to source',add_to_B:'Add to target',adds_to_A:'Added to source',adds_to_B:'Added to target',upd_to_A:'Update source',upd_to_B:'Update target',rem_from_A:'Remove from source',rem_from_B:'Remove from target'})) add(label, data[field]);
  if (data.totals && typeof data.totals === 'object') add('Requests', data.totals.total);
  if (data.overview && typeof data.overview === 'object') for (const [field,value] of Object.entries(data.overview)) add(humanize(field),value);
  add('Loaded adapters', data.loaded_adapters);
  add('Saved-state providers', data.saved_state_providers);
  for (const field of ['reason','error','detail','dry_run','mode','removals','providers','keys','scope']) add(humanize(field),data[field]);
  return {summary, pills:pills.slice(0,16), raw, structured:true};
}

export function readableLogHTML(row, highlight, query = '', providerMeta = null) {
  const item = describeLog(row);
  const pills = item.pills.map(p => {
    const brand = p.tone === 'provider' ? providerMeta?.brandInfo?.(p.value) : null;
    const logo = brand?.icon ? `<img src="${esc(brand.icon)}" alt="" width="14" height="14" aria-hidden="true">` : '';
    return `<span class="logs-pill${p.tone ? ` logs-pill-${p.tone}` : ''}"><small>${highlight(p.label,query)}</small>${logo}<span>${highlight(brand?.label || p.value,query)}</span></span>`;
  }).join('');
  const shown = [item.summary, ...item.pills.flatMap(p => [p.label,p.value])].join(' ').toLowerCase();
  const rawMatch = query && item.raw.toLowerCase().includes(query.toLowerCase()) && !shown.includes(query.toLowerCase());
  const cardTone = item.start ? 'start' : item.completion;
  const summaryIcon = cardTone ? `<span class="material-symbols-rounded" aria-hidden="true">${item.start ? 'sync' : item.completion === 'success' ? 'check_circle' : item.completion === 'error' ? 'error_outline' : 'warning'}</span>` : '';
  const body = `<span class="logs-readable-summary">${summaryIcon}${highlight(item.summary,query)}</span>${pills ? `<span class="logs-pill-list">${pills}</span>` : ''}`;
  return `${cardTone ? `<div class="logs-completion logs-completion-${cardTone}">${body}</div>` : body}<details class="logs-original"${rawMatch ? ' open' : ''}><summary>Original log</summary><pre>${highlight(item.raw,query)}</pre></details>`;
}
