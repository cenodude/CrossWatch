/* CrossWatch - Capture Compare page */
const I18N = () => globalThis.window?.CW?.I18n || globalThis.window?.__cwI18n;
const T = (key, vars) => I18N()?.t?.(key, vars) ?? key;
const TH = (key, vars) => I18N()?.html?.(key, vars) ?? key;
import {esc, displayTitle, displaySub, pretty, copyText, renderRecordCard, renderChanges} from './model.js';
import {STATUS, readRoute, compareRoute, filterRows, loadComparison} from './data.js';

const icon = name => `<span class="material-symbols-rounded" aria-hidden="true">${name}</span>`;
const label = value => String(value || '').replaceAll('_', ' ').replace(/^./, ch => ch.toUpperCase());
const pageSize = 50;
let host, context = '', controller, result, rows = [], selected = '', page = 0, route;
let filters = {search:'', status:'changes', type:'', sort:'status'};
const $ = selector => host?.querySelector(selector);
const currentContext = () => JSON.stringify([readRoute(location.hash), window.CW?.AuthState?.read?.(), window.CW?.OverviewProfile?.id]);

function layout() {
  return `<div class="capture-compare-page">
    <nav class="cp-back" aria-label="${TH("snapshots.compare.breadcrumb")}"><a href="#snapshots">${icon('arrow_back')}${TH("snapshots.compare.captures")}</a><span>/</span><span>${TH("snapshots.compare.compare")}</span></nav>
    <header class="cp-header"><div><h1>${TH("snapshots.compare.pageTitle")}</h1><p>${TH("snapshots.compare.intro")}</p></div><div class="cp-actions"><button type="button" data-action="swap">${icon('swap_horiz')}${TH("snapshots.compare.swap")}</button><button type="button" data-action="refresh">${icon('refresh')}${TH("snapshots.compare.refresh")}</button></div></header>
    <div class="cp-captures" id="cp-captures"></div>
    <div id="cp-notice" role="status" aria-live="polite"></div>
    <div class="cp-content" id="cp-content" hidden>
      <div class="cp-stats" id="cp-stats" aria-label="${TH("snapshots.compare.filter")}"></div>
      <div class="cp-toolbar"><label class="cp-search">${icon('search')}<input type="search" id="cp-search" placeholder="${TH("snapshots.compare.search")}" aria-label="${TH("snapshots.compare.searchLabel")}"></label><label class="cp-filter">${TH("snapshots.compare.feature")}<select id="cp-feature" aria-label="${TH("snapshots.compare.feature")}"></select></label><label class="cp-filter">${TH("snapshots.compare.type")}<select id="cp-type" aria-label="${TH("snapshots.compare.mediaType")}"><option value="">${TH("snapshots.compare.allTypes")}</option><option value="movie">${TH("snapshots.compare.movies")}</option><option value="show">${TH("snapshots.compare.shows")}</option><option value="season">${TH("snapshots.compare.seasons")}</option><option value="episode">${TH("snapshots.compare.episodes")}</option></select></label><label class="cp-filter">${TH("snapshots.compare.sort")}<select id="cp-sort" aria-label="${TH("snapshots.compare.sortLabel")}"><option value="status">${TH("snapshots.compare.change")}</option><option value="title">${TH("snapshots.compare.titleOpt")}</option><option value="key">${TH("snapshots.compare.key")}</option></select></label></div>
      <div class="cp-workspace"><section class="cp-results" aria-label="${TH("snapshots.compare.results")}"><div class="cp-results-head"><h2>${TH("snapshots.compare.items")}</h2><span id="cp-count" aria-live="polite"></span></div><div id="cp-list"></div><div class="cp-pagination" id="cp-pagination"></div></section><section class="cp-detail" id="cp-detail" aria-label="${TH("snapshots.compare.details")}"></section></div>
    </div></div>`;
}
function date(value) {
  if (!value) return T("snapshots.compare.dateUnavailable");
  const parsed = new Date(String(value).replace(' ', 'T'));
  return Number.isNaN(parsed.getTime()) ? String(value) : parsed.toLocaleString(I18N()?.locale, {dateStyle:'medium', timeStyle:'short'});
}
function renderCaptures() {
  $('#cp-captures').innerHTML = ['a','b'].map((side, i) => {
    const meta = result?.[side];
    const name = meta ? [meta.provider, meta.instance || meta.instance_id].filter(Boolean).join(' · ') : '';
    return `<article class="cp-capture"><span class="cp-letter">${side.toUpperCase()}</span><div><h2>${TH(i ? 'snapshots.compare.compared' : 'snapshots.compare.baseline')}${name ? ' · '+esc(name) : ''}</h2><p>${meta ? esc(date(meta.created_at))+' · '+esc(meta.count ?? result.summary?.['total_'+side] ?? 0)+' '+TH('snapshots.compare.itemsWord') : TH('snapshots.compare.selected')}</p><span class="cp-filename" title="${esc(route[side])}">${esc(meta?.label || route[side].split(/[\\/]/).pop())}</span></div></article>`;
  }).join('');
}
function renderStats() {
  const counts = Object.fromEntries(Object.keys(STATUS).map(status => [status, result.items.filter(row => row.status === status).length]));
  const options = [['changes',T('snapshots.compare.changes'),counts.added+counts.removed+counts.updated], ['all',T('snapshots.compare.allItems'),result.items.length], ...Object.entries(STATUS).map(([key,name]) => [key,name,counts[key]])];
  $('#cp-stats').innerHTML = options.map(([key,name,count]) => `<button type="button" data-status="${key}" class="cp-stat ${key}" aria-pressed="${filters.status === key}"><strong>${count.toLocaleString(I18N()?.locale)}</strong><span>${name}</span></button>`).join('');
}
function renderDetail() {
  const row = rows.find(item => item.key === selected);
  if (!row) {
    $('#cp-detail').innerHTML = `<div class="cp-empty">${icon('difference')}<h2>${TH("snapshots.compare.select")}</h2><p>${TH("snapshots.compare.selectSub")}</p></div>`;
    return;
  }
  $('#cp-detail').innerHTML = `<div class="cp-detail-head"><span class="cp-status ${row.status}">${STATUS[row.status]}</span><h2>${esc(displayTitle(row.brief))}</h2><code>${esc(row.key)}</code><div class="cp-actions"><button type="button" data-copy-record="key">${icon('content_copy')}${TH("snapshots.compare.copyKey")}</button><button type="button" data-copy-record="before" ${row.before ? '' : 'disabled'}>${TH("snapshots.compare.copyA")}</button><button type="button" data-copy-record="after" ${row.after ? '' : 'disabled'}>${TH("snapshots.compare.copyB")}</button></div><span id="cp-copy-status" role="status"></span></div>
    ${row.status === 'updated' ? `<details class="cp-changes" open><summary>${TH('snapshots.compare.changedFields', {count: row.changes.length})}</summary>${renderChanges(row.changes)}</details>` : ''}
    <div class="cp-records">${renderRecordCard(T('snapshots.compare.aBefore'), row.before, T('snapshots.compare.notInA'))}${renderRecordCard(T('snapshots.compare.bAfter'), row.after, T('snapshots.compare.notInB'))}</div>`;
}
function renderResults() {
  const active = document.activeElement;
  const restoreFocus = active?.dataset?.status ? `[data-status="${active.dataset.status}"]` : active?.dataset?.page ? `[data-page="${active.dataset.page}"]` : '';
  rows = filterRows(result.items, filters);
  page = Math.max(0, Math.min(page, Math.ceil(rows.length / pageSize)-1));
  const visible = rows.slice(page*pageSize, (page+1)*pageSize);
  if (!visible.some(row => row.key === selected)) selected = visible[0]?.key || '';
  $('#cp-count').textContent = T('snapshots.compare.countOf', {shown: rows.length.toLocaleString(I18N()?.locale), total: result.items.length.toLocaleString(I18N()?.locale)});
  const identical = !result.items.some(row => row.status !== 'unchanged');
  $('#cp-list').innerHTML = visible.length ? visible.map(row => `<button type="button" class="cp-row" data-key="${esc(row.key)}" aria-pressed="${selected === row.key}"><span class="cp-status ${row.status}">${STATUS[row.status]}</span><span class="cp-row-title"><strong>${esc(displayTitle(row.brief))}</strong><small>${esc(displaySub(row.brief))}</small></span>${icon('chevron_right')}</button>`).join('') : `<div class="cp-empty">${icon(identical ? 'check_circle' : 'search_off')}<h2>${identical && filters.status === 'changes' ? T("snapshots.compare.noChangesCaptures") : T("snapshots.compare.noMatching")}</h2><p>${identical ? T("snapshots.compare.same") : T("snapshots.compare.tryAnother")}</p><button type="button" data-action="reset">${TH("snapshots.compare.showAll")}</button></div>`;
  $('#cp-pagination').innerHTML = `<button type="button" data-page="-1" ${page === 0 ? 'disabled' : ''}>${icon('chevron_left')}${TH("snapshots.compare.previous")}</button><span>${TH('snapshots.compare.page', {page: page+1, pages: Math.max(1,Math.ceil(rows.length/pageSize))})}</span><button type="button" data-page="1" ${(page+1)*pageSize >= rows.length ? 'disabled' : ''}>${TH("snapshots.compare.next")}${icon('chevron_right')}</button>`;
  renderStats();
  renderDetail();
  if (restoreFocus) $(restoreFocus)?.focus({preventScroll:true});
}
async function load() {
  controller?.abort();
  const request = controller = new AbortController(), root = host;
  $('#cp-content').hidden = true;
  result = null;
  renderCaptures();
  const notice = $('#cp-notice');
  if (!route.a || !route.b) {
    notice.innerHTML = `<div class="cp-empty">${icon('photo_library')}<h2>${TH("snapshots.compare.choose")}</h2><p>${TH("snapshots.compare.chooseSub")}</p><a href="#snapshots">${TH("snapshots.compare.goCaptures")}</a></div>`;
    return;
  }
  notice.textContent = T("snapshots.compare.loading");
  root.setAttribute('aria-busy', 'true');
  const timeout = setTimeout(() => request.abort(), 60000);
  try {
    const next = await loadComparison(route, {signal:request.signal, onProgress:(count,total) => {
      if (controller === request) notice.textContent = T('snapshots.compare.loadingN', {count: count.toLocaleString(I18N()?.locale), total: total.toLocaleString(I18N()?.locale)});
    }});
    if (controller !== request || request.signal.aborted) return;
    result = next;
    renderCaptures();
    const features = result.available_features?.length ? result.available_features : [result.selected_feature || route.feature].filter(Boolean);
    const select = $('#cp-feature');
    select.innerHTML = features.map(feature => `<option value="${esc(feature)}">${esc(label(feature))}</option>`).join('');
    select.value = result.selected_feature || route.feature || features[0] || '';
    select.disabled = features.length <= 1;
    notice.textContent = '';
    $('#cp-content').hidden = false;
    renderResults();
    window.CW?.IconSelect?.enhancePlain?.(host);
  } catch (error) {
    if (controller !== request) return;
    notice.innerHTML = `<div class="cp-empty cp-error"><h2>${TH("snapshots.compare.couldNotLoad")}</h2><p>${esc(request.signal.aborted ? T("snapshots.compare.timeout") : error.message)}</p><button type="button" data-action="refresh">${TH("snapshots.compare.retry")}</button><a href="#snapshots">${TH("snapshots.compare.chooseCaptures")}</a></div>`;
  } finally {
    clearTimeout(timeout);
    if (controller === request) { controller = null; root.removeAttribute('aria-busy'); }
  }
}
async function onClick(event) {
  const button = event.target.closest('button');
  if (!button || button.disabled) return;
  if (button.dataset.action === 'refresh') return load();
  if (button.dataset.action === 'swap') { location.hash = compareRoute({...route, a:route.b, b:route.a}); return; }
  if (!result) return;
  if (button.dataset.status) { filters.status = button.dataset.status; page = 0; renderResults(); }
  else if (button.dataset.page) { page += Number(button.dataset.page); renderResults(); }
  else if (button.dataset.key) {
    selected = button.dataset.key;
    host.querySelectorAll('.cp-row').forEach(node => node.setAttribute('aria-pressed', String(node.dataset.key === selected)));
    renderDetail();
    if (window.matchMedia('(max-width:760px)').matches) {
      $('#cp-detail').scrollIntoView({block:'start', behavior:'auto'});
    }
  } else if (button.dataset.action === 'reset') {
    filters.search = ''; filters.type = ''; filters.status = 'all'; page = 0;
    $('#cp-search').value = ''; $('#cp-type').value = '';
    $('#cp-type').dispatchEvent(new Event('change', {bubbles:true}));
    renderResults();
  } else if (button.dataset.copyRecord || button.dataset.copy) {
    const row = rows.find(row => row.key === selected), field = button.dataset.copyRecord;
    const value = field ? (field === 'key' ? row?.key : pretty(row?.[field])) : button.dataset.copy;
    const status = $('#cp-copy-status');
    const ok = await copyText(value);
    if (status?.isConnected) status.textContent = ok ? T("snapshots.compare.copied") : T("snapshots.compare.blocked");
  }
}
function onFilter(event) {
  if (!result) return;
  const target = event.target;
  if (target.id === 'cp-feature') {
    if (target.value !== (route.feature || result.selected_feature)) location.hash = compareRoute({...route,feature:target.value});
    return;
  }
  const field = {'cp-search':'search','cp-type':'type','cp-sort':'sort'}[target.id];
  if (!field || filters[field] === target.value) return;
  filters[field] = target.value; page = 0; renderResults();
}
const CaptureComparePage = {
  async mount(root) {
    if (!root) return;
    const next = currentContext();
    if (host === root && context === next && (controller || result)) return;
    this.unmount(); host = root; context = next; route = readRoute(location.hash);
    filters = {search:'',status:'changes',type:'',sort:'status'}; page = 0; selected = '';
    if (!document.querySelector('link[data-capture-compare-page]')) {
      const link = document.createElement('link');
      link.rel = 'stylesheet'; link.dataset.captureComparePage = '';
      link.href = new URL('./styles.css?v='+encodeURIComponent(window.APP_VERSION || '1'), import.meta.url).href;
      document.head.append(link);
    }
    root.innerHTML = layout();
    root.addEventListener('click', onClick); root.addEventListener('input', onFilter); root.addEventListener('change', onFilter);
    await load();
  },
  hide() { if (controller) { const previous = controller; controller = null; previous.abort(); host?.removeAttribute('aria-busy'); } },
  unmount() {
    this.hide();
    host?.removeEventListener('click', onClick); host?.removeEventListener('input', onFilter); host?.removeEventListener('change', onFilter);
    host?.replaceChildren(); host = null; context = ''; result = null; rows = [];
  },
};
window.CaptureComparePage = CaptureComparePage;
for (const name of ['cw:overview-profile-changed','cw:auth-state-changed']) {
  window.addEventListener(name, () => {
    if (!host || context === currentContext()) return;
    const root = host;
    CaptureComparePage.unmount();
    if (document.documentElement.dataset.tab === 'capture_compare') void CaptureComparePage.mount(root);
  });
}
export default CaptureComparePage;
