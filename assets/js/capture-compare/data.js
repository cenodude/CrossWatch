/* CrossWatch - Capture comparison routing and results */
const I18N = () => globalThis.window?.CW?.I18n || globalThis.window?.__cwI18n;
const T = (key, vars) => I18N()?.t?.(key, vars) ?? key;
const TH = (key, vars) => I18N()?.html?.(key, vars) ?? key;
import {kindOf, displayTitle} from './model.js';

export const STATUS = Object.defineProperties({}, Object.fromEntries(['added', 'removed', 'updated', 'unchanged'].map(key => [key, {enumerable: true, get: () => T(`snapshots.compare.status.${key}`)}])));
export function readRoute(hash) {
  const query = new URLSearchParams(String(hash).split('?')[1] || '');
  return {a:query.get('a') || '', b:query.get('b') || '', feature:query.get('feature') || ''};
}
export function compareRoute({a, b, feature}) {
  const query = new URLSearchParams({a, b});
  if (feature) query.set('feature', feature);
  return '#capture_compare?' + query;
}
export function normalizeRow(row) {
  const status = Object.hasOwn(STATUS, row.status) ? row.status : 'unchanged';
  const before = status === 'added' ? null : row.old || row.item || null;
  const after = status === 'removed' ? null : row.new || row.item || null;
  return {key:String(row.key || ''), status, before, after, brief:after || before || row.brief || {}, changes:Array.isArray(row.changes) ? row.changes : []};
}
export function filterRows(rows, {search='', status='changes', type='', sort='status'} = {}) {
  const query = search.trim().toLowerCase(), order = Object.keys(STATUS);
  return rows.filter(row => (status === 'all' || (status === 'changes' ? row.status !== 'unchanged' : row.status === status)) &&
    (!type || kindOf(row.brief) === type) && (!query || [row.key, row.before, row.after, row.brief].map(value => typeof value === 'string' ? value : JSON.stringify(value)).join(' ').toLowerCase().includes(query)))
    .sort((a,b) => (sort === 'status' ? order.indexOf(a.status)-order.indexOf(b.status) : 0) ||
      (sort === 'key' ? a.key.localeCompare(b.key) : displayTitle(a.brief).localeCompare(displayTitle(b.brief))) || a.key.localeCompare(b.key));
}
export async function loadComparison(route, {signal, fetcher=fetch, onProgress=()=>{}} = {}) {
  let result = null, offset = 0;
  const items = [];
  do {
    const query = new URLSearchParams({...route, kind:'all', offset:String(offset), limit:'20000', max_changes:'250', max_depth:'6'});
    const response = await fetcher('/api/snapshots/diff/extended?' + query, {signal, cache:'no-store', credentials:'same-origin'});
    if (!response.ok) throw new Error(T("snapshots.compare.loadFailed"));
    const payload = await response.json(), diff = payload?.diff;
    if (!diff || diff.ok === false || !Array.isArray(diff.items)) throw new Error(T("snapshots.compare.readFailed"));
    result ||= diff;
    items.push(...diff.items);
    offset += diff.items.length;
    if (offset < Number(diff.total) && !diff.items.length) throw new Error(T("snapshots.compare.incomplete"));
    onProgress(offset, Number(diff.total) || offset);
  } while (offset < Number(result.total));
  return {...result, items:items.map(normalizeRow).filter(row => row.key)};
}
