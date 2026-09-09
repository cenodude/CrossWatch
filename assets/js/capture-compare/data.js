/* CrossWatch - Capture comparison routing and results */
import {kindOf, displayTitle} from './model.js';

export const STATUS = {added:'Added', removed:'Removed', updated:'Updated', unchanged:'Unchanged'};
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
    if (!response.ok) throw new Error('Could not load these captures. Check that both are still available.');
    const payload = await response.json(), diff = payload?.diff;
    if (!diff || diff.ok === false || !Array.isArray(diff.items)) throw new Error('Could not read the capture comparison.');
    result ||= diff;
    items.push(...diff.items);
    offset += diff.items.length;
    if (offset < Number(diff.total) && !diff.items.length) throw new Error('The comparison was incomplete. Please refresh.');
    onProgress(offset, Number(diff.total) || offset);
  } while (offset < Number(result.total));
  return {...result, items:items.map(normalizeRow).filter(row => row.key)};
}
