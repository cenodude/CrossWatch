import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import vm from 'node:vm';
import * as data from '../assets/js/capture-compare/data.js';
import * as model from '../assets/js/capture-compare/model.js';

test('capture routes retain paths with spaces, hashes and query characters', () => {
  const route = {a:'plex/A #1 & first.json',b:'simkl/第二.json',feature:'history'};
  assert.deepEqual(data.readRoute(data.compareRoute(route)), route);
  assert.deepEqual(data.readRoute('#capture_compare'), {a:'',b:'',feature:''});
});

test('Capture Compare uses the same managed-user access gate as Captures', async () => {
  const source = await readFile(new URL('../assets/helpers/core.js',import.meta.url),'utf8');
  const start = source.indexOf('  function routeSegment('), end = source.indexOf('  function allowedRouteTab(');
  const routes = source.match(/const ROUTE_TABS = new Set\([^;]+;/)[0];
  const auth = {isManaged:true,permissions:{write:false}};
  const context = vm.createContext({window:{CW:{AuthState:{read:()=>auth}}},document:{documentElement:{dataset:{}}}});
  vm.runInContext(routes+'\n'+source.slice(start,end),context);
  assert.equal(context.canUseRouteTab('capture_compare'),false);
  auth.permissions.write = true;
  assert.equal(context.canUseRouteTab('capture_compare'),true);
  assert.equal(context.normalizeRouteTab('capture-compare'),'capture_compare');
});

test('added and removed records have the correct missing side', () => {
  const record = {type:'movie',title:'Example'};
  const added = data.normalizeRow({key:'a',status:'added',item:record});
  const removed = data.normalizeRow({key:'b',status:'removed',item:record});
  assert.equal(added.before,null);
  assert.deepEqual(added.after,record);
  assert.equal(removed.after,null);
  assert.deepEqual(removed.before,record);
});

test('search matches original and corrected titles and IDs, with combined type and status filters', () => {
  const rows = [data.normalizeRow({key:'a',status:'updated',old:{title:'Original',ids:{tmdb:123}},new:{title:'Corrected',type:'movie',ids:{tmdb:456}}}),data.normalizeRow({key:'b',status:'unchanged',item:{title:'Original',type:'show'}})];
  for (const search of ['Original','Corrected','123','456']) assert.deepEqual(data.filterRows(rows,{search,type:'movie',status:'updated'}).map(row=>row.key),['a']);
  assert.equal(data.filterRows(rows,{type:'show'}).length,0);
  assert.equal(data.filterRows(rows,{type:'show',status:'all'}).length,1);
});

test('unchanged captures can be viewed through All items', () => {
  const rows = [data.normalizeRow({key:'a',status:'unchanged',item:{title:'Same'}})];
  assert.equal(data.filterRows(rows).length,0);
  assert.equal(data.filterRows(rows,{status:'all'}).length,1);
});

test('all 20,005 comparison rows are loaded and remain searchable beyond the old cap', async () => {
  const requests = [];
  const result = await data.loadComparison({a:'a',b:'b'}, {fetcher:async url => {
    const query = new URL(url,'http://localhost').searchParams;
    const offset = Number(query.get('offset')), limit = Number(query.get('limit'));
    requests.push(offset);
    return {ok:true,json:async()=>({diff:{ok:true,total:20005,items:Array.from({length:Math.min(limit,20005-offset)},(_,i)=>({key:String(offset+i),status:'unchanged',item:{title:'Movie '+(offset+i)}}))}})};
  }});
  assert.equal(result.items.length,20005);
  assert.equal(new Set(result.items.map(row=>row.key)).size,20005);
  assert.deepEqual(requests,[0,20000]);
  assert.equal(data.filterRows(result.items,{search:'Movie 20004',status:'all'}).length,1);
});

test('failed and incomplete responses cannot appear as a successful empty comparison', async () => {
  await assert.rejects(data.loadComparison({a:'a',b:'b'},{fetcher:async()=>({ok:false})}), /Could not load/);
  await assert.rejects(data.loadComparison({a:'a',b:'b'},{fetcher:async()=>({ok:true,json:async()=>({diff:{ok:true,total:1,items:[]}})})}), /incomplete/);
});

test('capture records and field changes escape untrusted text', () => {
  const record = {title:'<img src=x onerror=alert(1)>',ids:{tmdb:'"><script>bad</script>'}};
  const markup = model.renderRecordCard('A',record)+model.renderChanges([{path:'<script>',old:'<img>',new:'<svg>'}]);
  assert.doesNotMatch(markup,/<img|<script|<svg/);
  assert.match(markup,/&lt;img/);
});

test('leaving during load aborts the request and ignores its late response', async () => {
  let resolveRequest, requestSignal;
  const source = (await readFile(new URL('../assets/js/capture-compare/index.js',import.meta.url),'utf8'))
    .replace(/^import .*;$/gm,'').replaceAll('import.meta.url','"http://localhost/index.js"').replace('export default CaptureComparePage;','');
  const nodes = new Map();
  const root = {innerHTML:'',querySelector:selector=>{
    if (!nodes.has(selector)) nodes.set(selector,{innerHTML:'',textContent:'',hidden:false});
    return nodes.get(selector);
  },addEventListener(){},removeEventListener(){},replaceChildren(){this.innerHTML='';},setAttribute(){},removeAttribute(){}};
  const ctx = vm.createContext({...model,...data,URL,URLSearchParams,AbortController,setTimeout,clearTimeout,
    location:{hash:'#capture_compare?a=a&b=b'},document:{querySelector:()=>true,documentElement:{dataset:{}}},
    window:{addEventListener(){}},loadComparison:async(_route,{signal})=>{requestSignal=signal;return new Promise(resolve=>{resolveRequest=resolve;});}});
  vm.runInContext(source,ctx);
  const mounting = ctx.window.CaptureComparePage.mount(root);
  ctx.window.CaptureComparePage.unmount();
  assert.equal(requestSignal.aborted,true);
  resolveRequest({items:[]});
  await mounting;
  assert.equal(root.innerHTML,'');
});
