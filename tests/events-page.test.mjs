/* tests/events-page.test.mjs */
/* CrossWatch - Events navigation and profile isolation tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
import {pageBackLink,returnFromEvents} from '../assets/js/page-return.js';

test('skipped ratings display a readable reason without reporting failure', async () => {
  const source = await readFile(new URL('../assets/js/events/index.js', import.meta.url), 'utf8');
  const script = source.slice(source.indexOf('const sevOf ='), source.indexOf('const EVENT_TYPES'));
  const context = vm.createContext({reasonLabel: (_code, label) => label});
  vm.runInContext(script + 'globalThis.describe = e => ({title: titleLine(e), severity: sevOf(e)});', context);
  const event = {
    event_type: 'rating_skipped', severity: 'info', reason_code: 'episode_ratings_unsupported',
    reason_label: 'Episode ratings are unsupported by this integration',
  };
  assert.equal(context.describe(event).title, 'Rating skipped · Episode ratings are unsupported by this integration');
  assert.equal(context.describe(event).severity, 'info');
  assert.equal(context.describe({event_type: 'rating_skipped'}).title, 'Rating skipped');
});

test('Events captures its caller and keeps it when opened again',async()=>{
  const source=await readFile(new URL('../assets/js/modals.js',import.meta.url),'utf8');
  const script=source.slice(source.indexOf('window.openEvents ='),source.indexOf('window.openExporter ='));
  const location=new URL('http://localhost/?main=1#settings/sync');
  const context={location,URLSearchParams,ModalRegistry:{close(){}},document:{getElementById:()=>({})},window:{showTab(){}},history:{pushState(_state,_title,hash){location.hash=hash;}}};
  vm.runInNewContext(script,context);
  context.window.openEvents({runId:'run-one',returnContext:{modal:'statistics',day:20704}});
  let query=new URLSearchParams(location.hash.split('?')[1]);
  assert.equal(query.get('returnTo'),'/?main=1#settings/sync');
  assert.equal(query.get('runId'),'run-one');
  assert.equal(query.get('returnContext'),null);
  context.window.openEvents({groupId:'42'});
  query=new URLSearchParams(location.hash.split('?')[1]);
  assert.equal(query.get('returnTo'),'/?main=1#settings/sync');
  assert.equal(query.get('groupId'),'42');
});

test('Events safely labels callers',()=>{
  const current='http://localhost/#events';
  assert.deepEqual(pageBackLink('/#settings/sync',current,'events'),{href:'#settings/sync',label:'Sync pairs'});
  assert.deepEqual(pageBackLink('/profile',current,'events'),{href:'/profile',label:'Profile'});
  for(const url of ['https://example.com/','javascript:alert(1)','/#events?runId=one'])assert.equal(pageBackLink(url,current,'events').href,'#main');
});

test('Events returns to the settings pane',async()=>{
  const calls=[];
  globalThis.location=new URL('http://localhost/#events');
  globalThis.history={pushState(_state,_title,href){location.href=href;}};
  globalThis.window={async showTab(tab){calls.push([tab,window.__cwSettingsPane]);}};
  try {
    await returnFromEvents({returnTo:'/#settings/sync'});
    assert.deepEqual(calls,[['settings','sync']]);
    assert.equal(location.hash,'#settings/sync');
  } finally {delete globalThis.window;delete globalThis.location;delete globalThis.history;}
});

test('a return across pages navigates to the destination',async()=>{
  globalThis.location=new URL('http://localhost/?main=1#events');
  globalThis.window={};
  try {
    await returnFromEvents({returnTo:'/profile'});
    assert.equal(location.href,'http://localhost/profile');
  } finally {delete globalThis.window;delete globalThis.location;}
});

test('Events retains the view on return and isolates a new route or profile', async () => {
  const calls = [];
  const listeners = new Map();
  const root = { children: [], replaceChildren() { this.children = []; } };
  globalThis.window = {
    location: { hash: '#events' },
    CW: { OverviewProfile: { id: 'alice' }, AuthState: { read: () => ({ user: { id: 'admin' }, isManaged: false }) } },
    addEventListener(name, handler) { listeners.set(name, handler); },
  };
  globalThis.document = { documentElement: { dataset: { tab: 'events' } } };
  globalThis.eventsViewFixture = {
    async mount(host, props) { host.children = ['events']; calls.push(['mount', props]); },
    show() { calls.push(['show']); },
    hide() { calls.push(['hide']); },
    unmount() { calls.push(['unmount']); },
  };
  const source = (await readFile(new URL('../assets/js/events/page.js', import.meta.url), 'utf8'))
    .replace(/const viewURL =[\s\S]*?await import\(viewURL.href\);/, 'const EventsView = globalThis.eventsViewFixture;');
  const { default: page, readEventsRoute } = await import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`);
  assert.equal(readEventsRoute('#events?runId=sync%2Fone&domain=sync').runId, 'sync/one');
  assert.equal(readEventsRoute('#events?groupId=invalid%22id').groupId, '');
  await page.mount(root);
  page.hide();
  await page.mount(root);
  assert.deepEqual(calls.map(c => c[0]), ['mount', 'hide', 'show']);

  window.location.hash = '#events?groupId=17&visibility=all';
  await page.mount(root);
  assert.equal(calls.at(-1)[1].groupId, '17');

  document.documentElement.dataset.tab = 'main';
  window.CW.OverviewProfile.id = 'bob';
  listeners.get('cw:overview-profile-changed')();
  assert.equal(root.children.length, 0, 'old profile data is removed while the page is hidden');
  assert.equal(calls.at(-1)[0], 'unmount');
  document.documentElement.dataset.tab = 'events';
  await page.mount(root);
  assert.equal(calls.at(-1)[0], 'mount');
  assert.equal(calls.filter(c => c[0] === 'mount').length, 3);
  page.unmount();
  delete globalThis.eventsViewFixture;
  delete globalThis.window;
  delete globalThis.document;
});

test('sync runs are grouped per pair with quiet runs folded', async () => {
  globalThis.window ??= {};
  const {runGroups, runRowsHTML} = await import('../assets/js/events/titles.js');
  const pair = (src, dst, extra = {}) => ({src_provider: src, src_instance: 'default', dst_provider: dst, dst_instance: 'default', feature: 'watchlist', ...extra});
  const runs = [
    {run_id: 'late', started_at: 300, changes: 0, pair_rows: [], plans: [pair('SIMKL', 'TRAKT'), pair('TRAKT', 'SIMKL')]},
    {run_id: 'busy', started_at: 200, changes: 4, pair_rows: [pair('PLEX', 'CROSSWATCH', {added: 4})], plans: [pair('PLEX', 'CROSSWATCH'), pair('SIMKL', 'TRAKT')]},
    {run_id: 'old', started_at: 100, changes: 0, pair_rows: [], plans: []},
  ];
  const groups = runGroups(runs);
  assert.deepEqual(groups.map((g) => [g.key ? `${g.src}>${g.dst}` : 'other', g.dirs.size, g.runs.map((e) => `${e.run.run_id}:${e.active}`)]), [
    ['PLEX>CROSSWATCH', 1, ['busy:true']],
    ['SIMKL>TRAKT', 2, ['late:false', 'busy:false']],
    ['other', 0, ['old:false']],
  ]);
  const closed = runRowsHTML(runs);
  assert.equal((closed.match(/class="ev-rrow/g) || []).length, 1);
  assert.match(closed, /2 runs without changes/);
  const open = runRowsHTML(runs, {open: new Set([groups[1].key])});
  assert.equal((open.match(/class="ev-rrow/g) || []).length, 3);
  assert.match(open, /aria-expanded="true"/);
});
