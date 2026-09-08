/* tests/events-page.test.mjs */
/* CrossWatch - Events navigation and profile isolation tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
import {pageBackLink,statisticsReturn,returnFromEvents,resumeEventsReturn} from '../assets/js/page-return.js';

test('Events captures its caller and keeps Sync activity context when opened again',async()=>{
  const source=await readFile(new URL('../assets/js/modals.js',import.meta.url),'utf8');
  const script=source.slice(source.indexOf('window.openEvents ='),source.indexOf('window.openStatisticsModal ='));
  const location=new URL('http://localhost/?main=1#settings/sync');
  const context={location,URLSearchParams,statisticsReturn,ModalRegistry:{close(){}},document:{getElementById:()=>({})},window:{showTab(){}},history:{pushState(_state,_title,hash){location.hash=hash;}}};
  vm.runInNewContext(script,context);
  const restore={modal:'statistics',day:20704,range:'6m',metric:'runs',filter:'Plex',expanded:['run-one']};
  context.window.openEvents({runId:'run-one',returnContext:restore});
  let query=new URLSearchParams(location.hash.split('?')[1]);
  assert.equal(query.get('returnTo'),'/?main=1#settings/sync');
  assert.deepEqual(JSON.parse(query.get('returnContext')),restore);
  context.window.openEvents({groupId:'42'});
  query=new URLSearchParams(location.hash.split('?')[1]);
  assert.equal(query.get('returnTo'),'/?main=1#settings/sync');
  assert.deepEqual(JSON.parse(query.get('returnContext')),restore);
});

test('Events safely labels callers and only accepts the supported return modal',()=>{
  const current='http://localhost/#events';
  assert.deepEqual(pageBackLink('/#settings/sync',current,'events'),{href:'#settings/sync',label:'Sync pairs'});
  assert.deepEqual(pageBackLink('/profile',current,'events'),{href:'/profile',label:'Profile'});
  for(const url of ['https://example.com/','javascript:alert(1)','/#events?runId=one'])assert.equal(pageBackLink(url,current,'events').href,'#main');
  assert.equal(statisticsReturn('{invalid'),null);
  assert.equal(statisticsReturn({modal:'maintenance'}),null);
  assert.deepEqual(statisticsReturn({modal:'statistics',day:-1,range:'bad',metric:'bad'}),{modal:'statistics',day:null,range:'12m',metric:'changes',filter:'',expanded:[]});
});

test('Events returns to the settings pane before reopening Sync activity',async()=>{
  const calls=[];
  globalThis.location=new URL('http://localhost/#events');
  globalThis.history={pushState(_state,_title,href){location.href=href;}};
  globalThis.window={async showTab(tab){calls.push([tab,window.__cwSettingsPane]);},async openStatisticsModal(props){calls.push(props);}};
  const restore={modal:'statistics',day:20704,range:'3m',metric:'failed',filter:'SIMKL',expanded:['one']};
  try {
    await returnFromEvents({returnTo:'/#settings/sync',returnContext:JSON.stringify(restore)});
    assert.deepEqual(calls,[['settings','sync'],restore]);
    assert.equal(location.hash,'#settings/sync');
  } finally {delete globalThis.window;delete globalThis.location;delete globalThis.history;}
});

test('a return across pages restores Sync activity once on the destination',async()=>{
  const saved=new Map(),opened=[];
  globalThis.location=new URL('http://localhost/?main=1#events');
  globalThis.sessionStorage={getItem:key=>saved.get(key),setItem:(key,value)=>saved.set(key,value),removeItem:key=>saved.delete(key)};
  globalThis.window={openStatisticsModal:props=>opened.push(props)};
  try {
    await returnFromEvents({returnTo:'/profile',returnContext:{modal:'statistics',day:20704}});
    assert.equal(location.href,'http://localhost/profile');
    resumeEventsReturn();resumeEventsReturn();
    assert.equal(opened.length,1);
    assert.equal(opened[0].day,20704);
  } finally {delete globalThis.window;delete globalThis.location;delete globalThis.sessionStorage;}
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
