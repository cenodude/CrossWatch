/* tests/events-page.test.mjs */
/* CrossWatch - Events navigation and profile isolation tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

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
