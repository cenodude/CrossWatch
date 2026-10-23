/* tests/dashboard-mobile-layout.test.mjs */
/* CrossWatch dashboard layout storage isolation and recovery tests. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude */
import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const source = readFileSync(new URL('../assets/js/dashboard-widgets.js', import.meta.url), 'utf8');
const desktopKey = 'cw.dashboardWidgets.layout.v3';
const mobileKey = `${desktopKey}.mobile.v1`;

function session(storage, mobile, profile = false) {
  const context = vm.createContext({
    document: {
      readyState: 'loading',
      getElementById: id => profile && id === 'profile-hero' ? {} : null,
      querySelector: () => null,
      addEventListener() {},
    },
    window: {matchMedia: () => ({matches: mobile}), dispatchEvent() {}},
    localStorage: {getItem: key => storage.get(key) ?? null, setItem: (key, value) => storage.set(key, value)},
    CustomEvent: class {},
  });
  vm.runInContext(source.replace(/\}\)\(\);\s*$/, 'window.layoutTest = {readLayout, saveLayout, resetLayout};})();'), context);
  return context.window.layoutTest;
}

test('mobile starts with the existing layout and saves independently of desktop', () => {
  const stored = JSON.stringify({history: {hidden: true}, scrobble: {hidden: false}});
  const storage = new Map([[desktopKey, stored]]);
  const mobile = session(storage, true);
  const layout = mobile.readLayout();
  assert.equal(layout.history.hidden, true);
  layout.scrobble.hidden = true;
  mobile.saveLayout(layout);
  assert.equal(storage.get(desktopKey), stored);
  assert.equal(session(storage, true).readLayout().scrobble.hidden, true);
  assert.equal(session(storage, false).readLayout().scrobble.hidden, false);
});

test('reset recovers all hidden mobile widgets without changing desktop', () => {
  const storage = new Map();
  const mobile = session(storage, true);
  const layout = mobile.readLayout();
  for (const row of Object.values(layout)) row.hidden = true;
  mobile.saveLayout(layout);
  mobile.resetLayout();
  assert(Object.values(session(storage, true).readLayout()).every(row => !row.hidden));
  assert.equal(storage.has(desktopKey), false);
  assert.equal(storage.has(mobileKey), true);
});

test('invalid stored mobile data falls back to usable defaults', () => {
  const storage = new Map([[mobileKey, '{broken']]);
  assert.equal(session(storage, true).readLayout().history.hidden, false);
});

test('profile mobile preferences remain separate from the main dashboard', () => {
  const storage = new Map();
  const profile = session(storage, true, true);
  const layout = profile.readLayout();
  assert.equal(layout.watchlist, undefined);
  layout.history.hidden = true;
  profile.saveLayout(layout);
  assert.equal(session(storage, true).readLayout().history.hidden, false);
  assert.equal(session(storage, true, true).readLayout().history.hidden, true);
});
