/* tests/import-export-navigation.test.mjs */
/* CrossWatch - Import and Export caller navigation tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import {pageBackLink} from '../assets/js/page-return.js';

const modals = readFileSync(new URL('../assets/js/modals.js', import.meta.url), 'utf8');
const importer = readFileSync(new URL('../assets/js/import-export/index.js', import.meta.url), 'utf8');
const interactive = readFileSync(new URL('../assets/js/interactive-sync.js', import.meta.url), 'utf8');

function fixture(href, mounted = true) {
  class BrowserLocation extends URL {
    get href() { return super.href; }
    set href(value) { super.href = new URL(value, super.href).href; }
  }
  const location = new BrowserLocation(href), navigations = [];
  const link = {setAttribute(key, value) { this[key] = value; }};
  const host = {querySelector: () => link};
  const context = {
    location, URLSearchParams, pageBackLink,
    window: {location, showTab: tab => navigations.push(tab)},
    document: {getElementById: () => mounted ? {} : null},
    history: {pushState(_state, _title, hash) { location.hash = hash; }},
    ModalRegistry: {close() {}},
  };
  vm.createContext(context);
  vm.runInContext(modals.slice(modals.indexOf('window.openExporter ='), modals.indexOf('window.openMaintenance =')), context);
  vm.runInContext(importer.slice(importer.indexOf('const esc ='), importer.indexOf('const human =')) +
    importer.slice(importer.indexOf('function updateBackLink('), importer.indexOf('let active;')), context);
  return {context, location, navigations, link, host, back() { context.updateBackLink(host); return link; }};
}

test('recovery shortcut retains the exact sync report and selected Plex instance', () => {
  const caller = 'http://localhost/?main=1#interactive_sync?session=report-123';
  const f = fixture(caller);
  f.context.session = {pair: {source: 'SIMKL', target: 'PLEX', target_instance: 'second'}};
  f.context.name = 'recover-plex';
  const start = interactive.indexOf('    if (name === "recover-plex")');
  const end = interactive.indexOf('    if (name === "refresh")', start);
  vm.runInContext('(function () {' + interactive.slice(start, end) + '})()', f.context);
  const route = new URLSearchParams(f.location.hash.split('?')[1]);
  assert.equal(route.get('recovery'), '1');
  assert.equal(route.get('returnTo'), '/?main=1#interactive_sync?session=report-123');
  assert.equal(f.context.window.CW.pendingPlexRecovery.source_instance, 'second');
  assert.deepEqual(f.navigations, ['import_export']);
  assert.equal(f.back().href, '#interactive_sync?session=report-123');
  assert.match(f.link.innerHTML, /Interactive Sync$/);
  assert.equal(new URL(f.link.href, f.location.href).href, caller);
  const refreshed = fixture(f.location.href);
  assert.equal(refreshed.back().href, f.link.href);
});

test('reopening Import and Export preserves its original caller without a return loop', () => {
  const f = fixture('http://localhost/#settings/maintenance');
  f.context.window.openExporter();
  f.context.window.openExporter({recovery: true});
  f.context.window.openExporter();
  assert.equal(f.back().href, '#settings/maintenance');
  assert.match(f.link.innerHTML, /Maintenance$/);
  assert.equal(new URLSearchParams(f.location.hash.split('?')[1]).get('recovery'), '1');
});

test('navigation from another shell preserves the caller and opens the recovery tab', () => {
  const f = fixture('http://localhost/profile', false);
  f.context.window.openExporter({recovery: true});
  assert.equal(f.location.pathname, '/');
  assert.equal(f.location.search, '?main=1');
  assert.equal(f.back().href, '/profile');
  assert.match(f.link.innerHTML, /Profile$/);
});

test('direct recovery links and invalid return destinations use Main', () => {
  for (const returnTo of ['', 'https://example.com/', 'javascript:alert(1)', '/#import_export?recovery=1']) {
    const query = new URLSearchParams({recovery: '1', returnTo});
    const f = fixture('http://localhost/#import_export?' + query);
    assert.equal(f.back().href, '#main');
    assert.match(f.link.innerHTML, /Main$/);
  }
});

test('an already mounted page updates the back link when its caller changes', async () => {
  const f = fixture('http://localhost/#import_export?' + new URLSearchParams({returnTo: '/#editor'}));
  f.context.active = {host: f.host};
  const start = importer.indexOf('  async mount(host)');
  const end = importer.indexOf('    this.unmount();', start);
  vm.runInContext('globalThis.remount = async function(host) {' + importer.slice(importer.indexOf('\n', start), end) + '};', f.context);
  await f.context.remount(f.host);
  assert.equal(f.link.href, '#editor');
  f.location.hash = '#import_export?' + new URLSearchParams({returnTo: '/#interactive_sync?session=another'});
  await f.context.remount(f.host);
  assert.equal(f.link.href, '#interactive_sync?session=another');
});
