/* tests/wetrakr-surfaces.test.mjs */
/* CrossWatch - WeTrakr cleanup selection and Editor identity regression tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync, existsSync} from 'node:fs';
import vm from 'node:vm';
import {correctedItem} from '../assets/js/interactive-sync-mapping.js';

const read = path => readFileSync(new URL(`../${path}`, import.meta.url), 'utf8');

test('manual mapping accepts WeTrakr show IDs through the input handler', () => {
  const source = read('assets/js/interactive-sync-mapping.js');
  const handlers = {};
  const drafts = [{item: {type: 'episode', season: 1, episode: 2, ids: {wetrakr: 'old-episode'}, show_ids: {wetrakr: 'old-show'}}}];
  const context = {drafts, correctedItem, changed: draft => {draft.dirty = true;}, dialog: {addEventListener: (name, fn) => {handlers[name] = fn;}}};
  vm.createContext(context);
  vm.runInContext(source.slice(source.indexOf('const ID_FIELDS'), source.indexOf('const icon')), context);
  vm.runInContext(source.slice(source.indexOf('  dialog.addEventListener("input"'), source.indexOf('  dialog.addEventListener("change"')), context);
  handlers.input({target: {dataset: {field: 'wetrakr'}, value: '1391953', closest: () => ({dataset: {draft: '0'}})}});
  assert.equal(drafts[0].dirty, true);
  assert.deepEqual(drafts[0].item.show_ids, {wetrakr: '1391953'});
  assert.equal(drafts[0].item.episode, 2);
});

test('cleanup shows configured WeTrakr profiles, branding and only supported features', () => {
  const context = {window: {}};
  vm.createContext(context);
  vm.runInContext(read('assets/helpers/provider-meta.js'), context);
  const source = read('assets/js/modals/provider-cleanup/index.js');
  vm.runInContext(source.slice(0, source.indexOf('function injectCss()')), context);
  const provider = {id: 'WETRAKR', label: 'WeTrakr', configured: true,
    instances: [{id: 'P01', label: 'Test account', configured: true}],
    cleanup_features: {watchlist: true, history: true, ratings: true, progress: true, collection: false}};
  assert.equal(context.configuredProviders([provider, {...provider, configured: false}]).length, 1);
  assert.equal(context.providerLabel(provider), 'WeTrakr');
  assert.deepEqual(Array.from(context.providerFeatures(provider), row => row.key).sort(), ['history', 'progress', 'ratings', 'watchlist']);
  assert.equal(context.providerInstances(provider)[0].id, 'P01');
  const logo = context.providerLogo(provider);
  assert.match(logo, /WETRAKR.*\.svg/i);
  assert.ok(existsSync(new URL(`../${logo.replace(/^\//, '')}`, import.meta.url)));
});

test('Editor load and save preserve WeTrakr episode and rewatch identities', () => {
  const context = {window: {}};
  vm.createContext(context);
  vm.runInContext(read('assets/js/editor/rows.js'), context);
  vm.runInContext(read('assets/js/editor/persistence.js'), context);
  const key = 'wetrakr:1391953#s01e01@1790000000~play-2';
  const original = {type: 'episode', title: 'Pilot', year: null, season: 1, episode: 1,
    ids: {wetrakr: '174658', tmdb: '62085'}, show_ids: {wetrakr: '1391953', tmdb: '1396'},
    watched_at: '2026-09-21T14:13:20Z', provider_event_id: 'play-2'};
  const editor = context.window.CW.Editor;
  const rows = editor.Rows.buildRows({[key]: original});
  const result = editor.Persistence.buildSaveData({state: {rows, kind: 'history', source: 'snapshot'}});
  assert.deepEqual(JSON.parse(JSON.stringify(result.items[key])), original);
  assert.equal(editor.Rows.viewingBaseKey(key), 'wetrakr:1391953#s01e01');
});
