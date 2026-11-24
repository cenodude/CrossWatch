/* tests/media-art-identity.test.mjs */
/* CrossWatch - Episode artwork and metadata show identity regression checks */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude */
import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import './i18n-vm-setup.mjs';

const metadataSource = readFileSync(new URL('../assets/helpers/media-meta.js', import.meta.url), 'utf8');
const dashboardSource = readFileSync(new URL('../assets/js/dashboard-widgets.js', import.meta.url), 'utf8');
const episode = {
  type: 'episode', title: 'VisionQuest', season: 1, episode: 2,
  key: 'tmdb:6412926#s01e02',
  ids: {tmdb: '6412926', imdb: 'tt38934465', tvdb: '12023758'},
};

function metadata() {
  const requests = [];
  const context = {
    window: {addEventListener() {}},
    document: {documentElement: {dataset: {}}, addEventListener() {}},
    fetch: async (url, options) => {
      requests.push(JSON.parse(options.body));
      return {ok: true, json: async () => ({results: {}})};
    },
  };
  vm.runInNewContext(metadataSource, context);
  return {api: context.window.CW.Meta, requests};
}

test('resolved parent IDs override an old episode-based canonical key', async () => {
  const {api, requests} = metadata();
  const item = {...episode, ids: {...episode.ids, show_ids: {tmdb: '213375', imdb: 'tt23112594', tvdb: '426606'}}};
  assert.equal(api.tmdbId(item), '213375');
  await api.get(item);
  assert.equal(requests[0].items[0].tmdb, '213375');
  assert.deepEqual(requests[0].items[0].ids, {imdb: 'tt23112594', tvdb: '426606'});
});

test('unresolved episode and season IDs are never requested as show metadata', async () => {
  const {api, requests} = metadata();
  for (const type of ['episode', 'season']) {
    const item = {...episode, type};
    assert.equal(api.tmdbId(item), '');
    assert.equal(await api.get(item), null);
  }
  assert.deepEqual(requests, []);
});

test('resolved widget IDs and canonical-only show coordinates remain usable', () => {
  const {api} = metadata();
  assert.equal(api.tmdbId({...episode, tmdb: '213375'}), '213375');
  assert.equal(api.tmdbId({type: 'episode', key: 'tmdb:213375#s01e02'}), '213375');
  assert.equal(api.tmdbId({type: 'movie', ids: {tmdb: '550'}}), '550');
});

test('dashboard backgrounds do not fall back to an unresolved episode ID', () => {
  const context = {
    window: {matchMedia: () => ({matches: false})}, navigator: {language: 'en-US'},
    document: {readyState: 'loading', documentElement: {dataset: {}}, addEventListener() {}, querySelector() { return null; }, getElementById() { return null; }},
  };
  const source = dashboardSource.replace(
    '  if (document.readyState === "loading")',
    '  window.artForTest = {backdropPoster, poster, getDetailMeta};\n  if (document.readyState === "loading")',
  );
  vm.runInNewContext(source, context);
  const art = context.window.artForTest;
  assert.equal(art.backdropPoster(episode), '');
  assert.match(art.poster(episode), /placeholder_poster/);
  assert.match(art.backdropPoster({...episode, tmdb: '213375'}), /\/tv\/213375\?/);
});
