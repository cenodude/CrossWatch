/* tests/assistant-modal.test.mjs */
/* CrossWatch - Assistant chat modal rendering and lifecycle tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {runInNewContext} from 'node:vm';

const pending = [];
let ready = true;
globalThis.window = {setTimeout, clearTimeout};
globalThis.document = {getElementById: () => ({}), querySelector: () => null};
globalThis.fetch = async url => {
  if (String(url).endsWith('/settings')) return {ok: true, json: async () => ({ok: true, ready, model: 'test-model'})};
  return new Promise(resolve => pending.push(body => resolve({ok: body.ok !== false, json: async () => body})));
};
const modal = await import('../assets/js/modals/assistant/index.js');
const {linkify, lookedAt, renderAnswer, actionButtons, ACTIONS} = modal;

test('proposal cards only render known actions and escape titles and status', () => {
  const html = modal.proposalCards([
    {action: 'run_pair', title: '<img src=x onerror=alert(1)>', params: {pair_id: 'p1'}},
    {action: 'constructor', title: 'bad'},
    {action: '__proto__', title: 'bad'},
    {action: 'delete', title: 'bad'},
    {action: 'test_connections', title: 'Test', status: 'dismissed'},
    {action: 'create_capture', title: 'Capture', status: 'failed', message: '<script>error</script>'},
  ]);
  assert.equal((html.match(/data-as-proposal="/g) || []).length, 3);
  assert.ok(html.includes('&lt;img src=x onerror=alert(1)&gt;'));
  assert.ok(html.includes('&lt;script&gt;error&lt;/script&gt;'));
  assert.ok(html.includes('data-state="dismissed"'));
  assert.doesNotMatch(html, /<img|<script|>bad</i);
  for (const tag of ['SCRIPT', 'ScRiPt', 'IMG', 'ImG']) {
    const output = modal.proposalCards([{action: 'run_pair', title: `<${tag}>`, status: 'failed', message: `<${tag}>`}]);
    assert.equal(output.split(`&lt;${tag}&gt;`).length - 1, 2);
    assert.doesNotMatch(output, /<img|<script/i);
  }
  assert.equal(modal.proposalCards(undefined), '');
});

test('pair proposals never cancel a running sync and can only run once', async () => {
  const calls = [];
  window.runSync = opts => { calls.push(opts); return {ok: true}; };
  window.showTab = tab => calls.push(tab);
  window.confirm = () => { throw new Error('No extra confirmation'); };
  window.syncBar = {isRunning: () => true};
  const blocked = {action: 'run_pair', params: {pair_id: 'p1'}};
  await modal.runProposal(blocked);
  assert.equal(blocked.status, 'failed');
  assert.equal(blocked.message, 'A sync is already running');
  assert.deepEqual(calls, []);
  window.syncBar = {isRunning: () => false};
  const proposal = {action: 'run_pair', params: {pair_id: 'p1'}};
  await Promise.all([modal.runProposal(proposal), modal.runProposal(proposal)]);
  await modal.runProposal(proposal);
  assert.deepEqual(calls, ['main', {pair_id: 'p1', startOnly: true}]);
  assert.equal(proposal.status, 'done');
  assert.equal(proposal.message, 'Sync started');
});

test('pair cards report refused, skipped and missing results as failures', async () => {
  window.syncBar = {isRunning: () => false};
  for (const [payload, message] of [
    [{ok: false, error: 'Pair disabled'}, 'Pair disabled'],
    [{ok: true, skipped: 'no_pairs_configured'}, 'Sync skipped: no_pairs_configured'],
    [undefined, 'Sync did not start'],
  ]) {
    window.runSync = async () => payload;
    const proposal = {action: 'run_pair', params: {pair_id: 'p1'}};
    await modal.runProposal(proposal);
    assert.equal(proposal.status, 'failed');
    assert.equal(proposal.message, message);
  }
});

test('runSync returns outcomes and start-only calls never cancel existing runs', async () => {
  const source = await readFile(new URL('../assets/helpers/core.js', import.meta.url), 'utf8');
  const body = source.slice(source.indexOf('  async function runSync(opts)'), source.indexOf('  async function cancelSync()'));
  let cancelled = 0;
  let requests = 0;
  let response = {ok: true, json: async () => ({ok: true})};
  const context = {
    UI: {summary: {running: true}}, window: {syncBar: {isRunning: () => false}}, state: {busy: false},
    cancelSync: () => { cancelled++; }, cwCloseSyncMenu() {}, clearDetailsLogBeforeRun() {},
    setBusy(value) { context.state.busy = value; }, setSyncHeader() {}, resetSyncButtons() {},
    recomputeRunDisabled() {}, AUTO_STATUS: false,
    fetch: async (url, init) => {
      requests++;
      assert.equal(url, '/api/run');
      assert.deepEqual(JSON.parse(init.body), {pair_id: 'p1'});
      if (response instanceof Error) throw response;
      return response;
    },
  };
  const run = runInNewContext(`${body}\nrunSync`, context);
  assert.equal((await run({pair_id: 'p1', startOnly: true})).error, 'A sync is already running');
  assert.equal(cancelled, 0);
  await run('p1');
  assert.equal(cancelled, 1);
  context.UI.summary.running = false;
  context.window.syncBar.isRunning = () => true;
  assert.equal((await run({pair_id: 'p1', startOnly: true})).ok, false);
  assert.equal(cancelled, 1);
  context.window.syncBar.isRunning = () => false;
  context.state.busy = true;
  assert.match((await run({pair_id: 'p1', startOnly: true})).error, /busy/);
  assert.equal(requests, 0);
  context.state.busy = false;
  for (const payload of [{ok: true}, {ok: true, skipped: 'no_pairs_configured'}, {ok: false, error: 'Pair disabled'}]) {
    response = {ok: true, json: async () => payload};
    assert.equal(JSON.stringify(await run({pair_id: 'p1', startOnly: true})), JSON.stringify(payload));
    assert.equal(context.state.busy, false);
  }
  response = {ok: false, json: async () => ({ok: true})};
  assert.equal((await run('p1')).ok, false);
  response = {ok: true, json: async () => { throw new Error('Invalid JSON'); }};
  assert.equal((await run('p1')).ok, false);
  response = new Error('Offline');
  assert.equal((await run('p1')).error, 'Failed to reach server');
});

test('capture proposals poll progress and send only the intended parameters', async () => {
  const previousFetch = globalThis.fetch;
  const previousTimeout = window.setTimeout;
  const calls = [];
  const replies = [{ok: true, progress_id: 'job/1'}, {progress: {done: false, percent: 45}}, {progress: {done: true, ok: true}}];
  const proposal = {action: 'create_capture', params: {provider: 'TRAKT', instance: 'default', feature: 'history', label: 'evil', force: true}};
  window.setTimeout = (fn, delay) => setTimeout(fn, delay === 1500 ? 0 : delay);
  globalThis.fetch = async (url, options) => {
    calls.push({url, options, message: proposal.message});
    return {ok: true, json: async () => replies.shift()};
  };
  try {
    await modal.runProposal(proposal);
    assert.equal(proposal.status, 'done');
    assert.equal(proposal.message, 'Capture saved');
    assert.deepEqual(JSON.parse(calls[0].options.body), {provider: 'TRAKT', instance: 'default', feature: 'history', label: 'Ask CrossWatch', background: true});
    assert.equal(calls[1].url, '/api/snapshots/capture-progress/job%2F1');
    assert.equal(calls[2].message, 'Capturing… 45%');
    assert.ok(calls.every(c => c.options.credentials === 'same-origin' && c.options.cache === 'no-store'));
  } finally {
    globalThis.fetch = previousFetch;
    window.setTimeout = previousTimeout;
  }
});

test('proposal failures are escaped and cannot run again', async () => {
  const previousFetch = globalThis.fetch;
  let count = 0;
  globalThis.fetch = async () => { count++; return {ok: false, json: async () => ({error: '<img src=x onerror=alert(1)>'})}; };
  try {
    const proposal = {action: 'refresh_anime_mapping', title: 'Update anime mapping', params: {}};
    await modal.runProposal(proposal);
    await modal.runProposal(proposal);
    assert.equal(count, 1);
    assert.equal(proposal.status, 'failed');
    assert.match(modal.proposalCards([proposal]), /&lt;img/);
    assert.doesNotMatch(modal.proposalCards([proposal]), /<img/);
  } finally { globalThis.fetch = previousFetch; }
});

test('mapping and connection proposals use the actual endpoint result shapes', async () => {
  const previousFetch = globalThis.fetch;
  const calls = [];
  const replies = [{updated: true, identity_updated: true, mappings_updated: false}, {rebuilt: true}, {}, {providers: {PLEX: {connected: true}, TRAKT: {connected: false, reason: '<expired token>'}, JELLYFIN: {connected: false}}}];
  globalThis.fetch = async (url, options) => { calls.push({url, options}); return {ok: true, json: async () => replies.shift()}; };
  try {
    for (const expected of ['Anime identity data updated', 'Anime mapping index rebuilt', 'Anime mapping already up to date']) {
      const p = {action: 'refresh_anime_mapping'};
      await modal.runProposal(p);
      assert.equal(p.message, expected);
    }
    const p = {action: 'test_connections'};
    await modal.runProposal(p);
    assert.equal(p.message, 'PLEX: connected\nTRAKT: not connected (<expired token>)\nJELLYFIN: not connected');
    assert.match(modal.proposalCards([p]), /&lt;expired token&gt;/);
    assert.deepEqual(JSON.parse(calls[0].options.body), {force: false});
    assert.equal(calls[3].url, '/api/status?fresh=1');
    assert.equal(calls[3].options.body, undefined);
  } finally { globalThis.fetch = previousFetch; }
});

function makeRoot() {
  const nodes = {};
  const node = selector => nodes[selector] ||= {
    value: '', disabled: false, textContent: '', innerHTML: '', scrollTop: 0, scrollHeight: 0, listeners: {},
    focus() {}, addEventListener(type, fn) { this.listeners[type] = fn; },
  };
  return {
    node,
    root: {
      innerHTML: '', listeners: {},
      closest: () => ({classList: {add() {}}}),
      querySelector: node,
      addEventListener(type, fn) { this.listeners[type] = fn; },
    },
  };
}

const settle = () => new Promise(resolve => setTimeout(resolve, 5));
const click = (view, selector) => view.root.listeners.click({target: {closest: wanted => (wanted === selector ? {} : null)}});

async function submit(view, text) {
  view.node('#as-input').value = text;
  view.node('#as-form').listeners.submit({preventDefault() {}});
  await settle();
}

test('model output is escaped and only wiki addresses become links', () => {
  const html = linkify('Fix it <img src=x onerror=alert(1)> at https://wiki.crosswatch.app/a/b. Done');
  assert.ok(!html.includes('<img'));
  assert.ok(html.includes('&lt;img src=x onerror=alert(1)&gt;'));
  assert.ok(html.includes('<a href="https://wiki.crosswatch.app/a/b" target="_blank" rel="noopener noreferrer">https://wiki.crosswatch.app/a/b</a>. Done'));
  for (const text of [
    'https://evil.test/?leak=tmdb603',
    'http://wiki.crosswatch.app/a',
    'https://wiki.crosswatch.app.evil.test/a',
    'https://wiki.crosswatch.app@evil.test/a',
    'https://wiki.crosswatch.app:8443/a',
    'javascript:alert(1)',
  ]) assert.ok(!linkify(text).includes('<a '), text);
  assert.ok(!linkify('https://wiki.crosswatch.app/"><script>').includes('<script>'));
  assert.ok(linkify('See https://github.com/cenodude/CrossWatch/blob/v0.13.5/api/mcpAPI.py#L7 now').includes('<a href="https://github.com/cenodude/CrossWatch/blob/v0.13.5/api/mcpAPI.py#L7"'));
  for (const text of ['https://github.com/evil/CrossWatch/x', 'https://github.com/cenodude/CrossWatch-evil/x', 'https://github.com/cenodude/CrossWatchX']) assert.ok(!linkify(text).includes('<a '), text);
  assert.equal(linkify(null), '');
});

test('the looked-at line names each area once and skips unknown tools', () => {
  assert.equal(lookedAt(['get_health', 'read_logs', 'read_log_context', 'nope', 'search_docs', 'read_doc', 'search_code', 'read_code']), 'Looked at: health, logs, wiki, source code');
  assert.equal(lookedAt([]), '');
  assert.equal(lookedAt(undefined), '');
});

test('an answer that arrives after closing and reopening lands in the open window', async () => {
  const first = makeRoot();
  await modal.default.mount(first.root);
  assert.equal(first.node('#as-input').disabled, false);
  assert.match(first.node('#as-log').innerHTML, /data-as-ask/);

  await submit(first, 'why did it fail?');
  assert.equal(pending.length, 1);
  assert.equal(first.node('#as-send').disabled, true);
  assert.match(first.node('#as-log').innerHTML, /as-wait/);

  modal.default.unmount();
  const second = makeRoot();
  await modal.default.mount(second.root);
  assert.equal(second.node('#as-send').disabled, true);
  assert.match(second.node('#as-log').innerHTML, /as-wait/);
  await submit(second, 'ignored while busy');
  assert.equal(pending.length, 1);

  pending.shift()({ok: true, reply: 'The Trakt token expired.', tools: ['read_logs']});
  await settle();
  assert.equal(second.node('#as-send').disabled, false);
  assert.equal(second.node('#as-input').disabled, false);
  assert.match(second.node('#as-log').innerHTML, /The Trakt token expired\./);
  assert.match(second.node('#as-log').innerHTML, /Looked at: logs/);
  assert.doesNotMatch(second.node('#as-log').innerHTML, /as-wait|ignored while busy/);
});

test('an answer that arrives while the window is closed is kept, and errors are not resent', async () => {
  const first = makeRoot();
  await modal.default.mount(first.root);
  await submit(first, 'second question');
  modal.default.unmount();
  pending.shift()({ok: false, error: 'The AI provider did not answer in time'});
  await settle();

  const second = makeRoot();
  await modal.default.mount(second.root);
  assert.equal(second.node('#as-send').disabled, false);
  assert.match(second.node('#as-log').innerHTML, /as-error/);
  assert.match(second.node('#as-log').innerHTML, /did not answer in time/);

  let sent = null;
  const realFetch = globalThis.fetch;
  globalThis.fetch = async (url, opts) => {
    if (opts?.body) sent = JSON.parse(opts.body);
    return realFetch(url, opts);
  };
  await submit(second, 'third question');
  globalThis.fetch = realFetch;
  assert.deepEqual(sent.messages.map(m => m.content), ['why did it fail?', 'The Trakt token expired.', 'second question', 'third question']);
  pending.shift()({ok: true, reply: 'Done.', tools: []});
  await settle();

  click(second, '#as-new');
  assert.match(second.node('#as-log').innerHTML, /data-as-ask/);
});

test('without a provider the window points to settings and stays locked', async () => {
  ready = false;
  modal.default.unmount();
  const view = makeRoot();
  await modal.default.mount(view.root);
  assert.match(view.node('#as-log').innerHTML, /not set up yet/);
  assert.equal(view.node('#as-input').disabled, true);
  await submit(view, 'hello');
  assert.equal(pending.length, 0);
});

test('answers get simple formatting while everything else stays escaped', () => {
  const html = renderAnswer([
    'Sync failed at **03:12**.',
    '',
    'What happened:',
    '- Trakt said `401`',
    '- 14 items <b>waiting</b>',
    '',
    '1. Reconnect',
    '2. Run again',
    '# Heading',
    'See https://wiki.crosswatch.app/a and https://evil.test/x',
  ].join('\n'));
  assert.equal(html, '<p>Sync failed at <strong>03:12</strong>.</p><p>What happened:</p><ul><li>Trakt said <code>401</code></li><li>14 items &lt;b&gt;waiting&lt;/b&gt;</li></ul><ol><li>Reconnect</li><li>Run again</li></ol><p>Heading<br>See <a href="https://wiki.crosswatch.app/a" target="_blank" rel="noopener noreferrer">https://wiki.crosswatch.app/a</a> and https://evil.test/x</p>');
  for (const nasty of ['<img src=x onerror=alert(1)>', '**<script>x</script>**', '`<svg onload=alert(1)>`', '- [x](javascript:alert(1))', '"><iframe>']) {
    const out = renderAnswer(nasty);
    assert.ok(!/<(img|script|svg|iframe)/i.test(out), out);
    assert.ok(!out.includes('href="javascript'), out);
  }
  assert.equal(renderAnswer(''), '');
});

test('only known actions become buttons', () => {
  const html = actionButtons(['open_logs', 'run_sync', 'open_logs', 'delete_everything', '__proto__', 'constructor', '<b>']);
  assert.deepEqual([...html.matchAll(/data-as-action="([^"]+)"/g)].map(m => m[1]), ['open_logs', 'run_sync']);
  assert.equal(actionButtons(['nope']), '');
  assert.equal(actionButtons(undefined), '');
  assert.deepEqual(Object.keys(ACTIONS).sort(), ['open_analyzer', 'open_connections', 'open_events', 'open_logs', 'open_pairs', 'open_scheduling', 'open_scrobbler', 'run_sync']);
});

test('run sync asks first and never starts a second run', () => {
  const calls = [];
  window.runSync = () => calls.push('run');
  window.showTab = () => {};
  window.syncBar = {isRunning: () => true};
  window._cwShowToast = message => calls.push(`toast:${message}`);
  window.confirm = () => { calls.push('asked'); return true; };
  ACTIONS.run_sync.run();
  window.syncBar = {isRunning: () => false};
  window.confirm = () => { calls.push('asked'); return false; };
  ACTIONS.run_sync.run();
  window.confirm = () => { calls.push('asked'); return true; };
  ACTIONS.run_sync.run();
  assert.deepEqual(calls, ['toast:A sync is already running', 'asked', 'asked', 'run']);
});

test('wiki pages read for the answer become links and nothing else does', () => {
  const html = modal.sourceLinks([
    {title: 'Scheduler <b>Webhooks</b>', url: 'https://wiki.crosswatch.app/a/scheduler-webhooks'},
    {title: 'Again', url: 'https://wiki.crosswatch.app/a/scheduler-webhooks'},
    {title: 'Evil', url: 'https://evil.test/a'},
    {title: 'Look-alike', url: 'https://wiki.crosswatch.app.evil.test/a'},
    {title: 'Quote', url: 'https://wiki.crosswatch.app/a"onmouseover="x'},
    {title: 'Script', url: 'javascript:alert(1)'},
    {title: '', url: 'https://wiki.crosswatch.app/b'},
    {title: 'Two', url: 'https://wiki.crosswatch.app/c'},
    {title: 'Four', url: 'https://wiki.crosswatch.app/d'},
  ]);
  assert.equal(html, '<small class="as-sources">Wiki: <a href="https://wiki.crosswatch.app/a/scheduler-webhooks" target="_blank" rel="noopener noreferrer">Scheduler &lt;b&gt;Webhooks&lt;/b&gt;</a>, <a href="https://wiki.crosswatch.app/b" target="_blank" rel="noopener noreferrer">https://wiki.crosswatch.app/b</a>, <a href="https://wiki.crosswatch.app/c" target="_blank" rel="noopener noreferrer">Two</a></small>');
  assert.equal(modal.sourceLinks(undefined), '');
  assert.equal(modal.sourceLinks([{url: 'https://evil.test'}]), '');
});

test('proposals wait for a click and keep dismissed and done states across reopening', async () => {
  ready = true;
  const view = makeRoot();
  await modal.default.mount(view.root);
  click(view, '#as-new');
  const calls = [];
  window.syncBar = {isRunning: () => false};
  window.runSync = opts => { calls.push(opts); return {ok: true}; };
  window.showTab = () => {};
  await submit(view, 'sync this pair');
  pending.shift()({ok: true, reply: 'Review these actions.', proposals: [
    {action: 'run_pair', title: 'Run sync: PLEX → TRAKT', params: {pair_id: 'p1'}},
    {action: 'test_connections', title: 'Test all connections', params: {}},
  ]});
  await settle();
  assert.deepEqual(calls, []);
  const message = view.node('#as-log').innerHTML.match(/data-as-message="(\d+)"/)[1];
  function proposalClick(root, index, button) {
    root.listeners.click({target: {closest: selector => {
      if (selector === `[data-as-proposal-${button}]`) return {};
      if (selector === '[data-as-proposal]') return {getAttribute: () => String(index)};
      if (selector === '[data-as-message]') return {getAttribute: () => message};
      return null;
    }}});
  }
  proposalClick(view.root, 1, 'dismiss');
  proposalClick(view.root, 1, 'run');
  proposalClick(view.root, 0, 'run');
  proposalClick(view.root, 0, 'run');
  await settle();
  assert.deepEqual(calls, [{pair_id: 'p1', startOnly: true}]);
  modal.default.unmount();
  const reopened = makeRoot();
  await modal.default.mount(reopened.root);
  assert.match(reopened.node('#as-log').innerHTML, /data-state="done"/);
  assert.match(reopened.node('#as-log').innerHTML, /data-state="dismissed"/);
  proposalClick(reopened.root, 0, 'run');
  assert.deepEqual(calls, [{pair_id: 'p1', startOnly: true}]);
});
