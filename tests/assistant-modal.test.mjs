/* tests/assistant-modal.test.mjs */
/* CrossWatch - Assistant chat modal rendering and lifecycle tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {runInNewContext} from 'node:vm';

const pending = [];
let ready = true;
let mode;
const fetched = [];
globalThis.window = {setTimeout, clearTimeout};
globalThis.document = {getElementById: () => ({}), querySelector: () => null};
globalThis.fetch = async url => {
  fetched.push(String(url));
  if (String(url).endsWith('/settings')) return {ok: true, json: async () => ({ok: true, ready, model: 'test-model', ...(mode ? {mode} : {})})};
  return new Promise(resolve => pending.push(body => resolve({ok: body.ok !== false, json: async () => body})));
};
const modal = await import('../assets/js/modals/assistant/index.js');
const {linkify, lookedAt, renderAnswer, actionButtons, ACTIONS} = modal;

test('model picker filters specialist names, preserves IDs and invalidates stale tests', async () => {
  const source = await readFile(new URL('../assets/js/assistant-settings.js', import.meta.url), 'utf8');
  const nodes = new Map();
  const listeners = {};
  const element = () => ({value: '', textContent: '', disabled: false, hidden: false, checked: false, children: [], dataset: {},
    appendChild(child) { this.children.push(child); }, focus() {}, classList: {toggle() {}}});
  const doc = {getElementById(id) { if (!nodes.has(id)) nodes.set(id, element()); return nodes.get(id); },
    createElement: element, addEventListener(name, fn) { listeners[name] = fn; }, querySelectorAll: () => []};
  let resolveTest;
  const requests = [];
  runInNewContext(source, {window: {}, document: doc, fetch: async (url, opts) => {
    requests.push({url, body: JSON.parse(opts.body)});
    return {ok: true, json: async () => url.endsWith('/models') ? {models: ['models/chat-preview', 'models/robotics-2-preview', 'text-embedding-3', 'local-model', 'models/pro-latest', 'models/flash-latest', 'models/preview-latest', 'local:latest']} : new Promise(resolve => { resolveTest = resolve; })};
  }});
  const click = id => listeners.click({target: {closest: selector => selector === `#${id}` ? {} : null}});
  doc.getElementById('assistant_base_url').value = 'http://local.test/v1';
  click('btn-assistant-models');
  await settle();
  let options = doc.getElementById('assistant_models').children;
  assert.deepEqual(options.map(o => o.value), ['models/flash-latest', 'local:latest', 'models/pro-latest', 'local-model', 'models/chat-preview', 'models/preview-latest']);
  assert.match(options[4].textContent, /^chat-preview · Preview$/);
  doc.getElementById('assistant_models_all').value = 'all';
  doc.getElementById('assistant_models').children = [];
  listeners.change({target: {id: 'assistant_models_all'}});
  assert.equal(doc.getElementById('assistant_models').children.length, 8);
  doc.getElementById('assistant_model').value = 'models/chat-preview';
  listeners.input({target: {id: 'assistant_model'}});
  assert.equal(doc.getElementById('assistant_model').value, 'models/chat-preview');
  click('btn-assistant-test');
  await settle();
  assert.equal(requests[1].body.model, 'models/chat-preview');
  doc.getElementById('assistant_model').value = 'different';
  listeners.input({target: {id: 'assistant_model'}});
  resolveTest({compatible: true});
  await settle();
  assert.match(doc.getElementById('assistant_model_test').textContent, /^Optional check/);
  assert.equal(doc.getElementById('btn-assistant-test').disabled, false);
  click('btn-assistant-test');
  await settle();
  resolveTest({compatible: true});
  await settle();
  assert.match(doc.getElementById('assistant_model_test').textContent, /^Passed:/);
});

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

test('evidence navigation uses known destinations and escapes labels', () => {
  const calls = [];
  window.openEvents = options => calls.push(['event', options]);
  window.openLogs = options => calls.push(['log', options]);
  const html = modal.evidenceButtons([{kind: 'event', group_id: 3, label: '<img onerror=x>'}, {kind: 'javascript', label: 'bad'}], 2);
  assert.match(html, /data-as-evidence="2:0"/);
  assert.match(html, /&lt;img/);
  assert.doesNotMatch(html, /<img|javascript|>bad</i);
  modal.openEvidence({kind: 'event', group_id: 3});
  modal.openEvidence({kind: 'event', group_id: 'javascript:bad'});
  modal.openEvidence({kind: 'log', session_id: 'archive-1', channel: 'sync', run_id: 'run-2'});
  assert.deepEqual(calls, [['event', {groupId: 3, visibility: 'all'}], ['log', {sessionId: 'archive-1', channel: 'sync', runId: 'run-2'}]]);
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
  assert.deepEqual(Object.keys(ACTIONS).sort(), ['open_analyzer', 'open_anime_overrides', 'open_connections', 'open_events', 'open_logs', 'open_mappings', 'open_pairs', 'open_scheduling', 'open_scrobbler', 'run_sync']);
  assert.match(actionButtons(['open_mappings']), /Mappings &amp; blocks/);
  assert.equal(lookedAt(['list_saved_mappings']), 'Looked at: mappings and blocks');
  assert.match(actionButtons(['open_anime_overrides']), /Anime ID custom mappings/);
  assert.equal(lookedAt(['list_anime_overrides']), 'Looked at: custom anime mappings');
  let opened = 0;
  window.openAnimeOverridesModal = () => { opened++; };
  ACTIONS.open_anime_overrides.run();
  assert.equal(opened, 1);
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

test('markdown links only become links for wiki and GitHub addresses', () => {
  const html = renderAnswer('See [Scheduler **Webhooks**](https://wiki.crosswatch.app/a/b) and [x](https://evil.test/y) and [q](https://wiki.crosswatch.app/a"onmouseover=alert(1)) and [s](javascript:alert(1))');
  assert.ok(html.includes('<a href="https://wiki.crosswatch.app/a/b" target="_blank" rel="noopener noreferrer">Scheduler <strong>Webhooks</strong></a>'));
  assert.ok(html.includes('[x](https://evil.test/y)'));
  assert.ok(!html.includes('href="https://evil.test'));
  assert.ok(!/<a[^>]*onmouseover/.test(html));
  assert.ok(!html.includes('href="javascript'));
});

test('follow-up questions become escaped chips, at most three', () => {
  const html = modal.followUps(['Why <b>?', 'Why <b>?', ' ', 'Two', 'Three', 'Four']);
  assert.deepEqual([...html.matchAll(/data-as-ask="([^"]*)"/g)].map(m => m[1]), ['Why &lt;b&gt;?', 'Two', 'Three']);
  assert.ok(!html.includes('<b>'));
  assert.equal(modal.followUps(undefined), '');
  assert.equal(modal.followUps([]), '');
});

test('wiki mode opens without outside calls, shows the notice and asks the wiki', async () => {
  ready = false;
  mode = 'wiki';
  modal.default.unmount();
  fetched.length = 0;
  const view = makeRoot();
  await modal.default.mount(view.root, {question: 'Analyze the latest sync log'});
  click(view, '#as-new');
  assert.equal(pending.length, 0);
  assert.ok(fetched.every(url => url.startsWith('/api/assistant/')), fetched.join(','));
  assert.equal(view.node('#as-notice').hidden, false);
  assert.equal(view.node('.as-scope').hidden, true);
  assert.equal(view.node('#as-sub').textContent, 'Answers from the CrossWatch wiki');
  assert.match(view.node('#as-note').textContent, /CrossWatch wiki/);
  assert.match(view.node('#as-log').innerHTML, /How do I add a Trakt pair\?/);
  assert.equal(view.node('#as-input').disabled, false);

  await submit(view, 'phone alerts?');
  assert.equal(pending.length, 1);
  assert.match(view.node('#as-log').innerHTML, /Searching the CrossWatch wiki/);
  pending.shift()({ok: true, mode: 'wiki', reply: 'Use [Scheduler Webhooks](https://wiki.crosswatch.app/a).', tools: ['ask_wiki'], actions: [], proposals: [],
    sources: [{title: 'Scheduler Webhooks', url: 'https://wiki.crosswatch.app/a'}], followups: ['Next <q>?']});
  await settle();
  const log = view.node('#as-log').innerHTML;
  assert.match(log, /Looked at: wiki/);
  assert.match(log, /<a href="https:\/\/wiki\.crosswatch\.app\/a"[^>]*>Scheduler Webhooks<\/a>/);
  assert.match(log, /data-as-ask="Next &lt;q&gt;\?"/);
  assert.ok(fetched.filter(url => !url.startsWith('/api/assistant/')).length === 0);
  mode = undefined;
  ready = true;
});

test('the sparkle button opens wiki mode but analyze buttons stay hidden', async () => {
  const source = await readFile(new URL('../assets/js/assistant-settings.js', import.meta.url), 'utf8');
  const nodes = new Map();
  const element = () => ({value: '', textContent: '', disabled: false, hidden: false, title: '', dataset: {}, attrs: {},
    setAttribute(name, value) { this.attrs[name] = value; }, focus() {}, classList: {toggle() {}}});
  const root = {dataset: {}};
  const doc = {documentElement: root, getElementById(id) { if (!nodes.has(id)) nodes.set(id, element()); return nodes.get(id); },
    createElement: element, addEventListener() {}, querySelectorAll: () => []};
  const answers = [{ok: true, mode: 'wiki', ready: false, wiki_help: true, base_url: '', model: ''}, {ok: true, mode: 'off', ready: false, wiki_help: false, base_url: '', model: ''},
    {ok: true, mode: 'full', ready: true, enabled: true, wiki_help: false, base_url: 'http://x.test/v1', model: 'm'}];
  const win = {};
  runInNewContext(source, {window: win, document: doc, fetch: async () => ({ok: true, json: async () => answers.shift()})});
  const ask = doc.getElementById('cw-ask-button');

  await win.cwAssistantSettings.refresh();
  assert.equal(root.dataset.cwAssistant, 'off');
  assert.equal(ask.disabled, false);
  assert.match(ask.title, /wiki only/);
  assert.equal(doc.getElementById('assistant_wiki_help').value, 'on');
  assert.equal(doc.getElementById('assistant_pill').textContent, 'Wiki only');

  await win.cwAssistantSettings.refresh();
  assert.equal(ask.disabled, true);
  assert.equal(doc.getElementById('assistant_wiki_help').value, 'off');

  await win.cwAssistantSettings.refresh();
  assert.equal(root.dataset.cwAssistant, 'on');
  assert.equal(ask.disabled, false);
});

test('the context line only shows in full mode for a selected item or pair', async () => {
  modal.default.unmount();
  const page = makeRoot();
  await modal.default.mount(page.root, {context: {page: 'settings'}});
  assert.equal(page.node('#as-context').hidden, true);
  modal.default.unmount();
  const pair = makeRoot();
  await modal.default.mount(pair.root, {context: {page: 'main', pair_id: 'p1'}});
  assert.equal(pair.node('#as-context').textContent, 'Asking about the selected pair');
  assert.equal(pair.node('#as-context').hidden, false);
  modal.default.unmount();
  mode = 'wiki';
  ready = false;
  const wiki = makeRoot();
  await modal.default.mount(wiki.root, {context: {page: 'editor', item_key: 'tmdb:1'}});
  assert.equal(wiki.node('#as-context').hidden, true);
  mode = undefined;
  ready = true;
});
