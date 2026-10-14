/* tests/assistant-modal.test.mjs */
/* CrossWatch - Assistant chat modal rendering and lifecycle tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from 'node:test';
import assert from 'node:assert/strict';

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
