/* tests/syncbar-progress.test.mjs */
/* CrossWatch - Sync bar display regression tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const source = readFileSync(new URL('../assets/js/syncbar.js', import.meta.url), 'utf8');

const element = () => ({
  className: '', textContent: '', innerHTML: '', dataset: {}, style: {setProperty() {}}, children: [], clientWidth: 400,
  classList: {toggle() {}},
  append(...nodes) { this.children.push(...nodes); },
  appendChild(node) { this.children.push(node); },
  contains: () => true,
  querySelector: () => element(),
  querySelectorAll: () => [],
});

function createBar() {
  let now = 1000000;
  const OriginalDate = Date;
  class FakeDate extends OriginalDate { static now() { return now; } }
  const context = vm.createContext({window: {}, document: {createElement: element, getElementById: element}, setTimeout: () => 0, clearTimeout() {}, setInterval: () => 0, clearInterval() {}, Date: FakeDate});
  vm.runInContext(source, context);
  const bar = new context.window.SyncBar({el: element()});
  bar.markInit();
  bar.setPair({src: 'STREMIO', dst: 'SCROB', feature: 'history'});
  bar.advance = seconds => { now += seconds * 1000; };
  bar.text = () => bar._dom.phaseText.textContent;
  bar.width = () => parseInt(bar._dom.fill.style.width, 10);
  bar.server = (progress, extra = {}) => bar.fromSummary({running: true, _phase: {apply: {total: 1554, done: 0, progress, final: false}}, ...extra});
  return bar;
}

test('the bar shows the server progress during writes', () => {
  const bar = createBar();
  bar.snapStart({event: 'snapshot:start', provider: 'STREMIO', feature: 'history'});
  assert.equal(bar.text(), 'Reading STREMIO history');
  bar.applyStart({event: 'apply:add:start', dst: 'SCROB', feature: 'history', count: 1554});
  assert.equal(bar.width(), 67);
  assert.equal(bar.text(), `Adding to SCROB history · 0 of ${(1554).toLocaleString()}`);
  const widths = [0.25, 0.5, 0.75, 1].map(progress => { bar.server(progress); return bar.width(); });
  assert.deepEqual(widths, [75, 82, 90, 97]);
});

test('the bar works from summaries alone when the log is hidden', () => {
  const bar = createBar();
  bar.fromSummary({running: true, timeline: {start: true, pre: true, post: true}, _phase: {apply: {total: 100, done: 50, progress: 0.5, final: false}}});
  assert.equal(bar.width(), 82);
  assert.equal(bar.text(), 'Applying changes across enabled features');
  assert.equal(bar._dom.badge.textContent, 'Syncing');
});

test('a queued profile shows waiting without borrowing another pairs progress', () => {
  const bar = createBar();
  bar.fromSummary({run_id:'queued',running:true,run_busy:true,waiting:true,timeline:{start:true},_phase:{apply:{total:1,done:0,progress:0}}});
  assert.equal(bar._dom.badge.textContent, 'Waiting');
  assert.equal(bar.text(), 'Waiting for your sync pairs');
  assert.equal(bar.width(), 0);
  assert.equal(bar.isWaiting(), true);
  bar.fromSummary({run_id:'queued',running:true,run_busy:true,waiting:false,timeline:{start:true,pre:true,post:true},_phase:{apply:{total:10,done:5,progress:0.5}}});
  assert.equal(bar.isWaiting(), false);
  assert.equal(bar._dom.badge.textContent, 'Syncing');
  bar.fromSummary({run_id:'queued',running:false,run_busy:true,exit_code:0,timeline:{done:true}});
  assert.equal(bar.isRunBusy(), true);
  assert.equal(bar.isRunning(), false);
  assert.equal(bar.isWaiting(), false);
  bar.fromSummary({run_id:'queued',running:false,run_busy:false,exit_code:0,timeline:{done:true}});
  assert.equal(bar.isRunBusy(), false);
});

test('a stale summary cannot move the bar backwards', () => {
  const bar = createBar();
  bar.applyStart({event: 'apply:add:start', dst: 'SCROB', feature: 'history', count: 100});
  bar.server(0.5);
  bar.server(0.1);
  assert.equal(bar.width(), 82);
});

test('replayed events after a reconnect change nothing', () => {
  const bar = createBar();
  bar.applyStart({event: 'apply:add:start', dst: 'SCROB', feature: 'history', count: 1000});
  bar.server(0.5);
  for (let i = 0; i < 3; i++) {
    bar.applyStart({event: 'apply:add:start', dst: 'SCROB', feature: 'history', count: 1000});
    bar.applyProg({event: 'apply:add:progress', dst: 'SCROB', feature: 'history', done: 500, total: 1000});
    bar.applyDone({event: 'apply:add:done', dst: 'SCROB', feature: 'history', count: 1000});
  }
  assert.equal(bar.width(), 82);
});

test('slow reads show how far the lookups are', () => {
  const bar = createBar();
  bar.snapStart({event: 'snapshot:start', provider: 'STREMIO', feature: 'history'});
  bar.snap({event: 'snapshot:progress', dst: 'STREMIO', feature: 'history', done: 120, total: 384});
  assert.equal(bar.text(), 'Reading STREMIO history · 120 of 384');
  assert.equal(bar.width(), 42);
});

test('time left starts after ten seconds and uses the observed write speed', () => {
  const bar = createBar();
  bar.applyStart({event: 'apply:add:start', dst: 'SCROB', feature: 'history', count: 1000});
  bar.advance(5);
  bar.applyProg({event: 'apply:add:progress', dst: 'SCROB', feature: 'history', done: 10, total: 1000});
  assert.doesNotMatch(bar.text(), /left/);
  bar.advance(195);
  bar.applyProg({event: 'apply:add:progress', dst: 'SCROB', feature: 'history', done: 400, total: 1000});
  assert.match(bar.text(), /about 5 min left$/);
});

test('reloading mid-run replays progress without inventing a fast write speed', () => {
  const bar = createBar();
  bar.applyStart({event: 'apply:add:start', dst: 'SCROB', feature: 'history', count: 1000});
  for (const done of [100, 200, 300, 400]) bar.applyProg({event: 'apply:add:progress', dst: 'SCROB', feature: 'history', done, total: 1000});
  bar.advance(100);
  bar.applyProg({event: 'apply:add:progress', dst: 'SCROB', feature: 'history', done: 440, total: 1000});
  assert.match(bar.text(), /about 23 min left$/);
});

test('later events keep the full pair label', () => {
  const bar = createBar();
  bar.applyStart({event: 'apply:add:start', dst: 'SCROB', feature: 'history', count: 10});
  assert.equal(bar._pairText, 'STREMIO -> SCROB - history');
});

test('the status returns to reading for the next feature', () => {
  const bar = createBar();
  bar.applyStart({event: 'apply:add:start', dst: 'SCROB', feature: 'history', count: 10});
  bar.applyDone({event: 'apply:add:done', dst: 'SCROB', feature: 'history', count: 10});
  assert.equal(bar.text(), 'Applying changes across enabled features');
  bar.snapStart({event: 'snapshot:start', provider: 'SCROB', feature: 'ratings'});
  assert.equal(bar.text(), 'Reading SCROB ratings');
  bar.setPair({src: 'STREMIO', dst: 'SCROB', feature: 'ratings'});
  assert.equal(bar.text(), 'Applying changes across enabled features');
});

test('a run that stops mid-write leaves no stale activity', () => {
  for (const stop of [bar => bar.success(), bar => bar.fail(1), bar => bar.error(), bar => bar.done()]) {
    const bar = createBar();
    bar.snapStart({event: 'snapshot:start', provider: 'SCROB', feature: 'ratings'});
    bar.applyStart({event: 'apply:add:start', dst: 'SCROB', feature: 'history', count: 10});
    stop(bar);
    assert.equal(bar._writing, null);
    assert.equal(bar._reading, '');
    assert.doesNotMatch(bar.text(), /Adding to|Reading/);
  }
});

test('a finished run fills the bar', () => {
  const bar = createBar();
  bar.applyStart({event: 'apply:add:start', dst: 'SCROB', feature: 'history', count: 10});
  bar.server(1);
  assert.equal(bar.width(), 97);
  bar.success();
  assert.equal(bar.width(), 100);
});

test('a new run clears the previous progress', () => {
  const bar = createBar();
  bar.applyStart({event: 'apply:add:start', dst: 'SCROB', feature: 'history', count: 10});
  bar.server(1);
  bar.reset();
  assert.equal(bar._writing, null);
  assert.equal(bar._reading, '');
  assert.equal(bar.width(), 0);
  bar.markInit();
  bar.server(0.1);
  assert.equal(bar.width(), 70);
});
