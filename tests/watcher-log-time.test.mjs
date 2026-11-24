/* tests/watcher-log-time.test.mjs */
/* CrossWatch - Watcher Log Display Timestamp Regression Tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import './i18n-vm-setup.mjs';

const source = readFileSync(new URL('../assets/helpers/details-log.js', import.meta.url), 'utf8');
const parser = source.slice(source.indexOf('function _plainLogText('), source.indexOf('function _cwUnresEsc('));
const context = vm.createContext({_watchLogKnownTags: () => ['PLEX-WATCH', 'SCROBBLE']});
vm.runInContext(parser, context);

test('watcher replay uses the captured UTC timestamp converted to local time', () => {
  const stamp = '2026-09-09T21:49:02.000+00:00';
  const raw = `[${stamp}] [PLEX-WATCH] INFO event start episode p=18`;
  const expected = new Date(stamp).toLocaleTimeString([], {hour:'2-digit', minute:'2-digit', second:'2-digit', hour12:false});
  for (let i = 0; i < 2; i++) {
    const parsed = context._parseLogParts(raw, 'PLEX-WATCH');
    assert.equal(parsed.time, expected);
    assert.equal(parsed.provider, 'PLEX-WATCH');
    assert.equal(parsed.level, 'INFO');
    assert.equal(parsed.message, 'event start episode p=18');
  }
});

test('existing timestamped lines keep their time', () => {
  assert.equal(context._parseLogParts('[23:49:33] [SCROBBLE] DEBUG accepted stop').time, '23:49:33');
});

test('scheduled times stay in the message when a log timestamp is present', () => {
  const parsed = context._parseLogParts('[00:10:00] [SYNC] INFO [SCHED] next run scheduled at 2026-11-17 03:03:50 CET');
  assert.equal(parsed.time, '00:10:00');
  assert.equal(parsed.message, '[SCHED] next run scheduled at 2026-11-17 03:03:50 CET');
});

test('scheduled times are not mistaken for missing log timestamps', () => {
  const sandbox = vm.createContext({_watchLogKnownTags: () => ['SYNC']});
  vm.runInContext(parser, sandbox);
  sandbox._logTimeNow = () => '00:10:00';
  const parsed = sandbox._parseLogParts('[SYNC] INFO [SCHED] next run scheduled at 2026-11-17 03:03:50 CET');
  assert.equal(parsed.time, '00:10:00');
  assert.equal(parsed.message, '[SCHED] next run scheduled at 2026-11-17 03:03:50 CET');
});

test('bare leading timestamps remain supported without removing message times', () => {
  const parsed = context._parseLogParts('23:49:33.123 INFO retry at 23:50:00', 'SYNC');
  assert.equal(parsed.time, '23:49:33');
  assert.equal(parsed.level, 'INFO');
  assert.equal(parsed.message, 'retry at 23:50:00');
});
