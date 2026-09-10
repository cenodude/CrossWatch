/* tests/watcher-log-time.test.mjs */
/* CrossWatch - Watcher Log Display Timestamp Regression Tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

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
