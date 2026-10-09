/* tests/playback-timeline.test.mjs */
/* CrossWatch - Compact playback timeline presentation tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import {test} from 'node:test';
import assert from 'node:assert/strict';
import {playbackTimeline, playbackDetail} from '../assets/js/events/playback-timeline.js';

test('expands lifecycle transitions without duplicating the legacy start', () => {
  const events = [
    {id: 1, event_type: 'scrobble_started', created_at: 10},
    {id: 2, event_type: 'scrobble_playback', created_at: 40, detail: JSON.stringify({playback: [[10, 'start', 2], [20, 'pause', 2], [30, 'resume', 2], [40, 'stop', 2]], omitted: 12})},
    {id: 3, event_type: 'scrobble_failed', created_at: 41},
  ];
  const timeline = playbackTimeline(events);
  assert.deepEqual(timeline.map(e => e.event_type), ['scrobble_started', 'scrobble_paused', 'scrobble_resumed', 'scrobble_stopped', 'scrobble_failed']);
  assert.deepEqual(timeline.map(e => e.created_at), [10, 20, 30, 40, 41]);
  assert.equal(timeline[3].detail.progress, 2);
  assert.equal(playbackDetail(events).omitted, 12);
  assert.equal(events.length, 3);
});

test('legacy and malformed records retain their existing timeline', () => {
  for (const events of [[{event_type: 'scrobble_started'}], [{event_type: 'scrobble_playback', detail: '{bad'}]]) {
    assert.equal(playbackTimeline(events), events);
  }
});
