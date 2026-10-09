/* assets/js/events/playback-timeline.js */
/* CrossWatch - Expand compact playback session timelines */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

export function playbackDetail(events) {
  const row = events.find((e) => e.event_type === "scrobble_playback");
  try {
    return typeof row?.detail === "string" ? JSON.parse(row.detail) : (row?.detail || {});
  } catch {
    return {};
  }
}

export function playbackTimeline(events) {
  const playback = playbackDetail(events);
  if (!Array.isArray(playback.playback)) return events;
  const row = events.find((e) => e.event_type === "scrobble_playback");
  const types = { start: "scrobble_started", pause: "scrobble_paused", resume: "scrobble_resumed", stop: "scrobble_stopped" };
  const entries = playback.playback.filter((entry) => Array.isArray(entry) && types[entry[1]]).map(([at, action, progress], i) => ({
    ...row, id: String(row.id) + "-" + i, event_type: types[action], created_at: at,
    detail: { progress },
  }));
  return [...entries, ...events.filter((e) => !["scrobble_playback", "scrobble_started"].includes(e.event_type))]
    .sort((a, b) => Number(a.created_at) - Number(b.created_at));
}
