/* assets/js/events/playback-timeline.js */
/* CrossWatch - Expand compact playback session timelines */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const detailOf = row => {
  try {
    const detail = typeof row?.detail === "string" ? JSON.parse(row.detail) : row?.detail;
    return detail && typeof detail === "object" ? detail : {};
  } catch {
    return {};
  }
};

export function playbackUpdatedAt(row) {
  const history = detailOf(row).playback;
  return (Array.isArray(history) ? history : []).reduce((at, entry) => Math.max(at, Number(entry?.[0]) || 0), Number(row?.created_at) || 0);
}

export function playbackDetail(events) {
  const rows = events.filter(row => row.event_type === "scrobble_playback");
  const latest = rows.reduce((picked, row) => !picked || playbackUpdatedAt(row) >= playbackUpdatedAt(picked) ? row : picked, null);
  const omitted = rows.reduce((count, row) => count + (Number(detailOf(row).omitted) || 0), 0);
  return latest ? {...detailOf(latest), ...(omitted ? {omitted} : {})} : {};
}

export function playbackTimeline(events) {
  const route = row => row.group_id != null ? `group:${row.group_id}` : JSON.stringify([row.source_provider, row.source_instance, row.destination_provider, row.destination_instance, row.session_key]);
  const compact = events.filter(row => row.event_type === "scrobble_playback" && Array.isArray(detailOf(row).playback));
  if (!compact.length) return events;
  const routes = new Set(compact.map(route));
  const types = { start: "scrobble_started", pause: "scrobble_paused", resume: "scrobble_resumed", stop: "scrobble_stopped" };
  return events.flatMap(row => {
    if (row.event_type === "scrobble_started" && routes.has(route(row))) return [];
    if (!compact.includes(row)) return [row];
    return detailOf(row).playback.filter(entry => Array.isArray(entry) && types[entry[1]]).map(([at, action, progress], i) => ({
      ...row, id: String(row.id) + "-" + i, event_type: types[action], created_at: at,
      detail: { progress },
    }));
  }).sort((a, b) => Number(a.created_at) - Number(b.created_at));
}
