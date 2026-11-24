/* assets/js/import-export/tracker-instance.js */
/* CrossWatch - New CrossWatch tracker instance from the Import and Export page */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const I18N = () => globalThis.window?.CW?.I18n || globalThis.window?.__cwI18n;
const T = (key, vars) => I18N()?.t?.(key, vars) ?? key;
const TH = (key, vars) => I18N()?.html?.(key, vars) ?? key;
export const NEW_TRACKER = "new";

export const canCreateTracker = () => document.documentElement?.dataset?.cwRole !== "user";

export function trackerOptions(targets, option) {
  const rows = targets.map(target => option(target.id, (name => target.connected ? name : T("importExport.tracker.notConnected", {name}))(target.id === "default" && target.label === "Default" ? T("common.labels.default") : target.label)));
  if (canCreateTracker()) rows.push(option(NEW_TRACKER, T("importExport.tracker.newInstance")));
  return rows.join("");
}

export async function createTracker(label) {
  const response = await fetch("/api/provider-instances/CROSSWATCH/next", {
    method: "POST",
    credentials: "same-origin",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ label, template: { connected: true } }),
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok || data.ok === false || !data.id) {
    if (data.error === "profile_limit_reached") throw new Error(T("importExport.tracker.limit"));
    throw new Error(typeof data.detail === "string" ? data.detail : data.error || T("importExport.tracker.createFailed", {status: String(response.status)}));
  }
  return { id: data.id, label, connected: true };
}
