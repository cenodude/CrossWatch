/* assets/js/import-export/tracker-instance.js */
/* CrossWatch - New CrossWatch tracker instance from the Import and Export page */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

export const NEW_TRACKER = "new";

export const canCreateTracker = () => document.documentElement?.dataset?.cwRole !== "user";

export function trackerOptions(targets, option) {
  const rows = targets.map(target => option(target.id, `${target.label}${target.connected ? "" : " (not connected)"}`));
  if (canCreateTracker()) rows.push(option(NEW_TRACKER, "New instance"));
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
    if (data.error === "profile_limit_reached") throw new Error("No more tracker instances can be created. Remove one in Settings first.");
    throw new Error(typeof data.detail === "string" ? data.detail : data.error || `Could not create the instance (${response.status}).`);
  }
  return { id: data.id, label, connected: true };
}
