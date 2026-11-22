/* assets/js/tracker-month-notifications.js */
/* CrossWatch - Ask the server to notify when a monthly or yearly review left while preparing is ready */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

export function monthNotifications(scope, {yearly = false} = {}) {
  const kind = yearly ? "wrapped" : "month";
  const valid = period => (yearly ? /^\d{4}$/ : /^\d{4}-(0[1-9]|1[0-2])$/).test(String(period || ""));
  const profile = () => String(window.CW?.Notifications?.scope?.() || window.CW?.OverviewProfile?.id || window.CW?.AuthState?.profileId || "all");
  function send(body) {
    if (typeof fetch !== "function") return;
    fetch("/api/profile/notifications/reviews", {method: "POST", credentials: "same-origin", cache: "no-store",
      headers: {"Content-Type": "application/json"}, body: JSON.stringify({kind, ...body})})
      .then(response => { if (response.ok) window.dispatchEvent(new CustomEvent("cw:notifications-refresh")); })
      .catch(() => {});
  }
  return {
    leave(instance, period, label, owner) {
      if (owner !== scope() || !instance || !valid(period)) return;
      send({action: "wait", instance, period, label: String(label || instance), profile: profile()});
    },
    viewing(instance, period) {
      if (!instance || !valid(period)) return;
      send({action: "viewed", instance, period});
    },
  };
}
