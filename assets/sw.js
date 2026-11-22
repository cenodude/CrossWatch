/* CrossWatch PWA Service Worker */

self.addEventListener("install", () => {
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(self.clients.claim());
});

self.addEventListener("fetch", (event) => {
  if (event.request.mode !== "navigate") return;
  event.respondWith(fetch(event.request));
});

function cwNotificationUrl(value) {
  try {
    const url = new URL(String(value || "/"), self.location.origin);
    if (url.origin === self.location.origin) return url.href;
    if (url.origin === "https://github.com" && url.pathname.startsWith("/cenodude/CrossWatch/releases/") && !url.username && !url.password) return url.href;
  } catch {}
  return new URL("/", self.location.origin).href;
}

self.addEventListener("push", (event) => {
  let data = {};
  try { data = event.data ? event.data.json() : {}; } catch {}
  event.waitUntil(self.clients.matchAll({ type: "window", includeUncontrolled: true }).then((clients) => {
    const visible = clients.some((client) => client.focused || client.visibilityState === "visible");
    for (const client of clients) client.postMessage({ type: "cw:notifications-refresh" });
    return self.registration.showNotification(String(data.title || "CrossWatch"), {
      body: String(data.body || ""),
      tag: String(data.tag || "cw-notification"),
      icon: "/assets/pwa/icon-192.png",
      badge: "/assets/pwa/favicon-64.png",
      data: { url: cwNotificationUrl(data.url) },
      silent: visible,
    });
  }));
});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const target = cwNotificationUrl(event.notification.data?.url);
  const local = new URL(target).origin === self.location.origin;
  event.waitUntil(self.clients.matchAll({ type: "window", includeUncontrolled: true }).then(async (clients) => {
    const client = local ? clients.find((item) => new URL(item.url).origin === self.location.origin) : null;
    if (!client) return self.clients.openWindow(target);
    const focused = await client.focus();
    if (focused && "navigate" in focused && focused.url !== target) return focused.navigate(target).catch(() => undefined);
    return focused;
  }));
});
