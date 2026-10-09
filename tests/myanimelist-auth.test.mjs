/* tests/myanimelist-auth.test.mjs */
/* CrossWatch - MyAnimeList browser authorization and profile lifecycle tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import { readFileSync } from "node:fs";

const source = readFileSync(new URL("../assets/auth/auth.myanimelist.js", import.meta.url), "utf8");
const flush = async () => { for (let n = 0; n < 12; n++) await Promise.resolve(); };

function setup({ blocked = false } = {}) {
  const nodes = new Map(), calls = [], events = {}, timers = new Map();
  let instance = "P01", profileChanged, responseHandler, seq = 0;
  const node = id => {
    if (!nodes.has(id)) {
      const classes = new Set();
      nodes.set(id, { value: "", textContent: "", dataset: {}, disabled: false, listeners: {},
        classList: { add: (...v) => v.forEach(x => classes.add(x)), remove: (...v) => v.forEach(x => classes.delete(x)), toggle: (v, on) => on ? classes.add(v) : classes.delete(v), contains: v => classes.has(v) },
        addEventListener(name, handler) { this.listeners[name] = handler; },
        removeAttribute(name) { delete this[name]; }, focus() {},
      });
    }
    return nodes.get(id);
  };
  const popup = { document: { html: "", write(html) { this.html += html; }, close() {} }, closed: false, opener: {}, location: { href: "about:blank", replace(url) { this.href = url; } }, close() { this.closed = true; } };
  const shared = {
    el: node, notify() {}, reportProviderUsage: () => false,
    createProfileAdapter: () => ({ getInstance: () => instance, ensureUI: fn => { profileChanged = fn; } }),
    setConnectLocked() {}, setStatus: (id, ok, message) => { node(id).textContent = message; },
    fetchJSON: async (url, options = {}) => {
      calls.push({ url, options });
      if (responseHandler) {
        const response = await responseHandler(url, options);
        if (response) return response;
      }
      const data = url.includes("oauth/start") ? { ok: true, flow_id: "flow-1", expires_at: Date.now() / 1000 + 600, authorization_url: "https://myanimelist.net/v1/oauth2/authorize?state=flow-1&code_challenge=challenge" } : url.includes("/status") ? { connected: false, login_available: true } : { ok: true };
      return { ok: true, data };
    },
  };
  const window = { CW: { AuthShared: shared }, open: () => blocked ? null : popup, addEventListener() {}, dispatchEvent() {} };
  const document = { addEventListener: (name, fn) => { events[name] = fn; }, dispatchEvent() {} };
  vm.runInNewContext(source, { window, document, URL, Date, Event, CustomEvent: class {}, setTimeout: fn => { timers.set(++seq, fn); return seq; }, clearTimeout: id => timers.delete(id) });
  return { node, calls, popup, timers,
    tick: async () => { const entry = timers.entries().next().value; if (entry) { timers.delete(entry[0]); entry[1](); await flush(); } },
    set handler(value) { responseHandler = value; },
    click: async id => { node(id).listeners.click(); await flush(); },
    change: async id => { instance = id; await profileChanged(); await flush(); },
    close: async () => { events["cw-auth-modal-closed"]({ detail: { provider: "myanimelist" } }); await flush(); },
  };
}


test("hosted login opens MAL and polls only the local backend", async () => {
  const app = setup();
  await app.click("myanimelist_oauth_start");
  assert.equal(new URL(app.popup.location.href).origin, "https://myanimelist.net");
  assert.equal(app.popup.opener, null);
  app.handler = url => url.includes("oauth/poll") ? { ok: true, data: { ok: true, status: "authorized" } } : null;
  await app.tick();
  assert.equal(app.timers.size, 0);
  const request = app.calls.find(c => c.url.includes("oauth/poll"));
  assert.equal(request.url, "/api/myanimelist/oauth/poll?instance=P01");
  assert.deepEqual(JSON.parse(request.options.body), { flow_id: "flow-1" });
  assert.ok(app.calls.every(c => c.url.startsWith("/api/myanimelist/")));
});

test("blocked popup leaves an approval link", async () => {
  const app = setup({ blocked: true });
  await app.click("myanimelist_oauth_start");
  assert.equal(new URL(app.node("myanimelist_approval_link").href).origin, "https://myanimelist.net");
  assert.equal(app.node("myanimelist_oauth_panel").classList.contains("hidden"), false);
});

test("profile switching stops polling and cancels the originating session", async () => {
  const app = setup();
  await app.click("myanimelist_oauth_start");
  await app.change("P02");
  assert.equal(app.timers.size, 0);
  assert.equal(app.calls.find(c => c.url.includes("oauth/cancel")).url, "/api/myanimelist/oauth/cancel?instance=P01");
});

test("closing during start cancels the late session", async () => {
  const app = setup();
  let resolve;
  app.handler = url => url.includes("oauth/start") ? new Promise(done => { resolve = done; }) : null;
  await app.click("myanimelist_oauth_start");
  assert.match(app.popup.document.html, /Preparing your authorization page/);
  assert.equal(app.popup.location.href, "about:blank");
  await app.close();
  resolve({ ok: true, data: { ok: true, flow_id: "late-flow" } });
  await flush();
  assert.deepEqual(JSON.parse(app.calls.find(c => c.url.includes("oauth/cancel")).options.body), { flow_id: "late-flow" });
  assert.equal(app.popup.closed, true);
  assert.equal(app.timers.size, 0);
});

test("late poll after closing cannot mark the connection complete", async () => {
  const app = setup();
  await app.click("myanimelist_oauth_start");
  let resolve;
  app.handler = url => url.includes("oauth/poll") ? new Promise(done => { resolve = done; }) : null;
  await app.tick();
  await app.close();
  const before = app.calls.length;
  resolve({ ok: true, data: { ok: true, status: "authorized" } });
  await flush();
  assert.equal(app.calls.length, before);
  assert.equal(app.timers.size, 0);
});

test("pending polls reschedule and terminal errors stop", async () => {
  const app = setup();
  await app.click("myanimelist_oauth_start");
  app.handler = url => url.includes("oauth/poll") ? { ok: true, data: { ok: true, status: "pending", interval: 5 } } : null;
  await app.tick();
  assert.equal(app.timers.size, 1);
  app.handler = url => url.includes("oauth/poll") ? { ok: true, data: { ok: false, error: "expired_flow" } } : null;
  await app.tick();
  assert.equal(app.timers.size, 0);
  assert.match(app.node("myanimelist_msg").textContent, /expired/);
});

test("unconfigured service disables the connect button", async () => {
  const app = setup();
  await flush();
  app.handler = url => url.includes("/status") ? { ok: true, data: { connected: false, login_available: false } } : null;
  await app.change("P02");
  assert.equal(app.node("myanimelist_oauth_start").disabled, true);
  assert.match(app.node("myanimelist_msg").textContent, /not available/);
});
