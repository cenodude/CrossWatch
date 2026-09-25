/* tests/wetrakr-auth.test.mjs */
/* CrossWatch - WeTrakr browser authorization and profile lifecycle tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import { readFileSync } from "node:fs";

const source = readFileSync(new URL("../assets/auth/auth.wetrakr.js", import.meta.url), "utf8");
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
  const popup = { closed: false, opener: {}, location: { href: "about:blank", replace(url) { this.href = url; } }, close() { this.closed = true; } };
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
      const data = url.includes("oauth/start") ? { ok: true, flow_id: "flow-1", expires_at: Date.now() / 1000 + 600, authorization_url: "https://api.wetrakr.com/oauth/authorize?state=flow-1&code_challenge=challenge" } : url.includes("/status") ? { connected: false } : { ok: true };
      return { ok: true, data };
    },
  };
  const window = { CW: { AuthShared: shared }, open: () => blocked ? null : popup, addEventListener() {}, dispatchEvent() {} };
  const document = { addEventListener: (name, fn) => { events[name] = fn; }, dispatchEvent() {} };
  vm.runInNewContext(source, { window, document, URL, Date, Event, CustomEvent: class {}, setInterval: fn => { timers.set(++seq, fn); return seq; }, clearInterval: id => timers.delete(id) });
  return { node, calls, popup, timers,
    set handler(value) { responseHandler = value; },
    click: async id => { node(id).listeners.click(); await flush(); },
    change: async id => { instance = id; await profileChanged(); await flush(); },
    close: async () => { events["cw-auth-modal-closed"]({ detail: { provider: "wetrakr" } }); await flush(); },
  };
}

test("opens separate approval page and accepts a pasted code without device polling", async () => {
  const app = setup();
  await app.click("wetrakr_oauth_start");
  const approvalUrl = new URL(app.popup.location.href);
  assert.equal(approvalUrl.origin, "https://api.wetrakr.com");
  assert.equal(approvalUrl.pathname, "/oauth/authorize");
  assert.equal(app.popup.opener, null);
  assert.equal(app.node("wetrakr_oauth_panel").classList.contains("hidden"), false);
  app.node("wetrakr_code").value = " pasted-code ";
  await app.click("wetrakr_oauth_finish");
  const request = app.calls.find(c => c.url.includes("oauth/finish"));
  assert.equal(request.url, "/api/wetrakr/oauth/finish?instance=P01");
  assert.deepEqual(JSON.parse(request.options.body), { code: "pasted-code", flow_id: "flow-1" });
  assert.equal(app.node("wetrakr_code").value, "");
  assert.equal(app.timers.size, 0);
  assert.equal(app.calls.some(c => /device|poll/.test(c.url)), false);
});

test("popup blocking leaves a usable approval link and code input", async () => {
  const app = setup({ blocked: true });
  await app.click("wetrakr_oauth_start");
  const approvalUrl = new URL(app.node("wetrakr_approval_link").href);
  assert.equal(approvalUrl.origin, "https://api.wetrakr.com");
  assert.equal(approvalUrl.pathname, "/oauth/authorize");
  assert.equal(app.node("wetrakr_oauth_finish").classList.contains("hidden"), false);
});

test("profile change cancels the original flow and clears its code", async () => {
  const app = setup();
  await app.click("wetrakr_oauth_start");
  app.node("wetrakr_code").value = "secret-code";
  await app.change("P02");
  const cancel = app.calls.find(c => c.url.includes("oauth/cancel"));
  assert.equal(cancel.url, "/api/wetrakr/oauth/cancel?instance=P01");
  assert.deepEqual(JSON.parse(cancel.options.body), { flow_id: "flow-1" });
  assert.equal(app.node("wetrakr_code").value, "");
  assert.equal(app.timers.size, 0);
});

test("closing while start is in flight cancels only that returned flow", async () => {
  const app = setup();
  let resolve;
  app.handler = url => url.includes("oauth/start") ? new Promise(done => { resolve = done; }) : null;
  await app.click("wetrakr_oauth_start");
  await app.close();
  resolve({ ok: true, data: { ok: true, flow_id: "late-flow" } });
  await flush();
  const cancel = app.calls.find(c => c.url.includes("oauth/cancel"));
  assert.deepEqual(JSON.parse(cancel.options.body), { flow_id: "late-flow" });
  assert.equal(app.popup.closed, true);
  assert.equal(app.node("wetrakr_oauth_panel").classList.contains("hidden"), true);
});

test("duplicate submissions are blocked and terminal errors clear the code", async () => {
  const app = setup();
  await app.click("wetrakr_oauth_start");
  let resolve;
  app.handler = url => url.includes("oauth/finish") ? new Promise(done => { resolve = done; }) : null;
  app.node("wetrakr_code").value = "expired";
  await app.click("wetrakr_oauth_finish");
  await app.click("wetrakr_oauth_finish");
  assert.equal(app.calls.filter(c => c.url.includes("oauth/finish")).length, 1);
  assert.equal(app.node("wetrakr_oauth_finish").disabled, true);
  resolve({ ok: true, data: { ok: false, error: "invalid_code", retryable: false } });
  await flush();
  assert.equal(app.node("wetrakr_code").value, "");
  assert.equal(app.node("wetrakr_oauth_start").disabled, false);
});

test("retryable account validation leaves the active flow available", async () => {
  const app = setup();
  await app.click("wetrakr_oauth_start");
  app.handler = url => url.includes("oauth/finish") ? { ok: true, data: { ok: false, error: "server_error", retryable: true } } : null;
  app.node("wetrakr_code").value = "code";
  await app.click("wetrakr_oauth_finish");
  assert.equal(app.node("wetrakr_oauth_panel").classList.contains("hidden"), false);
  assert.equal(app.node("wetrakr_oauth_finish").disabled, false);
  assert.equal(app.calls.some(c => c.url.includes("oauth/cancel")), false);
});
