/* tests/auth-reconnect.test.mjs */
/* CrossWatch - Provider reconnection controls and authorization regression tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import { readFileSync } from "node:fs";

const source = name => readFileSync(new URL(`../assets/auth/auth.${name}.js`, import.meta.url), "utf8");

function setup() {
  const nodes = new Map(), requests = [], pollers = [], observers = [];
  let cfg = {}, instance = "default", response = { ok: true, data: {} }, gate = null;
  function node(id, label = "") {
    const classes = new Set(), attrs = new Map();
    let html = label;
    const item = {
      id, dataset: {}, value: "", disabled: false,
      get innerHTML() { return html; }, set innerHTML(value) { html = value; },
      get textContent() { return html; }, set textContent(value) { html = value; },
      classList: {
        add: (...names) => names.forEach(n => classes.add(n)),
        remove: (...names) => names.forEach(n => classes.delete(n)),
        contains: name => classes.has(name),
        toggle: (name, on) => on ? classes.add(name) : classes.delete(name),
      },
      setAttribute: (name, value) => attrs.set(name, value),
      getAttribute: name => attrs.get(name) ?? null,
      removeAttribute: name => attrs.delete(name),
      listeners: {},
      addEventListener(name, handler) { this.listeners[name] = handler; }, querySelectorAll: () => [],
    };
    nodes.set(id, item);
    return item;
  }
  const document = {
    readyState: "loading", hidden: false,
    getElementById: id => nodes.get(id) || null,
    querySelector: () => null, querySelectorAll: () => [],
    addEventListener() {}, removeEventListener() {},
  };
  const window = { CW: {}, open: () => null, addEventListener() {} };
  const context = vm.createContext({
    window, document, console, URL, location: { origin: "https://cw.test" },
    MutationObserver: class { constructor(cb) { observers.push(cb); } observe() {} disconnect() {} },
    setTimeout: () => 1, clearTimeout() {}, setInterval: () => 1, clearInterval() {},
    fetch: async (url, options) => {
      requests.push({ url, options });
      const current = response, wait = gate;
      if (wait) await wait;
      const data = url.startsWith("/api/config") ? cfg : current.data;
      return { ok: current.ok, status: current.ok ? 200 : 500, json: async () => data };
    },
  });
  vm.runInContext(source("shared"), context);
  const shared = window.CW.AuthShared;
  const profile = provider => ({
    getInstance: () => instance,
    api: path => `${path}?instance=${instance}`,
    cfgBlock: current => instance === "default" ? current?.[provider] || {} : current?.[provider]?.instances?.[instance] || {},
    ensureUI() {}, refreshOptions: async () => {},
  });
  shared.createProfileAdapter = options => profile(options.configKey);
  shared.createDevicePoll = options => {
    let running = false;
    const poller = { options, start() { running = true; }, stop() { running = false; }, isRunning: () => running };
    pollers.push(poller);
    return poller;
  };
  return {
    node, window, shared, profile, requests, pollers, observers,
    nodeValue: id => nodes.get(id)?.value,
    load: name => vm.runInContext(source(name), context),
    set config(value) { cfg = value; },
    set instance(value) { instance = value; },
    set response(value) { response = value; },
    set gate(value) { gate = value; },
  };
}

for (const [source, local, expected] of [
  ["local", true, "CrossWatch device-code limit reached"],
  ["provider", false, "PunchPlay requested a cooldown"],
  ["provider", true, "PunchPlay requested a cooldown"],
]) {
  test(`PunchPlay displays ${source} cooldown with local=${local} and suppresses duplicate starts`, async () => {
    const app = setup();
    const button = app.node("punchplay_device_start", "Connect PunchPlay");
    const status = app.node("punchplay_msg");
    app.response = { ok: true, data: { ok: false, error: "rate_limited", local, rate_limit_source: source, retry_after: 1671 } };
    app.load("punchplay");
    await Promise.all([button.listeners.click(), button.listeners.click()]);
    assert.equal(app.requests.length, 1);
    assert.equal(status.textContent, expected + " - try again in 27m 51s");
    assert.equal(button.disabled, false);
    await button.listeners.click();
    assert.equal(app.requests.length, 2);
  });
}

test("connected buttons offer Reconnect and restore their original labels per profile", () => {
  const app = setup(), button = app.node("btn-connect-simkl", "Connect SIMKL");
  app.shared.setConnectLocked(button, true);
  assert.equal(button.textContent, "Reconnect");
  assert.equal(button.disabled, false);
  assert.equal(button.getAttribute("aria-disabled"), null);
  app.shared.setConnectLocked(button, true);
  app.shared.setConnectLocked(button, false);
  assert.equal(button.textContent, "Connect SIMKL");
  assert.equal(button.getAttribute("title"), null);
});

test("reconnect status updates preserve busy buttons and restart labels", () => {
  const app = setup(), button = app.node("btn-connect-trakt", "Connect TRAKT");
  const restart = app.node("btn-trakt-restart", "Restart");
  button.classList.add("busy");
  app.shared.setConnectLocked([button, restart], true);
  assert.equal(button.disabled, true);
  assert.equal(restart.disabled, false);
  assert.equal(restart.textContent, "Restart");
});

test("OAuth completion ignores old masked tokens, stale caches and other instances", async () => {
  const app = setup();
  app.instance = "SIMKL-P01";
  app.window._cfgCache = { simkl: { instances: { "SIMKL-P01": { auth_completed_at: "stale" } } } };
  app.config = { simkl: { instances: { "SIMKL-P01": { access_token: "********", auth_completed_at: "before" } } } };
  const done = await app.shared.captureAuthCompletion(app.profile("simkl"));
  assert.equal(done({ simkl: { instances: { "SIMKL-P01": { access_token: "********", auth_completed_at: "before" } } } }), false);
  const updated = { simkl: { instances: { "SIMKL-P01": { access_token: "********", auth_completed_at: "after" } } } };
  assert.equal(done(updated), true);
  app.instance = "default";
  assert.equal(done(updated), false);
  assert.equal(app.requests.length, 1);
});

test("OAuth reconnect cannot use a failed config read as its baseline", async () => {
  const app = setup();
  app.response = { ok: false, data: {} };
  await assert.rejects(app.shared.captureAuthCompletion(app.profile("simkl")), /Could not load/);
});

test("SIMKL reconnect starts a fresh PIN for the selected instance without deleting tokens", async () => {
  const app = setup();
  app.instance = "SIMKL-P01";
  const button = app.node("btn-connect-simkl-pin", "Connect SIMKL");
  app.response = { ok: true, data: { ok: true, user_code: "123456", expires_in: 600 } };
  app.load("simkl");
  app.window.setSimklSuccess(true);
  assert.equal(button.textContent, "Reconnect");
  assert.equal(button.disabled, false);
  await app.window.startSimklPin();
  assert.deepEqual(app.requests.map(r => r.url), ["/api/simkl/pin/start?instance=SIMKL-P01"]);
  assert.equal(app.pollers[0].isRunning(), true);
  assert.equal(app.pollers[0].options.classify(200, { status: "pending" }).state, "pending");
  assert.equal(app.pollers[0].options.classify(200, { status: "authorized" }).state, "authorized");
});

test("SIMKL OAuth still requires client credentials when already connected", () => {
  const app = setup(), button = app.node("btn-connect-simkl", "Connect SIMKL");
  app.node("simkl_client_id"); app.node("simkl_client_secret");
  app.load("simkl");
  app.window.setSimklSuccess(true);
  app.window.updateSimklButtonState();
  assert.equal(button.disabled, true);
});

test("Trakt reconnect waits for its pending device authorization despite an existing token", async () => {
  const app = setup();
  app.instance = "TRAKT-P01";
  app.node("btn-connect-trakt", "Connect TRAKT");
  app.node("trakt_client_id").value = "client";
  app.node("trakt_client_secret").value = "********";
  app.node("trakt_pin");
  app.response = { ok: true, data: { ok: true, user_code: "123456", expiresIn: 600 } };
  app.load("trakt");
  await app.window.requestTraktPin();
  assert.deepEqual(app.requests.map(r => r.url), ["/api/trakt/pin/new?instance=TRAKT-P01"]);
  const classify = app.pollers[0].options.classify;
  assert.equal(classify(200, { trakt: { instances: { "TRAKT-P01": { access_token: "********", _pending_device: { user_code: "123456" } } } } }).state, "pending");
  assert.equal(classify(200, { trakt: { instances: { "TRAKT-P01": { access_token: "********" } } } }).state, "authorized");
  assert.equal(classify(200, { trakt: { instances: { "TRAKT-P01": { access_token: "********", _pending_device: { user_code: "", device_code: "", expires_at: 0 } } } } }).state, "authorized");
});

for (const clientId of ["client", "********"]) {
  for (const secret of ["", "legacy-secret", "********"]) {
    test(`Trakt own app connects with client ID ${clientId} and secret ${secret || "omitted"}`, async () => {
      const app = setup();
      app.instance = "TRAKT-P01";
      app.node("trakt_auth_method").value = "app";
      app.node("trakt_client_id").value = clientId;
      app.node("trakt_client_secret").value = secret;
      app.node("trakt_pin");
      const hint = app.node("trakt_hint");
      hint.style = {};
      app.response = { ok: true, data: { ok: true, user_code: "123456", expiresIn: 600 } };
      app.load("trakt");
      app.window.updateTraktHint();
      assert.equal(hint.classList.contains("hidden"), true);
      await app.window.requestTraktPin();
      assert.deepEqual(app.requests.map(r => r.url), ["/api/trakt/pin/new?instance=TRAKT-P01"]);
      const expected = { auth_method: "app" };
      if (clientId === "client") expected.client_id = clientId;
      if (secret === "legacy-secret") expected.client_secret = secret;
      assert.deepEqual(JSON.parse(app.requests[0].options.body), expected);
      assert.equal(app.pollers[0].isRunning(), true);
      assert.equal(app.nodeValue("trakt_pin"), "123456");
    });
  }
}

test("Trakt own app still requires a Client ID", async () => {
  const app = setup();
  app.node("trakt_auth_method").value = "app";
  app.node("trakt_client_id");
  app.node("trakt_client_secret");
  const hint = app.node("trakt_hint");
  hint.style = {};
  app.load("trakt");
  app.window.updateTraktHint();
  assert.equal(hint.classList.contains("hidden"), false);
  await app.window.requestTraktPin();
  assert.equal(app.requests.length, 0);
});

test("TMDb reconnect starts authorization even with a stored session", async () => {
  const app = setup();
  app.instance = "TMDB-P01";
  app.node("sec-tmdb-sync").__hydrated = true;
  app.node("tmdb_sync_api_key").value = "key";
  app.node("tmdb_sync_session_id").value = "old-session";
  const button = app.node("tmdb_sync_connect", "Connect TMDb");
  app.response = { ok: true, data: { ok: true, pending: true } };
  app.load("tmdb");
  await button.listeners.click();
  assert.equal(app.requests[0].url, "/api/tmdb_sync/connect/start?instance=TMDB-P01");
  assert.equal(app.requests[0].options.method, "POST");
  assert.equal(app.requests.some(r => r.url.includes("disconnect")), false);
});

for (const instance of ["default", "TRAKT-P01"]) {
  test(`Trakt ${instance} shows Reconnect with the empty pending-login defaults`, async () => {
    const app = setup();
    app.instance = instance;
    const button = app.node("btn-connect-trakt", "Connect TRAKT");
    const status = app.node("trakt_msg");
    const block = {
      access_token: "********",
      _pending_device: {
        user_code: "", device_code: "", verification_url: "https://trakt.tv/activate",
        interval: 5, expires_at: 0, created_at: 0,
      },
    };
    app.config = { trakt: instance === "default" ? block : { instances: { [instance]: block } } };
    app.load("trakt");
    await app.window.hydrateAuthFromConfig();
    assert.equal(button.textContent, "Reconnect");
    assert.equal(button.disabled, false);
    assert.equal(status.textContent, "Connected");
    delete block.access_token;
    await app.window.hydrateAuthFromConfig();
    assert.equal(button.textContent, "Connect TRAKT");
  });
}


function tofaApp() {
  const app = setup();
  let onSwitch = null;
  app.shared.createProfileAdapter = () => ({ ...app.profile("tofa"), ensureUI(cb) { onSwitch = cb; } });
  app.node("tofa_server_url").value = "https://tofa.test";
  const connect = app.node("tofa_connect", "Connect Tofa");
  const overlay = app.node("cw-auth-connection-overlay");
  const status = app.node("tofa_msg");
  app.response = { ok: true, data: { ok: true, user_code: "ABCD-1234", interval: 5, expires_at: 0 } };
  app.load("tofa");
  const hold = () => { let release; app.gate = new Promise(resolve => { release = resolve; }); return release; };
  const flush = () => new Promise(resolve => setImmediate(resolve));
  return {
    app, connect, overlay, status, hold, flush,
    switchTo(name) { app.instance = name; onSwitch(); },
    close() { overlay.classList.add("hidden"); app.observers.forEach(cb => cb([])); },
  };
}

test("Tofa polls the instance the login was started for", async () => {
  const { app, connect } = tofaApp();
  await connect.listeners.click();
  assert.equal(app.pollers.length, 1);
  app.instance = "TOFA-P01";
  assert.equal(app.pollers[0].options.url(), "/api/tofa/device/poll?instance=default");
  assert.equal(app.pollers[0].isRunning(), true);
});

test("Tofa drops a login start that returns after an instance switch", async () => {
  const { app, connect, hold, flush, switchTo } = tofaApp();
  const release = hold();
  const pending = connect.listeners.click();
  app.gate = null;
  switchTo("TOFA-P01");
  release();
  await pending;
  await flush();
  assert.equal(app.pollers.some(p => p.isRunning()), false);
});

test("Tofa drops a login start that returns after the window closed", async () => {
  const { app, connect, hold, flush, close } = tofaApp();
  const release = hold();
  const pending = connect.listeners.click();
  app.gate = null;
  close();
  release();
  await pending;
  await flush();
  assert.equal(app.pollers.some(p => p.isRunning()), false);
});

test("Tofa keeps the current instance status when an older reply arrives last", async () => {
  const { app, status, hold, flush, switchTo } = tofaApp();
  app.response = { ok: true, data: { connected: true, authenticated: false, profile_name: "Old" } };
  const releaseOld = hold();
  switchTo("TOFA-P01");
  app.response = { ok: true, data: { connected: true, authenticated: false, profile_name: "New" } };
  const releaseNew = hold();
  switchTo("TOFA-P02");
  app.gate = null;
  releaseNew();
  await flush();
  assert.equal(status.textContent, "Tofa connected - New");
  releaseOld();
  await flush();
  assert.equal(status.textContent, "Tofa connected - New");
});
