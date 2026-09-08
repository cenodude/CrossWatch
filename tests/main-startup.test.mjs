/* tests/main-startup.test.mjs */
/* CrossWatch - Main startup request ordering and refresh regression tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import { readFileSync } from "node:fs";

const sources = ["helpers/core.js", "js/insights.js"].map(path =>
  readFileSync(new URL(`../assets/${path}`, import.meta.url), "utf8"));

function setup({ authPending = false } = {}) {
  const requests = [], calls = [], timers = new Map(), storage = new Map();
  let timerId = 0;
  let resolveAuth;
  const eventTarget = () => {
    const listeners = new Map();
    return {
      addEventListener(name, fn) {
        if (!listeners.has(name)) listeners.set(name, []);
        listeners.get(name).push(fn);
      },
      dispatchEvent(event) { for (const fn of listeners.get(event.type) || []) fn(event); },
    };
  };
  const classList = { add() {}, remove() {}, toggle() {}, contains() { return false; } };
  const historyList = { innerHTML: "" };
  const document = {
    ...eventTarget(), readyState: "loading", documentElement: { dataset: {}, classList },
    body: { dataset: {} },
    getElementById: id => id === "det-log" ? {} : null,
    querySelector: selector => selector === "#sync-history" ? historyList : null,
    querySelectorAll: () => [],
  };
  const pending = label => {
    requests.push(label);
    return new Promise(() => {});
  };
  const window = {
    ...eventTarget(), CW: { API: {
      Pairs: { list: () => pending("pairs") }, Status: { get: () => pending("status") },
    } },
    cwIsAuthSetupPending: () => authPending,
    __cwAuthBootstrapPromise: new Promise(resolve => { resolveAuth = resolve; }),
    updatePreviewVisibility: () => { calls.push("preview"); return Promise.resolve(true); },
    openSummaryStream() { calls.push("summary"); window.esSum = {}; },
    openLogStream() { calls.push("logs"); window.esLogs = {}; },
    openDetailsLog() { calls.push("details"); window.esDet = {}; },
    refreshSchedulingBanner: () => calls.push("schedule"),
  };
  const context = {
    window, document, console, URL, URLSearchParams, queueMicrotask, AbortController,
    location: { hash: "#main", search: "", pathname: "/", origin: "http://localhost", href: "http://localhost/#main" },
    history: { replaceState() {} },
    CustomEvent: class { constructor(type, options = {}) { this.type = type; this.detail = options.detail; } },
    localStorage: { getItem: key => storage.get(key) ?? null, setItem: (key, value) => storage.set(key, value) },
    setTimeout(fn, delay) { timers.set(++timerId, { fn, delay }); return timerId; },
    clearTimeout: id => timers.delete(id),
    setInterval() {},
    fetch: url => pending(url),
  };
  window.location = context.location;
  window.history = context.history;
  for (const source of sources) vm.runInNewContext(source, context);
  const flush = async () => { for (let i = 0; i < 8; i++) await Promise.resolve(); };
  const fire = (target, type, detail) => target.dispatchEvent({ type, detail });
  const advance = async delay => {
    for (const [id, timer] of [...timers]) {
      if (timer.delay <= delay) { timers.delete(id); timer.fn(); }
    }
    await flush();
  };
  return { window, document, requests, calls, timers, flush, fire, advance,
    enableAuth() { authPending = false; }, resolveAuth };
}

test("Main starts the preview and streams without waiting for pairs, status or insights", async () => {
  const app = setup();
  void app.window.showTab("main");
  await app.advance(0);
  assert.ok(app.requests.includes("pairs"));
  assert.ok(app.requests.includes("status"), "status starts while pairs are still pending");
  for (const call of ["preview", "summary", "logs", "details", "schedule"]) {
    assert.ok(app.calls.includes(call), `${call} starts while API requests are still pending`);
  }
});

test("DOMContentLoaded and load share one lightweight and one full insights request", async () => {
  const app = setup();
  app.fire(app.document, "DOMContentLoaded");
  await app.advance(180);
  const insights = () => app.requests.filter(url => url.startsWith("/api/insights?"));
  assert.equal(insights().length, 2);
  assert.equal(insights().filter(url => url.includes("include_events=0")).length, 1);
  assert.equal(insights().filter(url => url.includes("history=60")).length, 1);
  const before = [...app.requests];
  const previews = app.calls.filter(call => call === "preview").length;
  app.fire(app.window, "load");
  await app.advance(180);
  assert.deepEqual(app.requests, before);
  assert.equal(app.calls.filter(call => call === "preview").length, previews);
});

test("an explicit full refresh consumes the scheduled full refresh", async () => {
  const app = setup();
  app.window.Insights.refreshInsightsFastThenFull();
  void app.window.refreshInsights(true);
  await app.advance(180);
  assert.equal(app.requests.filter(url => url.includes("history=60")).length, 1);
});

test("startup resumes after auth setup and ignores the later load refresh", async () => {
  const app = setup({ authPending: true });
  app.fire(app.document, "DOMContentLoaded");
  await app.flush();
  assert.equal(app.requests.filter(url => url === "status" || url.startsWith("/api/insights?")).length, 0);
  app.enableAuth();
  app.fire(app.window, "cw-auth-setup-pending", { pending: false });
  app.resolveAuth();
  await app.flush();
  await app.advance(180);
  assert.ok(app.calls.includes("preview"));
  assert.equal(app.calls.filter(call => call === "preview").length, 1);
  const before = [...app.requests];
  app.fire(app.window, "load");
  await app.advance(180);
  assert.deepEqual(app.requests, before);
});

test("returning from another page still refreshes Main", async () => {
  const app = setup();
  void app.window.showTab("main");
  await app.advance(180);
  await app.window.showTab("watchlist");
  void app.window.showTab("main");
  await app.advance(180);
  assert.equal(app.calls.filter(call => call === "preview").length, 2);
  assert.equal(app.requests.filter(url => url.includes("history=60")).length, 2);
});
