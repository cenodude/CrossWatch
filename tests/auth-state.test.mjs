/* tests/auth-state.test.mjs */
/* CrossWatch - Auth state change and notification request regression tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import {readFileSync} from "node:fs";

const source = path => readFileSync(new URL(`../assets/${path}`, import.meta.url), "utf8");
const flush = () => new Promise(resolve => setImmediate(resolve));
const status = () => ({authenticated: true, user: {id: "alice", is_admin: true, preferences: {theme: "dark"}}});
const notificationScripts = ["js/notification-feed.js"];

function setup({readyState = "complete"} = {}) {
  const listeners = new Map(), documentListeners = new Map(), events = [], requests = [];
  const window = {
    addEventListener(name, fn) {
      if (!listeners.has(name)) listeners.set(name, []);
      listeners.get(name).push(fn);
    },
    dispatchEvent(event) {
      events.push(event);
      for (const fn of listeners.get(event.type) || []) fn(event);
    },
  };
  const document = {hidden: false, readyState, documentElement: {dataset: {}},
    addEventListener(name, fn) {
      if (!documentListeners.has(name)) documentListeners.set(name, []);
      documentListeners.get(name).push(fn);
    },
  };
  const context = vm.createContext({window, document, AbortController,
    CustomEvent: class { constructor(type, options) { this.type = type; this.detail = options.detail; } },
    setTimeout() {}, clearTimeout() {}, setInterval() {},
    fetch(url, {signal}) {
      requests.push({url, signal});
      if (url === "/api/app-auth/status") return Promise.resolve({ok: true, json: async () => status()});
      return new Promise((resolve, reject) => signal.addEventListener("abort", () => reject(signal.reason), {once: true}));
    },
  });
  vm.runInContext(source("helpers/auth-state.js"), context);
  return {auth: window.CW.AuthState, window, document, events, requests, context,
    loadNotifications() {
      for (const path of notificationScripts) vm.runInContext(source(path), context);
    },
    domReady() {
      document.readyState = "interactive";
      for (const fn of documentListeners.get("DOMContentLoaded") || []) fn();
    },
  };
}

test("unchanged auth updates retain fresh status without emitting another change", () => {
  const app = setup();
  app.auth.apply(status());
  const next = {authenticated: true, session_expires_at: 1234,
    user: {preferences: {theme: "dark"}, is_admin: true, id: "alice"}};
  const state = app.auth.apply(next);
  assert.equal(app.events.length, 1);
  assert.equal(state.status, next);
  assert.equal(app.auth.read().status, next);
  assert.equal(app.auth.user, next.user);
});

test("account, role, profile, permission and preference changes still emit", () => {
  const app = setup(), current = status();
  app.auth.apply(current);
  const changes = [
    user => { user.id = "bob"; },
    user => { user.is_admin = false; user.profile_id = "home"; },
    user => { user.profile_id = "work"; },
    user => { user.permissions = {write: true, dashboard: false}; },
    user => { user.preferences.theme = "light"; },
    user => { user.is_admin = true; },
  ];
  for (const [index, change] of changes.entries()) {
    change(current.user);
    const state = app.auth.apply(current);
    assert.equal(app.events.length, index + 2);
    assert.equal(app.events.at(-1).detail, state);
    app.auth.apply(structuredClone(current));
    assert.equal(app.events.length, index + 2);
  }
  assert.equal(app.document.documentElement.dataset.cwRole, "admin");
  assert.equal(app.auth.profileId, "");
  assert.equal(app.auth.canWrite, true);
});

test("equivalent nested permission and preference objects do not restart auth listeners", () => {
  const app = setup();
  app.auth.apply({authenticated: true, user: {id: "alice", is_admin: false, profile_id: "home",
    permissions: {write: true, tracker: true}, preferences: {theme: "dark", mobile: {alerts: true, tabs: ["main", "tracker"]}}}});
  app.auth.apply({authenticated: true, user: {profile_id: "home", id: "alice", is_admin: false,
    preferences: {mobile: {tabs: ["main", "tracker"], alerts: true}, theme: "dark"}, permissions: {tracker: true, write: true}}});
  assert.equal(app.events.length, 1);
});

test("unchanged auth preserves pending notification requests while account changes cancel them", async () => {
  const app = setup();
  app.auth.apply(status());
  app.loadNotifications();
  await flush();
  assert.deepEqual(app.requests.map(request => request.url), ["/api/profile/notifications?profile=all&view_profile="]);
  const original = [...app.requests];
  app.auth.apply(status());
  await flush();
  assert.equal(app.requests.length, 1);
  assert.ok(original.every(request => !request.signal.aborted));
  const next = status();
  next.user.id = "bob";
  app.auth.apply(next);
  await flush();
  assert.ok(original.every(request => request.signal.aborted));
  assert.equal(app.requests.length, 2);
  assert.ok(app.requests.slice(1).every(request => !request.signal.aborted));
});

for (const readyState of ["loading", "interactive", "complete"]) {
  test(`notification startup waits for initial auth while the document is ${readyState}`, async () => {
    const app = setup({readyState});
    let resolveBootstrap;
    const bootstrap = new Promise(resolve => { resolveBootstrap = resolve; });
    if (readyState === "complete") app.window.__cwAuthBootstrapPromise = bootstrap;
    app.loadNotifications();
    await flush();
    assert.equal(app.requests.length, 0);
    app.window.__cwAuthBootstrapPromise = bootstrap;
    app.domReady();
    await flush();
    assert.equal(app.requests.length, 0);
    resolveBootstrap({status: status()});
    await flush();
    assert.equal(app.events.length, 1);
    assert.equal(app.requests.length, 1);
    assert.ok(app.requests.every(request => request.signal && !request.signal.aborted));
    app.auth.apply(status());
    await flush();
    assert.equal(app.requests.length, 1);
    assert.ok(app.requests.every(request => !request.signal.aborted));
  });
}

test("standalone pages share one initial auth request before checking notifications", async () => {
  const app = setup();
  app.loadNotifications();
  await flush();
  assert.equal(app.requests.filter(request => request.url === "/api/app-auth/status").length, 1);
  assert.equal(app.requests.length, 2);
  assert.ok(app.requests.slice(1).every(request => !request.signal.aborted));
});

test("bootstrap readiness preserves auth already applied by another startup component", async () => {
  const app = setup();
  let resolveBootstrap;
  app.window.__cwAuthBootstrapPromise = new Promise(resolve => { resolveBootstrap = resolve; });
  app.loadNotifications();
  const current = status();
  current.user.id = "bob";
  app.auth.apply(current);
  resolveBootstrap({status: status()});
  await flush();
  assert.equal(app.auth.user.id, "bob");
  assert.equal(app.events.length, 1);
  assert.equal(app.requests.length, 1);
  assert.ok(app.requests.every(request => !request.signal.aborted));
});

test("setup-pending bootstrap does not start notification requests", async () => {
  const app = setup();
  app.window.cwIsAuthSetupPending = () => true;
  app.window.__cwAuthBootstrapPromise = Promise.resolve({status: {authenticated: false, setup_required: true}});
  app.loadNotifications();
  await flush();
  assert.equal(app.requests.length, 0);
});
