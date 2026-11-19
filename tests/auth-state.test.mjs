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

function setup() {
  const listeners = new Map(), events = [], requests = [];
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
  const document = {hidden: false, documentElement: {dataset: {}}, addEventListener() {}};
  const context = vm.createContext({window, document, AbortController,
    CustomEvent: class { constructor(type, options) { this.type = type; this.detail = options.detail; } },
    setTimeout() {}, clearTimeout() {}, setInterval() {},
    fetch(url, {signal}) {
      requests.push({url, signal});
      return new Promise((resolve, reject) => signal.addEventListener("abort", () => reject(signal.reason), {once: true}));
    },
  });
  vm.runInContext(source("helpers/auth-state.js"), context);
  return {auth: window.CW.AuthState, document, events, requests, context};
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
  for (const path of ["helpers/update-notifications.js", "helpers/database-notifications.js",
    "helpers/maintenance-notifications.js", "js/tracker-notifications.js"]) {
    vm.runInContext(source(path), app.context);
  }
  await flush();
  assert.deepEqual(app.requests.map(request => request.url).sort(), [
    "/api/health/database", "/api/maintenance/jobs/current", "/api/tracker/check/current", "/api/version",
  ]);
  const original = [...app.requests];
  app.auth.apply(status());
  await flush();
  assert.equal(app.requests.length, 4);
  assert.ok(original.every(request => !request.signal.aborted));
  const next = status();
  next.user.id = "bob";
  app.auth.apply(next);
  await flush();
  assert.ok(original.every(request => request.signal.aborted));
  assert.equal(app.requests.length, 8);
  assert.ok(app.requests.slice(4).every(request => !request.signal.aborted));
});
