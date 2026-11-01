/* tests/maintenance-notifications.test.mjs */
/* CrossWatch - Global maintenance notification lifecycle tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import {readFileSync} from "node:fs";

const script = readFileSync(new URL("../assets/helpers/maintenance-notifications.js", import.meta.url), "utf8");
const flush = () => new Promise(resolve => setImmediate(resolve));

async function setup(role = "admin") {
  const events = {}, sources = [], timers = new Map();
  let nextTimer = 0, requests = 0, code = 200;
  let job = {ok: true, state: "idle"};
  const document = {hidden: false, documentElement: {dataset: {cwRole: role}}, addEventListener: (name, fn) => { events[name] = fn; }};
  const window = {CW: {Notifications: {setSource: (name, source) => sources.push({name, ...source})}},
    addEventListener: (name, fn) => { events[name] = fn; }};
  vm.runInNewContext(script, {window, document, AbortController,
    setTimeout: (fn, ms) => { timers.set(++nextTimer, {fn, ms}); return nextTimer; },
    clearTimeout: id => timers.delete(id),
    fetch: async (url, options) => {
      requests++;
      assert.equal(url, "/api/maintenance/jobs/current");
      assert.equal(options.credentials, "same-origin");
      return {ok: code === 200, status: code, json: async () => job};
    },
  });
  await flush();
  return {document, events, timers, sources, requests: () => requests, latest: () => sources.at(-1),
    async publish(value, status = 200) { job = value; code = status; await events["cw:maintenance-job-changed"](); },
  };
}

test("maintenance notifications update globally from running to completed without repeating each step", async () => {
  const app = await setup();
  const job = {id: "job-1", action: "events-optimize", state: "running", step: "vacuum", started_at: 123};
  await app.publish(job);
  const initial = app.latest().items[0];
  assert.equal(app.latest().scope, "account");
  assert.match(initial.title, /Optimize database - running/);
  assert.match(initial.detail, /Reclaiming unused space/);
  assert.equal(initial.href, "/#maintenance");
  assert.ok([...app.timers.values()].some(timer => timer.ms === 2000));
  await app.publish({...job, step: "analyze"});
  assert.equal(app.latest().items[0].revision, initial.revision);
  await app.publish({...job, state: "completed", result: {ok: true}});
  assert.notEqual(app.latest().items[0].revision, initial.revision);
  assert.match(app.latest().items[0].title, /completed/);
  assert.equal(app.latest().items[0].action, "View result");
});

test("failed, interrupted and unhealthy jobs report attention instead of success", async () => {
  const app = await setup();
  const job = {id: "job-2", action: "database-health", state: "failed", result: {ok: false}};
  await app.publish(job);
  assert.equal(app.latest().items[0].icon, "error_outline");
  assert.match(app.latest().items[0].detail, /failed/);
  await app.publish({...job, state: "interrupted"});
  assert.match(app.latest().items[0].detail, /worker stopped/);
  await app.publish({...job, state: "completed", result: {ok: true, healthy: false}});
  assert.match(app.latest().items[0].title, /needs attention/);
});

test("maintenance notifications are admin-only and clear on lost authorization", async () => {
  const managed = await setup("user");
  assert.equal(managed.requests(), 0);
  assert.equal(managed.latest().items.length, 0);
  assert.equal(managed.timers.size, 0);
  const admin = await setup();
  await admin.publish({id: "job-3", state: "running"});
  await admin.publish({}, 403);
  assert.equal(admin.latest().items.length, 0);
  assert.equal(admin.timers.size, 0);
});

test("polling pauses in hidden tabs and refreshes persisted results when visible", async () => {
  const app = await setup();
  app.document.hidden = true;
  const count = app.requests();
  await app.publish({id: "job-4", state: "completed", result: {ok: true}});
  assert.equal(app.requests(), count);
  assert.equal(app.timers.size, 0);
  app.document.hidden = false;
  await app.events.visibilitychange();
  assert.match(app.latest().items[0].title, /completed/);
});

test("transient failures retain the current notification and retry", async () => {
  const app = await setup();
  await app.publish({id: "job-5", state: "running"});
  await app.publish({}, 503);
  assert.equal(app.latest().items[0].id, "job-5");
  assert.match(app.latest().error, /Could not refresh/);
  assert.ok([...app.timers.values()].some(timer => timer.ms === 2000));
});
