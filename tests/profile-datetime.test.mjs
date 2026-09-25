/* tests/profile-datetime.test.mjs */
/* CrossWatch - Profile display timezone and clock tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import vm from "node:vm";

const source = readFileSync(new URL("../assets/helpers/profile-datetime.js", import.meta.url), "utf8");
const pageSource = readFileSync(new URL("../assets/js/profile-page.js", import.meta.url), "utf8");
const options = { hour: "2-digit", minute: "2-digit" };
function helper(language = "en-US") {
  const context = { window: {}, navigator: { language }, Intl };
  vm.runInNewContext(source, context);
  return context.window.CW.ProfileDateTime;
}

test("auto follows the browser locale and timezone", () => {
  const api = helper();
  const date = new Date("2026-09-01T00:30:00Z");
  assert.equal(api.timeZone(), new Intl.DateTimeFormat().resolvedOptions().timeZone);
  assert.equal(api.format(date, options), new Intl.DateTimeFormat("en-US", options).format(date));
});

test("12-hour and 24-hour clocks preserve the instant and format midnight as 00", () => {
  const api = helper();
  const date = new Date("2026-09-01T00:30:00Z");
  api.configure({ timezone: "UTC", time_format: "12h" });
  assert.equal(api.format(date, options), "12:30 AM");
  api.configure({ timezone: "UTC", time_format: "24h" });
  assert.equal(api.format(date, options), "00:30");
  assert.equal(date.toISOString(), "2026-09-01T00:30:00.000Z");
});

test("day and month keys follow the chosen zone across year boundaries", () => {
  const api = helper();
  const epoch = Date.parse("2026-01-01T00:30:00Z") / 1000;
  api.configure({ timezone: "America/Los_Angeles" });
  assert.equal(api.dayKey(epoch), "2025-12-31");
  assert.equal(api.monthKey(epoch), "2025-12");
  api.configure({ timezone: "Asia/Kathmandu", time_format: "24h" });
  assert.equal(api.dayKey(epoch), "2026-01-01");
  assert.equal(api.format(new Date(epoch * 1000), options), "06:15");
});

test("named zones apply daylight saving for each timestamp", () => {
  const api = helper();
  api.configure({ timezone: "Europe/Amsterdam", time_format: "24h" });
  assert.equal(api.format(new Date("2026-03-29T00:30:00Z"), options), "01:30");
  assert.equal(api.format(new Date("2026-03-29T01:30:00Z"), options), "03:30");
  assert.equal(api.format(new Date("2026-10-25T00:30:00Z"), options), "02:30");
  assert.equal(api.format(new Date("2026-10-25T01:30:00Z"), options), "02:30");
});

test("date-only labels can retain their calendar date and invalid zones fall back", () => {
  const api = helper();
  api.configure({ timezone: "America/Los_Angeles" });
  assert.equal(api.format(new Date("2026-01-01T00:00:00Z"), { timeZone: "UTC", year: "numeric", month: "2-digit", day: "2-digit" }), "01/01/2026");
  api.configure({ timezone: "Invalid/Zone" });
  assert.equal(api.timeZone(), new Intl.DateTimeFormat().resolvedOptions().timeZone);
});

test("timezone search ignores case and underscores while retaining the saved selection", () => {
  const zone = { value: "UTC", innerHTML: "" };
  const search = { value: "new york" };
  const status = { hidden: true, textContent: "" };
  const elements = { "#profile-pref-timezone": zone, "#profile-timezone-search": search, "#profile-timezone-results": status };
  const context = {
    $: (selector) => elements[selector],
    esc: (value) => value,
    profileTimezoneNames: ["America/New_York", "Europe/Amsterdam", "UTC"],
  };
  vm.createContext(context);
  vm.runInContext(pageSource.slice(pageSource.indexOf("  function renderTimezoneOptions("), pageSource.indexOf("  function renderPreferences(")), context);
  vm.runInContext("renderTimezoneOptions()", context);
  assert.match(zone.innerHTML, /America\/New_York/);
  assert.doesNotMatch(zone.innerHTML, /Europe\/Amsterdam/);
  assert.match(zone.innerHTML, /value="auto"/);
  assert.match(zone.innerHTML, /value="UTC"/);
  assert.equal(zone.value, "UTC");
  assert.equal(status.textContent, "1 matching timezone");
  search.value = "NEW_YORK";
  vm.runInContext("renderTimezoneOptions()", context);
  assert.match(zone.innerHTML, /America\/New_York/);
  search.value = "no such zone";
  vm.runInContext("renderTimezoneOptions()", context);
  assert.equal(zone.value, "UTC");
  assert.match(status.textContent, /No matching timezones/);
  search.value = "";
  vm.runInContext("renderTimezoneOptions()", context);
  assert.match(zone.innerHTML, /Europe\/Amsterdam/);
  assert.equal(status.hidden, true);
});

function preferenceForm({ fail = false } = {}) {
  const elements = Object.fromEntries(["playing-card", "quick-add", "timezone", "time-format"].map((key) => {
    const id = `profile-pref-${key}`;
    return [`#${id}`, { id, checked: true, value: "auto", disabled: false, handlers: {}, addEventListener(event, handler) { this.handlers[event] = handler; } }];
  }));
  const requests = [];
  let reloads = 0;
  const context = {
    $: (selector) => elements[selector],
    profile: { preferences: { timezone: "auto", time_format: "auto" } },
    api: async (url, options) => {
      requests.push(JSON.parse(options.body));
      if (fail) throw new Error("Save failed");
      return { user: { preferences: requests.at(-1).preferences } };
    },
    renderProfile: (data) => { context.profile = data.user; },
    renderPreferences: (user) => { elements["#profile-pref-timezone"].value = user.preferences.timezone; },
    toast() {},
    window: { location: { reload() { reloads += 1; } } },
  };
  vm.createContext(context);
  vm.runInContext(pageSource.slice(pageSource.indexOf("  function wirePreferences()"), pageSource.indexOf("  let nowTimer =")) + "\nwirePreferences();", context);
  return { elements, requests, get reloads() { return reloads; } };
}

test("timezone arrow-key changes save once on blur and unchanged blur does nothing", async () => {
  const form = preferenceForm();
  const zone = form.elements["#profile-pref-timezone"];
  await zone.handlers.blur();
  assert.equal(form.requests.length, 0);
  for (const value of ["UTC", "Europe/Amsterdam", "Europe/Berlin"]) {
    zone.value = value;
    await zone.handlers.change?.();
  }
  assert.equal(form.requests.length, 0);
  assert.equal(form.reloads, 0);
  await zone.handlers.blur();
  assert.equal(form.requests.length, 1);
  assert.equal(form.requests[0].preferences.timezone, "Europe/Berlin");
  assert.equal(form.reloads, 1);
  await zone.handlers.blur();
  assert.equal(form.requests.length, 1);
});

test("failed timezone blur save restores the selection without reloading", async () => {
  const form = preferenceForm({ fail: true });
  const zone = form.elements["#profile-pref-timezone"];
  zone.value = "UTC";
  zone.disabled = true;
  await zone.handlers.blur();
  assert.equal(form.requests.length, 0);
  zone.disabled = false;
  await zone.handlers.blur();
  assert.equal(zone.value, "auto");
  assert.equal(zone.disabled, false);
  assert.equal(form.reloads, 0);
  await zone.handlers.blur();
  assert.equal(form.requests.length, 1);
});

test("other preferences still save on change", async () => {
  const form = preferenceForm();
  const toggle = form.elements["#profile-pref-quick-add"];
  toggle.checked = false;
  await toggle.handlers.change();
  assert.equal(form.requests[0].preferences.quick_add, false);
  assert.equal(form.reloads, 0);
  const format = form.elements["#profile-pref-time-format"];
  format.value = "24h";
  await format.handlers.change();
  assert.equal(form.requests[1].preferences.time_format, "24h");
  assert.equal(form.reloads, 1);
});
