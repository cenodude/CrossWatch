/* tests/mdblist-probe.test.mjs */
/* CrossWatch - MDBList account and quota tooltip tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import { readFileSync } from "node:fs";

const source = path => readFileSync(new URL(path, import.meta.url), "utf8");
const metadata = source("../assets/helpers/provider-meta.js");
const main = source("../assets/js/main-status.js");
const core = source("../assets/helpers/core.js");

function setup() {
  const formats = [];
  const window = { CW: { ProfileDateTime: { format(date, options) { formats.push({ date, options }); return "11/10/2026, 01:00 AM CET"; } } } };
  const badge = { classList: { add() {}, remove() {} } };
  const context = vm.createContext({ window, Date, byId: () => badge, connState: () => "ok", instancesTooltip: () => "" });
  vm.runInContext(metadata, context);
  Object.assign(context, { meta: window.CW.ProviderMeta, META: window.CW.ProviderMeta, up: v => String(v).toUpperCase() });
  vm.runInContext(main.slice(main.indexOf("  function providerMeta("), main.indexOf("  function updateConn(")), context);
  vm.runInContext(core.slice(core.indexOf("  function setBadge("), core.indexOf("  function renderConnectorStatus(")), context);
  return { context, formats, badge };
}

for (const [plan, vip, label] of [["free", false, "Free"], ["supporter", true, "Supporter"]]) {
  test(`both MDBList tooltips show the ${plan} account and localized quota`, () => {
    const app = setup();
    const reset = Math.floor(Date.now() / 1000) + 3600;
    const data = { connected: true, username: "tester", plan, vip, daily_remaining: 881, daily_limit: 1000, daily_resets_at: reset };
    const info = app.context.providerMeta("MDBLIST", data);
    app.context.setBadge("badge-mdblist", "MDBList", data, false, "MDBLIST", data);
    assert.equal(info.vip, vip);
    assert.equal(info.detail, `Account: tester\nPlan: ${label}\nAPI calls left today: 881 / 1000\nResets: 11/10/2026, 01:00 AM CET`);
    for (const line of info.detail.split("\n")) assert.ok(app.badge.title.includes(line));
    assert.equal(app.formats[0].date.getTime(), reset * 1000);
    assert.equal(app.formats[0].options.timeZoneName, "short");
  });
}

test("MDBList shows exhausted quotas but hides missing observations", () => {
  const app = setup();
  const data = { username: "tester", plan: "free", daily_remaining: 0, daily_limit: 1000, daily_seen_at: Date.now() / 1000 };
  assert.match(app.context.providerMeta("MDBLIST", data).detail, /API calls left today: 0 \/ 1000/);
  for (const quota of [{ daily_remaining: null }, { daily_remaining: -1 }, { daily_remaining: "invalid" }]) {
    assert.equal(app.context.providerMeta("MDBLIST", { ...data, ...quota }).detail, "Account: tester\nPlan: Free");
  }
  assert.equal(app.context.providerMeta("MDBLIST", {}).detail, "");
});

test("MDBList preserves supporter details from older cached account data", () => {
  const app = setup();
  assert.equal(app.context.providerMeta("MDBLIST", { vip: true, patron_status: "active_patron" }).detail, "Plan: Patron");
  assert.equal(app.context.providerMeta("MDBLIST", { vip: false }).detail, "Plan: Free");
});
