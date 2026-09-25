/* tests/wetrakr-probe.test.mjs */
/* CrossWatch - WeTrakr probe quota display tests */
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
  const window = { CW: { ProfileDateTime: { format(date, options) { formats.push({ date, options }); return "2026-09-27 02:00 CEST"; } } } };
  const badge = { classList: { add() {}, remove() {} } };
  const context = vm.createContext({ window, Date, byId: () => badge, connState: () => "ok", instancesTooltip: () => "", svgCrown: () => "<svg></svg>" });
  vm.runInContext(metadata, context);
  Object.assign(context, { meta: window.CW.ProviderMeta, META: window.CW.ProviderMeta, txt: v => String(v ?? "").trim(), up: v => String(v).toUpperCase() });
  vm.runInContext(main.slice(main.indexOf("  function providerMeta("), main.indexOf("  function updateConn(")), context);
  vm.runInContext(core.slice(core.indexOf("  function setBadge("), core.indexOf("  function renderConnectorStatus(")), context);
  return { context, formats, badge, details: window.CW.ProviderMeta.dailyQuotaDetails };
}

test("both WeTrakr tooltips show the observed allowance and localized reset", () => {
  const app = setup();
  const reset = Math.floor(Date.now() / 1000) + 3600;
  const data = { connected: true, username: "tester", plan: "vip", vip: true, daily_remaining: 49850, daily_limit: 50000, daily_resets_at: reset };
  const mainInfo = app.context.providerMeta("WETRAKR", data);
  app.context.setBadge("badge-wetrakr", "WeTrakr", data, false, "WETRAKR", data);
  assert.equal(mainInfo.vip, true);
  for (const text of [mainInfo.detail, app.badge.title]) {
    assert.match(text, /API calls left today: 49850 \/ 50000/);
    assert.match(text, /Resets: 2026-09-27 02:00 CEST/);
    assert.match(text, /Plan: VIP/);
  }
  assert.equal(app.formats[0].date.getTime(), reset * 1000);
  assert.equal(app.formats[0].options.timeZoneName, "short");
});

test("zero is displayed and a missing daily limit is not invented", () => {
  const app = setup();
  const data = { daily_remaining: 0, daily_seen_at: Date.now() / 1000 };
  assert.equal(app.details(data)[0], "API calls left today: 0");
  assert.equal(app.details({ ...data, daily_limit: 1000 })[0], "API calls left today: 0 / 1000");
});

test("missing, malformed and expired observations are hidden", () => {
  const app = setup();
  const now = Date.now() / 1000;
  for (const data of [{}, { daily_remaining: null }, { daily_remaining: "invalid" }, { daily_remaining: -1 },
    { daily_remaining: 42, daily_resets_at: now - 1 }, { daily_remaining: 42, daily_seen_at: now - 301 }]) {
    assert.equal(app.details(data).length, 0);
  }
});
