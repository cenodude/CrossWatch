/* tests/connection-whitelist.browser.mjs */
/* CrossWatch - Library whitelist selections survive the settings collectors on save. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import assert from "node:assert/strict";
import {readFile} from "node:fs/promises";
import test from "node:test";
import {chromium} from "playwright";

const features = ["history", "ratings", "progress", "collection", "scrobble"];
const keys = {history: "hist", ratings: "rate", progress: "prog", collection: "coll", scrobble: "scr"};
const providers = [
  {name: "plex", script: "auth.plex.js", prefix: "plex", fields: {plex_server_url: "http://plex.test:32400"}},
  {name: "jellyfin", script: "auth.jellyfin.js", prefix: "jfy", fields: {jfy_server_url: "http://jf.test", jfy_username: "u"}, rehydrate: true},
  {name: "emby", script: "auth.emby.js", prefix: "emby", fields: {emby_server_url: "http://emby.test", emby_username: "u"}, rehydrate: true},
  {name: "kodi", script: "auth.kodi.js", prefix: "kodi", fields: {kodi_server: "http://kodi.test", kodi_username: "u"}},
];

async function page(t, provider) {
  const browser = await chromium.launch({headless: true});
  t.after(() => browser.close());
  const tab = await browser.newPage();
  const block = {server: "http://x.test", server_url: "http://x.test", user: "u", username: "u",
    ...Object.fromEntries(features.map(feature => [feature, {libraries: ["7"]}]))};
  await tab.route("**/*", async route => {
    const path = new URL(route.request().url()).pathname;
    if (path.startsWith("/assets/")) {
      try { return route.fulfill({body: await readFile(new URL(".." + path, import.meta.url)), contentType: "text/javascript"}); }
      catch { return route.fulfill({status: 404, body: ""}); }
    }
    if (path === "/api/config") return route.fulfill({json: {[provider.name]: block}});
    if (path.startsWith("/api/")) return route.fulfill({json: {}});
    const selects = features.map(feature => `<select multiple id="${provider.prefix}_lib_${feature}"></select>`).join("");
    const inputs = Object.entries(provider.fields).map(([id, value]) => `<input id="${id}" value="${value}">`).join("");
    return route.fulfill({contentType: "text/html", body: `<!doctype html><html><body><div id="${provider.prefix}_lib_matrix"></div>${selects}${inputs}
      <script>window.CW={};const collectors=new Set();window.registerSettingsCollector=fn=>collectors.add(fn);
      window.__emitSettingsCollect=cfg=>{document.dispatchEvent(new CustomEvent("settings-collect",{detail:{cfg}}));for(const fn of collectors)fn(cfg)};</script>
      <script src="/assets/auth/auth.shared.js"></script><script src="/assets/auth/${provider.script}"></script></body></html>`});
  });
  await tab.goto("https://whitelist.test/");
  await tab.waitForTimeout(1200);
  if (provider.rehydrate) {
    await tab.evaluate(name => window.cwAuth?.[name]?.rehydrate?.(), provider.name);
    await tab.waitForTimeout(600);
  }
  return tab;
}

for (const provider of providers) {
  test(`${provider.name}: connection dialog selections survive save`, async t => {
    const tab = await page(t, provider);
    const saved = await tab.evaluate(({provider, features, keys}) => {
      window.cwAuth[provider.name].whitelist.write("default", Object.fromEntries(Object.values(keys).map(key => [key, ["1", "2"]])));
      for (const feature of features) {
        document.getElementById(`${provider.prefix}_lib_${feature}`).innerHTML = '<option selected value="1">1</option><option selected value="2">2</option>';
      }
      const cfg = {[provider.name]: Object.fromEntries(features.map(feature => [feature, {libraries: [1, 2]}]))};
      window.__emitSettingsCollect(cfg);
      return features.map(feature => (cfg[provider.name][feature].libraries || []).map(String));
    }, {provider, features, keys});
    for (const libraries of saved) assert.deepEqual(libraries, ["1", "2"]);
  });

  test(`${provider.name}: selections for another instance never reach this one`, async t => {
    const tab = await page(t, provider);
    const result = await tab.evaluate(({provider, features, keys}) => {
      const owner = window.cwAuth[provider.name].whitelist;
      const own = owner.read("default");
      const other = owner.read("other");
      owner.write("other", Object.fromEntries(Object.values(keys).map(key => [key, ["9"]])));
      const cfg = {[provider.name]: Object.fromEntries(features.map(feature => [feature, {libraries: ["7"]}]))};
      window.__emitSettingsCollect(cfg);
      return {own: own?.hist || null, other, saved: features.map(feature => (cfg[provider.name][feature].libraries || []).map(String))};
    }, {provider, features, keys});
    assert.deepEqual(result.own, ["7"]);
    assert.equal(result.other, null);
    for (const libraries of result.saved) assert.deepEqual(libraries, ["7"]);
  });

  test(`${provider.name}: unrelated save keeps the stored whitelist`, async t => {
    const tab = await page(t, {...provider, rehydrate: false});
    const saved = await tab.evaluate(({provider}) => {
      const cfg = {[provider.name]: {history: {libraries: ["7"]}}};
      window.__emitSettingsCollect(cfg);
      return (cfg[provider.name].history.libraries || []).map(String);
    }, {provider});
    assert.deepEqual(saved, ["7"]);
  });
}

test("a late library response for the previous instance never reaches the current table", async t => {
  const browser = await chromium.launch({headless: true});
  t.after(() => browser.close());
  const tab = await browser.newPage();
  const errors = [];
  tab.on("pageerror", err => errors.push(err.message));
  await tab.route("**/*", async route => {
    const url = new URL(route.request().url());
    const path = url.pathname;
    if (path === "/assets/helpers/providers-ui.js") {
      const source = String(await readFile(new URL(".." + path, import.meta.url)));
      const hook = "  (window.CW ||= {}).ProvidersUI = ProvidersUI;";
      assert.ok(source.includes(hook));
      return route.fulfill({contentType: "text/javascript", body: source.replace(hook, `${hook}
  window.__cwRenderConnectionWhitelist = renderConnectionWhitelist;`)});
    }
    if (path.startsWith("/assets/")) {
      try { return route.fulfill({body: await readFile(new URL(".." + path, import.meta.url)), contentType: "text/javascript"}); }
      catch { return route.fulfill({status: 404, body: ""}); }
    }
    if (path === "/api/emby/libraries") {
      if (url.searchParams.get("instance") === "A") {
        await new Promise(resolve => setTimeout(resolve, 1500));
        return route.fulfill({json: {libraries: [{id: "9", title: "A library"}]}});
      }
      return route.fulfill({json: {libraries: [{id: "7", title: "B library"}]}});
    }
    if (path.startsWith("/api/")) return route.fulfill({json: {}});
    const selects = features.map(feature => `<select multiple id="emby_lib_${feature}"></select>`).join("");
    return route.fulfill({contentType: "text/html", body: `<!doctype html><html><body><div id="panel">
      <div class="cw-profile-switcher" data-cw-profile-provider="emby"><select><option value="A" selected>A</option><option value="B">B</option></select></div>
      <div class="cw-subpanel active" data-sub="whitelist"><div id="emby_lib_matrix"></div></div></div>${selects}
      <script>window.CW={HelpLinks:{url:()=>""}};</script><script src="/assets/helpers/providers-ui.js"></script></body></html>`});
  });
  await tab.goto("https://switch.test/");
  await tab.waitForFunction(() => typeof window.__cwRenderConnectionWhitelist === "function");
  const result = await tab.evaluate(async () => {
    const panel = document.getElementById("panel");
    const host = document.getElementById("emby_lib_matrix");
    const select = panel.querySelector(".cw-profile-switcher select");
    void window.__cwRenderConnectionWhitelist(panel, {key: "EMBY", provider: "emby"}, {force: true});
    await new Promise(resolve => setTimeout(resolve, 300));
    select.value = "B";
    select.dispatchEvent(new Event("change", {bubbles: true}));
    await new Promise(resolve => setTimeout(resolve, 2500));
    return {text: host.textContent, libs: (host.__cwConnectionWhitelistLibs || []).map(lib => String(lib.id)), inst: host.__cwConnectionWhitelistInstance};
  });
  assert.equal(result.inst, "B");
  assert.deepEqual(result.libs, ["7"]);
  assert.match(result.text, /B library/);
  assert.doesNotMatch(result.text, /A library|Missing library/);
  assert.deepEqual(errors, []);
});
