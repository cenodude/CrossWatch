/* tests/page-layout-audit.browser.mjs */
/* CrossWatch - Cross-device page layouts and flat-dark contrast */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from "node:test";
import assert from "node:assert/strict";
import {execFileSync} from "node:child_process";
import {readFile, mkdir} from "node:fs/promises";
import {chromium, webkit} from "playwright";

const viewports = [[320, 740], [390, 844], [740, 360], [844, 390], [932, 430], [768, 1024], [820, 1180], [1024, 768], [1194, 834], [1366, 1024], [1920, 1080]];
const item = {key: "movie:tmdb:1", type: "movie", title: "A long movie title to check wrapping and available space", year: 2026, ids: {tmdb: "1"}};
const pair = {id: "pair", source: "PLEX", target: "TRAKT", source_instance: "default", target_instance: "default", mode: "two-way", features: {history: true}};
const capture = {path: "history.json", provider: "PLEX", instance: "default", feature: "history", created_at: "2026-11-10T12:00:00Z", stats: {count: 1}, items: {[item.key]: item}};
const engines = {chromium, webkit};
const targets = {
  main: ["dashboard-widgets.js", "CW.DashboardWidgets.refresh({force:true})", "#dashboard-widgets-card"],
  snapshots: ["snapshots.js", "Promise.resolve()", "#ss-list"],
  playlists: ["playlists.js", "Playlists.mount()", ".pl-page"],
  interactive_sync: ["interactive-sync.js", "InteractiveSync.refresh()", ".is-route"],
  capture_compare: ["capture-compare/index.js", "CaptureComparePage.mount(document.querySelector('#page-capture_compare'))", ".cp-content"],
  import_export: ["import-export/index.js", "ImportExport.mount(document.querySelector('#page-import_export'))", ".ie-page"],
  logs: ["logs.js", "LogsPage.mount(document.querySelector('#page-logs'))", ".logs-page"],
  scrobbler: ["scrobbler.js", "Scrobbler.init()", ".sc2-summary"],
  scheduling: ["scheduler.js", "Promise.resolve()", "#sched_advanced_mount"],
  maintenance: ["maintenance/index.js", "MaintenancePage.mount(document.querySelector('#page-maintenance'))", ".cw-maint"],
};

async function open(t, kind, empty, theme = "flat-dark") {
  const browser = await engines[process.env.CW_AUDIT_BROWSER || "chromium"].launch({headless: true});
  t.after(() => browser.close());
  const page = await browser.newPage({viewport: {width: 1194, height: 834}, hasTouch: true});
  page.setDefaultTimeout(7000);
  const errors = [];
  page.on("pageerror", error => errors.push(error.message));
  t.after(() => assert.deepEqual(errors, []));
  const source = await readFile(new URL("../ui_frontend.py", import.meta.url), "utf8");
  const html = await page.evaluate(({source, kind, theme}) => {
    const doc = new DOMParser().parseFromString(source.slice(source.indexOf("<!doctype html>"), source.indexOf("</html>", source.indexOf("<!doctype html>")) + 7), "text/html");
    doc.querySelectorAll("script").forEach(el => el.remove());
    doc.querySelectorAll("style").forEach(el => el.remove());
    doc.querySelector("#cw-main-loading")?.remove();
    if (theme !== "original") doc.documentElement.dataset.cwTheme = theme;
    doc.documentElement.dataset.tab = kind;
    doc.documentElement.className = theme === "original" ? "cw-theme-original" : `cw-theme-${theme === "flat-light" ? "light" : "dark"}`;
    doc.querySelector(theme === "original" ? "#cw-theme-flat-css" : "#cw-theme-original-css")?.remove();
    const layout = doc.querySelector("#layout");
    layout.classList.add(kind === "main" ? "single" : "full");
    for (const el of layout.children) {
      el.classList.toggle("hidden", kind === "main" ? el.id.startsWith("page-") : el.id !== `page-${["scrobbler", "scheduling"].includes(kind) ? "settings" : kind}`);
    }
    if (["scrobbler", "scheduling"].includes(kind)) doc.querySelectorAll(".cw-settings-pane").forEach(el => el.classList.toggle("active", el.dataset.pane === kind));
    const setup = doc.createElement("script");
    setup.textContent = `window.CW={AuthState:{read:()=>({isAdmin:true,isManaged:false,permissions:{write:true}})}};window.cx={pairs:[]};window.cfg={};`;
    doc.body.append(setup);
    for (const file of ["provider-meta", "feature-meta", "icon-select", "selection-bar"]) {
      const script = doc.createElement("script"); script.src = `/assets/helpers/${file}.js`; doc.body.append(script);
    }
    return "<!doctype html>" + doc.documentElement.outerHTML.replaceAll("__CW_ASSET_BLOCK__", "").replaceAll("__CW_CURRENT_VERSION__", "Preview");
  }, {source, kind, theme});
  await page.route("https://audit.test/**", async route => {
    const url = new URL(route.request().url()), path = url.pathname;
    if (path.startsWith("/assets/")) {
      try { return route.fulfill({body: await readFile(new URL(".." + path, import.meta.url)), contentType: path.endsWith(".css") ? "text/css" : path.endsWith(".woff2") ? "font/woff2" : path.endsWith(".svg") ? "image/svg+xml" : "text/javascript"}); }
      catch { return route.fulfill({status: 404, body: ""}); }
    }
    if (!path.startsWith("/api/")) return route.fulfill({contentType: "text/html", body: html});
    const items = empty ? [] : [item];
    let json = {ok: true};
    if (path === "/api/config") json = {pairs: [pair], ui: {show_recent_history: true, show_latest_ratings: true, show_recent_scrobble: true}};
    if (path === "/api/scheduling") json = {enabled: true, mode: "daily_time", daily_time: "03:30", advanced: {enabled: true, jobs: []}};
    if (path === "/api/scrobbler/overview") json = {routes: empty ? [] : [{id: "route", name: item.title, provider: "plex", enabled: true, targets: ["trakt"]}], webhooks: [], runtime: {}, settings: {}, summary: {}};
    if (path === "/api/pairs") json = empty ? [] : [pair];
    if (path === "/api/dashboard/widgets") json = Object.fromEntries(["recent_history", "recent_scrobble", "recent_progress", "latest_ratings", "recent_playlists"].map(key => [key, {items}]));
    if (path === "/api/state/wall") json = {items};
    if (path === "/api/snapshots/manifest") json = {providers: [{id: "PLEX", label: "Plex", configured: true, features: {history: true}, instances: [{id: "default", label: "Default", configured: true}]}]};
    if (path === "/api/snapshots/list") json = {snapshots: empty ? [] : [capture]};
    if (path === "/api/snapshots/read") json = {snapshot: capture};
    if (path === "/api/snapshots/diff/extended") json = {diff: {ok: true, items: items.map(item => ({key: item.key, status: "added", new: item})), total: items.length, selected_feature: "history", available_features: ["history"]}};
    if (path === "/api/interactive-sync/review") json = {id: "review", revision: 1, status: empty ? "review" : "applying", pair, features: ["history"], counts: {changes: items.length, selected: items.length}, progress: {operation: "apply", stage: "finalizing", label: "Saving sync results", running: !empty, elapsed_seconds: 2, stage_seconds: 2, items_read: 8, recent: [{text: "Saving sync results", at: 1794335190}]}};
    if (path === "/api/interactive-sync/review/rows") json = {items: [], total: 0, offset: 0, counts: {}, selection_version: 1};
    if (path === "/api/playlists/providers") json = {providers: []};
    if (path === "/api/playlists/endpoints") json = {endpoints: empty ? [] : [{id: "endpoint", name: item.title, provider: "PLEX", instance: "default", kind: "playlist", playlist_id: "one", enabled: true}]};
    if (path === "/api/playlists/mappings") json = {mappings: []};
    if (path === "/api/playlists/rulesets") json = {rulesets: []};
    if (path === "/api/playlists/activity") json = {items: []};
    if (path === "/api/import/options") json = {sources: [{id: "trakt", label: "Trakt"}], targets: [{id: "default", label: "Default", connected: true}], limits: {upload_bytes: 104857600}};
    if (path === "/api/export/options") json = {providers: ["PLEX"], instances: {PLEX: [{id: "default", label: "Default"}]}, features: ["history", "watchlist", "ratings"], formats: ["json", "csv"]};
    if (path === "/api/logs/archive") json = {sessions: empty ? [] : [{id: "log", channel: "sync", started_at: 1794310000, finished_at: 1794310100, lines: 1, pairs: [pair]}], pairs: [pair], retention_days: 7, max_bytes: 10485760, total_bytes: 1200};
    if (path.endsWith("/lines")) json = {items: empty ? [] : [{id: 1, text: "Sync completed for " + item.title, level: "INFO", at: "2026-11-10T18:26:30Z"}], total: items.length};
    return route.fulfill({json});
  });
  const query = kind === "interactive_sync" ? "?session=review" : kind === "capture_compare" ? "?a=before.json&b=after.json" : "";
  await page.goto(`https://audit.test/#${kind}${query}`);
  const [script, mount, ready] = targets[kind];
  const module = script.includes("/index.js") || kind === "logs";
  if (module) await page.evaluate(path => import(path), `/assets/js/${script}`);
  else await page.addScriptTag({url: `/assets/js/${script}`});
  await page.evaluate(mount);
  await page.locator(ready).waitFor({state: "attached"});
  await page.evaluate(() => document.fonts.ready);
  return page;
}

for (const theme of ["original", "flat-dark", "flat-light"]) for (const kind of Object.keys(targets)) {
  for (const empty of [false, true]) {
    test(`${theme} ${kind}: ${empty ? "empty" : "populated"} across devices`, async t => {
      const page = await open(t, kind, empty, theme);
      for (const [width, height] of viewports) {
        await page.setViewportSize({width, height});
        await page.waitForTimeout(80);
        const overflow = await page.evaluate(() => ({width: innerWidth, scroll: document.documentElement.scrollWidth, offenders: [...document.querySelectorAll("body *")].filter(el => { const r = el.getBoundingClientRect(); return el.checkVisibility() && r.width && r.right > innerWidth + 1 && !el.closest('table, [role="listbox"]'); }).slice(0, 8).map(el => el.id || el.className)}));
        assert.ok(overflow.scroll <= width + 1, JSON.stringify({kind, ...overflow}));
        if (process.env.CW_AUDIT_SCREENSHOTS && !empty) {
          await mkdir(process.env.CW_AUDIT_SCREENSHOTS, {recursive: true});
          await page.screenshot({path: `${process.env.CW_AUDIT_SCREENSHOTS}/${theme}-${kind}-${width}x${height}.png`, fullPage: true});
        }
      }
    });
  }
}

test("flat-dark surfaces, secondary text and disabled sync actions remain distinguishable", async t => {
  const page = await open(t, "interactive_sync", false);
  const colors = await page.evaluate(() => {
    const root = getComputedStyle(document.documentElement);
    const token = name => root.getPropertyValue(name).trim();
    return {background: token("--bg"), panel: token("--panel"), raised: token("--cw-theme-surface-strong"), border: token("--border"), text: token("--fg"), muted: token("--muted"), disabled: getComputedStyle(document.querySelector(".is-header-actions button:disabled")).opacity};
  });
  const luminance = hex => {
    const linear = hex.slice(1).match(/../g).map(part => parseInt(part, 16) / 255).map(c => c <= .04045 ? c / 12.92 : ((c + .055) / 1.055) ** 2.4);
    return linear.reduce((sum, value, index) => sum + value * [.2126, .7152, .0722][index], 0);
  };
  const contrast = (a, b) => { const values = [luminance(a), luminance(b)].sort((a, b) => b - a); return (values[0] + .05) / (values[1] + .05); };
  assert.ok(contrast(colors.panel, colors.background) >= 1.1);
  assert.ok(contrast(colors.border, colors.panel) >= 1.6);
  for (const background of [colors.background, colors.panel, colors.raised]) {
    assert.ok(contrast(colors.text, background) >= 4.5);
    assert.ok(contrast(colors.muted, background) >= 4.5);
  }
  assert.ok(Number(colors.disabled) >= .6 && Number(colors.disabled) < 1);
});


test("sign-in and verification controls remain reachable in portrait and landscape", async t => {
  const html = execFileSync("python", ["-c", "from api.appAuthAPI import _login_html; print(_login_html(plex_sso_available=True, oidc_available=True, oidc_label='Sign in with your connected account'))"], {encoding: "utf8", maxBuffer: 1024 * 1024});
  const browser = await engines[process.env.CW_AUDIT_BROWSER || "chromium"].launch({headless: true});
  t.after(() => browser.close());
  const page = await browser.newPage();
  const layoutHtml = await page.evaluate(source => {
    const doc = new DOMParser().parseFromString(source, "text/html");
    doc.querySelectorAll("script").forEach(el => el.remove());
    return "<!doctype html>" + doc.documentElement.outerHTML;
  }, html);
  await page.route("https://signin.test/**", async route => {
    const path = new URL(route.request().url()).pathname;
    if (path.startsWith("/assets/")) {
      try { return route.fulfill({body: await readFile(new URL(".." + path, import.meta.url)), contentType: path.endsWith(".css") ? "text/css" : path.endsWith(".svg") ? "image/svg+xml" : "font/woff2"}); }
      catch { return route.fulfill({status: 404, body: ""}); }
    }
    return route.fulfill({contentType: "text/html", body: layoutHtml});
  });
  await page.goto("https://signin.test/login");
  assert.equal(await page.locator("script").count(), 0);
  await page.locator("#totp-wrap").evaluate(el => el.classList.remove("cw-totp-hidden"));
  for (const [width, height] of viewports) {
    await page.setViewportSize({width, height});
    assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1), `${width}x${height}`);
    for (const id of ["u", "p", "totp", "go", "go-plex", "go-oidc"]) {
      const control = page.locator(`#${id}`);
      await control.evaluate(el => el.scrollIntoView({block: "center", behavior: "instant"}));
      const box = await control.boundingBox();
      assert.ok(box && box.x >= 0 && box.x + box.width <= width + 1 && box.y >= 0 && box.y + box.height <= height + 1, JSON.stringify({id, width, height, box}));
    }
  }
});
