/* tests/overview-default-instances.browser.mjs */
/* CrossWatch - Default instance overview selection and admin access tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import assert from "node:assert/strict";
import test from "node:test";
import {readFile} from "node:fs/promises";
import {chromium} from "playwright";

const scope = "__default__";
const scripts = await Promise.all(["assets/helpers/provider-meta.js", "assets/helpers/api.js", "assets/js/overview-profile.js", "assets/js/settings-scope.js"].map(path => readFile(new URL(`../${path}`, import.meta.url), "utf8")));

async function fixture(t, {managed = false, profiles = [], stored = ""} = {}) {
  const browser = await chromium.launch({headless:true});
  t.after(() => browser.close());
  const page = await browser.newPage();
  page.setDefaultTimeout(5000);
  const requests = [], errors = [];
  page.on("pageerror", error => errors.push(error.message));
  t.after(() => assert.deepEqual(errors, []));
  await page.route("https://overview.test/**", async route => {
    const request = route.request(), url = new URL(request.url());
    requests.push({path:url.pathname, scope:url.searchParams.get("user_profile"), headers:request.headers(), method:request.method()});
    if (!url.pathname.startsWith("/api/")) return route.fulfill({contentType:"text/html", body:'<!doctype html><html><body><div id="cw-view-as"><select id="cw-view-as-select"></select></div></body></html>'});
    const data = url.pathname === "/api/app-auth/status" ? {is_admin:!managed,user:{is_admin:!managed,profile_id:managed ? "alice" : ""}}
      : url.pathname === "/api/user-profiles" ? {items:profiles}
      : url.pathname.startsWith("/api/user-profiles/") ? {profile:profiles.find(row => url.pathname.endsWith(row.id))}
      : {};
    return route.fulfill({json:data});
  });
  await page.goto("https://overview.test/");
  await page.evaluate(({stored,managed}) => {
    document.documentElement.dataset.cwRole = managed ? "user" : "admin";
    if (managed) document.documentElement.dataset.cwProfileId = "alice";
    if (stored) localStorage.setItem("cw.overview.profile", stored);
  }, {stored,managed});
  const boot = async () => {
    for (const content of scripts) await page.addScriptTag({content});
    await page.evaluate(() => window.CW.OverviewProfile.ready);
  };
  await boot();
  return {page,requests,boot};
}

test("admin can select and restore defaults without any user profiles", async t => {
  const {page,boot} = await fixture(t);
  const picker = page.locator("#cw-view-as-select");
  assert.equal(await picker.isEnabled(), true);
  assert.deepEqual(await picker.locator("option").allTextContents(), ["All profiles", "Admin / Default instances"]);
  await picker.selectOption(scope);
  const state = await page.evaluate(() => {
    const view = window.CW.OverviewProfile;
    return {id:view.id,label:view.label,profile:view.profile,defaults:view.isDefaultView,
      own:view.matchesEndpoint("PLEX","default"),other:view.matchesEndpoint("PLEX","PLEX-P01"),
      source:view.matchesSources({sources_by_provider:{plex:["default"]}}),
      otherSource:view.matchesSources({sources_by_provider:{plex:["PLEX-P01"]}}),
      tracker:view.instanceForProvider("CROSSWATCH"),settings:window.CW.SettingsScope.active()};
  });
  assert.deepEqual(state, {id:scope,label:"Admin / Default instances",profile:null,defaults:true,own:true,other:false,source:true,otherSource:false,tracker:"default",settings:false});
  await page.reload();
  await boot();
  assert.equal(await picker.inputValue(), scope);
  await picker.selectOption("");
  assert.equal(await page.evaluate(() => window.CW.OverviewProfile.matchesEndpoint("PLEX","PLEX-P01")), true);
});

test("default overview does not send impersonation or assignment headers", async t => {
  const {page,requests} = await fixture(t, {profiles:[{id:"alice",label:"Alice",instances:{PLEX:["PLEX-P01"]}}]});
  await page.locator("#cw-view-as-select").selectOption(scope);
  await page.evaluate(async () => {
    await fetch("/api/config");
    await fetch("/api/pairs", {method:"POST"});
    await fetch("/api/run", {method:"POST"});
    await fetch("/api/events/calendar");
    await fetch("/api/events/statistics?user_profile=all");
  });
  for (const request of requests.filter(row => ["/api/config","/api/pairs","/api/run"].includes(row.path))) {
    assert.equal(request.headers["x-cw-view-as"], undefined);
    assert.equal(request.headers["x-cw-assign-profile"], undefined);
  }
  assert.equal(requests.find(row => row.path === "/api/events/calendar").scope, scope);
  assert.equal(requests.find(row => row.path === "/api/events/statistics").scope, "all");
  await page.locator("#cw-view-as-select").selectOption("alice");
  await page.evaluate(() => fetch("/api/config"));
  assert.equal(requests.at(-1).headers["x-cw-view-as"], "alice");
});

test("managed users ignore a saved admin default selection", async t => {
  const {page} = await fixture(t, {managed:true,stored:scope,profiles:[{id:"alice",label:"Alice",instances:{PLEX:["PLEX-P01"]}}]});
  assert.equal(await page.locator("#cw-view-as").evaluate(el => el.classList.contains("hidden")), true);
  assert.deepEqual(await page.evaluate(scope => ({id:window.CW.OverviewProfile.id,changed:window.CW.OverviewProfile.setActive(scope),defaults:window.CW.OverviewProfile.isDefaultView}), scope), {id:"alice",changed:false,defaults:false});
});

test("late statistics responses cannot restore another scope's counts or cache", async t => {
  const {page} = await fixture(t);
  let release, requested;
  const held = new Promise(resolve => { release = resolve; });
  const started = new Promise(resolve => { requested = resolve; });
  await page.route("https://overview.test/api/insights?**", async route => {
    const selected = new URL(route.request().url()).searchParams.get("user_profile");
    if (!selected) { requested(); await held; }
    const count = selected === scope ? 1 : 99;
    return route.fulfill({json:{features:{watchlist:{now:count,providers:{plex:count},providers_active:{plex:true},providers_instances:{plex:{default:count}}}},instances_by_provider:{plex:["default"]}}});
  });
  await page.evaluate(() => {
    document.body.insertAdjacentHTML("beforeend", '<div id="stats-card"><span id="stat-now">0</span></div>');
    window.getConfiguredProviders = () => new Set(["PLEX"]);
    window._cfgCache = {plex:{}};
    localStorage.setItem("insights.settings.v1", JSON.stringify({instances:{plex:["default","PLEX-P01"]},known_instances:{plex:["default","PLEX-P01"]}}));
    localStorage.setItem("insights.tiles.v1", JSON.stringify({watchlist:{keys:["PLEX"],totals:{plex:999},active:{plex:true}}}));
  });
  await page.addScriptTag({content:await readFile(new URL("../assets/js/insights.js", import.meta.url), "utf8")});
  await page.evaluate(() => { window.pendingInsights = window.refreshInsights(); });
  await started;
  await page.locator("#cw-view-as-select").selectOption(scope);
  await page.waitForFunction(() => document.querySelector("#stat-now").dataset.v === "1");
  release();
  await page.evaluate(() => window.pendingInsights);
  assert.equal(await page.locator("#stat-now").textContent(), "1");
  assert.deepEqual(await page.evaluate(() => JSON.parse(localStorage.getItem("insights.settings.v1")).instances.plex), ["default","PLEX-P01"]);
  const cache = await page.evaluate(scope => JSON.parse(localStorage.getItem(`insights.tiles.v1.${scope}`)), scope);
  assert.equal(cache.watchlist.totals.plex, 1);
  assert.equal(await page.evaluate(() => localStorage.getItem("insights.tiles.v1.all")), null);
  await page.evaluate(() => window.dispatchEvent(new CustomEvent("cw:sync-state-cleared")));
  assert.equal(await page.evaluate(() => Object.keys(localStorage).some(key => key.startsWith("insights.tiles.v1"))), false);
});

test("default view displays its part of a mixed run and hides unattributed reports", async t => {
  const {page} = await fixture(t);
  const pairs = [{id:"own",source:"PLEX",target:"CROSSWATCH",features:{watchlist:{enable:true}}},
    {id:"other",source:"PLEX",source_instance:"PLEX-P01",target:"CROSSWATCH",target_instance:"CW-P01",features:{watchlist:{enable:true}}}];
  let summary = {running:true,run_id:"mixed",pair_scope_ids:["own","other"],features:{watchlist:{added:1,spotlight_add:[{title:"Other person's movie"}]}},enabled:{watchlist:true}};
  let own = {...summary,summary_scope:scope,pair_scope_ids:["own"],details_available:true,features:{watchlist:{added:1,spotlight_add:[{title:"Admin movie"}]}}};
  await page.route("https://overview.test/api/pairs?**", route => route.fulfill({json:pairs}));
  await page.route("https://overview.test/api/run/summary?**", route => route.fulfill({json:new URL(route.request().url()).searchParams.get("user_profile") === scope ? own : summary}));
  await page.evaluate(() => {
    document.body.insertAdjacentHTML("beforeend", '<div id="ux-progress"></div><div id="ux-lanes"></div><div id="ux-spotlight"></div>');
    window.EventSource = class extends EventTarget { readyState = 1; close() {} };
  });
  for (const path of ["assets/js/syncbar.js","assets/js/run-summary-stream.js","assets/js/main.js"]) {
    await page.addScriptTag({content:await readFile(new URL(`../${path}`, import.meta.url), "utf8")});
  }
  await page.evaluate(() => window.UX.refresh());
  await page.waitForFunction(() => document.querySelector("#ux-lanes").textContent.includes("Other person's movie"));
  await page.evaluate(() => { window.oldSummaryStream = window.esSum; });
  await page.locator("#cw-view-as-select").selectOption(scope);
  assert.equal((await page.locator("#ux-lanes").textContent()).includes("Other person's movie"), false);
  await page.waitForFunction(() => document.querySelector("#ux-lanes").textContent.includes("Admin movie"));
  await page.evaluate(() => {
    window.oldSummaryStream.onmessage({data:JSON.stringify({run_id:"stale",running:false,exit_code:1,features:{watchlist:{added:99,spotlight_add:[{title:"Other person's movie"}]}}})});
    window.oldSummaryStream.dispatchEvent(new MessageEvent("run:error", {data:"{}"}));
  });
  assert.equal((await page.locator("#ux-lanes").textContent()).includes("Other person's movie"), false);
  assert.equal(await page.evaluate(() => window.syncBar.isRunning()), true);
  own = {...own,pair_scope_ids:[],features:{}};
  await page.evaluate(() => window.UX.refresh());
  await page.waitForFunction(() => !document.querySelector("#ux-lanes").textContent.includes("Admin movie"));
  summary = {...summary,run_id:"unknown",pair_scope_ids:[],features:{watchlist:{added:1,spotlight_add:[{title:"Unscoped movie"}]}}};
  own = {...own,run_id:"unknown",details_available:false,features:{}};
  await page.evaluate(() => window.UX.refresh());
  assert.equal((await page.locator("#ux-lanes").textContent()).includes("Unscoped movie"), false);
  await page.waitForFunction(() => document.querySelector("#ux-lanes").textContent.includes("Item details are unavailable"));
  await page.locator("#cw-view-as-select").selectOption("");
  await page.waitForFunction(() => document.querySelector("#ux-lanes").textContent.includes("Unscoped movie"));
});

test("named users see their sync results and a neutral legacy fallback", async t => {
  const {page} = await fixture(t, {managed:true,profiles:[{id:"alice",label:"Alice",instances:{PLEX:["alice"],TRAKT:["alice"]}}]});
  const active = {run_id:"mixed",summary_scope:"alice",details_available:true,progress_scoped:true,running:true,run_busy:true,
    timeline:{start:true,pre:true,post:true,done:false},enabled:{watchlist:true},
    current_pair:{pair_id:"alice",src:"PLEX",dst:"TRAKT"},
    features:{watchlist:{added:2,spotlight_add:[{title:"Alice movie"}]}}};
  let summary = {...active,waiting:true,current_pair:null,timeline:{start:true},features:{}};
  await page.route("https://overview.test/api/run/summary?**", route => route.fulfill({json:summary}));
  await page.evaluate(() => {
    document.body.insertAdjacentHTML("beforeend", '<button id="run">Synchronize</button><div id="ux-progress"></div><div id="ux-lanes"></div><div id="ux-spotlight"></div>');
    window.recomputeRunDisabled = () => { document.getElementById("run").disabled = false; };
    window.EventSource = class extends EventTarget { readyState = 1; close() {} };
  });
  for (const path of ["assets/js/syncbar.js","assets/js/run-summary-stream.js","assets/js/main.js"]) {
    await page.addScriptTag({content:await readFile(new URL(`../${path}`, import.meta.url), "utf8")});
  }
  await page.evaluate(() => window.UX.refresh());
  await page.waitForFunction(() => window.syncBar.isWaiting());
  assert.equal(await page.locator("#run").isDisabled(), true);
  assert.equal((await page.locator("#ux-progress").textContent()).includes("Waiting for your sync pairs"), true);
  summary = active;
  await page.evaluate(() => window.UX.refresh());
  await page.waitForFunction(() => document.querySelector("#ux-lanes").textContent.includes("Alice movie"));
  assert.equal(await page.evaluate(() => window.syncBar.isRunning()), true);
  summary = {...summary,running:false,exit_code:0,timeline:{start:true,pre:true,post:true,done:true}};
  await page.evaluate(() => window.UX.refresh());
  assert.equal(await page.evaluate(() => window.syncBar.isRunning()), false);
  assert.equal(await page.locator("#run").isDisabled(), true);
  summary = {...summary,run_id:"legacy",details_available:false,features:{}};
  await page.evaluate(() => window.UX.refresh());
  await page.waitForFunction(() => document.querySelector("#ux-lanes").textContent.includes("Item details are unavailable"));
  const text = await page.locator("#ux-lanes").textContent();
  assert.equal(text.includes("Alice movie"), false);
  assert.equal(text.includes("All profiles"), false);
  assert.equal(await page.locator("#ux-lanes .delta").count(), 0);
});

test("a pending now-playing response cannot replace the default view", async t => {
  const {page} = await fixture(t);
  let release, requested;
  const held = new Promise(resolve => { release = resolve; });
  const started = new Promise(resolve => { requested = resolve; });
  await page.route("https://overview.test/api/watch/currently_watching?**", async route => {
    return route.fulfill({json:{streams:[{_key:"own",source:"plex",provider_instance:"default",title:"Admin playback",media_type:"movie",state:"playing",updated:Math.floor(Date.now()/1000)}]}});
  });
  await page.route("https://overview.test/api/watch/currently_watching", async route => {
    requested();
    await held;
    return route.fulfill({json:{streams:[{_key:"other",source:"plex",provider_instance:"PLEX-P01",title:"Other playback",media_type:"movie",state:"playing",updated:Math.floor(Date.now()/1000)}]}});
  });
  await page.evaluate(() => { window._cfgCache = {tmdb_configured:true,ui:{}}; window.CW.Meta = {peek:()=>null,get:async()=>null,tmdbId:()=>"",isEpisode:()=>false}; document.documentElement.dataset.tab = "main"; });
  for (const path of ["assets/helpers/playing-card.js","assets/js/playingcard.js"]) {
    await page.addScriptTag({content:await readFile(new URL(`../${path}`, import.meta.url), "utf8")});
  }
  await started;
  await page.locator("#cw-view-as-select").selectOption(scope);
  await page.locator(".pc-title").filter({hasText:"Admin playback"}).waitFor({state:"attached"});
  const oldResponse = page.waitForResponse("https://overview.test/api/watch/currently_watching");
  release();
  await oldResponse;
  await page.evaluate(() => window.updatePlayingCard());
  assert.equal(await page.locator(".pc-title").textContent(), "Admin playback");
});

test("switching views clears watchlist posters and ignores a late failed request", async t => {
  const {page} = await fixture(t);
  await page.route("https://overview.test/api/config", route => route.fulfill({json:{tmdb_configured:true,ui:{}}}));
  let release, requested, hold = false;
  const held = new Promise(resolve => { release = resolve; });
  const started = new Promise(resolve => { requested = resolve; });
  await page.route("https://overview.test/api/state/wall?**", async route => {
    const own = new URL(route.request().url()).searchParams.get("user_profile") === scope;
    if (!own && hold) {
      requested();
      await held;
      return route.fulfill({status:500,json:{error:"Unavailable"}});
    }
    return route.fulfill({json:{ok:true,items:[{key:own ? "own" : "other",tmdb:own ? "1" : "2",type:"movie",title:own ? "Admin movie" : "Other movie",status:"both"}]}});
  });
  await page.evaluate(() => {
    document.documentElement.dataset.tab = "main";
    document.body.insertAdjacentHTML("beforeend", '<div id="placeholder-card"><div class="wall-wrap"><div id="wall-msg"></div><div id="poster-row"></div></div></div>');
    window._cfgCache = {tmdb_configured:true,ui:{}};
  });
  await page.addScriptTag({content:await readFile(new URL("../assets/helpers/watchlist-preview.js", import.meta.url), "utf8")});
  await page.evaluate(() => window.loadWall());
  assert.equal(await page.locator('#poster-row [data-key="other"]').count(), 1);
  hold = true;
  await page.evaluate(() => { window.pendingWall = window.loadWall(); });
  await started;
  await page.locator("#cw-view-as-select").selectOption(scope);
  assert.equal(await page.locator('#poster-row [data-key="other"]').count(), 0);
  await page.locator('#poster-row [data-key="own"]').waitFor({state:"attached"});
  release();
  await page.evaluate(() => window.pendingWall);
  assert.equal(await page.locator('#poster-row [data-key="other"]').count(), 0);
  assert.equal(await page.locator('#poster-row [data-key="own"]').count(), 1);
});
