/* tests/hub-airing.browser.mjs */
/* CrossWatch - Hub upcoming episode navigation and cached refresh */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude */
import './i18n-browser-setup.mjs';
import test from "node:test";
import assert from "node:assert/strict";
import {readFile, mkdir} from "node:fs/promises";
import {execFileSync} from "node:child_process";
import {chromium} from "playwright";

const template = execFileSync("python", ["-c", "import ui_frontend; print(ui_frontend.get_hub_html({'is_admin':True,'username':'preview'}))"], {encoding:"utf8", maxBuffer:1024*1024});

for (const theme of ["original", "flat-dark", "flat-light"]) for (const width of [390, 1440]) {
  test(`${theme} ${width}: Hub airing calendar shows scoped results and refreshes in place`, async t => {
    const browser = await chromium.launch({headless:true});
    t.after(() => browser.close());
    const page = await browser.newPage({viewport:{width, height:1000}, reducedMotion:"reduce", timezoneId:"UTC"});
    page.setDefaultTimeout(8000);
    await page.clock.install({time:new Date("2026-11-22T12:00:00Z")});
    const errors = [];
    page.on("pageerror", error => errors.push(error.message));
    const html = await page.evaluate(({source, theme}) => {
      const doc = new DOMParser().parseFromString(source, "text/html");
      doc.querySelectorAll("script").forEach(node => node.remove());
      if (theme !== "original") doc.documentElement.dataset.cwTheme = theme;
      else doc.documentElement.removeAttribute("data-cw-theme");
      doc.documentElement.className = theme === "original" ? "cw-theme-original" : `cw-theme-${theme === "flat-light" ? "light" : "dark"}`;
      doc.querySelector(theme === "original" ? "#cw-theme-flat-css" : "#cw-theme-original-css")?.remove();
      const boot = doc.createElement("script");
      boot.textContent = 'window.CW={AuthState:{read:()=>({isAdmin:true,permissions:{write:true}})},ProfileMediaModal:{canOpen:()=>true,open:item=>window.opened=item}};';
      doc.body.append(boot);
      for (const path of ["helpers/view-cache", "helpers/card-actions", "helpers/airing-card", "helpers/selection-bar", "helpers/profile-datetime", "helpers/month-bar", "helpers/icon-select", "js/profile-page"]) {
        const script = doc.createElement("script");
        script.src = `/assets/${path}.js`;
        doc.body.append(script);
      }
      return "<!doctype html>" + doc.documentElement.outerHTML;
    }, {source:template, theme});
    let phase = "cold", reads = 0;
    const rows = [{key:"tmdb:12:1:2", title:"The Institute", season:1, episode:2, episode_title:"The next chapter", air_date:"2026-11-22", show_ids:{tmdb:"12"}, art:{tmdb:"12"}, has_still:true},
      ...[3, 4].map(episode => ({key:`tmdb:13:2:${episode}`, title:"South Park", season:2, episode, episode_title:`Episode ${episode}`, air_date:"2026-11-25", show_ids:{tmdb:"13"}, art:{tmdb:"13"}}))];
    await page.route("https://hub-airing.test/**", async route => {
      const url = new URL(route.request().url());
      if (url.pathname.startsWith("/assets/")) {
        try { return route.fulfill({body:await readFile(new URL(".." + url.pathname, import.meta.url)), contentType:url.pathname.endsWith(".css") ? "text/css" : url.pathname.endsWith(".woff2") ? "font/woff2" : "text/javascript"}); }
        catch { return route.fulfill({status:404, body:""}); }
      }
      if (url.pathname.startsWith("/art/")) return route.fulfill({contentType:"image/svg+xml", body:'<svg xmlns="http://www.w3.org/2000/svg" width="780" height="440"><rect width="780" height="440" fill="#294353"/><circle cx="530" cy="100" r="230" fill="#50766e"/><path d="M0 440 390 100 780 440" fill="#16222e"/></svg>'});
      if (url.pathname.startsWith("/api/")) {
        let json = {ok:true};
        if (url.pathname === "/api/profile") json.user = {is_admin:true, username:"preview", permissions:{write:true}, preferences:{}, sessions:[]};
        if (url.pathname === "/api/playback_progress/items") json = {items:[{provider:"crosswatch", instance_id:"default", canonical_key:"tmdb:42", media_type:"movie", title:"Almost finished", ids:{tmdb:"42"}, progress_percent:99, remaining_seconds:24}], total:1};
        if (url.pathname === "/api/profile/airing") {
          reads++;
          if (phase === "retry" && url.searchParams.get("retry_view") === "1") phase = "fresh";
          assert.equal(url.searchParams.get("background"), "1");
          assert.equal(url.searchParams.get("day"), "2026-11-22");
          json = phase === "cold" ? {ok:true, _view:{ready:false, refreshing:true, retry_after:1}} : {ok:true, metadata_available:phase !== "unconfigured", airing:phase === "empty" || phase === "unconfigured" ? [] : rows, _view:{ready:true, refreshing:phase === "cached", version:phase, error:["failed", "retry"].includes(phase) ? "refresh_failed" : null, retry_after:1}};
        }
        return route.fulfill({json});
      }
      return route.fulfill({contentType:"text/html", body:html});
    });
    await page.goto("https://hub-airing.test/hub");
    const section = page.locator("#profile-airing"), calendar = page.locator("#profile-airing-days");
    await page.locator(".cw-hub-continue").getByText("1m left", {exact:true}).waitFor();
    assert.equal(await page.locator(".cw-hub-continue").getByText("0m left", {exact:true}).count(), 0);
    await page.locator("#profile-airing[data-view-loading]").waitFor();
    assert.equal(await section.getByRole("progressbar").count(), 0);
    assert.equal(await section.evaluate(node => node.previousElementSibling.classList.contains("cw-hub-continue")), true);
    assert.equal(await calendar.locator("[data-calendar-date]").count(), 30);
    const sectionTop = await section.evaluate(node => node.getBoundingClientRect().top);
    phase = "cached";
    await calendar.locator('.cw-airing-front').getByText("The Institute", {exact:true}).waitFor();
    assert.equal(await section.locator('[data-view-status]').isHidden(), true);
    assert.equal(await section.locator('[data-view-status]').textContent(), "");
    assert.equal(await calendar.locator('.trk-calendar-release').count(), 3);
    assert.equal(await calendar.locator('.trk-calendar-label').first().evaluate(node => getComputedStyle(node).color), "rgba(255, 255, 255, 0.85)");
    assert.equal(await calendar.locator('[data-calendar-date="2026-11-25"] .trk-calendar-release').count(), 2);
    assert.equal(await section.evaluate(node => node.getBoundingClientRect().top), sectionTop);
    await calendar.locator('.cw-airing-front').getByText("The Institute", {exact:true}).click();
    await calendar.locator('.cw-airing-next').first().click();
    assert.equal(await page.evaluate(() => window.opened.tmdb), "12");
    await section.getByRole("button", {name:"Next days", exact:true}).click();
    assert.ok(await calendar.evaluate(node => node.scrollLeft > 0));
    const scrolled = await calendar.evaluate(node => node.scrollLeft);
    await page.evaluate(() => { window.retainedReleases = [...document.querySelectorAll('.cw-airing-card')]; });
    phase = "failed";
    await section.getByText(/Refresh failed/).waitFor();
    assert.equal(await calendar.locator('.trk-calendar-release').count(), 3);
    assert.equal(await calendar.evaluate(node => node.scrollLeft), scrolled);
    if (theme === "original" && width === 1440) {
      phase = "fresh";
      await page.clock.runFor(30001);
    } else {
      await section.getByRole("button", {name:"Retry"}).evaluate(node => node.scrollIntoView({block:"center", behavior:"instant"}));
      phase = "retry";
      await section.getByRole("button", {name:"Retry"}).click();
    }
    await section.locator('[data-view-status]').waitFor({state:"hidden"});
    assert.equal(await page.evaluate(() => retainedReleases.every(node => node.isConnected)), true);
    await section.getByRole("button", {name:"Today", exact:true}).click();
    assert.equal(await calendar.evaluate(node => node.scrollLeft), 0);
    await mkdir(".tmp/hub-airing", {recursive:true});
    await section.screenshot({path:`.tmp/hub-airing/${theme}-${width}.png`});
    assert.ok(await calendar.evaluate(node => node.getBoundingClientRect().right <= innerWidth + 1));
    phase = "empty";
    await page.evaluate(() => window.dispatchEvent(new CustomEvent("cw:view-updated", {detail:{url:"/api/profile/airing?day=2026-11-22"}})));
    await section.getByText("No episodes scheduled in the next 30 days.").waitFor();
    await section.screenshot({path:`.tmp/hub-airing/${theme}-${width}-empty.png`});
    phase = "unconfigured";
    await page.evaluate(() => window.dispatchEvent(new CustomEvent("cw:view-updated", {detail:{url:"/api/profile/airing?day=2026-11-22"}})));
    await section.getByText("Add a TMDB API key in Settings to see upcoming episodes.").waitFor();
    assert.ok(reads >= 4);
    assert.deepEqual(errors, []);
  });
}
