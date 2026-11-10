/* tests/provider-picker.browser.mjs */
/* CrossWatch - Connection picker categories, search, keyboard and theme layout */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import assert from "node:assert/strict";
import {readFile, mkdir} from "node:fs/promises";
import test from "node:test";
import {chromium} from "playwright";

async function fixture(t) {
  const browser = await chromium.launch({headless:true});
  t.after(()=>browser.close());
  const page = await browser.newPage({viewport:{width:1400,height:1000}});
  page.setDefaultTimeout(6000);
  const errors = [];
  page.on("pageerror", error=>errors.push(error.message));
  t.after(()=>assert.deepEqual(errors,[]));
  await page.route("**/*",async route=>{
    const path = new URL(route.request().url()).pathname;
    if(path.startsWith("/assets/")) {
      try {return await route.fulfill({body:await readFile(new URL(".."+path,import.meta.url)),contentType:path.endsWith(".css")?"text/css":path.endsWith(".svg")?"image/svg+xml":path.endsWith(".woff2")?"font/woff2":path.endsWith(".png")?"image/png":"text/javascript"});}
      catch{return route.fulfill({status:404,body:""});}
    }
    if(path.startsWith("/api/")) return route.fulfill({json:{}});
    return route.fulfill({contentType:"text/html",body:`<!doctype html><html data-cw-theme="flat-dark"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
      ${["themes/tokens","css/base","crosswatch","css/layout","css/components","css/auth-providers","css/pages","css/page-theme","fonts/material-symbols-rounded","ui-shell","themes/flat","themes/original-coverage"].map(file=>`<link rel="stylesheet" href="/assets/${file}.css">`).join("")}
      </head><body><section id="page-settings"><button id="add" onclick="openAddConnection()">Add provider</button><div class="cw-settings-panel cw-settings-shell active"><div id="auth-providers"></div></div><div id="meta-provider-panel"><section class="section" id="sec-meta-tmdb"><div class="body"></div></section></div></section>
      <script>window.CW={HelpLinks:{url:()=>"#"}};window._cfgCache={};window.cwEnsureAuthSection=async()=>{};window.__CW_PROVIDER_STATUS__={providers:{plex:{connected:true},jellyfin:{connected:false,instances:{default:{connected:false}}}}};</script>
      <script src="/assets/helpers/provider-meta.js"></script><script src="/assets/helpers/dialog-focus.js"></script>
      <script>
      CW.API={Config:{load:async()=>window._cfgCache},j:async()=>{
        const groups=new Map();
        CW.ProviderMeta.authProviders().forEach(info=>{const key=CW.ProviderMeta.authGroupId(info.key);if(!groups.has(key))groups.set(key,[]);groups.get(key).push('<section class="section" id="'+CW.ProviderMeta.sectionId(info.key)+'"><div class="head"></div><div class="body"><div class="auth-card">Connection settings</div></div></section>');});
        return [...groups].map(([id,sections])=>'<section class="section" id="'+id+'"><div class="body">'+sections.join('')+'</div></section>').join('');
      }};
      </script><script src="/assets/helpers/providers-ui.js"></script><script>mountAuthProviders()</script></body></html>`});
  });
  await page.goto("https://picker.test/");
  await page.locator('[data-cw-auth-category="sec-auth-media"]').waitFor();
  return {page,dialog:page.getByRole("dialog"),picker:page.locator("#cw-auth-provider-picker")};
}

test("category Add buttons open their matching providers and generic Add resets filters",async t=>{
  const {page,picker}=await fixture(t);
  for(const [category,count] of [["sec-auth-media",5],["sec-auth-trackers",15],["sec-auth-clients",3],["sec-auth-metadata",2]]) {
    await page.locator(`[data-cw-auth-category="${category}"]`).click();
    assert.equal(await picker.locator(`[data-cw-picker-category="${category}"]`).getAttribute("aria-selected"),"true");
    assert.equal(await picker.locator(".cw-auth-picker-row").count(),count);
    assert.equal(await picker.locator(".cw-auth-picker-group").count(),1);
    await page.keyboard.press("Escape");
    assert.equal(await page.locator(`[data-cw-auth-category="${category}"]`).evaluate(el=>el===document.activeElement),true);
  }
  await page.locator("#add").click();
  assert.equal(await picker.getByRole("tab",{name:"All providers",exact:true}).getAttribute("aria-selected"),"true");
  assert.equal(await picker.locator(".cw-auth-picker-row").count(),25);
  await picker.getByRole("searchbox").fill("plex");
  assert.equal(await picker.locator(".cw-auth-picker-row").count(),1);
  assert.equal(await picker.getByRole("searchbox").evaluate(el=>el===document.activeElement),true);
  await page.evaluate(()=>CW.ProvidersUI.refreshAuthPresentation());
  assert.equal(await picker.getByRole("searchbox").inputValue(),"plex");
  assert.equal(await picker.locator(".cw-auth-picker-row").count(),1);
  await page.keyboard.press("Escape");
  await page.locator("#add").click();
  assert.equal(await picker.getByRole("searchbox").inputValue(),"");
});

test("search stays inside the selected category, tabs support keys, and a provider opens its form",async t=>{
  const {page,picker,dialog}=await fixture(t);
  await page.locator('[data-cw-auth-category="sec-auth-trackers"]').click();
  const search=picker.getByRole("searchbox");
  await search.fill("Plex");
  assert.equal(await picker.locator(".cw-auth-picker-row").count(),0);
  assert.match(await picker.locator(".cw-auth-picker-empty").textContent(),/No providers found/);
  await picker.getByRole("tab",{name:"Trackers",exact:true}).focus();
  await page.keyboard.press("ArrowLeft");
  assert.equal(await picker.getByRole("tab",{name:"Media servers",exact:true}).getAttribute("aria-selected"),"true");
  assert.equal(await picker.locator(".cw-auth-picker-row").count(),1);
  await page.keyboard.press("Home");
  assert.equal(await picker.getByRole("tab",{name:"All providers",exact:true}).getAttribute("aria-selected"),"true");
  await picker.locator('[data-cw-auth-pick="PLEX"]').click();
  await page.locator("#cw-auth-provider-form #sec-plex").waitFor();
  assert.equal(await picker.isVisible(),false);
  assert.equal(await dialog.getByRole("heading",{name:"Plex Authentication",exact:true}).isVisible(),true);
});

test("picker fits desktop, tablet and mobile in every theme",async t=>{
  const {page,picker,dialog}=await fixture(t);
  await page.locator("#add").click();
  await mkdir(".tmp",{recursive:true});
  for(const theme of ["original","flat-dark","flat-light"]) for(const [width,height] of [[1400,900],[1194,834],[1024,768],[800,1024],[932,430],[844,390],[740,360],[390,844],[320,740]]) {
    await page.setViewportSize({width,height});
    await page.evaluate(theme=>{
      document.documentElement.dataset.cwTheme=theme==="original"?"":theme;
      document.documentElement.className=theme==="original"?"cw-theme-original":theme==="flat-light"?"cw-theme-light":"";
    },theme);
    const box=await dialog.boundingBox();
    assert.ok(box.x>=0&&box.x+box.width<=width&&box.y>=0&&box.y+box.height<=height);
    assert.equal(await picker.evaluate(el=>el.scrollWidth<=el.clientWidth),true);
    assert.equal(await picker.locator(".cw-auth-picker-results").evaluate(el=>el.scrollWidth<=el.clientWidth),true);
    const inputBox=await picker.getByRole("searchbox").boundingBox();
    const iconBox=await picker.locator(".cw-auth-picker-search .material-symbols-rounded").boundingBox();
    assert.ok(Math.abs(inputBox.y+inputBox.height/2-iconBox.y-iconBox.height/2)<2);
    const columns=await picker.locator(".cw-auth-picker-grid").first().evaluate(el=>getComputedStyle(el).gridTemplateColumns.split(" ").length);
    assert.equal(columns,width>960?3:width>600?2:1);
    await picker.locator(".cw-auth-picker-results").evaluate(el=>el.scrollTop=el.scrollHeight);
    assert.equal(await picker.getByRole("searchbox").isVisible(),true);
    await picker.locator(".cw-auth-picker-results").evaluate(el=>el.scrollTop=0);
    await dialog.screenshot({path:`.tmp/provider-picker-${theme}-${width}.png`});
  }
});


test("mobile opening keeps the keyboard closed and keyboard focus stays in the picker",async t=>{
  const {page,picker,dialog}=await fixture(t);
  await page.setViewportSize({width:390,height:700});
  await page.locator('[data-cw-auth-category="sec-auth-clients"]').click();
  await page.waitForFunction(()=>document.activeElement?.getAttribute("role")==="tab");
  assert.equal(await picker.getByRole("tab",{name:"Media clients",exact:true}).evaluate(el=>el===document.activeElement),true);
  const last=picker.locator(".cw-auth-picker-row").last();
  await last.focus();
  await page.keyboard.press("Tab");
  assert.equal(await dialog.getByRole("button",{name:"Close",exact:true}).evaluate(el=>el===document.activeElement),true);
  await page.keyboard.press("Shift+Tab");
  assert.equal(await last.evaluate(el=>el===document.activeElement),true);
  await picker.getByRole("searchbox").fill("No matching provider");
  assert.equal(await picker.locator(".cw-auth-picker-row").count(),0);
  await dialog.getByRole("button",{name:"Close",exact:true}).click();
  assert.equal(await dialog.isVisible(),false);
});
