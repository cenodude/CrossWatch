/* tests/status-mapping.browser.mjs */
/* CrossWatch - Pair status mapping drafts, themes and mobile navigation. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import assert from "node:assert/strict";
import {readFile, mkdir} from "node:fs/promises";
import test from "node:test";
import {chromium} from "playwright";

const states = ["watching", "on_hold", "dropped", "completed"];
const caps = {labels:{watching:"Watching",on_hold:"On hold",dropped:"Dropped",completed:"Completed"},media:Object.fromEntries(["shows","movies"].map(key=>[key,{readable:states,writable:states.slice(0,3),defaults:{on_hold:{include:true},dropped:{include:true}},effects:{completed:"Completed expands progress and is unavailable."}}]))};
const initialPair = {id:"pair-1",source:"ANILIST",target:"MYANIMELIST",source_instance:"default",target_instance:"default",mode:"two-way",enabled:true,
  features:{watchlist:{enable:true,add:true,remove:false},history:{enable:false,add:false,remove:false,use_source_status:false}}};

async function setSwitch(input, checked) {
  if (await input.isChecked() === checked) return;
  await input.focus();
  await input.press("Space");
}

async function fixture(t, theme="original", width=1280, pair=initialPair) {
  const browser = await chromium.launch({headless:true});
  t.after(()=>browser.close());
  const page = await browser.newPage({viewport:{width,height:1000}});
  page.setDefaultTimeout(7000);
  const errors=[], saves=[];
  page.on("pageerror",err=>errors.push(err.message));
  page.on("dialog",dialog=>dialog.accept());
  t.after(()=>assert.deepEqual(errors,[]));
  await page.addInitScript(value => {window.testPair = value;}, pair);
  await page.route("**/*",async route=>{
    const path = new URL(route.request().url()).pathname;
    if(path.startsWith("/assets/")){
      try{return route.fulfill({body:await readFile(new URL(".."+path,import.meta.url)),contentType:path.endsWith(".css")?"text/css":path.endsWith(".svg")?"image/svg+xml":path.endsWith(".woff2")?"font/woff2":"text/javascript"});}
      catch{return route.fulfill({status:404,body:""});}
    }
    if(path==="/api/sync/providers") return route.fulfill({json:["ANILIST","MYANIMELIST"].map(name=>({name,label:name==="ANILIST"?"AniList":"MyAnimeList",features:{history:true,watchlist:true,ratings:true},capabilities:{bidirectional:true,status_mapping:caps},configured:true}))});
    if(path==="/api/provider-instances") return route.fulfill({json:{ANILIST:[{id:"default",label:"Alice",configured:true}],MYANIMELIST:[{id:"default",label:"Bob",configured:true}]}});
    if(path==="/api/user-profiles") return route.fulfill({json:[]});
    if(path==="/api/config") return route.fulfill({json:{anime_mapping:{enabled:true},sync:{},pairs:[pair]}});
    if(path.startsWith("/api/pairs")&&route.request().method()==="PUT") {saves.push(route.request().postDataJSON());return route.fulfill({json:{ok:true}});}
    if(path==="/api/pairs") return route.fulfill({json:[pair]});
    if(path.startsWith("/api/")) return route.fulfill({json:{}});
    return route.fulfill({contentType:"text/html",body:`<!doctype html><html data-cw-theme="${theme}"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">${["themes/tokens","css/base","crosswatch","css/layout","css/components","css/pages","css/page-theme","js/modals/core/styles","fonts/material-symbols-rounded","ui-shell","themes/flat","themes/original-coverage"].map(file=>`<link rel="stylesheet" href="/assets/${file}.css">`).join("")}</head><body><div id="host" class="cx-modal-shell"></div><script>window.CW={};window.cxCloseModal=()=>document.querySelector('#host').remove();</script><script type="module">import modal from '/assets/js/modals/pair-config/index.js';await modal.mount(document.querySelector('#host'),{pairOrId:window.testPair});window.ready=true;</script></body></html>`});
  });
  await page.goto("https://status-mapping.test/");
  await page.waitForFunction(()=>window.ready);
  if(width<700) await page.locator("#cx-mobile-section").selectOption("providers");
  else await page.locator("#cx-tab-providers").click();
  await page.locator("#cx-status-title").waitFor();
  return {page,saves};
}

test("fixture passes script-like pair data without executing it",async t=>{
  const pair = {...initialPair, name:'</script><script>window.fixtureInjected=true</script>'};
  const {page}=await fixture(t,"original",1280,pair);
  assert.equal(await page.evaluate(()=>window.testPair.name),pair.name);
  assert.equal(await page.evaluate(()=>window.fixtureInjected),undefined);
});

test("draft rules keep Watchlist and opt-ins unchanged and save with pair",async t=>{
  const {page,saves}=await fixture(t);
  assert.match(await page.locator(".cw-status-mapping").textContent(),/History is off/);
  await page.locator('[data-status-target="dropped"]').selectOption("on_hold");
  await page.locator('[data-status-include="on_hold"]').uncheck();
  await page.locator("[data-status-direction]").selectOption("b_to_a");
  assert.equal(await page.locator('[data-status-target="dropped"]').inputValue(),"dropped");
  await page.locator("[data-status-media]").selectOption("movies");
  await page.locator('[data-status-include="dropped"]').uncheck();
  await page.locator("[data-status-reset]").click();
  await page.locator("#cx-inline-save").click();
  await page.waitForFunction(()=>!document.querySelector("#host"));
  assert.equal(saves.length,1);
  const saved=saves[0];
  assert.equal(saved.features.history.enable,false);
  assert.equal(saved.features.history.use_source_status,false);
  for(const key of ["enable","add","remove"]) assert.equal(saved.features.watchlist[key],initialPair.features.watchlist[key]);
  assert.deepEqual(saved.status_mapping.directions.a_to_b.shows.dropped,{include:true,target:"on_hold"});
  assert.equal(saved.status_mapping.directions.a_to_b.shows.on_hold.include,false);
  assert.equal(saved.status_mapping.directions.b_to_a.movies.dropped.include,true);
});

test("opening mapping does not opt existing pairs into custom behavior; Cancel discards changes",async t=>{
  const {page,saves}=await fixture(t);
  assert.equal(await page.evaluate(()=>document.querySelector("#cx-modal").__state.statusMapping),null);
  await page.locator('[data-status-include="dropped"]').uncheck();
  assert.equal(await page.locator('[data-status-target="dropped"]').isDisabled(),true);
  await page.getByRole("button",{name:"Cancel",exact:true}).click();
  assert.equal(saves.length,0);
});

test("keyboard focus and existing History opt-in stay synchronized",async t=>{
  const {page,saves}=await fixture(t);
  await page.locator("[data-status-optin]").focus();
  await page.keyboard.press("Space");
  assert.equal(await page.locator("[data-status-optin]").isChecked(),true);
  assert.equal(await page.locator("[data-status-optin]").evaluate(el=>el===document.activeElement),true);
  await page.locator("#cx-tab-history").click();
  assert.equal(await page.locator("#cx-hs-source-status").isChecked(),true);
  await setSwitch(page.locator("#cx-hs-enable"),true);
  await setSwitch(page.locator("#cx-hs-source-status"),false);
  await page.locator("#cx-tab-providers").click();
  assert.equal(await page.getByRole("checkbox",{name:"Enable status syncing"}).isChecked(),false);
  await setSwitch(page.getByRole("checkbox",{name:"Enable status syncing"}),true);
  await page.locator("#cx-tab-history").click();
  assert.equal(await page.locator("#cx-hs-source-status").isChecked(),true);
  await page.locator("#cx-tab-providers").click();
  await setSwitch(page.getByRole("checkbox",{name:"Enable status syncing"}),false);
  await page.locator("#cx-inline-save").click();
  await page.waitForFunction(()=>!document.querySelector("#host"));
  assert.equal(saves[0].features.history.enable,true);
  assert.equal(saves[0].features.history.use_source_status,false);
  assert.equal(saves[0].status_mapping,null);
});

test("mapping switch saves the shared opt-in without enabling History or discarding rules",async t=>{
  const {page,saves}=await fixture(t);
  await page.locator('[data-status-target="dropped"]').selectOption("on_hold");
  await setSwitch(page.getByRole("checkbox",{name:"Enable status syncing"}),true);
  await setSwitch(page.getByRole("checkbox",{name:"Enable status syncing"}),false);
  await setSwitch(page.getByRole("checkbox",{name:"Enable status syncing"}),true);
  await page.locator("#cx-inline-save").click();
  await page.waitForFunction(()=>!document.querySelector("#host"));
  assert.equal(saves[0].features.history.enable,false);
  assert.equal(saves[0].features.history.use_source_status,true);
  assert.deepEqual(saves[0].status_mapping.directions.a_to_b.shows.dropped,{include:true,target:"on_hold"});
  const reopened=await fixture(t,"flat-light",390,saves[0]);
  assert.equal(await reopened.page.getByRole("checkbox",{name:"Enable status syncing"}).isChecked(),true);
  assert.equal(await reopened.page.locator('[data-status-target="dropped"]').inputValue(),"on_hold");
});

for(const theme of ["original","flat-dark","flat-light"]) for(const width of [1280,390]) {
  test(`${theme} at ${width}px keeps controls visible without horizontal overflow`,async t=>{
    const {page}=await fixture(t,theme,width);
    const section=page.locator(".cw-status-mapping");
    await mkdir(new URL("../.pytest_tmp_status_visual/",import.meta.url),{recursive:true});
    await page.screenshot({path:new URL(`../.pytest_tmp_status_visual/${theme}-${width}-modal.png`,import.meta.url).pathname.replace(/^\/(\w:)/,"$1"),fullPage:true});
    assert.equal(await section.evaluate(el=>el.scrollWidth<=el.clientWidth+1),true);
    assert.equal(await page.locator("[data-status-conflict] option").count(),3);
    assert.match(await page.locator("[data-status-direction]").textContent(),/Alice.*Bob/);
    assert.equal(await section.locator('option[value="planned"]').count(),0);
    await page.locator('[data-status-include="watching"]').check();
    await page.locator("[data-status-conflict]").focus();
    assert.notEqual(await page.locator("[data-status-conflict]").evaluate(el=>getComputedStyle(el).outlineStyle),"none");
    await section.screenshot({path:new URL(`../.pytest_tmp_status_visual/${theme}-${width}.png`,import.meta.url).pathname.replace(/^\/(\w:)/,"$1")});
  });
}
