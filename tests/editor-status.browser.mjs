/* tests/editor-status.browser.mjs */
/* CrossWatch - Optional Editor status column, layout and provider refresh. */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import assert from "node:assert/strict";
import {readFile, readdir, mkdir} from "node:fs/promises";
import test from "node:test";
import {chromium} from "playwright";

async function fixture(t, theme, editable = false) {
  const browser = await chromium.launch({headless:true});
  t.after(()=>browser.close());
  const page = await browser.newPage({viewport:{width:1500,height:1000}});
  page.setDefaultTimeout(7000);
  const errors=[], reads=[];
  page.on("pageerror",error=>errors.push(error.message));
  t.after(()=>assert.deepEqual(errors,[]));
  const data = {empty:false, fail:false, status:"on_hold"};
  const items = {"show:1":{type:"show",title:"A show",ids:{tmdb:"1"}},"episode:2":{type:"episode",title:"An episode",ids:{tmdb:"2"},show_ids:{tmdb:"1"},season:1,episode:2}};
  const modules=(await readdir(new URL("../assets/js/editor/",import.meta.url))).filter(name=>name.endsWith(".js")&&!['episode-groups.js','episode-alignment.js','mapping.js','origin.js','saved-mappings.js'].includes(name));
  await page.addInitScript(()=>localStorage.setItem("cw-editor-ui",JSON.stringify({advanced:true,columnLayoutVersion:4,columnWidths:{title:340},columnVisibility:{year:false,status:false}})));
  await page.route("**/*",async route=>{
    const url=new URL(route.request().url()), path=url.pathname;
    if(path.startsWith("/assets/")) {
      try{return route.fulfill({body:await readFile(new URL(".."+path,import.meta.url)),contentType:path.endsWith(".css")?"text/css":path.endsWith(".woff2")?"font/woff2":"text/javascript"});}
      catch{return route.fulfill({status:404,body:""});}
    }
    if(path==="/api/editor/statuses") {
      reads.push(Object.fromEntries(url.searchParams));
      await new Promise(resolve=>setTimeout(resolve,300));
      return route.fulfill({status:data.fail?502:200,json:data.fail?{detail:"Read failed"}:{supported:true,records:[{media:"shows",ids:{tmdb:"1"},status:data.status}]}});
    }
    if(path.startsWith("/api/")) {
      let json={ok:true};
      if(path==="/api/editor/state/providers") json={providers:["CROSSWATCH"]};
      if(path==="/api/editor") json={provider:"CROSSWATCH",provider_instance:"default",items:data.empty?{}:items,manual_adds:editable&&!data.empty?items:{},manual_blocks:[]};
      if(path.includes("provider-instances")) json={instances:[{id:"default",label:"Personal"}]};
      if(path==="/api/editor/send/options") json={providers:[]};
      return route.fulfill({json});
    }
    return route.fulfill({contentType:"text/html",body:`<!doctype html><html data-cw-theme="${theme}"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">${["themes/tokens","css/base","crosswatch","css/layout","css/components","css/pages","css/page-theme","fonts/material-symbols-rounded","ui-shell","themes/flat","themes/original-coverage"].map(file=>`<link rel="stylesheet" href="/assets/${file}.css">`).join("")}</head><body><div id="layout"><section id="page-editor" class="cw-themed-page"></section></div><script src="/assets/helpers/selection-bar.js"></script>${[...modules.map(name=>"editor/"+name),"editor.js"].map(file=>`<script src="/assets/js/${file}"></script>`).join("")}</body></html>`});
  });
  await page.goto("https://editor-status.test/");
  await page.locator("#cw-tbody tr").first().waitFor();
  return {page,reads,data};
}

async function toggle(page) {
  await page.locator("#cw-columns-btn").click();
  await page.locator('.cw-column-toggle[data-column="status"]').click();
  await page.keyboard.press("Escape");
}

for(const theme of ["original","flat-dark","flat-light"]) test(`${theme}: status is optional, compact, read-only and responsive`,async t=>{
  const {page,reads,data}=await fixture(t,theme);
  const rows=page.locator("#cw-tbody tr"), status=page.locator("#cw-tbody .cw-col-status");
  assert.equal(await status.first().isVisible(),false);
  assert.equal(reads.length,0);
  const before=await rows.first().boundingBox();
  await toggle(page);
  const rowCheckbox=page.locator("#cw-tbody .cw-checkbox").first();
  await rowCheckbox.focus();
  await page.waitForFunction(()=>document.querySelector("#cw-tbody .cw-col-status")?.textContent==="On hold");
  assert(await rowCheckbox.evaluate(node=>node===document.activeElement));
  assert.equal(reads.length,1);
  assert.deepEqual(reads[0],{provider:"CROSSWATCH",provider_instance:"default"});
  assert.deepEqual(await status.allTextContents(),["On hold"]);
  assert.equal(await status.locator("input,select,button").count(),0);
  assert.equal(await page.locator("#cw-change-bar").isVisible(),false);
  assert(Math.abs((await rows.first().boundingBox()).height-before.height)<1);
  await toggle(page);
  const after=await rows.first().boundingBox();
  assert(Math.abs(after.width-before.width)<1);
  const saved=await page.evaluate(()=>JSON.parse(localStorage.getItem("cw-editor-ui")));
  assert.equal(saved.columnWidths.title,340);
  await toggle(page);
  await page.waitForFunction(()=>document.querySelector("#cw-tbody .cw-col-status")?.textContent==="On hold");
  await mkdir(".pytest_tmp_editor_status_visual",{recursive:true});
  await page.locator("#cw-table-wrap").screenshot({path:`.pytest_tmp_editor_status_visual/${theme}-desktop.png`});
  await page.setViewportSize({width:390,height:900});
  await rows.first().locator(".cw-mobile-fields-toggle").click();
  assert.equal(await status.first().isVisible(),true);
  assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1));
  await rows.first().screenshot({path:`.pytest_tmp_editor_status_visual/${theme}-mobile.png`});
  await page.setViewportSize({width:1500,height:1000});
  data.fail=true;
  await page.locator("#cw-reload").click();
  await page.waitForFunction(()=>document.querySelector("#cw-tbody .cw-col-status")?.textContent==="Unavailable");
  data.fail=false; data.status="dropped";
  await page.locator("#cw-reload").click();
  await page.waitForFunction(()=>document.querySelector("#cw-tbody .cw-col-status")?.textContent==="Dropped");
  data.empty=true;
  await page.locator("#cw-reload").click();
  await page.locator("#cw-empty").waitFor();
  assert.equal(await rows.count(),0);
});

for(const theme of ["original","flat-dark","flat-light"]) test(`${theme}: keyboard toggles preserve the popup and ID edits update only the status cell`,async t=>{
  const {page,reads}=await fixture(t,theme,true);
  await page.locator("#cw-columns-btn").click();
  const toggle=page.locator('.cw-column-toggle[data-column="status"]');
  await toggle.focus();
  await toggle.press("Enter");
  assert.equal(await toggle.getAttribute("aria-pressed"),"true");
  assert(await toggle.evaluate(node=>node===document.activeElement));
  await page.waitForFunction(()=>document.querySelector("#cw-tbody .cw-col-status")?.textContent==="On hold");
  assert(await toggle.evaluate(node=>node===document.activeElement));
  await toggle.press("Space");
  assert.equal(await toggle.getAttribute("aria-pressed"),"false");
  assert(await toggle.evaluate(node=>node===document.activeElement));
  await toggle.press("Enter");
  assert(await toggle.evaluate(node=>node===document.activeElement));
  await page.keyboard.press("Escape");
  await page.waitForFunction(()=>document.querySelector("#cw-tbody .cw-col-status")?.textContent==="On hold");
  const input=page.locator("#cw-tbody .cw-col-id input").first();
  await input.evaluate(node=>window.statusIdentityInput=node);
  const requestCount=reads.length;
  await input.fill("999");
  assert.equal(await page.locator("#cw-tbody .cw-col-status").first().textContent(),"Unknown");
  assert(await input.evaluate(node=>node===window.statusIdentityInput&&node===document.activeElement));
  await input.press("Tab");
  assert.equal(await page.locator("#cw-tbody .cw-col-status").first().textContent(),"Unknown");
  await input.fill("1");
  assert.equal(await page.locator("#cw-tbody .cw-col-status").first().textContent(),"On hold");
  assert(await input.evaluate(node=>node===window.statusIdentityInput&&node===document.activeElement));
  await input.fill("");
  assert.equal(await page.locator("#cw-tbody .cw-col-status").first().textContent(),"Unknown");
  assert.equal(reads.length,requestCount);
});
