/* tests/shelf-display.browser.mjs */
/* CrossWatch - Shared shelf artwork, glass rows and collection media card browser coverage */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import './i18n-browser-setup.mjs';
import test from "node:test";
import assert from "node:assert/strict";
import {readFile, mkdir} from "node:fs/promises";
import {chromium, firefox} from "playwright";
import {englishFrontendSource} from './i18n-test-helper.mjs';

const frontend = englishFrontendSource();
const formats = ["bluray","4k_bluray","4k_bluray","dvd","digital","hd_dvd","vcd","vhs","betamax","laserdisc"];
const items = formats.map((format,index) => ({key:`movie:${index + 1}`,type:"movie",title:["Jerry & Marge Go Large","Toy Story","Toy Story 2","DVD edition","Digital edition"][index] || `${format.toUpperCase()} edition`,year:2000 + index,tmdb:String(index + 1),ids:{tmdb:String(index + 1)},art:{kind:"movie",tmdb:String(index + 1)},copies:[{format,packaging:index === 2 ? "steelbook" : "keepcase",shelf:"Movie night"}],shelf:{formats:[format],packaging:index === 2 ? "steelbook" : "keepcase",status:index === 3 ? "wishlist" : index === 1 ? "ordered" : "owned",copies:1,shelves:["Movie night"]}}));
const poster = '<svg xmlns="http://www.w3.org/2000/svg" width="400" height="600"><defs><linearGradient id="g" x2="0" y2="1"><stop stop-color="#1c6792"/><stop offset="1" stop-color="#da973d"/></linearGradient></defs><rect width="400" height="600" fill="url(#g)"/><circle cx="200" cy="280" r="130" fill="#e9ae68"/><path d="M0 600L200 300L400 600" fill="#273654"/><text x="200" y="90" fill="white" font-size="38" text-anchor="middle" font-family="sans-serif">CROSSWATCH</text></svg>';
const disc = '<svg xmlns="http://www.w3.org/2000/svg" width="400" height="400"><defs><mask id="m"><circle cx="200" cy="200" r="196" fill="white"/><circle cx="200" cy="200" r="25" fill="black"/></mask></defs><g mask="url(#m)"><circle cx="200" cy="200" r="196" fill="#b75851" stroke="#d4d9e3" stroke-width="6"/><path d="M0 0H400V210L0 380" fill="#244f82"/><circle cx="200" cy="200" r="48" fill="#bcc5d2"/><text x="200" y="95" text-anchor="middle" fill="white" font-size="32" font-family="sans-serif">DISC ART</text></g></svg>';

async function fixture(t,view,theme,entries=items) {
  const browser = await (process.env.CW_SHELF_BROWSER === "firefox" ? firefox : chromium).launch({headless:true});
  t.after(() => browser.close());
  const page = await browser.newPage({viewport:{width:1440,height:1050},reducedMotion:"reduce",locale:"en-US"});
  page.setDefaultTimeout(8000);
  const requests = [];
  page.on("request",request => requests.push(request.url()));
  const errors = [];
  page.on("pageerror",error => errors.push(error.message));
  t.after(() => assert.deepEqual(errors,[]));
  const start = frontend.indexOf(view === "hub" ? "<!DOCTYPE html>" : "<!doctype html>");
  const html = await page.evaluate(({raw,theme,view}) => {
    const doc = new DOMParser().parseFromString(raw,"text/html");
    doc.querySelectorAll("script").forEach(node => node.remove());
    doc.documentElement.dataset.cwTheme = theme;
    doc.documentElement.dataset.cwArt = "fanart";
    doc.documentElement.dataset.cwRole = "admin";
    doc.documentElement.dataset.tab = view === "hub" ? "profile" : "tracker";
    doc.documentElement.className = theme === "original" ? "cw-theme-original" : `cw-theme-${theme === "flat-light" ? "light" : "dark"}`;
    doc.querySelector(theme === "original" ? "#cw-theme-flat-css" : "#cw-theme-original-css")?.remove();
    doc.querySelector("#cw-main-loading")?.remove();
    if (view === "tracker") {
      doc.querySelector("#layout").classList.add("full");
      for (const child of doc.querySelector("#layout").children) child.classList.toggle("hidden",child.id !== "page-tracker");
    } else {
      doc.querySelectorAll(".cw-profile-panel").forEach(node => node.classList.toggle("active",node.id === "profile-panel-collection"));
      const shell = doc.createElement("main");
      shell.className = "cw-profile-shell";
      shell.append(doc.querySelector("#profile-panel-collection"));
      doc.body.replaceChildren(shell);
    }
    return "<!doctype html>" + doc.documentElement.outerHTML;
  },{raw:frontend.slice(start,frontend.indexOf("</html>",start)+7).replaceAll("{{","{").replaceAll("}}","}").replace(/\{[a-z_]+\}/g,""),theme,view});
  const controls = {empty:false,failDisc:false};
  await page.route("https://shelf.test/**",async route => {
    const url = new URL(route.request().url()), path = url.pathname;
    if (path.startsWith("/assets/")) {
      try {
        return route.fulfill({body:await readFile(new URL(".."+path,import.meta.url)),contentType:path.endsWith(".css") ? "text/css" : path.endsWith(".svg") ? "image/svg+xml" : path.endsWith(".woff2") ? "font/woff2" : "text/javascript"});
      } catch {return route.fulfill({status:404,body:""});}
    }
    if (path.startsWith("/art/")) {
      if (url.searchParams.get("kind") === "disc") return route.fulfill(controls.failDisc ? {status:404,body:""} : {contentType:"image/svg+xml",body:disc});
      if (process.env.CW_SHELF_ARTWORK && /\/[123]$/.test(path)) {
        const file = ["jerry-and-marge.jpg","toy-story.jpg","toy-story-2.jpg"][Number(path.split("/").pop())-1];
        return route.fulfill({contentType:"image/jpeg",body:await readFile(`${process.env.CW_SHELF_ARTWORK}/${file}`)});
      }
      return route.fulfill({contentType:"image/svg+xml",body:poster});
    }
    if (!path.startsWith("/api/")) return route.fulfill({contentType:"text/html",body:html});
    const list = controls.empty ? [] : entries;
    let json = {ok:true,items:[]};
    if (path === "/api/profile/shelf") json = {items:Object.fromEntries(list.map(item => [`movie:${item.tmdb}`,item.shelf]))};
    if (path === "/api/profile/collection") json = {items:list,total:list.length,page:1,pages:1,page_size:24,counts:{all:list.length,movie:list.length},providers:[],months:[]};
    if (path.startsWith("/api/profile/shelf/movie/")) {
      const item = entries.find(item => item.tmdb === path.split("/").pop()) || entries[0];
      json = {ok:true,can_edit:true,entry:{status:item.shelf.status,copies:[{format:item.shelf.formats[0],packaging:item.shelf.packaging,store:"Collection",shelf:"Movie night"}]}};
    }
    if (path === "/api/profile/title") json = {ok:true,plays:[],collection:{present:[],missing:[]}};
    if (path === "/api/tracker/summary") json = {provider_instance:"default",instances:[{id:"default",label:"Default"}],counts:{collection:list.length},notes:{total:0,statuses:{},tags:[]}};
    if (path === "/api/tracker/items") json = {items:list,total:list.length};
    if (path === "/api/tracker/title") json = {title:entries.find(item => item.key === url.searchParams.get("key")) || entries[0],plays:[],ratings:[],watchlist:[],collection:[],progress:[],attention:[],seasons:[]};
    return route.fulfill({json});
  });
  await page.goto(`https://shelf.test/${view === "hub" ? "profile#collection" : "#tracker"}`);
  await page.evaluate(() => {
    window.CW = {AuthState:{read:()=>({isAdmin:true,user:{id:"admin"},permissions:{write:true}})}};
    localStorage.setItem("cw.profile.collection",JSON.stringify({display:"shelf",shelf:"shelf"}));
    localStorage.setItem("cw.tracker.view.v3",JSON.stringify({view:"collection",viewSelectedAt:Date.now(),layouts:{collection:"shelf"}}));
  });
  for (const file of ["helpers/media-meta","helpers/profile-datetime","helpers/card-actions","helpers/selection-bar","helpers/month-bar","helpers/icon-select","helpers/account-menu"]) await page.addScriptTag({url:`/assets/${file}.js`});
  await page.evaluate(async () => {
    const meta = item => ({title:item.title,year:item.year,overview:"A collected edition, displayed with its cover and disc artwork.",genres:[],detail:{},runtime_minutes:100});
    window.CW.Meta = {peek:meta,get:async item => meta(item)};
    await window.CW.Shelf.load();
  });
  const modalSource = await readFile(new URL("../assets/js/profile-media-modal.js",import.meta.url),"utf8");
  await page.evaluate(modalSource.replace("  (window.CW ||= {}).ProfileMediaModal =","  window.shelfModalState = state;\n  (window.CW ||= {}).ProfileMediaModal ="));
  assert.equal(await page.evaluate(() => typeof window.CW.ProfileMediaModal?.open),"function");
  if (view === "hub") {
    const source = await readFile(new URL("../assets/js/profile-page.js",import.meta.url),"utf8");
    await page.addScriptTag({content:source.replace('  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init, { once: true });\n  else init();','  window.shelfFixture = {collectionState,paintCollectionItems,wireCollection,wirePosterOverlay,loadCollection};')});
    await page.evaluate(items => {
      const api = window.shelfFixture;
      api.collectionState.items = items;
      api.wireCollection();
      api.wirePosterOverlay();
      api.paintCollectionItems();
    },entries);
  } else {
    await page.addScriptTag({url:"/assets/js/tracker.js"});
    await page.evaluate(() => window.TrackerPage.mount(document.querySelector("#page-tracker")));
  }
  await page.locator(".cw-shelfv-item").first().waitFor();
  await page.evaluate(() => document.fonts.ready);
  return {page,controls,requests};
}

async function snapshot(page,name,selector=".cw-shelfv") {
  if (!process.env.CW_SHELF_SCREENSHOTS) return;
  await mkdir(process.env.CW_SHELF_SCREENSHOTS,{recursive:true});
  if (!selector) return page.screenshot({path:`${process.env.CW_SHELF_SCREENSHOTS}/${name}.png`});
  const viewport = page.viewportSize();
  const bounds = await page.locator(selector).boundingBox();
  if (bounds.height > viewport.height - 160) await page.setViewportSize({...viewport,height:Math.ceil(bounds.height + 160)});
  for (let attempt=0;attempt<3;attempt++) {
    await page.waitForTimeout(150);
    try {
      await page.locator(selector).evaluate(async node => {
        const images = [...node.querySelectorAll("img")];
        images.forEach(image => {image.loading = "eager";});
        await Promise.all(images.map(image => image.decode()));
      });
      await page.locator(selector).screenshot({path:`${process.env.CW_SHELF_SCREENSHOTS}/${name}.png`});
      await page.setViewportSize(viewport);
      return;
    } catch (error) {
      if (attempt === 2 || !error.message.includes("not attached")) throw error;
    }
  }
}

async function resize(page,width,height=1050) {
  await page.setViewportSize({width,height});
  await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
  await page.waitForFunction(() => !window.shelfFixture?.collectionState.loading);
}

for (const view of ["hub","tracker"]) for (const theme of ["original","flat-dark","flat-light"]) {
  test(`${view}: shelf and collection artwork in ${theme}`,async t => {
    const {page,controls} = await fixture(t,view,theme);
    assert.equal(await page.locator(".cw-shelfv-item").count(),formats.length);
    const kinds = await page.locator(".cw-shelfv-item").evaluateAll(nodes => nodes.map(node => node.dataset.case));
    assert.deepEqual(kinds,["bluray","uhd","steelbook","dvd","plain","dvd","dvd","dvd","dvd","dvd"]);
    for (const width of [1440,1150,790,390,320]) {
      await resize(page,width);
      await page.waitForFunction(() => document.querySelectorAll(".cw-shelfv-item").length === 10);
      await page.locator(".cw-shelfv").scrollIntoViewIfNeeded();
      const layout = await page.locator(".cw-shelfv-item").evaluateAll(nodes => nodes.map(node => {
        const rect = selector => {const r=node.querySelector(selector).getBoundingClientRect();return {x:r.x,y:r.y,right:r.right,bottom:r.bottom,width:r.width};};
        return {base:rect(".cw-shelfv-base"),caption:rect(".cw-shelfv-cap strong"),badge:rect(".cw-shelfv-cap>span"),note:rect(".cw-shelfv-cap small"),art:rect(".cw-shelfv-art"),case:rect(".cw-case"),filter:getComputedStyle(node.querySelector(".cw-case")).filter};
      }));
      for (let i=0;i<layout.length;i++) {
        assert.equal(layout[i].filter,"none");
        assert.ok(layout[i].caption.y >= layout[i].base.bottom + 8);
        assert.ok(layout[i].case.bottom <= layout[i].base.y + 3);
        if (width >= 1150) {
          assert.ok(layout[i].badge.x >= layout[i].caption.right + 11);
          assert.ok(Math.abs((layout[i].badge.y + layout[i].badge.bottom) / 2 - (layout[i].caption.y + layout[i].caption.bottom) / 2) < 1);
          assert.ok(layout[i].note.y >= Math.max(layout[i].caption.bottom,layout[i].badge.bottom) + 3);
        }
        if (i && Math.abs(layout[i].base.y-layout[i-1].base.y)<1) assert.ok(Math.abs(layout[i].base.x-layout[i-1].base.right)<1);
      }
      if (width <= 600) assert.equal(layout.filter(item => Math.abs(item.base.y-layout[0].base.y)<1).length,3);
      const rack = await page.locator(".cw-shelfv-rack").first().boundingBox();
      assert.ok(layout.at(-1).base.right >= rack.x + rack.width - 1);
      assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth),true);
      await snapshot(page,`${view}-${theme}-${width}`);
    }
    await resize(page,1440);
    const button = page.locator(".cw-shelfv-art").first();
    await button.focus();
    await page.locator(".cw-shelfv-pop.is-open").waitFor();
    const popup = await page.locator(".cw-shelfv-pop").boundingBox();
    const coverBounds = await button.boundingBox();
    assert.ok(popup.width <= 220);
    assert.ok(popup.y + popup.height <= coverBounds.y || popup.y >= coverBounds.y + coverBounds.height);
    assert.ok(Math.abs(popup.x + popup.width / 2 - coverBounds.x - coverBounds.width / 2) <= 1);
    await snapshot(page,`${view}-${theme}-info`,null);
    assert.equal(await button.evaluate(node => getComputedStyle(node).outlineStyle),"solid");
    assert.equal(await button.locator(".cw-shelf-disc img").first().evaluate(node => getComputedStyle(node).animationName),"none");
    const caseArt = await button.locator(".cw-case-art").first().getAttribute("src");
    const discArt = await button.locator(".cw-shelf-disc img").first().getAttribute("src");
    await button.press("Enter");
    if (view === "tracker") await page.locator('[data-trk-detail="card"]').first().click();
    try {await page.locator(".cw-mm-coll-art").waitFor();}
    catch (error) {
      t.diagnostic(JSON.stringify(await page.evaluate(() => ({url:location.href,open:window.CW?.ProfileMediaModal?.isOpen(),item:window.shelfModalState?.item,collection:window.shelfModalState?.collection,loading:window.shelfModalState?.loading,presence:window.shelfModalState?.presence,html:document.querySelector(".cw-mm-coll")?.outerHTML}))));
      throw error;
    }
    const modal = page.locator(".cw-mm-coll-art");
    assert.equal(new URL(await modal.locator(".cw-case-art").getAttribute("src"),page.url()).pathname,new URL(caseArt,page.url()).pathname);
    assert.equal(await modal.locator(".cw-shelf-disc img").getAttribute("src"),discArt);
    await snapshot(page,`${view}-${theme}-card`,".cw-mm-coll");
    await resize(page,320,844);
    const cardBounds = await modal.boundingBox();
    assert.ok(cardBounds.x >= 0 && cardBounds.x + cardBounds.width <= 320);
    await snapshot(page,`${view}-${theme}-card-phone`,".cw-mm-coll");
    await page.evaluate(() => window.CW.Shelf.setCases(false));
    await page.locator('.cw-mm-coll-art[data-case="plain"]').waitFor();
    await page.locator(".cw-mm-close").click();
    assert.equal(await page.locator('.cw-shelfv-item[data-case="plain"]').count(),formats.length);
    await resize(page,390,844);
    await snapshot(page,`${view}-${theme}-posters`);
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth),true);
    await page.locator("[data-shelf-cases]").click();
    assert.equal(await page.locator('.cw-shelfv-item[data-case="steelbook"]').count(),1);
    if (view === "hub") {
      assert.equal(await page.locator('.cw-shelfv-status[data-status="wishlist"]').textContent(),"Wishlist");
      assert.equal(await page.locator('.cw-shelfv-status[data-status="ordered"]').textContent(),"Ordered");
      assert.equal(await page.locator('.is-wishlist .cw-shelfv-art>.cw-shelf-scene').evaluate(node => getComputedStyle(node).opacity),"0.55");
    }
    controls.empty = true;
    await page.evaluate(async view => {
      if (view === "hub") await window.shelfFixture.loadCollection({reset:true});
      else await window.TrackerPage.mount(document.querySelector("#page-tracker"));
    },view);
    assert.equal(await page.locator(".cw-shelfv-item").count(),0);
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth),true);
  });
}

test("disc failure, artwork changes and reduced motion retain a usable shelf",async t => {
  const {page,controls} = await fixture(t,"hub","original");
  const art = page.locator(".cw-shelfv-art").first();
  const tilt = () => art.locator(".cw-shelf-disc").first().evaluate(node => new DOMMatrixReadOnly(getComputedStyle(node).transform).m13);
  const restingTilt = await tilt();
  assert.ok(Math.abs(restingTilt) > .55);
  await page.emulateMedia({reducedMotion:"no-preference"});
  await art.hover();
  await page.waitForFunction(() => Math.abs(new DOMMatrixReadOnly(getComputedStyle(document.querySelector(".cw-shelf-disc")).transform).m13) < .001);
  assert.ok(Math.abs(await tilt()) < .001);
  assert.ok(Math.abs(await art.locator(".cw-case").first().evaluate(node => new DOMMatrixReadOnly(getComputedStyle(node).transform).m13)) < .001);
  assert.equal(await art.locator(".cw-shelf-disc img").first().evaluate(node => getComputedStyle(node).animationPlayState),"running");
  await snapshot(page,"hub-original-hover",null);
  await page.emulateMedia({reducedMotion:"reduce"});
  assert.equal(await tilt(),restingTilt);
  assert.equal(await art.locator(".cw-shelf-disc img").first().evaluate(node => getComputedStyle(node).animationName),"none");
  controls.failDisc = true;
  await page.evaluate(() => window.CW.ArtChoice.set("movie","1","new-art"));
  await page.waitForFunction(() => !document.querySelector(".cw-shelfv-item").classList.contains("has-disc"));
  assert.equal(await art.locator(".cw-shelf-disc:visible").count(),0);
  assert.equal(await art.locator('.cw-case-art[src*="ov=new-art"]').count(),2);
  await art.press("Enter");
  await page.locator(".cw-mm-coll-art .cw-case").waitFor();
});

for (const [view,count] of [["hub",120],["tracker",200]]) {
  test(`${view}: large shelves defer artwork and avoid idle animation work`,async t => {
    const entries = Array.from({length:count},(_,index) => ({...items[0],key:`movie:${index + 1}`,tmdb:String(index + 1),ids:{tmdb:String(index + 1)},art:{kind:"movie",tmdb:String(index + 1)}}));
    const {page,requests} = await fixture(t,view,"original",entries);
    await page.emulateMedia({reducedMotion:"no-preference"});
    await page.waitForTimeout(500);
    const idle = await page.locator(".cw-shelfv").evaluate(node => ({
      items:node.querySelectorAll(".cw-shelfv-item").length,
      animations:node.getAnimations({subtree:true}).length,
      loadedPosters:[...node.querySelectorAll(".cw-shelfv-art>.cw-shelf-scene .cw-case-art")].filter(image => image.complete && image.naturalWidth).length,
      reflectionHeight:node.querySelector(".cw-shelf-reflection").getBoundingClientRect().height,
    }));
    const artRequests = requests.filter(url => url.includes("/art/"));
    t.diagnostic(JSON.stringify({view,...idle,artRequests:artRequests.length,uniqueArtRequests:new Set(artRequests).size}));
    assert.equal(idle.items,count);
    assert.equal(idle.animations,0);
    assert.ok(idle.loadedPosters > 0 && idle.loadedPosters < count);
    assert.ok(idle.reflectionHeight <= 36);
    assert.equal(artRequests.length,new Set(artRequests).size);
    const shelf = page.locator(".cw-shelfv");
    const spins = () => shelf.evaluate(node => node.getAnimations({subtree:true}).filter(animation => animation.animationName === "cwDiscSpin").length);
    await page.locator(".cw-shelfv-art").first().hover();
    assert.equal(await spins(),2);
    await page.mouse.move(0,0);
    assert.equal(await spins(),0);
    const lastPoster = page.locator(".cw-shelfv-art>.cw-shelf-scene .cw-case-art").last();
    await lastPoster.scrollIntoViewIfNeeded();
    await page.waitForFunction(() => {
      const image = [...document.querySelectorAll(".cw-shelfv-art>.cw-shelf-scene .cw-case-art")].at(-1);
      return image.complete && image.naturalWidth > 0;
    });
    assert.ok(requests.some(url => url.includes(`/movie/${count}?`) && !url.includes("kind=disc")));
  });
}
