/* tests/tracker-logo.browser.mjs */
/* CrossWatch - Header title logos, text fallback and responsive sizing */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import {readFile, mkdir} from "node:fs/promises";
import {chromium} from "playwright";

const items = [
  {key:"movie:1",title:"Arrival",type:"movie",year:2016,art:{tmdb:1,kind:"movie"}},
  {key:"movie:2",title:"A movie without title artwork",type:"movie",year:2026,art:{tmdb:2,kind:"movie"}},
  {key:"episode:3",title:"The Red Sowing",series_title:"House of the Dragon",type:"episode",season:2,episode:7,art:{tmdb:3,kind:"tv"}},
].map(item=>({...item,date:"2026-11-08T12:00:00Z"}));
const logo = '<svg xmlns="http://www.w3.org/2000/svg" width="1000" height="150"><text x="5" y="110" fill="white" font-family="serif" font-size="100">ARRIVAL</text></svg>';

async function fixture(t,theme,width) {
  const browser=await chromium.launch({headless:true});
  t.after(()=>browser.close());
  const page=await browser.newPage({viewport:{width,height:900},isMobile:width<800,hasTouch:width<800,reducedMotion:'reduce'});
  page.setDefaultTimeout(6000);
  const errors=[];
  page.on('pageerror',error=>errors.push(error.message));
  t.after(()=>assert.deepEqual(errors,[]));
  let release;
  const heldLogo=new Promise(resolve=>{release=resolve;});
  let requested;
  const logoRequested=new Promise(resolve=>{requested=resolve;});
  await page.route('https://tracker-logo.test/**',async route=>{
    const url=new URL(route.request().url()),path=url.pathname;
    if(path.startsWith('/assets/')) {
      try{return route.fulfill({body:await readFile(new URL('..'+path,import.meta.url)),contentType:path.endsWith('.css')?'text/css':path.endsWith('.woff2')?'font/woff2':path.endsWith('.svg')?'image/svg+xml':'text/javascript'});}
      catch{return route.fulfill({status:404,body:''});}
    }
    if(path.startsWith('/art/')) {
      if(url.searchParams.get('kind')==='logo') {
        if(path.endsWith('/2')) return route.fulfill({status:404,body:'Logo not available'});
        if(path.endsWith('/3')) {requested();await heldLogo;}
        return route.fulfill({contentType:'image/svg+xml',body:path.endsWith('/3')?logo.replace('1000','260').replace('150','400').replace('100"','30"').replace('ARRIVAL','DRAGON'):logo});
      }
      return route.fulfill({contentType:'image/svg+xml',body:'<svg xmlns="http://www.w3.org/2000/svg" width="1280" height="720"><rect width="1280" height="720" fill="#33445c"/></svg>'});
    }
    if(path.startsWith('/api/')) {
      let json={ok:true};
      if(path==='/api/tracker/summary') json={provider_instance:'default',instances:[{id:'default',label:'Default'}],counts:{history:3},health:{},notes:{total:0,statuses:{},tags:[]}};
      if(path==='/api/tracker/stats') json={week:3,month:3,year:3,total:3,timed:3,minutes:300,streak:1};
      if(path==='/api/tracker/items') json={items,total:items.length};
      if(path==='/api/tracker/title') {
        const item=items.find(item=>item.key===url.searchParams.get('key'))||items[0];
        json={title:{...item,title:item.series_title||item.title},show:item.type==='episode',plays:[item],ratings:[],watchlist:[],collection:[],progress:[],attention:[],notes:{},seasons:[]};
      }
      return route.fulfill({json});
    }
    const attrs=theme==='original'?'class="cw-theme-original"':`data-cw-theme="${theme}" class="cw-theme-${theme==='flat-light'?'light':'dark'}"`;
    const css=['fonts/material-symbols-rounded','themes/tokens','css/base','crosswatch','css/layout','css/components','css/pages','css/page-theme','ui-shell',theme==='original'?'themes/original-coverage':'themes/flat'];
    return route.fulfill({contentType:'text/html',body:`<!doctype html><html ${attrs}><head><meta name="viewport" content="width=device-width,initial-scale=1">${css.map(file=>`<link rel="stylesheet" href="/assets/${file}.css">`).join('')}</head><body><main id="layout"><section id="page-tracker" class="cw-themed-page"></section></main><script>window.CW={AuthState:{read:()=>({isAdmin:true})}};</script><script src="/assets/helpers/selection-bar.js"></script><script src="/assets/js/tracker.js"></script><script>TrackerPage.mount(document.querySelector('#page-tracker'));</script></body></html>`});
  });
  await page.goto('https://tracker-logo.test/');
  await page.locator('.trk-play').first().waitFor();
  const select=async index=>{
    await page.locator('.trk-play').nth(index).click();
    if(width<800) {
      await page.locator('.trk-sheet').waitFor();
      await page.locator('.trk-sheet [data-trk-sheet="close"]').first().click();
    }
  };
  return {page,select,release,logoRequested};
}

for(const theme of ['original','flat-dark','flat-light']) for(const width of [320,390,1440]) {
  test(`${theme}: header logos and fallback at ${width}px`,async t=>{
    const {page,select,release,logoRequested}=await fixture(t,theme,width);
    const heading=page.locator('.trk-hero-heading');
    await select(0);
    await heading.locator('img').waitFor();
    assert.equal(await heading.locator('strong').isVisible(),false);
    assert.equal(await heading.locator('img').getAttribute('alt'),'Arrival');
    assert.equal(await heading.locator('img').evaluate(image=>getComputedStyle(image).backgroundColor),'rgba(0, 0, 0, 0)');
    const bounds=await heading.locator('img').boundingBox();
    assert.ok(bounds.width<=280&&bounds.height<=44&&bounds.x>=0&&bounds.x+bounds.width<=width);
    assert.match(await page.locator('.trk-play').first().textContent(),/Arrival/);
    if(process.env.CW_LOGO_SCREENSHOTS) {
      await mkdir(process.env.CW_LOGO_SCREENSHOTS,{recursive:true});
      await page.locator('.cw-page-hero').screenshot({path:`${process.env.CW_LOGO_SCREENSHOTS}/${theme}-${width}.png`});
    }
    await select(1);
    await heading.locator('strong').waitFor();
    assert.equal(await heading.locator('strong').textContent(),items[1].title);
    await page.waitForFunction(()=>{const image=document.querySelector('.trk-hero-logo');return image?.complete&&!image.naturalWidth;});
    assert.equal(await heading.locator('img').isVisible(),false);
    await select(2);
    await logoRequested;
    assert.equal(await heading.locator('strong').textContent(),'House of the Dragon');
    assert.match(await page.locator('.trk-hero-meta').textContent(),/S02.*E07/);
    await select(0);
    release();
    await heading.locator('img').waitFor();
    assert.equal(await heading.locator('img').getAttribute('alt'),'Arrival');
    await select(2);
    await heading.locator('img').waitFor();
    assert.equal(await heading.locator('img').getAttribute('alt'),'House of the Dragon');
    assert.match(await page.locator('.trk-hero-selection small').textContent(),/The Red Sowing/);
    assert.equal(await page.evaluate(w=>document.documentElement.scrollWidth>w+1,width),false);
  });
}
