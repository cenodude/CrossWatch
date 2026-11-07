/* tests/profile-mobile.browser.mjs */
/* CrossWatch - Profile artwork and touch workflows at phone widths */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude */
import test from "node:test";
import assert from "node:assert/strict";
import {execFileSync} from "node:child_process";
import {readFile, mkdir} from "node:fs/promises";
import {chromium} from "playwright";

const template = execFileSync("python", ["-c", "import ui_frontend; print(ui_frontend.get_profile_html({'is_admin':True,'username':'mobile','display_name':'Mobile preview'}))"], {encoding:"utf8",maxBuffer:1024*1024}).replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, "");
const items = ["The Grand Budapest Hotel", "A long episode title with plenty of words", "Arrival"].map((title,index)=>({key:`movie:tmdb:${index+1}`,type:"movie",title,year:2014,ids:{tmdb:String(index+1)},tmdb:String(index+1),sort_epoch:1793989800,watched_at:"2026-11-06T18:30:00Z",last_collected_at:1793989800,rating:8.5,watch_count:3,watch_epochs:[1793989800],providers:["PLEX","TRAKT"],sources_by_provider:{PLEX:["default"],TRAKT:["default"]},progress:45,can_update_progress:true,can_mark_watched:true,can_remove_progress:true,provider:"PLEX",provider_instance:"default",duration:120,remaining_minutes:66,media:{resolution:"4k",hdr:"dolby_vision",audio:"truehd"}}));

async function fixture(t,theme,width) {
  const browser=await chromium.launch({headless:true});
  t.after(()=>browser.close());
  const page=await browser.newPage({viewport:{width,height:844},hasTouch:width<800,isMobile:width<800,reducedMotion:'reduce'});
  page.setDefaultTimeout(6000);
  const errors=[];
  page.on('pageerror',error=>errors.push(error.message));
  t.after(()=>assert.deepEqual(errors,[]));
  const state={empty:false};
  await page.route('https://profile-mobile.test/**',async route=>{
    const url=new URL(route.request().url()),path=url.pathname;
    if(path.startsWith('/assets/')) {
      try{return route.fulfill({body:await readFile(new URL('..'+path,import.meta.url)),contentType:path.endsWith('.css')?'text/css':path.endsWith('.woff2')?'font/woff2':path.endsWith('.svg')?'image/svg+xml':'text/javascript'});}
      catch{return route.fulfill({status:404,body:''});}
    }
    if(path.startsWith('/art/')) return route.fulfill({contentType:'image/svg+xml',body:'<svg xmlns="http://www.w3.org/2000/svg" width="400" height="600"><rect width="400" height="600" fill="#243c57"/><circle cx="200" cy="200" r="100" fill="#447a89"/><path d="M0 600L200 250L400 600" fill="#c89162"/></svg>'});
    if(path.startsWith('/api/')) {
      const list=state.empty?[]:items;
      let json={ok:true};
      if(path==='/api/profile') json={user:{is_admin:true,username:'mobile',display_name:'Mobile preview',permissions:{write:true},preferences:{},sessions:[]}};
      if(['/api/profile/collection','/api/profile/history','/api/profile/ratings','/api/profile/watchlist'].includes(path)) json={items:list,total:list.length,page:1,page_size:24,pages:1,counts:{total:list.length,movies:list.length},providers:['PLEX','TRAKT'],months:[]};
      if(path==='/api/playback_progress/items') json={items:list,total:list.length,providers:[],errors:[]};
      if(path==='/api/dashboard/widgets') json={recent_history:{items:list},recent_scrobble:{items:list},recent_progress:{items:list},latest_ratings:{items:list}};
      if(path==='/api/state/wall') json={items:list};
      if(path==='/api/profile/title') json={item:items[0],title:items[0].title};
      if(path==='/api/profile/preferences') json={preferences:{}};
      return route.fulfill({json});
    }
    const attrs=theme==='original'?'class="cw-theme-original"':`data-cw-theme="${theme}" class="cw-theme-${theme==='flat-light'?'light':'dark'}"`;
    const html=template.replace(/<html\b[^>]*>/,`<html ${attrs} data-cw-role="admin" data-profile-write="on">`).replace(/<link[^>]+id="cw-theme-(flat|original)-css"[^>]*>/g,tag=>tag.includes(theme==='original'?'original-css':'flat-css')?tag:'').replace('</body>',`<script>window.CW={AuthState:{read:()=>({isAdmin:true,permissions:{write:true}})}};</script><script src="/assets/helpers/selection-bar.js"></script><script src="/assets/helpers/profile-datetime.js"></script><script src="/assets/js/profile-page.js"></script></body>`);
    return route.fulfill({contentType:'text/html',body:html.replace('<script src="/assets/js/profile-page.js">','<script src="/assets/helpers/month-bar.js"></script><script src="/assets/helpers/icon-select.js"></script><script src="/assets/helpers/account-menu.js"></script><script src="/assets/js/profile-page.js">')});
  });
  await page.goto('https://profile-mobile.test/profile');
  await page.locator('.cw-profile-mobile-view').waitFor({state:'attached'});
  await page.evaluate(()=>document.fonts.ready);
  return {page,state};
}

async function snapshot(page,name) {
  if(!process.env.CW_PROFILE_SCREENSHOTS) return;
  await mkdir(process.env.CW_PROFILE_SCREENSHOTS,{recursive:true});
  await page.locator('.cw-profile-panel.active').screenshot({path:`${process.env.CW_PROFILE_SCREENSHOTS}/${name}.png`});
}

for(const theme of ['original','flat-dark','flat-light']) {
  for(const width of [320,390,1440]) {
    test(`${theme}: profile covers and controls at ${width}px`,async t=>{
      const {page,state}=await fixture(t,theme,width);
      for(const section of ['overview','collection','history','ratings','watchlist','playback','account']) {
        if(width<800) await page.locator('.cw-profile-mobile-view select').selectOption(section);
        else await page.locator(`[data-profile-tab="${section}"]`).click();
        const panel=page.locator(`#profile-panel-${section}`);
        await panel.waitFor();
        if(['collection','history','ratings','watchlist','playback'].includes(section)) {
          const first=panel.locator('.cw-collection-card,.cw-hist-card').first();
          await first.waitFor();
          await first.scrollIntoViewIfNeeded();
          await page.waitForFunction(()=>{const img=document.querySelector('.cw-profile-panel.active .cw-hist-art img');return !img||img.classList.contains('is-loaded');});
        }
        await snapshot(page,`${theme}-${width}-${section}`);
        assert.equal(await page.evaluate(w=>document.documentElement.scrollWidth>w+1,width),false,section);
        if(width<800&&section==='overview') {
          assert.ok((await panel.locator('.cw-cw-card').first().boundingBox()).width>=180);
          const cover=await panel.locator('.cw-cw-art').first().boundingBox();
          const badge=await panel.locator('.cw-cw-providers .cw-profile-provider-badge').first().boundingBox();
          assert.ok(badge.width<=22&&badge.height<cover.height/4);
          const title=await panel.locator('.cw-profile-watchlist-copy').first().boundingBox();
          const status=await panel.locator('.cw-profile-watchlist-status').first().boundingBox();
          assert.ok(title.y+title.height<=status.y);
        }
        if(width<800&&['collection','history','ratings','watchlist','playback'].includes(section)) {
          const select=panel.locator('.cw-tl-select').first();
          assert.ok((await select.boundingBox()).width>=44);
          assert.equal(await select.evaluate(el=>getComputedStyle(el,'::before').width),'22px');
          await select.tap();
          assert.equal(await select.locator('input').isChecked(),true);
          await snapshot(page,`${theme}-${width}-${section}-selected`);
          await select.tap();
          if(section==='ratings') {
            const box=await select.boundingBox();
            const rating=await panel.locator('.cw-media-card-score').first().boundingBox();
            assert.ok(box.x+box.width<=rating.x);
          }
          if(section==='playback') {
            await panel.locator('[data-playback-action="update_progress"]').first().tap();
            await page.locator('#profile-playback-edit[open]').waitFor();
            const dialog=await page.locator('#profile-playback-edit').boundingBox();
            assert.ok(dialog.x>=0&&dialog.x+dialog.width<=width,JSON.stringify(dialog));
            if(process.env.CW_PROFILE_SCREENSHOTS) await page.screenshot({path:`${process.env.CW_PROFILE_SCREENSHOTS}/${theme}-${width}-dialog.png`});
            await page.locator('#profile-playback-edit [data-playback-dialog-cancel]').tap();
          }
          const list=panel.locator('[data-timeline-view="list"],[data-playback-view="list"],[data-collection-view="list"]');
          if(await list.count()) {
            await list.tap();
            await panel.locator('.cw-hist-row,.cw-collection-row').first().waitFor();
            assert.equal(await page.evaluate(w=>document.documentElement.scrollWidth>w+1,width),false,`${section} list`);
          }
        }
      }
      state.empty=true;
      if(width<800) await page.locator('.cw-profile-mobile-view select').selectOption('watchlist');
      else await page.locator('[data-profile-tab="watchlist"]').click();
      await page.locator('#profile-watchlist-refresh').click();
      await page.locator('#profile-watchlist-list .cw-profile-empty').waitFor();
      await snapshot(page,`${theme}-${width}-empty`);
    });
  }
}
