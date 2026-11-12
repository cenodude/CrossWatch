/* tests/tracker-month.browser.mjs */
/* CrossWatch - Monthly review interactions, responsive artwork and themes */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import {readFile, mkdir} from "node:fs/promises";
import {chromium, webkit} from "playwright";

const names = ["The Midnight Garden","North of Nowhere","The Last Orbit","A Quiet Revolution","After the Rain","Paper Moons","The Long Way Home","Parallel Lives"];
const titles = names.map((title,i)=>({key:`title:${i}`,title,type:i<5?"show":"movie",year:2026,ids:{tmdb:String(i+1)},ref:{feature:"history",key:String(i)},plays:12-i,episodes:12-i,minutes:(12-i)*42,timed:12-i,days:[2,8,12,17,23],genres:[i%2?"Mystery":"Drama","Science Fiction"],poster:`https://image.tmdb.org/t/p/w342/poster-${i}.jpg`,backdrop:`https://image.tmdb.org/t/p/w1280/backdrop-${i}.jpg`}));
const days = Array.from({length:31},(_,i)=>({day:i+1,movies:i%4===0?1:0,episodes:i%3===0?i%5+1:0,movie_minutes:i%4===0?100:0,show_minutes:i%3===0?(i%5+1)*42:0,minutes:(i%4===0?100:0)+(i%3===0?(i%5+1)*42:0),titles:i%4===0||i%3===0?titles.slice(0,6).map(t=>t.key):[]}));
const report = {month:"2026-10",timezone:"Europe/Amsterdam",current:false,fingerprint:"first",plays:39,timed:37,minutes:2185,movies:3,shows:5,active_days:17,streak:4,elapsed_days:31,busiest:13,first:{title_key:"title:0",date:"2026-10-01T22:10:00+02:00",season:1,episode:1},last:{title_key:"title:5",date:"2026-10-30T21:00:00+01:00"},titles,days,genres:[{name:"Drama",count:4,titles:titles.filter((_,i)=>i%2===0).map(t=>t.key)},{name:"Mystery",count:4,titles:titles.filter((_,i)=>i%2===1).map(t=>t.key)},{name:"Science Fiction",count:8,titles:titles.map(t=>t.key)}]};

async function fixture(t,theme,width=1440,height=1000,{motion=false}={}) {
  const browser = await (process.env.CW_AUDIT_BROWSER === "webkit" ? webkit : chromium).launch({headless:true});
  t.after(()=>browser.close());
  const page = await browser.newPage({viewport:{width,height},hasTouch:width<1400,reducedMotion:motion?"no-preference":"reduce"});
  page.setDefaultTimeout(8000);
  let requests=0, mode="ready";
  const errors=[];
  page.on("pageerror",e=>errors.push(e.message));
  t.after(()=>assert.deepEqual(errors,[]));
  await page.route("https://image.tmdb.org/**",async route=>{
    const index = Number(/-(\d+)/.exec(route.request().url())?.[1]||0), poster = route.request().url().includes("poster"), h = poster?900:720, w = poster?600:1280;
    const hue = index*43+180;
    const words = names[index].split(" ");
    const title = words.map((word,i)=>`<tspan x="${w/2}" dy="${i?58:0}">${word.toUpperCase()}</tspan>`).join("");
    return route.fulfill({contentType:"image/svg+xml",body:`<svg xmlns="http://www.w3.org/2000/svg" width="${w}" height="${h}" viewBox="0 0 ${w} ${h}"><defs><linearGradient id="g" x2=".6" y2="1"><stop stop-color="hsl(${hue} 38% 35%)"/><stop offset="1" stop-color="hsl(${hue+70} 45% 9%)"/></linearGradient><radialGradient id="r"><stop stop-color="hsl(${hue+100} 70% 80%)"/><stop offset="1" stop-color="hsl(${hue+70} 40% 30%)" stop-opacity="0"/></radialGradient></defs><rect width="100%" height="100%" fill="url(#g)"/><circle cx="${w*.65}" cy="${h*.35}" r="${w*.38}" fill="url(#r)"/><path d="M0 ${h*.8} ${w*.25} ${h*.45} ${w*.5} ${h*.65} ${w*.8} ${h*.4} ${w} ${h*.7} V${h}H0Z" fill="#070c18" opacity=".65"/><circle cx="${w*.3}" cy="${h*.24}" r="${w*.08}" fill="#f5e3c7" opacity=".9"/><text x="${w/2}" y="${h*.59}" text-anchor="middle" font-family="Georgia" font-weight="bold" font-size="49" letter-spacing="4" fill="#f3e8db">${title}</text><text x="${w/2}" y="${h*.91}" text-anchor="middle" font-family="sans-serif" font-size="12" letter-spacing="6" fill="#ded5c5">A CROSSWATCH SAMPLE</text></svg>`});
  });
  await page.route("https://month.test/**",async route=>{
    const url = new URL(route.request().url()),path=url.pathname;
    if(path.startsWith("/assets/")) {
      try{return route.fulfill({body:await readFile(new URL(".."+path,import.meta.url)),contentType:path.endsWith(".css")?"text/css":path.endsWith(".woff2")?"font/woff2":"text/javascript"});}
      catch{return route.fulfill({status:404,body:""});}
    }
    if(path==="/api/tracker/overview/month") {
      requests++;
      if(mode==="denied") return route.fulfill({status:403,json:{detail:"profile_scope_denied"}});
      if(mode==="error") return route.fulfill({status:500,json:{error:"failed"}});
      if(mode==="disabled") return route.fulfill({json:{ok:true,disabled:true,report:null,pending:false,stale:false,error:null}});
      const month=url.searchParams.get("month")||"2026-10";
      const data = mode==="empty"?{...report,month,plays:0,titles:[],genres:[]}: {...report,month};
      return route.fulfill({json:{ok:true,report:mode==="pending"?null:data,month,current_month:"2026-12",generated:1,pending:mode.startsWith("pending"),stale:mode.startsWith("pending")}});
    }
    const css=["fonts/material-symbols-rounded","themes/tokens","css/base","crosswatch","css/layout","css/components","css/pages","css/page-theme","ui-shell",theme==="original"?"themes/original-coverage":"themes/flat"];
    return route.fulfill({contentType:"text/html",body:`<!doctype html><html ${theme==="original"?'class="cw-theme-original"':`data-cw-theme="${theme}" class="cw-theme-${theme==="flat-light"?"light":"dark"}"`}><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">${css.map(p=>`<link rel="stylesheet" href="/assets/${p}.css">`).join("")}</head><body><main id="layout" class="full"><section id="page-tracker" class="cw-themed-page"><div class="cw-root"><button id="entry">Month in review</button></div></section></main><script type="module">import {openMonthReview} from '/assets/js/tracker-month.js';window.openReview=(options={})=>openMonthReview({root:document.querySelector('#page-tracker'),instance:'default',label:'Pascal',openMedia:row=>window.openedTitle=row,...options});document.querySelector('#entry').onclick=window.openReview;</script></body></html>`});
  });
  await page.goto("https://month.test/");
  await page.locator("#entry").click();
  await page.locator(".mr-poster h1").waitFor();
  await page.evaluate(()=>document.fonts.ready);
  return {page,requests:()=>requests,setMode:value=>{mode=value;}};
}

async function noOverflow(page,label) {
  const result=await page.evaluate(()=>({viewport:innerWidth,width:document.documentElement.scrollWidth,outside:[...document.querySelectorAll("body *")].filter(el=>el.getBoundingClientRect().right>innerWidth+1).slice(0,8).map(el=>[el.className,el.getBoundingClientRect().right]),nested:[...document.querySelectorAll('.mr-page *')].filter(el=>{const s=getComputedStyle(el);return /auto|scroll/.test(s.overflowY)&&el.scrollHeight>el.clientHeight+2;}).map(el=>el.className)}));
  assert.ok(result.width<=result.viewport+1,`${label}: ${JSON.stringify(result)}`);
  assert.deepEqual(result.nested,[],label);
}

for(const theme of ["flat-dark","flat-light","original"]) test(`${theme}: monthly review adapts across phones, tablets and desktops`,async t=>{
  const {page,setMode}=await fixture(t,theme);
  for(const [width,height] of [[1920,1080],[1440,900],[1366,768],[1024,1366],[1366,1024],[1180,820],[820,1180],[390,844],[844,390],[320,740]]) {
    await page.setViewportSize({width,height});
    await page.locator(".mr-activity").scrollIntoViewIfNeeded();
    assert.ok(await page.locator('[data-mr-day="25"] .mr-bar-track i').evaluate(el=>el.getBoundingClientRect().height)>100,"Reduced motion shows the full chart");
    await noOverflow(page,`${theme} ${width}x${height}`);
    await page.locator('[data-mr-day="1"]').click();
    await noOverflow(page,`${theme} ${width} selected day`);
    assert.equal(await page.locator(".mr-day-panel").count(),1);
    await page.locator('[data-mr="clear-day"]').click();
    await page.locator(".mr-options>summary").click();
    await noOverflow(page,`${theme} ${width} options`);
    await page.keyboard.press("Escape");
  }
  await page.setViewportSize({width:1440,height:1000});
  await page.locator(".mr-poster").scrollIntoViewIfNeeded();
  await mkdir(".tmp/month-review",{recursive:true});
  await page.screenshot({path:`.tmp/month-review/${theme}-desktop.png`});
  await page.locator(".mr-activity").scrollIntoViewIfNeeded();
  await page.locator(".mr-activity").screenshot({path:`.tmp/month-review/${theme}-activity.png`});
  await page.locator(".mr-ranked").first().scrollIntoViewIfNeeded();
  await page.locator(".mr-ranked").first().screenshot({path:`.tmp/month-review/${theme}-ranked.png`});
  await page.locator(".mr-poster").scrollIntoViewIfNeeded();
  await page.setViewportSize({width:390,height:844});
  await page.screenshot({path:`.tmp/month-review/${theme}-mobile.png`});
  await page.locator("[data-mr-day-select]").selectOption("1");
  assert.equal(await page.locator(".mr-day-panel").count(),1);
  setMode("empty");
  await page.locator('.mr-nav [data-mr="next"]').click();
  await page.locator(".mr-empty").waitFor();
  await noOverflow(page,theme + " empty month");
});

test("Chart hover, keyboard, touch details, filters and media cards",async t=>{
  const {page,requests}=await fixture(t,"flat-dark",1440,1000,{motion:true});
  const bar=page.locator('[data-mr-day="1"]');
  await bar.hover();
  await page.waitForFunction(()=>getComputedStyle(document.querySelector('[data-mr-day="1"] [role="tooltip"]')).opacity === "1");
  await bar.focus(); await page.keyboard.press("ArrowRight");
  assert.equal(await page.locator(":focus").getAttribute("data-mr-day"),"2");
  await page.keyboard.press("Home"); await page.keyboard.press("Enter");
  assert.equal(await page.locator(".mr-day-titles button").count(),4);
  await page.locator('[data-mr="more-day"]').click();
  assert.equal(await page.locator(".mr-day-titles button").count(),6);
  await page.locator(".mr-day-titles button").first().click();
  assert.equal(await page.evaluate(()=>openedTitle.key),"title:0");
  await page.locator('[data-mr-filter="movie"]').click();
  assert.equal(await page.locator(".mr-ranked").count(),1);
  assert.match(await page.locator(".mr-ranked h2").textContent(),/Movies/);
  await page.locator('[data-mr-filter="all"]').click();
  await page.locator('[data-mr-genre="Mystery"]').click();
  assert.equal(await page.locator(".mr-filter-note").count(),1);
  assert.equal(requests(),1);
  await page.locator('[data-mr="clear-genre"]').click();
  await page.locator(".mr-options>summary").click();
  await page.locator("[data-mr-effects]").uncheck();
  assert.equal(await page.locator(".mr-page").evaluate(el=>el.classList.contains("mr-effects-off")),true);
  await page.keyboard.press("Escape");
  await page.locator('.mr-nav [data-mr="back"]').click();
  assert.equal(await page.locator(".mr-page").count(),0);
  assert.equal(await page.locator("#entry").evaluate(el=>el===document.activeElement),true);
});

test("Month navigation, empty states, retry and pending generation",async t=>{
  const {page,setMode}=await fixture(t,"flat-dark");
  await page.locator('.mr-nav [data-mr="next"]').click();
  await page.locator('.mr-poster h1').filter({hasText:'November'}).waitFor();
  await page.locator('.mr-nav [data-mr="next"]').click();
  await page.locator('.mr-poster h1').filter({hasText:'December'}).waitFor();
  assert.equal(await page.locator('.mr-nav [data-mr="next"]').isDisabled(),true);
  assert.equal(await page.locator('.mr-ornaments').count(),1);
  setMode("empty");
  await page.locator('.mr-nav [data-mr="previous"]').click();
  await page.locator('.mr-empty').waitFor();
  assert.match(await page.locator('.mr-empty').textContent(),/No watches/);
  setMode("error");
  await page.locator('.mr-nav [data-mr="previous"]').click();
  await page.locator('[data-mr="retry"]').waitFor();
  setMode("pending");
  await page.locator('.mr-month-input input').fill("2026-08");
  await page.locator('.mr-month-input input').dispatchEvent("change");
  await page.locator('.mr-loading').waitFor();
  setMode("ready");
  await page.locator('.mr-poster').waitFor();
  await page.locator('.mr-cover').first().click();
  assert.equal(await page.evaluate(()=>openedTitle.key),"title:0");
});

test("Monthly poster uses the active profile and isolates account changes",async t=>{
  const {page,setMode}=await fixture(t,"flat-dark");
  await page.route("https://month.test/api/profile/avatar**",route=>route.fulfill({contentType:"image/svg+xml",body:'<svg xmlns="http://www.w3.org/2000/svg" width="48" height="48"><circle cx="24" cy="24" r="24" fill="purple"/></svg>'}));
  await page.evaluate(()=>{
    window.identity={user:{id:"ann",display_name:"Ann",avatar_url:"/api/profile/avatar?ts=1"},profileId:"ann",role:"user",isAdmin:false,permissions:{write:false}};
    window.CW={AuthState:{read:()=>window.identity}};
    window.openReview({month:"2026-10"});
  });
  await page.locator(".mr-person strong").filter({hasText:"Ann"}).waitFor();
  assert.match(await page.locator(".mr-avatar").getAttribute("src"),/\/api\/profile\/avatar\?ts=1$/);
  assert.equal(await page.locator(".mr-avatar").isVisible(),true);
  await page.setViewportSize({width:390,height:844});
  await noOverflow(page,"Mobile profile poster");
  setMode("pending");
  await page.evaluate(()=>{
    window.identity={...window.identity,user:{id:"bob",display_name:"Bob"},profileId:"bob"};
    window.dispatchEvent(new Event("cw:auth-state-changed"));
  });
  assert.equal(await page.locator(".mr-page").count(),0);
  await page.evaluate(()=>window.openReview({month:"2026-10"}));
  await page.locator(".mr-loading").waitFor();
  assert.equal(await page.locator(".mr-cover").count(),0);
  setMode("ready");
  await page.locator(".mr-person strong").filter({hasText:"Bob"}).waitFor();
  assert.equal(await page.locator(".mr-avatar").count(),0);
  await page.evaluate(()=>{
    window.identity={user:{id:"admin",display_name:"Administrator"},role:"admin",isAdmin:true};
    window.CW.OverviewProfile={id:"ann",profile:{label:"Ann's profile",avatar_url:"/api/profile/avatar/ann?ts=2"}};
    window.openReview({month:"2026-10"});
  });
  await page.locator(".mr-person strong").filter({hasText:"Ann's profile"}).waitFor();
  assert.match(await page.locator(".mr-avatar").getAttribute("src"),/\/avatar\/ann\?ts=2$/);
  setMode("denied");
  await page.evaluate(()=>window.openReview({month:"2026-10"}));
  await page.locator(".mr-loading [data-mr=retry]").waitFor();
  assert.equal(await page.locator(".mr-cover").count(),0);
});

test("Monthly themes switch artwork, persist choices and keep controls accessible",async t=>{
  const {page,requests}=await fixture(t,"flat-dark",1440,1000);
  await page.waitForFunction(()=>document.querySelector('.mr-page')?.dataset.monthTheme === 'halloween');
  await page.locator('.mr-options>summary').click();
  for (const id of ['new-year','valentine','patrick','easter','spring','summer','autumn','winter','thanksgiving','carnival','diwali','lunar-new-year','eid','halloween','christmas']) {
    await page.selectOption('[data-mr-theme]',id);
    assert.equal(await page.locator('.mr-page').getAttribute('data-month-theme'),id);
    assert.equal(await page.locator('.mr-themed-art').count(),1);
    await noOverflow(page,id);
  }
  assert.equal(requests(),1);
  await page.locator('.mr-options>summary').focus();
  await page.keyboard.press('Escape');
  await mkdir('.tmp/month-review',{recursive:true});
  await page.locator('.mr-poster').screenshot({path:'.tmp/month-review/christmas-poster.png'});
  await page.locator('.mr-options>summary').click();
  await page.locator('.mr-holiday-options>summary').click();
  await page.setViewportSize({width:390,height:844});
  await noOverflow(page,'Mobile theme and holiday picker');
  await page.locator('[data-mr-holiday="christmas"]').uncheck();
  await page.selectOption('[data-mr-region]','south');
  await page.selectOption('[data-mr-theme]','auto');
  await page.locator('[data-mr-holiday="halloween"]').uncheck();
  assert.equal(await page.locator('.mr-page').getAttribute('data-month-theme'),'spring');
  await page.selectOption('[data-mr-theme]','halloween');
  await page.locator('.mr-options>summary').focus();
  await page.keyboard.press('Escape');
  await page.locator('.mr-poster').screenshot({path:'.tmp/month-review/halloween-mobile.png'});
  await page.locator('.mr-nav [data-mr="back"]').click();
  await page.evaluate(()=>window.openReview({month:'2026-10'}));
  await page.waitForFunction(()=>document.querySelector('.mr-page')?.dataset.monthTheme === 'halloween');
  await page.locator('.mr-options>summary').click();
  await page.locator('[data-mr-effects]').uncheck();
  assert.equal(await page.locator('.mr-themed-art').isVisible(),false);
});

test("Monthly completion notifications follow abandoned requests and stay profile scoped",async t=>{
  const {page,setMode,requests}=await fixture(t,"flat-dark");
  await page.evaluate(()=>{
    window.notices=[];
    window.CW={Notifications:{setSource:(name,data)=>{window.notices=data.items;},removeSource:()=>{window.notices=[];}}};
  });
  await page.locator('.mr-nav [data-mr="back"]').click();
  await page.evaluate(()=>window.dispatchEvent(new Event('cw:notifications-refresh')));
  assert.equal(requests(),1);
  assert.deepEqual(await page.evaluate(()=>window.notices),[]);
  setMode("pending");
  await page.evaluate(()=>window.openReview({month:"2026-08"}));
  await page.waitForResponse(response=>response.url().includes('month=2026-08'));
  await page.locator('.mr-nav [data-mr="back"]').click();
  setMode("ready");
  await page.evaluate(()=>window.dispatchEvent(new Event('auth-changed')));
  await page.evaluate(()=>window.dispatchEvent(new Event('cw:notifications-refresh')));
  await page.waitForFunction(()=>window.notices.length===1);
  const [notice]=await page.evaluate(()=>window.notices);
  const monthName=await page.evaluate(()=>new Date('2026-08-02T12:00:00').toLocaleDateString(undefined,{month:'long'}));
  assert.equal(notice.title,`Your ${monthName} review is ready`);
  assert.equal(notice.href,'#tracker?instance=default&month=2026-08');
  await page.evaluate(()=>{
    window.addEventListener('auth-changed',()=>{window.notices=[];},{once:true});
    window.dispatchEvent(new Event('auth-changed'));
  });
  assert.deepEqual(await page.evaluate(()=>window.notices),[notice]);
  await page.evaluate(()=>window.openReview({month:"2026-08"}));
  await page.locator('.mr-poster').waitFor();
  assert.deepEqual(await page.evaluate(()=>window.notices),[]);
  setMode("pending");
  await page.evaluate(()=>window.openReview({month:"2026-07"}));
  await page.waitForResponse(response=>response.url().includes('month=2026-07'));
  await page.locator('.mr-nav [data-mr="back"]').click();
  const before=requests();
  await page.evaluate(()=>{
    window.CW.AuthState={read:()=>({user:{id:'other'},profileId:'other'})};
    window.dispatchEvent(new Event('cw:auth-state-changed'));
    window.dispatchEvent(new Event('cw:notifications-refresh'));
  });
  assert.equal(requests(),before);
  assert.deepEqual(await page.evaluate(()=>window.notices),[]);
});

test("Monthly notification polling is bounded and resumes when the bell opens",async t=>{
  const {page,setMode,requests}=await fixture(t,"flat-dark");
  setMode("pending");
  await page.evaluate(()=>{
    window.notices=[];
    window.CW={Notifications:{setSource:(name,data)=>{window.notices=data.items;},removeSource:()=>{window.notices=[];}}};
    const schedule=window.setTimeout;
    window.setTimeout=(fn,delay,...args)=>schedule(fn,delay===10000?1:delay,...args);
    window.openReview({month:'2026-06'});
  });
  await page.waitForResponse(response=>response.url().includes('month=2026-06'));
  await page.locator('.mr-nav [data-mr="back"]').click();
  await page.waitForFunction(()=>!document.querySelector('.mr-page'));
  await page.waitForTimeout(1500);
  assert.equal(requests(),32);
  setMode('ready');
  await page.waitForTimeout(100);
  assert.deepEqual(await page.evaluate(()=>window.notices),[]);
  await page.evaluate(()=>window.dispatchEvent(new Event('cw:notifications-refresh')));
  await page.waitForFunction(()=>window.notices.length===1);
  assert.equal(requests(),33);
});

test("Disabled reports clear cached content and stop polling",async t=>{
  const {page,setMode,requests}=await fixture(t,"flat-dark");
  setMode("disabled");
  await page.evaluate(()=>{const schedule=window.setTimeout; window.setTimeout=(fn,delay,...args)=>schedule(fn,delay>=1500&&delay<=10000?1:delay,...args); window.openReview({month:"2026-10"});});
  await page.getByText("Month in review is off",{exact:true}).waitFor();
  assert.equal(await page.locator('.mr-poster').count(),0);
  const count=requests();
  await page.waitForTimeout(150);
  assert.equal(requests(),count);
  await page.keyboard.press('Escape');
  await page.locator('[data-mr="back"]').click();
  assert.equal(await page.locator('.mr-page').count(),0);
});

test("Saved reviews stop bounded polling with a retry and recover when ready",async t=>{
  const {page,setMode,requests}=await fixture(t,"flat-dark");
  await page.evaluate(()=>{const schedule=window.setTimeout; window.setTimeout=(fn,delay,...args)=>schedule(fn,delay>=1500&&delay<=10000?1:delay,...args);});
  setMode("pending-saved");
  await page.evaluate(()=>window.openReview({month:"2026-10"}));
  await page.locator('.mr-filter-note [data-mr="retry"]').waitFor();
  assert.equal(requests(),31);
  assert.equal(await page.locator('.mr-poster').count(),1);
  assert.doesNotMatch(await page.locator('.mr-footer').textContent(),/Updating this review/);
  setMode("ready");
  await page.locator('.mr-filter-note [data-mr="retry"]').click();
  await page.waitForFunction(()=>!document.querySelector('.mr-filter-note [data-mr="retry"]'));
  assert.equal(requests(),32);
  assert.match(await page.locator('.mr-footer').textContent(),/Built from your CrossWatch history/);
});
