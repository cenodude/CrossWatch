/* tests/tracker-wrapped.browser.mjs */
/* CrossWatch - Yearly watch stories, responsive themes, export and scoped notifications */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import {readFile, mkdir} from "node:fs/promises";
import {chromium, webkit} from "playwright";

const names = ["The Midnight Garden","North of Nowhere","The Last Orbit","A Quiet Revolution","After the Rain","Paper Moons","The Long Way Home","Parallel Lives"];
const titles = names.map((title,i)=>({key:`title:${i}`,title,type:i<3?"show":"movie",year:2025,ids:{tmdb:String(i+1)},ref:{feature:"history",key:String(i)},plays:12-i,episodes:10-i,replays:2,minutes:(12-i)*42,timed:12-i,genres:["Drama"],poster:`https://image.tmdb.org/t/p/w342/poster-${i}.jpg`,backdrop:`https://image.tmdb.org/t/p/w1280/backdrop-${i}.jpg`}));
const days=Array.from({length:365},(_,i)=>({day:i+1,date:new Date(Date.UTC(2025,0,i+1)).toISOString().slice(0,10),movies:i%7===0?1:0,episodes:i%4===0?3:0,minutes:100,titles:[]}));
const report={year:"2025",month:"2025",timezone:"Europe/Amsterdam",current:false,plays:328,timed:300,minutes:18740,movies:5,shows:3,active_days:178,streak:12,streak_start:"2025-04-25",streak_end:"2025-05-06",replays:16,titles,days,months:Array.from({length:12},(_,i)=>({month:`2025-${String(i+1).padStart(2,"0")}`,plays:[10,12,18,50,65,45,20,15,20,18,25,30][i],minutes:1200})),hours:Array.from({length:24},(_,i)=>i>18?40:5),weekdays:[40,32,30,40,40,80,66],genres:[{name:"Drama",count:8}],first:{title_key:"title:0",date:"2025-01-01T22:10:00+01:00"},last:{title_key:"title:7",date:"2025-12-31T21:00:00+01:00"}};

async function fixture(t,theme,width=1440,height=1000,{motion=false,keyboard=false}={}) {
  const browser = await (process.env.CW_AUDIT_BROWSER === "webkit" ? webkit : chromium).launch({headless:true});
  t.after(()=>browser.close());
  const page = await browser.newPage({viewport:{width,height},hasTouch:width<1400,reducedMotion:motion?"no-preference":"reduce"});
  page.setDefaultTimeout(8000);
  let requests=0, mode="ready", savedReport=report, generated=1;
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
  await page.route("https://wrapped.test/**",async route=>{
    const url = new URL(route.request().url()),path=url.pathname;
    if(path.startsWith("/assets/")) {
      try{return route.fulfill({body:await readFile(new URL(".."+path,import.meta.url)),contentType:path.endsWith(".css")?"text/css":path.endsWith(".woff2")?"font/woff2":"text/javascript"});}
      catch{return route.fulfill({status:404,body:""});}
    }
    if(path==="/api/tracker/overview/wrapped") {
      requests++;
      if(mode==="denied") return route.fulfill({status:403,json:{detail:"profile_scope_denied"}});
      if(mode==="error") return route.fulfill({status:500,json:{error:"failed"}});
      if(mode==="disabled") return route.fulfill({json:{ok:true,disabled:true,report:null,pending:false,stale:false,error:null}});
      const year=url.searchParams.get("year")||"2025";
      const data = mode==="empty"?{...savedReport,year,plays:0,titles:[],genres:[]}: {...savedReport,year,current:year==="2026"};
      return route.fulfill({json:{ok:true,report:mode==="pending"?null:data,year,current_year:"2026",generated,pending:mode.startsWith("pending"),stale:mode.startsWith("pending")}});
    }
    const css=["fonts/material-symbols-rounded","themes/tokens","css/base","crosswatch","css/layout","css/components","css/pages","css/page-theme","ui-shell",theme==="original"?"themes/original-coverage":"themes/flat"];
    return route.fulfill({contentType:"text/html",body:`<!doctype html><html ${theme==="original"?'class="cw-theme-original"':`data-cw-theme="${theme}" class="cw-theme-${theme==="flat-light"?"light":"dark"}"`}><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">${css.map(p=>`<link rel="stylesheet" href="/assets/${p}.css">`).join("")}</head><body><main id="layout" class="full"><section id="page-tracker" class="cw-themed-page"><div class="cw-root"><button id="entry">CrossWatch Wrapped</button></div></section></main><script type="module">import {openWrapped} from '/assets/js/tracker-wrapped.js';window.auth={user:{username:"Pascal"},profileId:"home"};window.notices={};window.CW={AuthState:{read:()=>window.auth},Notifications:{setSource:(name,data)=>window.notices[name]=data,removeSource:name=>delete window.notices[name]}};window.openReview=(options={})=>openWrapped({root:document.querySelector('#page-tracker'),instance:'default',label:'Pascal',openMedia:row=>window.openedTitle=row,...options});document.querySelector('#entry').onclick=window.openReview;</script></body></html>`});
  });
  await page.goto("https://wrapped.test/");
  if (keyboard) {await page.locator("#entry").focus();await page.keyboard.press("Enter");}
  else await page.locator("#entry").click();
  await page.locator(".wr-premiere h1").waitFor();
  await page.evaluate(()=>document.fonts.ready);
  return {page,requests:()=>requests,setMode:value=>{mode=value;},setReport:value=>{savedReport=value;generated++;}};
}


async function noOverflow(page,label) {
  const result=await page.evaluate(()=>({viewport:innerWidth,width:document.documentElement.scrollWidth}));
  assert.ok(result.width<=result.viewport+1,`${label}: ${JSON.stringify(result)}`);
}
for(const theme of ["original","flat-dark","flat-light"]) test(`${theme}: eight chapters, empty states and controls fit every screen`,async t=>{
  const {page,requests,setMode}=await fixture(t,theme);
  await mkdir(".tmp/wrapped-audit",{recursive:true});
  for(const width of [1440,768,320]) {
    await page.setViewportSize({width,height:900});
    for(let chapter=0;chapter<8;chapter++) {
      await page.locator(`[data-wr-chapter="${chapter}"]`).click();
      assert.equal(await page.locator('.wr-page').getAttribute('data-chapter'),String(chapter));
      await noOverflow(page,`${theme} ${width} chapter ${chapter}`);
      if(chapter===7) assert.equal(await page.locator('.wr-export label').evaluate(el=>getComputedStyle(el).color),'rgb(217, 207, 213)');
      if(chapter===0 || chapter===7) await page.screenshot({path:`.tmp/wrapped-audit/${theme}-${width}-${chapter}.png`});
    }
  }
  assert.equal(requests(),1);
  assert.equal(await page.locator('.wr-burst i').first().evaluate(el=>getComputedStyle(el).animationName),'none');
  assert.equal(await page.locator('[data-wr-effects]').count(),0);
  assert.equal(await page.locator('.wr-wordmark').innerText(),'CrossWatch WRAPPED');
  setMode('empty');
  await page.locator('[data-wr-year]').fill('2024');await page.locator('[data-wr-year]').press('Tab');
  await page.getByText('Your 2024 awaits.',{exact:true}).waitFor();
  assert.equal(await page.locator('.wr-navigation').count(),0);
  await noOverflow(page,theme+' empty');
  await page.screenshot({path:`.tmp/wrapped-audit/${theme}-empty.png`});
  setMode('disabled');
  await page.locator('[data-wr-year]').fill('2023');await page.locator('[data-wr-year]').press('Tab');
  await page.getByText('Wrapped is turned off.',{exact:true}).waitFor();
});

test('Keyboard, touch, title cards, PNG export and current-year preview',async t=>{
  const {page,requests}=await fixture(t,'flat-dark',390,844,{motion:true,keyboard:true});
  await page.locator('.wr-stage h1').focus();await page.keyboard.press('ArrowRight');
  assert.equal(await page.locator('.wr-page').getAttribute('data-chapter'),'1');
  await page.locator('.wr-stage').evaluate(el=>{for(const [type,x] of [['touchstart',250],['touchend',70]]){const event=new Event(type,{bubbles:true});Object.defineProperty(event,type==='touchstart'?'touches':'changedTouches',{value:[{clientX:x,clientY:300}]});el.dispatchEvent(event);}});
  assert.equal(await page.locator('.wr-page').getAttribute('data-chapter'),'2');
  await page.locator('[data-wr-title]').first().click();
  assert.equal(await page.evaluate(()=>openedTitle.key),'title:3');
  await page.locator('[data-wr-chapter="7"]').click();
  assert.equal(await page.locator('[data-wr-personal]').isChecked(),false);
  const download=page.waitForEvent('download');await page.locator('[data-wr="download"]').click();
  const file=await download;assert.equal(file.suggestedFilename(),'CrossWatch-Wrapped-2025.png');
  const bytes=await readFile(await file.path());
  assert.equal(bytes.subarray(1,4).toString(),'PNG');assert.equal(bytes.readUInt32BE(16),1200);assert.equal(bytes.readUInt32BE(20),1600);
  assert.equal(requests(),1);
  await page.locator('[data-wr-year]').fill('2026');await page.locator('[data-wr-year]').press('Tab');
  await page.getByText('Pascal, this is your year so far.',{exact:true}).waitFor();
  await page.locator('[data-wr="close"]').click();assert.equal(await page.locator('.wr-page').count(),0);
  assert.equal(await page.locator('#entry').evaluate(el=>el===document.activeElement),true);
});

test('Preparing reviews stay nonblocking and notify only the requesting profile',async t=>{
  const {page,setMode}=await fixture(t,'flat-dark');
  await page.clock.install();
  setMode('pending');await page.locator('[data-wr-year]').fill('2024');await page.locator('[data-wr-year]').press('Tab');
  await page.getByText('Preparing your premiere.',{exact:true}).waitFor();
  await page.locator('[data-wr="close"]').click();
  setMode('ready');await page.clock.fastForward(10001);
  await page.waitForFunction(()=>window.notices['cw-wrapped']?.items.length===1);
  const notice=await page.evaluate(()=>window.notices['cw-wrapped'].items[0]);
  assert.equal(notice.href,'#tracker?instance=default&year=2024');assert.equal(notice.title,'Your 2024 CrossWatch Wrapped is ready');
  await page.evaluate(()=>{window.dispatchEvent(new CustomEvent('cw:auth-state-changed'));});
  assert.equal(await page.evaluate(()=>window.notices['cw-wrapped'].items.length),1);
  await page.evaluate(()=>{auth.profileId='different';window.dispatchEvent(new CustomEvent('cw:auth-state-changed'));});
  assert.equal(await page.evaluate(()=>window.notices['cw-wrapped']?.items.length||0),0);
});

test('Saved stories remain usable through failed updates and retry',async t=>{
  const {page,setMode}=await fixture(t,'flat-light');
  await page.clock.install();setMode('pending-saved');
  await page.evaluate(()=>document.dispatchEvent(new CustomEvent('config-saved')));
  await page.getByText(/Updating your story in the background/).waitFor();
  setMode('error');await page.clock.fastForward(3000);
  await page.locator('[data-wr="retry"]').waitFor();
  await page.locator('[data-wr-chapter="4"]').click();
  assert.equal(await page.locator('.wr-streak strong').textContent(),'12');
  setMode('ready');await page.locator('[data-wr="retry"]').click();
  await page.locator('[data-wr="retry"]').waitFor({state:'detached'});
  assert.equal(await page.locator('.wr-page').getAttribute('data-chapter'),'4');
  setMode('denied');await page.evaluate(()=>document.dispatchEvent(new CustomEvent('config-saved')));
  await page.getByText('A brief intermission.',{exact:true}).waitFor();
  assert.equal(await page.locator('.wr-navigation').count(),0);
});

test('A profile change closes the story and cancels abandoned requests',async t=>{
  const {page,setMode,requests}=await fixture(t,'flat-dark');
  await page.clock.install();setMode('pending');
  await page.locator('[data-wr-year]').fill('2024');await page.locator('[data-wr-year]').press('Tab');
  await page.getByText('Preparing your premiere.',{exact:true}).waitFor();
  await page.evaluate(()=>{auth.profileId='other';window.dispatchEvent(new CustomEvent('cw:auth-state-changed'));});
  assert.equal(await page.locator('.wr-page').count(),0);
  const before=requests();await page.clock.fastForward(60000);assert.equal(requests(),before);
});

test('Background completion preserves focus, expanded movies and keyboard navigation',async t=>{
  const {page,setMode,setReport}=await fixture(t,'flat-dark');
  await page.clock.install();
  const expandedReport={...report,titles:[...titles,{...titles[3],key:'extra',title:'Another movie'}]};
  setReport(expandedReport);setMode('pending-saved');
  await page.evaluate(()=>document.dispatchEvent(new CustomEvent('config-saved')));
  await page.getByText(/Updating your story in the background/).waitFor();
  await page.locator('[data-wr-chapter="2"]').click();
  await page.locator('.wr-gallery summary').click();
  await page.locator('[data-wr-title="extra"]').focus();
  setMode('ready');setReport({...expandedReport,plays:329});
  await page.clock.fastForward(3000);
  await page.getByText(/Updating your story in the background/).waitFor({state:'detached'});
  assert.equal(await page.locator('.wr-gallery').evaluate(el=>el.open),true);
  assert.equal(await page.evaluate(()=>document.activeElement.getAttribute('data-wr-title')),'extra');
  await page.keyboard.press('ArrowRight');
  assert.equal(await page.locator('.wr-page').getAttribute('data-chapter'),'3');
  setMode('pending-saved');
  await page.evaluate(()=>document.dispatchEvent(new CustomEvent('config-saved')));
  await page.getByText(/Updating your story in the background/).waitFor();
  await page.locator('[data-wr-year]').fill('202');
  setMode('ready');
  await page.clock.fastForward(3000);
  await page.getByText(/Updating your story in the background/).waitFor({state:'detached'});
  assert.equal(await page.locator('[data-wr-year]').inputValue(),'202');
  assert.equal(await page.locator('[data-wr-year]').evaluate(el=>el===document.activeElement),true);
});

test('Missing and failed cached artwork loads lazily through Tracker artwork with a safe fallback',async t=>{
  const {page,setReport,requests}=await fixture(t,'flat-dark');
  const artwork=[];
  await page.route('https://wrapped.test/art/tmdb/**',route=>{
    const url=new URL(route.request().url());
    artwork.push(url);
    if (url.pathname.endsWith('/5')) return route.fulfill({status:404,body:''});
    return route.fulfill({contentType:'image/svg+xml',body:'<svg xmlns="http://www.w3.org/2000/svg" width="200" height="300"><rect width="200" height="300" fill="#684d85"/></svg>'});
  });
  await page.route('https://image.tmdb.org/**/missing.jpg',route=>route.fulfill({status:404,body:''}));
  const uncached=titles.map((row,i)=>({...row,poster:i===7?row.poster:i===5?'https://image.tmdb.org/t/p/w342/missing.jpg':'',backdrop:'',plays:i===3?1:row.plays,ids:i===6?{tmdb:'../unsafe'}:row.ids}));
  uncached.push({...uncached[3],key:'extra',ids:{tmdb:'999'},title:'Extra movie'});
  setReport({...report,titles:uncached});
  await page.evaluate(()=>document.dispatchEvent(new CustomEvent('config-saved')));
  await page.waitForFunction(()=>document.querySelector('.wr-reel img[src*="/art/tmdb/"]'));
  await page.locator('[data-wr-chapter="2"]').click();
  const loaded=key=>page.waitForFunction(key=>{
    const img=document.querySelector(`[data-wr-title="${key}"] img`);
    return img?.complete && img.naturalWidth>0;
  },key);
  await loaded('title:3');await loaded('title:5');await loaded('title:7');
  assert.match(await page.locator('[data-wr-title="title:3"] img').getAttribute('src'),/^\/art\/tmdb\/movie\/4\?/);
  assert.equal(await page.locator('[data-wr-title="title:3"] small').innerText(),'1 play');
  await page.locator('[data-wr-title="title:4"] .wr-art-empty').waitFor();
  assert.match(await page.locator('[data-wr-title="title:5"] img').getAttribute('src'),/^\/art\/tmdb\/movie\/6\?/);
  assert.equal(await page.locator('[data-wr-title="title:6"] img').count(),0);
  assert.equal(artwork.some(url=>url.pathname.endsWith('/8') || url.pathname.endsWith('/999')),false);
  await page.locator('.wr-gallery summary').click();
  await page.locator('[data-wr-title="extra"]').scrollIntoViewIfNeeded();
  await loaded('extra');
  assert.equal(artwork.some(url=>url.pathname.endsWith('/999')),true);
  await page.locator('[data-wr-chapter="3"]').click();
  await page.waitForFunction(()=>{const img=document.querySelector('.wr-feature img');return img?.complete && img.naturalWidth>0;});
  assert.equal(artwork.some(url=>url.pathname==='/art/tmdb/tv/1' && url.searchParams.get('kind')==='backdrop' && url.searchParams.get('size')==='w1280'),true);
  assert.equal(requests(),2);
});


test('Integrated navigation reveals artwork on hover and supports keyboard and reduced motion',async t=>{
  const {page,requests}=await fixture(t,'flat-dark',1440,1000,{motion:true});
  await page.emulateMedia({reducedMotion:'no-preference'});
  const chip=page.locator('[data-wr-chapter="4"]');
  assert.equal(await page.locator('.wr-stage .wr-navigation').count(),1);
  assert.equal(await page.locator('.wr-navigation select').count(),0);
  await chip.hover();
  await page.waitForFunction(()=>getComputedStyle(document.querySelector('[data-wr-chapter="4"] .wr-chapter-preview')).opacity==='1');
  await chip.click();
  assert.equal(await page.locator('[data-wr-chapter="4"]').getAttribute('aria-current'),'step');
  await page.locator('.wr-stage').hover({position:{x:750,y:180}});
  await page.waitForFunction(()=>document.querySelector('.wr-stage').style.getPropertyValue('--wr-light-x')!=='');
  await page.waitForFunction(()=>getComputedStyle(document.querySelector('.wr-content')).opacity==='1');
  await page.screenshot({path:'.tmp/wrapped-audit/integrated-streak.png'});
  await page.locator('[data-wr-chapter="3"]').hover();
  await page.waitForFunction(()=>getComputedStyle(document.querySelector('[data-wr-chapter="3"] .wr-chapter-preview')).opacity==='1');
  await page.screenshot({path:'.tmp/wrapped-audit/integrated-preview.png'});
  await page.locator('.wr-edge-next').focus();await page.keyboard.press('Enter');
  assert.equal(await page.locator('.wr-page').getAttribute('data-chapter'),'5');
  await page.emulateMedia({reducedMotion:'reduce'});
  assert.equal(await page.locator('[data-wr-effects]').count(),0);
  assert.equal(await page.locator('.wr-content').evaluate(el=>getComputedStyle(el).animationName),'none');
  await page.locator('.wr-stage').hover({position:{x:600,y:150}});
  assert.equal(await page.locator('.wr-stage').evaluate(el=>el.style.getPropertyValue('--wr-light-x')),'');
  await page.emulateMedia({reducedMotion:'no-preference'});
  await page.emulateMedia({reducedMotion:'reduce'});
  await page.locator('[data-wr-chapter="4"]').click();
  assert.equal(await page.locator('.wr-content').evaluate(el=>getComputedStyle(el).animationName),'none');
  assert.equal(requests(),1);
});
