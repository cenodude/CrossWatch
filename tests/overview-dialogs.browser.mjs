/* tests/overview-dialogs.browser.mjs */
/* CrossWatch - Activity and provider preferences across themes and viewports */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile, mkdir} from 'node:fs/promises';
import {chromium,webkit} from 'playwright';

async function openPlayback(page) {
  if(page.viewportSize().width<=760)await page.getByRole('button',{name:'Filters and display options',exact:true}).click();
  await page.locator('#profile-playback-settings').click();
}

async function fixture(t,theme='',width=1100,empty=false) {
  const browser=await (process.env.CW_OVERVIEW_BROWSER==='webkit'?webkit:chromium).launch({headless:true});
  t.after(()=>browser.close());
  const page=await browser.newPage({viewport:{width,height:820}});
  page.setDefaultTimeout(10000);
  const errors=[],writes=[];
  page.on('pageerror',e=>errors.push(e.message));
  t.after(()=>assert.deepEqual(errors,[]));
  const source=await readFile(new URL('../ui_frontend.py',import.meta.url),'utf8');
  const playback=source.slice(source.indexOf('def _profile_playback_panel()')).match(/return """([\s\S]*?)"""/)[1];
  const instances=empty?{}:{CROSSWATCH:[{id:'default',label:'Default'},{id:'CW-P01',label:'CW-P01',friendly_label:'Family tracker'},{id:'CW-P02',label:'CW-P02'}],SIMKL:[{id:'default',label:'Default'},{id:'SIMKL-P01',display_label:'Anime account',label:'SIMKL-P01'}],PLEX:[{id:'default',label:'Living room'}]};
  const tz=-new Date().getTimezoneOffset()*60, today=Math.floor((Date.now()/1000+tz)/86400);
  await page.route('**/*',async route=>{
    const url=new URL(route.request().url());
    if(url.pathname.startsWith('/assets/')){
      try{return route.fulfill({contentType:url.pathname.endsWith('.css')?'text/css':url.pathname.endsWith('.svg')?'image/svg+xml':url.pathname.endsWith('.png')?'image/png':url.pathname.endsWith('.woff2')?'font/woff2':'text/javascript',body:await readFile(new URL('..'+url.pathname,import.meta.url))});}
      catch{return route.fulfill({status:404,body:''});}
    }
    if(url.pathname.startsWith('/api/')){
      if(route.request().method()==='POST')writes.push({path:url.pathname,body:route.request().postDataJSON()});
      let data={ok:true};
      if(url.pathname==='/api/provider-instances')data=instances;
      if(url.pathname==='/api/events/calendar')data={ok:true,days:empty?[]:Array.from({length:63},(_,i)=>({d:today-62+i,runs:i%3===0?0:3,changes:i%3===0?0:i*7,failed:i===60?1:0})),totals:{runs:126,changes:8000,failed:1,active_days:42,added:7900,removed:100}};
      if(url.pathname==='/api/events/calendar/day')data={ok:true,runs:empty?[]:[{run_id:'test-run',started_at:Date.now()/1000,duration:3,added:3,changes:3,pair_rows:[{src_provider:'CROSSWATCH',src_instance:'CW-P01',dst_provider:'SIMKL',dst_instance:'SIMKL-P01',feature:'history',added:3}]}]};
      if(url.pathname==='/api/playback_progress/settings')data={ok:true,grouping:'separate',provider_timeout_seconds:12,profiles:empty?[]:[{provider:'crosswatch',instance_id:'default',instance_label:'CrossWatch Default',configured:true,read:true,included:true},{provider:'crosswatch',instance_id:'CW-P01',instance_label:'CrossWatch Family tracker',configured:true,read:true,included:true},{provider:'crosswatch',instance_id:'CW-P02',instance_label:'CrossWatch CW-P02',configured:true,read:true,included:false},{provider:'simkl',instance_id:'SIMKL-P01',instance_label:'SIMKL Anime account',configured:true,read:true,included:true},{provider:'plex',instance_id:'default',instance_label:'Plex Living room',configured:true,read:true,included:true},{provider:'emby',instance_id:'default',configured:false,read:false,included:false}]};
      if(url.pathname==='/api/profile')data={user:{username:'admin',role:'admin'}};
      return route.fulfill({json:data});
    }
    const css=['themes/tokens','css/base','crosswatch','css/layout','css/components','css/pages','css/page-theme','css/profile-page','ui-shell','themes/flat','themes/original-coverage','js/modals/core/styles','fonts/material-symbols-rounded'];
    return route.fulfill({contentType:'text/html',body:`<!doctype html><html data-cw-role="admin"${theme?` data-cw-theme="${theme}"`:''}><head><meta name="viewport" content="width=device-width,initial-scale=1">${css.map(f=>`<link rel="stylesheet" href="/assets/${f}.css">`).join('')}</head><body class="cw-profile-page"><div id="profile-quick-stats" hidden></div><button id="insights">Insights</button>${playback}<script>window.CW={};window.getConfiguredProviders=()=>new Set(${JSON.stringify(Object.keys(instances))});</script><script src="/assets/helpers/provider-meta.js"></script><script src="/assets/helpers/feature-meta.js"></script><script src="/assets/helpers/icon-select.js"></script><script src="/assets/helpers/profile-datetime.js"></script><script src="/assets/helpers/month-bar.js"></script><script src="/assets/js/profile-page.js"></script><script type="module">
      import {ModalHost} from '/assets/js/modals/core/host.js';
      import insights from '/assets/js/modals/insight-settings/index.js';
      const host=new ModalHost();
      document.querySelector('#insights').onclick=()=>host.mount(insights);
      window.openEvents=props=>{window.eventsProps=props;host.unmount();};
    </script></body></html>`});
  });
  await page.goto('http://cw.test/');
  await page.waitForFunction(()=>!!document.querySelector('#insights').onclick);
  await page.locator("#profile-panel-playback").evaluate(n=>n.classList.add("active"));
  return {page,writes,today};
}

test('insight selection saves instance IDs while showing friendly names',async t=>{
  const {page}=await fixture(t);
  await page.locator('#insights').click();
  await page.getByLabel('Family tracker',{exact:true}).uncheck();
  const parent=page.getByRole('checkbox',{name:'CrossWatch',exact:true});
  assert.equal(await parent.evaluate(node=>node.indeterminate),true);
  await page.getByRole('button',{name:'History',exact:true}).click();
  await page.getByRole('button',{name:'Apply changes',exact:true}).click();
  const prefs=await page.evaluate(()=>JSON.parse(localStorage.getItem('insights.settings.v1')));
  assert.deepEqual(prefs.instances.crosswatch,['default','CW-P02']);
  assert.equal(prefs.features.history,false);
  await page.locator('#insights').click();
  assert.equal(await page.getByLabel('Family tracker',{exact:true}).isChecked(),false);
  assert.equal(await page.getByLabel('CW-P02',{exact:true}).isChecked(),true);
  assert.equal(await page.getByRole('button',{name:'History',exact:true}).getAttribute('aria-pressed'),'false');
  await parent.focus();
  await page.keyboard.press('Space');
  assert.equal(await page.getByLabel('Family tracker',{exact:true}).isChecked(),true);
  await page.keyboard.press('Space');
  assert.equal(await page.locator('[data-provider="crosswatch"] input[data-inst]:checked').count(),0);
  await page.getByRole('button',{name:'Restore defaults',exact:true}).click();
  assert.equal(await page.getByRole('button',{name:'History',exact:true}).getAttribute('aria-pressed'),'true');
  assert.equal(await parent.isChecked(),true);
  assert.equal(await parent.evaluate(node=>node.indeterminate),false);
  assert.equal(await page.evaluate(()=>JSON.parse(localStorage.getItem('insights.settings.v1')).features.history),false);
  await page.getByRole('button',{name:'Apply changes',exact:true}).click();
  const restored=await page.evaluate(()=>JSON.parse(localStorage.getItem('insights.settings.v1')));
  assert.equal(restored.features.history,true);
  assert.equal(restored.instances.crosswatch,undefined);
});

test('insight feature buttons retain keyboard toggles and require one enabled feature',async t=>{
  const {page}=await fixture(t,'flat-dark',390);
  await page.locator('#insights').click();
  const buttons=page.locator('button[data-feat]');
  await buttons.first().waitFor();
  for(const button of await buttons.all()) {
    await button.focus();
    await page.keyboard.press('Space');
    assert.equal(await button.getAttribute('aria-pressed'),'false');
  }
  await page.getByRole('button',{name:'Apply changes',exact:true}).click();
  const saved=await page.evaluate(()=>JSON.parse(localStorage.getItem('insights.settings.v1')));
  assert.equal(saved.features.watchlist,true);
  assert.equal(saved.features.history,false);
});

test('playback provider preferences retain friendly names and save stable IDs',async t=>{
  const {page,writes}=await fixture(t);
  await openPlayback(page);
  await page.getByLabel('Family tracker',{exact:true}).waitFor();
  assert.equal((await page.locator('#profile-playback-grouping + .cw-icon-select button').first().innerText()).trim(),'Separate instances');
  assert.equal(await page.locator('#profile-playback-provider-list input').count(),5);
  await page.getByLabel('Family tracker',{exact:true}).uncheck();
  assert.equal(await page.getByLabel('CW-P02',{exact:true}).isChecked(),false);
  await page.getByLabel('Living room',{exact:true}).waitFor();
  await page.locator('#profile-playback-grouping + .cw-icon-select button').first().click();
  await page.locator('.cw-icon-select-item[data-value="separate"]').click();
  await page.getByText('Advanced',{exact:true}).click();
  await page.getByLabel('Timeout in seconds',{exact:true}).fill('18');
  await page.getByRole('button',{name:'Apply',exact:true}).click();
  await page.waitForFunction(()=>!document.querySelector('#profile-playback-providers').open);
  const saved=writes.find(w=>w.path==='/api/playback_progress/settings');
  assert.equal(saved.body.provider_timeout_seconds,18);
  assert.equal(saved.body.grouping,'separate');
  assert.equal(saved.body.profiles.find(p=>p.provider==='emby').included,false);
  assert.equal(saved.body.profiles.find(p=>p.instance_id==='CW-P01').included,false);
});

for(const theme of ['', 'flat-dark','flat-light'])for(const width of [1100,390])test(`overview dialogs ${theme||'original'} ${width}`,async t=>{
  const {page}=await fixture(t,theme,width);
  const folder=new URL('../build/overview-dialogs/',import.meta.url);await mkdir(folder,{recursive:true});
  const capture=async name=>{
    const dialog=page.getByRole('dialog');
    assert.equal(await dialog.evaluate(n=>{const r=n.getBoundingClientRect();return r.left>=0&&r.top>=0&&r.right<=innerWidth+1&&r.bottom<=innerHeight+1&&n.scrollWidth<=n.clientWidth;}),true);
    assert.equal(await dialog.locator('.pf-body').evaluate(n=>n.scrollWidth<=n.clientWidth),true);
    await page.screenshot({path:new URL(`${name}-${theme||'original'}-${width}.png`,folder).pathname.replace(/^\/(?=[A-Za-z]:)/,'')});
  };
  await page.locator('#insights').click();await page.getByLabel('Family tracker',{exact:true}).waitFor();
  await page.getByLabel('Family tracker',{exact:true}).uncheck();
  await page.getByRole('checkbox',{name:'Plex',exact:true}).uncheck();
  await page.getByRole('button',{name:'History',exact:true}).click();
  await page.getByRole('button',{name:'Collections',exact:true}).evaluate(node=>node.disabled=true);
  await capture('insights');
  await page.getByRole('button',{name:'Close',exact:true}).click();
  await openPlayback(page);await page.getByLabel('Family tracker',{exact:true}).waitFor();await capture('playback');
});

for(const theme of ['', 'flat-dark','flat-light'])test(`empty overview dialogs ${theme||'original'}`,async t=>{
  const {page}=await fixture(t,theme,390,true);
  await page.locator('#insights').click();await page.getByText('No configured providers yet.',{exact:true}).waitFor();
  await page.getByRole('button',{name:'Close',exact:true}).click();
  await openPlayback(page);await page.getByText('No connected playback providers were found.',{exact:true}).waitFor();
});
