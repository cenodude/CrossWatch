/* tests/notifications.test.mjs */
/* CrossWatch - Notification dismissal, persistence and profile isolation tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import {readFileSync} from "node:fs";

const script = readFileSync(new URL("../assets/helpers/notifications.js", import.meta.url), "utf8");
function setup(storage = new Map()) {
  class Element {
    hidden = false; innerHTML = ""; style = {}; dataset = {}; children = new Map(); listeners = {};
    setAttribute() {} append() {} before() {} focus() {} contains() { return true; }
    getBoundingClientRect() { return {bottom:40,right:900}; }
    querySelector(selector) {
      if (!this.children.has(selector)) this.children.set(selector, new Element());
      return this.children.get(selector);
    }
    addEventListener(name, fn) { this.listeners[name] = fn; }
  }
  const elements = [];
  const document = {head:new Element(),body:new Element(),getElementById:id=>id==="cw-notifications-css" ? null : new Element(),
    createElement:()=>{const el=new Element();elements.push(el);return el;},addEventListener:(name,fn)=>events[name]=fn};
  const events = {};
  const window = {CW:{AuthState:{user:{id:"alice"}},OverviewProfile:{id:"home"}},
    innerHeight:900,innerWidth:1200,addEventListener:(name,fn)=>events[name]=fn};
  const context = {window,document,location:{href:"http://localhost/",origin:"http://localhost"},URL,
    localStorage:{getItem:key=>storage.get(key),setItem:(key,value)=>storage.set(key,value)}};
  vm.runInNewContext(script, context);
  const panel = elements[2], body = panel.querySelector(".cw-notifications-body");
  const item = (id,revision="review") => ({id,revision,title:`Review ${id}`,href:`/#review=${id}`});
  function publish(items, name="sync-reviews", scope="profile") { window.CW.Notifications.setSource(name,{items,scope}); }
  function clear(id,revision="review") {
    const button = {dataset:{notificationClear:JSON.stringify(["sync-reviews",id,revision])}};
    panel.listeners.click({target:{closest:selector=>selector==="[data-notification-clear]" ? button : null}});
  }
  function clearAll() {
    panel.listeners.click({target:{closest:selector=>selector==="[data-notifications-clear-all]" ? {} : null}});
  }
  return {window,events,publish,clear,clearAll,item,html:()=>body.innerHTML,storage,context};
}

test("clear one survives polling and reload without clearing another review", () => {
  const app=setup(), items=[app.item("one"),app.item("two")];
  app.publish(items); app.clear("one"); app.publish(items);
  assert.doesNotMatch(app.html(), /Review one/);
  assert.match(app.html(), /Review two/);
  const reloaded=setup(app.storage); reloaded.publish(items);
  assert.doesNotMatch(reloaded.html(), /Review one/);
  assert.match(reloaded.html(), /Review two/);
});

test("clear all affects current notifications but allows new notifications and status changes", () => {
  const app=setup(); app.publish([app.item("one"),app.item("two")]); app.clearAll();
  assert.match(app.html(), /all caught up/);
  app.publish([app.item("one"),app.item("two","complete"),app.item("three")]);
  assert.doesNotMatch(app.html(), /Review one/);
  assert.match(app.html(), /Review two/);
  assert.match(app.html(), /Review three/);
});

test("dismissals are isolated by account, profile and notification source", () => {
  const app=setup(), items=[app.item("one")]; app.publish(items); app.clearAll();
  app.window.CW.OverviewProfile.id="work"; app.events["cw:overview-profile-changed"](); app.publish(items);
  assert.match(app.html(), /Review one/);
  app.window.CW.OverviewProfile.id="home"; app.events["cw:overview-profile-changed"](); app.publish(items);
  assert.doesNotMatch(app.html(), /Review one/);
  app.publish(items,"another-source"); assert.match(app.html(), /Review one/);
  app.window.CW.AuthState.user.id="bob"; app.events["auth-changed"](); app.publish(items);
  assert.match(app.html(), /Review one/);
});

test("storage events reflect clearing in another tab", () => {
  const app=setup(), other=setup(app.storage), items=[app.item("one")];
  app.publish(items); other.publish(items); other.clearAll();
  app.events.storage({key:[...app.storage.keys()][0]});
  assert.doesNotMatch(app.html(), /Review one/);
});

test("release dismissals follow the account across profiles and allow newer releases", () => {
  const app=setup(), release=app.item("0.12.0");
  app.publish([release],"updates","account"); app.clearAll();
  app.window.CW.OverviewProfile.id="work"; app.events["cw:overview-profile-changed"]();
  app.publish([release],"updates","account"); assert.doesNotMatch(app.html(),/Review 0.12.0/);
  app.publish([app.item("0.12.1")],"updates","account"); assert.match(app.html(),/Review 0.12.1/);
  app.window.CW.AuthState.user.id="bob"; app.events["auth-changed"]();
  app.publish([release],"updates","account"); assert.match(app.html(),/Review 0.12.0/);
});

test("release announcements use the read-only version endpoint and official release links", async () => {
  const app=setup();
  Object.assign(app.context,{AbortController,setTimeout,clearTimeout,setInterval(){},fetch:async url=>{
    assert.equal(url,"/api/version");
    return {ok:true,json:async()=>({current:"0.11.7",latest:"0.12.0",update_available:true})};
  }});
  vm.runInNewContext(readFileSync(new URL("../assets/helpers/update-notifications.js",import.meta.url),"utf8"),app.context);
  await new Promise(resolve=>setImmediate(resolve));
  assert.match(app.html(), /CrossWatch v0.12.0 is available/);
  assert.match(app.html(), /https:\/\/github.com\/cenodude\/CrossWatch\/releases\/tag\/v0.12.0/);
  assert.match(app.html(), /target="_blank" rel="noopener noreferrer"/);
  app.events["cw:overview-profile-changed"](); assert.match(app.html(),/CrossWatch v0.12.0/);
  app.events["cw-update-status"]({detail:{current:"0.12.0",latest:"0.12.0",available:false}});
  assert.doesNotMatch(app.html(),/CrossWatch v0.12.0 is available/);
});
