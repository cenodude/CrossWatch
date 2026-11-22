/* tests/notifications.test.mjs */
/* CrossWatch - Notification menu read state, dismissals, devices, alerts and the notification feed */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import {readFileSync} from "node:fs";

const script = readFileSync(new URL("../assets/helpers/notifications.js", import.meta.url), "utf8");
const feedScript = readFileSync(new URL("../assets/js/notification-feed.js", import.meta.url), "utf8");
const key = (name, id, revision = "review") => JSON.stringify([name, id, revision]);
const settle = async () => { for (let i = 0; i < 20; i++) await new Promise(resolve => setImmediate(resolve)); };

function setup(storage = new Map(), {pairs = true, role = "admin", write = "on", fetch} = {}) {
  class Element {
    hidden = false; innerHTML = ""; textContent = ""; style = {}; dataset = {}; children = new Map(); listeners = {}; checked = false;
    setAttribute() {} append() {} before() {} focus() {} contains() { return true; }
    getBoundingClientRect() { return {bottom:40,right:900}; }
    querySelector(selector) {
      if (!this.children.has(selector)) this.children.set(selector, new Element());
      return this.children.get(selector);
    }
    addEventListener(name, fn) { this.listeners[name] = fn; }
  }
  const elements = [], dispatched = [];
  const events = {};
  const document = {head:new Element(),body:new Element(),documentElement:{dataset:{cwRole:role,cwPermWrite:write}},
    getElementById:id=>id==="cw-notifications-css" || (id==="pairs_list" && !pairs) ? null : new Element(),
    createElement:()=>{const el=new Element();elements.push(el);return el;},addEventListener:(name,fn)=>events[name]=fn};
  const window = {CW:{AuthState:{user:{id:"alice"}},OverviewProfile:{id:"home"}},
    innerHeight:900,innerWidth:1200,addEventListener:(name,fn)=>events[name]=fn,
    dispatchEvent:event=>dispatched.push(event.type)};
  const context = {window,document,clearTimeout(){},setTimeout(){},navigator:{},location:{href:"http://localhost/",origin:"http://localhost"},URL,
    CustomEvent:class { constructor(type) { this.type = type; } },
    localStorage:{getItem:key=>storage.get(key),setItem:(key,value)=>storage.set(key,value)},...(fetch ? {fetch} : {})};
  vm.runInNewContext(script, context);
  const panel = elements[2], body = panel.querySelector(".cw-notifications-body"), badge = elements[1].querySelector(".cw-notifications-count");
  const item = (id,revision="review",extra={}) => ({id,revision,title:`Review ${id}`,href:`/#review=${id}`,...extra});
  const published = new Map();
  function publish(items, name="sync-reviews", scope="profile", state=null) {
    published.set(name, items.map(row => ({...row, source:name, scope, key:key(name,row.id,row.revision || "")})));
    deliver({items:[...published.values()].flat(), errors:[], state});
  }
  function deliver(data, ctx = {user:window.CW.AuthState.user.id, scope:window.CW.OverviewProfile.id}) {
    window.CW.Notifications.receive(data, ctx);
  }
  function clear(id,revision="review",name="sync-reviews") {
    const button = {dataset:{notificationClear:key(name,id,revision)}};
    panel.listeners.click({target:{closest:selector=>selector==="[data-notification-clear]" ? button : null}});
  }
  function clearAll() {
    panel.listeners.click({target:{closest:selector=>selector==="[data-notifications-clear-all]" ? {} : null}});
  }
  const open = () => elements[1].listeners.click();
  return {window,events,publish,deliver,clear,clearAll,open,item,html:()=>body.innerHTML,badge,storage,context,alert:elements[3],dispatched,published};
}

function server({failPosts = 0} = {}) {
  const rows = new Map(), calls = [];
  const list = (scope, kind) => rows.get(`${scope}|${kind}`) || [];
  const fetch = async (url, options = {}) => {
    const method = options.method || "GET";
    calls.push(`${method} ${url}`);
    if (failPosts-- > 0) throw new Error("offline");
    const kind = url.endsWith("/seen") ? "seen" : "dismissed";
    const {scope, keys} = JSON.parse(options.body);
    const saved = list(scope, kind);
    for (const value of keys) if (!saved.includes(value)) saved.push(value);
    rows.set(`${scope}|${kind}`, saved);
    return {ok:true,status:200,json:async()=>({ok:true,scope,keys:[...saved]})};
  };
  const state = (profile = "home") => ({account:{dismissed:[...list("account","dismissed")],seen:[...list("account","seen")]},
    profile:{dismissed:[...list(profile,"dismissed")],seen:[...list(profile,"seen")]}});
  return {rows, calls, fetch, state, list};
}

test("alerts are opt-in, show the title, skip initial and quiet items and do not repeat", () => {
  const app = setup(), pulses = [];
  app.context.navigator.vibrate = value => pulses.push([...value]);
  app.publish([app.item("old")]);
  app.publish([app.item("old"),app.item("disabled")]);
  assert.equal(app.alert.hidden,true);
  app.storage.set("cw.mobile.notificationAlerts","1");
  app.publish([app.item("old"),app.item("disabled"),app.item("progress","running",{quiet:true})]);
  assert.equal(app.alert.hidden,true);
  app.publish([app.item("old"),app.item("disabled"),app.item("new")]);
  assert.equal(app.alert.hidden,false);
  assert.equal(app.alert.querySelector("[role=status]").textContent,"Review new");
  assert.deepEqual(pulses,[[30,60,30]]);
  app.publish([app.item("old"),app.item("new")]);
  app.publish([app.item("new")]);
  assert.equal(pulses.length,1);
  app.context.document.hidden = true;
  app.events.visibilitychange();
  app.publish([app.item("hidden")]);
  assert.equal(app.alert.hidden,true);
  app.context.document.hidden = false;
  app.publish([app.item("d"),app.item("e")]);
  assert.equal(app.alert.querySelector("[role=status]").textContent,"2 new notifications");
});

test("the alert baseline is per profile and survives vibration errors", () => {
  const app=setup(new Map([["cw.mobile.notificationAlerts","1"]]));
  app.publish([]); app.publish([app.item("desktop")]);
  assert.equal(app.alert.hidden,false);
  app.window.CW.OverviewProfile.id="other";
  app.events["cw:overview-profile-changed"]();
  assert.equal(app.alert.hidden,true);
  app.publish([app.item("initial")]);
  assert.equal(app.alert.hidden,true);
  app.context.navigator.vibrate = () => { throw new Error("blocked"); };
  app.publish([app.item("initial"),app.item("another")]);
  assert.equal(app.alert.hidden,false);
});

test("sync reviews link works from the main app and standalone profiles", () => {
  assert.match(setup().html(), /class="cw-notifications-footer" href="#settings\/sync"/);
  const profile = setup(new Map(), {pairs:false});
  assert.match(profile.html(), /class="cw-notifications-footer" href="\/#settings\/sync"/);
  const managed = setup(new Map(), {pairs:false,role:"user"});
  assert.match(managed.html(), /href="\/\?main=1#settings\/sync"/);
});

test("sync reviews link respects write access and setup state", () => {
  const profile = setup(new Map(), {pairs:false,role:"user",write:"off"});
  assert.doesNotMatch(profile.html(), /cw-notifications-footer/);
  profile.context.document.documentElement.dataset.cwPermWrite = "on";
  profile.events["cw:auth-state-changed"]();
  assert.match(profile.html(), /cw-notifications-footer/);
  profile.window.cwIsAuthSetupPending = () => true;
  profile.publish([]);
  assert.doesNotMatch(profile.html(), /cw-notifications-footer/);
});

test("the menu shows loading until the first server answer, then errors keep the last items", () => {
  const app = setup();
  assert.match(app.html(), /Loading notifications/);
  app.publish([app.item("one")]);
  assert.doesNotMatch(app.html(), /Loading notifications/);
  app.deliver({ok:false,items:null,errors:["Could not load notifications."]});
  assert.match(app.html(), /Review one/);
  assert.match(app.html(), /Could not load notifications\..*data-notifications-retry/);
  app.deliver({ok:false,denied:true,items:[]});
  assert.match(app.html(), /all caught up/);
});

test("answers for another account or profile are ignored", () => {
  const app = setup();
  app.publish([app.item("one")]);
  app.deliver({items:[{...app.item("stale"),source:"sync-reviews",scope:"profile",key:key("sync-reviews","stale")}]}, {user:"alice",scope:"work"});
  app.deliver({items:[]}, {user:"bob",scope:"home"});
  assert.match(app.html(), /Review one/);
  assert.doesNotMatch(app.html(), /Review stale/);
});

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

test("release dismissals follow the account across profiles", () => {
  const app=setup(), release=app.item("0.12.0","");
  app.publish([release],"updates","account"); app.clearAll();
  app.window.CW.OverviewProfile.id="work"; app.events["cw:overview-profile-changed"]();
  app.publish([release],"updates","account"); assert.doesNotMatch(app.html(),/Review 0.12.0/);
  app.publish([app.item("0.12.1","")],"updates","account"); assert.match(app.html(),/Review 0.12.1/);
});

test("storage events reflect clearing in another tab", () => {
  const app=setup(), other=setup(app.storage), items=[app.item("one")];
  app.publish(items); other.publish(items); other.clearAll();
  app.events.storage({key:[...app.storage.keys()][0]});
  assert.doesNotMatch(app.html(), /Review one/);
});

test("profile shell initializes account state and shares Main's dismissals", () => {
  const frontend = readFileSync(new URL("../ui_frontend.py", import.meta.url), "utf8");
  const profileShell = frontend.slice(frontend.indexOf("def get_profile_html("));
  const authScript = profileShell.indexOf('<script src="/assets/helpers/auth-state.js?');
  assert.ok(authScript >= 0);
  assert.ok(authScript < profileShell.indexOf('<script src="/assets/helpers/notifications.js?'));
  assert.ok(profileShell.indexOf('<script src="/assets/js/overview-profile.js?') < profileShell.indexOf('<script src="/assets/js/notification-feed.js?'));
  assert.ok(profileShell.indexOf('<script src="/assets/helpers/api.js?') < profileShell.indexOf('<script src="/assets/js/notification-feed.js?'));
  const main = setup(), profile = setup(main.storage, {pairs: false});
  delete profile.window.CW.AuthState;
  vm.runInNewContext(readFileSync(new URL("../assets/helpers/auth-state.js", import.meta.url), "utf8"), profile.context);
  profile.window.CW.AuthState.apply({authenticated: true, user: {id: "alice", is_admin: true}});
  const items = [main.item("import"), main.item("metadata")];
  main.publish(items, "tracker-check", "account");
  main.clearAll();
  profile.publish(items, "tracker-check", "account");
  assert.doesNotMatch(profile.html(), /Review import|Review metadata/);
});

test("unseen notifications drive the badge and opening the menu marks them read on the server", async () => {
  const remote = server(), app = setup(new Map(), {fetch:remote.fetch});
  app.publish([app.item("one"),app.item("two")], "sync-reviews", "profile", remote.state());
  assert.equal(app.badge.hidden,false);
  assert.equal(app.badge.textContent,"2");
  app.open(); await settle();
  assert.match(app.html(), /class="is-new".*cw-notification-new">New</);
  assert.deepEqual(remote.list("home","seen"), [key("sync-reviews","one"),key("sync-reviews","two")]);
  app.events.keydown({key:"Escape",preventDefault(){}});
  assert.doesNotMatch(app.html(), /is-new/);
  app.publish([app.item("one"),app.item("two")], "sync-reviews", "profile", remote.state());
  assert.equal(app.badge.hidden,true);
  const phone = setup(new Map(), {fetch:remote.fetch});
  phone.publish([phone.item("one"),phone.item("two"),phone.item("three")], "sync-reviews", "profile", remote.state());
  assert.equal(phone.badge.textContent,"1");
  assert.match(phone.html(), /Review one/);
});

test("dismissals reach the same user's other devices", async () => {
  const remote = server(), desktop = setup(new Map(), {fetch:remote.fetch}), phone = setup(new Map(), {fetch:remote.fetch});
  const items = [desktop.item("one"), desktop.item("two")];
  desktop.publish(items, "sync-reviews", "profile", remote.state()); phone.publish(items, "sync-reviews", "profile", remote.state());
  desktop.clear("one"); desktop.publish([desktop.item("0.12.0","")], "updates", "account", remote.state()); desktop.clear("0.12.0","","updates"); await settle();
  assert.deepEqual(remote.list("home","dismissed"), [key("sync-reviews","one")]);
  assert.deepEqual(remote.list("account","dismissed"), [key("updates","0.12.0","")]);
  phone.publish([phone.item("0.12.0","")], "updates", "account", remote.state());
  assert.doesNotMatch(phone.html(), /Review one|Review 0.12.0/);
  assert.match(phone.html(), /Review two/);
});

test("dismissals saved only in this browser move to the server once", async () => {
  const remote = server(), first = setup();
  first.publish([first.item("old")]); first.clear("old");
  const app = setup(first.storage, {fetch:remote.fetch});
  app.publish([app.item("old"),app.item("new")], "sync-reviews", "profile", remote.state()); await settle();
  assert.deepEqual(remote.list("home","dismissed"), [key("sync-reviews","old")]);
  assert.doesNotMatch(app.html(), /Review old/);
  const posts = remote.calls.length;
  app.publish([app.item("old"),app.item("new")], "sync-reviews", "profile", remote.state()); await settle();
  assert.equal(remote.calls.length, posts);
});

test("a dismissal made offline is sent with the next server answer", async () => {
  const remote = server({failPosts:1}), app = setup(new Map(), {fetch:remote.fetch}), items = [app.item("one")];
  app.publish(items, "sync-reviews", "profile", remote.state()); app.clear("one"); await settle();
  assert.deepEqual(remote.list("home","dismissed"), []);
  app.publish(items, "sync-reviews", "profile", remote.state());
  assert.doesNotMatch(app.html(), /Review one/);
  await settle();
  assert.deepEqual(remote.list("home","dismissed"), [key("sync-reviews","one")]);
});

test("a finished tracker move tells the tracker page to reload", () => {
  const app = setup();
  app.deliver({items:[],signals:{tracker_migration:"running"}});
  app.deliver({items:[],signals:{tracker_migration:"completed"}});
  app.deliver({items:[],signals:{tracker_migration:"completed"}});
  assert.deepEqual(app.dispatched.filter(type => type === "cw:tracker-migrated"), ["cw:tracker-migrated"]);
});

function pushDevice({secure = true, permission = "default", answer = "granted", supported = true, agent = "Mozilla/5.0 (Windows NT 10.0)", installed = false} = {}) {
  const calls = [], subscribed = {current:null};
  const subscription = {endpoint:"https://fcm.googleapis.com/fcm/send/abc", toJSON:()=>({endpoint:"https://fcm.googleapis.com/fcm/send/abc",keys:{p256dh:"k",auth:"a"}}),
    unsubscribe:async()=>{subscribed.current=null; calls.push("unsubscribe");}};
  const Notification = Object.assign(function Notification() {}, {permission, requestPermission:async()=>{calls.push("permission"); Notification.permission = answer; return answer;}});
  const headers = [];
  const fetch = async (url, options = {}) => {
    calls.push(`${options.method || "GET"} ${url}`);
    if (options.method === "POST" && url === "/api/profile/notifications/push") headers.push(options.headers?.["X-CW-View-As"] ?? null);
    if (url === "/api/profile/notifications/push" && !options.method) return {ok:true,status:200,json:async()=>({public_key:"BAAA",endpoints:subscribed.current ? [subscribed.current.endpoint] : []})};
    return {ok:true,status:200,json:async()=>({ok:true})};
  };
  const app = setup(new Map(), {fetch});
  Object.assign(app.context.navigator, {userAgent:agent, maxTouchPoints:0});
  if (supported) Object.assign(app.context.navigator, {serviceWorker:{getRegistration:async()=>({pushManager:{
    getSubscription:async()=>subscribed.current,
    subscribe:async options=>{calls.push(`subscribe ${options.userVisibleOnly}`); subscribed.current = subscription; return subscription;}}})}});
  Object.assign(app.window, {isSecureContext:secure, matchMedia:()=>({matches:installed}), ...(supported ? {PushManager:function PushManager() {}, Notification} : {})});
  Object.assign(app.context, {Notification, atob:value=>Buffer.from(value, "base64").toString("binary")});
  return {app, calls, headers, push:app.window.CW.Notifications.push};
}

test("the shared push switch asks the browser first and keeps both menus in step", async () => {
  const {app, calls, push} = pushDevice();
  assert.equal((await push.status()).on, false);
  const on = await push.set(true);
  assert.equal(on.on, true);
  assert.match(on.note, /even when CrossWatch is closed/);
  assert.deepEqual(calls.filter(call => !call.startsWith("GET")), ["permission", "subscribe true", "POST /api/profile/notifications/push"]);
  assert.ok(app.dispatched.includes("cw:notification-push-changed"));
  const off = await push.set(false);
  assert.equal(off.on, false);
  assert.ok(calls.includes("DELETE /api/profile/notifications/push") && calls.includes("unsubscribe"));
});

test("turning push on while viewing as a profile sends that profile like other requests", async () => {
  const viewing = pushDevice();
  await viewing.push.set(true);
  const main = pushDevice();
  main.app.window.CW.OverviewProfile.isDefaultView = true;
  await main.push.set(true);
  const managed = pushDevice();
  managed.app.context.document.documentElement.dataset.cwRole = "user";
  await managed.push.set(true);
  assert.equal(JSON.stringify([viewing.headers, main.headers, managed.headers]), JSON.stringify([["home"], [null], [null]]));
});

test("the push switch explains why it cannot turn on", async () => {
  assert.match((await pushDevice({secure:false}).push.status()).note, /Needs HTTPS/);
  assert.equal((await pushDevice({secure:false}).push.status()).available, false);
  assert.match((await pushDevice({permission:"denied"}).push.status()).note, /blocked for this site/);
  const iphone = "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X)";
  assert.match((await pushDevice({supported:false, agent:iphone}).push.status()).note, /add CrossWatch to the Home Screen/);
  assert.match((await pushDevice({supported:false, agent:iphone, installed:true}).push.status()).note, /16\.4 or later/);
  assert.match((await pushDevice({supported:false}).push.status()).note, /^This browser does not support/);
  const refused = pushDevice({answer:"denied"});
  const result = await refused.push.set(true);
  assert.equal(result.on, false);
  assert.match(result.note, /were not allowed|blocked/);
  assert.ok(!refused.calls.some(call => call.startsWith("subscribe")));
});

function feed({hidden = false, status = 200, data = {ok:true,items:[],poll:3}} = {}) {
  const events = {}, timers = [], requests = [], received = [];
  const window = {CW:{AuthState:{user:{id:"alice"}},OverviewProfile:{id:"home"},Notifications:{receive:(value, ctx)=>received.push([value, ctx])}},
    addEventListener:(name,fn)=>{(events[name] ||= []).push(fn);}};
  const document = {hidden, addEventListener:(name,fn)=>{(events[name] ||= []).push(fn);}};
  const context = {window, document, AbortController, clearTimeout(){},
    setTimeout:(fn, ms)=>{timers.push(ms); return timers.length;},
    fetch:async url=>{requests.push(url); return {ok:status < 300,status,json:async()=>data};}};
  vm.runInNewContext(feedScript, context);
  return {window, document, events, timers, requests, received, fire:name=>events[name].forEach(fn=>fn())};
}

test("the feed asks once for the whole menu and follows the server's pace", async () => {
  const app = feed();
  for (let i = 0; i < 3; i++) app.fire("cw:notifications-refresh");
  await settle();
  assert.deepEqual(app.requests, ["/api/profile/notifications?profile=home&view_profile=home"]);
  assert.equal(JSON.stringify(app.received[0][1]), JSON.stringify({user:"alice",scope:"home"}));
  assert.deepEqual(app.timers, [3000]);
  app.fire("cw:overview-profile-changed"); await settle();
  assert.equal(app.requests.length, 2);
});

test("the feed stops for hidden tabs and refused accounts", async () => {
  const hidden = feed({hidden:true});
  await settle();
  assert.deepEqual(hidden.requests, []);
  const refused = feed({status:403});
  await settle();
  assert.equal(refused.received[0][0].denied, true);
  assert.deepEqual(refused.timers, []);
  const broken = feed({status:500});
  await settle();
  assert.equal(JSON.stringify(broken.received[0][0].errors), JSON.stringify(["Could not load notifications."]));
  assert.deepEqual(broken.timers, [10000]);
});
