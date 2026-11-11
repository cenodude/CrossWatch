/* tests/tracker-month-themes.test.mjs */
/* CrossWatch - Monthly calendar themes and scoped preferences */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import {themes,holidayMonths,resolveTheme,readPreferences,savePreferences,decorations} from "../assets/js/tracker-month-themes.js";

test("Automatic celebrations follow the report month and moving calendars",()=>{
  const prefs=readPreferences("calendar");
  assert.equal(resolveTheme("2026-10",prefs).id,"halloween");
  assert.equal(resolveTheme("2026-12",prefs).id,"christmas");
  assert.deepEqual(holidayMonths("easter",2024),[3]);
  assert.deepEqual(holidayMonths("easter",2026),[4]);
  assert.deepEqual(holidayMonths("carnival",2024),[2]);
  assert.deepEqual(holidayMonths("carnival",2025),[3]);
  assert.deepEqual(holidayMonths("lunar-new-year",2025),[1]);
  assert.deepEqual(holidayMonths("lunar-new-year",2026),[2]);
  assert.deepEqual(holidayMonths("eid",2026),[3,5]);
  assert.deepEqual(holidayMonths("diwali",2025),[10]);
  assert.deepEqual(holidayMonths("diwali",2026),[11]);
  assert.deepEqual(holidayMonths("diwali",2027),[10]);
  assert.deepEqual(holidayMonths("diwali",2040),[]);
  assert.deepEqual(holidayMonths("thanksgiving",2026,"canada"),[10]);
  assert.deepEqual(holidayMonths("thanksgiving",2026,"us"),[11]);
});

test("Seasons respect hemisphere and explicit monthly choices win",()=>{
  const prefs={hemisphere:"south",enabled:[],overrides:{}};
  assert.equal(resolveTheme("2026-12",prefs).id,"summer");
  assert.equal(resolveTheme("2026-06",prefs).id,"winter");
  assert.equal(resolveTheme("2026-03",prefs).id,"autumn");
  assert.equal(resolveTheme("2026-09",prefs).id,"spring");
  assert.equal(resolveTheme("2026-10",{...prefs,overrides:{"2026-10":"diwali"}}).id,"diwali");
  assert.equal(resolveTheme("2026-02",{...prefs,enabled:["lunar-new-year","valentine"]}).id,"lunar-new-year");
  assert.equal(themes.length,15);
  for(const theme of themes) {
    assert.match(decorations(theme),/aria-hidden="true"/);
    assert.match(decorations(theme,true),/viewBox="0 0 1000 180"/);
    assert.doesNotMatch(decorations(theme),/https?:|<image|<script/);
  }
});

test("Theme preferences stay separate across users, trackers and viewed profiles",()=>{
  const data=new Map();
  globalThis.localStorage={getItem:key=>data.get(key)||null,setItem:(key,value)=>data.set(key,value)};
  let auth={user:{id:"ann"},profileId:"ann",role:"user"};
  globalThis.window={CW:{AuthState:{read:()=>auth}},dispatchEvent:()=>{}};
  try {
    savePreferences("one",{enabled:[],effects:false,overrides:{"2026-12":"winter"}});
    assert.equal(readPreferences("one").effects,false);
    assert.equal(resolveTheme("2026-12",readPreferences("one")).id,"winter");
    assert.equal(readPreferences("two").effects,true);
    auth={user:{id:"bob"},profileId:"bob",role:"user"};
    assert.equal(resolveTheme("2026-12",readPreferences("one")).id,"christmas");
    auth={user:{id:"admin"},role:"admin",isAdmin:true};
    window.CW.OverviewProfile={id:"ann"};
    savePreferences("one",{effects:false});
    window.CW.OverviewProfile.id="bob";
    assert.equal(readPreferences("one").effects,true);
  } finally {delete globalThis.window; delete globalThis.localStorage;}
});
