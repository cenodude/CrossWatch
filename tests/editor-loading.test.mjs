/* CrossWatch - Editor first-load feedback and dependency ordering */
import test from "node:test";
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import vm from "node:vm";

const core = readFileSync(new URL("../assets/helpers/core.js", import.meta.url), "utf8");
const showTabSource = core.slice(core.indexOf("  async function showTab("), core.indexOf('  window.addEventListener("hashchange"'));
function deferred() {
  let resolve, reject;
  const promise = new Promise((yes,no) => {resolve=yes; reject=no;});
  return {promise,resolve,reject};
}
function routeContext() {
  const pending=[], calls=[], attributes=new Map();
  const button={addEventListener(name,handler){this[name]=handler;}};
  const root={innerHTML:"",setAttribute:(key,value)=>attributes.set(key,value),removeAttribute:key=>attributes.delete(key),
    querySelector(selector){
      if(selector==="button") return button;
      if(selector==="[data-editor-load-error]") return this.innerHTML.includes("data-editor-load-error") ? {} : null;
      return null;
    }};
  const context=vm.createContext({window:{},document:{documentElement:{dataset:{}},dispatchEvent(){}},
    CustomEvent:class {}, console:{warn(){}}, state:{currentTab:"main",navSeq:0},
    allowedRouteTab:value=>value, writeRouteHash(){}, setTabHeaderState(){},setPageVisibility(){},
    byId:id=>id==="page-editor"?root:null,
    ensurePageModule(key){calls.push(key);if(key==="editor") return Promise.resolve();const wait=deferred();pending.push(wait);return wait.promise;},
  });
  vm.runInContext(showTabSource,context);
  return {context,root,pending,calls,button,attributes};
}

test("Editor shows loading feedback while all helpers load concurrently",async()=>{
  const {context,root,pending,calls,attributes}=routeContext();
  const loading=context.showTab("editor");
  assert.match(root.innerHTML,/Loading Editor/);
  assert.match(root.innerHTML,/cw-page-hero-editor/);
  assert.match(root.innerHTML,/cw-wrap/);
  assert.match(root.innerHTML,/cw-side/);
  assert.doesNotMatch(root.innerHTML,/cw-editor-loading-head/);
  assert.equal(attributes.get("aria-busy"),"true");
  assert.equal(pending.length,15);
  assert(!calls.includes("editor"));
  pending.slice(1).forEach(wait=>wait.resolve());
  await Promise.resolve();
  assert(!calls.includes("editor"));
  pending[0].resolve();
  await loading;
  assert.equal(calls.at(-1),"editor");
});

test("Editor initialization stops if navigation changes during helper loading",async()=>{
  const {context,pending,calls}=routeContext();
  const loading=context.showTab("editor");
  context.state.navSeq++;
  pending.forEach(wait=>wait.resolve());
  await loading;
  assert(!calls.includes("editor"));
});

test("failed Editor dependencies show a working retry on the same tab",async()=>{
  const {context,root,pending,button,attributes}=routeContext();
  const loading=context.showTab("editor");
  pending[0].reject(new Error("offline"));
  pending.slice(1).forEach(wait=>wait.resolve());
  await loading;
  assert.match(root.innerHTML,/Could not load the Editor/);
  assert.equal(attributes.has("aria-busy"),false);
  const retried=[];
  context.ensurePageModule=async key=>{retried.push(key);};
  await button.click();
  assert.equal(retried.at(-1),"editor");
});

test("provider and playlist discovery start together",async()=>{
  const context=vm.createContext({window:{},console});
  vm.runInContext(readFileSync(new URL("../assets/js/editor/sources.js",import.meta.url),"utf8"),context);
  const providers=deferred(),endpoints=deferred(),calls=[];
  const state={source:"state",snapshot:"",instance:"default"};
  const loading=context.window.CW.Editor.Sources.loadSnapshots({state,
    fetchJSON(url){calls.push(url);return url.endsWith("endpoints")?endpoints.promise:providers.promise;},
    renderInstanceOptions:()=>"default",
  });
  assert.deepEqual(calls,["/api/editor/playlists/endpoints","/api/editor/state/providers"]);
  endpoints.resolve({endpoints:[]}); providers.resolve({providers:[]});
  await loading;
  assert.equal(state.playlistEndpointsLoaded,true);
});

function rowLoadingContext() {
  const sandbox=vm.createContext({window:{},document:{getElementById:()=>null},URLSearchParams,
    console:{error(){}},setTimeout:resolve=>resolve()});
  vm.runInContext(readFileSync(new URL("../assets/js/editor/load-controller.js",import.meta.url),"utf8"),sandbox);
  const response=deferred(),requests=[],renders=[];
  let discoveries=0;
  const state={source:"state",kind:"watchlist",snapshot:"PLEX",instance:"default",
    snapshots:["PLEX"],rows:[{key:"movie",title:"Old watchlist row"}],typeFilter:{season:false,episode:false}};
  const noop=()=>{};
  const ctx={state,
    normalizeSource:value=>value,isProviderPickerSource:()=>true,isPolicySource:()=>true,
    fetchJSON(url){requests.push(url);return response.promise;},
    loadSnapshots:async()=>{discoveries++;},
    buildRows:items=>Object.entries(items).map(([key,value])=>({key,...value})),
    renderRows:()=>renders.push({loading:state.loading,keys:state.rows.map(row=>row.key)}),
    syncActionButtons:noop,showStateHint:noop,setTag:noop,setStatus:noop,syncHeaderPills:noop,
  };
  return {sandbox,ctx,state,response,requests,renders,discoveries:()=>discoveries,
    controller:sandbox.window.CW.Editor.LoadController};
}

for (const items of [{},{episode:{title:"History episode"}}]) {
  test(`Kind change applies ${Object.keys(items).length ? "populated" : "empty"} History rows once`,async()=>{
    const fixture=rowLoadingContext();
    const {sandbox,ctx,state,response,requests,renders,controller}=fixture;
    let onChange;
    Object.assign(sandbox,{state,kindSel:{value:"history",addEventListener:(_,handler)=>{onChange=handler;}},
      syncKindUI(){},syncTypeFilterUI(){},syncStateBulkUI(){},clearSelection(){},persistUIState(){},
      isProviderPickerSource:ctx.isProviderPickerSource,loadSnapshots:ctx.loadSnapshots,
      renderRows:ctx.renderRows,loadState:()=>controller.loadState(ctx)});
    const editor=readFileSync(new URL("../assets/js/editor.js",import.meta.url),"utf8");
    vm.runInContext(editor.slice(editor.indexOf("  if (kindSel) {\n",editor.indexOf('sourceSel.addEventListener("change"')),
      editor.indexOf("  if (snapSel) {",editor.indexOf('sourceSel.addEventListener("change"'))),sandbox);
    const loading=onChange();
    await new Promise(setImmediate);
    assert.equal(state.kind,"history");
    assert.equal(requests.length,1);
    assert.equal(new URL(requests[0],"http://localhost").searchParams.get("kind"),"history");
    assert.equal(renders.length,0,"old rows must not be redrawn with the new Kind");
    response.resolve({ok:true,items});
    await loading;
    assert.equal(fixture.discoveries(),1);
    assert.equal(requests.length,1);
    assert.deepEqual(renders,[{loading:false,keys:Object.keys(items)}]);
  });
}

test("refreshing an empty feature accepts the result without repeated discovery or reloads",async()=>{
  const fixture=rowLoadingContext();
  const {controller,ctx,response,state,requests,renders}=fixture;
  state.kind="progress";
  state.rows=[];
  const loading=controller.refreshEditor(ctx,{force:true});
  await new Promise(setImmediate);
  assert.equal(renders.length,1);
  assert.equal(renders[0].loading,true);
  response.resolve({ok:true,items:{}});
  await loading;
  assert.equal(requests.length,1);
  assert.equal(fixture.discoveries(),1);
  assert.equal(renders.length,2,"only loading and the completed empty view should render");
  assert.equal(state.loading,false);
});
