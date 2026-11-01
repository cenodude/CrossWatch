/* tests/standalone-dialog-escape.test.mjs */
/* CrossWatch - Standalone dialog Escape and focus regression tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import vm from "node:vm";

function fixture() {
  const listeners = {};
  const classes = new Set();
  const opener = {isConnected:true, focus(){document.activeElement=this;}};
  const close = {focus(){document.activeElement=this;},addEventListener(){}};
  const modal = {dataset:{},parentNode:{},classList:{add:x=>classes.add(x),remove:x=>classes.delete(x)},
    addEventListener:(key,fn)=>{listeners[key]=fn;}};
  const document = {activeElement:opener,createElement:()=>modal,body:{appendChild(){}}};
  return {document,modal,opener,close,classes,listeners,
    $:selector=>selector.includes("close") ? close : null,
    escape() {
      let prevented=false,stopped=false;
      listeners.keydown({key:"Escape",preventDefault(){prevented=true;},stopPropagation(){stopped=true;}});
      assert.equal(prevented && stopped,true);
    }};
}

test("snapshot picker closes during loading without reopening after the request",async()=>{
  const f=fixture();
  let resolve;
  const loading=new Promise(done=>{resolve=done;});
  const source=readFileSync(new URL("../assets/js/insights.js",import.meta.url),"utf8");
  const setup=source.slice(source.indexOf("  let snapshotReturnFocus"),source.indexOf("  async function loadCrosswatchSnapshotChoices"));
  const open=source.slice(source.indexOf("  async function openCrosswatchSnapshotPicker"),source.lastIndexOf("})(window, document)"));
  const context=vm.createContext({...f,d:f.document,w:{},_cwSnapModal:null,featureLabel:x=>x,loadCrosswatchSnapshotChoices:()=>loading});
  vm.runInContext(setup+open+";this.open=openCrosswatchSnapshotPicker;",context);
  const pending=context.open("history");
  assert.equal(f.document.activeElement,f.close);
  f.escape();
  assert.equal(f.classes.has("cw-snap-hidden"),true);
  assert.equal(f.document.activeElement,f.opener);
  resolve();await pending;
  assert.equal(f.classes.has("cw-snap-hidden"),true);
});

test("activity dialog consumes Escape and restores its opener",()=>{
  const f=fixture();
  const source=readFileSync(new URL("../assets/js/activity.js",import.meta.url),"utf8");
  const setup=source.slice(source.indexOf("  function ensureModal()"),source.indexOf("  function modalParams()"));
  const open=source.slice(source.indexOf("  function openModal()"),source.indexOf("  function bindViewAllButton()"));
  const context=vm.createContext({...f,window:{},state:{},loadActivityPage(){}});
  vm.runInContext(setup+open+";this.open=openModal;",context);
  context.open();
  assert.equal(f.document.activeElement,f.close);
  f.escape();
  assert.equal(f.classes.has("hidden"),true);
  assert.equal(f.document.activeElement,f.opener);
});


test("standalone focus containment wraps both directions and handles disabled controls",()=>{
  const document={activeElement:null,addEventListener(){}};
  const handlers=[];
  const root={tabIndex:0,addEventListener:(_,fn)=>handlers.push(fn),focus(){document.activeElement=this;}};
  const control=(disabled=false,visible=true)=>({disabled,tabIndex:0,getClientRects:()=>visible?[{}]:[],closest:()=>null,focus(){document.activeElement=this;}});
  const first=control(),disabled=control(true),hidden=control(false,false),last=control();
  root.querySelectorAll=()=>[first,disabled,hidden,last];
  const unlisted=control();
  root.contains=node=>[first,disabled,hidden,last,unlisted].includes(node);
  const window={};
  vm.runInNewContext(readFileSync(new URL("../assets/helpers/dialog-focus.js",import.meta.url),"utf8"),{window,document,WeakSet});
  window.CW.DialogFocus.trap(root);
  window.CW.DialogFocus.trap(root);
  assert.equal(handlers.length,1);
  const tab=shiftKey=>{let prevented=false;handlers[0]({key:"Tab",shiftKey,preventDefault(){prevented=true;}});return prevented;};
  document.activeElement=last;
  assert.equal(tab(false),true);
  assert.equal(document.activeElement,first);
  assert.equal(tab(true),true);
  assert.equal(document.activeElement,last);
  document.activeElement=unlisted;
  assert.equal(tab(false),false);
  assert.equal(tab(true),false);
  first.disabled=true;last.disabled=true;
  assert.equal(tab(false),true);
  assert.equal(document.activeElement,root);
});
