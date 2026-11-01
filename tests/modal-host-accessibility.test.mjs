/* tests/modal-host-accessibility.test.mjs */
/* CrossWatch - Shared modal semantics and keyboard focus regression tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import vm from "node:vm";

function harness() {
  const document = {activeElement:null,addEventListener(){},removeEventListener(){},getElementById(){return null;}};
  function element() {
    return {isConnected:true,attrs:{},style:{},dataset:{},children:[],classList:{add(){},remove(){}},
      setAttribute(k,v){this.attrs[k]=v;},appendChild(n){this.children.push(n);},
      addEventListener(k,fn){this[k]=fn;},removeEventListener(){},remove(){this.isConnected=false;},
      querySelector(){return {textContent:"Configure Connection"};},querySelectorAll(){return [];},
      focus(){document.activeElement=this;}};
  }
  document.createElement=element;
  document.body=element();
  document.activeElement=element();
  const window={addEventListener(){},removeEventListener(){}};
  const context=vm.createContext({document,window,console,URL,requestAnimationFrame(){}});
  const state=readFileSync(new URL("../assets/js/modals/core/state.js",import.meta.url),"utf8").replaceAll("export ","");
  vm.runInContext(state,context);
  let host=readFileSync(new URL("../assets/js/modals/core/host.js",import.meta.url),"utf8");
  host=host.replace(/const _cwV = [\s\S]*?export class ModalHost/,"class ModalHost");
  vm.runInContext(host+"\nthis.ModalHost=ModalHost;",context);
  return {context,document,element};
}

test("shared modal announces its title and restores its opener on close",async()=>{
  const h=harness();
  const opener=h.document.activeElement;
  const host=new h.context.ModalHost();
  await host.mount({mount(){}});
  assert.equal(host.shell.attrs.role,"dialog");
  assert.equal(host.shell.attrs["aria-modal"],"true");
  assert.equal(host.shell.attrs["aria-label"],"Configure Connection");
  assert.equal(h.document.activeElement,host.shell);
  host.unmount();
  assert.equal(h.document.activeElement,opener);
});

test("Tab and Shift Tab from the modal shell stay inside the dialog",()=>{
  const h=harness();
  const container=h.element(),first=h.element(),last=h.element();
  first.offsetParent=container;last.offsetParent=container;
  container.querySelectorAll=()=>[first,last];
  h.context.trapFocus(container);
  for(const shiftKey of [false,true]){
    h.document.activeElement=container;
    let prevented=false;
    container.keydown({key:"Tab",shiftKey,preventDefault(){prevented=true;}});
    assert.equal(h.document.activeElement,shiftKey?last:first);
    assert.equal(prevented,true);
  }
});


test("shared host respects handled Escape, native children and busy locks", async () => {
  const h=harness();
  const host=new h.context.ModalHost();
  await host.mount({mount(){}});
  const event = {key:"Escape",preventDefault(){},stopPropagation(){},stopImmediatePropagation(){}};
  host._esc({...event,defaultPrevented:true});
  assert.ok(host.shell);
  h.document.querySelector=()=>({open:true});
  host._esc(event);
  assert.ok(host.shell);
  h.document.querySelector=()=>null;
  host.setOptions({dismissible:false});
  host._esc(event);
  assert.ok(host.shell);
  host.setOptions({dismissible:true});
  host._esc(event);
  assert.equal(host.shell,null);
});
