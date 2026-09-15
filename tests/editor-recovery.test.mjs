/* tests/editor-recovery.test.mjs */
/* CrossWatch - Editor recovery selection tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from "node:test";
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import vm from "node:vm";

const editor = readFileSync(new URL("../assets/js/editor.js",import.meta.url),"utf8");
const table = readFileSync(new URL("../assets/js/editor/table-controller.js",import.meta.url),"utf8");
const handoff = editor.slice(editor.indexOf("  async function openRecoveredHistory()"),editor.indexOf('  window.addEventListener("cw:open-recovery"'));

test("recovery no longer hides rows behind a separate view",()=>{
  assert.doesNotMatch(editor,/Showing recovered history/);
  assert.doesNotMatch(editor,/recoverySelection/);
  assert.doesNotMatch(table,/recoverySelection/);
});

function fixture({fallback=false}={}) {
  const state={rows:[],typeFilter:{movie:true,episode:true},selected:new Set()};
  const receipt={instance:"recovered",keys:["tmdb:1@100"]};
  const messages=[];
  const noop=()=>{};
  const sandbox=vm.createContext({state,window:{CW:{pendingRecovery:receipt}},recoveryReady:true,
    filterInput:{},syncSourceUI:noop,syncTypeFilterUI:noop,
    async loadSnapshots(){if(fallback)state.instance="other";},
    async loadState(){state.rows=[{key:"tmdb:1@100",_rid:1},{key:"tmdb:1@200",_rid:2},{key:"tmdb:2@100",_rid:3}];},
    renderRows:noop,syncBulkBar:noop,setStatus:message=>messages.push(message)});
  vm.runInContext(handoff,sandbox);
  return {sandbox,state,messages};
}

test("Editor handoff selects only imported watch events and keeps full tracker rows",async()=>{
  const {sandbox,state,messages}=fixture();
  await sandbox.openRecoveredHistory();
  assert.equal(state.snapshot,"CROSSWATCH");
  assert.equal(state.instance,"recovered");
  assert.equal(state.kind,"history");
  assert.deepEqual(Array.from(state.selected),[1]);
  assert.equal(state.rows.length,3);
  assert.match(messages.at(-1),/^1 recovered history row selected/);
});

test("Editor handoff never selects matching keys from a substituted profile",async()=>{
  const {sandbox,state,messages}=fixture({fallback:true});
  await sandbox.openRecoveredHistory();
  assert.equal(state.selected.size,0);
  assert.match(messages.at(-1),/profile is unavailable/);
});
