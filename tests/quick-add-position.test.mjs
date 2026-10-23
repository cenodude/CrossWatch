/* tests/quick-add-position.test.mjs */
/* CrossWatch - Mobile Quick add drag bounds and position restoration tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';

const source = readFileSync(new URL('../assets/js/main.js', import.meta.url), 'utf8');
function setup(saved = 22) {
  const values = new Map();
  const context = {
    window: {innerHeight:800, visualViewport:{offsetTop:0,height:800}},
    document: {
      querySelector: () => ({getBoundingClientRect:()=>({bottom:72})}),
      getElementById: () => ({getBoundingClientRect:()=>({top:735,height:65})}),
      body:{classList:{contains:()=>true}},
    },
    fabBtn:{getBoundingClientRect:()=>({height:52})},
    root:{style:{setProperty:(key,value)=>values.set(key,value)},classList:{add(){}}},
    clamp:(n,min,max)=>Math.min(max,Math.max(min,n)),
    readStoredNumber:()=>saved, DESKTOP_POS_KEY:'desktop', MOBILE_POS_KEY:'mobile',
    dragState:{kind:'mobile',startX:300,startY:650,startBottom:80,pointerId:1,moved:false},
  };
  vm.createContext(context);
  vm.runInContext(source.slice(source.indexOf('    const mobilePositionBounds ='),source.indexOf('    const openModal =')) +
    source.slice(source.indexOf('    const moveDrag ='),source.indexOf('    const endDrag =')) +
    '\nglobalThis.restore=applyStoredPosition;globalThis.move=moveDrag;',context);
  return {context, bottom:()=>parseFloat(values.get('--cw-qa-mobile-bottom'))};
}

test('mobile Quick add travels beyond 120px and stops below the header and above navigation',()=>{
  const app=setup();
  app.context.move({pointerId:1,clientX:300,clientY:300});
  assert.equal(app.bottom(),430);
  app.context.move({pointerId:1,clientX:300,clientY:-100});
  assert.equal(app.bottom(),644);
  app.context.move({pointerId:1,clientX:300,clientY:900});
  assert.equal(app.bottom(),80);
});

test('saved positions survive reload and are clamped when the viewport shrinks',()=>{
  const app=setup(430);
  app.context.restore();
  assert.equal(app.bottom(),430);
  app.context.window.innerHeight=400;
  app.context.window.visualViewport.height=400;
  app.context.restore();
  assert.equal(app.bottom(),244);
});
