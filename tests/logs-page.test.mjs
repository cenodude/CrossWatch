/* tests/logs-page.test.mjs */
/* CrossWatch - Log search highlighting tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from 'node:test';
import assert from 'node:assert/strict';
globalThis.window={addEventListener(){}};
const {highlight}=await import('../assets/js/logs.js');
test('search highlights literal matches and escapes log HTML',()=>{
  assert.equal(highlight('<script>Error & ERROR</script>','error'),'&lt;script&gt;<mark>Error</mark> &amp; <mark>ERROR</mark>&lt;/script&gt;');
  assert.equal(highlight('x.* x.*','.*'),'x<mark>.*</mark> x<mark>.*</mark>');
  assert.equal(highlight('plain <text>',''),'plain &lt;text&gt;');
});
