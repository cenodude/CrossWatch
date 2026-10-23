/* tests/insights-html.test.mjs */
/* CrossWatch - Insights rendering escapes feature and history text */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';

const source = readFileSync(new URL('../assets/js/insights.js', import.meta.url), 'utf8');
test('history renders markup in feature labels and results as text', () => {
  const payload = '<img src=x onerror="alert(1)">';
  const list = {innerHTML:''};
  const wrap = {dataset:{listInit:'1'}};
  const context = {
    $: selector => selector === '.list' ? list : wrap,
    window:{CW:{}}, _feature:payload,
    recentSyncsDisplay:()=>({mode:'count',limit:10}),
    totalsFor:()=>({sum:0,a:0,r:0,u:0}),
    fmtWhen:()=>payload, fmtDur:()=>payload, featureLabel:()=>payload,
  };
  vm.createContext(context);
  const esc = source.split('\n').find(line=>line.includes('const esc ='));
  const render = source.slice(source.indexOf('  function renderHistoryTabs('),source.indexOf('  function renderTopStats('));
  vm.runInContext(esc + '\n' + render + '\nglobalThis.render = renderHistoryTabs;',context);
  context.render([{result:payload}]);
  assert.ok(!list.innerHTML.includes('<img'));
  assert.equal((list.innerHTML.match(/&lt;img/g)||[]).length,4);
  assert.match(list.innerHTML,/&quot;alert\(1\)&quot;/);
});
