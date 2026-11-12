/* tests/settings-switches.test.mjs */
/* CrossWatch - Interface switch hydration and save regression tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from "node:test";
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import vm from "node:vm";

const uiSource = readFileSync(new URL("../assets/helpers/settings-ui.js", import.meta.url), "utf8");
const saveSource = readFileSync(new URL("../assets/helpers/settings-save.js", import.meta.url), "utf8");
const markup = readFileSync(new URL("../ui_frontend.py", import.meta.url), "utf8");
const ids = [...markup.matchAll(/<input id="(ui_show_[^"]+)"[^>]+role="switch"/g)].map(match => match[1]);
const helperStart = uiSource.indexOf("  const _setChecked =");
const helperEnd = uiSource.indexOf("\n  };", helperStart) + "\n  };".length;
const hydrateStart = uiSource.indexOf("  (function () {\n    const ui =");
const hydrateEnd = uiSource.indexOf("\n  })();", hydrateStart) + "\n  })();".length;
const baselineStart = saveSource.indexOf("    const prevUi = {");
const baselineEnd = saveSource.indexOf("\n    };", baselineStart) + "\n    };".length;
const saveStart = saveSource.indexOf('    [["ui_show_watchlist_preview"');
const saveEnd = saveSource.indexOf('    [["ui_recent_activity_display"', saveStart);

function hydrate(cfg, elements) {
  vm.runInNewContext(uiSource.slice(helperStart, helperEnd) + uiSource.slice(hydrateStart, hydrateEnd), {
    cfg, document: {getElementById: id => elements[id]}, window: {},
    _setSelectValue() {}, _cwTrustedProxiesEl: () => null,
  });
}

function save(serverCfg, elements) {
  const cfg = structuredClone(serverCfg);
  let changed = false;
  vm.runInNewContext(saveSource.slice(baselineStart, baselineEnd) + saveSource.slice(saveStart, saveEnd), {
    cfg, serverCfg, _cwEl: id => elements[id],
    _cwNorm: value => String(value || "").trim(),
    normalizeUiDisplay: value => value || "count:3",
    ensureObj: (object, key) => object[key] ||= {},
    mark: () => { changed = true; },
  });
  return {cfg, changed};
}

test("interface switches hydrate defaults without creating pending changes", () => {
  assert.equal(ids.length, 10);
  assert.equal(new Set(ids).size, 10);
  const elements = Object.fromEntries(ids.map(id => [id, {checked: false}]));
  hydrate({}, elements);
  for (const id of ids) {
    assert.equal(elements[id].checked, !["ui_show_recent_progress_widget", "ui_show_recent_playlists_widget"].includes(id), id);
  }
  assert.equal(save({}, elements).changed, false);
});

for (const checked of [true, false]) {
  test(`every interface switch saves ${checked} and stays that way after reloading`, () => {
    const serverCfg = {ui: Object.fromEntries(ids.map(id => [id.slice(3), !checked]))};
    serverCfg.ui.theme = "original";
    const elements = Object.fromEntries(ids.map(id => [id, {checked, value: String(!checked)}]));
    const result = save(serverCfg, elements);
    assert.equal(result.changed, true);
    assert.equal(result.cfg.ui.theme, "original");
    const reopened = Object.fromEntries(ids.map(id => [id, {checked: !checked}]));
    hydrate(result.cfg, reopened);
    for (const id of ids) assert.equal(reopened[id].checked, checked, id);
    assert.equal(save(result.cfg, reopened).changed, false);
  });
}

test("unavailable switches leave stored preferences unchanged", () => {
  const serverCfg = {ui: {show_recent_progress_widget: true, show_watchlist_preview: false}};
  const result = save(serverCfg, {});
  assert.equal(result.changed, false);
  assert.deepEqual(result.cfg, serverCfg);
});
