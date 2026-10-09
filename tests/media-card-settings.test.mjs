/* tests/media-card-settings.test.mjs */
/* CrossWatch - Media card settings save regression tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import vm from "node:vm";

const source = readFileSync(new URL("../assets/helpers/settings-save.js", import.meta.url), "utf8");
const baselineStart = source.indexOf("    const prevUi = {");
const baselineEnd = source.indexOf("\n    };", baselineStart) + "\n    };".length;
const saveStart = source.indexOf("    const mediaCardEl =");
const saveEnd = source.indexOf("    const protoEl =", saveStart);
const saveCode = source.slice(baselineStart, baselineEnd) + source.slice(saveStart, saveEnd);

function save(serverCfg, selection) {
  const cfg = structuredClone(serverCfg);
  let changed = false;
  vm.runInNewContext(saveCode, {
    serverCfg, cfg,
    _cwEl: () => ({ value: selection }),
    _cwNorm: (value) => String(value || "").trim(),
    normalizeUiDisplay: (value) => value || "count:3",
    ensureObj: (object, key) => object[key] ||= {},
    mark: () => { changed = true; },
  });
  return { cfg, changed };
}

for (const [before, after] of [["full", "compact"], ["compact", "full"]]) {
  test("media card saves from " + before + " to " + after + " and remains saved on reopening", () => {
    const result = save({ ui: { media_card: before } }, after);
    assert.equal(result.changed, true);
    assert.equal(result.cfg.ui.media_card, after);
    assert.equal(save(result.cfg, after).changed, false);
  });
}

test("unset media card defaults to full and can switch back after compact", () => {
  assert.equal(save({}, "full").changed, false);
  const compact = save({}, "compact");
  assert.equal(compact.cfg.ui.media_card, "compact");
  const full = save(compact.cfg, "full");
  assert.equal(full.changed, true);
  assert.equal(full.cfg.ui.media_card, "full");
});
