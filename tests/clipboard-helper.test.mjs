/* tests/clipboard-helper.test.mjs */
/* CrossWatch - Clipboard helper fallback order tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from "node:test";
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import vm from "node:vm";
import './i18n-vm-setup.mjs';

const source = readFileSync(new URL("../assets/helpers/clipboard.js", import.meta.url), "utf8");

function load({execResult = true, secure = true, writeText = null, write = null} = {}) {
  const calls = [];
  const listeners = [];
  const area = {
    value: "", style: {}, setAttribute() {}, focus() {}, select() {}, setSelectionRange() {},
    remove() { calls.push("remove"); },
  };
  const document = {
    activeElement: null,
    body: {appendChild() { calls.push("append"); }},
    querySelector: () => null,
    createElement: () => area,
    execCommand: (cmd) => { calls.push(`exec:${cmd}`); return execResult; },
    addEventListener(type, handler, capture) { listeners.push({type, capture}); },
  };
  const navigator = {clipboard: {}};
  if (writeText) navigator.clipboard.writeText = (text) => { calls.push(`writeText:${text}`); return writeText(text); };
  if (write) navigator.clipboard.write = (items) => { calls.push("write"); return write(items); };
  class ClipboardItem { constructor(data) { this.data = data; } }
  const window = {isSecureContext: secure, ClipboardItem};
  vm.runInNewContext(source, {window, document, navigator, Promise, Blob, String});
  return {window, calls, area, listeners};
}

test("copies synchronously through the selection first", async () => {
  const {window, calls, area} = load({writeText: async () => {}});
  assert.equal(await window.cwCopyText("https://example.test/webhook"), true);
  assert.equal(area.value, "https://example.test/webhook");
  assert.deepEqual(calls, ["append", "exec:copy", "remove"]);
});

test("falls back to the Clipboard API only when the selection copy fails", async () => {
  const {window, calls} = load({execResult: false, writeText: async () => {}});
  assert.equal(await window.cwCopyText("abc"), true);
  assert.deepEqual(calls, ["append", "exec:copy", "remove", "writeText:abc"]);
});

test("reports failure instead of pretending to copy", async () => {
  assert.equal(await load({execResult: false, secure: false, writeText: async () => {}}).window.cwCopyText("abc"), false);
  assert.equal(await load({execResult: false, writeText: async () => { throw new Error("NotAllowedError"); }}).window.cwCopyText("abc"), false);
  assert.equal(await load().window.cwCopyText(""), false);
});

test("copy field listeners run in the capture phase so modals cannot block them", () => {
  const {listeners} = load();
  assert.deepEqual(listeners, [
    {type: "mousedown", capture: true},
    {type: "focusin", capture: true},
    {type: "mouseup", capture: true},
    {type: "click", capture: true},
  ]);
});

test("pending text uses a ClipboardItem so Safari keeps the click", async () => {
  let item;
  const {window, calls} = load({write: async (items) => { item = items[0]; }});
  assert.equal(await window.cwCopyTextFrom(Promise.resolve("log text")), true);
  assert.deepEqual(calls, ["write"]);
  assert.equal(await (await item.data["text/plain"]).text(), "log text");
});
