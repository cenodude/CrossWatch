/* tests/auth-wait-screen.test.mjs */
/* CrossWatch - Shared branded waiting screen for provider approval popups */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import { readFileSync } from "node:fs";

const metaSource = readFileSync(new URL("../assets/helpers/provider-meta.js", import.meta.url), "utf8");
const sharedSource = readFileSync(new URL("../assets/auth/auth.shared.js", import.meta.url), "utf8");

function setup() {
  const timers = [];
  const popups = [];
  const makePopup = () => {
    const popup = { closed: false, opener: {}, html: "", location: { href: "about:blank" }, close() { this.closed = true; } };
    popup.document = { open() { popup.html = ""; }, write(html) { popup.html += html; }, close() {} };
    popups.push(popup);
    return popup;
  };
  const window = { CW: {}, location: { origin: "http://cw.local" }, open: () => makePopup() };
  window.window = window;
  const document = { getElementById: () => null, addEventListener() {}, querySelector: () => null, querySelectorAll: () => [], readyState: "complete", body: {} };
  const context = vm.createContext({ window, document, location: window.location, console, setTimeout: (fn, ms) => { timers.push({ fn, ms }); return timers.length; }, MutationObserver: class { observe() {} disconnect() {} } });
  vm.runInContext(metaSource, context);
  vm.runInContext(sharedSource, context);
  window.CW.connectionBrand = (key) => (key === "MOVIEPARADISE" ? { c1: "215,98,143", c2: "139,63,161" } : key === "SIMKL" ? { c1: "218,225,232", c2: "12,12,12" } : null);
  return { Shared: window.CW.AuthShared, timers, popups };
}

test("preparing screen shows the provider logo and message without navigation", () => {
  const { Shared, timers } = setup();
  const win = Shared.openWaitWindow("Movie Paradise", "Requesting a Movie Paradise code...", "MOVIEPARADISE");

  assert.equal(win.opener, null);
  assert.match(win.html, /<img src="http:\/\/cw\.local\/assets\/img\/MOVIEPARADISE\.png/);
  assert.match(win.html, /Requesting a Movie Paradise code\.\.\./);
  assert.doesNotMatch(win.html, /cw-wait-go/);
  assert.equal(timers.length, 0);
});

test("code screen uses brand colours, copy button and countdown", () => {
  const { Shared, timers } = setup();
  const win = Shared.openWaitWindow("Movie Paradise", "", "MOVIEPARADISE");

  assert.equal(Shared.showWaitCode(win, { key: "MOVIEPARADISE", label: "Movie Paradise", code: "XSWV-R42J", url: "https://movieparadise.app/oauth/device" }), true);
  assert.match(win.html, /XSWV-R42J/);
  assert.match(win.html, /Enter this code on the Movie Paradise approval page/);
  assert.match(win.html, /linear-gradient\(135deg,rgb\(215,98,143\),rgb\(139,63,161\)\)/);
  assert.match(win.html, /Copy code and continue/);
  assert.equal(timers.at(-1).ms, 6000);

  timers.at(-1).fn();
  assert.equal(win.location.href, "https://movieparadise.app/oauth/device");
});

test("code and url are escaped and a light brand gets dark button text", () => {
  const { Shared } = setup();
  const win = Shared.openWaitWindow("SIMKL", "", "SIMKL");

  Shared.showWaitCode(win, { key: "SIMKL", label: "SIMKL", code: "<b>AB12</b>", url: "https://simkl.com/pin</script><script>x()" });
  assert.match(win.html, /AB12/);
  assert.doesNotMatch(win.html, /<b>AB12/);
  assert.doesNotMatch(win.html, /<\/script><script>x\(\)/);
  assert.match(win.html, /color:#0b0d12;background:linear-gradient\(135deg,rgb\(218,225,232\),rgb\(218,225,232\)\)/);
});

test("timed navigation skips popups that continued or were cancelled", () => {
  const { Shared, timers } = setup();
  const done = Shared.openWaitWindow("Movie Paradise", "", "MOVIEPARADISE");
  Shared.showWaitCode(done, { key: "MOVIEPARADISE", label: "Movie Paradise", code: "AAAA-BBBB", url: "https://a.example/" });
  done.__cwWaitDone = true;
  timers.at(-1).fn();
  assert.equal(done.location.href, "about:blank");

  const stale = Shared.openWaitWindow("Tofa", "", "TOFA");
  Shared.showWaitCode(stale, { key: "TOFA", label: "Tofa", code: "", url: "https://b.example/", shouldContinue: () => false });
  assert.equal(timers.at(-1).ms, 3000);
  timers.at(-1).fn();
  assert.equal(stale.closed, true);
  assert.equal(stale.location.href, "about:blank");
});

test("no popup or no url reports a blocked popup", () => {
  const { Shared } = setup();
  assert.equal(Shared.showWaitCode(null, { code: "X", url: "https://a.example/" }), false);
  assert.equal(Shared.showWaitCode(Shared.openWaitWindow("Plex", "", "PLEX"), { code: "X" }), false);
});
