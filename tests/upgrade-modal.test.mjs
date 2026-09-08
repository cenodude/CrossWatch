import test from "node:test";
import assert from "node:assert/strict";
import {readFile} from "node:fs/promises";
import vm from "node:vm";
import * as notes from "../assets/js/modals/upgrade-warning/notes.js";

const source = (await readFile(new URL("../assets/js/modals/upgrade-warning/index.js", import.meta.url), "utf8"))
  .replace(/const \{[\s\S]*?\} = await import\([^;]+\);/g, "")
  .replaceAll("import.meta.url", '"https://example.com/modal.js"')
  .replace("export default", "globalThis.modal =");

async function setup({body = "", version = "0.12.0", config = "0.11.7", auth = true, notesFail = false} = {}) {
  let resolveMigration, rejectMigration, closed = false, dismissible = false;
  const migration = new Promise((resolve, reject) => { resolveMigration = resolve; rejectMigration = reject; });
  const buttons = new Map(), calls = [];
  const host = {
    innerHTML: "", closest: () => ({style:{}}),
    querySelector: selector => {
      if (!buttons.has(selector)) buttons.set(selector, {addEventListener: (_, fn) => { buttons.get(selector).click = fn; }, focus() {}});
      return buttons.get(selector);
    },
  };
  const context = vm.createContext({
    ...notes, URL, console: {warn() {}}, setTimeout() {},
    escapeHtml: text => String(text ?? "").replaceAll("<", "&lt;").replaceAll(">", "&gt;"),
    window: {__CW_VERSION__:version, notify() {}, cxCloseModal: () => { closed = true; }},
    fetchAppAuthStatus: async () => ({enabled:auth, configured:auth, authenticated:auth}),
    hasEnabledAppAuth: status => status.enabled && status.configured,
    setModalDismissible: value => { dismissible = value; }, setModalShellInline() {},
    renderAppAuthFields: () => "Credential fields", wireLiveAppAuthValidation() {},
    getJson: async url => { calls.push(url); if (notesFail) throw Error("offline"); return {version, body, html_url:"https://github.com/cenodude/CrossWatch/releases/tag/v0.12.0"}; },
    postJson: url => { calls.push(url); return migration; },
  });
  vm.runInContext(source, context);
  await context.modal.mount(host, {current_version:version, config_version:config});
  await new Promise(resolve => setImmediate(resolve));
  return {
    host, calls, resolveMigration, rejectMigration,
    click: name => buttons.get(`[data-x="${name}"]`).click(),
    get closed() { return closed; }, get dismissible() { return dismissible; },
    settle: () => new Promise(resolve => setImmediate(resolve)),
  };
}

test("saving cannot be dismissed; success shows only confirmed migration facts", async () => {
  const app = await setup();
  assert.equal(app.dismissible, false);
  assert.match(app.host.innerHTML, /data-x="continue" disabled/);
  assert.doesNotMatch(app.host.innerHTML, /Update completed successfully/);
  app.resolveMigration({ok:true, backup:"/config/backups/pre-upgrade.zip"});
  await app.settle();
  assert.equal(app.dismissible, true);
  assert.match(app.host.innerHTML, /Update completed successfully/);
  assert.match(app.host.innerHTML, /Backup created: <code>pre-upgrade.zip<\/code>/);
  assert.doesNotMatch(app.host.innerHTML, /\/config\/backups/);
  app.click("continue");
  assert.equal(app.closed, true);
});

test("missing backup and compatibility adjustments are reported honestly", async () => {
  const app = await setup();
  app.resolveMigration({ok:true, backup:null, forced_paths:["sync.mode"]});
  await app.settle();
  assert.match(app.host.innerHTML, /No backup was reported/);
  assert.match(app.host.innerHTML, /compatibility adjustments/);
  assert.doesNotMatch(app.host.innerHTML, /Existing settings retained|No manual action required/);
});

test("failed migration stays visible with Continue disabled and Close available", async () => {
  const app = await setup();
  app.rejectMigration(Error("save failed"));
  await app.settle();
  assert.match(app.host.innerHTML, /Configuration update failed/);
  assert.match(app.host.innerHTML, /data-x="continue" disabled/);
  assert.doesNotMatch(app.host.innerHTML, /data-x="close" disabled|Update completed successfully/);
  app.click("close");
  assert.equal(app.closed, true);
});

test("five highlights expand to all; upgrade notes remain visible separately", async () => {
  const body = "**Upgrade note:**\nThe next sync will rebuild baselines.\n## ✨ Highlights\n" +
    Array.from({length:7}, (_, i) => `- Change ${i + 1}\n  - Detail ${i + 1}`).join("\n") + "\n## Fixes\n- A fix";
  const app = await setup({body});
  app.resolveMigration({ok:true});
  await app.settle();
  assert.match(app.host.innerHTML, /The next sync will rebuild baselines/);
  assert.match(app.host.innerHTML, /Review the upgrade note below/);
  assert.match(app.host.innerHTML, /<details><summary>Change 1<\/summary>/);
  assert.match(app.host.innerHTML, /Change 5/);
  assert.doesNotMatch(app.host.innerHTML, /Change 6|A fix/);
  app.click("highlights");
  assert.match(app.host.innerHTML, /Change 7/);
  assert.match(app.host.innerHTML, /aria-expanded="true"/);
  app.click("highlights");
  assert.doesNotMatch(app.host.innerHTML, /Change 6/);
  assert.equal(app.calls.filter(url => url === "/api/version/release-notes").length, 1);
});

test("unavailable release notes do not block successful migration", async () => {
  const app = await setup({notesFail:true});
  app.resolveMigration({ok:true});
  await app.settle();
  assert.match(app.host.innerHTML, /Highlights are unavailable/);
  assert.match(app.host.innerHTML, /View full release notes/);
  assert.doesNotMatch(app.host.innerHTML, /data-x="continue" disabled/);
});

test("legacy reset and required sign-in flows do not auto-migrate", async () => {
  const legacy = await setup({config:"0.8.0"});
  assert.match(legacy.host.innerHTML, /Clean reset required/);
  assert.equal(legacy.dismissible, false);
  assert.equal(legacy.calls.length, 0);
  const signIn = await setup({auth:false});
  assert.match(signIn.host.innerHTML, /Migration now requires admin credentials/);
  assert.equal(signIn.dismissible, false);
  assert.equal(signIn.calls.length, 0);
});


test("optional Wiki updates render as safe compact links alongside release highlights", async () => {
  const app = await setup({body:"## Highlights\n- New feature\n## Updated Wiki\nPlaylists - [https://wiki.crosswatch.app/playlists](https://wiki.crosswatch.app/playlists)\n<img src=x> - [bad](https://wiki.crosswatch.app/cli)\n[unsafe](javascript:alert)"});
  assert.match(app.host.innerHTML, /aria-label="Updated Wiki"/);
  assert.match(app.host.innerHTML, /href="https:\/\/wiki.crosswatch.app\/playlists"/);
  assert.match(app.host.innerHTML, /menu_book<\/span>Playlists/);
  assert.match(app.host.innerHTML, /&lt;img src=x&gt;/);
  assert.doesNotMatch(app.host.innerHTML, /<img src=x>|javascript:alert/);
  const withoutWiki = await setup({body:"## Highlights\n- New feature"});
  assert.doesNotMatch(withoutWiki.host.innerHTML, /aria-label="Updated Wiki"/);
});
