/* tests/emby-user-settings.test.mjs */
/* CrossWatch - Emby selected user settings regression tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import vm from "node:vm";

const authSource = readFileSync(new URL("../assets/auth/auth.emby.js", import.meta.url), "utf8");
const saveSource = readFileSync(new URL("../assets/helpers/settings-save.js", import.meta.url), "utf8");

function setup(instance, unrelatedChange = false) {
  const fields = Object.fromEntries(Object.entries({
    emby_instance: instance,
    emby_username: "Admin",
    emby_user: "Admin",
    emby_user_id: "admin-id",
    emby_server_url: "http://emby.test/",
    app_auth_username: "operator",
    ...(unrelatedChange ? { debug: "on" } : {}),
  }).map(([id, value]) => [id, {
    value, dataset: {}, classList: { remove() {} }, addEventListener() {},
  }]));
  const admin = {
    server: "http://emby.test/", user: "Admin", username: "Admin",
    user_id: "admin-id", access_token: "test-token", verify_ssl: true,
    history: { libraries: ["movies"] },
  };
  let saved = {
    app_auth: { enabled: true, username: "operator" },
    emby: { ...structuredClone(admin), instances: { secondary: structuredClone(admin) } },
  };
  const original = structuredClone(saved.emby);
  const writes = [];
  let picker;
  const context = {
    console, queueMicrotask,
    setTimeout() {}, clearTimeout() {},
    CustomEvent: class {}, Event: class {},
    localStorage: { getItem() { return null; } },
    MutationObserver: class { observe() {} },
    document: {
      readyState: "loading",
      activeElement: { closest() { return true; } },
      getElementById(id) { return fields[id] || null; },
      querySelector(selector) { return fields[selector.slice(1)] || null; },
      querySelectorAll() { return []; },
      addEventListener() {}, dispatchEvent() {},
    },
    dispatchEvent() {},
    cwMediaUserPicker: { open(options) { picker = options; } },
    CW: {
      API: {
        Config: {
          load: async () => structuredClone(saved),
          save: async cfg => { saved = { ...structuredClone(cfg), app_auth: saved.app_auth }; writes.push(saved); },
        },
        f: async () => ({
          ok: true, headers: { get: () => "application/json" },
          json: async () => ({ enabled: true, configured: true }),
        }),
      },
      AuthShared: {
        createProfileAdapter: () => ({
          getInstance: () => instance,
          cfgBlock: cfg => instance === "default" ? cfg.emby : cfg.emby.instances[instance],
        }),
      },
    },
  };
  context.window = context;
  vm.createContext(context);
  vm.runInContext(authSource, context);
  vm.runInContext(saveSource, context);
  return {
    context, fields, writes, original,
    selected: () => instance === "default" ? saved.emby : saved.emby.instances[instance],
    root: () => saved.emby,
    async pick() {
      await context.embyPickUser();
      assert.equal(picker.instance, instance);
      picker.onPick({ id: "child-id", name: "Child" });
    },
  };
}

for (const instance of ["default", "secondary"]) {
  for (const unrelatedChange of [false, true]) {
    test(`picked user persists for ${instance}, unrelated change: ${unrelatedChange}`, async () => {
      const app = setup(instance, unrelatedChange);
      await app.pick();
      await app.context.saveSettings();
      assert.equal(app.writes.length, 1);
      assert.equal(app.selected().user_id, "child-id");
      assert.equal(app.selected().user, "Child");
      assert.equal(app.selected().username, "Child");
      assert.equal(app.selected().access_token, "test-token");
      assert.equal(app.selected().verify_ssl, true);
      assert.deepEqual(app.selected().history.libraries, ["movies"]);
      if (instance === "secondary") {
        const { instances, ...defaultProfile } = app.root();
        const { instances: originalInstances, ...originalDefault } = app.original;
        assert.deepEqual(defaultProfile, originalDefault);
      } else {
        assert.deepEqual(app.root().instances, app.original.instances);
      }
      await app.context.saveSettings();
      assert.equal(app.writes.length, 1);
    });
  }
}

test("saving another setting with absent Emby inputs preserves the profile", async () => {
  const app = setup("secondary", true);
  for (const key of Object.keys(app.fields)) {
    if (key.startsWith("emby_") && key !== "emby_instance") delete app.fields[key];
  }
  await app.context.saveSettings();
  assert.equal(app.writes.length, 1);
  assert.deepEqual(app.root(), app.original);
});

test("saving the selected name repairs a stale user alias", async () => {
  const app = setup("secondary");
  app.selected().username = "Child";
  await app.pick();
  await app.context.saveSettings();
  assert.equal(app.selected().user, "Child");
  assert.equal(app.selected().username, "Child");
});

test("Emby server and SSL edits persist in the selected profile", async () => {
  const app = setup("secondary");
  app.fields.emby_server_url.value = "https://emby.test/";
  app.fields.emby_verify_ssl = { checked: false };
  await app.context.saveSettings();
  assert.equal(app.writes.length, 1);
  assert.equal(app.selected().server, "https://emby.test/");
  assert.equal(app.selected().verify_ssl, false);
  assert.equal(app.root().server, "http://emby.test/");
  assert.equal(app.root().verify_ssl, true);
});
