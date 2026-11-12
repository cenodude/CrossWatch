/* tests/security-settings.test.mjs */
/* CrossWatch - Collapsed security settings validation */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from "node:test";
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import vm from "node:vm";

const source = readFileSync(new URL("../assets/helpers/settings-save.js", import.meta.url), "utf8");
const start = source.indexOf("async function _cwSaveAppAuth(serverCfg) {");
const end = source.indexOf("\n}\n", start) + 2;

for (const [password, confirmation, configured, expectedField, error] of [
  ["new-password", "different-password", true, "app_auth_password2", "Password mismatch"],
  ["short", "short", true, "app_auth_password", "Password too short"],
  ["", "", false, "app_auth_password", "Password required"],
]) {
  test(`${error} opens the password fields before focusing the invalid input`, async () => {
    const details = {open: false};
    let focused = "";
    const fields = Object.fromEntries([
      ["app_auth_username", "admin"], ["app_auth_password", password],
      ["app_auth_password2", confirmation], ["app_auth_remember_days", "30"],
    ].map(([id, value]) => [id, {
      value, closest: () => details,
      focus: () => { assert.equal(details.open, true); focused = id; },
    }]));
    const context = vm.createContext({
      _cwEl: id => fields[id], _getVal: id => fields[id]?.value || "",
      _cwNorm: value => String(value || "").trim(),
      _cwRequest: async (url) => { assert.equal(url, "/api/app-auth/status"); return {ok: true}; },
      _cwReadBody: async () => ({configured}),
      _cwSetAuthError() {}, _cwShowToast() {},
      _cwAbortSave: message => { throw new Error(message); },
    });
    vm.runInContext(source.slice(start, end), context);
    await assert.rejects(context._cwSaveAppAuth({app_auth: {enabled: true, username: "admin"}}), {message: error});
    assert.equal(focused, expectedField);
    assert.equal(fields.app_auth_password.value, password);
  });
}
