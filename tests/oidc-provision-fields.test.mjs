// tests/oidc-provision-fields.test.mjs
// CrossWatch - OIDC new login fields visibility tests
// Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
import test from "node:test";
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import vm from "node:vm";

const source = readFileSync(new URL("../assets/helpers/settings-ui.js", import.meta.url), "utf8");
const markup = readFileSync(new URL("../ui_frontend.py", import.meta.url), "utf8");
const start = source.indexOf("function cwAppAuthOidcProvisionFields()");
const end = source.indexOf("async function cwAppAuthOidcRefreshStatus()", start);
assert.ok(start >= 0 && end > start);

function setup() {
  const fields = {hidden:true}, provision = {id:"app_auth_oidc_provision", value:"false"};
  const listeners = [];
  const elements = {app_auth_oidc_provision_fields:fields, app_auth_oidc_provision:provision};
  const document = {
    getElementById:id => elements[id] || null,
    addEventListener:(type, handler, capture) => listeners.push({type, handler, capture}),
  };
  vm.runInContext(source.slice(start, end), vm.createContext({document}));
  return {fields, provision, listener:listeners[0]};
}

test("New login fields follow the create logins select", () => {
  const {fields, provision, listener} = setup();
  listener.handler({target:provision});
  assert.equal(fields.hidden, true);
  provision.value = "true";
  listener.handler({target:provision});
  assert.equal(fields.hidden, false);
  provision.value = "false";
  listener.handler({target:provision});
  assert.equal(fields.hidden, true);
});

test("Listener catches the change event from cwSetSelectValue, which does not bubble", () => {
  const {listener} = setup();
  assert.equal(listener.type, "change");
  assert.equal(listener.capture, true);
});

test("Other selects leave the fields alone", () => {
  const {fields, provision, listener} = setup();
  provision.value = "true";
  listener.handler({target:{id:"app_auth_oidc_enabled"}});
  assert.equal(fields.hidden, true);
});

test("New login fields start hidden and hold every dependent setting", () => {
  const open = markup.indexOf('<div id="app_auth_oidc_provision_fields" hidden>');
  const close = markup.indexOf('<div class="cw-app-oidc-actions">', open);
  assert.ok(open >= 0 && close > open);
  const block = markup.slice(open, close);
  for (const id of ["provision_access", "allowed_groups", "groups_claim", "match_identities", "identity_claim"]) {
    assert.ok(block.includes(`id="app_auth_oidc_${id}"`), id);
  }
  assert.ok(!block.includes('id="app_auth_oidc_provision"'));
});
