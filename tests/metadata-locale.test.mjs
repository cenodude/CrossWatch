/* tests/metadata-locale.test.mjs */
/* CrossWatch - Metadata locale settings save regression tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from "node:test";
import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import vm from "node:vm";
import {catalog, registry, withI18n} from "./i18n-test-helper.mjs";

const source = readFileSync(new URL("../assets/helpers/settings-save.js", import.meta.url), "utf8");
const start = source.indexOf("    const prevMetaLocale =");
const end = source.indexOf("    try { delete cfg.app_auth; }", start);

function save(value, previous = "en-US") {
  const serverCfg = {metadata: {locale: previous, meta_cache_enable: true}};
  const cfg = structuredClone(serverCfg);
  let changed = false;
  vm.runInNewContext(source.slice(start, end), withI18n({
    cfg, serverCfg,
    _cwEl: () => value === undefined ? null : {value},
    _cwNorm: value => String(value ?? "").trim(),
    ensureObj: (object, key) => object[key] ||= {},
    mark: () => { changed = true; },
  }));
  return {cfg, changed};
}

test("clearing metadata locale sends an explicit blank for the config merge", () => {
  for (const value of ["", " \t"]) {
    const result = save(value);
    assert.equal(result.changed, true);
    assert.deepEqual(result.cfg.metadata, {locale: "", meta_cache_enable: true});
    assert.equal(save("", result.cfg.metadata.locale).changed, false);
  }
});

test("valid metadata locale tags are trimmed and saved", () => {
  for (const value of [" nl-NL ", "zh-hans", "pt-BR"]) {
    assert.equal(save(value).cfg.metadata.locale, value.trim());
  }
});

test("missing locale control preserves the saved setting", () => {
  assert.equal(save(undefined).changed, false);
  assert.equal(save(undefined).cfg.metadata.locale, "en-US");
});

test("invalid metadata locales prevent saving", () => {
  for (const value of ["en_US", "en US", "../en", "en\\US", "en-US\u200b", "abcdefgh-".repeat(8) + "en"]) {
    assert.throws(() => save(value), /valid language tag/);
  }
});

for (const {code: lang} of registry()) {
  test(`invalid locale stops all settings writes and shows the ${lang} error`, async () => {
    const writes = [];
    const messages = [];
    const context = vm.createContext(withI18n({
      window: {
        cwAppUsersSavePending: async () => writes.push("users"),
        cwAppAuthOidcDraft: () => ({}),
        cwAppAuthOidcSavePending: async () => { writes.push("oidc"); return {ok: true}; },
      },
      document: {activeElement: null, getElementById: id => id === "metadata_locale" ? {value: "en_US"} : null},
      console: {warn() {}, error() {}},
      writes, messages,
    }, lang));
    vm.runInContext(source, context);
    vm.runInContext(`
      _cwWireTouched = () => {};
      _cwWireAuthPair = () => {};
      _cwGetConfigFresh = async () => ({metadata: {locale: 'en-US'}});
      _cwSaveAppAuth = async () => { writes.push('authentication'); return true; };
      _cwSaveConfig = async () => { writes.push('config'); return {ok: true}; };
      _cwShowToast = (message, ok) => messages.push({message, ok});
    `, context);
    const expected = catalog(lang)["settings.meta.tmdb.localeInvalid"];
    assert.equal(typeof expected, "string");
    assert.ok(expected.trim());

    await assert.rejects(vm.runInContext("saveSettings()", context), {message: expected});

    assert.deepEqual(writes, []);
    assert.equal(messages.length, 1);
    assert.equal(messages[0].message, expected);
    assert.equal(messages[0].ok, false);
  });
}
