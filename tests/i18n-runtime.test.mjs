/* tests/i18n-runtime.test.mjs */
/* CrossWatch - Translation lookup, interpolation, plurals, fallback, escaping and catalog load failures */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import test from 'node:test';
import assert from 'node:assert/strict';
import {catalog, loadRuntime} from './i18n-test-helper.mjs';

const en = catalog('en');
const nl = catalog('nl');

test('English is the default language and returns catalog text', () => {
  const {I18n, context} = loadRuntime('en');
  assert.equal(I18n.lang, 'en');
  assert.equal(context.document.documentElement.lang, 'en');
  assert.equal(I18n.t('common.actions.save'), en['common.actions.save']);
});

test('Dutch text is used when the Dutch catalog loaded', () => {
  const {I18n, context} = loadRuntime('nl');
  assert.equal(I18n.lang, 'nl');
  assert.equal(context.document.documentElement.lang, 'nl');
  assert.equal(I18n.t('common.actions.save'), nl['common.actions.save']);
  assert.notEqual(I18n.t('common.actions.save'), en['common.actions.save']);
});

for (const lang of ['vi', 'zh-hans', 'zh-hant']) {
  test(`${lang} translates navigation and common settings without English fallback`, () => {
    const {I18n, context} = loadRuntime(lang);
    const messages = catalog(lang);
    assert.equal(I18n.lang, lang);
    assert.equal(context.document.documentElement.lang, lang);
    for (const key of [
      'navigation.tabs.tracker', 'navigation.tabs.hub', 'navigation.settingsMenu.scrobbler',
      'dashboard.syncHub.title', 'dashboard.details.tabs.watcher',
      'settings.ui.appearance.flatDark', 'settings.ui.appearance.flatLight',
      'settings.security.oidc.clientSecret', 'settings.security.oidc.scopes',
      'profile.manager.claim.subject', 'editor.pulse',
    ]) {
      assert.equal(I18n.t(key), messages[key], key);
      assert.notEqual(I18n.t(key), en[key], key);
    }
    assert.deepEqual([...I18n.report().fallbacks], []);
    assert.deepEqual([...I18n.report().missing], []);
  });
}

test('named interpolation replaces known values and keeps unknown placeholders', () => {
  const {I18n} = loadRuntime('en');
  assert.equal(I18n.t('profile.records.refresh', {title: 'History'}), 'Refresh History');
  assert.equal(I18n.t('profile.records.refresh'), 'Refresh {{title}}');
});

test('plural forms follow the locale rules in both languages', () => {
  const english = loadRuntime('en').I18n;
  const dutch = loadRuntime('nl').I18n;
  assert.equal(english.t('auth.login.rememberHint', {count: 1}), 'Keep this browser signed in for up to 1 day.');
  assert.equal(english.t('auth.login.rememberHint', {count: 30}), 'Keep this browser signed in for up to 30 days.');
  assert.equal(dutch.t('auth.login.rememberHint', {count: 1}), 'Houd deze browser maximaal 1 dag ingelogd.');
  assert.equal(dutch.t('auth.login.rememberHint', {count: 30}), 'Houd deze browser maximaal 30 dagen ingelogd.');
});

test('numbers in interpolation use the selected language', () => {
  assert.equal(loadRuntime('en').I18n.t('auth.login.rememberHint', {count: 1200}), 'Keep this browser signed in for up to 1,200 days.');
  assert.equal(loadRuntime('nl').I18n.t('auth.login.rememberHint', {count: 1200}), 'Houd deze browser maximaal 1.200 dagen ingelogd.');
});

test('a key missing in Dutch falls back to English', () => {
  const {I18n, context} = loadRuntime('nl', {mutate: (ctx) => { delete ctx.window.__cwLocales.nl.messages['common.actions.save']; }});
  assert.equal(I18n.t('common.actions.save'), en['common.actions.save']);
  assert.deepEqual([...I18n.report().fallbacks], ['common.actions.save']);
  assert.equal(context.warnings, undefined);
});

test('a key missing everywhere returns the key and warns once', () => {
  const {I18n, context} = loadRuntime('en');
  assert.equal(I18n.t('common.does.not.exist'), 'common.does.not.exist');
  I18n.t('common.does.not.exist');
  assert.equal(context.warnings.length, 1);
  assert.match(context.warnings[0], /missing key: common\.does\.not\.exist/);
  assert.deepEqual([...I18n.report().missing], ['common.does.not.exist']);
});

test('html() escapes interpolated values and catalog text', () => {
  const {I18n} = loadRuntime('en');
  const out = I18n.html('profile.records.refresh', {title: '<img src=x onerror="alert(1)">'});
  assert.equal(out, 'Refresh &lt;img src=x onerror=&quot;alert(1)&quot;&gt;');
  assert.equal(I18n.t('profile.records.refresh', {title: '<b>'}), 'Refresh <b>');
});

test('a failed Dutch catalog request falls back to English', () => {
  const {I18n, context} = loadRuntime('nl', {mutate: (ctx) => { ctx.window.__cwLocaleFailed = ['nl']; }});
  assert.equal(I18n.lang, 'en');
  assert.equal(I18n.requested, 'nl');
  assert.equal(context.document.documentElement.lang, 'en');
  assert.equal(I18n.t('common.actions.save'), en['common.actions.save']);
});

test('a missing Dutch bundle falls back to English without throwing', () => {
  const {I18n} = loadRuntime('nl', {mutate: (ctx) => { delete ctx.window.__cwLocales.nl; }});
  assert.equal(I18n.lang, 'en');
  assert.equal(I18n.t('common.actions.close'), en['common.actions.close']);
});

test('no catalogs at all still yields a working runtime', () => {
  const {I18n} = loadRuntime('en', {mutate: (ctx) => { ctx.window.__cwLocales = {}; ctx.window.__cwLocaleMeta = undefined; }});
  assert.equal(I18n.lang, 'en');
  assert.equal(I18n.t('common.actions.save'), 'common.actions.save');
  assert.equal(I18n.html('common.actions.save'), 'common.actions.save');
});

test('an unknown requested language is not used', () => {
  const {I18n} = loadRuntime('fr');
  assert.equal(I18n.lang, 'en');
});

test('setLanguage stores a browser choice without an account and rejects unknown codes', async () => {
  const {I18n, context} = loadRuntime('en');
  await I18n.setLanguage('nl', {reload: false});
  assert.match(context.document.cookie, /^cw_lang=nl; Path=\/; Max-Age=31536000; SameSite=Lax$/);
  await assert.rejects(I18n.setLanguage('xx', {reload: false}));
});

test('setLanguage writes the account preference when signed in', async () => {
  const calls = [];
  const {I18n, context} = loadRuntime('en', {globals: {fetch: async (url, init) => { calls.push([url, JSON.parse(init.body)]); return {ok: true}; }}});
  await I18n.setLanguage('nl', {account: true, reload: false});
  assert.deepEqual(calls, [['/api/profile', {preferences: {language: 'nl'}}]]);
  assert.equal(context.document.cookie, '');
});

test('a failed account save reports an error and does not reload', async () => {
  const {I18n, context} = loadRuntime('en', {globals: {fetch: async () => ({ok: false})}});
  await assert.rejects(I18n.setLanguage('nl', {account: true}), {message: en['common.language.saveFailed']});
  assert.equal(context.window.reloaded, undefined);
});

test('a language change waits while there is unsaved work', async () => {
  const {I18n, context} = loadRuntime('en');
  const release = I18n.registerUnsavedCheck(() => true);
  await I18n.setLanguage('nl');
  assert.equal(context.window.reloaded, undefined);
  release();
  await I18n.setLanguage('nl');
  assert.equal(context.window.reloaded, 1);
});

test('dates, relative times and lists use the selected language', () => {
  const english = loadRuntime('en').I18n;
  const dutch = loadRuntime('nl').I18n;
  const when = Date.UTC(2026, 0, 15, 12, 0, 0);
  assert.match(english.date(when, {month: 'long', timeZone: 'UTC'}), /January/);
  assert.match(dutch.date(when, {month: 'long', timeZone: 'UTC'}), /januari/);
  assert.equal(dutch.relative(Date.now() - 3 * 864e5), 'drie dagen geleden'.replace('drie', '3'));
  assert.equal(english.list(['a', 'b', 'c']), 'a, b, and c');
  assert.equal(dutch.list(['a', 'b', 'c']), 'a, b en c');
  assert.equal(dutch.relative(Date.now() - 1000), nl['common.time.justNow']);
});

test('rich text keeps only bold, emphasis, code and line breaks and escapes everything else', () => {
  const {I18n} = loadRuntime('en');
  assert.equal(I18n.rich('about.motto'), 'Your media. Your data.<br>On your terms.');
  assert.match(I18n.rich('modals.upgrade.refreshHelp'), /<b>Ctrl \+ F5<\/b>/);
  assert.equal(I18n.rich('modals.topology.activePairs', {count: 2}), '<strong>2</strong> active pairs');
  assert.equal(I18n.rich('connections.auth.connectedAs', {name: '<img src=x>'}), 'Connected as &lt;img src=x&gt;');
});

test('rich text follows the selected language', () => {
  const {I18n} = loadRuntime('nl');
  assert.equal(I18n.rich('modals.topology.activePairs', {count: 1}), '<strong>1</strong> actief paar');
});
