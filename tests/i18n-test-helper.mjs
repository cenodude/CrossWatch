/* tests/i18n-test-helper.mjs */
/* CrossWatch - Shared translation runtime, catalogs and locale bundles for frontend tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
import {execFileSync} from 'node:child_process';
import {existsSync, readFileSync, readdirSync} from 'node:fs';
import vm from 'node:vm';

const localesRoot = new URL('../assets/locales/', import.meta.url);
const runtimeSource = readFileSync(new URL('../assets/helpers/i18n.js', import.meta.url), 'utf8');
const bundleCache = new Map();

function flatten(tree, prefix, out) {
  for (const [key, value] of Object.entries(tree)) {
    const name = prefix ? `${prefix}.${key}` : key;
    if (value && typeof value === 'object') flatten(value, name, out);
    else out[name] = value;
  }
  return out;
}

export function registry() {
  return JSON.parse(readFileSync(new URL('languages.json', localesRoot), 'utf8')).languages.map(({code, name}) => ({code, name}));
}

export function catalog(lang = 'en') {
  const folder = new URL(`${lang}/`, localesRoot);
  const out = {};
  for (const file of readdirSync(folder).filter(name => name.endsWith('.json')).sort()) {
    flatten(JSON.parse(readFileSync(new URL(file, folder), 'utf8')), '', out);
  }
  return out;
}

export function localeBundle(lang = 'en') {
  if (!bundleCache.has(lang)) {
    bundleCache.set(lang, execFileSync('python', ['-c', `from cw_platform import i18n; import sys; sys.stdout.write(i18n.bundle_js(${JSON.stringify(lang)}))`], {encoding: 'utf8', cwd: new URL('..', import.meta.url), env: {...process.env, PYTHONIOENCODING: 'utf-8'}, maxBuffer: 64 * 1024 * 1024}));
  }
  return bundleCache.get(lang);
}

export function i18nHead(lang = 'en', {account = false} = {}) {
  const tags = [`<script>window.__cwLanguage={"code":"${lang}","account":${account}};</script>`, '<script src="/assets/locales/en.js"></script>'];
  if (lang !== 'en') tags.push(`<script src="/assets/locales/${lang}.js"></script>`);
  tags.push('<script src="/assets/helpers/i18n.js"></script>');
  return tags.join('');
}

export function localeRoute(path) {
  const match = /^\/assets\/locales\/([a-z]{2,3})\.js$/.exec(path);
  return match ? {status: 200, contentType: 'text/javascript', body: localeBundle(match[1])} : null;
}

function stubContext(lang, extra = {}) {
  const listeners = new Map();
  const on = (type, fn) => { listeners.set(type, [...(listeners.get(type) || []), fn]); };
  const documentElement = {lang: '', dataset: {}, getAttribute: () => lang};
  const window = {
    __cwLocales: {en: {code: 'en', messages: catalog('en')}},
    __cwLocaleMeta: {default: 'en', cookie: 'cw_lang', languages: registry()},
    __cwLanguage: {code: lang, account: false},
    addEventListener: on,
    dispatchEvent: () => true,
    location: {search: '', protocol: 'http:', reload() { window.reloaded = (window.reloaded || 0) + 1; }},
    ...extra.window,
  };
  if (lang !== 'en' && existsSync(new URL(`${lang}/`, localesRoot))) window.__cwLocales[lang] = {code: lang, messages: catalog(lang)};
  const context = {
    window,
    document: {documentElement, readyState: 'complete', cookie: '', addEventListener: on, querySelectorAll: () => [], getElementById: () => null},
    navigator: {languages: [lang], language: lang},
    location: window.location,
    localStorage: {getItem: () => null, setItem() {}},
    console: {warn: (...args) => { (context.warnings ||= []).push(args.join(' ')); }},
    URLSearchParams,
    Intl,
    CustomEvent: class { constructor(type, init) { this.type = type; this.detail = init?.detail; } },
    fetch: async () => ({ok: true, json: async () => ({ok: true})}),
    ...extra.globals,
  };
  if (extra.mutate) extra.mutate(context);
  return context;
}

export function loadRuntime(lang = 'en', extra = {}) {
  const context = stubContext(lang, extra);
  vm.createContext(context);
  vm.runInContext(runtimeSource, context);
  return {I18n: context.window.CW.I18n, context};
}

export function englishI18n() {
  return loadRuntime('en').I18n;
}

export function withI18n(context, lang = 'en') {
  const I18n = loadRuntime(lang).I18n;
  context.window ||= {};
  context.window.CW = {...(context.window.CW || {}), I18n};
  context.T = I18n.t;
  context.TH = I18n.html;
  context.TR = I18n.rich;
  return context;
}

export function installVmI18n(lang = 'en') {
  if (vm.__cwI18nInstalled) return;
  vm.__cwI18nInstalled = true;
  const I18n = loadRuntime(lang).I18n;
  const decorate = (ctx) => {
    if (!ctx || typeof ctx !== 'object') return ctx;
    if (!('T' in ctx)) ctx.T = I18n.t;
    if (!('TH' in ctx)) ctx.TH = I18n.html;
    if (!('window' in ctx)) ctx.window = {CW: {I18n}, __cwI18n: I18n};
    const win = ctx.window;
    if (win && typeof win === 'object') {
      if (!win.CW || typeof win.CW !== 'object') win.CW = {};
      if (!win.CW.I18n) win.CW.I18n = I18n;
      if (!win.__cwI18n) win.__cwI18n = I18n;
    }
    return ctx;
  };
  const createContext = vm.createContext;
  const runInNewContext = vm.runInNewContext;
  vm.createContext = (ctx = {}, ...rest) => createContext(decorate(ctx), ...rest);
  vm.runInNewContext = (code, ctx = {}, ...rest) => runInNewContext(code, decorate(ctx), ...rest);
}

export function i18nInitScript(lang = 'en', {account = false} = {}) {
  const bundles = lang === 'en' ? localeBundle('en') : localeBundle('en') + localeBundle(lang);
  return `${bundles}window.__cwLanguage={"code":${JSON.stringify(lang)},"account":${account}};${runtimeSource}`;
}

const TOKEN_RE = /\[\[t:([A-Za-z0-9_.-]+)((?:\|[A-Za-z0-9_]+[@=][^|\]]*)*)\]\]/g;

export function resolveTokens(text, lang = 'en') {
  const messages = {...catalog('en'), ...(lang === 'en' ? {} : catalog(lang))};
  const lookup = (key, values) => {
    let msg = messages[key];
    if (msg === undefined && values.count !== undefined) msg = messages[`${key}_${Number(values.count) === 1 ? 'one' : 'other'}`] ?? messages[`${key}_other`];
    if (msg === undefined) return key;
    return msg.replace(/\{\{\s*(\w+)\s*\}\}/g, (match, name) => (name in values ? String(values[name]) : match));
  };
  return text.replace(TOKEN_RE, (match, key, args) => {
    const values = {};
    for (const [, name, kind, raw] of (args || '').matchAll(/\|([A-Za-z0-9_]+)([@=])([^|\]]*)/g)) values[name] = kind === '@' ? lookup(raw, {}) : raw;
    return lookup(key, values).replace(/[&<>"]/g, (ch) => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;'}[ch]));
  });
}

export function englishFrontendSource() {
  const source = readFileSync(new URL('../ui_frontend.py', import.meta.url), 'utf8');
  const options = registry().map(({code, name}) => `<option value="${code}" lang="${code}"${code === 'en' ? ' selected' : ''}>${name}</option>`).join('');
  return resolveTokens(source.replaceAll('__CW_I18N_HEAD__', '').replaceAll('__CW_LANGUAGE_OPTIONS__', options).replaceAll('__CW_LANGUAGE_HINT__', '[[t:common.language.hintBrowser]]'));
}

export function englishSource(text) {
  const messages = catalog('en');
  const find = (key) => messages[key] ?? messages[`${key}_other`];
  const rich = (key) => find(key)?.replace(/\[\[(\/?)(b|strong|i|em|code|br)\]\]/g, (_, close, tag) => `<${close}${tag}>`);
  return String(text)
    .replace(/\$\{\s*(?:window\.)?TR\(\s*"([a-z][\w.-]*)"\s*\)\s*\}|\b(?:window\.)?TR\(\s*"([a-z][\w.-]*)"\s*\)/g, (match, a, b) => rich(a || b) ?? match)
    .replace(/(["'])\s*\+\s*(?:window\.)?TH?\(\s*"([a-z][\w.-]*)"\s*\)\s*\+\s*\1/g, (match, quote, key) => find(key) ?? match)
    .replace(/\$\{\s*(?:window\.)?TH?\(\s*"([a-z][\w.-]*)"\s*\)\s*\}/g, (match, key) => find(key) ?? match)
    .replace(/\b(?:window\.)?TH?\(\s*"([a-z][\w.-]*)"\s*\)/g, (match, key) => (find(key) === undefined ? match : JSON.stringify(find(key))))
    .replace(/\b((?:window\.)?TH?)\(\s*"([a-z][\w.-]*)"\s*,/g, (match, fn, key) => (find(key) === undefined ? match : `${fn}(${JSON.stringify(find(key))},`));
}

export function readEnglishSource(url) {
  return englishSource(readFileSync(url, 'utf8'));
}
