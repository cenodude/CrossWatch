import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const core = readFileSync(new URL('../assets/helpers/account-menu.js', import.meta.url), 'utf8');
const source = core.slice(core.indexOf('  function initMobileNavigation()'), core.indexOf('  function normalizeAvatarUrl'));

function fixture(compact = false) {
  const nodes = [];
  let document;
  class Element extends EventTarget {
    constructor() { super(); this.attrs = new Map(); this.classes = new Set(); this.children = []; this.classList = {
      add: value => this.classes.add(value), remove: value => this.classes.delete(value),
      contains: value => this.classes.has(value),
    }; }
    setAttribute(key, value) { this.attrs.set(key, value); }
    getAttribute(key) { return this.attrs.get(key); }
    insertBefore(child) { this.children.push(child); child.parentElement = this; }
    appendChild(child) { this.insertBefore(child); }
    querySelectorAll() { return this.children.filter(child => child.destination); }
    contains(node) { return node === this || this.children.some(child => child.contains(node)); }
    focus() { document.activeElement = this; }
  }
  const header = new Element(), nav = new Element(), media = new EventTarget();
  header.insertBefore(nav);
  const destination = new Element(), account = new Element();
  destination.destination = true;
  nav.insertBefore(destination);
  nav.insertBefore(account);
  document = Object.assign(new EventTarget(), {
    documentElement: {classList: {contains: name => compact && name === 'cw-compact'}},
    querySelector: () => nav,
    createElement: () => { const node = new Element(); nodes.push(node); return node; },
    activeElement: null,
  });
  const window = Object.assign(new EventTarget(), {matchMedia: () => media});
  const context = vm.createContext({document, window, $: id => nodes.find(node => node.id === id)});
  vm.runInContext(source + '\ninitMobileNavigation();', context);
  const toggle = nodes.find(node => node.id === 'cw-mobile-nav-toggle');
  const panel = nodes.find(node => node.id === 'cw-mobile-navigation');
  const fire = (target, name, props = {}) => {
    const event = new Event(name);
    Object.entries(props).forEach(([key, value]) => Object.defineProperty(event, key, {value}));
    target.dispatchEvent(event);
  };
  return {header, nav, panel, destination, account, media, document, window, toggle, fire, context, nodes};
}

test('mobile menu opens, closes and restores focus with Escape', () => {
  const {toggle, header, panel, document, fire} = fixture();
  assert.equal(toggle.getAttribute('aria-controls'), panel.id);
  assert.equal(toggle.getAttribute('aria-expanded'), 'false');
  fire(toggle, 'click');
  assert(header.classList.contains('cw-nav-open'));
  assert.equal(toggle.getAttribute('aria-expanded'), 'true');
  document.activeElement = panel;
  fire(document, 'keydown', {key: 'Escape'});
  assert.equal(toggle.getAttribute('aria-expanded'), 'false');
  assert.equal(document.activeElement, toggle);
  assert(!header.classList.contains('cw-nav-open'));
});

test('compact mode does not add a mobile navigation menu', () => {
  const {nodes, header} = fixture(true);
  assert.equal(nodes.length, 0);
  assert(!header.classList.contains('cw-nav-open'));
});

test('menu keeps submenus open and closes when a destination is selected', () => {
  const {toggle, nav, fire} = fixture();
  fire(toggle, 'click');
  fire(nav, 'click', {target: {closest: () => null}});
  assert.equal(toggle.getAttribute('aria-expanded'), 'true');
  fire(nav, 'click', {target: {closest: () => ({})}});
  assert.equal(toggle.getAttribute('aria-expanded'), 'false');
});

test('outside click, route changes and desktop resizing close the mobile menu', () => {
  const {toggle, document, window, media, fire, context, nodes} = fixture();
  for (const [target, name, props] of [[document, 'click', {target: {}}], [window, 'hashchange', {}], [media, 'change', {}]]) {
    fire(toggle, 'click');
    fire(target, name, props);
    assert.equal(toggle.getAttribute('aria-expanded'), 'false');
  }
  vm.runInContext('initMobileNavigation();', context);
  assert.equal(nodes.length, 2);
});

test('navigation panel contains destinations while account controls stay outside', () => {
  const {panel, nav, destination, account} = fixture();
  assert.equal(destination.parentElement, panel);
  assert.equal(account.parentElement, nav);
  assert(!panel.contains(account));
});


test('entering compact mode returns to Home from every full UI route', () => {
  const script = readFileSync(new URL('../assets/crosswatch.js', import.meta.url), 'utf8');
  const start = script.indexOf('  if (typeof window.cwSetUiMode');
  const end = script.indexOf('  try { _cwApplyUiMode', start);
  for (const route of ['#playlists', '#editor', '#settings/scheduling', '#main']) {
    let destination;
    const saved = new Map();
    const context = vm.createContext({URL, Date, console,
      window: {location: {
        href: `https://example.test/?full=1&other=keep${route}`,
        assign: value => { destination = new URL(value); },
      }},
      localStorage: {setItem: (key, value) => saved.set(key, value)},
    });
    vm.runInContext(script.slice(start, end), context);
    context.window.cwSetUiMode('compact');
    assert.equal(destination.hash, '#main');
    assert.equal(destination.searchParams.get('ui'), 'compact');
    assert.equal(destination.searchParams.has('full'), false);
    assert.equal(destination.searchParams.get('other'), 'keep');
    assert.equal(saved.get('cw_ui_mode'), 'compact');
    context.window.cwSetUiMode('full');
    assert.equal(destination.hash, route);
    assert.equal(destination.searchParams.get('ui'), 'full');
  }
});


test('mobile mode detection respects saved choices during bootstrap and runtime', () => {
  const script = readFileSync(new URL('../assets/crosswatch.js', import.meta.url), 'utf8');
  const getter = script.slice(script.indexOf('  const _cwGetUiMode ='), script.indexOf('  const _cwApplyUiMode ='));
  const frontend = readFileSync(new URL('../ui_frontend.py', import.meta.url), 'utf8');
  const marker = frontend.indexOf('    const explicit = ui ===');
  const bootstrap = frontend.slice(frontend.lastIndexOf('(() => {', marker), frontend.indexOf('</script>', marker));
  for (const [search, saved, mobile, expected] of [
    ['', 'full', true, 'full'], ['', 'compact', false, 'compact'],
    ['', null, true, 'compact'], ['', null, false, 'full'],
    ['?ui=full', 'compact', true, 'full'], ['?ui=compact', 'full', true, 'compact'],
    ['', 'invalid', true, 'compact'], ['', 'blocked', true, 'compact'],
  ]) {
    let compact = false;
    const context = vm.createContext({URL, URLSearchParams,
      window: {location: {href: `https://example.test/${search}#settings/sync`, search}, matchMedia: () => ({matches: mobile})},
      navigator: {userAgent: mobile ? 'Android SamsungBrowser' : 'Desktop', maxTouchPoints: mobile ? 5 : 0},
      document: {documentElement: {classList: {add: () => { compact = true; }}}},
      localStorage: {getItem: () => { if (saved === 'blocked') throw new Error('Storage unavailable'); return saved; }},
      _cwShouldAutoCompact: () => mobile,
    });
    vm.runInContext(bootstrap, context);
    assert.equal(compact, expected === 'compact', `bootstrap: ${search}/${saved}`);
    assert.equal(vm.runInContext(getter + '\n_cwGetUiMode()', context), expected, `runtime: ${search}/${saved}`);
  }
});


test('profile Compact switch saves the mode and opens compact Home', () => {
  let click, destination;
  const saved = new Map();
  const context = vm.createContext({
    $: id => id === 'cw-profile-compact-switch' ? {addEventListener: (_, handler) => { click = handler; }} : null,
    localStorage: {setItem: (key, value) => saved.set(key, value)},
    window: {location: {assign: value => { destination = value; }}},
    initMobileNavigation() {}, initMobileBottomNavigation() {},
  });
  const start = core.indexOf('  function initNavigation()');
  const end = core.indexOf('  if (document.readyState', start);
  vm.runInContext(core.slice(start, end) + '\ninitNavigation();', context);
  click();
  assert.equal(saved.get('cw_ui_mode'), 'compact');
  assert.equal(destination, '/?ui=compact#main');
});


test('mobile haptics default on, respect opt-out and safely ignore unsupported or blocked APIs', () => {
  const start = core.indexOf('  function mobileNavigationFeedback()');
  const end = core.indexOf('  function initMobileBottomNavigation()', start);
  for (const mode of ['default', 'enabled', 'disabled', 'unsupported', 'blocked', 'storage-blocked']) {
    const pulses = [];
    const context = vm.createContext({
      localStorage: {getItem: () => {
        if (mode === 'storage-blocked') throw new Error('Storage unavailable');
        if (mode === 'default') return null;
        return mode === 'disabled' ? '0' : '1';
      }},
      navigator: mode === 'unsupported' ? {} : {vibrate: duration => {
        if (mode === 'blocked') throw new Error('Vibration unavailable');
        pulses.push(duration);
      }},
    });
    vm.runInContext(core.slice(start, end) + '\nmobileNavigationFeedback();', context);
    assert.deepEqual(pulses, ['default', 'enabled'].includes(mode) ? [30] : []);
  }
});
