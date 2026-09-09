import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const core = readFileSync(new URL('../assets/helpers/core.js', import.meta.url), 'utf8');
const source = core.slice(core.indexOf('  function initMobileNavigation()'), core.indexOf('  const PROVIDER_ORDER'));

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
    contains(node) { return node === this || this.children.some(child => child.contains(node)); }
    focus() { document.activeElement = this; }
  }
  const header = new Element(), nav = new Element(), media = new EventTarget();
  header.insertBefore(nav);
  document = Object.assign(new EventTarget(), {
    documentElement: {classList: {contains: name => compact && name === 'cw-compact'}},
    querySelector: () => nav,
    createElement: () => { const node = new Element(); nodes.push(node); return node; },
    activeElement: null,
  });
  const window = Object.assign(new EventTarget(), {matchMedia: () => media});
  const context = vm.createContext({document, window, byId: id => nodes.find(node => node.id === id)});
  vm.runInContext(source + '\ninitMobileNavigation();', context);
  const toggle = nodes[0];
  const fire = (target, name, props = {}) => {
    const event = new Event(name);
    Object.entries(props).forEach(([key, value]) => Object.defineProperty(event, key, {value}));
    target.dispatchEvent(event);
  };
  return {header, nav, media, document, window, toggle, fire, context, nodes};
}

test('mobile menu opens, closes and restores focus with Escape', () => {
  const {toggle, header, nav, document, fire} = fixture();
  assert.equal(toggle.getAttribute('aria-controls'), nav.id);
  assert.equal(toggle.getAttribute('aria-expanded'), 'false');
  fire(toggle, 'click');
  assert(header.classList.contains('cw-nav-open'));
  assert.equal(toggle.getAttribute('aria-expanded'), 'true');
  document.activeElement = nav;
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
  assert.equal(nodes.length, 1);
});
