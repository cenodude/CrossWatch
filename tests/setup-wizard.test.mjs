import test from "node:test";
import assert from "node:assert/strict";
import {readFile} from "node:fs/promises";
import vm from "node:vm";
import * as auth from "../assets/js/modals/core/app-auth-setup.js";

const source = (await readFile(new URL("../assets/js/modals/setup-wizard/index.js", import.meta.url), "utf8"))
  .replace(/const \{[\s\S]*?\} = await import\([^;]+\);/, "")
  .replaceAll("import.meta.url", '"https://example.com/setup.js"')
  .replace("export default", "globalThis.modal =");

async function setup(recovery = false) {
  const nodes = new Map(), created = [], calls = [], navigation = [];
  let closed = false, dismissible, resolveSave, rejectSave;
  const saved = new Promise((resolve, reject) => { resolveSave = resolve; rejectSave = reject; });
  function element() {
    const classes = new Set();
    return {
      value:"", type:"", disabled:false, dataset:{}, attributes:{}, listeners:{},
      firstElementChild:{textContent:""}, textContent:"",
      classList:{add: c => classes.add(c), toggle: (c, flag) => flag ? classes.add(c) : classes.delete(c)},
      setAttribute(key, value) { this.attributes[key] = value; },
      addEventListener(event, handler) { this.listeners[event] = handler; },
      focus() {}, before() {}, append() {},
    };
  }
  const host = {
    html:"", closest: () => ({style:{}}),
    get innerHTML() { return this.html; },
    set innerHTML(html) {
      this.html = html;
      nodes.clear();
      for (const match of html.matchAll(/<input\b([^>]+)>/g)) {
        const attrs = Object.fromEntries([...match[1].matchAll(/([\w-]+)="([^"]*)"/g)].map(m => [m[1], m[2]]));
        const node = element();
        node.value = attrs.value || "";
        node.type = attrs.type;
        nodes.set(`#${attrs.id}`, node);
        nodes.set(`[data-field="${attrs["data-field"]}"]`, node);
      }
      for (const match of html.matchAll(/<button\b([^>]*data-x="([^"]+)"[^>]*)>/g)) {
        const node = element();
        node.disabled = match[1].includes(" disabled");
        nodes.set(`[data-x="${match[2]}"]`, node);
      }
      nodes.set("#sw-auth-error", element());
    },
    querySelector: selector => nodes.get(selector) || null,
  };
  const context = vm.createContext({
    ...auth, URL,
    document: {getElementById: () => null, createElement: () => { const node = element(); created.push(node); return node; }},
    window: {__CW_VERSION__:"0.12.0", notify() {}, cxCloseModal: () => {closed = true;}, showTab: tab => navigation.push(tab), cwSettingsSelect: tab => navigation.push(tab)},
    setTimeout: handler => handler(),
    setModalDismissible: flag => {dismissible = flag;},
    saveRequiredAppAuth: credentials => {calls.push(credentials); return saved;},
  });
  vm.runInContext(source, context);
  await context.modal.mount(host, {auth_reset_required:recovery});
  return {
    host, nodes, created, calls, navigation, resolveSave, rejectSave,
    get closed() { return closed; }, get dismissible() { return dismissible; },
    click: name => nodes.get(`[data-x="${name}"]`).listeners.click(),
    fill(field, value) { const node = nodes.get(`[data-field="${field}"]`); node.value = value; node.listeners.input(); },
  };
}

test("welcome has one sign-in action; credentials remain in step one", async () => {
  const app = await setup();
  assert.equal(app.dismissible, false);
  assert.match(app.host.innerHTML, /Set up sign-in/);
  assert.doesNotMatch(app.host.innerHTML, />Next<|data-x="save"/);
  app.click("next");
  assert.match(app.host.innerHTML, /Create your sign-in credentials/);
  assert.match(app.host.innerHTML, /aria-current="step"><span class="sw-step-number">1/);
  assert.equal(app.nodes.get('[data-x="save"]').disabled, true);
  app.fill("username", "new-admin");
  app.click("back");
  app.click("next");
  assert.equal(app.nodes.get("#sw-auth-user").value, "new-admin");
});

test("passwords can be revealed independently without changing their values", async () => {
  const app = await setup();
  app.click("next");
  app.fill("password", "test-password");
  const toggle = app.created.find(node => node.attributes["aria-controls"] === "sw-auth-pass");
  toggle.listeners.click();
  assert.equal(app.nodes.get("#sw-auth-pass").type, "text");
  assert.equal(app.nodes.get("#sw-auth-pass").value, "test-password");
  assert.equal(app.nodes.get("#sw-auth-pass2").type, "password");
  assert.equal(toggle.attributes["aria-pressed"], "true");
  toggle.listeners.click();
  assert.equal(app.nodes.get("#sw-auth-pass").type, "password");
});

test("validation gates saving; successful sign-in opens Settings once", async () => {
  const app = await setup();
  app.click("next");
  app.fill("password", "short");
  app.fill("password2", "short");
  assert.equal(app.nodes.get('[data-x="save"]').disabled, true);
  app.fill("password", "test-password");
  app.fill("password2", "different-password");
  assert.equal(app.nodes.get('[data-x="save"]').disabled, true);
  app.fill("password2", "test-password");
  assert.equal(app.nodes.get('[data-x="save"]').disabled, false);
  const saving = app.click("save");
  assert.equal(app.nodes.get('[data-x="back"]').disabled, true);
  assert.equal(app.nodes.get("#sw-auth-user").disabled, true);
  assert.equal(app.closed, false);
  await app.click("save");
  assert.equal(app.calls.length, 1);
  app.resolveSave({ok:true});
  await saving;
  assert.equal(app.closed, true);
  assert.deepEqual(app.navigation, ["settings", "overview"]);
});

test("server failures remain visible and credentials stay available for retry", async () => {
  const app = await setup();
  app.click("next");
  app.fill("password", "test-password");
  app.fill("password2", "test-password");
  const saving = app.click("save");
  app.rejectSave(Error("Could not save sign-in"));
  await saving;
  assert.equal(app.closed, false);
  assert.equal(app.nodes.get("#sw-auth-error").textContent, "Could not save sign-in");
  assert.equal(app.nodes.get('[data-x="save"]').disabled, false);
  assert.equal(app.nodes.get("#sw-auth-pass").value, "test-password");
});

test("authentication recovery opens credentials directly without Back", async () => {
  const app = await setup(true);
  assert.match(app.host.innerHTML, /Recovery required|Background activity is paused/);
  assert.doesNotMatch(app.host.innerHTML, /data-x="next"|data-x="back"|Setup progress/);
  assert.equal(app.dismissible, false);
});
