/* tests/topology-loading.test.mjs */
/* CrossWatch - Versioned topology module loading regression tests */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createContext, SourceTextModule } from "node:vm";

for (const queryVersion of ["release-test", ""]) {
  test(`topology entry points bypass stale state and share acknowledgement state (${queryVersion || "window version"})`, async () => {
    const version = queryVersion || "window-test";
    const modules = new Map(), stateImports = [];
    const document = Object.assign(new EventTarget(), { readyState: "loading", getElementById: () => null });
    const context = createContext({
      URL, Event, document,
      window: Object.assign(new EventTarget(), { __CW_VERSION__: "window-test" }),
      fetch: async () => ({ ok: true, json: async () => ({ baseline: null }) }),
    });
    const stateUrl = new URL("../assets/js/topology/state.js", import.meta.url).href;
    const baselineUrl = new URL("../assets/js/topology/baseline.js", import.meta.url).href;
    function load(url) {
      if (modules.has(url)) return modules.get(url);
      const source = url === stateUrl
        ? "export function currentTopology() { return { baselineReady: false }; }"
        : readFileSync(new URL(url), "utf8");
      const module = new SourceTextModule(source, {
        context, identifier: url,
        initializeImportMeta(meta) { meta.url = url; },
        importModuleDynamically: async (specifier, parent) => {
          const dependency = resolve(specifier, parent);
          if (dependency.status === "unlinked") await dependency.link(resolve);
          if (dependency.status === "linked") await dependency.evaluate();
          return dependency;
        },
      });
      modules.set(url, module);
      return module;
    }
    function resolve(specifier, parent) {
      const url = new URL(specifier, parent.identifier).href;
      if (url.startsWith(stateUrl)) stateImports.push(url);
      return load(url);
    }
    for (const entry of ["../assets/js/topology/advisor.js", "../assets/js/modals/sync-topology/index.js"]) {
      const url = new URL(entry, import.meta.url);
      if (queryVersion) url.searchParams.set("v", queryVersion);
      const module = load(url.href);
      await module.link(resolve);
      await module.evaluate();
    }
    assert.deepEqual(stateImports, [`${stateUrl}?v=${version}`, `${stateUrl}?v=${version}`]);
    const { currentTopology } = modules.get(stateImports[0]).namespace;
    const { loadBaseline, saveBaseline } = modules.get(baselineUrl).namespace;
    assert.equal(currentTopology().baselineReady, false);
    await loadBaseline("");
    assert.equal(currentTopology().baselineReady, true);
    assert.equal(currentTopology().baseline, null);
    const baseline = { ...currentTopology().snapshot, accepted_at: "2026-09-24T12:00:00Z" };
    context.fetch = async () => ({ ok: true, json: async () => ({ baseline }) });
    await saveBaseline(currentTopology());
    assert.equal(currentTopology().acknowledged, true);
  });
}
