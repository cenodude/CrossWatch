/* assets/js/modals/sync-topology/index.js */
/* CrossWatch - Sync topology modal and findings */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import { connectionLabel, esc, featureName, findingConnections, mergeGraphs, nodeName, pairBadge, renderGraph, topologySummary } from "../../topology/graph.js";
import { bindGraphViewport } from "../../topology/viewport.js";
import { baselineCopy, saveBaseline } from "../../topology/baseline.js";

const stateVersion = new URL(import.meta.url).searchParams.get("v") || window.__CW_VERSION__ || "";
const { currentTopology } = await import(`../../topology/state.js?v=${encodeURIComponent(stateVersion)}`);

const I18N = () => globalThis.window?.CW?.I18n || globalThis.window?.__cwI18n;
const T = (key, vars) => I18N()?.t?.(key, vars) ?? key;
const TH = (key, vars) => I18N()?.html?.(key, vars) ?? key;
const TITLE_TYPES = new Set(["conflict", "loop", "redundancy", "observation"]);
const titleOf = (type) => TITLE_TYPES.has(type) ? T(`modals.topology.titles.${type}`) : undefined;
const icons = { conflict: "call_merge", loop: "sync", redundancy: "alt_route", observation: "lightbulb" };

function findingCopy(finding, nodes) {
  const paths = finding.paths;
  const feature = featureName(finding.feature);
  const lower = feature.toLowerCase();
  if (finding.reason === "two-way-sharing") return T("modals.topology.copy.twoWay", {node: nodeName(nodes.get(finding.destination)), feature: lower, count: paths.length});
  if (finding.type === "conflict") return T("modals.topology.copy.conflict", {node: nodeName(nodes.get(finding.destination)), feature: lower});
  if (finding.type === "loop") return T("modals.topology.copy.loop", {feature});
  if (finding.type === "redundancy" && paths.length > 1) return T("modals.topology.copy.redundancyPaths", {feature, from: nodeName(nodes.get(paths[0][0])), to: nodeName(nodes.get(paths[0].at(-1)))});
  if (finding.type === "redundancy") return T("modals.topology.copy.redundancy", {feature: lower});
  return T("modals.topology.copy.observation", {feature, count: finding.nodes.length});
}
function findingAdvice(finding) {
  if (finding.reason === "two-way-sharing") return ["ratings", "progress"].includes(finding.feature)
    ? T("modals.topology.adviceTwoWayValues")
    : T("modals.topology.adviceTwoWay");
  if (finding.type === "conflict") return T("modals.topology.adviceConflict");
  if (finding.type === "loop") return T("modals.topology.adviceLoop");
  if (finding.type === "redundancy") return T("modals.topology.adviceRedundancy");
  return T("modals.topology.adviceValid");
}

let cleanup = null;
export default {
  mount(host, props = {}) {
    cleanup?.();
    host.classList.add("cw-topology-shell");
    host.setAttribute("role", "dialog");
    host.setAttribute("aria-modal", "true");
    host.setAttribute("aria-labelledby", "topology-modal-title");
    host.setAttribute("aria-describedby", "topology-modal-description");
    let result = currentTopology(), selectedFeature = "all", selectedId = "", analyzedAt = new Date();
    if (props.review && result.findings.length) {
      selectedId = result.findings[0].id;
      selectedFeature = result.findings[0].feature;
    }
    host.innerHTML = `<div class="topology-modal">
      <header class="topology-modal-head"><span class="topology-heading-icon material-symbol" aria-hidden="true">share</span><div><h2 id="topology-modal-title">${TH("modals.topology.title")}</h2>
        <p id="topology-modal-description">${TH("modals.topology.description")}</p></div>
        <button type="button" class="btn topology-close" aria-label="${TH("modals.topology.close")}"><span class="material-symbol" aria-hidden="true">close</span></button></header>
      <div class="topology-modal-body">
        <div class="topology-content"><section class="topology-map" aria-label="${TH("modals.topology.providerRoutes")}">
          <div class="topology-map-heading"><div class="topology-map-context"><span class="material-symbol topology-context-icon" aria-hidden="true">stacks</span><span class="topology-map-label"></span><span class="topology-scope" hidden></span><button type="button" class="topology-clear" hidden>${TH("modals.topology.clearHighlight")}</button></div><div class="topology-features" role="group" aria-label="${TH("modals.topology.featureGroup")}"></div></div>
          <div class="topology-graph" id="topology-graph" tabindex="0" role="region" aria-label="${TH("modals.topology.graph")}" aria-describedby="topology-pan-hint"></div>
          <div class="topology-map-footer"><div class="topology-legend"><span aria-label="${TH("modals.topology.oneWay")}" title="${TH("modals.topology.oneWay")}"><b aria-hidden="true">→</b><span class="topology-legend-label">${TH("modals.topology.oneWay")}</span></span><span aria-label="${TH("modals.topology.twoWay")}" title="${TH("modals.topology.twoWay")}"><b aria-hidden="true">↔</b><span class="topology-legend-label">${TH("modals.topology.twoWay")}</span></span><span aria-label="${TH("modals.topology.pairNumber")}" title="${TH("modals.topology.pairNumber")}"><span class="topology-pair-badge" aria-hidden="true">#</span><span class="topology-legend-label">${TH("modals.topology.pairNumber")}</span></span></div>
          <div class="topology-zoom" role="group" aria-label="${TH("modals.topology.zoomGroup")}">
            <button type="button" data-zoom="out" aria-label="${TH("modals.topology.zoomOut")}"><span class="material-symbol" aria-hidden="true">remove</span></button>
            <input id="topology-zoom" type="range" min="50" max="250" step="10" value="100" aria-label="${TH("modals.topology.zoom")}" aria-controls="topology-graph">
            <button type="button" data-zoom="in" aria-label="${TH("modals.topology.zoomIn")}"><span class="material-symbol" aria-hidden="true">add</span></button>
            <output for="topology-zoom">100%</output><button type="button" data-zoom="fit" aria-label="${TH("modals.topology.fitLabel")}">${TH("modals.topology.fit")}</button>
            <span class="topology-pan-hint" id="topology-pan-hint" hidden>${TH("modals.topology.panHint")}</span>
          </div></div>
          <details class="topology-route-details"><summary><span>${TH("modals.topology.routeDetails")}</span><small>${TH("modals.topology.routeDetailsSub")}</small></summary><ul class="topology-route-list"></ul></details>
        </section><section class="topology-findings" aria-labelledby="topology-findings-title"><div class="topology-findings-heading"><span class="topology-heading-icon material-symbol" aria-hidden="true">monitor_heart</span><div><h3 id="topology-findings-title">${TH("modals.topology.findingsNotes")}</h3><p>${TH("modals.topology.findingsHelp")}</p></div><span class="topology-finding-count" role="status"></span></div>
          <div class="topology-health-stats" aria-label="${TH("modals.topology.summary")}"></div>
          <div class="topology-baseline">
            <p class="topology-copy topology-baseline-status" role="status"></p>
            <div class="topology-baseline-actions">
              <button type="button" class="cw-page-button topology-acknowledge"><span class="material-symbol" aria-hidden="true">task_alt</span>${TH("modals.topology.acknowledge")}</button>
              <button type="button" class="cw-page-button topology-reset"><span class="material-symbol" aria-hidden="true">restart_alt</span>${TH("modals.topology.resetAck")}</button>
            </div>
            <details class="topology-accepted"><summary>${TH("modals.topology.accepted")}</summary><ul></ul></details>
          </div>
          <div class="topology-finding-list"></div>
          <div class="topology-analysis-footer"><span class="topology-analyzed-at"></span><button type="button" class="topology-refresh" aria-label="${TH("modals.topology.reanalyze")}" title="${TH("modals.topology.reanalyze")}"><span class="material-symbol" aria-hidden="true">refresh</span></button></div></section></div>
        <p class="topology-footnote"><span class="material-symbol" aria-hidden="true">info</span>${TH("modals.topology.footnote")}</p>
      </div></div>`;
    const $ = selector => host.querySelector(selector);
    $(".topology-close").addEventListener("click", () => window.cxCloseModal?.());
    let displayedGraph = null, displayedFinding = null, saving = false, baselineError = "";
    const viewport = bindGraphViewport($(".topology-graph"), $(".topology-zoom"));
    let compactGraph = $(".topology-graph").clientWidth < 460;
    const applyGraphAspect = () => {
      const host = $(".topology-graph"), svg = host?.querySelector("svg");
      const w = Number(svg?.getAttribute("width")), h = Number(svg?.getAttribute("height"));
      if (!svg || !(w > 0) || !(h > 0)) host?.style.removeProperty("--topology-aspect");
      else host.style.setProperty("--topology-aspect", `${w} / ${h}`);
    };
    const drawGraph = () => {
      if (displayedGraph) $(".topology-graph").innerHTML = renderGraph(displayedGraph, displayedFinding, compactGraph);
      applyGraphAspect();
      viewport.update();
    };
    const resizeObserver = new ResizeObserver(([entry]) => {
      const compact = entry.contentRect.width < 460;
      if (compact === compactGraph) return;
      compactGraph = compact;
      drawGraph();
    });
    resizeObserver.observe($(".topology-graph"));

    function draw() {
      const focused = document.activeElement;
      const focusFeature = focused?.dataset?.feature;
      const focusFinding = focused?.dataset?.finding;
      const features = result.graphs.map(graph => graph.feature);
      if (!features.includes(selectedFeature)) selectedFeature = "all";
      const finding = result.findings.find(item => item.id === selectedId) || null;
      if (!finding) selectedId = "";
      const graph = mergeGraphs(result.graphs.filter(item => selectedFeature === "all" || item.feature === selectedFeature));
      const nodes = new Map(graph.nodes.map(node => [node.id, node]));
      const findings = result.findings.filter(item => selectedFeature === "all" || item.feature === selectedFeature);
      $(".topology-features").innerHTML = ["all", ...features].map(feature => `<button type="button" data-feature="${esc(feature)}" aria-pressed="${feature === selectedFeature}">${feature === "all" ? TH("modals.topology.all") : esc(featureName(feature))}</button>`).join("");
      $(".topology-map-context .topology-scope").textContent = result.profileLabel;
      $(".topology-map-context .topology-scope").hidden = !result.profileLabel;
      $(".topology-map-label").textContent = `${T("modals.topology.instances", {count: graph.nodes.length})} · ${selectedFeature === "all" ? T("modals.topology.allFeatures") : featureName(selectedFeature)}`;
      displayedGraph = graph;
      displayedFinding = finding;
      drawGraph();
      $(".topology-clear").hidden = !finding;
      $(".topology-route-list").innerHTML = graph.connections.map(connection => `<li>${pairBadge(connection)}<span>${esc(connectionLabel(connection, nodes))}<small>${connection.twoWay ? T("modals.topology.twoWayPair") : T("modals.topology.oneWayPair")} · ${esc(connection.features.map(featureName).join(", "))}</small></span></li>`).join("");
      const health = topologySummary(graph, findings, result.unsupportedPairs);
      if (result.acknowledged) Object.assign(health, { status: "review", icon: "info", label: T("modals.topology.acceptedLabel") });
      else if (result.baseline && ["healthy", "empty"].includes(health.status)) Object.assign(health, { status: "review", icon: "info", label: T("modals.topology.review") });
      $(".topology-baseline").hidden = !result.baseline && !result.findings.some(item => !item.informational);
      $(".topology-baseline-status").textContent = baselineError || [baselineCopy(result),
        !result.baselineReady ? T("modals.topology.ackUnavailable") : !result.acknowledged && selectedFeature !== "all" ? T("modals.topology.selectAll") : ""
      ].filter(Boolean).join(" ") || T("modals.topology.acceptanceApplies");
      $(".topology-acknowledge").hidden = result.acknowledged || !result.findings.some(item => !item.informational);
      $(".topology-acknowledge").disabled = saving || !result.baselineReady || selectedFeature !== "all" || !!result.unsupportedPairs;
      $(".topology-reset").hidden = !result.baseline;
      $(".topology-reset").disabled = saving;
      $(".topology-accepted").hidden = !result.baseline;
      $(".topology-accepted ul").innerHTML = (result.baseline?.findings || []).filter(item => !item.informational)
        .map(item => `<li>${esc(featureName(item.feature))}: ${esc(titleOf(item.type) || item.type)}
          ${(item.paths || []).map(path => `<div>${esc(path.map(id => JSON.parse(id).join(":")).join(" → "))}</div>`).join("")}</li>`).join("");
      $(".topology-health-stats").innerHTML = `<div><span class="material-symbol" aria-hidden="true">hub</span><strong>${health.providers}</strong><small>${TH("modals.topology.providers")}</small></div>
        <div><span class="material-symbol" aria-hidden="true">link</span><strong>${health.twoWay}</strong><small>${TH("modals.topology.twoWayPairs")}</small></div>
        <div><span class="material-symbol" aria-hidden="true">arrow_forward</span><strong>${health.oneWay}</strong><small>${TH("modals.topology.oneWayPairs")}</small></div>
        <div class="topology-health-status is-${health.status}"><span class="material-symbol" aria-hidden="true">${health.icon}</span><strong>${health.label}</strong><small>${TH("modals.topology.routeAnalysis")}</small></div>`;
      $(".topology-analyzed-at").textContent = T("modals.topology.analyzedAt", {time: analyzedAt.toLocaleTimeString(I18N()?.locale || [], { hour: "2-digit", minute: "2-digit" })});
      const onlyNotes = findings.length && findings.every(item => item.informational);
      $(".topology-finding-count").textContent = T(onlyNotes ? "modals.topology.notes" : "modals.topology.findings", {count: findings.length});
      $(".topology-finding-list").innerHTML = `${result.unsupportedPairs ? '<div class="topology-capability-note">' + TH("modals.topology.unsupported") + '</div>' : ""}` + (findings.length ? findings.map(item => {
        const active = item.id === selectedId;
        const severity = item.informational ? T("modals.topology.info") : item.severity === "conflict" ? T("modals.topology.potentialConflict") : item.severity === "warning" ? T("modals.topology.review") : T("modals.topology.observation");
        const connections = findingConnections(item, graph);
        const routes = item.destination
          ? connections.map(connection => `<span class="topology-numbered-route">${pairBadge(connection)}<span><small>${connection.twoWay ? T("modals.topology.twoWayPair") : T("modals.topology.oneWayPair")}</small>${esc(connectionLabel(connection, nodes))}</span></span>`).join("")
          : item.paths.map((path, index) => `<span><small>${item.type === "redundancy" && item.paths.length > 1 ? (path.length === 2 ? T("modals.topology.direct") : T("modals.topology.indirect")) : item.type === "loop" ? T("modals.topology.examplePath") : T("modals.topology.route", {n: String(index + 1)})}</small>${esc(path.map(id => nodeName(nodes.get(id))).join(" → "))}</span>`).join("");
        return `<button type="button" class="topology-finding ${active ? "is-selected" : ""}" data-finding="${esc(item.id)}" aria-pressed="${active}">
          <span class="topology-finding-meta"><span>${esc(featureName(item.feature))}</span><span class="topology-severity is-${item.severity}">${severity}</span></span>
          <span class="topology-finding-title"><span class="material-symbol" aria-hidden="true">${item.informational ? "sync_alt" : icons[item.type]}</span>${item.informational ? TH("modals.topology.sharedTwoWay") : esc(titleOf(item.type))}</span>
          <span class="topology-finding-copy">${esc(findingCopy(item, nodes))}</span>
          ${!item.destination && connections.length ? `<span class="topology-finding-pairs"><span>${TH("modals.topology.pairs")}</span>${connections.map(pairBadge).join("")}</span>` : ""}
          <span class="topology-paths">${routes}</span>
          ${active ? `<span class="topology-advice">${esc(findingAdvice(item))}</span>` : '<span class="topology-finding-hint"><span class="material-symbol" aria-hidden="true">auto_awesome</span><span><strong>' + TH("modals.topology.highlight") + '</strong><small>' + TH("modals.topology.focus") + '</small></span><span class="material-symbol" aria-hidden="true">arrow_forward</span></span>'}</button>`;
      }).join("") : `<div class="topology-empty"><span class="material-symbol topology-good" aria-hidden="true">${graph.nodes.length ? "check_circle" : "route"}</span><strong>${result.unsupportedPairs ? T("modals.topology.noFindings") : graph.nodes.length ? T("modals.topology.healthy") : T("modals.topology.noRoutes")}</strong><p>${graph.nodes.length ? T("modals.topology.noneFound") : T("modals.topology.enableFeature")}</p></div>`);
      if (focusFeature) [...$(".topology-features").children].find(item => item.dataset.feature === focusFeature)?.focus({ preventScroll: true });
      if (focusFinding) [...$(".topology-finding-list").querySelectorAll("button")].find(item => item.dataset.finding === focusFinding)?.focus({ preventScroll: true });
    }
    $(".topology-features").addEventListener("click", event => {
      const button = event.target.closest("button[data-feature]");
      if (!button) return;
      selectedFeature = button.dataset.feature;
      selectedId = "";
      draw();
      [...$(".topology-features").children].find(item => item.dataset.feature === selectedFeature)?.focus();
    });
    $(".topology-finding-list").addEventListener("click", event => {
      const button = event.target.closest("button[data-finding]");
      if (!button) return;
      const id = button.dataset.finding;
      selectedId = selectedId === id ? "" : id;
      if (selectedId) selectedFeature = result.findings.find(item => item.id === selectedId).feature;
      draw();
      [...$(".topology-finding-list").querySelectorAll("button")].find(item => item.dataset.finding === id)?.focus({ preventScroll: true });
    });
    $(".topology-clear").addEventListener("click", () => {
      selectedId = "";
      draw();
      $(".topology-graph").focus({ preventScroll: true });
    });
    const update = () => { result = currentTopology(); analyzedAt = new Date(); draw(); };
    const accept = async reset => {
      saving = true;
      baselineError = "";
      draw();
      try { await saveBaseline(result, reset); }
      catch (error) { baselineError = error.message; }
      finally { saving = false; if (host.isConnected) update(); }
    };
    $(".topology-acknowledge").addEventListener("click", () => accept(false));
    $(".topology-reset").addEventListener("click", () => accept(true));
    $(".topology-refresh").addEventListener("click", () => {
      baselineError = "";
      document.dispatchEvent(new Event("cw:sync-data-changed"));
    });
    document.addEventListener("cw:topology-updated", update);
    cleanup = () => {
      viewport.dispose();
      resizeObserver.disconnect();
      document.removeEventListener("cw:topology-updated", update);
      host.removeAttribute("role");
      host.removeAttribute("aria-modal");
      host.removeAttribute("aria-labelledby");
      host.removeAttribute("aria-describedby");
      requestAnimationFrame(() => {
        const trigger = props.trigger?.isConnected ? props.trigger : document.querySelector('#sync-topology-health [data-action="view"]');
        trigger?.focus({ preventScroll: true });
      });
      cleanup = null;
    };
    draw();
    requestAnimationFrame(() => { if (host.isConnected) $(".topology-close")?.focus({ preventScroll: true }); });
  },
  unmount() { cleanup?.(); }
};
