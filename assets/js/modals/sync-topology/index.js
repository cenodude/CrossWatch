/* assets/js/modals/sync-topology/index.js */
/* CrossWatch - Sync topology modal and findings */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import { currentTopology } from "../../topology/state.js";
import { connectionLabel, esc, featureName, findingConnections, mergeGraphs, nodeName, pairBadge, renderGraph } from "../../topology/graph.js";
import { bindGraphViewport } from "../../topology/viewport.js";

const titles = { conflict: "Updates from multiple providers", loop: "Return route through additional pairs",
  redundancy: "Potential redundant route", observation: "Consider simplifying your routes" };
const icons = { conflict: "call_merge", loop: "sync", redundancy: "alt_route", observation: "lightbulb" };

function findingCopy(finding, nodes) {
  const paths = finding.paths;
  const feature = featureName(finding.feature);
  if (finding.reason === "two-way-sharing") return `${nodeName(nodes.get(finding.destination))} shares ${feature.toLowerCase()} with ${paths.length} providers through two-way pairs. Changes can travel in either direction. This is expected for these pairs.`;
  if (finding.type === "conflict") return `${nodeName(nodes.get(finding.destination))} receives ${feature.toLowerCase()} updates from several providers. If they disagree about the same item, their updates may compete. Two-way pairs shown below also send changes back.`;
  if (finding.type === "loop") return `${feature} can return to its starting provider through separately configured pairs, beyond a two-way pair's normal return path.`;
  if (finding.type === "redundancy" && paths.length > 1) return `${feature} from ${nodeName(nodes.get(paths[0][0]))} can reach ${nodeName(nodes.get(paths[0].at(-1)))} through multiple paths.`;
  if (finding.type === "redundancy") return `Multiple pairs carry ${feature.toLowerCase()} along the same route.`;
  return `${feature} spans ${finding.nodes.length} provider instances. A central source may make these routes easier to maintain.`;
}
function findingAdvice(finding) {
  if (finding.reason === "two-way-sharing") return ["ratings", "progress"].includes(finding.feature)
    ? "Each linked provider can update these values. Different ratings or playback positions for the same item may compete. This note does not mean a conflict has occurred."
    : "Two-way sync does not make the configured source authoritative. Keep these pairs if changes should flow both ways; use one-way pairs if only the source should send changes.";
  if (finding.type === "conflict") return "Review which provider should supply this feature. This is a potential conflict, not evidence that values have been overwritten.";
  if (finding.type === "loop") return "The example follows multiple pairs back to its starting provider. CrossWatch has safeguards, so this does not imply an infinite loop; the additional route may cause extra provider activity.";
  if (finding.type === "redundancy") return "CrossWatch can process this configuration, but the additional route may cause unnecessary provider activity.";
  return "This topology is valid. Keep the routes you need and consider consolidating pairs that serve the same purpose.";
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
    let result = currentTopology(), selectedFeature = "all", selectedId = "";
    if (props.review && result.findings.length) {
      selectedId = result.findings[0].id;
      selectedFeature = result.findings[0].feature;
    }
    host.innerHTML = `<div class="topology-modal">
      <header class="topology-modal-head"><div><h2 id="topology-modal-title">Sync topology</h2>
        <p id="topology-modal-description">Visualize how media state moves between your connected providers.</p></div>
        <button type="button" class="btn topology-close" aria-label="Close topology"><span class="material-symbol" aria-hidden="true">close</span></button></header>
      <div class="topology-modal-body">
        <div class="topology-content"><section class="topology-map" aria-label="Provider routes">
          <div class="topology-map-heading"><div class="topology-map-context"><span class="topology-map-label"></span><span class="topology-scope" hidden></span><button type="button" class="topology-clear" hidden>Clear highlight</button></div><div class="topology-features" role="group" aria-label="Synchronization feature"></div></div>
          <div class="topology-graph" id="topology-graph" tabindex="0" role="region" aria-label="Synchronization topology graph" aria-describedby="topology-pan-hint"></div>
          <div class="topology-map-footer"><div class="topology-legend"><span aria-label="One way" title="One way"><b aria-hidden="true">→</b><span class="topology-legend-label">One way</span></span><span aria-label="Two way" title="Two way"><b aria-hidden="true">↔</b><span class="topology-legend-label">Two way</span></span><span aria-label="Pair number" title="Pair number"><span class="topology-pair-badge" aria-hidden="true">#</span><span class="topology-legend-label">Pair number</span></span></div>
          <div class="topology-zoom" role="group" aria-label="Graph zoom">
            <button type="button" data-zoom="out" aria-label="Zoom out"><span class="material-symbol" aria-hidden="true">remove</span></button>
            <input id="topology-zoom" type="range" min="50" max="250" step="10" value="100" aria-label="Topology zoom" aria-controls="topology-graph">
            <button type="button" data-zoom="in" aria-label="Zoom in"><span class="material-symbol" aria-hidden="true">add</span></button>
            <output for="topology-zoom">100%</output><button type="button" data-zoom="fit" aria-label="Reset zoom and fit graph">Fit</button>
            <span class="topology-pan-hint" id="topology-pan-hint" hidden>Drag or use arrow keys to pan</span>
          </div></div>
          <details class="topology-route-details"><summary>Route details</summary><ul class="topology-route-list"></ul></details>
        </section><section class="topology-findings" aria-labelledby="topology-findings-title"><div class="topology-findings-heading"><h3 id="topology-findings-title">Topology health</h3><span class="topology-finding-count" role="status"></span></div>
          <div class="topology-finding-list"></div></section></div>
        <p class="topology-footnote"><span class="material-symbol" aria-hidden="true">info</span>Analyzed locally from enabled pair features. Media filters, library scopes and sync safeguards can reduce actual overlap. No provider data is changed.</p>
      </div></div>`;
    const $ = selector => host.querySelector(selector);
    $(".topology-close").addEventListener("click", () => window.cxCloseModal?.());
    let displayedGraph = null, displayedFinding = null;
    const viewport = bindGraphViewport($(".topology-graph"), $(".topology-zoom"));
    let compactGraph = $(".topology-graph").clientWidth < 460;
    const drawGraph = () => {
      if (displayedGraph) $(".topology-graph").innerHTML = renderGraph(displayedGraph, displayedFinding, compactGraph);
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
      $(".topology-features").innerHTML = ["all", ...features].map(feature => `<button type="button" data-feature="${esc(feature)}" aria-pressed="${feature === selectedFeature}">${feature === "all" ? "All" : esc(featureName(feature))}</button>`).join("");
      $(".topology-map-context .topology-scope").textContent = result.profileLabel;
      $(".topology-map-context .topology-scope").hidden = !result.profileLabel;
      $(".topology-map-label").textContent = `${graph.nodes.length} provider ${graph.nodes.length === 1 ? "instance" : "instances"} · ${selectedFeature === "all" ? "All features" : featureName(selectedFeature)}`;
      displayedGraph = graph;
      displayedFinding = finding;
      drawGraph();
      $(".topology-clear").hidden = !finding;
      $(".topology-route-list").innerHTML = graph.connections.map(connection => `<li>${pairBadge(connection)}<span>${esc(connectionLabel(connection, nodes))}<small>${connection.twoWay ? "Two-way pair" : "One-way pair"} · ${esc(connection.features.map(featureName).join(", "))}</small></span></li>`).join("");
      const onlyNotes = findings.length && findings.every(item => item.informational);
      $(".topology-finding-count").textContent = `${findings.length} ${onlyNotes ? (findings.length === 1 ? "note" : "notes") : (findings.length === 1 ? "finding" : "findings")}`;
      $(".topology-finding-list").innerHTML = `${result.unsupportedPairs ? '<div class="topology-capability-note">Some enabled features are unsupported or unavailable in provider metadata and are omitted. Review their pair settings.</div>' : ""}` + (findings.length ? findings.map(item => {
        const active = item.id === selectedId;
        const severity = item.informational ? "Info" : item.severity === "conflict" ? "Potential conflict" : item.severity === "warning" ? "Review" : "Observation";
        const connections = findingConnections(item, graph);
        const routes = item.destination
          ? connections.map(connection => `<span class="topology-numbered-route">${pairBadge(connection)}<span><small>${connection.twoWay ? "Two-way pair" : "One-way pair"}</small>${esc(connectionLabel(connection, nodes))}</span></span>`).join("")
          : item.paths.map((path, index) => `<span><small>${item.type === "redundancy" && item.paths.length > 1 ? (path.length === 2 ? "Direct" : "Indirect") : item.type === "loop" ? "Example return path" : `Route ${index + 1}`}</small>${esc(path.map(id => nodeName(nodes.get(id))).join(" → "))}</span>`).join("");
        return `<button type="button" class="topology-finding ${active ? "is-selected" : ""}" data-finding="${esc(item.id)}" aria-pressed="${active}">
          <span class="topology-finding-meta"><span>${esc(featureName(item.feature))}</span><span class="topology-severity is-${item.severity}">${severity}</span></span>
          <span class="topology-finding-title"><span class="material-symbol" aria-hidden="true">${item.informational ? "sync_alt" : icons[item.type]}</span>${item.informational ? "Shared two-way updates" : titles[item.type]}</span>
          <span class="topology-finding-copy">${esc(findingCopy(item, nodes))}</span>
          ${!item.destination && connections.length ? `<span class="topology-finding-pairs"><span>Pairs</span>${connections.map(pairBadge).join("")}</span>` : ""}
          <span class="topology-paths">${routes}</span>
          ${active ? `<span class="topology-advice">${esc(findingAdvice(item))}</span>` : '<span class="topology-finding-hint">Highlight routes <span aria-hidden="true">↗</span></span>'}</button>`;
      }).join("") : `<div class="topology-empty"><span class="material-symbol topology-good" aria-hidden="true">${graph.nodes.length ? "check_circle" : "route"}</span><strong>${result.unsupportedPairs ? "No findings in available routes" : graph.nodes.length ? "Your routes look healthy" : "No active routes yet"}</strong><p>${graph.nodes.length ? "No conflicting writers, additional return routes or redundant paths found for this selection." : "Enable a supported feature in a sync pair to get started."}</p></div>`);
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
    const update = () => { result = currentTopology(); draw(); };
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
