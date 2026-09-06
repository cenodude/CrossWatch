/* assets/js/topology/advisor.js */
/* CrossWatch - Sync topology health summary and refresh handling */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import { currentTopology } from "./state.js";
import { esc } from "./graph.js";

export const statusLabel = result => ({ attention: "Attention required", review: "Review recommended", healthy: "Healthy" }[result.status]);
export const statusIcon = result => ({ attention: "error", review: "info", healthy: "check_circle" }[result.status]);
export function summaryCopy(result) {
  if (!result.activePairs) return "Enable a sync pair to visualize your routes.";
  if (result.unsupportedPairs) return "Some enabled features lack provider support. Review pair settings and available routes.";
  if (!result.graphs.length) return "No synchronization features are enabled in these pairs.";
  if (result.status === "attention") return "Multiple routes may write different values. Review the affected features.";
  if (result.status === "review") return "A few routes deserve a closer look.";
  if (result.activePairs === 1) return "One active pair. Your routes are easy to follow.";
  return "Your synchronization routes are healthy.";
}

function renderSummary() {
  const host = document.getElementById("sync-topology-health");
  if (!host) return;
  if (!Array.isArray(window.cx?.pairs) || !Array.isArray(window.cx?.providers)) {
    host.innerHTML = '<div class="topology-kicker">Topology health</div><p class="topology-copy" role="status">Waiting for sync configuration…</p>';
    return;
  }
  const result = currentTopology();
  const minimal = result.activePairs <= 1 && !result.findings.length;
  host.classList.toggle("is-minimal", minimal);
  const state = result.unsupportedPairs ? "review" : result.status;
  const label = result.unsupportedPairs ? "Review recommended" : result.activePairs ? statusLabel(result) : "No active pairs";
  const focus = document.activeElement?.closest?.("#sync-topology-health button")?.dataset.action;
  host.innerHTML = `<div class="topology-summary-content"><div class="topology-summary-icon" aria-hidden="true"><span class="material-symbol">hub</span></div><div class="topology-summary-main">
    <div class="topology-summary-title"><h4 class="topology-kicker" id="topology-health-title">Topology health</h4>
      <span class="topology-status is-${state}"><span class="material-symbol" aria-hidden="true">${result.unsupportedPairs ? "info" : statusIcon(result)}</span>${label}</span></div>
    <div class="topology-counters" aria-label="Topology summary"><span><strong>${result.activePairs}</strong> active ${result.activePairs === 1 ? "pair" : "pairs"}</span>
      ${minimal ? "" : `<span><strong>${result.conflicts}</strong> ${result.conflicts === 1 ? "conflict" : "conflicts"}</span><span><strong>${result.loops}</strong> ${result.loops === 1 ? "loop" : "loops"}</span><span><strong>${result.suggestions}</strong> ${result.suggestions === 1 ? "suggestion" : "suggestions"}</span>`}</div>
    <p class="topology-copy">${summaryCopy(result)} <span class="topology-scope">${esc(result.scope)}</span></p>
    </div></div><div class="topology-summary-actions"><button type="button" class="btn topology-primary" data-action="view"><span class="material-symbol" aria-hidden="true">hub</span>View topology</button>
      ${result.findings.some(finding => !finding.informational) ? `<button type="button" class="btn" data-action="review">${result.status === "healthy" ? `Review ${result.suggestions === 1 ? "suggestion" : "suggestions"}` : "Review findings"}</button>` : ""}</div>`;
  host.querySelectorAll("button").forEach(button => button.addEventListener("click", async () => {
    button.disabled = true;
    try {
      const version = window.__CW_VERSION__ || "";
      const { openModal } = await import(`../modals.js?v=${encodeURIComponent(version)}`);
      await openModal("sync-topology", { review: button.dataset.action === "review", trigger: button });
    } catch (error) {
      console.warn("[topology] modal failed", error);
      host.querySelector(".topology-copy").textContent = "Could not open topology. Please try again.";
    } finally { button.disabled = false; }
  }));
  if (focus) host.querySelector(`[data-action="${focus}"]`)?.focus({ preventScroll: true });
}

let scheduled = false;
function refresh() {
  if (scheduled) return;
  scheduled = true;
  queueMicrotask(() => {
    scheduled = false;
    renderSummary();
    document.dispatchEvent(new Event("cw:topology-updated"));
  });
}
document.addEventListener("cw:sync-data-changed", refresh);
document.addEventListener("cx-state-change", refresh);
window.addEventListener("cx:pairs:changed", refresh);
window.addEventListener("cw:overview-profile-changed", refresh);
window.addEventListener("cw:auth-state-changed", refresh);
window.addEventListener("auth-changed", refresh);
if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", refresh, { once: true });
else refresh();
