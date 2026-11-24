/* assets/js/topology/advisor.js */
/* CrossWatch - Sync topology health summary and refresh handling */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const I18N = () => globalThis.window?.CW?.I18n || globalThis.window?.__cwI18n;
const T = (key, vars) => I18N()?.t?.(key, vars) ?? key;
const TH = (key, vars) => I18N()?.html?.(key, vars) ?? key;
import { esc } from "./graph.js";
import { baselineCopy, clearBaselines, loadBaseline } from "./baseline.js";

const stateVersion = new URL(import.meta.url).searchParams.get("v") || window.__CW_VERSION__ || "";
const { currentTopology } = await import(`./state.js?v=${encodeURIComponent(stateVersion)}`);

const TR = (key, vars) => I18N()?.rich?.(key, vars) ?? key;
export const statusLabel = result => ["attention", "review", "healthy"].includes(result.status) ? T(`modals.topology.status.${result.status}`) : undefined;
export const statusIcon = result => ({ attention: "error", review: "info", healthy: "check_circle" }[result.status]);
export function summaryCopy(result) {
  if (result.baseline) return baselineCopy(result);
  if (!result.activePairs) return T("modals.topology.enablePair");
  if (result.unsupportedPairs) return T("modals.topology.lackSupport");
  if (!result.graphs.length) return T("modals.topology.noFeatures");
  if (result.status === "attention") return T("modals.topology.multiple");
  if (result.status === "review") return T("modals.topology.closer");
  if (result.activePairs === 1) return T("modals.topology.onePair");
  return T("modals.topology.healthyRoutes");
}

function renderSummary() {
  const host = document.getElementById("sync-topology-health");
  if (!host) return;
  if (!Array.isArray(window.cx?.pairs) || !Array.isArray(window.cx?.providers)) {
    host.innerHTML = `<div class="topology-kicker">${TH("modals.topology.health.title")}</div><p class="topology-copy" role="status">${TH("modals.topology.waiting")}</p>`;
    return;
  }
  const result = currentTopology();
  if (result.baseline && !result.acknowledged && result.status === "healthy") result.status = "review";
  const minimal = result.activePairs <= 1 && !result.findings.length;
  host.classList.toggle("is-minimal", minimal);
  const state = result.acknowledged ? "review" : result.unsupportedPairs ? "review" : result.status;
  const label = result.acknowledged ? T("modals.topology.acknowledged") : result.unsupportedPairs ? T("modals.topology.status.review") : result.activePairs || result.baseline ? statusLabel(result) : T("modals.topology.noActivePairs");
  const focus = document.activeElement?.closest?.("#sync-topology-health button")?.dataset.action;
  host.innerHTML = `<div class="topology-summary-content"><div class="topology-summary-main">
    <div class="topology-summary-title"><h4 class="topology-kicker" id="topology-health-title">${TH("modals.topology.health.title")}</h4>
      <span class="topology-status is-${state}"><span class="material-symbol" aria-hidden="true">${result.acknowledged || result.unsupportedPairs ? "info" : statusIcon(result)}</span>${label}</span></div>
    <div class="topology-counters" aria-label="${TH("modals.topology.summaryLabel")}"><span>${TR("modals.topology.activePairs", {count: result.activePairs})}</span>
      ${minimal ? "" : `<span>${TR("modals.topology.conflicts", {count: result.conflicts})}</span><span>${TR("modals.topology.loops", {count: result.loops})}</span><span>${TR("modals.topology.suggestions", {count: result.suggestions})}</span>`}</div>
    <p class="topology-copy">${summaryCopy(result)} <span class="topology-scope">${esc(result.scope)}</span></p>
    </div></div><div class="topology-summary-actions"><button type="button" class="btn topology-primary" data-action="view">${TH("modals.topology.view")}</button>
      ${result.findings.some(finding => !finding.informational) ? `<button type="button" class="btn" data-action="review">${result.status === "healthy" ? TH("modals.topology.reviewSuggestions", {count: result.suggestions}) : TH("modals.topology.reviewFindings")}</button>` : ""}</div>`;
  host.querySelectorAll("button").forEach(button => button.addEventListener("click", async () => {
    button.disabled = true;
    try {
      const version = window.__CW_VERSION__ || "";
      const { openModal } = await import(`../modals.js?v=${encodeURIComponent(version)}`);
      await openModal("sync-topology", { review: button.dataset.action === "review", trigger: button });
    } catch (error) {
      console.warn("[topology] modal failed", error);
      host.querySelector(".topology-copy").textContent = T("modals.topology.openFailed");
    } finally { button.disabled = false; }
  }));
  if (focus) host.querySelector(`[data-action="${focus}"]`)?.focus({ preventScroll: true });
}

let scheduled = false;
function render() {
  renderSummary();
  document.dispatchEvent(new Event("cw:topology-updated"));
}
function refresh() {
  if (scheduled) return;
  scheduled = true;
  queueMicrotask(async () => {
    scheduled = false;
    render();
    if (!document.getElementById("sync-topology-health")) return;
    await loadBaseline(currentTopology().profileId);
    render();
  });
}
document.addEventListener("cw:topology-baseline-changed", render);
document.addEventListener("cw:sync-data-changed", refresh);
document.addEventListener("cx-state-change", refresh);
window.addEventListener("cx:pairs:changed", refresh);
window.addEventListener("cw:overview-profile-changed", refresh);
window.addEventListener("cw:auth-state-changed", () => { clearBaselines(); refresh(); });
window.addEventListener("auth-changed", () => { clearBaselines(); refresh(); });
if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", refresh, { once: true });
else refresh();
