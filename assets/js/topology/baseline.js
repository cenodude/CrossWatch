/* assets/js/topology/baseline.js */
/* CrossWatch - Persisted topology acknowledgement */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const baselines = new Map();
let revision = 0;
const canonical = value => JSON.stringify(value, (_, item) => item && typeof item === "object" && !Array.isArray(item)
  ? Object.fromEntries(Object.keys(item).sort().map(key => [key, item[key]])) : item);

export function topologySnapshot(pairs, result) {
  return JSON.parse(canonical({ version: 1, pairs: pairs.filter(pair => pair.enabled !== false).map(pair => ({
    id: String(pair.id || ""),
    source: String(pair.source || "").trim().toUpperCase(), target: String(pair.target || "").trim().toUpperCase(),
    source_instance: String(pair.source_instance || "default").trim() || "default",
    target_instance: String(pair.target_instance || "default").trim() || "default",
    mode: String(pair.mode || "one-way").toLowerCase().startsWith("two") ? "two-way" : "one-way",
    features: pair.features || {}
  })).sort((a, b) => a.id < b.id ? -1 : a.id > b.id ? 1 : 0),
  findings: result.findings, unsupportedPairs: result.unsupportedPairs }));
}

export function baselineMatches(snapshot, baseline) {
  if (!baseline) return false;
  const { accepted_at, ...saved } = baseline;
  return canonical(snapshot) === canonical(saved);
}

const scopeId = value => {
  const id = String(value ?? "").trim();
  return id === "undefined" || id === "null" ? "" : id;
};

export const savedBaseline = profileId => baselines.get(scopeId(profileId));
export function clearBaselines() { revision++; baselines.clear(); }

export async function loadBaseline(profileId) {
  const scope = scopeId(profileId);
  const current = ++revision;
  try {
    const response = await fetch(`/api/sync/topology/baseline?profile_id=${encodeURIComponent(scope)}`, { cache: "no-store" });
    if (!response.ok) throw new Error("Could not load topology acknowledgement.");
    const data = await response.json();
    if (current === revision) baselines.set(scope, data.baseline);
  } catch {
    if (current === revision) baselines.delete(scope);
  }
}

export async function saveBaseline(result, reset = false) {
  const scope = scopeId(result.profileId);
  revision++;
  const response = await fetch(`/api/sync/topology/baseline?profile_id=${encodeURIComponent(scope)}`, {
    method: reset ? "DELETE" : "PUT", headers: { "Content-Type": "application/json" },
    ...(reset ? {} : { body: JSON.stringify(result.snapshot) })
  });
  const data = await response.json();
  if (!response.ok) throw new Error(typeof data.detail === "string" ? data.detail : "Could not save topology acknowledgement.");
  revision++;
  baselines.set(scope, data.baseline);
  document.dispatchEvent(new Event("cw:topology-baseline-changed"));
}

export function baselineCopy(result) {
  if (result.acknowledged) {
    const count = result.baseline.findings.filter(finding => !finding.informational).length;
    return `${count} known ${count === 1 ? "finding" : "findings"} accepted on ${new Date(result.baseline.accepted_at).toLocaleString()}.`;
  }
  return result.baseline ? "Topology differs from the acknowledged baseline. Review the current routes." : "";
}
