/* assets/js/topology/state.js */
/* CrossWatch - Shared sync topology state */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

import { analyzeTopology, pairsForProfile } from "./analysis.js";

export function currentTopology() {
  const profile = window.CW?.OverviewProfile?.profile;
  const allPairs = window.cx?.pairs || [];
  const pairs = pairsForProfile(allPairs, profile);
  const numbers = new Map(allPairs.map((pair, index) => [String(pair.id), index + 1]));
  const result = analyzeTopology(pairs, window.cx?.providers || [], window.CW?.FeatureMeta?.order || []);
  result.graphs.forEach(graph => graph.connections.forEach(connection => { connection.number = numbers.get(connection.id); }));
  return { ...result,
    profileLabel: profile?.id ? (profile.label || profile.id) : "",
    scope: profile?.label || "All configured pairs" };
}
