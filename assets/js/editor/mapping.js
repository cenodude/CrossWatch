/* assets/js/editor/mapping.js */
/* CrossWatch - Shared mapping workspace with staged Editor corrections */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
export async function openEditorMapping(row, ctx) {
  const {state} = ctx;
  const scope = state.mappingPair ? "pair" : "shared";
  const original = structuredClone(row.raw || {});
  original.type = row.type || original.type;
  original.title = row.title || original.title;
  original.ids = {...original.ids};
  for (const key of ["imdb", "tmdb", "tvdb", "trakt", "simkl", "mal", "anilist"]) {
    if (!(key in row)) continue;
    if (String(row[key] || "").trim()) original.ids[key] = String(row[key]).trim();
    else delete original.ids[key];
  }
  const reference = {provider:state.snapshot, instance:state.instance || "default", pair_id:state.mappingPair || "",
    feature:state.kind, key:row.key, original};
  const checkCurrentRow = () => {
    if (state.snapshot !== reference.provider || (state.instance || "default") !== reference.instance
        || (state.mappingPair || "") !== reference.pair_id || state.kind !== reference.feature || !state.rows.includes(row)) {
      throw new Error("The Editor view changed. Reopen mapping for the current item.");
    }
  };
  const request = (action, extra = {}, options = {}) => ctx.fetchJSON("/api/editor/mapping", {
    ...options, method:"POST", headers:{"Content-Type":"application/json"},
    body:JSON.stringify({...reference, action, ...extra}),
  });
  const version = encodeURIComponent(window.APP_VERSION || "1");
  if (!document.querySelector('link[href*="/assets/css/interactive-sync.css"]')) {
    const style = document.createElement("link");
    style.rel = "stylesheet";
    style.href = `/assets/css/interactive-sync.css?v=${version}`;
    document.head.append(style);
  }
  const {openMappingWorkspace} = await import(`/assets/js/interactive-sync-mapping.js?v=${version}`);
  const context = await request("catalogs");
  checkCurrentRow();
  return openMappingWorkspace({
    rows:[context.row],
    total:1, standalone:true, staged:true, scope,
    scopes:[{id:scope, label:scope === "pair" ? "This sync pair" : "All pairs using this provider instance"}],
    onClose:() => { state.mappingEditing = false; }, onSaved:() => { state.mappingEditing = false; },
    mappingApi:{
      catalogs:async () => context,
      search:(_row, q, catalog, options) => request("search", {q, catalog}, options),
      episodes:(edits, options) => request("episodes", {item:edits[0].item}, options),
      save:async edits => {
        const result = await request("prepare", {item:edits[0].item});
        checkCurrentRow();
        const error = ctx.commitReplacement(row, result.item, result.key, "Correction ready. Save changes to apply it.");
        if (error) throw new Error(error);
        return result;
      },
    },
  });
}
