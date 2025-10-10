/* helpers/legacy-bridge.js – map legacy globals to CW + shims */
(function(){
  (window.CW ||= {}); const Legacy = (window.CW.Legacy ||= {});
  try {
    if (typeof window["_el"]==="function") Legacy["_el"] = window["_el"];
    if (typeof window["_val"]==="function") Legacy["_val"] = window["_val"];
    if (typeof window["_boolSel"]==="function") Legacy["_boolSel"] = window["_boolSel"];
    if (typeof window["_text"]==="function") Legacy["_text"] = window["_text"];
    if (typeof window["_setVal"]==="function") Legacy["_setVal"] = window["_setVal"];
    if (typeof window["_setText"]==="function") Legacy["_setText"] = window["_setText"];
    if (typeof window["_setChecked"]==="function") Legacy["_setChecked"] = window["_setChecked"];
    if (typeof window["setValIfExists"]==="function") Legacy["setValIfExists"] = window["setValIfExists"];
    if (typeof window["stateAsBool"]==="function") Legacy["stateAsBool"] = window["stateAsBool"];
    if (typeof window["applyServerSecret"]==="function") Legacy["applyServerSecret"] = window["applyServerSecret"];
    if (typeof window["startSecretLoad"]==="function") Legacy["startSecretLoad"] = window["startSecretLoad"];
    if (typeof window["finishSecretLoad"]==="function") Legacy["finishSecretLoad"] = window["finishSecretLoad"];
    if (typeof window["getConfiguredProviders"]==="function") Legacy["getConfiguredProviders"] = window["getConfiguredProviders"];
    if (typeof window["resolveProviderKeyFromNode"]==="function") Legacy["resolveProviderKeyFromNode"] = window["resolveProviderKeyFromNode"];
    if (typeof window["applySyncVisibility"]==="function") Legacy["applySyncVisibility"] = window["applySyncVisibility"];
    if (typeof window["scheduleApplySyncVisibility"]==="function") Legacy["scheduleApplySyncVisibility"] = window["scheduleApplySyncVisibility"];
    if (typeof window["bindSyncVisibilityObservers"]==="function") Legacy["bindSyncVisibilityObservers"] = window["bindSyncVisibilityObservers"];
    if (typeof window["_invalidatePairsCache"]==="function") Legacy["_invalidatePairsCache"] = window["_invalidatePairsCache"];
    if (typeof window["_savePairsCache"]==="function") Legacy["_savePairsCache"] = window["_savePairsCache"];
    if (typeof window["_loadPairsCache"]==="function") Legacy["_loadPairsCache"] = window["_loadPairsCache"];
    if (typeof window["normalizeProviders"]==="function") Legacy["normalizeProviders"] = window["normalizeProviders"];
    if (typeof window["saveStatusCache"]==="function") Legacy["saveStatusCache"] = window["saveStatusCache"];
    if (typeof window["loadStatusCache"]==="function") Legacy["loadStatusCache"] = window["loadStatusCache"];
    if (typeof window["toggleProviderBadges"]==="function") Legacy["toggleProviderBadges"] = window["toggleProviderBadges"];
    if (typeof window["connState"]==="function") Legacy["connState"] = window["connState"];
    if (typeof window["pickCase"]==="function") Legacy["pickCase"] = window["pickCase"];
    if (typeof window["svgCrown"]==="function") Legacy["svgCrown"] = window["svgCrown"];
    if (typeof window["svgCheck"]==="function") Legacy["svgCheck"] = window["svgCheck"];
    if (typeof window["setBadge"]==="function") Legacy["setBadge"] = window["setBadge"];
    if (typeof window["renderConnectorStatus"]==="function") Legacy["renderConnectorStatus"] = window["renderConnectorStatus"];
    if (typeof window["toLocal"]==="function") Legacy["toLocal"] = window["toLocal"];
    if (typeof window["computeRedirectURI"]==="function") Legacy["computeRedirectURI"] = window["computeRedirectURI"];
    if (typeof window["flashCopy"]==="function") Legacy["flashCopy"] = window["flashCopy"];
    if (typeof window["recomputeRunDisabled"]==="function") Legacy["recomputeRunDisabled"] = window["recomputeRunDisabled"];
    if (typeof window["setSyncHeader"]==="function") Legacy["setSyncHeader"] = window["setSyncHeader"];
    if (typeof window["relTimeFromEpoch"]==="function") Legacy["relTimeFromEpoch"] = window["relTimeFromEpoch"];
    if (typeof window["enforceMainLayout"]==="function") Legacy["enforceMainLayout"] = window["enforceMainLayout"];
    if (typeof window["ensureScrobbler"]==="function") Legacy["ensureScrobbler"] = window["ensureScrobbler"];
    if (typeof window["toggleSection"]==="function") Legacy["toggleSection"] = window["toggleSection"];
    if (typeof window["setBusy"]==="function") Legacy["setBusy"] = window["setBusy"];
    if (typeof window["setStatsExpanded"]==="function") Legacy["setStatsExpanded"] = window["setStatsExpanded"];
    if (typeof window["isElementOpen"]==="function") Legacy["isElementOpen"] = window["isElementOpen"];
    if (typeof window["findDetailsButton"]==="function") Legacy["findDetailsButton"] = window["findDetailsButton"];
    if (typeof window["findDetailsPanel"]==="function") Legacy["findDetailsPanel"] = window["findDetailsPanel"];
    if (typeof window["wireDetailsToStats"]==="function") Legacy["wireDetailsToStats"] = window["wireDetailsToStats"];
    if (typeof window["scheduleInsights"]==="function") Legacy["scheduleInsights"] = window["scheduleInsights"];
    if (typeof window["renderSparkline"]==="function") Legacy["renderSparkline"] = window["renderSparkline"];
    if (typeof window["ensureMainUpdateSlot"]==="function") Legacy["ensureMainUpdateSlot"] = window["ensureMainUpdateSlot"];
    if (typeof window["renderMainUpdatePill"]==="function") Legacy["renderMainUpdatePill"] = window["renderMainUpdatePill"];
    if (typeof window["renderSummary"]==="function") Legacy["renderSummary"] = window["renderSummary"];
    if (typeof window["_ease"]==="function") Legacy["_ease"] = window["_ease"];
    if (typeof window["animateNumber"]==="function") Legacy["animateNumber"] = window["animateNumber"];
    if (typeof window["animateChart"]==="function") Legacy["animateChart"] = window["animateChart"];
    if (typeof window["_setBarValues"]==="function") Legacy["_setBarValues"] = window["_setBarValues"];
    if (typeof window["_initStatsTooltip"]==="function") Legacy["_initStatsTooltip"] = window["_initStatsTooltip"];
    if (typeof window["scanForEvents"]==="function") Legacy["scanForEvents"] = window["scanForEvents"];
    if (typeof window["openDetailsLog"]==="function") Legacy["openDetailsLog"] = window["openDetailsLog"];
    if (typeof window["closeDetailsLog"]==="function") Legacy["closeDetailsLog"] = window["closeDetailsLog"];
    if (typeof window["toggleDetails"]==="function") Legacy["toggleDetails"] = window["toggleDetails"];
    if (typeof window["downloadSummary"]==="function") Legacy["downloadSummary"] = window["downloadSummary"];
    if (typeof window["setRefreshBusy"]==="function") Legacy["setRefreshBusy"] = window["setRefreshBusy"];
    if (typeof window["_getVal"]==="function") Legacy["_getVal"] = window["_getVal"];
    if (typeof window["setTraktSuccess"]==="function") Legacy["setTraktSuccess"] = window["setTraktSuccess"];
    if (typeof window["isPlaceholder"]==="function") Legacy["isPlaceholder"] = window["isPlaceholder"];
    if (typeof window["isSettingsVisible"]==="function") Legacy["isSettingsVisible"] = window["isSettingsVisible"];
    if (typeof window["setBtnBusy"]==="function") Legacy["setBtnBusy"] = window["setBtnBusy"];
    if (typeof window["flashBtnOK"]==="function") Legacy["flashBtnOK"] = window["flashBtnOK"];
    if (typeof window["updateEdges"]==="function") Legacy["updateEdges"] = window["updateEdges"];
    if (typeof window["scrollWall"]==="function") Legacy["scrollWall"] = window["scrollWall"];
    if (typeof window["initWallInteractions"]==="function") Legacy["initWallInteractions"] = window["initWallInteractions"];
    if (typeof window["cxBrandInfo"]==="function") Legacy["cxBrandInfo"] = window["cxBrandInfo"];
    if (typeof window["cxBrandLogo"]==="function") Legacy["cxBrandLogo"] = window["cxBrandLogo"];
    if (typeof window["updateFlowRailLogos"]==="function") Legacy["updateFlowRailLogos"] = window["updateFlowRailLogos"];
    if (typeof window["artUrl"]==="function") Legacy["artUrl"] = window["artUrl"];
    if (typeof window["isOnMain"]==="function") Legacy["isOnMain"] = window["isOnMain"];
    if (typeof window["renderSyncPairs"]==="function") Legacy["renderSyncPairs"] = window["renderSyncPairs"];
    if (typeof window["logToSyncOutput"]==="function") Legacy["logToSyncOutput"] = window["logToSyncOutput"];
    if (typeof window["_cap"]==="function") Legacy["_cap"] = window["_cap"];
    if (typeof window["_byName"]==="function") Legacy["_byName"] = window["_byName"];
    if (typeof window["_normWatchlistFeature"]==="function") Legacy["_normWatchlistFeature"] = window["_normWatchlistFeature"];
    if (typeof window["_pairFeatureObj"]==="function") Legacy["_pairFeatureObj"] = window["_pairFeatureObj"];
    if (typeof window["renderConnections"]==="function") Legacy["renderConnections"] = window["renderConnections"];
    if (typeof window["fixFormLabels"]==="function") Legacy["fixFormLabels"] = window["fixFormLabels"];
  } catch(e) { console.warn('legacy map failed', e); }

  // Proxies – use legacy implementations if available
  (window.CW.Providers ||= {});
  CW.Providers.load = CW.Providers.load || (Legacy.loadProviders ? ()=>Legacy.loadProviders() : ()=>{});
  CW.Providers.mountMetadataProviders = CW.Providers.mountMetadataProviders || (Legacy.mountMetadataProviders ? ()=>Legacy.mountMetadataProviders() : ()=>{});

  (window.CW.Pairs ||= {});
  CW.Pairs.list = CW.Pairs.list || (Legacy.loadPairs ? ()=>Legacy.loadPairs() : ()=>{});
  CW.Pairs.remove = CW.Pairs.remove || (Legacy.deletePair ? (id)=>Legacy.deletePair(id) : ()=>{});
  CW.Pairs.save = CW.Pairs.save || (Legacy.cxSavePair ? (p)=>Legacy.cxSavePair(p) : ()=>{});

  (window.CW.Modal ||= {});
  CW.Modal.openFor = CW.Modal.openFor || (Legacy.cxOpenModalFor ? (p,id)=>Legacy.cxOpenModalFor(p,id) : ()=>{});

  (window.CW.Insights ||= {});
  CW.Insights.loadLight = CW.Insights.loadLight || (Legacy.refreshSettingsInsight ? ()=>Legacy.refreshSettingsInsight() : ()=>{});

  (window.CW.Scheduling ||= {});
  CW.Scheduling.load = CW.Scheduling.load || (typeof window.loadScheduling==='function' ? ()=>window.loadScheduling() : ()=>{});

  (window.CW.UX ||= {});
  CW.UX.initWallInteractions = CW.UX.initWallInteractions || (Legacy.initWallInteractions ? ()=>Legacy.initWallInteractions() : ()=>{});


  // Global inline handler shim
  if (typeof window.toggleDetails !== "function") {
    window.toggleDetails = function(btnOrId, id){
      var btn = (typeof btnOrId === "string") ? null : btnOrId;
      var panel = id ? document.getElementById(id) : (btn && btn.nextElementSibling);
      if (!panel) return false;
      panel.classList.toggle("hidden");
      var b = btn || (id ? document.querySelector('[aria-controls="'+id+'"]') : null);
      if (b && b.setAttribute) { var exp = b.getAttribute("aria-expanded")==="true"; b.setAttribute("aria-expanded", String(!exp)); }
      return true;
    };
  }


  if (typeof window.showTab !== "function" && typeof Legacy.showTab === "function") { window.showTab = Legacy.showTab; }

})();