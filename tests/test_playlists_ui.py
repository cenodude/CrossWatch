from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parents[1]


def test_index_html_registers_playlists_page():
    from ui_frontend import get_index_html

    html = get_index_html()
    assert 'id="tab-playlists"' in html
    assert 'id="page-playlists"' in html


def test_core_js_routes_playlists_tab():
    core = (REPO / "assets" / "helpers" / "core.js").read_text(encoding="utf-8")
    assert '"/assets/js/playlists.js"' in core
    assert 'byId("page-playlists")' in core
    assert '"playlists"' in core


def test_playlists_assets_exist():
    assert (REPO / "assets" / "js" / "playlists.js").is_file()


def test_playlists_page_is_modal_first_overview():
    js = (REPO / "assets" / "js" / "playlists.js").read_text(encoding="utf-8")
    css = (REPO / "assets" / "css" / "pages.css").read_text(encoding="utf-8")
    assert "Playlist endpoints" in js
    assert "Mappings" in js
    assert "Activity overview" in js
    assert "New endpoint" in js
    assert "New mapping" in js
    assert 'data-action="endpoint-new">+ New endpoint' not in js
    assert 'data-action="mapping-new" ${need ? "disabled" : ""}' not in js
    assert "Manage rulesets" in js
    assert 'id="pl-rulesets-summary"' not in js
    assert 'id="pl-manage-rulesets"' not in js
    assert 'id="pl-refresh"' not in js
    assert 'data-action="rulesets-manage"' not in js
    assert "function renderRulesetSummary" not in js
    assert 'id="pl-map-manage-rulesets"' in js
    assert "pl-ep-editor" not in js
    assert "pl-map-editor" not in js
    assert '<header class="pl-header">' not in js
    assert "#page-playlists .pl-header{display:flex;" in css
    assert "padding:18px 20px" in css
    assert "#page-playlists .pl-title{margin:0;font-size:28px;line-height:1.1;font-weight:850" in css
    assert "#page-playlists .pl-sub{margin-top:6px;color:var(--pl-soft);font-size:16px" in css
    assert "#page-playlists .pl-header .pl-btn{min-height:0;padding:10px 14px;border-radius:10px;font-size:14px;font-weight:850;gap:8px" in css
    assert '<button class="pl-btn" id="pl-new-endpoint"><span class="material-symbols-rounded" aria-hidden="true">add</span>New endpoint</button>' in js
    assert '<button class="pl-btn" id="pl-new-mapping"' in js
    assert "--pl-shell-bg" in css
    assert "--pl-shell-bg:#171d26" in css
    assert 'html[data-cw-theme="flat-dark"] #page-playlists' in css
    assert 'html[data-cw-theme="flat-light"] #page-playlists' in css


def test_playlists_modals_cover_create_edit_delete_flows():
    js = (REPO / "assets" / "js" / "playlists.js").read_text(encoding="utf-8")
    assert "function openEndpointModal" in js
    assert "function openMappingModal" in js
    assert "function openRulesetManager" in js
    assert "function openRulesetForm" in js
    assert "function openEndpointDelete" in js
    assert "function openMappingDelete" in js
    assert "function openRulesetDelete" in js
    assert "Discard unsaved changes?" in js
    assert "Source and destination endpoints must be different." in js
    assert "Built in rulesets cannot be edited." in js
    assert "Built in rulesets cannot be deleted." in js
    assert "mappingDraft.ruleset_id" in js
    assert 'runPair: (id) => request("/api/run"' in js
    assert "m.assigned_pair" in js
    assert "const NAME_MAX = 10" in js
    assert "const PLAYLIST_NAME_MAX = 20" in js
    assert "SAFE_NAME_CHARS" in js
    assert "function bindNameValidation" in js
    assert "function nameFieldError" in js
    assert "function playlistNameError" in js
    assert "must be ${max} characters or fewer" in js
    assert "safeNameError(name, label, NAME_MAX)" in js
    assert "safeNameError(name, \"New playlist name\", PLAYLIST_NAME_MAX)" in js
    assert "can only use letters, numbers, spaces" in js
    assert "maxlength=\"${PLAYLIST_NAME_MAX}\"" in js
    assert 'id="pl-ep-type"' not in js
    assert "await API.resourceCreate({ provider, instance, name: createName, media_type: mediaType })" in js
    assert "create: true, create_name: createName, media_type: createType" in js
    assert 'placeholder="Enter endpoint name"' in js
    assert 'placeholder="Enter mapping name"' in js
    assert 'placeholder="Weekend"' not in js
    assert 'placeholder="Weekend movies"' not in js
    assert "pl-ep-name-error" in js
    assert "pl-ep-create-name-error" in js
    assert "pl-map-name-error" in js
    assert "pl-rs-name-error" in js


def test_endpoint_modal_uses_guided_playlist_picker():
    js = (REPO / "assets" / "js" / "playlists.js").read_text(encoding="utf-8")
    css = (REPO / "assets" / "css" / "pages.css").read_text(encoding="utf-8")
    assert "function nextSequenceName" in js
    assert "function nextEndpointName" in js
    assert 'nextSequenceName(state.endpoints, "EP"' in js
    assert "root.dataset.epNameDirty" in js
    assert "Name uses the next available endpoint number" in js
    assert "window.CW?.ProfileSelect?.enhanceProvider?.(providerSelect)" in js
    assert "window.CW?.ProfileSelect?.enhanceProfile?.(instanceSelect)" in js
    assert 'id="pl-ep-playlist-search"' in js
    assert 'id="pl-ep-resource-list"' in js
    assert 'id="pl-ep-list-create"' in js
    assert 'id="pl-ep-list-edit"' in js
    assert 'id="pl-ep-list-delete"' in js
    assert "resourceCreate: (body)" in js
    assert "resourceRename: (provider, instance, id, name)" in js
    assert "resourceDelete: (provider, instance, id)" in js
    assert 'root.dataset.epSingleSelect = isEdit ? "1" : "";' in js
    assert 'singleSelect ? "radio_button_checked" : "check_box"' in js
    assert 'singleSelect ? "radio_button_unchecked" : "check_box_outline_blank"' in js
    assert 'id="pl-ep-create-submit"' in js
    assert 'id="pl-ep-create-cancel"' in js
    assert 'id="pl-ep-edit-submit"' in js
    assert 'id="pl-ep-delete-submit"' in js
    assert "Create or cancel the provider playlist before creating the endpoint." in js
    assert "Save or cancel the provider playlist action before creating the endpoint." in js
    assert "createProviderPlaylistFromPanel(root, ctx, isEdit)" in js
    assert "renameProviderPlaylistFromPanel(root, ctx, isEdit)" in js
    assert "deleteProviderPlaylistsFromPanel(root, ctx, isEdit)" in js
    assert "await loadEndpointResources(root, resource.id || \"\", isEdit)" in js
    assert "function resourceBadgeMeta" in js
    assert 'class="pl-resource-badges"' in js
    assert 'class="pl-resource-badge material-symbols-rounded"' in js
    assert "Create a new list instead" not in js
    assert ".pl-endpoint-wizard" in css
    assert ".pl-resource-list" in css
    assert ".pl-resource-badges" in css


def test_mapping_modal_uses_guided_endpoint_picker():
    js = (REPO / "assets" / "js" / "playlists.js").read_text(encoding="utf-8")
    css = (REPO / "assets" / "css" / "pages.css").read_text(encoding="utf-8")
    assert "function generatedMappingName" in js
    assert "function nextMappingName" in js
    assert 'nextSequenceName(state.mappings, "MAP"' in js
    assert "function endpointOptionData" in js
    assert "root.dataset.mapNameDirty" in js
    assert "Name uses the next available mapping number" in js
    assert "updateMappingGeneratedName(root)" in js
    assert 'className: "pl-endpoint-select"' in js
    assert 'menuClassName: "pl-endpoint-select-menu"' in js
    assert 'getOptionData: endpointOptionData' in js
    assert 'const isWatchlist = rawType.includes("watchlist")' in js
    assert 'isWatchlist ? "Watchlist"' in js
    assert "selectedShowNote: false" in js
    assert "selectedData.showNote = data.selectedShowNote" in (REPO / "assets" / "helpers" / "icon-select.js").read_text(encoding="utf-8")
    assert 'class="pl-mapping-wizard"' in js
    assert 'class="pl-map-ruleset-card pl-map-advanced"' in js
    assert 'class="pl-map-toggle"' in js
    assert ".pl-mapping-wizard" in css
    assert ".pl-map-grid" in css
    assert ".pl-map-ruleset-card" in css
    assert ".pl-endpoint-select-menu" in css
    assert ".pl-endpoint-select .cw-icon-select-text" in css


def test_playlist_actions_use_partial_overview_refresh():
    js = (REPO / "assets" / "js" / "playlists.js").read_text(encoding="utf-8")
    assert "async function refreshOverview" in js
    assert "function refreshSection" in js
    assert "function updateMappingActions" in js
    assert 'refreshOverview(["mappings", "activity"])' in js
    assert "await reload(true)" not in js


def test_playlists_initial_load_uses_page_shell_and_section_skeletons():
    js = (REPO / "assets" / "js" / "playlists.js").read_text(encoding="utf-8")
    assert "loaded: false" in js
    assert "loading: false" in js
    assert "const EMPTY_PROVIDER_RETRIES = 2" in js
    assert "function loadPlaylistDataAttempt" in js
    assert "if ((data.providers.providers || []).length || attempt >= EMPTY_PROVIDER_RETRIES) break;" in js
    assert "function renderSkeleton" in js
    assert "state.loading && !state.loaded" in js
    assert "state.loading = true" in js
    assert "Fetching endpoints, mappings, rulesets and activity" not in js


def test_playlists_provider_banner():
    js = (REPO / "assets" / "js" / "playlists.js").read_text(encoding="utf-8")
    assert "PLAYLIST_COMPATIBLE_PROVIDERS" in js
    assert '["PLEX", "TRAKT", "MDBLIST", "JELLYFIN", "EMBY", "PUBLICMETADB", "SIMKL", "CROSSWATCH"]' in js
    assert "Playlists need at least one compatible provider" in js
    assert "Plex, Trakt, MDBList, Jellyfin, Emby, PublicMetaDB, SIMKL or CrossWatch" in js
    assert "Playlists are highly experimental and cause issues" not in js
    assert "SIMKL Custom Lists are not supported" in js
    assert "pl-ep-simkl-warning" in js
    assert "pl-map-simkl-warning" in js
    assert "data-action=\"open-connections\"" in js
    assert "function openConnections" in js


def test_endpoint_and_mapping_tables_use_compact_icon_actions():
    js = (REPO / "assets" / "js" / "playlists.js").read_text(encoding="utf-8")
    css = (REPO / "assets" / "css" / "pages.css").read_text(encoding="utf-8")
    assert "function endpointRef" in js
    assert "function mappingTargetRefs" in js
    assert "function actionButton" in js
    assert "function syncPairIndicator" in js
    assert "Sync pair: ${esc(id)}" in js
    assert "runningEndpoints: new Set()" in js
    assert "syncSummary: null" in js
    assert "function syncSummaryRunning" in js
    assert "function sharedSyncBusy" in js
    assert "function mappingIsRunning" in js
    assert "API.runSummary()" in js
    assert "Synchronization is already running" in js
    assert 'state.syncSummary = { ...(state.syncSummary || {}), running: true, pair_scope_ids: [String(mapping.assigned_pair || "")] };' in js
    assert "async function syncEndpoint" in js
    assert '"endpoint-sync"' in js
    assert "await API.epSync(id)" in js
    assert ".pl-action-btn.sync" in css
    assert ".pl-action-btn.refresh" in css
    assert ".pl-action-btn.edit" in css
    assert ".pl-action-btn.delete" in css
    assert ".pl-icon-status.ok" in css
    assert ".pl-state.ok" in css
    assert '<select id="pl-map-targets">${selectOptions(endpointOpts, target)}</select>' in js
    assert '<select id="pl-map-targets" multiple' not in js
    assert 'target_endpoints: val("#pl-map-targets", root) ? [val("#pl-map-targets", root)] : []' in js
    assert 'data-action="endpoint-clone"' not in js
    assert 'data-action="mapping-clone"' not in js
    assert "assigned_pair_label" not in js
    assert '<table class="pl-endpoints-table">' in js
    assert '<table class="pl-mappings-table">' in js
    assert '<th>Mapping</th><th>Source</th><th>Direction</th><th>Destination</th><th>Ruleset</th><th>Sync pair</th><th>Status</th><th>Result</th>' in js
    assert "<td>${syncPairIndicator(m.assigned_pair)}</td>" in js
    assert 'class="pl-result-cell"' in js
    assert '<th aria-label="Actions">Actions</th>' in js
    assert '<th>Actions</th></tr></thead>' in js
    assert "#page-playlists .pl-endpoints-table{table-layout:fixed;min-width:0}" in css
    assert "#page-playlists .pl-endpoints-table td:nth-child(8) .pl-actions{justify-content:flex-start}" in css
    assert "#page-playlists .pl-mappings-table{width:100%!important;table-layout:fixed;min-width:0!important}" in css
    assert "#page-playlists .pl-mappings-table th,#page-playlists .pl-mappings-table td{box-sizing:border-box;padding-left:10px;padding-right:10px;overflow:hidden}" in css
    assert "#page-playlists .pl-mappings-table th:nth-child(4),#page-playlists .pl-mappings-table td:nth-child(4){width:9%}" in css
    assert "#page-playlists .pl-mappings-table th:nth-child(5),#page-playlists .pl-mappings-table td:nth-child(5){width:7%}" in css
    assert "#page-playlists .pl-mappings-table th:nth-child(6),#page-playlists .pl-mappings-table td:nth-child(6){width:5%}" in css
    assert "#page-playlists .pl-mappings-table th:nth-child(8),#page-playlists .pl-mappings-table td:nth-child(8){width:19%}" in css
    assert "#page-playlists .pl-mappings-table th:nth-child(9),#page-playlists .pl-mappings-table td:nth-child(9){width:14%;text-align:left;overflow:visible}" in css
    assert "#page-playlists .pl-mappings-table td:nth-child(9) .pl-actions{justify-content:flex-start;gap:6px}" in css
    assert "#page-playlists .pl-mappings-table td:nth-child(9) .pl-action-btn{width:34px;height:34px}" in css
    assert "#page-playlists .pl-mappings-table .pl-muted{white-space:nowrap;overflow:hidden;text-overflow:ellipsis}" in css


def test_playlists_overview_uses_dashboard_table_body():
    js = (REPO / "assets" / "js" / "playlists.js").read_text(encoding="utf-8")
    css = (REPO / "assets" / "css" / "pages.css").read_text(encoding="utf-8")
    assert "function endpointIdentity" in js
    assert "function statCard" in js
    assert "function activityChangeSummary" in js
    assert "function activityStatus" in js
    assert 'if (counts.unresolved) chips.push(activityChangeChip("Unresolved", counts.unresolved, "help", "unresolved"));' in js
    assert 'chips.length ? chips.join("") : `<span class="pl-muted">No changes</span>`' in js
    assert 'const unresolved = Number(result.unresolved_count || result.unresolved || 0);' in js
    assert 'warningBits.push(`${unresolved} unresolved`)' in js
    assert "function openActivityClear" in js
    assert "activityClear: ()" in js
    assert 'data-action="activity-clear"' in js
    assert '<table class="pl-activity-table">' in js
    assert '<th>Time</th><th>Mapping</th><th>Result</th><th>Changes</th><th>Status</th>' in js
    assert "<th>Changes</th>" in js
    assert 'class="pl-change-chip' in js
    assert "pl-entity-icon" in js
    assert 'class="pl-state ok"' in js
    assert 'class="pl-btn small accent"' in js
    assert "#page-playlists .pl-grid>.pl-section" in css
    assert "#page-playlists .pl-entity" in css
    assert "#page-playlists .pl-state" in css
    assert "#page-playlists .pl-stat-icon" in css
    assert "#page-playlists .pl-activity-table{width:100%!important;table-layout:fixed;min-width:0!important}" in css
    assert "#page-playlists .pl-activity-table th:nth-child(4),#page-playlists .pl-activity-table td:nth-child(4){width:18%}" in css
    assert "#page-playlists .pl-activity-table th:nth-child(5),#page-playlists .pl-activity-table td:nth-child(5){width:22%;overflow:visible}" in css
    assert "#page-playlists .pl-change-set" in css
    assert "#page-playlists .pl-change-chip" in css
    assert "#page-playlists .pl-change-chip.unresolved" in css


def test_discovery_mapping_modal_collapses_advanced_options():
    js = (REPO / "assets" / "js" / "playlists.js").read_text(encoding="utf-8")
    assert "function endpointIsDiscovery" in js
    assert 'id="pl-map-discovery-help"' in js
    assert '["#pl-map-direction", "#pl-map-ruleset", "#pl-map-membership", "#pl-map-order"]' in js
    assert 'el.classList.toggle("hidden", sourceDiscovery)' in js
    assert '$("#pl-map-ruleset", root).value = ""' in js
    assert '$("#pl-map-membership", root).value = "mirror"' in js
    assert "Discovery sources use direct mirror mappings." in js


def test_playlist_modals_use_styled_scrollbars_and_muted_selection():
    css = (REPO / "assets" / "css" / "pages.css").read_text(encoding="utf-8")
    assert "--pl-scroll-track" in css
    assert "--pl-scroll-thumb" in css
    assert "--pl-select-active-bg" in css
    assert ".pl-dialog-body::-webkit-scrollbar" in css
    assert ".pl-dialog .pl-table-wrap::-webkit-scrollbar" in css
    assert ".pl-field select[multiple]::-webkit-scrollbar" in css
    assert "max-height:min(34vh,260px)" in css
    assert "box-shadow:inset 3px 0 0 var(--pl-green)" not in css
    assert "box-shadow:inset 3px 0 0 #8d72ff" not in css
    assert "#4167b7" not in css


def test_pair_overlay_playlist_managed_pairs_open_playlist_mapping():
    js = (REPO / "assets" / "js" / "connections.pairs.overlay.js").read_text(encoding="utf-8")
    css = (REPO / "assets" / "css" / "components.css").read_text(encoding="utf-8")
    assert "const isPlaylistManagedPair" in js
    assert "window.cxOpenPlaylistMappingsForPair = openPlaylistMappingsForPair" in js
    assert "board.innerHTML = pairs.map((pair, index) => renderPairCard(pair, index + 1)).join(\"\");" in js
    assert 'board.querySelectorAll(".pair-card")' in js
    assert "Sync order" in js
    assert "Playlist pairs" not in js
    assert "async function setPlaylistManagedPairEnabled" in js
    assert "function playlistMappingPayload" in js
    assert "await setPlaylistManagedPairEnabled(id, on, it)" in js
    assert 'fetch("/api/playlists/mappings", {' in js
    assert "async function deletePlaylistManagedPair" in js
    assert 'fetch(`/api/playlists/mappings/${encodeURIComponent(mapping.id)}`' in js
    assert "isPlaylistManagedPair(pair)) return openPlaylistMappingsForPair(id, btn)" in js
    assert 'pair-pill mode playlist-managed' not in js
    assert 'data-tip="Managed by Playlists"' not in js
    assert '${managedPlaylist ? ""' not in js
    assert "window.showTab(\"playlists\")" in js
    assert "api?.openMappingForPair" in js
    assert "returnToSyncPairs: true" in js


def test_playlists_module_exposes_pair_mapping_modal_entrypoint():
    js = (REPO / "assets" / "js" / "playlists.js").read_text(encoding="utf-8")
    pair_modal = (REPO / "assets" / "js" / "modals" / "pair-config" / "index.js").read_text(encoding="utf-8")
    assert "pairMappings: (id) => request(`${BASE}/pairs/${encodeURIComponent(id)}/mappings`)" in js
    assert "async function openMappingForPair" in js
    assert "function returnToSyncPairsOverview" in js
    assert 'window.showTab("settings")' in js
    assert "function renderManagedPlaylistPairModal" in pair_modal
    assert "isManagedPlaylistPair(pair)" in pair_modal
    assert "Open playlist mapping" in pair_modal
    assert 'window.cwSettingsSelect("sync")' in js
    assert "openMappingModal({ mapping, trigger, onDone:" in js
    assert "returnToSyncPairs ? returnToSyncPairsOverview : null" in js
    assert "mappingDone: ctx.opts.onDone" in js
    assert "window.Playlists = { mount: init, openMappingForPair }" in js


def test_pair_config_hides_playlist_feature_tab():
    rules = (REPO / "assets" / "js" / "modals" / "pair-config" / "custom-rules.js").read_text(encoding="utf-8")
    flow = (REPO / "assets" / "js" / "modals" / "pair-config" / "flow.js").read_text(encoding="utf-8")
    pair_modal = (REPO / "assets" / "js" / "modals" / "pair-config" / "index.js").read_text(encoding="utf-8")

    assert 'const keys = ["watchlist", "ratings", "history", "progress", "collection"];' in rules
    assert '["playlists", "pl"]' not in flow
    assert 'sharedFeatureOrder().filter((key) => String(key || "").toLowerCase() !== "playlists")' in flow
    assert "function renderManagedPlaylistPairModal" in pair_modal
    assert "Open playlist mapping" in pair_modal


def test_main_hub_treats_playlists_as_first_class_feature():
    main = (REPO / "assets" / "js" / "main.js").read_text(encoding="utf-8")
    css = (REPO / "assets" / "crosswatch.css").read_text(encoding="utf-8")
    assert '["playlists", "queue_music"]' in main
    assert "progress: true, playlists: true" in main
    assert 'const getDisplayFeats = () => FEATS' in main
    assert 'enabled.progress ? "progress" : "playlists"' not in main
    assert "lanes-count-${displayFeats.length}" in main
    assert ".lanes.lanes-count-5{grid-template-columns:repeat(6,minmax(0,1fr))}" in css
    assert ".lanes.lanes-count-5>.lane:nth-child(-n+2){grid-column:span 3}" in css
    assert ".lanes.lanes-count-5>.lane:nth-child(n+3){grid-column:span 2}" in css


def test_insights_settings_enables_playlist_statistics():
    insights = (REPO / "assets" / "js" / "insights.js").read_text(encoding="utf-8")
    modal = (REPO / "assets" / "js" / "modals" / "insight-settings" / "index.js").read_text(encoding="utf-8")
    assert "playlists: f.playlists !== false" in insights
    assert "playlists: f.playlists !== false" in modal
    assert "Show playlist sync tiles." in modal
    assert "Not supported currently." not in modal
    assert "key === \"playlists\"" not in modal
    assert 'seg.dataset.count = String(Math.max(1, _visibleFeats.length))' in insights


def test_insights_playlist_statistics_use_endpoint_counts():
    api = (REPO / "api" / "insightAPI.py").read_text(encoding="utf-8")
    svc = (REPO / "services" / "playlists.py").read_text(encoding="utf-8")
    assert "def provider_count_summary" in svc
    assert "playlists_svc.provider_count_summary(cfg)" in api
    assert "def _playlist_endpoint_provider_counts" not in api
    assert "def _playlist_mapping_provider_counts" not in api
    assert 'providers_by_feature.setdefault("playlists"' in api
    assert 'providers_instances_by_feature.setdefault("playlists"' in api


def test_insights_main_load_uses_lightweight_stats_first():
    insights = (REPO / "assets" / "js" / "insights.js").read_text(encoding="utf-8")
    api = (REPO / "api" / "insightAPI.py").read_text(encoding="utf-8")

    assert "refreshInsightsFastThenFull" in insights
    assert "/api/insights?limit_samples=0&history=0" in insights
    assert "return refreshInsightsFastThenFull()" in insights
    assert "optimisticConfigured" in insights
    assert "configuredProvidersSnapshot(blk.active)" in insights
    assert "_configuredProvidersCache" in insights
    assert "new Set(Object.entries(active || {}).filter" in insights
    assert "new Set([...Object.keys(blk.providers || {}), ...Object.keys(blk.active || {})])" not in insights
    assert "const configured = await getConfiguredProviders" not in insights
    assert 'if sample_limit > 0:' in api
    assert "samples = []" in api
    assert "history_limit = max(0, int(history))" in api
    assert "if history_limit > 0 else []" in api


def test_playlist_runner_emits_live_summary_events():
    runner = (REPO / "cw_platform" / "playlists_runner.py").read_text(encoding="utf-8")
    assert '"apply:add:done"' in runner
    assert '"apply:remove:done"' in runner
    assert '"apply:update:done"' in runner
    assert 'feature="playlists"' in runner
    assert "def _spotlight_items" in runner


def test_ruleset_modal_uses_guided_visual_builder():
    js = (REPO / "assets" / "js" / "playlists.js").read_text(encoding="utf-8")
    css = (REPO / "assets" / "css" / "pages.css").read_text(encoding="utf-8")
    assert "RULESET_PRESETS" in js
    assert "Direct sync" in js
    assert "Mirror source" in js
    assert "Split large playlists" in js
    assert "Merge playlists" in js
    assert "Limited account sharing" in js
    assert "function rulesetBuilderHtml" in js
    assert "function detectRulesetPreset" in js
    assert "function rulesetPreview" in js
    assert "function validateRulesetBuilder" in js
    assert "Readable summary" in js
    assert "Advanced policies" in js
    assert "Source item count" in js
    assert "Split into target lists" in js
    assert "data-rs-field" in js
    assert ".pl-builder-grid .pl-field{align-content:start}" in css
    assert ".pl-builder-grid :is(input,select,.cw-icon-select-btn){height:44px;min-height:44px}" in css
    assert "function rulesetActionButton" in js
    assert 'rulesetActionButton("view", rs.id, "View ruleset", "visibility")' in js
    assert 'rulesetActionButton("clone", rs.id, "Clone ruleset", "content_copy", "sync")' in js
    assert '<table class="pl-ruleset-table">' in js
    assert "<th>Direction</th>" in js
    assert "Direction support" not in js
    assert "<th>Mappings</th>" in js
    assert "Mappings using it" not in js
    assert ".pl-ruleset-table{table-layout:fixed;min-width:0}" in css
    assert ".pl-ruleset-table{width:100%;min-width:0!important}" in css
    assert ".pl-ruleset-table th,#page-playlists .pl-ruleset-table td{box-sizing:border-box" in css
    assert "td:nth-child(1){width:17%}" in css
    assert "td:nth-child(4){width:17%}" in css
    assert "td:nth-child(5){width:17%}" in css
    assert "td:nth-child(2){width:10%;overflow:visible;text-overflow:clip}" in css
    assert "td:nth-child(7) .pl-action-btn{width:34px;height:34px}" in css


def test_ruleset_builder_preserves_payload_shape():
    js = (REPO / "assets" / "js" / "playlists.js").read_text(encoding="utf-8")
    for key in [
        "direction",
        "initial_sync",
        "read_mode",
        "write_mode",
        "membership",
        "order",
        "deduplicate",
        "allocation",
        "rebalance",
        "overflow",
        "per_endpoint_capacity",
        "aggregate_capacity",
        "maximum_targets",
        "track_assignments",
    ]:
        assert f"{key}:" in js
    assert "pl-limit-partition" in js
    assert "pl-limit-aggregate" in js
    assert "The current backend only supports blocking overflow." in js


def test_playlists_api_routes_registered():
    from api.playlistsAPI import router

    paths = {r.path for r in router.routes}
    assert "/api/playlists/providers" in paths
    assert "/api/playlists/resources" in paths
    assert any(r.path == "/api/playlists/resources" and "POST" in (r.methods or set()) for r in router.routes)
    assert any(r.path == "/api/playlists/resources/{playlist_id}" and "PATCH" in (r.methods or set()) for r in router.routes)
    assert any(r.path == "/api/playlists/resources/{playlist_id}" and "DELETE" in (r.methods or set()) for r in router.routes)
    assert "/api/playlists/endpoints" in paths
    assert "/api/playlists/mappings" in paths
    assert "/api/playlists/overview" in paths
    assert any(r.path == "/api/playlists/activity" and "DELETE" in (r.methods or set()) for r in router.routes)
    assert "/api/playlists/rulesets" in paths
    assert "/api/playlists/rulesets/{ruleset_id}" in paths
    assert "/api/playlists/rulesets/{ruleset_id}/clone" in paths
    assert "/api/playlists/mappings/{mapping_id}/preview" in paths
    assert "/api/playlists/mappings/{mapping_id}/run" in paths
    assert "/api/playlists/pairs/{pair_id}/mappings" in paths
