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
    assert "Playlist endpoints" in js
    assert "Mappings" in js
    assert "Activity overview" in js
    assert "+ New endpoint" in js
    assert "+ New mapping" in js
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
    assert "#page-playlists .pl-header{position:static;" in js
    assert "padding:18px 20px" in js
    assert "#page-playlists .pl-title{margin:0;font-size:28px;line-height:1.1;font-weight:850" in js
    assert "#page-playlists .pl-sub{margin-top:6px;color:var(--pl-soft);font-size:16px" in js
    assert "#page-playlists .pl-header .pl-btn{min-height:0;padding:10px 14px;border-radius:10px;font-size:14px;font-weight:850;gap:8px}" in js
    assert '<button class="pl-btn" id="pl-new-endpoint"><span class="material-symbols-rounded" aria-hidden="true">add</span>New endpoint</button>' in js
    assert '<button class="pl-btn" id="pl-new-mapping"' in js
    assert "--pl-shell-bg" in js
    assert "--pl-shell-bg:#171d26" in js
    assert 'html[data-cw-theme="flat-dark"] #page-playlists' in js
    assert 'html[data-cw-theme="flat-light"] #page-playlists' in js


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
    assert "media_type: mediaType" not in js
    assert 'placeholder="Endpoint name"' in js
    assert 'placeholder="Mapping name"' in js
    assert 'placeholder="Weekend"' not in js
    assert 'placeholder="Weekend movies"' not in js
    assert "pl-ep-name-error" in js
    assert "pl-ep-create-name-error" in js
    assert "pl-map-name-error" in js
    assert "pl-rs-name-error" in js


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
    assert "function renderSkeleton" in js
    assert "state.loading && !state.loaded" in js
    assert "state.loading = true" in js
    assert "Fetching endpoints, mappings, rulesets and activity" not in js


def test_playlists_provider_and_experimental_banners():
    js = (REPO / "assets" / "js" / "playlists.js").read_text(encoding="utf-8")
    assert "PLAYLIST_COMPATIBLE_PROVIDERS" in js
    assert '["PLEX", "TRAKT", "MDBLIST", "JELLYFIN", "EMBY", "PUBLICMETADB", "SIMKL"]' in js
    assert "Playlists need at least one compatible provider" in js
    assert "Plex, Trakt, MDBList, Jellyfin, Emby, PublicMetaDB or SIMKL" in js
    assert "Playlists are highly experimental and cause issues" in js
    assert "SIMKL Custom Lists are not supported" in js
    assert "pl-ep-simkl-warning" in js
    assert "pl-map-simkl-warning" in js
    assert "data-action=\"open-connections\"" in js
    assert "function openConnections" in js


def test_endpoint_and_mapping_tables_use_compact_icon_actions():
    js = (REPO / "assets" / "js" / "playlists.js").read_text(encoding="utf-8")
    assert "function endpointRef" in js
    assert "function mappingTargetRefs" in js
    assert "function actionButton" in js
    assert ".pl-action-btn.sync" in js
    assert ".pl-action-btn.edit" in js
    assert ".pl-action-btn.delete" in js
    assert '<select id="pl-map-targets">${selectOptions(endpointOpts, target)}</select>' in js
    assert '<select id="pl-map-targets" multiple' not in js
    assert 'target_endpoints: val("#pl-map-targets", root) ? [val("#pl-map-targets", root)] : []' in js
    assert 'data-action="endpoint-clone"' not in js
    assert 'data-action="mapping-clone"' not in js
    assert "assigned_pair_label" not in js
    assert '<th aria-label="Actions"></th>' in js
    assert '<th>Actions</th></tr></thead>' in js


def test_playlist_modals_use_styled_scrollbars_and_muted_selection():
    js = (REPO / "assets" / "js" / "playlists.js").read_text(encoding="utf-8")
    assert "--pl-scroll-track" in js
    assert "--pl-scroll-thumb" in js
    assert "--pl-select-active-bg" in js
    assert ".pl-dialog-body::-webkit-scrollbar" in js
    assert ".pl-dialog .pl-table-wrap::-webkit-scrollbar" in js
    assert ".pl-field select[multiple]::-webkit-scrollbar" in js
    assert "max-height:min(34vh,260px)" in js
    assert "box-shadow:inset 3px 0 0 var(--pl-green)" in js
    assert "#4167b7" not in js


def test_pair_overlay_playlist_manage_button_requires_enabled_playlist_feature():
    js = (REPO / "assets" / "js" / "connections.pairs.overlay.js").read_text(encoding="utf-8")
    assert "const hasPlaylistMappings" in js
    assert "window.cxOpenPlaylistMappingsForPair = openPlaylistMappingsForPair" in js
    assert '${hasPlaylistMappings(f.playlists) ? `<button class="icon-btn" data-tip="Manage playlist mappings"' in js
    assert '${f.playlists ? `<button class="icon-btn" data-tip="Manage playlist mappings"' not in js
    assert "window.showTab(\"playlists\")" in js
    assert "api?.openMappingForPair" in js
    assert "returnToSyncPairs: true" in js


def test_playlists_module_exposes_pair_mapping_modal_entrypoint():
    js = (REPO / "assets" / "js" / "playlists.js").read_text(encoding="utf-8")
    assert "pairMappings: (id) => request(`${BASE}/pairs/${encodeURIComponent(id)}/mappings`)" in js
    assert "async function openMappingForPair" in js
    assert "function returnToSyncPairsOverview" in js
    assert 'window.showTab("settings")' in js
    assert 'window.cwSettingsSelect("sync")' in js
    assert "openMappingModal({ mapping, trigger, onDone:" in js
    assert "returnToSyncPairs ? returnToSyncPairsOverview : null" in js
    assert "mappingDone: ctx.opts.onDone" in js
    assert "window.Playlists = { mount: init, openMappingForPair }" in js


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


def test_playlist_runner_emits_live_summary_events():
    runner = (REPO / "cw_platform" / "playlists_runner.py").read_text(encoding="utf-8")
    assert '"apply:add:done"' in runner
    assert '"apply:remove:done"' in runner
    assert '"apply:update:done"' in runner
    assert 'feature="playlists"' in runner
    assert "def _spotlight_items" in runner


def test_ruleset_modal_uses_guided_visual_builder():
    js = (REPO / "assets" / "js" / "playlists.js").read_text(encoding="utf-8")
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
    assert "/api/playlists/endpoints" in paths
    assert "/api/playlists/mappings" in paths
    assert "/api/playlists/overview" in paths
    assert "/api/playlists/rulesets" in paths
    assert "/api/playlists/rulesets/{ruleset_id}" in paths
    assert "/api/playlists/rulesets/{ruleset_id}/clone" in paths
    assert "/api/playlists/mappings/{mapping_id}/preview" in paths
    assert "/api/playlists/mappings/{mapping_id}/run" in paths
    assert "/api/playlists/pairs/{pair_id}/mappings" in paths
