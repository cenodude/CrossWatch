from __future__ import annotations

from typing import Any

import pytest
from fastapi import HTTPException

from api import editorAPI as api
from cw_platform.playlists import PLAYLIST_KIND_SMART, PlaylistItem, PlaylistResource, PlaylistSnapshot


class FakePlaylistOps:
    def __init__(self, resource: PlaylistResource | None = None):
        self.resource = resource or PlaylistResource(
            provider="PLEX",
            id="pl1",
            name="Movies",
            can_add=True,
            can_remove=True,
            can_reorder=True,
            media_types=("movie", "show"),
        )
        self.items = [
            {"type": "movie", "title": "One", "ids": {"tmdb": "1"}},
            {"type": "movie", "title": "Two", "ids": {"tmdb": "2"}},
        ]
        self.calls: list[tuple[str, Any]] = []

    def list_playlist_resources(self, cfg, *, instance=None):
        return [self.resource]

    def get_playlist_snapshot(self, cfg, playlist_id, *, instance=None):
        return PlaylistSnapshot(
            resource=self.resource,
            items=[PlaylistItem.from_media(item, position=i) for i, item in enumerate(self.items)],
        )

    def create_playlist(self, cfg, name, *, media_type=None, instance=None, dry_run=False):
        return self.resource

    def add_playlist_items(self, cfg, playlist_id, items, *, instance=None, dry_run=False):
        self.calls.append(("add", [dict(x) for x in items]))
        self.items.extend(dict(x) for x in items)
        return {"ok": True, "count": len(items), "unresolved": []}

    def remove_playlist_items(self, cfg, playlist_id, items, *, instance=None, dry_run=False):
        self.calls.append(("remove", [dict(x) for x in items]))
        remove_ids = {str((x.get("ids") or {}).get("tmdb") or "") for x in items}
        self.items = [x for x in self.items if str((x.get("ids") or {}).get("tmdb") or "") not in remove_ids]
        return {"ok": True, "count": len(items), "unresolved": []}

    def reorder_playlist_items(self, cfg, playlist_id, ordered_keys, *, instance=None, dry_run=False):
        self.calls.append(("reorder", list(ordered_keys)))
        return {"ok": True, "reordered": len(ordered_keys)}


def _endpoint() -> dict[str, Any]:
    return {
        "id": "EP-01",
        "name": "Films",
        "provider": "PLEX",
        "provider_label": "Plex",
        "instance": "default",
        "playlist_id": "pl1",
        "playlist_name": "Movies",
    }


@pytest.fixture
def editor_playlist(monkeypatch):
    ops = FakePlaylistOps()
    cfg = {"playlists": {"endpoints": [_endpoint()]}}
    monkeypatch.setattr(api, "load_config", lambda: cfg)
    monkeypatch.setattr(api.playlist_svc, "list_endpoints", lambda _cfg: [_endpoint()])
    monkeypatch.setattr(api.playlists_runner, "get_endpoint", lambda _cfg, eid: _endpoint() if eid == "EP-01" else None)
    monkeypatch.setattr(api, "load_sync_ops", lambda provider: ops if provider == "PLEX" else None)
    monkeypatch.setattr(api, "build_provider_config_view", lambda cfg, provider, instance: {"provider": provider, "instance": instance})
    return ops


def test_editor_lists_playlist_endpoints(editor_playlist) -> None:
    data = api.api_editor_playlist_endpoints()
    assert data["ok"] is True
    assert data["endpoints"][0]["id"] == "EP-01"


def test_editor_loads_playlist_endpoint_snapshot(editor_playlist) -> None:
    data = api.api_editor_get_state(kind="watchlist", source="playlist", endpoint="EP-01")
    assert data["source"] == "playlist"
    assert data["resource"]["can_add"] is True
    assert data["resource"]["media_types"] == ["movie", "show"]
    assert set(data["items"]) == {"tmdb:1", "tmdb:2"}
    assert data["original_keys"] == ["tmdb:1", "tmdb:2"]


def test_editor_saves_playlist_diff_through_provider_ops(editor_playlist) -> None:
    result = api.api_editor_save_state(
        {
            "source": "playlist",
            "endpoint": "EP-01",
            "items": {
                "tmdb:2": {"type": "movie", "title": "Two", "ids": {"tmdb": "2"}},
                "tmdb:3": {"type": "movie", "title": "Three", "ids": {"tmdb": "3"}},
            },
        }
    )
    assert result["planned_additions"] == 1
    assert result["planned_removals"] == 1
    assert result["added"] == 1
    assert result["removed"] == 1
    assert any(call[0] == "add" and call[1][0]["ids"]["tmdb"] == "3" for call in editor_playlist.calls)
    assert any(call[0] == "remove" for call in editor_playlist.calls)


def test_editor_blocks_readonly_playlist_endpoint(monkeypatch, editor_playlist) -> None:
    editor_playlist.resource = PlaylistResource(
        provider="PLEX",
        id="pl1",
        name="Smart",
        kind=PLAYLIST_KIND_SMART,
        can_read=True,
        can_add=False,
        can_remove=False,
        can_reorder=False,
    )
    with pytest.raises(HTTPException) as exc:
        api.api_editor_save_state(
            {
                "source": "playlist",
                "endpoint": "EP-01",
                "items": {"tmdb:1": {"type": "movie", "title": "One", "ids": {"tmdb": "1"}}},
            }
        )
    assert exc.value.status_code == 400


def test_editor_ui_exposes_playlist_source() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    js = (root / "assets" / "js" / "editor.js").read_text(encoding="utf-8")
    chrome_js = (root / "assets" / "js" / "editor" / "chrome.js").read_text(encoding="utf-8")
    load_js = (root / "assets" / "js" / "editor" / "load-controller.js").read_text(encoding="utf-8")
    sources_js = (root / "assets" / "js" / "editor" / "sources.js").read_text(encoding="utf-8")
    persistence_js = (root / "assets" / "js" / "editor" / "persistence.js").read_text(encoding="utf-8")
    assert 'sourceSelect.querySelector(\'option[value="playlist"]\')?.remove();' in chrome_js
    assert 'option[value="manual"]' in chrome_js
    assert "Manual Overrides" in chrome_js
    assert "/api/editor/playlists/endpoints" in sources_js
    assert "function hasPlaylistEndpoints(state)" in sources_js
    assert "state.playlistEndpointsLoaded = true;" in sources_js
    assert "if (showPlaylist && !sourceSel.querySelector('option[value=\"playlist\"]'))" in sources_js
    assert "payload.endpoint = state.snapshot" in persistence_js
    assert "state.playlistOriginalKeys" in load_js
    assert 'const SOURCES = ["state", "manual", "playlist"];' in sources_js
    assert "/api/editor/tracker/workspaces" not in sources_js
    assert "state.source = ctx.normalizeSource(state.source);" in load_js


def test_editor_ui_allows_extra_policy_corrections() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    js = (root / "assets" / "js" / "editor.js").read_text(encoding="utf-8")
    table_js = (root / "assets" / "js" / "editor" / "table.js").read_text(encoding="utf-8")
    load_js = (root / "assets" / "js" / "editor" / "load-controller.js").read_text(encoding="utf-8")
    assert "function promoteBaselineEdit(row)" in js
    assert "function manualCorrectionKindLabel" in js
    assert 'if (k === "history") return "history date";' in js
    assert 'if (k === "ratings") return "rating";' in js
    assert "Save changes to use it on the next sync." in js
    assert "const status = extraCorrectionStatus(row, promoted);" in js
    assert 'call(ctx, "isExtraKindEditable")' in table_js
    assert 'row._origin = manualKeys.has(rowKey) ? "manual"' in load_js


def test_editor_manual_override_source_loads_policy_only(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(api, "_STATE_BASE", tmp_path)

    api.api_editor_save_state(
        {
            "source": "manual",
            "kind": "history",
            "provider": "TRAKT",
            "provider_instance": "default",
            "items": {
                "tmdb:76479#s01e01": {
                    "type": "episode",
                    "series_title": "The Boys",
                    "season": 1,
                    "episode": 1,
                    "watched_at": "2019-07-25T08:02:00.000Z",
                    "show_ids": {"tmdb": "76479"},
                }
            },
            "blocks": ["tmdb:76479#s01e02"],
        }
    )

    data = api.api_editor_get_state(kind="history", source="manual", provider="TRAKT")

    assert data["source"] == "manual"
    assert data["provider"] == "TRAKT"
    assert list(data["items"]) == ["tmdb:76479#s01e01"]
    assert data["manual_blocks"] == ["tmdb:76479#s01e02"]


def test_editor_ui_displays_percent_only_progress() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    js = (root / "assets" / "js" / "editor.js").read_text(encoding="utf-8")
    datetime_js = (root / "assets" / "js" / "editor" / "datetime.js").read_text(encoding="utf-8")
    extra_js = (root / "assets" / "js" / "editor" / "extra-editors.js").read_text(encoding="utf-8")
    assert "function progressPercentValue(raw)" in js
    assert "function progressPercentValue(raw)" in datetime_js
    assert '"progress_percent", "progressPercent", "percent", "position_percent", "resume_percent"' in datetime_js
    assert "if (percent != null) label = dt.formatProgressPercent" in extra_js
    assert "row.raw.progress_percent = row.raw.progress_ms != null" in extra_js
    assert "cw-progress-edit-grid" in extra_js
    assert "cw-progress-percent-suffix" in extra_js


def test_editor_ui_exposes_raw_fields_modal() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    js = (root / "assets" / "js" / "editor.js").read_text(encoding="utf-8")
    table_js = (root / "assets" / "js" / "editor" / "table.js").read_text(encoding="utf-8")
    modals = (root / "assets" / "js" / "modals.js").read_text(encoding="utf-8")
    css = (root / "assets" / "css" / "components.css").read_text(encoding="utf-8")
    raw_modal = root / "assets" / "js" / "modals" / "editor-raw" / "index.js"

    assert raw_modal.exists()
    assert "function openRawFieldsModal(row)" in js
    assert "database" in table_js
    assert "window.openEditorRawModal" in modals
    assert "ModalRegistry.register('editor-raw'" in modals
    assert ".cx-modal-shell:has(#cx-modal.editor-raw-modal)" in css
    assert "#cx-modal.editor-raw-modal .raw-path" in css


def test_editor_refresh_is_source_aware() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    js = (root / "assets" / "js" / "editor.js").read_text(encoding="utf-8")
    load_js = (root / "assets" / "js" / "editor" / "load-controller.js").read_text(encoding="utf-8")
    assert "function syncSelectedScopeFromControls()" in js
    assert "function syncSelectedScopeFromControls(ctx = {})" in load_js
    assert "state.workspace = (ctx.snapSel && ctx.snapSel.value)" not in load_js
    assert 'state.snapshot = "";' in js
    assert "await refreshEditor({ force: true });" in js or "refreshEditor({ force: true });" in js
    assert 'window.addEventListener("sync-complete"' in js


def test_editor_filter_supports_season_episode_queries() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    js = (root / "assets" / "js" / "editor.js").read_text(encoding="utf-8")
    search_js = (root / "assets" / "js" / "editor" / "search.js").read_text(encoding="utf-8")
    core_js = (root / "assets" / "helpers" / "core.js").read_text(encoding="utf-8")
    assert 'await ensurePageModule("editor-search", "/assets/js/editor/search.js", "CrossWatchEditorSearch");' in core_js
    assert 'filterInput.placeholder = "Filter by title / S01 / S01E02 / id...";' in js
    assert 'const editorSearch = requireEditorModule("Search");' in js
    assert "function parseSeasonEpisodeText(value)" in search_js
    assert "function rowSearchHaystack(row, options = {})" in search_js
    assert 'pieces.push(`season ${season}`, `season ${s2}`, `s${s2}`, `s${season}`);' in search_js
    assert 'pieces.push(`s${s2}e${e2}`, `${season}x${episode}`, `${s2}x${e2}`, `season ${season} episode ${episode}`);' in search_js
    table_controller_js = (root / "assets" / "js" / "editor" / "table-controller.js").read_text(encoding="utf-8")
    assert "queryTokens.every(token => haystack.includes(token))" in table_controller_js
    assert "state.page = 0;\n      clearSelection();\n      persistUIState();" not in js
    assert "state.pageRids = [];\n      syncSelectPageCheckbox();\n      clearSelection();" not in js


def test_editor_send_button_opens_visible_modal() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    js = (root / "assets" / "js" / "editor.js").read_text(encoding="utf-8")
    send_js = (root / "assets" / "js" / "editor" / "send-modal.js").read_text(encoding="utf-8")
    core_js = (root / "assets" / "helpers" / "core.js").read_text(encoding="utf-8")
    css = (root / "assets" / "css" / "pages.css").read_text(encoding="utf-8")
    assert 'await ensurePageModule("editor-send-modal", "/assets/js/editor/send-modal.js", "CrossWatchEditorSendModal");' in core_js
    assert 'const editorSendModal = requireEditorModule("SendModal");' in js
    assert "return editorSendModal.open({" in js
    assert 'shell.className = "cw-editor-send-overlay";' in send_js
    assert "Loading providers..." in send_js
    assert "Could not load providers." in send_js
    assert 'PM.logoHtml(p, "cw-editor-send-logo")' in send_js
    assert "function formatSendResult(data, esc)" in send_js
    assert "Send complete" in send_js
    assert "attempted</span>" in send_js
    assert "Provider target" not in js
    assert "Provider target" not in send_js
    assert "Data to send" in send_js
    assert "Send selected data" in send_js
    assert "const currentSourceProviderKey = () =>" in send_js
    assert ".filter(p => providerKey(p) !== sourceKey)" in send_js
    assert "No other configured target provider is available for this view." in send_js
    assert 'body?.scrollTo({ top: body.scrollHeight, behavior: "smooth" });' in send_js
    assert "cw-editor-send-result-icon" in send_js
    assert ".cw-editor-send-overlay{position:fixed" in css
    assert ".cw-editor-send-provider.active .cw-editor-send-check" in css
    assert ".cw-editor-send-list{display:grid;grid-template-columns:repeat(3,minmax(0,1fr))" in css
    assert ".cw-editor-send-progress-bar" in css
    assert ".cw-editor-send-run-card" in css
    assert ".cw-editor-send-result-icon" in css
    assert ".cw-editor-send-card .body::-webkit-scrollbar-thumb" in css


def test_editor_send_failure_does_not_expose_exception_detail(monkeypatch) -> None:
    class FailingOps:
        pass

    monkeypatch.setattr(api, "load_config", lambda: {})
    monkeypatch.setattr(
        api,
        "_editor_send_targets",
        lambda cfg, feature: [
            {
                "provider": "TRAKT",
                "instance": "default",
                "history_enabled": True,
            }
        ],
    )
    monkeypatch.setattr(api, "load_sync_ops", lambda provider: FailingOps())
    monkeypatch.setattr(api, "build_provider_config_view", lambda cfg, provider, instance: {})

    def fail_apply_add(**kwargs):
        raise RuntimeError("stack trace with token=super-secret")

    monkeypatch.setattr(api, "apply_add", fail_apply_add)

    res = api.api_editor_send(
        {
            "kind": "history",
            "providers": [{"provider": "TRAKT", "instance": "default"}],
            "items": [{"type": "movie", "title": "Heat", "ids": {"tmdb": "949"}, "watched_at": "2026-01-01T00:00:00Z"}],
        }
    )

    assert res["ok"] is False
    assert res["errors"] == 1
    assert res["results"] == [{"provider": "TRAKT", "instance": "default", "ok": False, "error": "send_failed"}]
    assert "super-secret" not in str(res)


def test_editor_bulk_action_buttons_use_playback_action_colors() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    css = (root / "assets" / "css" / "pages.css").read_text(encoding="utf-8")
    assert "#page-editor .cw-bulk .cw-btn.cw-icon-only{box-sizing:border-box !important;width:44px !important;min-width:44px !important;height:44px !important;min-height:44px !important;padding:0 !important;border-radius:13px !important" in css
    assert "#page-editor .cw-bulk #cw-bulk-send.cw-icon-only{background:linear-gradient(180deg,rgba(63,126,255,0.56),rgba(35,78,188,0.74))!important" in css
    assert "#page-editor .cw-bulk #cw-bulk-restore.cw-icon-only{background:linear-gradient(180deg,rgba(64,166,105,0.48),rgba(34,119,76,0.68))!important" in css
    assert "#page-editor .cw-bulk #cw-bulk-remove.cw-icon-only{background:linear-gradient(180deg,rgba(139,41,64,0.64),rgba(95,29,46,0.78))!important" in css
    assert "#page-editor .cw-bulk #cw-bulk-clear.cw-icon-only{background:rgba(12,16,25,0.34)!important" in css
    assert "#page-editor .cw-bulk .cw-btn.cw-icon-only .material-symbols-rounded{color:currentColor !important;-webkit-text-fill-color:currentColor !important}" in css


def test_editor_datetime_fields_use_utc() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    js = (root / "assets" / "js" / "editor.js").read_text(encoding="utf-8")
    datetime_js = (root / "assets" / "js" / "editor" / "datetime.js").read_text(encoding="utf-8")
    extra_js = (root / "assets" / "js" / "editor" / "extra-editors.js").read_text(encoding="utf-8")
    core_js = (root / "assets" / "helpers" / "core.js").read_text(encoding="utf-8")
    assert 'await ensurePageModule("editor-datetime", "/assets/js/editor/datetime.js", "CrossWatchEditorDateTime");' in core_js
    assert 'await ensurePageModule("editor-extra-editors", "/assets/js/editor/extra-editors.js", "CrossWatchEditorExtraEditors");' in core_js
    assert 'const editorDateTime = requireEditorModule("DateTime");' in js
    assert 'const editorExtraEditors = requireEditorModule("ExtraEditors");' in js
    assert "editorExtraEditors.openHistoryEditor" in js
    assert "editorExtraEditors.openProgressEditor" in js
    assert "editorExtraEditors.openRatingEditor" in js
    assert "d.getUTCFullYear()" in datetime_js
    assert "d.getUTCHours()" in datetime_js
    assert "new Date(Date.UTC(y, m - 1, dDay, hh, mm, 0))" in datetime_js
    assert "Times are shown and saved in UTC." in datetime_js
    assert "function openHistoryEditor(row, anchor, displayEl, ctx = {})" in extra_js
    assert "function openProgressEditor(row, anchor, displayEl, ctx = {})" in extra_js
    assert "function updateExtraDisplay(row, el, ctx = {})" in extra_js


def test_editor_metadata_replacer_is_extracted() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    js = (root / "assets" / "js" / "editor.js").read_text(encoding="utf-8")
    meta_js = (root / "assets" / "js" / "editor" / "metadata-replacer.js").read_text(encoding="utf-8")
    core_js = (root / "assets" / "helpers" / "core.js").read_text(encoding="utf-8")
    assert 'await ensurePageModule("editor-metadata-replacer", "/assets/js/editor/metadata-replacer.js", "CrossWatchEditorMetadataReplacer");' in core_js
    assert "function metadataReplacerContext()" in js
    assert 'const editorMetadataReplacer = requireEditorModule("MetadataReplacer");' in js
    assert "editorMetadataReplacer.openItemReplacer" in js
    assert "editorMetadataReplacer.openTitleSearchEditor" in js
    assert "function openTitleSearchEditor(row, anchor, refs, ctx = {})" in meta_js
    assert "function openMetadataReplacer(row, anchor, ctx = {})" in meta_js
    assert "function openEpisodeReplacer(row, anchor, ctx = {})" in meta_js
    assert "function coordinateKeyFor(row, season, episode)" in meta_js
    assert "Search metadata" in meta_js
    assert "Replace episode" in meta_js
    assert "function correctedEpisodeItem" not in js
    assert "const EPISODE_ID_FIELDS" not in js


def test_editor_row_building_is_extracted() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    js = (root / "assets" / "js" / "editor.js").read_text(encoding="utf-8")
    rows_js = (root / "assets" / "js" / "editor" / "rows.js").read_text(encoding="utf-8")
    core_js = (root / "assets" / "helpers" / "core.js").read_text(encoding="utf-8")
    assert 'await ensurePageModule("editor-rows", "/assets/js/editor/rows.js", "CrossWatchEditorRows");' in core_js
    assert 'const editorRows = requireEditorModule("Rows");' in js
    assert "function buildRows(items, options = {})" in rows_js
    assert "function buildManualOverrideRows(items, blocks, options = {})" in rows_js
    assert "function applyManualRow(row, item, key)" in rows_js
    assert "function imdbFromKey(key)" in rows_js
    assert "JSON.parse(JSON.stringify(raw))" not in js
    assert "window.CrossWatchEditorRows = Editor.Rows;" in rows_js


def test_editor_table_row_rendering_is_extracted() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    js = (root / "assets" / "js" / "editor.js").read_text(encoding="utf-8")
    table_js = (root / "assets" / "js" / "editor" / "table.js").read_text(encoding="utf-8")
    table_controller_js = (root / "assets" / "js" / "editor" / "table-controller.js").read_text(encoding="utf-8")
    core_js = (root / "assets" / "helpers" / "core.js").read_text(encoding="utf-8")
    assert 'await ensurePageModule("editor-table", "/assets/js/editor/table.js", "CrossWatchEditorTable");' in core_js
    assert 'await ensurePageModule("editor-table-controller", "/assets/js/editor/table-controller.js", "CrossWatchEditorTableController");' in core_js
    assert 'const editorTable = requireEditorModule("Table");' in js
    assert 'const editorTableController = requireEditorModule("TableController");' in js
    assert "function tableControllerContext()" in js
    assert "function tableRowContext(ctx = {}, anilistMode, wideActions)" in table_controller_js
    assert "ctx.editorTable.createRowElement(row, rowCtx)" in table_controller_js
    assert "function renderRows(ctx = {})" in table_controller_js
    assert "function applyFilter(rows, ctx = {})" in table_controller_js
    assert "function sortRows(rows, ctx = {})" in table_controller_js
    assert "function createRowElement(row, ctx = {})" in table_js
    assert "cw-title-search-btn" in table_js
    assert "openRawFieldsModal" in table_js
    assert "openTitleSearchEditor" in table_js
    assert "const fieldName = suffix => `cw-row-${row._rid || \"new\"}-${suffix}`;" not in js
    assert "function tableRowContext(anilistMode, wideActions)" not in js
    assert "window.CrossWatchEditorTable = Editor.Table;" in table_js
    assert "window.CrossWatchEditorTableController = Editor.TableController;" in table_controller_js


def test_editor_row_editor_is_extracted() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    js = (root / "assets" / "js" / "editor.js").read_text(encoding="utf-8")
    row_editor_js = (root / "assets" / "js" / "editor" / "row-editor.js").read_text(encoding="utf-8")
    core_js = (root / "assets" / "helpers" / "core.js").read_text(encoding="utf-8")
    assert 'await ensurePageModule("editor-row-editor", "/assets/js/editor/row-editor.js", "CrossWatchEditorRowEditor");' in core_js
    assert 'const editorRowEditor = requireEditorModule("RowEditor");' in js
    assert "function rowEditorContext()" in js
    assert "return editorRowEditor.updateTypeDisplay(row, el);" in js
    assert "return editorRowEditor.openTypeEditor(row, anchor, rowEditorContext());" in js
    assert "return editorRowEditor.addRow(rowEditorContext());" in js
    assert "function updateTypeDisplay(row, el)" in row_editor_js
    assert "function openTypeEditor(row, anchor, ctx = {})" in row_editor_js
    assert "function addRow(ctx = {})" in row_editor_js
    assert "window.CrossWatchEditorRowEditor = Editor.RowEditor;" in row_editor_js
    assert "const raw = { ids: {}, type: \"movie\", title: \"\", year: null };" not in js


def test_editor_file_utils_are_extracted() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    js = (root / "assets" / "js" / "editor.js").read_text(encoding="utf-8")
    files_js = (root / "assets" / "js" / "editor" / "file-utils.js").read_text(encoding="utf-8")
    core_js = (root / "assets" / "helpers" / "core.js").read_text(encoding="utf-8")
    assert 'await ensurePageModule("editor-file-utils", "/assets/js/editor/file-utils.js", "CrossWatchEditorFileUtils");' in core_js
    assert 'const editorFileUtils = requireEditorModule("FileUtils");' in js
    assert "return editorFileUtils.fetchJSON(url, opts);" in js
    assert "return editorFileUtils.downloadFile(url, filename, toast, { setTag, setStatus });" in js
    assert "return editorFileUtils.bindFileImport(btn, input, url, done, { on, setTag, setStatus });" in js
    assert "async function fetchBlob(url)" in files_js
    assert "function saveBlob(blob, filename)" in files_js
    assert "async function uploadJSON(url, file)" in files_js
    assert "function bindFileImport(btn, input, url, done, ctx = {})" in files_js
    assert "window.CrossWatchEditorFileUtils = Editor.FileUtils;" in files_js
    assert "URL.createObjectURL(blob)" not in js


def test_editor_load_controller_is_extracted() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    js = (root / "assets" / "js" / "editor.js").read_text(encoding="utf-8")
    load_js = (root / "assets" / "js" / "editor" / "load-controller.js").read_text(encoding="utf-8")
    core_js = (root / "assets" / "helpers" / "core.js").read_text(encoding="utf-8")
    assert 'await ensurePageModule("editor-load-controller", "/assets/js/editor/load-controller.js", "CrossWatchEditorLoadController");' in core_js
    assert 'const editorLoadController = requireEditorModule("LoadController");' in js
    assert "function loadControllerContext()" in js
    assert "return editorLoadController.loadState(loadControllerContext());" in js
    assert "return editorLoadController.refreshEditor(loadControllerContext(), { force });" in js
    assert "return editorLoadController.queueEditorRefresh(loadControllerContext(), delay);" in js
    assert "async function loadState(ctx = {})" in load_js
    assert "async function refreshEditor(ctx = {}, options = {})" in load_js
    assert "function queueEditorRefresh(ctx = {}, delay = 250)" in load_js
    assert "window.CrossWatchEditorLoadController = Editor.LoadController;" in load_js
    assert "const endpointId = String(state.snapshot || \"\").trim();" not in js


def test_editor_chrome_decoration_is_extracted() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    js = (root / "assets" / "js" / "editor.js").read_text(encoding="utf-8")
    chrome_js = (root / "assets" / "js" / "editor" / "chrome.js").read_text(encoding="utf-8")
    core_js = (root / "assets" / "helpers" / "core.js").read_text(encoding="utf-8")
    assert 'await ensurePageModule("editor-chrome", "/assets/js/editor/chrome.js", "CrossWatchEditorChrome");' in core_js
    assert 'const editorChrome = requireEditorModule("Chrome");' in js
    assert "editorChrome.wireStaticLabels(host);" in js
    assert "editorChrome.decorateImportPanel({" in js
    assert "editorChrome.decoratePolicyBackupPanel({" in js
    assert "editorChrome.decorateEditorChrome({" in js
    assert "function wireStaticLabels(root)" in chrome_js
    assert "function prepareSourceOptions(root)" in chrome_js
    assert "function decorateImportPanel(ctx = {})" in chrome_js
    assert "function decoratePolicyBackupPanel(ctx = {})" in chrome_js
    assert "function decorateEditorChrome(ctx = {})" in chrome_js
    assert "window.CrossWatchEditorChrome = Editor.Chrome;" in chrome_js
    assert "function wireStaticLabels(root)" not in js
    assert "function decorateImportPanel()" not in js
    assert "function decoratePolicyBackupPanel()" not in js
    assert "function decorateEditorChrome()" not in js


def test_editor_source_loading_is_extracted() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    js = (root / "assets" / "js" / "editor.js").read_text(encoding="utf-8")
    sources_js = (root / "assets" / "js" / "editor" / "sources.js").read_text(encoding="utf-8")
    core_js = (root / "assets" / "helpers" / "core.js").read_text(encoding="utf-8")
    assert 'await ensurePageModule("editor-sources", "/assets/js/editor/sources.js", "CrossWatchEditorSources");' in core_js
    assert 'const editorSources = requireEditorModule("Sources");' in js
    assert "function sourceContext()" in js
    assert "return editorSources.syncSourceUI(sourceContext());" in js
    assert "return editorSources.loadSnapshots(sourceContext());" in js
    assert "function syncSourceUI(ctx = {})" in sources_js
    assert "async function loadSnapshots(ctx = {})" in sources_js
    assert "async function loadTrackerWorkspaces(ctx = {})" not in sources_js
    assert "function rebuildSnapshots(ctx = {})" in sources_js
    assert "function showStateHint(mode, ctx = {})" in sources_js
    assert "window.CrossWatchEditorSources = Editor.Sources;" in sources_js


def test_editor_import_panel_is_extracted() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    js = (root / "assets" / "js" / "editor.js").read_text(encoding="utf-8")
    importers_js = (root / "assets" / "js" / "editor" / "importers.js").read_text(encoding="utf-8")
    core_js = (root / "assets" / "helpers" / "core.js").read_text(encoding="utf-8")
    assert 'await ensurePageModule("editor-importers", "/assets/js/editor/importers.js", "CrossWatchEditorImporters");' in core_js
    assert 'const editorImporters = requireEditorModule("Importers");' in js
    assert "function importContext()" in js
    assert "return editorImporters.syncImportUI(importContext());" in js
    assert "return editorImporters.loadImportProviders(importContext());" in js
    assert "return editorImporters.runStateImport(importContext());" in js
    assert "function syncImportUI(ctx = {})" in importers_js
    assert "async function loadImportProviders(ctx = {})" in importers_js
    assert "function collectImportFeatures(ctx = {})" in importers_js
    assert "async function runStateImport(ctx = {})" in importers_js
    assert "/api/editor/state/import/providers" in importers_js
    assert "/api/editor/state/import" in importers_js
    assert "window.CrossWatchEditorImporters = Editor.Importers;" in importers_js


def test_editor_persistence_is_extracted() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    js = (root / "assets" / "js" / "editor.js").read_text(encoding="utf-8")
    persistence_js = (root / "assets" / "js" / "editor" / "persistence.js").read_text(encoding="utf-8")
    core_js = (root / "assets" / "helpers" / "core.js").read_text(encoding="utf-8")
    assert 'await ensurePageModule("editor-persistence", "/assets/js/editor/persistence.js", "CrossWatchEditorPersistence");' in core_js
    assert 'const editorPersistence = requireEditorModule("Persistence");' in js
    assert "function persistenceContext()" in js
    assert "return editorPersistence.findRowsMissingKey(state);" in js
    assert "return editorPersistence.saveState(persistenceContext());" in js
    assert "function findRowsMissingKey(state)" in persistence_js
    assert "function buildSaveData(ctx = {})" in persistence_js
    assert "function confirmPlaylistRemovals(ctx, items)" in persistence_js
    assert "async function saveState(ctx = {})" in persistence_js
    assert "/api/editor" in persistence_js
    assert "payload.blocks = blocks;" in persistence_js
    assert "window.CrossWatchEditorPersistence = Editor.Persistence;" in persistence_js


def test_editor_uses_page_scroll_for_table() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    js = (root / "assets" / "js" / "editor.js").read_text(encoding="utf-8")
    css = (root / "assets" / "css" / "pages.css").read_text(encoding="utf-8")
    assert 'const tableScroll = host.querySelector(".cw-table-scroll");' not in js
    assert "function wireLinkedTableScroll()" not in js
    assert "scrollDocumentByTableDelta(delta)" not in js
    assert '#page-editor .cw-table-scroll{position:static!important;inset:auto!important;overflow:visible!important;border-radius:22px!important}' in css
    assert "cw-table-overflow-x" in css
    assert ".cw-pop.cw-columns-pop{width:min(370px" not in css
    assert "columnOrder: state.columnOrder" in js
    assert "columnWidths: state.columnWidths" in js
    assert "wideView: state.wideView" in js
    assert "cw-editor-wide" in js
    assert "year: false" in js
    assert "imdb: false" in js
    assert "tvdb: false" in js
    assert "COLUMN_LAYOUT_VERSION = 4" in js
    assert "function syncColumnGroup" in js
    assert "applyColumnWidths," in js
    assert "function startColumnResize" in js
    assert "#page-editor.cw-editor-wide .cw-side{display:none!important}" in css
    assert ".cw-col-resize" in css


def test_connected_crosswatch_is_always_listed_even_without_synced_state(monkeypatch: pytest.MonkeyPatch) -> None:
    import api.editorAPI as editor

    class Store:
        def __init__(self, base: object) -> None:
            self.base = base

        def provider_names(self) -> list[str]:
            return ["MDBLIST", "PLEX"]

    monkeypatch.setattr(editor, "StateStore", Store)
    monkeypatch.setattr(editor, "_load_policy", lambda: {})
    monkeypatch.setattr(editor, "load_config", lambda: {"crosswatch": {"connected": True, "enabled": True}})

    assert editor.api_editor_state_providers()["providers"] == ["MDBLIST", "PLEX", "CROSSWATCH"]


def test_disconnected_crosswatch_is_not_listed(monkeypatch: pytest.MonkeyPatch) -> None:
    import api.editorAPI as editor

    class Store:
        def __init__(self, base: object) -> None:
            self.base = base

        def provider_names(self) -> list[str]:
            return ["PLEX"]

    monkeypatch.setattr(editor, "StateStore", Store)
    monkeypatch.setattr(editor, "_load_policy", lambda: {})
    monkeypatch.setattr(editor, "load_config", lambda: {"crosswatch": {"connected": False, "enabled": True}})

    assert editor.api_editor_state_providers()["providers"] == ["PLEX"]


def test_crosswatch_already_in_state_is_not_duplicated(monkeypatch: pytest.MonkeyPatch) -> None:
    import api.editorAPI as editor

    class Store:
        def __init__(self, base: object) -> None:
            self.base = base

        def provider_names(self) -> list[str]:
            return ["CROSSWATCH", "PLEX"]

    monkeypatch.setattr(editor, "StateStore", Store)
    monkeypatch.setattr(editor, "_load_policy", lambda: {})
    monkeypatch.setattr(editor, "load_config", lambda: {"crosswatch": {"connected": True, "enabled": True}})

    assert editor.api_editor_state_providers()["providers"] == ["CROSSWATCH", "PLEX"]


def test_crosswatch_falls_back_to_tracker_when_no_baseline(monkeypatch: pytest.MonkeyPatch) -> None:
    import api.editorAPI as editor

    seen: dict[str, object] = {}

    class Ops:
        def build_index(self, cfg: dict, *, feature: str) -> dict:
            seen["cfg"] = dict(cfg)
            seen["feature"] = feature
            return {"tmdb:1@100": {"type": "movie", "title": "Heat"}}

    monkeypatch.setattr(editor, "load_sync_ops", lambda name: Ops() if name == "CROSSWATCH" else None)
    monkeypatch.setattr(editor, "load_config", lambda: {"crosswatch": {"connected": True, "enabled": True}})

    items = editor._load_state_items("history", "CROSSWATCH", "default", raw_state={})

    assert items == {"tmdb:1@100": {"type": "movie", "title": "Heat"}}
    assert seen["feature"] == "history"
    assert seen["cfg"]["_cw_readonly"] is True
    assert seen["cfg"]["_cw_history_rewatches"] is True


def test_crosswatch_baseline_wins_over_the_tracker_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    import api.editorAPI as editor

    class Ops:
        def build_index(self, cfg: dict, *, feature: str) -> dict:
            raise AssertionError("tracker must not be read when a baseline exists")

    monkeypatch.setattr(editor, "load_sync_ops", lambda name: Ops())
    monkeypatch.setattr(editor, "load_config", lambda: {"crosswatch": {"connected": True}})

    raw = {"providers": {"CROSSWATCH": {"history": {"baseline": {"items": {"tmdb:9": {"title": "Baseline"}}}}}}}

    assert editor._load_state_items("history", "CROSSWATCH", "default", raw_state=raw) == {"tmdb:9": {"title": "Baseline"}}


def test_other_providers_never_read_the_crosswatch_tracker(monkeypatch: pytest.MonkeyPatch) -> None:
    import api.editorAPI as editor

    def boom(name: str) -> object:
        raise AssertionError("no provider other than CROSSWATCH may fall back")

    monkeypatch.setattr(editor, "load_sync_ops", boom)

    assert editor._load_state_items("history", "PLEX", "default", raw_state={}) == {}
