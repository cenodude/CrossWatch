from types import SimpleNamespace

import pytest

from providers.sync.plex import _history as history
from providers.sync._mod_PLEX import PLEXModule
from test_plex_playback_dates import PLAYED, playback


SCAN = history._iter_marked_watched_from_library
BUILD = history._build_history_catalog


def response(rows=None, total=0, meta=None, status=200):
    return SimpleNamespace(ok=status == 200, status_code=status, headers={"content-type": "application/json"},
                           json=lambda: {"MediaContainer": {"Metadata": rows or [], "totalSize": total, "Meta": meta or {}}})


@pytest.fixture
def library(playback, monkeypatch):
    ad = playback.adapter()
    ad.libraries = lambda **kw: [SimpleNamespace(key="1", type="show", title="Shows")]
    monkeypatch.setattr(history, "_iter_marked_watched_from_library", SCAN)
    monkeypatch.setattr(history, "_build_guid_index", lambda *a, **kw: {"movies": {}, "shows": {}})
    monkeypatch.setattr(history, "normalize_discover_row", lambda row, **kw: {
        "type": row["type"], "ids": {"plex": row["ratingKey"]}, "title": "Item",
    })
    monkeypatch.setattr(history._INDEX_CACHE, "watched_filters", None, raising=False)
    return ad


CAPABILITY = {"Type": [{"Field": [{"key": "episode.viewCount", "type": "integer"}]}],
              "FieldType": [{"type": "integer", "Operator": [{"key": ">>="}]}]}


@pytest.mark.parametrize("format", ["json", "xml"])
def test_confirmed_empty_episode_filter_never_scans_all_episodes(library, format):
    calls = []

    def get(*a, **kw):
        params = kw["params"]
        calls.append(params)
        if params.get("includeMeta"):
            if format == "json":
                return response(meta=CAPABILITY)
            return SimpleNamespace(ok=True, headers={"content-type": "application/xml"}, text=
                '<MediaContainer><Meta><Type><Field key="episode.viewCount" type="integer" /></Type>'
                '<FieldType type="integer"><Operator key="&gt;&gt;=" /></FieldType></Meta></MediaContainer>')
        assert params.get("episode.viewCount>>") == 0
        return response()

    library.client.server._session = SimpleNamespace(headers={}, get=get)
    assert BUILD(library, {"1"}).live_complete
    assert BUILD(library, {"1"}).live_complete
    assert len(calls) == 3


def test_unsupported_filter_retains_full_fallback_for_false_empty_response(library):
    calls = []

    def get(*a, **kw):
        params = kw["params"]
        calls.append(params)
        if params.get("includeMeta") or "unwatched" in params:
            return response()
        return response([{"ratingKey": "42", "type": "episode", "viewCount": 1, "lastViewedAt": PLAYED}], 1)

    library.client.server._session = SimpleNamespace(headers={}, get=get)
    cat = BUILD(library, {"1"})
    assert cat.live_complete
    assert cat.by_rk["42"]["watched"]
    assert len(calls) == 3


def test_advertised_filter_rejected_by_server_falls_back(library):
    def get(*a, **kw):
        params = kw["params"]
        if params.get("includeMeta"):
            return response(meta=CAPABILITY)
        if "episode.viewCount>>" in params:
            return response(status=400)
        return response([{"ratingKey": "42", "type": "episode", "viewCount": 1}], 1)

    library.client.server._session = SimpleNamespace(headers={}, get=get)
    cat = BUILD(library, {"1"})
    assert cat.live_complete and cat.by_rk["42"]["watched"]


def test_missing_library_does_not_block_other_selected_libraries(library, monkeypatch):
    warnings = []
    monkeypatch.setattr(history, "_warn", lambda event, **kw: warnings.append((event, kw)))
    requests = []

    def get(url, **kw):
        requests.append(url)
        return response(meta=CAPABILITY)

    library.client.server._session = SimpleNamespace(headers={}, get=get)
    allow = {"1", "99"}
    assert BUILD(library, allow).live_complete
    assert allow == {"1", "99"}
    assert all("/sections/1/" in path for path in requests)
    assert warnings == [("history_libraries_unavailable", {"library_ids": ["99"]})]


def test_no_selected_library_available_does_not_expand_scope(library):
    library.client.server._session = SimpleNamespace(headers={}, get=lambda *a, **kw: pytest.fail("Outside whitelist"))
    with pytest.raises(RuntimeError, match="no_accessible_selected_libraries: 99"):
        BUILD(library, {"99"})


def test_repeated_page_recovers_with_one_retry(library):
    library.libraries = lambda **kw: [SimpleNamespace(key="1", type="movie")]
    calls = []

    def get(*a, **kw):
        start = kw["params"]["X-Plex-Container-Start"]
        calls.append(start)
        rk = "42" if len(calls) <= 2 else "43"
        return response([{"ratingKey": rk, "type": "movie", "viewCount": 1}], 2)

    library.client.server._session = SimpleNamespace(headers={}, get=get)
    cat = BUILD(library, {"1"})
    assert cat.live_complete and set(cat.by_rk) == {"42", "43"}
    assert calls == [0, 1, 1]


def test_successful_write_is_reused_without_claiming_live_or_date_confirmation(playback, monkeypatch):
    ad = playback.adapter()
    playback.catalog.by_rk["42"]["watched"] = False
    writes = []
    monkeypatch.setattr(history, "_scrobble_with_date", lambda srv, rk, ts: writes.append(rk) or True)
    monkeypatch.setattr(history, "_ensure_session_pool", lambda *a: None)
    monkeypatch.setattr(history, "_shadow_add_batch", lambda *a: None)
    monkeypatch.setattr(history, "_get_history_catalog", lambda *a, **kw: playback.catalog)
    item = {"type": "movie", "ids": {"tmdb": "123", "plex": "42"}, "watched_at": history._iso(PLAYED)}
    assert history.add(ad, [item]) == (1, [])
    assert history.add(ad, [item]) == (1, [])
    assert writes == ["42"]
    assert ad._plex_history_write_meta["accepted_not_seen_live_keys"]
    assert ad._plex_history_write_meta["live_confirmed_keys"] == []
    assert ad._plex_history_write_meta["date_confirmed_keys"] == []


def test_missing_live_timestamp_is_not_date_confirmation(playback, monkeypatch):
    playback.catalog.by_rk["42"]["last_viewed_at"] = None
    monkeypatch.setattr(history, "_get_history_catalog", lambda *a, **kw: playback.catalog)
    monkeypatch.setattr(history, "_shadow_add_batch", lambda *a: None)
    ad = playback.adapter()
    item = {"type": "movie", "ids": {"tmdb": "123"}, "watched_at": history._iso(PLAYED)}
    assert history.add(ad, [item]) == (1, [])
    assert ad._plex_history_write_meta["live_confirmed_keys"]
    assert not ad._plex_history_write_meta["date_confirmed_keys"]


def test_failed_apply_is_reported_as_errors(playback, monkeypatch):
    ad = playback.adapter()
    ad._is_enabled = lambda feature: True

    def fail(*a, **kw):
        raise RuntimeError("plex_watched_scan_http_500")

    monkeypatch.setattr(history, "_get_history_catalog", fail)
    result = PLEXModule.add(ad, "history", [{"type": "movie", "ids": {"tmdb": "123"}}])
    assert result["ok"] is False
    assert result["errors"] == 1
    assert result["count"] == 0
    assert result["confirmed_keys"] == []


def test_failed_remove_is_reported_as_errors(playback, monkeypatch):
    ad = playback.adapter()
    ad._is_enabled = lambda feature: True

    def fail(*a, **kw):
        raise RuntimeError("Server unavailable")

    monkeypatch.setattr(history, "remove", fail)
    result = PLEXModule.remove(ad, "history", [{"type": "movie", "ids": {"tmdb": "123"}}])
    assert result["ok"] is False
    assert result["errors"] == 1
    assert result["count"] == 0
    assert result["confirmed_keys"] == []


def test_episode_catalog_hydration_preserves_pending_write(library):
    cat = history.HistoryCatalog()
    cat.add({"rk": "42", "type": "episode", "ids": {"plex": "42"}, "watched": True,
             "write_accepted": True, "last_viewed_at": None})
    library.client.server._session = SimpleNamespace(headers={}, get=lambda *a, **kw:
        response([{"ratingKey": "42", "type": "episode", "viewCount": 0}], 1))
    assert history._populate_catalog_episode_leaves(library, {"1"}, cat) == 1
    assert cat.by_rk["42"]["write_accepted"] is True
    assert cat.by_rk["42"]["last_viewed_at"] is None
