from __future__ import annotations

from types import SimpleNamespace

import pytest

from cw_platform.log_context import log_run_id
from providers.sync.plex import _common as common, _history as history


@pytest.fixture
def isolated_state(monkeypatch, tmp_path):
    monkeypatch.setenv("CW_PAIR_KEY", "cw2_history_options")
    monkeypatch.setattr(common, "STATE_DIR", tmp_path)
    monkeypatch.setattr(common, "_FBGUID_MEMO", {})
    monkeypatch.setattr(common, "_FBGUID_MEMO_PATH", None)
    monkeypatch.setattr(common, "_FBGUID_MEMO_DIRTY", False)
    monkeypatch.setattr(common, "plex_context", lambda: {"baseurl": "http://plex", "token": "token", "account_token": "token"})
    monkeypatch.setattr(common, "hydrate_external_ids", lambda *args: {})
    monkeypatch.setattr(history, "_dbg", lambda *args, **kwargs: None)
    monkeypatch.setattr(history, "_info", lambda *args, **kwargs: None)
    monkeypatch.setattr(history, "_emit", lambda *args, **kwargs: None)
    history._clear_guid_index()
    token = log_run_id.set("options-run1")
    yield
    log_run_id.reset(token)
    history._clear_guid_index()


def test_fallback_guid_recovers_old_item_and_keeps_match_when_disabled(isolated_state, monkeypatch):
    raw = SimpleNamespace(type="movie", ratingKey="42", title="Old movie", year=1990, viewedAt=1787093918)
    server = SimpleNamespace(baseurl="http://plex", _token="token", machineIdentifier="server", history=lambda **kwargs: [raw])
    config = {"_cw_pair_scope": "cw2_history_options", "plex": {
        "fallback_GUID": False, "history_workers": 1, "history": {"include_marked_watched": False},
    }}
    ad = SimpleNamespace(client=SimpleNamespace(server=server), config=config, libraries=lambda **kwargs: [])
    searches = []

    def discover(*args, **kwargs):
        searches.append(1)
        return {"type": "movie", "title": "Old movie", "year": 1990, "Guid": [{"id": "tmdb://123"}]}

    monkeypatch.setattr(common, "_discover_search_title", discover)
    assert history.build_index(ad) == {}
    assert not searches
    config["plex"]["fallback_GUID"] = True
    recovered = history.build_index(ad)
    assert len(recovered) == 1
    assert next(iter(recovered.values()))["ids"]["tmdb"] == "123"
    assert next(iter(recovered.values()))["watched_at"] == history._iso(1787093918)
    assert searches == [1]
    assert common._fbguid_cache_path().exists()

    # Simulate another run/process, with recovery switched off. The persistent
    # fallback memo must survive retirement of the separate library GUID index.
    log_run_id.set("options-run2")
    config["plex"]["fallback_GUID"] = False
    common._FBGUID_MEMO.clear()
    monkeypatch.setattr(common, "_FBGUID_MEMO_PATH", None)
    history._clear_guid_index()
    assert history.build_index(ad) == recovered
    assert searches == [1]


@pytest.mark.parametrize("timestamp", [1787093918, None])
def test_manual_watched_changes_survive_cached_guid_index(isolated_state, timestamp):
    row = {"type": "movie", "ratingKey": "42", "title": "Manual movie", "year": 1990,
           "Guid": [{"id": "tmdb://123"}], "viewCount": 0}
    if timestamp is not None:
        row["lastViewedAt"] = timestamp
    index_reads = []

    def get(url, *, params=None, **kwargs):
        params = params or {}
        if params.get("X-Plex-Container-Size") == 1000:
            index_reads.append(1)
        rows = [] if "unwatched" in params and not row["viewCount"] else [dict(row)]
        return SimpleNamespace(ok=True, status_code=200, headers={"content-type": "application/json"},
                               json=lambda: {"MediaContainer": {"Metadata": rows, "totalSize": len(rows)}})

    server = SimpleNamespace(baseurl="http://plex", _token="token", machineIdentifier="server",
                             history=lambda **kwargs: [], _session=SimpleNamespace(headers={}, get=get))
    ad = SimpleNamespace(client=SimpleNamespace(server=server),
                         config={"_cw_pair_scope": "cw2_history_options", "plex": {"history": {"include_marked_watched": True}}},
                         libraries=lambda **kwargs: [SimpleNamespace(key="1", type="movie", title="Movies")])
    assert history.build_index(ad) == {}
    row["viewCount"] = 1
    watched = history.build_index(ad)
    assert len(watched) == 1
    item = next(iter(watched.values()))
    assert item["watched"] is True
    if timestamp is not None:
        assert item["watched_at"] == history._iso(timestamp)
    else:
        assert not item.get("watched_at")
    assert history.build_index(ad) == watched
    row["viewCount"] = 0
    assert history.build_index(ad) == {}
    assert len(index_reads) == 1


@pytest.mark.parametrize("catalog_kind", ["current", "missing", "id_only", "no_external_ids", "wrong_user_scope"])
@pytest.mark.parametrize("include_marked", [False, True])
def test_history_metadata_selection_preserves_fallback_and_watched_dates(
    isolated_state, monkeypatch, catalog_kind, include_marked,
):
    played_at = 1787093918
    live_at = played_at + 3600
    raw = SimpleNamespace(type="movie", ratingKey="42", title="Historical title", year=2020, viewedAt=played_at)
    server = SimpleNamespace(history=lambda **kwargs: [raw])
    adapter = SimpleNamespace(client=SimpleNamespace(server=server), config={"plex": {
        "history_workers": 1, "history": {"include_marked_watched": include_marked},
    }})
    catalog = history.HistoryCatalog()
    catalog.live_complete = True
    if catalog_kind != "missing":
        catalog.add({
            "rk": "42", "type": "movie", "title": None if catalog_kind == "id_only" else "Current title",
            "watched": True,
            "ids": {"plex": "42"} if catalog_kind == "no_external_ids" else {"plex": "42", "tmdb": "123"},
        })
    fallback_calls = []

    def fallback(*args, **kwargs):
        fallback_calls.append(1)
        return {"type": "movie", "title": "Historical title", "ids": {"plex": "42", "tmdb": "123"}}

    monkeypatch.setattr(history, "home_scope_enter", lambda _: (catalog_kind == "wrong_user_scope", False, None, None))
    monkeypatch.setattr(history, "home_scope_exit", lambda *_: None)
    monkeypatch.setattr(history, "plex_feature_library_ids", lambda *_: set())
    monkeypatch.setattr(history, "_history_force_full", lambda _: False)
    monkeypatch.setattr(history, "_build_history_catalog", lambda *a, **k: catalog)
    monkeypatch.setattr(history, "_store_history_catalog", lambda *_: None)
    monkeypatch.setattr(history, "_keep_in_snapshot", lambda *_: True)
    monkeypatch.setattr(history, "minimal_from_history_row", fallback)
    monkeypatch.setattr(history, "_pms_fetch_metadata_row", lambda *_, **kw: {"viewCount": 1, "lastViewedAt": live_at})
    monkeypatch.setattr(history, "_iter_marked_watched_from_library", lambda *a, **k: [])

    result = history.build_index(adapter)
    assert len(result) == 1
    item = next(iter(result.values()))
    assert item["watched_at"] == history._iso(played_at)
    assert item["title"] == ("Current title" if catalog_kind == "current" else "Historical title")
    assert len(fallback_calls) == (0 if catalog_kind == "current" else 1)
