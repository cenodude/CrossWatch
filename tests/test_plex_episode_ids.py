# tests/test_plex_episode_ids.py
# CrossWatch - Plex history catalog resolves episodes by their own episode ids
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

from providers.sync.plex import _history as plex


SHOW = {"tmdb": "61709", "tvdb": "88031", "imdb": "tt1409055"}
WATCHED_AT = "2025-01-01T12:00:00Z"


def entry(rk, season, number, ids=None, *, watched=False, show_ids=None):
    return {"rk": rk, "type": "episode", "ids": dict(ids or {}), "show_ids": dict(SHOW if show_ids is None else show_ids),
            "show_rk": "100", "season": season, "episode": number, "watched": watched}


def tmdb_ordered_catalog():
    cat = plex.HistoryCatalog()
    cat.add(entry("206", 2, 6, {"tmdb": "1024341", "tvdb": "1280931", "imdb": "tt1544685"}))
    cat.add(entry("132", 1, 32, {"tmdb": "1040000", "tvdb": "4000032"}))
    return cat


def tvdb_numbered(ids, season=1, number=32):
    return {"type": "episode", "ids": dict(ids), "show_ids": dict(SHOW), "season": season, "episode": number,
            "watched_at": WATCHED_AT}


def test_episode_id_beats_coordinates():
    rk, klass = tmdb_ordered_catalog().resolve(tvdb_numbered({"tvdb": "1280931"}), strict=True)

    assert (rk, klass) == ("206", plex.CLASS_IN_CATALOG_UNWATCHED)


def test_conflicting_coordinate_hit_is_not_used():
    cat = plex.HistoryCatalog()
    cat.add(entry("132", 1, 32, {"tmdb": "1040000", "tvdb": "4000032"}))

    rk, klass = cat.resolve(tvdb_numbered({"tvdb": "1280931"}), strict=True)

    assert rk is None
    assert klass == plex.CLASS_SHOW_MATCHED_EPISODE_MISSING


def test_coordinate_hit_without_own_ids_still_matches():
    cat = plex.HistoryCatalog()
    cat.add(entry("132", 1, 32, {}))

    assert cat.resolve(tvdb_numbered({"tvdb": "1280931"}), strict=True)[0] == "132"


def test_item_without_episode_ids_keeps_coordinates():
    assert tmdb_ordered_catalog().resolve(tvdb_numbered({}), strict=True)[0] == "132"


def test_legacy_agent_show_id_in_episode_slot_is_not_an_episode_id():
    cat = plex.HistoryCatalog()
    cat.add(entry("132", 1, 32, {"tvdb": "88031"}))

    assert cat.episode_id_index == {}
    assert cat.resolve(tvdb_numbered({"tvdb": "88031"}), strict=True)[0] == "132"


def test_episode_without_external_show_ids_is_not_indexed():
    cat = plex.HistoryCatalog()
    cat.add(entry("206", 2, 6, {"tmdb": "1024341", "tvdb": "1280931"}, show_ids={"plex": "100", "guid": "plex://show/x"}))

    assert cat.episode_id_index == {}


def test_same_episode_id_in_two_items_is_ambiguous():
    cat = tmdb_ordered_catalog()
    cat.add(entry("306", 2, 6, {"tvdb": "1280931"}))

    assert cat.resolve(tvdb_numbered({"tvdb": "1280931"}), strict=True) == (None, plex.CLASS_RESOLVE_AMBIGUOUS)


def test_rebuilt_catalog_keeps_the_episode_id_index():
    rebuilt = plex.build_catalog_from_entries(list(tmdb_ordered_catalog().by_rk.values()))

    assert rebuilt.resolve(tvdb_numbered({"tvdb": "1280931"}), strict=True)[0] == "206"


def test_add_loads_episode_leaves_and_writes_the_id_match(monkeypatch):
    cat = plex.HistoryCatalog()
    cat.add(entry("132", 1, 32, {"tmdb": "1040000", "tvdb": "4000032"}, watched=True))
    leaves = [entry("206", 2, 6, {"tmdb": "1024341", "tvdb": "1280931"})]

    def populate(adapter, allow, catalog, **_kwargs):
        for leaf in leaves:
            catalog.add(leaf)
        return len(leaves)

    monkeypatch.setattr(plex, "home_scope_enter", lambda *a: (False, False, None, None))
    monkeypatch.setattr(plex, "home_scope_exit", lambda *a: None)
    monkeypatch.setattr(plex, "plex_feature_library_ids", lambda *a: set())
    monkeypatch.setattr(plex, "_get_history_catalog", lambda *a, **k: cat)
    monkeypatch.setattr(plex, "_populate_catalog_episode_leaves", populate)
    monkeypatch.setattr(plex, "_store_history_catalog", lambda *a: None)
    monkeypatch.setattr(plex, "_shadow_add_batch", lambda *a: None)
    monkeypatch.setattr(plex, "_ensure_session_pool", lambda *a: None)
    write = Mock(return_value=True)
    monkeypatch.setattr(plex, "_scrobble_with_date", write)
    adapter = SimpleNamespace(client=SimpleNamespace(server=object()), config={})

    count, unresolved = plex.add(adapter, [tvdb_numbered({"tvdb": "1280931"})])

    assert count == 1
    assert unresolved == []
    assert write.call_args[0][1] == "206"
