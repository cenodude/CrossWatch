from __future__ import annotations

import json
from types import SimpleNamespace


AOT_SHOW_IDS = {"tmdb": "1429", "tvdb": "267440", "simkl": 1429000}
BB_SHOW_IDS = {"tmdb": "1396", "tvdb": "81189", "simkl": 1396000}
OLD_WATCHED = "2024-01-01T00:00:00Z"
NEW_WATCHED = "2024-06-01T00:00:00Z"


def _state_store(monkeypatch, m):
    store: dict[str, str] = {}
    monkeypatch.setattr(m, "state_file", lambda name: name)
    monkeypatch.setattr(m, "_load_json", lambda path: json.loads(store.get(path) or "{}"))
    monkeypatch.setattr(m, "_save_json", lambda path, data: store.__setitem__(path, json.dumps(data)))
    return store


def _show_row(coords, watched_at=OLD_WATCHED, *, title="Attack on Titan", year=2013, ids=None):
    seasons: dict[int, list[dict]] = {}
    for s, e in coords:
        seasons.setdefault(s, []).append({"number": e, "watched_at": watched_at})
    return {
        "show": {"title": title, "year": year, "ids": dict(ids or AOT_SHOW_IDS)},
        "seasons": [{"number": s, "episodes": eps} for s, eps in sorted(seasons.items())],
    }


def _aot_row(coords, watched_at=OLD_WATCHED):
    return _show_row(coords, watched_at, title="Attack on Titan", year=2013, ids=AOT_SHOW_IDS)


def _bb_row(coords, watched_at=OLD_WATCHED):
    return _show_row(coords, watched_at, title="Breaking Bad", year=2008, ids=BB_SHOW_IDS)


def _coords_of(index):
    return sorted(
        (v["season"], v["episode"])
        for v in index.values()
        if str(v.get("type")) == "episode" and v.get("series_title") == "Attack on Titan"
    )


def _series_coords_of(index, title):
    return sorted(
        (v["season"], v["episode"])
        for v in index.values()
        if str(v.get("type")) == "episode" and v.get("series_title") == title
    )


def _patch_index_env(monkeypatch, m, *, watermark, removed_watermark, acts, rows):
    monkeypatch.setattr(m, "normalize_flat_watermarks", lambda: None)
    monkeypatch.setattr(
        m,
        "get_watermark",
        lambda kind: removed_watermark if kind == "history_removed" else watermark,
    )
    monkeypatch.setattr(m, "update_watermark_if_new", lambda *a, **k: None)
    monkeypatch.setattr(m, "fetch_activities", lambda *a, **k: (acts, None))
    monkeypatch.setattr(m, "_headers", lambda *a, **k: {})
    monkeypatch.setattr(m, "_unfreeze", lambda *a, **k: None)
    monkeypatch.setattr(m, "_fetch_all_items", lambda *a, **k: dict(rows))
    return SimpleNamespace(client=SimpleNamespace(session=object()), cfg=SimpleNamespace(timeout=5))


def _acts(watched_iso, removed_iso=None):
    out = {"tv_shows": {"all": watched_iso}}
    if removed_iso:
        out["tv_shows"]["removed_from_list"] = removed_iso
    return out


def _seed_cache(m, coords, watched_at=OLD_WATCHED):
    seed, *_ = m._parse_rows([], [_aot_row(coords, watched_at)], [], limit=None)
    m._cache_save(seed)
    return seed


def test_activity_change_fetches_delta_with_watermark_and_replaces_touched_show(monkeypatch):
    import sync.simkl._history as m

    _state_store(monkeypatch, m)
    seeded = [(1, 1), (1, 2), (1, 3), (2, 1), (2, 2), (3, 5), (4, 28)]
    seed = _seed_cache(m, seeded)
    assert len(seed) == len(seeded)

    stale_key = next(k for k, v in seed.items() if (v["season"], v["episode"]) == (4, 28))
    stale = m._cache_load()
    stale[stale_key]["series_title"] = "STALE"
    m._cache_save(stale)

    since_seen: list = []
    full_rows = {
        "movies": [],
        "shows": [_aot_row(seeded, OLD_WATCHED), _aot_row([(4, 29)], NEW_WATCHED)],
        "anime": [],
    }
    adapter = _patch_index_env(
        monkeypatch,
        m,
        watermark=OLD_WATCHED,
        removed_watermark="",
        acts=_acts(NEW_WATCHED),
        rows=full_rows,
    )
    monkeypatch.setattr(
        m, "_fetch_all_items",
        lambda *a, since_iso=None, **k: (since_seen.append(since_iso), dict(full_rows))[1],
    )

    out = m.build_index(adapter)

    assert since_seen == [OLD_WATCHED]
    assert _coords_of(out) == sorted(seeded + [(4, 29)])
    assert out[stale_key]["series_title"] == "Attack on Titan"
    assert m._cache_load().keys() == out.keys()


def test_absent_episode_is_dropped_immediately(monkeypatch):
    import sync.simkl._history as m

    _state_store(monkeypatch, m)
    seeded = [(1, 1), (1, 2), (2, 1)]
    _seed_cache(m, seeded)
    legacy = m._cache_load()
    for row in legacy.values():
        row.pop("_cw_scope", None)
    m._cache_save(legacy)

    surviving = {"movies": [], "shows": [_aot_row([(1, 1), (2, 1)], OLD_WATCHED)], "anime": []}
    adapter = _patch_index_env(
        monkeypatch,
        m,
        watermark=OLD_WATCHED,
        removed_watermark="",
        acts=_acts(NEW_WATCHED),
        rows=surviving,
    )

    out = m.build_index(adapter)

    assert _coords_of(out) == [(1, 1), (2, 1)]
    assert m._cache_load().keys() == out.keys()


def test_delta_touching_one_show_leaves_other_show_cached(monkeypatch):
    import sync.simkl._history as m

    _state_store(monkeypatch, m)
    seed, *_ = m._parse_rows([], [_aot_row([(1, 1)]), _bb_row([(1, 1), (1, 2)])], [], limit=None)
    m._cache_save(seed)

    rows = {"movies": [], "shows": [_aot_row([(1, 1), (1, 2)], NEW_WATCHED)], "anime": []}
    adapter = _patch_index_env(
        monkeypatch,
        m,
        watermark=OLD_WATCHED,
        removed_watermark="",
        acts=_acts(NEW_WATCHED),
        rows=rows,
    )

    out = m.build_index(adapter)

    assert _series_coords_of(out, "Attack on Titan") == [(1, 1), (1, 2)]
    assert _series_coords_of(out, "Breaking Bad") == [(1, 1), (1, 2)]


DBZ_S1_IDS = {"tmdb": "12971", "tvdb": "81472", "simkl": 46128}
DBZ_S2_IDS = {"tmdb": "12972", "tvdb": "81472", "simkl": 46129}


def _anime_row(coords, watched_at=OLD_WATCHED, *, title, ids):
    row = _show_row(coords, watched_at, title=title, year=1989, ids=ids)
    row["anime_type"] = "tv"
    return row


def test_delta_on_split_anime_record_keeps_the_sibling_record(monkeypatch):
    import sync.simkl._history as m

    _state_store(monkeypatch, m)
    seed, *_ = m._parse_rows(
        [],
        [],
        [
            _anime_row([(1, 1), (1, 2)], title="Dragon Ball Z", ids=DBZ_S1_IDS),
            _anime_row([(1, 1), (1, 2)], title="Dragon Ball Z Season 2", ids=DBZ_S2_IDS),
        ],
        limit=None,
    )
    m._cache_save(seed)
    assert _series_coords_of(seed, "Dragon Ball Z Season 2") == [(1, 1), (1, 2)]

    rows = {
        "movies": [],
        "shows": [],
        "anime": [_anime_row([(1, 1), (1, 2), (1, 3)], NEW_WATCHED, title="Dragon Ball Z", ids=DBZ_S1_IDS)],
    }
    adapter = _patch_index_env(
        monkeypatch,
        m,
        watermark=OLD_WATCHED,
        removed_watermark="",
        acts=_acts(NEW_WATCHED),
        rows=rows,
    )

    out = m.build_index(adapter)

    assert _series_coords_of(out, "Dragon Ball Z") == [(1, 1), (1, 2), (1, 3)]
    assert _series_coords_of(out, "Dragon Ball Z Season 2") == [(1, 1), (1, 2)]


def test_empty_delta_keeps_cache(monkeypatch):
    import sync.simkl._history as m

    _state_store(monkeypatch, m)
    seed = _seed_cache(m, [(1, 1), (1, 2)])
    since_seen: list = []
    adapter = _patch_index_env(
        monkeypatch,
        m,
        watermark=OLD_WATCHED,
        removed_watermark="",
        acts=_acts(NEW_WATCHED),
        rows={"movies": [], "shows": [], "anime": []},
    )
    monkeypatch.setattr(
        m, "_fetch_all_items",
        lambda *a, since_iso=None, **k: (since_seen.append(since_iso), {"movies": [], "shows": [], "anime": []})[1],
    )

    out = m.build_index(adapter)

    assert since_seen == [OLD_WATCHED]
    assert out == seed


def test_rewatch_mode_uses_the_watermark_and_keeps_cache(monkeypatch):
    import sync.simkl._history as m

    _state_store(monkeypatch, m)
    seed, *_ = m._parse_rows([], [_aot_row([(1, 1)]), _bb_row([(1, 1), (1, 2)])], [], limit=None)
    m._cache_save(seed, rewatches=True)

    since_seen: list = []
    rows = {"movies": [], "shows": [_aot_row([(1, 1), (1, 2)], NEW_WATCHED)], "anime": []}
    adapter = _patch_index_env(
        monkeypatch,
        m,
        watermark=OLD_WATCHED,
        removed_watermark="",
        acts=_acts(NEW_WATCHED),
        rows=rows,
    )
    adapter.config = {"_cw_history_rewatches": True}
    monkeypatch.setattr(m, "_rewatch_account_ok", lambda *a, **k: True)
    monkeypatch.setattr(
        m, "_fetch_all_items",
        lambda *a, since_iso=None, **k: (since_seen.append(since_iso), dict(rows))[1],
    )

    out = m.build_index(adapter)

    assert since_seen == [OLD_WATCHED]
    assert _series_coords_of(out, "Attack on Titan") == [(1, 1), (1, 2)]
    assert _series_coords_of(out, "Breaking Bad") == [(1, 1), (1, 2)]


def test_rewatch_toggle_forces_a_cold_full_pull(monkeypatch):
    import sync.simkl._history as m

    _state_store(monkeypatch, m)
    seed = _seed_cache(m, [(1, 1), (1, 2)])
    m._cache_save(seed, rewatches=False)
    assert m._cache_doc_is_stale(True) is True
    assert m._cache_doc_is_stale(False) is False

    since_seen: list = []
    rows = {"movies": [], "shows": [_aot_row([(1, 1)], NEW_WATCHED)], "anime": []}
    adapter = _patch_index_env(
        monkeypatch,
        m,
        watermark=OLD_WATCHED,
        removed_watermark="",
        acts=_acts(NEW_WATCHED),
        rows=rows,
    )
    adapter.config = {"_cw_history_rewatches": True}
    monkeypatch.setattr(m, "_rewatch_account_ok", lambda *a, **k: True)
    monkeypatch.setattr(
        m, "_fetch_all_items",
        lambda *a, since_iso=None, **k: (since_seen.append(since_iso), dict(rows))[1],
    )

    out = m.build_index(adapter)

    assert since_seen == [None]
    assert _coords_of(out) == [(1, 1)]


def test_removal_refresh_replaces_cache_and_prunes(monkeypatch):
    import sync.simkl._history as m

    _state_store(monkeypatch, m)
    seed = _seed_cache(m, [(1, 1), (1, 2), (2, 1), (4, 28)])

    surviving = {"movies": [], "shows": [_aot_row([(1, 1), (1, 2)])], "anime": []}
    adapter = _patch_index_env(
        monkeypatch,
        m,
        watermark=OLD_WATCHED,
        removed_watermark=OLD_WATCHED,
        acts=_acts(NEW_WATCHED, removed_iso=NEW_WATCHED),
        rows=surviving,
    )

    out = m.build_index(adapter)

    assert _coords_of(out) == [(1, 1), (1, 2)]
    assert len(out) == 2
    assert set(out) < set(seed)
    assert m._cache_load().keys() == out.keys()


def test_injected_episode_survives_delta_replace_within_grace(monkeypatch):
    import sync.simkl._history as m

    _state_store(monkeypatch, m)
    _seed_cache(m, [(1, 1), (1, 2)])

    historical = {
        "type": "episode",
        "season": 3,
        "episode": 7,
        "watched_at": "2019-03-03T00:00:00Z",
        "ids": {},
        "show_ids": {k: str(v) for k, v in AOT_SHOW_IDS.items()},
        "series_title": "Attack on Titan",
    }
    m._inject_adds_into_cache([historical])
    injected_key = next(k for k, v in m._cache_load().items() if (v.get("season"), v.get("episode")) == (3, 7))

    full_rows = {"movies": [], "shows": [_aot_row([(1, 1), (1, 2), (1, 3)], NEW_WATCHED)], "anime": []}
    adapter = _patch_index_env(
        monkeypatch,
        m,
        watermark=OLD_WATCHED,
        removed_watermark="",
        acts=_acts(NEW_WATCHED),
        rows=full_rows,
    )

    out = m.build_index(adapter)

    assert injected_key in out
    assert injected_key in m._cache_load()
    assert _coords_of(out) == [(1, 1), (1, 2), (1, 3), (3, 7)]


def test_injected_episode_is_dropped_after_grace_window(monkeypatch):
    import sync.simkl._history as m

    _state_store(monkeypatch, m)
    _seed_cache(m, [(1, 1), (1, 2)])

    historical = {
        "type": "episode",
        "season": 3,
        "episode": 7,
        "watched_at": "2019-03-03T00:00:00Z",
        "ids": {},
        "show_ids": {k: str(v) for k, v in AOT_SHOW_IDS.items()},
        "series_title": "Attack on Titan",
    }
    m._inject_adds_into_cache([historical])
    stale = m._cache_load()
    injected_key = next(k for k, v in stale.items() if (v.get("season"), v.get("episode")) == (3, 7))
    stale[injected_key]["_cw_injected_at"] = 0
    m._cache_save(stale)

    full_rows = {"movies": [], "shows": [_aot_row([(1, 1), (1, 2)], NEW_WATCHED)], "anime": []}
    adapter = _patch_index_env(
        monkeypatch,
        m,
        watermark=OLD_WATCHED,
        removed_watermark="",
        acts=_acts(NEW_WATCHED),
        rows=full_rows,
    )

    out = m.build_index(adapter)

    assert injected_key not in out
    assert _coords_of(out) == [(1, 1), (1, 2)]
