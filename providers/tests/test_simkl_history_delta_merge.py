from __future__ import annotations

import json
from types import SimpleNamespace


AOT_SHOW_IDS = {"tmdb": "1429", "tvdb": "267440", "simkl": 1429000}
OLD_WATCHED = "2024-01-01T00:00:00Z"
NEW_WATCHED = "2024-06-01T00:00:00Z"


def _state_store(monkeypatch, m):
    store: dict[str, str] = {}
    monkeypatch.setattr(m, "state_file", lambda name: name)
    monkeypatch.setattr(m, "_load_json", lambda path: json.loads(store.get(path) or "{}"))
    monkeypatch.setattr(m, "_save_json", lambda path, data: store.__setitem__(path, json.dumps(data)))
    return store


def _aot_row(coords, watched_at=OLD_WATCHED):
    seasons: dict[int, list[dict]] = {}
    for s, e in coords:
        seasons.setdefault(s, []).append({"number": e, "watched_at": watched_at})
    return {
        "show": {"title": "Attack on Titan", "year": 2013, "ids": dict(AOT_SHOW_IDS)},
        "seasons": [{"number": s, "episodes": eps} for s, eps in sorted(seasons.items())],
    }


def _coords_of(index):
    return sorted(
        (v["season"], v["episode"])
        for v in index.values()
        if str(v.get("type")) == "episode" and v.get("series_title") == "Attack on Titan"
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


def test_activity_change_refetches_full_library_without_date_from(monkeypatch):
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

    assert since_seen == [None]
    assert _coords_of(out) == sorted(seeded + [(4, 29)])
    assert out[stale_key]["series_title"] == "Attack on Titan"
    assert m._cache_load().keys() == out.keys()


def test_absent_episode_is_dropped_immediately(monkeypatch):
    import sync.simkl._history as m

    _state_store(monkeypatch, m)
    seeded = [(1, 1), (1, 2), (2, 1)]
    _seed_cache(m, seeded)

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


def test_injected_episode_survives_full_replace_within_grace(monkeypatch):
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
