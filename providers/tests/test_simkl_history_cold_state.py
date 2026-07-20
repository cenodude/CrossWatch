from __future__ import annotations

import json
from types import SimpleNamespace

from cw_platform.id_map import canonical_key


WATCHED = "2024-05-05T00:00:00Z"


def _alias_store(monkeypatch, m):
    """In-memory state FS; _write_json no-ops without a pair scope."""
    store: dict[str, str] = {}
    monkeypatch.setattr(m, "_source_alias_path", lambda: "simkl_history.source_alias.json")
    monkeypatch.setattr(m, "_load_json", lambda path: json.loads(store.get(path) or "{}"))
    monkeypatch.setattr(m, "_save_json", lambda path, data: store.__setitem__(path, json.dumps(data)))
    return store


def _src_episode(*, show_ids, season, episode, ep_tvdb=None, title=None, series_title=None, number_abs=None):
    item = {
        "type": "episode",
        "season": season,
        "episode": episode,
        "watched": True,
        "watched_at": WATCHED,
        "ids": {"tvdb": str(ep_tvdb)} if ep_tvdb else {},
        "show_ids": dict(show_ids),
        "title": title,
        "series_title": series_title,
    }
    if number_abs is not None:
        item["_trakt_number_abs"] = number_abs
    return item


def _monsters_source():
    return [
        _src_episode(
            show_ids={"tmdb": "225634"},
            season=1,
            episode=n,
            ep_tvdb=10649168 + n,
            title=f"Monsters S01E{n:02d}",
            series_title="Monsters",
        )
        for n in range(1, 10)
    ]


def _black_mirror_source():
    return [
        _src_episode(
            show_ids={"tmdb": "42009", "tvdb": "253463"},
            season=2,
            episode=4,
            ep_tvdb=5057304,
            title="White Christmas",
            series_title="Black Mirror",
        )
    ]


def _dbz_source():
    coords = {41: (2, 2), 43: (2, 4), 255: (8, 12), 257: (8, 14)}
    return [
        _src_episode(
            show_ids={"tmdb": "12971", "tvdb": "81472"},
            season=s,
            episode=e,
            title=f"DBZ abs {abs_num}",
            series_title="Dragon Ball Z",
            number_abs=abs_num,
        )
        for abs_num, (s, e) in coords.items()
    ]


def _aot_source():
    return [
        _src_episode(
            show_ids={"tmdb": "1429", "tvdb": "267440"},
            season=0,
            episode=1,
            ep_tvdb=4635717,
            title="AOT Special 1",
            series_title="Attack on Titan",
        )
    ]


def _cold_source_snapshot():
    items = _monsters_source() + _black_mirror_source() + _dbz_source() + _aot_source()
    return {canonical_key(it): it for it in items}


def _simkl_rows():
    monsters_row = {
        "show": {"title": "Monster", "ids": {"tmdb": "113988", "simkl": 5150}},
        "seasons": [
            {
                "number": 2,
                "episodes": [
                    {"number": n, "watched_at": WATCHED, "ids": {"tvdb": str(10649168 + n)}}
                    for n in range(1, 10)
                ],
            }
        ],
    }
    black_mirror_row = {
        "show": {"title": "Black Mirror", "year": 2011, "ids": {"tmdb": "42009", "tvdb": "253463", "simkl": 12345}},
        "seasons": [
            {"number": 0, "episodes": [{"number": 1, "watched_at": WATCHED, "ids": {"tvdb": "5057304"}}]}
        ],
    }
    dbz_row = {
        "show": {"title": "Dragon Ball Z", "ids": {"simkl": 41487, "tvdb": "81472", "tmdb": "12971"}},
        "seasons": [
            {
                "number": 1,
                "episodes": [
                    {"number": 41, "watched_at": WATCHED},
                    {"number": 43, "watched_at": WATCHED},
                    {"number": 255, "watched_at": WATCHED},
                    {"number": 257, "watched_at": WATCHED},
                ],
            }
        ],
    }
    aot_row = {
        "show": {"title": "AoT OVA", "ids": {"simkl": 999999, "tvdb": "888888"}},
        "seasons": [
            {"number": 1, "episodes": [{"number": 1, "watched_at": WATCHED, "ids": {"tvdb": "4635717"}}]}
        ],
    }
    return [monsters_row, black_mirror_row], [dbz_row, aot_row]


def _parse_cold(m):
    show_rows, anime_rows = _simkl_rows()
    out, *_ = m._parse_rows([], show_rows, anime_rows, limit=None)
    return out


def test_cold_state_alias_file_starts_empty(monkeypatch):
    import sync.simkl._history as m

    store = _alias_store(monkeypatch, m)
    assert store == {}
    assert m._load_source_aliases() == {}

    produced = m.prepare_source_snapshot(list(_cold_source_snapshot().values()))

    assert produced == 15
    stored = json.loads(store["simkl_history.source_alias.json"])["items"]
    assert len(stored) == 15


def test_cold_state_prepare_reproduces_aliases_without_writes(monkeypatch):
    import sync.simkl._history as m

    _alias_store(monkeypatch, m)
    posts: list = []
    monkeypatch.setattr(
        m,
        "_post",
        lambda *a, **k: posts.append(a) or SimpleNamespace(status_code=200, text="{}", json=lambda: {}),
        raising=False,
    )

    m.prepare_source_snapshot(list(_cold_source_snapshot().values()))

    assert posts == []
    recs = m._load_source_aliases()
    by_ep_tvdb = {r["ids"].get("tvdb"): r for r in recs.values() if r.get("ids")}
    assert by_ep_tvdb["5057304"]["season"] == 2
    assert by_ep_tvdb["5057304"]["episode"] == 4
    assert by_ep_tvdb["10649169"]["show_ids"] == {"tmdb": "225634"}
    abs_recs = sorted(r["number_abs"] for r in recs.values() if r.get("number_abs"))
    assert abs_recs == [41, 43, 255, 257]


def test_cold_state_monsters_anthology_normalizes_to_source(monkeypatch):
    import sync.simkl._history as m

    _alias_store(monkeypatch, m)
    m.prepare_source_snapshot(list(_cold_source_snapshot().values()))

    out = _parse_cold(m)
    monsters = [v for v in out.values() if v.get("series_title") == "Monsters"]

    assert len(monsters) == 9
    assert sorted((e["season"], e["episode"]) for e in monsters) == [(1, n) for n in range(1, 10)]
    assert all(e["show_ids"] == {"tmdb": "225634"} for e in monsters)


def test_cold_state_black_mirror_normalizes_to_s02e04(monkeypatch):
    import sync.simkl._history as m

    _alias_store(monkeypatch, m)
    m.prepare_source_snapshot(list(_cold_source_snapshot().values()))

    out = _parse_cold(m)
    bm = [v for v in out.values() if v.get("series_title") == "Black Mirror"]

    assert len(bm) == 1
    assert (bm[0]["season"], bm[0]["episode"]) == (2, 4)


def test_cold_state_dbz_native_numbers_normalize_to_source(monkeypatch):
    import sync.simkl._history as m

    _alias_store(monkeypatch, m)
    m.prepare_source_snapshot(list(_cold_source_snapshot().values()))

    out = _parse_cold(m)
    dbz = [v for v in out.values() if v.get("series_title") == "Dragon Ball Z"]

    assert sorted((e["season"], e["episode"]) for e in dbz) == [(2, 2), (2, 4), (8, 12), (8, 14)]


def test_cold_state_first_planner_run_produces_zero_adds(monkeypatch):
    import sync.simkl._history as m

    _alias_store(monkeypatch, m)
    source = _cold_source_snapshot()
    assert len(source) == 15

    m.prepare_source_snapshot(list(source.values()))
    dest = {canonical_key(v): v for v in _parse_cold(m).values()}

    adds = [k for k in source if k not in dest]
    assert adds == []


def test_cold_state_without_prepare_would_add_everything(monkeypatch):
    import sync.simkl._history as m

    _alias_store(monkeypatch, m)
    source = _cold_source_snapshot()
    dest = {canonical_key(v): v for v in _parse_cold(m).values()}

    adds = [k for k in source if k not in dest]
    assert len(adds) == 15


def test_cold_state_unmapped_anime_s00_remains_unresolved(monkeypatch):
    import sync.simkl._history as m
    from test_simkl_history_anime_mapping import _Resp, _Session, _adapter, _ok_add, _patch_fs

    _patch_fs(monkeypatch, m)
    _alias_store(monkeypatch, m)
    m.prepare_source_snapshot(list(_cold_source_snapshot().values()))

    specials = [
        {
            "type": "episode",
            "season": 0,
            "episode": n,
            "watched_at": WATCHED,
            "ids": {},
            "show_ids": {"tvdb": "267440", "tmdb": "1429", "anidb": "9541"},
            "series_title": "Attack on Titan",
        }
        for n in range(2, 7)
    ]

    session = _Session(
        post_handler=lambda url, body: _Resp(200, _ok_add(episodes=0)),
        redirect_map={"267440": "39687"},
    )
    ok, unresolved = m.add(_adapter(session), specials)

    assert ok == 0
    assert len(unresolved) == 5
    assert all(u.get("hint") == "simkl_anime_season_zero_unmapped" for u in unresolved)


def test_exact_ids_read_top_level_and_nested_shapes():
    import sync.simkl._history as m

    assert m._episode_exact_ids({"ids": {"tvdb": "5057304"}}) == {"tvdb": "5057304"}
    assert m._episode_exact_ids({"ids": {"tvdb_id": 5057304}}) == {"tvdb": "5057304"}
    assert m._episode_exact_ids({"tvdb_id": 5057304}) == {"tvdb": "5057304"}
    assert m._episode_exact_ids({"tvdb": 5057304}) == {"tvdb": "5057304"}
    assert m._episode_exact_ids({"anidb_id": 999}) == {"anidb": "999"}
    assert m._episode_exact_ids({"tvdb": {"season": 5, "episode": 9}}) == {}
    assert m._episode_exact_ids({"number": 1, "watched_at": WATCHED}) == {}


def test_top_level_episode_tvdb_id_normalizes_show_row(monkeypatch):
    import sync.simkl._history as m

    _alias_store(monkeypatch, m)
    m.prepare_source_snapshot(_black_mirror_source())

    show_row = {
        "show": {"title": "Black Mirror", "ids": {"tmdb": "42009", "tvdb": "253463", "simkl": 12345}},
        "seasons": [
            {"number": 0, "episodes": [{"number": 1, "watched_at": WATCHED, "tvdb_id": 5057304}]}
        ],
    }

    out, *_ = m._parse_rows([], [show_row], [], limit=None)
    eps = [v for v in out.values() if str(v.get("type")) == "episode"]

    assert len(eps) == 1
    assert (eps[0]["season"], eps[0]["episode"]) == (2, 4)


def test_cross_show_alias_not_blocked_by_same_coordinate_in_other_show(monkeypatch):
    import sync.simkl._history as m

    _alias_store(monkeypatch, m)
    m.prepare_source_snapshot(_monsters_source())

    show_row = {
        "show": {"title": "Monster", "ids": {"tmdb": "113988", "simkl": 5150}},
        "seasons": [
            {
                "number": 1,
                "episodes": [
                    {"number": n, "watched_at": WATCHED, "ids": {"tvdb": str(90000 + n)}}
                    for n in range(1, 10)
                ],
            },
            {
                "number": 2,
                "episodes": [
                    {"number": n, "watched_at": WATCHED, "ids": {"tvdb": str(10649168 + n)}}
                    for n in range(1, 10)
                ],
            },
        ],
    }

    out, *_ = m._parse_rows([], [show_row], [], limit=None)
    mapped = [v for v in out.values() if v.get("show_ids") == {"tmdb": "225634"}]
    native = [v for v in out.values() if v.get("show_ids") == {"tmdb": "113988", "simkl": "5150"}]

    assert len(mapped) == 9
    assert sorted((e["season"], e["episode"]) for e in mapped) == [(1, n) for n in range(1, 10)]
    assert len(native) == 9
    assert sorted((e["season"], e["episode"]) for e in native) == [(1, n) for n in range(1, 10)]
    assert len({canonical_key(v) for v in out.values()}) == 18


def test_cold_state_preserves_winter_vol_liefde_collision_protection(monkeypatch):
    import sync.simkl._history as m

    _alias_store(monkeypatch, m)
    source = [
        _src_episode(show_ids={"tmdb": "5000", "tvdb": "6000"}, season=3, episode=5, ep_tvdb=111005, title="WVL 5"),
        _src_episode(show_ids={"tmdb": "5000", "tvdb": "6000"}, season=3, episode=5, ep_tvdb=111007, title="WVL 7"),
    ]
    m.prepare_source_snapshot(source)

    show_row = {
        "show": {"title": "Winter vol Liefde", "ids": {"tmdb": "5000", "tvdb": "6000", "simkl": 4242}},
        "seasons": [
            {
                "number": 3,
                "episodes": [
                    {"number": 5, "watched_at": WATCHED, "ids": {"tvdb": "111005"}},
                    {"number": 7, "watched_at": WATCHED, "ids": {"tvdb": "111007"}},
                ],
            }
        ],
    }

    out, *_ = m._parse_rows([], [show_row], [], limit=None)
    eps = [v for v in out.values() if str(v.get("type")) == "episode"]

    assert sorted((e["season"], e["episode"]) for e in eps) == [(3, 5), (3, 7)]
