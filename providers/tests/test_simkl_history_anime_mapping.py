from __future__ import annotations

import json
from types import SimpleNamespace


class _Resp:
    def __init__(self, status=200, payload=None, text=None, headers=None):
        self.status_code = status
        self._payload = payload if payload is not None else {}
        self.text = text if text is not None else json.dumps(self._payload)
        self.headers = headers if headers is not None else {}

    def json(self):
        return self._payload

    @property
    def ok(self):
        return 200 <= self.status_code < 300


def _ok_add(episodes=0, movies=0, shows=0, not_found=None):
    return {
        "added": {"movies": movies, "shows": shows, "episodes": episodes},
        "not_found": not_found or {"movies": [], "shows": [], "episodes": []},
    }


class _Session:
    def __init__(self, post_handler=None, redirect_map=None, episodes_map=None, all_items=None):
        self.posts = []
        self.gets = []
        self._post_handler = post_handler or (lambda url, body: _Resp(200, _ok_add(episodes=1)))
        self._redirect_map = {str(k): str(v) for k, v in (redirect_map or {}).items()}
        self._episodes_map = {str(k): v for k, v in (episodes_map or {}).items()}
        self._all_items = all_items

    def post(self, url, headers=None, params=None, json=None, timeout=None):
        self.posts.append({"url": url, "json": json})
        return self._post_handler(url, json)

    def get(self, url, headers=None, params=None, timeout=None, allow_redirects=None):
        self.gets.append({"url": url, "params": params})
        import sync.simkl._history as m
        if url == m.URL_ALL_ITEMS:
            if callable(self._all_items):
                return _Resp(200, self._all_items(len([g for g in self.gets if g["url"] == url])))
            return _Resp(200, self._all_items or {"movies": [], "shows": [], "anime": []})
        if url == m.URL_REDIRECT:
            tvdb = str((params or {}).get("tvdb") or "")
            simkl = self._redirect_map.get(tvdb)
            if simkl:
                return _Resp(302, {}, headers={"Location": f"https://simkl.com/anime/{simkl}/x"})
            return _Resp(404, {})
        if "/anime/episodes/" in url:
            simkl = url.rsplit("/", 1)[-1]
            return _Resp(200, self._episodes_map.get(simkl, []))
        return _Resp(404, {})


def _patch_fs(monkeypatch, m):
    store: dict[str, str] = {}
    monkeypatch.setattr(m, "state_file", lambda name: name)
    monkeypatch.setattr(m, "_load_json", lambda path: json.loads(store.get(path) or "{}"))
    monkeypatch.setattr(m, "_save_json", lambda path, data: store.__setitem__(path, json.dumps(data)))
    monkeypatch.setattr(m, "cache_anime_mappings", lambda *a, **k: None)
    monkeypatch.setattr(m, "_headers", lambda *a, **k: {})
    monkeypatch.setattr(m, "simkl_api_params_from_headers", lambda headers=None, **k: dict(k))
    monkeypatch.setattr(m, "_unfreeze", lambda *a, **k: None)
    monkeypatch.setattr(m, "_freeze", lambda *a, **k: None)
    monkeypatch.setattr(m, "_inject_adds_into_cache", lambda *a, **k: None)
    monkeypatch.setattr(m, "_load_anime_resolve_cache", lambda: m._AnimeResolveState({}, {}))
    monkeypatch.setattr(m, "_save_anime_resolve_cache", lambda *a, **k: None)
    monkeypatch.setattr(m, "_load_anime_episode_map_cache", lambda: {})
    monkeypatch.setattr(m, "_save_anime_episode_map_cache", lambda *a, **k: None)
    monkeypatch.setattr(m, "_load_anime_episode_alias_cache", lambda: {})
    monkeypatch.setattr(m, "_save_anime_episode_alias_cache", lambda *a, **k: None)


def _adapter(session):
    return SimpleNamespace(client=SimpleNamespace(session=session), cfg=SimpleNamespace(timeout=5, history_chunk_size=100))


def _episode(tvdb, tmdb, season, episode, series, *, number_abs=None):
    item = {
        "type": "episode",
        "season": season,
        "episode": episode,
        "watched_at": "2024-01-01T00:00:00Z",
        "show_ids": {"tvdb": tvdb, "tmdb": tmdb},
        "series_title": series,
    }
    if number_abs is not None:
        item["_trakt_number_abs"] = number_abs
    return item


def _dbz(season, episode, number_abs):
    return _episode("81472", "12971", season, episode, "Dragon Ball Z", number_abs=number_abs)


def _aot(season, episode):
    return _episode("267440", "1429", season, episode, "Attack on Titan")


def _s00(show_tvdb, show_tmdb, episode, series, *, ep_tvdb=None):
    item = {
        "type": "episode",
        "season": 0,
        "episode": episode,
        "watched_at": "2024-01-01T00:00:00Z",
        "show_ids": {"tvdb": show_tvdb, "tmdb": show_tmdb},
        "series_title": series,
    }
    if ep_tvdb is not None:
        item["ids"] = {"tvdb": ep_tvdb}
    return item


# DBZ redirects to a native SIMKL anime entry whose absolute episodes exist (E40 present).
_DBZ_REDIRECT = {"81472": "41487"}
_DBZ_EPISODES = {"41487": [{"episode": n, "title": f"dbz{n}", "tvdb": {"season": 1, "episode": n}} for n in range(1, 60)]}
# Attack on Titan's parent TVDB redirects to a cour whose native map only covers S1 (no S4).
_AOT_REDIRECT = {"267440": "39687"}
_AOT_EPISODES = {"39687": [{"episode": n, "title": f"aot{n}", "tvdb": {"season": 1, "episode": n}} for n in range(1, 26)]}


def _shows_of(session, url):
    for p in session.posts:
        if p["url"] == url and isinstance(p["json"], dict) and "shows" in p["json"]:
            for show in p["json"]["shows"]:
                yield show


def _anime_of(session, url):
    for p in session.posts:
        if p["url"] == url and isinstance(p["json"], dict) and "anime" in p["json"]:
            for grp in p["json"]["anime"]:
                yield grp


def test_dbz_resolves_to_anime_e40_absent_from_shows(monkeypatch):
    import sync.simkl._history as m

    _patch_fs(monkeypatch, m)
    session = _Session(redirect_map=_DBZ_REDIRECT, episodes_map=_DBZ_EPISODES)
    adapter = _adapter(session)

    ok, unresolved = m.add(adapter, [_dbz(2, 1, 40)])

    anime_numbers = [e["number"] for grp in _anime_of(session, m.URL_ADD) for e in grp["episodes"]]
    assert anime_numbers == [40]
    assert list(_shows_of(session, m.URL_ADD)) == []
    assert ok == 1
    assert unresolved == []


def test_aot_s04e17_falls_to_shows_with_flag(monkeypatch):
    import sync.simkl._history as m

    _patch_fs(monkeypatch, m)
    session = _Session(redirect_map=_AOT_REDIRECT, episodes_map=_AOT_EPISODES)
    adapter = _adapter(session)

    ok, unresolved = m.add(adapter, [_aot(4, 17)])

    assert list(_anime_of(session, m.URL_ADD)) == []
    shows = list(_shows_of(session, m.URL_ADD))
    assert len(shows) == 1
    assert shows[0]["use_tvdb_anime_seasons"] is True
    assert shows[0]["ids"].get("tvdb") == "267440"
    seasons = {s["number"]: s for s in shows[0]["seasons"]}
    ep = seasons[4]["episodes"][0]
    assert ep["number"] == 17
    assert ep["watched_at"] == "2024-01-01T00:00:00Z"
    assert unresolved == []


def test_aot_range_not_marked_missing(monkeypatch):
    import sync.simkl._history as m

    _patch_fs(monkeypatch, m)
    session = _Session(redirect_map=_AOT_REDIRECT, episodes_map=_AOT_EPISODES)
    adapter = _adapter(session)

    ok, unresolved = m.add(adapter, [_aot(4, 16), _aot(4, 17), _aot(4, 18)])

    assert unresolved == []
    assert list(_anime_of(session, m.URL_ADD)) == []
    numbers = sorted(
        e["number"]
        for show in _shows_of(session, m.URL_ADD)
        for s in show["seasons"]
        for e in s["episodes"]
    )
    assert numbers == [16, 17, 18]


def test_no_duplicate_between_anime_and_shows(monkeypatch):
    import sync.simkl._history as m

    _patch_fs(monkeypatch, m)
    session = _Session(redirect_map={**_DBZ_REDIRECT, **_AOT_REDIRECT}, episodes_map={**_DBZ_EPISODES, **_AOT_EPISODES})
    adapter = _adapter(session)

    m.add(adapter, [_dbz(2, 1, 40), _aot(4, 17)])

    anime_numbers = [e["number"] for grp in _anime_of(session, m.URL_ADD) for e in grp["episodes"]]
    show_ids = [show["ids"].get("tvdb") for show in _shows_of(session, m.URL_ADD)]
    assert anime_numbers == [40]
    assert show_ids == ["267440"]
    assert "81472" not in show_ids


def _remove_session():
    return _Session(
        post_handler=lambda url, body: _Resp(200, {"deleted": {"episodes": 1}, "not_found": {"shows": [], "movies": []}}),
        redirect_map={**_DBZ_REDIRECT, **_AOT_REDIRECT},
        episodes_map={**_DBZ_EPISODES, **_AOT_EPISODES},
    )


def test_dbz_mapped_removal_uses_anime(monkeypatch):
    import sync.simkl._history as m

    _patch_fs(monkeypatch, m)
    session = _remove_session()
    adapter = _adapter(session)

    ok, unresolved = m.remove(adapter, [_dbz(2, 1, 40)])

    anime_numbers = [e["number"] for grp in _anime_of(session, m.URL_REMOVE) for e in grp["episodes"]]
    assert anime_numbers == [40]
    assert list(_shows_of(session, m.URL_REMOVE)) == []
    assert unresolved == []


def test_aot_unmapped_removal_never_falls_to_shows(monkeypatch):
    import sync.simkl._history as m

    _patch_fs(monkeypatch, m)
    session = _remove_session()
    adapter = _adapter(session)

    ok, unresolved = m.remove(adapter, [_aot(4, 17)])

    assert list(_anime_of(session, m.URL_REMOVE)) == []
    assert list(_shows_of(session, m.URL_REMOVE)) == []
    assert ok == 0
    assert [u["hint"] for u in unresolved] == ["simkl_anime_remove_unmapped"]


def test_native_identity_removal_groups_by_simkl_record(monkeypatch):
    import sync.simkl._history as m

    _patch_fs(monkeypatch, m)
    session = _remove_session()
    adapter = _adapter(session)

    def _native(record, native_number, season, episode):
        item = _aot(season, episode)
        item["show_ids"] = {"tvdb": "267440", "tmdb": "1429", "simkl": record}
        item["_simkl_episode_number"] = native_number
        item["simkl_bucket"] = "anime"
        return item

    items = [
        _native("39687", 1, 1, 1),
        _native("39687", 2, 1, 2),
        _native("1579947", 1, 4, 17),
    ]
    ok, unresolved = m.remove(adapter, items)

    groups = {grp["ids"]["simkl"]: [e["number"] for e in grp["episodes"]] for grp in _anime_of(session, m.URL_REMOVE)}
    assert groups == {"39687": [1, 2], "1579947": [1]}
    assert list(_shows_of(session, m.URL_REMOVE)) == []
    assert session.gets == [g for g in session.gets if g["url"] == m.URL_ALL_ITEMS]
    assert ok == 3
    assert unresolved == []


def test_removal_no_adjacent_inference(monkeypatch):
    import sync.simkl._history as m

    _patch_fs(monkeypatch, m)
    session = _remove_session()
    adapter = _adapter(session)

    # S02E01 maps to native E40 via number_abs; the adjacent S02E02 has no native
    # mapping and must NOT be inferred into anime[] from its neighbour.
    mapped = _dbz(2, 1, 40)
    unmapped = _episode("81472", "12971", 2, 2, "Dragon Ball Z")
    _ok, unresolved = m.remove(adapter, [mapped, unmapped])

    anime_numbers = [e["number"] for grp in _anime_of(session, m.URL_REMOVE) for e in grp["episodes"]]
    assert anime_numbers == [40]
    assert list(_shows_of(session, m.URL_REMOVE)) == []
    assert [u["hint"] for u in unresolved] == ["simkl_anime_remove_unmapped"]


def test_mixed_response_confirms_all_but_not_found(monkeypatch):
    import sync.simkl._history as m

    _patch_fs(monkeypatch, m)

    not_found_obj = {
        "ids": {"tvdb": "121361", "tmdb": "1399"},
        "seasons": [{"number": 1, "episodes": [{"number": 1}]}],
    }
    # One not_found item plus deliberately underreported added counts.
    session = _Session(
        post_handler=lambda url, body: _Resp(
            200,
            {"added": {"movies": 0, "shows": 0, "episodes": 0}, "not_found": {"movies": [], "shows": [], "episodes": [not_found_obj]}},
        )
    )
    adapter = _adapter(session)

    items = [
        _episode("81189", "1396", 3, 8, "Breaking Bad"),
        _episode("121361", "1399", 1, 1, "Game of Thrones"),
        _episode("153021", "1408", 2, 5, "House"),
    ]
    ok, unresolved = m.add(adapter, items)

    assert ok == 2
    assert len(unresolved) == 1


def test_anime_like_s00_unmapped_is_unresolved(monkeypatch):
    import sync.simkl._history as m

    _patch_fs(monkeypatch, m)
    session = _Session(redirect_map=_AOT_REDIRECT, episodes_map=_AOT_EPISODES)
    adapter = _adapter(session)

    item = _s00("267440", "1429", 2, "Attack on Titan")
    ok, unresolved = m.add(adapter, [item])

    assert ok == 0
    assert len(unresolved) == 1
    assert list(_shows_of(session, m.URL_ADD)) == []
    assert list(_anime_of(session, m.URL_ADD)) == []


def test_non_anime_s00_uses_scoped_shows_payload(monkeypatch):
    import sync.simkl._history as m

    _patch_fs(monkeypatch, m)
    session = _Session()
    adapter = _adapter(session)

    item = _s00("81189", "1396", 5, "Breaking Bad")
    ok, unresolved = m.add(adapter, [item])

    shows = list(_shows_of(session, m.URL_ADD))
    assert len(shows) == 1
    seasons = {s["number"]: s for s in shows[0]["seasons"]}
    assert 0 in seasons
    assert [e["number"] for e in seasons[0]["episodes"]] == [5]
    assert list(_anime_of(session, m.URL_ADD)) == []
    assert ok == 1
    assert unresolved == []


def _anime_bucket_s00():
    return {
        "type": "episode",
        "season": 0,
        "episode": 2,
        "watched_at": "2024-01-01T00:00:00Z",
        "show_ids": {"tvdb": "555555", "tmdb": "1"},
        "series_title": "Some Anime",
        "simkl_bucket": "anime",
    }


def test_anime_bucket_s00_without_redirect_is_unresolved(monkeypatch):
    import sync.simkl._history as m

    _patch_fs(monkeypatch, m)
    session = _Session()
    adapter = _adapter(session)

    ok, unresolved = m.add(adapter, [_anime_bucket_s00()])

    assert ok == 0
    assert len(unresolved) == 1
    assert list(_shows_of(session, m.URL_ADD)) == []
    assert list(_anime_of(session, m.URL_ADD)) == []


def test_anime_bucket_s00_removal_is_unresolved(monkeypatch):
    import sync.simkl._history as m

    _patch_fs(monkeypatch, m)
    session = _Session()
    adapter = _adapter(session)

    ok, unresolved = m.remove(adapter, [_anime_bucket_s00()])

    assert ok == 0
    assert len(unresolved) == 1
    assert list(_shows_of(session, m.URL_REMOVE)) == []
    assert list(_anime_of(session, m.URL_REMOVE)) == []


def test_normal_tv_confirms_all_when_no_not_found(monkeypatch):
    import sync.simkl._history as m

    _patch_fs(monkeypatch, m)
    session = _Session(post_handler=lambda url, body: _Resp(200, _ok_add(episodes=1)))
    adapter = _adapter(session)

    items = [
        _episode("81189", "1396", 3, 8, "Breaking Bad"),
        _episode("121361", "1399", 1, 1, "Game of Thrones"),
        _episode("153021", "1408", 2, 5, "House"),
    ]
    ok, unresolved = m.add(adapter, items)

    assert unresolved == []
    assert ok == 3


def test_normal_tv_uses_shows_payload(monkeypatch):
    import sync.simkl._history as m

    _patch_fs(monkeypatch, m)
    session = _Session()
    adapter = _adapter(session)

    m.add(adapter, [_episode("81189", "1396", 3, 8, "Breaking Bad")])

    shows = list(_shows_of(session, m.URL_ADD))
    assert shows and list(_anime_of(session, m.URL_ADD)) == []
    seasons = {s["number"]: s for s in shows[0]["seasons"]}
    assert [e["number"] for e in seasons[3]["episodes"]] == [8]


def test_anime_retry_confirms_all_when_no_not_found(monkeypatch):
    import sync.simkl._history as m

    _patch_fs(monkeypatch, m)
    session = _Session(
        post_handler=lambda url, body: _Resp(200, _ok_add(episodes=0)),
        redirect_map=_DBZ_REDIRECT,
        episodes_map=_DBZ_EPISODES,
    )
    adapter = _adapter(session)

    ok, unresolved = m.add(adapter, [_dbz(2, 1, 40)])

    assert unresolved == []
    assert ok == 1


def test_dbz_absolute_fallback_group_mapping(monkeypatch):
    import sync.simkl._history as m

    _patch_fs(monkeypatch, m)
    session = _Session(episodes_map=_DBZ_EPISODES)
    adapter = _adapter(session)

    item = _dbz(2, 1, 40)
    mapped = m._anime_retry_episode_numbers_for_group(
        [item],
        {"simkl": "41487", "tvdb": "81472"},
        session=session,
        headers={},
        timeout=5,
        episode_cache={},
    )
    assert mapped == {m._thaw_key(item): 40}


def test_read_back_native_e01_maps_to_tvdb_s04e17():
    import sync.simkl._history as m

    anime_rows = [
        {
            "show": {
                "title": "Shingeki no Kyojin The Final Season Part 2",
                "ids": {"simkl": 1579947, "tvdb": "267440", "tmdb": "1429"},
                "anime_type": "tv",
            },
            "seasons": [
                {
                    "number": 1,
                    "episodes": [
                        {
                            "number": 1,
                            "watched_at": "2024-01-01T00:00:00Z",
                            "tvdb": {"season": 4, "episode": 17},
                        }
                    ],
                }
            ],
        }
    ]

    out, *_ = m._parse_rows([], [], anime_rows, limit=None)
    eps = [v for v in out.values() if str(v.get("type")) == "episode"]
    assert len(eps) == 1
    assert eps[0]["season"] == 4
    assert eps[0]["episode"] == 17


def _set_source_aliases(monkeypatch, m, aliases):
    monkeypatch.setattr(m, "_load_source_aliases", lambda: {k: dict(v) for k, v in aliases.items()})
    monkeypatch.setattr(m, "_save_source_aliases", lambda *a, **k: None)


def test_s00_special_read_back_maps_to_trakt_s00e02(monkeypatch):
    import sync.simkl._history as m
    from cw_platform.id_map import canonical_key

    trakt_item = {
        "type": "episode",
        "ids": {"tvdb": "4635717"},
        "show_ids": {"tmdb": "1429", "tvdb": "267440"},
        "season": 0,
        "episode": 2,
        "title": "AOT Special 2",
        "series_title": "Attack on Titan",
    }
    _set_source_aliases(
        monkeypatch,
        m,
        {
            "k1": {
                "season": 0,
                "episode": 2,
                "ids": {"tvdb": "4635717"},
                "show_ids": {"tmdb": "1429", "tvdb": "267440"},
                "title": "AOT Special 2",
                "series_title": "Attack on Titan",
            }
        },
    )

    anime_row = {
        "show": {"title": "AoT OVA", "ids": {"simkl": 999999, "tvdb": "888888"}, "anime_type": "special"},
        "seasons": [
            {
                "number": 1,
                "episodes": [
                    {
                        "number": 2,
                        "watched_at": "2024-05-05T00:00:00Z",
                        "ids": {"tvdb_id": "4635717"},
                        "tvdb": {"season": 5, "episode": 9},
                    }
                ],
            }
        ],
    }

    out, *_ = m._parse_rows([], [], [anime_row], limit=None)
    eps = [v for v in out.values() if str(v.get("type")) == "episode"]
    assert len(eps) == 1
    ep = eps[0]
    assert (ep["season"], ep["episode"]) == (0, 2)
    assert {k: ep["show_ids"][k] for k in ("tmdb", "tvdb")} == {"tmdb": "1429", "tvdb": "267440"}
    assert ep["show_ids"]["simkl"] == "999999"
    assert ep["_simkl_episode_number"] == 2
    assert ep["series_title"] == "Attack on Titan"
    assert ep["watched_at"] == "2024-05-05T00:00:00Z"
    assert canonical_key(ep) == canonical_key(trakt_item)


def test_special_without_episode_id_not_mapped_to_s00(monkeypatch):
    import sync.simkl._history as m

    _set_source_aliases(
        monkeypatch,
        m,
        {"k1": {"season": 0, "episode": 2, "ids": {"tvdb": "4635717"}, "show_ids": {"tmdb": "1429", "tvdb": "267440"}}},
    )

    anime_row = {
        "show": {"title": "AoT OVA", "ids": {"simkl": 999999, "tvdb": "888888"}, "anime_type": "special"},
        "seasons": [{"number": 1, "episodes": [{"number": 2, "watched_at": "2024-05-05T00:00:00Z"}]}],
    }

    out, *_ = m._parse_rows([], [], [anime_row], limit=None)
    eps = [v for v in out.values() if str(v.get("type")) == "episode"]
    assert len(eps) == 1
    assert eps[0]["season"] != 0
    assert (eps[0]["season"], eps[0]["episode"]) == (1, 2)


def test_unrelated_anime_movie_stays_movie(monkeypatch):
    import sync.simkl._history as m

    _set_source_aliases(
        monkeypatch,
        m,
        {"k1": {"season": 0, "episode": 3, "ids": {"tvdb": "111222"}, "show_ids": {"tmdb": "12971", "tvdb": "81472"}}},
    )

    anime_row = {
        "show": {"title": "Some Anime Film", "ids": {"simkl": 777, "tvdb": "999000"}, "anime_type": "movie"},
        "last_watched_at": "2024-06-06T00:00:00Z",
        "seasons": [],
    }

    out, *_ = m._parse_rows([], [], [anime_row], limit=None)
    items = list(out.values())
    assert len(items) == 1
    assert items[0]["type"] == "movie"


def test_no_simkl_history_code_reads_trakt_index():
    import inspect
    import sync.simkl._history as m

    src = inspect.getsource(m)
    assert "trakt_history.index" not in src
    assert not hasattr(m, "_trakt_history_index_path")


def test_confirmed_item_populates_source_alias(monkeypatch):
    import sync.simkl._history as m

    _patch_fs(monkeypatch, m)
    saved: dict = {}
    monkeypatch.setattr(m, "_load_source_aliases", lambda: {})
    monkeypatch.setattr(m, "_save_source_aliases", lambda items: saved.update({"items": dict(items)}))

    session = _Session(post_handler=lambda url, body: _Resp(200, _ok_add(episodes=1)))
    adapter = _adapter(session)

    item = _episode("81189", "1396", 3, 8, "Breaking Bad")
    item["ids"] = {"tvdb": "700700"}
    item["_trakt_number_abs"] = 33
    ok, unresolved = m.add(adapter, [item])

    assert ok == 1
    assert saved.get("items")
    rec = next(iter(saved["items"].values()))
    assert (rec["season"], rec["episode"]) == (3, 8)
    assert rec["ids"] == {"tvdb": "700700"}
    assert rec["number_abs"] == 33
    assert rec["show_ids"].get("tvdb") == "81189"


def test_number_abs_preserves_dbz_read_back(monkeypatch):
    import sync.simkl._history as m

    _set_source_aliases(
        monkeypatch,
        m,
        {"k1": {"season": 2, "episode": 1, "ids": {}, "show_ids": {"tvdb": "81472", "tmdb": "12971"}, "number_abs": 40, "series_title": "Dragon Ball Z"}},
    )

    anime_row = {
        "show": {"title": "Dragon Ball Z", "ids": {"simkl": 41487, "tvdb": "81472", "tmdb": "12971"}, "anime_type": "tv"},
        "seasons": [{"number": 1, "episodes": [{"number": 40, "watched_at": "2024-01-01T00:00:00Z"}]}],
    }

    out, *_ = m._parse_rows([], [], [anime_row], limit=None)
    eps = [v for v in out.values() if str(v.get("type")) == "episode"]
    assert len(eps) == 1
    assert (eps[0]["season"], eps[0]["episode"]) == (2, 1)


def test_single_episode_stays_granular(monkeypatch):
    import sync.simkl._history as m

    _patch_fs(monkeypatch, m)
    session = _Session()
    adapter = _adapter(session)

    m.add(adapter, [_episode("81189", "1396", 1, 5, "Breaking Bad")])

    shows = list(_shows_of(session, m.URL_ADD))
    seasons = {s["number"]: s for s in shows[0]["seasons"]}
    assert set(seasons.keys()) == {1}
    assert [e["number"] for e in seasons[1]["episodes"]] == [5]
    assert "watched_at" not in seasons[1]


class _RedirectSession:
    def __init__(self, location=None, raise_exc=False, status=302):
        self.gets = 0
        self._location = location
        self._raise = raise_exc
        self._status = status

    def get(self, url, headers=None, params=None, timeout=None, allow_redirects=None):
        self.gets += 1
        if self._raise:
            raise RuntimeError("boom")
        headers_out = {"Location": self._location} if self._location else {}
        return _Resp(self._status, {}, headers=headers_out)


def _patch_params(monkeypatch, m):
    monkeypatch.setattr(m, "simkl_api_params_from_headers", lambda headers=None, **k: dict(k))


def test_resolve_positive_cache_no_http():
    import sync.simkl._history as m

    state = m._AnimeResolveState({"267440": "39687"}, {})
    sess = _RedirectSession()
    out = m._resolved_anime_ids_for_tvdb(sess, {}, 5, "267440", state)
    assert out == {"simkl": "39687", "tvdb": "267440"}
    assert sess.gets == 0
    assert state.cached_positive == 1


def test_resolve_negative_cache_no_http():
    import sync.simkl._history as m

    state = m._AnimeResolveState({}, {"999": m._now_epoch()})
    sess = _RedirectSession()
    out = m._resolved_anime_ids_for_tvdb(sess, {}, 5, "999", state)
    assert out == {}
    assert sess.gets == 0
    assert state.cached_negative == 1


def test_resolve_non_anime_persists_negative_and_is_not_failed(monkeypatch):
    import sync.simkl._history as m

    _patch_params(monkeypatch, m)
    saved = []
    monkeypatch.setattr(m, "_save_anime_resolve_cache", lambda st: saved.append(st))
    state = m._AnimeResolveState({}, {})
    sess = _RedirectSession(location="https://simkl.com/tv/12345/foo")

    out = m._resolved_anime_ids_for_tvdb(sess, {}, 5, "888", state)
    assert out == {}
    assert "888" in state.misses
    assert state.non_anime == 1
    assert state.failed == 0
    assert saved

    out2 = m._resolved_anime_ids_for_tvdb(sess, {}, 5, "888", state)
    assert out2 == {}
    assert sess.gets == 1
    assert state.cached_negative == 1


def test_resolve_positive_removes_expired_negative(monkeypatch):
    import sync.simkl._history as m

    _patch_params(monkeypatch, m)
    monkeypatch.setattr(m, "_save_anime_resolve_cache", lambda st: None)
    old = m._now_epoch() - m._ANIME_RESOLVE_MISS_TTL - 10
    state = m._AnimeResolveState({}, {"777": old})
    sess = _RedirectSession(location="https://simkl.com/anime/41487/dbz")

    out = m._resolved_anime_ids_for_tvdb(sess, {}, 5, "777", state)
    assert out == {"simkl": "41487", "tvdb": "777"}
    assert "777" not in state.misses
    assert state.resolved["777"] == "41487"
    assert sess.gets == 1


def test_resolve_http_exception_is_failed(monkeypatch):
    import sync.simkl._history as m

    _patch_params(monkeypatch, m)
    state = m._AnimeResolveState({}, {})
    sess = _RedirectSession(raise_exc=True)

    out = m._resolved_anime_ids_for_tvdb(sess, {}, 5, "500", state)
    assert out == {}
    assert state.failed == 1
    assert state.non_anime == 0
    assert "500" not in state.misses


def test_resolve_error_status_not_negatively_cached(monkeypatch):
    import sync.simkl._history as m

    _patch_params(monkeypatch, m)
    saved = []
    monkeypatch.setattr(m, "_save_anime_resolve_cache", lambda st: saved.append(st))
    for status in (401, 403, 429, 500, 503):
        state = m._AnimeResolveState({}, {})
        sess = _RedirectSession(location="https://simkl.com/tv/12345/foo", status=status)
        out = m._resolved_anime_ids_for_tvdb(sess, {}, 5, "888", state)
        assert out == {}
        assert "888" not in state.misses
        assert state.non_anime == 0
        assert state.failed == 1
    assert saved == []


def test_resolve_redirect_without_location_not_negatively_cached(monkeypatch):
    import sync.simkl._history as m

    _patch_params(monkeypatch, m)
    monkeypatch.setattr(m, "_save_anime_resolve_cache", lambda st: None)
    state = m._AnimeResolveState({}, {})
    sess = _RedirectSession(location=None, status=302)

    out = m._resolved_anime_ids_for_tvdb(sess, {}, 5, "888", state)
    assert out == {}
    assert "888" not in state.misses
    assert state.non_anime == 0
    assert state.failed == 1


def test_load_resolve_cache_backward_compat_flat(monkeypatch):
    import sync.simkl._history as m

    monkeypatch.setattr(m, "_load_json", lambda path: {"tvdb_to_simkl": {"111": "222"}})
    state = m._load_anime_resolve_cache()
    assert state.resolved == {"111": "222"}
    assert state.misses == {}


def test_load_resolve_cache_new_format(monkeypatch):
    import sync.simkl._history as m

    monkeypatch.setattr(m, "_load_json", lambda path: {"resolved": {"1": "2"}, "misses": {"3": 1700000000}})
    state = m._load_anime_resolve_cache()
    assert state.resolved == {"1": "2"}
    assert state.misses == {"3": 1700000000}


def test_show_exact_episode_id_alias_restores_season_episode(monkeypatch):
    import sync.simkl._history as m
    from cw_platform.id_map import canonical_key

    trakt_item = {
        "type": "episode",
        "ids": {"tvdb": "5057304"},
        "show_ids": {"tmdb": "42009", "tvdb": "253463"},
        "season": 2,
        "episode": 4,
        "title": "White Christmas",
        "series_title": "Black Mirror",
        "series_year": 2011,
    }
    _set_source_aliases(
        monkeypatch,
        m,
        {
            "k1": {
                "season": 2,
                "episode": 4,
                "ids": {"tvdb": "5057304"},
                "show_ids": {"tmdb": "42009", "tvdb": "253463"},
                "title": "White Christmas",
                "series_title": "Black Mirror",
                "series_year": 2011,
            }
        },
    )

    show_row = {
        "show": {"title": "Black Mirror", "year": 2011, "ids": {"tmdb": "42009", "tvdb": "253463", "simkl": 12345}},
        "seasons": [
            {
                "number": 0,
                "episodes": [
                    {"number": 1, "watched_at": "2024-05-05T00:00:00Z", "ids": {"tvdb": "5057304"}}
                ],
            }
        ],
    }

    out, *_ = m._parse_rows([], [show_row], [], limit=None)
    eps = [v for v in out.values() if str(v.get("type")) == "episode"]
    assert len(eps) == 1
    ep = eps[0]
    assert (ep["season"], ep["episode"]) == (2, 4)
    assert ep["ids"] == {"tvdb": "5057304"}
    assert ep["show_ids"] == {"tmdb": "42009", "tvdb": "253463"}
    assert ep["title"] == "White Christmas"
    assert ep["series_title"] == "Black Mirror"
    assert ep["series_year"] == 2011
    assert ep["watched_at"] == "2024-05-05T00:00:00Z"
    assert canonical_key(ep) == canonical_key(trakt_item)


def test_show_unique_exact_id_maps_across_anthology_split(monkeypatch):
    import sync.simkl._history as m

    aliases = {}
    for n in range(1, 10):
        aliases[f"m{n}"] = {
            "season": 1,
            "episode": n,
            "ids": {"tvdb": str(10649168 + n)},
            "show_ids": {"tmdb": "225634"},
            "title": f"Monsters S01E{n:02d}",
            "series_title": "Monsters",
        }
    _set_source_aliases(monkeypatch, m, aliases)

    show_row = {
        "show": {"title": "Monster", "ids": {"tmdb": "113988", "simkl": 5150}},
        "seasons": [
            {
                "number": 2,
                "episodes": [
                    {
                        "number": n,
                        "watched_at": "2024-05-05T00:00:00Z",
                        "ids": {"tvdb": str(10649168 + n)},
                    }
                    for n in range(1, 10)
                ],
            }
        ],
    }

    out, *_ = m._parse_rows([], [show_row], [], limit=None)
    eps = [v for v in out.values() if str(v.get("type")) == "episode"]
    assert len(eps) == 9
    assert sorted((e["season"], e["episode"]) for e in eps) == [(1, n) for n in range(1, 10)]
    assert all(e["show_ids"] == {"tmdb": "225634"} for e in eps)
    assert all(e["series_title"] == "Monsters" for e in eps)


def _wvl_aliases():
    return {
        "a5": {
            "season": 3,
            "episode": 5,
            "ids": {"tvdb": "111005"},
            "show_ids": {"tmdb": "5000", "tvdb": "6000"},
            "title": "WVL S03E05",
        },
        "a7": {
            "season": 3,
            "episode": 5,
            "ids": {"tvdb": "111007"},
            "show_ids": {"tmdb": "5000", "tvdb": "6000"},
            "title": "WVL S03E07",
        },
    }


def test_show_alias_rejected_when_target_coordinate_already_watched(monkeypatch):
    import sync.simkl._history as m

    _set_source_aliases(monkeypatch, m, _wvl_aliases())

    show_row = {
        "show": {"title": "Winter vol Liefde", "ids": {"tmdb": "5000", "tvdb": "6000", "simkl": 4242}},
        "seasons": [
            {
                "number": 3,
                "episodes": [
                    {"number": 5, "watched_at": "2024-05-05T00:00:00Z", "ids": {"tvdb": "111005"}},
                    {"number": 7, "watched_at": "2024-05-06T00:00:00Z", "ids": {"tvdb": "111007"}},
                ],
            }
        ],
    }

    out, *_ = m._parse_rows([], [show_row], [], limit=None)
    eps = [v for v in out.values() if str(v.get("type")) == "episode"]
    coords = sorted((e["season"], e["episode"]) for e in eps)
    assert coords == [(3, 5), (3, 7)]


def test_show_alias_accepted_when_target_not_otherwise_watched(monkeypatch):
    import sync.simkl._history as m

    _set_source_aliases(monkeypatch, m, {"a7": _wvl_aliases()["a7"]})

    show_row = {
        "show": {"title": "Winter vol Liefde", "ids": {"tmdb": "5000", "tvdb": "6000", "simkl": 4242}},
        "seasons": [
            {
                "number": 3,
                "episodes": [
                    {"number": 7, "watched_at": "2024-05-06T00:00:00Z", "ids": {"tvdb": "111007"}},
                ],
            }
        ],
    }

    out, *_ = m._parse_rows([], [show_row], [], limit=None)
    eps = [v for v in out.values() if str(v.get("type")) == "episode"]
    assert len(eps) == 1
    assert (eps[0]["season"], eps[0]["episode"]) == (3, 5)


def test_unwatched_rows_do_not_block_alias_coordinates(monkeypatch):
    import sync.simkl._history as m

    _set_source_aliases(monkeypatch, m, {"a7": _wvl_aliases()["a7"]})

    show_row = {
        "show": {"title": "Winter vol Liefde", "ids": {"tmdb": "5000", "tvdb": "6000", "simkl": 4242}},
        "seasons": [
            {
                "number": 3,
                "episodes": [
                    {"number": 5},
                    {"number": 7, "watched_at": "2024-05-06T00:00:00Z", "ids": {"tvdb": "111007"}},
                ],
            }
        ],
    }

    out, *_ = m._parse_rows([], [show_row], [], limit=None)
    eps = [v for v in out.values() if str(v.get("type")) == "episode"]
    assert len(eps) == 1
    assert (eps[0]["season"], eps[0]["episode"]) == (3, 5)


def test_collision_gate_not_applied_to_anime(monkeypatch):
    import sync.simkl._history as m

    _set_source_aliases(
        monkeypatch,
        m,
        {
            "k1": {
                "season": 1,
                "episode": 2,
                "ids": {"tvdb": "4635717"},
                "show_ids": {"tmdb": "1429", "tvdb": "267440"},
                "title": "AOT Special 2",
            }
        },
    )

    anime_row = {
        "show": {"title": "AoT OVA", "ids": {"simkl": 999999, "tvdb": "888888"}},
        "seasons": [
            {
                "number": 1,
                "episodes": [
                    {"number": 2, "watched_at": "2024-05-05T00:00:00Z"},
                    {"number": 3, "watched_at": "2024-05-06T00:00:00Z", "ids": {"tvdb": "4635717"}},
                ],
            }
        ],
    }

    out, *_ = m._parse_rows([], [], [anime_row], limit=None)
    eps = [v for v in out.values() if str(v.get("type")) == "episode"]
    mapped = [e for e in eps if e["show_ids"].get("tvdb") == "267440"]
    assert len(mapped) == 1
    assert (mapped[0]["season"], mapped[0]["episode"]) == (1, 2)


def test_show_no_abs_or_title_mapping_applied(monkeypatch):
    import sync.simkl._history as m

    _set_source_aliases(
        monkeypatch,
        m,
        {
            "k1": {
                "season": 2,
                "episode": 4,
                "ids": {"tvdb": "5057304"},
                "show_ids": {"tmdb": "42009", "tvdb": "253463"},
                "title": "White Christmas",
                "number_abs": 7,
            }
        },
    )

    show_row = {
        "show": {"title": "Black Mirror", "ids": {"tmdb": "42009", "tvdb": "253463", "simkl": 12345}},
        "seasons": [
            {
                "number": 1,
                "episodes": [
                    {"number": 7, "watched_at": "2024-05-05T00:00:00Z", "title": "White Christmas"}
                ],
            }
        ],
    }

    out, *_ = m._parse_rows([], [show_row], [], limit=None)
    eps = [v for v in out.values() if str(v.get("type")) == "episode"]
    assert len(eps) == 1
    assert (eps[0]["season"], eps[0]["episode"]) == (1, 7)

