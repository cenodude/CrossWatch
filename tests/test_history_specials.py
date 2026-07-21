from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from cw_platform.id_map import canonical_key, minimal
from cw_platform.orchestrator import _applier, _unresolved
from providers.sync.mdblist._history import _bucketize
from providers.sync.publicmetadb._history import _payload_for_item, _to_minimal
from providers.sync._mod_SIMKL import _confirmed_keys
from providers.sync.simkl import _history as simkl_history
from providers.sync.simkl._common import key_of as simkl_key_of
from providers.sync.trakt._history import _batch_add, _batch_remove
from providers.sync.trakt._history import _correlate_not_found as th_correlate


WATCHED_AT = "2026-01-02T03:04:05Z"
SHOW_IDS = {"tmdb": "63372", "tvdb": "295685", "trakt": "100"}
ISSUE_311_ITEM = {
    "type": "episode",
    "title": "The Making of The Walking Dead",
    "season": 0,
    "episode": 48,
    "watched_at": "2023-10-12T12:28:00.000Z",
    "ids": {"tmdb": "63372", "tvdb": "2960601", "trakt": "5942444"},
    "show_ids": {
        "tmdb": "1402",
        "imdb": "tt1520211",
        "tvdb": "153021",
        "trakt": "1393",
        "slug": "the-walking-dead",
    },
}


def _special(number: int) -> dict[str, object]:
    return {
        "type": "episode",
        "ids": dict(SHOW_IDS),
        "show_ids": dict(SHOW_IDS),
        "season": 0,
        "episode": number,
        "watched_at": WATCHED_AT,
    }


def test_special_episode_canonical_key_keeps_season_zero() -> None:
    item = {"type": "episode", "show_ids": {"tmdb": "63372"}, "season": 0, "episode": 1}

    assert canonical_key(item) == "tmdb:63372#s00e01"


def test_publicmetadb_special_episode_round_trip_shape() -> None:
    row = {
        "id": "history-1",
        "tmdb_id": 63372,
        "media_type": "tv",
        "season": 0,
        "episode": 1,
        "watched_at": WATCHED_AT,
    }

    parsed = _to_minimal(row)
    payload, hint = _payload_for_item(_special(1))

    assert parsed is not None
    assert parsed["season"] == 0
    assert parsed["episode"] == 1
    assert parsed["show_ids"] == {"tmdb": "63372"}
    assert payload == {
        "tmdb_id": 63372,
        "media_type": "tv",
        "season": 0,
        "episode": 1,
        "watched_at": WATCHED_AT,
    }
    assert hint is None


def test_mdblist_groups_multiple_specials_under_one_season() -> None:
    body, accepted = _bucketize([_special(1), _special(2)], unwatch=False)

    shows = body["shows_nested"]
    assert len(shows) == 1
    assert shows[0]["seasons"] == [
        {
            "number": 0,
            "episodes": [
                {"number": 1, "watched_at": WATCHED_AT},
                {"number": 2, "watched_at": WATCHED_AT},
            ],
        }
    ]
    assert len(accepted) == 2


def test_trakt_special_episode_add_and_remove_shapes() -> None:
    adapter = SimpleNamespace(config={})

    add_body, add_unresolved, *_ = _batch_add(adapter, [_special(1), _special(2)])
    remove_body, remove_unresolved, *_ = _batch_remove(adapter, [_special(1), _special(2)])

    add_season = add_body["shows"][0]["seasons"][0]
    remove_season = remove_body["shows"][0]["seasons"][0]
    assert add_season["number"] == 0
    assert [episode["number"] for episode in add_season["episodes"]] == [1, 2]
    assert remove_season == {"number": 0, "episodes": [{"number": 1}, {"number": 2}]}
    assert add_unresolved == []
    assert remove_unresolved == []


def test_simkl_special_episode_is_available_as_history_source() -> None:
    row = {
        "show": {
            "title": "Example Show",
            "year": 2020,
            "ids": dict(SHOW_IDS),
        },
        "seasons": [
            {
                "number": 0,
                "episodes": [
                    {
                        "number": 1,
                        "watched_at": WATCHED_AT,
                        "ids": {"tvdb": "900001"},
                    },
                    {"number": 2, "watched_at": WATCHED_AT},
                ],
            }
        ],
    }

    items, _thaw, _movies_ts, shows_ts, _anime_ts, movies_count, episode_count = simkl_history._parse_rows(
        [], [row], [], limit=None
    )

    assert len(items) == 2
    by_episode = {item["episode"]: item for item in items.values()}
    assert by_episode[1]["season"] == 0
    assert by_episode[1]["ids"] == {"tvdb": "900001"}
    assert by_episode[2]["season"] == 0
    assert by_episode[2]["ids"] == SHOW_IDS
    assert by_episode[2]["show_ids"] == SHOW_IDS
    assert shows_ts is not None
    assert movies_count == 0
    assert episode_count == 2


def test_simkl_special_episode_remove_uses_episode_lookup_ids(monkeypatch) -> None:
    requests: list[dict[str, object]] = []

    class Session:
        def post(self, _url, **kwargs):
            requests.append(kwargs["json"])
            return SimpleNamespace(status_code=200, text="", json=lambda: {})

        def get(self, _url, **kwargs):
            payload = {"movies": [], "shows": [], "anime": []}
            return SimpleNamespace(status_code=200, ok=True, text="json", headers={}, json=lambda: payload)

    monkeypatch.setattr(simkl_history, "_cache_save", lambda _items: None)
    monkeypatch.setattr(simkl_history, "_forget_source_aliases", lambda _items: None)
    monkeypatch.setattr(simkl_history, "_unfreeze", lambda _keys: None)
    adapter = SimpleNamespace(
        client=SimpleNamespace(session=Session()),
        cfg=SimpleNamespace(timeout=5, history_chunk_size=100, api_key="key", access_token="token"),
    )
    item = _special(1)
    item["ids"] = {"tvdb": "900001"}

    applied, unresolved = simkl_history.remove(adapter, [item])

    assert applied == 1
    assert unresolved == []
    episode = requests[0]["shows"][0]["seasons"][0]["episodes"][0]
    assert episode == {"number": 1, "ids": {"tvdb": "900001"}}


def test_issue_311_simkl_add_sends_episode_lookup_id(monkeypatch) -> None:
    requests: list[dict[str, object]] = []

    class Session:
        def post(self, _url, **kwargs):
            requests.append(kwargs["json"])
            payload = {
                "added": {"movies": 0, "shows": 0, "episodes": 1},
                "not_found": {"movies": [], "shows": [], "episodes": []},
            }
            return SimpleNamespace(status_code=201, text="json", json=lambda: payload)

    monkeypatch.setattr(simkl_history, "_unfreeze", lambda _keys: None)
    monkeypatch.setattr(simkl_history, "_inject_adds_into_cache", lambda _items: None)
    adapter = SimpleNamespace(
        client=SimpleNamespace(session=Session()),
        cfg=SimpleNamespace(timeout=5, api_key="key", access_token="token"),
    )

    applied, unresolved = simkl_history.add(adapter, [dict(ISSUE_311_ITEM)])

    assert applied == 1
    assert unresolved == []
    show = requests[0]["shows"][0]
    assert show["ids"]["tmdb"] == "1402"
    assert show["seasons"] == [
        {
            "number": 0,
            "episodes": [
                {
                    "number": 48,
                    "watched_at": "2023-10-12T12:28:00.000Z",
                    "ids": {"tvdb": "2960601"},
                }
            ],
        }
    ]


def test_issue_311_rejected_special_is_blocked_after_first_attempt(monkeypatch, tmp_path) -> None:
    class Session:
        def post(self, _url, **kwargs):
            payload = {
                "added": {"movies": 0, "shows": 0, "episodes": 0},
                "not_found": {"movies": [], "shows": [], "episodes": kwargs["json"]["shows"]},
            }
            return SimpleNamespace(status_code=201, text="json", json=lambda: payload)

    monkeypatch.setattr(simkl_history, "_unfreeze", lambda _keys: None)
    monkeypatch.setattr(_unresolved, "STATE_DIR", tmp_path)
    monkeypatch.setenv("CW_PAIR_KEY", "TRAKT-SIMKL-history-issue311")
    adapter = SimpleNamespace(
        client=SimpleNamespace(session=Session()),
        cfg=SimpleNamespace(timeout=5, api_key="key", access_token="token"),
    )
    source_item = dict(ISSUE_311_ITEM)

    applied, unresolved = simkl_history.add(adapter, [source_item])
    confirmed = _confirmed_keys(simkl_key_of, [source_item], unresolved)
    normalized = _applier._normalize(
        {"ok": True, "count": applied, "unresolved": unresolved, "confirmed_keys": confirmed},
        [minimal(source_item)],
        "add",
        dst="SIMKL",
        feature="history",
        emit=lambda *_args, **_kwargs: None,
    )
    blocked = _unresolved.load_unresolved_keys("SIMKL", "history", cross_features=True)

    expected_key = canonical_key(minimal(source_item))
    assert applied == 0
    assert normalized["unresolved"] == 1
    assert confirmed == []
    assert blocked == {expected_key}


# --- SIMKL -> TRAKT history mapping and accounting ---

TRAKT_WATCHED = "2024-01-01T00:00:00Z"


class _TResp:
    def __init__(self, status=200, payload=None):
        self.status_code = status
        self._payload = payload if payload is not None else {}
        self.text = ""

    def json(self):
        return self._payload


class _TraktFake:
    def __init__(self, *, catalog=None, ep_search=None, post_payload=None, show_hits=None,
                 remove_payload=None, history_rows=None, remove_status=200, retry_payload=None,
                 remove_coord_payload=None):
        self.catalog = catalog or {}
        self.ep_search = ep_search or {}
        self.post_payload = post_payload or {"added": {}, "existing": {}, "not_found": {}}
        self.retry_payload = retry_payload
        self.show_hits = show_hits or {}
        self.remove_payload = remove_payload or {"deleted": {"episodes": 0}, "not_found": {"ids": []}}
        self.remove_coord_payload = remove_coord_payload
        self.remove_status = remove_status
        self.history_rows = history_rows or {}
        self.posts = []
        self.removes = []
        self.gets = []

    def __call__(self, sess, method, url, **kw):
        if method == "POST":
            if url.endswith("/sync/history/remove"):
                sent = kw.get("json") or {}
                self.removes.append(sent)
                if "ids" not in sent and self.remove_coord_payload is not None:
                    return _TResp(self.remove_status, self.remove_coord_payload)
                return _TResp(self.remove_status, self.remove_payload)
            self.posts.append(kw.get("json"))
            if self.retry_payload is not None and len(self.posts) > 1:
                return _TResp(200, self.retry_payload)
            return _TResp(200, self.post_payload)
        self.gets.append({'url': url, 'params': dict(kw.get('params') or {})})
        params = kw.get("params") or {}
        if "/sync/history/episodes/" in url:
            ep_id = url.rsplit("/", 1)[-1]
            return _TResp(200, list(self.history_rows.get(ep_id, [])))
        if "/search/" in url:
            _, _, tail = url.partition("/search/")
            service, _, value = tail.partition("/")
            if params.get("type") == "episode":
                hit = self.ep_search.get((service, value))
                return _TResp(200, [hit] if hit else [])
            return _TResp(200, self.show_hits.get((service, value), []))
        if "/seasons" in url:
            show = url.split("/shows/")[1].split("/")[0]
            return _TResp(200, self.catalog.get(show, []))
        return _TResp(404, {})


def _episode_searches(fake):
    return [g for g in fake.gets if "/search/" in g["url"] and g["params"].get("type") == "episode"]


def _catalogue_calls(fake):
    return [g for g in fake.gets if "/seasons" in g["url"]]


def _trakt_adapter():
    return SimpleNamespace(
        client=SimpleNamespace(session=object()),
        config={"timeout": 5, "max_retries": 1, "history_write_timeout": 5},
    )


def _patch_trakt(monkeypatch, fake, tmp_path=None):
    import providers.sync.trakt._history as th
    monkeypatch.setenv("CW_PAIR_SRC", "SIMKL")
    monkeypatch.setenv("CW_PAIR_DST", "TRAKT")
    monkeypatch.setenv("CW_PAIR_KEY", "SIMKL-TRAKT-test")
    monkeypatch.setattr(th, "request_with_retries", fake)
    monkeypatch.setattr(th, "headers_for_adapter", lambda *a, **k: {})
    monkeypatch.setattr(th, "_cache_merge_from_source_items", lambda *a, **k: None)
    monkeypatch.setattr(th, "_bust_index_cache", lambda *a, **k: None)
    monkeypatch.setenv("CW_RUN_ID", "run-1")
    if tmp_path is None:
        monkeypatch.setattr(th, "_alias_save", lambda: None)
        monkeypatch.setattr(th, "_cache_remove_event_keys", lambda *a, **k: None)
        monkeypatch.setattr(th, "_cache_remove_source_items", lambda *a, **k: None)
    else:
        def _scoped_state_file(name):
            p = Path(name)
            scope = th._pair_scope() or "unscoped"
            return Path(tmp_path) / f"{p.stem}.{scope}{p.suffix}"
        monkeypatch.setattr(th, "state_file", _scoped_state_file)
    th._ALIAS_STATE["scope"] = th._alias_scope()
    th._ALIAS_STATE["items"] = {}
    th._ALIAS_REBUILD.update({"scope": "", "rebuilt": 0, "ambiguous": 0})
    th._SRC_SNAPSHOT["scope"] = ""
    th._SRC_SNAPSHOT["by_key"] = {}
    th._reset_simkl_maps(th._map_scope())
    return th


def _simkl_ep(season, episode, *, native=None, ep_ids=None, title=None, show_ids=None, series="Dragon Ball Z", bucket="anime"):
    it = {
        "type": "episode",
        "season": season,
        "episode": episode,
        "watched_at": TRAKT_WATCHED,
        "show_ids": dict(show_ids or {"tvdb": "81472", "tmdb": "12971", "simkl": "41487"}),
        "ids": dict(ep_ids or {}),
        "series_title": series,
    }
    if bucket:
        it["simkl_bucket"] = bucket
    if native is not None:
        it["_simkl_episode_number"] = native
    if title is not None:
        it["title"] = title
    return it


DBZ_SHOW_HITS = {("tmdb", "12971"): [{"show": {"ids": {"trakt": 12971}}}]}
MONSTER_SHOW_HITS = {("tmdb", "225634"): [{"show": {"ids": {"trakt": 225634}}}]}


def _dbz_catalog():
    return {
        "12971": [
            {"number": 1, "episodes": [
                {"number": n, "number_abs": n, "title": f"Ep{n}", "ids": {"trakt": str(8000 + n)}}
                for n in range(1, 40)
            ]},
            {"number": 2, "episodes": [
                {"number": 1, "number_abs": 40, "title": "Ep40", "ids": {"trakt": "9001"}},
                {"number": 2, "number_abs": 41, "title": "Ep41", "ids": {"trakt": "9002"}},
            ]},
        ]
    }


def test_dbz_native_numbers_map_to_trakt_season_coordinates(monkeypatch) -> None:
    fake = _TraktFake(catalog=_dbz_catalog(), show_hits=DBZ_SHOW_HITS, post_payload={"added": {"episodes": 2}, "existing": {}, "not_found": {}})
    th = _patch_trakt(monkeypatch, fake)

    src = [_simkl_ep(1, 40, native=40), _simkl_ep(1, 41, native=41)]
    before = [dict(x) for x in src]

    res = th.add(_trakt_adapter(), src)

    body = fake.posts[0]
    sent = sorted(int(e["ids"]["trakt"]) for e in (body.get("episodes") or []))
    assert sent == [9001, 9002]
    assert "seasons" not in json.dumps(body)
    assert len(res["confirmed_keys"]) == 2
    assert res["unresolved"] == []
    assert src == before


def test_monster_split_show_uses_exact_episode_id(monkeypatch) -> None:
    fake = _TraktFake(
        ep_search={("tvdb", "10649169"): {
            "episode": {"ids": {"trakt": "555", "tvdb": "10649169"}, "season": 1, "number": 1, "title": "M1"},
            "show": {"ids": {"trakt": "777", "tmdb": "225634"}},
        }},
        post_payload={
            "added": {}, "existing": {},
            "not_found": {"shows": [{"ids": {"tmdb": 113988}, "seasons": [{"number": 2, "episodes": [{"number": 1}]}]}]},
        },
    )
    th = _patch_trakt(monkeypatch, fake)

    src = [_simkl_ep(2, 1, ep_ids={"tvdb": "10649169"}, show_ids={"tmdb": "113988"}, series="Monster", bucket="shows")]
    before = [dict(x) for x in src]
    res = th.add(_trakt_adapter(), src)

    first = fake.posts[0]
    assert "shows" in first
    retry = fake.posts[1]
    assert int(retry["episodes"][0]["ids"]["trakt"]) == 555
    assert len(res["confirmed_keys"]) == 1
    assert src == before


def test_black_mirror_special_stays_unresolved(monkeypatch) -> None:
    fake = _TraktFake(
        show_hits={("tmdb", "42009"): [{"show": {"ids": {"trakt": 42009}}}]},
        post_payload={
            "added": {}, "existing": {},
            "not_found": {"shows": [{"ids": {"tmdb": 42009}, "seasons": [{"number": 0, "episodes": [{"number": 1}]}]}]},
        },
    )
    th = _patch_trakt(monkeypatch, fake)

    src = [_simkl_ep(0, 1, title="Unknown Special", show_ids={"tmdb": "42009"}, series="Black Mirror", bucket="shows")]
    res = th.add(_trakt_adapter(), src)

    assert res["confirmed_keys"] == []
    assert res["count"] == 0
    assert [u["hint"] for u in res["unresolved"]] == ["trakt_episode_not_found"]
    assert _episode_searches(fake) == []


def test_show_level_not_found_expands_to_all_attempted(monkeypatch) -> None:
    fake = _TraktFake(
        catalog=_dbz_catalog(),
        show_hits=DBZ_SHOW_HITS,
        post_payload={
            "added": {}, "existing": {},
            "not_found": {"shows": [{"ids": {"tmdb": 12971}}]},
        },
    )
    th = _patch_trakt(monkeypatch, fake)

    src = [_simkl_ep(1, 40, native=40), _simkl_ep(1, 41, native=41)]
    res = th.add(_trakt_adapter(), src)

    assert res["count"] == 0
    assert res["confirmed_keys"] == []
    assert len(res["unresolved_keys"]) == 2
    assert set(u["hint"] for u in res["unresolved"]) == {"trakt_parent_not_found"}
    for u in res["unresolved"]:
        assert u["item"].get("ids") or u["item"].get("show_ids")


def test_season_level_not_found_gives_nine_unresolved_zero_skipped(monkeypatch) -> None:
    catalog = {"225634": [{"number": 1, "episodes": [
        {"number": n, "number_abs": n, "title": f"M{n}", "ids": {"trakt": str(600 + n)}} for n in range(1, 10)
    ]}]}
    fake = _TraktFake(
        catalog=catalog,
        show_hits=MONSTER_SHOW_HITS,
        post_payload={
            "added": {}, "existing": {},
            "not_found": {"shows": [{"ids": {"tmdb": 225634}, "seasons": [{"number": 1}]}]},
        },
    )
    th = _patch_trakt(monkeypatch, fake)

    src = [
        _simkl_ep(1, n, native=n, show_ids={"tmdb": "225634", "simkl": "5150"}, series="Monster")
        for n in range(1, 10)
    ]
    res = th.add(_trakt_adapter(), src)

    assert len(res["unresolved_keys"]) == 9
    assert res["skipped_keys"] == []
    assert res["count"] == 0

    norm = _applier._normalize(res, src, "apply:add", dst="TRAKT", feature="history", emit=lambda *a, **k: None)
    assert norm["attempted"] == 9
    assert norm["confirmed"] == 0
    assert norm["unresolved"] == 9
    assert norm["skipped"] == 0


def test_aggregate_added_counters_do_not_inflate_count(monkeypatch) -> None:
    fake = _TraktFake(
        catalog=_dbz_catalog(),
        show_hits=DBZ_SHOW_HITS,
        post_payload={
            "added": {"shows": 1, "seasons": 1, "episodes": 2},
            "existing": {}, "not_found": {},
        },
    )
    th = _patch_trakt(monkeypatch, fake)

    src = [_simkl_ep(1, 40, native=40), _simkl_ep(1, 41, native=41)]
    res = th.add(_trakt_adapter(), src)

    assert res["count"] == 2
    assert len(res["confirmed_keys"]) == 2


def test_mod_trakt_passes_through_exact_fields(monkeypatch) -> None:
    from providers.sync import _mod_TRAKT as mod

    exact = {
        "ok": True,
        "count": 99,
        "confirmed_keys": ["k1"],
        "unresolved": [{"item": {}, "hint": "x"}],
        "unresolved_keys": ["k2", "k3"],
        "skipped_keys": ["k4"],
        "reason_counts": {"x": 1},
        "ambiguous": True,
    }
    fake_mod = SimpleNamespace(add=lambda adapter, lst: exact)
    monkeypatch.setitem(mod._FEATURES, "history", fake_mod)

    adapter = SimpleNamespace(
        _is_enabled=lambda f: True,
        key_of=lambda it: "SHOULD_NOT_BE_USED",
    )
    out = mod.TRAKTModule.add(adapter, "history", [{"a": 1}, {"b": 2}, {"c": 3}, {"d": 4}])

    assert out["confirmed_keys"] == ["k1"]
    assert out["count"] == 1
    assert out["unresolved_keys"] == ["k2", "k3"]
    assert out["skipped_keys"] == ["k4"]
    assert out["reason_counts"] == {"x": 1}
    assert out["ambiguous"] is True


def test_applier_ambiguous_result_leaves_remainder_unresolved(monkeypatch) -> None:
    monkeypatch.setattr(_applier, "record_unresolved", lambda *a, **k: {"ok": True})
    items = [{"type": "movie", "ids": {"tmdb": str(i)}} for i in range(10)]
    keys = [canonical_key(it) for it in items]
    res = {
        "ok": True,
        "count": 4,
        "confirmed_keys": keys[:4],
        "unresolved": [],
        "skipped_keys": [],
        "ambiguous": True,
    }

    norm = _applier._normalize(res, items, "apply:add", dst="TRAKT", feature="history", emit=lambda *a, **k: None)

    assert norm["attempted"] == 10
    assert norm["confirmed"] == 4
    assert norm["skipped"] == 0
    assert norm["skipped_inferred"] == 0
    assert norm["unresolved"] == 6
    assert sorted(norm["unresolved_keys"]) == sorted(keys[4:])


def test_applier_prefers_provider_unresolved_keys(monkeypatch) -> None:
    monkeypatch.setattr(_applier, "record_unresolved", lambda *a, **k: {"ok": True})
    items = [{"type": "movie", "ids": {"tmdb": str(i)}} for i in range(3)]
    keys = [canonical_key(it) for it in items]
    res = {
        "ok": True,
        "count": 1,
        "confirmed_keys": [keys[0]],
        "unresolved": [{"item": {"type": "movie", "ids": {}}, "hint": "x"}],
        "unresolved_keys": [keys[1], keys[2]],
        "skipped_keys": [],
    }

    norm = _applier._normalize(res, items, "apply:add", dst="TRAKT", feature="history", emit=lambda *a, **k: None)

    assert sorted(norm["unresolved_keys"]) == sorted(keys[1:])
    assert norm["confirmed"] == 1
    assert norm["skipped"] == 0


def test_unresolved_reports_simkl_source_and_cache_gets_trakt_item(monkeypatch) -> None:
    fake = _TraktFake(
        catalog=_dbz_catalog(),
        show_hits=DBZ_SHOW_HITS,
        post_payload={
            "added": {"episodes": 1}, "existing": {},
            "not_found": {"shows": [{"ids": {"tmdb": 12971}, "seasons": [{"number": 2, "episodes": [{"number": 2}]}]}]},
        },
    )
    th = _patch_trakt(monkeypatch, fake)
    cached: list = []
    monkeypatch.setattr(th, "_cache_merge_from_source_items", lambda adapter, its: cached.extend(its))

    src = [_simkl_ep(1, 40, native=40), _simkl_ep(1, 41, native=41)]
    res = th.add(_trakt_adapter(), src)

    assert len(res["unresolved"]) == 1
    bad = res["unresolved"][0]["item"]
    assert bad.get("season") == 1 and bad.get("episode") == 41

    assert len(cached) == 1
    good = cached[0]
    assert (good["season"], good["episode"]) == (2, 1)
    assert str(good["ids"]["trakt"]) == "9001"
    assert "simkl" not in json.dumps(good.get("ids") or {})


def test_aot_oad_separate_record_stays_unresolved(monkeypatch) -> None:
    fake = _TraktFake(
        catalog={"1429": [{"number": 1, "episodes": [
            {"number": n, "number_abs": None, "title": f"AoT {n}", "ids": {"trakt": str(300 + n)}}
            for n in range(1, 26)
        ]}]},
        show_hits={("tmdb", "1429"): [{"show": {"ids": {"trakt": 1429}}}]},
    )
    th = _patch_trakt(monkeypatch, fake)

    src = [_simkl_ep(1, 1, native=1, show_ids={"tmdb": "1429", "tvdb": "267440", "simkl": "999999"},
                     title="OAD", series="Attack on Titan")]
    res = th.add(_trakt_adapter(), src)

    assert res["confirmed_keys"] == []
    assert fake.posts == []
    assert res["unresolved"][0]["hint"] in {"trakt_absolute_number_unresolved", "trakt_episode_id_unresolved"}


def test_destination_collision_is_reported(monkeypatch) -> None:
    fake = _TraktFake(
        catalog=_dbz_catalog(), show_hits=DBZ_SHOW_HITS,
        post_payload={"added": {"episodes": 1}, "existing": {}, "not_found": {}},
    )
    th = _patch_trakt(monkeypatch, fake)

    a = _simkl_ep(1, 40, native=40)
    b = _simkl_ep(1, 41, native=40)
    res = th.add(_trakt_adapter(), [a, b])

    assert len(res["confirmed_keys"]) == 1
    assert [u["hint"] for u in res["unresolved"]] == ["trakt_destination_collision"]


def test_uncorrelated_not_found_makes_result_ambiguous(monkeypatch) -> None:
    fake = _TraktFake(
        catalog=_dbz_catalog(),
        show_hits=DBZ_SHOW_HITS,
        post_payload={
            "added": {"episodes": 1}, "existing": {},
            "not_found": {"shows": [{"ids": {"tmdb": 999999}, "seasons": [{"number": 5}]}]},
        },
    )
    th = _patch_trakt(monkeypatch, fake)

    src = [_simkl_ep(1, 40, native=40), _simkl_ep(1, 41, native=41)]
    res = th.add(_trakt_adapter(), src)

    assert res["ambiguous"] is True
    assert res["confirmed_keys"] == []
    assert len(res["unresolved_keys"]) == 2
    assert "trakt_response_ambiguous" in res["reason_counts"]


def test_destination_item_never_carries_simkl_native_ids(monkeypatch) -> None:
    fake = _TraktFake(
        catalog=_dbz_catalog(),
        show_hits=DBZ_SHOW_HITS,
        post_payload={"added": {"episodes": 1}, "existing": {}, "not_found": {}},
    )
    th = _patch_trakt(monkeypatch, fake)
    cached: list = []
    monkeypatch.setattr(th, "_cache_merge_from_source_items", lambda adapter, its: cached.extend(its))

    src = [_simkl_ep(
        1, 40, native=40,
        show_ids={"tmdb": "12971", "tvdb": "81472", "simkl": "41487", "mal": "813", "anidb": "3", "anilist": "223"},
    )]
    res = th.add(_trakt_adapter(), src)

    assert len(res["confirmed_keys"]) == 1
    assert src[0]["show_ids"]["simkl"] == "41487"

    sent = json.dumps(fake.posts[0])
    for banned in ("simkl", "mal", "anidb", "anilist", "kitsu"):
        assert banned not in sent

    assert len(cached) == 1
    dest = cached[0]
    assert set(dest.get("show_ids") or {}) <= {"trakt", "slug", "tmdb", "imdb", "tvdb"}
    assert set(dest.get("ids") or {}) <= {"trakt", "slug", "tmdb", "imdb", "tvdb"}
    assert "simkl" not in json.dumps(dest)


def test_same_destination_different_timestamps_is_not_a_collision(monkeypatch) -> None:
    fake = _TraktFake(
        catalog=_dbz_catalog(), show_hits=DBZ_SHOW_HITS,
        post_payload={"added": {"episodes": 2}, "existing": {}, "not_found": {}},
    )
    th = _patch_trakt(monkeypatch, fake)

    a = _simkl_ep(1, 40, native=40)
    b = _simkl_ep(1, 41, native=40)
    b["watched_at"] = "2024-06-06T00:00:00Z"

    res = th.add(_trakt_adapter(), [a, b])

    assert len(res["confirmed_keys"]) == 2
    assert res["unresolved"] == []


def test_destination_claims_persist_across_chunks_and_reset_next_run(monkeypatch) -> None:
    fake = _TraktFake(
        catalog=_dbz_catalog(), show_hits=DBZ_SHOW_HITS,
        post_payload={"added": {"episodes": 1}, "existing": {}, "not_found": {}},
    )
    th = _patch_trakt(monkeypatch, fake)
    adapter = _trakt_adapter()

    first = th.add(adapter, [_simkl_ep(1, 40, native=40)])
    assert len(first["confirmed_keys"]) == 1

    second = th.add(adapter, [_simkl_ep(1, 41, native=40)])
    assert second["confirmed_keys"] == []
    assert [u["hint"] for u in second["unresolved"]] == ["trakt_destination_collision"]

    monkeypatch.setenv("CW_RUN_ID", "run-2")
    third = th.add(adapter, [_simkl_ep(1, 41, native=40)])
    assert len(third["confirmed_keys"]) == 1
    assert not hasattr(th, "_MAP_TTL_SEC")


_DBZ_SEASON_SIZES = (39, 35, 33, 32, 26, 33, 27, 28, 38)


def _dbz_full_catalog():
    rows = []
    n = 0
    for season, size in enumerate(_DBZ_SEASON_SIZES, start=1):
        eps = []
        for ep in range(1, size + 1):
            n += 1
            eps.append({"number": ep, "number_abs": n, "title": f"DBZ {n}", "ids": {"trakt": str(498758 + n)}})
        rows.append({"number": season, "episodes": eps})
    return {"12971": rows}


def test_dbz_copied_show_ids_are_never_searched_as_episode_ids(monkeypatch) -> None:
    fake = _TraktFake(
        catalog=_dbz_full_catalog(),
        show_hits=DBZ_SHOW_HITS,
        post_payload={"added": {"episodes": 2}, "existing": {}, "not_found": {}},
    )
    th = _patch_trakt(monkeypatch, fake)

    src = [
        _simkl_ep(1, 40, native=40, ep_ids={"tvdb": "81472", "tmdb": "12971"}),
        _simkl_ep(1, 291, native=291, ep_ids={"tvdb": "81472", "tmdb": "12971"}),
    ]
    res = th.add(_trakt_adapter(), src)

    assert not any("/search/tvdb/81472" in g["url"] for g in fake.gets)
    assert _episode_searches(fake) == []

    sent = fake.posts[0]["episodes"]
    by_trakt = sorted(int(e["ids"]["trakt"]) for e in sent)
    assert by_trakt == [498798, 499049]
    assert len(res["confirmed_keys"]) == 2
    assert res["unresolved"] == []

    coords = {
        d["item"]["ids"]["trakt"]: (d["item"]["season"], d["item"]["episode"])
        for d in res["confirmed_destinations"].values()
    }
    assert coords["498798"] == (2, 1)
    assert coords["499049"] == (9, 38)


def test_dbz_batch_uses_one_catalogue_request(monkeypatch) -> None:
    fake = _TraktFake(
        catalog=_dbz_full_catalog(),
        show_hits=DBZ_SHOW_HITS,
        post_payload={"added": {"episodes": 291}, "existing": {}, "not_found": {}},
    )
    th = _patch_trakt(monkeypatch, fake)

    src = [_simkl_ep(1, n, native=n) for n in range(1, 292)]
    res = th.add(_trakt_adapter(), src)

    assert len(_catalogue_calls(fake)) == 1
    assert _episode_searches(fake) == []
    assert not any("/search/tvdb/81472" in g["url"] for g in fake.gets)
    assert len(res["confirmed_keys"]) == 291


def test_regular_episodes_need_no_pre_resolution_searches(monkeypatch) -> None:
    fake = _TraktFake(post_payload={"added": {"episodes": 100}, "existing": {}, "not_found": {}})
    th = _patch_trakt(monkeypatch, fake)

    src = [
        _simkl_ep(1, n, show_ids={"tmdb": "1396", "tvdb": "81189"}, series="Breaking Bad", bucket="shows")
        for n in range(1, 101)
    ]
    res = th.add(_trakt_adapter(), src)

    assert fake.gets == []
    body = fake.posts[0]
    assert "shows" in body
    episodes = [e for show in body["shows"] for s in show["seasons"] for e in s["episodes"]]
    assert len(episodes) == 100
    assert len(res["confirmed_keys"]) == 100


def test_confirmed_destinations_are_trakt_native_and_sanitized(monkeypatch) -> None:
    fake = _TraktFake(
        catalog=_dbz_full_catalog(),
        show_hits=DBZ_SHOW_HITS,
        post_payload={"added": {"episodes": 1}, "existing": {}, "not_found": {}},
    )
    th = _patch_trakt(monkeypatch, fake)

    src = [_simkl_ep(1, 40, native=40, show_ids={"tmdb": "12971", "tvdb": "81472", "simkl": "41487", "mal": "813"})]
    res = th.add(_trakt_adapter(), src)

    dests = res["confirmed_destinations"]
    assert len(dests) == 1
    rec = next(iter(dests.values()))
    item = rec["item"]
    assert (item["season"], item["episode"]) == (2, 1)
    assert int(item["ids"]["trakt"]) == 498798
    assert rec["status"] == "added"
    assert set(item.get("show_ids") or {}) <= {"trakt", "slug", "tmdb", "imdb", "tvdb"}
    for banned in ("simkl", "mal", "anidb", "anilist", "kitsu", "_simkl_episode_number", "simkl_bucket"):
        assert banned not in json.dumps(item)


def test_existing_only_response_reports_skipped_not_added(monkeypatch) -> None:
    fake = _TraktFake(
        catalog=_dbz_full_catalog(),
        show_hits=DBZ_SHOW_HITS,
        post_payload={"added": {}, "existing": {"episodes": 2}, "not_found": {}},
    )
    th = _patch_trakt(monkeypatch, fake)

    src = [_simkl_ep(1, 40, native=40), _simkl_ep(1, 41, native=41)]
    res = th.add(_trakt_adapter(), src)

    assert res["count"] == 0
    assert res["confirmed_keys"] == []
    assert len(res["skipped_keys"]) == 2
    assert len(res["presence_confirmed_keys"]) == 2
    assert all(d["status"] == "existing" for d in res["confirmed_destinations"].values())


def test_show_season_counters_do_not_inflate_episode_confirmation(monkeypatch) -> None:
    fake = _TraktFake(
        post_payload={"added": {"shows": 5, "seasons": 9, "episodes": 2}, "existing": {}, "not_found": {}},
    )
    th = _patch_trakt(monkeypatch, fake)

    src = [
        _simkl_ep(1, n, show_ids={"tmdb": "1396", "tvdb": "81189"}, series="Breaking Bad", bucket="shows")
        for n in (1, 2)
    ]
    res = th.add(_trakt_adapter(), src)

    assert res["count"] == 2
    assert res["ambiguous"] is False


def test_incomplete_response_leaves_unaccounted_keys_unresolved(monkeypatch) -> None:
    fake = _TraktFake(
        post_payload={"added": {"episodes": 1}, "existing": {}, "not_found": {}},
    )
    th = _patch_trakt(monkeypatch, fake)

    src = [
        _simkl_ep(1, n, show_ids={"tmdb": "1396", "tvdb": "81189"}, series="Breaking Bad", bucket="shows")
        for n in (1, 2, 3)
    ]
    res = th.add(_trakt_adapter(), src)

    assert res["ambiguous"] is True
    assert res["confirmed_keys"] == []
    assert len(res["unresolved_keys"]) == 3
    assert res["confirmed_destinations"] == {}


def test_alias_rekeys_destination_into_source_keyspace(monkeypatch) -> None:
    fake = _TraktFake(
        catalog=_dbz_full_catalog(),
        show_hits=DBZ_SHOW_HITS,
        post_payload={"added": {"episodes": 1}, "existing": {}, "not_found": {}},
    )
    th = _patch_trakt(monkeypatch, fake)

    src_item = _simkl_ep(1, 40, native=40)
    res = th.add(_trakt_adapter(), [src_item])

    src_event_key = th._source_event_key(src_item)
    src_key = canonical_key(src_item)
    dest = next(iter(res["confirmed_destinations"].values()))

    canonicalized = {dest["key"]: dest["item"]}
    view = th.destination_comparison_view(canonicalized)
    assert src_key != dest["key"]
    assert src_key in view
    assert view[src_key]["ids"]["trakt"] == dest["item"]["ids"]["trakt"]
    assert view[src_key]["season"] == 2
    assert view[src_key]["episode"] == 1
    assert dest["key"] not in view
    assert list(canonicalized) == [dest["key"]]

    raw = {dest["event_key"]: dest["item"]}
    raw_view = th.destination_comparison_view(raw)
    assert src_event_key in raw_view
    assert dest["event_key"] not in raw_view


def test_pre_resolution_failures_are_not_marked_skipped(monkeypatch) -> None:
    fake = _TraktFake(
        catalog=_dbz_catalog(),
        show_hits=DBZ_SHOW_HITS,
        post_payload={"added": {}, "existing": {"episodes": 1}, "not_found": {}},
    )
    th = _patch_trakt(monkeypatch, fake)

    src = [
        _simkl_ep(1, 40, native=40),
        _simkl_ep(0, 1, title="No Match", show_ids={"tmdb": "12971", "simkl": "41487"}),
    ]
    res = th.add(_trakt_adapter(), src)

    special_key = canonical_key(src[1])
    assert special_key not in res["skipped_keys"]
    assert special_key in res["unresolved_keys"]
    assert len(res["skipped_keys"]) == 1


def test_simkl_native_anime_identity_regression() -> None:
    item = {
        "type": "episode",
        "season": 4,
        "episode": 17,
        "watched_at": TRAKT_WATCHED,
        "show_ids": {"tvdb": "267440", "tmdb": "1429", "simkl": "1579947"},
        "_simkl_episode_number": 1,
        "simkl_bucket": "anime",
    }
    body, thaw, mapped, detected, unmapped = simkl_history._native_anime_remove_body(
        [item], session=None, headers={}, timeout=5, state=simkl_history._AnimeResolveState({}, {})
    )

    assert body == {"anime": [{"ids": {"simkl": "1579947"}, "episodes": [{"number": 1}]}]}
    assert unmapped == set()
    assert item["_simkl_episode_number"] == 1
    assert item["show_ids"]["simkl"] == "1579947"


# --- alias store, exact deletion and provider-native baselines ---


def _alias_setup(monkeypatch, fake, tmp_path):
    th = _patch_trakt(monkeypatch, fake, tmp_path=tmp_path)
    src_item = _simkl_ep(1, 40, native=40)
    res = th.add(_trakt_adapter(), [src_item])
    return th, src_item, res


def _dbz_dest_key(th):
    return th._destination_event_key({
        "type": "episode",
        "show_ids": {"tmdb": "12971"},
        "season": 2,
        "episode": 1,
        "watched_at": TRAKT_WATCHED,
    })


def test_direct_items_create_no_alias(monkeypatch, tmp_path) -> None:
    fake = _TraktFake(
        catalog=_dbz_full_catalog(),
        show_hits=DBZ_SHOW_HITS,
        post_payload={"added": {"episodes": 1}, "existing": {}, "not_found": {}},
    )
    th = _patch_trakt(monkeypatch, fake, tmp_path=tmp_path)

    direct = _simkl_ep(2, 1, bucket=None, ep_ids={"trakt": "498798"})
    th.add(_trakt_adapter(), [direct])

    assert th._alias_load() == {}
    assert not list(Path(tmp_path).glob("*pair_alias*"))


def test_translated_items_create_compact_alias(monkeypatch, tmp_path) -> None:
    fake = _TraktFake(
        catalog=_dbz_full_catalog(),
        show_hits=DBZ_SHOW_HITS,
        post_payload={"added": {"episodes": 1}, "existing": {}, "not_found": {}},
    )
    th, src_item, _ = _alias_setup(monkeypatch, fake, tmp_path)

    aliases = th._alias_load()
    src_event_key = th._source_event_key(src_item)
    assert list(aliases) == [src_event_key]

    rec = aliases[src_event_key]
    assert rec["destination_episode_id"] == "498798"
    assert rec["season"] == 2
    assert rec["episode"] == 1
    assert rec["watched_at"] == th._iso8601(TRAKT_WATCHED)
    assert rec["destination_key"] == "tmdb:12971#s02e01"
    assert rec["destination_event_key"] == _dbz_dest_key(th)
    assert "destination_item" not in rec
    assert set(rec) <= {
        "destination_event_key", "destination_key", "destination_episode_id",
        "destination_show_id", "season", "episode", "watched_at", "history_id", "basis",
    }

    written = list(Path(tmp_path).glob("*pair_alias*"))
    assert [p.name for p in written] == ["trakt_history.pair_alias.SIMKL-TRAKT-test.json"]
    assert json.loads(written[0].read_text("utf-8"))["items"] == aliases


def test_exact_deletion_uses_stored_history_id(monkeypatch, tmp_path) -> None:
    fake = _TraktFake(
        catalog=_dbz_full_catalog(),
        show_hits=DBZ_SHOW_HITS,
        post_payload={"added": {"episodes": 1}, "existing": {}, "not_found": {}},
        remove_payload={"deleted": {"episodes": 1}, "not_found": {"ids": []}},
    )
    th, src_item, _ = _alias_setup(monkeypatch, fake, tmp_path)

    src_event_key = th._source_event_key(src_item)
    th._alias_load()[src_event_key]["history_id"] = 777
    th._alias_save()

    res = th.remove(_trakt_adapter(), [src_item])

    assert fake.removes == [{"ids": [777]}]
    assert _episode_searches(fake) == []
    assert res["confirmed_keys"] == [canonical_key(src_item)]
    assert res["unresolved"] == []
    assert res["removed_destination_keys"] == ["tmdb:12971#s02e01"]
    assert th._alias_load() == {}


def test_exact_deletion_resolves_missing_history_id_by_lookup(monkeypatch, tmp_path) -> None:
    fake = _TraktFake(
        catalog=_dbz_full_catalog(),
        show_hits=DBZ_SHOW_HITS,
        post_payload={"added": {"episodes": 1}, "existing": {}, "not_found": {}},
        remove_payload={"deleted": {"episodes": 1}, "not_found": {"ids": []}},
        history_rows={"498798": [{"id": 4242, "watched_at": TRAKT_WATCHED}]},
    )
    th, src_item, _ = _alias_setup(monkeypatch, fake, tmp_path)

    assert "history_id" not in th._alias_load()[th._source_event_key(src_item)]

    res = th.remove(_trakt_adapter(), [src_item])

    lookups = [g for g in fake.gets if "/sync/history/episodes/" in g["url"]]
    assert len(lookups) == 1
    assert lookups[0]["params"]["start_at"] == th._iso8601(TRAKT_WATCHED)
    assert lookups[0]["params"]["end_at"] == th._iso8601(TRAKT_WATCHED)
    assert fake.removes == [{"ids": [4242]}]
    assert res["confirmed_keys"] == [canonical_key(src_item)]
    assert th._alias_load() == {}


def test_repeated_watches_remove_only_the_matching_event(monkeypatch, tmp_path) -> None:
    other_watched = "2024-06-06T00:00:00Z"
    fake = _TraktFake(
        catalog=_dbz_full_catalog(),
        show_hits=DBZ_SHOW_HITS,
        post_payload={"added": {"episodes": 1}, "existing": {}, "not_found": {}},
        remove_payload={"deleted": {"episodes": 1}, "not_found": {"ids": []}},
        history_rows={"498798": [
            {"id": 111, "watched_at": TRAKT_WATCHED},
            {"id": 222, "watched_at": other_watched},
        ]},
    )
    th, src_item, _ = _alias_setup(monkeypatch, fake, tmp_path)

    second = dict(src_item)
    second["watched_at"] = other_watched
    th.add(_trakt_adapter(), [second])
    assert len(th._alias_load()) == 2

    res = th.remove(_trakt_adapter(), [src_item])

    assert fake.removes == [{"ids": [111]}]
    assert res["confirmed_keys"] == [canonical_key(src_item)]
    assert list(th._alias_load()) == [th._source_event_key(second)]


def test_ambiguous_lookup_submits_no_deletion(monkeypatch, tmp_path) -> None:
    fake = _TraktFake(
        catalog=_dbz_full_catalog(),
        show_hits=DBZ_SHOW_HITS,
        post_payload={"added": {"episodes": 1}, "existing": {}, "not_found": {}},
        history_rows={"498798": [
            {"id": 111, "watched_at": TRAKT_WATCHED},
            {"id": 999, "watched_at": TRAKT_WATCHED},
        ]},
    )
    th, src_item, _ = _alias_setup(monkeypatch, fake, tmp_path)

    res = th.remove(_trakt_adapter(), [src_item])

    assert fake.removes == []
    assert res["confirmed_keys"] == []
    assert res["reason_counts"] == {"trakt_history_event_ambiguous": 1}
    assert res["unresolved_keys"] == [canonical_key(src_item)]
    assert list(th._alias_load()) == [th._source_event_key(src_item)]


def test_missing_alias_never_falls_back_to_coordinates(monkeypatch, tmp_path) -> None:
    fake = _TraktFake(catalog=_dbz_full_catalog(), show_hits=DBZ_SHOW_HITS)
    th = _patch_trakt(monkeypatch, fake, tmp_path=tmp_path)

    res = th.remove(_trakt_adapter(), [_simkl_ep(1, 40, native=40)])

    assert fake.removes == []
    assert res["confirmed_keys"] == []
    assert res["reason_counts"] == {"trakt_history_alias_missing": 1}


def test_already_absent_history_id_converges_without_unresolved(monkeypatch, tmp_path) -> None:
    fake = _TraktFake(
        catalog=_dbz_full_catalog(),
        show_hits=DBZ_SHOW_HITS,
        post_payload={"added": {"episodes": 1}, "existing": {}, "not_found": {}},
        remove_payload={"deleted": {"episodes": 1}, "not_found": {"ids": [222]}},
    )
    fake.post_payload = {"added": {"episodes": 2}, "existing": {}, "not_found": {}}
    th = _patch_trakt(monkeypatch, fake, tmp_path=tmp_path)

    first = _simkl_ep(1, 40, native=40)
    second = _simkl_ep(1, 100, native=100)
    th.add(_trakt_adapter(), [first, second])

    aliases = th._alias_load()
    aliases[th._source_event_key(first)]["history_id"] = 111
    aliases[th._source_event_key(second)]["history_id"] = 222
    th._alias_save()

    res = th.remove(_trakt_adapter(), [first, second])

    assert fake.removes == [{"ids": [111, 222]}]
    assert sorted(res["confirmed_keys"]) == sorted([canonical_key(first), canonical_key(second)])
    assert res["unresolved"] == []
    assert th._alias_load() == {}


def test_unconfirmed_remove_count_leaves_everything_unresolved(monkeypatch, tmp_path) -> None:
    fake = _TraktFake(
        catalog=_dbz_full_catalog(),
        show_hits=DBZ_SHOW_HITS,
        post_payload={"added": {"episodes": 1}, "existing": {}, "not_found": {}},
        remove_payload={"deleted": {"episodes": 0}, "not_found": {"ids": []}},
    )
    th, src_item, _ = _alias_setup(monkeypatch, fake, tmp_path)
    th._alias_load()[th._source_event_key(src_item)]["history_id"] = 777
    th._alias_save()

    res = th.remove(_trakt_adapter(), [src_item])

    assert res["confirmed_keys"] == []
    assert res["reason_counts"] == {"trakt_history_remove_unconfirmed": 1}
    assert list(th._alias_load()) == [th._source_event_key(src_item)]


def test_second_run_converges_and_enriches_history_ids(monkeypatch, tmp_path) -> None:
    fake = _TraktFake(
        catalog=_dbz_full_catalog(),
        show_hits=DBZ_SHOW_HITS,
        post_payload={"added": {"episodes": 1}, "existing": {}, "not_found": {}},
    )
    th, src_item, res = _alias_setup(monkeypatch, fake, tmp_path)

    dest = next(iter(res["confirmed_destinations"].values()))
    live_index = {dest["key"]: dict(dest["item"], _trakt_history_id=5150)}

    view = th.destination_comparison_view(live_index)

    src_event_key = th._source_event_key(src_item)
    src_key = canonical_key(src_item)
    assert src_key in view
    assert th._alias_load()[src_event_key]["history_id"] == 5150

    src_index = {src_key: src_item}
    assert [k for k in src_index if k not in view] == []


# --- native anime detection and absolute catalogue validation ---


def _dbz_catalog_with_specials():
    catalog = _dbz_full_catalog()
    rows = list(catalog["12971"])
    rows.insert(0, {"number": 0, "episodes": [
        {"number": 1, "title": "DBZ Special 1", "ids": {"trakt": "700001"}},
        {"number": 2, "title": "DBZ Special 2", "ids": {"trakt": "700002"}, "number_abs": 0},
    ]})
    rows.append({"number": 10, "episodes": [
        {"number": 1, "title": "DBZ Extra", "ids": {"trakt": "700003"}},
    ]})
    return {"12971": rows}


def test_regular_simkl_show_is_not_native_anime() -> None:
    from providers.sync.trakt._history import _is_native_anime

    monster = _simkl_ep(1, 12, bucket=None, series="Monster",
                        show_ids={"tvdb": "78857", "tmdb": "225634", "simkl": "12345"})

    assert _is_native_anime(monster) is False
    assert _is_native_anime(_simkl_ep(1, 40, native=40)) is True
    assert _is_native_anime(_simkl_ep(1, 40, bucket="anime")) is True
    assert _is_native_anime(_simkl_ep(1, 40, native=0, bucket=None)) is False


def test_regular_simkl_show_uses_direct_write_path(monkeypatch) -> None:
    fake = _TraktFake(
        catalog=_dbz_full_catalog(),
        show_hits=MONSTER_SHOW_HITS,
        post_payload={"added": {"episodes": 1}, "existing": {}, "not_found": {}},
    )
    th = _patch_trakt(monkeypatch, fake)

    monster = _simkl_ep(1, 12, bucket=None, series="Monster",
                        show_ids={"tvdb": "78857", "tmdb": "225634", "simkl": "12345"})
    res = th.add(_trakt_adapter(), [monster])

    assert _catalogue_calls(fake) == []
    assert _episode_searches(fake) == []
    season = fake.posts[0]["shows"][0]["seasons"][0]
    assert season["number"] == 1
    assert season["episodes"][0]["number"] == 12
    assert len(res["confirmed_keys"]) == 1


def test_catalogue_absolute_validation_tolerates_gaps_and_specials(monkeypatch) -> None:
    from providers.sync.trakt._history import _catalog_is_continuous

    flat = []
    for season in _dbz_catalog_with_specials()["12971"]:
        for ep in season["episodes"]:
            flat.append(dict(ep, season=season["number"], episode=ep["number"]))

    assert _catalog_is_continuous(flat) is True

    duplicated = flat + [dict(flat[-1], season=11, episode=1, number_abs=40)]
    assert _catalog_is_continuous(duplicated) is False


def test_dbz_absolute_mapping_with_specials_and_missing_abs(monkeypatch) -> None:
    fake = _TraktFake(
        catalog=_dbz_catalog_with_specials(),
        show_hits=DBZ_SHOW_HITS,
        post_payload={"added": {"episodes": 4}, "existing": {}, "not_found": {}},
    )
    th = _patch_trakt(monkeypatch, fake)

    src = [
        _simkl_ep(1, 40, native=40),
        _simkl_ep(1, 41, native=41),
        _simkl_ep(1, 100, native=100),
        _simkl_ep(1, 291, native=291),
    ]
    res = th.add(_trakt_adapter(), src)

    assert len(_catalogue_calls(fake)) == 1
    assert _episode_searches(fake) == []

    sent = sorted(int(e["ids"]["trakt"]) for e in fake.posts[0]["episodes"])
    assert sent == [498798, 498799, 498858, 499049]

    mapped = {
        int(d["item"]["ids"]["trakt"]): (d["item"]["season"], d["item"]["episode"])
        for d in res["confirmed_destinations"].values()
    }
    assert mapped[498798] == (2, 1)
    assert mapped[498799] == (2, 2)
    assert mapped[498858] == (3, 26)
    assert mapped[499049] == (9, 38)
    assert len(res["confirmed_keys"]) == 4
    assert res["unresolved"] == []


# --- not_found scope correlation is narrow, never whole-chunk ---


def _movie(n):
    return {
        "type": "movie",
        "title": f"Movie {n}",
        "year": 2000,
        "watched_at": TRAKT_WATCHED,
        "ids": {"tmdb": str(90000 + n)},
    }


def test_one_unmatched_episode_scope_spares_confirmed_movies(monkeypatch) -> None:
    fake = _TraktFake(
        catalog=_dbz_full_catalog(),
        show_hits=DBZ_SHOW_HITS,
        post_payload={
            "added": {"movies": 98, "episodes": 0},
            "existing": {},
            "not_found": {"episodes": [{"ids": {"trakt": "99999999"}}]},
        },
    )
    th = _patch_trakt(monkeypatch, fake)

    movies = [_movie(n) for n in range(98)]
    episodes = [
        _simkl_ep(1, 40, native=40),
        _simkl_ep(1, 41, native=41),
    ]
    res = th.add(_trakt_adapter(), movies + episodes)

    movie_keys = {canonical_key(m) for m in movies}
    episode_keys = {canonical_key(e) for e in episodes}

    assert set(res["unresolved_keys"]) == episode_keys
    assert movie_keys <= set(res["confirmed_keys"])
    assert len(res["confirmed_keys"]) == 98
    assert res["reason_counts"].get("trakt_response_ambiguous") == 2


def test_unattributable_scope_with_shortfall_stays_conservative(monkeypatch) -> None:
    catalog = {
        "12971": _dbz_full_catalog()["12971"],
        "225634": [{"number": 1, "episodes": [
            {"number": n, "number_abs": n, "title": f"M{n}", "ids": {"trakt": str(600 + n)}}
            for n in range(1, 11)
        ]}],
    }
    fake = _TraktFake(
        catalog=catalog,
        show_hits={**DBZ_SHOW_HITS, **MONSTER_SHOW_HITS},
        post_payload={
            "added": {"episodes": 10},
            "existing": {},
            "not_found": {"shows": [{"ids": {"tmdb": 555555}}]},
        },
    )
    th = _patch_trakt(monkeypatch, fake)

    dbz = [_simkl_ep(1, n, native=n) for n in range(1, 11)]
    monster = [
        _simkl_ep(1, n, native=n, show_ids={"tmdb": "225634", "simkl": "5150"}, series="Monster")
        for n in range(1, 11)
    ]
    res = th.add(_trakt_adapter(), dbz + monster)

    assert res["confirmed_keys"] == []
    assert len(res["unresolved_keys"]) == 20
    assert res["reason_counts"].get("trakt_response_ambiguous") == 20


def test_unmatched_show_scope_flags_only_that_show(monkeypatch) -> None:
    catalog = {
        "12971": _dbz_full_catalog()["12971"],
        "225634": [{"number": 1, "episodes": [
            {"number": n, "number_abs": n, "title": f"M{n}", "ids": {"trakt": str(600 + n)}}
            for n in range(1, 11)
        ]}],
    }
    fake = _TraktFake(
        catalog=catalog,
        show_hits={**DBZ_SHOW_HITS, **MONSTER_SHOW_HITS},
        post_payload={
            "added": {"episodes": 10},
            "existing": {},
            "not_found": {"shows": [{"ids": {"tmdb": "225634"}, "seasons": [{"number": 4}]}]},
        },
    )
    th = _patch_trakt(monkeypatch, fake)

    dbz = [_simkl_ep(1, n, native=n) for n in range(1, 11)]
    monster = [
        _simkl_ep(1, n, native=n, show_ids={"tmdb": "225634", "simkl": "5150"}, series="Monster")
        for n in range(1, 11)
    ]
    res = th.add(_trakt_adapter(), dbz + monster)

    dbz_keys = {canonical_key(e) for e in dbz}
    monster_keys = {canonical_key(e) for e in monster}

    assert dbz_keys <= set(res["confirmed_keys"])
    assert set(res["unresolved_keys"]) == monster_keys
    assert not (monster_keys & set(res["confirmed_keys"]))


def test_nested_not_found_shapes_are_correlated(monkeypatch) -> None:
    from providers.sync.trakt._history import _correlate_not_found

    req_index = {
        "by_ep_ids": {("trakt", "498798"): ["k-ep"]},
        "by_movie_ids": {("tmdb", "77"): ["k-mov"]},
        "by_show": {("tmdb", "12971"): ["k-ep"]},
        "by_show_season": {("tmdb", "12971", 2): ["k-ep"]},
        "by_show_ep": {("tmdb", "12971", 2, 1): ["k-ep"]},
        "leaf_kind": {"k-ep": "episode", "k-mov": "movie"},
        "src_items": {},
    }

    nf = {
        "movies": [{"movie": {"ids": {"tmdb": "77"}}}],
        "shows": [{
            "show": {"ids": {"tmdb": "12971"}},
            "seasons": [{"season": {"number": 2}, "episodes": [{"episode": {"number": 1}}]}],
        }],
    }
    matched, reasons, _expanded, matched_scopes, unmatched = _correlate_not_found(nf, req_index)

    assert set(matched) == {"k-mov", "k-ep"}
    assert matched_scopes == 2
    assert unmatched == []
    assert reasons["k-ep"] == "trakt_episode_not_found"


def test_unmatched_scope_reports_safe_debug_fields() -> None:
    from providers.sync.trakt._history import _correlate_not_found

    req_index = {
        "by_ep_ids": {}, "by_movie_ids": {}, "by_show": {("tmdb", "12971"): ["k-ep"]},
        "by_show_season": {}, "by_show_ep": {},
        "leaf_kind": {"k-ep": "episode"}, "src_items": {},
    }
    nf = {"shows": [{"ids": {"tmdb": "12971"}, "seasons": [{"number": 9, "episodes": [{"number": 3}]}]}]}

    _matched, _reasons, _expanded, matched_scopes, unmatched = _correlate_not_found(nf, req_index)

    assert matched_scopes == 0
    assert len(unmatched) == 1
    scope = unmatched[0]
    assert scope["bucket"] == "shows.episodes"
    assert scope["ids"] == {"tmdb": "12971"}
    assert scope["season"] == 9
    assert scope["episode"] == 3
    assert scope["candidates"] == ["k-ep"]


def test_matched_show_children_still_reach_exact_retry(monkeypatch) -> None:
    catalog = {"225634": [{"number": 1, "episodes": [
        {"number": n, "number_abs": n, "title": f"M{n}", "ids": {"trakt": str(600 + n)}} for n in range(1, 10)
    ]}]}
    fake = _TraktFake(
        catalog=catalog,
        show_hits=MONSTER_SHOW_HITS,
        post_payload={
            "added": {"episodes": 8}, "existing": {},
            "not_found": {"shows": [{"ids": {"tmdb": 225634}, "seasons": [
                {"number": 1, "episodes": [{"number": 3}]}
            ]}]},
        },
    )
    th = _patch_trakt(monkeypatch, fake)

    src = [
        _simkl_ep(1, n, native=n, show_ids={"tmdb": "225634", "simkl": "5150"},
                  series="Monster", ep_ids={"tvdb": str(70000 + n)})
        for n in range(1, 10)
    ]
    res = th.add(_trakt_adapter(), src)

    reasons = res["reason_counts"]
    assert reasons.get("trakt_episode_not_found") == 1
    assert "trakt_response_ambiguous" not in reasons
    assert len(res["unresolved_keys"]) == 1


# --- not_found ids may be show ids; correlation stays show-scoped ---


BLACK_MIRROR_SHOW_HITS = {("tmdb", "42009"): [{"show": {"ids": {"trakt": 42009}}}]}


def _flat_catalog(show, count, base_trakt):
    return {show: [{"number": 1, "episodes": [
        {"number": n, "number_abs": n, "title": f"{show}-{n}", "ids": {"trakt": str(base_trakt + n)}}
        for n in range(1, count + 1)
    ]}]}


def _plain_ep(show_tmdb, series, n, *, ep_tvdb=None):
    return _simkl_ep(
        1, n, bucket=None,
        series=series,
        show_ids={"tmdb": show_tmdb},
        ep_ids={"tvdb": str(ep_tvdb)} if ep_tvdb else None,
    )


def test_not_found_seasons_with_show_ids_matches_only_that_show(monkeypatch) -> None:
    req_index = {
        "by_ep_ids": {},
        "by_movie_ids": {},
        "by_show": {("tmdb", "225634"): ["monster-1", "monster-2"], ("tmdb", "71912"): ["ahsoka-1"]},
        "by_show_season": {("tmdb", "225634", 1): ["monster-1", "monster-2"]},
        "by_show_ep": {},
        "leaf_kind": {"monster-1": "episode", "monster-2": "episode", "ahsoka-1": "episode"},
        "src_items": {},
    }
    nf = {"seasons": [{"ids": {"tmdb": "225634"}, "number": 1}]}

    matched, reasons, _expanded, matched_scopes, unmatched = th_correlate(nf, req_index)

    assert set(matched) == {"monster-1", "monster-2"}
    assert "ahsoka-1" not in matched
    assert matched_scopes == 1
    assert unmatched == []
    assert reasons["monster-1"] == "trakt_parent_not_found"


def test_not_found_episode_ids_fall_back_to_show_scope(monkeypatch) -> None:
    req_index = {
        "by_ep_ids": {},
        "by_movie_ids": {},
        "by_show": {("tmdb", "42009"): ["bm-1", "bm-2"], ("tmdb", "71912"): ["ahsoka-1"]},
        "by_show_season": {},
        "by_show_ep": {},
        "leaf_kind": {"bm-1": "episode", "bm-2": "episode", "ahsoka-1": "episode"},
        "src_items": {},
    }
    nf = {"episodes": [{"ids": {"tmdb": "42009"}}]}

    matched, _reasons, _expanded, matched_scopes, unmatched = th_correlate(nf, req_index)

    assert set(matched) == {"bm-1", "bm-2"}
    assert matched_scopes == 1
    assert unmatched == []


def test_unmatched_scope_never_lists_whole_batch_as_candidates() -> None:
    req_index = {
        "by_ep_ids": {}, "by_movie_ids": {}, "by_show": {}, "by_show_season": {}, "by_show_ep": {},
        "leaf_kind": {f"k{n}": "episode" for n in range(100)},
        "src_items": {},
    }
    nf = {"episodes": [{"ids": {"trakt": "99999999"}}]}

    _matched, _reasons, _expanded, matched_scopes, unmatched = th_correlate(nf, req_index)

    assert matched_scopes == 0
    assert len(unmatched) == 1
    assert unmatched[0]["candidates"] == []


def test_failed_show_leaves_unrelated_shows_confirmed(monkeypatch) -> None:
    catalog = {}
    for show, base in (("225634", 600), ("71912", 700), ("100088", 800), ("112036", 900)):
        catalog.update(_flat_catalog(show, 25, base))
    fake = _TraktFake(
        catalog=catalog,
        show_hits={("tmdb", s): [{"show": {"ids": {"trakt": int(s)}}}]
                   for s in ("225634", "71912", "100088", "112036")},
        post_payload={
            "added": {"episodes": 75},
            "existing": {},
            "not_found": {"seasons": [{"ids": {"tmdb": "225634"}, "number": 1}]},
        },
        ep_search={("tvdb", str(70000 + n)): {
            "episode": {"ids": {"trakt": str(600 + n), "tvdb": str(70000 + n)}, "season": 1, "number": n},
            "show": {"ids": {"trakt": "225634", "tmdb": "225634"}},
        } for n in range(1, 26)},
    )
    th = _patch_trakt(monkeypatch, fake)

    monster = [_plain_ep("225634", "Monster", n, ep_tvdb=70000 + n) for n in range(1, 26)]
    ahsoka = [_plain_ep("71912", "Ahsoka", n) for n in range(1, 26)]
    tlou = [_plain_ep("100088", "The Last of Us", n) for n in range(1, 26)]
    squid = [_plain_ep("112036", "Squid Game", n) for n in range(1, 26)]

    res = th.add(_trakt_adapter(), monster + ahsoka + tlou + squid)

    unrelated = {canonical_key(e) for e in ahsoka + tlou + squid}
    monster_keys = {canonical_key(e) for e in monster}

    assert unrelated <= set(res["confirmed_keys"])
    assert monster_keys <= set(res["confirmed_keys"])
    assert res["unresolved_keys"] == []
    assert "trakt_response_ambiguous" not in res["reason_counts"]


def test_failed_show_that_cannot_recover_leaves_others_confirmed(monkeypatch) -> None:
    catalog = {}
    for show, base in (("225634", 600), ("71912", 700), ("100088", 800), ("112036", 900)):
        catalog.update(_flat_catalog(show, 25, base))
    fake = _TraktFake(
        catalog=catalog,
        show_hits={("tmdb", s): [{"show": {"ids": {"trakt": int(s)}}}]
                   for s in ("225634", "71912", "100088", "112036")},
        post_payload={
            "added": {"episodes": 75},
            "existing": {},
            "not_found": {"seasons": [{"ids": {"tmdb": "225634"}, "number": 1}]},
        },
    )
    th = _patch_trakt(monkeypatch, fake)

    monster = [_plain_ep("225634", "Monster", n) for n in range(1, 26)]
    ahsoka = [_plain_ep("71912", "Ahsoka", n) for n in range(1, 26)]
    tlou = [_plain_ep("100088", "The Last of Us", n) for n in range(1, 26)]
    squid = [_plain_ep("112036", "Squid Game", n) for n in range(1, 26)]

    res = th.add(_trakt_adapter(), monster + ahsoka + tlou + squid)

    unrelated = {canonical_key(e) for e in ahsoka + tlou + squid}
    monster_keys = {canonical_key(e) for e in monster}

    assert unrelated <= set(res["confirmed_keys"])
    assert set(res["unresolved_keys"]) == monster_keys
    assert not (monster_keys & set(res["confirmed_keys"]))
    assert "trakt_response_ambiguous" not in res["reason_counts"]
    assert res["reason_counts"].get("trakt_parent_not_found") == 25


def test_matched_show_scope_reaches_exact_tvdb_retry(monkeypatch) -> None:
    fake = _TraktFake(
        catalog=_flat_catalog("225634", 5, 600),
        show_hits=MONSTER_SHOW_HITS,
        post_payload={
            "added": {"episodes": 0},
            "existing": {},
            "not_found": {"seasons": [{"ids": {"tmdb": "225634"}, "number": 1}]},
        },
        ep_search={("tvdb", str(70000 + n)): {
            "episode": {"ids": {"trakt": str(600 + n), "tvdb": str(70000 + n)}, "season": 1, "number": n},
            "show": {"ids": {"trakt": "225634", "tmdb": "225634"}},
        } for n in range(1, 6)},
        retry_payload={"added": {"episodes": 5}, "existing": {}, "not_found": {}},
    )
    th = _patch_trakt(monkeypatch, fake)

    monster = [_plain_ep("225634", "Monster", n, ep_tvdb=70000 + n) for n in range(1, 6)]
    res = th.add(_trakt_adapter(), monster)

    searches = _episode_searches(fake)
    assert len(searches) == 5
    assert all(s["url"].split("/search/")[1].startswith("tvdb/7000") for s in searches)
    assert len(res["confirmed_keys"]) == 5
    assert "trakt_response_ambiguous" not in res["reason_counts"]


def test_black_mirror_episode_not_found_reaches_retry(monkeypatch) -> None:
    fake = _TraktFake(
        catalog=_flat_catalog("42009", 6, 5000),
        show_hits=BLACK_MIRROR_SHOW_HITS,
        post_payload={
            "added": {"episodes": 5},
            "existing": {},
            "not_found": {"episodes": [{"ids": {"tvdb": "60003"}}]},
        },
        ep_search={("tvdb", "60003"): {
            "episode": {"ids": {"trakt": "5003", "tvdb": "60003"}, "season": 1, "number": 3},
            "show": {"ids": {"trakt": "42009", "tmdb": "42009"}},
        }},
        retry_payload={"added": {"episodes": 1}, "existing": {}, "not_found": {}},
    )
    th = _patch_trakt(monkeypatch, fake)

    src = [_plain_ep("42009", "Black Mirror", n, ep_tvdb=60000 + n) for n in range(1, 7)]
    res = th.add(_trakt_adapter(), src)

    searches = _episode_searches(fake)
    assert [s["url"].rsplit("/", 1)[-1] for s in searches] == ["60003"]
    assert len(res["confirmed_keys"]) == 6
    assert "trakt_response_ambiguous" not in res["reason_counts"]


# --- SIMKL delta must expose deletions; Trakt exact-identity deletion ---


def _simkl_index_env(monkeypatch, tmp_path, *, cached, rows, acts):
    import providers.sync.simkl._history as sh

    calls = {"since": [], "saved": []}

    monkeypatch.setattr(sh, "_cache_load", lambda: dict(cached))
    monkeypatch.setattr(sh, "_cache_doc_is_stale", lambda: False)
    monkeypatch.setattr(sh, "_cache_save", lambda items: calls["saved"].append(dict(items)))
    monkeypatch.setattr(sh, "normalize_flat_watermarks", lambda: None)
    monkeypatch.setattr(sh, "get_watermark", lambda name: "2024-01-01T00:00:00Z" if name == "history" else "")
    monkeypatch.setattr(sh, "update_watermark_if_new", lambda *a, **k: None)
    monkeypatch.setattr(sh, "_unfreeze", lambda *a, **k: None)
    monkeypatch.setattr(sh, "fetch_activities", lambda *a, **k: (acts, None))
    monkeypatch.setattr(sh, "_headers", lambda *a, **k: {})

    def _fetch(session, headers, *, since_iso=None, timeout=None):
        calls["since"].append(since_iso)
        return dict(rows)

    monkeypatch.setattr(sh, "_fetch_all_items", _fetch)
    return sh, calls


def _simkl_adapter():
    return SimpleNamespace(
        client=SimpleNamespace(session=object()),
        cfg=SimpleNamespace(timeout=5, api_key="k", access_token="t"),
    )


def _simkl_acts(*, movies=None, shows=None, anime=None):
    return {"tv_shows": {"all": shows or "2024-01-01T00:00:00Z"},
            "movies": {"all": movies or "2024-01-01T00:00:00Z"},
            "anime": {"all": anime or "2024-01-01T00:00:00Z"}}


def test_simkl_anime_removal_triggers_full_replacement(monkeypatch, tmp_path) -> None:
    cached = {"tmdb:12971#s01e40@1704067200": {"type": "episode", "season": 1, "episode": 40}}
    acts = _simkl_acts(anime="2026-07-21T00:00:00Z")
    sh, calls = _simkl_index_env(monkeypatch, tmp_path, cached=cached, rows={"movies": [], "shows": [], "anime": []}, acts=acts)

    out = sh.build_index(_simkl_adapter())

    assert calls["since"] == [None]
    assert out == {}
    assert calls["saved"] and calls["saved"][-1] == {}


def test_simkl_regular_show_removal_triggers_full_replacement(monkeypatch, tmp_path) -> None:
    cached = {"tmdb:225634#s01e01@1704067200": {"type": "episode", "season": 1, "episode": 1}}
    acts = _simkl_acts(shows="2026-07-21T00:00:00Z")
    sh, calls = _simkl_index_env(monkeypatch, tmp_path, cached=cached, rows={"movies": [], "shows": [], "anime": []}, acts=acts)

    out = sh.build_index(_simkl_adapter())

    assert calls["since"] == [None]
    assert out == {}


def test_simkl_unchanged_activities_still_use_cache(monkeypatch, tmp_path) -> None:
    cached = {"tmdb:225634#s01e01@1704067200": {"type": "episode", "season": 1, "episode": 1}}
    acts = _simkl_acts()
    sh, calls = _simkl_index_env(monkeypatch, tmp_path, cached=cached, rows={"movies": [], "shows": [], "anime": []}, acts=acts)

    out = sh.build_index(_simkl_adapter())

    assert calls["since"] == []
    assert out == cached


def test_aot_exact_episode_id_deletes_without_alias(monkeypatch, tmp_path) -> None:
    fake = _TraktFake(
        catalog={},
        show_hits={},
        ep_search={("tvdb", "4183742"): {
            "episode": {"ids": {"trakt": "3011", "tvdb": "4183742"}, "season": 2, "number": 1},
            "show": {"ids": {"trakt": "1420", "tmdb": "1429"}},
        }},
        remove_payload={"deleted": {"episodes": 1}, "not_found": {"ids": []}},
        history_rows={"3011": [{"id": 8801, "watched_at": TRAKT_WATCHED}]},
    )
    th = _patch_trakt(monkeypatch, fake, tmp_path=tmp_path)

    aot = _simkl_ep(2, 1, bucket="anime",
                    show_ids={"tvdb": "267440", "tmdb": "1429", "simkl": "1579947"},
                    ep_ids={"tvdb": "4183742"}, series="Attack on Titan")

    assert th._alias_load() == {}
    res = th.remove(_trakt_adapter(), [aot])

    assert fake.removes == [{"ids": [8801]}]
    assert res["confirmed_keys"] == [canonical_key(aot)]
    assert res["unresolved"] == []
    assert not any("shows" in (body or {}) for body in fake.removes)


def test_dbz_translated_removal_still_uses_alias(monkeypatch, tmp_path) -> None:
    fake = _TraktFake(
        catalog=_dbz_full_catalog(),
        show_hits=DBZ_SHOW_HITS,
        post_payload={"added": {"episodes": 1}, "existing": {}, "not_found": {}},
        remove_payload={"deleted": {"episodes": 1}, "not_found": {"ids": []}},
        history_rows={"498798": [{"id": 4242, "watched_at": TRAKT_WATCHED}]},
    )
    th = _patch_trakt(monkeypatch, fake, tmp_path=tmp_path)

    dbz = _simkl_ep(1, 40, native=40)
    th.add(_trakt_adapter(), [dbz])
    assert th._alias_load()

    res = th.remove(_trakt_adapter(), [dbz])

    lookups = [g for g in fake.gets if "/sync/history/episodes/" in g["url"]]
    assert [g["url"].rsplit("/", 1)[-1] for g in lookups] == ["498798"]
    assert _episode_searches(fake) == []
    assert fake.removes == [{"ids": [4242]}]
    assert res["confirmed_keys"] == [canonical_key(dbz)]


def test_anime_without_alias_or_exact_identity_is_unresolved(monkeypatch, tmp_path) -> None:
    fake = _TraktFake(catalog=_dbz_full_catalog(), show_hits=DBZ_SHOW_HITS)
    th = _patch_trakt(monkeypatch, fake, tmp_path=tmp_path)

    dbz = _simkl_ep(1, 40, native=40)
    res = th.remove(_trakt_adapter(), [dbz])

    assert fake.removes == []
    assert res["confirmed_keys"] == []
    assert res["reason_counts"] == {"trakt_history_alias_missing": 1}


def test_copied_show_id_is_never_searched_for_removal(monkeypatch, tmp_path) -> None:
    fake = _TraktFake(catalog=_dbz_full_catalog(), show_hits=DBZ_SHOW_HITS)
    th = _patch_trakt(monkeypatch, fake, tmp_path=tmp_path)

    dbz = _simkl_ep(1, 40, native=40, ep_ids={"tvdb": "81472", "tmdb": "12971"})
    res = th.remove(_trakt_adapter(), [dbz])

    assert not any("/search/tvdb/81472" in g["url"] for g in fake.gets)
    assert fake.removes == []
    assert res["reason_counts"] == {"trakt_history_alias_missing": 1}


# --- removal identity guards and activity watermark ---


def test_copied_trakt_id_is_never_queried_as_episode(monkeypatch, tmp_path) -> None:
    fake = _TraktFake(catalog={}, show_hits={})
    th = _patch_trakt(monkeypatch, fake, tmp_path=tmp_path)

    copied = _simkl_ep(2, 1, bucket="anime",
                       show_ids={"trakt": "1420", "tmdb": "1429", "simkl": "1579947"},
                       ep_ids={"trakt": "1420"}, series="Attack on Titan")

    res = th.remove(_trakt_adapter(), [copied])

    assert fake.removes == []
    assert not any("/sync/history/episodes/1420" in g["url"] for g in fake.gets)
    assert res["confirmed_keys"] == []
    assert res["reason_counts"] == {"trakt_history_alias_missing": 1}


def test_genuine_trakt_episode_id_is_used_directly(monkeypatch, tmp_path) -> None:
    fake = _TraktFake(
        catalog={}, show_hits={},
        remove_payload={"deleted": {"episodes": 1}, "not_found": {"ids": []}},
        history_rows={"3011": [{"id": 8801, "watched_at": TRAKT_WATCHED}]},
    )
    th = _patch_trakt(monkeypatch, fake, tmp_path=tmp_path)

    aot = _simkl_ep(2, 1, bucket="anime",
                    show_ids={"trakt": "1420", "tmdb": "1429", "simkl": "1579947"},
                    ep_ids={"trakt": "3011"}, series="Attack on Titan")

    res = th.remove(_trakt_adapter(), [aot])

    assert _episode_searches(fake) == []
    assert fake.removes == [{"ids": [8801]}]
    assert res["confirmed_keys"] == [canonical_key(aot)]


def test_history_watermark_prefers_activity_timestamp(monkeypatch, tmp_path) -> None:
    marks: dict[str, str] = {}
    acts = _simkl_acts(shows="2026-07-21T10:00:00Z")
    old_row = {
        "show": {"title": "Attack on Titan", "year": 2013,
                 "ids": {"tmdb": "1429", "tvdb": "267440", "simkl": 1429000}},
        "seasons": [{"number": 1, "episodes": [{"number": 1, "watched_at": "2019-03-03T00:00:00Z"}]}],
    }
    sh, calls = _simkl_index_env(
        monkeypatch, tmp_path,
        cached={"tmdb:1#s01e01@1704067200": {"type": "episode", "season": 1, "episode": 1}},
        rows={"movies": [], "shows": [old_row], "anime": []},
        acts=acts,
    )
    monkeypatch.setattr(sh, "update_watermark_if_new", lambda name, value: marks.__setitem__(name, value))

    sh.build_index(_simkl_adapter())

    assert marks.get("history") == "2026-07-21T10:00:00Z"
    assert not str(marks.get("history") or "").startswith("2019")


def test_unchanged_activities_without_expired_injection_use_cache(monkeypatch, tmp_path) -> None:
    import time as _time
    fresh = {"tmdb:1#s01e01@1704067200": {
        "type": "episode", "season": 1, "episode": 1, "_cw_injected_at": int(_time.time())}}
    sh, calls = _simkl_index_env(
        monkeypatch, tmp_path, cached=fresh,
        rows={"movies": [], "shows": [], "anime": []}, acts=_simkl_acts(),
    )

    out = sh.build_index(_simkl_adapter())

    assert calls["since"] == []
    assert out == fresh


def test_expired_injection_forces_full_replacement(monkeypatch, tmp_path) -> None:
    expired = {"tmdb:1#s01e01@1704067200": {
        "type": "episode", "season": 1, "episode": 1, "_cw_injected_at": 0}}
    sh, calls = _simkl_index_env(
        monkeypatch, tmp_path, cached=expired,
        rows={"movies": [], "shows": [], "anime": []}, acts=_simkl_acts(),
    )

    out = sh.build_index(_simkl_adapter())

    assert calls["since"] == [None]
    assert out == {}



# --- raw history-ID removals must not leave stale aliases ---


RAW_EPOCH = 1704067200


def _raw_item(n, history_id, *, show_tmdb="500", series="Raw Show"):
    it = _simkl_ep(1, n, bucket=None, series=series, show_ids={"tmdb": show_tmdb})
    it["_trakt_history_id"] = history_id
    return it


def _seed_alias(th, src_key, history_id, dest_base="tmdb:12971#s02e01"):
    items = th._alias_load()
    items[src_key] = {
        "destination_event_key": f"{dest_base}@{RAW_EPOCH}",
        "destination_key": dest_base,
        "history_id": history_id,
        "watched_at": TRAKT_WATCHED,
    }
    th._alias_save()


def _counts(res):
    return res["removed_exact"], res["removed_coordinate"]


def test_confirmed_raw_history_id_removal_deletes_translated_alias(monkeypatch, tmp_path) -> None:
    fake = _TraktFake(
        catalog={}, show_hits={},
        remove_payload={"deleted": {"episodes": 1}, "not_found": {"ids": []}},
    )
    th = _patch_trakt(monkeypatch, fake, tmp_path=tmp_path)
    _seed_alias(th, "tmdb:12971#s01e40@1704067200", 111)
    _seed_alias(th, "tmdb:12971#s01e41@1704067200", 999, dest_base="tmdb:12971#s02e02")
    assert len(th._alias_load()) == 2

    res = th.remove(_trakt_adapter(), [_raw_item(1, 111)])

    assert fake.removes == [{"ids": [111]}]
    assert res["count"] == 1
    assert list(th._alias_load()) == ["tmdb:12971#s01e41@1704067200"]


def test_already_absent_raw_history_id_clears_stale_alias(monkeypatch, tmp_path) -> None:
    fake = _TraktFake(
        catalog={}, show_hits={},
        remove_payload={"deleted": {"episodes": 1}, "not_found": {"ids": [222]}},
    )
    th = _patch_trakt(monkeypatch, fake, tmp_path=tmp_path)
    _seed_alias(th, "tmdb:12971#s01e40@1704067200", 111)
    _seed_alias(th, "tmdb:12971#s01e41@1704067200", 222, dest_base="tmdb:12971#s02e02")

    res = th.remove(_trakt_adapter(), [_raw_item(1, 111), _raw_item(2, 222)])

    assert th._alias_load() == {}
    assert res["unresolved"] == []
    assert res["count"] == 2


def test_unrelated_aliases_survive_raw_removal(monkeypatch, tmp_path) -> None:
    fake = _TraktFake(
        catalog={}, show_hits={},
        remove_payload={"deleted": {"episodes": 1}, "not_found": {"ids": []}},
    )
    th = _patch_trakt(monkeypatch, fake, tmp_path=tmp_path)
    for n in range(5):
        _seed_alias(th, f"tmdb:70{n}#s01e40@1704067200", 900 + n, dest_base=f"tmdb:70{n}#s02e01")
    _seed_alias(th, "tmdb:12971#s01e40@1704067200", 111)
    assert len(th._alias_load()) == 6

    th.remove(_trakt_adapter(), [_raw_item(1, 111)])

    remaining = th._alias_load()
    assert len(remaining) == 5
    assert "tmdb:12971#s01e40@1704067200" not in remaining


def test_six_raw_removals_report_exact_not_coordinate(monkeypatch, tmp_path) -> None:
    fake = _TraktFake(
        catalog={}, show_hits={},
        remove_payload={"deleted": {"episodes": 6}, "not_found": {"ids": []}},
    )
    th = _patch_trakt(monkeypatch, fake, tmp_path=tmp_path)
    for n in range(1, 7):
        _seed_alias(th, f"tmdb:12971#s01e4{n}@1704067200", 100 + n, dest_base=f"tmdb:12971#s02e0{n}")
    for n in range(4):
        _seed_alias(th, f"tmdb:80{n}#s01e40@1704067200", 700 + n, dest_base=f"tmdb:80{n}#s02e01")
    assert len(th._alias_load()) == 10

    res = th.remove(_trakt_adapter(), [_raw_item(n, 100 + n) for n in range(1, 7)])

    assert fake.removes == [{"ids": [101, 102, 103, 104, 105, 106]}]
    assert res["count"] == 6
    assert len(th._alias_load()) == 4

    assert _counts(res) == (6, 0)


def test_mixed_raw_and_coordinate_removal_counts_separately(monkeypatch, tmp_path) -> None:
    fake = _TraktFake(
        catalog={}, show_hits={},
        remove_payload={"deleted": {"episodes": 1}, "not_found": {}},
        remove_coord_payload={"deleted": {"episodes": 2}, "not_found": {}},
    )
    th = _patch_trakt(monkeypatch, fake, tmp_path=tmp_path)
    _seed_alias(th, "tmdb:12971#s01e40@1704067200", 111)

    items = [
        _raw_item(1, 111),
        _simkl_ep(1, 5, bucket=None, series="Plain", show_ids={"tmdb": "600"}),
        _simkl_ep(1, 6, bucket=None, series="Plain", show_ids={"tmdb": "600"}),
    ]
    res = th.remove(_trakt_adapter(), items)

    assert _counts(res) == (1, 2)
    assert th._alias_load() == {}


def test_pure_coordinate_removal_reports_no_exact(monkeypatch, tmp_path) -> None:
    fake = _TraktFake(
        catalog={}, show_hits={},
        remove_payload={"deleted": {"episodes": 2}, "not_found": {}},
    )
    th = _patch_trakt(monkeypatch, fake, tmp_path=tmp_path)
    _seed_alias(th, "tmdb:12971#s01e40@1704067200", 111)

    items = [_simkl_ep(1, n, bucket=None, series="Plain", show_ids={"tmdb": "600"}) for n in (1, 2)]
    res = th.remove(_trakt_adapter(), items)

    assert _counts(res) == (0, 2)
    assert list(th._alias_load()) == ["tmdb:12971#s01e40@1704067200"]


def test_coordinate_removal_clears_alias_matched_by_destination_key(monkeypatch, tmp_path) -> None:
    fake = _TraktFake(
        catalog={}, show_hits={},
        remove_payload={"deleted": {"episodes": 1}, "not_found": {}},
    )
    th = _patch_trakt(monkeypatch, fake, tmp_path=tmp_path)
    _seed_alias(th, f"tmdb:12971#s01e40@{RAW_EPOCH}", 111, dest_base="tmdb:12971#s02e01")

    item = _simkl_ep(2, 1, bucket=None, show_ids={"tmdb": "12971"})
    res = th.remove(_trakt_adapter(), [item])

    assert _counts(res) == (0, 1)
    assert th._alias_load() == {}


def test_coordinate_removal_keeps_alias_for_other_watch_event(monkeypatch, tmp_path) -> None:
    fake = _TraktFake(
        catalog={}, show_hits={},
        remove_payload={"deleted": {"episodes": 1}, "not_found": {}},
    )
    th = _patch_trakt(monkeypatch, fake, tmp_path=tmp_path)
    items = th._alias_load()
    items["tmdb:12971#s01e40@1557014400"] = {
        "destination_event_key": "tmdb:12971#s02e01@1557014400",
        "destination_key": "tmdb:12971#s02e01",
        "history_id": 111,
        "watched_at": "2019-05-05T00:00:00Z",
    }
    th._alias_save()

    item = _simkl_ep(2, 1, bucket=None, show_ids={"tmdb": "12971"})
    res = th.remove(_trakt_adapter(), [item])

    assert _counts(res) == (0, 1)
    assert list(th._alias_load()) == ["tmdb:12971#s01e40@1557014400"]


def test_five_coordinate_removals_clear_exactly_five_aliases(monkeypatch, tmp_path) -> None:
    fake = _TraktFake(
        catalog={}, show_hits={},
        remove_payload={"deleted": {"episodes": 5}, "not_found": {}},
    )
    th = _patch_trakt(monkeypatch, fake, tmp_path=tmp_path)
    for n in range(1, 11):
        _seed_alias(th, f"tmdb:12971#s01e{n:02d}@{RAW_EPOCH}", 100 + n,
                    dest_base=f"tmdb:12971#s02e{n:02d}")
    assert len(th._alias_load()) == 10

    items = [_simkl_ep(2, n, bucket=None, show_ids={"tmdb": "12971"}) for n in range(1, 6)]
    res = th.remove(_trakt_adapter(), items)

    assert _counts(res) == (0, 5)
    remaining = th._alias_load()
    assert len(remaining) == 5
    assert all(int(k.split("e")[-1].split("@")[0]) > 5 for k in remaining)


def test_exact_removal_evicts_index_cache_entry_without_history_id(monkeypatch, tmp_path) -> None:
    fake = _TraktFake(
        catalog={}, show_hits={},
        remove_payload={"deleted": {"episodes": 1}, "not_found": {"ids": []}},
    )
    th = _patch_trakt(monkeypatch, fake, tmp_path=tmp_path)

    item = _simkl_ep(1, 40, native=40)
    src_event_key = th._source_event_key(item)
    _seed_alias(th, src_event_key, 111)

    th._save_cache_doc({
        f"tmdb:12971#s01e40@{RAW_EPOCH}": {
            "type": "episode",
            "season": 1,
            "episode": 40,
            "show_ids": {"tmdb": "12971"},
            "watched_at": TRAKT_WATCHED,
        },
        f"tmdb:99999#s01e01@{RAW_EPOCH}": {
            "type": "episode",
            "season": 1,
            "episode": 1,
            "show_ids": {"tmdb": "99999"},
            "watched_at": TRAKT_WATCHED,
        },
    }, TRAKT_WATCHED)
    assert len(th._load_cache_doc().get("items") or {}) == 2

    res = th.remove(_trakt_adapter(), [item])

    assert _counts(res) == (1, 0)
    remaining = th._load_cache_doc().get("items") or {}
    assert list(remaining) == [f"tmdb:99999#s01e01@{RAW_EPOCH}"]


# --- alias reconstruction after Sync cleanup ---


def _trakt_event(season, episode, *, show_tmdb="12971", ep_trakt, watched=TRAKT_WATCHED, history_id=None):
    item = {
        "type": "episode",
        "series_title": "Dragon Ball Z",
        "season": season,
        "episode": episode,
        "watched_at": watched,
        "show_ids": {"tmdb": show_tmdb},
        "ids": {"trakt": str(ep_trakt)},
    }
    if history_id is not None:
        item["_trakt_history_id"] = history_id
    return item


def test_first_sync_after_cleanup_rebuilds_alias_without_writes(monkeypatch, tmp_path) -> None:
    fake = _TraktFake(catalog=_dbz_full_catalog(), show_hits=DBZ_SHOW_HITS)
    th = _patch_trakt(monkeypatch, fake, tmp_path=tmp_path)

    src_item = _simkl_ep(1, 40, native=40)
    th.prepare_source_snapshot([src_item])
    assert th._alias_load() == {}

    dest_key = "tmdb:12971#s02e01"
    live_index = {dest_key: _trakt_event(2, 1, ep_trakt=498798, history_id=5150)}

    view = th.destination_comparison_view(live_index, _trakt_adapter())

    src_event_key = th._source_event_key(src_item)
    aliases = th._alias_load()
    assert list(aliases) == [src_event_key]
    assert aliases[src_event_key]["destination_event_key"] == dest_key
    assert aliases[src_event_key]["history_id"] == 5150
    assert aliases[src_event_key]["basis"] == "rebuilt_from_resolver"

    assert canonical_key(src_item) in view
    assert fake.posts == []
    assert fake.removes == []


def test_rebuilt_alias_makes_second_plan_empty(monkeypatch, tmp_path) -> None:
    fake = _TraktFake(catalog=_dbz_full_catalog(), show_hits=DBZ_SHOW_HITS)
    th = _patch_trakt(monkeypatch, fake, tmp_path=tmp_path)

    src_item = _simkl_ep(1, 40, native=40)
    th.prepare_source_snapshot([src_item])
    dest_key = "tmdb:12971#s02e01"
    view = th.destination_comparison_view({dest_key: _trakt_event(2, 1, ep_trakt=498798)}, _trakt_adapter())

    src_index = {canonical_key(src_item): src_item}
    missing = [k for k in src_index if k not in view]

    assert missing == []
    assert fake.posts == []


def test_ambiguous_rebuild_records_no_alias(monkeypatch, tmp_path) -> None:
    fake = _TraktFake(catalog=_dbz_full_catalog(), show_hits=DBZ_SHOW_HITS)
    th = _patch_trakt(monkeypatch, fake, tmp_path=tmp_path)

    src_item = _simkl_ep(1, 40, native=40)
    th.prepare_source_snapshot([src_item])

    live_index = {
        "tmdb:12971#s02e01": _trakt_event(2, 1, ep_trakt=498798),
        "tmdb:12971#s02e01~dup": _trakt_event(2, 1, ep_trakt=498798),
    }

    th.destination_comparison_view(live_index, _trakt_adapter())

    assert th._alias_load() == {}
    assert th._ALIAS_REBUILD["ambiguous"] == 1
    assert th._alias_rebuild_incomplete() is True


def test_ambiguous_rebuild_blocks_removal(monkeypatch, tmp_path) -> None:
    fake = _TraktFake(
        catalog=_dbz_full_catalog(), show_hits=DBZ_SHOW_HITS,
        remove_payload={"deleted": {"episodes": 1}, "not_found": {"ids": []}},
    )
    th = _patch_trakt(monkeypatch, fake, tmp_path=tmp_path)

    src_item = _simkl_ep(1, 40, native=40, ep_ids={"tvdb": "4183742"})
    th.prepare_source_snapshot([src_item])
    th.destination_comparison_view({
        "tmdb:12971#s02e01": _trakt_event(2, 1, ep_trakt=498798),
        "tmdb:12971#s02e01~dup": _trakt_event(2, 1, ep_trakt=498798),
    }, _trakt_adapter())
    assert th._alias_rebuild_incomplete() is True

    res = th.remove(_trakt_adapter(), [src_item])

    assert fake.removes == []
    assert _episode_searches(fake) == []
    assert res["confirmed_keys"] == []
    assert res["reason_counts"] == {"trakt_history_alias_rebuild_pending": 1}


def test_rebuild_never_maps_two_sources_onto_one_destination(monkeypatch, tmp_path) -> None:
    fake = _TraktFake(catalog=_dbz_full_catalog(), show_hits=DBZ_SHOW_HITS)
    th = _patch_trakt(monkeypatch, fake, tmp_path=tmp_path)

    first = _simkl_ep(1, 40, native=40)
    second = _simkl_ep(1, 41, native=41)
    th.prepare_source_snapshot([first, second])

    live_index = {"tmdb:12971#s02e01": _trakt_event(2, 1, ep_trakt=498798)}

    th.destination_comparison_view(live_index, _trakt_adapter())

    aliases = th._alias_load()
    assert list(aliases) == [th._source_event_key(first)]
    assert th._source_event_key(second) not in aliases
    destinations = [rec["destination_event_key"] for rec in aliases.values()]
    assert len(destinations) == len(set(destinations))


def test_identity_mapped_episode_gets_no_rebuilt_alias(monkeypatch, tmp_path) -> None:
    fake = _TraktFake(catalog=_dbz_full_catalog(), show_hits=DBZ_SHOW_HITS)
    th = _patch_trakt(monkeypatch, fake, tmp_path=tmp_path)

    same = _simkl_ep(2, 1, native=1)
    th.prepare_source_snapshot([same])

    live_index = {"tmdb:12971#s02e01": _trakt_event(2, 1, ep_trakt=498798)}

    th.destination_comparison_view(live_index, _trakt_adapter())

    assert th._alias_load() == {}
    assert th._ALIAS_REBUILD["ambiguous"] == 0


WATCHED_EPOCH_REBUILD = 1704067200


# --- removal accounting consistency ---


def _assert_consistent(res):
    assert res["removed_exact"] + res["removed_coordinate"] == res["count"]
    return res


def test_successful_alias_plan_removal_counts_once(monkeypatch, tmp_path) -> None:
    fake = _TraktFake(
        catalog=_dbz_full_catalog(), show_hits=DBZ_SHOW_HITS,
        post_payload={"added": {"episodes": 1}, "existing": {}, "not_found": {}},
        remove_payload={"deleted": {"episodes": 1}, "not_found": {"ids": []}},
        history_rows={"498798": [{"id": 4242, "watched_at": TRAKT_WATCHED}]},
    )
    th = _patch_trakt(monkeypatch, fake, tmp_path=tmp_path)

    dbz = _simkl_ep(1, 40, native=40)
    th.add(_trakt_adapter(), [dbz])

    res = _assert_consistent(th.remove(_trakt_adapter(), [dbz]))

    assert res["count"] == 1
    assert res["removed_exact"] == 1
    assert res["removed_coordinate"] == 0


def test_alias_plan_already_absent_event_still_resolves(monkeypatch, tmp_path) -> None:
    fake = _TraktFake(
        catalog=_dbz_full_catalog(), show_hits=DBZ_SHOW_HITS,
        post_payload={"added": {"episodes": 2}, "existing": {}, "not_found": {}},
        remove_payload={"deleted": {"episodes": 1}, "not_found": {"ids": [4243]}},
        history_rows={
            "498798": [{"id": 4242, "watched_at": TRAKT_WATCHED}],
            "498799": [{"id": 4243, "watched_at": TRAKT_WATCHED}],
        },
    )
    th = _patch_trakt(monkeypatch, fake, tmp_path=tmp_path)

    first = _simkl_ep(1, 40, native=40)
    second = _simkl_ep(1, 41, native=41)
    th.add(_trakt_adapter(), [first, second])

    res = _assert_consistent(th.remove(_trakt_adapter(), [first, second]))

    assert res["count"] == 2
    assert res["removed_exact"] == 2
    assert sorted(res["confirmed_keys"]) == sorted([canonical_key(first), canonical_key(second)])
    assert res["unresolved"] == []


def test_raw_id_removal_clears_alias_and_cache(monkeypatch, tmp_path) -> None:
    cached: list = []
    fake = _TraktFake(
        catalog={}, show_hits={},
        remove_payload={"deleted": {"episodes": 1}, "not_found": {"ids": []}},
    )
    exact_purged: list = []
    th = _patch_trakt(monkeypatch, fake, tmp_path=tmp_path)
    monkeypatch.setattr(th, "_cache_remove_source_items", lambda adapter, items: cached.extend(items))
    monkeypatch.setattr(th, "_cache_remove_event_keys", lambda keys, ids: exact_purged.extend(ids))
    _seed_alias(th, "tmdb:12971#s01e40@1704067200", 111)

    res = _assert_consistent(th.remove(_trakt_adapter(), [_raw_item(1, 111)]))

    assert fake.removes == [{"ids": [111]}]
    assert res["removed_exact"] == 1
    assert res["removed_coordinate"] == 0
    assert th._alias_load() == {}
    assert exact_purged == [111]
    assert cached == []


def test_already_absent_raw_id_clears_alias_and_exact_cache(monkeypatch, tmp_path) -> None:
    cached: list = []
    fake = _TraktFake(
        catalog={}, show_hits={},
        remove_payload={"deleted": {"episodes": 0}, "not_found": {"ids": [111]}},
    )
    th = _patch_trakt(monkeypatch, fake, tmp_path=tmp_path)
    monkeypatch.setattr(th, "_cache_remove_source_items", lambda adapter, items: cached.extend(items))
    _seed_alias(th, "tmdb:12971#s01e40@1704067200", 111)

    exact_purged: list = []
    monkeypatch.setattr(th, "_cache_remove_event_keys", lambda keys, ids: exact_purged.extend(ids))

    res = _assert_consistent(th.remove(_trakt_adapter(), [_raw_item(1, 111)]))

    assert res["count"] == 1
    assert res["removed_exact"] == 1
    assert res["confirmed_keys"] == [canonical_key(_raw_item(1, 111))]
    assert th._alias_load() == {}
    assert exact_purged == [111]
    assert cached == []
    assert res["unresolved"] == []


def test_deleted_ids_mismatch_confirms_no_raw_ids(monkeypatch, tmp_path) -> None:
    cached: list = []
    fake = _TraktFake(
        catalog={}, show_hits={},
        remove_payload={"deleted": {"episodes": 1}, "not_found": {"ids": []}},
    )
    th = _patch_trakt(monkeypatch, fake, tmp_path=tmp_path)
    monkeypatch.setattr(th, "_cache_remove_source_items", lambda adapter, items: cached.extend(items))
    _seed_alias(th, "tmdb:12971#s01e40@1704067200", 111)
    _seed_alias(th, "tmdb:12971#s01e41@1704067200", 112, dest_base="tmdb:12971#s02e02")

    res = _assert_consistent(th.remove(_trakt_adapter(), [_raw_item(1, 111), _raw_item(2, 112)]))

    assert res["count"] == 0
    assert res["removed_exact"] == 0
    assert res["confirmed_keys"] == []
    assert len(th._alias_load()) == 2
    assert cached == []
    assert res["reason_counts"].get("trakt_history_remove_unconfirmed") == 2


def test_http_failure_preserves_aliases_and_cache(monkeypatch, tmp_path) -> None:
    cached: list = []
    fake = _TraktFake(catalog={}, show_hits={}, remove_status=500)
    th = _patch_trakt(monkeypatch, fake, tmp_path=tmp_path)
    monkeypatch.setattr(th, "_cache_remove_source_items", lambda adapter, items: cached.extend(items))
    _seed_alias(th, "tmdb:12971#s01e40@1704067200", 111)

    res = _assert_consistent(th.remove(_trakt_adapter(), [_raw_item(1, 111)]))

    assert res["count"] == 0
    assert list(th._alias_load()) == ["tmdb:12971#s01e40@1704067200"]
    assert cached == []
    assert res["reason_counts"].get("http:500") == 1


def test_mixed_raw_batch_resolves_deleted_and_already_absent(monkeypatch, tmp_path) -> None:
    fake = _TraktFake(
        catalog={}, show_hits={},
        remove_payload={"deleted": {"episodes": 2}, "not_found": {"ids": [113]}},
    )
    th = _patch_trakt(monkeypatch, fake, tmp_path=tmp_path)
    for n, hid in ((1, 111), (2, 112), (3, 113)):
        _seed_alias(th, f"tmdb:12971#s01e4{n}@1704067200", hid, dest_base=f"tmdb:12971#s02e0{n}")

    res = _assert_consistent(th.remove(_trakt_adapter(), [_raw_item(n, 110 + n) for n in (1, 2, 3)]))

    assert res["count"] == 3
    assert res["removed_exact"] == 3
    assert res["removed_coordinate"] == 0
    assert th._alias_load() == {}
    assert res["unresolved"] == []


def test_coordinate_and_raw_counts_sum_to_removed(monkeypatch, tmp_path) -> None:
    fake = _TraktFake(
        catalog={}, show_hits={},
        remove_payload={"deleted": {"episodes": 1}, "not_found": {}},
        remove_coord_payload={"deleted": {"episodes": 2}, "not_found": {}},
    )
    th = _patch_trakt(monkeypatch, fake, tmp_path=tmp_path)
    _seed_alias(th, "tmdb:12971#s01e40@1704067200", 111)

    items = [
        _raw_item(1, 111),
        _simkl_ep(1, 5, bucket=None, series="Plain", show_ids={"tmdb": "600"}),
        _simkl_ep(1, 6, bucket=None, series="Plain", show_ids={"tmdb": "600"}),
    ]
    res = _assert_consistent(th.remove(_trakt_adapter(), items))

    assert res["removed_exact"] == 1
    assert res["removed_coordinate"] == 2
    assert res["count"] == 3
