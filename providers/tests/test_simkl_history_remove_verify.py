from __future__ import annotations

from types import SimpleNamespace

from cw_platform.id_map import canonical_key, minimal

from test_simkl_history_anime_mapping import _Resp, _Session, _adapter, _patch_fs


WATCHED = "2024-01-01T00:00:00Z"
RECORD_A = 39687
RECORD_B = 1579947


def _anime_row(record, tvdb_season, numbers, *, title="Attack on Titan"):
    return {
        "show": {"title": title, "ids": {"simkl": record, "tvdb": "267440", "tmdb": "1429"}, "anime_type": "tv"},
        "seasons": [
            {
                "number": 1,
                "episodes": [
                    {"number": n, "watched_at": WATCHED, "tvdb": {"season": tvdb_season, "episode": n}}
                    for n in numbers
                ],
            }
        ],
    }


def _aot_rows(a_numbers, b_numbers):
    rows = []
    if a_numbers:
        rows.append(_anime_row(RECORD_A, 1, a_numbers))
    if b_numbers:
        rows.append(_anime_row(RECORD_B, 4, b_numbers))
    return rows


def _payload(anime_rows=None, show_rows=None, movie_rows=None):
    return {"movies": movie_rows or [], "shows": show_rows or [], "anime": anime_rows or []}


def _parsed(m, anime_rows=None, show_rows=None, movie_rows=None):
    out, *_ = m._parse_rows(movie_rows or [], show_rows or [], anime_rows or [], limit=None)
    return out


def _no_sleep(monkeypatch, m):
    monkeypatch.setattr(m.time, "sleep", lambda *_a, **_k: None)


def test_split_records_preserve_native_identity(monkeypatch):
    import sync.simkl._history as m

    _patch_fs(monkeypatch, m)
    parsed = _parsed(m, _aot_rows([1, 2], [17]))
    eps = list(parsed.values())

    assert len(eps) == 3
    by_coord = {(e["season"], e["episode"]): e for e in eps}
    assert set(by_coord) == {(1, 1), (1, 2), (4, 17)}
    assert by_coord[(1, 1)]["show_ids"]["simkl"] == str(RECORD_A)
    assert by_coord[(1, 1)]["_simkl_episode_number"] == 1
    assert by_coord[(4, 17)]["show_ids"]["simkl"] == str(RECORD_B)
    assert by_coord[(4, 17)]["_simkl_episode_number"] == 17
    assert all(e["simkl_bucket"] == "anime" for e in eps)
    assert all(e["show_ids"]["tvdb"] == "267440" for e in eps)

    trakt_like = {"type": "episode", "show_ids": {"tvdb": "267440", "tmdb": "1429"}, "season": 4, "episode": 17}
    assert canonical_key(by_coord[(4, 17)]) == canonical_key(trakt_like)


def test_minimal_preserves_native_identity_without_changing_key():
    import sync.simkl._history as m

    item = {
        "type": "episode",
        "season": 4,
        "episode": 17,
        "show_ids": {"tvdb": "267440", "tmdb": "1429", "simkl": "1579947"},
        "simkl_bucket": "anime",
        "anime_type": "tv",
        "_simkl_episode_number": 17,
    }
    out = minimal(item)

    assert out["simkl_bucket"] == "anime"
    assert out["anime_type"] == "tv"
    assert out["_simkl_episode_number"] == 17
    assert out["show_ids"]["simkl"] == "1579947"
    assert canonical_key(out) == canonical_key(item)
    assert m._thaw_key(out) == m._thaw_key(item)


def test_verified_absence_confirms_removal(monkeypatch):
    import sync.simkl._history as m

    _patch_fs(monkeypatch, m)
    _no_sleep(monkeypatch, m)
    items = list(_parsed(m, _aot_rows([1, 2], [17])).values())
    session = _Session(
        post_handler=lambda url, body: _Resp(200, {"deleted": {"episodes": 3}}),
        all_items=_payload(),
    )
    adapter = _adapter(session)

    ok, unresolved = m.remove(adapter, items)

    assert ok == 3
    assert unresolved == []
    assert len(getattr(adapter, "_simkl_history_remove_confirmed_keys")) == 3
    assert getattr(adapter, "_simkl_history_remove_skipped_keys") == []


def test_http_ok_but_item_still_present_is_not_confirmed(monkeypatch):
    import sync.simkl._history as m

    _patch_fs(monkeypatch, m)
    _no_sleep(monkeypatch, m)
    rows = _aot_rows([1], [])
    items = list(_parsed(m, rows).values())
    forgotten: list = []
    monkeypatch.setattr(m, "_forget_source_aliases", lambda its: forgotten.extend(its))
    session = _Session(
        post_handler=lambda url, body: _Resp(200, {"deleted": {"episodes": 1}}),
        all_items=_payload(anime_rows=rows),
    )
    adapter = _adapter(session)

    ok, unresolved = m.remove(adapter, items)

    assert ok == 0
    assert [u["hint"] for u in unresolved] == ["simkl_remove_not_confirmed"]
    assert getattr(adapter, "_simkl_history_remove_confirmed_keys") == []
    assert forgotten == []


def test_verification_failure_confirms_nothing(monkeypatch):
    import sync.simkl._history as m

    _patch_fs(monkeypatch, m)
    _no_sleep(monkeypatch, m)
    items = list(_parsed(m, _aot_rows([1, 2], [])).values())
    saved: list = []
    monkeypatch.setattr(m, "_cache_save", lambda payload: saved.append(payload))
    forgotten: list = []
    monkeypatch.setattr(m, "_forget_source_aliases", lambda its: forgotten.extend(its))

    class _Dead(_Session):
        def get(self, url, headers=None, params=None, timeout=None, allow_redirects=None):
            import sync.simkl._history as mod
            if url == mod.URL_ALL_ITEMS:
                return _Resp(503, {})
            return super().get(url, headers=headers, params=params, timeout=timeout, allow_redirects=allow_redirects)

    session = _Dead(post_handler=lambda url, body: _Resp(200, {"deleted": {"episodes": 2}}))
    adapter = _adapter(session)

    ok, unresolved = m.remove(adapter, items)

    assert ok == 0
    assert [u["hint"] for u in unresolved] == ["simkl_remove_verification_failed"] * 2
    assert getattr(adapter, "_simkl_history_remove_confirmed_keys") == []
    assert saved == []
    assert forgotten == []


def test_partial_removal_confirms_only_absent_items(monkeypatch):
    import sync.simkl._history as m

    _patch_fs(monkeypatch, m)
    _no_sleep(monkeypatch, m)
    a_numbers = list(range(1, 51))
    b_numbers = list(range(1, 38))
    items = list(_parsed(m, _aot_rows(a_numbers, b_numbers)).values())
    assert len(items) == 87

    remaining_rows = _aot_rows(a_numbers[25:], b_numbers)
    session = _Session(
        post_handler=lambda url, body: _Resp(200, {"deleted": {"episodes": 87}}),
        all_items=_payload(anime_rows=remaining_rows),
    )
    adapter = _adapter(session)

    ok, unresolved = m.remove(adapter, items)

    assert ok == 25
    assert len(unresolved) == 62
    assert {u["hint"] for u in unresolved} == {"simkl_remove_not_confirmed"}

    confirmed = set(getattr(adapter, "_simkl_history_remove_confirmed_keys"))
    expected = {m._thaw_key(it) for it in items[:25]}
    assert confirmed == expected

    cached = m._cache_load()
    assert len(cached) == 62
    assert not (confirmed & {k.split("@", 1)[0] for k in cached})


def test_full_removal_confirms_every_item(monkeypatch):
    import sync.simkl._history as m

    _patch_fs(monkeypatch, m)
    _no_sleep(monkeypatch, m)
    items = list(_parsed(m, _aot_rows(list(range(1, 51)), list(range(1, 38)))).values())
    session = _Session(
        post_handler=lambda url, body: _Resp(200, {"deleted": {"episodes": 87}}),
        all_items=_payload(),
    )
    adapter = _adapter(session)

    ok, unresolved = m.remove(adapter, items)

    groups = {
        grp["ids"]["simkl"]: len(grp["episodes"])
        for p in session.posts
        if p["url"] == m.URL_REMOVE
        for grp in (p["json"].get("anime") or [])
    }
    assert groups == {str(RECORD_A): 50, str(RECORD_B): 37}
    assert ok == 87
    assert unresolved == []
    assert m._cache_load() == {}


def test_non_anime_removal_still_works(monkeypatch):
    import sync.simkl._history as m

    _patch_fs(monkeypatch, m)
    _no_sleep(monkeypatch, m)
    show_rows = [
        {
            "show": {"title": "Breaking Bad", "ids": {"tvdb": "81189", "tmdb": "1396"}},
            "seasons": [{"number": 3, "episodes": [{"number": 8, "watched_at": WATCHED}]}],
        }
    ]
    movie_rows = [{"movie": {"title": "Heat", "year": 1995, "ids": {"tmdb": "949"}}, "last_watched_at": WATCHED}]
    items = list(_parsed(m, show_rows=show_rows, movie_rows=movie_rows).values())
    assert len(items) == 2

    session = _Session(
        post_handler=lambda url, body: _Resp(200, {"deleted": {"episodes": 1, "movies": 1}}),
        all_items=_payload(),
    )
    adapter = _adapter(session)

    ok, unresolved = m.remove(adapter, items)

    body = next(p["json"] for p in session.posts if p["url"] == m.URL_REMOVE)
    assert [mv["ids"]["tmdb"] for mv in body["movies"]] == ["949"]
    assert body["shows"][0]["seasons"][0]["episodes"][0]["number"] == 8
    assert "anime" not in body
    assert ok == 2
    assert unresolved == []


def test_source_aliases_survive_a_shrinking_source_snapshot(monkeypatch):
    import sync.simkl._history as m

    _patch_fs(monkeypatch, m)
    full = [
        {
            "type": "episode",
            "season": 1,
            "episode": n,
            "watched_at": WATCHED,
            "ids": {"tvdb": str(700000 + n)},
            "show_ids": {"tvdb": "267440", "tmdb": "1429"},
            "title": f"AoT S01E{n:02d}",
            "series_title": "Attack on Titan",
        }
        for n in range(1, 4)
    ]
    m.prepare_source_snapshot(full)
    assert len(m._load_source_aliases()) == 3

    m.prepare_source_snapshot(full[:1])

    assert len(m._load_source_aliases()) == 3


def test_confirmed_removal_forgets_only_its_own_aliases(monkeypatch):
    import sync.simkl._history as m

    _patch_fs(monkeypatch, m)
    items = [
        {
            "type": "episode",
            "season": 1,
            "episode": n,
            "watched_at": WATCHED,
            "ids": {"tvdb": str(700000 + n)},
            "show_ids": {"tvdb": "267440", "tmdb": "1429"},
            "title": f"AoT S01E{n:02d}",
            "series_title": "Attack on Titan",
        }
        for n in range(1, 4)
    ]
    m.prepare_source_snapshot(items)
    assert len(m._load_source_aliases()) == 3

    m._forget_source_aliases(items[:2])

    remaining = m._load_source_aliases()
    assert list(remaining) == [m._thaw_key(items[2])]


def test_module_remove_returns_exact_confirmed_keys(monkeypatch):
    import sync.simkl._history as m
    import sync._mod_SIMKL as mod

    _patch_fs(monkeypatch, m)
    _no_sleep(monkeypatch, m)
    rows = _aot_rows([1, 2], [])
    items = list(_parsed(m, rows).values())
    session = _Session(
        post_handler=lambda url, body: _Resp(200, {"deleted": {"episodes": 2}}),
        all_items=_payload(anime_rows=_aot_rows([2], [])),
    )

    adapter = SimpleNamespace(
        client=SimpleNamespace(session=session),
        cfg=SimpleNamespace(timeout=5, history_chunk_size=100),
        key_of=m._thaw_key,
    )
    res = mod.SIMKLModule.remove(adapter, "history", items)

    assert res["count"] == 1
    assert res["confirmed_keys"] == [m._thaw_key(items[0])]
    assert len(res["confirmed_keys"]) == res["count"]
    assert len(res["unresolved"]) == 1
    assert m._thaw_key(items[1]) in res["skipped_keys"]
