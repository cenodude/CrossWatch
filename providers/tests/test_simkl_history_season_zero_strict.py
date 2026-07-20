from __future__ import annotations

import json

from test_simkl_history_anime_mapping import _Resp, _Session, _adapter, _ok_add, _anime_of, _shows_of


AOT_REDIRECT = {"267440": "39687"}
AOT_ROWS = {
    "39687": [
        {"episode": n, "title": f"aot{n}", "tvdb": {"season": 1, "episode": n}}
        for n in range(1, 26)
    ]
}
WATCHED = "2024-01-01T00:00:00Z"


def _alias_store(monkeypatch, m, aliases=None):
    store: dict[str, str] = {}
    monkeypatch.setattr(m, "state_file", lambda name: name)
    monkeypatch.setattr(m, "_load_json", lambda path: json.loads(store.get(path) or "{}"))
    monkeypatch.setattr(m, "_save_json", lambda path, data: store.__setitem__(path, json.dumps(data)))
    if aliases:
        m._save_anime_episode_alias_cache(aliases)
    return store


def _patch_write_env(monkeypatch, m):
    monkeypatch.setattr(m, "_headers", lambda *a, **k: {})
    monkeypatch.setattr(m, "simkl_api_params_from_headers", lambda headers=None, **k: dict(k))
    monkeypatch.setattr(m, "_unfreeze", lambda *a, **k: None)
    monkeypatch.setattr(m, "_freeze", lambda *a, **k: None)
    monkeypatch.setattr(m, "_load_anime_resolve_cache", lambda: m._AnimeResolveState({"267440": "39687"}, {}))
    monkeypatch.setattr(m, "_save_anime_resolve_cache", lambda *a, **k: None)
    injected: list = []
    monkeypatch.setattr(m, "_inject_adds_into_cache", lambda items: injected.extend(items))
    return injected


def _aot_s00(episode, *, ep_tvdb=None, title=None, number_abs=None):
    item = {
        "type": "episode",
        "season": 0,
        "episode": episode,
        "watched_at": WATCHED,
        "show_ids": {"tvdb": "267440", "tmdb": "1429"},
        "series_title": "Attack on Titan",
        "title": title,
    }
    if ep_tvdb is not None:
        item["ids"] = {"tvdb": str(ep_tvdb)}
    if number_abs is not None:
        item["_trakt_number_abs"] = number_abs
    return item


def _poisoned_alias():
    return {
        "39687:1": {
            "season": 0,
            "episode": 1,
            "show_ids": {"tvdb": "267440", "tmdb": "1429"},
            "title": "Ilse's Notebook",
            "series_title": "Attack on Titan",
        }
    }


def _alias_for(native, *, episode=1):
    return {
        f"39687:{native}": {
            "season": 0,
            "episode": episode,
            "show_ids": {"tvdb": "267440", "tmdb": "1429"},
            "title": "Ilse's Notebook",
            "series_title": "Attack on Titan",
        }
    }


def _resolve(m, monkeypatch, rows, item, *, aliases=None):
    store = _alias_store(monkeypatch, m, aliases)
    monkeypatch.setattr(m, "simkl_api_params_from_headers", lambda headers=None, **k: dict(k))
    session = _Session(episodes_map={"39687": rows})
    native = m._anime_retry_episode_number(
        item,
        {"simkl": "39687", "tvdb": "267440"},
        session=session,
        headers={},
        timeout=5,
        episode_cache={},
        alias_cache=m._load_anime_episode_alias_cache(),
    )
    return native, m._load_anime_episode_alias_cache(), store


def test_row_confirms_source_ignores_titles():
    import sync.simkl._history as m

    row = {"episode": 5, "title": "Ilse's Notebook", "ids": {}, "tvdb": {"season": None, "episode": None}}
    item = _aot_s00(1, title="Ilse's Notebook")

    assert m._anime_row_confirms_source(row, item, 0, 1) is False


def test_title_only_alias_is_invalidated_then_resolved_by_unique_title(monkeypatch):
    import sync.simkl._history as m

    rows = [
        {"episode": 5, "title": "Ilse's Notebook"},
        {"episode": 9, "title": "aot9", "tvdb": {"season": 1, "episode": 9}},
    ]
    native, aliases, _ = _resolve(
        m, monkeypatch, rows, _aot_s00(1, title="Ilse's Notebook"), aliases=_alias_for(5)
    )

    assert native == 5
    assert aliases == {}


def test_duplicate_titles_block_alias_and_unique_title_fallback(monkeypatch):
    import sync.simkl._history as m

    rows = [
        {"episode": 5, "title": "Ilse's Notebook"},
        {"episode": 6, "title": "ILSE'S  NOTEBOOK!"},
    ]
    native, aliases, _ = _resolve(
        m, monkeypatch, rows, _aot_s00(1, title="Ilse's Notebook"), aliases=_alias_for(5)
    )

    assert native is None
    assert aliases == {}


def test_duplicate_titles_leave_season_zero_item_unresolved(monkeypatch):
    import sync.simkl._history as m

    _alias_store(monkeypatch, m, _alias_for(5))
    injected = _patch_write_env(monkeypatch, m)
    session = _Session(
        post_handler=lambda url, body: _Resp(200, _ok_add(episodes=1)),
        redirect_map=AOT_REDIRECT,
        episodes_map={"39687": [
            {"episode": 5, "title": "Ilse's Notebook"},
            {"episode": 6, "title": "ILSE'S  NOTEBOOK!"},
        ]},
    )

    ok, unresolved = m.add(_adapter(session), [_aot_s00(1, title="Ilse's Notebook")])

    assert ok == 0
    assert [u["hint"] for u in unresolved] == ["simkl_anime_season_zero_unmapped"]
    assert list(_anime_of(session, m.URL_ADD)) == []
    assert injected == []


def test_alias_with_exact_tvdb_coordinates_is_kept(monkeypatch):
    import sync.simkl._history as m

    rows = [
        {"episode": 26, "title": "unrelated", "tvdb": {"season": 0, "episode": 1}},
        {"episode": 9, "title": "aot9", "tvdb": {"season": 1, "episode": 9}},
    ]
    native, aliases, _ = _resolve(m, monkeypatch, rows, _aot_s00(1), aliases=_alias_for(26))

    assert native == 26
    assert aliases.keys() == {"39687:26"}


def test_alias_with_exact_episode_ids_is_kept(monkeypatch):
    import sync.simkl._history as m

    for field, value in (("tvdb", "4635717"), ("anidb", "9541")):
        rows = [{"episode": 27, "title": "unrelated", "ids": {field: value}}]
        item = _aot_s00(1)
        item["ids"] = {field: value}
        native, aliases, _ = _resolve(m, monkeypatch, rows, item, aliases=_alias_for(27))

        assert native == 27
        assert aliases.keys() == {"39687:27"}


def _duplicate_native_rows():
    return [
        {"episode": 1, "title": "To You, 2000 Years in the Future", "tvdb": {"season": 1, "episode": 1}},
        {"episode": 1, "title": "Since That Day", "tvdb": {"season": None, "episode": None}},
    ]


def test_unique_title_with_duplicate_native_number_is_rejected(monkeypatch):
    import sync.simkl._history as m

    rows = _duplicate_native_rows()
    item = _aot_s00(1, title="Since That Day")

    title_hits = [r for r in rows if m._title_match_key(r.get("title")) == m._title_match_key("Since That Day")]
    assert len(title_hits) == 1

    native, aliases, _ = _resolve(m, monkeypatch, rows, item)

    assert native is None
    assert aliases == {}


def test_duplicate_native_number_leaves_item_unresolved(monkeypatch):
    import sync.simkl._history as m

    _alias_store(monkeypatch, m)
    injected = _patch_write_env(monkeypatch, m)
    session = _Session(
        post_handler=lambda url, body: _Resp(200, _ok_add(episodes=1)),
        redirect_map=AOT_REDIRECT,
        episodes_map={"39687": _duplicate_native_rows()},
    )
    adapter = _adapter(session)

    ok, unresolved = m.add(adapter, [_aot_s00(1, title="Since That Day")])

    assert ok == 0
    assert [u["hint"] for u in unresolved] == ["simkl_anime_season_zero_unmapped"]
    assert list(_anime_of(session, m.URL_ADD)) == []
    assert list(_shows_of(session, m.URL_ADD)) == []
    assert session.posts == []
    assert injected == []
    assert getattr(adapter, "_simkl_history_add_confirmed_keys") == []
    assert m._load_anime_episode_alias_cache() == {}


def test_alias_to_duplicate_native_number_is_invalidated(monkeypatch):
    import sync.simkl._history as m

    native, aliases, _ = _resolve(
        m,
        monkeypatch,
        _duplicate_native_rows(),
        _aot_s00(1, title="Since That Day"),
        aliases=_alias_for(1),
    )

    assert native is None
    assert aliases == {}


def test_exact_episode_id_with_duplicate_native_number_is_rejected(monkeypatch):
    import sync.simkl._history as m

    rows = [
        {"episode": 1, "title": "To You, 2000 Years in the Future", "tvdb": {"season": 1, "episode": 1}},
        {"episode": 1, "title": "Since That Day", "ids": {"tvdb": "4546796"}},
    ]
    item = _aot_s00(1, ep_tvdb=4546796)
    native, _aliases, _ = _resolve(m, monkeypatch, rows, item)

    assert native is None


def test_unique_native_number_season_zero_row_is_accepted(monkeypatch):
    import sync.simkl._history as m

    rows = [
        {"episode": 1, "title": "To You, 2000 Years in the Future", "tvdb": {"season": 1, "episode": 1}},
        {"episode": 26, "title": "Since That Day", "tvdb": {"season": 0, "episode": 1}},
    ]
    native, _aliases, _ = _resolve(m, monkeypatch, rows, _aot_s00(1, title="Since That Day"))

    assert native == 26


def test_cache_schema_bumped_invalidates_legacy_documents(monkeypatch):
    import sync.simkl._history as m

    assert m._CACHE_SCHEMA == 4
    store: dict[str, str] = {}
    monkeypatch.setattr(m, "state_file", lambda name: name)
    monkeypatch.setattr(m, "_load_json", lambda path: json.loads(store.get(path) or "{}"))
    monkeypatch.setattr(m, "_save_json", lambda path, data: store.__setitem__(path, json.dumps(data)))
    store[m._cache_path()] = json.dumps({"schema": 3, "items": {"poisoned@1": {"type": "episode", "season": 0, "episode": 1}}})

    assert m._cache_doc_is_stale() is True
    assert m._cache_load() == {}


def test_conflicting_season_zero_alias_is_rejected_and_purged(monkeypatch):
    import sync.simkl._history as m

    _alias_store(monkeypatch, m, _poisoned_alias())
    injected = _patch_write_env(monkeypatch, m)
    session = _Session(
        post_handler=lambda url, body: _Resp(200, _ok_add(episodes=1)),
        redirect_map=AOT_REDIRECT,
        episodes_map=AOT_ROWS,
    )

    ok, unresolved = m.add(_adapter(session), [_aot_s00(1, title="Ilse's Notebook")])

    assert ok == 0
    assert [u["hint"] for u in unresolved] == ["simkl_anime_season_zero_unmapped"]
    assert list(_anime_of(session, m.URL_ADD)) == []
    assert list(_shows_of(session, m.URL_ADD)) == []
    assert injected == []
    assert m._load_anime_episode_alias_cache() == {}


def test_authoritative_season_zero_mapping_is_used(monkeypatch):
    import sync.simkl._history as m

    rows = {
        "39687": AOT_ROWS["39687"] + [
            {"episode": 26, "title": "Ilse's Notebook", "tvdb": {"season": 0, "episode": 1}}
        ]
    }
    _alias_store(monkeypatch, m)
    _patch_write_env(monkeypatch, m)
    session = _Session(
        post_handler=lambda url, body: _Resp(200, _ok_add(episodes=1)),
        redirect_map=AOT_REDIRECT,
        episodes_map=rows,
    )

    ok, unresolved = m.add(_adapter(session), [_aot_s00(1)])

    numbers = [e["number"] for grp in _anime_of(session, m.URL_ADD) for e in grp["episodes"]]
    assert numbers == [26]
    assert ok == 1
    assert unresolved == []


def test_season_zero_episode_id_match_beats_absolute_number(monkeypatch):
    import sync.simkl._history as m

    rows = {
        "39687": [
            {"episode": 3, "title": "aot3", "tvdb": {"season": 1, "episode": 3}},
            {"episode": 27, "title": "OVA", "ids": {"tvdb": "4635717"}},
        ]
    }
    _alias_store(monkeypatch, m)
    _patch_write_env(monkeypatch, m)
    session = _Session(
        post_handler=lambda url, body: _Resp(200, _ok_add(episodes=1)),
        redirect_map=AOT_REDIRECT,
        episodes_map=rows,
    )

    ok, unresolved = m.add(_adapter(session), [_aot_s00(1, ep_tvdb=4635717, number_abs=3)])

    numbers = [e["number"] for grp in _anime_of(session, m.URL_ADD) for e in grp["episodes"]]
    assert numbers == [27]
    assert ok == 1
    assert unresolved == []


def test_ok_response_does_not_confirm_unvalidated_season_zero(monkeypatch):
    import sync.simkl._history as m

    _alias_store(monkeypatch, m, _poisoned_alias())
    injected = _patch_write_env(monkeypatch, m)
    session = _Session(
        post_handler=lambda url, body: _Resp(200, _ok_add(episodes=1, not_found=None)),
        redirect_map=AOT_REDIRECT,
        episodes_map=AOT_ROWS,
    )
    adapter = _adapter(session)

    ok, unresolved = m.add(adapter, [_aot_s00(1, title="Ilse's Notebook"), _aot_s00(2), _aot_s00(3)])

    assert session.posts == []
    assert ok == 0
    assert len(unresolved) == 3
    assert all(u["hint"] == "simkl_anime_season_zero_unmapped" for u in unresolved)
    assert getattr(adapter, "_simkl_history_add_confirmed_keys") == []
    assert injected == []


def test_season_zero_group_anchor_inference_disabled(monkeypatch):
    import sync.simkl._history as m

    rows = {
        "39687": AOT_ROWS["39687"] + [
            {"episode": 26, "title": "Ilse's Notebook", "tvdb": {"season": 0, "episode": 1}}
        ]
    }
    _alias_store(monkeypatch, m)
    monkeypatch.setattr(m, "_headers", lambda *a, **k: {})
    monkeypatch.setattr(m, "simkl_api_params_from_headers", lambda headers=None, **k: dict(k))
    session = _Session(episodes_map=rows)

    mapped = m._anime_retry_episode_numbers_for_group(
        [_aot_s00(1), _aot_s00(2), _aot_s00(3)],
        {"simkl": "39687", "tvdb": "267440"},
        session=session,
        headers={},
        timeout=5,
        episode_cache={},
    )

    assert list(mapped.values()) == [26]


def test_normal_anime_season_one_still_writes(monkeypatch):
    import sync.simkl._history as m

    _alias_store(monkeypatch, m)
    _patch_write_env(monkeypatch, m)
    session = _Session(
        post_handler=lambda url, body: _Resp(200, _ok_add(episodes=1)),
        redirect_map=AOT_REDIRECT,
        episodes_map=AOT_ROWS,
    )

    item = {
        "type": "episode",
        "season": 1,
        "episode": 1,
        "watched_at": WATCHED,
        "show_ids": {"tvdb": "267440", "tmdb": "1429"},
        "series_title": "Attack on Titan",
    }
    ok, unresolved = m.add(_adapter(session), [item])

    numbers = [e["number"] for grp in _anime_of(session, m.URL_ADD) for e in grp["episodes"]]
    assert numbers == [1]
    assert ok == 1
    assert unresolved == []


