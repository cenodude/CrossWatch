"""Real provider item shapes -> SIMKL anime history.

Dragon Ball Z S02E13 is native episode 52 on SIMKL. Verified against the live
SIMKL API: /anime/episodes/41487 returns 291 episodes, and episode 52 carries
tvdb {"season": 1, "episode": 52} -- SIMKL models DBZ as one flat 291-episode
season, while TVDB (and AniBridge) split it across S1-S9.

So SIMKL's own coordinate lookup cannot match a source reporting S02E13, and the
mapping layer has to supply the answer. A source reporting the flat S01E52 does
match SIMKL directly, which is covered below too.
"""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

SIMKL_DBZ = "41487"
DBZ_NATIVE_EPISODES = 291
WATCHED = "2024-01-01T00:00:00Z"

# The slice of AniBridge that covers DBZ season 2.
MAPPINGS = {
    "tvdb_show:81472:s1": {
        "mal:813": {"1-39": "1-39"},
        "anilist:813": {"1-39": "1-39"},
        "anidb:1530:R": {"1-39": "1-39"},
        "tmdb_show:12971:s1": {"1-39": "1-39"},
    },
    "tvdb_show:81472:s2": {
        "mal:813": {"1-35": "40-74"},
        "anilist:813": {"1-35": "40-74"},
        "tmdb_show:12971:s2": {"1-35": "1-35"},
    },
    "tmdb_show:12971:s2": {
        "mal:813": {"1-35": "40-74"},
        "anilist:813": {"1-35": "40-74"},
    },
}

IDENTITY_TSV = "\n".join([
    "\t".join(["title", "anidb", "anilist", "kitsu", "myanimelist", "simkl", "themoviedb", "thetvdb"]),
    "\t".join(["Dragon Ball Z", "1530", "813", "720", "813", SIMKL_DBZ, "12971", "81472"]),
])


class _Resp:
    def __init__(self, status=200, payload=None, headers=None):
        self.status_code = status
        self._payload = payload if payload is not None else {}
        self.text = json.dumps(self._payload)
        self.headers = headers or {}

    def json(self):
        return self._payload

    @property
    def ok(self):
        return 200 <= self.status_code < 300


class _Session:
    def __init__(self, m):
        self._m = m
        self.posts = []
        self.redirects = []

    def get(self, url, headers=None, params=None, timeout=None, allow_redirects=None):
        p = dict(params or {})
        if url == self._m.URL_ALL_ITEMS:
            return _Resp(200, {"movies": [], "shows": [], "anime": []})
        if url == self._m.URL_REDIRECT:
            self.redirects.append(p)
            for ns, val in (("tvdb", "81472"), ("anidb", "1530"), ("mal", "813"), ("anilist", "813")):
                if str(p.get(ns) or "") == val:
                    return _Resp(302, {}, headers={"Location": f"https://simkl.com/anime/{SIMKL_DBZ}/x"})
            return _Resp(404, {})
        if "/anime/episodes/" in url:
            if url.rsplit("/", 1)[-1] != SIMKL_DBZ:
                return _Resp(200, [])
            # Flat season 1 tvdb map, exactly as the live API returns it for DBZ.
            return _Resp(200, [
                {"episode": n, "title": f"DBZ {n}", "tvdb": {"season": 1, "episode": n}}
                for n in range(1, DBZ_NATIVE_EPISODES + 1)
            ])
        return _Resp(404, {})

    def post(self, url, headers=None, params=None, json=None, timeout=None):
        self.posts.append({"url": url, "json": json})
        return _Resp(200, {
            "added": {"movies": 0, "shows": 1, "episodes": 1},
            "not_found": {"movies": [], "shows": [], "episodes": []},
        })


@pytest.fixture()
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("CONFIG_BASE", str(tmp_path))

    from cw_platform.anime_mapping import storage

    storage._PATHS_CACHE.clear()
    storage.close_readers()
    paths = storage.paths("v3")
    paths["root"].mkdir(parents=True, exist_ok=True)
    paths["mappings"].write_text(json.dumps(MAPPINGS), encoding="utf-8")
    paths["identity"].write_text(IDENTITY_TSV, encoding="utf-8")
    storage.rebuild_sqlite_from_mappings(release_tag="v3")

    import sync.simkl._history as m

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

    yield m
    storage.close_readers()
    storage._PATHS_CACHE.clear()


def _write(m, item):
    session = _Session(m)
    adapter = SimpleNamespace(
        client=SimpleNamespace(session=session),
        cfg=SimpleNamespace(timeout=5, history_chunk_size=100),
        raw_cfg={"anime_mapping": {"enabled": True, "features": ["watchlist", "ratings", "history"], "release_tag": "v3"}},
    )
    ok, unresolved = m.add(adapter, [dict(item)])
    anime, shows = [], []
    for p in session.posts:
        body = p["json"] if isinstance(p["json"], dict) else {}
        anime += body.get("anime") or []
        shows += body.get("shows") or []
    return {
        "ok": ok,
        "unresolved": unresolved,
        "anime": anime,
        "shows": shows,
        "numbers": [e["number"] for g in anime for e in g.get("episodes", [])],
        "simkl": anime[0]["ids"].get("simkl") if anime else None,
        "redirect_namespaces": sorted({k for r in session.redirects for k in r if k != "to"}),
    }


def _assert_landed_on_52(res):
    assert res["numbers"] == [52], res
    assert str(res["simkl"]) == SIMKL_DBZ
    assert res["shows"] == [], "must not also be written under the source's aired coordinates"
    assert res["ok"] == 1
    assert res["unresolved"] == []


# MDBList emits ints for tmdb/tvdb/trakt (mdblist/_history.py::_ids_pick).
MDBLIST = {
    "type": "episode", "season": 2, "episode": 13, "watched_at": WATCHED,
    "series_title": "Dragon Ball Z",
    "show_ids": {"tmdb": 12971, "imdb": "tt0142233", "tvdb": 81472, "trakt": 1234},
}

TRAKT = {
    "type": "episode", "season": 2, "episode": 13, "watched_at": WATCHED,
    "series_title": "Dragon Ball Z",
    "show_ids": {"tmdb": "12971", "imdb": "tt0142233", "tvdb": "81472", "trakt": "1234"},
    "_trakt_number_abs": 52,
}

# FLOPPY is TMDB-only (floppy/_common.py::item_from_row).
FLOPPY = {
    "type": "episode", "season": 2, "episode": 13, "watched_at": WATCHED,
    "series_title": "Dragon Ball Z",
    "show_ids": {"tmdb": "12971"},
}


def test_mdblist_to_simkl(env) -> None:
    res = _write(env, MDBLIST)
    _assert_landed_on_52(res)
    assert "tvdb" in res["redirect_namespaces"]


def test_mdblist_integer_ids_are_handled(env) -> None:
    """MDBList stores tmdb/tvdb/trakt as ints rather than strings."""
    assert isinstance(MDBLIST["show_ids"]["tvdb"], int)
    _assert_landed_on_52(_write(env, MDBLIST))


def test_trakt_to_simkl(env) -> None:
    res = _write(env, TRAKT)
    _assert_landed_on_52(res)


def test_trakt_without_its_absolute_hint_falls_back_to_anibridge(env) -> None:
    item = {k: v for k, v in TRAKT.items() if k != "_trakt_number_abs"}
    res = _write(env, item)
    _assert_landed_on_52(res)
    assert "mal" in res["redirect_namespaces"], "AniBridge's namespace must be verified"


def test_floppy_to_simkl(env) -> None:
    _assert_landed_on_52(_write(env, FLOPPY))


def test_floppy_needs_no_redirect_when_identity_is_local(env) -> None:
    """TMDB-only source: the offline identity table resolves the entity with no HTTP call."""
    res = _write(env, FLOPPY)
    _assert_landed_on_52(res)
    assert res["redirect_namespaces"] == []


def test_all_three_providers_agree(env) -> None:
    results = [_write(env, item) for item in (MDBLIST, TRAKT, FLOPPY)]
    assert {r["numbers"][0] for r in results} == {52}
    assert {str(r["simkl"]) for r in results} == {SIMKL_DBZ}


def test_simkl_own_map_wins_for_a_flat_numbered_source(env) -> None:
    """A source reporting SIMKL's own flat coordinate resolves at step 3, before AniBridge.

    SIMKL maps DBZ native 52 to tvdb S01E52, so a Shoko-style flat library asking
    for S01E52 matches SIMKL directly and never needs the mapping layer.
    """
    flat = {
        "type": "episode", "season": 1, "episode": 52, "watched_at": WATCHED,
        "series_title": "Dragon Ball Z",
        "show_ids": {"tmdb": "12971", "tvdb": "81472"},
    }
    res = _write(env, flat)
    assert res["numbers"] == [52]
    assert str(res["simkl"]) == SIMKL_DBZ
    # AniBridge's namespace never had to be verified, so only the tvdb discovery call happened.
    assert res["redirect_namespaces"] == ["tvdb"]


def test_out_of_range_episode_is_refused(env) -> None:
    """Season 2 has 35 episodes; E99 must not be invented."""
    bad = {**FLOPPY, "episode": 99}
    res = _write(env, bad)
    assert res["numbers"] == []


def test_non_anime_show_is_untouched(env) -> None:
    """A show with no AniBridge edges keeps the normal shows[] payload."""
    lost = {
        "type": "episode", "season": 1, "episode": 1, "watched_at": WATCHED,
        "series_title": "Lost", "show_ids": {"tmdb": "4607", "tvdb": "73739"},
    }
    res = _write(env, lost)
    assert res["anime"] == []
    assert len(res["shows"]) == 1
    assert res["redirect_namespaces"] == ["tvdb"]


def test_user_override_redirects_all_three_providers(env) -> None:
    """One rule keyed on tvdb also covers the TMDB-only source via its own match."""
    from cw_platform.anime_mapping import overrides as ov

    for provider in ("tvdb", "tmdb"):
        ov.upsert_override({
            "media_type": "show",
            "title": "DBZ override",
            "match_provider": provider,
            "match_id": "81472" if provider == "tvdb" else "12971",
            "match_season": 2,
            "target_namespace": "mal",
            "target_id": "813",
            "episode_from": 1,
            "episode_to": 35,
            "episode_start_at": 200,
        })

    for item in (MDBLIST, TRAKT, FLOPPY):
        res = _write(env, item)
        # Trakt's own absolute hint is checked before the mapping layer, so it keeps 52.
        expected = 52 if item.get("_trakt_number_abs") else 212
        assert res["numbers"] == [expected], (item["show_ids"], res)
