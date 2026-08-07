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
    def __init__(self, post_handler=None, redirect_map=None, episodes_map=None):
        self.posts = []
        self.gets = []
        self._post_handler = post_handler or (lambda url, body: _Resp(200, _ok_add()))
        self._redirect_map = redirect_map or {}
        self._episodes_map = {str(k): v for k, v in (episodes_map or {}).items()}

    def post(self, url, headers=None, params=None, json=None, timeout=None):
        self.posts.append({"url": url, "json": json})
        return self._post_handler(url, json)

    def get(self, url, headers=None, params=None, timeout=None, allow_redirects=None):
        self.gets.append({"url": url, "params": dict(params or {})})
        import sync.simkl._history as m

        if url == m.URL_ALL_ITEMS:
            return _Resp(200, {"movies": [], "shows": [], "anime": []})
        if url == m.URL_REDIRECT:
            p = dict(params or {})
            for ns in ("tvdb", "anidb", "mal", "anilist", "kitsu"):
                value = str(p.get(ns) or "")
                if value and (ns, value) in self._redirect_map:
                    target = self._redirect_map[(ns, value)]
                    return _Resp(302, {}, headers={"Location": f"https://simkl.com/anime/{target}/x"})
            return _Resp(404, {})
        if "/anime/episodes/" in url:
            return _Resp(200, self._episodes_map.get(url.rsplit("/", 1)[-1], []))
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
    monkeypatch.setattr(m, "_offline_simkl_id", lambda *a, **k: None)
    monkeypatch.setattr(m, "_save_anime_episode_alias_cache", lambda *a, **k: None)


_CFG = {"anime_mapping": {"enabled": True, "features": ["watchlist", "ratings", "history"], "release_tag": "v3"}}


def _adapter(session):
    return SimpleNamespace(
        client=SimpleNamespace(session=session),
        cfg=SimpleNamespace(timeout=5, history_chunk_size=100),
        raw_cfg=_CFG,
    )


def _anime_of(session, url):
    for p in session.posts:
        if p["url"] == url and isinstance(p["json"], dict) and "anime" in p["json"]:
            yield from p["json"]["anime"]


def _shows_of(session, url):
    for p in session.posts:
        if p["url"] == url and isinstance(p["json"], dict) and "shows" in p["json"]:
            yield from p["json"]["shows"]


# FLOPPY emits TMDB-only show_ids (providers/sync/floppy/_common.py::item_from_row).
def _floppy_episode(tmdb, season, episode, title):
    return {
        "type": "episode",
        "season": season,
        "episode": episode,
        "watched_at": "2024-01-01T00:00:00Z",
        "show_ids": {"tmdb": tmdb},
        "title": title,
    }


# One Piece: AniBridge maps tmdb_show:37854:s22 -> anidb:69 absolute 1089+.
_OP_EPISODES = {
    "1234": [
        {"episode": n, "title": f"op{n}", "tvdb": {"season": 21, "episode": n - 891}}
        for n in range(1080, 1100)
    ]
}
_OP_REDIRECT = {("anidb", "69"): "1234"}


def _patch_one_piece_resolution(monkeypatch, m):
    from cw_platform.anime_mapping.episodes import Resolution

    def fake_resolve(item, **kw):
        show_ids = item.get("show_ids") or {}
        if str(show_ids.get("tmdb")) == "37854" and item.get("season") == 22:
            return Resolution(
                absolute=1088 + int(item["episode"]),
                namespace="anidb",
                target_id="69",
                basis="anibridge_absolute",
                entry="tmdb_direct",
            )
        return None

    monkeypatch.setattr(m, "resolve_absolute", fake_resolve)


def test_tmdb_only_anime_routes_to_native_absolute(monkeypatch):
    """Issue #660: a TMDB-only source (FLOPPY) must still reach the SIMKL anime[] payload."""
    import sync.simkl._history as m

    _patch_fs(monkeypatch, m)
    _patch_one_piece_resolution(monkeypatch, m)
    session = _Session(redirect_map=_OP_REDIRECT, episodes_map=_OP_EPISODES)
    adapter = _adapter(session)

    ok, unresolved = m.add(adapter, [_floppy_episode("37854", 22, 1, "One Piece")])

    groups = list(_anime_of(session, m.URL_ADD))
    assert len(groups) == 1
    assert groups[0]["ids"] == {"simkl": "1234"}
    assert [e["number"] for e in groups[0]["episodes"]] == [1089]
    # It must not also be written under the source's aired coordinates.
    assert list(_shows_of(session, m.URL_ADD)) == []
    assert ok == 1
    assert unresolved == []


def test_tmdb_only_anime_entity_is_resolved_via_native_namespace(monkeypatch):
    """The SIMKL entity must be looked up through the AniBridge namespace, not TVDB."""
    import sync.simkl._history as m

    _patch_fs(monkeypatch, m)
    _patch_one_piece_resolution(monkeypatch, m)
    session = _Session(redirect_map=_OP_REDIRECT, episodes_map=_OP_EPISODES)
    adapter = _adapter(session)

    m.add(adapter, [_floppy_episode("37854", 22, 1, "One Piece")])

    redirects = [g["params"] for g in session.gets if g["url"] == m.URL_REDIRECT]
    assert redirects, "expected a SIMKL redirect lookup"
    assert all("tvdb" not in p for p in redirects)
    assert any(p.get("anidb") == "69" for p in redirects)


def test_tmdb_only_anime_without_mapping_is_not_diverted(monkeypatch):
    """Without an AniBridge resolution the item must stay on the normal shows[] path."""
    import sync.simkl._history as m

    _patch_fs(monkeypatch, m)
    monkeypatch.setattr(m, "resolve_absolute", lambda item, **kw: None)
    session = _Session(redirect_map=_OP_REDIRECT, episodes_map=_OP_EPISODES)
    adapter = _adapter(session)

    m.add(adapter, [_floppy_episode("37854", 22, 1, "One Piece")])

    assert list(_anime_of(session, m.URL_ADD)) == []
    shows = list(_shows_of(session, m.URL_ADD))
    assert len(shows) == 1
    assert shows[0]["ids"] == {"tmdb": "37854"}


def test_tmdb_only_anime_rejected_when_absolute_absent_from_simkl(monkeypatch):
    """An absolute that SIMKL's own episode list does not contain must not be written."""
    import sync.simkl._history as m

    _patch_fs(monkeypatch, m)
    _patch_one_piece_resolution(monkeypatch, m)
    # SIMKL only knows episodes 1..20 for this entry, so absolute 1089 cannot be confirmed.
    short_map = {"1234": [{"episode": n, "title": f"op{n}"} for n in range(1, 21)]}
    session = _Session(redirect_map=_OP_REDIRECT, episodes_map=short_map)
    adapter = _adapter(session)

    m.add(adapter, [_floppy_episode("37854", 22, 1, "One Piece")])

    assert [e["number"] for grp in _anime_of(session, m.URL_ADD) for e in grp["episodes"]] == []


def test_tmdb_only_normal_tv_is_unaffected(monkeypatch):
    """Non-anime TMDB-only episodes (Lost) keep using the plain shows[] payload."""
    import sync.simkl._history as m

    _patch_fs(monkeypatch, m)
    monkeypatch.setattr(m, "resolve_absolute", lambda item, **kw: None)
    session = _Session()
    adapter = _adapter(session)

    m.add(adapter, [_floppy_episode("4607", 1, 1, "Lost")])

    assert list(_anime_of(session, m.URL_ADD)) == []
    shows = list(_shows_of(session, m.URL_ADD))
    assert len(shows) == 1
    assert shows[0]["ids"] == {"tmdb": "4607"}
    seasons = {s["number"]: s for s in shows[0]["seasons"]}
    assert [e["number"] for e in seasons[1]["episodes"]] == [1]
    assert [g["params"] for g in session.gets if g["url"] == m.URL_REDIRECT] == []


# Dragon Ball Z as FLOPPY actually reports it: aired seasons, per-season episode numbers.
# AniBridge maps tmdb_show:12971:s2 -> mal:813 "1-35" -> "40-74", so S02E13 is absolute 52.
_DBZ_EPISODES = {"41487": [{"episode": n, "title": f"dbz{n}"} for n in range(1, 300)]}
_DBZ_REDIRECT = {("mal", "813"): "41487"}


def test_tmdb_only_aired_numbering_maps_to_simkl_absolute(monkeypatch):
    """FLOPPY reports DBZ S02E13; SIMKL stores it as absolute episode 52."""
    import sync.simkl._history as m
    from cw_platform.anime_mapping.episodes import Resolution

    _patch_fs(monkeypatch, m)
    monkeypatch.setattr(
        m,
        "resolve_absolute",
        lambda item, **kw: Resolution(
            absolute=52, namespace="mal", target_id="813", basis="anibridge_absolute", entry="show_pair_hop"
        )
        if str((item.get("show_ids") or {}).get("tmdb")) == "12971" and (item.get("season"), item.get("episode")) == (2, 13)
        else None,
    )
    session = _Session(redirect_map=_DBZ_REDIRECT, episodes_map=_DBZ_EPISODES)
    adapter = _adapter(session)

    ok, unresolved = m.add(adapter, [_floppy_episode("12971", 2, 13, "Dragon Ball Z")])

    groups = list(_anime_of(session, m.URL_ADD))
    assert len(groups) == 1
    assert groups[0]["ids"] == {"simkl": "41487"}
    assert [e["number"] for e in groups[0]["episodes"]] == [52]
    assert list(_shows_of(session, m.URL_ADD)) == []
    assert ok == 1
    assert unresolved == []


def test_offline_identity_serves_entity_discovery(monkeypatch):
    """The local identity table resolves the SIMKL entry without a discovery redirect."""
    import sync.simkl._history as m

    _patch_fs(monkeypatch, m)
    _patch_one_piece_resolution(monkeypatch, m)
    monkeypatch.setattr(m, "_offline_simkl_id", lambda ns, ident, tag: "1234" if (ns, ident) == ("anidb", "69") else None)
    # No redirect_map: every live redirect fails, so only the offline table can discover.
    session = _Session(redirect_map={}, episodes_map=_OP_EPISODES)
    adapter = _adapter(session)

    m.add(adapter, [_floppy_episode("37854", 22, 1, "One Piece")])

    groups = list(_anime_of(session, m.URL_ADD))
    assert len(groups) == 1
    assert groups[0]["ids"] == {"simkl": "1234"}
    assert [e["number"] for e in groups[0]["episodes"]] == [1089]


def test_entity_verification_still_uses_the_live_redirect(monkeypatch):
    """Verification of an already-known SIMKL id must not be served from the local table."""
    import sync.simkl._history as m

    calls: list[tuple] = []

    def _offline(ns, ident, tag):
        calls.append((ns, ident))
        return "41487"

    _patch_fs(monkeypatch, m)
    monkeypatch.setattr(m, "_offline_simkl_id", _offline)
    session = _Session(redirect_map={}, episodes_map={})
    item = {
        "type": "episode",
        "season": 2,
        "episode": 1,
        "_cw_anime_map": {"absolute": 40, "namespace": "anidb", "target_id": "1530", "release_tag": "v3"},
    }

    got = m._anibridge_absolute(
        item,
        {"simkl": "41487"},
        session=session,
        headers={},
        timeout=5,
        resolve_state=m._AnimeResolveState({}, {}),
    )

    assert got is None, "verification must fail when the live redirect cannot confirm the entity"
    assert calls == [], "verification must not consult the offline identity table"


def test_written_absolute_is_remembered_as_source_coordinates(monkeypatch):
    """The alias written back must map SIMKL's absolute onto the source's aired key.

    Without this the next run would re-plan the same episode forever (issue #660).
    """
    import sync.simkl._history as m

    _patch_fs(monkeypatch, m)
    _patch_one_piece_resolution(monkeypatch, m)
    saved: dict[str, dict] = {}
    monkeypatch.setattr(m, "_save_anime_episode_alias_cache", lambda rows: saved.update(rows))
    session = _Session(redirect_map=_OP_REDIRECT, episodes_map=_OP_EPISODES)
    adapter = _adapter(session)

    m.add(adapter, [_floppy_episode("37854", 22, 1, "One Piece")])

    assert "1234:1089" in saved
    alias = saved["1234:1089"]
    assert (alias["season"], alias["episode"]) == (22, 1)
    assert alias["show_ids"] == {"tmdb": "37854"}
