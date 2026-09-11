# tests/test_media_server_scrobble_sinks.py
# CrossWatch - Media Server Scrobble Sink Tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
from dataclasses import replace
from importlib import import_module
from types import SimpleNamespace

import pytest

from providers.scrobble import _media_server as common
from providers.scrobble.routes import build_route_cfg
from providers.scrobble.scrobble import ScrobbleEvent


def config(provider):
    block = {"server": "http://default", "access_token": "default-token", "user_id": "default-user", "connection_verified": True}
    return {provider: {**block, "instances": {"P01": {**block, "server": "http://p01", "access_token": "p01-token", "user_id": "p01-user"}}},
            "scrobble": {"watch": {"route_provider": "plex", "route_provider_instance": "default", "route_sink": provider}}}


def event(**kwargs):
    return replace(ScrobbleEvent("stop", "movie", {"tmdb": "42"}, "Movie", 2020, None, None, 95,
                                "source-user", "source-server", "session", {}), **kwargs)


def row(iid="10", kind="Movie", ids=None, **kwargs):
    return {"Id": iid, "Type": kind, "ProviderIds": ids or {"Tmdb": "42"}, "Name": "Movie", "LibraryId": "L1",
            "RunTimeTicks": 1_000_000_000, "UserData": {"Played": False, "PlayCount": 0, "PlaybackPositionTicks": 0,
                                                     "IsFavorite": True, "Rating": 8, "LastPlayedDate": "2020-01-01T00:00:00Z"}, **kwargs}


class Http:
    def __init__(self, provider):
        self.provider = provider
        self.rows = {"10": row()}
        self.calls = []
        self.sessions = []
        self.fail = False
        self.fail_sessions = False
        self.supported = True
        self.session = SimpleNamespace(close=lambda: None)

    def response(self, value, status=200):
        return SimpleNamespace(status_code=status, json=lambda: copy.deepcopy(value))

    def progress_write_supported(self):
        return self.supported, "10.11.0"

    def get(self, path, params=None):
        params = params or {}
        if params.get("Fields"):
            assert set(params["Fields"].split(",")) <= {"ProviderIds", "ParentId"}, "Fields must use valid API enum names"
        self.calls.append(("GET", path, dict(params)))
        if path == "/Sessions":
            return self.response(self.sessions, 503 if self.fail_sessions else 200)
        if path.endswith("/Ancestors"):
            return self.response([{"Id": "L1"}])
        if path.endswith("/Items"):
            rows = list(self.rows.values())
            if params.get("IncludeItemTypes"):
                rows = [r for r in rows if r["Type"] in params["IncludeItemTypes"].split(",")]
            if params.get("SearchTerm"):
                rows = [r for r in rows if params["SearchTerm"] == r["Name"]]
            if params.get("AnyProviderIdEquals"):
                key, val = params["AnyProviderIdEquals"].split(".", 1)
                rows = [r for r in rows if {k.lower(): v for k, v in r["ProviderIds"].items()}.get(key) == val]
            if params.get("ParentId"):
                rows = [r for r in rows if r.get("SeriesId") == params["ParentId"]]
            for key in ("IndexNumber", "ParentIndexNumber"):
                if key in params:
                    rows = [r for r in rows if r.get(key) == params[key]]
            start, count = params.get("StartIndex", 0), params.get("Limit", 100)
            return self.response({"Items": rows[start:start+count], "TotalRecordCount": len(rows)})
        iid = path.rsplit("/", 1)[-1]
        return self.response(self.rows.get(iid), 200 if iid in self.rows else 404)

    def post(self, path, params=None, json=None):
        self.calls.append(("POST", path, {"params": params or {}, "json": copy.deepcopy(json)}))
        if self.fail:
            return self.response({}, 503)
        if path.endswith("/UserData"):
            self.rows[path.split("/")[-2]]["UserData"].update(json)
        else:
            self.rows[path.rsplit("/", 1)[-1]]["UserData"].update(Played=True, PlayCount=1)
        def empty():
            raise ValueError("No JSON in response")
        return SimpleNamespace(status_code=200, json=empty)


@pytest.fixture(params=["jellyfin", "emby"])
def media_server(request, monkeypatch):
    provider = request.param
    http = Http(provider)
    configs = []
    def adapter(cfg):
        configs.append(copy.deepcopy(cfg))
        block = cfg[provider]
        return SimpleNamespace(config=cfg, cfg=SimpleNamespace(user_id=block["user_id"],
            history_libraries=(block.get("history") or {}).get("libraries"),
            progress_libraries=(block.get("progress") or {}).get("libraries")), client=http)
    monkeypatch.setattr(import_module(f"providers.sync._mod_{provider.upper()}"), f"{provider.upper()}Module", adapter)
    monkeypatch.setattr(common, "record_watch", lambda *a, **kw: None)
    monkeypatch.setattr(common, "record_scrobble_event", lambda *a, **kw: None)
    cls = getattr(import_module(f"providers.scrobble.{provider}.sink"), "JellyfinSink" if provider == "jellyfin" else "EmbySink")
    return provider, cls, http, configs


def test_media_server_completion_resume_and_duplicate(media_server):
    provider, cls, http, _ = media_server
    cfg = config(provider)
    sink = cls()
    http.rows["10"]["UserData"]["PlaybackPositionTicks"] = 300_000_000
    assert sink.send(event(ids={"tmdb": "42", provider: "source-id"}), cfg)["ok"]
    assert sink.send(event(ids={"tmdb": "42", provider: "source-id"}), cfg)["reason"] == "duplicate"
    data = http.rows["10"]["UserData"]
    assert data["Played"] and data["PlayCount"] == 1 and data["PlaybackPositionTicks"] == 0
    assert data["IsFavorite"] and data["Rating"] == 8
    writes = [c for c in http.calls if c[0] == "POST"]
    assert len(writes) == 2
    if provider == "jellyfin":
        assert writes[0][1] == "/UserPlayedItems/10"
        assert all(c[2]["params"]["userId"] == "default-user" for c in writes)
    else:
        assert writes[0][1] == "/Users/default-user/PlayedItems/10"
        assert writes[1][2]["json"]["IsFavorite"] is True


def test_media_server_partial_uses_destination_ticks_and_preserves_watched(media_server):
    provider, cls, http, _ = media_server
    sink = cls()
    assert sink.send(event(action="pause", progress=30, duration_ms=200_000), config(provider))["ok"]
    assert http.rows["10"]["UserData"]["PlaybackPositionTicks"] == 300_000_000
    assert not http.rows["10"]["UserData"]["Played"]
    http.rows["10"]["UserData"]["Played"] = True
    http.calls.clear()
    assert sink.send(event(action="pause", progress=40), config(provider))["reason"] == "destination_already_watched"
    assert not any(c[0] == "POST" for c in http.calls)


@pytest.mark.parametrize("source,target", [("default", "P01"), ("P01", "default"), ("default", "default"), ("P01", "P01")])
def test_media_server_instance_isolation(media_server, source, target):
    provider, cls, http, configs = media_server
    cfg = config(provider)
    view = build_route_cfg(cfg, {"provider": provider, "provider_instance": source, "sink": provider, "sink_instance": target})
    result = cls(instance_id=target).send(event(), view)
    if source == target:
        assert result["error"] == "same_source_destination" and not configs
    else:
        assert result["ok"] and configs[0][provider]["access_token"] == f"{target.lower()}-token"
        assert view[provider]["user_id"] == f"{source.lower()}-user"


@pytest.mark.parametrize("case", ["missing_ids", "conflict", "ambiguous", "library", "active", "sessions_error", "missing_userdata"])
def test_media_server_guards(media_server, case):
    provider, cls, http, _ = media_server
    cfg, ev = config(provider), event()
    if case == "missing_ids":
        ev = event(ids={provider: "10"})
    elif case == "conflict":
        ev = event(ids={"tmdb": "42", "imdb": "tt0000111"})
        http.rows["10"]["ProviderIds"]["Imdb"] = "tt0000999"
    elif case == "ambiguous":
        http.rows["11"] = row("11")
    elif case == "library":
        cfg[provider]["history"] = {"libraries": ["other"]}
    elif case == "active":
        http.sessions = [{"UserId": "default-user", "NowPlayingItem": {"Id": "10"}}]
    elif case == "sessions_error":
        http.fail_sessions = True
    else:
        http.rows["10"].pop("UserData")
    result = cls().send(ev, cfg)
    assert not result["ok"] or result.get("skipped")
    assert not any(c[0] == "POST" for c in http.calls)


def test_media_server_other_users_playback_does_not_block(media_server):
    provider, cls, http, _ = media_server
    http.sessions = [{"UserId": "someone-else", "NowPlayingItem": {"Id": "10"}}]
    assert cls().send(event(), config(provider))["ok"]


def test_media_server_active_playback_is_retryable_and_not_acknowledged(media_server):
    provider, cls, http, _ = media_server
    sink = cls()
    http.sessions = [{"UserId": "default-user", "NowPlayingItem": {"Id": "10"}}]
    assert sink.send(event(), config(provider)) == {"ok": False, "retryable": True, "error": "active_destination_session"}
    assert not sink._sent
    http.sessions.clear()
    assert sink.send(event(), config(provider))["ok"]


def test_legacy_default_auth_credentials_are_captured_before_source_projection(media_server):
    from providers.webhooks.config import sink_configured

    provider, cls, http, configs = media_server
    cfg = config(provider)
    cfg["auth"] = {provider: {k: cfg[provider].pop(k) for k in ("server", "user_id", "access_token")}}
    view = build_route_cfg(cfg, {"provider": provider, "provider_instance": "P01", "sink": provider, "sink_instance": "default"})
    assert sink_configured(view, provider, "default")
    assert cls().send(event(), view)["ok"]
    assert configs[0][provider]["user_id"] == "default-user"
    assert configs[0][provider]["access_token"] == "default-token"
    assert not sink_configured(cfg, provider, "missing-instance")


@pytest.mark.parametrize("action", ["pause", "stop"])
def test_partial_events_do_not_archive_a_start_or_completion(media_server, monkeypatch, action):
    from cw_platform.event_archive import scrobble_recorder

    provider, cls, _, _ = media_server
    records = []
    monkeypatch.setattr(common, "record_watch", scrobble_recorder.record_watch)
    monkeypatch.setattr(scrobble_recorder, "record", lambda *a, **kw: records.append(kw))
    assert cls().send(event(action=action, progress=30), config(provider))["ok"]
    assert not records


def test_media_server_failed_delivery_retries(media_server):
    provider, cls, http, _ = media_server
    sink = cls()
    http.fail = True
    assert not sink.send(event(), config(provider))["ok"]
    http.fail = False
    assert sink.send(event(), config(provider))["ok"]


def test_media_server_stale_mapping_and_catalog_revalidation(media_server):
    provider, cls, http, _ = media_server
    sink = cls()
    assert sink.send(event(action="pause", progress=30), config(provider))["ok"]
    http.rows["10"]["ProviderIds"] = {"Tmdb": "999"}
    http.rows["20"] = row("20")
    assert sink.send(event(), config(provider))["ok"]
    assert not http.rows["10"]["UserData"]["Played"] and http.rows["20"]["UserData"]["Played"]


def test_media_server_episode_show_ids_and_specials(media_server):
    provider, cls, http, _ = media_server
    http.rows = {"show": row("show", "Series"), "ep": row("ep", "Episode", {"Tmdb": "123"},
                                                              SeriesId="show", ParentIndexNumber=0, IndexNumber=1)}
    ev = event(media_type="episode", ids={"tmdb_show": "42", "tmdb": "42"}, season=0, number=1)
    assert cls().send(ev, config(provider))["ok"]
    assert http.rows["ep"]["UserData"]["Played"] and not http.rows["show"]["UserData"]["Played"]


@pytest.mark.parametrize("season", [0, 1])
@pytest.mark.parametrize("action,progress", [("start", 20), ("stop", 95)])
def test_episode_conflict_matches_verified_show_and_coordinates(media_server, season, action, progress):
    provider, cls, http, _ = media_server
    http.rows = {"show": row("show", "Series", {"Tmdb": "42", "Tvdb": "43"}),
                 "ep": row("ep", "Episode", {"Imdb": "tt0000999", "Tvdb": "456"},
                           SeriesId="show", ParentIndexNumber=season, IndexNumber=2)}
    ev = event(action=action, progress=progress, media_type="episode", season=season, number=2,
               ids={"imdb": "tt0000111", "tvdb": "456", "tmdb_show": "42", "tvdb_show": "43"})
    assert cls().send(ev, config(provider))["ok"]
    assert bool(http.rows["ep"]["UserData"]["Played"]) == (action == "stop")
    assert not http.rows["show"]["UserData"]["Played"]
    assert any(c[0] == "POST" for c in http.calls)


@pytest.mark.parametrize("case", ["different_show", "conflicting_show_id", "wrong_season", "wrong_episode",
                                  "no_show_ids", "missing_coordinates", "not_a_show", "library", "ambiguous"])
def test_episode_conflict_fallback_still_rejects_invalid_matches(media_server, case):
    provider, cls, http, _ = media_server
    cfg = config(provider)
    ids = {"imdb": "tt0000111", "tvdb": "456", "tmdb_show": "42", "tvdb_show": "43"}
    http.rows = {"show": row("show", "Series", {"Tmdb": "42", "Tvdb": "43"}),
                 "ep": row("ep", "Episode", {"Imdb": "tt0000999", "Tvdb": "456"},
                           SeriesId="show", ParentIndexNumber=1, IndexNumber=2)}
    if case == "different_show":
        http.rows["show"]["ProviderIds"] = {"Tmdb": "999"}
    elif case == "conflicting_show_id":
        http.rows["show"]["ProviderIds"]["Tvdb"] = "999"
    elif case == "wrong_season":
        http.rows["ep"]["ParentIndexNumber"] = 2
    elif case == "wrong_episode":
        http.rows["ep"]["IndexNumber"] = 3
    elif case == "no_show_ids":
        ids = {"imdb": "tt0000111", "tvdb": "456"}
    elif case == "not_a_show":
        http.rows["show"]["Type"] = "Movie"
    elif case == "library":
        cfg[provider]["history"] = {"libraries": ["other"]}
    elif case == "ambiguous":
        http.rows["ep2"] = {**copy.deepcopy(http.rows["ep"]), "Id": "ep2"}
    ev = event(media_type="episode", ids=ids, season=None if case == "missing_coordinates" else 1, number=2)
    assert not cls().send(ev, cfg)["ok"]
    assert not any(c[0] == "POST" for c in http.calls)


def test_direct_episode_id_match_allows_different_numbering(media_server):
    provider, cls, http, _ = media_server
    http.rows = {"ep": row("ep", "Episode", {"Tmdb": "123"}, ParentIndexNumber=2, IndexNumber=3)}
    ev = event(media_type="episode", ids={"tmdb": "123"}, season=1, number=2)
    assert cls().send(ev, config(provider))["ok"]
    assert http.rows["ep"]["UserData"]["Played"]


def test_media_server_id_only_lookup_catalog_is_reused(media_server):
    provider, cls, http, _ = media_server
    if provider == "emby":
        pytest.skip("Emby supports targeted external ID queries")
    sink = cls()
    cfg = config(provider)
    assert sink.send(event(title=None), cfg)["ok"]
    http.rows["10"]["UserData"]["Played"] = False
    assert sink.send(event(title=None, session_key="second"), cfg)["ok"]
    assert not sink.send(event(title=None, ids={"tmdb": "999"}), cfg)["ok"]
    assert not sink.send(event(title=None, ids={"tmdb": "888"}), cfg)["ok"]
    catalogs = [c for c in http.calls if c[0] == "GET" and c[2].get("IncludeItemTypes") == "Movie,Episode,Series"]
    assert len(catalogs) == 1


def test_jellyfin_unsupported_resume_does_not_simulate_playback(media_server):
    provider, cls, http, _ = media_server
    if provider != "jellyfin":
        pytest.skip("Jellyfin version gate")
    http.supported = False
    assert cls().send(event(action="pause", progress=30), config(provider))["error"] == "jellyfin_progress_write_unsupported"
    assert not any(c[0] == "POST" for c in http.calls)


@pytest.mark.parametrize("provider", ["plex", "jellyfin", "emby", "kodi"])
def test_all_media_server_registries_and_self_route_validation(provider):
    from api.scrobblerManagementAPI import _validate_route, ValidationFailure
    from providers.scrobble.watch_manager import _make_sink
    from providers.webhooks import dispatch
    from providers.webhooks.config import sink_configured
    cfg = config(provider)
    if provider == "plex":
        cfg[provider] = {"server_url": "http://plex", "pms_token": "token"}
    assert sink_configured(cfg, provider, "default")
    assert not sink_configured(cfg, provider, "unknown")
    assert _make_sink(provider, lambda: cfg, "default").name == provider
    dispatch._SINKS.clear()
    assert dispatch._make_sink(provider, "default", lambda: cfg).name == provider
    dispatch._SINKS.clear()
    with pytest.raises(ValidationFailure) as exc:
        _validate_route(cfg, {"provider": provider, "sink": provider})
    assert "same_source_destination" in str(exc.value.errors)
    if provider != "plex":
        assert _validate_route(cfg, {"provider": provider, "sink": provider, "sink_instance": "P01"})["sink_instance"] == "P01"


def test_webhook_partial_failure_is_not_acknowledged_as_complete(monkeypatch):
    from providers.webhooks import dispatch
    cfg = {"plex": {"server_url": "http://plex", "pms_token": "token"}, "scrobble": {"webhook": {"sinks": ["crosswatch", "plex"]}}}
    monkeypatch.setattr(dispatch, "_make_sink", lambda name, *a: SimpleNamespace(send=lambda *a, **kw: {"ok": name == "crosswatch", "error": "offline"}))
    result = dispatch.dispatch_scrobble("jellyfin", "/scrobble/stop", media_type="movie", ids={"tmdb": "42"}, progress=95, cfg=cfg)
    assert result.status_code == 502
    assert {t["target"]: t["ok"] for t in result.json()["targets"]} == {"crosswatch": True, "plex": False}


def kodi_row(iid=10, kind="movie", **kwargs):
    key = {"movie": "movieid", "episode": "episodeid", "show": "tvshowid"}[kind]
    return {key: iid, "title": "Movie", "uniqueid": {"tmdb": "42"}, "runtime": 100,
            "resume": {"position": 0, "total": 100}, "playcount": 0, "file": "/media/movie.mkv", **kwargs}


class Kodi:
    def __init__(self):
        self.rows = {"movie:10": kodi_row()}
        self.calls = []
        self.playing = None
        self.fail = False
        self.profile = "Default"
        self.switch = False
        self.probes = 0

    def rpc(self, method, params=None):
        params = params or {}
        self.calls.append((method, copy.deepcopy(params)))
        if method == "Profiles.GetCurrentProfile":
            self.probes += 1
            return {"profile": {"label": "Changed" if self.switch and self.probes > 1 else self.profile}}
        if method == "Player.GetActivePlayers":
            return [{"type": "video", "playerid": 1}] if self.playing else []
        if method == "Player.GetItem":
            return {"item": self.playing}
        kind = "show" if "TVShow" in method else "episode" if "Episode" in method else "movie"
        name = {"movie": "movie", "episode": "episode", "show": "tvshow"}[kind]
        iid_key = name + "id"
        if method.endswith("Details"):
            key = f"{kind}:{params[iid_key]}"
            if method.startswith("VideoLibrary.Set"):
                if self.fail:
                    raise RuntimeError("failed token=secret-must-not-leak")
                self.rows[key].update({k: v for k, v in params.items() if k != iid_key})
                return "OK"
            return {name + "details": copy.deepcopy(self.rows[key])}
        rows = [r for k, r in self.rows.items() if k.startswith(kind + ":")]
        if params.get("filter"):
            rows = [r for r in rows if r["title"] == params["filter"]["value"]]
        for field in ("tvshowid", "season"):
            if field in params:
                rows = [r for r in rows if r.get(field) == params[field]]
        return {name + "s": copy.deepcopy(rows[params["limits"]["start"]:params["limits"]["end"]]), "limits": {"total": len(rows)}}

    def set_movie(self, iid, payload):
        return self.rpc("VideoLibrary.SetMovieDetails", {"movieid": iid, **payload})

    def set_episode(self, iid, payload):
        return self.rpc("VideoLibrary.SetEpisodeDetails", {"episodeid": iid, **payload})


@pytest.fixture
def kodi(monkeypatch):
    from providers.scrobble.kodi.sink import KodiSink
    from providers.sync import _mod_KODI
    client, configs = Kodi(), []
    def adapter(cfg):
        configs.append(copy.deepcopy(cfg))
        return SimpleNamespace(config=cfg, client=client)
    monkeypatch.setattr(_mod_KODI, "KODIModule", adapter)
    monkeypatch.setattr(common, "record_watch", lambda *a, **kw: None)
    monkeypatch.setattr(common, "record_scrobble_event", lambda *a, **kw: None)
    return KodiSink, client, configs


def test_kodi_completion_resume_and_duplicate(kodi):
    cls, client, _ = kodi
    client.rows["movie:10"]["resume"]["position"] = 30
    sink = cls()
    assert sink.send(event(ids={"tmdb": "42", "kodi": "999"}), config("kodi"))["ok"]
    assert sink.send(event(ids={"tmdb": "42", "kodi": "999"}), config("kodi"))["reason"] == "duplicate"
    row = client.rows["movie:10"]
    assert row["playcount"] == 1 and row["resume"]["position"] == 0 and row["lastplayed"]
    assert len([c for c in client.calls if c[0].startswith("VideoLibrary.Set")]) == 1


def test_kodi_partial_uses_seconds_and_preserves_watched(kodi):
    cls, client, _ = kodi
    sink = cls()
    assert sink.send(event(action="pause", progress=30, duration_ms=200_000), config("kodi"))["ok"]
    assert client.rows["movie:10"]["resume"] == {"position": 30, "total": 100}
    client.rows["movie:10"]["playcount"] = 3
    assert sink.send(event(action="pause", progress=40), config("kodi"))["reason"] == "destination_already_watched"
    assert client.rows["movie:10"]["playcount"] == 3


@pytest.mark.parametrize("source,target", [("default", "P01"), ("P01", "default"), ("default", "default"), ("P01", "P01")])
def test_kodi_instance_isolation(kodi, source, target):
    cls, client, configs = kodi
    cfg = build_route_cfg(config("kodi"), {"provider": "kodi", "provider_instance": source, "sink": "kodi", "sink_instance": target})
    result = cls(instance_id=target).send(event(), cfg)
    if source == target:
        assert result["error"] == "same_source_destination" and not configs
    else:
        assert result["ok"] and configs[0]["kodi"]["server"] == f"http://{target.lower()}"


@pytest.mark.parametrize("case", ["missing_ids", "conflict", "ambiguous", "library", "active", "profile_change"])
def test_kodi_guards(kodi, case):
    cls, client, _ = kodi
    cfg, ev = config("kodi"), event()
    if case == "missing_ids":
        ev = event(ids={"kodi": "10"})
    elif case == "conflict":
        ev = event(ids={"tmdb": "42", "imdb": "tt0000111"})
        client.rows["movie:10"]["uniqueid"]["imdb"] = "tt0000999"
    elif case == "ambiguous":
        client.rows["movie:11"] = kodi_row(11)
    elif case == "library":
        cfg["kodi"]["history"] = {"libraries": ["/somewhere/else"]}
    elif case == "active":
        client.playing = {"type": "movie", "id": 10}
    else:
        client.switch = True
    result = cls().send(ev, cfg)
    assert not result["ok"] or result.get("skipped")
    assert not any(c[0].startswith("VideoLibrary.Set") for c in client.calls)


def test_kodi_failed_delivery_retries_without_exposing_credentials(kodi):
    cls, client, _ = kodi
    sink = cls()
    client.fail = True
    assert sink.send(event(), config("kodi")) == {"ok": False, "error": "kodi_scrobble_failed", "retryable": True}
    client.fail = False
    assert sink.send(event(), config("kodi"))["ok"]


@pytest.mark.parametrize("playing", [{"type": "unknown"}, {"type": "movie", "id": 99}])
def test_kodi_unrelated_playback_does_not_block_writes(kodi, playing):
    cls, client, _ = kodi
    client.playing = playing
    assert cls().send(event(), config("kodi"))["ok"]
    assert client.rows["movie:10"]["playcount"] == 1


def test_kodi_active_target_is_retryable(kodi):
    cls, client, _ = kodi
    sink = cls()
    client.playing = {"type": "movie", "id": 10}
    assert sink.send(event(), config("kodi")) == {"ok": False, "retryable": True, "error": "active_destination_session"}
    client.playing = None
    assert sink.send(event(), config("kodi"))["ok"]


def test_kodi_stale_ids_are_revalidated(kodi):
    cls, client, _ = kodi
    sink = cls()
    assert sink.send(event(action="pause", progress=30), config("kodi"))["ok"]
    client.rows["movie:10"]["uniqueid"] = {"tmdb": "999"}
    client.rows["movie:20"] = kodi_row(20)
    assert sink.send(event(), config("kodi"))["ok"]
    assert client.rows["movie:10"]["playcount"] == 0 and client.rows["movie:20"]["playcount"] == 1


def test_kodi_episode_show_ids_and_specials(kodi):
    cls, client, _ = kodi
    client.rows = {"show:30": kodi_row(30, "show"), "episode:20": kodi_row(20, "episode", uniqueid={"tmdb": "123"}, tvshowid=30, season=0, episode=1)}
    ev = event(media_type="episode", ids={"tmdb_show": "42", "tmdb": "42"}, season=0, number=1)
    assert cls().send(ev, config("kodi"))["ok"]
    assert client.rows["episode:20"]["playcount"] == 1 and client.rows["show:30"]["playcount"] == 0


@pytest.mark.parametrize("season", [0, 1])
@pytest.mark.parametrize("action,progress", [("start", 20), ("stop", 95)])
def test_kodi_episode_conflict_accepts_verified_show_and_coordinates(kodi, season, action, progress):
    cls, client, _ = kodi
    client.rows = {"show:30": kodi_row(30, "show"),
                   "episode:20": kodi_row(20, "episode", uniqueid={"imdb": "tt0000999", "tvdb": "456"}, tvshowid=30, season=season, episode=2)}
    ev = event(action=action, progress=progress, media_type="episode", ids={"imdb": "tt0000111", "tvdb": "456", "tmdb_show": "42"}, season=season, number=2)
    assert cls().send(ev, config("kodi"))["ok"]
    assert client.rows["episode:20"]["playcount"] == (1 if action == "stop" else 0)
    assert client.rows["show:30"]["playcount"] == 0
    assert any(c[0].startswith("VideoLibrary.Set") for c in client.calls)


@pytest.mark.parametrize("case", ["show", "season", "episode", "missing_show", "missing_coordinates", "library", "ambiguous"])
def test_kodi_episode_conflict_fallback_rejects_invalid_matches(kodi, case):
    cls, client, _ = kodi
    cfg = config("kodi")
    ids = {"imdb": "tt0000111", "tvdb": "456", "tmdb_show": "42", "tvdb_show": "43"}
    client.rows = {"show:30": kodi_row(30, "show", uniqueid={"tmdb": "42", "tvdb": "43"}),
                   "episode:20": kodi_row(20, "episode", uniqueid={"imdb": "tt0000999", "tvdb": "456"}, tvshowid=30, season=1, episode=2)}
    if case == "show":
        client.rows["show:30"]["uniqueid"]["tvdb"] = "999"
    elif case == "season":
        client.rows["episode:20"]["season"] = 2
    elif case == "episode":
        client.rows["episode:20"]["episode"] = 3
    elif case == "missing_show":
        ids = {"imdb": "tt0000111", "tvdb": "456"}
    elif case == "library":
        cfg["kodi"]["history"] = {"libraries": ["/somewhere/else"]}
    elif case == "ambiguous":
        client.rows["episode:21"] = {**copy.deepcopy(client.rows["episode:20"]), "episodeid": 21}
    ev = event(media_type="episode", ids=ids, season=None if case == "missing_coordinates" else 1, number=2)
    assert not cls().send(ev, cfg)["ok"]
    assert not any(c[0].startswith("VideoLibrary.Set") for c in client.calls)


def test_kodi_direct_episode_id_keeps_different_numbering_support(kodi):
    cls, client, _ = kodi
    client.rows = {"episode:20": kodi_row(20, "episode", uniqueid={"tmdb": "100"}, season=2, episode=3)}
    assert cls().send(event(media_type="episode", ids={"tmdb": "100"}, season=1, number=2), config("kodi"))["ok"]
    assert client.rows["episode:20"]["playcount"] == 1


def test_kodi_id_only_catalog_reused_and_isolated_by_current_profile(kodi):
    cls, client, _ = kodi
    sink = cls()
    cfg = config("kodi")
    assert sink.send(event(title=None), cfg)["ok"]
    assert not sink.send(event(title=None, ids={"tmdb": "999"}), cfg)["ok"]
    assert not sink.send(event(title=None, ids={"tmdb": "888"}), cfg)["ok"]
    assert len([c for c in client.calls if c[0] == "VideoLibrary.GetMovies"]) == 1
    client.profile = "Other"
    assert not sink.send(event(title=None, ids={"tmdb": "777"}), cfg)["ok"]
    assert len([c for c in client.calls if c[0] == "VideoLibrary.GetMovies"]) == 2


def test_kodi_completion_acknowledgements_are_profile_specific(kodi):
    cls, client, _ = kodi
    sink = cls()
    assert sink.send(event(), config("kodi"))["ok"]
    client.profile = "Other"
    client.rows["movie:10"]["playcount"] = 0
    assert sink.send(event(), config("kodi")) == {"ok": True}
    assert client.rows["movie:10"]["playcount"] == 1


@pytest.mark.parametrize("provider", ["jellyfin", "emby", "kodi"])
def test_real_adapter_construction_has_no_source_or_sync_instance_leak(provider, monkeypatch):
    monkeypatch.setenv("CW_PAIR_DST_INSTANCE", "unrelated-sync-instance")
    cfg = config(provider)
    view = build_route_cfg(cfg, {"provider": provider, "provider_instance": "P01", "sink": provider, "sink_instance": "default"})
    cls = getattr(import_module(f"providers.scrobble.{provider}.sink"), {"jellyfin": "JellyfinSink", "emby": "EmbySink", "kodi": "KodiSink"}[provider])
    selected = common.scrobble_sink_config(view, provider, "default")
    adapter = cls()._connect(selected)
    assert adapter.cfg.server == "http://default"
    if provider != "kodi":
        assert adapter.cfg.user_id == "default-user" and adapter.cfg.access_token == "default-token"
        adapter.client.session.close()


@pytest.mark.parametrize("provider", ["jellyfin", "emby"])
@pytest.mark.parametrize("source,target", [("default", "P01"), ("P01", "default"), ("default", "default"), ("P01", "P01")])
def test_media_server_webhook_management_guards(provider, source, target, monkeypatch):
    from api import scrobblerManagementAPI as api
    cfg, saved = config(provider), []
    monkeypatch.setattr(api, "load_config", lambda: cfg)
    monkeypatch.setattr(api, "_save_and_runtime", lambda request, before, after: saved.append(after) or {"ok": True})
    response = api.api_profile_webhook_save(None, {"provider": provider, "provider_instance": source,
                                                 "sinks": [provider], "sink_instances": {provider: target}})
    assert response.status_code == (400 if source == target else 200)
    assert bool(saved) is (source != target)
    if source == target:
        assert b"same_source_destination" in response.body


def test_catalog_survives_connection_renewal_but_refreshes_after_six_hours(kodi, monkeypatch):
    cls, client, _ = kodi
    current = [100.0]
    monkeypatch.setattr(common.time, "monotonic", lambda: current[0])
    sink = cls()
    cfg = config("kodi")
    ev = event(title=None)
    assert sink.send(ev, cfg)["ok"]
    current[0] += 1000
    assert sink.send(ev, cfg)["ok"]
    assert len([c for c in client.calls if c[0] == "VideoLibrary.GetMovies"]) == 1
    current[0] += 21600
    assert sink.send(ev, cfg)["ok"]
    assert len([c for c in client.calls if c[0] == "VideoLibrary.GetMovies"]) == 2
