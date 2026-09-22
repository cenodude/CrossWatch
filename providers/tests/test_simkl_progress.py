from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any, Mapping
from unittest.mock import Mock

import pytest


class Response:
    def __init__(self, status_code: int, payload: Any = None) -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = "" if payload is None else json.dumps(payload)
        self.headers: dict[str, str] = {}

    def json(self) -> Any:
        return self._payload


class FakeClient:
    BASE = "https://api.simkl.com"

    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.rows = list(rows or [])
        self.calls: list[tuple[str, str, dict[str, Any] | None]] = []
        self.next_id = 1000
        self.session: Any = None

    def _request(self, method: str, url: str, **kwargs: Any) -> Response:
        if method == "GET":
            self.calls.append(("GET", url, kwargs.get("params")))
            return Response(200, list(self.rows))
        if method == "POST":
            body = dict(kwargs.get("json") or {})
            self.calls.append(("POST", url, body))
            self.next_id += 1
            if "movie" in body:
                movie = dict(body["movie"])
                self.rows = [row for row in self.rows if not (row.get("type") == "movie" and (row.get("movie") or {}).get("ids") == movie.get("ids"))]
                self.rows.append({"id": self.next_id, "type": "movie", "progress": body["progress"], "paused_at": "2026-07-25T12:00:00Z", "movie": movie})
            elif "show" in body:
                show = dict(body["show"])
                episode = dict(body["episode"])
                self.rows.append({"id": self.next_id, "type": "episode", "progress": body["progress"], "paused_at": "2026-07-25T12:00:00Z", "show": show, "episode": episode})
            elif "anime" in body:
                anime = dict(body["anime"])
                episode = dict(body["episode"])
                self.rows.append({"id": self.next_id, "type": "episode", "progress": body["progress"], "paused_at": "2026-07-25T12:00:00Z", "anime": anime, "episode": episode})
            return Response(201, {"id": self.next_id, "action": "pause", "progress": body.get("progress")})
        if method == "DELETE":
            self.calls.append(("DELETE", url, None))
            playback_id = int(url.rsplit("/", 1)[-1])
            self.rows = [row for row in self.rows if int(row.get("id") or 0) != playback_id]
            return Response(204)
        return Response(405, {})


class FakeAdapter:
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.client = FakeClient(rows)
        self.cfg = type("Cfg", (), {"progress_limit": 10000, "date_from": ""})()
        self.raw_cfg: dict[str, Any] = {}


def movie_row(progress: float = 20.0) -> dict[str, Any]:
    return {
        "id": 42,
        "type": "movie",
        "progress": progress,
        "paused_at": "2026-07-25T10:00:00Z",
        "movie": {
            "title": "Fight Club",
            "year": 1999,
            "runtime": 10,
            "ids": {"simkl": 10, "slug": "fight-club", "imdb": "tt0137523", "tmdb": 550},
        },
    }


def episode_row() -> dict[str, Any]:
    return {
        "id": 43,
        "type": "episode",
        "progress": 50,
        "paused_at": "2026-07-25T10:00:00Z",
        "episode": {"season": 1, "number": 3, "title": "Holly Jolly", "runtime": 40},
        "show": {"title": "Stranger Things", "year": 2016, "ids": {"simkl": 39687, "imdb": "tt4574334", "tvdb": 305288}},
    }


def test_build_index_maps_official_playback_rows() -> None:
    from providers.sync.simkl import _progress

    index = _progress.build_index(FakeAdapter([movie_row(), episode_row()]))

    assert index["tmdb:550"]["progress_ms"] == 120_000
    assert index["tmdb:550"]["duration_ms"] == 600_000
    assert index["tmdb:550"]["progress_percent"] == 20.0
    assert index["tmdb:550"]["_simkl_playback_id"] == 42
    assert index["imdb:tt4574334#s01e03"]["progress_ms"] == 1_200_000


def test_add_movie_uses_documented_scrobble_pause_payload() -> None:
    from providers.sync.simkl import _progress

    adapter = FakeAdapter([])
    item = {
        "type": "movie",
        "title": "Fight Club",
        "year": 1999,
        "ids": {"tmdb": "550", "slug": "fight-club"},
        "progress_ms": 120_000,
        "duration_ms": 600_000,
        "progress_at": "2026-07-25T11:00:00Z",
    }

    result = _progress.add(adapter, [item])

    assert result["ok"] is True
    assert result["confirmed_keys"] == ["tmdb:550"]
    post = [call for call in adapter.client.calls if call[0] == "POST"][0]
    assert post[1] == "https://api.simkl.com/scrobble/pause"
    assert post[2] == {"progress": 20.0, "movie": {"ids": {"tmdb": 550}, "title": "Fight Club", "year": 1999}}


def test_add_episode_uses_show_and_episode_shape() -> None:
    from providers.sync.simkl import _progress

    adapter = FakeAdapter([])
    item = {
        "type": "episode",
        "series_title": "Stranger Things",
        "show_ids": {"tvdb": "305288", "slug": "stranger-things"},
        "season": 1,
        "episode": 3,
        "progress_ms": 1_200_000,
        "duration_ms": 2_400_000,
        "progress_at": "2026-07-25T11:00:00Z",
    }

    result = _progress.add(adapter, [item])

    assert result["ok"] is True
    post = [call for call in adapter.client.calls if call[0] == "POST"][0]
    assert post[2] == {
        "progress": 50.0,
        "show": {"ids": {"tvdb": "305288"}, "title": "Stranger Things"},
        "episode": {"season": 1, "number": 3},
    }


def test_add_episode_prefers_show_coordinate_when_ids_duplicate_show_id() -> None:
    from providers.sync.simkl import _progress

    adapter = FakeAdapter([])
    item = {
        "type": "episode",
        "series_title": "House of the Dragon",
        "ids": {"tmdb": "94997"},
        "show_ids": {"tmdb": "94997"},
        "season": 2,
        "episode": 7,
        "progress_percent": 21.0,
        "progress_at": "2026-07-25T20:41:00Z",
    }

    result = _progress.add(adapter, [item])

    assert result["ok"] is True
    assert result["confirmed_keys"] == ["tmdb:94997#s02e07"]
    post = [call for call in adapter.client.calls if call[0] == "POST"][0]
    assert post[2] == {
        "progress": 21.0,
        "show": {"ids": {"tmdb": 94997}, "title": "House of the Dragon"},
        "episode": {"season": 2, "number": 7},
    }


def test_add_episode_requires_documented_parent_or_episode_identity() -> None:
    from providers.sync.simkl import _progress

    adapter = FakeAdapter([])
    item = {
        "type": "episode",
        "show_ids": {"slug": "stranger-things"},
        "season": 1,
        "episode": 3,
        "progress_ms": 1_200_000,
        "duration_ms": 2_400_000,
        "progress_at": "2026-07-25T11:00:00Z",
    }

    result = _progress.add(adapter, [item])

    assert result["ok"] is False
    assert result["attempted"] == 0
    assert result["unresolved"][0]["reason"] == "simkl_parent_id_missing"
    assert not any(call[0] == "POST" for call in adapter.client.calls)


def test_remove_deletes_current_playback_id() -> None:
    from providers.sync.simkl import _progress

    adapter = FakeAdapter([movie_row()])

    result = _progress.remove(adapter, [{"type": "movie", "ids": {"tmdb": "550"}}])

    assert result["ok"] is True
    assert result["confirmed_keys"] == ["tmdb:550"]
    assert ("DELETE", "https://api.simkl.com/sync/playback/42", None) in adapter.client.calls


def test_simkl_module_exposes_progress_feature() -> None:
    import providers.sync._mod_SIMKL as simkl_mod

    assert simkl_mod.OPS.features()["progress"] is True
    assert simkl_mod.OPS.capabilities()["progress"]["upsert"] is True
    assert simkl_mod.OPS.capabilities()["progress"]["remove"] is True
    assert simkl_mod.OPS.capabilities()["progress"]["completion_policy"]["progress_write"]["mode"] == "none"
    assert simkl_mod.OPS.capabilities()["progress"]["completion_policy"]["stop_scrobble"]["marks_watched_percent"] == 80


@pytest.fixture
def anime_progress(tmp_path, monkeypatch):
    from cw_platform.anime_mapping.episodes import Resolution
    from providers.sync.simkl import _progress as progress

    monkeypatch.setenv("CONFIG_BASE", str(tmp_path))
    monkeypatch.setenv("CW_PAIR_KEY", "cw2_progress_anime_test")
    monkeypatch.setattr(progress, "_activities_latest", lambda adapter: None)
    monkeypatch.setattr(progress, "state_file", lambda name: tmp_path / name)
    monkeypatch.setattr(progress.anime_mapping, "state_file", lambda name: tmp_path / name)
    monkeypatch.setattr(progress, "adapter_headers", lambda adapter: {})
    monkeypatch.setattr(progress.anime_mapping, "_offline_simkl_id", lambda *a: None)
    resolver = Mock(side_effect=lambda item, **kw: Resolution(
        absolute=1088 + int(item["episode"]), namespace="anidb", target_id="69", basis="anibridge_absolute", entry="tmdb_direct",
    ) if str((item.get("show_ids") or {}).get("tmdb")) == "37854" else None)
    monkeypatch.setattr(progress.anime_mapping, "resolve_absolute", resolver)
    adapter = FakeAdapter()
    adapter.raw_cfg = {"anime_mapping": {"enabled": True, "features": ["watchlist", "ratings"]},
                       "_cw_pair_feature_options": {"feature": "progress", "use_anime_mapping": True}}

    def get(url, **kwargs):
        if url == progress.anime_mapping.URL_REDIRECT:
            response = Response(301, {})
            response.headers["Location"] = "https://simkl.com/anime/38636/one-piece/"
            return response
        if url.endswith("/anime/episodes/38636"):
            return Response(200, [{"episode": 1086, "tvdb": {"season": 22, "episode": 1}},
                                  {"episode": 1089, "tvdb": {"season": 22, "episode": 4}}])
        raise AssertionError(f"Unexpected mapping request: {url}")

    adapter.client.session = SimpleNamespace(get=Mock(side_effect=get))
    item = {"type": "episode", "show_ids": {"tmdb": "37854"}, "season": 22, "episode": 1,
            "progress_percent": 37.5, "progress_at": "2026-07-25T11:00:00Z"}
    return progress, adapter, item, resolver


@pytest.mark.parametrize("offline", [False, True])
def test_anime_progress_uses_local_numbering_and_verifies_native_readback(anime_progress, monkeypatch, offline):
    progress, adapter, item, _ = anime_progress
    if offline:
        monkeypatch.setattr(progress.anime_mapping, "_offline_simkl_id", lambda *a: "38636")
    result = progress.add(adapter, [item])
    assert result["ok"] is True
    assert result["confirmed_keys"] == ["tmdb:37854#s22e01"]
    posts = [call for call in adapter.client.calls if call[0] == "POST"]
    assert len(posts) == 1
    assert posts[0][2] == {"progress": 37.5, "anime": {"ids": {"simkl": 38636}}, "episode": {"number": 1089}}
    redirects = [call for call in adapter.client.session.get.call_args_list if call.args[0] == progress.anime_mapping.URL_REDIRECT]
    assert len(redirects) == (0 if offline else 1)
    repeated = progress.add(adapter, [item])
    assert repeated["ok"] is True
    assert repeated["skipped"] == 1
    assert len([call for call in adapter.client.calls if call[0] == "POST"]) == 1
    assert len(adapter.client.session.get.call_args_list) == (1 if offline else 2)
    removed = progress.remove(adapter, [item])
    assert removed["ok"] is True
    assert removed["confirmed_keys"] == ["tmdb:37854#s22e01"]
    assert not adapter.client.rows
    assert len(adapter.client.session.get.call_args_list) == (1 if offline else 2)


@pytest.mark.parametrize("disabled_by", ["pair", "global", "history_only", "missing_pair_option", "no_runtime_options"])
def test_progress_mapping_is_independent_and_opt_in(anime_progress, disabled_by):
    progress, adapter, item, resolver = anime_progress
    if disabled_by == "global":
        adapter.raw_cfg["anime_mapping"]["enabled"] = False
    elif disabled_by == "history_only":
        adapter.raw_cfg["_cw_pair_feature_options"]["feature"] = "history"
    elif disabled_by == "missing_pair_option":
        adapter.raw_cfg["_cw_pair_feature_options"].pop("use_anime_mapping")
    elif disabled_by == "no_runtime_options":
        adapter.raw_cfg.pop("_cw_pair_feature_options")
        adapter.raw_cfg["anime_mapping"]["features"].append("progress")
    else:
        adapter.raw_cfg["_cw_pair_feature_options"]["use_anime_mapping"] = False
    result = progress.add(adapter, [item])
    assert result["ok"] is True
    body = next(call[2] for call in adapter.client.calls if call[0] == "POST")
    assert body["episode"] == {"season": 22, "number": 1}
    assert body["show"]["ids"] == {"tmdb": 37854}
    resolver.assert_not_called()
    adapter.client.session.get.assert_not_called()


def test_progress_native_identity_works_without_mapping(anime_progress):
    progress, adapter, item, resolver = anime_progress
    adapter.raw_cfg["_cw_pair_feature_options"]["use_anime_mapping"] = False
    item.update(show_ids={"simkl": "38636"}, _simkl_episode_number=1089)
    item.pop("season")
    assert progress.add(adapter, [item])["ok"] is True
    body = next(call[2] for call in adapter.client.calls if call[0] == "POST")
    assert body["episode"] == {"number": 1089}
    assert body["anime"]["ids"] == {"simkl": 38636}
    assert progress.remove(adapter, [item])["ok"] is True
    resolver.assert_not_called()
    adapter.client.session.get.assert_not_called()


def test_progress_mapping_does_not_probe_ordinary_tv(anime_progress):
    progress, adapter, item, _ = anime_progress
    item["show_ids"] = {"tvdb": "161511"}
    assert progress.add(adapter, [item])["ok"] is True
    adapter.client.session.get.assert_not_called()


def test_progress_write_verification_refreshes_unchanged_activity_cache(anime_progress, monkeypatch):
    progress, adapter, item, _ = anime_progress
    monkeypatch.setattr(progress, "_activities_latest", lambda adapter: "2026-07-25T10:00:00Z")
    monkeypatch.setattr(progress, "get_watermark", lambda feature: "2026-07-25T10:00:00Z")
    monkeypatch.setattr(progress, "update_watermark_if_new", lambda *args: None)
    assert progress.add(adapter, [item])["ok"] is True
    assert progress.remove(adapter, [item])["ok"] is True
    assert len([call for call in adapter.client.calls if call[0] == "GET"]) == 3


@pytest.mark.parametrize("failure", ["identity", "episode"])
def test_unresolved_anime_progress_does_not_write_source_numbering(anime_progress, failure):
    progress, adapter, item, _ = anime_progress
    if failure == "identity":
        adapter.client.session.get.return_value = Response(404, {})
        adapter.client.session.get.side_effect = None
    else:
        original = adapter.client.session.get.side_effect
        adapter.client.session.get.side_effect = lambda url, **kw: Response(200, []) if "/anime/episodes/" in url else original(url, **kw)
    result = progress.add(adapter, [item])
    assert result["ok"] is False
    assert result["attempted"] == 0
    assert not any(call[0] == "POST" for call in adapter.client.calls)
    assert result["unresolved"][0]["reason"] == ("simkl_anime_id_unresolved" if failure == "identity" else "simkl_anime_episode_unmapped")


def test_anime_playback_rows_preserve_distinct_native_episode_numbers():
    from providers.sync.simkl import _progress as progress
    rows = [{"id": n, "progress": 25, "anime": {"ids": {"simkl": 38636}}, "episode": {"number": n}} for n in (1089, 1090)]
    parsed = [progress._item_from_row(row) for row in rows]
    assert [row[0] for row in parsed] == ["simkl:38636#s01e1089", "simkl:38636#s01e1090"]
    assert [row[1]["_simkl_episode_number"] for row in parsed] == [1089, 1090]


def test_progress_mapping_option_survives_api_normalization():
    from api.syncAPI import _normalize_features
    from cw_platform.anime_mapping.service import anime_mapping_pair_feature_options
    features = _normalize_features({"progress": {"enable": True, "use_anime_mapping": True}})
    assert features["progress"]["use_anime_mapping"] is True
    cfg = {"anime_mapping": {"enabled": True, "features": ["progress"]}}
    assert not anime_mapping_pair_feature_options(cfg, {}, "progress", "PLEX", "SIMKL")["use_anime_mapping"]
    assert anime_mapping_pair_feature_options(cfg, features["progress"], "progress", "PLEX", "SIMKL")["use_anime_mapping"]
