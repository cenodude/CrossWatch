# tests/test_anilist_ratings.py
# CrossWatch test scripts
from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

import providers.scrobble.anilist.ratings as anilist_ratings
from providers.sync.anilist._ratings import plan_scores, score_raw


@pytest.mark.parametrize("rating,raw", [(8, 80), (7.5, 80), (1, 10), (10, 100), (0, 0), (None, 0), (-1, 0), ("x", 0)])
def test_score_raw_maps_plex_ten_point_to_hundred(rating: Any, raw: int) -> None:
    assert score_raw(rating) == raw


def test_movie_and_season_overwrite_only_listed_entries() -> None:
    writes, owned = plan_scores("season", [99147, 104578, 555], {99147: 0, 104578: 70}, 80)

    assert writes == {99147: 80, 104578: 80}
    assert owned == []


def test_unchanged_scores_are_not_rewritten() -> None:
    assert plan_scores("movie", [21519], {21519: 90}, 90) == ({}, [])


def test_unlisted_entries_are_never_created() -> None:
    assert plan_scores("movie", [21519], {}, 90) == ({}, [])
    assert plan_scores("show", [1, 2], {}, 90) == ({}, [])


def test_show_fills_only_unscored_entries() -> None:
    writes, owned = plan_scores("show", [16498, 99147, 104578], {16498: 70, 99147: 0, 104578: 0}, 60)

    assert writes == {99147: 60, 104578: 60}
    assert owned == [99147, 104578]


def test_show_updates_its_own_scores_but_not_manual_ones() -> None:
    record = {"entries": [99147, 104578], "score": 60}
    writes, owned = plan_scores("show", [16498, 99147, 104578], {16498: 60, 99147: 60, 104578: 90}, 70, record)

    assert writes == {99147: 70}
    assert owned == [99147]


def test_show_removal_clears_only_its_own_scores() -> None:
    record = {"entries": [99147, 104578], "score": 60}
    writes, owned = plan_scores("show", [16498, 99147, 104578], {16498: 70, 99147: 60, 104578: 90}, 0, record)

    assert writes == {99147: 0}
    assert owned == []


def test_season_removal_clears_its_entries() -> None:
    assert plan_scores("season", [99147, 104578], {99147: 80, 104578: 0}, 0) == ({99147: 0}, [])


class _Client:
    def __init__(self, listed: dict[int, int]) -> None:
        self.listed = dict(listed)
        self.saves: list[dict[str, Any]] = []

    def gql(self, query: str, variables: dict[str, Any], **_: Any) -> dict[str, Any]:
        if "SaveMediaListEntry" in query:
            self.saves.append(dict(variables))
            self.listed[variables["mediaId"]] = variables["scoreRaw"]
            return {"SaveMediaListEntry": {"id": 1}}
        media = [{"id": mid, "mediaListEntry": {"id": mid, "score": self.listed[mid]}} for mid in variables["ids"] if mid in self.listed]
        return {"Page": {"media": media}}


@pytest.fixture
def env(monkeypatch: Any, tmp_path: Path) -> dict[str, Any]:
    state: dict[str, Any] = {"client": _Client({}), "targets": {}}

    class _Module:
        def __init__(self, cfg: Any) -> None:
            self.client = state["client"]

    monkeypatch.setattr(anilist_ratings, "ANILISTModule", _Module)
    monkeypatch.setattr(anilist_ratings, "_mapping_ready", lambda cfg: bool((cfg.get("anime_mapping") or {}).get("enabled")))
    monkeypatch.setattr(anilist_ratings, "_record_path", lambda: tmp_path / "anilist_show_scores.json")
    monkeypatch.setattr(anilist_ratings, "_targets", lambda cfg, level, md, ids, sids: list(state["targets"].get(level, [])))
    return state


def _cfg(mapping: bool = True) -> dict[str, Any]:
    return {"anilist": {"access_token": "token"}, "anime_mapping": {"enabled": mapping}}


AOT = {"tvdb": 267440}


def test_send_skips_episode_ratings(env: dict[str, Any]) -> None:
    assert anilist_ratings.send_plex_rating(_cfg(), "default", "episode", {}, AOT, AOT, 8)["reason"] == "episode_not_supported"


def test_send_requires_connection_and_mapping(env: dict[str, Any]) -> None:
    assert anilist_ratings.send_plex_rating({"anime_mapping": {"enabled": True}}, "default", "movie", {}, {}, None, 8)["error"] == "not_configured"
    assert anilist_ratings.send_plex_rating(_cfg(False), "default", "movie", {}, {}, None, 8)["error"] == "anime_mapping_unavailable"


def test_send_skips_non_anime(env: dict[str, Any]) -> None:
    res = anilist_ratings.send_plex_rating(_cfg(), "default", "show", {"title": "Breaking Bad"}, {"tvdb": 81189}, {"tvdb": 81189}, 8)

    assert res == {"ok": True, "skipped": True, "reason": "not_mapped"}


def test_send_reports_entries_not_on_list(env: dict[str, Any]) -> None:
    env["targets"] = {"movie": [21519]}

    res = anilist_ratings.send_plex_rating(_cfg(), "default", "movie", {"title": "Your Name."}, {"tmdb": 372058}, None, 9)

    assert res["reason"] == "not_on_list"
    assert env["client"].saves == []


def test_show_rating_round_trip_keeps_manual_scores(env: dict[str, Any]) -> None:
    env["client"] = _Client({16498: 70, 99147: 0, 104578: 0})
    env["targets"] = {"show": [16498, 99147, 104578], "season": [99147, 104578]}
    send = anilist_ratings.send_plex_rating

    send(_cfg(), "default", "show", {"title": "Attack on Titan"}, AOT, AOT, 6)
    assert env["client"].listed == {16498: 70, 99147: 60, 104578: 60}

    send(_cfg(), "default", "season", {"index": 3, "parentTitle": "Attack on Titan"}, {}, AOT, 9)
    send(_cfg(), "default", "show", {"title": "Attack on Titan"}, AOT, AOT, 7)
    assert env["client"].listed == {16498: 70, 99147: 90, 104578: 90}

    send(_cfg(), "default", "show", {"title": "Attack on Titan"}, AOT, AOT, 0)
    assert env["client"].listed == {16498: 70, 99147: 90, 104578: 90}
    assert anilist_ratings._read_records() == {}


def test_show_record_is_scoped_per_profile(env: dict[str, Any]) -> None:
    env["client"] = _Client({99147: 0})
    env["targets"] = {"show": [99147]}

    cfg = _cfg()
    cfg["anilist"]["instances"] = {"P01": {"access_token": "token-p01"}}

    res = anilist_ratings.send_plex_rating(cfg, "P01", "show", {"title": "Attack on Titan"}, AOT, AOT, 6)

    assert res["ok"] is True

    assert anilist_ratings._read_records() == {"P01|tvdb:267440": {"entries": [99147], "score": 60}}


def test_plex_webhook_routes_season_ratings_only_to_anilist(monkeypatch: Any) -> None:
    from providers.webhooks import plex

    calls: list[tuple[Any, ...]] = []
    monkeypatch.setattr(plex, "_save_config", lambda _cfg: None)
    monkeypatch.setattr(plex, "_post_trakt", lambda *a, **k: pytest.fail("season rating must not reach Trakt"))
    monkeypatch.setattr(anilist_ratings, "send_plex_rating", lambda *a: calls.append(a) or {"ok": True, "written": {99147: 80}})
    plex._LAST_RATING_BY_ACC.clear()
    cfg = {
        "scrobble": {
            "enabled": True,
            "sources": {"webhook": True},
            "webhook": {"sinks": ["anilist", "trakt"], "plex_anilist_ratings": True, "plex_trakt_ratings": True},
        },
        "anilist": {"access_token": "token"},
        "trakt": {"access_token": "token"},
    }
    payload = {
        "event": "media.rate",
        "Account": {"title": "test-user"},
        "Metadata": {
            "type": "season",
            "index": 3,
            "parentTitle": "Attack on Titan",
            "parentGuid": "com.plexapp.agents.thetvdb://267440?lang=en",
            "ratingKey": "rk-s3",
            "userRating": 8,
        },
    }

    result = plex.process_webhook(payload, {}, cfg=cfg)

    assert result["ok"] is True
    assert result["anilist"] == {"ok": True, "written": {99147: 80}}
    assert "trakt" not in result
    _, instance, media_type, md, ids, show_ids, rating = calls[0]
    assert (instance, media_type, show_ids, rating) == ("default", "season", {"tvdb": 267440}, 8.0)


def test_plex_webhook_ignores_season_ratings_without_anilist(monkeypatch: Any) -> None:
    from providers.webhooks import plex

    monkeypatch.setattr(plex, "_save_config", lambda _cfg: None)
    monkeypatch.setattr(plex, "_post_trakt", lambda *a, **k: pytest.fail("season rating must not reach Trakt"))
    plex._LAST_RATING_BY_ACC.clear()
    cfg = {"scrobble": {"enabled": True, "sources": {"webhook": True}, "webhook": {"sinks": ["trakt"], "plex_trakt_ratings": True}}, "trakt": {"access_token": "t"}}
    payload = {"event": "media.rate", "Account": {"title": "u"}, "Metadata": {"type": "season", "index": 3, "parentGuid": "com.plexapp.agents.thetvdb://267440", "ratingKey": "rk", "userRating": 8}}

    assert plex.process_webhook(payload, {}, cfg=cfg) == {"ok": True, "ignored": True}


def test_plex_webhook_sends_movie_ratings_to_anilist(monkeypatch: Any) -> None:
    from providers.webhooks import plex

    calls: list[tuple[Any, ...]] = []
    monkeypatch.setattr(plex, "_save_config", lambda _cfg: None)
    monkeypatch.setattr(anilist_ratings, "send_plex_rating", lambda *a: calls.append(a) or {"ok": True})
    plex._LAST_RATING_BY_ACC.clear()
    cfg = {"scrobble": {"enabled": True, "sources": {"webhook": True}, "webhook": {"sinks": ["anilist"], "plex_anilist_ratings": True}}, "anilist": {"access_token": "t"}}
    payload = {"event": "media.rate", "Account": {"title": "u"}, "Metadata": {"type": "movie", "title": "Your Name.", "ratingKey": "rk-m", "userRating": 9, "Guid": [{"id": "tmdb://372058"}]}}

    result = plex.process_webhook(payload, {}, cfg=cfg)

    assert result["anilist"] == {"ok": True}
    assert calls[0][2] == "movie" and calls[0][4]["tmdb"] == 372058 and calls[0][5] is None


def _anime_only_rows() -> list[dict[str, Any]]:
    return [
        {"type": "show", "title": "Shingeki no Kyojin", "ids": {"simkl": "39687", "mal": "16498", "anilist": "16498"}},
        {"type": "movie", "title": "Kimi no Na wa.", "ids": {"mal": "32281"}},
        {"type": "show", "title": "Dark", "ids": {"tmdb": "70523", "simkl": "715234"}},
    ]


def test_anime_only_plan_keeps_only_items_with_an_anilist_identity(monkeypatch: Any) -> None:
    from cw_platform.anime_mapping import service

    monkeypatch.setattr(service.AnimeMappingService, "ready", lambda self: True)
    kept, skipped = service.anime_only_adds(_anime_only_rows(), {}, {"use_anime_mapping": True, "anime_only_sync": True})

    assert [row["title"] for row in kept] == ["Shingeki no Kyojin", "Kimi no Na wa."]
    assert skipped == 1


@pytest.mark.parametrize("ready,options", [
    (True, {"use_anime_mapping": True, "anime_only_sync": False}),
    (True, {}),
    (False, {"use_anime_mapping": True, "anime_only_sync": True}),
])
def test_anime_only_plan_filter_is_off_without_the_option_or_the_mapping_data(monkeypatch: Any, ready: bool, options: dict[str, Any]) -> None:
    from cw_platform.anime_mapping import service

    monkeypatch.setattr(service.AnimeMappingService, "ready", lambda self: ready)
    kept, skipped = service.anime_only_adds(_anime_only_rows(), {}, options)

    assert len(kept) == 3 and skipped == 0
