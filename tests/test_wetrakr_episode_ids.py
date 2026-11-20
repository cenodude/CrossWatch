# tests/test_wetrakr_episode_ids.py
# CrossWatch - WeTrakr episode writes and matching by episode ids
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import pytest

from providers.sync.wetrakr import _common as common
from test_wetrakr_sync import WHEN, Response, env

LATER = "2026-09-15T21:10:03Z"
JEOPARDY = {"id": 1393843, "type": "show", "title": "Jeopardy!", "ids": {"tmdb": 2912, "imdb": "tt0159881", "tvdb": 77075}}
E45 = {"id": 7091841, "type": "episode", "title": "Show #9660", "ids": {"tmdb": 8086031, "imdb": "tt50921222", "tvdb": 12069169},
       "season_number": 43, "number": 45, "show": JEOPARDY}
E44 = {"id": 7091840, "type": "episode", "title": "Show #9659", "ids": {"tmdb": 8086030, "imdb": "tt50920944", "tvdb": 12069168},
       "season_number": 43, "number": 44, "show": JEOPARDY}
E44_SHARED_TVDB = {**E44, "ids": {**E44["ids"], "tvdb": 12069169}}


def trakt_episode(tvdb="12069169", episode=192, when=WHEN, **ids):
    return {"type": "episode", "title": "Jeopardy!", "ids": {"tvdb": tvdb, "trakt": "14617148", **ids},
            "show_ids": {"tmdb": "2912", "imdb": "tt0159881", "tvdb": "77075", "trakt": "1418"},
            "season": 2026, "episode": episode, "watched_at": when}


def coordinate_episode(episode):
    return {"type": "episode", "title": "Jeopardy!", "ids": {}, "show_ids": {"tmdb": "2912"}, "season": 43, "episode": episode}


def daily_server(env, catalog=(E45, E44)):
    def resolve(ids):
        for source in common.EPISODE_ID_SOURCES:
            if source in ids:
                media = next((e for e in catalog if str(e["ids"].get(source)) == str(ids[source])), None)
                if media is not None:
                    return media
        return None

    def hook(method, path, kwargs):
        if method != "POST" or not path.startswith("/sync/tracking"):
            return None
        env.server.bump()
        missing = []
        for row in kwargs["json"].get("episodes", []):
            media = resolve(row["ids"])
            if media is None:
                missing.append(row)
            elif path.endswith("/remove/all"):
                env.server.history = [play for play in env.server.history if play.get("episode", {}).get("id") != media["id"]]
            else:
                env.server.play(media, row["tracked_at"])
        return Response({"added": {"total": 1}, "notFound": {"episodes": missing} if missing else {}})

    env.server.hook = hook


def posts(env, path="/sync/tracking"):
    return [kwargs["json"] for method, called, kwargs in env.server.calls if method == "POST" and called == path]


def replay(env, monkeypatch, response, write):
    from cw_platform.interactive_reads import ReviewReads, use_review_reads

    reads = ReviewReads()
    try:
        with use_review_reads(reads, collecting=True):
            env.adapter.build_index("history")
        original = common.request

        def request(adapter, method, path, **kwargs):
            if method == "POST":
                return Response(response)
            return original(adapter, method, path, **kwargs)

        monkeypatch.setattr(common, "request", request)
        with use_review_reads(reads):
            return write()
    finally:
        reads.close()


def test_daily_episode_is_written_by_episode_id_and_matched_on_reread(env):
    daily_server(env)
    result = env.adapter.add("history", [trakt_episode()])
    assert result["ok"] and len(result["confirmed_keys"]) == 1
    assert posts(env) == [{"episodes": [{"ids": {"tvdb": 12069169}, "status": "watched", "tracked_at": WHEN}]}]
    env.server.calls.clear()
    assert env.adapter.add("history", [trakt_episode()])["ok"]
    assert not posts(env)
    assert env.adapter.remove("history", [trakt_episode()])["ok"]
    assert posts(env, "/sync/tracking/remove/all") == [
        {"episodes": [{"ids": {"tmdb": 8086031, "imdb": "tt50921222", "tvdb": 12069169}, "status": "watched"}]}]
    assert not env.server.history


def test_unknown_episode_id_is_not_verified(env):
    daily_server(env)
    result = env.adapter.add("history", [trakt_episode(tvdb="999999991")])
    assert not result["ok"]
    assert result["unresolved"][0]["reason"] == "write_not_verified"


@pytest.mark.parametrize("remove", [False, True])
def test_conflicting_episode_ids_are_unresolved_per_item(env, remove):
    daily_server(env)
    env.server.play(E45)
    env.server.play(E44)
    conflict = {**trakt_episode(tvdb="12069168", tmdb="8086031"), "season": 43, "episode": 44}
    other = trakt_episode()
    result = (env.adapter.remove if remove else env.adapter.add)("history", [conflict, other])
    key = common.item_key(env.adapter, "history", conflict)
    assert {row["key"]: row["reason"] for row in result["unresolved"]} == {key: "ambiguous_media_identity"}
    assert len(result["confirmed_keys"]) == 1
    sent = [row for body in posts(env, "/sync/tracking/remove/all" if remove else "/sync/tracking") for row in body["episodes"]]
    assert all(row["ids"].get("tvdb") != 12069168 for row in sent)


def test_shared_episode_id_without_a_deciding_id_is_ambiguous(env):
    env.server.play(E45)
    env.server.play(E44_SHARED_TVDB)
    with pytest.raises(common.WeTrakrSyncError, match="ambiguous_media_identity"):
        common.matching(env.adapter, "history", {"source": trakt_episode()}, env.adapter.build_index("history"))


def test_event_matching_resolves_the_episode_before_the_timestamp(env):
    env.adapter.config["_cw_history_rewatches"] = True
    env.server.play(E44_SHARED_TVDB, WHEN)
    env.server.play(E45, LATER)
    index = env.adapter.build_index("history")
    source = trakt_episode(tmdb="8086031")
    matches = common.matching(env.adapter, "history", {"source": source}, index)
    assert index[matches["source"]]["ids"]["wetrakr"] == str(E45["id"])
    with pytest.raises(common.WeTrakrSyncError, match="ambiguous_media_identity"):
        common.matching(env.adapter, "history", {"source": trakt_episode()}, index)


def test_identity_tokens_stay_coordinate_based_for_playlists_and_scrobbles():
    tokens = common.identity_tokens(trakt_episode())
    assert tokens and all(token.endswith(":s2026e192") for token in tokens)
    assert common.episode_tokens(trakt_episode(tmdb="8086031")) == {"episode:tvdb:12069169", "episode:tmdb:8086031"}


def test_episode_id_match_wins_over_conflicting_coordinates(env):
    source = {"type": "episode", "ids": {"tvdb": "12069169"}, "show_ids": {"tmdb": "2912"}, "season": 43, "episode": 44}
    dest = {"e44": common.media_item(E44, "episode"), "e45": common.media_item(E45, "episode")}
    assert common.matching(env.adapter, "history", {"source": source}, dest) == {"source": "e45"}


def test_episode_ids_that_repeat_show_ids_keep_coordinate_payload():
    source = {"type": "episode", "ids": {"tmdb": "2912"}, "show_ids": {"tmdb": "2912"}, "season": 43, "episode": 45, "watched_at": WHEN}
    group, row = common.payload_item(source, "history")
    assert group == "shows"
    assert row["ids"] == {"tmdb": 2912}
    assert row["seasons"][0]["episodes"][0]["number"] == 45
    assert common.episode_tokens(source) == set()


def test_interactive_write_accepts_already_added_and_rejects_other_errors(env, monkeypatch):
    missing, existing, failed = trakt_episode(), trakt_episode("12069168", 191), trakt_episode("11912832", 1)
    keys = [common.item_key(env.adapter, "history", item) for item in (missing, existing, failed)]
    result = replay(env, monkeypatch, {
        "notFound": {"episodes": [{"ids": {"tvdb": 12069169}, "status": "watched", "tracked_at": WHEN}]},
        "errored": {"episodes": [
            {"ids": {"tvdb": 12069168}, "status": "watched", "tracked_at": WHEN, "error": common.ALREADY_ADDED},
            {"ids": {"tvdb": 11912832}, "status": "watched", "tracked_at": WHEN, "error": "Something failed"}]}},
        lambda: env.adapter.add("history", [missing, existing, failed]))
    assert result["accepted_keys"] == [keys[1]]
    assert {row["key"]: row["reason"] for row in result["unresolved"]} == {
        keys[0]: "write_partially_rejected", keys[2]: "write_partially_rejected"}


def test_interactive_removal_rejection_matches_the_submitted_row(env, monkeypatch):
    env.server.play(E45)
    env.server.play(E44)
    kept, gone = coordinate_episode(44), coordinate_episode(45)
    keys = [common.item_key(env.adapter, "history", item) for item in (kept, gone)]
    result = replay(env, monkeypatch, {"notFound": {"episodes": [{"ids": {"tmdb": 8086030, "imdb": "tt50920944", "tvdb": 12069168}, "status": "watched"}]}},
                    lambda: env.adapter.remove("history", [kept, gone]))
    assert result["confirmed_keys"] == [keys[1]]
    assert [(row["key"], row["reason"]) for row in result["unresolved"]] == [(keys[0], "write_partially_rejected")]


@pytest.mark.parametrize("native", [False, True])
def test_retained_rewatches_share_one_identity_in_later_replay_writes(env, monkeypatch, native):
    env.adapter.config["_cw_history_rewatches"] = True
    if native:
        env.server.play(E45, "2026-09-01T12:00:00Z")
    plays = [trakt_episode(when=WHEN), trakt_episode(when="2026-09-16T21:09:53Z")]
    unrelated = trakt_episode("12069168", 191)

    def writes():
        return env.adapter.add("history", plays), env.adapter.add("history", [unrelated]), env.adapter.add("history", plays)

    first, second, repeat = replay(env, monkeypatch, {"added": {"total": 1}}, writes)
    assert first["ok"] and len(first["accepted_keys"]) == 2
    assert second["ok"] and second["accepted_keys"] == [common.item_key(env.adapter, "history", unrelated)]
    assert repeat["ok"] and repeat["accepted_keys"] == []


def test_interactive_rewatch_rejection_keeps_the_other_play(env, monkeypatch):
    env.adapter.config["_cw_history_rewatches"] = True
    first, second = trakt_episode(when=WHEN), trakt_episode(when=LATER)
    keys = [common.item_key(env.adapter, "history", item) for item in (first, second)]
    assert keys[0] != keys[1]
    result = replay(env, monkeypatch, {"errored": {"episodes": [
        {"ids": {"tvdb": 12069169}, "status": "watched", "tracked_at": LATER, "error": "Something failed"}]}},
        lambda: env.adapter.add("history", [first, second]))
    assert result["accepted_keys"] == [keys[0]]
    assert [(row["key"], row["reason"]) for row in result["unresolved"]] == [(keys[1], "write_partially_rejected")]
