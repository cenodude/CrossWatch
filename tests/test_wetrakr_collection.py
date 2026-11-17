# tests/test_wetrakr_collection.py
# CrossWatch - WeTrakr My Library sync, copies, media info and CW tracker copy merge
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy

import pytest

from cw_platform.collection_media import copies_differ, media_update
from cw_platform.id_map import minimal
from providers.sync._mod_WETRAKR import OPS
from providers.sync.wetrakr import _collection as coll
from test_wetrakr_sync import EPISODE, MOVIE, Response, env

assert env

PAGE = {"X-Pagination-Page": "1", "X-Pagination-Page-Count": "1"}


def _page(rows):
    return Response(rows, headers={**PAGE, "X-Pagination-Item-Count": str(len(rows))})


def _wt_copy(copy_id, *, key=None, media_type="bluray", resolution="hd_1080p", ownership="owned", **extra):
    row = {"id": copy_id, "target": "movie", "media_id": MOVIE["id"], "ownership": ownership,
           "metadata": {"media_type": media_type, "resolution": resolution, "3d": False},
           "collected_at": "2026-10-01T10:00:00.000Z", "collected_at_unknown": False, "created_at": "2026-10-01T10:00:00.000Z"}
    if key:
        row["copy_key"] = key
    row.update(extra)
    return row


class Library:
    def __init__(self, server):
        self.server = server
        self.movies = []
        self.episodes = []
        self.posts = []
        self.response = None
        self.status = 200
        server.hook = self

    def __call__(self, method, path, kwargs):
        if not path.startswith("/sync/library"):
            return None
        if method == "GET":
            if path == "/sync/library/movies":
                assert kwargs["params"]["extended"] == "metadata"
                return _page(copy.deepcopy(self.movies))
            if path == "/sync/library/episodes":
                return _page(copy.deepcopy(self.episodes))
        self.server.bump()
        self.posts.append((path, copy.deepcopy(kwargs["json"])))
        if self.status != 200:
            return Response({"error": "PLAN_LIMIT_REACHED"}, status=self.status)
        if path.endswith("/remove"):
            return Response({"deleted": {"movies": 0, "episodes": 0, "copies": 0}})
        body = {"added": {"movies": 1, "episodes": 0, "copies": 1}, "updated": {"movies": 0, "episodes": 0, "copies": 0},
                "existing": {"movies": 0, "episodes": 0, "copies": 0}, "copies": []}
        body.update(self.response or {})
        return Response(body)


@pytest.fixture
def lib(env):
    return Library(env.server)


def _movie_row(copies, metadata=None):
    return {**MOVIE, "library": {"collected_at": "2026-10-01T10:00:00.000Z", "collected_at_unknown": False,
                                 "metadata": metadata or copies[0]["metadata"], "copies": copies}}


def _episode_row(copy_id, *, key=None, metadata=None):
    block = {"copy_id": copy_id, "collected_at": "2026-10-02T10:00:00.000Z", "collected_at_unknown": False,
             "ownership": "owned", "metadata": metadata or {"media_type": "digital", "resolution": "hd_1080p"}}
    if key:
        block["copy_key"] = key
    return {**EPISODE, "library": block}


def test_capabilities_enable_collection_with_media_updates_and_copies():
    caps = OPS.capabilities()["collection"]
    assert OPS.features()["collection"]
    assert caps["read"] and caps["write"] and caps["media_updates"] and caps["copies"]
    assert caps["types"] == {"movies": True, "shows": False, "seasons": False, "episodes": True}


def test_index_reads_best_copy_all_owned_copies_and_folds_episode_copies(env, lib):
    lib.movies = [_movie_row([
        _wt_copy(11, media_type="uhd_bluray", resolution="uhd_4k", edition="Steelbook 4K", packaging="digibook",
                 condition="sealed", region=["B", "free"], discs=2, purchase={"price": 29.99, "currency": "EUR", "store": "Amazon"},
                 location="Shelf B2", loan={"to": "Alex"}),
        _wt_copy(12, key="crosswatch:0123456789ab", media_type="digital"),
        _wt_copy(13, ownership="wanted"),
    ])]
    lib.episodes = [_episode_row(21, key="crosswatch:show:tmdb:1396:aaaa"),
                    _episode_row(22, metadata={"media_type": "bluray", "resolution": "uhd_4k", "audio": "dts_x"})]
    index = env.adapter.build_index("collection")
    movie = index["tmdb:155"] if "tmdb:155" in index else next(v for v in index.values() if v["type"] == "movie")
    assert movie["format"] == "4k_bluray" and movie["media_info"]["resolution"] == "uhd_4k"
    assert movie["collected_at"] == "2026-10-01T10:00:00Z"
    first, second = movie["copies"]
    assert first["source"] == "wetrakr" and first["ref"] == "11" and len(first["id"]) == 12
    assert first["packaging"] == "other" and first["condition"] == "mint" and first["region"] == "B,FREE"
    assert first["price"] == 29.99 and first["store"] == "Amazon" and first["shelf"] == "Shelf B2" and first["lent_to"] == "Alex"
    assert first["purchased"] == "2026-10-01"
    assert second == {**second, "id": "0123456789ab", "source": "crosswatch", "format": "digital"}
    assert [ref["id"] for ref in movie["_wetrakr_copy_refs"]] == [11, 12]
    episode = next(v for v in index.values() if v["type"] == "episode")
    assert episode["format"] == "bluray" and episode["media_info"] == {"resolution": "uhd_4k", "audio": "dts_x"}
    assert episode["_wetrakr_copy_keys"] == ["crosswatch:show:tmdb:1396:aaaa"]


def test_unchanged_library_activity_reuses_snapshot(env, lib):
    env.server.activities["library"] = {"all": "2026-10-01T10:00:00Z", "last_updated_at": "2026-10-01T10:00:00Z"}
    lib.movies = [_movie_row([_wt_copy(11)])]
    env.adapter.build_index("collection")
    reads = sum(1 for _m, path, _k in env.server.calls if path.startswith("/sync/library/"))
    env.adapter.build_index("collection")
    assert sum(1 for _m, path, _k in env.server.calls if path.startswith("/sync/library/")) == reads


def _cw_movie(**extra):
    return {"type": "movie", "title": "Test movie", "ids": {"tmdb": "155", "imdb": "tt0468569"}, **extra}


def test_add_sends_cw_copies_with_keys_and_keeps_private_fields_home_by_default(env, lib):
    item = _cw_movie(collected_at="2026-09-01T00:00:00Z", copies=[
        {"id": "aaaaaaaaaaaa", "format": "4k_bluray", "resolution": "uhd_4k", "hdr": "dolby_vision", "packaging": "keep_case",
         "condition": "mint", "region": "B", "discs": 2, "price": 30, "store": "Amazon", "shelf": "A1", "lent_to": "Sam"},
        {"id": "bbbbbbbbbbbb", "format": "digital", "service": {"id": 8, "name": "Netflix"}, "source": "wetrakr", "ref": "77"},
    ])
    result = env.adapter.add("collection", [item])
    assert result["ok"] and result["confirmed_keys"]
    path, payload = lib.posts[-1]
    assert path == "/sync/library"
    row = payload["movies"][0]
    assert row["ids"] == {"tmdb": 155} and row["collected_at"] == "2026-09-01T00:00:00Z"
    disc, stream = row["copies"]
    assert disc["copy_key"] == "crosswatch:aaaaaaaaaaaa"
    assert disc["media_type"] == "uhd_bluray" and disc["resolution"] == "uhd_4k" and disc["hdr"] == "dolby_vision"
    assert disc["packaging"] == "standard" and disc["condition"] == "sealed" and disc["region"] == ["B"] and disc["discs"] == 2
    assert not {"purchase", "location", "loan", "notes"} & set(disc)
    assert stream["copy_key"] == "crosswatch:bbbbbbbbbbbb" and "copy_id" not in stream and stream["digital_source"] == "netflix"


def test_private_copy_details_follow_the_pair_option(env, lib, monkeypatch):
    monkeypatch.setattr(coll, "_currency", lambda: "EUR")
    env.adapter.config = {**env.adapter.config, "wetrakr": {**env.adapter.config["wetrakr"], "collection": {"copy_details": True}}}
    item = _cw_movie(copies=[{"id": "aaaaaaaaaaaa", "format": "dvd", "price": 9.5, "store": "Shop", "shelf": "A1", "notes": "n", "lent_to": "Sam"}])
    env.adapter.add("collection", [item])
    entry = lib.posts[-1][1]["movies"][0]["copies"][0]
    assert entry["purchase"] == {"price": 9.5, "currency": "EUR", "store": "Shop"}
    assert entry["location"] == "A1" and entry["notes"] == "n" and entry["loan"] == {"to": "Sam"}


def test_movie_without_copies_upserts_one_crosswatch_copy_with_pair_source(env, lib, monkeypatch):
    monkeypatch.setenv("CW_PAIR_SRC", "PLEX")
    monkeypatch.setenv("CW_PAIR_DST", "WETRAKR")
    env.adapter.add("collection", [_cw_movie(format="digital", media_info={"resolution": "uhd_4k", "audio": "dolby_atmos", "is_3d": True})])
    row = lib.posts[-1][1]["movies"][0]
    assert row["copy_key"] == "crosswatch:movie:plex:tmdb:155"
    assert row["digital_source"] == "plex" and row["media_type"] == "digital" and row["3d"] is True


def test_copy_update_sends_only_changed_fields_and_removes_stale_crosswatch_copies(env, lib):
    dst = _cw_movie(format="bluray", media_info={"resolution": "hd_1080p"}, copies=[
        {"id": "aaaaaaaaaaaa", "format": "bluray", "resolution": "hd_1080p", "packaging": "other", "source": "crosswatch", "ref": "11"},
        {"id": "cccccccccccc", "format": "dvd", "source": "crosswatch", "ref": "12"},
        {"id": "dddddddddddd", "format": "dvd", "source": "wetrakr", "ref": "13"},
    ], _wetrakr_copy_refs=[{"id": 11, "key": "crosswatch:aaaaaaaaaaaa"}, {"id": 12, "key": "crosswatch:cccccccccccc"}, {"id": 13, "key": ""}])
    src = _cw_movie(format="bluray", copies=[{"id": "aaaaaaaaaaaa", "format": "bluray", "resolution": "uhd_4k", "packaging": "digipak"}])
    update = media_update(src, dst, copies=True)
    assert update and update["_cw_media_update"]
    env.adapter.add("collection", [update])
    (add_path, payload), (remove_path, removal) = lib.posts[-2:]
    entry = payload["movies"][0]["copies"][0]
    assert entry == {"copy_id": 11, "resolution": "uhd_4k"}
    assert "collected_at" not in payload["movies"][0]
    assert remove_path == "/sync/library/remove"
    assert removal["movies"] == [{"ids": {"tmdb": 155}, "copy_id": 12}]


def test_rejected_episode_update_keeps_the_old_copy(env, lib, monkeypatch):
    monkeypatch.delenv("CW_PAIR_SRC", raising=False)
    monkeypatch.delenv("CW_PAIR_DST", raising=False)
    lib.response = {"errored": {"shows": [{"ids": {"tmdb": 1396}, "error": "INVALID_FIELD", "field": "audio"}]}}
    upgraded = {"type": "episode", "show_ids": {"tmdb": "1396"}, "ids": {"tmdb": "62085"}, "season": 1, "episode": 1,
                "media_info": {"resolution": "uhd_4k"}, "_cw_media_update": True, "_wetrakr_copy_keys": ["crosswatch:show:cw:tmdb:1396:old"]}
    result = env.adapter.add("collection", [upgraded])
    assert result["unresolved"][0]["reason"] == "INVALID_FIELD:audio"
    assert [path for path, _payload in lib.posts] == ["/sync/library"]


def test_movie_media_update_reuses_its_existing_crosswatch_copy_key(env, lib, monkeypatch):
    monkeypatch.setenv("CW_PAIR_SRC", "PLEX")
    monkeypatch.setenv("CW_PAIR_DST", "WETRAKR")
    dst = {"type": "movie", "ids": {"wetrakr": "126", "imdb": "tt0468569"}, "format": "digital", "media_info": {"resolution": "hd_1080p"},
           "_wetrakr_copy_refs": [{"id": 30, "key": "crosswatch:movie:jellyfin:tmdb:155"}, {"id": 31, "key": "crosswatch:movie:plex:imdb:tt0468569"}]}
    update = media_update({"type": "movie", "ids": {"tmdb": "155"}, "format": "digital", "media_info": {"resolution": "uhd_4k"}}, dst)
    env.adapter.add("collection", [update])
    row = lib.posts[-1][1]["movies"][0]
    assert row["copy_key"] == "crosswatch:movie:plex:imdb:tt0468569" and row["resolution"] == "uhd_4k" and "collected_at" not in row


def test_media_update_from_a_source_without_copies_does_not_rewrite_destination_copies(env, lib, monkeypatch):
    monkeypatch.setenv("CW_PAIR_SRC", "PLEX")
    monkeypatch.setenv("CW_PAIR_DST", "WETRAKR")
    dst = _cw_movie(format="digital", media_info={"resolution": "hd_1080p"},
                    copies=[{"id": "dddddddddddd", "format": "digital", "resolution": "hd_1080p", "source": "wetrakr", "ref": "13"}],
                    _wetrakr_copy_refs=[{"id": 13, "key": "crosswatch:movie:plex:tmdb:155"}])
    update = media_update(_cw_movie(format="digital", media_info={"resolution": "uhd_4k"}), dst, copies=True)
    env.adapter.add("collection", [update])
    assert len(lib.posts) == 1
    row = lib.posts[-1][1]["movies"][0]
    assert "copies" not in row and row["copy_key"] == "crosswatch:movie:plex:tmdb:155" and row["resolution"] == "uhd_4k"


def test_unchanged_copies_do_not_plan_aggregate_media_updates():
    copies = [{"id": "aaaaaaaaaaaa", "format": "bluray", "resolution": "hd_1080p"}]
    src = _cw_movie(format="bluray", media_info={"resolution": "hd_1080p"}, copies=copies)
    dst = _cw_movie(format="4k_bluray", media_info={"resolution": "uhd_4k"}, copies=[{**copies[0], "source": "crosswatch", "ref": "11"},
                    {"id": "eeeeeeeeeeee", "format": "4k_bluray", "source": "wetrakr", "ref": "12"}])
    assert media_update(src, dst, copies=True) is None


def test_wetrakr_origin_copies_are_only_removed_when_the_source_mirrors_them(env, lib):
    refs = [{"id": 13, "key": ""}]
    item = {**_cw_movie(copies=[{"id": "aaaaaaaaaaaa", "format": "dvd"}]), "_cw_media_update": True, "_wetrakr_copy_refs": refs}
    assert coll._stale_refs(item, item["copies"]) == []
    mirrored = [{"id": "aaaaaaaaaaaa", "format": "dvd", "source": "wetrakr", "ref": "14"}]
    assert coll._stale_refs({**item, "copies": mirrored}, mirrored) == [13]


def test_episode_adds_group_by_show_copy_and_updates_leave_old_crosswatch_copy(env, lib, monkeypatch):
    monkeypatch.delenv("CW_PAIR_SRC", raising=False)
    monkeypatch.delenv("CW_PAIR_DST", raising=False)
    episode = {"type": "episode", "series_title": "Test show", "show_ids": {"tmdb": "1396"}, "ids": {"tmdb": "62085"},
               "season": 1, "episode": 1, "format": "digital", "media_info": {"resolution": "hd_1080p"}}
    second = {**episode, "ids": {"tmdb": "62086"}, "episode": 2}
    env.adapter.add("collection", [episode, second])
    show = lib.posts[-1][1]["shows"]
    assert len(show) == 1 and show[0]["ids"] == {"tmdb": 1396} and show[0]["copy_key"].startswith("crosswatch:show:cw:tmdb:1396:")
    assert show[0]["seasons"] == [{"number": 1, "episodes": [{"number": 1}, {"number": 2}]}]
    old_key = show[0]["copy_key"]
    other = "crosswatch:show:plex:tmdb:1396:abcdef0123"
    upgraded = {**episode, "media_info": {"resolution": "uhd_4k"}, "_cw_media_update": True, "_wetrakr_copy_keys": [old_key, other]}
    env.adapter.add("collection", [upgraded])
    (_p, added), (remove_path, removal) = lib.posts[-2:]
    assert added["shows"][0]["copy_key"] != old_key and added["shows"][0]["resolution"] == "uhd_4k"
    assert remove_path == "/sync/library/remove"
    assert removal["shows"] == [{"ids": {"tmdb": 1396}, "copy_key": old_key, "seasons": [{"number": 1, "episodes": [{"number": 1}]}]}]


def test_leftover_copy_from_the_same_pair_hides_episode_media_so_cleanup_is_planned_again(env, lib):
    lib.episodes = [_episode_row(21, key="crosswatch:show:cw:tmdb:1396:aaaa"),
                    _episode_row(22, key="crosswatch:show:cw:tmdb:1396:bbbb", metadata={"media_type": "digital", "resolution": "uhd_4k"})]
    episode = next(v for v in env.adapter.build_index("collection").values() if v["type"] == "episode")
    assert "media_info" not in episode and "format" not in episode
    lib.episodes[1]["library"]["copy_key"] = "crosswatch:show:plex:tmdb:1396:bbbb"
    episode = next(v for v in env.adapter.build_index("collection", force=True).values() if v["type"] == "episode")
    assert episode["media_info"] == {"resolution": "uhd_4k"}


def test_invalid_library_rows_fail_the_read(env, lib):
    lib.movies = [{**MOVIE, "type": "show", "library": {"copies": []}}]
    with pytest.raises(coll.WeTrakrSyncError, match="invalid_library_records"):
        coll.build_index(env.adapter, force=True)
    lib.movies = [{**MOVIE, "library": {"status": "removed"}}]
    lib.episodes = [{**_episode_row(21), "show": None}]
    with pytest.raises(coll.WeTrakrSyncError, match="invalid_library_records"):
        coll.build_index(env.adapter, force=True)


def test_not_found_and_errored_items_are_unresolved_others_confirmed(env, lib):
    lib.response = {"not_found": {"movies": [{"ids": {"imdb": "tt0000001"}}]},
                    "errored": {"movies": [{"ids": {"tmdb": 603}, "error": "INVALID_FIELD", "field": "resolution"}]}}
    items = [_cw_movie(), {"type": "movie", "ids": {"imdb": "tt0000001"}}, {"type": "movie", "ids": {"tmdb": "603"}}]
    result = env.adapter.add("collection", items)
    reasons = {row["key"]: row["reason"] for row in result["unresolved"]}
    assert reasons == {"imdb:tt0000001": "not_found", "tmdb:603": "INVALID_FIELD:resolution"}
    assert len(result["confirmed_keys"]) == 1


def test_plan_limit_marks_the_batch_unresolved(env, lib):
    lib.status = 420
    result = env.adapter.add("collection", [_cw_movie()])
    assert not result["ok"] and result["unresolved"][0]["reason"] == "plan_limit_reached"


def test_remove_deletes_movies_and_episodes(env, lib, monkeypatch):
    monkeypatch.delenv("CW_PAIR_SRC", raising=False)
    monkeypatch.delenv("CW_PAIR_DST", raising=False)
    episode = {"type": "episode", "show_ids": {"tmdb": "1396"}, "season": 1, "episode": 3, "ids": {}}
    result = env.adapter.remove("collection", [_cw_movie(), episode])
    assert result["ok"] and len(result["confirmed_keys"]) == 2
    assert lib.posts[-1] == ("/sync/library/remove", {"movies": [{"ids": {"tmdb": 155}}],
                             "shows": [{"ids": {"tmdb": 1396}, "seasons": [{"number": 1, "episodes": [{"number": 3}]}]}]})


def test_pair_removals_only_touch_that_pairs_copies(env, lib, monkeypatch):
    monkeypatch.setenv("CW_PAIR_SRC", "PLEX")
    monkeypatch.setenv("CW_PAIR_DST", "WETRAKR")
    monkeypatch.setenv("CW_PAIR_SRC_INSTANCE", "P02")
    episode = {"type": "episode", "show_ids": {"tmdb": "1396"}, "season": 1, "episode": 3, "ids": {}}
    assert env.adapter.remove("collection", [_cw_movie(), episode])["ok"]
    assert lib.posts[-1][1] == {"movies": [{"ids": {"tmdb": 155}, "copy_key_prefix": "crosswatch:movie:plex.p02:"}],
                                "shows": [{"ids": {"tmdb": 1396}, "copy_key_prefix": "crosswatch:show:plex.p02:",
                                           "seasons": [{"number": 1, "episodes": [{"number": 3}]}]}]}
    monkeypatch.setenv("CW_PAIR_SRC", "CROSSWATCH")
    monkeypatch.delenv("CW_PAIR_SRC_INSTANCE")
    movie = _cw_movie(copies=[{"id": "aaaaaaaaaaaa", "format": "dvd", "source": "crosswatch", "ref": "11"},
                              {"id": "bbbbbbbbbbbb", "format": "dvd", "source": "wetrakr", "ref": "12"}])
    env.adapter.remove("collection", [movie])
    assert lib.posts[-1][1] == {"movies": [{"ids": {"tmdb": 155}, "copy_key_prefix": "crosswatch:movie:crosswatch:"},
                                           {"ids": {"tmdb": 155}, "copy_id": 11}]}


def test_two_way_removals_remove_the_whole_title(env, lib, monkeypatch):
    monkeypatch.setenv("CW_PAIR_SRC", "WETRAKR")
    monkeypatch.setenv("CW_PAIR_DST", "CROSSWATCH")
    monkeypatch.setenv("CW_PAIR_MODE", "two-way")
    env.adapter.remove("collection", [_cw_movie(copies=[{"id": "aaaaaaaaaaaa", "format": "dvd", "source": "crosswatch", "ref": "11"}])])
    assert lib.posts[-1][1] == {"movies": [{"ids": {"tmdb": 155}}]}


def test_malformed_copies_fail_the_read(env, lib):
    lib.movies = [_movie_row([_wt_copy(11), {**_wt_copy(12), "id": None}])]
    with pytest.raises(coll.WeTrakrSyncError, match="invalid_library_records"):
        coll.build_index(env.adapter, force=True)


def test_episode_cleanup_scope_ignores_show_id_changes():
    old = "crosswatch:show:plex:imdb:tt0903747:aaaa"
    new = "crosswatch:show:plex:tmdb:1396:bbbb"
    item = {"show_ids": {"tmdb": "1396"}, "_cw_media_update": True, "_wetrakr_copy_keys": [old, "crosswatch:show:jellyfin:tmdb:1396:cccc"]}
    assert [row["copy_key"] for row in coll._old_episode_rows(item, new, 1, 1)] == [old]


def test_failed_stale_cleanup_is_not_confirmed(env, lib):
    real = lib.__call__

    def failing(method, path, kwargs):
        if path == "/sync/library/remove":
            return Response({"error": "boom"}, status=500)
        return real(method, path, kwargs)

    env.server.hook = failing
    dst = _cw_movie(copies=[{"id": "aaaaaaaaaaaa", "format": "dvd", "source": "crosswatch", "ref": "11"},
                            {"id": "cccccccccccc", "format": "dvd", "source": "crosswatch", "ref": "12"}],
                    _wetrakr_copy_refs=[{"id": 11, "key": "crosswatch:aaaaaaaaaaaa"}, {"id": 12, "key": "crosswatch:cccccccccccc"}])
    update = media_update(_cw_movie(copies=[{"id": "aaaaaaaaaaaa", "format": "dvd"}]), dst, copies=True)
    result = env.adapter.add("collection", [update])
    assert not result["ok"] and not result["confirmed_keys"]
    assert result["unresolved"][0]["reason"] == "stale_copy_remove_failed"


def test_errored_removals_stay_unresolved(env, lib, monkeypatch):
    monkeypatch.delenv("CW_PAIR_SRC", raising=False)
    monkeypatch.delenv("CW_PAIR_DST", raising=False)
    real = lib.__call__

    def errored(method, path, kwargs):
        if path == "/sync/library/remove":
            lib.posts.append((path, kwargs["json"]))
            return Response({"deleted": {"movies": 0}, "errored": {"movies": [{"ids": {"tmdb": 155}, "error": "LOCKED"}]}})
        return real(method, path, kwargs)

    env.server.hook = errored
    result = env.adapter.remove("collection", [_cw_movie(), {"type": "movie", "ids": {"tmdb": "603"}}])
    assert [(row["key"], row["reason"]) for row in result["unresolved"]] == [("tmdb:155", "LOCKED")]
    assert result["confirmed_keys"] == ["tmdb:603"]


def test_dry_run_and_unsupported_types_do_not_write(env, lib):
    result = env.adapter.add("collection", [_cw_movie(), {"type": "show", "ids": {"tmdb": "1396"}}], dry_run=True)
    assert result["count"] == 1 and result["unresolved"][0]["reason"] == "unsupported_media_type"
    assert not lib.posts


def test_copies_differ_ignores_copies_the_source_does_not_own():
    src = [{"id": "aaaaaaaaaaaa", "format": "dvd"}]
    assert not copies_differ(src, src + [{"id": "eeeeeeeeeeee", "format": "dvd", "source": "wetrakr"}])
    assert copies_differ(src, src + [{"id": "eeeeeeeeeeee", "format": "dvd", "source": "crosswatch"}])
    assert not copies_differ(src, [{"id": "aaaaaaaaaaaa", "format": "dvd", "price": 4}])
    assert copies_differ(src, [{"id": "aaaaaaaaaaaa", "format": "bluray"}])
    assert not copies_differ([{"id": "a", "format": "dvd", "packaging": "digipak"}], [{"id": "a", "format": "dvd", "packaging": "other"}])


def test_4k_bluray_copies_compare_equal_after_the_round_trip():
    src = [{"id": "a", "format": "4k_bluray"}]
    assert not copies_differ(src, [{"id": "a", "format": "4k_bluray", "resolution": "uhd_4k"}])
    assert not copies_differ(src, [{"id": "a", "format": "bluray", "resolution": "uhd_4k"}])
    assert coll._metadata({"format": "4k_bluray"}) == {"media_type": "uhd_bluray", "resolution": "uhd_4k"}


def test_private_fields_and_purchase_date_trigger_updates_one_way():
    src = [{"id": "a", "format": "dvd", "price": 9.5, "notes": "line one\nline two", "purchased": "2026-01-02"}]
    same = [{"id": "a", "format": "dvd", "price": 9.5, "notes": "line one line two", "purchased": "2026-01-02"}]
    assert not copies_differ(src, same, private=True)
    assert copies_differ(src, [{**same[0], "price": 12}], private=True)
    assert not copies_differ(src, [{**same[0], "price": 12}])
    assert copies_differ(src, [{**same[0], "purchased": "2026-03-04"}])
    assert not copies_differ([{"id": "a", "format": "dvd"}], same, private=True)


def test_copy_details_capability_follows_the_pair_option():
    from cw_platform.orchestrator._pairs_utils import collection_copy_details_for

    class Ops:
        def __init__(self, mode):
            self.mode = mode

        def capabilities(self):
            return {"collection": {"copies": True, "copy_details": self.mode}}

    assert collection_copy_details_for(Ops(True), {})
    assert not collection_copy_details_for(Ops("option"), {})
    assert collection_copy_details_for(Ops("option"), {"copy_details": True})
    assert not collection_copy_details_for(Ops(None), {"copy_details": True})


def test_media_update_without_copy_support_ignores_copies():
    src = {"type": "movie", "ids": {"tmdb": "1"}, "format": "dvd", "copies": [{"id": "aaaaaaaaaaaa", "format": "bluray"}]}
    dst = {"type": "movie", "ids": {"tmdb": "1"}, "format": "dvd"}
    assert media_update(src, dst) is None
    assert media_update(src, dst, copies=True)["copies"] == src["copies"]


def test_minimal_keeps_copies():
    item = {"type": "movie", "ids": {"tmdb": "1"}, "copies": [{"id": "aaaaaaaaaaaa", "format": "dvd"}]}
    assert minimal(item)["copies"] == item["copies"]


def test_shelf_copy_keeps_media_source_and_ref():
    from services.shelf_store import clean_copy

    row = clean_copy({"format": "bluray", "resolution": "4K", "hdr": "DV", "audio": "TrueHD Atmos", "audio_channels": "8",
                      "is_3d": "true", "source": "wetrakr", "ref": "77"})
    assert {k: row[k] for k in ("resolution", "hdr", "audio", "audio_channels", "is_3d", "source", "ref")} == {
        "resolution": "uhd_4k", "hdr": "dolby_vision", "audio": "dolby_atmos", "audio_channels": "7.1", "is_3d": True,
        "source": "wetrakr", "ref": "77"}
    assert "ref" not in clean_copy({"format": "dvd", "ref": "77"})


def test_cw_tracker_merges_synced_copies_and_keeps_its_own():
    from providers.sync.crosswatch._collection import _keep_manual_media

    existing = {"copies": [
        {"id": "aaaaaaaaaaaa", "format": "dvd", "price": 5, "shelf": "A1", "volume": "Part 1", "packaging": "digipak"},
        {"id": "bbbbbbbbbbbb", "format": "dvd"},
        {"id": "cccccccccccc", "format": "dvd", "source": "wetrakr", "ref": "3"},
    ]}
    incoming = {"copies": [
        {"id": "aaaaaaaaaaaa", "format": "bluray", "packaging": "other", "source": "crosswatch", "ref": "1"},
        {"id": "dddddddddddd", "format": "digital", "source": "wetrakr", "ref": "4"},
    ]}
    merged = {row["id"]: row for row in _keep_manual_media(existing, dict(incoming))["copies"]}
    assert set(merged) == {"aaaaaaaaaaaa", "bbbbbbbbbbbb", "dddddddddddd"}
    first = merged["aaaaaaaaaaaa"]
    assert first["format"] == "bluray" and first["packaging"] == "digipak"
    assert first["price"] == 5 and first["shelf"] == "A1" and first["volume"] == "Part 1" and "source" not in first
    assert merged["dddddddddddd"]["source"] == "wetrakr"


def test_pair_option_reaches_wetrakr_config():
    from cw_platform.orchestrator._pairs import _config_with_pair_feature_options

    cfg = {"wetrakr": {"access_token": "x"}}
    out = _config_with_pair_feature_options(cfg, {"copy_details": True}, ("CROSSWATCH", "WETRAKR"), "collection")
    assert out["wetrakr"]["collection"] == {"copy_details": True}
    assert "collection" not in cfg["wetrakr"]
    assert _config_with_pair_feature_options(cfg, {}, ("CROSSWATCH", "TRAKT"), "collection") is cfg
