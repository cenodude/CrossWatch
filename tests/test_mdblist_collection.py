# tests/test_mdblist_collection.py
# CrossWatch - MDBList collection media info
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import json
from types import SimpleNamespace

from cw_platform.collection_media import media_update
from providers.sync import _mod_MDBLIST as mdblist
from providers.sync.mdblist import _collection as coll


SHOW = {"title": "Fleabag", "year": 2016, "ids": {"tmdb": 67070, "imdb": "tt5687612", "tvdb": 314614}}


def response(data, status=200):
    return SimpleNamespace(status_code=status, text=json.dumps(data), json=lambda: data, headers={})


def copy(**fields):
    base = {"copy_key": "", "source": "api", "media_type": None, "resolution": None, "hdr": None, "audio": None, "audio_channels": None, "3d": None}
    return {**base, **fields}


def adapter():
    return SimpleNamespace(config={"mdblist": {"api_key": "k", "collection_verify_after_write": False, "collection_write_delay_ms": 0}},
                           cfg=SimpleNamespace(timeout=1, max_retries=0), client=SimpleNamespace(session=None))


def setup_reader(monkeypatch, tmp_path, page):
    calls = []
    monkeypatch.setattr(coll, "_shadow_path", lambda: tmp_path / "shadow.json")
    monkeypatch.setattr(coll, "_shadow_save", lambda items: None)
    monkeypatch.setattr(coll, "get_watermark", lambda *a: None)
    monkeypatch.setattr(coll, "save_watermark", lambda *a: None)
    monkeypatch.setattr(coll, "_fetch_last_activities", lambda *a, **kw: {})
    monkeypatch.setattr(coll, "mdblist_request", lambda *a, **kw: calls.append(kw["params"]) or response(page))
    return calls


def capture_writes(monkeypatch):
    sent = []
    monkeypatch.setattr(coll, "_shadow_bust", lambda: None)
    monkeypatch.setattr(coll.time, "sleep", lambda s: None)
    monkeypatch.setattr(coll, "mdblist_request", lambda *a, **kw: sent.append(kw["json"]) or response({"updated": {"movies": 1}}))
    return sent


def test_index_reads_media_copies_and_derives_season_media(monkeypatch, tmp_path):
    page = {
        "movies": [{"collected_at": "2024-01-02T03:04:05.000Z", "movie": {"title": "The Dark Knight", "year": 2008, "ids": {"tmdb": 155}},
                    "media": [copy(copy_key="a", media_type="bluray", resolution="uhd_4k", hdr="dolby_vision", audio="dolby_truehd", audio_channels="7.1"),
                              copy(copy_key="b", media_type="digital", resolution="hd_1080p", audio="aac", audio_channels="2.0")]}],
        "seasons": [{"collected_at": "2021-01-01T00:00:00.000Z", "season": {"number": 1, "ids": {"tmdb": 78415}, "show": SHOW}}],
        "episodes": [{"collected_at": "2021-01-01T00:00:00.000Z", "media": [copy(resolution="hd_1080p", audio="aac", audio_channels="2.0")],
                      "episode": {"season": 1, "number": n, "ids": {"tmdb": 1000 + n}, "show": SHOW}} for n in (1, 2)],
        "pagination": {"next_cursor": None},
    }
    calls = setup_reader(monkeypatch, tmp_path, page)
    idx = coll.build_index(adapter())
    assert calls[0]["append_to_response"] == "media"
    movie = next(v for v in idx.values() if v["type"] == "movie")
    assert movie["format"] == "bluray"
    assert movie["media_info"] == {"resolution": "uhd_4k", "hdr": "dolby_vision", "audio": "dolby_truehd", "audio_channels": "7.1"}
    season = next(v for v in idx.values() if v["type"] == "season")
    assert season["media_info"] == {"resolution": "hd_1080p", "audio": "aac", "audio_channels": "2.0"}
    episode = next(v for v in idx.values() if v["type"] == "episode")
    assert episode["media_info"] == season["media_info"]
    assert "format" not in episode


def test_index_without_media_stays_plain(monkeypatch, tmp_path):
    page = {"movies": [{"collected_at": "2024-01-02T03:04:05.000Z", "movie": {"ids": {"tmdb": 155}}, "media": []}], "pagination": {}}
    setup_reader(monkeypatch, tmp_path, page)
    movie = next(iter(coll.build_index(adapter()).values()))
    assert "format" not in movie and "media_info" not in movie


def test_unversioned_shadow_is_not_reused(monkeypatch, tmp_path):
    path = tmp_path / "shadow.json"
    path.write_text(json.dumps({"ts": 1, "items": {"tmdb:155": {"type": "movie", "ids": {"tmdb": "155"}}}}))
    monkeypatch.setattr(coll, "_shadow_path", lambda: path)
    assert coll._shadow_load()["items"] == {}
    path.write_text(json.dumps({"v": coll.SHADOW_VERSION, "ts": 1, "items": {"tmdb:155": {"type": "movie"}}}))
    assert list(coll._shadow_load()["items"]) == ["tmdb:155"]


def test_add_sends_trakt_media_fields_only_when_source_has_them(monkeypatch):
    sent = capture_writes(monkeypatch)
    items = [
        {"type": "movie", "ids": {"tmdb": "155"}, "collected_at": "2024-01-02T03:04:05Z", "format": "4k_bluray",
         "media_info": {"hdr": "dolby_vision", "audio": "dolby_truehd", "audio_channels": "8", "is_3d": True}},
        {"type": "movie", "ids": {"tmdb": "27205"}, "collected_at": "2023-03-03T00:00:00Z", "format": "hd_dvd"},
        {"type": "movie", "ids": {"tmdb": "603"}, "collected_at": "2022-02-02T00:00:00Z"},
    ]
    coll.add(adapter(), items)
    movies = sent[0]["movies"]
    assert movies[0] == {"ids": {"tmdb": 155}, "collected_at": "2024-01-02T03:04:05Z", "media_type": "bluray", "resolution": "uhd_4k",
                         "hdr": "dolby_vision", "audio": "dolby_truehd", "audio_channels": "7.1", "3d": True}
    assert movies[1] == {"ids": {"tmdb": 27205}, "collected_at": "2023-03-03T00:00:00Z", "media_type": "hddvd"}
    assert movies[2] == {"ids": {"tmdb": 603}, "collected_at": "2022-02-02T00:00:00Z"}


def test_media_update_keeps_stored_collected_at_and_nests_episode_media(monkeypatch):
    sent = capture_writes(monkeypatch)
    dst = {"type": "episode", "show_ids": {"tmdb": "87108"}, "ids": {"tmdb": "1725580"}, "season": 1, "episode": 1,
           "collected_at": "2022-02-02T00:00:00.000Z", "media_info": {"resolution": "hd_720p"}}
    src = {"type": "episode", "show_ids": {"tmdb": "87108"}, "season": 1, "episode": 1, "collected_at": "2026-10-28T00:00:00Z",
           "media_info": {"resolution": "hd_1080p", "audio": "dts_ma", "audio_channels": "5.1"}}
    upd = media_update(src, dst)
    assert upd and upd["_cw_media_update"]
    coll.add(adapter(), [upd])
    episode = sent[0]["shows"][0]["seasons"][0]["episodes"][0]
    assert episode == {"number": 1, "resolution": "hd_1080p", "audio": "dts_ma", "audio_channels": "5.1", "collected_at": "2022-02-02T00:00:00.000Z"}


def test_season_media_goes_on_season_entry(monkeypatch):
    sent = capture_writes(monkeypatch)
    coll.add(adapter(), [{"type": "season", "show_ids": {"tmdb": "67070"}, "season": 1, "collected_at": "2021-01-01T00:00:00Z",
                          "media_info": {"resolution": "hd_1080p"}}])
    assert sent[0]["shows"][0]["seasons"][0] == {"number": 1, "collected_at": "2021-01-01T00:00:00Z", "resolution": "hd_1080p"}


def test_remove_never_sends_media(monkeypatch):
    sent = capture_writes(monkeypatch)
    coll.remove(adapter(), [{"type": "movie", "ids": {"tmdb": "155"}, "format": "bluray", "media_info": {"resolution": "uhd_4k"}}])
    assert sent[0] == {"movies": [{"ids": {"tmdb": 155}}]}


def test_manifest_offers_media_updates():
    assert mdblist.get_manifest()["capabilities"]["collection"]["media_updates"] is True
