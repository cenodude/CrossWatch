# tests/test_collection_media.py
# CrossWatch - Collection media info normalization and Trakt mapping
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json

import pytest

from cw_platform import collection_media as cm
from cw_platform.id_map import minimal


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("uhd_4k", "uhd_4k"), ("4K", "uhd_4k"), ("4K UHD", "uhd_4k"), ("2160p", "uhd_4k"), ("1080p", "hd_1080p"), ("1080i", "hd_1080i"),
        ("Full HD 1080", "hd_1080p"), ("720p", "hd_720p"), ("576i", "sd_576i"), ("480p", "sd_480p"), ("1440p", ""), ("14K", ""), ("8K", ""),
    ],
)
def test_normalize_resolution(raw: str, expected: str) -> None:
    assert cm.normalize_resolution(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("Dolby Vision", "dolby_vision"), ("DV HDR10", "dolby_vision"), ("DoVi", "dolby_vision"), ("HDR10+", "hdr10_plus"), ("HDR10 +", "hdr10_plus"), ("HDR10 Plus", "hdr10_plus"), ("HDR10", "hdr10"), ("HDR", "hdr10"), ("HLG", "hlg"), ("SDR", "")],
)
def test_normalize_hdr(raw: str, expected: str) -> None:
    assert cm.normalize_hdr(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("TrueHD", "dolby_truehd"),
        ("TrueHD Atmos", "dolby_atmos"),
        ("Atmos", "dolby_atmos"),
        ("DD+", "dolby_digital_plus"),
        ("E-AC-3 Atmos", "dolby_digital_plus_atmos"),
        ("AC3", "dolby_digital"),
        ("DTS-HD Master Audio", "dts_ma"),
        ("DTS-HD HRA", "dts_hr"),
        ("DTS (German)", "dts"),
        ("DTS Mandarin", "dts"),
        ("DTSHD-MA", "dts_ma"),
        ("DTS XLL", "dts_ma"),
        ("DTS XLL X", "dts_x"),
        ("DTS XBR", "dts_hr"),
        ("DTS-X", "dts_x"),
        ("DTS:X Master Audio", "dts_x"),
        ("dca", "dts"),
        ("dca-ma", "dts_ma"),
        ("E-AC-3 JOC", "dolby_digital_plus_atmos"),
        ("MLP FBA", "dolby_truehd"),
        ("MLP FBA 16-ch", "dolby_atmos"),
        ("HE-AAC", "aac"),
        ("HE-AAC v2", "aac"),
        ("DTS:X", "dts_x"),
        ("DTS", "dts"),
        ("PCM", "lpcm"),
        ("Opus", "ogg_opus"),
        ("FLAC", "flac"),
        ("Atrac", ""),
    ],
)
def test_normalize_audio(raw: str, expected: str) -> None:
    assert cm.normalize_audio(raw) == expected


@pytest.mark.parametrize(("raw", "expected"), [("5.1", "5.1"), ("7.1.4", "7.1.4"), ("Stereo", "2.0"), ("6.0", "5.1"), ("7.0", "6.1"), ("2ch", "2.0"), ("22.2", ""), ("lots", "")])
def test_normalize_channels(raw: str, expected: str) -> None:
    assert cm.normalize_channels(raw) == expected


def test_floppy_labels_round_trip_to_the_same_values() -> None:
    for value in cm.RESOLUTIONS:
        assert cm.normalize_resolution(cm.to_floppy_fields({"media_info": {"resolution": value}})["resolution"]) == value
    for value in cm.HDRS:
        assert cm.normalize_hdr(cm.to_floppy_fields({"media_info": {"hdr": value}})["hdr"]) == value
    for value in cm.AUDIOS:
        assert cm.normalize_audio(cm.to_floppy_fields({"media_info": {"audio": value}})["audio_codec"]) == value


def test_media_block_keeps_unknown_raw_values_and_drops_empty() -> None:
    assert cm.media_block({"resolution": "1440p", "hdr": "", "audio": None, "audio_channels": "5.1", "is_3d": None}) == {"resolution": "1440p", "audio_channels": "5.1"}


def test_to_trakt_metadata_sends_only_valid_values() -> None:
    item = {"format": "Blu-ray", "media_info": {"resolution": "1440p", "hdr": "hdr10", "audio": "dts_x", "audio_channels": "22.2", "is_3d": True}}
    assert cm.to_trakt_metadata(item) == {"media_type": "bluray", "hdr": "hdr10", "audio": "dts_x", "3d": True}


@pytest.mark.parametrize(
    ("fmt", "expected"),
    [("4k_bluray", {"media_type": "bluray", "resolution": "uhd_4k"}), ("hd_dvd", {"media_type": "hddvd"}), ("vhs", {"media_type": "vhs"}), ("steelbook", {}), ("", {})],
)
def test_to_trakt_metadata_maps_formats(fmt: str, expected: dict) -> None:
    assert cm.to_trakt_metadata({"format": fmt}) == expected


def test_from_trakt_metadata() -> None:
    fmt, block = cm.from_trakt_metadata({"3d": 1, "hdr": None, "audio": "dolby_truehd", "media_type": "hddvd", "resolution": "uhd_4k", "audio_channels": "7.1"})
    assert fmt == "hd_dvd"
    assert block == {"resolution": "uhd_4k", "audio": "dolby_truehd", "audio_channels": "7.1", "is_3d": True}
    assert cm.from_trakt_metadata({"3d": None, "hdr": None, "audio": None, "media_type": None, "resolution": None, "audio_channels": None}) == ("", {})


def test_punchplay_format_folds_4k_bluray() -> None:
    assert cm.punchplay_format({"format": "bluray", "media_info": {"resolution": "uhd_4k"}}) == "4k_bluray"
    assert cm.punchplay_format({"format": "digital", "media_info": {"resolution": "uhd_4k"}}) == "digital"


def test_minimal_keeps_media_info() -> None:
    out = minimal({"type": "movie", "ids": {"tmdb": "603"}, "format": "bluray", "media_info": {"resolution": "uhd_4k"}})
    assert out["format"] == "bluray"
    assert out["media_info"] == {"resolution": "uhd_4k"}


def test_trakt_reader_maps_media_endpoint_rows() -> None:
    from providers.sync.trakt import _collection

    movie = _collection._items_from_collection_row(
        {"type": "movie", "collected_at": "2026-10-18T13:00:07.000Z", "metadata": {"3d": 1, "hdr": None, "audio": None, "media_type": "bluray", "resolution": "uhd_4k", "audio_channels": None}, "movie": {"title": "The Matrix", "year": 1999, "ids": {"trakt": 481, "tmdb": 603}}}
    )[0]
    episode = _collection._items_from_collection_row(
        {"type": "episode", "metadata": {"audio": "aac", "resolution": "hd_1080p"}, "show": {"title": "Game of Thrones", "ids": {"tmdb": 1399}}, "episode": {"season": 1, "number": 1, "ids": {"tmdb": 63056}}}
    )[0]
    plain = _collection._items_from_collection_row({"type": "movie", "movie": {"title": "Alien", "ids": {"tmdb": 348}}})[0]

    assert movie["format"] == "bluray"
    assert movie["media_info"] == {"resolution": "uhd_4k", "is_3d": True}
    assert episode["media_info"] == {"resolution": "hd_1080p", "audio": "aac"}
    assert "format" not in plain and "media_info" not in plain


def test_trakt_add_payload_carries_media_info_on_movies_and_episodes() -> None:
    from providers.sync.trakt._common import build_watchlist_body

    items = [
        minimal({"type": "movie", "ids": {"tmdb": "603"}, "collected_at": "2024-05-01T00:00:00Z", "format": "4k_bluray", "media_info": {"hdr": "Dolby Vision", "audio": "TrueHD Atmos", "audio_channels": "7.1"}}),
        minimal({"type": "episode", "show_ids": {"tmdb": "1399"}, "season": 1, "episode": 1, "media_info": {"resolution": "1080p"}}),
    ]
    body = build_watchlist_body(items, date_field="collected_at", item_fields=cm.to_trakt_metadata)

    movie = dict(body["movies"][0])
    movie.pop("ids")
    assert movie == {"media_type": "bluray", "resolution": "uhd_4k", "hdr": "dolby_vision", "audio": "dolby_atmos", "audio_channels": "7.1", "collected_at": "2024-05-01T00:00:00Z"}
    assert body["shows"][0]["seasons"][0]["episodes"] == [{"number": 1, "resolution": "hd_1080p"}]


def test_trakt_payload_without_item_fields_is_unchanged() -> None:
    from providers.sync.trakt._common import build_watchlist_body

    body = build_watchlist_body([minimal({"type": "movie", "ids": {"tmdb": "603"}, "format": "bluray", "media_info": {"resolution": "uhd_4k"}})])
    assert set(body["movies"][0]) == {"ids"}


class _TwoWayOps:
    def __init__(self) -> None:
        self.added: list[dict] = []

    def add(self, _cfg, items, *, feature, dry_run=False):
        from cw_platform.id_map import canonical_key

        rows = [dict(i) for i in items]
        self.added.extend(rows)
        return {"ok": True, "count": len(rows), "confirmed_keys": [canonical_key(i) for i in rows], "unresolved": []}

    def remove(self, _cfg, items, *, feature, dry_run=False):
        return {"ok": True, "count": 0, "confirmed_keys": [], "unresolved": []}

    def update(self, *_a, **_k):
        return {"ok": True, "count": 0}

    def capabilities(self):
        return {"collection": {}}


class _TwoWayStore:
    def __init__(self) -> None:
        self.state: dict = {"providers": {}}
        self.tomb: dict = {"keys": {}}

    def load_state(self):
        return self.state

    def save_state(self, value):
        self.state = value

    def load_tomb(self):
        return self.tomb

    def save_tomb(self, value):
        self.tomb = value


def test_twoway_collection_carries_media_info_both_directions(monkeypatch: pytest.MonkeyPatch) -> None:
    from types import SimpleNamespace

    from cw_platform.id_map import canonical_key
    from cw_platform.orchestrator import _pairs_twoway as twoway

    for name, val in (
        ("_supports_feature", lambda _o, _f: True),
        ("_health_feature_ok", lambda _h, _f: True),
        ("_health_status", lambda _h: "up"),
        ("_resolve_flags", lambda _f, _s: {"allow_adds": True, "allow_removals": False}),
        ("_anime_pair_feature_options", lambda *_a, **_k: {"use_anime_mapping": False}),
        ("_anime_config_with_pair_feature_options", lambda cfg, _o: cfg),
        ("_index_semantics", lambda *_a, **_k: "full"),
        ("prev_checkpoint", lambda *_a, **_k: None),
        ("module_checkpoint", lambda *_a, **_k: None),
        ("keys_for_feature", lambda *_a, **_k: {}),
        ("_manual_policy", lambda *_a, **_k: ([], set())),
        ("_provider_ignore_dropped_enabled", lambda *_a, **_k: False),
        ("apply_blocklist", lambda _s, items, **_k: list(items)),
        ("_maybe_block_massdelete", lambda items, **_k: list(items)),
        ("effective_chunk_size", lambda *_a, **_k: 100),
        ("load_blackbox_keys", lambda *_a, **_k: set()),
        ("record_attempts", lambda *_a, **_k: {"ok": True, "count": 0}),
        ("record_success", lambda *_a, **_k: {"ok": True, "count": 0}),
        ("record_unresolved", lambda *_a, **_k: {"ok": True}),
        ("clear_unresolved", lambda *_a, **_k: {"ok": True}),
    ):
        monkeypatch.setattr(twoway, name, val)

    trakt_item = {"type": "movie", "title": "The Matrix", "ids": {"tmdb": "603"}, "format": "bluray", "media_info": {"resolution": "uhd_4k", "audio": "dolby_atmos"}}
    floppy_item = {"type": "movie", "title": "Interstellar", "ids": {"tmdb": "157336"}, "format": "Blu-ray", "media_info": {"resolution": "uhd_4k", "hdr": "dolby_vision"}}
    monkeypatch.setattr(
        twoway,
        "build_snapshots_for_feature",
        lambda **_k: {"TRAKT": {canonical_key(trakt_item): dict(trakt_item)}, "FLOPPY": {canonical_key(floppy_item): dict(floppy_item)}},
    )

    trakt, floppy = _TwoWayOps(), _TwoWayOps()
    ctx = SimpleNamespace(
        config={"sync": {"include_observed_deletes": False, "blackbox": {"enabled": False}}, "runtime": {}},
        providers={"TRAKT": trakt, "FLOPPY": floppy},
        emit=lambda *_a, **_k: None,
        emit_info=lambda *_a, **_k: None,
        dbg=lambda *_a, **_k: None,
        dry_run=False,
        snap_cache={},
        snap_ttl_sec=0,
        state_store=_TwoWayStore(),
        stats_manual_blocked=0,
        apply_chunk_pause_ms=0,
    )
    twoway._two_way_sync(ctx, "TRAKT", "FLOPPY", feature="collection", fcfg={}, health_map={})

    assert [(i["format"], i["media_info"]) for i in floppy.added] == [("bluray", {"resolution": "uhd_4k", "audio": "dolby_atmos"})]
    assert [(i["format"], i["media_info"]) for i in trakt.added] == [("Blu-ray", {"resolution": "uhd_4k", "hdr": "dolby_vision"})]


def test_crosswatch_tracker_collection_stores_media_info(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    from types import SimpleNamespace

    from providers.sync.crosswatch import _collection as cw_collection

    monkeypatch.setenv("CW_CROSSWATCH_PAIR_SCOPED", "1")
    monkeypatch.setenv("CW_PAIR_SCOPE", "media")
    monkeypatch.setenv("CW_PAIR_SRC", "TRAKT")
    adapter = SimpleNamespace(cfg=SimpleNamespace(base_path=str(tmp_path)))
    item = minimal({"type": "movie", "title": "The Matrix", "ids": {"tmdb": "603"}, "format": "bluray", "media_info": {"resolution": "uhd_4k", "audio": "dolby_atmos", "is_3d": True}})

    assert cw_collection.add(adapter, [item]) == (1, [])
    stored = cw_collection.build_index(adapter)["tmdb:603"]

    assert stored["format"] == "bluray"
    assert stored["media_info"] == {"resolution": "uhd_4k", "audio": "dolby_atmos", "is_3d": True}
    assert minimal(stored)["media_info"] == stored["media_info"]


@pytest.mark.parametrize(
    ("width", "height", "interlaced", "expected"),
    [
        (3840, 1600, False, "uhd_4k"), (2880, 2160, False, "uhd_4k"), (1920, 800, False, "hd_1080p"), (1918, 1040, False, "hd_1080p"),
        (1440, 1080, False, "hd_1080p"), (1788, 1078, False, "hd_1080p"), (1920, 1080, True, "hd_1080i"), (1280, 534, False, "hd_720p"),
        (960, 720, False, "hd_720p"), (720, 576, True, "sd_576i"), (720, 480, False, "sd_480p"), (0, 0, False, ""),
    ],
)
def test_resolution_from_dimensions(width: int, height: int, interlaced: bool, expected: str) -> None:
    assert cm.resolution_from_dimensions(width, height, interlaced=interlaced) == expected


@pytest.mark.parametrize(
    ("media", "hdr", "expected"),
    [
        ({"width": 3840, "height": 2076, "videoResolution": "4k", "audioCodec": "truehd", "audioProfile": "dolby truehd + dolby atmos", "audioChannels": 8}, "hdr10", {"resolution": "uhd_4k", "hdr": "hdr10", "audio": "dolby_atmos", "audio_channels": "7.1"}),
        ({"width": 1920, "height": 800, "audioCodec": "eac3", "audioProfile": "dolby digital plus + dolby atmos", "audioChannels": 6}, "", {"resolution": "hd_1080p", "audio": "dolby_digital_plus_atmos", "audio_channels": "5.1"}),
        ({"width": 1920, "height": 1080, "audioCodec": "dca", "audioProfile": "ma + dts:x", "audioChannels": 8}, "", {"resolution": "hd_1080p", "audio": "dts_x", "audio_channels": "7.1"}),
        ({"width": 1920, "height": 1080, "audioCodec": "dca-ma", "audioProfile": "ma", "audioChannels": 6}, "", {"resolution": "hd_1080p", "audio": "dts_ma", "audio_channels": "5.1"}),
        ({"videoResolution": "720", "audioCodec": "aac", "audioProfile": "lc", "audioChannels": 2}, "", {"resolution": "hd_720p", "audio": "aac", "audio_channels": "2.0"}),
    ],
)
def test_plex_media_info_from_list_rows(media: dict, hdr: str, expected: dict) -> None:
    assert cm.plex_media_info({"Media": [media]}, hdr) == expected


def test_plex_media_info_picks_widest_copy() -> None:
    row = {"Media": [{"width": 1920, "height": 1080, "audioCodec": "ac3", "audioChannels": 6}, {"width": 3840, "height": 2160, "audioCodec": "eac3", "audioChannels": 6}]}
    assert cm.plex_media_info(row)["resolution"] == "uhd_4k"
    assert cm.plex_media_info({"Media": []}) == {}


def _plex_collection_adapter(media_info: bool):
    from types import SimpleNamespace

    sections = [SimpleNamespace(key="1", type="movie")]
    collection_cfg = {"media_info": True} if media_info else {}
    return SimpleNamespace(client=SimpleNamespace(server=object()), config={"plex": {"collection": collection_cfg}}, libraries=lambda types=(): sections)


def _plex_movie_row(rating_key: str, tmdb: str, width: int, codec: str, profile: str) -> dict:
    return {
        "ratingKey": rating_key,
        "type": "movie",
        "title": f"Movie {rating_key}",
        "year": 2020,
        "Guid": [{"id": f"tmdb://{tmdb}"}],
        "Media": [{"width": width, "height": 1080, "audioCodec": codec, "audioProfile": profile, "audioChannels": 6}],
    }


@pytest.mark.parametrize("media_info", [False, True])
def test_plex_collection_reads_media_info_only_when_enabled(monkeypatch: pytest.MonkeyPatch, media_info: bool) -> None:
    from providers.sync.plex import _collection as plex_collection

    rows = [_plex_movie_row("10", "603", 3840, "truehd", "dolby truehd + dolby atmos"), _plex_movie_row("11", "550", 3840, "eac3", ""), _plex_movie_row("12", "680", 1920, "ac3", "")]
    calls: list[dict] = []

    def fake_fetch(_srv, _sid, _plex_type, *, extra_params=None, **_kwargs):
        calls.append(dict(extra_params or {}))
        flt = dict(extra_params or {})
        if not flt:
            return rows, 1
        if flt == {"dovi": 1}:
            return [rows[0]], 1
        if flt == {"hdr": 1}:
            return [rows[0], rows[1]], 1
        return [], 1

    monkeypatch.setattr(plex_collection, "_fetch_section_guid_rows", fake_fetch)
    monkeypatch.setattr(plex_collection, "active_pms_token", lambda _a: None)
    idx = plex_collection.build_index(_plex_collection_adapter(media_info))

    if not media_info:
        assert calls == [{}]
        assert all("media_info" not in v and "format" not in v for v in idx.values())
        return
    assert sorted(map(str, calls)) == sorted(map(str, [{}, {"dovi": 1}, {"hdr10plus": 1}, {"hdr": 1}]))
    assert idx["tmdb:603"]["format"] == "digital"
    assert idx["tmdb:603"]["media_info"] == {"resolution": "uhd_4k", "hdr": "dolby_vision", "audio": "dolby_atmos", "audio_channels": "5.1"}
    assert idx["tmdb:550"]["media_info"]["hdr"] == "hdr10"
    assert "hdr" not in idx["tmdb:680"]["media_info"]


def test_plex_collection_ignores_hdr_filter_that_returns_whole_library(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.plex import _collection as plex_collection

    rows = [_plex_movie_row("10", "603", 3840, "eac3", ""), _plex_movie_row("11", "550", 1920, "eac3", "")]
    monkeypatch.setattr(plex_collection, "_fetch_section_guid_rows", lambda *_a, extra_params=None, **_k: (rows, 1))
    monkeypatch.setattr(plex_collection, "active_pms_token", lambda _a: None)
    idx = plex_collection.build_index(_plex_collection_adapter(True))

    assert all("hdr" not in v["media_info"] for v in idx.values())


def test_pair_option_injects_media_info_for_collection_only() -> None:
    from cw_platform.orchestrator._pairs import _config_with_pair_feature_options

    cfg = {"plex": {"server_url": "x"}}
    on = _config_with_pair_feature_options(cfg, {"enable": True, "media_info": True}, ("PLEX", "TRAKT"), "collection")
    off = _config_with_pair_feature_options(cfg, {"enable": True}, ("PLEX", "TRAKT"), "collection")
    history = _config_with_pair_feature_options(cfg, {"enable": True, "media_info": True}, ("PLEX", "TRAKT"), "history")

    assert on["plex"]["collection"]["media_info"] is True
    assert "collection" not in off["plex"]
    assert "collection" not in history["plex"]
    assert cfg == {"plex": {"server_url": "x"}}


@pytest.mark.parametrize(
    ("video", "expected"),
    [
        ({"VideoRangeType": "DOVIWithHDR10"}, "dolby_vision"),
        ({"VideoRangeType": "DOVIInvalid", "ColorTransfer": "smpte2084"}, "hdr10"),
        ({"VideoRangeType": "HDR10Plus"}, "hdr10_plus"),
        ({"VideoRangeType": "HLG"}, "hlg"),
        ({"VideoRangeType": "SDR", "VideoRange": "SDR"}, ""),
        ({"ExtendedVideoType": "DolbyVision", "VideoRange": "DolbyVision"}, "dolby_vision"),
        ({"ExtendedVideoType": "Hdr10", "VideoRange": "HDR 10"}, "hdr10"),
        ({"ExtendedVideoType": "None", "VideoRange": "SDR", "ColorTransfer": "bt709"}, ""),
    ],
)
def test_stream_hdr(video: dict, expected: str) -> None:
    assert cm.stream_hdr(video) == expected


@pytest.mark.parametrize(
    ("stream", "expected"),
    [
        ({"Codec": "eac3", "Profile": "Dolby Digital Plus + Dolby Atmos"}, "dolby_digital_plus_atmos"),
        ({"Codec": "eac3", "AudioSpatialFormat": "DolbyAtmos"}, "dolby_digital_plus_atmos"),
        ({"Codec": "truehd", "AudioSpatialFormat": "DolbyAtmos"}, "dolby_atmos"),
        ({"Codec": "eac3", "Title": "English DDP Atmos 5.1"}, "dolby_digital_plus_atmos"),
        ({"Codec": "eac3", "Title": "English DDP 5.1"}, "dolby_digital_plus"),
        ({"Codec": "dts", "Profile": "DTS-HD MA", "AudioSpatialFormat": "DTSX"}, "dts_x"),
        ({"Codec": "dts", "Profile": "DTS-HD MA", "Title": "DTS-HD MA 7.1"}, "dts_ma"),
        ({"Codec": "pcm_s24le"}, "lpcm"),
    ],
)
def test_stream_audio(stream: dict, expected: str) -> None:
    assert cm.stream_audio(stream) == expected


def test_stream_media_info_picks_best_audio_and_skips_commentary() -> None:
    item = {
        "Width": 1920,
        "Height": 800,
        "MediaSources": [
            {
                "MediaStreams": [
                    {"Type": "Video", "Width": 1920, "Height": 800, "VideoRangeType": "SDR"},
                    {"Type": "Audio", "Codec": "ac3", "Channels": 6, "ChannelLayout": "5.1", "IsDefault": True},
                    {"Type": "Audio", "Codec": "truehd", "AudioSpatialFormat": "DolbyAtmos", "Channels": 8, "ChannelLayout": "7.1"},
                    {"Type": "Audio", "Codec": "flac", "Channels": 8, "ChannelLayout": "7.1", "Title": "Director's Commentary"},
                    {"Type": "Subtitle", "Codec": "subrip"},
                ]
            }
        ],
    }
    assert cm.stream_media_info(item) == {"resolution": "hd_1080p", "audio": "dolby_atmos", "audio_channels": "7.1"}


def test_stream_media_info_uses_channel_count_when_layout_is_odd() -> None:
    item = {"MediaStreams": [{"Type": "Video", "Width": 3840, "Height": 2160, "ExtendedVideoType": "Hdr10"}, {"Type": "Audio", "Codec": "eac3", "Channels": 6, "ChannelLayout": "5.1(side)"}], "Video3DFormat": "HalfSideBySide"}
    assert cm.stream_media_info(item) == {"resolution": "uhd_4k", "hdr": "hdr10", "audio": "dolby_digital_plus", "audio_channels": "5.1", "is_3d": True}


@pytest.mark.parametrize("media_info", [False, True])
def test_emby_collection_reads_media_info_only_when_enabled(monkeypatch: pytest.MonkeyPatch, media_info: bool) -> None:
    from types import SimpleNamespace

    from providers.sync.emby import _collection as emby_collection

    requested: list[dict] = []

    class _Resp:
        status_code = 200

        def __init__(self, body):
            self._body = body

        def json(self):
            return self._body

    movie = {
        "Id": "1",
        "Type": "Movie",
        "Name": "Ant-Man",
        "ProductionYear": 2015,
        "ProviderIds": {"Tmdb": "102899"},
        "Width": 3840,
        "Height": 2160,
        "MediaSources": [{"MediaStreams": [{"Type": "Video", "ExtendedVideoType": "Hdr10", "VideoRange": "HDR 10"}, {"Type": "Audio", "Codec": "eac3", "Title": "English DDP Atmos 5.1", "Channels": 6, "ChannelLayout": "5.1"}]}],
    }

    def fake_get(path, params=None):
        requested.append(dict(params or {}))
        return _Resp({"Items": [movie], "TotalRecordCount": 1})

    adapter = SimpleNamespace(client=SimpleNamespace(get=fake_get), cfg=SimpleNamespace(user_id="u1", collection_media_info=media_info, collection_libraries=None))
    monkeypatch.setattr(emby_collection, "emby_selected_library_ids", lambda *_a, **_k: set())
    monkeypatch.setattr(emby_collection, "emby_get_library_roots", lambda _a: {})
    monkeypatch.setattr(emby_collection, "_series_ids_by_item_id", lambda *_a: {})
    monkeypatch.setattr(emby_collection, "emby_resolve_library_id", lambda *_a, **_k: None)
    idx = emby_collection.build_index(adapter)
    item = next(iter(idx.values()))

    assert ("MediaSources" in requested[0]["Fields"]) is media_info
    if not media_info:
        assert "media_info" not in item and "format" not in item
        return
    assert item["format"] == "digital"
    assert item["media_info"] == {"resolution": "uhd_4k", "hdr": "hdr10", "audio": "dolby_digital_plus_atmos", "audio_channels": "5.1"}


@pytest.mark.parametrize("media_info", [False, True])
def test_jellyfin_collection_reads_media_info_only_when_enabled(monkeypatch: pytest.MonkeyPatch, media_info: bool) -> None:
    from types import SimpleNamespace

    from providers.sync.jellyfin import _collection as jf_collection

    requested: list[dict] = []

    class _Resp:
        status_code = 200

        def __init__(self, body):
            self._body = body

        def json(self):
            return self._body

    movie = {
        "Id": "1",
        "Type": "Movie",
        "Name": "A Perfect World",
        "ProductionYear": 1993,
        "ProviderIds": {"Tmdb": "9559"},
        "Width": 3840,
        "Height": 2076,
        "MediaStreams": [
            {"Type": "Video", "VideoRangeType": "DOVIWithHDR10"},
            {"Type": "Audio", "Codec": "eac3", "Profile": "Dolby Digital Plus + Dolby Atmos", "AudioSpatialFormat": "DolbyAtmos", "Channels": 6, "ChannelLayout": "5.1"},
        ],
    }

    def fake_get(path, params=None):
        requested.append(dict(params or {}))
        return _Resp({"Items": [movie], "TotalRecordCount": 1})

    adapter = SimpleNamespace(client=SimpleNamespace(get=fake_get), cfg=SimpleNamespace(user_id="u1", collection_media_info=media_info, collection_libraries=None))
    monkeypatch.setattr(jf_collection, "jf_selected_library_ids", lambda *_a, **_k: set())
    monkeypatch.setattr(jf_collection, "jf_get_library_roots", lambda _a: {})
    monkeypatch.setattr(jf_collection, "_series_ids_by_item_id", lambda *_a: {})
    monkeypatch.setattr(jf_collection, "jf_resolve_library_id", lambda *_a, **_k: None)
    idx = jf_collection.build_index(adapter)
    item = next(iter(idx.values()))
    fields = next(p["Fields"] for p in requested if "Fields" in p)

    assert ("MediaStreams" in fields) is media_info
    assert "MediaSources" not in fields
    if not media_info:
        assert "media_info" not in item and "format" not in item
        return
    assert item["format"] == "digital"
    assert item["media_info"] == {"resolution": "uhd_4k", "hdr": "dolby_vision", "audio": "dolby_digital_plus_atmos", "audio_channels": "5.1"}


def _plex_hdr_index(monkeypatch: pytest.MonkeyPatch, rows: list[dict], filters: dict) -> dict:
    from providers.sync.plex import _collection as plex_collection

    def fake_fetch(_srv, _sid, _plex_type, *, extra_params=None, **_kwargs):
        flt = next(iter(dict(extra_params or {})), None)
        if flt is None:
            return rows, 1
        result = filters.get(flt, [])
        if isinstance(result, Exception):
            raise result
        return result, 1

    monkeypatch.setattr(plex_collection, "_fetch_section_guid_rows", fake_fetch)
    monkeypatch.setattr(plex_collection, "active_pms_token", lambda _a: None)
    return plex_collection.build_index(_plex_collection_adapter(True))


def test_plex_hdr_ignored_filters_on_single_item_library(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = [_plex_movie_row("10", "603", 1920, "ac3", "")]
    idx = _plex_hdr_index(monkeypatch, rows, {"dovi": rows, "hdr10plus": rows, "hdr": rows})
    assert "hdr" not in idx["tmdb:603"]["media_info"]


def test_plex_hdr_kept_for_library_where_every_item_is_hdr10(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = [_plex_movie_row("10", "603", 3840, "eac3", ""), _plex_movie_row("11", "550", 3840, "eac3", "")]
    idx = _plex_hdr_index(monkeypatch, rows, {"dovi": [], "hdr10plus": [], "hdr": rows})
    assert {v["media_info"]["hdr"] for v in idx.values()} == {"hdr10"}


def test_plex_hdr_skipped_when_a_filter_query_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = [_plex_movie_row("10", "603", 3840, "eac3", "")]
    idx = _plex_hdr_index(monkeypatch, rows, {"dovi": RuntimeError("boom"), "hdr": rows})
    assert "hdr" not in idx["tmdb:603"]["media_info"]


def test_plex_media_info_keeps_interlaced_scan() -> None:
    row = {"Media": [{"width": 1920, "height": 1080, "videoResolution": "1080i", "audioCodec": "ac3", "audioChannels": 6}]}
    assert cm.plex_media_info(row)["resolution"] == "hd_1080i"


TRAKT_DST = {"type": "movie", "title": "Ant-Man", "ids": {"tmdb": "102899", "trakt": "77"}, "collected_at": "2024-01-01T00:00:00.000Z", "format": "digital", "media_info": {"resolution": "hd_1080p", "audio": "dolby_digital_plus", "audio_channels": "5.1"}}


def test_media_update_merges_changes_and_keeps_destination_values() -> None:
    src = {"type": "movie", "ids": {"tmdb": "102899"}, "format": "digital", "media_info": {"resolution": "uhd_4k", "hdr": "hdr10"}}
    upd = cm.media_update(src, TRAKT_DST)

    assert upd["_cw_media_update"] is True
    assert upd["ids"] == TRAKT_DST["ids"]
    assert upd["collected_at"] == "2024-01-01T00:00:00.000Z"
    assert upd["media_info"] == {"resolution": "uhd_4k", "audio": "dolby_digital_plus", "audio_channels": "5.1", "hdr": "hdr10"}
    assert upd["format"] == "digital"


@pytest.mark.parametrize(
    "src",
    [
        {"format": "digital", "media_info": {"resolution": "hd_1080p", "audio": "dolby_digital_plus"}},
        {"format": "digital", "media_info": {}},
        {"media_info": {"resolution": "1440p"}},
        {"format": "Digital", "media_info": {"resolution": "1080p", "audio": "DD+"}},
    ],
)
def test_media_update_skips_equal_empty_or_unknown_values(src: dict) -> None:
    assert cm.media_update({"type": "movie", "ids": {"tmdb": "102899"}, **src}, TRAKT_DST) is None


def test_media_update_never_touches_physical_destination_from_digital_source() -> None:
    dst = {**TRAKT_DST, "format": "bluray"}
    assert cm.media_update({"format": "digital", "media_info": {"resolution": "uhd_4k"}}, dst) is None
    assert cm.media_update({"format": "4k_bluray", "media_info": {}}, dst)["format"] == "4k_bluray"


def test_media_update_treats_bluray_with_4k_as_4k_bluray() -> None:
    dst = {**TRAKT_DST, "format": "bluray", "media_info": {"resolution": "uhd_4k"}}
    assert cm.media_update({"format": "4k_bluray", "media_info": {"resolution": "uhd_4k"}}, dst) is None


def test_floppy_labels_do_not_cause_repeat_updates() -> None:
    src = {"format": "digital", "media_info": {"resolution": "uhd_4k", "hdr": "dolby_vision", "audio": "dts_x", "audio_channels": "7.1.4"}}
    floppy_dst = {"format": "digital", "media_info": cm.media_block({"resolution": "4K", "hdr": "Dolby Vision", "audio": "DTS:X", "audio_channels": "7.1.4"})}
    assert cm.media_update(src, floppy_dst) is None


class _FloppyStub:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict]] = []

    def request(self, method, url, headers=None, timeout=None, verify=None, json=None, **_kw):
        from types import SimpleNamespace

        self.calls.append((method, url.rsplit("/api/v1/", 1)[-1], dict(json or {})))
        return SimpleNamespace(status_code=200, headers={"content-type": "application/json"}, json=lambda: {"id": 5}, text="{}")


def _floppy_adapter(stub):
    from types import SimpleNamespace

    return SimpleNamespace(client=SimpleNamespace(api_base="http://floppy/api/v1", api_token="t", session=stub, timeout=5, verify_ssl=False), config={})


def test_floppy_update_patches_existing_entry() -> None:
    from providers.sync.floppy import _collection as floppy_collection

    stub = _FloppyStub()
    dst = {"type": "movie", "ids": {"tmdb": "102899"}, "format": "digital", "_floppy_collection_id": "5", "_floppy_collection_ids": ["5"], "media_info": {"resolution": "hd_1080p"}}
    upd = cm.media_update({"format": "digital", "media_info": {"resolution": "uhd_4k", "audio": "dolby_atmos"}}, dst)
    res = floppy_collection.add(_floppy_adapter(stub), [upd])

    assert res["count"] == 1
    assert stub.calls == [("PATCH", "collection/5", {"media_type": "digital", "resolution": "4K", "audio_codec": "Dolby Atmos"})]


def test_floppy_update_without_entry_id_is_unresolved_not_posted() -> None:
    from providers.sync.floppy import _collection as floppy_collection

    stub = _FloppyStub()
    res = floppy_collection.add(_floppy_adapter(stub), [{"type": "movie", "ids": {"tmdb": "102899"}, "_cw_media_update": True, "media_info": {"resolution": "uhd_4k"}}])

    assert res["count"] == 0
    assert stub.calls == []


def test_floppy_plain_add_with_foreign_entry_id_still_posts() -> None:
    from providers.sync.floppy import _collection as floppy_collection

    stub = _FloppyStub()
    floppy_collection.add(_floppy_adapter(stub), [{"type": "movie", "ids": {"tmdb": "102899"}, "_floppy_collection_id": "5"}])

    assert stub.calls[0][:2] == ("POST", "media/movie")
    assert "PATCH" not in [c[0] for c in stub.calls]


def test_update_marker_is_not_sent_to_trakt() -> None:
    from providers.sync.trakt._common import build_watchlist_body

    upd = cm.media_update({"format": "digital", "media_info": {"resolution": "uhd_4k"}}, TRAKT_DST)
    body = build_watchlist_body([minimal(upd)], date_field="collected_at", item_fields=cm.to_trakt_metadata)

    assert "_cw_media_update" not in json.dumps(body)
    assert body["movies"][0]["resolution"] == "uhd_4k" and body["movies"][0]["audio"] == "dolby_digital_plus"
    assert body["movies"][0]["collected_at"] == "2024-01-01T00:00:00.000Z"


@pytest.mark.parametrize(("flag", "option"), [(True, True), (True, False), (False, True)])
def test_oneway_collection_sends_media_update_only_when_enabled_and_capable(config_base, monkeypatch: pytest.MonkeyPatch, flag: bool, option: bool) -> None:
    from cw_platform.id_map import canonical_key
    from cw_platform.orchestrator import Orchestrator
    from test_interactive_sync import item
    from test_interactive_sync_features import feature_setup

    source = item(1, format="digital", media_info={"resolution": "uhd_4k", "hdr": "hdr10"})
    target = item(1, format="digital", collected_at="2024-01-01T00:00:00Z", media_info={"resolution": "hd_1080p", "audio": "aac"})
    unchanged = item(2, format="digital", media_info={"resolution": "hd_1080p"})
    cfg, src, dst = feature_setup(config_base, monkeypatch, "collection", [source, unchanged], [target, dict(unchanged)], "one-way")
    cfg["pairs"][0]["features"]["collection"]["media_updates"] = option
    collection_caps = {"media_updates": True} if flag else {}
    dst.capabilities = lambda: {"features": {"collection": True}, "index_semantics": "present", "collection": collection_caps}
    monkeypatch.setattr("cw_platform.orchestrator._pairs.record_health", lambda *args: None)

    assert Orchestrator(cfg).run()["ok"] is True
    writes = [row for batch in dst.add_calls for row in batch]
    if not (flag and option):
        assert writes == []
        return
    assert len(writes) == 1
    assert canonical_key(writes[0]) == canonical_key(target)
    assert writes[0]["media_info"] == {"resolution": "uhd_4k", "audio": "aac", "hdr": "hdr10"}
    assert writes[0]["collected_at"] == "2024-01-01T00:00:00Z"

    dst.add_calls.clear()
    assert Orchestrator(cfg).run()["ok"] is True
    assert [row for batch in dst.add_calls for row in batch] == []


def _write_and_reread(update: dict, destination: str) -> dict:
    payload = minimal(update)
    if destination == "trakt":
        fmt, media_info = cm.from_trakt_metadata({**cm.to_trakt_metadata(payload), "3d": 1 if cm.to_trakt_metadata(payload).get("3d") else None})
    else:
        fields = cm.to_floppy_fields(payload)
        fmt = str(payload.get("format") or "")
        media_info = cm.media_block({"resolution": fields.get("resolution"), "hdr": fields.get("hdr"), "audio": fields.get("audio_codec"), "audio_channels": fields.get("audio_channels"), "is_3d": True if fields.get("is_3d") else None})
    return {**update, "format": fmt, "media_info": media_info}


@pytest.mark.parametrize("destination", ["trakt", "floppy"])
@pytest.mark.parametrize(
    ("src", "dst"),
    [
        ({"format": "bluray", "media_info": {}}, {"format": "bluray", "media_info": {"resolution": "uhd_4k"}}),
        ({"format": "4k_bluray", "media_info": {}}, {"format": "bluray", "media_info": {"resolution": "hd_1080p"}}),
        ({"format": "4k_bluray", "media_info": {"hdr": "hdr10"}}, {"format": "digital", "media_info": {"resolution": "hd_1080p"}}),
        ({"format": "bluray", "media_info": {"resolution": "uhd_4k"}}, {"format": "4k_bluray", "media_info": {}}),
        ({"format": "digital", "media_info": {"resolution": "uhd_4k", "audio": "dts_x", "audio_channels": "7.1.4"}}, {"format": "", "media_info": {"resolution": "hd_1080p"}}),
    ],
)
def test_media_update_settles_after_one_write(src: dict, dst: dict, destination: str) -> None:
    dst = {"type": "movie", "ids": {"tmdb": "603"}, **dst}
    upd = cm.media_update(src, dst)
    current = _write_and_reread(upd, destination) if upd else dst
    assert cm.media_update(src, current) is None


def test_media_update_bluray_without_resolution_keeps_4k_destination() -> None:
    assert cm.media_update({"format": "bluray", "media_info": {}}, {"format": "bluray", "media_info": {"resolution": "uhd_4k"}}) is None


def test_media_update_4k_bluray_source_upgrades_1080p_destination() -> None:
    upd = cm.media_update({"format": "4k_bluray", "media_info": {}}, {"format": "bluray", "media_info": {"resolution": "hd_1080p"}})
    assert upd["media_info"]["resolution"] == "uhd_4k"
    assert cm.to_trakt_metadata(upd) == {"media_type": "bluray", "resolution": "uhd_4k"}


def _audio_item(*tracks: dict) -> dict:
    return {"Width": 1920, "Height": 1080, "MediaStreams": [{"Type": "Video", "VideoRangeType": "SDR"}, *({"Type": "Audio", **t} for t in tracks)]}


@pytest.mark.parametrize(
    ("tracks", "audio", "channels"),
    [
        (
            [{"Codec": "eac3", "Language": "eng", "Channels": 8, "ChannelLayout": "7.1", "IsDefault": True, "AudioSpatialFormat": "None"}, {"Codec": "flac", "Language": "eng", "Channels": 2, "ChannelLayout": "stereo", "AudioSpatialFormat": "None"}],
            "dolby_digital_plus",
            "7.1",
        ),
        (
            [{"Codec": "ac3", "Language": "eng", "Channels": 6, "IsDefault": True, "AudioSpatialFormat": "None"}, {"Codec": "flac", "Language": "eng", "Channels": 1, "ChannelLayout": "mono", "AudioSpatialFormat": "None"}],
            "dolby_digital",
            "5.1",
        ),
        (
            [{"Codec": "ac3", "Language": "eng", "Channels": 2, "IsDefault": True, "AudioSpatialFormat": "None"}, {"Codec": "flac", "Language": "zxx", "Channels": 3, "ChannelLayout": "2.1", "Title": "Isolated Score", "AudioSpatialFormat": "None"}],
            "dolby_digital",
            "2.0",
        ),
        (
            [{"Codec": "ac3", "Language": "jpn", "Channels": 6, "IsDefault": True, "AudioSpatialFormat": "None"}, {"Codec": "dts", "Profile": "DTS-HD MA", "Language": "eng", "Channels": 6, "AudioSpatialFormat": "None"}],
            "dolby_digital",
            "5.1",
        ),
        (
            [{"Codec": "eac3", "Profile": "Dolby Digital Plus", "Language": "eng", "Channels": 8, "ChannelLayout": "7.1", "Title": "Dolby Atmos 7.1 Upmix", "IsDefault": True, "AudioSpatialFormat": "None"}],
            "dolby_digital_plus",
            "7.1",
        ),
        (
            [{"Codec": "ac3", "Language": "eng", "Channels": 6, "IsDefault": True, "AudioSpatialFormat": "None"}, {"Codec": "truehd", "Language": "eng", "Channels": 8, "ChannelLayout": "7.1", "AudioSpatialFormat": "DolbyAtmos"}],
            "dolby_atmos",
            "7.1",
        ),
        (
            [{"Codec": "ac3", "Language": "eng", "Channels": 6, "IsDefault": True, "AudioSpatialFormat": "None"}, {"Codec": "eac3", "Language": "eng", "Channels": 6, "Title": "Audio Description", "AudioSpatialFormat": "DolbyAtmos"}],
            "dolby_digital",
            "5.1",
        ),
        (
            [{"Codec": "eac3", "Language": "eng", "Channels": 6, "ChannelLayout": "5.1", "Title": "English DDP Atmos 5.1", "IsDefault": True}],
            "dolby_digital_plus_atmos",
            "5.1",
        ),
    ],
    ids=["lossless_stereo_vs_71_default", "lossless_mono_vs_51_default", "isolated_score_zxx", "other_language_dub", "atmos_title_with_spatial_none", "same_language_lossless_atmos_upgrade", "audio_description_skipped", "emby_title_fallback"],
)
def test_stream_audio_track_choice(tracks: list[dict], audio: str, channels: str) -> None:
    info = cm.stream_media_info(_audio_item(*tracks))
    assert (info.get("audio"), info.get("audio_channels")) == (audio, channels)
