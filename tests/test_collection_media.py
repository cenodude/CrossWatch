# tests/test_collection_media.py
# CrossWatch - Collection media info normalization and Trakt mapping
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

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
