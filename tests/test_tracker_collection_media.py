# tests/test_tracker_collection_media.py
# CrossWatch - Manual media info on CW tracker collection items wins over synced values
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import pytest

from api import trackerAPI
from providers.sync.crosswatch import _collection

ITEM = {"type": "movie", "title": "Dune", "year": 2021, "ids": {"tmdb": "438631"}, "format": "bluray", "media_info": {"resolution": "hd_1080p"}}


def test_edit_sets_manual_media_and_reset_clears_it() -> None:
    out = trackerAPI._edited("collection", ITEM, {"media": {"format": "4k_bluray", "resolution": "UHD_4K", "hdr": "dolby_vision", "audio": "dolby_atmos", "audio_channels": "7.1", "is_3d": True}})
    assert out["format"] == "4k_bluray" and out["media_manual"] is True
    assert out["media_info"] == {"resolution": "uhd_4k", "hdr": "dolby_vision", "audio": "dolby_atmos", "audio_channels": "7.1", "is_3d": True}

    cleared = trackerAPI._edited("collection", out, {"media": {"format": "", "resolution": "", "is_3d": False}})
    assert not {"format", "media_info", "media_manual"} & set(cleared)

    only_3d = trackerAPI._edited("collection", ITEM, {"media": {"is_3d": True}})
    assert only_3d["media_info"] == {"is_3d": True} and only_3d["media_manual"] is True and "format" not in only_3d

    reset = trackerAPI._edited("collection", out, {"media": None})
    assert not {"format", "media_info", "media_manual"} & set(reset)


@pytest.mark.parametrize("changes", [
    {"media": {"format": "betacam"}},
    {"media": {"hdr": "hdr20"}},
    {"media": {"audio_channels": "99.9"}},
    {"media": "bluray"},
])
def test_edit_rejects_bad_media(changes) -> None:
    with pytest.raises(ValueError):
        trackerAPI._edited("collection", ITEM, changes)


def test_media_edit_only_for_collection() -> None:
    with pytest.raises(ValueError):
        trackerAPI._edited("history", ITEM, {"media": {"format": "dvd"}})


def test_rows_show_media_for_collection_only() -> None:
    manual = {**ITEM, "media_manual": True}
    row = trackerAPI._row("movie:438631", manual, "collection")
    assert row["format"] == "bluray" and row["media_info"] == {"resolution": "hd_1080p"} and row["media_manual"] is True
    assert "format" not in trackerAPI._row("movie:438631", manual, "history")


def test_sync_keeps_manual_media() -> None:
    existing = _collection._accepted({**ITEM, "media_manual": True})
    assert existing["media_manual"] is True and existing["format"] == "bluray"
    def incoming() -> dict:
        return _collection._accepted({**ITEM, "format": "dvd", "media_info": {"resolution": "sd_480p"}})

    kept = _collection._keep_manual_media(existing, incoming())
    assert kept["format"] == "bluray" and kept["media_info"] == {"resolution": "hd_1080p"} and kept["media_manual"] is True

    blank = _collection._accepted({**{k: v for k, v in ITEM.items() if k not in ("format", "media_info")}, "media_manual": True})
    kept_blank = _collection._keep_manual_media(blank, incoming())
    assert "format" not in kept_blank and "media_info" not in kept_blank

    synced = _collection._keep_manual_media(_collection._accepted(ITEM), incoming())
    assert synced["format"] == "dvd" and "media_manual" not in synced


def test_tracker_copies_are_validated_and_kept_by_sync() -> None:
    out = trackerAPI._edited("collection", ITEM, {"copies": [{"format": "4k_bluray", "edition": "Steelbook", "price": "19,99"}]})
    assert out["copies"][0]["format"] == "4k_bluray" and out["copies"][0]["price"] == 19.99
    assert trackerAPI._row("movie:438631", out, "collection")["copies"][0]["edition"] == "Steelbook"
    cleared = trackerAPI._edited("collection", out, {"copies": []})
    assert "copies" not in cleared
    with pytest.raises(ValueError):
        trackerAPI._edited("collection", ITEM, {"copies": [{"format": "betacam"}]})
    with pytest.raises(ValueError):
        trackerAPI._edited("history", ITEM, {"copies": []})

    existing = _collection._accepted(out)
    assert existing["copies"][0]["edition"] == "Steelbook"
    synced = _collection._keep_manual_media(existing, _collection._accepted({**ITEM, "format": "dvd"}))
    assert synced["copies"] == existing["copies"] and synced["format"] == "dvd"
