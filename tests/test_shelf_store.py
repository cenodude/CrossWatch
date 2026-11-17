# tests/test_shelf_store.py
# CrossWatch - Personal shelf store, shelf endpoints and shelf modes in the Media Hub collection
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from api import profileAPI
from services import shelf_store

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def shelf_home(tmp_path, monkeypatch):
    monkeypatch.setenv("CONFIG_BASE", str(tmp_path / "cfg"))
    shelf_store.clear_memory()
    yield tmp_path / "cfg"
    shelf_store.clear_memory()


def _baseline(items: dict) -> dict:
    return {"providers": {"PLEX": {"collection": {"baseline": {"items": items}}}}}


def test_put_validates_and_keeps_history(shelf_home) -> None:
    entry = shelf_store.put("movie", "603", {
        "status": "owned",
        "title": "The Matrix",
        "year": "1999",
        "ids": {"imdb": "tt0133093", "bad": "x", "tvdb": "<script>"},
        "copies": [
            {"format": "4k_bluray", "packaging": "steelbook", "condition": "mint", "discs": "2", "region": "B", "notes": "Line one\n  Line   two", "purchased": "2024-05-01", "lent_to": "  Mark  "},
            {"format": "dvd", "packaging": "cardboard", "condition": "broken", "discs": 500, "purchased": "yesterday"},
        ],
    })
    assert entry["type"] == "movie" and entry["year"] == 1999
    assert entry["ids"] == {"imdb": "tt0133093", "tmdb": "603"}
    first, second = entry["copies"]
    assert first["notes"] == "Line one\nLine two" and first["lent_to"] == "Mark" and first["discs"] == 2
    assert len(first["id"]) == 12 and first["id"] != second["id"]
    assert set(second) == {"format", "id", "added_at"}

    saved = json.loads((shelf_home / ".cw_state" / "shelf.json").read_text("utf-8"))
    assert saved["items"]["movie:603"]["copies"][0]["format"] == "4k_bluray"

    again = shelf_store.put("movie", 603, {"status": "ordered", "copies": [first]})
    assert again["title"] == "The Matrix" and again["year"] == 1999 and again["added_at"] == entry["added_at"]
    assert again["copies"][0]["id"] == first["id"] and again["copies"][0]["added_at"] == first["added_at"]

    shelf_store.clear_memory()
    assert shelf_store.get("movie", 603)["status"] == "ordered"
    assert shelf_store.remove("movie", 603) is True
    assert shelf_store.remove("movie", 603) is False
    assert shelf_store.get("movie", 603) is None


def test_put_rejects_bad_input() -> None:
    for entity, tmdb, payload in (
        ("movie", "abc", {"status": "owned"}),
        ("movie", 0, {"status": "owned"}),
        ("movie", 1, {"status": "stolen"}),
        ("movie", 1, {"copies": [{"format": "betacam"}]}),
        ("movie", 1, {"copies": [{"format": "bluray_3d"}]}),
        ("movie", 1, {"copies": ["dvd"]}),
        ("movie", 1, {"copies": [{"format": "dvd"}] * (shelf_store.MAX_COPIES + 1)}),
    ):
        with pytest.raises(shelf_store.ShelfError):
            shelf_store.put(entity, tmdb, payload)
    assert shelf_store.entries() == {}


def test_wishlist_without_copies_and_summary() -> None:
    shelf_store.put("show", 1399, {"status": "wishlist", "title": "Game of Thrones"})
    shelf_store.put("movie", 10, {"title": "Box", "copies": [{"format": "dvd"}, {"format": "4k_bluray", "packaging": "steelbook", "lent_to": "Ann"}, {"format": "dvd"}]})
    assert shelf_store.summaries() == {
        "tv:1399": {"status": "wishlist", "copies": 0, "formats": []},
        "movie:10": {"status": "owned", "copies": 3, "formats": ["4k_bluray", "dvd"], "per_format": {"4k_bluray": 1, "dvd": 2}, "packaging": "steelbook", "lent": True},
    }
    assert shelf_store.get("tv", 1399)["type"] == "show"


def test_collection_shelf_modes() -> None:
    state = _baseline({
        "tmdb:10": {"type": "movie", "title": "Synced Owned", "ids": {"tmdb": "10"}, "collected_at": "2026-01-02T00:00:00Z"},
        "tmdb:20": {"type": "movie", "title": "Synced Only", "ids": {"tmdb": "20"}, "collected_at": "2026-01-03T00:00:00Z"},
    })
    shelf_store.put("movie", 10, {"title": "Synced Owned", "copies": [{"format": "bluray"}]})
    shelf_store.put("show", 30, {"title": "Shelf Only Show", "year": 2020, "status": "ordered"})

    synced = profileAPI.build_profile_collection_payload(state, {})
    assert synced["shelf"] == "synced" and synced["total"] == 2
    by_title = {item["title"]: item for item in synced["items"]}
    assert by_title["Synced Owned"]["shelf"]["formats"] == ["bluray"]
    assert "shelf" not in by_title["Synced Only"]

    shelf = profileAPI.build_profile_collection_payload(state, {}, shelf="shelf", include_keys=True)
    assert sorted(item["title"] for item in shelf["items"]) == ["Shelf Only Show", "Synced Owned"]
    assert shelf["counts"]["movie"] == 1 and shelf["counts"]["show"] == 1
    only = next(item for item in shelf["items"] if item.get("shelf_only"))
    assert only["key"] == "shelf:tv:30" and only["type"] == "show" and only["providers"] == []
    assert only["ids"]["tmdb"] == "30" and only["shelf"]["status"] == "ordered"
    assert [row["key"] for row in shelf["selection"]] == ["tmdb:10"]

    both = profileAPI.build_profile_collection_payload(state, {}, shelf="all", search="shelf only")
    assert [item["title"] for item in both["items"]] == ["Shelf Only Show"]
    assert profileAPI.build_profile_collection_payload(state, {}, shelf="all")["total"] == 3
    assert profileAPI.build_profile_collection_payload(state, {}, shelf="bogus")["shelf"] == "synced"

    cached = profileAPI.build_profile_collection_payload(state, {}, cache_key=("x",))
    assert any("shelf" in item for item in cached["items"])
    shelf_store.remove("movie", 10)
    after = profileAPI.build_profile_collection_payload(state, {}, cache_key=("x",))
    assert not any("shelf" in item for item in after["items"])


def test_collection_shelf_filters() -> None:
    state = {"providers": {
        "PLEX": {"collection": {"baseline": {"items": {
            "tmdb:1": {"type": "movie", "title": "Ripped", "ids": {"tmdb": "1"}},
            "tmdb:2": {"type": "movie", "title": "Server Only", "ids": {"tmdb": "2"}},
        }}}},
        "TRAKT": {"collection": {"baseline": {"items": {
            "tmdb:3": {"type": "movie", "title": "Tracker Only", "ids": {"tmdb": "3"}},
        }}}},
    }}
    shelf_store.put("movie", 1, {"title": "Ripped", "copies": [{"format": "bluray"}]})
    shelf_store.put("movie", 3, {"title": "Tracker Only", "copies": [{"format": "dvd"}]})
    shelf_store.put("movie", 4, {"title": "Disc Only", "copies": [{"format": "4k_bluray"}]})

    def titles(mode: str, **kw: Any) -> list[str]:
        return sorted(item["title"] for item in profileAPI.build_profile_collection_payload(state, {}, shelf=mode, **kw)["items"])

    assert titles("not_on_server") == ["Disc Only", "Tracker Only"]
    assert titles("not_on_shelf") == ["Server Only"]
    assert titles("unwatched", watched=lambda media, tmdb: tmdb == "3") == ["Disc Only", "Ripped"]
    ripped = next(item for item in profileAPI.build_profile_collection_payload(state, {}, shelf="shelf")["items"] if item["title"] == "Ripped")
    assert ripped["shelf"]["on_server"] is True
    assert "on_server" not in shelf_store.summaries()["movie:1"]


def test_shelf_stats_and_locations() -> None:
    shelf_store.put("movie", 1, {"title": "A", "copies": [{"format": "bluray", "shelf": "Living B"}, {"format": "bluray", "shelf": "Attic", "lent_to": "Kim"}]})
    shelf_store.put("movie", 2, {"title": "B", "copies": [{"format": "4k_bluray", "shelf": "Living A"}]})
    shelf_store.put("tv", 3, {"title": "C", "status": "wishlist"})
    assert shelf_store.summaries()["movie:1"]["shelves"] == ["Living B", "Attic"]
    payload = profileAPI.build_profile_collection_payload({}, {}, shelf="shelf")
    assert payload["shelf_stats"] == {"titles": 3, "copies": 3, "formats": {"bluray": 2, "4k_bluray": 1}, "lent": 1, "spent": 0.0, "currency": ""}
    assert "shelf_stats" not in profileAPI.build_profile_collection_payload({}, {})


def test_prices_currency_and_spending() -> None:
    shelf_store.put("movie", 1, {"title": "A", "copies": [{"format": "bluray", "price": "19,99", "store": "Shop"}, {"format": "dvd", "price": -3}, {"format": "dvd", "price": "abc"}]})
    copies = shelf_store.get("movie", 1)["copies"]
    assert copies[0]["price"] == 19.99 and copies[0]["store"] == "Shop"
    assert "price" not in copies[1] and "price" not in copies[2]
    shelf_store.put("movie", 2, {"title": "B", "copies": [{"format": "4k_bluray", "price": 30.01}]})
    assert shelf_store.summaries()["movie:1"]["spent"] == 19.99
    assert shelf_store.set_currency("eur") == "EUR"
    with pytest.raises(shelf_store.ShelfError):
        shelf_store.set_currency("euro")
    shelf_store.clear_memory()
    assert shelf_store.currency() == "EUR"
    stats = profileAPI.build_profile_collection_payload({}, {}, shelf="shelf")["shelf_stats"]
    assert stats["spent"] == 50.0 and stats["currency"] == "EUR"
    assert shelf_store.set_currency("") == "" and shelf_store.currency() == ""


def test_shelf_index_marks_server_titles(monkeypatch) -> None:
    state = _baseline({"tmdb:7": {"type": "movie", "title": "Ripped", "ids": {"tmdb": "7"}}})
    shelf_store.put("movie", 7, {"title": "Ripped", "copies": [{"format": "bluray"}]})
    shelf_store.put("movie", 8, {"title": "Disc", "copies": [{"format": "dvd"}]})
    monkeypatch.setattr(profileAPI, "_collection_state", lambda *a, **k: state)
    monkeypatch.setattr(profileAPI, "collection_cache_fingerprint", lambda: ("index-server",))
    monkeypatch.setattr(profileAPI, "_profile_context", lambda request: ({}, {}, profileAPI.ADMIN_USER_ID, {}, {}, ""))
    items = json.loads(profileAPI.api_profile_shelf_index(_request()).body)["items"]
    assert items["movie:7"]["on_server"] is True and "on_server" not in items["movie:8"]


def test_digital_service_and_shelf_filters() -> None:
    shelf_store.put("movie", 1, {"title": "Stream", "copies": [{"format": "digital", "service": {"id": "2", "name": "Apple TV", "logo": "/abcd1234.jpg"}, "purchased": "2024-01-02"}]})
    shelf_store.put("movie", 2, {"title": "Disc", "copies": [{"format": "dvd", "service": {"id": 2, "name": "Apple TV"}, "shelf": "Attic", "lent_to": "Kim", "price": 5, "purchased": "2025-03-04"}]})
    shelf_store.put("movie", 3, {"title": "Bad", "copies": [{"format": "digital", "service": {"id": 0, "name": "x", "logo": "../x.png"}}]})
    assert shelf_store.get("movie", 1)["copies"][0]["service"] == {"id": 2, "name": "Apple TV", "logo": "/abcd1234.jpg"}
    assert "service" not in shelf_store.get("movie", 2)["copies"][0]
    assert "service" not in shelf_store.get("movie", 3)["copies"][0]
    assert shelf_store.summaries()["movie:1"]["service"] == {"name": "Apple TV", "logo": "/abcd1234.jpg"}
    assert shelf_store.summaries()["movie:2"]["purchased"] == "2025-03-04"

    def titles(**kw: Any) -> list[str]:
        return [item["title"] for item in profileAPI.build_profile_collection_payload({}, {}, shelf="shelf", **kw)["items"]]

    assert titles(shelf_filter="lent") == ["Disc"]
    assert sorted(titles(shelf_filter="format:digital")) == ["Bad", "Stream"]
    assert titles(shelf_filter="location:Attic") == ["Disc"]
    assert sorted(titles(shelf_filter="price")) == ["Bad", "Stream"]
    assert titles(sort="price_desc")[0] == "Disc"
    assert titles(sort="purchased_desc")[:2] == ["Disc", "Stream"]
    payload = profileAPI.build_profile_collection_payload({}, {}, shelf="shelf", shelf_filter="lent")
    assert payload["shelf_locations"] == ["Attic"] and payload["total"] == 1


def _request() -> Any:
    return SimpleNamespace(cookies={}, headers={}, client=None)


def test_shelf_formats_match_collection_media() -> None:
    from cw_platform.collection_media import FORMATS

    assert shelf_store.FORMATS == FORMATS
    labels = (ROOT / "assets/helpers/media-meta.js").read_text("utf-8").split("const SHELF_FORMATS = {", 1)[1].split("};", 1)[0]
    for name in FORMATS:
        assert f'{name}:' in labels or f'"{name}":' in labels


def test_shelf_entry_includes_synced_media(monkeypatch) -> None:
    state = _baseline({"tmdb:603": {"type": "movie", "title": "The Matrix", "ids": {"tmdb": "603"}, "format": "4k_bluray", "media_info": {"hdr": "dolby_vision", "audio": "dolby_atmos"}}})
    monkeypatch.setattr(profileAPI, "_collection_state", lambda *a, **k: state)
    monkeypatch.setattr(profileAPI, "collection_cache_fingerprint", lambda: ("synced-media",))
    monkeypatch.setattr(profileAPI, "_profile_context", lambda request: ({}, {}, profileAPI.ADMIN_USER_ID, {}, {}, ""))
    found = json.loads(profileAPI.api_profile_shelf_entry(_request(), "movie", 603).body)
    assert found["synced"] == {"providers": ["plex"], "format": "4k_bluray", "media_info": {"hdr": "dolby_vision", "audio": "dolby_atmos"}}
    assert "synced" not in json.loads(profileAPI.api_profile_shelf_entry(_request(), "movie", 604).body)


def test_shelf_endpoints_need_admin_for_changes(monkeypatch) -> None:
    monkeypatch.setattr(profileAPI, "_collection_state", lambda *a, **k: {})
    monkeypatch.setattr(profileAPI, "collection_cache_fingerprint", lambda: ("endpoints",))
    monkeypatch.setattr(profileAPI, "_origin_allowed", lambda request: True)
    monkeypatch.setattr(profileAPI, "_profile_context", lambda request: ({}, {}, "alice", {}, {}, ""))
    blocked = profileAPI.api_profile_shelf_save(_request(), "movie", 603, {"status": "owned"})
    assert blocked.status_code == 403 and shelf_store.entries() == {}
    view = json.loads(profileAPI.api_profile_shelf_entry(_request(), "movie", 603).body)
    assert view == {"ok": True, "entry": None, "can_edit": False}

    monkeypatch.setattr(profileAPI, "_profile_context", lambda request: ({}, {}, profileAPI.ADMIN_USER_ID, {}, {}, ""))
    saved = json.loads(profileAPI.api_profile_shelf_save(_request(), "show", 1399, {"status": "wishlist", "title": "GoT"}).body)
    assert saved["ok"] and saved["summary"] == {"status": "wishlist", "copies": 0, "formats": []}
    assert json.loads(profileAPI.api_profile_shelf_index(_request()).body)["items"] == {"tv:1399": saved["summary"]}
    assert profileAPI.api_profile_shelf_save(_request(), "book", 1, {}).status_code == 400
    assert profileAPI.api_profile_shelf_save(_request(), "movie", 5, {"copies": [{"format": "nope"}]}).status_code == 400
    assert json.loads(profileAPI.api_profile_shelf_remove(_request(), "tv", 1399).body) == {"ok": True, "removed": True}


def test_shelf_ui_is_wired() -> None:
    html = (ROOT / "ui_frontend.py").read_text("utf-8")
    assert 'id="profile-collection-shelf"' in html and 'data-collection-display="case"' in html and 'data-collection-display="shelf"' in html
    modal = (ROOT / "assets/js/profile-media-modal.js").read_text("utf-8")
    assert 'lazyScript("shelf-editor.js", "ShelfEditor")' in modal and "data-mm-shelf" in modal
    assert "window.CW.Shelf = {" in (ROOT / "assets/helpers/media-meta.js").read_text("utf-8")
    assert "CW ||= {}).ShelfEditor" in (ROOT / "assets/js/shelf-editor.js").read_text("utf-8")


def test_watch_providers_are_cached_per_region(monkeypatch) -> None:
    from api import metaAPI

    metaAPI._WATCH_PROVIDERS.clear()
    calls: list[dict] = []
    data = {"results": [
        {"provider_id": 3, "provider_name": "Google Play", "logo_path": "/google12.jpg", "display_priorities": {"NL": 5}},
        {"provider_id": 2, "provider_name": "Apple TV", "logo_path": "/apple123.jpg", "display_priorities": {"NL": 1}},
        {"provider_id": 0, "provider_name": "Broken"},
        {"provider_id": 9, "provider_name": "No Logo", "logo_path": "../x.svg", "display_priority": 9},
    ]}
    monkeypatch.setattr(metaAPI, "load_config", lambda: {"tmdb": {"api_key": "t"}})
    monkeypatch.setattr(metaAPI, "_cfg_ui_locale", lambda: "nl-NL")
    monkeypatch.setattr(metaAPI, "_tmdb_get_json", lambda url, params=None, **kw: (calls.append(params or {}), data)[1])
    res = json.loads(metaAPI.api_watch_providers("movie", None).body)
    assert res["region"] == "NL"
    assert res["providers"] == [{"id": 2, "name": "Apple TV", "logo": "/apple123.jpg"}, {"id": 3, "name": "Google Play", "logo": "/google12.jpg"}, {"id": 9, "name": "No Logo"}]
    metaAPI.api_watch_providers("movie", None)
    assert len(calls) == 1 and calls[0]["watch_region"] == "NL"
    assert json.loads(metaAPI.api_watch_providers("show", "zz").body)["region"] == "ZZ"
    assert json.loads(metaAPI.api_watch_providers("movie", "1x").body)["region"] == "US"
