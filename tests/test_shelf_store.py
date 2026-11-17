# tests/test_shelf_store.py
# CrossWatch - Personal shelf store per user, shelf endpoints and shelf modes in the Media Hub collection
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
ADMIN = shelf_store.ADMIN_OWNER
MANAGED_ID = "0123456789abcdef0123456789abcdef"


@pytest.fixture(autouse=True)
def shelf_home(tmp_path, monkeypatch):
    monkeypatch.setenv("CONFIG_BASE", str(tmp_path / "cfg"))
    shelf_store.clear_memory()
    yield tmp_path / "cfg"
    shelf_store.clear_memory()


def _baseline(items: dict) -> dict:
    return {"providers": {"PLEX": {"collection": {"baseline": {"items": items}}}}}


def _request() -> Any:
    return SimpleNamespace(cookies={}, headers={}, client=None)


def _as(monkeypatch, uid: str, write: bool = True) -> None:
    raw = {"permissions": {"write": write}}
    monkeypatch.setattr(profileAPI, "_profile_context", lambda request: ({}, {}, uid, raw, {"permissions": {"write": write}}, ""))
    monkeypatch.setattr(profileAPI, "_origin_allowed", lambda request: True)
    monkeypatch.setattr(profileAPI, "_collection_state", lambda *a, **k: {})
    monkeypatch.setattr(profileAPI, "collection_cache_fingerprint", lambda: ("tests", uid))


def test_put_validates_and_keeps_history(shelf_home) -> None:
    entry = shelf_store.put(ADMIN, "movie", "603", {
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

    saved = json.loads((shelf_home / ".cw_state" / "shelf" / f"{ADMIN}.json").read_text("utf-8"))
    assert saved["items"]["movie:603"]["copies"][0]["format"] == "4k_bluray"

    again = shelf_store.put(ADMIN, "movie", 603, {"status": "ordered", "copies": [first]})
    assert again["title"] == "The Matrix" and again["year"] == 1999 and again["added_at"] == entry["added_at"]
    assert again["copies"][0]["id"] == first["id"] and again["copies"][0]["added_at"] == first["added_at"]

    shelf_store.clear_memory()
    assert shelf_store.get(ADMIN, "movie", 603)["status"] == "ordered"
    assert shelf_store.remove(ADMIN, "movie", 603) is True
    assert shelf_store.remove(ADMIN, "movie", 603) is False
    assert shelf_store.get(ADMIN, "movie", 603) is None


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
            shelf_store.put(ADMIN, entity, tmdb, payload)
    with pytest.raises(shelf_store.ShelfError):
        shelf_store.put("../escape", "movie", 1, {})
    assert shelf_store.entries(ADMIN) == {}


def test_shelves_are_per_user_and_legacy_file_moves_to_admin(shelf_home) -> None:
    legacy = shelf_home / ".cw_state" / "shelf.json"
    legacy.parent.mkdir(parents=True)
    legacy.write_text(json.dumps({"version": 1, "items": {"movie:5": {"title": "Old", "copies": [{"format": "dvd"}]}}}), "utf-8")
    assert shelf_store.get(ADMIN, "movie", 5)["title"] == "Old"
    assert not legacy.exists() and (shelf_home / ".cw_state" / "shelf" / f"{ADMIN}.json").exists()

    shelf_store.put(MANAGED_ID, "movie", 6, {"title": "Mine", "copies": [{"format": "bluray"}]})
    assert set(shelf_store.entries(MANAGED_ID)) == {"movie:6"}
    assert set(shelf_store.entries(ADMIN)) == {"movie:5"}
    assert shelf_store.set_currency(MANAGED_ID, "usd") == "USD" and shelf_store.currency(ADMIN) == ""


def test_wishlist_without_copies_and_summary() -> None:
    shelf_store.put(ADMIN, "show", 1399, {"status": "wishlist", "title": "Game of Thrones"})
    shelf_store.put(ADMIN, "movie", 10, {"title": "Box", "copies": [{"format": "dvd"}, {"format": "4k_bluray", "packaging": "steelbook", "lent_to": "Ann", "edition": "Collector"}, {"format": "dvd"}]})
    assert shelf_store.summaries(ADMIN) == {
        "tv:1399": {"status": "wishlist", "copies": 0, "formats": []},
        "movie:10": {"status": "owned", "copies": 3, "formats": ["4k_bluray", "dvd"], "per_format": {"4k_bluray": 1, "dvd": 2}, "packaging": "steelbook", "edition": "Collector", "lent": True},
    }
    assert shelf_store.get(ADMIN, "tv", 1399)["type"] == "show"


def test_clean_copies_for_tracker_items() -> None:
    copies = shelf_store.clean_copies([{"format": "bluray", "price": "9,50"}, {"format": "dvd", "id": "abcdefabcdef"}, {"format": "dvd", "id": "abcdefabcdef"}])
    assert copies[0]["price"] == 9.5 and copies[1]["id"] == "abcdefabcdef" and copies[2]["id"] != "abcdefabcdef"
    assert shelf_store.clean_copies(None) == []
    with pytest.raises(shelf_store.ShelfError):
        shelf_store.clean_copies([{"format": "nope"}])


def test_collection_shelf_modes() -> None:
    state = _baseline({
        "tmdb:10": {"type": "movie", "title": "Synced Owned", "ids": {"tmdb": "10"}, "collected_at": "2026-01-02T00:00:00Z"},
        "tmdb:20": {"type": "movie", "title": "Synced Only", "ids": {"tmdb": "20"}, "collected_at": "2026-01-03T00:00:00Z"},
    })
    shelf_store.put(ADMIN, "movie", 10, {"title": "Synced Owned", "copies": [{"format": "bluray"}]})
    shelf_store.put(ADMIN, "show", 30, {"title": "Shelf Only Show", "year": 2020, "status": "ordered"})
    shelf_store.put(MANAGED_ID, "movie", 20, {"title": "Synced Only", "copies": [{"format": "dvd"}]})

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
    assert [row["key"] for row in shelf["selection"]] == ["tmdb:10"]

    mine = profileAPI.build_profile_collection_payload(state, {}, shelf="shelf", shelf_owner=MANAGED_ID)
    assert [item["title"] for item in mine["items"]] == ["Synced Only"]

    both = profileAPI.build_profile_collection_payload(state, {}, shelf="all", search="shelf only")
    assert [item["title"] for item in both["items"]] == ["Shelf Only Show"]
    assert profileAPI.build_profile_collection_payload(state, {}, shelf="all")["total"] == 3
    assert profileAPI.build_profile_collection_payload(state, {}, shelf="bogus")["shelf"] == "synced"

    cached = profileAPI.build_profile_collection_payload(state, {}, cache_key=("x",))
    assert any("shelf" in item for item in cached["items"])
    shelf_store.remove(ADMIN, "movie", 10)
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
    shelf_store.put(ADMIN, "movie", 1, {"title": "Ripped", "copies": [{"format": "bluray"}]})
    shelf_store.put(ADMIN, "movie", 3, {"title": "Tracker Only", "copies": [{"format": "dvd", "lent_to": "Kim"}]})
    shelf_store.put(ADMIN, "movie", 4, {"title": "Disc Only", "copies": [{"format": "4k_bluray"}]})

    def titles(mode: str, **kw: Any) -> list[str]:
        return sorted(item["title"] for item in profileAPI.build_profile_collection_payload(state, {}, shelf=mode, **kw)["items"])

    assert titles("not_on_server") == ["Disc Only", "Tracker Only"]
    assert titles("not_on_shelf") == ["Server Only"]
    assert titles("not_on_shelf", shelf_filter="lent") == ["Server Only"]
    assert titles("unwatched", watched=lambda media, tmdb: tmdb == "3") == ["Disc Only", "Ripped"]
    ripped = next(item for item in profileAPI.build_profile_collection_payload(state, {}, shelf="shelf")["items"] if item["title"] == "Ripped")
    assert ripped["shelf"]["on_server"] is True
    assert "on_server" not in shelf_store.summaries(ADMIN)["movie:1"]


def test_shelf_stats_locations_and_quality() -> None:
    state = _baseline({"tmdb:2": {"type": "movie", "title": "B", "year": 2021, "ids": {"tmdb": "2"}, "media_info": {"hdr": "dolby_vision", "audio": "dolby_atmos", "resolution": "uhd_4k"}}})
    shelf_store.put(ADMIN, "movie", 1, {"title": "A", "year": 1999, "copies": [{"format": "bluray", "shelf": "Living B", "price": 10}, {"format": "bluray", "shelf": "Attic", "lent_to": "Kim"}]})
    shelf_store.put(ADMIN, "movie", 2, {"title": "B", "copies": [{"format": "4k_bluray", "shelf": "Living A", "price": 20.5}]})
    shelf_store.put(ADMIN, "tv", 3, {"title": "C", "status": "wishlist"})
    shelf_store.set_currency(ADMIN, "EUR")
    assert shelf_store.summaries(ADMIN)["movie:1"]["shelves"] == ["Living B", "Attic"]
    payload = profileAPI.build_profile_collection_payload(state, {}, shelf="shelf")
    stats = payload["shelf_stats"]
    assert stats["titles"] == 3 and stats["copies"] == 3 and stats["lent"] == 1 and stats["spent"] == 30.5 and stats["currency"] == "EUR"
    assert stats["formats"] == {"bluray": 2, "4k_bluray": 1} and stats["uhd"] == 1
    assert stats["status"] == {"owned": 2, "wishlist": 1}
    assert stats["picture"] == {"Dolby Vision": 1, "Dolby Atmos": 1}
    assert stats["decades"] == {"1990s": 1, "2020s": 1}
    assert payload["shelf_locations"] == ["Attic", "Living A", "Living B"]
    assert "shelf_stats" not in profileAPI.build_profile_collection_payload(state, {})


def test_digital_service_and_shelf_filters() -> None:
    shelf_store.put(ADMIN, "movie", 1, {"title": "Stream", "copies": [{"format": "digital", "service": {"id": "2", "name": "Apple TV", "logo": "/abcd1234.jpg"}, "purchased": "2024-01-02"}]})
    shelf_store.put(ADMIN, "movie", 2, {"title": "Disc", "copies": [{"format": "dvd", "service": {"id": 2, "name": "Apple TV"}, "shelf": "Attic", "lent_to": "Kim", "price": 5, "purchased": "2025-03-04"}]})
    shelf_store.put(ADMIN, "movie", 3, {"title": "Bad", "copies": [{"format": "digital", "service": {"id": 0, "name": "x", "logo": "../x.png"}}]})
    assert shelf_store.get(ADMIN, "movie", 1)["copies"][0]["service"] == {"id": 2, "name": "Apple TV", "logo": "/abcd1234.jpg"}
    assert "service" not in shelf_store.get(ADMIN, "movie", 2)["copies"][0]
    assert "service" not in shelf_store.get(ADMIN, "movie", 3)["copies"][0]
    assert shelf_store.summaries(ADMIN)["movie:1"]["service"] == {"name": "Apple TV", "logo": "/abcd1234.jpg"}

    def titles(**kw: Any) -> list[str]:
        return [item["title"] for item in profileAPI.build_profile_collection_payload({}, {}, shelf="shelf", **kw)["items"]]

    assert titles(shelf_filter="lent") == ["Disc"]
    assert sorted(titles(shelf_filter="format:digital")) == ["Bad", "Stream"]
    assert titles(shelf_filter="location:Attic") == ["Disc"]
    assert sorted(titles(shelf_filter="price")) == ["Bad", "Stream"]
    assert titles(sort="price_desc")[0] == "Disc"
    assert titles(sort="purchased_desc")[:2] == ["Disc", "Stream"]


def test_purchase_dates_must_be_real_dates() -> None:
    copies = shelf_store.put(ADMIN, "movie", 6, {"title": "D", "copies": [{"format": "dvd", "purchased": "2024-02-30"}, {"format": "dvd", "purchased": "2024-02-29"}]})["copies"]
    assert "purchased" not in copies[0] and copies[1]["purchased"] == "2024-02-29"


def test_entry_includes_synced_media(monkeypatch) -> None:
    state = _baseline({"tmdb:603": {"type": "movie", "title": "The Matrix", "ids": {"tmdb": "603"}, "format": "4k_bluray", "media_info": {"hdr": "dolby_vision", "audio": "dolby_atmos"}}})
    _as(monkeypatch, profileAPI.ADMIN_USER_ID)
    monkeypatch.setattr(profileAPI, "_collection_state", lambda *a, **k: state)
    found = json.loads(profileAPI.api_profile_shelf_entry(_request(), "movie", 603).body)
    assert found["synced"] == {"providers": ["plex"], "format": "4k_bluray", "media_info": {"hdr": "dolby_vision", "audio": "dolby_atmos"}}
    assert "synced" not in json.loads(profileAPI.api_profile_shelf_entry(_request(), "movie", 604).body)


def test_managed_users_edit_their_own_shelf(monkeypatch) -> None:
    _as(monkeypatch, profileAPI.ADMIN_USER_ID)
    profileAPI.api_profile_shelf_save(_request(), "movie", 1, {"title": "Admin copy", "copies": [{"format": "dvd", "price": 4}]})

    _as(monkeypatch, MANAGED_ID, write=False)
    assert profileAPI.api_profile_shelf_save(_request(), "movie", 2, {"title": "Nope"}).status_code == 403
    view = json.loads(profileAPI.api_profile_shelf_entry(_request(), "movie", 1).body)
    assert view == {"ok": True, "entry": None, "can_edit": False}
    assert json.loads(profileAPI.api_profile_shelf_index(_request()).body)["items"] == {}

    _as(monkeypatch, MANAGED_ID, write=True)
    saved = json.loads(profileAPI.api_profile_shelf_save(_request(), "show", 1399, {"status": "wishlist", "title": "GoT"}).body)
    assert saved["ok"] and saved["summary"] == {"status": "wishlist", "copies": 0, "formats": []}
    index = json.loads(profileAPI.api_profile_shelf_index(_request()).body)
    assert index["items"] == {"tv:1399": saved["summary"]} and index["can_edit"] is True
    assert json.loads(profileAPI.api_profile_shelf_settings(_request(), {"currency": "gbp"}).body)["currency"] == "GBP"
    assert profileAPI.api_profile_shelf_save(_request(), "book", 1, {}).status_code == 400
    bad = profileAPI.api_profile_shelf_save(_request(), "movie", 5, {"copies": [{"format": "nope"}]})
    assert bad.status_code == 400 and json.loads(bad.body)["error"] == "Choose a format for each copy"
    assert json.loads(profileAPI.api_profile_shelf_remove(_request(), "tv", 1399).body) == {"ok": True, "removed": True}

    assert shelf_store.get(ADMIN, "movie", 1)["copies"][0]["price"] == 4
    assert shelf_store.currency(ADMIN) == ""


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
    assert json.loads(metaAPI.api_watch_providers("movie", "1x").body)["region"] == "US"


def test_watch_providers_survive_bad_priorities(monkeypatch) -> None:
    from api import metaAPI

    metaAPI._WATCH_PROVIDERS.clear()
    data = {"results": [
        {"provider_id": 1, "provider_name": "Zero", "display_priorities": {"US": 0}},
        {"provider_id": 2, "provider_name": "Odd", "display_priorities": {"US": "x"}, "display_priority": None},
        {"provider_id": 3, "provider_name": "Five", "display_priority": 5},
    ]}
    monkeypatch.setattr(metaAPI, "load_config", lambda: {"tmdb": {"api_key": "t"}})
    monkeypatch.setattr(metaAPI, "_cfg_ui_locale", lambda: "en-US")
    monkeypatch.setattr(metaAPI, "_tmdb_get_json", lambda url, params=None, **kw: data)
    names = [row["name"] for row in json.loads(metaAPI.api_watch_providers("movie", None).body)["providers"]]
    assert names == ["Zero", "Five", "Odd"]


def test_errors_only_return_known_messages() -> None:
    from services import artwork_choices

    assert shelf_store.public_error(shelf_store.ShelfError("Invalid currency")) == "Invalid currency"
    assert shelf_store.public_error(RuntimeError(r"C:\secret\path")) == "Invalid request"
    assert artwork_choices.public_error(artwork_choices.ArtworkChoiceError("Image too large")) == "Image too large"
    assert artwork_choices.public_error(ValueError("Traceback (most recent call last)")) == "Invalid request"


def test_shelf_ui_is_wired() -> None:
    html = (ROOT / "ui_frontend.py").read_text("utf-8")
    assert 'id="profile-collection-shelf"' in html and 'data-collection-display="shelf"' in html and 'id="profile-collection-shelfstats"' in html
    assert 'data-collection-display="case"' not in html
    modal = (ROOT / "assets/js/profile-media-modal.js").read_text("utf-8")
    assert 'lazyScript("shelf-editor.js", "ShelfEditor")' in modal and "collectionSection()" in modal
    meta = (ROOT / "assets/helpers/media-meta.js").read_text("utf-8")
    assert "pill: shelfPill" in meta and "shelfBand" not in meta
    tracker = (ROOT / "assets/js/tracker.js").read_text("utf-8")
    assert "Add to collection" in tracker and 'data-trk-media="${name}"' not in tracker and "[data-trk-mi=\"format\"]" in tracker
    assert "CW ||= {}).ShelfEditor" in (ROOT / "assets/js/shelf-editor.js").read_text("utf-8")
