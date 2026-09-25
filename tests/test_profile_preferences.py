# /tests/test_profile_preferences.py
# CrossWatch - Profile display preferences and calendar boundaries
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from copy import deepcopy
from datetime import datetime

import pytest

from api import profileAPI
from cw_platform.profile_preferences import clean_timezone, clean_user_preferences
from services import profile_history
from services.playback_progress import service as playback
from tests.test_app_auth_api import _auth_cfg, _roundtrip_app_auth_root, _roundtrip_app_auth_user
from tests.test_profile_history import _synced_index


def test_preferences_default_and_invalid_values():
    defaults = clean_user_preferences(None)
    assert defaults["timezone"] == defaults["time_format"] == "auto"
    assert clean_user_preferences({"timezone": "../invalid", "time_format": "invalid"}) == defaults
    assert clean_user_preferences({"timezone": "Asia/Kathmandu", "time_format": "12h"})["timezone"] == "Asia/Kathmandu"


@pytest.mark.parametrize("name", ["utc", "europe/amsterdam", "Europe/AMSTERDAM", "../UTC"])
def test_timezone_names_are_case_sensitive_on_every_platform(name):
    assert clean_timezone(name) == "auto"
    assert clean_timezone("UTC") == "UTC"
    assert clean_timezone("Europe/Amsterdam") == "Europe/Amsterdam"


@pytest.mark.parametrize("value,expected", [(0, False), (None, False), (False, False), ("", False), (1, True), (True, True), ("false", True)])
def test_toggle_api_preserves_boolean_conversion(monkeypatch, value, expected):
    from tests.test_app_auth_api import _request, _json_body

    raw = {"username": "Alice", "preferences": clean_user_preferences({"timezone": "UTC", "time_format": "24h"})}
    cfg = {"app_auth": {"users": {"alice": raw}}}
    def write(request, mutate):
        return cfg, mutate(cfg, cfg["app_auth"], "alice", raw, {"username": "Alice"}, "")
    monkeypatch.setattr(profileAPI, "_profile_write", write)
    monkeypatch.setattr(profileAPI, "_audit", lambda *args, **kwargs: None)
    response = profileAPI.api_profile_update(_request("/api/profile"), {"preferences": {"playing_card": value, "quick_add": value}})
    prefs = _json_body(response)["user"]["preferences"]
    assert prefs["playing_card"] is expected
    assert prefs["quick_add"] is expected
    assert prefs["timezone"] == "UTC"
    assert prefs["time_format"] == "24h"


def test_utc_history_reuses_cached_months(monkeypatch):
    index = _synced_index()
    expected = profile_history.build_history_payload(index, resolve_art=False)
    def unexpected_conversion(*args):
        pytest.fail("UTC month keys should come from the cached index")
    monkeypatch.setattr(profile_history, "month_key", unexpected_conversion)
    assert profile_history.build_history_payload(index, display_tz="UTC", resolve_art=False) == expected


def test_preferences_survive_config_roundtrip(monkeypatch, tmp_path):
    prefs = {"playing_card": False, "quick_add": False, "timezone": "Europe/Amsterdam", "time_format": "24h"}
    admin = _roundtrip_app_auth_root(monkeypatch, tmp_path, {"preferences": prefs})
    assert admin["preferences"] == prefs
    managed_path = tmp_path / "managed"
    managed_path.mkdir()
    managed = _roundtrip_app_auth_user(monkeypatch, managed_path, {"username": "Alice", "password": _auth_cfg()["app_auth"]["password"], "preferences": prefs})
    assert managed["preferences"] == prefs


def test_partial_preference_update_preserves_other_settings_and_users(monkeypatch, tmp_path):
    from tests.test_app_auth_api import _request, _json_body
    from cw_platform import config_base

    cfg = _auth_cfg()
    password = cfg["app_auth"]["password"]
    raw = {"username": "Alice", "password": password, "preferences": clean_user_preferences({"quick_add": False, "time_format": "12h"})}
    other = {"username": "Bob", "password": password, "preferences": clean_user_preferences(None)}
    cfg["app_auth"]["users"] = {"alice": raw, "bob": other}
    monkeypatch.setattr(config_base, "CONFIG", tmp_path)
    monkeypatch.setattr(config_base, "_get_cipher", lambda **kwargs: None)
    config_base.save_config(cfg)
    before = deepcopy(config_base.load_config()["app_auth"]["users"]["bob"])
    monkeypatch.setattr(profileAPI, "current_user", lambda *args: {"id": "alice", "username": "Alice"})
    monkeypatch.setattr(profileAPI, "_audit", lambda *args, **kwargs: None)
    result = profileAPI.api_profile_update(_request("/api/profile"), {"preferences": {"timezone": "Europe/Amsterdam"}})
    prefs = _json_body(result)["user"]["preferences"]
    assert prefs == {"playing_card": True, "quick_add": False, "timezone": "Europe/Amsterdam", "time_format": "12h"}
    saved = config_base.load_config()["app_auth"]["users"]
    assert saved["alice"]["preferences"] == prefs
    assert saved["bob"] == before


@pytest.mark.parametrize("source", ["synced", "scrobble", "ratings", "watchlist"])
def test_history_month_counts_and_navigation_follow_timezone_without_mutating_cache(source):
    index = _synced_index()
    index["source"] = source
    stamps = ["2026-01-01T12:00:00+00:00", "2026-01-01T00:30:00+00:00", "2025-12-01T12:00:00+00:00"]
    for row, stamp in zip(index["rows"], stamps):
        row["sort_epoch"] = int(datetime.fromisoformat(stamp).timestamp())
        row["_month"] = stamp[:7]
    original = deepcopy(index)
    local = profile_history.build_history_payload(index, display_tz="America/Los_Angeles", month="2025-12", page_size=1, resolve_art=False)
    assert local["months"] == [{"month": "2026-01", "count": 1}, {"month": "2025-12", "count": 2}]
    assert local["page"] == 2
    assert local["items"][0]["sort_epoch"] == index["rows"][1]["sort_epoch"]
    utc = profile_history.build_history_payload(index, resolve_art=False)
    assert utc["months"] == [{"month": "2026-01", "count": 2}, {"month": "2025-12", "count": 1}]
    assert index == original


def test_collection_month_counts_and_navigation_follow_timezone():
    items = {f"tmdb:{i}": {"ids": {"tmdb": str(i)}, "type": "movie", "title": str(i), "collected_at": stamp} for i, stamp in enumerate(["2026-01-01T12:00:00Z", "2026-01-01T00:30:00Z"], 1)}
    state = {"providers": {"PLEX": {"collection": {"baseline": {"items": items}}}}}
    local = profileAPI.build_profile_collection_payload(state, {}, display_tz="America/Los_Angeles", month="2025-12", page_size=1)
    assert local["months"] == [{"month": "2026-01", "count": 1}, {"month": "2025-12", "count": 1}]
    assert local["page"] == 2
    assert local["items"][0]["last_collected_at"] == "2026-01-01T00:30:00Z"


def test_progress_month_counts_and_navigation_follow_timezone(monkeypatch):
    rows = [{"updated_at": stamp, "ids": {}, "media_type": "movie"} for stamp in ["2026-01-01T12:00:00Z", "2026-01-01T00:30:00Z"]]
    service = playback.PlaybackProgressService()
    monkeypatch.setattr(playback, "load_config", lambda: {})
    monkeypatch.setattr(playback, "_overlay_live_streams", lambda items: None)
    monkeypatch.setattr(service, "_apply_filters", lambda *args, **kwargs: deepcopy(rows))
    local = service.items(provider="plex", display_tz="America/Los_Angeles", month="2025-12", page_size=1)
    assert local["months"] == [{"month": "2026-01", "count": 1}, {"month": "2025-12", "count": 1}]
    assert local["page"] == 2
    assert local["items"][0]["updated_at"] == "2026-01-01T00:30:00Z"
