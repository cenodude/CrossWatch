from importlib import import_module
from types import SimpleNamespace

import pytest


@pytest.mark.parametrize("provider", ["emby", "jellyfin"])
@pytest.mark.parametrize("played_at", [None, "2026-09-01T12:00:00Z"])
def test_metadata_edits_do_not_become_playback_timestamps(provider, played_at):
    progress = import_module(f"providers.sync.{provider}._progress")
    row = {"Id": "1", "DateLastSaved": "2026-09-10T12:00:00Z",
           "UserData": {"Played": False, "PlaybackPositionTicks": 0, "LastPlayedDate": played_at}}
    http = SimpleNamespace(get=lambda *a, **kw: SimpleNamespace(status_code=200, json=lambda: row))
    assert progress._target_state(http, "user", "1")["timestamp"] == played_at


@pytest.mark.parametrize("provider", ["emby", "jellyfin"])
def test_newer_progress_is_reported_as_skipped_not_added(provider, monkeypatch):
    progress = import_module(f"providers.sync.{provider}._progress")
    common = import_module(f"providers.sync.{provider}._common")
    module = import_module(f"providers.sync._mod_{provider.upper()}")
    monkeypatch.setattr(progress, "_active_item_ids", lambda *a: set())
    monkeypatch.setattr(progress, "resolve_item_ids", lambda *a, **k: ["1"])
    monkeypatch.setattr(progress, "_target_state", lambda *a: {
        "timestamp": "2026-09-10T12:00:00Z", "progress_ms": 2000, "duration_ms": 600000})
    ad = SimpleNamespace(client=object(), cfg=SimpleNamespace(user_id="user"))
    source = {"type": "movie", "ids": {"tmdb": "1"}, "progress_ms": 1000, "progress_at": "2026-09-01T12:00:00Z"}
    count, unresolved = progress.add(ad, [source])
    result = module._finalize_result(ad, common.key_of, "progress", [source], count, unresolved)
    assert result["count"] == 0
    assert result["confirmed_keys"] == []
    assert result["skipped_keys"] == ["tmdb:1"]


@pytest.mark.parametrize("provider", ["emby", "jellyfin"])
def test_history_existing_items_are_reported_as_skipped(provider):
    common = import_module(f"providers.sync.{provider}._common")
    module = import_module(f"providers.sync._mod_{provider.upper()}")
    source = {"type": "movie", "ids": {"tmdb": "1"}}
    ad = SimpleNamespace(_history_write_meta={"confirmed_keys": ["tmdb:1"],
        "results": [{"key": "tmdb:1", "action": "skip", "reason": "existing_newer"}]})
    result = module._finalize_result(ad, common.key_of, "history", [source], 0, [])
    assert result["confirmed_keys"] == []
    assert result["skipped_keys"] == ["tmdb:1"]
    removed = module._finalize_result(ad, common.key_of, "history", [source], 1, [], op="remove")
    assert removed["confirmed_keys"] == ["tmdb:1"]
    assert not removed.get("skipped_keys")


@pytest.mark.parametrize("provider", ["emby", "jellyfin"])
@pytest.mark.parametrize("scenario", ["metadata_only", "watched", "replay", "unwatch_failed", "write_failed", "active"])
def test_progress_write_guards_and_failure_accounting(provider, scenario, monkeypatch):
    progress = import_module(f"providers.sync.{provider}._progress")
    common = import_module(f"providers.sync.{provider}._common")
    module = import_module(f"providers.sync._mod_{provider.upper()}")
    monkeypatch.setattr(progress, "resolve_item_ids", lambda *a, **k: ["1"])
    monkeypatch.setattr(progress, "_same_origin", lambda: False)
    writes = []

    class Http:
        def get(self, path, **kwargs):
            if path == "/Sessions":
                body = [{"UserId": "user", "NowPlayingItem": {"Id": "1"}}] if scenario == "active" else []
            else:
                body = {"Id": "1", "RunTimeTicks": 600000 * 10000, "DateLastSaved": "2026-09-10T12:00:00Z",
                        "UserData": {"Played": scenario in {"watched", "replay", "unwatch_failed"}, "PlaybackPositionTicks": 0}}
            return SimpleNamespace(status_code=200, json=lambda: body)

        def delete(self, path, **kwargs):
            writes.append("DELETE")
            return SimpleNamespace(status_code=500 if scenario == "unwatch_failed" else 204)

        def post(self, path, **kwargs):
            writes.append("POST")
            assert kwargs["json"]["PlaybackPositionTicks"] == 60000 * 10000
            return SimpleNamespace(status_code=500 if scenario == "write_failed" else 204)

    ad = SimpleNamespace(client=Http(), cfg=SimpleNamespace(user_id="user", progress_replay_enabled=scenario in {"replay", "unwatch_failed"}))
    source = {"type": "movie", "ids": {"tmdb": "1"}, "progress_ms": 60000, "progress_at": "2026-09-01T12:00:00Z"}
    count, unresolved = progress.add(ad, [source])
    result = module._finalize_result(ad, common.key_of, "progress", [source], count, unresolved)
    if scenario in {"watched", "active"}:
        assert writes == []
        assert count == 0 and not unresolved
        assert result["skipped_keys"] == ["tmdb:1"]
        assert result["confirmed_keys"] == []
    elif scenario in {"unwatch_failed", "write_failed"}:
        assert writes == (["DELETE"] if scenario == "unwatch_failed" else ["POST"])
        assert count == 0 and unresolved
        assert result["confirmed_keys"] == []
        assert result["unresolved_keys"] == ["tmdb:1"]
    else:
        assert writes == (["DELETE", "POST"] if scenario == "replay" else ["POST"])
        assert count == 1 and not unresolved
        assert result["confirmed_keys"] == ["tmdb:1"]
