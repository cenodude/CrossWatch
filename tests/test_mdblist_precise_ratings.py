# tests/test_mdblist_precise_ratings.py
# CrossWatch - MDBList precise ratings and legacy cache compatibility
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from providers.sync.mdblist import _ratings as ratings


WHEN = "2026-09-01T12:00:00Z"
IDS = {"tmdb": 123}


@pytest.mark.parametrize("kind", ["movie", "show", "season", "episode"])
@pytest.mark.parametrize("fields,expected", [
    ({"rating": 8, "rating_precise": 7.5}, 7.5),
    ({"rating": 8}, 8),
    ({"rating": 8, "rating_precise": None}, 8),
    ({"rating_precise": 7.5}, 7.5),
    ({"rating": None, "rating_precise": None}, None),
])
def test_api_rows_and_journal_prefer_precise_rating(kind, fields, expected):
    media = {"ids": IDS, "season": 1, "number": 2, "show": {"ids": IDS}}
    row = {kind: media, **fields, "rated_at": WHEN}
    parsed = getattr(ratings, f"_row_{kind}")(row)
    journal = ratings._journal_item({"category": "rated", "item_type": kind, "ids": IDS,
                                    "season": 1, "episode": 2, "value_at": WHEN, **fields})
    for item in (parsed, journal):
        assert item is not None
        assert item.get("rating") == expected
        assert bool(item.get("_removed")) is (expected is None)
        assert "rating_precise" not in item


@pytest.mark.parametrize("value,expected", [(7, 7), (7.5, 7.5), ("7.5", 7.5), (7.25, 7.5),
                                           (0, None), (None, None), (True, None), (float("nan"), None), (11, None)])
def test_rating_validation(value, expected):
    assert ratings._valid_rating(value) == expected


@pytest.mark.parametrize("kind", ["movie", "show", "season", "episode"])
def test_writes_keep_rating_field_and_remove_omits_it(kind):
    item = {"type": kind, "ids": IDS, "show_ids": IDS, "season": 1, "episode": 2, "rating": 7.5}
    body, accepted = ratings._bucketize([item])
    assert accepted[0]["rating"] == 7.5
    assert '"rating": 7.5' in json.dumps(body)
    assert "rating_precise" not in json.dumps(body)
    removed, _ = ratings._bucketize([item], unrate=True)
    assert '"rating"' not in json.dumps(removed)


@pytest.fixture
def cache_env(tmp_path, monkeypatch):
    path = tmp_path / "ratings.json"
    monkeypatch.delenv("CW_CAPTURE_MODE", raising=False)
    monkeypatch.setattr(ratings, "_cache_path", lambda: path)
    watermarks = {"ratings": WHEN, "ratings_journal": WHEN}
    monkeypatch.setattr(ratings, "get_watermark", watermarks.get)
    monkeypatch.setattr(ratings, "save_watermark", lambda k, v: watermarks.update({k: v}))
    monkeypatch.setattr(ratings, "update_watermark_if_new", lambda k, v: watermarks.update({k: v}))
    monkeypatch.setattr(ratings, "_fetch_last_activities", lambda *a, **kw: {"rated_at": WHEN, "journal_at": WHEN})
    adapter = SimpleNamespace(config={"mdblist": {"api_key": "test-key"}},
                              cfg=SimpleNamespace(timeout=1, max_retries=0), client=SimpleNamespace(session=None))
    return path, adapter


@pytest.mark.parametrize("empty", [False, True])
def test_old_cache_refreshes_once_even_when_activities_unchanged(cache_env, monkeypatch, empty):
    path, adapter = cache_env
    path.write_text(json.dumps({"items": {"tmdb:123": {"type": "movie", "ids": IDS, "rating": 8}}}))
    calls = []
    row = {"movie": {"ids": IDS}, "rating": 8, "rating_precise": 7.5, "rated_at": WHEN}
    payload = {"movies": [] if empty else [row], "pagination": {"has_more": False}}

    def request(*a, **kw):
        calls.append(kw)
        return SimpleNamespace(status_code=200, text="json", json=lambda: payload)

    monkeypatch.setattr(ratings, "mdblist_request", request)
    first = ratings.build_index(adapter)
    assert len(calls) == 1
    assert calls[0]["params"]["since"] < WHEN
    assert [x["rating"] for x in first.values()] == ([] if empty else [7.5])
    assert json.loads(path.read_text())["version"] == 2
    assert ratings.build_index(adapter) == first
    assert len(calls) == 1


@pytest.mark.parametrize("reason", ["missing_journal_watermark", "stale_journal_window"])
@pytest.mark.parametrize("count", [1, 50])
def test_empty_journal_full_refresh_preserves_existing_cache(cache_env, monkeypatch, reason, count):
    path, adapter = cache_env
    items = {f"tmdb:{n}": {"type": "movie", "ids": {"tmdb": str(n)}, "rating": 7.5}
             for n in range(1, count + 1)}
    ratings._save_cache(items, precise=True)
    before = path.read_bytes()
    newer = "2026-09-03T12:00:00Z"
    watermarks = {"ratings": WHEN}
    if reason == "stale_journal_window":
        watermarks["ratings_journal"] = WHEN
    monkeypatch.setattr(ratings, "get_watermark", watermarks.get)
    monkeypatch.setattr(ratings, "_fetch_last_activities", lambda *a, **kw:
                        {"rated_at": newer, "journal_at": newer})
    journal_calls = []

    def journal(**kwargs):
        journal_calls.append(kwargs)
        return {"journal_oldest_at": "2026-09-02T12:00:00Z", "rows": []}

    adapter.fetch_journal = journal
    reads = []

    def request(*a, **kwargs):
        reads.append(kwargs)
        return SimpleNamespace(status_code=200, text="json", json=lambda:
                               {"movies": [], "pagination": {"has_more": False}})

    monkeypatch.setattr(ratings, "mdblist_request", request)
    assert ratings.build_index(adapter) == items
    assert len(reads) == 1
    assert reads[0]["params"]["since"] < WHEN
    assert len(journal_calls) == (1 if reason == "stale_journal_window" else 0)
    assert path.read_bytes() == before


def test_cache_write_does_not_hide_needed_precision_refresh(cache_env):
    path, _ = cache_env
    ratings._save_cache({"tmdb:123": {"type": "movie", "ids": IDS, "rating": 7.5}})
    assert json.loads(path.read_text())["version"] == 1
    ratings._save_cache(ratings._load_cache(), precise=True)
    ratings._save_cache(ratings._load_cache())
    assert json.loads(path.read_text())["version"] == 2
    assert next(iter(ratings._load_cache().values()))["rating"] == 7.5


def test_incomplete_refresh_does_not_replace_cache(cache_env, monkeypatch):
    path, adapter = cache_env
    original = {"items": {"tmdb:123": {"type": "movie", "ids": IDS, "rating": 8}}}
    path.write_text(json.dumps(original))
    monkeypatch.setattr(ratings, "mdblist_request", lambda *a, **kw: SimpleNamespace(
        status_code=200, text="json", json=lambda: {"pagination": {"has_more": True}}))
    with pytest.raises(ratings.MDBListFetchError):
        ratings.build_index(adapter, max_pages=1)
    assert json.loads(path.read_text()) == original


def test_nested_seasons_and_episodes_use_precise_ratings(cache_env, monkeypatch):
    _, adapter = cache_env
    payload = {"shows": [{"show": {"ids": IDS}, "rating": 8, "rating_precise": 7.5,
                          "seasons": [{"number": 1, "rating": 7, "rating_precise": 6.5,
                                       "episodes": [{"number": 2, "rating": 6, "rating_precise": 5.5}]}]}],
               "pagination": {"has_more": False}}
    monkeypatch.setattr(ratings, "mdblist_request", lambda *a, **kw: SimpleNamespace(
        status_code=200, text="json", json=lambda: payload))
    assert {row["type"]: row["rating"] for row in ratings.build_index(adapter).values()} == {
        "show": 7.5, "season": 6.5, "episode": 5.5}


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
@pytest.mark.parametrize("destination_step,expected", [(0.1, 7.5), (0.5, 7.5), (1, 8)])
def test_pair_reread_keeps_precise_ratings_without_repeat_writes(config_base, monkeypatch, mode, destination_step, expected):
    from cw_platform.orchestrator import Orchestrator
    from test_interactive_sync import item
    from test_interactive_sync_features import feature_setup

    cfg, src, dst = feature_setup(config_base, monkeypatch, "ratings", [item(1, rating=7.5, rated_at=WHEN)], [], mode)
    src.capabilities = lambda: {"features": {"ratings": True}, "ratings": {"step": 0.5}, "index_semantics": "present"}
    dst.capabilities = lambda: {"features": {"ratings": True}, "ratings": {"step": destination_step}, "index_semantics": "present"}
    assert Orchestrator(cfg).run()["ok"]
    assert [row["rating"] for batch in dst.add_calls for row in batch] == [expected]
    dst.add_calls.clear()
    assert Orchestrator(cfg).run()["ok"]
    assert not src.add_calls and not dst.add_calls


@pytest.mark.parametrize("entrypoint", ["watcher", "webhook"])
def test_plex_forwarding_preserves_half_points(monkeypatch, entrypoint):
    from providers.scrobble.plex import watch
    from providers.webhooks import plex

    calls = []
    monkeypatch.setattr(plex, "_save_config", lambda cfg: None)
    monkeypatch.setattr(watch, "_mdblist_send_rating", lambda media_type, ids, rating, cfg, logger:
                        calls.append(rating) or {"ok": True})
    for mod in (watch, plex):
        mod._LAST_RATING_BY_ACC.clear()
    cfg = {"mdblist": {"api_key": "test-key"}, "scrobble": {"enabled": True, "sources": {"webhook": True, "watcher": True},
           "watch": {"plex_mdblist_ratings": True},
           "webhook": {"sinks": ["mdblist"], "plex_mdblist_ratings": True}}}
    payload = {"event": "media.rate", "Account": {"title": "synthetic-user"}, "Metadata": {
        "type": "movie", "title": "Synthetic movie", "ratingKey": "test-123", "Guid": [{"id": "tmdb://123"}]}}
    for rating in (7.5, 7, 0):
        payload["Metadata"]["userRating"] = rating
        if entrypoint == "webhook":
            assert plex.process_webhook(payload, {}, cfg=cfg)["ok"]
        else:
            result = watch.process_rating_webhook(payload, {}, cfg_override=cfg)
            assert result["ok"]
    assert calls == [7.5, 7, 0]


@pytest.mark.parametrize("value,expected", [(7.5, 7.5), (7.3, 7.5), (7, 7), (0, None)])
def test_plex_mdblist_request_uses_precise_rating_in_legacy_write_field(monkeypatch, value, expected):
    from providers.scrobble.plex import watch

    calls = []
    auth = SimpleNamespace(is_configured=lambda *a: True, request_with_auth=lambda *a, **kw:
        calls.append((a[3], kw["json"])) or SimpleNamespace(status_code=200, content=b"{}", json=lambda: {}))
    monkeypatch.setattr(watch, "_provider_auth", lambda: auth)
    assert watch._mdblist_send_rating("movie", IDS, value, {"mdblist": {"api_key": "test-key"}}, None)["ok"]
    path, body = calls[0]
    row = body["movies"][0]
    assert "rating_precise" not in row
    if expected is None:
        assert path.endswith("/remove") and "rating" not in row
    else:
        assert path.endswith("/ratings") and row["rating"] == expected
