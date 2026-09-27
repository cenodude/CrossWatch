# tests/test_wetrakr_api_updates.py
# CrossWatch - WeTrakr regular pagination, decimal ratings and cancel contracts
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from types import SimpleNamespace
import json

import pytest

from providers.sync.wetrakr import _common as common
from test_wetrakr_sync import EPISODE, MOVIE, SHOW, WHEN, Response, env, item
from test_wetrakr_ratings_progress import SEASON, features
from test_wetrakr_scrobble import event, live


@pytest.mark.parametrize("media", [MOVIE, SHOW, SEASON, EPISODE])
@pytest.mark.parametrize("rating,expected", [(0, 0), (0.1, 0.1), (8.45, 8.5), (9.9, 9.9), (10, 10)])
def test_decimal_ratings_roundtrip_and_separate_removal(features, media, rating, expected):
    source = {**item(media), "rating": rating}
    assert features.adapter.add("ratings", [source])["ok"]
    rows = features.adapter.build_index("ratings", force_refresh=True)
    assert next(iter(rows.values()))["rating"] == expected
    assert features.adapter.add("ratings", [source])["ok"]
    assert len([call for call in features.server.calls if call[0] == "POST"]) == 1
    assert features.adapter.remove("ratings", [source])["ok"]
    assert not features.ratings


def test_regular_reads_include_metadata_without_fallback(env):
    env.server.planning = [MOVIE]
    first = env.adapter.build_index("watchlist", force_refresh=True)
    assert next(iter(first.values()))["title"] == MOVIE["title"]
    reads = [(path, kwargs["params"]) for _, path, kwargs in env.server.calls if "/planning/" in path]
    assert len(reads) == 2 and all(params == {"page": 1, "limit": 100} for _, params in reads)
    assert not any(path.startswith("/shows/") or path.startswith("/movies/") for _, path, _ in env.server.calls)
    env.server.calls.clear()
    assert env.adapter.build_index("watchlist") == first
    assert not any("/planning/" in path for _, path, _ in env.server.calls)


@pytest.mark.parametrize("section", [None, {"rows": None}, {"rows": [None]}])
def test_bad_metadata_cache_falls_back_to_live_rows(env, section):
    env.adapter.build_index("watchlist")
    path = common.cache_path(env.adapter, "watchlist")
    path.write_text(json.dumps({"schema": common.SCHEMA, "sections": {"movies": section}}))
    env.server.planning = [MOVIE]
    rows = env.adapter.build_index("watchlist")
    assert len(rows) == 1 and next(iter(rows.values()))["title"] == MOVIE["title"]


@pytest.mark.parametrize("duplicate", [False, True])
def test_journal_tied_timestamps_require_distinct_entry_ids(env, duplicate):
    rows = [{"entry_id": "entry-1" if duplicate else f"entry-{i}", "category": "ratings", "type": "movie", "id": MOVIE["id"], "status": "removed", "action_at": "2026-09-17T12:00:00Z"} for i in (1, 2)]
    env.server.hook = lambda *args: Response({"retention_days": 30, "journal": rows}, headers={"X-Pagination-Item-Count": "2", "X-Pagination-Page": "1", "X-Pagination-Page-Count": "1"})
    if duplicate:
        with pytest.raises(common.WeTrakrSyncError, match="repeated_journal_entry"):
            common.journal_rows(env.adapter, "ratings", WHEN)
    else:
        result, checkpoint = common.journal_rows(env.adapter, "ratings", WHEN)
        assert result == rows and checkpoint == rows[0]["action_at"]


@pytest.mark.parametrize("kind,native", [("movie", MOVIE["id"]), ("episode", EPISODE["id"])])
def test_native_scrobble_echo_is_verified(live, kind, native):
    previous = live.server.hook
    live.server.hook = lambda method, path, kw: Response({"action": "start", "media" if kind == "movie" else "episode": {"id": native}}) if method == "POST" else previous(method, path, kw)
    result = live.sink.send(event(media_type=kind, ids={"wetrakr": str(native)}, season=None, number=None))
    assert result["ok"]
    body = next(kw["json"] for method, _, kw in live.server.calls if method == "POST")
    assert body[kind] == {"id": native}
    assert not any(path.startswith("/media/external") for _, path, _ in live.server.calls)


def test_scrobble_wrong_echo_never_records_success(live):
    previous = live.server.hook
    live.server.hook = lambda method, path, kw: Response({"action": "start", "media": {"id": 999}}) if method == "POST" else previous(method, path, kw)
    assert live.sink.send(event())["error"] == "scrobble_identity_mismatch"
    assert all(row["status"] == "fail" for row in live.archived) and not live.completed


@pytest.mark.parametrize("rating,step,expected", [(8.4, 0.1, 8.4), (8.4, 1, 8), (8.45, 0.1, 8.5)])
def test_plex_rating_forwarding_uses_destination_precision(monkeypatch, rating, step, expected):
    from providers.scrobble.plex import ratings_sync

    writes = []
    ops = SimpleNamespace(capabilities=lambda: {"ratings": {"step": step}}, add=lambda cfg, items, **kw: writes.extend(items) or {"ok": True})
    monkeypatch.setattr(ratings_sync, "_ops", lambda _: ops)
    result = ratings_sync.dispatch_ops_ratings("movie", {"title": "Synthetic movie"}, {"tmdb": 155}, rating, {}, enabled=["wetrakr"], instance_for=lambda _: "default")
    assert result["wetrakr"]["ok"] and writes[0]["rating"] == expected


def test_zero_rating_is_opt_in_and_never_a_removal():
    from cw_platform.orchestrator._planner import diff_ratings

    source = {"tmdb:155": {**item(MOVIE), "rating": 0}}
    assert diff_ratings(source, {}, step=0.1) == ([], [])
    additions, removals = diff_ratings(source, {}, step=0.1, allow_zero=True)
    assert additions[0]["rating"] == 0 and not removals
    additions, removals = diff_ratings({}, source, step=0.1, allow_zero=True)
    assert not additions and len(removals) == 1


def test_plex_webhook_does_not_deduplicate_fractional_rating_updates(monkeypatch):
    from providers.scrobble.plex import ratings_sync
    from providers.webhooks import plex

    writes = []
    monkeypatch.setattr(plex, "_save_config", lambda _: None)
    monkeypatch.setattr(ratings_sync, "send_rating", lambda provider, cfg, instance, item, rating: writes.append(rating) or {"ok": True})
    plex._LAST_RATING_BY_ACC.clear()
    cfg = {"scrobble": {"enabled": True, "sources": {"webhook": True}, "webhook": {"sinks": ["wetrakr"], "plex_wetrakr_ratings": True}}, "wetrakr": {"access_token": "test-token"}}
    payload = {"event": "media.rate", "Account": {"title": "synthetic-user"}, "Metadata": {"type": "movie", "title": "Synthetic movie", "ratingKey": "test-155", "Guid": [{"id": "tmdb://155"}]}}
    for value in (8.2, 8.4, 0):
        payload["Metadata"]["userRating"] = value
        assert plex.process_webhook(payload, {}, cfg=cfg)["ok"]
    assert writes == [8.2, 8.4, 0]


@pytest.mark.parametrize("removed", [False, True])
def test_cancel_no_session_requires_readback_and_preserves_other_title(features, removed):
    assert features.adapter.add("progress", [{**item(media), "progress_percent": 30, "progress_at": WHEN} for media in (MOVIE, EPISODE)])["ok"]
    previous = features.server.hook

    def response(method, path, kwargs):
        if method == "DELETE":
            assert path == "/scrobble/playing" and "movie" in kwargs["json"]
            if removed:
                features.playing.pop(MOVIE["id"])
            return Response({"error": "NO_SESSION"}, 404)
        return previous(method, path, kwargs)

    features.server.hook = response
    result = features.adapter.remove("progress", [item(MOVIE)])
    assert result["ok"] is removed
    assert EPISODE["id"] in features.playing
    assert not features.server.history


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
@pytest.mark.parametrize("value", [0, 8.4])
def test_pair_rating_updates_use_fractional_and_zero_capabilities(config_base, monkeypatch, mode, value):
    from cw_platform.orchestrator import Orchestrator
    from test_interactive_sync import item as pair_item
    from test_interactive_sync_features import feature_setup

    source = pair_item(1, rating=value, rated_at="2026-09-02T12:00:00Z")
    target = pair_item(1, rating=7, rated_at="2026-09-01T12:00:00Z")
    cfg, src, dst = feature_setup(config_base, monkeypatch, "ratings", [source], [target], mode)
    for ops in (src, dst):
        ops.capabilities = lambda: {"features": {"ratings": True}, "ratings": {"step": 0.1, "zero_is_rating": True}, "index_semantics": "present"}
    monkeypatch.setattr("cw_platform.orchestrator._pairs.record_health", lambda *args: None)
    assert Orchestrator(cfg).run()["ok"]
    assert [row["rating"] for batch in dst.add_calls for row in batch] == [value]


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
@pytest.mark.parametrize("step,expected", [(0.1, 8.5), (0.5, 8.5), (1.0, 8)])
def test_rating_reread_does_not_repeat_rounded_write(config_base, monkeypatch, mode, step, expected):
    from cw_platform.id_map import canonical_key
    from cw_platform.orchestrator import Orchestrator
    from cw_platform.orchestrator._state_store import StateStore
    from test_interactive_sync import item as pair_item
    from test_interactive_sync_features import feature_setup

    source = pair_item(1, rating=8.45, rated_at="2026-09-02T12:00:00Z")
    target = pair_item(1, rating=7, rated_at="2026-09-01T12:00:00Z")
    cfg, src, dst = feature_setup(config_base, monkeypatch, "ratings", [source], [target], mode)
    src.capabilities = lambda: {"features": {"ratings": True}, "ratings": {"step": 0.1}, "index_semantics": "present"}
    dst.capabilities = lambda: {"features": {"ratings": True}, "ratings": {"step": step}, "index_semantics": "present"}
    assert Orchestrator(cfg).run()["ok"]
    assert [row["rating"] for batch in dst.add_calls for row in batch] == [expected]
    assert StateStore(config_base).load_state_features({"ratings"})
    src.index[canonical_key(source)] = dict(source)
    dst.index[canonical_key(target)] = {**target, "rating": expected, "rated_at": source["rated_at"]}
    src.add_calls.clear()
    dst.add_calls.clear()
    assert Orchestrator(cfg).run()["ok"]
    assert not src.add_calls and not dst.add_calls
    assert src.index[canonical_key(source)]["rating"] == 8.45
    assert dst.index[canonical_key(target)]["rating"] == expected
