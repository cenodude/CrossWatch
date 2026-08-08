# CrossWatch test scripts
from __future__ import annotations

import json
from typing import Any

import pytest


class _Resp:
    def __init__(self, status_code: int = 200, payload: Any = None, headers: dict[str, str] | None = None) -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = json.dumps(payload) if payload is not None else ""
        self.headers = headers or {}

    def json(self) -> Any:
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


class FakeHTTP:
    def __init__(self, responses: list[_Resp] | None = None) -> None:
        self.responses = list(responses or [])
        self.calls: list[dict[str, Any]] = []

    def __call__(self, adapter: Any, method: str, url: str, **kwargs: Any) -> _Resp:
        self.calls.append({"method": method, "url": url, **kwargs})
        if not self.responses:
            return _Resp(200, {"results": []})
        return self.responses.pop(0)


def _patch(monkeypatch: pytest.MonkeyPatch, module: Any, http: FakeHTTP) -> None:
    from providers.sync.punchplay import _common

    monkeypatch.setattr(_common, "punchplay_request", http)
    monkeypatch.setattr(module, "punchplay_request", http, raising=False)


class Adapter:
    def __init__(self) -> None:
        self.config = {"punchplay": {"access_token": "at"}}
        self.instance_id = "default"
        self.session = None


# --- registration -------------------------------------------------------------

def test_module_is_registered_and_discoverable() -> None:
    from cw_platform.modules_registry import MODULES, load_sync_ops, sync_provider_names, sync_provider_supports_feature

    assert MODULES["SYNC"]["_mod_PUNCHPLAY"] == "providers.sync._mod_PUNCHPLAY"
    assert "PUNCHPLAY" in sync_provider_names()

    ops = load_sync_ops("PUNCHPLAY")
    assert ops is not None
    assert ops.name() == "PUNCHPLAY"
    assert ops.label() == "PunchPlay"
    assert dict(ops.features()) == {
        "watchlist": True, "ratings": True, "history": True, "progress": True, "playlists": False,
    }
    assert sync_provider_supports_feature("PUNCHPLAY", "history") is True
    assert sync_provider_supports_feature("PUNCHPLAY", "playlists") is False


def test_manifest_matches_validated_contract() -> None:
    from providers.sync._mod_PUNCHPLAY import get_manifest

    man = get_manifest()
    caps = man["capabilities"]

    assert man["name"] == "PUNCHPLAY"
    assert man["version"] == "0.1"
    assert man["experimental"] is True
    assert man["bidirectional"] is True

    for feature in ("watchlist", "ratings", "history", "progress"):
        assert caps[feature]["observed_deletes"] is True, feature

    assert caps["ratings"]["scale"] == "1-10"
    assert caps["history"]["rewatch"] is True
    assert caps["history"]["requires_watched_at"] is True
    for feature in ("watchlist", "ratings", "history"):
        assert caps[feature]["batch_size"] == 100
        assert caps[feature]["accepted_ids"] == ["tmdb", "imdb", "tvdb", "mal"]


def test_ui_feature_flags_match_the_sync_manifest() -> None:
    import re
    from pathlib import Path

    from providers.sync._mod_PUNCHPLAY import get_manifest

    root = Path(__file__).resolve().parents[1]
    meta = (root / "assets" / "helpers" / "provider-meta.js").read_text(encoding="utf-8")
    line = next(ln for ln in meta.splitlines() if "PUNCHPLAY: { key:" in ln)

    features = get_manifest()["features"]
    for name, enabled in features.items():
        found = re.search(rf"\b{name}: (true|false)\b", line)
        assert found, f"provider-meta.js does not declare {name} for PUNCHPLAY"
        assert (found.group(1) == "true") is bool(enabled), (
            f"provider-meta.js {name}={found.group(1)} disagrees with manifest {name}={enabled}"
        )


def test_ops_is_configured_tracks_access_token() -> None:
    from providers.sync._mod_PUNCHPLAY import OPS

    assert OPS.is_configured({"punchplay": {"access_token": "at"}}) is True
    assert OPS.is_configured({"punchplay": {}}) is False
    assert OPS.is_configured({}) is False


# --- bulk write contract ------------------------------------------------------

def test_bulk_write_sends_required_idempotency_key(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.punchplay import _watchlist as wl

    http = FakeHTTP([_Resp(200, {"results": [{"index": 0, "status": "inserted", "resolved_tmdb_id": 550}], "inserted": 1})])
    _patch(monkeypatch, wl, http)

    res = wl.add(Adapter(), [{"type": "movie", "title": "Fight Club", "ids": {"tmdb": "550"}}])

    call = http.calls[0]
    assert call["method"] == "POST"
    assert call["url"].endswith("/sync/watchlist")
    key = call["headers"]["Idempotency-Key"]
    assert 8 <= len(key) <= 200
    assert res["confirmed_keys"] == ["tmdb:550"]


def test_bulk_batches_at_one_hundred_items(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.punchplay import _watchlist as wl

    http = FakeHTTP([_Resp(200, {"results": []}), _Resp(200, {"results": []})])
    _patch(monkeypatch, wl, http)

    items = [{"type": "movie", "title": f"M{i}", "ids": {"tmdb": str(1000 + i)}} for i in range(150)]
    wl.add(Adapter(), items)

    assert len(http.calls) == 2
    assert len(http.calls[0]["json"]["items"]) == 100
    assert len(http.calls[1]["json"]["items"]) == 50


def test_deferred_is_not_treated_as_unresolved(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.punchplay import _watchlist as wl

    http = FakeHTTP([_Resp(200, {"results": [
        {"index": 0, "status": "deferred", "reason": "unmatched", "client_item_id": "a"},
        {"index": 1, "status": "invalid", "error": "bad id"},
        {"index": 2, "status": "inserted", "resolved_tmdb_id": 550},
    ]})])
    _patch(monkeypatch, wl, http)

    res = wl.add(Adapter(), [
        {"type": "movie", "title": "A", "ids": {"imdb": "tt0000001"}},
        {"type": "movie", "title": "B", "ids": {"imdb": "tt0000002"}},
        {"type": "movie", "title": "C", "ids": {"tmdb": "550"}},
    ])

    assert res["deferred_keys"] == ["imdb:tt0000001"]
    assert res["unresolved_keys"] == ["imdb:tt0000002"]
    assert res["confirmed_keys"] == ["tmdb:550"]


def test_http_failure_marks_batch_unresolved(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.punchplay import _watchlist as wl

    http = FakeHTTP([_Resp(500, {"error": "server_error"})])
    _patch(monkeypatch, wl, http)

    res = wl.add(Adapter(), [{"type": "movie", "title": "A", "ids": {"tmdb": "550"}}])

    assert res["ok"] is False
    assert res["confirmed_keys"] == []
    assert res["unresolved_keys"] == ["tmdb:550"]


def test_request_in_progress_retries_same_key(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.punchplay import _common, _watchlist as wl

    monkeypatch.setattr(_common.time, "sleep", lambda *_a: None)
    http = FakeHTTP([
        _Resp(409, {"error": "request_in_progress"}),
        _Resp(200, {"results": [{"index": 0, "status": "inserted", "resolved_tmdb_id": 550}]}),
    ])
    _patch(monkeypatch, wl, http)

    res = wl.add(Adapter(), [{"type": "movie", "title": "A", "ids": {"tmdb": "550"}}])

    assert len(http.calls) == 2
    assert http.calls[0]["headers"]["Idempotency-Key"] == http.calls[1]["headers"]["Idempotency-Key"]
    assert res["confirmed_keys"] == ["tmdb:550"]


def test_indeterminate_retries_with_a_new_key(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.punchplay import _watchlist as wl

    http = FakeHTTP([
        _Resp(409, {"error": "idempotency_indeterminate"}),
        _Resp(200, {"results": [{"index": 0, "status": "inserted", "resolved_tmdb_id": 550}]}),
    ])
    _patch(monkeypatch, wl, http)

    wl.add(Adapter(), [{"type": "movie", "title": "A", "ids": {"tmdb": "550"}}])

    assert http.calls[0]["headers"]["Idempotency-Key"] != http.calls[1]["headers"]["Idempotency-Key"]


# --- watchlist ----------------------------------------------------------------

def test_watchlist_remove_sets_remove_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.punchplay import _watchlist as wl

    http = FakeHTTP([_Resp(200, {"results": [{"index": 0, "status": "removed", "resolved_tmdb_id": 550}]})])
    _patch(monkeypatch, wl, http)

    wl.remove(Adapter(), [{"type": "movie", "title": "A", "ids": {"tmdb": "550"}}])

    assert http.calls[0]["json"]["items"][0]["remove"] is True


def test_watchlist_skips_externally_managed_lists(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.punchplay import _watchlist as wl

    http = FakeHTTP([_Resp(200, {"items": [
        {"id": 1, "isWatchlist": True, "externalSource": "trakt", "name": "Mirrored"},
        {"id": 2, "isWatchlist": True, "externalSource": None, "name": "Watchlist"},
        {"id": 3, "isWatchlist": False, "externalSource": None, "name": "Other"},
    ], "nextCursor": None})])
    _patch(monkeypatch, wl, http)

    found = wl.watchlist_ids(Adapter())

    assert [f["id"] for f in found] == [2]


def test_watchlist_index_parses_tmdb_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.punchplay import _watchlist as wl

    http = FakeHTTP([
        _Resp(200, {"items": [{"id": 2, "isWatchlist": True, "externalSource": None}], "nextCursor": None}),
        _Resp(200, {"items": [
            {"id": 1, "tmdbId": 550, "type": "movie", "isAnime": False, "title": "Fight Club",
             "posterPath": None, "addedAt": "2026-01-01T00:00:00.000Z", "runtime": 139,
             "popularity": 1.0, "releaseDate": "1999-10-15T00:00:00.000Z", "watched": False},
        ], "nextOffset": None, "total": 1}),
    ])
    _patch(monkeypatch, wl, http)

    idx = wl.build_index(Adapter())

    assert idx == {"tmdb:550": {"type": "movie", "ids": {"tmdb": "550"}, "title": "Fight Club", "year": 1999}}


# --- ratings ------------------------------------------------------------------

def test_ratings_round_to_integer_scale(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.punchplay import _ratings as rt

    http = FakeHTTP([_Resp(200, {"results": [{"index": 0, "status": "updated", "resolved_tmdb_id": 550}]})])
    _patch(monkeypatch, rt, http)

    rt.add(Adapter(), [{"type": "movie", "ids": {"tmdb": "550"}, "rating": 7.6}])

    sent = http.calls[0]["json"]["items"][0]
    assert sent["rating"] == 8
    assert isinstance(sent["rating"], int)


def test_ratings_remove_clears_with_null(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.punchplay import _ratings as rt

    http = FakeHTTP([_Resp(200, {"results": [{"index": 0, "status": "updated", "resolved_tmdb_id": 550}]})])
    _patch(monkeypatch, rt, http)

    rt.remove(Adapter(), [{"type": "movie", "ids": {"tmdb": "550"}, "rating": 8}])

    assert http.calls[0]["json"]["items"][0]["rating"] is None


def test_ratings_episode_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.punchplay import _ratings as rt

    http = FakeHTTP([_Resp(200, {"results": [{"index": 0, "status": "updated", "resolved_tmdb_id": 1399}]})])
    _patch(monkeypatch, rt, http)

    rt.add(Adapter(), [{"type": "episode", "ids": {"tmdb": "1399"}, "season": 1, "episode": 2, "rating": 9}])

    sent = http.calls[0]["json"]["items"][0]
    assert sent["scope"] == "episode"
    assert sent["season"] == 1 and sent["episode"] == 2


# --- history ------------------------------------------------------------------

def test_history_client_item_id_is_per_play(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.punchplay import _history as hi

    http = FakeHTTP([_Resp(200, {"results": [
        {"index": 0, "status": "inserted", "resolved_tmdb_id": 550},
        {"index": 1, "status": "inserted", "resolved_tmdb_id": 550},
    ]})])
    _patch(monkeypatch, hi, http)

    hi.add(Adapter(), [
        {"type": "movie", "ids": {"tmdb": "550"}, "watched_at": "2026-01-01T20:00:00Z"},
        {"type": "movie", "ids": {"tmdb": "550"}, "watched_at": "2026-06-01T20:00:00Z"},
    ])

    sent = http.calls[0]["json"]["items"]
    assert sent[0]["client_item_id"] != sent[1]["client_item_id"]
    assert sent[0]["watched_at"] == "2026-01-01T20:00:00.000Z"


def test_history_episode_identifies_by_show_id(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.punchplay import _history as hi

    http = FakeHTTP([_Resp(200, {"results": [{"index": 0, "status": "inserted", "resolved_tmdb_id": 95396}]})])
    _patch(monkeypatch, hi, http)

    hi.add(Adapter(), [{
        "type": "episode", "show_ids": {"tmdb": "95396"}, "ids": {"tmdb": "111"},
        "season": 1, "episode": 2, "series_title": "Severance", "title": "Half Loop",
        "watched_at": "2026-07-18T20:00:00Z",
    }])

    sent = http.calls[0]["json"]["items"][0]
    assert sent["tmdb_id"] == 95396
    assert sent["kind"] == "episode"
    assert sent["season"] == 1 and sent["episode"] == 2
    assert sent["episode_title"] == "Half Loop"


def test_history_rejects_missing_watched_at(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.punchplay import _history as hi

    http = FakeHTTP()
    _patch(monkeypatch, hi, http)

    res = hi.add(Adapter(), [{"type": "movie", "ids": {"tmdb": "550"}}])

    assert http.calls == []
    assert res["unresolved_keys"] == ["tmdb:550"]


def test_history_index_collects_entry_ids_for_rewatches(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.punchplay import _history as hi

    http = FakeHTTP([_Resp(200, {"items": [
        {"id": 7, "type": "movie", "tmdbId": 550, "title": "Fight Club", "year": 1999, "watchedAt": "2026-01-01T20:00:00.000Z"},
        {"id": 9, "type": "movie", "tmdbId": 550, "title": "Fight Club", "year": 1999, "watchedAt": "2026-06-01T20:00:00.000Z"},
    ], "nextCursor": None})])
    _patch(monkeypatch, hi, http)

    idx = hi.build_index(Adapter())

    entry = idx["tmdb:550"]
    assert sorted(entry["_punchplay_history_ids"]) == ["7", "9"]
    assert entry["watched_at"] == "2026-06-01T20:00:00.000Z"


def test_history_remove_deletes_each_entry_id(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.punchplay import _history as hi

    http = FakeHTTP([_Resp(204), _Resp(204)])
    _patch(monkeypatch, hi, http)

    res = hi.remove(Adapter(), [{"type": "movie", "ids": {"tmdb": "550"}, "_punchplay_history_ids": ["7", "9"]}])

    assert [c["method"] for c in http.calls] == ["DELETE", "DELETE"]
    assert http.calls[0]["url"].endswith("/watch-history/7")
    assert res["confirmed_keys"] == ["tmdb:550"]


def test_history_episode_remove_without_entry_id_is_unresolved(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.punchplay import _history as hi

    http = FakeHTTP()
    _patch(monkeypatch, hi, http)

    res = hi.remove(Adapter(), [{"type": "episode", "show_ids": {"tmdb": "95396"}, "season": 1, "episode": 2}])

    assert http.calls == []
    assert res["unresolved_keys"] == ["tmdb:95396#s01e02"]


# --- progress -----------------------------------------------------------------

def test_progress_write_uses_playback_progress_action(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.punchplay import _progress as pr

    http = FakeHTTP([_Resp(200, {})])
    _patch(monkeypatch, pr, http)

    res = pr.add(Adapter(), [{
        "type": "movie", "title": "Fight Club", "year": 1999, "ids": {"tmdb": "550"},
        "position_seconds": 1800, "duration_seconds": 8340,
    }])

    call = http.calls[0]
    assert call["url"].endswith("/playback/progress")
    assert call["json"]["media_type"] == "movie"
    assert round(call["json"]["progress"], 4) == 0.2158
    assert res["confirmed_keys"] == ["tmdb:550"]


def test_progress_remove_dismisses_in_progress_entry(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.punchplay import _progress as pr

    http = FakeHTTP([_Resp(204)])
    _patch(monkeypatch, pr, http)

    res = pr.remove(Adapter(), [{"type": "movie", "ids": {"tmdb": "550"}, "_punchplay_progress_id": "4"}])

    assert http.calls[0]["method"] == "DELETE"
    assert http.calls[0]["url"].endswith("/playback/in-progress/4")
    assert res["confirmed_keys"] == ["tmdb:550"]


# --- rate limits --------------------------------------------------------------

def test_documented_budgets_match_the_docs() -> None:
    from providers.sync.punchplay._common import RATE_BUDGETS

    assert RATE_BUDGETS["bulk"] == (30, 60.0)
    assert RATE_BUDGETS["playback"] == (120, 300.0)
    assert RATE_BUDGETS["sync_read"] == (120, 60.0)
    assert RATE_BUDGETS["history_read"] == (120, 60.0)
    assert RATE_BUDGETS["token_read"] == (300, 60.0)
    assert RATE_BUDGETS["token_write"] == (120, 60.0)


def test_endpoint_bucket_routing() -> None:
    from providers.sync.punchplay._common import _endpoint_bucket

    assert _endpoint_bucket("POST", "https://punchplay.tv/api/platform/v1/sync/history") == "bulk"
    assert _endpoint_bucket("POST", "https://punchplay.tv/api/platform/v1/sync/watchlist") == "bulk"
    assert _endpoint_bucket("POST", "https://punchplay.tv/api/platform/v1/playback/progress") == "playback"
    assert _endpoint_bucket("GET", "https://punchplay.tv/api/platform/v1/me/sync/snapshot") == "sync_read"
    assert _endpoint_bucket("GET", "https://punchplay.tv/api/platform/v1/me/history") == "history_read"
    assert _endpoint_bucket("GET", "https://punchplay.tv/api/platform/v1/me/lists") == "lists_read"
    assert _endpoint_bucket("GET", "https://punchplay.tv/api/platform/v1/me") is None


def test_governor_blocks_once_a_bucket_is_exhausted(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.punchplay._common import RateGovernor

    slept: list[float] = []
    clock = {"t": 1000.0}
    gov = RateGovernor({"bulk": (3, 60.0), "token_write": (100, 60.0)})

    monkeypatch.setattr("providers.sync.punchplay._common.time.monotonic", lambda: clock["t"])
    monkeypatch.setattr("providers.sync.punchplay._common.time.sleep", lambda s: (slept.append(s), clock.__setitem__("t", clock["t"] + s)))

    url = "https://punchplay.tv/api/platform/v1/sync/history"
    for _ in range(3):
        assert gov.acquire("POST", url) == 0.0
    assert slept == []

    gov.acquire("POST", url)

    assert slept and abs(slept[0] - 60.0) < 0.01


def test_governor_backs_off_on_low_remaining_header(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.punchplay._common import RateGovernor

    clock = {"mono": 500.0, "wall": 1_000_000.0}
    slept: list[float] = []
    monkeypatch.setattr("providers.sync.punchplay._common.time.monotonic", lambda: clock["mono"])
    monkeypatch.setattr("providers.sync.punchplay._common.time.time", lambda: clock["wall"])
    monkeypatch.setattr("providers.sync.punchplay._common.time.sleep", lambda s: (slept.append(s), clock.__setitem__("mono", clock["mono"] + s)))

    gov = RateGovernor()
    url = "https://punchplay.tv/api/platform/v1/me/history"

    gov.observe("GET", url, {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": str(int(clock["wall"]) + 12)})
    gov.acquire("GET", url)

    assert slept and abs(slept[0] - 12.0) < 0.5


def test_governor_ignores_healthy_remaining_header() -> None:
    from providers.sync.punchplay._common import RateGovernor

    gov = RateGovernor()
    url = "https://punchplay.tv/api/platform/v1/me/history"
    gov.observe("GET", url, {"X-RateLimit-Remaining": "250", "X-RateLimit-Reset": "99999999999"})

    assert gov.acquire("GET", url) == 0.0


def test_budgets_are_configurable() -> None:
    from providers.sync.punchplay._common import budgets_from_cfg

    out = budgets_from_cfg({"rate_limit": {"bulk_per_min": 10, "playback_per_5min": 60}})

    assert out["bulk"] == (10, 60.0)
    assert out["playback"] == (60, 300.0)
    assert out["sync_read"] == (120, 60.0)


def test_default_config_exposes_rate_limits() -> None:
    from cw_platform.config_base import DEFAULT_CFG

    rl = DEFAULT_CFG["punchplay"]["rate_limit"]

    assert rl["bulk_per_min"] == 30
    assert rl["playback_per_5min"] == 120
    assert rl["sync_read_per_min"] == 120


# --- module dispatch ----------------------------------------------------------

def test_dry_run_does_not_hit_the_api(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync._mod_PUNCHPLAY import OPS
    from providers.sync.punchplay import _common

    http = FakeHTTP()
    monkeypatch.setattr(_common, "punchplay_request", http)

    cfg = {"punchplay": {"access_token": "at"}}
    res = OPS.add(cfg, [{"type": "movie", "ids": {"tmdb": "550"}}], feature="watchlist", dry_run=True)

    assert http.calls == []
    assert res["dry_run"] is True
    assert res["count"] == 1


def test_unsupported_feature_is_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync._mod_PUNCHPLAY import OPS

    res = OPS.add({"punchplay": {"access_token": "at"}}, [], feature="playlists")

    assert res["unsupported"] is True
    assert res["count"] == 0
