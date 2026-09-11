import json
import os
from copy import deepcopy
from types import SimpleNamespace

import pytest

from providers.sync.plex import _history as history
from test_plex_playback_dates import MARKED, PLAYED, playback


BUILD_CATALOG = history._build_history_catalog
SCAN_WATCHED = history._iter_marked_watched_from_library
READ_METADATA = history._pms_fetch_metadata_row


def response(status=200, metadata=None, total=None):
    body = {"Metadata": metadata or []}
    if total is not None:
        body["totalSize"] = total
    return SimpleNamespace(ok=status == 200, status_code=status, headers={"content-type": "application/json"},
                           json=lambda: {"MediaContainer": body})


def test_cached_library_items_do_not_require_individual_metadata_requests(playback, monkeypatch):
    for i in range(40):
        rk = str(100 + i)
        playback.catalog.add({"rk": rk, "type": "movie", "title": rk, "ids": {"tmdb": rk, "plex": rk},
                              "watched": True, "last_viewed_at": MARKED})
        playback.state["history"].append(SimpleNamespace(type="movie", ratingKey=rk, title=rk,
                                                       viewedAt=PLAYED + i, accountID=17))
    monkeypatch.setattr(history, "_pms_fetch_metadata_row", lambda *a, **kw: pytest.fail("Unnecessary item request"))
    monkeypatch.setattr(history, "_iter_marked_watched_from_library", lambda *a, **kw: pytest.fail("Second library scan"))
    first = history.build_index(playback.adapter())
    assert len(first) == 41
    assert history.build_index(playback.adapter()) == first


@pytest.mark.parametrize("status", [401, 500])
def test_unknown_item_state_aborts_without_resurrecting_cached_play(playback, monkeypatch, status):
    ad = playback.adapter()
    history.build_index(ad)
    playback.catalog.by_rk["42"]["watched"] = False
    assert history.build_index(ad) == {}
    path = history._playback_cache_path(ad, {"1"})
    before = path.read_bytes()
    playback.catalog.by_rk.clear()
    ad.client.server._session = SimpleNamespace(headers={}, get=lambda *a, **kw: response(status))
    monkeypatch.setattr(history, "_pms_fetch_metadata_row", READ_METADATA)
    with pytest.raises(RuntimeError, match=f"metadata_http_{status}"):
        history.build_index(ad)
    assert path.read_bytes() == before


def test_removed_library_item_keeps_real_history_until_history_itself_is_deleted(playback, monkeypatch):
    ad = playback.adapter()
    expected = history.build_index(ad)
    playback.catalog.by_rk.clear()
    ad.client.server._session = SimpleNamespace(headers={}, get=lambda *a, **kw: response(404))
    monkeypatch.setattr(history, "_pms_fetch_metadata_row", READ_METADATA)
    result = history.build_index(ad)
    assert set(result) == set(expected)
    assert next(iter(result.values()))["watched_at"] == history._iso(PLAYED)
    playback.state["history"] = []
    assert history.build_index(ad, force=True) == {}


@pytest.mark.parametrize("failure", ["timeout", "empty", "wrong_item"])
def test_invalid_metadata_check_cannot_publish_a_snapshot(playback, monkeypatch, failure):
    ad = playback.adapter()
    history.build_index(ad)
    playback.catalog.by_rk.clear()

    def get(*a, **kw):
        if failure == "timeout":
            raise TimeoutError("Unavailable")
        return response(metadata=[{"ratingKey": "different", "viewCount": 1}] if failure == "wrong_item" else [])

    ad.client.server._session = SimpleNamespace(headers={}, get=get)
    monkeypatch.setattr(history, "_pms_fetch_metadata_row", READ_METADATA)
    with pytest.raises(RuntimeError, match="plex_history_metadata"):
        history.build_index(ad)


@pytest.mark.parametrize("offset", [0, -60])
def test_cursor_overlap_recovers_recent_late_plays(playback, offset):
    history.build_index(playback.adapter())
    late = deepcopy(playback.state["history"][0])
    late.ratingKey = "43"
    late.viewedAt = PLAYED + offset
    playback.state["history"].append(late)
    playback.catalog.add({"rk": "43", "type": "movie", "title": "Late play", "ids": {"plex": "43", "tmdb": "124"},
                          "watched": True, "last_viewed_at": MARKED})
    assert len(history.build_index(playback.adapter())) == 2


@pytest.mark.parametrize("elapsed", [12 * 3600, 24 * 3600, 30 * 86400])
def test_scheduled_runs_stay_incremental_and_pick_up_new_plays(playback, monkeypatch, elapsed):
    now = [2000000000]
    monkeypatch.setattr(history.time, "time", lambda: now[0])
    first = history.build_index(playback.adapter())
    assert "mindate" not in playback.calls[-1]
    now[0] += elapsed
    latest = deepcopy(playback.state["history"][0])
    latest.viewedAt = now[0] - 60
    playback.state["history"].append(latest)
    result = history.build_index(playback.adapter())
    assert playback.calls[-1]["mindate"].timestamp() == PLAYED - 120
    assert set(first).issubset(result)
    assert {row["watched_at"] for row in result.values()} == {history._iso(PLAYED), history._iso(latest.viewedAt)}


def test_force_refresh_recovers_backfills_and_reconciles_deleted_events(playback, monkeypatch):
    now = [2000000000]
    monkeypatch.setattr(history.time, "time", lambda: now[0])
    ad = playback.adapter()
    old = deepcopy(playback.state["history"][0])
    old.viewedAt = PLAYED - 86400
    playback.state["history"].append(old)
    assert len(history.build_index(ad)) == 2
    playback.state["history"].remove(old)
    backfill = deepcopy(old)
    backfill.viewedAt -= 86400
    playback.state["history"].append(backfill)
    assert old.viewedAt != backfill.viewedAt
    assert {row["watched_at"] for row in history.build_index(ad).values()} == {history._iso(PLAYED), history._iso(old.viewedAt)}
    now[0] += 12 * 3600
    assert {row["watched_at"] for row in history.build_index(ad).values()} == {history._iso(PLAYED), history._iso(old.viewedAt)}
    assert "mindate" in playback.calls[-1]
    refreshed = history.build_index(ad, force=True)
    assert {row["watched_at"] for row in refreshed.values()} == {history._iso(PLAYED), history._iso(backfill.viewedAt)}
    assert "mindate" not in playback.calls[-1]


def test_clear_state_makes_next_playback_read_full(playback, monkeypatch):
    from api import maintenanceAPI

    ad = playback.adapter()
    history.build_index(ad)
    path = history._playback_cache_path(ad, {"1"})
    config_dir = playback.directory / "config"
    config_dir.mkdir()
    monkeypatch.setattr(maintenanceAPI, "_cw", lambda: (
        config_dir / "cache", config_dir, playback.directory, None, None, None,
    ))
    late = deepcopy(playback.state["history"][0])
    late.viewedAt -= 86400
    playback.state["history"] = [late]
    result = maintenanceAPI.clear_state_minimal()
    assert result["ok"] is True
    assert path.name in result["removed_sync_state"]
    assert not path.exists()
    refreshed = history.build_index(playback.adapter())
    assert "mindate" not in playback.calls[-1]
    assert {row["watched_at"] for row in refreshed.values()} == {history._iso(late.viewedAt)}


def test_date_filter_fallback_reconciles_full_results(playback):
    ad = playback.adapter()
    history.build_index(ad)
    late = deepcopy(playback.state["history"][0])
    late.viewedAt -= 86400
    playback.state["history"] = [late]

    def get(**kwargs):
        if "mindate" in kwargs:
            raise ValueError("Unsupported date filter")
        return [late]

    ad.client.server.history = get
    result = history.build_index(ad)
    assert len(result) == 1
    assert next(iter(result.values()))["watched_at"] == history._iso(late.viewedAt)


def test_unchanged_playback_cache_is_not_written_again(playback, monkeypatch):
    now = 2000000000
    monkeypatch.setattr(history.time, "time", lambda: now)
    ad = playback.adapter()
    history.build_index(ad)
    monkeypatch.setattr(history, "write_json", lambda *a, **kw: pytest.fail("Unchanged cache rewritten"))
    history.build_index(ad)


def test_both_fallback_guid_spellings_share_effective_cache_options(playback):
    ad = playback.adapter()
    disabled = history._playback_cache_path(ad, {"1"})
    ad.config["plex"]["fallback_guid"] = True
    lowercase = history._playback_cache_path(ad, {"1"})
    assert lowercase != disabled
    ad.config["plex"].pop("fallback_guid")
    ad.config["plex"]["fallback_GUID"] = True
    assert history._playback_cache_path(ad, {"1"}) == lowercase


def test_failed_force_refresh_preserves_playback_cache_and_snapshot(playback):
    ad = playback.adapter()
    ad.config["plex"]["history"]["include_marked_watched"] = False
    expected = history.build_index(ad)
    path = history._playback_cache_path(ad, {"1"})
    original = path.read_bytes()
    playback.state["denied"] = True
    assert history.build_index(ad, force=True) == expected
    assert path.read_bytes() == original


def test_cold_playback_failure_cannot_replace_dates_with_manual_state(playback):
    ad = playback.adapter()

    def fail(**kwargs):
        raise TimeoutError("Playback history unavailable")

    ad.client.server.history = fail
    with pytest.raises(RuntimeError, match="playback_unavailable_without_cache"):
        history.build_index(ad)
    assert not list(playback.directory.glob("*playback*"))


def test_incomplete_catalog_does_not_publish_or_update_cache(playback):
    ad = playback.adapter()
    history.build_index(ad)
    path = history._playback_cache_path(ad, {"1"})
    original = path.read_bytes()
    playback.catalog.live_complete = False
    with pytest.raises(RuntimeError, match="incomplete_watched_catalog"):
        history.build_index(ad)
    assert path.read_bytes() == original


@pytest.mark.parametrize("failure", ["http", "malformed", "repeated", "incomplete"])
def test_actual_library_scan_rejects_incomplete_data(playback, monkeypatch, failure):
    ad = playback.adapter()
    ad.libraries = lambda **kw: [SimpleNamespace(key="1", type="movie", title="Movies")]
    row = {"ratingKey": "42", "type": "movie", "title": "Movie", "viewCount": 1, "lastViewedAt": MARKED}

    def get(*a, **kw):
        if failure == "http":
            return response(500)
        if failure == "malformed":
            return SimpleNamespace(ok=True, status_code=200, headers={"content-type": "application/json"}, json=lambda: {})
        start = kw["params"]["X-Plex-Container-Start"]
        return response(metadata=[] if failure == "incomplete" and start else [row], total=3)

    ad.client.server._session = SimpleNamespace(headers={}, get=get)
    monkeypatch.setattr(history, "_iter_marked_watched_from_library", SCAN_WATCHED)
    monkeypatch.setattr(history, "_build_guid_index", lambda *a, **kw: {"movies": {"tmdb://123": "42"}, "shows": {}})
    monkeypatch.setattr(history, "normalize_discover_row", lambda *a, **kw: {"type": "movie", "ids": {"plex": "42", "tmdb": "123"}})
    with pytest.raises(RuntimeError, match="plex_watched_scan"):
        BUILD_CATALOG(ad, {"1"}, live=True)


def test_cleanup_only_removes_old_variants_of_the_same_pair_and_user(playback):
    now = 2000000000
    current = history._playback_cache_path(playback.adapter(), {"1"})
    old = history._playback_cache_path(playback.adapter(), {"2"})
    other_pair = history._playback_cache_path(playback.adapter(pair="different"), {"2"})
    other_user = history._playback_cache_path(playback.adapter(account=18), {"2"})
    for path in (current, old, other_pair, other_user):
        path.write_text(json.dumps({}))
        os.utime(path, (now - 31 * 86400, now - 31 * 86400))
    history._prune_playback_caches(current, now)
    assert not old.exists()
    assert all(path.exists() for path in (current, other_pair, other_user))
