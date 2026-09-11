from copy import deepcopy
from types import SimpleNamespace

import pytest

from cw_platform.log_context import log_run_id
from providers.sync.plex import _common as common, _history as history


PLAYED = 1726353929
MARKED = 1771430695


@pytest.fixture
def playback(monkeypatch, tmp_path):
    monkeypatch.setenv("CW_PAIR_KEY", "cw2_playback_dates")
    monkeypatch.setattr(common, "STATE_DIR", tmp_path)
    token = log_run_id.set("playback-first")
    calls = []
    raw = SimpleNamespace(type="movie", ratingKey="42", title="Recorded title", viewedAt=PLAYED, accountID=17)
    state = {"history": [raw], "denied": False, "scope_ok": True}
    catalog = history.HistoryCatalog()
    catalog.live_complete = True
    catalog.add({"rk": "42", "type": "movie", "title": "Current title", "ids": {"tmdb": "123", "plex": "42"},
                 "watched": True, "view_count": 1, "last_viewed_at": MARKED, "library_id": "1"})

    def read_history(**kwargs):
        calls.append(kwargs)
        if state["denied"]:
            raise PermissionError("History unavailable to this user")
        since = kwargs.get("mindate")
        return [r for r in state["history"] if since is None or r.viewedAt > since.timestamp()]

    def adapter(pair="cw2_playback_dates", account=17, server="http://plex.test", token="test-token"):
        srv = SimpleNamespace(baseurl=server, _token=token, machineIdentifier="server", history=read_history)
        return SimpleNamespace(client=SimpleNamespace(server=srv, user_account_id=account), config={
            "_cw_pair_scope": pair, "plex": {"account_id": account, "history_workers": 1,
                "history": {"include_marked_watched": True}},
        })

    monkeypatch.setattr(history, "home_scope_enter", lambda _: (not state["scope_ok"], False, 17, None))
    monkeypatch.setattr(history, "home_scope_exit", lambda *a: None)
    monkeypatch.setattr(history, "plex_feature_library_ids", lambda *a: {"1"})
    monkeypatch.setattr(history, "_history_force_full", lambda _: False)
    monkeypatch.setattr(history, "_build_history_catalog", lambda *a, **kw: catalog if kw.get("live", True) else history.HistoryCatalog())
    monkeypatch.setattr(history, "_store_history_catalog", lambda *a: None)
    monkeypatch.setattr(history, "_keep_in_snapshot", lambda *a: True)
    monkeypatch.setattr(history, "minimal_from_history_row", lambda r, **kw: {
        "type": r.type, "title": r.title, "ids": {"plex": r.ratingKey, "tmdb": "123"},
    })
    monkeypatch.setattr(history, "_pms_fetch_metadata_row", lambda *a, **kw: {
        "viewCount": int(catalog.by_rk["42"]["watched"]), "lastViewedAt": catalog.by_rk["42"]["last_viewed_at"],
    })
    monkeypatch.setattr(history, "_iter_marked_watched_from_library", lambda *a, **kw: [
        (history._catalog_entry_to_minimal(row), row["last_viewed_at"])
        for row in catalog.by_rk.values() if row["watched"]
    ])
    for name in ("_emit", "_info", "_warn", "_dbg", "_fb_cache_flush"):
        monkeypatch.setattr(history, name, lambda *a, **kw: None)
    yield SimpleNamespace(adapter=adapter, state=state, catalog=catalog, calls=calls, directory=tmp_path)
    log_run_id.reset(token)


def test_playback_dates_survive_restart_and_ignore_legacy_watermark(playback):
    common.state_file("plex_history.watermark.json").write_text('{"by_user":{"acct:17":1771430695}}')
    first = history.build_index(playback.adapter())
    assert len(first) == 1
    assert next(iter(first.values()))["watched_at"] == history._iso(PLAYED)
    assert "mindate" not in playback.calls[0]
    log_run_id.set("playback-second")
    second = history.build_index(playback.adapter())
    assert second == first
    assert playback.calls[-1]["mindate"].timestamp() == PLAYED - 120
    assert all("test-token" not in path.read_text() for path in playback.directory.glob("*"))


@pytest.mark.parametrize("offset", [1, 86400])
def test_real_rewatch_keeps_both_recorded_dates(playback, offset):
    history.build_index(playback.adapter())
    latest = deepcopy(playback.state["history"][0])
    latest.viewedAt = PLAYED + offset
    playback.state["history"].append(latest)
    result = history.build_index(playback.adapter())
    assert {row["watched_at"] for row in result.values()} == {history._iso(PLAYED), history._iso(latest.viewedAt)}
    assert history.build_index(playback.adapter()) == result


def test_manual_unwatch_and_rewatch_keep_recorded_playback_date(playback):
    expected = history.build_index(playback.adapter())
    playback.catalog.by_rk["42"]["watched"] = False
    assert history.build_index(playback.adapter()) == {}
    playback.catalog.by_rk["42"]["watched"] = True
    playback.catalog.by_rk["42"]["last_viewed_at"] += 3600
    assert history.build_index(playback.adapter()) == expected


def test_complete_library_scan_detects_unwatch_without_individual_checks(playback, monkeypatch):
    history.build_index(playback.adapter())
    playback.catalog.by_rk["42"]["watched"] = False
    monkeypatch.setattr(history, "_pms_fetch_metadata_row", lambda *a, **kw: pytest.fail("Catalogue already confirms unwatch"))
    assert history.build_index(playback.adapter()) == {}


def test_disabled_marked_watched_excludes_manual_only_items(playback):
    playback.state["history"] = []
    ad = playback.adapter()
    ad.config["plex"]["history"]["include_marked_watched"] = False
    assert history.build_index(ad) == {}


def test_disabled_marked_watched_keeps_actual_plays_without_manual_state_checks(playback, monkeypatch):
    ad = playback.adapter()
    ad.config["plex"]["history"]["include_marked_watched"] = False
    monkeypatch.setattr(history, "_pms_fetch_metadata_row", lambda *a: pytest.fail("Manual state checks are disabled"))
    first = history.build_index(ad)
    playback.catalog.by_rk["42"]["watched"] = False
    assert history.build_index(ad) == first
    assert next(iter(first.values()))["watched_at"] == history._iso(PLAYED)


def test_marked_watched_toggle_applies_to_existing_playback_cache(playback):
    ad = playback.adapter()
    expected = history.build_index(ad)
    playback.catalog.by_rk["42"]["watched"] = False
    assert history.build_index(ad) == {}
    ad.config["plex"]["history"]["include_marked_watched"] = False
    assert history.build_index(ad) == expected
    ad.config["plex"]["history"]["include_marked_watched"] = True
    assert history.build_index(ad) == {}


def test_manual_only_item_keeps_last_viewed_date_and_sees_updates(playback):
    playback.state["history"] = []
    result = history.build_index(playback.adapter())
    assert next(iter(result.values()))["watched_at"] == history._iso(MARKED)
    playback.catalog.by_rk["42"]["last_viewed_at"] += 3600
    result = history.build_index(playback.adapter())
    assert next(iter(result.values()))["watched_at"] == history._iso(MARKED + 3600)


@pytest.mark.parametrize("change", ["pair", "account", "server", "token", "libraries"])
def test_playback_cache_cannot_cross_scope(playback, monkeypatch, change):
    history.build_index(playback.adapter())
    kwargs = {change: 18 if change == "account" else "different"} if change != "libraries" else {}
    if change == "libraries":
        monkeypatch.setattr(history, "plex_feature_library_ids", lambda *a: {"2"})
    playback.state["history"] = []
    result = history.build_index(playback.adapter(**kwargs))
    assert "mindate" not in playback.calls[-1]
    assert next(iter(result.values()))["watched_at"] == history._iso(MARKED)


def test_shared_user_does_not_use_owner_history(playback):
    playback.state["history"][0].accountID = 1
    result = history.build_index(playback.adapter(account=17))
    assert next(iter(result.values()))["watched_at"] == history._iso(MARKED)
    assert all(call.get("accountID") in (None, 17) for call in playback.calls)


def test_shared_user_without_history_access_uses_own_watched_state(playback):
    playback.state["denied"] = True
    result = history.build_index(playback.adapter(account=17))
    assert next(iter(result.values()))["watched_at"] == history._iso(MARKED)
    assert not list(playback.directory.glob("*playback*"))
    assert all(call.get("accountID") in (None, 17) for call in playback.calls)


def test_failed_user_switch_does_not_read_owner_watched_state(playback, monkeypatch):
    playback.state["scope_ok"] = False
    monkeypatch.setattr(history, "_pms_fetch_metadata_row", lambda *a: pytest.fail("Owner metadata must not be read"))
    result = history.build_index(playback.adapter(account=17))
    assert next(iter(result.values()))["watched_at"] == history._iso(PLAYED)
    assert not list(playback.directory.glob("*playback*"))


def test_corrupt_cache_reloads_history_instead_of_using_old_watermark(playback):
    expected = history.build_index(playback.adapter())
    path = history._playback_cache_path(playback.adapter(), {"1"})
    path.write_text("{}")
    assert history.build_index(playback.adapter()) == expected
    assert "mindate" not in playback.calls[-1]


def test_failed_history_read_does_not_replace_cached_plays(playback):
    expected = history.build_index(playback.adapter())
    path = history._playback_cache_path(playback.adapter(), {"1"})
    original = path.read_bytes()
    playback.state["denied"] = True
    assert history.build_index(playback.adapter()) == expected
    assert path.read_bytes() == original


def test_force_refresh_rebuilds_cached_playback_history(playback):
    history.build_index(playback.adapter())
    playback.state["history"] = []
    result = history.build_index(playback.adapter(), force=True)
    assert next(iter(result.values()))["watched_at"] == history._iso(MARKED)
    assert "mindate" not in playback.calls[-1]


def test_unscoped_adapter_reads_full_history_without_sharing_cache(playback, monkeypatch):
    monkeypatch.setattr(history, "_pair_scope", lambda: None)
    first = history.build_index(playback.adapter(pair=""))
    assert history.build_index(playback.adapter(pair="")) == first
    assert all("mindate" not in call for call in playback.calls)
    assert not list(playback.directory.glob("*playback*"))
