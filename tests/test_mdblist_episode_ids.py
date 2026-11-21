# tests/test_mdblist_episode_ids.py
# CrossWatch - MDBList history writes and matching by episode ids
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from cw_platform.history_events import history_sync_key
from providers.sync.mdblist import _common, _history as history


WATCHED = "2025-01-01T12:00:00Z"
KAI_SHOW = {"tmdb": 61709, "tvdb": 88031}
KAI_S2E6 = {"tmdb": 1024341, "tvdb": 1280931}
KAI_S2E5 = {"tmdb": 1024340, "tvdb": 1272341}


def episode(ids, show_ids=None, season=1, number=32, *, rewatch=False):
    item = {
        "type": "episode",
        "ids": dict(ids),
        "show_ids": dict(KAI_SHOW if show_ids is None else show_ids),
        "season": season,
        "episode": number,
        "series_title": "Dragon Ball Z Kai",
        "watched_at": WATCHED,
    }
    if rewatch:
        item.update(_cw_rewatch_sync=True, _cw_event_key=history_sync_key(item, event_mode=True))
    return item


def echo(ids, season=2, number=6, added=True):
    return {
        "type": "episode",
        "watched_at": WATCHED.replace("Z", ".000Z"),
        "ids": dict(ids),
        "season": season,
        "number": number,
        "show": {"mdblist": "2xel1", "tmdb": 61709},
        "added": added,
    }


def adapter(*, rewatches):
    cfg = {"mdblist": {"auth_method": "api_key", "api_key": "k", "history_write_delay_ms": 0}}
    if rewatches:
        cfg["_cw_history_rewatches"] = True
    return SimpleNamespace(
        config=cfg,
        instance_id="default",
        cfg=SimpleNamespace(timeout=1, max_retries=0),
        client=SimpleNamespace(session=object()),
    )


@pytest.fixture
def remote(tmp_path, monkeypatch):
    monkeypatch.setattr(_common, "STATE_DIR", tmp_path)
    monkeypatch.setattr(history, "_load_cache", lambda: {})
    monkeypatch.setattr(history, "_save_cache", lambda *_a, **_k: None)
    monkeypatch.setattr(history, "update_watermark_if_new", lambda *_a, **_k: None)
    monkeypatch.setattr(history.time, "sleep", lambda _seconds: None)
    calls, responses = [], []

    def request(_adapter, method, url, **kwargs):
        calls.append({"method": method, "url": url, **kwargs})
        payload = responses.pop(0)
        return SimpleNamespace(status_code=200, text=json.dumps(payload), json=lambda: payload)

    monkeypatch.setattr(history, "mdblist_request", request)
    return calls, responses


def test_tvdb_only_episode_goes_flat():
    body, accepted = history._bucketize(None, [episode({"tvdb": 1280931})], unwatch=False)

    assert body == {"episodes": [{"ids": {"tvdb": 1280931}, "watched_at": WATCHED}]}
    assert accepted[0]["season"] == 1 and accepted[0]["episode"] == 32


def test_all_episode_ids_are_sent():
    body, _ = history._bucketize(None, [episode({**KAI_S2E6, "imdb": "tt1544685"})], unwatch=False)

    assert body == {"episodes": [{"ids": KAI_S2E6, "watched_at": WATCHED}]}


@pytest.mark.parametrize(
    "ids, show_ids",
    [
        ({"tvdb": 1280931}, {"tmdb": 61709}),
        ({"imdb": "tt1544685"}, KAI_SHOW),
        (KAI_SHOW, KAI_SHOW),
    ],
    ids=["show_tvdb_unknown", "imdb_only", "show_ids_only"],
)
def test_episode_without_usable_own_ids_stays_nested(ids, show_ids):
    body, _ = history._bucketize(None, [episode(ids, show_ids)], unwatch=False)

    assert "episodes" not in body
    seasons = body["shows_nested"][0]["seasons"]
    assert seasons == [{"number": 1, "episodes": [{"number": 32, "watched_at": WATCHED}]}]


def test_unwatch_uses_flat_episode_ids(remote):
    calls, responses = remote
    responses.append({"removed": {"episodes": 1}})

    applied, unresolved = history.remove(adapter(rewatches=False), [episode({"tvdb": 1280931})])

    assert applied == 1 and unresolved == []
    assert calls[0]["url"] == history.URL_REMOVE
    assert calls[0]["json"] == {"episodes": [{"ids": {"tvdb": 1280931}, "watched_at": WATCHED}]}


@pytest.mark.parametrize("source_ids", [{"tvdb": 1280931}, KAI_S2E6], ids=["tvdb_only", "tmdb_and_tvdb"])
def test_play_echo_in_other_numbering_is_confirmed(remote, source_ids):
    calls, responses = remote
    item = episode(source_ids, rewatch=True)
    responses.append({"updated": {"episodes": 1}, "not_found": {}, "plays": [echo(KAI_S2E6)]})

    result = history.add(adapter(rewatches=True), [item])

    assert calls[0]["params"]["report_added"] == "true"
    assert result["unresolved"] == []
    assert result["confirmed_keys"] == [item["_cw_event_key"]]


def test_conflicting_ids_follow_the_tmdb_winner(remote):
    _, responses = remote
    item = episode({"tmdb": 1024340, "tvdb": 1280931}, rewatch=True)
    responses.append({"updated": {"episodes": 1}, "not_found": {}, "plays": [echo(KAI_S2E5, number=5)]})

    result = history.add(adapter(rewatches=True), [item])

    assert result["unresolved"] == []
    assert result["confirmed_keys"] == [item["_cw_event_key"]]


def test_echo_for_another_episode_is_not_confirmed(remote):
    _, responses = remote
    item = episode({"tvdb": 1280931}, rewatch=True)
    responses.append({"updated": {"episodes": 1}, "not_found": {}, "plays": [echo(KAI_S2E5, number=5)]})

    result = history.add(adapter(rewatches=True), [item])

    assert result["confirmed_keys"] == []
    assert {entry["hint"] for entry in result["unresolved"]} == {"play_response_unmatched", "mdblist_play_report_missing"}


def test_already_added_play_is_skipped_not_failed(remote):
    _, responses = remote
    item = episode({"tvdb": 1280931}, rewatch=True)
    responses.append({"updated": {"episodes": 0}, "not_found": {}, "plays": [echo(KAI_S2E6, added=False)]})

    result = history.add(adapter(rewatches=True), [item])

    assert result["unresolved"] == []
    assert result["skipped_keys"] == [item["_cw_event_key"]]


def test_not_found_by_tvdb_reports_the_sent_item(remote):
    _, responses = remote
    item = episode({"tvdb": 1280931})
    responses.append({"updated": {}, "not_found": {"episodes": [{"ids": {"tvdb": 1280931}}]}})

    applied, unresolved = history.add(adapter(rewatches=False), [item])

    assert applied == 0
    assert len(unresolved) == 1
    assert unresolved[0]["hint"] == "not_found"
    assert unresolved[0]["item"]["season"] == 1 and unresolved[0]["item"]["episode"] == 32


def test_error_rows_are_unresolved(remote):
    _, responses = remote
    item = episode({"tvdb": 1280931})
    responses.append({
        "updated": {"episodes": 0},
        "not_found": {},
        "errors": [{"type": "episode", "ids": {"tvdb": 1280931}, "error": "Episode ids missing; expected tmdb or tvdb id"}],
        "plays": [],
    })

    applied, unresolved = history.add(adapter(rewatches=False), [item])

    assert applied == 0
    assert [entry["hint"] for entry in unresolved] == ["mdblist_error"]
    assert unresolved[0]["item"]["episode"] == 32
