# /tests/test_simkl_anime_retry_confirmation.py
# CrossWatch - SIMKL history mapping opt-in and single-attempt writes
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy
import json
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock

import pytest

from cw_platform.anime_mapping.episodes import Resolution
from providers.sync.simkl import _history as history


def _episode(*, anime=False):
    return {
        "type": "episode", "season": 22 if anime else 1, "episode": 1,
        "watched_at": "2026-07-18T20:00:00Z",
        "show_ids": {"tmdb": "37854", "tvdb": "81797"} if anime else {"tmdb": "34307", "tvdb": "161511"},
    }


def _response(payload, status=200, headers=None):
    return SimpleNamespace(status_code=status, ok=status < 400, headers=headers or {}, text=json.dumps(payload), json=lambda: payload)


@pytest.fixture
def env(config_base, monkeypatch):
    monkeypatch.setattr(history, "_headers", lambda *a, **k: {})
    monkeypatch.setattr(history, "_inject_adds_into_cache", Mock())
    monkeypatch.setattr(history, "_remember_source_aliases", Mock())
    monkeypatch.setattr(history, "_verify_removals", lambda adapter, items: (items, [], True))
    monkeypatch.setattr(history, "state_file", lambda name: config_base / name)
    resolver = Mock(side_effect=lambda item, **kw: Resolution(
        absolute=1088 + item["episode"], namespace="anidb", target_id="69",
        basis="anibridge_absolute", entry="tmdb_direct",
    ) if item["show_ids"].get("tmdb") == "37854" else None)
    monkeypatch.setattr(history, "resolve_absolute", resolver)
    monkeypatch.setattr(history, "_offline_simkl_id", lambda *a: None)

    def get(url, **kwargs):
        if url == history.URL_REDIRECT:
            return _response({}, 301, {"Location": "https://simkl.com/anime/38636/one-piece/"})
        if url.endswith("/anime/episodes/38636"):
            return _response([{"episode": n, "tvdb": {"season": 22, "episode": n - 1085}} for n in (1089, 1090)])
        raise AssertionError(f"Unexpected GET: {url}")

    session = SimpleNamespace(get=Mock(side_effect=get), post=Mock(return_value=_response({
        "added": {"episodes": 1}, "not_found": {},
    })))
    adapter = SimpleNamespace(
        client=SimpleNamespace(session=session), cfg=SimpleNamespace(timeout=5, history_chunk_size=100),
        raw_cfg={"anime_mapping": {"enabled": True, "features": ["watchlist", "ratings"]},
                 "_cw_pair_feature_options": {"feature": "history", "use_anime_mapping": True}},
    )
    return adapter, session, resolver


@pytest.mark.parametrize("operation", ["add", "remove"])
@pytest.mark.parametrize("enabled", [False, True])
def test_ordinary_tv_never_triggers_anime_lookups(env, operation, enabled):
    adapter, session, _ = env
    adapter.raw_cfg["_cw_pair_feature_options"]["use_anime_mapping"] = enabled
    ok, unresolved = getattr(history, operation)(adapter, [_episode()])
    assert (ok, unresolved) == (1, [])
    session.get.assert_not_called()
    session.post.assert_called_once()
    assert "shows" in session.post.call_args.kwargs["json"]


@pytest.mark.parametrize("operation", ["add", "remove"])
@pytest.mark.parametrize("disabled_by", ["pair", "global", "wrong_feature", "missing_option"])
def test_mapping_disabled_skips_lookups_even_with_anime_hints(env, monkeypatch, operation, disabled_by):
    adapter, session, resolver = env
    if disabled_by == "global":
        adapter.raw_cfg["anime_mapping"]["enabled"] = False
    elif disabled_by == "wrong_feature":
        adapter.raw_cfg["_cw_pair_feature_options"]["feature"] = "ratings"
    elif disabled_by == "missing_option":
        adapter.raw_cfg["_cw_pair_feature_options"].pop("use_anime_mapping")
    else:
        adapter.raw_cfg["_cw_pair_feature_options"]["use_anime_mapping"] = False
    monkeypatch.setattr(history, "_load_anime_resolve_cache", lambda: history._AnimeResolveState({"81797": "38636"}, {}))
    item = _episode(anime=True)
    item.update(simkl_bucket="anime", _trakt_number_abs=1089,
                _cw_anime_map={"absolute": 1089, "namespace": "anidb", "target_id": "69"})
    getattr(history, operation)(adapter, [item])
    resolver.assert_not_called()
    session.get.assert_not_called()
    for call in session.post.call_args_list:
        assert "anime" not in call.kwargs["json"]


@pytest.mark.parametrize("operation", ["add", "remove"])
@pytest.mark.parametrize("offline", [False, True])
@pytest.mark.parametrize("absolute_hint", [False, True])
def test_unmarked_anime_maps_before_write_and_reuses_identity(env, monkeypatch, operation, offline, absolute_hint):
    adapter, session, resolver = env
    if offline:
        monkeypatch.setattr(history, "_offline_simkl_id", lambda *a: "38636")
    item = _episode(anime=True)
    item["show_ids"].pop("tvdb")
    if absolute_hint:
        item["_trakt_number_abs"] = 1089
    second = deepcopy(item)
    second["episode"] = 2
    if absolute_hint:
        second["_trakt_number_abs"] = 1090
    ok, unresolved = getattr(history, operation)(adapter, [item, second])
    assert (ok, unresolved) == (2, [])
    assert resolver.call_count == 2
    session.post.assert_called_once()
    body = session.post.call_args.kwargs["json"]
    assert "shows" not in body
    assert body["anime"][0]["ids"] == {"simkl": "38636"}
    assert [ep["number"] for ep in body["anime"][0]["episodes"]] == [1089, 1090]
    redirects = [call for call in session.get.call_args_list if call.args[0] == history.URL_REDIRECT]
    assert len(redirects) == (0 if offline else 1)
    if redirects:
        assert redirects[0].kwargs["params"]["anidb"] == "69"
        assert "tvdb" not in redirects[0].kwargs["params"]


@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("confirmed_anime", [False, True])
def test_not_found_never_starts_another_anime_write(env, enabled, confirmed_anime):
    adapter, session, _ = env
    adapter.raw_cfg["_cw_pair_feature_options"]["use_anime_mapping"] = enabled
    item = _episode()
    rejected = {"ids": item["show_ids"], "seasons": [{"number": 1, "episodes": [{"number": 1}]}]}
    if confirmed_anime:
        rejected["response"] = {"simkl_type": "anime", "ids": {"simkl": 38636}}
    session.post.return_value = _response({"added": {"episodes": 0}, "not_found": {"episodes": [rejected]}})
    ok, unresolved = history.add(adapter, [item])
    assert ok == 0
    assert [row["hint"] for row in unresolved] == ["simkl_not_found:episodes"]
    assert adapter._simkl_history_add_confirmed_keys == []
    cast(Mock, history._inject_adds_into_cache).assert_not_called()
    session.post.assert_called_once()
    session.get.assert_not_called()


def test_native_anime_not_found_does_not_fall_back_to_shows(env):
    adapter, session, _ = env
    session.post.return_value = _response({"added": {"episodes": 0}, "not_found": {"episodes": [
        {"ids": {"simkl": "38636"}, "episodes": [{"number": 1089}]},
    ]}})
    ok, unresolved = history.add(adapter, [_episode(anime=True)])
    assert ok == 0
    assert len(unresolved) == 1
    assert adapter._simkl_history_add_confirmed_keys == []
    cast(Mock, history._inject_adds_into_cache).assert_not_called()
    session.post.assert_called_once()
    assert "anime" in session.post.call_args.kwargs["json"]


@pytest.mark.parametrize("native", [False, True])
def test_not_found_survives_orchestrator_storage_and_analyzer(env, config_base, monkeypatch, native):
    import services.analyzer as analyzer
    from cw_platform.id_map import canonical_key
    from cw_platform.orchestrator import _applier, _unresolved
    from cw_platform.reason_labels import friendly_reason

    adapter, session, _ = env
    item = _episode(anime=native)
    rejected = ({"ids": {"simkl": "38636"}, "episodes": [{"number": 1089}]} if native else
                {"ids": item["show_ids"], "seasons": [{"number": 1, "episodes": [{"number": 1}]}]})
    session.post.return_value = _response({"added": {"episodes": 0}, "not_found": {"episodes": [rejected]}})
    count, failures = history.add(adapter, [item])
    reason = "simkl_not_found:anime:episodes" if native else "simkl_not_found:episodes"
    monkeypatch.setattr(_unresolved, "STATE_DIR", config_base)
    monkeypatch.setenv("CW_PAIR_KEY", "anime-unresolved")
    normalized = _applier._normalize(
        {"ok": False, "count": count, "confirmed_keys": adapter._simkl_history_add_confirmed_keys, "unresolved": failures},
        [item], "apply:add", dst="SIMKL", feature="history", emit=lambda *a, **kw: None,
    )
    assert normalized["confirmed"] == 0
    assert normalized["unresolved_keys"] == [canonical_key(item)]
    pending = _unresolved.load_unresolved_pending("SIMKL", "history")
    assert len(pending) == 1
    assert pending[0]["item"]["show_ids"] == item["show_ids"]
    assert pending[0]["item"]["season"] == item["season"]
    monkeypatch.setattr(analyzer, "_cfg", lambda: {})
    monkeypatch.setattr(analyzer, "_read_cw_state", lambda allowed: {
        path.name: json.loads(path.read_text()) for path in config_base.glob("*.json")
    })
    records = analyzer._unresolved_records(None)
    assert len(records) == 1
    assert records[0]["reason"] == reason
    assert records[0]["reason_message"]
    assert "retry payload" not in records[0]["reason_message"]
    assert friendly_reason(reason) != reason
    assert analyzer._attention_model([], records)["counts"]["pending_retry"] == 1
    session.post.assert_called_once()


@pytest.mark.parametrize("reason", [
    "simkl_not_found:anime:episodes", "simkl_not_found:anime:shows", "simkl_anime_unmapped:episodes",
    "simkl_write_response_ambiguous:anime_count", "simkl_not_found:anime_retry:episodes",
    "simkl_anime_retry_unmapped:episodes", "simkl_write_response_ambiguous:anime_retry_count",
])
def test_current_and_saved_legacy_anime_failures_have_labels(reason):
    from cw_platform.reason_labels import friendly_reason, reason_message

    assert friendly_reason(reason) != reason
    assert reason_message(reason, provider="SIMKL", feature="history")


def test_mapping_off_still_removes_exact_native_identity_without_lookups(env):
    adapter, session, _ = env
    adapter.raw_cfg["_cw_pair_feature_options"]["use_anime_mapping"] = False
    item = _episode(anime=True)
    item["show_ids"]["simkl"] = "38636"
    item.update(simkl_bucket="anime", _simkl_episode_number=1089)
    ok, unresolved = history.remove(adapter, [item])
    assert (ok, unresolved) == (1, [])
    session.get.assert_not_called()
    assert session.post.call_args.kwargs["json"] == {"anime": [{"ids": {"simkl": "38636"}, "episodes": [{"number": 1089}]}]}
