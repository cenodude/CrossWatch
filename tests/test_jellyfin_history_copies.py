# tests/test_jellyfin_history_copies.py
# CrossWatch - Jellyfin history update all copies tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, Iterable, Mapping

import pytest

from cw_platform.orchestrator import _history_copies as copies
from cw_platform.orchestrator.facade import Orchestrator
from providers.sync.jellyfin import _common as common
from providers.sync.jellyfin import _history as history
from providers.sync.jellyfin import _id_lookup

SHOW = {"Tvdb": "393159", "Tmdb": "203057"}
SERIES = {
    "S_HD": {"Id": "S_HD", "Type": "Series", "ProviderIds": SHOW},
    "S_4K": {"Id": "S_4K", "Type": "Series", "ProviderIds": SHOW},
    "S_BAD": {"Id": "S_BAD", "Type": "Series", "ProviderIds": {"Tvdb": "393159", "Tmdb": "999"}},
}
EPISODES = {
    "S_HD": [{"Id": "E_HD", "Type": "Episode", "SeriesId": "S_HD", "ParentIndexNumber": 2, "IndexNumber": 1,
              "ProviderIds": {"Tvdb": "11729060"}}],
    "S_4K": [{"Id": "E_4K", "Type": "Episode", "SeriesId": "S_4K", "ParentIndexNumber": 2, "IndexNumber": 1,
              "ProviderIds": {"Tvdb": "11729060"}},
             {"Id": "E_4K_2", "Type": "Episode", "SeriesId": "S_4K", "ParentIndexNumber": 2, "IndexNumber": 2}],
    "S_BAD": [{"Id": "E_BAD", "Type": "Episode", "SeriesId": "S_BAD", "ParentIndexNumber": 2, "IndexNumber": 1}],
}
MOVIES = {
    "M1": {"Id": "M1", "Type": "Movie", "ProviderIds": {"Tmdb": "27205", "Imdb": "tt1375666"}},
    "M2": {"Id": "M2", "Type": "Movie", "ProviderIds": {"Tmdb": "27205"}},
    "M3": {"Id": "M3", "Type": "Movie", "ProviderIds": {"Tmdb": "27205", "Imdb": "tt0000001"}},
}


def _catalogue_index() -> dict[str, list[dict[str, Any]]]:
    index: dict[str, list[dict[str, Any]]] = {}
    for row in [*SERIES.values(), *MOVIES.values()]:
        common._index_row_provider_ids(index, row)
    return index


@pytest.fixture
def jf(monkeypatch: pytest.MonkeyPatch):
    fetched: list[str] = []
    cache: dict[Any, Any] = {}
    index = _catalogue_index()
    episode_rows = {sid: list(rows) for sid, rows in EPISODES.items()}
    gone: set[str] = set()
    broken: set[str] = set()

    def episodes(http, uid, sid, start=0, limit=500):
        fetched.append(sid)
        return {"Items": episode_rows.get(sid, [])}

    def find(adapter, feature, kind, pairs, *, strict=False):
        rows = {str(r["Id"]): r for p in pairs for r in index.get(p, []) if r.get("Type") == kind and str(r["Id"]) not in gone}
        if strict and broken & set(rows):
            raise LookupError("identity_validation_failed")
        return [r for _, r in sorted(rows.items()) if r["Id"] not in broken]

    monkeypatch.setattr(history, "get_series_episodes", episodes)
    monkeypatch.setattr(history, "_targeted_cache", lambda adapter, feature: cache)
    monkeypatch.setattr(history, "_merged_guid_priority", lambda adapter: ["tmdb", "imdb", "tvdb"])
    monkeypatch.setattr(_id_lookup, "_catalogue", lambda adapter, feature: {"index": index})
    monkeypatch.setattr(_id_lookup, "find", find)
    for name in ("_dbg", "_info", "_warn"):
        monkeypatch.setattr(history, name, lambda *a, **k: None)
    adapter = SimpleNamespace(cfg=SimpleNamespace(user_id="user"), client=None, history_all_copies=True)
    return SimpleNamespace(adapter=adapter, fetched=fetched, episodes=episode_rows, gone=gone, broken=broken, index=index)


def _episode_event(**extra: Any) -> dict[str, Any]:
    return {"type": "episode", "show_ids": {"tvdb": "393159", "tmdb": "203057"}, "season": 2, "episode": 1,
            "ids": {"tvdb": "11729060"}, "watched_at": "2026-10-01T00:00:00Z", "watched": True, **extra}


def _unplayed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(history, "_dst_user_states", lambda http, uid, iids: {i: (False, None) for i in iids})


def test_copies_requested_only_for_one_way_jellyfin_history() -> None:
    assert copies.history_copies_requested("history", {"update_all_copies": True}, "JELLYFIN")
    assert not copies.history_copies_requested("history", {"update_all_copies": False}, "JELLYFIN")
    assert not copies.history_copies_requested("history", {}, "JELLYFIN")
    assert not copies.history_copies_requested("history", {"update_all_copies": True}, "EMBY")
    assert not copies.history_copies_requested("progress", {"update_all_copies": True}, "JELLYFIN")


def test_take_copy_gaps_strips_field_without_touching_snapshot() -> None:
    row = {"type": "episode", "watched": True, copies.COPIES_FIELD: ["E_4K"]}
    done = {"type": "episode", "watched": True, copies.COPIES_FIELD: []}
    snapshot = {"a": row, "b": done, "c": {"type": "movie"}}
    stripped, gaps = copies.take_copy_gaps(snapshot)
    assert copies.COPIES_FIELD in row
    assert all(copies.COPIES_FIELD not in v for v in stripped.values())
    assert list(gaps) == ["a"] and copies.COPIES_FIELD not in gaps["a"]


def test_copy_gap_adds_plans_only_unplanned_watched_matches() -> None:
    source = {"x": {"id": "x", "watched_at": "2026-10-01T00:00:00Z"}, "y": {"id": "y", "watched_at": "2026-10-01T00:00:00Z"},
              "z": {"id": "z"}, "w": {"id": "w", "watched_at": "2026-10-01T00:00:00Z"}}
    gaps = {"gx": {"id": "x"}, "gy": {"id": "y"}, "gz": {"id": "z"}}
    out = copies.copy_gap_adds(
        source, gaps, [{"id": "y"}],
        alias=lambda idx: {v["id"]: k for k, v in idx.items()},
        find=lambda idx, alias, it: idx.get(alias.get(it["id"], "")),
        minimal=lambda it: {"id": it["id"]},
        key_of=lambda it: it["id"],
    )
    assert out == [{"id": "x"}]


def test_index_attaches_unplayed_copies_from_matching_series(jf, monkeypatch: pytest.MonkeyPatch) -> None:
    _unplayed(monkeypatch)
    events = {"k@1": _episode_event(jellyfin_item_id="E_HD")}
    history._attach_unplayed_copies(jf.adapter, events, {"k@1": "S_HD"})
    assert events["k@1"][history._COPIES_FIELD] == ["E_4K"]
    assert jf.fetched == ["S_HD", "S_4K"]


def test_index_finds_copies_inside_the_same_series(jf, monkeypatch: pytest.MonkeyPatch) -> None:
    _unplayed(monkeypatch)
    jf.episodes["S_HD"].append({**jf.episodes["S_HD"][0], "Id": "E_HD_2160"})
    events = {"k@1": _episode_event(jellyfin_item_id="E_HD")}
    history._attach_unplayed_copies(jf.adapter, events, {"k@1": "S_HD"})
    assert events["k@1"][history._COPIES_FIELD] == ["E_HD_2160", "E_4K"]


def test_index_ignores_stale_catalogue_series(jf, monkeypatch: pytest.MonkeyPatch) -> None:
    _unplayed(monkeypatch)
    jf.gone.add("S_4K")
    events = {"k@1": _episode_event(jellyfin_item_id="E_HD")}
    history._attach_unplayed_copies(jf.adapter, events, {"k@1": "S_HD"})
    assert history._COPIES_FIELD not in events["k@1"]
    assert "S_4K" not in jf.fetched


@pytest.mark.parametrize("states", [{"E_4K": (True, 1)}, {}])
def test_index_skips_played_or_unread_copies(jf, monkeypatch: pytest.MonkeyPatch, states) -> None:
    monkeypatch.setattr(history, "_dst_user_states", lambda http, uid, iids: dict(states))
    events = {"k@1": _episode_event(jellyfin_item_id="E_HD")}
    history._attach_unplayed_copies(jf.adapter, events, {"k@1": "S_HD"})
    assert history._COPIES_FIELD not in events["k@1"]


def test_index_movie_copies_need_agreeing_ids(jf, monkeypatch: pytest.MonkeyPatch) -> None:
    _unplayed(monkeypatch)
    events = {"m@1": {"type": "movie", "ids": {"tmdb": "27205", "imdb": "tt1375666"}, "jellyfin_item_id": "M1"}}
    history._attach_unplayed_copies(jf.adapter, events, {})
    assert events["m@1"][history._COPIES_FIELD] == ["M2"]


def _items_client(rows: Mapping[str, Mapping[str, Any]]):
    def get(path, *, params=None):
        iid = path.rsplit("/", 1)[-1]
        row = rows.get(iid)
        return SimpleNamespace(status_code=200 if row else 404, json=lambda: dict(row or {}))
    return SimpleNamespace(get=get)


def test_item_copies_expands_episode_and_movie(jf) -> None:
    jf.adapter.client = _items_client({**SERIES, **MOVIES, "E_HD": EPISODES["S_HD"][0]})
    assert history._item_copies(jf.adapter, "E_HD") == ["E_HD", "E_4K"]
    assert history._item_copies(jf.adapter, "M1") == ["M1", "M2"]


def test_item_copies_reports_failed_discovery(jf, monkeypatch: pytest.MonkeyPatch) -> None:
    jf.adapter.client = _items_client({**SERIES, "E_HD": EPISODES["S_HD"][0]})
    assert history._item_copies(jf.adapter, "missing") is None
    monkeypatch.setattr(history, "get_series_episodes", lambda *a, **k: None)
    assert history._item_copies(jf.adapter, "E_HD") is None
    monkeypatch.setattr(_id_lookup, "_catalogue", lambda adapter, feature: None)
    assert history._item_copies(jf.adapter, "E_HD") is None


def test_validation_error_is_a_discovery_failure(jf, monkeypatch: pytest.MonkeyPatch) -> None:
    _unplayed(monkeypatch)
    jf.broken.add("S_4K")
    jf.adapter.client = _items_client({**SERIES, "E_HD": EPISODES["S_HD"][0]})
    assert history._item_copies(jf.adapter, "E_HD") is None
    events = {"k@1": _episode_event(jellyfin_item_id="E_HD")}
    history._attach_unplayed_copies(jf.adapter, events, {"k@1": "S_HD"})
    assert history._COPIES_FIELD not in events["k@1"]


def test_episode_fetch_exception_is_a_discovery_failure(jf, monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*a, **k):
        raise ConnectionError("down")
    monkeypatch.setattr(history, "get_series_episodes", boom)
    jf.adapter.client = _items_client({**SERIES, "E_HD": EPISODES["S_HD"][0]})
    assert history._item_copies(jf.adapter, "E_HD") is None


@pytest.mark.parametrize("status,strict,expected,body", [
    (500, True, LookupError, {}), (500, False, [], {}), (404, True, [], {}),
    (200, True, LookupError, {}), (200, True, LookupError, {"Id": "other"}), (200, True, LookupError, []),
    (200, False, [], {}),
])
def test_find_strict_separates_errors_from_absence(monkeypatch: pytest.MonkeyPatch, status: int, strict: bool, expected, body) -> None:
    index = _catalogue_index()
    monkeypatch.setattr(_id_lookup, "_catalogue", lambda adapter, feature: {"index": index, "fresh": set(), "validated": {}})
    monkeypatch.setattr(common, "jf_selected_library_ids", lambda cfg, feature: set())
    adapter = SimpleNamespace(cfg=SimpleNamespace(user_id="user"),
                              client=SimpleNamespace(get=lambda path, params=None: SimpleNamespace(status_code=status, json=lambda: body)))
    if expected is LookupError:
        with pytest.raises(LookupError):
            _id_lookup.find(adapter, "history", "Movie", ["tmdb.27205"], strict=strict)
    else:
        assert _id_lookup.find(adapter, "history", "Movie", ["tmdb.27205"], strict=strict) == expected


@pytest.fixture
def add_env(monkeypatch: pytest.MonkeyPatch):
    marked: list[str] = []
    unmarked: list[str] = []
    shadow: dict[str, Any] = {}
    for name in ("_bb_save", "_unres_flush", "_thaw_if_present", "sleep_ms"):
        monkeypatch.setattr(history, name, lambda *a, **k: None)
    monkeypatch.setattr(history, "_shadow_load", lambda: {k: dict(v) for k, v in shadow.items()})
    monkeypatch.setattr(history, "_shadow_save", lambda d: (shadow.clear(), shadow.update(d)))
    monkeypatch.setattr(history, "_unmark_played", lambda http, uid, iid: unmarked.append(iid) or True)
    monkeypatch.setattr(history, "_bb_load", lambda: {})
    monkeypatch.setattr(history, "_freeze", lambda *a, **k: None)
    monkeypatch.setattr(_id_lookup, "prepare", lambda *a, **k: None)
    monkeypatch.setattr(history, "_try_resolve_iid", lambda adapter, m: "E_HD")
    monkeypatch.setattr(history, "_item_copies", lambda adapter, iid: [iid, "E_4K"])
    src_ts = history._parse_iso_to_epoch("2026-10-01T00:00:00Z")
    monkeypatch.setattr(history, "_dst_user_states",
                        lambda http, uid, iids: {"E_HD": (True, src_ts), "E_4K": (False, None)})
    monkeypatch.setattr(history, "_mark_played", lambda http, uid, iid, **k: marked.append(iid) or True)
    monkeypatch.setattr(history, "_normalize_watched", lambda *a, **k: True)
    for name in ("_dbg", "_info", "_warn"):
        monkeypatch.setattr(history, name, lambda *a, **k: None)
    return SimpleNamespace(marked=marked, unmarked=unmarked, shadow=shadow)


@pytest.mark.parametrize("all_copies", [True, False])
def test_add_marks_unplayed_copy_and_counts_it_as_added(add_env, all_copies: bool) -> None:
    adapter = SimpleNamespace(cfg=SimpleNamespace(user_id="user"), client=None, history_all_copies=all_copies)
    ok, unresolved = history.add(adapter, [_episode_event()])
    meta = adapter._history_write_meta
    key = "tmdb:203057#s02e01"
    assert unresolved == []
    if all_copies:
        assert add_env.marked == ["E_4K"] and ok == 1
        assert meta["confirmed_keys"] == [key]
        assert not any(r.get("action") == "skip" for r in meta["results"])
    else:
        assert add_env.marked == [] and ok == 0
        assert [r["action"] for r in meta["results"]] == ["skip"]


KEY = "tmdb:203057#s02e01"


def _copies_adapter() -> SimpleNamespace:
    return SimpleNamespace(cfg=SimpleNamespace(user_id="user"), client=None, history_all_copies=True)


def test_add_counts_shadow_reference_once_per_key(add_env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(history, "_dst_user_states", lambda http, uid, iids: {i: (False, None) for i in iids})
    ok, _ = history.add(_copies_adapter(), [_episode_event()])
    assert add_env.marked == ["E_HD", "E_4K"] and ok == 1
    assert add_env.shadow[KEY]["count"] == 1


def test_partial_add_retry_does_not_add_a_second_reference(add_env, monkeypatch: pytest.MonkeyPatch) -> None:
    src_ts = history._parse_iso_to_epoch("2026-10-01T00:00:00Z")
    played: dict[str, Any] = {}
    monkeypatch.setattr(history, "_dst_user_states", lambda http, uid, iids: {i: played.get(i, (False, None)) for i in iids})

    def mark(http, uid, iid, **k):
        if iid == "E_4K" and not played.get("fail_done"):
            played["fail_done"] = True
            return False
        played[iid] = (True, src_ts)
        add_env.marked.append(iid)
        return True

    monkeypatch.setattr(history, "_mark_played", mark)
    ok, unresolved = history.add(_copies_adapter(), [_episode_event()])
    assert ok == 0 and [u["key"] for u in unresolved] == [KEY]
    assert add_env.shadow[KEY]["count"] == 1
    ok, unresolved = history.add(_copies_adapter(), [_episode_event()])
    assert ok == 1 and unresolved == []
    assert add_env.marked == ["E_HD", "E_4K"]
    assert add_env.shadow[KEY]["count"] == 1


def test_filling_a_copy_next_to_an_older_watch_keeps_one_reference(add_env, monkeypatch: pytest.MonkeyPatch) -> None:
    src_ts = history._parse_iso_to_epoch("2026-10-01T00:00:00Z")
    monkeypatch.setattr(history, "_dst_user_states",
                        lambda http, uid, iids: {"E_HD": (True, src_ts - 86400), "E_4K": (False, None)})
    add_env.shadow[KEY] = {"count": 1, "iid": "E_HD"}
    ok, unresolved = history.add(_copies_adapter(), [_episode_event()])
    assert ok == 1 and unresolved == []
    assert "E_4K" in add_env.marked
    assert add_env.shadow[KEY]["count"] == 1


def test_add_keeps_key_unresolved_when_copy_discovery_fails(add_env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(history, "_item_copies", lambda adapter, iid: None)
    adapter = _copies_adapter()
    ok, unresolved = history.add(adapter, [_episode_event()])
    assert ok == 0 and add_env.marked == []
    assert [u["hint"] for u in unresolved] == ["copies_lookup_failed"]
    assert adapter._history_write_meta["confirmed_keys"] == []


@pytest.mark.parametrize("count,expected", [(1, ["E_HD", "E_4K"]), (2, [])])
def test_remove_unmarks_every_copy_once_per_reference(add_env, count: int, expected: list[str]) -> None:
    add_env.shadow[KEY] = {"count": count, "iid": "E_HD"}
    adapter = _copies_adapter()
    ok, unresolved = history.remove(adapter, [_episode_event()])
    assert unresolved == [] and add_env.unmarked == expected
    assert ok == (1 if expected else 0)
    assert adapter._history_write_meta["confirmed_keys"] == [KEY]
    assert (KEY in add_env.shadow) is (count > 1)
    if count > 1:
        assert add_env.shadow[KEY]["count"] == count - 1


def test_remove_failed_copy_keeps_key_unresolved(add_env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(history, "_unmark_played", lambda http, uid, iid: iid != "E_4K")
    adapter = _copies_adapter()
    ok, unresolved = history.remove(adapter, [_episode_event()])
    assert ok == 0 and [u["key"] for u in unresolved] == [KEY]
    assert adapter._history_write_meta["confirmed_keys"] == []


@dataclass
class FakeOps:
    provider: str
    index: dict[str, dict[str, Any]]
    configs: list[Mapping[str, Any]] = field(default_factory=list)
    add_calls: list[list[dict[str, Any]]] = field(default_factory=list)

    def name(self) -> str:
        return self.provider

    def label(self) -> str:
        return self.provider

    def features(self) -> Mapping[str, bool]:
        return {"history": True}

    def capabilities(self) -> Mapping[str, Any]:
        return {"features": {"history": True}, "observed_deletes": True, "index_semantics": "present"}

    def is_configured(self, cfg: Mapping[str, Any]) -> bool:
        return True

    def health(self, cfg: Mapping[str, Any], **_: Any) -> dict[str, Any]:
        return {"ok": True, "status": "ok", "features": {"history": True}, "api": {}}

    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, dict[str, Any]]:
        self.configs.append(cfg)
        return {k: dict(v) for k, v in self.index.items()}

    def add(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        batch = [dict(x) for x in items]
        self.add_calls.append(batch)
        return {"ok": True, "count": len(batch)}

    def remove(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        return {"ok": True, "count": 0}


def _orchestrate(monkeypatch: pytest.MonkeyPatch, update_all_copies: bool, dst_row: dict[str, Any],
                 *, two_way: bool = False) -> tuple[Orchestrator, FakeOps, FakeOps]:
    key = "tvdb:393159#s02e01"
    src = FakeOps("SRC", {key: _episode_event()})
    dst = FakeOps("JELLYFIN", {key: dst_row})
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"SRC": src, "JELLYFIN": dst})
    monkeypatch.setattr("cw_platform.orchestrator._snapshots.provider_configured", lambda _cfg, _name: True)
    monkeypatch.setattr("cw_platform.orchestrator._pairs.record_health", lambda *args: None)
    source, target = ("JELLYFIN", "SRC") if two_way else ("SRC", "JELLYFIN")
    cfg = {
        "runtime": {"debug": False, "snapshot_ttl_sec": 0, "apply_chunk_size": 0, "apply_chunk_pause_ms": 0},
        "sync": {"dry_run": False, "enable_add": True, "enable_remove": False, "include_observed_deletes": False},
        "pairs": [{
            "id": "p1", "enabled": True, "source": source, "target": target,
            "mode": "two-way" if two_way else "one-way", "feature": "history",
            "features": {"history": {"enable": True, "add": True, "remove": False, "update_all_copies": update_all_copies}},
        }],
    }
    return Orchestrator(cfg), src, dst


@pytest.mark.parametrize("update_all_copies", [True, False])
def test_oneway_plans_copy_gaps_only_when_enabled(config_base: Any, monkeypatch: pytest.MonkeyPatch, update_all_copies: bool) -> None:
    row = {**_episode_event(), copies.COPIES_FIELD: ["E_4K"]}
    orch, src, dst = _orchestrate(monkeypatch, update_all_copies, row)
    orch.run()
    planned = [it for batch in dst.add_calls for it in batch]
    assert len(planned) == (1 if update_all_copies else 0)
    assert bool(dst.configs[-1].get(copies.COPIES_FLAG)) is update_all_copies
    assert not src.configs[-1].get(copies.COPIES_FLAG)


@pytest.mark.parametrize("update_all_copies", [True, False])
def test_twoway_plans_copy_gaps_for_the_jellyfin_side(config_base: Any, monkeypatch: pytest.MonkeyPatch, update_all_copies: bool) -> None:
    row = {**_episode_event(), copies.COPIES_FIELD: ["E_4K"]}
    orch, src, dst = _orchestrate(monkeypatch, update_all_copies, row, two_way=True)
    orch.run()
    assert len([it for batch in dst.add_calls for it in batch]) == (1 if update_all_copies else 0)
    assert [it for batch in src.add_calls for it in batch] == []
    assert bool(dst.configs[-1].get(copies.COPIES_FLAG)) is update_all_copies
    assert not src.configs[-1].get(copies.COPIES_FLAG)


@pytest.mark.parametrize("two_way", [False, True])
def test_copy_gaps_do_not_survive_into_the_next_run(config_base: Any, monkeypatch: pytest.MonkeyPatch, two_way: bool) -> None:
    row = {**_episode_event(), copies.COPIES_FIELD: ["E_4K"]}
    orch, _, dst = _orchestrate(monkeypatch, True, row, two_way=two_way)
    orch.run()
    assert len(dst.add_calls) == 1
    dst.index = {k: {f: v for f, v in r.items() if f != copies.COPIES_FIELD} for k, r in dst.index.items()}
    orch.run()
    assert len(dst.add_calls) == 1
