# tests/test_trakt_rewatch_resets.py
# CrossWatch - Trakt rewatch resets: hide watches before the reset and mark them unwatched on media servers
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, Iterable, Mapping

import pytest

from cw_platform.event_archive import recorder
from cw_platform.history_events import history_sync_key
from cw_platform.id_map import canonical_key
from cw_platform.orchestrator._interactive import InteractivePlan
from cw_platform.orchestrator._history_resets import ResetPlan, apply_reset_unwatches, load_reset_plan
from cw_platform.orchestrator.facade import Orchestrator
from providers.sync.trakt import _resets
from tests.test_history_baseline_native import HistoryOps

RESET = "2025-06-01T00:00:00.000Z"
BEFORE = "2025-01-01T00:00:00Z"
AFTER = "2025-07-01T00:00:00Z"


@dataclass
class TraktOps(HistoryOps):
    resets: list[dict[str, Any]] = field(default_factory=list)
    reset_reads: int = 0
    fail_reads: bool = False

    dropped: set[str] = field(default_factory=set)

    def dropped_show_tokens(self, cfg: Mapping[str, Any]) -> set[str]:
        return set(self.dropped)

    def rewatch_resets(self, cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
        self.reset_reads += 1
        if self.fail_reads:
            raise RuntimeError("trakt_status_http_failed")
        return [dict(x) for x in self.resets]


@dataclass
class ServerOps(HistoryOps):
    def remove(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        batch = [dict(x) for x in items]
        self.remove_calls.append(batch)
        keys = {history_sync_key(it) for it in batch}
        for key in [k for k, v in self.index.items() if history_sync_key(v, k) in keys]:
            self.index.pop(key)
        return {"ok": True, "count": len(batch), "confirmed_keys": sorted(keys), "unresolved": []}


def _ep(show: str, season: int, episode: int, watched_at: str, **extra: Any) -> dict[str, Any]:
    return {"type": "episode", "season": season, "episode": episode, "show_ids": {"tmdb": show},
            "series_title": f"Show {show}", "watched": True, "watched_at": watched_at, **extra}


def _events(*items: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return {history_sync_key(it, event_mode=True): dict(it) for it in items}


def _reset(show: str, when: str = RESET) -> dict[str, Any]:
    return {"title": f"Show {show}", "ids": {"tmdb": show, "trakt": f"9{show}"}, "reset_at": when}


def _coords(batches: list[list[dict[str, Any]]]) -> set[tuple[str, Any, Any]]:
    return {(str((it.get("show_ids") or {}).get("tmdb")), it.get("season"), it.get("episode")) for batch in batches for it in batch}


def _run(monkeypatch, trakt, server, *, mode: str = "one-way", option: bool = True, remove: bool = False, dry_run: bool = False,
         trakt_first: bool = True, remove_mode: str | None = None, plan: InteractivePlan | None = None,
         providers_cfg: Mapping[str, Any] | None = None) -> dict[str, Any]:
    server_name = str(server.provider)
    providers = {"TRAKT": trakt, server_name: server}
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: providers)
    monkeypatch.setattr("cw_platform.orchestrator._snapshots.provider_configured", lambda _cfg, _name: True)
    monkeypatch.setattr("cw_platform.orchestrator._pairs_blocklist.load_blackbox_keys", lambda *_a, **_k: set())
    monkeypatch.setattr("cw_platform.orchestrator._pairs_blocklist.load_unresolved_keys", lambda *_a, **_k: set())
    monkeypatch.setattr("cw_platform.orchestrator._pairs.record_health", lambda *_a, **_k: None)
    history: dict[str, Any] = {"enable": True, "add": True, "remove": remove}
    if option:
        history["trakt_rewatch_resets"] = True
    if remove_mode:
        history["remove_mode"] = remove_mode
    source, target = ("TRAKT", server_name) if trakt_first else (server_name, "TRAKT")
    pair: dict[str, Any] = {"id": "p1", "enabled": True, "source": source, "target": target, "mode": mode, "feature": "history",
                            "features": {"history": history}}
    if providers_cfg:
        pair["providers"] = dict(providers_cfg)
    config: dict[str, Any] = {
        "runtime": {"debug": False, "snapshot_ttl_sec": 0, "apply_chunk_size": 0, "apply_chunk_pause_ms": 0},
        "sync": {"dry_run": dry_run, "enable_add": True, "enable_remove": remove, "include_observed_deletes": remove,
                 "allow_mass_delete": True},
        "pairs": [pair],
    }
    Orchestrator(config, interactive=plan).run(dry_run=bool(plan and plan.preview), pair_scope_ids=["p1"],
                                               write_state_json=not bool(plan and plan.preview))
    return config


@pytest.fixture(autouse=True)
def _isolated(config_base):
    return config_base


def test_plan_hides_watches_before_the_reset_and_picks_server_unwatches():
    plan = ResetPlan([_reset("1")], trakt_side=0, trakt="TRAKT", server="PLEX")
    trakt = _events(_ep("1", 1, 1, BEFORE), _ep("1", 1, 2, BEFORE), _ep("1", 1, 2, AFTER), _ep("2", 1, 1, BEFORE),
                    {"type": "movie", "ids": {"tmdb": "1"}, "watched_at": BEFORE})
    server = _events(_ep("1", 1, 1, BEFORE), _ep("1", 1, 2, BEFORE), _ep("1", 1, 3, AFTER), _ep("2", 1, 1, BEFORE))

    (t_prev, t_cur, t_eff), (s_prev, s_cur, s_eff) = plan.view((dict(trakt), dict(trakt), dict(trakt)),
                                                               (dict(server), dict(server), dict(server)), emit=lambda *a, **k: None)

    assert sorted((v.get("season"), v.get("episode"), v["watched_at"]) for v in t_cur.values() if v["type"] == "episode"
                  and v["show_ids"]["tmdb"] == "1") == [(1, 2, AFTER)]
    assert any(v["type"] == "movie" for v in t_cur.values())
    assert _coords([list(s_eff.values())]) == {("1", 1, 2), ("1", 1, 3), ("2", 1, 1)}
    assert _coords([plan.unwatch]) == {("1", 1, 1)}
    assert t_prev == t_cur == t_eff and s_prev == s_cur == s_eff


def test_plan_matches_shows_by_any_shared_id():
    plan = ResetPlan([{"ids": {"trakt": "5", "tvdb": "77"}, "reset_at": RESET}], trakt_side=0, trakt="TRAKT", server="PLEX")
    item = {**_ep("x", 1, 1, BEFORE), "show_ids": {"tvdb": "77", "plex": "123"}}

    plan.view(({}, {}, {}), ({}, {}, _events(item)), emit=lambda *a, **k: None)

    assert len(plan.unwatch) == 1


def test_option_off_reads_nothing_and_syncs_as_before(monkeypatch):
    trakt = TraktOps(provider="TRAKT", index=_events(_ep("1", 1, 1, BEFORE)), resets=[_reset("1")])
    server = ServerOps(provider="PLEX", index={})

    _run(monkeypatch, trakt, server, option=False)

    assert trakt.reset_reads == 0
    assert _coords(server.add_calls) == {("1", 1, 1)}
    assert server.remove_calls == []


SERVERS = ["PLEX", "JELLYFIN", "EMBY", "KODI", "TOFA"]


@pytest.mark.parametrize("server_name", SERVERS)
def test_one_way_unwatches_server_episodes_without_touching_trakt(monkeypatch, server_name):
    trakt = TraktOps(provider="TRAKT", index=_events(_ep("1", 1, 1, BEFORE), _ep("1", 1, 2, BEFORE), _ep("2", 1, 1, BEFORE)),
                     resets=[_reset("1")])
    server = ServerOps(provider=server_name, index=_events(_ep("1", 1, 1, BEFORE), _ep("1", 1, 2, BEFORE), _ep("2", 1, 1, BEFORE)))

    _run(monkeypatch, trakt, server)

    assert _coords(server.remove_calls) == {("1", 1, 1), ("1", 1, 2)}
    assert server.add_calls == [] or _coords(server.add_calls) == set()
    assert trakt.add_calls == [] and trakt.remove_calls == []

    server.remove_calls.clear()
    _run(monkeypatch, trakt, server)

    assert server.remove_calls == []
    assert _coords(server.add_calls) == set()


def test_one_way_keeps_server_watches_after_the_reset_and_sends_new_trakt_plays(monkeypatch):
    trakt = TraktOps(provider="TRAKT", index=_events(_ep("1", 1, 1, BEFORE), _ep("1", 1, 2, BEFORE), _ep("1", 1, 2, AFTER)),
                     resets=[_reset("1")])
    server = ServerOps(provider="PLEX", index=_events(_ep("1", 1, 1, AFTER)))

    _run(monkeypatch, trakt, server)

    assert server.remove_calls == []
    assert _coords(server.add_calls) == {("1", 1, 2)}


def test_one_way_dry_run_writes_nothing(monkeypatch):
    trakt = TraktOps(provider="TRAKT", index=_events(_ep("1", 1, 1, BEFORE)), resets=[_reset("1")])
    server = ServerOps(provider="PLEX", index=_events(_ep("1", 1, 1, BEFORE)))

    _run(monkeypatch, trakt, server, dry_run=True)

    assert server.remove_calls == []


def test_one_way_from_server_to_trakt_is_not_affected(monkeypatch):
    trakt = TraktOps(provider="TRAKT", index=_events(_ep("1", 1, 1, BEFORE)), resets=[_reset("1")])
    server = ServerOps(provider="PLEX", index=_events(_ep("1", 1, 1, BEFORE)))

    _run(monkeypatch, trakt, server, trakt_first=False)

    assert trakt.reset_reads == 0
    assert server.remove_calls == []


def test_failed_reset_read_blocks_history_writes(monkeypatch):
    trakt = TraktOps(provider="TRAKT", index=_events(_ep("1", 1, 1, BEFORE)), resets=[_reset("1")], fail_reads=True)
    server = ServerOps(provider="PLEX", index={})

    _run(monkeypatch, trakt, server)

    assert server.add_calls == [] and server.remove_calls == []


@pytest.mark.parametrize("server_name", SERVERS)
@pytest.mark.parametrize("trakt_first", [True, False])
def test_two_way_never_deletes_trakt_history_and_stays_stable(monkeypatch, trakt_first, server_name):
    trakt = TraktOps(provider="TRAKT", index=_events(_ep("1", 1, 1, BEFORE), _ep("1", 1, 2, BEFORE)), resets=[_reset("1")])
    server = ServerOps(provider=server_name, index=_events(_ep("1", 1, 1, BEFORE), _ep("1", 1, 2, BEFORE), _ep("1", 1, 3, AFTER)))

    for _ in range(3):
        _run(monkeypatch, trakt, server, mode="two-way", remove=True, trakt_first=trakt_first)

    assert _coords(server.remove_calls) == {("1", 1, 1), ("1", 1, 2)}
    assert trakt.remove_calls == []
    assert _coords(trakt.add_calls) == {("1", 1, 3)}
    assert _coords(server.add_calls) == set()


@pytest.mark.parametrize("remove_mode", ["source_deletes", "mirror"])
def test_one_way_with_removals_unwatches_once_and_then_stays_quiet(monkeypatch, remove_mode):
    trakt = TraktOps(provider="TRAKT", index=_events(_ep("1", 1, 1, BEFORE), _ep("1", 1, 2, BEFORE)), resets=[])
    server = ServerOps(provider="PLEX", index=_events(_ep("1", 1, 1, BEFORE), _ep("1", 1, 2, BEFORE)))
    _run(monkeypatch, trakt, server, remove=True, remove_mode=remove_mode)
    assert server.remove_calls == [] and _coords(server.add_calls) == set()

    trakt.resets = [_reset("1")]
    _run(monkeypatch, trakt, server, remove=True, remove_mode=remove_mode)
    removed = [_coords([batch]) for batch in server.remove_calls]
    assert removed == [{("1", 1, 1), ("1", 1, 2)}]

    server.remove_calls.clear()
    for _ in range(2):
        _run(monkeypatch, trakt, server, remove=True, remove_mode=remove_mode)
    assert server.remove_calls == []
    assert _coords(server.add_calls) == set()
    assert trakt.add_calls == [] and trakt.remove_calls == []


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_undoing_the_reset_brings_the_watches_back_without_trakt_deletes(monkeypatch, mode):
    trakt = TraktOps(provider="TRAKT", index=_events(_ep("1", 1, 1, BEFORE)), resets=[_reset("1")])
    server = ServerOps(provider="PLEX", index=_events(_ep("1", 1, 1, BEFORE)))
    _run(monkeypatch, trakt, server, mode=mode, remove=True)
    assert _coords(server.remove_calls) == {("1", 1, 1)}

    trakt.resets = []
    _run(monkeypatch, trakt, server, mode=mode, remove=True)

    assert _coords(server.add_calls) == {("1", 1, 1)}
    assert trakt.remove_calls == []
    assert any(v.get("episode") == 1 for v in server.index.values())

    server.add_calls.clear()
    server.remove_calls.clear()
    _run(monkeypatch, trakt, server, mode=mode, remove=True)
    assert server.add_calls == [] and server.remove_calls == [] and trakt.remove_calls == []


def test_switching_the_option_off_syncs_the_earlier_watches_again(monkeypatch):
    trakt = TraktOps(provider="TRAKT", index=_events(_ep("1", 1, 1, BEFORE)), resets=[_reset("1")])
    server = ServerOps(provider="PLEX", index=_events(_ep("1", 1, 1, BEFORE)))
    _run(monkeypatch, trakt, server)
    assert _coords(server.remove_calls) == {("1", 1, 1)}

    _run(monkeypatch, trakt, server, option=False)

    assert _coords(server.add_calls) == {("1", 1, 1)}


def _reset_rows(plan: InteractivePlan) -> list[dict[str, Any]]:
    return [row for row in plan.rows.values() if row["operation"] == "rewatch_reset" and row["provider"] == "PLEX"]


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_interactive_preview_lists_unwatches_and_apply_respects_the_selection(monkeypatch, mode):
    trakt = TraktOps(provider="TRAKT", index=_events(_ep("1", 1, 1, BEFORE), _ep("1", 1, 2, BEFORE)), resets=[_reset("1")])
    server = ServerOps(provider="PLEX", index=_events(_ep("1", 1, 1, BEFORE), _ep("1", 1, 2, BEFORE)))

    preview = InteractivePlan()
    _run(monkeypatch, trakt, server, mode=mode, plan=preview)
    rows = _reset_rows(preview)
    assert sorted((row["source"], row["item"]["episode"]) for row in rows) == [("TRAKT", 1), ("TRAKT", 2)]
    assert server.remove_calls == []

    keep = {row["id"] for row in rows if row["item"]["episode"] == 1}
    execution = InteractivePlan(preview=False, selected=keep)
    _run(monkeypatch, trakt, server, mode=mode, plan=execution)

    assert _coords(server.remove_calls) == {("1", 1, 1)}
    assert trakt.remove_calls == []
    assert [row for row in execution.deferred_removes if row["provider"] == "PLEX"] == []


def _no_emit(*_a, **_k):
    return None


def test_episode_ids_never_match_a_reset_show():
    plan = ResetPlan([_reset("1")], trakt_side=0, trakt="TRAKT", server="PLEX")
    stray = {"type": "episode", "season": 1, "episode": 1, "ids": {"tmdb": "1"}, "watched_at": BEFORE}

    (_, _, t_eff), (_, _, s_eff) = plan.view(({}, {}, _events(stray)), ({}, {}, _events(stray)), emit=_no_emit)

    assert plan.unwatch == []
    assert len(t_eff) == 1 and len(s_eff) == 1


def test_rewatch_under_other_numbering_keeps_the_server_episode():
    plan = ResetPlan([_reset("1")], trakt_side=0, trakt="TRAKT", server="PLEX")
    trakt_row = _ep("1", 2, 1, AFTER, _cw_anime_aliases=["tmdb:1#s01e13"])
    server_row = _ep("1", 1, 13, BEFORE)

    (_, _, _), (_, _, s_eff) = plan.view(({}, {}, _events(trakt_row)), ({}, {}, _events(server_row)), emit=_no_emit)

    assert plan.unwatch == []
    assert len(s_eff) == 1


def test_plex_cloud_history_rows_are_never_unwatched():
    plan = ResetPlan([_reset("1")], trakt_side=0, trakt="TRAKT", server="PLEX")
    cloud = _ep("1", 1, 1, BEFORE, _plex_cloud_history=True)

    plan.view(({}, {}, {}), ({}, {}, _events(cloud, _ep("1", 1, 2, BEFORE))), emit=_no_emit)

    assert _coords([plan.unwatch]) == {("1", 1, 2)}
    assert all(item["_cw_force_clear"] is True for item in plan.unwatch)


def test_plex_without_marked_watched_reads_is_reported_and_skipped():
    trakt = TraktOps(provider="TRAKT", index={}, resets=[_reset("1")])
    events: list[tuple[str, dict[str, Any]]] = []
    plex_cfg = {"plex": {"history": {"include_marked_watched": False}}}

    plan = load_reset_plan("history", {"trakt_rewatch_resets": True}, [("TRAKT", trakt, {}), ("PLEX", None, plex_cfg)],
                           event_mode=False, one_way=True, emit=lambda name, **kw: events.append((name, kw)))

    assert plan is None and trakt.reset_reads == 0
    assert [name for name, _ in events] == ["history:rewatch_reset_unavailable"]


def _policy(adds: dict[str, Any] | None = None, blocks: set[str] | None = None):
    def manual_policy(_state, provider, _feature, _instance="default"):
        return (dict(adds or {}), set(blocks or set())) if provider == "TRAKT" else ({}, set())
    return manual_policy


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_manual_blocks_stop_the_unwatch(monkeypatch, mode):
    blocked = _ep("1", 1, 1, BEFORE)
    for module in ("_pairs_oneway", "_pairs_twoway"):
        monkeypatch.setattr(f"cw_platform.orchestrator.{module}._manual_policy",
                            _policy(blocks={canonical_key(blocked).lower()}))
    trakt = TraktOps(provider="TRAKT", index=_events(_ep("1", 1, 1, BEFORE), _ep("1", 1, 2, BEFORE)), resets=[_reset("1")])
    server = ServerOps(provider="PLEX", index=_events(_ep("1", 1, 1, BEFORE), _ep("1", 1, 2, BEFORE)))

    _run(monkeypatch, trakt, server, mode=mode)

    assert _coords(server.remove_calls) == {("1", 1, 2)}


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_manual_history_adds_before_the_reset_do_not_loop(monkeypatch, mode):
    manual = _ep("1", 1, 1, BEFORE)
    for module in ("_pairs_oneway", "_pairs_twoway"):
        monkeypatch.setattr(f"cw_platform.orchestrator.{module}._manual_policy",
                            _policy(adds={history_sync_key(manual): manual}))
    trakt = TraktOps(provider="TRAKT", index=_events(_ep("1", 1, 1, BEFORE)), resets=[_reset("1")])
    server = ServerOps(provider="PLEX", index=_events(_ep("1", 1, 1, BEFORE)))

    for _ in range(3):
        _run(monkeypatch, trakt, server, mode=mode)

    assert [_coords([batch]) for batch in server.remove_calls] == [{("1", 1, 1)}]
    assert _coords(server.add_calls) == set()


def test_a_planned_add_for_the_same_episode_wins_over_the_unwatch():
    plan = ResetPlan([_reset("1")], trakt_side=0, trakt="TRAKT", server="PLEX")
    plan.view(({}, {}, {}), ({}, {}, _events(_ep("1", 1, 1, BEFORE), _ep("1", 1, 2, BEFORE))), emit=_no_emit)

    def alias(index):
        return {canonical_key(v): k for k, v in index.items()}

    def find(index, aliases, item):
        key = aliases.get(canonical_key(item))
        return index.get(key) if key else None

    kept = plan.settle([_ep("1", 1, 1, AFTER)], alias=alias, find=find, unblocked=lambda items: items)

    assert _coords([kept]) == {("1", 1, 2)}

def test_apply_reports_results_and_events():
    plan = ResetPlan([_reset("1")], trakt_side=0, trakt="TRAKT", server="PLEX")
    plan.view(({}, {}, {}), ({}, {}, _events(_ep("1", 1, 1, BEFORE))), emit=lambda *a, **k: None)
    server = ServerOps(provider="PLEX", index=_events(_ep("1", 1, 1, BEFORE)))
    events: list[tuple[str, dict[str, Any]]] = []

    confirmed = apply_reset_unwatches(plan, dst_ops=server, dst_cfg={}, feature="history", dry_run=False,
                                      emit=lambda name, **kw: events.append((name, kw)))

    assert confirmed == 1
    reset_events = [kw for name, kw in events if name == "history:rewatch_reset"]
    assert reset_events and reset_events[0]["confirmed"] == 1 and reset_events[0]["items"] == ["Show 1 S01E01"]


def test_load_plan_requires_the_option_and_a_media_server():
    trakt = TraktOps(provider="TRAKT", index={}, resets=[_reset("1")])
    other = HistoryOps(provider="SIMKL", index={})

    assert load_reset_plan("history", {}, [("TRAKT", trakt, {}), ("PLEX", other, {})], event_mode=False, one_way=True) is None
    assert load_reset_plan("history", {"trakt_rewatch_resets": True}, [("TRAKT", trakt, {}), ("SIMKL", other, {})],
                           event_mode=False, one_way=True) is None
    assert load_reset_plan("history", {"trakt_rewatch_resets": True}, [("TRAKT", trakt, {}), ("PLEX", other, {})],
                           event_mode=False, one_way=True) is not None
    assert load_reset_plan("history", {"trakt_rewatch_resets": True}, [("TRAKT", trakt, {}), ("SILO", other, {})],
                           event_mode=False, one_way=True) is None
    assert trakt.reset_reads == 1


class _Response:
    def __init__(self, rows: list[Any], page: int, pages: int, items: int) -> None:
        self.status_code = 200
        self._rows = rows
        self.headers = {"X-Pagination-Page": str(page), "X-Pagination-Page-Count": str(pages), "X-Pagination-Item-Count": str(items)}

    def json(self) -> Any:
        return self._rows

    def raise_for_status(self) -> None:
        return None


def test_trakt_reader_parses_reset_rows(monkeypatch):
    monkeypatch.setattr(_resets, "account_is_vip", lambda _adapter: True)
    rows = [
        {"show": {"title": "A", "year": 2020, "ids": {"trakt": 1, "slug": "a", "tmdb": 10, "tvdb": None,
                                                     "plex": {"guid": "5d9c", "slug": "a"}}}, "progress": {"reset_at": RESET}},
        {"show": {"title": "B", "ids": {"trakt": 2}}, "progress": {"reset_at": None}},
    ]
    calls: list[dict[str, Any]] = []

    def get(url: str, params: Mapping[str, Any]) -> _Response:
        calls.append({"url": url, **params})
        page = int(params["page"])
        return _Response(rows if page == 1 else [], page, 1, len(rows))

    out = _resets.load_rewatch_resets(SimpleNamespace(client=SimpleNamespace(get=get)))

    assert out == [{"title": "A", "year": 2020, "ids": {"trakt": "1", "tmdb": "10"}, "reset_at": RESET}]
    assert calls[0]["url"].endswith("/sync/progress/watched") and calls[0]["only_rewatching"] == "true"


def test_trakt_reader_rejects_malformed_rows(monkeypatch):
    monkeypatch.setattr(_resets, "account_is_vip", lambda _adapter: True)

    def get(url: str, params: Mapping[str, Any]) -> _Response:
        page = int(params["page"])
        return _Response([{"show": "x"}] if page == 1 else [], page, 1, 1)

    with pytest.raises(RuntimeError):
        _resets.load_rewatch_resets(SimpleNamespace(client=SimpleNamespace(get=get)))


def _settings_adapter() -> SimpleNamespace:
    calls: list[str] = []

    def get(url: str, params: Mapping[str, Any]) -> _Response:
        calls.append(url)
        return _Response([], int(params["page"]), 0, 0)

    return SimpleNamespace(client=SimpleNamespace(get=get, session=object()), calls=calls,
                           cfg=SimpleNamespace(client_id="c", access_token="t", timeout=5, max_retries=1))


@pytest.mark.parametrize("user", [{"vip": False}, {"vip": False, "vip_og": False, "vip_ep": False}])
def test_trakt_reader_skips_accounts_without_vip(monkeypatch, user):
    monkeypatch.setattr(_resets, "fetch_user_settings", lambda *_a, **_k: {"user": user})
    adapter = _settings_adapter()

    assert _resets.load_rewatch_resets(adapter) == []
    assert adapter.calls == []


@pytest.mark.parametrize("flag", ["vip", "vip_og", "vip_ep"])
def test_trakt_reader_reads_resets_for_any_vip_flag(monkeypatch, flag):
    monkeypatch.setattr(_resets, "fetch_user_settings", lambda *_a, **_k: {"user": {"vip": False, flag: True}})
    adapter = _settings_adapter()

    assert _resets.load_rewatch_resets(adapter) == []
    assert adapter.calls and adapter.calls[0].endswith("/sync/progress/watched")


@pytest.mark.parametrize("settings", [None, {}, {"user": None}, {"user": {}}, {"user": {"vip": None}}, {"user": {"vip": "yes"}}])
def test_trakt_reader_fails_when_vip_status_cannot_be_read(monkeypatch, settings):
    monkeypatch.setattr(_resets, "fetch_user_settings", lambda *_a, **_k: settings)
    adapter = _settings_adapter()

    with pytest.raises(RuntimeError, match="settings_unavailable"):
        _resets.load_rewatch_resets(adapter)
    assert adapter.calls == []

def _recorded(monkeypatch, *events: tuple[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    monkeypatch.setattr(recorder, "record_events", lambda batch, **_k: rows.extend(batch) or len(batch))
    rec = recorder.RunRecorder(lambda *_a, **_k: None, run_id="r1")
    for name, fields in events:
        rec.emit(name, **fields)
    rec._flush(force=True)
    return rows


def test_events_record_rewatch_reset_summary_and_failures(monkeypatch):
    rows = _recorded(
        monkeypatch,
        ("feature:start", {"src": "TRAKT", "dst": "PLEX", "feature": "history"}),
        ("history:rewatch_reset", {"dst": "PLEX", "feature": "history", "attempted": 2, "confirmed": 1, "not_found": 0,
                                   "errors": 1, "error": "boom", "items": ["Show 1 S01E01", "Show 1 S01E02"]}),
    )

    [row] = [r for r in rows if r.get("operation") == "rewatch_reset" and r["event_type"] == "write_attempted"]
    assert row["source_provider"] == "TRAKT" and row["destination_provider"] == "PLEX"
    detail = row["detail"] if isinstance(row["detail"], dict) else json.loads(row["detail"])
    assert detail["unwatched"] == 1 and detail["failed"] == 1 and detail["shows"] == ["Show 1 S01E01", "Show 1 S01E02"]
    assert any(r["event_type"] == "write_failed" and r.get("reason_code") == "rewatch_reset_failed" for r in rows)


@pytest.mark.parametrize("force", [True, False])
def test_emby_force_clear_unmarks_even_when_other_sources_hold_it(monkeypatch, force):
    from providers.sync.emby import _history as emby

    saved: list[dict[str, int]] = []
    unmarked: list[str] = []
    monkeypatch.setattr(emby, "_prepare_mids", lambda _adapter, items: ({"k": dict(items[0])}, [("k", "iid-1")], []))
    monkeypatch.setattr(emby, "_shadow_load", lambda: {"k": 2})
    monkeypatch.setattr(emby, "_shadow_save", lambda data: saved.append(dict(data)))
    monkeypatch.setattr(emby, "_unmark_played", lambda _http, _uid, iid: unmarked.append(iid) or True)
    monkeypatch.setattr(emby, "_history_limit", lambda _adapter: 25)
    monkeypatch.setattr(emby, "_history_delay_ms", lambda _adapter: 0)
    monkeypatch.setattr(emby, "_thaw_if_present", lambda _keys: None)
    monkeypatch.setattr(emby, "_unres_flush", lambda: None)
    monkeypatch.setattr(emby, "_set_write_meta", lambda *_a, **_k: None)
    adapter = SimpleNamespace(client=object(), cfg=SimpleNamespace(user_id="u"))
    item = {**_ep("1", 1, 1, BEFORE), **({"_cw_force_clear": True} if force else {})}

    ok, unresolved = emby.remove(adapter, [item])

    assert unresolved == []
    assert (ok, unmarked, saved[-1]) == ((1, ["iid-1"], {}) if force else (0, [], {"k": 1}))


@pytest.mark.parametrize("trakt_first", [True, False])
def test_two_way_server_manual_adds_before_the_reset_are_not_sent_to_trakt(monkeypatch, trakt_first):
    manual = _ep("1", 1, 1, BEFORE)

    def manual_policy(_state, provider, _feature, _instance="default"):
        return ({history_sync_key(manual): manual}, set()) if provider == "PLEX" else ({}, set())

    monkeypatch.setattr("cw_platform.orchestrator._pairs_twoway._manual_policy", manual_policy)
    trakt = TraktOps(provider="TRAKT", index=_events(_ep("1", 1, 1, BEFORE)), resets=[_reset("1")])
    server = ServerOps(provider="PLEX", index=_events(_ep("1", 1, 1, BEFORE)))

    for _ in range(3):
        _run(monkeypatch, trakt, server, mode="two-way", trakt_first=trakt_first)

    assert _coords(trakt.add_calls) == set()
    assert trakt.remove_calls == []
    assert [_coords([batch]) for batch in server.remove_calls] == [{("1", 1, 1)}]


def _unavailable_event() -> dict[str, Any]:
    from cw_platform.orchestrator._history_resets import RESET_UNAVAILABLE

    events: list[tuple[str, dict[str, Any]]] = []
    load_reset_plan("history", {"trakt_rewatch_resets": True},
                    [("TRAKT", TraktOps(provider="TRAKT", index={}), {}),
                     ("PLEX", None, {"plex": {"history": {"include_marked_watched": False}}})],
                    event_mode=False, one_way=True, emit=lambda name, **kw: events.append((name, kw)))
    [(name, fields)] = events
    assert name == RESET_UNAVAILABLE
    return {"event": name, **fields}


def test_unavailable_rewatch_resets_show_up_in_interactive_notices(config_base, monkeypatch):
    from services import interactive_sync as svc

    event = _unavailable_event()

    class PlanWithNotice:
        def __init__(self, cfg, *, on_progress, interactive):
            self.progress = on_progress

        def run(self, **kwargs):
            self.progress(json.dumps(event))
            return dict(ok=True)

    monkeypatch.setattr(svc, "Orchestrator", PlanWithNotice)
    monkeypatch.setattr(svc, "mapping_version", lambda cfg: "test")
    session = svc.Session(pair_id="p1", owner="local", pair=dict(source="TRAKT", target="PLEX"))
    svc.refresh(session, {}, {})
    public = session.public()

    notice = next(n for n in public["notices"] if n["event"] == event["event"])
    assert "marked watched" in notice["reason"]
    assert public["status"] != "empty"


def test_unavailable_rewatch_resets_show_up_in_progress_and_the_report(config_base):
    from cw_platform.orchestrator._interactive import InteractivePlan
    from services.interactive_sync import Session
    from services.interactive_sync_progress import SyncProgress
    from services.interactive_sync_report import SyncReport

    event = _unavailable_event()
    progress = SyncProgress()
    progress.event(dict(event))
    assert any("Trakt rewatch resets skipped" in row["text"] for row in progress.public()["recent"])

    collector = SyncReport()
    collector.event(dict(event))
    report = collector.finish(Session(pair_id="p1", owner="local"), InteractivePlan(preview=False), {"ok": True})
    assert any(n["event"] == event["event"] and "marked watched" in n["reason"] for n in report["notices"])
