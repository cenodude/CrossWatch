# tests/test_dropped_show_modes.py
# CrossWatch - Dropped show modes: sync, ignore and tag dropped on the target
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, Iterable, Mapping, cast

import pytest

from api.syncAPI import _normalize_pair_providers
from cw_platform import run_control
from cw_platform.event_archive import recorder
from cw_platform.history_events import history_sync_key
from cw_platform.orchestrator._interactive import InteractivePlan
from cw_platform.orchestrator._dropped import apply_dropped_tags, dropped_tag_candidates, provider_dropped_mode
from cw_platform.orchestrator.facade import Orchestrator
from providers.sync._mod_MDBLIST import MDBLISTModule
from providers.sync.simkl import _common as simkl_common
from providers.sync.simkl import _history as simkl_history
from tests.test_history_baseline_native import HistoryOps


@dataclass
class DroppedOps(HistoryOps):
    dropped: set[str] = field(default_factory=set)
    dropped_reads: int = 0
    tag_calls: list[list[dict[str, Any]]] = field(default_factory=list)

    def dropped_show_tokens(self, cfg: Mapping[str, Any]) -> set[str]:
        self.dropped_reads += 1
        return set(self.dropped)

    def tag_dropped_shows(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
        batch = [dict(x) for x in items]
        self.tag_calls.append(batch)
        return {"attempted": len(batch), "confirmed": len(batch)}


def _episode(show: str, season: int, episode: int) -> dict[str, Any]:
    return {"type": "episode", "season": season, "episode": episode, "show_ids": {"tmdb": show},
            "series_title": f"Show {show}", "watched_at": "2024-01-01T00:00:00Z"}


def _index(*items: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return {history_sync_key(it): dict(it) for it in items}


def _run(monkeypatch, src, dst, *, providers: Mapping[str, Any] | None = None, mode: str = "one-way", dry_run: bool = False,
         plan: InteractivePlan | None = None):
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"SRC": src, "DST": dst})
    monkeypatch.setattr("cw_platform.orchestrator._snapshots.provider_configured", lambda _cfg, _name: True)
    monkeypatch.setattr("cw_platform.orchestrator._pairs_blocklist.load_blackbox_keys", lambda *_a, **_k: set())
    monkeypatch.setattr("cw_platform.orchestrator._pairs_blocklist.load_unresolved_keys", lambda *_a, **_k: set())
    monkeypatch.setattr("cw_platform.orchestrator._pairs.record_health", lambda *_a, **_k: None)
    pair: dict[str, Any] = {
        "id": "p1", "enabled": True, "source": "SRC", "target": "DST", "mode": mode, "feature": "history",
        "features": {"history": {"enable": True, "add": True, "remove": False}},
    }
    if providers:
        pair["providers"] = dict(providers)
    Orchestrator({
        "runtime": {"debug": False, "snapshot_ttl_sec": 0, "apply_chunk_size": 0, "apply_chunk_pause_ms": 0},
        "sync": {"dry_run": dry_run, "enable_add": True, "enable_remove": False, "include_observed_deletes": False, "allow_mass_delete": True},
        "pairs": [pair],
    }, interactive=plan).run(dry_run=bool(plan and plan.preview), pair_scope_ids=["p1"], write_state_json=not bool(plan and plan.preview))


def _sent(ops: HistoryOps) -> set[tuple[str, Any, Any]]:
    return {(str((it.get("show_ids") or {}).get("tmdb")), it.get("season"), it.get("episode")) for batch in ops.add_calls for it in batch}


def _tagged(ops: DroppedOps) -> list[str]:
    return sorted(str(show["ids"].get("tmdb")) for batch in ops.tag_calls for show in batch)


def test_mode_falls_back_to_the_legacy_ignore_switch():
    assert provider_dropped_mode({}, "trakt", "history") == "sync"
    assert provider_dropped_mode({"trakt": {"history_ignore_dropped_shows": True}}, "trakt", "history") == "ignore"
    assert provider_dropped_mode({"trakt": {"history_ignore_dropped_shows": False}}, "trakt", "history") == "sync"
    assert provider_dropped_mode({"trakt": {"history_dropped_mode": "tag", "history_ignore_dropped_shows": True}}, "trakt", "history") == "tag"
    assert provider_dropped_mode({"trakt": {"history_dropped_mode": "bogus"}}, "trakt", "history") == "sync"


def test_tag_candidates_need_the_show_on_the_target_and_skip_movies():
    source = _index(_episode("1", 1, 1), _episode("1", 1, 2), _episode("2", 1, 1),
                    {"type": "movie", "ids": {"tmdb": "1"}, "watched_at": "2024-01-01T00:00:00Z"})
    target = _index({**_episode("1", 1, 1), "show_ids": {"tmdb": "1", "trakt": "77"}})

    shows = dropped_tag_candidates(source, {"tmdb:1", "tmdb:2"}, target)

    assert shows == [{"type": "show", "ids": {"tmdb": "1", "trakt": "77"}, "title": "Show 1"}]


def test_apply_skips_shows_already_dropped_on_the_target():
    dst = DroppedOps(provider="DST", index={})
    events: list[dict[str, Any]] = []
    target = _index(_episode("1", 1, 1), _episode("2", 1, 1))

    confirmed = apply_dropped_tags(
        src="SRC", dst="DST", src_cfg={"src": {"history_dropped_mode": "tag"}}, dst_ops=dst, dst_cfg={}, feature="history",
        source=target, source_tokens={"tmdb:1", "tmdb:2"}, target=target, target_tokens=lambda: {"tmdb:2"},
        dry_run=False, emit=lambda *_a, **kw: events.append(kw),
    )

    assert confirmed == 1
    assert _tagged(dst) == ["1"]
    assert any(e.get("msg") == "provider.dropped.tagged" for e in events)


def test_one_way_tag_mode_syncs_the_plays_and_tags_the_target(monkeypatch):
    src = DroppedOps(provider="SRC", index=_index(_episode("1", 1, 1), _episode("2", 1, 1)), dropped={"tmdb:1"})
    dst = DroppedOps(provider="DST", index={})

    _run(monkeypatch, src, dst, providers={"src": {"history_dropped_mode": "tag"}})

    assert _sent(dst) == {("1", 1, 1), ("2", 1, 1)}
    assert _tagged(dst) == ["1"]


def test_one_way_tag_mode_does_not_retag_a_show_dropped_on_the_target(monkeypatch):
    src = DroppedOps(provider="SRC", index=_index(_episode("1", 1, 1)), dropped={"tmdb:1"})
    dst = DroppedOps(provider="DST", index={}, dropped={"tmdb:1"})

    _run(monkeypatch, src, dst, providers={"src": {"history_dropped_mode": "tag"}})

    assert _sent(dst) == {("1", 1, 1)}
    assert dst.tag_calls == []


def test_one_way_tag_mode_writes_nothing_on_a_dry_run(monkeypatch):
    src = DroppedOps(provider="SRC", index=_index(_episode("1", 1, 1)), dropped={"tmdb:1"})
    dst = DroppedOps(provider="DST", index={})

    _run(monkeypatch, src, dst, providers={"src": {"history_dropped_mode": "tag"}}, dry_run=True)

    assert dst.tag_calls == []


def test_legacy_ignore_switch_still_skips_dropped_shows(monkeypatch):
    src = DroppedOps(provider="SRC", index=_index(_episode("1", 1, 1), _episode("2", 1, 1)), dropped={"tmdb:1"})
    dst = DroppedOps(provider="DST", index={})

    _run(monkeypatch, src, dst, providers={"src": {"history_ignore_dropped_shows": True}})

    assert _sent(dst) == {("2", 1, 1)}
    assert dst.tag_calls == []


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_sync_mode_does_not_read_or_tag_dropped_shows(monkeypatch, mode):
    src = DroppedOps(provider="SRC", index=_index(_episode("1", 1, 1)), dropped={"tmdb:1"})
    dst = DroppedOps(provider="DST", index={})

    _run(monkeypatch, src, dst, mode=mode)

    assert _sent(dst) == {("1", 1, 1)}
    assert src.dropped_reads == 0
    assert dst.dropped_reads == 0
    assert src.tag_calls == []
    assert dst.tag_calls == []


def test_tag_mode_without_a_tagging_target_syncs_normally(monkeypatch):
    src = DroppedOps(provider="SRC", index=_index(_episode("1", 1, 1)), dropped={"tmdb:1"})
    dst = HistoryOps(provider="DST", index={})

    _run(monkeypatch, src, dst, providers={"src": {"history_dropped_mode": "tag"}})

    assert _sent(dst) == {("1", 1, 1)}
    assert src.dropped_reads == 0


def test_two_way_tag_mode_tags_the_other_side(monkeypatch):
    src = DroppedOps(provider="SRC", index=_index(_episode("1", 1, 1)), dropped={"tmdb:1"})
    dst = DroppedOps(provider="DST", index={})

    _run(monkeypatch, src, dst, providers={"src": {"history_dropped_mode": "tag"}}, mode="two-way")

    assert _sent(dst) == {("1", 1, 1)}
    assert _tagged(dst) == ["1"]
    assert src.tag_calls == []


def test_pair_save_keeps_dropped_modes_for_every_supported_provider():
    out = _normalize_pair_providers({
        "wetrakr": {"history_dropped_mode": "TAG"},
        "flicklist": {"history_ignore_dropped_shows": "true"},
        "trakt": {"history_dropped_mode": "nope", "history_collection": True},
    })

    assert out["wetrakr"] == {"history_dropped_mode": "tag"}
    assert out["flicklist"] == {"history_ignore_dropped_shows": True}
    assert out["trakt"] == {"history_collection": True}


def test_tag_candidates_deduplicate_using_target_aliases():
    source = {
        "a": {"type": "show", "ids": {"tmdb": "1"}},
        "b": {"type": "show", "ids": {"tvdb": "2"}},
    }
    target = {"a": {"type": "show", "ids": {"tmdb": "1", "tvdb": "2"}}}

    assert dropped_tag_candidates(source, {"tmdb:1", "tvdb:2"}, target) == [
        {"type": "show", "ids": {"tmdb": "1", "tvdb": "2"}},
    ]


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_destination_legacy_ignore_blocks_writes(monkeypatch, mode):
    src = DroppedOps(provider="SRC", index=_index(_episode("1", 1, 1), _episode("2", 1, 1)))
    dst = DroppedOps(provider="DST", index={}, dropped={"tmdb:1"})

    _run(monkeypatch, src, dst, providers={"dst": {"history_ignore_dropped_shows": True}}, mode=mode)

    assert _sent(dst) == {("2", 1, 1)}
    assert src.tag_calls == dst.tag_calls == []


def test_two_way_destination_tag_mode_tags_source(monkeypatch):
    src = DroppedOps(provider="SRC", index={})
    dst = DroppedOps(provider="DST", index=_index(_episode("1", 1, 1)), dropped={"tmdb:1"})

    _run(monkeypatch, src, dst, providers={"dst": {"history_dropped_mode": "tag"}}, mode="two-way")

    assert _sent(src) == {("1", 1, 1)}
    assert _tagged(src) == ["1"]
    assert dst.tag_calls == []


def test_apply_logs_provider_exception(monkeypatch):
    dst = DroppedOps(provider="DST", index={})
    events: list[dict[str, Any]] = []
    target = _index(_episode("1", 1, 1))

    def fail(*_args):
        raise RuntimeError("tag failed")

    monkeypatch.setattr(dst, "tag_dropped_shows", fail)
    assert apply_dropped_tags(
        src="SRC", dst="DST", src_cfg={"src": {"history_dropped_mode": "tag"}}, dst_ops=dst,
        dst_cfg={}, feature="history", source=target, source_tokens={"tmdb:1"}, target=target,
        target_tokens=set, dry_run=False, emit=lambda *_a, **kw: events.append(kw),
    ) == 0
    assert any(e.get("msg") == "provider.dropped.tag_failed" for e in events)


class _Resp:
    def __init__(self, body: Any, status: int = 200):
        self._body = body
        self.status_code = status
        self.ok = 200 <= status < 300
        self.text = "x"

    def json(self) -> Any:
        return self._body


class _Session:
    def __init__(self, listing: Mapping[str, Any], reply: Mapping[str, Any] | None = None):
        self.listing = listing
        self.reply = reply
        self.gets = 0
        self.posts: list[dict[str, Any]] = []

    def get(self, *_a: Any, **_k: Any) -> _Resp:
        self.gets += 1
        return _Resp(self.listing)

    def post(self, *_a: Any, json: Mapping[str, Any], **_k: Any) -> _Resp:
        self.posts.append(dict(json))
        return _Resp(self.reply if self.reply is not None else {"added": {"shows": len(json.get("shows") or [])}})


def _simkl_adapter(monkeypatch, listing: Mapping[str, Any], *, marker: str = "", tmp_path: Any = None,
                   reply: Mapping[str, Any] | None = None) -> Any:
    monkeypatch.setattr(simkl_history, "_headers", lambda *_a, **_k: {})
    monkeypatch.setattr(simkl_history, "_params", lambda *_a, **_k: {})
    monkeypatch.setattr(simkl_history, "fetch_activities", lambda *_a, **_k: ({"all": marker} if marker else None, {}))
    if tmp_path is not None:
        monkeypatch.setenv("CW_PAIR_SCOPE", "dropped-test")
        monkeypatch.setattr(simkl_history, "state_file", lambda name: tmp_path / name)
    return SimpleNamespace(client=SimpleNamespace(session=_Session(listing, reply)), cfg=SimpleNamespace(timeout=5))


def test_simkl_status_lookup_is_cached_until_activities_change(monkeypatch, tmp_path):
    listing = {"shows": [{"status": "completed", "show": {"ids": {"simkl": 9, "tmdb": 1}}}]}
    adapter = _simkl_adapter(monkeypatch, listing, marker="2026-10-18T10:00:00Z", tmp_path=tmp_path)

    simkl_history.tag_dropped_shows(adapter, [{"type": "show", "ids": {"tmdb": "1"}}])
    simkl_history.tag_dropped_shows(adapter, [{"type": "show", "ids": {"tmdb": "1"}}])
    assert adapter.client.session.gets == 1

    monkeypatch.setattr(simkl_history, "fetch_activities", lambda *_a, **_k: ({"all": "2026-10-18T11:00:00Z"}, {}))
    simkl_history.tag_dropped_shows(adapter, [{"type": "show", "ids": {"tmdb": "1"}}])
    assert adapter.client.session.gets == 2
    assert adapter.client.session.posts == []


def test_simkl_reports_shows_it_could_not_resolve(monkeypatch):
    adapter = _simkl_adapter(monkeypatch, {"shows": [{"status": "watching", "show": {"ids": {"simkl": 9, "tmdb": 1}}}]},
                             reply={"added": {"shows": 0}, "not_found": {"shows": [{"ids": {"tmdb": "1"}}]}})

    res = simkl_history.tag_dropped_shows(adapter, [{"type": "show", "ids": {"tmdb": "1"}}])

    assert res == {"attempted": 1, "confirmed": 0, "skipped": 0, "not_found": 1, "errors": 0}


def test_simkl_tags_a_show_known_only_by_tmdb(monkeypatch):
    adapter = _simkl_adapter(monkeypatch, {"shows": [{"status": "watching", "show": {"ids": {"simkl": 9, "tmdb": 1}}}]})

    res = simkl_history.tag_dropped_shows(adapter, [{"type": "show", "ids": {"tmdb": "1"}}, {"type": "show", "ids": {"tvdb": "5"}}])

    assert res["confirmed"] == 2
    assert [s["ids"] for s in adapter.client.session.posts[0]["shows"]] == [{"tmdb": "1"}, {"tvdb": "5"}]


def test_simkl_skips_a_show_completed_under_any_shared_id(monkeypatch):
    adapter = _simkl_adapter(monkeypatch, {"anime": [
        {"status": "watching", "show": {"ids": {"simkl": 8, "tmdb": 1}}},
        {"status": "completed", "show": {"ids": {"simkl": 9, "tmdb": 1}}},
    ]})

    res = simkl_history.tag_dropped_shows(adapter, [{"type": "show", "ids": {"tmdb": "1"}}])

    assert res["skipped"] == 1
    assert adapter.client.session.posts == []


def test_mdblist_counts_item_errors_in_a_200_response():
    class _Client:
        BASE = "https://api.mdblist.com"

        def post(self, *_a: Any, **_k: Any) -> _Resp:
            return _Resp({"updated": {"shows": 1}, "not_found": {"shows": []}, "errors": [{"ids": {"tmdb": 2}, "error": "not watched"}]})

    adapter = SimpleNamespace(client=_Client(), cfg=SimpleNamespace(api_key="k"))

    res = MDBLISTModule.tag_dropped_shows(cast(MDBLISTModule, adapter), [{"ids": {"tmdb": "1"}}, {"ids": {"tmdb": "2"}}])

    assert res["confirmed"] == 1
    assert res["errors"] == 1


def test_mdblist_dropped_reader_follows_total_without_has_more(monkeypatch, tmp_path):
    import providers.sync._mod_MDBLIST as mdblist_mod

    pages = [
        {"shows": [{"show": {"ids": {"tmdb": n}}} for n in range(1, 1001)], "pagination": {"total": 1001, "limit": 1000, "offset": 0}},
        {"shows": [{"show": {"ids": {"tmdb": 1001}}}], "pagination": {"total": 1001, "limit": 1000, "offset": 1000}},
    ]
    offsets: list[int] = []

    class _Client:
        BASE = "https://api.mdblist.com"

        def last_activities(self) -> Mapping[str, Any]:
            return {}

        def get(self, _url: str, *, params: Mapping[str, Any]) -> _Resp:
            offsets.append(int(params["offset"]))
            return _Resp(pages[len(offsets) - 1])

    monkeypatch.setattr(mdblist_mod, "_dropped_cache_path", lambda: tmp_path / "mdblist_dropped.index.json")
    adapter = SimpleNamespace(client=_Client(), cfg=SimpleNamespace(api_key="k"))

    tokens = MDBLISTModule.dropped_show_tokens(cast(MDBLISTModule, adapter))

    assert offsets == [0, 1000]
    assert "tmdb:1001" in tokens


def test_simkl_activities_force_bypasses_the_short_memo(monkeypatch):
    calls: list[int] = []

    class _ActResp:
        status_code = 200
        headers: dict[str, str] = {}
        text = "x"

        def json(self) -> Any:
            return {"all": f"t{len(calls)}"}

    class _ActSession:
        def get(self, *_a: Any, **_k: Any) -> _ActResp:
            calls.append(1)
            return _ActResp()

    monkeypatch.setattr(simkl_common, "_ACT_MEMO", {})
    headers = {"Authorization": "Bearer dropped-test"}

    simkl_common.fetch_activities(_ActSession(), headers)
    simkl_common.fetch_activities(_ActSession(), headers)
    assert len(calls) == 1
    acts, _ = simkl_common.fetch_activities(_ActSession(), headers, force=True)
    assert len(calls) == 2
    assert acts == {"all": "t2"}


def test_simkl_tagger_requests_fresh_activities(monkeypatch):
    seen: list[Mapping[str, Any]] = []
    adapter = _simkl_adapter(monkeypatch, {"shows": [{"status": "watching", "show": {"ids": {"simkl": 9, "tmdb": 1}}}]})
    monkeypatch.setattr(simkl_history, "fetch_activities", lambda *_a, **k: (seen.append(k), (None, {}))[1])

    simkl_history.tag_dropped_shows(adapter, [{"type": "show", "ids": {"tmdb": "1"}}])

    assert seen and seen[0].get("force") is True


def _tag_rows(plan: InteractivePlan) -> list[dict[str, Any]]:
    return [row for row in plan.rows.values() if row["operation"] == "tag_dropped"]


def test_interactive_preview_lists_planned_tags_without_writing(monkeypatch):
    src = DroppedOps(provider="SRC", index=_index(_episode("1", 1, 1), _episode("2", 1, 1)), dropped={"tmdb:1"})
    dst = DroppedOps(provider="DST", index={})
    preview = InteractivePlan()

    _run(monkeypatch, src, dst, providers={"src": {"history_dropped_mode": "tag"}}, plan=preview)

    rows = _tag_rows(preview)
    assert [(row["provider"], row["source"], row["item"]["ids"]) for row in rows] == [("DST", "SRC", {"tmdb": "1"})]
    assert dst.add_calls == [] and dst.tag_calls == []


def test_interactive_apply_tags_only_when_the_tag_row_is_selected(monkeypatch):
    src = DroppedOps(provider="SRC", index=_index(_episode("1", 1, 1)), dropped={"tmdb:1"})
    dst = DroppedOps(provider="DST", index={})
    providers = {"src": {"history_dropped_mode": "tag"}}
    preview = InteractivePlan()
    _run(monkeypatch, src, dst, providers=providers, plan=preview)
    adds_only = {rid for rid, row in preview.rows.items() if row["operation"] == "add"}

    skip_tag = InteractivePlan(preview=False, selected=adds_only)
    _run(monkeypatch, src, dst, providers=providers, plan=skip_tag)
    assert _sent(dst) == {("1", 1, 1)}
    assert dst.tag_calls == []

    preview = InteractivePlan()
    _run(monkeypatch, src, dst, providers=providers, plan=preview)
    tag_ids = {row["id"] for row in _tag_rows(preview)}
    assert len(tag_ids) == 1

    with_tag = InteractivePlan(preview=False, selected=tag_ids)
    _run(monkeypatch, src, dst, providers=providers, plan=with_tag)
    assert _tagged(dst) == ["1"]
    assert tag_ids <= with_tag.seen


def test_interactive_two_way_preview_lists_tags_for_the_other_side(monkeypatch):
    src = DroppedOps(provider="SRC", index=_index(_episode("1", 1, 1)), dropped={"tmdb:1"})
    dst = DroppedOps(provider="DST", index={})
    preview = InteractivePlan()

    _run(monkeypatch, src, dst, providers={"src": {"history_dropped_mode": "tag"}}, mode="two-way", plan=preview)

    assert [(row["provider"], row["source"]) for row in _tag_rows(preview)] == [("DST", "SRC")]


def _recorded(monkeypatch, *events: tuple[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    monkeypatch.setattr(recorder, "record_events", lambda batch, **_k: rows.extend(batch) or len(batch))
    rec = recorder.RunRecorder(lambda *_a, **_k: None, run_id="r1")
    for name, fields in events:
        rec.emit(name, **fields)
    rec._flush(force=True)
    return rows


def test_events_record_a_dropped_tag_summary(monkeypatch):
    rows = _recorded(
        monkeypatch,
        ("feature:start", {"src": "SRC", "dst": "DST", "feature": "history"}),
        ("dropped:tagged", {"dst": "DST", "feature": "history", "attempted": 2, "confirmed": 2, "skipped": 0,
                            "not_found": 0, "errors": 0, "shows": [{"title": "Show 1"}, {"title": "Show 2"}]}),
    )

    [row] = [r for r in rows if r.get("operation") == "tag_dropped"]
    assert row["event_type"] == "write_attempted"
    assert row["source_provider"] == "SRC" and row["destination_provider"] == "DST"
    assert row["severity"] == "info"
    detail = row["detail"] if isinstance(row["detail"], dict) else json.loads(row["detail"])
    assert detail["tagged"] == 2 and detail["shows"] == ["Show 1", "Show 2"]
    assert not {"added", "removed", "updated", "errors"} & set(detail)


def test_events_record_failed_dropped_tags(monkeypatch):
    rows = _recorded(
        monkeypatch,
        ("feature:start", {"src": "SRC", "dst": "DST", "feature": "history"}),
        ("dropped:tagged", {"dst": "DST", "feature": "history", "attempted": 1, "confirmed": 0, "errors": 1, "error": "boom"}),
    )

    types = [(r["event_type"], r.get("reason_code")) for r in rows if r.get("operation") == "tag_dropped"]
    assert types == [("write_attempted", "errors"), ("write_failed", "tag_dropped_failed")]


def test_events_route_two_way_tags_to_the_tagged_side(monkeypatch):
    rows = _recorded(
        monkeypatch,
        ("two:start", {"a": "SRC", "b": "DST", "feature": "history"}),
        ("dropped:tagged", {"dst": "SRC", "feature": "history", "attempted": 1, "confirmed": 1}),
    )

    [row] = [r for r in rows if r.get("operation") == "tag_dropped"]
    assert (row["source_provider"], row["destination_provider"]) == ("DST", "SRC")


def test_apply_emits_the_dropped_tagged_event():
    dst = DroppedOps(provider="DST", index={})
    target = _index(_episode("1", 1, 1))
    names: list[tuple[str, dict[str, Any]]] = []

    apply_dropped_tags(
        src="SRC", dst="DST", src_cfg={"src": {"history_dropped_mode": "tag"}}, dst_ops=dst, dst_cfg={}, feature="history",
        source=target, source_tokens={"tmdb:1"}, target=target, target_tokens=set, dry_run=False,
        emit=lambda name, **kw: names.append((name, kw)), dst_inst="p2",
    )

    [(_, fields)] = [(n, kw) for n, kw in names if n == "dropped:tagged"]
    assert fields["dst"] == "DST" and fields["destination_instance"] == "p2"
    assert fields["confirmed"] == 1 and fields["shows"] == [{"title": "Show 1", "ids": {"tmdb": "1"}}]


def test_interactive_two_way_tag_survives_reverse_history_writes(monkeypatch):
    richer = {**_episode("1", 1, 1), "show_ids": {"tmdb": "1", "tvdb": "9"}, "series_title": "Show One (B)"}
    src = DroppedOps(provider="SRC", index=_index(_episode("1", 1, 2)), dropped={"tmdb:1"})
    dst = DroppedOps(provider="DST", index=_index(richer))
    providers = {"src": {"history_dropped_mode": "tag"}}
    preview = InteractivePlan()
    _run(monkeypatch, src, dst, providers=providers, mode="two-way", plan=preview)
    tag_ids = {row["id"] for row in _tag_rows(preview)}
    assert len(tag_ids) == 1

    execution = InteractivePlan(preview=False, selected=set(preview.rows))
    _run(monkeypatch, src, dst, providers=providers, mode="two-way", plan=execution)

    assert _tagged(dst) == ["1"]
    assert tag_ids <= execution.seen


def test_event_detail_stays_valid_json_with_many_long_titles(monkeypatch):
    shows = [{"title": f"A fairly long television show title number {n} with a subtitle"} for n in range(100)]
    rows = _recorded(
        monkeypatch,
        ("feature:start", {"src": "SRC", "dst": "DST", "feature": "history"}),
        ("dropped:tagged", {"dst": "DST", "feature": "history", "attempted": 100, "confirmed": 100, "shows": shows}),
    )

    [row] = [r for r in rows if r.get("operation") == "tag_dropped"]
    assert len(row["detail"]) <= 4000
    detail = json.loads(row["detail"])
    assert detail["tagged"] == 100
    assert detail["shows"] and detail["shows_omitted"] == 100 - len(detail["shows"])


def test_cancel_during_target_lookup_prevents_the_tag_write():
    dst = DroppedOps(provider="DST", index={})
    target = _index(_episode("1", 1, 1))

    def lookup() -> set[str]:
        run_control.request_cancel()
        return set()

    try:
        assert apply_dropped_tags(
            src="SRC", dst="DST", src_cfg={"src": {"history_dropped_mode": "tag"}}, dst_ops=dst, dst_cfg={},
            feature="history", source=target, source_tokens={"tmdb:1"}, target=target, target_tokens=lookup,
            dry_run=False, emit=lambda *_a, **_k: None,
        ) == 0
    finally:
        run_control.clear_cancel()
        run_control.clear_queue_stop()
    assert dst.tag_calls == []
