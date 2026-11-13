# tests/test_scoped_run_summary.py
# CrossWatch - Per-profile sync results, progress, persistence and transport isolation
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest

from api import syncAPI as sync
from cw_platform.local_db import close_conn
from cw_platform.local_db.sync_reports import clear_reports, list_reports, save_report
from services import run_summary


DEFAULT = "__default__"


@pytest.fixture
def run(monkeypatch):
    pairs = [
        {"id": name, "profile_id": name if name != "admin" else "", "source": "PLEX", "target": "TRAKT",
         "source_instance": "default" if name == "admin" else name, "target_instance": "default" if name == "admin" else name,
         "features": {"watchlist": {"enable": True}}}
        for name in ("admin", "alice", "bob")
    ]
    cfg = {"pairs": pairs, "user_profiles": {name: {"instances": {"PLEX": [name], "TRAKT": [name]}} for name in ("alice", "bob")}}
    cfg.update({provider: {"instances": {name: {} for name in ("alice", "bob")}} for provider in ("plex", "trakt")})
    monkeypatch.setattr(sync, "_rt", lambda: ({"SYNC": []}, None, None, None, None, None, None, lambda line: line))
    monkeypatch.setattr(sync, "_env", lambda: (lambda: cfg, lambda *_: None))
    monkeypatch.setattr(sync, "_hydrate_summary_spotlights_from_log", lambda *_: None)
    monkeypatch.setattr(sync, "_provider_counts_current", lambda: {"PLEX": 999})
    monkeypatch.setattr("cw_platform.config_base.load_config", lambda: cfg)
    monkeypatch.setattr(sync, "SUMMARY", {})
    sync._summary_reset()
    sync.SUMMARY.update(run_id="mixed", running=True, timeline={"start": True, "pre": True, "post": True, "done": False})
    run_summary.initialize(sync.SUMMARY, pairs)
    return cfg


def send(event, **data):
    sync._parse_sync_line(json.dumps({"event": event, **data}))


def start(cfg, name):
    pair = next(pair for pair in cfg["pairs"] if pair["id"] == name)
    send("run:pair", pair_id=name, src=pair["source"], dst=pair["target"], src_instance=pair["source_instance"],
         dst_instance=pair["target_instance"], i=cfg["pairs"].index(pair) + 1, n=3, features=["watchlist"])
    send("run:feature", pair_id=name, i=1, n=1, feature="watchlist")


def write(name, action="add", count=1):
    item = {"key": name, "title": f"{name} movie", "type": "movie"}
    send(f"apply:{action}:done", feature="watchlist", count=count, spotlight=[item])
    send("ui:spotlight", feature="watchlist", action=action, items=[item])


def scoped(cfg, name):
    user = {"is_admin": name == "admin", "profile_id": name}
    return sync._scope_summary_for_user(cfg, user, sync._summary_snapshot(), DEFAULT if name == "admin" else "")


def request(name, *, admin_view=False):
    user = {"is_admin": name == "admin" or admin_view, "profile_id": "" if admin_view else name}
    scope = DEFAULT if name == "admin" else name if admin_view else ""
    return SimpleNamespace(state=SimpleNamespace(cw_user=user), query_params={"user_profile": scope}, cookies={}, headers={})


def test_mixed_run_keeps_each_profiles_counts_and_titles_separate(run):
    for index, name in enumerate(("admin", "alice", "bob"), 1):
        start(run, name)
        for action in ("add", "remove", "update"):
            write(name, action, index)
    snapshot = sync._summary_snapshot()
    for index, name in enumerate(("admin", "alice", "bob"), 1):
        data = scoped(run, name)
        assert data["details_available"] is True
        assert data["pair_scope_ids"] == [name]
        lane = data["features"]["watchlist"]
        assert [lane[field] for field in ("added", "removed", "updated")] == [index] * 3
        assert [item["title"] for item in lane["spotlight_add"]] == [f"{name} movie"]
        assert data["added_last"] == index
        assert "pair_summaries" not in data
        assert data["provider_counts"] == {}
    all_data = sync._scope_summary_for_user(run, {"is_admin": True}, snapshot)
    assert all_data["features"]["watchlist"]["added"] == 6
    assert "pair_summaries" not in all_data
    assert sync._summary_snapshot() == snapshot


def test_named_progress_waits_for_own_pairs_and_finishes_before_other_users(run):
    start(run, "admin")
    assert scoped(run, "alice")["waiting"] is True
    assert scoped(run, "alice")["run_busy"] is True
    assert scoped(run, "alice")["current_pair"] is None
    start(run, "alice")
    send("one:plan", feature="watchlist", adds=10)
    send("apply:add:progress", feature="watchlist", done=4, total=10)
    data = scoped(run, "alice")
    assert data["running"] is True
    assert data["_phase"]["apply"]["progress"] == 0.4
    assert data["current_pair"]["pair_id"] == "alice"
    write("alice", count=10)
    start(run, "bob")
    data = scoped(run, "alice")
    assert data["timeline"]["done"] is True
    assert data["running"] is False
    assert data["exit_code"] == 0
    assert scoped(run, "admin")["running"] is True
    assert sync._scope_request_summary(run, request("alice", admin_view=True), sync._summary_snapshot())["running"] is True


@pytest.mark.parametrize("name", ["admin", "alice", "bob"])
def test_legacy_reports_hide_unattributed_items_and_counts(run, name):
    snap = {"run_id": "old", "running": True, "pair_scope_ids": ["admin", "alice", "bob"],
            "features": {"watchlist": {"added": 99, "spotlight_add": [{"title": "Private title"}]}},
            "unresolved_items": [{"title": "Private unresolved"}], "provider_counts": {"PLEX": 99}}
    user = {"is_admin": name == "admin", "profile_id": name}
    result = sync._scope_summary_for_user(run, user, snap, DEFAULT if name == "admin" else "")
    assert result["details_available"] is False
    assert result["features"]["watchlist"]["added"] == 0
    assert "Private" not in json.dumps(result)
    assert result["running"] is (name == "admin")


def test_unattributed_events_and_changed_pair_ownership_do_not_leak(run):
    start(run, "alice")
    write("alice")
    run["pairs"][1]["source_instance"] = "bob"
    assert scoped(run, "alice")["features"]["watchlist"]["added"] == 0
    send("apply:add:done", pair_id="unknown", feature="watchlist", count=10, spotlight=[{"title": "Unknown"}])
    assert scoped(run, "bob")["details_available"] is False


def test_managed_users_cannot_request_default_or_all_scope(run):
    start(run, "admin")
    write("admin")
    start(run, "alice")
    write("alice")
    req = request("alice")
    for scope in (DEFAULT, "all", "bob"):
        req.query_params["user_profile"] = scope
        data = sync._scope_request_summary(run, req, sync._summary_snapshot())
        assert data["summary_scope"] == "alice"
        assert data["features"]["watchlist"]["spotlight_add"][0]["title"] == "alice movie"


def test_same_pair_id_with_different_instances_keeps_separate_results(run):
    for pair in run["pairs"]:
        pair["id"] = "shared"
    run_summary.initialize(sync.SUMMARY, run["pairs"])
    for pair in run["pairs"]:
        send("run:pair", pair_id="shared", src="PLEX", dst="TRAKT", src_instance=pair["source_instance"],
             dst_instance=pair["target_instance"], i=1, n=3, features=["watchlist"])
        write(pair["profile_id"] or "admin")
    assert scoped(run, "admin")["features"]["watchlist"]["spotlight_add"][0]["title"] == "admin movie"
    assert scoped(run, "alice")["features"]["watchlist"]["spotlight_add"][0]["title"] == "alice movie"


def test_pair_failures_and_cancellation_do_not_change_other_profiles_results(run):
    start(run, "admin")
    write("admin")
    start(run, "alice")
    send("feature:error", feature="watchlist", error="Failure")
    assert scoped(run, "alice")["running"] is True
    start(run, "bob")
    assert scoped(run, "alice")["exit_code"] == 1
    send("run:done", cancelled=True)
    assert scoped(run, "bob")["cancelled"] is True
    assert scoped(run, "alice")["cancelled"] is False


def test_reassigning_a_pair_does_not_transfer_historical_results(run):
    start(run, "alice")
    write("alice")
    run["pairs"][1]["profile_id"] = "bob"
    run["user_profiles"]["bob"]["instances"] = {"PLEX": ["alice", "bob"], "TRAKT": ["alice", "bob"]}
    assert scoped(run, "bob")["features"]["watchlist"]["added"] == 0


@pytest.mark.parametrize("name", ["admin", "alice"])
def test_matched_users_are_authorized_through_their_parent_pair(run, name):
    from copy import deepcopy
    from cw_platform.orchestrator._pairs import expand_sync_user_pairs
    from test_sync_users import _pair_cfg, LINKS

    cfg = _pair_cfg(match_users=True, user_matches=LINKS)
    if name == "alice":
        for provider in ("emby", "jellyfin"):
            parent = deepcopy(cfg[provider])
            parent.pop("instances", None)
            cfg[provider].setdefault("instances", {})["alice"] = parent
        cfg["pairs"][0].update(profile_id="alice", source_instance="alice", target_instance="alice")
        cfg["user_profiles"] = {"alice": {"instances": {"EMBY": ["alice"], "JELLYFIN": ["alice"]}}}
    pairs = expand_sync_user_pairs(cfg)
    assert len(pairs) == 2
    run_summary.initialize(sync.SUMMARY, pairs)
    for pair in pairs:
        send("run:pair", pair_id=pair["id"], src=pair["source"], dst=pair["target"], src_instance=pair["source_instance"],
             dst_instance=pair["target_instance"], i=1, n=2, features=["watchlist"])
        write(pair["source_instance"])
    data = scoped(cfg, name)
    assert data["details_available"] is True
    assert data["features"]["watchlist"]["added"] == 2
    assert len(data["features"]["watchlist"]["spotlight_add"]) == 2
    if name == "alice":
        cfg["user_profiles"]["alice"]["instances"] = {}
        assert scoped(cfg, name)["features"]["watchlist"]["added"] == 0


def test_deferred_status_mapping_notices_do_not_invalidate_pair_results(run):
    start(run, "alice")
    write("alice")
    start(run, "bob")
    write("bob")
    for event in ("status_mapping:start", "status_mapping:review", "apply:unresolved", "status_mapping:updated", "status_mapping:done"):
        send(event, pair_id="alice", src="PLEX", dst="TRAKT", feature="history", title="Status title")
    assert scoped(run, "alice")["details_available"] is True
    assert scoped(run, "alice")["features"]["watchlist"]["added"] == 1
    assert scoped(run, "bob")["features"]["watchlist"]["added"] == 1


@pytest.mark.parametrize("same_id", [False, True])
def test_early_same_instance_skip_does_not_change_the_previous_pair(run, same_id):
    pairs = [dict(run["pairs"][1]), dict(run["pairs"][2], source="PLEX", target="PLEX", source_instance="bob", target_instance="bob")]
    if same_id:
        pairs[1]["id"] = pairs[0]["id"]
    run["pairs"] = pairs
    run_summary.initialize(sync.SUMMARY, pairs)
    start(run, "alice")
    write("alice")
    send("pair:skip", pair_id=pairs[1]["id"], src="PLEX", dst="PLEX", reason="same_provider_instance")
    send("run:done")
    assert sync.SUMMARY["pair_attribution_complete"] is True
    assert [row["status"] for row in sync.SUMMARY["pair_summaries"]] == ["done", "skipped"]


@pytest.mark.parametrize("reason", ["auth_failed", "provider_down", "missing_provider_ops"])
def test_provider_failure_skips_show_failure_in_the_named_view(run, reason):
    start(run, "alice")
    send("pair:skip", pair_id="alice", src="PLEX", dst="TRAKT", reason=reason)
    assert scoped(run, "alice")["exit_code"] == 1
    assert scoped(run, "alice")["timeline"]["done"] is True


def test_profile_id_case_is_normalized_in_saved_and_current_pairs(run):
    run["pairs"][1]["profile_id"] = "Alice"
    run_summary.initialize(sync.SUMMARY, run["pairs"])
    start(run, "alice")
    write("alice")
    assert scoped(run, "alice")["features"]["watchlist"]["added"] == 1
    sync.SUMMARY["pair_summaries"][1]["pair"]["profile_id"] = "ALICE"
    assert scoped(run, "alice")["features"]["watchlist"]["added"] == 1


def test_open_stream_rechecks_profile_instance_access(run):
    start(run, "alice")
    write("alice")
    req = request("alice")
    req.is_disconnected = lambda: asyncio.sleep(0, result=False)

    async def messages():
        response = await sync.api_run_summary_stream(req)
        first = await anext(response.body_iterator)
        run["user_profiles"]["alice"]["instances"] = {}
        second = await asyncio.wait_for(anext(response.body_iterator), 3)
        await response.body_iterator.aclose()
        return [json.loads(chunk.removeprefix("data: ")) for chunk in (first, second)]

    before, after = asyncio.run(messages())
    assert before["features"]["watchlist"]["added"] == 1
    assert after["features"]["watchlist"]["added"] == 0
    assert "alice movie" not in json.dumps(after)


@pytest.mark.parametrize("name", ["admin", "alice"])
def test_poll_download_and_sse_return_only_scoped_results(run, monkeypatch, name):
    for person in ("admin", "alice", "bob"):
        start(run, person)
        write(person)
    req = request(name)
    req.is_disconnected = lambda: asyncio.sleep(0, result=False)
    logs = [json.dumps({"event": "ui:spotlight", "items": [{"title": "bob private event"}]})]
    monkeypatch.setattr(sync, "_rt", lambda: ({"SYNC": logs},))
    for response in (sync.api_run_summary(req), sync.api_run_summary_file(req)):
        payload = json.loads(response.body)
        assert payload["features"]["watchlist"]["spotlight_add"][0]["title"] == f"{name} movie"
        assert "bob movie" not in response.body.decode()

    async def first_snapshot():
        response = await sync.api_run_summary_stream(req)
        chunks = []
        async for chunk in response.body_iterator:
            chunks.append(chunk)
            if chunk.startswith("data:"):
                break
        await response.body_iterator.aclose()
        return "".join(chunks)

    streamed = asyncio.run(first_snapshot())
    assert "event:" not in streamed
    assert "bob movie" not in streamed
    if name == "alice":
        assert "bob" not in streamed
    assert json.loads(streamed.removeprefix("data: "))["features"]["watchlist"]["added"] == 1


def test_pair_results_survive_report_storage_and_restart(run, monkeypatch, tmp_path):
    monkeypatch.setenv("CROSSWATCH_DB", str(tmp_path / "crosswatch.sqlite3"))
    monkeypatch.setattr("cw_platform.config_base.CONFIG", tmp_path)
    close_conn()
    try:
        for person in ("admin", "alice", "bob"):
            start(run, person)
            write(person)
        send("run:done")
        sync.SUMMARY.update(running=False, exit_code=0, finished_at="2026-11-13T12:00:00Z")
        save_report(tmp_path, sync._summary_snapshot())
        assert "pair_summaries" not in list_reports(tmp_path)[0]
        sync.SUMMARY.clear()
        restored = sync._summary_snapshot_for_response()
        assert restored["pair_attribution_complete"] is True
        result = sync._scope_summary_for_user(run, {"is_admin": False, "profile_id": "alice"}, restored)
        assert result["features"]["watchlist"]["spotlight_add"][0]["title"] == "alice movie"
        assert result["timeline"]["done"] is True
        assert clear_reports(tmp_path) == 1
    finally:
        close_conn()


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_real_orchestrator_events_retain_pair_attribution(config_base, monkeypatch, run, mode):
    from cw_platform.orchestrator import Orchestrator
    from test_interactive_sync_features import feature_setup

    items = [{"type": "movie", "title": f"Movie {i}", "ids": {"tmdb": str(i + 1)}} for i in range(3)]
    cfg, _, _ = feature_setup(config_base, monkeypatch, "watchlist", items, [], mode)
    run_summary.initialize(sync.SUMMARY, cfg["pairs"])
    assert Orchestrator(cfg, on_progress=sync._parse_sync_line).run()["ok"]
    snap = sync._summary_snapshot()
    assert snap["pair_attribution_complete"] is True
    row = snap["pair_summaries"][0]
    assert row["status"] == "done"
    assert row["features"]["watchlist"]["added"] == 3
    assert len(row["features"]["watchlist"]["spotlight_add"]) == 3
