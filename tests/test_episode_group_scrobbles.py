# tests/test_episode_group_scrobbles.py
# CrossWatch - Completed episode group scrobbling and delivery isolation regressions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from copy import deepcopy
from dataclasses import replace
from concurrent.futures import ThreadPoolExecutor

import pytest

from cw_platform.local_db import manual_policy
from cw_platform.local_db.db import close_conn
from cw_platform.scrobble_ack import confirm_scrobble, delivery_receipt
from providers.scrobble.episode_groups import send_grouped
from providers.scrobble.scrobble import Dispatcher, ScrobbleEvent


def episode(number):
    return dict(type="episode", title="Example", show_ids={"tmdb": "100"}, season=1, episode=number)


def event(number=2, **changes):
    return replace(ScrobbleEvent("stop", "episode", {"tmdb_show": "100", "tmdb": "999", "plex": "native"},
        "Example", 2020, 1, number, 95, "alice", "server", f"session-{number}",
        {"NowPlayingItem": {"Id": "native"}}, position_ms=9000, duration_ms=10000), **changes)


@pytest.fixture
def setup(config_base):
    group = dict(id="finale", name="Finale", scrobble=True,
        source=dict(provider="PLEX", instance="default", episodes=[episode(2), episode(3)]),
        target=dict(provider="TRAKT", instance="default", episodes=[episode(2)]))
    manual_policy.save_policy(config_base, dict(version=1, providers={}, pairs={"p1": dict(providers={}, episode_groups=[group])}))
    cfg = dict(pairs=[dict(id="p1", enabled=True, source="PLEX", target="TRAKT", mode="two-way",
                          features={"history": {"enable": True}})], scrobble=dict(trakt=dict(watched_at=90),
                          watch=dict(route_provider="plex", route_sink="trakt")))
    return cfg, group


def test_split_completions_persist_and_send_once(config_base, setup):
    cfg, _ = setup
    calls = []
    send = lambda ev: calls.append(ev) or {"ok": True}
    assert send_grouped(event(), cfg, send)["reason"] == "episode_group_waiting_for_parts"
    close_conn()
    assert send_grouped(event(3), cfg, send)["ok"]
    assert len(calls) == 1
    mapped = calls[0]
    assert mapped.ids == {"tmdb_show": "100"}
    assert mapped.number == 2 and mapped.progress == 100 and mapped.action == "stop"
    assert mapped.position_ms is None and mapped.duration_ms is None
    assert "NowPlayingItem" not in mapped.raw
    close_conn()
    assert send_grouped(event(3), cfg, send)["reason"] == "episode_group_already_completed"
    assert len(calls) == 1


@pytest.mark.parametrize("action,progress", [("start", 100), ("pause", 100), ("stop", 89), ("stop", float("nan"))])
def test_partial_or_live_events_do_not_complete_group(setup, action, progress):
    cfg, _ = setup
    calls = []
    send = lambda ev: calls.append(ev) or {"ok": True}
    assert send_grouped(event(action=action, progress=progress), cfg, send)["reason"] == "episode_group_completion_only"
    send_grouped(event(3), cfg, send)
    assert not calls


def test_combined_episode_retries_only_failed_destination_parts(setup):
    cfg, _ = setup
    cfg["scrobble"]["watch"].update(route_provider="trakt", route_sink="plex")
    calls = []
    def send(ev):
        calls.append(ev.number)
        return {"ok": len(calls) != 2, "retryable": True}
    assert not send_grouped(event(), cfg, send)["ok"]
    close_conn()
    assert send_grouped(event(), cfg, send)["ok"]
    assert calls == [2, 3, 3]


@pytest.mark.parametrize("changes", [{"account": "bob"}, {"server_uuid": "other"}])
def test_accounts_and_servers_do_not_share_partial_completions(setup, changes):
    cfg, _ = setup
    calls = []
    send = lambda ev: calls.append(ev) or {"ok": True}
    send_grouped(event(), cfg, send)
    send_grouped(event(3, **changes), cfg, send)
    assert not calls
    send_grouped(event(3), cfg, send)
    assert len(calls) == 1


@pytest.mark.parametrize("case", ["profile", "instance", "one_way", "opt_out", "unrelated"])
def test_only_opted_in_matching_routes_are_mapped(config_base, setup, case):
    cfg, group = setup
    original = event()
    if case == "profile":
        cfg["scrobble"]["watch"]["route_effective_profile_id"] = "other"
    elif case == "instance":
        cfg["scrobble"]["watch"]["route_sink_instance"] = "other"
    elif case == "one_way":
        cfg["pairs"][0]["mode"] = "one-way"
        cfg["scrobble"]["watch"].update(route_provider="trakt", route_sink="plex")
    elif case == "opt_out":
        policy = manual_policy.load_policy(config_base)
        policy["pairs"]["p1"]["episode_groups"][0]["scrobble"] = False
        manual_policy.save_policy(config_base, policy)
    else:
        original = event(4)
    calls = []
    send_grouped(original, cfg, lambda ev: calls.append(ev))
    assert calls == [original]


def test_unknown_account_cannot_mix_completions(setup):
    cfg, _ = setup
    assert send_grouped(event(account=None), cfg, lambda ev: pytest.fail("unexpected write"))["reason"] == "episode_group_account_required"


def test_ambiguous_pair_groups_fail_without_sending(config_base, setup):
    cfg, _ = setup
    cfg["pairs"].append({**deepcopy(cfg["pairs"][0]), "id": "p2"})
    policy = manual_policy.load_policy(config_base)
    policy["pairs"]["p2"] = deepcopy(policy["pairs"]["p1"])
    manual_policy.save_policy(config_base, policy)
    with pytest.raises(ValueError, match="Multiple episode groups"):
        send_grouped(event(), cfg, lambda ev: pytest.fail("unexpected write"))


def test_group_rename_and_reordering_keep_delivery_state(config_base, setup):
    cfg, _ = setup
    calls = []
    send = lambda ev: calls.append(ev) or {"ok": True}
    send_grouped(event(), cfg, send)
    policy = manual_policy.load_policy(config_base)
    group = policy["pairs"]["p1"]["episode_groups"][0]
    group["name"] = "Renamed"
    group["source"]["episodes"].reverse()
    for side in ("source", "target"):
        for member in group[side]["episodes"]:
            member["title"] = "Renamed"
    manual_policy.save_policy(config_base, policy)
    send_grouped(event(3), cfg, send)
    send_grouped(event(3), cfg, send)
    assert len(calls) == 1


def test_concurrent_completions_are_sent_once(setup):
    cfg, _ = setup
    calls = []
    send = lambda ev: calls.append(ev) or {"ok": True}
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda number: send_grouped(event(number), cfg, send), [2, 3, 2, 3]))
    assert all(row["ok"] for row in results)
    assert len(calls) == 1


def test_void_sink_needs_confirmed_completion_receipt(setup):
    cfg, _ = setup
    cfg["scrobble"]["watch"].update(route_provider="trakt", route_sink="plex")
    assert not send_grouped(event(), cfg, lambda ev: None)["ok"]
    calls = []
    def send(ev):
        calls.append(ev)
        confirm_scrobble(ev, "plex", "default")
    assert send_grouped(event(), cfg, send)["ok"]
    assert len(calls) == 2


def test_failure_after_confirmed_watch_does_not_repeat_it(setup):
    cfg, _ = setup
    calls = []
    def send(ev):
        calls.append(ev)
        confirm_scrobble(ev, "trakt", "default")
        raise RuntimeError("Post-delivery cleanup failed")
    send_grouped(event(), cfg, send)
    assert send_grouped(event(3), cfg, send)["ok"]
    assert send_grouped(event(3), cfg, send)["ok"]
    assert len(calls) == 1


def test_receipts_require_correct_destination_and_success(monkeypatch):
    from services import activity
    monkeypatch.setattr(activity, "add_event", lambda *a, **k: None)
    ev = event(raw={"_cw_episode_group_delivery": "expected"})
    with delivery_receipt("expected", "trakt", "default") as receipt:
        activity.record_scrobble_event(ev, source="plex", target="trakt", status="fail")
        assert not receipt["confirmed"]
        activity.record_scrobble_event(ev, source="plex", target="trakt", target_instance="other")
        assert not receipt["confirmed"]
        activity.record_scrobble_event(ev, source="plex", target="trakt")
        assert receipt["confirmed"]


def test_watcher_uses_grouping_and_existing_retry_queue(setup, monkeypatch):
    cfg, _ = setup
    now, calls = [100.0], []
    monkeypatch.setattr("providers.scrobble.scrobble.time.monotonic", lambda: now[0])
    class Sink:
        def send(self, ev, cfg=None):
            calls.append(ev)
            return {"ok": len(calls) > 1, "retryable": True}
    dispatcher = Dispatcher([Sink()], cfg_provider=lambda: cfg)
    assert dispatcher.dispatch(event(action="start", progress=95))
    assert dispatcher.dispatch(event())
    assert not dispatcher.dispatch(event(3))
    now[0] += 31
    dispatcher.retry_pending()
    assert len(calls) == 2 and all(row.number == 2 for row in calls)
    assert not dispatcher._pending


def test_webhook_and_watcher_share_account_completion_state(setup, monkeypatch):
    from providers.webhooks import dispatch
    cfg, _ = setup
    calls = []
    class Sink:
        def send(self, ev, cfg=None):
            calls.append(ev)
            return {"ok": True}
    monkeypatch.setattr(dispatch, "webhook_sinks", lambda *a: ["trakt"])
    monkeypatch.setattr(dispatch, "sink_configured", lambda *a: True)
    monkeypatch.setattr(dispatch, "_make_sink", lambda *a: Sink())
    send_grouped(event(), cfg, Sink().send)
    response = dispatch.dispatch_scrobble("plex", "/scrobble/stop", media_type="episode", ids={"tmdb_show": "100"},
        season=1, episode=3, progress=95, title="Example", account="alice", server_uuid="server", cfg=cfg)
    assert response.status_code == 200
    assert len(calls) == 1 and calls[0].number == 2
    send_grouped(event(3), cfg, Sink().send)
    assert len(calls) == 1


@pytest.mark.parametrize("provider,cls_name", [("trakt", "TraktSink"), ("mdblist", "MDBListSink"), ("simkl", "SimklSink")])
def test_tracker_waits_for_confirmed_delivery_and_retries_failure(config_base, setup, monkeypatch, provider, cls_name):
    from importlib import import_module
    from services import activity

    cfg, _ = setup
    policy = manual_policy.load_policy(config_base)
    policy["pairs"]["p1"]["episode_groups"][0]["target"]["provider"] = provider.upper()
    manual_policy.save_policy(config_base, policy)
    cfg["pairs"][0]["target"] = provider.upper()
    cfg["scrobble"]["watch"]["route_sink"] = provider
    cfg[provider] = {"client_id": "test-client", "api_key": "test-key", "access_token": "test-token"}
    module = import_module(f"providers.scrobble.{provider}.sink")
    sink = getattr(module, cls_name)()
    monkeypatch.setattr(activity, "add_event", lambda *a, **k: None)
    monkeypatch.setattr(sink, "_note_watch", lambda *a, **k: None)
    monkeypatch.setattr(module, "_auto_remove_across", lambda *a, **k: None)
    if provider == "trakt":
        monkeypatch.setattr(sink, "_enqueue", lambda *a: pytest.fail("Grouped completions must await delivery"))
        monkeypatch.setattr(sink, "_bind_event", lambda ev, cfg: ev)
        monkeypatch.setattr(module, "_guid_search", lambda *a: None)
    if provider == "simkl":
        monkeypatch.setattr(module, "_fresh_token", lambda block, *a: block.get("access_token", ""))
    calls = []
    def http(path, body, *args):
        calls.append((path, body))
        return {"ok": len(calls) > 1, "status": 201 if len(calls) > 1 else 503, "resp": {"action": "scrobble"}}
    monkeypatch.setattr(sink, "_send_http", http)
    send = lambda ev: sink.send(ev, cfg=cfg)
    send_grouped(event(), cfg, send)
    assert not calls
    assert not send_grouped(event(3), cfg, send)["ok"]
    assert send_grouped(event(3), cfg, send)["ok"]
    assert send_grouped(event(3), cfg, send)["reason"] == "episode_group_already_completed"
    assert len(calls) == 2
    assert all(path.endswith("/stop") and body["progress"] == 100 for path, body in calls)


def test_floppy_group_completion_does_not_invent_duration():
    from providers.scrobble.floppy.sink import _payload

    ev = event(ids={"tmdb_show": "100"}, progress=100, position_ms=None, duration_ms=None,
               raw={"_cw_episode_group": "finale"})
    body = _payload(ev)
    assert body["completed"] is True
    assert "duration_seconds" not in body and "position_seconds" not in body


def test_pair_reset_clears_scrobble_group_tracking(config_base, setup):
    from cw_platform.local_db.state import clear_pair_state
    from cw_platform.pair_scope import pair_feature_scope

    cfg, _ = setup
    send = lambda ev: {"ok": True}
    send_grouped(event(), cfg, send)
    send_grouped(event(3), cfg, send)
    clear_pair_state(config_base, pair_feature_scope(cfg, cfg["pairs"][0], "history"))
    assert send_grouped(event(3), cfg, send)["reason"] == "episode_group_waiting_for_parts"


def test_explicit_history_block_holds_scrobble_group(config_base, setup):
    from cw_platform.mapping_policy import feature_node

    cfg, _ = setup
    policy = manual_policy.load_policy(config_base)
    feature_node(policy, "PLEX", "default", "history")["blocks"] = ["tmdb:100#s01e02"]
    manual_policy.save_policy(config_base, policy)
    result = send_grouped(event(), cfg, lambda ev: pytest.fail("Blocked group was sent"))
    assert result["reason"] == "episode_group_blocked"


@pytest.mark.parametrize("reason", ["destination_already_watched", "session_completed"])
def test_confirmed_existing_destination_is_not_retried(setup, reason):
    cfg, _ = setup
    calls = []
    send = lambda ev: calls.append(ev) or {"ok": True, "skipped": True, "reason": reason}
    send_grouped(event(), cfg, send)
    assert send_grouped(event(3), cfg, send)["ok"]
    assert send_grouped(event(3), cfg, send)["reason"] == "episode_group_already_completed"
    assert len(calls) == 1


def test_history_unwatched_hold_prevents_scrobble_readdition(config_base, setup):
    from cw_platform.local_db import episode_groups as storage
    from cw_platform.pair_scope import pair_feature_scope
    from providers.scrobble.episode_groups import _hash
    from cw_platform.episode_groups import endpoint, tokens

    cfg, group = setup
    signature = _hash({side: dict(endpoint=endpoint(group[side]), episodes=sorted(
        sorted(tokens(member)) for member in group[side]["episodes"])) for side in ("source", "target")})
    scope = pair_feature_scope(cfg, cfg["pairs"][0], "history")
    storage.save(config_base, scope, {group["id"]: {"mapping": signature, "held": True}})
    result = send_grouped(event(), cfg, lambda ev: pytest.fail("Held group was sent"))
    assert result["reason"] == "episode_group_unwatched_hold"


def test_destination_watched_threshold_controls_each_part(setup):
    cfg, _ = setup
    cfg["scrobble"]["trakt"]["watched_at"] = 98
    calls = []
    send = lambda ev: calls.append(ev) or {"ok": True}
    send_grouped(event(progress=95), cfg, send)
    send_grouped(event(3, progress=100), cfg, send)
    assert not calls
    send_grouped(event(progress=98), cfg, send)
    assert len(calls) == 1
