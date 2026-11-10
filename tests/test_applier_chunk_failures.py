# tests/test_applier_chunk_failures.py
# CrossWatch - A failing write chunk does not stop the remaining chunks.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
import requests

from cw_platform.id_map import canonical_key
from cw_platform.orchestrator import _applier, _unresolved
from cw_platform.orchestrator._pairs_oneway import compute_effective_add


@pytest.fixture(autouse=True)
def _isolated(config_base, monkeypatch):
    monkeypatch.setattr("time.sleep", lambda *_a, **_k: None)
    monkeypatch.setattr(_unresolved, "STATE_DIR", config_base / ".cw_state")
    return config_base


def _run(call, items, events, tag="apply:add"):
    return _applier._apply_chunked(tag, dst="DEST", feature="watchlist", items=items, call=call,
                                   emit=lambda ev, **kw: events.append((ev, kw)), dbg=lambda *a, **k: None,
                                   chunk_size=2, chunk_pause_ms=0)


def _items(n: int) -> list[dict[str, Any]]:
    return [{"type": "movie", "ids": {"tmdb": str(i)}, "title": f"Movie {i}"} for i in range(1, n + 1)]


def _keys(chunk) -> list[str]:
    return [f"tmdb:{item['ids']['tmdb']}" for item in chunk]


def _failing_on(key, exact):
    sent: list[list[str]] = []

    def call(chunk):
        sent.append(_keys(chunk))
        if key in _keys(chunk):
            raise ValueError("bad item")
        res = {"ok": True, "count": len(chunk)}
        return {**res, "confirmed_keys": _keys(chunk)} if exact else res
    return call, sent


def test_failed_chunk_is_reported_and_later_chunks_are_still_written():
    call, sent = _failing_on("tmdb:3", exact=True)
    events: list[tuple[str, dict]] = []

    res = _run(call, _items(6), events)

    assert sent == [["tmdb:1", "tmdb:2"]] + [["tmdb:3", "tmdb:4"]] * 3 + [["tmdb:5", "tmdb:6"]]
    assert res["confirmed_keys"] == ["tmdb:1", "tmdb:2", "tmdb:5", "tmdb:6"]
    assert res["unresolved_keys"] == ["tmdb:3", "tmdb:4"]
    assert res["attempted"] == 6 and res["confirmed"] == 4 and res["unresolved"] == 2 and res["errors"] == 1 and res["ok"] is False
    failed = [kw for ev, kw in events if ev == "apply:add:chunk_failed"]
    assert failed == [{"dst": "DEST", "feature": "watchlist", "done": 2, "total": 6, "count": 2, "error": "ValueError: bad item"}]
    unresolved = [kw for ev, kw in events if ev == "apply:unresolved"]
    assert [row["reason"] for row in unresolved[0]["items"]] == ["apply:add:failed", "apply:add:failed"]


@pytest.mark.parametrize("verify", [True, False])
def test_count_only_results_never_confirm_the_failed_items(verify):
    call, _ = _failing_on("tmdb:3", exact=False)

    res = _run(call, _items(6), [])

    attempted = _keys(_items(6))
    still_unresolved = set(res["unresolved_keys"])
    decision = compute_effective_add(attempted_keys=attempted, prov_confirmed=res["confirmed"],
                                     confirmed_keys=[k for k in attempted if k not in still_unresolved],
                                     still_unresolved=still_unresolved, skipped_keys=set(), have_exact_keys=False,
                                     verify_after_write=verify, provider_skipped=False)
    assert decision["failed_keys"] == ["tmdb:3", "tmdb:4"]
    assert decision["ambiguous_partial"] is False
    if verify:
        assert decision["success_keys"] == ["tmdb:1", "tmdb:2", "tmdb:5", "tmdb:6"]
    else:
        assert decision["success_keys"] == [] and decision["effective"] == 0


def test_count_only_unconfirmed_items_are_not_failed_attempts():
    decision = compute_effective_add(attempted_keys=["a", "b", "c"], prov_confirmed=2, confirmed_keys=["a", "c"],
                                     still_unresolved={"b"}, skipped_keys=set(), have_exact_keys=False,
                                     verify_after_write=False, provider_skipped=False)
    assert decision["success_keys"] == [] and decision["failed_keys"] == ["b"]


@pytest.mark.parametrize("tag", ["apply:remove", "apply:update"])
def test_removes_and_updates_report_the_failed_items(tag):
    call, _ = _failing_on("tmdb:3", exact=True)

    res = _run(call, _items(6), [], tag=tag)

    assert res["confirmed_keys"] == ["tmdb:1", "tmdb:2", "tmdb:5", "tmdb:6"]
    assert res["unresolved_keys"] == ["tmdb:3", "tmdb:4"]


@pytest.mark.parametrize("tag", ["apply:add", "apply:update", "apply:remove"])
def test_failed_chunk_is_not_saved_as_unresolved(tag):
    from cw_platform.orchestrator._unresolved import load_unresolved_keys

    call, _ = _failing_on("tmdb:3", exact=True)
    _run(call, _items(6), [], tag=tag)
    assert not set(load_unresolved_keys("DEST", "watchlist") or [])

    retry = lambda chunk: {"ok": True, "count": len(chunk), "confirmed_keys": _keys(chunk)}
    res = _run(retry, _items(6), [], tag=tag)
    assert res["confirmed"] == 6
    assert not set(load_unresolved_keys("DEST", "watchlist") or [])


def test_provider_reported_unresolved_items_are_still_saved():
    from cw_platform.orchestrator._unresolved import load_unresolved_keys

    def call(chunk):
        keys = _keys(chunk)
        return {"ok": True, "count": len(chunk) - 1, "confirmed_keys": keys[:-1],
                "unresolved": [{"item": dict(chunk[-1]), "hint": "not_found"}]}

    _run(call, _items(2), [])
    assert set(load_unresolved_keys("DEST", "watchlist") or []) == {"tmdb:2"}


def test_finalizer_only_receives_written_chunks():
    call, _ = _failing_on("tmdb:3", exact=True)
    finalized: list[list[dict]] = []

    res = _applier._apply_chunked("apply:add", dst="DEST", feature="history", items=_items(6), call=call,
                                  emit=lambda *a, **k: None, dbg=lambda *a, **k: None, chunk_size=2, chunk_pause_ms=0,
                                  finalize=finalized.append)

    assert [raw["confirmed_keys"] for raw in finalized[0]] == [["tmdb:1", "tmdb:2"], ["tmdb:5", "tmdb:6"]]
    assert res["confirmed"] == 4 and res["unresolved_keys"] == ["tmdb:3", "tmdb:4"]


def test_cancel_after_a_failed_chunk_keeps_the_counts(monkeypatch):
    state = {"cancel": False}
    monkeypatch.setattr(_applier, "cancel_requested", lambda: state["cancel"])

    def call(chunk):
        if "tmdb:3" in _keys(chunk):
            state["cancel"] = True
            raise ValueError("bad item")
        return {"ok": True, "count": len(chunk), "confirmed_keys": _keys(chunk)}

    res = _run(call, _items(6), [])

    assert res["cancelled"] is True
    assert res["confirmed_keys"] == ["tmdb:1", "tmdb:2"] and res["unresolved_keys"] == ["tmdb:3", "tmdb:4"]


def _http_error(status: int) -> requests.HTTPError:
    return requests.HTTPError(f"{status} Server Error", response=SimpleNamespace(status_code=status))


def _wrapped_connection_error() -> RuntimeError:
    try:
        try:
            raise requests.ConnectionError("refused")
        except requests.ConnectionError as exc:
            raise RuntimeError("write failed") from exc
    except RuntimeError as outer:
        return outer


@pytest.mark.parametrize("error", [
    ConnectionError("refused"),
    TimeoutError("slow"),
    requests.ConnectionError("refused"),
    requests.Timeout("slow"),
    _http_error(401),
    _http_error(503),
    _wrapped_connection_error(),
    RuntimeError("scrob_status_write_http_401"),
    RuntimeError("HTTP 429 Too Many Requests"),
    RuntimeError("401 Client Error: Unauthorized for url: https://api.example/sync"),
    RuntimeError("request failed with status=403"),
])
def test_provider_down_still_stops_the_write(error):
    assert _applier._provider_down(error) is True

    def call(chunk):
        if "tmdb:3" in _keys(chunk):
            raise error
        return {"ok": True, "count": len(chunk), "confirmed_keys": _keys(chunk)}

    with pytest.raises(type(error)):
        _run(call, _items(6), [])


@pytest.mark.parametrize("message", ["bad item tmdb:401", "invalid episode 403", "GET https://api.example/list/429 failed", "tt0401792", "HTTP 500"])
def test_item_identifiers_are_not_mistaken_for_provider_status(message):
    assert _applier._provider_down(ValueError(message)) is False


def test_too_many_failed_chunks_in_a_row_stops_the_write():
    sent: list[list[str]] = []

    def call(chunk):
        sent.append(_keys(chunk))
        raise ValueError("bad")

    with pytest.raises(ValueError):
        _run(call, _items(10), [])
    assert len(sent) == 3 * _applier._MAX_FAILED_CHUNKS


def test_a_successful_chunk_resets_the_failure_count():
    def call(chunk):
        if int(chunk[0]["ids"]["tmdb"]) % 4 == 1:
            raise ValueError("bad")
        return {"ok": True, "count": len(chunk), "confirmed_keys": _keys(chunk)}

    res = _run(call, _items(12), [])

    assert res["errors"] == 3 and res["unresolved"] == 6 and res["confirmed"] == 6


class _HistoryOps:
    def __init__(self, raise_on=(), report=()) -> None:
        self.raise_on = set(raise_on)
        self.report = set(report)
        self.added: list[str] = []
        self.sent: list[str] = []

    def add(self, _cfg, items, *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        rows = list(items)
        keys = [canonical_key(item) for item in rows]
        self.sent.extend(keys)
        if self.raise_on & set(keys):
            raise ValueError("bad item")
        ok = [key for key in keys if key not in self.report]
        self.added.extend(ok)
        unresolved = [{"item": dict(item), "reason": "not_found", "key": key} for key, item in zip(keys, rows) if key in self.report]
        return {"ok": True, "count": len(ok), "confirmed_keys": ok, "unresolved": unresolved}

    def remove(self, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        return {"ok": True, "count": 0}

    def capabilities(self) -> dict[str, Any]:
        return {"history": {"observed_deletes": False}}


class _StateStore:
    def load_state(self) -> dict[str, Any]:
        return {}

    def save_state(self, _value: dict[str, Any]) -> None:
        return None

    def load_tomb(self) -> dict[str, Any]:
        return {}

    def save_tomb(self, _value: dict[str, Any]) -> None:
        return None


def _stored_keys(pattern: str) -> set[str]:
    import json

    keys: set[str] = set()
    for path in _unresolved.STATE_DIR.glob(pattern):
        data = json.loads(path.read_text("utf-8"))
        keys |= set(data.get("keys") or []) if isinstance(data.get("keys"), list) else set(data)
    return keys


_ROWS = {f"tmdb:{n}": {"type": "movie", "title": f"Movie {n}", "year": 2000, "ids": {"tmdb": str(n)}, "watched_at": f"2026-01-0{n}T20:00:00Z"}
         for n in range(1, 7)}


def _pair_runner(monkeypatch, mode, *, verify, real_blackbox):
    from cw_platform.orchestrator import _blackbox, _pairs_oneway as oneway, _pairs_twoway as twoway

    module = oneway if mode == "one-way" else twoway
    monkeypatch.setattr(_blackbox, "STATE_DIR", _unresolved.STATE_DIR)
    calls: dict[str, list[list[str]]] = {"attempts": [], "successes": []}
    hooks = [
        ("_supports_feature", lambda _ops, _feature: True),
        ("_health_feature_ok", lambda _health, _feature: True),
        ("_health_status", lambda _health: "up"),
        ("_resolve_flags", lambda _fcfg, _sync: {"allow_adds": True, "allow_removals": False}),
        ("_anime_pair_feature_options", lambda *_args, **_kwargs: {"use_anime_mapping": False}),
        ("_anime_config_with_pair_feature_options", lambda cfg, _opts: cfg),
        ("_index_semantics", lambda *_args, **_kwargs: "full"),
        ("prev_checkpoint", lambda *_args, **_kwargs: None),
        ("module_checkpoint", lambda *_args, **_kwargs: None),
        ("keys_for_feature", lambda *_args, **_kwargs: {}),
        ("_manual_policy", lambda *_args, **_kwargs: ([], set())),
        ("_provider_ignore_dropped_enabled", lambda *_args, **_kwargs: False),
        ("apply_blocklist", lambda _state, items, **_kwargs: list(items)),
        ("_maybe_block_massdelete", lambda items, **_kwargs: list(items)),
        ("effective_chunk_size", lambda *_args, **_kwargs: 2),
        ("_apply_verify_after_write_supported", lambda _ops: True),
    ]
    if not real_blackbox:
        hooks += [
            ("load_blackbox_keys", lambda *_args, **_kwargs: set()),
            ("record_attempts", lambda _dst, _feature, keys, **_kwargs: calls["attempts"].append(sorted(keys)) or {"ok": True, "promoted_keys": []}),
            ("record_success", lambda _dst, _feature, keys, **_kwargs: calls["successes"].append(sorted(keys)) or {"ok": True}),
        ]
    for name, val in hooks:
        if hasattr(module, name):
            monkeypatch.setattr(module, name, val)

    written: dict[str, Any] = {}
    source, target = ("DEST", "SRC") if mode == "two-way to A" else ("SRC", "DEST")
    monkeypatch.setattr(module, "build_snapshots_for_feature", lambda **_kwargs: {source: dict(_ROWS), target: dict(written)})

    def run(ops):
        ctx = SimpleNamespace(
            config={"sync": {"include_observed_deletes": False, "verify_after_write": verify,
                             "blackbox": {"enabled": real_blackbox, "promote_after": 3, "cooldown_days": 30}}, "runtime": {}},
            providers={source: _HistoryOps(), target: ops},
            emit=lambda *_args, **_kwargs: None, emit_info=lambda *_args, **_kwargs: None, dbg=lambda *_args, **_kwargs: None,
            dry_run=False, snap_cache={}, snap_ttl_sec=0, state_store=_StateStore(), stats_manual_blocked=0, apply_chunk_pause_ms=0,
        )
        if mode == "one-way":
            oneway.run_one_way_feature(ctx, "SRC", "DEST", feature="history", fcfg={}, health_map={})
        else:
            twoway._two_way_sync(ctx, "SRC", "DEST", feature="history", fcfg={}, health_map={})
        written.update({key: _ROWS[key] for key in ops.added if key in _ROWS})
        return ops

    return run, calls, target


@pytest.mark.parametrize("mode", ["one-way", "two-way to B", "two-way to A"])
def test_exact_confirmed_retry_clears_earlier_failures_with_verification(monkeypatch, mode):
    run, calls, target = _pair_runner(monkeypatch, mode, verify=True, real_blackbox=False)

    first = run(_HistoryOps(report={"tmdb:3", "tmdb:4"}))
    assert sorted(first.added) == ["tmdb:1", "tmdb:2", "tmdb:5", "tmdb:6"]
    assert calls["attempts"] == [["tmdb:3", "tmdb:4"]]
    assert {"tmdb:3", "tmdb:4"} <= _stored_keys(f"{target.lower()}_history.*unresolved*.json")

    calls["attempts"].clear()
    calls["successes"].clear()
    second = run(_HistoryOps())
    assert sorted(second.added) == ["tmdb:3", "tmdb:4"]
    assert calls["successes"] == [["tmdb:3", "tmdb:4"]] and calls["attempts"] == []
    assert not {"tmdb:3", "tmdb:4"} & _stored_keys(f"{target.lower()}_history.*unresolved*.json")


@pytest.mark.parametrize("mode", ["one-way", "two-way to B", "two-way to A"])
def test_failed_chunks_never_blackbox_their_neighbors(monkeypatch, mode):
    run, _calls, target = _pair_runner(monkeypatch, mode, verify=False, real_blackbox=True)
    stuck = ["tmdb:1", "tmdb:2", "tmdb:3", "tmdb:4"]

    first = run(_HistoryOps(raise_on={"tmdb:1", "tmdb:3"}))
    assert sorted(first.added) == ["tmdb:5", "tmdb:6"]
    for _ in range(3):
        retry = run(_HistoryOps(raise_on={"tmdb:1", "tmdb:3"}))
        assert sorted(set(retry.sent)) == stuck and retry.added == []

    assert not _stored_keys(f"{target.lower()}_history.*blackbox.json")
    assert not set(stuck) & _stored_keys(f"{target.lower()}_history.*unresolved*.json")
    fixed = run(_HistoryOps())
    assert sorted(fixed.added) == stuck


@pytest.mark.parametrize("mode", ["one-way", "two-way to B", "two-way to A"])
def test_cancelled_writes_never_blackbox_unsent_items(monkeypatch, mode):
    from cw_platform.orchestrator import _pairs_oneway, _pairs_twoway

    run, _calls, target = _pair_runner(monkeypatch, mode, verify=False, real_blackbox=True)
    module = _pairs_oneway if mode == "one-way" else _pairs_twoway
    source = "DEST" if mode == "two-way to A" else "SRC"
    cancel = {"on": False}
    monkeypatch.setattr(_applier, "cancel_requested", lambda *_a, **_k: cancel["on"])
    written: dict[str, Any] = {}

    def movie(n):
        return {"type": "movie", "title": f"Movie {n}", "year": 2000, "ids": {"tmdb": str(n)}, "watched_at": "2026-02-01T20:00:00Z"}

    class _CancellingOps(_HistoryOps):
        def add(self, cfg, items, *, feature, dry_run=False):
            if "tmdb:3" in [canonical_key(item) for item in items]:
                cancel["on"] = True
            return super().add(cfg, items, feature=feature, dry_run=dry_run)

    for attempt in range(4):
        fresh = {f"tmdb:{n}": movie(n) for n in (100 + 2 * attempt, 101 + 2 * attempt)}
        rows = {**fresh, **{key: _ROWS[key] for key in ("tmdb:3", "tmdb:4", "tmdb:5", "tmdb:6")}}
        other = target
        monkeypatch.setattr(module, "build_snapshots_for_feature",
                            lambda rows=rows, **_kwargs: {source: dict(rows), other: dict(written)})
        cancel["on"] = False
        ops = _CancellingOps(raise_on={"tmdb:3"})
        run(ops)
        written.update({key: rows[key] for key in ops.added})
        assert sorted(ops.added) == sorted(fresh)
        assert "tmdb:5" not in ops.sent and "tmdb:6" not in ops.sent

    assert not _stored_keys(f"{target.lower()}_history.*blackbox.json")
