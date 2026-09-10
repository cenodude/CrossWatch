from __future__ import annotations

from types import SimpleNamespace

import pytest

from cw_platform.orchestrator import _applier
from cw_platform.orchestrator._pairs_oneway import compute_effective_add
from cw_platform.log_context import log_run_id
from providers.sync import _mod_JELLYFIN as module
from providers.sync.jellyfin import _common as common, _history as history, _progress as progress


class Response:
    status_code = 200

    def __init__(self, payload):
        self.payload = payload

    def json(self):
        return self.payload


class Library:
    def __init__(self, count):
        self.rows = [
            {
                "Id": str(i), "Type": "Movie", "Name": f"Movie {i}",
                "ProviderIds": {"Tmdb": str(i)},
                "UserData": {"Played": True, "LastPlayedDate": "2026-09-01T00:00:00Z"},
            }
            for i in range(1, count + 1)
        ]
        self.calls = []

    def get(self, path, *, params):
        assert path == "/Items"
        self.calls.append(dict(params))
        start = int(params.get("StartIndex", 0))
        rows = self.rows[start:start + int(params.get("Limit", 500))]
        # Jellyfin reports a page count when the real total is disabled.
        total = len(rows) if params.get("EnableTotalRecordCount") == "false" else len(self.rows)
        return Response({"Items": rows, "TotalRecordCount": total})


@pytest.fixture
def empty_history_state(monkeypatch):
    monkeypatch.setattr(history, "_history_page_size", lambda adapter: 500)
    monkeypatch.setattr(history, "_shadow_load", lambda: {})
    monkeypatch.setattr(history, "_bb_load", lambda: {})
    for name in ("_shadow_save", "_bb_save", "_thaw_if_present"):
        monkeypatch.setattr(history, name, lambda *args: None)
    monkeypatch.setattr(history, "_unres_flush", lambda: None)
    monkeypatch.setattr(history, "jf_get_library_roots", lambda adapter: {})
    monkeypatch.setattr(history, "jf_resolve_library_id", lambda *args, **kwargs: "")


@pytest.mark.parametrize("count,starts", [(1422, [0, 500, 1000]), (1000, [0, 500, 1000])])
def test_history_reads_all_pages_without_shadow(empty_history_state, count, starts):
    http = Library(count)
    adapter = SimpleNamespace(client=http, cfg=SimpleNamespace(user_id="user"))
    for _ in range(2):
        http.calls.clear()
        snapshot = history.build_index(adapter)
        assert len(snapshot) == count
        assert all(item.get("watched_at") for item in snapshot.values())
        assert [call["StartIndex"] for call in http.calls] == starts


def test_history_explicit_limit_still_applies(empty_history_state):
    http = Library(1422)
    adapter = SimpleNamespace(client=http, cfg=SimpleNamespace(user_id="user"))
    assert len(history.build_index(adapter, limit=600)) == 600
    assert [call["StartIndex"] for call in http.calls] == [0, 500]


@pytest.mark.parametrize("resolved", ["episode-id", None])
def test_progress_episode_does_not_expand_to_library_index(monkeypatch, resolved):
    monkeypatch.setattr(common, "resolve_item_id", lambda *args, **kwargs: resolved)
    monkeypatch.setattr(common, "build_provider_index", lambda *args, **kwargs: pytest.fail("redundant index"))
    adapter = SimpleNamespace(client=Library(0), cfg=SimpleNamespace(user_id="user"))
    assert common.resolve_item_ids(adapter, {"type": "episode"}, feature="progress") == ([resolved] if resolved else [])


def test_progress_movies_still_resolve_multiple_copies(monkeypatch):
    monkeypatch.setattr(common, "resolve_item_id", lambda *args, **kwargs: "copy1")
    monkeypatch.setattr(common, "build_provider_index", lambda *args, **kwargs: {
        "tmdb.1": [{"Id": "copy1", "Type": "Movie"}, {"Id": "copy2", "Type": "Movie"}],
    })
    adapter = SimpleNamespace(client=Library(0), cfg=SimpleNamespace(user_id="user"))
    assert common.resolve_item_ids(adapter, {"type": "movie", "ids": {"tmdb": "1"}}, feature="progress") == ["copy1", "copy2"]


@pytest.mark.parametrize("count", [0, 2])
def test_index_cache_respects_library_scope_including_empty_results(count):
    http = Library(count)
    adapter = SimpleNamespace(client=http, cfg=SimpleNamespace(user_id="user", history_libraries=["A"]))
    common.build_provider_index(adapter)
    common.build_provider_index(adapter)
    assert len(http.calls) == 1
    adapter.cfg.history_libraries = ["B"]
    common.build_provider_index(adapter)
    assert [call["ParentId"] for call in http.calls] == ["A", "B"]


def test_apply_reuses_index_across_batches_but_not_next_run(monkeypatch):
    adapters = []
    monkeypatch.setenv("CW_PAIR_KEY", "pair1")

    class Adapter:
        def __init__(self, cfg):
            self.cfg = SimpleNamespace(server="http://jellyfin", user_id=cfg["user_id"], access_token="token")
            self.client = Library(2)
            adapters.append(self)

        def add(self, feature, items, *, dry_run):
            common.build_provider_index(self, feature=feature)
            return {"ok": True, "count": len(items), "confirmed_keys": [common.key_of(item) for item in items]}

    monkeypatch.setattr(module, "JELLYFINModule", Adapter)
    monkeypatch.setattr(_applier, "cancel_requested", lambda: False)
    ops = module._JellyfinOPS()
    for run in ("first", "second"):
        token = log_run_id.set(run)
        try:
            result = _applier.apply_add(
                dst_ops=ops, cfg={"user_id": "user1"}, dst_name="JELLYFIN", feature="history",
                items=[{"type": "movie", "ids": {"tmdb": str(i)}} for i in range(1, 4)],
                dry_run=False, emit=lambda *args, **kwargs: None, dbg=lambda *args, **kwargs: None,
                chunk_size=1, chunk_pause_ms=0,
            )
            assert result["confirmed"] == 3
        finally:
            log_run_id.reset(token)
    assert len(adapters) == 6
    assert [len(adapter.client.calls) for adapter in adapters] == [1, 0, 0, 1, 0, 0]


@pytest.mark.parametrize("change", ["server", "user_id", "access_token", "history_libraries", "pair", "feature"])
def test_run_index_never_crosses_identity_or_scope(monkeypatch, change):
    token = log_run_id.set("isolation")
    monkeypatch.setattr(common._RUN_PROVIDER_INDEX, "entry", None, raising=False)
    monkeypatch.setenv("CW_PAIR_KEY", "pair1")
    cfg = dict(server="http://jellyfin", user_id="user1", access_token="secret", history_libraries=["A"], progress_libraries=["A"])
    first = SimpleNamespace(client=Library(2), cfg=SimpleNamespace(**cfg))
    first.client.rows[0]["Path"] = "/media/movie.mkv"
    try:
        expected = common.build_provider_index(first)
        same = SimpleNamespace(client=Library(2), cfg=SimpleNamespace(**cfg))
        assert common.build_provider_index(same) is expected
        assert same.client.calls == []
        assert same._path_index_cache is first._path_index_cache
        assert same._path_index_cache
        if change == "pair":
            monkeypatch.setenv("CW_PAIR_KEY", "pair2")
        elif change != "feature":
            cfg[change] = ["B"] if change == "history_libraries" else "changed"
        changed = SimpleNamespace(client=Library(2), cfg=SimpleNamespace(**cfg))
        assert common.build_provider_index(changed, feature="progress" if change == "feature" else "history") is not expected
        assert len(changed.client.calls) == 1
    finally:
        log_run_id.reset(token)


@pytest.mark.parametrize("scope", [None, "", "unscoped", "default", "none"])
def test_missing_pair_scope_disables_cross_adapter_reuse(monkeypatch, scope):
    for name in ("CW_PAIR_KEY", "CW_PAIR_SCOPE", "CW_SYNC_PAIR", "CW_PAIR"):
        monkeypatch.delenv(name, raising=False)
    token = log_run_id.set("active-run")
    cfg = SimpleNamespace(server="http://jellyfin", user_id="user", access_token="token")
    try:
        monkeypatch.setenv("CW_PAIR_KEY", "pair1")
        common.build_provider_index(SimpleNamespace(client=Library(2), cfg=cfg))
        assert common._RUN_PROVIDER_INDEX.entry is not None
        monkeypatch.delenv("CW_PAIR_KEY")
        if scope is not None:
            monkeypatch.setenv("CW_PAIR_KEY", scope)
        for _ in range(2):
            adapter = SimpleNamespace(client=Library(2), cfg=cfg)
            common.build_provider_index(adapter)
            assert len(adapter.client.calls) == 1
        assert common._RUN_PROVIDER_INDEX.entry is None
    finally:
        log_run_id.reset(token)


def test_stale_environment_run_id_does_not_enable_sharing(monkeypatch):
    monkeypatch.setenv("CW_RUN_ID", "finished-run")
    token = log_run_id.set("")
    try:
        for _ in range(2):
            adapter = SimpleNamespace(client=Library(2), cfg=SimpleNamespace(server="http://jellyfin", user_id="user"))
            common.build_provider_index(adapter)
            assert len(adapter.client.calls) == 1
        assert common._RUN_PROVIDER_INDEX.entry is None
    finally:
        log_run_id.reset(token)


def test_failed_index_page_is_not_shared_with_next_batch(monkeypatch):
    class FailingLibrary(Library):
        def get(self, path, *, params):
            response = super().get(path, params=params)
            if params["StartIndex"] == 500:
                response.status_code = 503
            return response

    token = log_run_id.set("failed-index-run")
    monkeypatch.setenv("CW_PAIR_KEY", "pair1")
    monkeypatch.setattr(common._RUN_PROVIDER_INDEX, "entry", None, raising=False)
    cfg = SimpleNamespace(server="http://jellyfin", user_id="user", access_token="token")
    try:
        failed = SimpleNamespace(client=FailingLibrary(600), cfg=cfg)
        with pytest.raises(RuntimeError, match="jellyfin_provider_index_http_503"):
            common.build_provider_index(failed)
        assert common._RUN_PROVIDER_INDEX.entry is None
        recovered = SimpleNamespace(client=Library(600), cfg=cfg)
        assert len(common.build_provider_index(recovered)) == 600
        assert [call["StartIndex"] for call in recovered.client.calls] == [0, 500]
    finally:
        log_run_id.reset(token)


@pytest.mark.parametrize("write_last", [False, True])
def test_history_existing_items_count_as_skips_and_remain_successful(monkeypatch, empty_history_state, write_last):
    timestamp = history._parse_iso_to_epoch("2026-09-01T00:00:00Z")
    items = [
        {"type": "movie", "ids": {"tmdb": str(i)}, "watched_at": "2026-09-01T00:00:00Z"}
        for i in range(1, 4)
    ]
    monkeypatch.setattr(history, "_try_resolve_iid", lambda adapter, item: item["ids"]["tmdb"])
    monkeypatch.setattr(history, "_dst_user_states", lambda *args: {
        "1": (True, timestamp), "2": (True, timestamp + 3600), "3": (not write_last, timestamp),
    })
    writes = []
    monkeypatch.setattr(history, "_mark_played", lambda http, uid, iid, **kwargs: writes.append(iid) or True)
    monkeypatch.setattr(history, "_normalize_watched", lambda *args, **kwargs: True)
    adapter = SimpleNamespace(client=object(), cfg=SimpleNamespace(user_id="user"))
    count, unresolved = history.add(adapter, items)
    raw = module._finalize_result(adapter, common.key_of, "history", items, count, unresolved)
    result = _applier._normalize(raw, items, "apply:add", dst="JELLYFIN", feature="history", emit=lambda *args, **kwargs: None)
    assert writes == (["3"] if write_last else [])
    assert result["confirmed"] == int(write_last)
    assert result["skipped"] == 3 - int(write_last)
    assert result["unresolved"] == 0
    outcome = compute_effective_add(
        attempted_keys=[common.key_of(item) for item in items], prov_confirmed=result["confirmed"],
        confirmed_keys=result["confirmed_keys"], still_unresolved=set(), skipped_keys=result["skipped_keys"],
        have_exact_keys=True, verify_after_write=False, provider_skipped=True,
    )
    assert outcome["effective"] == int(write_last)
    assert len(outcome["success_keys"]) == 3
    assert outcome["failed_keys"] == []


def test_progress_newer_target_is_reported_as_skipped(monkeypatch):
    item = {"type": "movie", "ids": {"tmdb": "1"}, "progress_ms": 1000, "progress_at": "2026-09-01T00:00:00Z"}
    monkeypatch.setattr(progress, "_active_item_ids", lambda *args: set())
    monkeypatch.setattr(progress, "resolve_item_ids", lambda *args, **kwargs: ["movie"])
    monkeypatch.setattr(progress, "_target_state", lambda *args: {"timestamp": "2026-09-02T00:00:00Z", "progress_ms": 2000, "duration_ms": 600000})
    adapter = SimpleNamespace(client=object(), cfg=SimpleNamespace(user_id="user"))
    count, unresolved = progress.add(adapter, [item])
    raw = module._finalize_result(adapter, common.key_of, "progress", [item], count, unresolved)
    assert raw["count"] == 0
    assert raw["confirmed_keys"] == []
    assert raw["skipped_keys"] == ["tmdb:1"]
    assert raw["results"][0]["reason"] == "target_newer"
