from __future__ import annotations

from types import SimpleNamespace

import pytest

from cw_platform.log_context import log_run_id
from cw_platform.orchestrator import _applier
from providers.sync import _mod_EMBY as module
from providers.sync.emby import _common as common


class Library:
    def __init__(self, count=2):
        self.rows = [
            {"Id": str(i), "Type": "Movie", "Name": f"Movie {i}", "ProviderIds": {"Tmdb": str(i)}}
            for i in range(1, count + 1)
        ]
        self.calls = []
        self.fail_start = None

    def get(self, path, *, params):
        self.calls.append(dict(params))
        start = params.get("StartIndex", 0)
        rows = self.rows[start:start + params.get("Limit", 500)]
        return SimpleNamespace(
            status_code=503 if start == self.fail_start else 200,
            json=lambda: {"Items": rows, "TotalRecordCount": len(self.rows)},
        )


def adapter(count=2, **config):
    return SimpleNamespace(client=Library(count), cfg=SimpleNamespace(**{
        "server": "http://emby", "user_id": "user", "access_token": "token", **config,
    }))


@pytest.fixture(autouse=True)
def isolated_context(monkeypatch):
    for name in ("CW_PAIR_KEY", "CW_PAIR_SCOPE", "CW_SYNC_PAIR", "CW_PAIR"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(common._RUN_PROVIDER_INDEX, "entry", None, raising=False)
    monkeypatch.setattr(common, "cw_log", lambda *args, **kwargs: None)
    token = log_run_id.set("")
    yield
    log_run_id.reset(token)


def test_real_apply_reuses_index_across_batches_and_refreshes_next_run(monkeypatch):
    monkeypatch.setenv("CW_PAIR_KEY", "pair1")
    adapters = []

    class Adapter:
        def __init__(self, cfg):
            self.cfg = adapter().cfg
            self.client = Library()
            adapters.append(self)

        def add(self, feature, items, *, dry_run):
            common.provider_index(self, feature=feature)
            return {"ok": True, "count": len(items), "confirmed_keys": [common.key_of(it) for it in items]}

    monkeypatch.setattr(module, "EMBYModule", Adapter)
    monkeypatch.setattr(_applier, "cancel_requested", lambda: False)
    for run in ("first", "second"):
        log_run_id.set(run)
        result = _applier.apply_add(
            dst_ops=module._EmbyOPS(), cfg={}, dst_name="EMBY", feature="history",
            items=[{"type": "movie", "ids": {"tmdb": str(i)}} for i in range(1, 4)],
            dry_run=False, emit=lambda *args, **kwargs: None, dbg=lambda *args, **kwargs: None,
            chunk_size=1, chunk_pause_ms=0,
        )
        assert result["confirmed"] == 3
    assert [len(ad.client.calls) for ad in adapters] == [1, 0, 0, 1, 0, 0]


@pytest.mark.parametrize("change", ["server", "user_id", "access_token", "history_libraries", "feature", "pair", "run"])
@pytest.mark.parametrize("reuse_adapter", [False, True])
def test_cache_never_crosses_identity_or_scope(monkeypatch, change, reuse_adapter):
    monkeypatch.setenv("CW_PAIR_KEY", "pair/a")
    log_run_id.set("run1")
    first = adapter(history_libraries=["A"], progress_libraries=["A"])
    index = common.provider_index(first)
    second = first if reuse_adapter else adapter(**vars(first.cfg))
    assert common.provider_index(second) is index
    before = len(second.client.calls)
    feature = "history"
    if change == "pair":
        # These collide after filename sanitization, but must not share a cache.
        monkeypatch.setenv("CW_PAIR_KEY", "pair?a")
    elif change == "run":
        log_run_id.set("run2")
    elif change == "feature":
        feature = "progress"
    else:
        setattr(second.cfg, change, ["B"] if change == "history_libraries" else "changed")
    assert common.provider_index(second, feature=feature) is not index
    assert len(second.client.calls) == before + 1


@pytest.mark.parametrize("scope", [None, "", "unscoped", "default", "none"])
def test_missing_scope_disables_cross_adapter_reuse(monkeypatch, scope):
    log_run_id.set("run1")
    if scope is not None:
        monkeypatch.setenv("CW_PAIR_KEY", scope)
    first, second = adapter(), adapter()
    assert common.provider_index(first) == common.provider_index(second)
    assert len(first.client.calls) == len(second.client.calls) == 1
    assert common._RUN_PROVIDER_INDEX.entry is None


def test_stale_environment_run_does_not_enable_sharing(monkeypatch):
    monkeypatch.setenv("CW_RUN_ID", "finished-run")
    monkeypatch.setenv("CW_PAIR_KEY", "pair1")
    first, second = adapter(), adapter()
    assert common.provider_index(first) == common.provider_index(second)
    assert len(first.client.calls) == len(second.client.calls) == 1


@pytest.mark.parametrize("count", [0, 2])
def test_long_run_reuses_even_empty_index_and_force_refreshes(monkeypatch, count):
    log_run_id.set("run1")
    monkeypatch.setenv("CW_PAIR_KEY", "pair1")
    now = [100.0]
    monkeypatch.setattr(common.time, "monotonic", lambda: now[0])
    first = adapter(count)
    index = common.provider_index(first)
    now[0] += 900
    second = adapter(count)
    assert common.provider_index(second) is index
    assert not second.client.calls
    refreshed = common.provider_index(second, force_refresh=True)
    assert refreshed is not index
    assert common.provider_index(first) is refreshed
    assert len(second.client.calls) == 1


def test_standalone_adapter_retains_ttl(monkeypatch):
    now = [100.0]
    monkeypatch.setattr(common.time, "monotonic", lambda: now[0])
    ad = adapter()
    index = common.provider_index(ad, ttl_sec=10)
    now[0] += 9
    assert common.provider_index(ad, ttl_sec=10) is index
    now[0] += 2
    assert common.provider_index(ad, ttl_sec=10) is not index
    assert len(ad.client.calls) == 2


def test_failed_page_is_not_cached(monkeypatch):
    log_run_id.set("run1")
    monkeypatch.setenv("CW_PAIR_KEY", "pair1")
    failed = adapter(600)
    failed.client.fail_start = 500
    with pytest.raises(RuntimeError, match="emby_provider_index_http_503"):
        common.provider_index(failed)
    assert common._cached_provider_index(failed, "history") is None
    recovered = adapter(600)
    assert len(common.provider_index(recovered)) == 600
    assert len(recovered.client.calls) == 2


def test_resolved_episode_needs_only_provider_id_query():
    ad = adapter(1)
    ad.client.rows = [{"Id": "123", "Type": "Episode", "ProviderIds": {"Tvdb": "456"}, "ParentIndexNumber": 1, "IndexNumber": 2}]
    item = {"type": "episode", "ids": {"tvdb": "456"}, "season": 1, "episode": 2}
    assert common.resolve_item_ids(ad, item, feature="progress") == ["123"]
    assert len(ad.client.calls) == 1
    assert ad.client.calls[0]["AnyProviderIdEquals"] == "tvdb.456"


def test_movie_copies_still_expand_and_reuse_direct_query():
    ad = adapter(2)
    for row in ad.client.rows:
        row["ProviderIds"] = {"Tmdb": "1"}
    for _ in range(2):
        assert common.resolve_item_ids(ad, {"type": "movie", "ids": {"tmdb": "1"}}, feature="progress") == ["1", "2"]
    assert len(ad.client.calls) == 1
    assert ad.client.calls[0]["AnyProviderIdEquals"] == "tmdb.1"
