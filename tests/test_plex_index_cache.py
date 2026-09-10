from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from types import SimpleNamespace
from xml.etree import ElementTree as ET

import pytest

from cw_platform.log_context import log_run_id
from providers.sync.plex import _common as common, _history as history


def adapter(*, pair="cw2_pair_a", token="token", user="viewer", base="http://plex", movie="11"):
    server = SimpleNamespace(machineIdentifier="server", _token=token, url=lambda path: base + path, movie=movie)
    return SimpleNamespace(
        client=SimpleNamespace(server=server),
        config={"_cw_pair_scope": pair, "plex": {"username": user}},
        libraries=lambda **kwargs: [SimpleNamespace(key="1", type="movie")],
    )


@pytest.fixture(autouse=True)
def context(monkeypatch):
    for name in ("CW_PAIR_KEY", "CW_PAIR_SCOPE", "CW_SYNC_PAIR", "CW_PAIR"):
        monkeypatch.delenv(name, raising=False)
    token = log_run_id.set("run1")
    history._clear_guid_index()
    monkeypatch.setattr(history._INDEX_CACHE, "catalog", None, raising=False)
    monkeypatch.setattr(history, "_dbg", lambda *args, **kwargs: None)
    yield
    log_run_id.reset(token)
    history._clear_guid_index()


@pytest.fixture
def fetches(monkeypatch):
    calls = []

    def fetch(server, section, kind):
        calls.append((server.movie, section))
        rows = [{"ratingKey": server.movie, "Guid": [{"id": "tmdb://1"}]}] if server.movie else []
        return rows, 1

    monkeypatch.setattr(history, "_fetch_section_guid_rows", fetch)
    return calls


@pytest.mark.parametrize("change", ["pair", "token", "user", "base", "machine", "libraries", "feature", "run"])
def test_guid_index_isolated_across_batches_and_contexts(fetches, change):
    first = adapter()
    expected = history._build_guid_index(first, {"1"})
    assert history._build_guid_index(adapter(), {"1"}) is expected
    assert len(fetches) == 1
    config = {change: "different"} if change in {"pair", "token", "user", "base"} else {}
    second = adapter(**config)
    if change == "machine":
        second.client.server.machineIdentifier = "other-server"
    if change == "run":
        log_run_id.set("run2")
    allowed = {"1", "2"} if change == "libraries" else {"1"}
    feature = "progress" if change == "feature" else "history"
    assert history._build_guid_index(second, allowed, feature=feature) is not expected
    assert len(fetches) == 2


@pytest.mark.parametrize("pair", [None, "", "unscoped", "default", "none"])
def test_missing_pair_scope_prevents_cross_adapter_reuse(fetches, pair):
    first, second = adapter(pair=pair), adapter(pair=pair)
    history._build_guid_index(first, {"1"})
    history._build_guid_index(first, {"1"})
    history._build_guid_index(second, {"1"})
    assert len(fetches) == 2


def test_environment_run_id_does_not_enable_sharing(fetches, monkeypatch):
    log_run_id.set("")
    monkeypatch.setenv("CW_RUN_ID", "finished-run")
    history._build_guid_index(adapter(), {"1"})
    history._build_guid_index(adapter(), {"1"})
    assert len(fetches) == 2


def test_new_run_sees_library_changes_without_loading_old_disk_index(fetches, monkeypatch):
    monkeypatch.setattr(history, "read_json", lambda *args: pytest.fail("persisted GUID cache must not be loaded"))
    first = adapter()
    history._build_guid_index(first, {"1"})
    assert history._pms_find_in_guid_index("movie", ["tmdb://1"]) == "11"
    first.client.server.movie = "22"
    log_run_id.set("run2")
    history._build_guid_index(first, {"1"})
    assert history._pms_find_in_guid_index("movie", ["tmdb://1"]) == "22"
    assert len(fetches) == 2


@pytest.mark.parametrize("movie", ["11", ""])
def test_long_run_reuses_index_including_empty_results(fetches, monkeypatch, movie):
    now = [100.0]
    monkeypatch.setattr(history.time, "monotonic", lambda: now[0])
    first = history._build_guid_index(adapter(movie=movie), {"1"})
    now[0] += 900
    assert history._build_guid_index(adapter(movie=movie), {"1"}) is first
    assert len(fetches) == 1


def test_standalone_ttl_and_force_refresh(fetches, monkeypatch):
    log_run_id.set("")
    now = [100.0]
    monkeypatch.setattr(history.time, "monotonic", lambda: now[0])
    ad = adapter()
    first = history._build_guid_index(ad, {"1"})
    now[0] += 299
    assert history._build_guid_index(ad, {"1"}) is first
    now[0] += 2
    second = history._build_guid_index(ad, {"1"})
    assert second is not first
    assert history._build_guid_index(ad, {"1"}, force=True) is not second
    assert len(fetches) == 3


def test_threads_do_not_overwrite_each_others_index(fetches):
    barrier = Barrier(2)

    def run(pair, movie):
        log_run_id.set("parallel-run")
        ad = adapter(pair=pair, movie=movie)
        history._build_guid_index(ad, {"1"})
        barrier.wait(timeout=5)
        return history._pms_find_in_guid_index("movie", ["tmdb://1"])

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(run, "pair_a", "11")
        second = pool.submit(run, "pair_b", "22")
        assert (first.result(), second.result()) == ("11", "22")


@pytest.mark.parametrize("change", ["pair", "token", "user", "base", "run"])
def test_history_catalog_respects_scope(monkeypatch, change):
    calls = []

    def build(*args, **kwargs):
        calls.append(1)
        return history.HistoryCatalog()

    monkeypatch.setattr(history, "_build_history_catalog", build)
    first = history._get_history_catalog(adapter(), {"1"})
    assert history._get_history_catalog(adapter(), {"1"}) is first
    if change == "run":
        log_run_id.set("run2")
    second = adapter(**({change: "different"} if change != "run" else {}))
    assert history._get_history_catalog(second, {"1"}) is not first
    assert len(calls) == 2


def test_history_catalog_keeps_live_state_ttl(monkeypatch):
    now = [100.0]
    monkeypatch.setattr(history.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(history, "_build_history_catalog", lambda *args, **kwargs: history.HistoryCatalog())
    ad = adapter()
    first = history._get_history_catalog(ad, {"1"})
    now[0] += 91
    second = history._get_history_catalog(ad, {"1"})
    assert second is not first
    assert history._get_history_catalog(ad, {"1"}, force=True) is not second


def test_failed_page_never_leaves_partial_index(monkeypatch):
    calls = []

    def get(url, **kwargs):
        start = kwargs["params"]["X-Plex-Container-Start"]
        calls.append(start)
        rows = [{"ratingKey": str(i), "Guid": [{"id": f"tmdb://{i}"}]} for i in range(1000)]
        return SimpleNamespace(ok=start == 0, status_code=200 if start == 0 else 503,
                               headers={"content-type": "application/json"},
                               json=lambda: {"MediaContainer": {"Metadata": rows, "totalSize": 1200}})

    ad = adapter()
    ad.client.server._session = SimpleNamespace(headers={}, get=get)
    with pytest.raises(RuntimeError, match="plex_guid_index_http_503"):
        history._build_guid_index(ad, {"1"})
    assert calls == [0, 1000]
    assert history._cached_guid_index(ad, {"1"}) is None
    assert history._pms_find_in_guid_index("movie", ["tmdb://1"]) is None


def test_empty_guid_query_is_not_repeated_through_raw_http():
    calls = []

    def query(path):
        calls.append(path)
        return ET.fromstring('<MediaContainer size="0"/>')

    server = SimpleNamespace(query=query, _token="token", url=lambda path: "http://plex" + path,
                             _session=SimpleNamespace(headers={}, get=lambda *args, **kwargs: pytest.fail("duplicate query")))
    assert common.server_find_rating_key_by_guid(server, iter(["tmdb://1", "tmdb://1"])) is None
    assert len(calls) == 1


def test_short_guid_page_with_remaining_total_keeps_paging():
    starts = []
    rows = [{"ratingKey": str(i), "Guid": [{"id": f"tmdb://{i}"}]} for i in range(3)]

    def get(url, **kwargs):
        start = kwargs["params"]["X-Plex-Container-Start"]
        starts.append(start)
        return SimpleNamespace(ok=True, headers={"content-type": "application/json"},
                               json=lambda: {"MediaContainer": {"Metadata": rows[start:start + 2], "totalSize": 3}})

    ad = adapter()
    ad.client.server._session = SimpleNamespace(headers={}, get=get)
    index = history._build_guid_index(ad, {"1"})
    assert len(index["movies"]) == 3
    assert starts == [0, 2]


def test_failed_guid_query_still_uses_raw_http_fallback():
    calls = []

    def query(path):
        raise RuntimeError("PlexAPI request failed")

    def get(*args, **kwargs):
        calls.append(kwargs["params"]["guid"])
        return SimpleNamespace(ok=True, headers={"Content-Type": "application/json"},
                               json=lambda: {"MediaContainer": {"Metadata": [{"ratingKey": "11"}]}})

    server = SimpleNamespace(query=query, _token="token", url=lambda path: "http://plex" + path,
                             _session=SimpleNamespace(headers={}, get=get))
    assert common.server_find_rating_key_by_guid(server, ["tmdb://1"]) == "11"
    assert calls == ["tmdb://1"]
