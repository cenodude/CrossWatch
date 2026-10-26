# tests/test_plex_cloud_history.py
# Plex account history reading, pagination and merge regression tests.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from types import SimpleNamespace as NS

import pytest

from providers.sync.plex import _cloud_history as cloud
from providers.sync.plex._discover_history import CloudError


@pytest.fixture
def remote(monkeypatch):
    movie = {"ratingKey":"movie", "type":"movie", "title":"Example", "Guid":[{"id":"tmdb://1"}]}
    episode = {"ratingKey":"episode", "type":"episode", "title":"Pilot", "grandparentTitle":"Show",
               "grandparentRatingKey":"show", "parentIndex":1, "index":2, "Guid":[{"id":"tvdb://3"}]}
    show = {"ratingKey":"show", "type":"show", "Guid":[{"id":"tmdb://2"}]}
    node = {"id":"play1", "date":"2020-01-01T00:00:00Z", "metadataItem":{"id":"movie"}}
    state = NS(pages=[{"nodes":[node], "pageInfo":{"hasNextPage":False}}], metadata={"movie":movie,"episode":episode,"show":show},
               account={"id":1000,"uuid":"uuid","username":"owner"}, calls=[], closed=False)
    class Reader:
        def __init__(self, adapter):
            self.token = "token"
        def _request(self, method, url):
            assert method == "GET"
            return state.account
        def _gql(self, query, variables):
            assert query.startswith("query ")
            state.calls.append(("page", variables))
            page = state.pages.pop(0)
            if isinstance(page, Exception):
                raise page
            return {"user":{"watchHistory":page}}
        def _metadata(self, key, **kw):
            state.calls.append(("metadata", key))
            row = state.metadata.get(key)
            if row is None:
                raise CloudError("plex_discover_endpoint_unavailable")
            if isinstance(row, Exception):
                raise row
            return {"Metadata":[row]}
        def close(self):
            state.closed = True
    monkeypatch.setattr(cloud, "Writer", Reader)
    monkeypatch.setattr(cloud, "cancel_requested", lambda: False)
    state.adapter = NS(config={"plex":{"history":{"read_cloud_history":True}}}, client=NS(user_username="owner"))
    return state


def test_read_preserves_dates_and_resolves_episode_show_ids(remote):
    remote.pages[0]["nodes"].append({"id":"play2", "date":"2021-02-03T04:05:06Z", "metadataItem":{"id":"episode"}})
    rows, unmatched = cloud.read(remote.adapter)
    assert unmatched == 0
    assert len(rows) == 2
    assert rows[1]["show_ids"] == {"tmdb":"2"}
    assert rows[1]["season"] == 1 and rows[1]["episode"] == 2
    assert rows[1]["watched_at"] == "2021-02-03T04:05:06+00:00"
    assert all(row["_plex_cloud_history"] and "plex" not in row["ids"] for row in rows)
    assert remote.closed


def test_pagination_keeps_rewatches_and_reuses_metadata(remote):
    first = remote.pages[0]
    first["pageInfo"] = {"hasNextPage":True,"endCursor":"next"}
    remote.pages.append({"nodes":[dict(first["nodes"][0], id="play2", date="2021-01-01T00:00:00Z")], "pageInfo":{"hasNextPage":False}})
    rows, _ = cloud.read(remote.adapter)
    assert len(rows) == 2
    assert [c for c in remote.calls if c[0] == "metadata"] == [("metadata","movie")]
    assert [c for c in remote.calls if c[0] == "page"][1][1]["after"] == "next"


@pytest.mark.parametrize("failure", ["page", "cursor", "metadata"])
def test_failed_read_never_returns_partial_history(remote, failure):
    if failure == "metadata":
        remote.metadata["movie"] = CloudError("plex_discover_rate_limited")
    else:
        remote.pages[0]["pageInfo"] = {"hasNextPage":True,"endCursor":"same"}
        remote.pages.append(CloudError("plex_discover_request_failed") if failure == "page" else remote.pages[0])
    with pytest.raises(CloudError):
        cloud.read(remote.adapter)
    assert remote.closed


def test_missing_catalog_metadata_is_counted_without_inventing_ids(remote):
    remote.metadata.clear()
    rows, unmatched = cloud.read(remote.adapter)
    assert rows == [] and unmatched == 1


@pytest.mark.parametrize("case", ["restricted", "username", "account_id", "unresolved_id"])
def test_other_users_are_rejected(remote, case):
    if case == "restricted":
        remote.account["restricted"] = True
    elif case == "username":
        remote.adapter.client.selected_username = "child"
    elif case == "account_id":
        remote.adapter.client.selected_account_id = 2
        remote.adapter.client.token_account_id = 1
    else:
        remote.adapter.client.selected_account_id = 2
    with pytest.raises(CloudError):
        cloud.read(remote.adapter)
    assert not remote.calls


def test_server_account_ids_are_not_compared_to_plex_global_user_id(remote):
    remote.adapter.client.selected_account_id = 1
    remote.adapter.client.token_account_id = 1
    assert len(cloud.read(remote.adapter)[0]) == 1


def test_merge_deduplicates_server_play_but_keeps_later_rewatch(remote):
    rows, _ = cloud.read(remote.adapter)
    server = {"server":dict(rows[0], ids={"tmdb":"1","plex":"42"}, _plex_cloud_history=False)}
    later = dict(rows[0], watched_at="2022-01-01T00:00:00Z", _plex_cloud_activity_id="later")
    merged = cloud.merge(server, rows + [later])
    assert len(merged) == 2
    assert merged["server"]["ids"]["plex"] == "42"
    assert len(server) == 1


def test_read_switch_is_independent_of_write_switch():
    adapter = NS(config={"plex":{"history":{"discover_fallback":True}}})
    assert not cloud.enabled(adapter)
    adapter.config["plex"]["history"] = {"read_cloud_history":True,"discover_fallback":False}
    assert cloud.enabled(adapter)


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
@pytest.mark.parametrize("reverse", [False, True])
def test_cloud_read_pair_suppresses_removals_and_keeps_additions(config_base, monkeypatch, mode, reverse):
    from test_interactive_sync_features import feature_setup, feature_item
    from test_interactive_sync import run
    from cw_platform.orchestrator._interactive import InteractivePlan

    cfg, src, dst = feature_setup(config_base, monkeypatch, "history", [feature_item("history", 1)], [feature_item("history", 2)], mode)
    pair = cfg["pairs"][0]
    plex_side = "target" if reverse else "source"
    pair[plex_side] = "PLEX"
    calls = []
    def remove(*args, **kwargs):
        calls.append(args)
        return {"ok": True, "count": 0}
    monkeypatch.setattr(src, "remove", remove)
    monkeypatch.setattr(dst, "remove", remove)
    ops = {pair["source"]:src, pair["target"]:dst}
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: ops)
    cfg["plex"] = {"history":{"read_cloud_history":True}}
    plex = dst if reverse else src
    snapshots = []
    original_build = plex.build_index
    def build(config, **kwargs):
        snapshots.append(config["plex"]["history"]["read_cloud_history"])
        return original_build(config, **kwargs)
    monkeypatch.setattr(plex, "build_index", build)
    pair["features"]["history"]["remove_mode"] = "mirror"
    plan = InteractivePlan()
    run(cfg, plan)
    assert any(row["operation"] == "add" for row in plan.rows.values())
    if mode == "two-way" or not reverse:
        assert not any(row["operation"] == "remove" for row in plan.rows.values())
    result = run(cfg, InteractivePlan(preview=False, selected=set(plan.rows)))
    assert not result.get("errors")
    assert snapshots and all(flag is (mode == "two-way" or not reverse) for flag in snapshots)
    assert cfg["plex"]["history"]["read_cloud_history"] is True
    if mode == "two-way" or not reverse:
        assert not calls


def test_empty_catalog_result_is_unmatched(remote, monkeypatch):
    monkeypatch.setattr(cloud.Writer, "_metadata", lambda *a, **kw: {"Metadata": []})
    rows, unmatched = cloud.read(remote.adapter)
    assert rows == []
    assert unmatched == 1
