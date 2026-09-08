# tests/test_interactive_sync_mapping.py
# CrossWatch - Batch mapping and destination search regression tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from copy import deepcopy

import pytest

from cw_platform.orchestrator._interactive import InteractivePlan
from services.interactive_sync_store import ReviewStore
from test_interactive_sync import api_client, setup_ops, item, _cfg


def synchronous_launch(current, task, *args, prepare=None, **kwargs):
    if prepare:
        prepare()
    task(current, *args)


@pytest.mark.parametrize("selected", [True, False])
def test_ten_corrections_one_policy_write_one_refresh(api_client, config_base, monkeypatch, selected):
    from api import editorAPI
    from services import interactive_sync as svc

    client, session, api = api_client
    src, dst = setup_ops(config_base, monkeypatch, [item(n) for n in range(1, 12)])
    monkeypatch.setattr(editorAPI, "_STATE_BASE", config_base)
    svc.refresh(session, _cfg(False), {})
    session.store.select(True)
    reads, writes = [], []
    build, update = svc.build, editorAPI.sqlite_manual_policy.update_policy

    def track_build(*args, **kwargs):
        reads.append(True)
        return build(*args, **kwargs)

    def track_write(*args, **kwargs):
        writes.append(True)
        return update(*args, **kwargs)

    monkeypatch.setattr(svc, "build", track_build)
    monkeypatch.setattr(editorAPI.sqlite_manual_policy, "update_policy", track_write)
    monkeypatch.setattr(api, "launch", synchronous_launch)
    rows = list(session.plan.rows.values())
    edits = [dict(row_id=row["id"], item=item(100 + n), selected=selected) for n, row in enumerate(rows[:10])]
    response = client.post(f"/api/interactive-sync/{session.id}/mappings", json=dict(
        revision=session.revision, selection_version=session.selection_version, edits=edits))
    assert response.status_code == 200, response.text
    assert len(reads) == len(writes) == 1
    assert len(session.plan.rows) == 11
    assert session.store.counts["selected"] == (11 if selected else 1)
    assert not dst.add_calls and not src.add_calls
    policy = editorAPI.sqlite_manual_policy.load_policy(config_base)
    records = policy["pairs"][session.pair_id]["providers"]["SRC"]["watchlist"]["mappings"]
    assert len(records) == 10
    for n, row in enumerate(rows[:10]):
        record = records[next(iter(editorAPI._canonicalize_manual_items({"key": item(100+n)}, "watchlist")))]
        assert record["original_key"] == row["key"]
        assert record["original"]["ids"] == row["item"]["ids"]
        assert record["origin"] == "interactive_sync"


@pytest.mark.parametrize("bad", ["invalid_id", "missing_row", "duplicate_row", "duplicate_target", "bad_episode"])
def test_invalid_batch_saves_nothing(api_client, config_base, monkeypatch, bad):
    from api import editorAPI

    client, session, api = api_client
    monkeypatch.setattr(editorAPI, "_STATE_BASE", config_base)
    session.plan.filter("watchlist", "DST", "default", "add", [item(2)], source="SRC")
    session.store.finish()
    rows = list(session.plan.rows)
    edits = [dict(row_id=rows[0], item=item(10)), dict(row_id=rows[1], item=item(20))]
    if bad == "invalid_id": edits[1]["item"]["ids"] = {"tmdb": "bad"}
    if bad == "missing_row": edits[1]["row_id"] = "forged"
    if bad == "duplicate_row": edits[1]["row_id"] = rows[0]
    if bad == "duplicate_target": edits[1]["item"] = item(10)
    if bad == "bad_episode": edits[1]["item"].update(type="episode", season=1, episode=0)
    monkeypatch.setattr(api, "launch", lambda *a, **k: pytest.fail("Invalid batch reached save"))
    response = client.post(f"/api/interactive-sync/{session.id}/mappings", json=dict(revision=1, selection_version=0, edits=edits))
    assert response.status_code == 400
    assert editorAPI._load_policy_manual("watchlist", "SRC") == ({}, [])


def test_ids_and_episode_changed_together_preserve_watch_value(api_client, config_base, monkeypatch):
    from api import editorAPI

    client, session, api = api_client
    monkeypatch.setattr(editorAPI, "_STATE_BASE", config_base)
    original = dict(type="episode", title="Monster", season=2, episode=9, ids={"tvdb": "10649177"},
                    watched_at="2024-09-19T08:00:00Z", _trakt_history_id="private-event")
    session.plan.filter("history", "DST", "second", "add", [original], source="SRC")
    row = next(row for row in session.plan.rows.values() if row["feature"] == "history")
    corrected = dict(original, ids={"tmdb": "1398"}, show_ids={"tmdb": "1398"}, season=1)
    corrected["watched_at"] = "2099-01-01T00:00:00Z"
    launches = []

    def launch(current, task, *args, prepare=None, **kwargs):
        prepare()
        launches.append(args[-1])

    monkeypatch.setattr(api, "launch", launch)
    response = client.post(f"/api/interactive-sync/{session.id}/mappings", json=dict(
        revision=1, selection_version=0, edits=[dict(row_id=row["id"], item=corrected)]))
    assert response.status_code == 200
    adds, blocks = editorAPI._load_policy_manual("history", "SRC", pair_id=session.pair_id)
    saved = next(iter(adds.values()))
    assert saved["ids"] == saved["show_ids"] == {"tmdb": "1398"}
    assert (saved["season"], saved["episode"]) == (1, 9)
    assert saved["watched_at"] == original["watched_at"]
    assert "_trakt_history_id" not in saved
    assert row["key"] in blocks
    assert launches[0][0]["identity"][-1] == "second"


def test_unresolved_starts_unchecked_but_can_be_retried():
    store = ReviewStore()
    try:
        plan = InteractivePlan(rows=store.rows)
        plan.filter("watchlist", "DST", "default", "add", [dict(type="movie", title="Needs lookup"), item(1)])
        store.finish()
        unresolved = next(row for row in store.page()["items"] if row["result"] == "unresolved")
        assert unresolved["selectable"] and not unresolved["selected"]
        assert store.counts["selected"] == 1
        store.select(True, ids=[unresolved["id"]])
        assert store.counts["selected"] == 2
    finally:
        store.close()


def test_corrected_selection_stays_with_its_route_and_skips_unresolved():
    store = ReviewStore()
    try:
        plan = InteractivePlan(rows=store.rows)
        for instance in ("one", "two"):
            plan.filter("history", "DST", instance, "update", [item(1)], source="SRC", source_instance="source-one")
        unresolved = dict(type="movie", title="Still unresolved")
        plan.filter("history", "DST", "one", "add", [unresolved], source="SRC", source_instance="source-one")
        store.finish()
        store.select(False)
        pending = next(row for row in store.rows.values() if row["result"] == "unresolved")
        store.select_corrected([
            dict(identity=("history", "imdb:tt0000001", "SRC", "source-one", "DST", "one"), selected=True),
            dict(identity=("history", pending["key"], "SRC", "source-one", "DST", "one"), selected=True),
        ])
        selected = [row for row in store.page()["items"] if row["selected"]]
        assert len(selected) == 1 and selected[0]["instance"] == "one"
        assert selected[0]["result"] == "update"
    finally:
        store.close()


def test_batch_limit_rejects_oversized_request(api_client):
    client, session, api = api_client
    edit = dict(row_id=next(iter(session.plan.rows)), item=item(1))
    response = client.post(f"/api/interactive-sync/{session.id}/mappings", json=dict(revision=1, selection_version=0, edits=[edit] * 201))
    assert response.status_code == 422


def test_shared_scope_is_explicit_and_invalid_scope_cannot_save(api_client, config_base, monkeypatch):
    from api import editorAPI
    client, session, api = api_client
    monkeypatch.setattr(editorAPI, "_STATE_BASE", config_base)
    monkeypatch.setattr(api, "launch", lambda *args, prepare=None, **kwargs: prepare())
    edit = dict(row_id=next(iter(session.plan.rows)), item=item(10))
    payload = dict(revision=1, selection_version=0, edits=[edit])
    assert client.post(f"/api/interactive-sync/{session.id}/mappings", json={**payload, "scope":"unknown"}).status_code == 422
    assert not editorAPI._load_policy()["providers"]
    response = client.post(f"/api/interactive-sync/{session.id}/mappings", json={**payload, "scope":"shared"})
    assert response.status_code == 200, response.text
    policy = editorAPI._load_policy()
    assert policy["providers"]["SRC"]["watchlist"]["adds"]["items"]
    assert not policy.get("pairs")


@pytest.mark.parametrize("path", ["mappings", "mapping-search", "mapping-catalogs", "mapping-episodes"])
def test_mapping_requires_current_revision_and_owner(api_client, path):
    client, session, api = api_client
    row = next(iter(session.plan.rows))
    def request(revision, user="alice", version=0):
        if path in {"mapping-search", "mapping-catalogs"}:
            return client.get(f"/api/interactive-sync/{session.id}/{path}", params=dict(revision=revision, row_id=row, q="Movie"), headers={"x-test-user": user})
        return client.post(f"/api/interactive-sync/{session.id}/{path}", json=dict(revision=revision, selection_version=version,
            edits=[dict(row_id=row, item=item(2))]), headers={"x-test-user":user})
    assert request(0).status_code == 409
    assert request(1, "bob").status_code == 404
    if path == "mappings": assert request(1, version=9).status_code == 409


@pytest.mark.parametrize("provider,body,expected", [
    ("MDBLIST", {"search":[{"title":"Monster", "year":2022, "id":"tt13207736", "tmdbid":113988}]}, {"imdb":"tt13207736", "tmdb":"113988"}),
    ("SIMKL", [{"title":"Monster", "year":2022, "ids":{"simkl_id":123, "tmdb":113988}}], {"simkl":"123", "tmdb":"113988"}),
    ("TRAKT", [{"show":{"title":"Monster", "year":2022, "ids":{"trakt":123, "tmdb":113988}}}], {"trakt":"123", "tmdb":"113988"}),
    ("PLEX", {"MediaContainer":{"Metadata":[{"title":"Monster", "type":"show", "year":2022, "Guid":[{"id":"tmdb://113988"}]}]}}, {"tmdb":"113988"}),
])
def test_destination_search_normalizes_and_uses_instance(monkeypatch, provider, body, expected):
    from services.interactive_sync_mapping import search_candidates
    from providers.sync.mdblist import _auth
    import requests

    calls = []
    class Response:
        status_code = 200
        def json(self): return body
    def get(self, url, **kwargs):
        calls.append((url, kwargs))
        return Response()
    monkeypatch.setattr(requests.Session, "get", get)
    def authenticated(client, method, url, **kwargs):
        assert kwargs["instance_id"] == "second"
        return get(client, url, **kwargs)
    monkeypatch.setattr(_auth, "request_with_auth", authenticated)
    cfg = {provider.lower(): {"client_id":"default-key", "instances":{"second":{"client_id":"second-key", "api_key":"second-key", "access_token":"second-token", "account_token":"plex-second-token"}}},
           "tmdb":{"api_key":"metadata-key"}}
    result = search_candidates(cfg, dict(provider=provider, instance="second", item=dict(type="episode")), "Monster")
    assert result["results"][0]["ids"] == expected
    assert result["results"][0]["exact_title"]
    assert "key" not in str(result)
    if provider == "SIMKL": assert calls[0][1]["params"]["client_id"] == "second-key"
    if provider == "TRAKT": assert calls[0][1]["headers"]["trakt-api-key"] == "second-key"
    if provider == "PLEX": assert calls[0][1]["headers"]["X-Plex-Token"] == "plex-second-token"


def test_search_failure_does_not_expose_credentials(monkeypatch):
    import requests
    from fastapi import HTTPException
    from services.interactive_sync_mapping import search_candidates

    def fail(*args, **kwargs): raise requests.Timeout("https://example/?api_key=SECRET")
    monkeypatch.setattr(requests.Session, "get", fail)
    with pytest.raises(HTTPException) as error:
        search_candidates({"plex":{"account_token":"SECRET"}}, dict(provider="PLEX", item=dict(type="movie")), "Title")
    assert error.value.status_code == 502
    assert "SECRET" not in error.value.detail


@pytest.mark.parametrize("entity", ["movie", "show"])
def test_mdblist_search_fetches_external_ids_in_one_batch(monkeypatch, entity):
    from services.interactive_sync_mapping import search_candidates
    from providers.sync.mdblist import _auth

    calls = []
    class Response:
        status_code = 200
        def __init__(self, body): self.body = body
        def json(self): return self.body
    def request(client, method, url, **kwargs):
        calls.append((method, url, kwargs))
        assert kwargs["instance_id"] == "second"
        if method == "GET":
            return Response({"search": [{"title": "Monster", "ids": {"mdblist": "o6b1"}},
                                        {"title": "Another title", "ids": {"mdblist": "n3n8"}}]})
        assert kwargs["json"] == {"ids": ["o6b1", "n3n8"]}
        return Response([{"mdblist_id": "n3n8", "tmdbid": 22, "imdbid": "tt22222"},
                         {"mdblist_id": "o6b1", "tmdbid": 11, "imdbid": "tt11111"}])
    monkeypatch.setattr(_auth, "request_with_auth", request)
    cfg = {"mdblist": {"instances": {"second": {"api_key": "test-key"}}}}
    result = search_candidates(cfg, {"provider": "MDBLIST", "instance": "second", "item": {"type": entity}}, "Monster")
    assert result["results"][0]["ids"] == {"mdblist": "o6b1", "tmdb": "11", "imdb": "tt11111"}
    assert result["results"][1]["ids"]["tmdb"] == "22"
    assert len(calls) == 2
    assert calls[1][1] == f"https://api.mdblist.com/mdblist/{entity}/"
    assert all(not item.get("mapping_unavailable") for item in result["results"])


@pytest.mark.parametrize("failure", ["timeout", "http", "missing", "wrong_id", "ambiguous", "malformed"])
def test_mdblist_incomplete_matches_cannot_replace_mapping(monkeypatch, failure):
    import requests
    from services.interactive_sync_mapping import search_candidates
    from providers.sync.mdblist import _auth

    class Response:
        status_code = 200
        def __init__(self, body): self.body = body
        def json(self): return self.body
    def request(client, method, url, **kwargs):
        if method == "GET":
            return Response({"search": [{"title": "Monster", "ids": {"mdblist": "o6b1"}}]})
        if failure == "timeout": raise requests.Timeout("SECRET")
        if failure == "http":
            response = Response({}); response.status_code = 429; return response
        if failure == "wrong_id": return Response([{"mdblist_id": "other", "tmdbid": 11}])
        if failure == "ambiguous": return Response([{"mdblist_id": "o6b1", "tmdbid": n} for n in (11, 22)])
        if failure == "malformed": return Response({"error": "SECRET"})
        return Response([{"mdblist_id": "o6b1"}])
    monkeypatch.setattr(_auth, "request_with_auth", request)
    result = search_candidates({"mdblist": {"api_key": "test-key"}},
                               {"provider": "MDBLIST", "item": {"type": "show"}}, "Monster")
    assert result["results"][0]["ids"] == {"mdblist": "o6b1"}
    assert "TMDb ID" in result["results"][0]["mapping_unavailable"]
    assert "SECRET" not in str(result)
