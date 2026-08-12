# CrossWatch test scripts
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from cw_platform.anime_mapping import overrides as ov


@pytest.fixture()
def client(config_base: Path) -> TestClient:
    from api.animeMappingAPI import router

    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


_RULE = {
    "media_type": "show",
    "title": "Dragon Ball Z",
    "match_provider": "tvdb",
    "match_id": "81472",
    "match_season": 2,
    "target_namespace": "mal",
    "target_id": "813",
    "episode_from": 1,
    "episode_to": 35,
    "episode_start_at": 40,
}


def test_list_is_empty_and_exposes_the_schema(client: TestClient) -> None:
    r = client.get("/api/anime-mapping/overrides")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["overrides"] == []
    assert "tvdb" in body["schema"]["match_providers"]
    assert "simkl" in body["schema"]["target_namespaces"]
    assert body["schema"]["media_types"] == ["show", "movie"]


def test_create_then_list(client: TestClient) -> None:
    r = client.post("/api/anime-mapping/overrides", json=_RULE)
    assert r.status_code == 200
    created = r.json()["override"]
    assert created["target_id"] == "813"

    listed = client.get("/api/anime-mapping/overrides").json()
    assert [x["id"] for x in listed["overrides"]] == [created["id"]]
    assert listed["stats"]["enabled"] == 1
    assert listed["stats"]["episode_rules"] == 1


def test_update_in_place(client: TestClient) -> None:
    created = client.post("/api/anime-mapping/overrides", json=_RULE).json()["override"]
    updated = client.post(
        "/api/anime-mapping/overrides",
        json={**_RULE, "id": created["id"], "episode_start_at": 41},
    ).json()
    assert updated["ok"] is True
    assert len(updated["overrides"]) == 1
    assert updated["overrides"][0]["episode_start_at"] == 41


def test_invalid_rule_returns_400_with_a_readable_message(client: TestClient) -> None:
    r = client.post("/api/anime-mapping/overrides", json={**_RULE, "target_namespace": "tvdb"})
    assert r.status_code == 400
    body = r.json()
    assert body["ok"] is False
    assert body["error"] == "invalid_rule"
    assert "target_namespace" in body["message"]
    assert ov.load_overrides() == []


def test_delete_removes_the_rule(client: TestClient) -> None:
    created = client.post("/api/anime-mapping/overrides", json=_RULE).json()["override"]
    r = client.delete(f"/api/anime-mapping/overrides/{created['id']}")
    assert r.status_code == 200
    assert r.json()["overrides"] == []


def test_delete_unknown_rule_returns_404(client: TestClient) -> None:
    r = client.delete("/api/anime-mapping/overrides/ovr_missing")
    assert r.status_code == 404
    assert r.json()["error"] == "not_found"


def test_rules_survive_a_dataset_rebuild(client: TestClient, config_base: Path) -> None:
    import json

    from cw_platform.anime_mapping import storage

    created = client.post("/api/anime-mapping/overrides", json=_RULE).json()["override"]

    paths = storage.paths("v3")
    paths["root"].mkdir(parents=True, exist_ok=True)
    paths["mappings"].write_text(json.dumps({"tvdb_show:1:s1": {"mal:1": {"1": "1"}}}), encoding="utf-8")
    storage.rebuild_sqlite_from_mappings(release_tag="v3")

    assert [x["id"] for x in ov.load_overrides()] == [created["id"]]


def test_simkl_search_errors_do_not_expose_exception_text(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    from api import animeMappingAPI as api

    def fail_search(*_args, **_kwargs):
        raise api.SimklCatalogError("secret upstream path /srv/crosswatch/simkl.py")

    monkeypatch.setattr(api, "simkl_search", fail_search)

    r = client.get("/api/anime-mapping/simkl/search", params={"q": "frieren"})

    assert r.status_code == 502
    body = r.json()
    assert body["error"] == "simkl_search_failed"
    assert "secret upstream path" not in body["message"]
    assert "server logs" in body["message"]


def test_simkl_plan_errors_do_not_expose_exception_text(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    from api import animeMappingAPI as api

    def fail_plan(*_args, **_kwargs):
        raise api.SimklCatalogError("secret plan input /srv/crosswatch/rules.py")

    monkeypatch.setattr(api, "simkl_plan_rules", fail_plan)

    r = client.post("/api/anime-mapping/simkl/plan", json={"simkl": "12345"})

    assert r.status_code == 400
    body = r.json()
    assert body["error"] == "simkl_plan_failed"
    assert "secret plan input" not in body["message"]
    assert "server logs" in body["message"]


@pytest.mark.parametrize(
    ("method", "path", "kwargs"),
    [
        ("get", "/api/anime-mapping/simkl/search", {"params": {"q": "frieren"}}),
        ("post", "/api/anime-mapping/simkl/plan", {"json": {"simkl": "12345"}}),
    ],
)
def test_simkl_not_configured_errors_do_not_expose_exception_text(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    method: str,
    path: str,
    kwargs: dict[str, object],
) -> None:
    from api import animeMappingAPI as api

    def fail_lookup(*_args, **_kwargs):
        raise api.SimklNotConfigured("SIMKL rejected client id secret-client-id")

    monkeypatch.setattr(api, "simkl_search", fail_lookup)
    monkeypatch.setattr(api, "simkl_plan_rules", fail_lookup)

    r = getattr(client, method)(path, **kwargs)

    assert r.status_code == 409
    body = r.json()
    assert body["error"] == "simkl_not_configured"
    assert "secret-client-id" not in body["message"]
    assert body["message"] == "SIMKL lookup is not configured. Connect SIMKL and try again."
