# tests/test_sync_topology.py
# CrossWatch - Sync topology integration tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)

from pathlib import Path
from copy import deepcopy
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]


def test_topology_analysis_and_layout():
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is needed for the local frontend topology tests")
    result = subprocess.run(
        [node, "--test", "tests/topology.test.mjs"], cwd=ROOT,
        capture_output=True, text=True, encoding="utf-8", timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_topology_summary_extends_sync_and_stays_out_of_managed_shell():
    from ui_frontend import get_index_html

    html = get_index_html()
    assert html.index('id="providers_list"') < html.index('id="sync-topology-health"') < html.index('id="pairs_list"')
    assert '/assets/helpers/feature-meta.js' in html
    assert '/assets/js/topology/advisor.js' in html
    assert 'id="sync-topology-health"' not in get_index_html(include_admin=False)


@pytest.fixture()
def topology_api(monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from api import syncAPI
    from cw_platform import config_base

    cfg = {"pairs": [
        {"id": "a", "source": "EMBY", "target": "MDBLIST", "profile_id": "one", "features": {"history": {"enable": True, "remove": False}}},
        {"id": "b", "source": "NUVIO", "target": "MDBLIST", "profile_id": "two", "features": {"history": True}},
        {"id": "legacy", "source": "EMBY", "target": "MDBLIST", "features": {"history": True}},
    ]}

    def update(mutator):
        draft = deepcopy(cfg)
        result = mutator(draft)
        cfg.clear()
        cfg.update(draft)
        return draft, result

    monkeypatch.setattr(syncAPI, "_env", lambda: (lambda: deepcopy(cfg), lambda _: None))
    monkeypatch.setattr(config_base, "update_config", update)
    monkeypatch.setattr(syncAPI, "list_user_profiles", lambda _: [
        {"id": "one", "instances": {"EMBY": ["default"], "MDBLIST": ["default"]}},
        {"id": "two", "instances": {"NUVIO": ["default"], "MDBLIST": ["default"]}},
    ])
    app = FastAPI()
    app.include_router(syncAPI.router)
    with TestClient(app) as client:
        yield client, cfg, syncAPI


def test_topology_baseline_persists_and_isolates_scopes(topology_api):
    client, cfg, api = topology_api
    url = "/api/sync/topology/baseline"
    pairs = api._topology_pairs(cfg, "one")
    assert [pair["id"] for pair in pairs] == ["a", "legacy"]
    payload = {"version": 1, "pairs": pairs, "findings": [{"type": "conflict", "feature": "history"}]}
    saved = client.put(url, params={"profile_id": "one"}, json=payload)
    assert saved.status_code == 200
    baseline = saved.json()["baseline"]
    assert baseline["accepted_at"]
    assert client.get(url, params={"profile_id": "one"}).json()["baseline"] == baseline
    assert client.get(url).json()["baseline"] is None
    assert client.get(url, params={"profile_id": "two"}).json()["baseline"] is None
    assert client.delete(url, params={"profile_id": "two"}).status_code == 200
    assert client.get(url, params={"profile_id": "one"}).json()["baseline"] == baseline
    assert client.delete(url, params={"profile_id": "one"}).json()["baseline"] is None
    assert client.get(url, params={"profile_id": "one"}).json()["baseline"] is None


@pytest.mark.parametrize("change", ["direction", "removal", "feature", "instance", "enabled", "new_pair"])
def test_topology_acknowledge_rejects_changed_configuration(topology_api, change):
    client, cfg, api = topology_api
    payload = {"version": 1, "pairs": deepcopy(api._topology_pairs(cfg, "")), "findings": []}
    pair = cfg["pairs"][0]
    if change == "direction":
        pair["mode"] = "two-way"
    elif change == "removal":
        pair["features"]["history"]["remove_mode"] = "both"
    elif change == "feature":
        pair["features"]["history"]["enable"] = False
    elif change == "instance":
        pair["target_instance"] = "other"
    elif change == "enabled":
        pair["enabled"] = False
    else:
        cfg["pairs"].append({**deepcopy(pair), "id": "new"})
    assert client.put("/api/sync/topology/baseline", json=payload).status_code == 409
    assert "topology_baselines" not in cfg


def test_topology_baseline_ignores_labels_and_pair_order(topology_api):
    client, cfg, api = topology_api
    payload = {"version": 1, "pairs": deepcopy(api._topology_pairs(cfg, "")), "findings": []}
    cfg["pairs"].reverse()
    cfg["pairs"][0]["label"] = "Renamed"
    assert client.put("/api/sync/topology/baseline", json=payload).status_code == 200


def test_topology_baseline_validates_scope_version_and_access(topology_api):
    from api.appAuthAPI import non_admin_api_allowed

    client, _, _ = topology_api
    url = "/api/sync/topology/baseline"
    assert client.get(url, params={"profile_id": "deleted"}).status_code == 404
    assert client.put(url, json={"version": 2, "pairs": [], "findings": []}).status_code == 422
    for method in ("GET", "PUT", "DELETE"):
        assert not non_admin_api_allowed(url, method)


def test_topology_baseline_survives_config_reload(config_base):
    from cw_platform import config_base as cb

    baseline = {"version": 1, "pairs": [], "findings": [], "accepted_at": "2026-09-21T12:00:00+00:00"}
    cb.update_config(lambda cfg: cfg.update({"topology_baselines": {"all": baseline}}))
    assert cb.load_config()["topology_baselines"]["all"] == baseline
