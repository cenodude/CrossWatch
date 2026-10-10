# tests/test_mapping_transfer_groups.py
# CrossWatch - Episode group mapping transfer and atomic import regressions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from copy import deepcopy
import json

import pytest
from fastapi import HTTPException

from cw_platform.id_map import canonical_key
from cw_platform.local_db import episode_groups, manual_policy
from test_episode_groups import group, watched
from test_episode_groups_api import client


def bundle(*groups, records=(), version=3):
    data = dict(format="crosswatch-mappings-blocks", version=version, records=list(records))
    if groups:
        data["episode_groups"] = [dict(pair_id="p1", group=value) for value in groups]
    return data


def upload(client, data):
    return client.post("/api/editor/mappings/import", files={
        "file": ("mappings.json", json.dumps(data), "application/json")})


def correction(number=2, pair_id="p1"):
    original, corrected = watched(number), watched(9)
    return dict(provider="SRC", instance="default", pair_id=pair_id, feature="history", entry_type="mapping",
                key=canonical_key(corrected), item=corrected, original=original, original_key=canonical_key(original))


def block():
    return dict(provider="SRC", instance="default", pair_id="", feature="history", entry_type="block", key="tmdb:999#show")


def test_transfer_roundtrip_preserves_rules_and_scrobble_setting_without_runtime_state(client, config_base):
    client, api, _ = client
    value = {**group(), "scrobble": True}
    response = upload(client, bundle(value, records=[correction(8), block()]))
    assert response.status_code == 200, response.text
    assert response.json() == dict(ok=True, imported=2, skipped=0, groups_imported=1, groups_skipped=0)
    before = deepcopy(api._load_policy())
    episode_groups.save(config_base, "p1", {"finale": {"held": True}})
    episode_groups.save_scrobble(config_base, "p1", "route", {"completed": ["part"], "sent": ["target"]})
    exported = client.get("/api/editor/mappings/export").json()
    assert exported["version"] == 3
    assert set(exported) == {"format", "version", "records", "episode_groups"}
    assert exported["episode_groups"] == [dict(pair_id="p1", group=value)]
    assert len(exported["records"]) == 2
    manual_policy.clear_policy(config_base)
    assert upload(client, exported).status_code == 200
    assert api._load_policy() == before
    repeated = upload(client, exported).json()
    assert repeated == dict(ok=True, imported=0, skipped=2, groups_imported=0, groups_skipped=1)
    assert episode_groups.load(config_base, "p1") == {"finale": {"held": True}}
    assert episode_groups.load_scrobble(config_base, "p1", "route") == {"completed": ["part"], "sent": ["target"]}


@pytest.mark.parametrize("params,included", [({}, True), ({"provider": "src"}, True),
    ({"provider": "DST", "instance": "default"}, True), ({"feature": "history"}, True),
    ({"pair_id": "p1"}, True), ({"pair_id": "shared"}, False), ({"pair_id": "other"}, False),
    ({"feature": "ratings"}, False), ({"provider": "OTHER"}, False), ({"instance": "other"}, False)])
def test_export_filters_keep_both_sides_of_each_matching_group(client, params, included):
    client, _, _ = client
    assert upload(client, bundle(group())).status_code == 200
    exported = client.get("/api/editor/mappings/export", params=params).json()
    assert exported["episode_groups"] == ([dict(pair_id="p1", group=group())] if included else [])


@pytest.mark.parametrize("denied", ["pair", "source", "target", "profile_endpoint", "profile_pair"])
def test_export_omits_groups_outside_access_and_profile_scope(client, monkeypatch, denied):
    from cw_platform import access_policy

    client, api, cfg = client
    assert upload(client, bundle(group())).status_code == 200
    params = {}
    if denied == "pair":
        monkeypatch.setattr(access_policy, "user_can_access_pair", lambda *_: False)
    elif denied in {"source", "target"}:
        monkeypatch.setattr(api, "user_can_access_instance", lambda _cfg, _user, provider, _instance:
                            provider != ("SRC" if denied == "source" else "DST"))
    else:
        params = {"user_profile": "alice"}
        scope = {"SRC": ["default"], "DST": ["default"]} if denied == "profile_pair" else {"SRC": ["default"]}
        monkeypatch.setattr(access_policy, "profile_instances_map", lambda *_: scope)
        cfg["pairs"][0]["profile_id"] = "bob" if denied == "profile_pair" else "alice"
    response = client.get("/api/editor/mappings/export", params=params)
    assert response.status_code == 200
    assert response.json()["episode_groups"] == []


def test_export_includes_groups_in_matching_profile(client, monkeypatch):
    from cw_platform import access_policy

    client, _, cfg = client
    cfg["pairs"][0]["profile_id"] = "alice"
    monkeypatch.setattr(access_policy, "profile_instances_map", lambda *_: {"SRC": ["default"], "DST": ["default"]})
    assert upload(client, bundle(group())).status_code == 200
    assert len(client.get("/api/editor/mappings/export", params={"user_profile": "alice"}).json()["episode_groups"]) == 1


def test_same_provider_groups_keep_distinct_instances(client):
    client, _, cfg = client
    value = group()
    value["source"].update(provider="SRC", instance="first")
    value["target"].update(provider="SRC", instance="second")
    cfg["pairs"][0].update(source="SRC", target="SRC", source_instance="first", target_instance="second")
    assert upload(client, bundle(value)).status_code == 200
    for instance in ("first", "second"):
        exported = client.get("/api/editor/mappings/export", params={"provider": "SRC", "instance": instance}).json()
        assert exported["episode_groups"] == [dict(pair_id="p1", group=value)]


@pytest.mark.parametrize("mismatch", ["pair", "source", "target", "source_instance", "target_instance", "feature"])
def test_import_requires_existing_history_pair_and_exact_endpoints(client, mismatch):
    client, api, cfg = client
    data = bundle(group(), records=[block()])
    if mismatch == "pair":
        data["episode_groups"][0]["pair_id"] = "missing"
    elif mismatch == "feature":
        cfg["pairs"][0]["features"] = {"ratings": {"enable": True}}
    elif mismatch.endswith("_instance"):
        data["episode_groups"][0]["group"][mismatch.split("_")[0]]["instance"] = "other"
    else:
        data["episode_groups"][0]["group"][mismatch]["provider"] = "OTHER"
    before = deepcopy(api._load_policy())
    assert upload(client, data).status_code in (400, 404)
    assert api._load_policy() == before


@pytest.mark.parametrize("denied", ["pair", "source", "target", "write"])
def test_import_authorizes_groups_before_writing_any_records(client, monkeypatch, denied):
    from services import editor_mapping

    client, api, _ = client
    before = deepcopy(api._load_policy())
    if denied == "pair":
        monkeypatch.setattr(editor_mapping, "user_can_access_pair", lambda *_: False)
    elif denied == "write":
        monkeypatch.setattr(api, "request_user", lambda *_: {"permissions": {"write": False}})
    else:
        def require(_cfg, _request, provider, _instance):
            if provider == ("SRC" if denied == "source" else "DST"):
                raise HTTPException(403, "Denied")
        monkeypatch.setattr(api, "_require_instance_scope", require)
    assert upload(client, bundle(group(), records=[block()])).status_code in (403, 404)
    assert api._load_policy() == before


@pytest.mark.parametrize("conflict", ["id", "overlap", "incoming_correction", "shared_correction", "saved_correction", "both_incoming"])
def test_group_conflicts_roll_back_entire_import(client, conflict):
    client, api, _ = client
    if conflict == "saved_correction":
        assert upload(client, bundle(records=[correction()])).status_code == 200
    elif conflict != "both_incoming":
        assert upload(client, bundle(group())).status_code == 200
    incoming = group()
    records = [block()]
    groups = [incoming]
    if conflict == "id":
        incoming["scrobble"] = True
    elif conflict == "overlap":
        incoming["id"] = "different"
    elif conflict in {"incoming_correction", "shared_correction", "both_incoming"}:
        records.append(correction(pair_id="" if conflict == "shared_correction" else "p1"))
        if conflict != "both_incoming":
            groups = []
    before = deepcopy(api._load_policy())
    assert upload(client, bundle(*groups, records=records)).status_code == 400
    assert api._load_policy() == before


@pytest.mark.parametrize("bad", ["duplicate", "overlap", "runtime", "scrobble_type", "v2_groups", "empty_pair"])
def test_malformed_group_files_are_rejected_before_mutation(client, bad):
    client, api, _ = client
    data = bundle(group(), records=[block()])
    value = data["episode_groups"][0]
    if bad in {"duplicate", "overlap"}:
        second = deepcopy(value)
        if bad == "overlap":
            second["group"]["id"] = "other"
        data["episode_groups"].append(second)
    elif bad == "runtime":
        value["group"]["completed"] = ["part"]
    elif bad == "scrobble_type":
        value["group"]["scrobble"] = "true"
    elif bad == "v2_groups":
        data["version"] = 2
    else:
        value["pair_id"] = ""
    before = deepcopy(api._load_policy())
    assert upload(client, data).status_code == 400
    assert api._load_policy() == before


@pytest.mark.parametrize("version", [1, 2])
def test_legacy_imports_preserve_existing_groups(client, version):
    client, api, _ = client
    assert upload(client, bundle(group())).status_code == 200
    response = upload(client, bundle(records=[block()], version=version))
    assert response.status_code == 200
    assert response.json()["imported"] == 1
    assert api._load_policy()["pairs"]["p1"]["episode_groups"] == [group()]
