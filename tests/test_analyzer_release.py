# tests/test_analyzer_release.py
# CrossWatch - Analyzer release tests for blocked items
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)

import json

import pytest
from fastapi import HTTPException

from cw_platform.orchestrator._blackbox import clear_path_keys
from services import analyzer as an
from services import analyzer_release as release

KEY = "tmdb:123#s01e02"
EVENT = f"{KEY}@1767229200"


@pytest.fixture
def release_case(monkeypatch, tmp_path):
    pair = dict(id="p1", source="SIMKL", source_instance="alice", target="MDBLIST",
                target_instance="destination", mode="one-way", features={"history": True})
    analysis = {"problems": [], "attention": {"rows": []}}
    files = []
    monkeypatch.setattr(an, "_cfg", lambda: {"pairs": [pair]})
    monkeypatch.setattr(an, "_cached_analysis", lambda pid: analysis)
    monkeypatch.setattr(an, "_load_analysis_state", lambda pid: ({}, None, {"scope"}, {}, {}))
    monkeypatch.setattr(an, "_iter_unresolved_files", lambda allowed: iter(files))
    monkeypatch.setattr(an, "CWS_DIR", tmp_path)
    monkeypatch.setattr(release, "request_user", lambda request: None)
    return analysis, files, tmp_path


def payload(kind, provider, key=KEY):
    return release.ReleaseRequest(pair_id="p1", provider=provider, feature="history", key=key, kind=kind)


def write(path, keys):
    path.write_text(json.dumps({key: {"reason": "flapper", "since": 1} for key in keys}), encoding="utf-8")


def test_clear_path_keys_removes_blackbox_and_flap_entries(tmp_path):
    blackbox = tmp_path / "mdblist_history.scope.blackbox.json"
    flap = tmp_path / "mdblist_history.scope.flap.json"
    write(blackbox, [EVENT, "tmdb:9"])
    write(flap, [EVENT, "tmdb:9"])
    result = clear_path_keys(blackbox, [EVENT])
    assert (result["blackbox_removed"], result["flap_removed"]) == (1, 1)
    assert list(json.loads(blackbox.read_text("utf-8"))) == ["tmdb:9"]
    assert list(json.loads(flap.read_text("utf-8"))) == ["tmdb:9"]
    assert clear_path_keys(tmp_path / "mdblist_history.scope.flap.json", [EVENT])["flap_removed"] == 0


def test_retry_release_clears_only_the_matching_destination_file(release_case):
    analysis, files, tmp_path = release_case
    analysis["attention"]["rows"].append(dict(provider="MDBLIST@destination", feature="history", key=KEY, keys=[KEY],
                                               unresolved=True, retry_blocked=True))
    own = "mdblist_history.scope.blackbox.json"
    other = "simkl_history.scope.blackbox.json"
    write(tmp_path / own, [EVENT, "tmdb:9"])
    write(tmp_path / other, [EVENT])
    files.extend([
        ("MDBLIST@destination", "history", "blackbox", False, own, [(EVENT, {}, {}), ("tmdb:9", {}, {})]),
        ("SIMKL@alice", "history", "blackbox", False, other, [(EVENT, {}, {})]),
        ("MDBLIST@destination", "history", "unresolved", False, "mdblist_history.scope.unresolved.json", [(EVENT, {}, {})]),
    ])
    result = release.handle_release(payload("retry", "MDBLIST@destination"), None)
    assert result == dict(ok=True, released="retry", removed=1)
    assert list(json.loads((tmp_path / own).read_text("utf-8"))) == ["tmdb:9"]
    assert list(json.loads((tmp_path / other).read_text("utf-8"))) == [EVENT]


@pytest.mark.parametrize("change", ["not_blocked", "other_provider", "file_gone"])
def test_retry_release_rejects_changed_items(release_case, change):
    analysis, files, tmp_path = release_case
    entry = dict(provider="MDBLIST@destination", feature="history", key=KEY, keys=[KEY], unresolved=True, retry_blocked=True)
    analysis["attention"]["rows"].append(entry)
    name = "mdblist_history.scope.blackbox.json"
    write(tmp_path / name, [EVENT])
    files.append(("MDBLIST@destination", "history", "blackbox", False, name, [(EVENT, {}, {})]))
    provider = "MDBLIST@destination"
    if change == "not_blocked": entry["retry_blocked"] = False
    if change == "other_provider": provider = "MDBLIST@other"
    if change == "file_gone": files.clear()
    with pytest.raises(HTTPException) as error:
        release.handle_release(payload("retry", provider), None)
    assert error.value.status_code == 409
    assert list(json.loads((tmp_path / name).read_text("utf-8"))) == [EVENT]


def test_rule_release_lifts_a_plain_block(release_case, monkeypatch):
    from services import editor_mapping
    analysis = release_case[0]
    analysis["problems"].append(dict(type="blocked_manual", provider="SIMKL@alice", feature="history", key=KEY,
                                     block=dict(provider="SIMKL", instance="alice", key="imdb:tt1#s01e02", mapping_key=None)))
    calls = []
    monkeypatch.setattr(editor_mapping, "update_block", lambda body, request: calls.append(body) or {"ok": True})
    assert release.handle_release(payload("rule", "SIMKL@alice"), None) == dict(ok=True, released="rule")
    body = calls[0]
    assert (body.provider, body.instance, body.pair_id, body.key, body.blocked) == ("SIMKL", "alice", "", "imdb:tt1#s01e02", False)


def test_rule_release_removes_the_owning_mapping(release_case, monkeypatch):
    from services import editor_mapping, mapping_transfer
    analysis = release_case[0]
    analysis["problems"].append(dict(type="blocked_manual", provider="SIMKL@alice", feature="history", key=KEY,
                                     block=dict(provider="SIMKL", instance="alice", key=KEY, mapping_key="tmdb:456#s01e03")))
    calls = []
    monkeypatch.setattr(mapping_transfer, "delete_rule", lambda request, rule: calls.append(rule) or {"ok": True})
    monkeypatch.setattr(editor_mapping, "update_block", lambda *args: pytest.fail("block must not be edited directly"))
    assert release.handle_release(payload("rule", "SIMKL@alice"), None) == dict(ok=True, released="mapping")
    rule = calls[0]
    assert (rule.provider, rule.instance, rule.pair_id, rule.key, rule.entry_type) == ("SIMKL", "alice", "", "tmdb:456#s01e03", "mapping")


def test_rule_release_rejects_rows_without_a_known_block(release_case):
    analysis = release_case[0]
    analysis["problems"].append(dict(type="blocked_manual", provider="SIMKL@alice", feature="history", key=KEY))
    with pytest.raises(HTTPException) as error:
        release.handle_release(payload("rule", "SIMKL@alice"), None)
    assert error.value.status_code == 409


@pytest.mark.parametrize("write_allowed,access,status", [(False, True, 403), (True, False, 404)])
def test_permissions_checked_before_releasing(release_case, monkeypatch, write_allowed, access, status):
    monkeypatch.setattr(release, "request_user", lambda request: {"is_admin": False, "permissions": {"write": write_allowed}})
    monkeypatch.setattr(release, "user_can_access_pair", lambda *args: access)
    with pytest.raises(HTTPException) as error:
        release.handle_release(payload("retry", "MDBLIST@destination"), None)
    assert error.value.status_code == status


def test_block_ref_finds_instance_then_default_and_flags_mapping_blocks():
    manual = {"providers": {
        "simkl": {
            "history": {"blocks": ["tmdb:1"]},
            "instances": {"alice": {"history": {
                "blocks": [KEY, "tmdb:7"],
                "adds": {"items": {"tmdb:456#s01e03": {"type": "episode"}}},
                "mappings": {"tmdb:456#s01e03": {"original_key": KEY}},
            }}},
        },
    }}
    assert release.block_ref(manual, "SIMKL@alice", "history", KEY) == dict(
        provider="SIMKL", instance="alice", key=KEY, mapping_key="tmdb:456#s01e03")
    assert release.block_ref(manual, "SIMKL@alice", "history", "tmdb:7")["mapping_key"] is None
    assert release.block_ref(manual, "SIMKL@alice", "history", "tmdb:1") == dict(
        provider="SIMKL", instance="default", key="tmdb:1", mapping_key=None)
    assert release.block_ref(manual, "SIMKL@alice", "history", "tmdb:404") is None


def test_blocked_problem_carries_its_block_reference(monkeypatch):
    item = dict(type="movie", title="Blocked", year=2020, ids={"tmdb": "55"})
    state = {"providers": {"SIMKL": {"watchlist": {"baseline": {"items": {"tmdb:55": item}}}},
                           "MDBLIST": {"watchlist": {"baseline": {"items": {}}}}}}
    cfg = {"pairs": [dict(id="p1", enabled=True, mode="one-way", source="SIMKL", target="MDBLIST", features={"watchlist": True})]}
    monkeypatch.setattr(an, "_load_manual_state", lambda: {"providers": {"SIMKL": {"watchlist": {"blocks": ["tmdb:55"]}}}})
    problems = an._problems(state, cfg=cfg, include_system=False, include_hints=False)
    blocked = [row for row in problems if row["type"] == "blocked_manual"]
    assert len(blocked) == 1
    assert blocked[0]["block"] == dict(provider="SIMKL", instance="default", key="tmdb:55", mapping_key=None)
