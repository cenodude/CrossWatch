# tests/test_media_identity_policy.py
# CrossWatch - Movie/show identity in manual blocks, overrides and remapping
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy

import pytest

from cw_platform.local_db import manual_policy as sqlite_manual_policy
from cw_platform.local_db.db import get_conn
from cw_platform.mapping_policy import effective_policy, update_mappings
from cw_platform.orchestrator._pairs_utils import filter_manual_block, manual_policy, merge_manual_adds
from services.mapping_transfer import RuleBundle, _typed_rules


MOVIE = {"type": "movie", "title": "The Two Towers", "year": 2002, "ids": {"tmdb": "121", "imdb": "tt0167261"}}
SHOW = {"type": "show", "title": "Doctor Who", "year": 1963, "ids": {"tmdb": "121", "imdb": "tt0056751"}}


def _legacy_db(tmp_path, policy):
    sqlite_manual_policy.save_policy(tmp_path, policy)
    conn = get_conn(tmp_path)
    with conn:
        conn.execute("DELETE FROM local_meta WHERE key='manual_policy_media_identity'")


@pytest.mark.parametrize("blocked,remaining", [({"tmdb:121"}, ["show"]), ({"tmdb:121#show"}, ["movie"])])
def test_manual_block_matches_only_its_media_type(blocked, remaining):
    assert [row["type"] for row in filter_manual_block([MOVIE, SHOW], blocked)] == remaining


@pytest.mark.parametrize("alias", ["tv", "series", "anime"])
def test_manual_block_treats_show_aliases_as_shows(alias):
    assert filter_manual_block([{**SHOW, "type": alias}], {"tmdb:121"}) == [{**SHOW, "type": alias}]


def test_legacy_policy_migrates_overrides_mappings_and_blocks(tmp_path):
    _legacy_db(tmp_path, {"version": 1, "providers": {"TRAKT": {"watchlist": {
        "adds": {"items": {"tmdb:121": SHOW, "tmdb:77": {"type": "show", "title": "X", "ids": {"tmdb": "77"}}}},
        "mappings": {"tmdb:77": {"original_key": "tmdb:5", "original": {"type": "show", "ids": {"tmdb": "5"}}}},
        "blocks": ["tmdb:5", "tmdb:999", "imdb:tt1"],
    }}}, "pairs": {"p1": {"version": 1, "providers": {"TRAKT": {"ratings": {"blocks": ["tmdb:42"]}}}}}})

    policy = sqlite_manual_policy.load_policy(tmp_path)
    node = policy["providers"]["TRAKT"]["watchlist"]
    assert set(node["adds"]["items"]) == {"tmdb:121#show", "tmdb:77#show"}
    assert node["mappings"] == {"tmdb:77#show": {"original_key": "tmdb:5#show", "original": {"type": "show", "ids": {"tmdb": "5"}}}}
    assert node["blocks"] == ["tmdb:5#show", "tmdb:999", "tmdb:999#show", "imdb:tt1"]
    assert policy["pairs"]["p1"]["providers"]["TRAKT"]["ratings"]["blocks"] == ["tmdb:42", "tmdb:42#show"]
    assert sqlite_manual_policy.load_policy(tmp_path)["providers"]["TRAKT"]["watchlist"]["blocks"] == node["blocks"]


def test_current_policy_is_not_migrated_again(tmp_path):
    sqlite_manual_policy.save_policy(tmp_path, {"version": 1, "providers": {"TRAKT": {"watchlist": {"blocks": ["tmdb:121"]}}}})
    assert sqlite_manual_policy.load_policy(tmp_path)["providers"]["TRAKT"]["watchlist"]["blocks"] == ["tmdb:121"]


def test_legacy_movie_override_keeps_its_key(tmp_path):
    _legacy_db(tmp_path, {"version": 1, "providers": {"TRAKT": {"ratings": {"adds": {"items": {"tmdb:121": MOVIE}}}}}})
    assert set(sqlite_manual_policy.load_policy(tmp_path)["providers"]["TRAKT"]["ratings"]["adds"]["items"]) == {"tmdb:121"}


def test_legacy_show_override_does_not_replace_movie():
    state = {"providers": {"TRAKT": {"manual": {"watchlist": {"adds": {"items": {"tmdb:121": {**SHOW, "title": "Fixed"}}}}}}}}
    adds, _ = manual_policy(state, "TRAKT", "watchlist")
    merged = merge_manual_adds({"tmdb:121": MOVIE, "tmdb:121#show": SHOW}, adds)
    assert {key: (row["type"], row["title"]) for key, row in merged.items()} == {
        "tmdb:121": ("movie", "The Two Towers"),
        "tmdb:121#show": ("show", "Fixed"),
    }


@pytest.mark.parametrize("original_key,original,target_key,target", [
    ("tmdb:121", MOVIE, "tmdb:121#show", SHOW),
    ("tmdb:121#show", SHOW, "tmdb:121", MOVIE),
])
def test_cross_type_correction_blocks_original(original_key, original, target_key, target):
    raw = {"version": 1, "providers": {}}
    records = {target_key: {"original_key": original_key, "original": copy.deepcopy(original)}}
    update_mappings(raw, [("watchlist", "TRAKT", {target_key: target}, [], "default")],
                    mappings={("watchlist", "TRAKT", "default"): records})
    blocks = effective_policy(raw)["providers"]["TRAKT"]["watchlist"]["blocks"]
    assert blocks == [original_key]
    assert filter_manual_block([MOVIE, SHOW], {key.lower() for key in blocks}) == [target]


def test_correcting_show_keeps_existing_movie_block():
    raw = {"version": 1, "providers": {"TRAKT": {"watchlist": {"blocks": ["tmdb:121"]}}}}
    records = {"tmdb:121#show": {"original_key": "tmdb:888", "original": {"type": "show"}}}
    update_mappings(raw, [("watchlist", "TRAKT", {"tmdb:121#show": SHOW}, [], "default")],
                    mappings={("watchlist", "TRAKT", "default"): records})
    assert raw["providers"]["TRAKT"]["watchlist"]["blocks"] == ["tmdb:121", "tmdb:888"]


def test_legacy_policy_document_migrates_items_and_bare_blocks():
    legacy = {"providers": {"TRAKT": {"watchlist": {"adds": {"items": {"tmdb:121": SHOW}}, "blocks": ["tmdb:9"]}}}}
    sqlite_manual_policy.migrate_media_policy(legacy)
    assert set(legacy["providers"]["TRAKT"]["watchlist"]["adds"]["items"]) == {"tmdb:121#show"}
    assert legacy["providers"]["TRAKT"]["watchlist"]["blocks"] == ["tmdb:9", "tmdb:9#show"]


def _bundle(version):
    return RuleBundle.model_validate({"format": "crosswatch-mappings-blocks", "version": version, "records": [
        {"provider": "TRAKT", "feature": "watchlist", "key": "tmdb:121", "entry_type": "mapping",
         "item": copy.deepcopy(SHOW), "original_key": "tmdb:5", "original": {"type": "show", "ids": {"tmdb": "5"}}},
        {"provider": "TRAKT", "feature": "watchlist", "key": "tmdb:9", "entry_type": "block"},
    ]})


def test_version_one_rule_bundle_is_migrated():
    rules = _typed_rules(_bundle(1))
    assert [(rule.entry_type, rule.key, rule.original_key) for rule in rules] == [
        ("mapping", "tmdb:121#show", "tmdb:5#show"),
        ("block", "tmdb:9", None),
        ("block", "tmdb:9#show", None),
    ]


def test_version_two_rule_bundle_is_kept():
    assert [(rule.key, rule.original_key) for rule in _typed_rules(_bundle(2))] == [("tmdb:121", "tmdb:5"), ("tmdb:9", None)]
