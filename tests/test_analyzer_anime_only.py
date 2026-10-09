# tests/test_analyzer_anime_only.py
# CrossWatch - Analyzer anime-only pair filtering tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json

import pytest

import services.analyzer as A
from cw_platform.anime_mapping import storage
from cw_platform.anime_mapping.overrides import upsert_override


@pytest.fixture()
def anime_mapping(config_base, tmp_path, monkeypatch):
    paths = storage.paths("v3")
    paths["root"].mkdir(parents=True, exist_ok=True)
    paths["mappings"].write_text(json.dumps({"tvdb_show:10:s1": {
        "anilist:100": {"1-10": "1-10"}, "mal:25777": {"1-10": "1-10"},
    }}), encoding="utf-8")
    paths["identity"].write_text("anidb\tmyanimelist\tanilist\tsimkl\tkitsu\n\t25777\t100\t\t1\n", encoding="utf-8")
    storage.rebuild_sqlite_from_mappings(release_tag="v3")
    monkeypatch.setattr(A, "CWS_DIR", tmp_path)
    return {"anime_mapping": {"enabled": True}}


def setup_pair(cfg, target, feature, mode="one-way"):
    cfg["pairs"] = [{"id": "anime", "source": "SIMKL", "source_instance": "SIMKL-P01",
                     "target": target, "mode": mode, "features": {feature: {"enable": True, "anime_only_sync": False}}}]
    if feature == "history":
        valid = {"type": "episode", "show_ids": {"tvdb": "10"}, "season": 1, "episode": 1}
        excluded = {**valid, "show_ids": {"tvdb": "999"}, "title": "1883"}
    else:
        valid = {"type": "movie", "ids": {("mal" if target == "MYANIMELIST" else target.lower()): "100" if target == "ANILIST" else "1"}}
        excluded = {"type": "movie", "ids": {"tmdb": "999"}, "title": "Live action"}
    items = {"valid": valid, "excluded": excluded}
    state = {"providers": {
        "SIMKL": {"instances": {"SIMKL-P01": {feature: {"baseline": {"items": items}}}}},
        target: {feature: {"baseline": {"items": {}}}},
    }}
    return state, items


@pytest.mark.parametrize("target", ["KITSU", "ANILIST", "MYANIMELIST"])
@pytest.mark.parametrize("feature", ["history", "watchlist", "ratings"])
@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_anime_only_findings_stats_and_exclusions(anime_mapping, target, feature, mode):
    state, items = setup_pair(anime_mapping, target, feature, mode)
    ctx = A._analysis_context(state, anime_mapping)
    source = "SIMKL@SIMKL-P01"
    assert A._missing_targets(ctx, source, feature, "excluded", items["excluded"]) == []
    assert A._missing_targets(ctx, source, feature, "valid", items["valid"]) == [target]
    stats = next(row for row in A._pair_stats(state, ctx=ctx) if row["source"] == source)
    assert (stats["total"], stats["synced"], stats["unsynced"]) == (1, 0, 1)
    exclusions = A._pair_exclusions(state, ctx=ctx)
    assert len(exclusions) == 1
    assert exclusions[0]["excluded_anime_only"] == 1
    assert exclusions[0]["excluded_total"] == 1
    assert exclusions[0]["accepted_total"] == 1
    problems = A._problems(state, None, cfg=anime_mapping, ctx=ctx, include_system=False, include_hints=False)
    assert [row["key"] for row in problems if row["type"] == "missing_peer"] == ["valid"]


@pytest.mark.parametrize("target", ["KITSU", "ANILIST", "MYANIMELIST"])
def test_custom_episode_mapping_is_eligible(anime_mapping, target):
    state, items = setup_pair(anime_mapping, target, "history")
    upsert_override({"media_type": "show", "match_provider": "tvdb", "match_id": "999", "match_season": 1,
                     "episode_from": 1, "episode_to": 10, "episode_start_at": 1,
                     "target_namespace": ("mal" if target == "MYANIMELIST" else target.lower()), "target_id": "100" if target == "ANILIST" else "1"})
    ctx = A._analysis_context(state, anime_mapping)
    assert A._missing_targets(ctx, "SIMKL@SIMKL-P01", "history", "excluded", items["excluded"]) == [target]
    assert A._pair_exclusions(state, ctx=ctx) == []


@pytest.mark.parametrize("target", ["KITSU", "ANILIST", "MYANIMELIST"])
@pytest.mark.parametrize("feature", ["watchlist", "ratings"])
def test_custom_title_mapping_is_eligible_without_native_snapshot_id(anime_mapping, target, feature):
    state, items = setup_pair(anime_mapping, target, feature)
    upsert_override({"media_type": "movie", "match_provider": "tmdb", "match_id": "999",
                     "target_namespace": ("mal" if target == "MYANIMELIST" else target.lower()), "target_id": "100" if target == "ANILIST" else "1"})
    ctx = A._analysis_context(state, anime_mapping)
    assert A._missing_targets(ctx, "SIMKL@SIMKL-P01", feature, "excluded", items["excluded"]) == [target]
    assert items["excluded"]["ids"] == {"tmdb": "999"}


@pytest.mark.parametrize("target", ["KITSU", "ANILIST", "MYANIMELIST"])
def test_present_anime_is_counted_as_synced(anime_mapping, target):
    state, items = setup_pair(anime_mapping, target, "ratings")
    state["providers"][target]["ratings"]["baseline"]["items"] = {"valid": dict(items["valid"])}
    ctx = A._analysis_context(state, anime_mapping)
    stats = A._pair_stats(state, ctx=ctx)[0]
    assert (stats["total"], stats["synced"], stats["unsynced"]) == (1, 1, 0)


def test_anime_filter_is_scoped_to_target_and_cached(anime_mapping, monkeypatch):
    state, items = setup_pair(anime_mapping, "KITSU", "ratings")
    anime_mapping["pairs"].append({**anime_mapping["pairs"][0], "id": "regular", "target": "TRAKT"})
    state["providers"]["TRAKT"] = {"ratings": {"baseline": {"items": {}}}}
    original = A.anime_only_adds
    calls = []

    def counted(rows, *args, **kwargs):
        calls.append(len(rows))
        return original(rows, *args, **kwargs)

    monkeypatch.setattr(A, "anime_only_adds", counted)
    ctx = A._analysis_context(state, anime_mapping)
    for _ in range(2):
        assert A._missing_targets(ctx, "SIMKL@SIMKL-P01", "ratings", "excluded", items["excluded"]) == ["TRAKT"]
        A._pair_stats(state, ctx=ctx)
        A._pair_exclusions(state, ctx=ctx)
    assert calls == [2]


@pytest.mark.parametrize("target", ["KITSU", "ANILIST", "MYANIMELIST"])
def test_two_way_reverse_route_still_reports_missing_anime(anime_mapping, target):
    state, items = setup_pair(anime_mapping, target, "ratings", "two-way")
    state["providers"][target]["ratings"]["baseline"]["items"] = {"other": {
        "type": "movie", "ids": {("mal" if target == "MYANIMELIST" else target.lower()): "500"}}}
    ctx = A._analysis_context(state, anime_mapping)
    row = state["providers"][target]["ratings"]["baseline"]["items"]["other"]
    assert A._missing_targets(ctx, target, "ratings", "other", row) == ["SIMKL@SIMKL-P01"]


@pytest.mark.parametrize("target", ["KITSU", "ANILIST", "MYANIMELIST"])
@pytest.mark.parametrize("direction", ["forward", "reverse", "two-way"])
def test_history_health_excludes_non_anime(anime_mapping, target, direction):
    state, items = setup_pair(anime_mapping, target, "history")
    items.update({f"live-{i}": {**items["excluded"], "show_ids": {"tvdb": str(1000 + i)}} for i in range(20)})
    state["providers"][target]["history"]["baseline"]["items"] = {"valid": dict(items["valid"])}
    pair = anime_mapping["pairs"][0]
    if direction == "reverse":
        pair.update(source=target, source_instance="default", target="SIMKL", target_instance="SIMKL-P01")
    elif direction == "two-way":
        pair["mode"] = "two-way"
    problems = A._problems(state, cfg=anime_mapping, include_system=False, include_hints=False)
    assert not [row for row in problems if row["type"] == "history_show_normalization"]


@pytest.mark.parametrize("target", ["KITSU", "ANILIST", "MYANIMELIST"])
def test_history_health_keeps_real_anime_gaps_and_regular_pair_gaps(anime_mapping, target):
    state, items = setup_pair(anime_mapping, target, "history")
    items.update({f"live-{i}": {**items["excluded"], "show_ids": {"tvdb": str(1000 + i)}} for i in range(20)})
    anime_mapping["pairs"].append({**anime_mapping["pairs"][0], "id": "regular", "target": "TRAKT"})
    state["providers"]["TRAKT"] = {"history": {"baseline": {"items": {}}}}
    issues = A._history_normalization_issues(state, anime_mapping)
    by_target = {row["target"]: row for row in issues}
    assert by_target["TRAKT"]["show_delta"]["source"] == 22
    assert target not in by_target
    upsert_override({"media_type": "show", "match_provider": "tvdb", "match_id": "999", "match_season": 1,
                     "episode_from": 1, "episode_to": 10, "episode_start_at": 1,
                     "target_namespace": ("mal" if target == "MYANIMELIST" else target.lower()), "target_id": "100" if target == "ANILIST" else "1"})
    issues = A._history_normalization_issues(state, anime_mapping)
    gap = next(row for row in issues if row["target"] == target)
    assert gap["show_delta"] == {"source": 2, "target": 0}


@pytest.mark.parametrize("target", ["KITSU", "ANILIST", "MYANIMELIST"])
@pytest.mark.parametrize("feature", ["ratings", "watchlist"])
@pytest.mark.parametrize("namespace,ident", [("mal", "25777"), ("anilist", "100"), ("kitsu", "1")])
def test_health_accepts_mapped_anime_ids(anime_mapping, target, feature, namespace, ident):
    state, items = setup_pair(anime_mapping, target, feature)
    items["valid"] = {"type": "show", "title": "Anime", "ids": {namespace: ident}}
    state["providers"][target][feature]["baseline"]["items"] = {"valid": dict(items["valid"])}
    problems = A._problems(state, cfg=anime_mapping, include_system=False, include_hints=False)
    assert not [row for row in problems if row["type"] == "key_missing_ids"]
    assert items["valid"]["ids"] == {namespace: ident}


@pytest.mark.parametrize("target", ["KITSU", "ANILIST", "MYANIMELIST"])
def test_health_keeps_unusable_id_findings(anime_mapping, target):
    state, items = setup_pair(anime_mapping, target, "ratings")
    items["valid"] = {"type": "show", "ids": {"simkl": "123"}}
    problems = A._problems(state, cfg=anime_mapping, include_system=False, include_hints=False)
    assert any(row["type"] == "key_missing_ids" and row["key"] == "valid" for row in problems)
    items["valid"]["ids"] = {"mal": "25777"}
    anime_mapping["pairs"].append({**anime_mapping["pairs"][0], "id": "regular", "target": "TRAKT"})
    state["providers"]["TRAKT"] = {"ratings": {"baseline": {"items": {}}}}
    problems = A._problems(state, cfg=anime_mapping, include_system=False, include_hints=False)
    assert any(row["type"] == "key_missing_ids" and row["key"] == "valid" for row in problems)
