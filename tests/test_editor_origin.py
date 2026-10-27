# tests/test_editor_origin.py
# CrossWatch - Editor item origin through anime mapping and custom mappings
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import pytest

from cw_platform.anime_mapping import overrides, storage
from services.editor_origin import feeding_pairs, trace

MAPPINGS: dict[str, Any] = {
    "anilist:813": {"tmdb_show:12971:s1": {"1-39": "1-39"}, "tvdb_show:81472:s1": {"1-39": "1-39"}},
    "anilist:4181": {"tmdb_show:24835:s0": {"1-2": "3-4"}, "tmdb_show:24835:s2": {"3-24": "1-22"}},
    "anilist:2167": {"tmdb_show:24835:s1": {"1-23": "1-23"}},
    "anilist:9001": {"tmdb_movie:603": {}},
    "anilist:100": {"tmdb_show:10:s1": {"1-12": "1-12"}},
    "anilist:200": {"tmdb_show:10:s2": {"1-12": "1-12"}},
    "mal:300": {"tmdb_show:900": {}, "anilist:700": {}},
    "anilist:800": {"tvdb_movie:77": {}},
    "anilist:1100": {"tmdb_show:1010": {}},
    "anilist:1200": {"mal:1300": {"1-12": "1-12"}},
    "anilist:5100": {"mal:5300": {"1-5": "1-5"}},
    "anilist:5200": {"mal:5300": {"1-12": "1-12"}},
}
IDENTITY_TSV = "anidb\tmyanimelist\tanilist\tsimkl\tkitsu\n\t\t100\t\t600\n\t\t1100\t\t1600\n"

CFG = {"anime_mapping": {"enabled": True, "release_tag": "v3"}}
TRAKT = {"pair_id": "p1", "pair_label": "Trakt → AniList", "provider": "TRAKT", "instance": "default"}


@pytest.fixture()
def anime_index(config_base: Path) -> Path:
    paths = storage.paths("v3")
    paths["root"].mkdir(parents=True, exist_ok=True)
    paths["mappings"].write_text(json.dumps(MAPPINGS), encoding="utf-8")
    paths["identity"].write_text(IDENTITY_TSV, encoding="utf-8")
    storage.rebuild_sqlite_from_mappings(release_tag="v3")
    overrides.invalidate_cache()
    yield paths["db"]
    storage.close_readers()
    overrides.invalidate_cache()


def _episode(show_ids: dict[str, str], title: str, season: int, episode: int) -> dict[str, Any]:
    return {"type": "episode", "series_title": title, "title": f"E{episode}", "season": season,
            "episode": episode, "ids": {}, "show_ids": dict(show_ids)}


def _rule(**fields: Any) -> dict[str, Any]:
    return overrides.upsert_override({"media_type": "show", "target_namespace": "anilist", **fields})


def test_anilist_watchlist_row_traces_to_trakt_show(anime_index: Path) -> None:
    row = {"type": "anime", "title": "Dragon Ball Z", "ids": {"anilist": "813", "mal": "813"}}
    source = {"a": {"type": "show", "title": "Dragon Ball Z", "year": 1989, "ids": {"tmdb": "12971"}},
              "b": {"type": "show", "title": "Naruto", "ids": {"tmdb": "46260"}}}
    out = trace(CFG, row, [(TRAKT, source, {})], provider="ANILIST")
    assert out["anime"][0] == {"namespace": "anilist", "id": "813", "via": "direct", "episodes": []}
    assert len(out["sources"]) == 1
    group = out["sources"][0]
    assert group["title"] == "Dragon Ball Z"
    assert group["via"] == "mapping"
    assert group["link"]["namespace"] == "tmdb" and group["link"]["id"] == "12971"
    assert group["anime"] == {"namespace": "anilist", "id": "813"}
    assert group["kind_mismatch"] is False
    assert {(link["namespace"], link["id"]) for link in out["links"]} == {("tmdb", "12971"), ("tvdb", "81472")}


def test_history_row_in_tmdb_coordinates_groups_episodes_per_season(anime_index: Path) -> None:
    row = _episode({"tmdb": "24835"}, "Clannad After Story", 2, 5)
    source = {f"s2e{n}": _episode({"tmdb": "24835"}, "Clannad", 2, n) for n in range(1, 4)}
    source["s1e1"] = _episode({"tmdb": "24835"}, "Clannad", 1, 1)
    source["s0e3"] = _episode({"tmdb": "24835"}, "Clannad", 0, 3)
    out = trace(CFG, row, [(TRAKT, source, {})], provider="ANILIST")
    assert out["anime"] == [{"namespace": "anilist", "id": "4181", "via": "mapping", "episodes": [7]}]
    seasons = {g["season"]: g for g in out["sources"]}
    assert set(seasons) == {0, 2}
    assert seasons[2]["count"] == 3
    assert (seasons[2]["first_episode"], seasons[2]["last_episode"]) == (1, 3)
    assert seasons[0]["count"] == 1


def test_movie_mapping_matches_movie_source(anime_index: Path) -> None:
    row = {"type": "movie", "title": "Some Anime Movie", "ids": {"anilist": "9001"}}
    source = {"m": {"type": "movie", "title": "Movie 603", "ids": {"tmdb": "603"}}}
    out = trace(CFG, row, [(TRAKT, source, {})], provider="ANILIST")
    assert [(g["title"], g["kind_mismatch"]) for g in out["sources"]] == [("Movie 603", False)]


def test_show_sharing_a_movie_tmdb_number_is_not_a_source(anime_index: Path) -> None:
    row = {"type": "movie", "title": "Some Anime Movie", "ids": {"anilist": "9001"}}
    source = {"s": {"type": "show", "title": "Show 603", "ids": {"tmdb": "603"}}}
    assert trace(CFG, row, [(TRAKT, source, {})], provider="ANILIST")["sources"] == []


def test_unknown_kind_sharing_a_movie_tmdb_number_is_flagged(anime_index: Path) -> None:
    row = {"type": "movie", "title": "Some Anime Movie", "ids": {"anilist": "9001"}}
    source = {"a": {"type": "anime", "title": "Anime 603", "ids": {"tmdb": "603"}}}
    out = trace(CFG, row, [(TRAKT, source, {})], provider="ANILIST")
    assert len(out["sources"]) == 1
    group = out["sources"][0]
    assert group["kind_mismatch"] is True
    assert group["link"]["kind"] == "movie"


def test_custom_episode_rule_wins_and_is_reported(anime_index: Path) -> None:
    _rule(match_provider="tmdb", match_id="12971", match_season=1, episode_from=1, episode_to=39,
          episode_start_at=1, target_id="2167", title="Wrong rule")
    row = {"type": "anime", "title": "Clannad", "ids": {"anilist": "2167"}}
    source = {"e1": _episode({"tmdb": "12971"}, "Dragon Ball Z", 1, 1)}
    out = trace(CFG, row, [(TRAKT, source, {})], provider="ANILIST")
    assert len(out["sources"]) == 1
    group = out["sources"][0]
    assert group["via"] == "custom"
    assert group["rule_id"]
    assert group["link"]["namespace"] == "tmdb" and group["link"]["id"] == "12971"
    assert any(link["via"] == "custom" for link in out["links"])


def test_custom_rule_wins_over_direct_id_like_the_writer(anime_index: Path) -> None:
    _rule(match_provider="tmdb", match_id="10", target_id="200")
    source = {"x": {"type": "show", "title": "Ten", "ids": {"tmdb": "10", "anilist": "100"}}}
    ruled = trace(CFG, {"type": "anime", "title": "Two hundred", "ids": {"anilist": "200"}}, [(TRAKT, source, {})], provider="ANILIST")
    assert [(g["title"], g["via"]) for g in ruled["sources"]] == [("Ten", "custom")]
    direct = trace(CFG, {"type": "anime", "title": "One hundred", "ids": {"anilist": "100"}}, [(TRAKT, source, {})], provider="ANILIST")
    assert direct["sources"] == []


def test_identity_only_source_is_found(anime_index: Path) -> None:
    row = {"type": "anime", "title": "One hundred", "ids": {"anilist": "100"}}
    source = {"k": {"type": "anime", "title": "Kitsu entry", "ids": {"kitsu": "600"}}}
    out = trace(CFG, row, [(TRAKT, source, {})], provider="ANILIST")
    assert [(g["title"], g["via"], g["link"], g["anime"]) for g in out["sources"]] == [
        ("Kitsu entry", "direct", {"namespace": "kitsu", "id": "600"}, {"namespace": "anilist", "id": "100"})]
    out = trace(CFG, {"type": "anime", "title": "Kitsu row", "ids": {"kitsu": "600"}}, [(TRAKT, source, {})], provider="KITSU")
    assert [(g["title"], g["via"], g["anime"]) for g in out["sources"]] == [
        ("Kitsu entry", "direct", {"namespace": "kitsu", "id": "600"})]


def test_anime_match_uses_the_row_providers_namespace(anime_index: Path) -> None:
    source = {"x": {"type": "show", "title": "Ten", "ids": {"tmdb": "10", "anilist": "100"}}}
    _rule(match_provider="tmdb", match_id="10", target_id="200")
    kitsu = trace(CFG, {"type": "anime", "title": "Kitsu 600", "ids": {"kitsu": "600"}}, [(TRAKT, source, {})],
                  provider="KITSU")
    assert [g["anime"] for g in kitsu["sources"]] == [{"namespace": "kitsu", "id": "600"}]
    anilist = trace(CFG, {"type": "anime", "title": "One hundred", "ids": {"anilist": "100"}}, [(TRAKT, source, {})],
                    provider="ANILIST")
    assert anilist["sources"] == []


def test_ambiguous_whole_show_mapping_is_not_reported_as_a_source(anime_index: Path) -> None:
    row = {"type": "anime", "title": "One hundred", "ids": {"anilist": "100"}}
    source = {"show": {"type": "show", "title": "Ten", "ids": {"tmdb": "10"}},
              "s1e3": _episode({"tmdb": "10"}, "Ten", 1, 3),
              "s2e3": _episode({"tmdb": "10"}, "Ten", 2, 3)}
    out = trace(CFG, row, [(TRAKT, source, {})], provider="ANILIST")
    assert [(g["kind"], g["season"], g["count"]) for g in out["sources"]] == [("show", 1, 1)]


def test_custom_rule_attribution_respects_media_type(anime_index: Path) -> None:
    movie = _rule(media_type="movie", match_provider="tmdb", match_id="10", target_id="100")
    show = _rule(match_provider="tmdb", match_id="10", target_id="100")
    row = {"type": "anime", "title": "One hundred", "ids": {"anilist": "100"}}
    source = {"x": {"type": "show", "title": "Ten", "ids": {"tmdb": "10"}}}
    out = trace(CFG, row, [(TRAKT, source, {})], provider="ANILIST")
    assert [g["rule_id"] for g in out["sources"]] == [show["id"]]
    assert movie["id"] != show["id"]


def test_custom_movie_rule_traces_both_ways(anime_index: Path) -> None:
    rule = _rule(media_type="movie", match_provider="tmdb", match_id="777", target_id="5555")
    source = {"m": {"type": "movie", "title": "Some Movie", "ids": {"tmdb": "777"}}}
    for row in ({"type": "movie", "title": "AniList movie", "ids": {"anilist": "5555"}},
                {"type": "movie", "title": "Some Movie", "ids": {"tmdb": "777"}}):
        out = trace(CFG, row, [(TRAKT, source, {})], provider="ANILIST")
        assert [(a["namespace"], a["id"]) for a in out["anime"]] == [("anilist", "5555")]
        assert [(g["via"], g["rule_id"], g["link"]["kind"]) for g in out["sources"]] == [("custom", rule["id"], "movie")]
    forward = trace(CFG, {"type": "movie", "title": "Some Movie", "ids": {"tmdb": "777"}}, [])
    assert forward["anime"][0]["via"] == "custom"


def test_many_unrelated_rules_stay_fast(anime_index: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    rules = [overrides.normalize_override({"media_type": "show", "match_provider": "tmdb", "match_id": str(90000 + n),
                                           "match_season": 1, "episode_from": 1, "episode_start_at": 1,
                                           "target_namespace": "anilist", "target_id": "1"}) for n in range(5000)]
    monkeypatch.setattr("services.editor_origin.load_overrides", lambda: rules)
    row = {"type": "anime", "title": "Dragon Ball Z", "ids": {"anilist": "813"}}
    source = {f"e{n}": _episode({"tmdb": "12971"}, "Dragon Ball Z", 1, (n % 39) + 1) for n in range(1000)}
    started = time.perf_counter()
    out = trace(CFG, row, [(TRAKT, source, {})], provider="ANILIST")
    assert time.perf_counter() - started < 1.0
    assert out["sources"][0]["count"] == 1000


def test_no_source_match_still_returns_mapping_links(anime_index: Path) -> None:
    row = {"type": "anime", "title": "Dragon Ball Z", "ids": {"anilist": "813"}}
    out = trace(CFG, row, [(TRAKT, {"x": {"type": "show", "title": "Other", "ids": {"tmdb": "1"}}}, {})], provider="ANILIST")
    assert out["sources"] == []
    assert out["links_total"] == 2
    assert out["mapping_ready"] is True


def test_item_origin_reads_source_state_of_feeding_pairs(anime_index: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import api.editorAPI as editor_api
    from services.editor_origin import OriginRequest, item_origin

    cfg = {**CFG, "pairs": [{"id": "p1", "name": "Trakt to AniList", "source": "TRAKT", "target": "ANILIST",
                             "mode": "one-way", "features": {"watchlist": {"enable": True}}}]}
    loaded: list[tuple[str, str, str]] = []

    def load_items(kind: str, provider: str, instance: str | None = None, raw_state: Any = None) -> dict[str, Any]:
        loaded.append((kind, provider, str(instance)))
        return {"a": {"type": "show", "title": "Dragon Ball Z", "ids": {"tmdb": "12971"}}}

    monkeypatch.setattr(editor_api, "load_config", lambda: cfg)
    monkeypatch.setattr(editor_api, "_require_instance_scope", lambda *a, **k: None)
    monkeypatch.setattr(editor_api, "_load_current_state_features", lambda features: {})
    monkeypatch.setattr(editor_api, "_load_state_items", load_items)
    monkeypatch.setattr(editor_api, "_load_policy", lambda: {})
    monkeypatch.setattr("services.editor_origin.request_user", lambda request: None)

    payload = OriginRequest(provider="anilist", feature="watchlist",
                            original={"type": "anime", "title": "Dragon Ball Z", "ids": {"anilist": "813"}})
    out = item_origin(payload, None)
    assert loaded == [("watchlist", "TRAKT", "default")]
    assert out["pairs"][0]["pair_label"] == "Trakt to AniList"
    assert [g["title"] for g in out["sources"]] == ["Dragon Ball Z"]


def test_feeding_pairs_respects_direction_and_feature() -> None:
    cfg = {"pairs": [
        {"id": "a", "source": "TRAKT", "target": "ANILIST", "mode": "one-way", "features": {"watchlist": {"enable": True}}},
        {"id": "b", "source": "ANILIST", "target": "SIMKL", "mode": "one-way", "features": {"watchlist": {"enable": True}}},
        {"id": "c", "source": "ANILIST", "target": "PLEX", "mode": "two-way", "features": {"watchlist": True}},
        {"id": "d", "source": "SIMKL", "target": "ANILIST", "mode": "one-way", "features": {"watchlist": {"enable": False}}},
    ]}
    pairs = feeding_pairs(cfg, None, "ANILIST", "default", "watchlist")
    assert [(p["pair_id"], p["provider"]) for p in pairs] == [("a", "TRAKT"), ("c", "PLEX")]


PLEX = {"pair_id": "p1", "pair_label": "Plex → Trakt", "provider": "PLEX", "instance": "default"}
WRONG = {"type": "movie", "title": "Masters of the Universe", "year": 1987, "ids": {"tmdb": "111"}}
RIGHT = {"type": "movie", "title": "Masters of the Universe", "year": 2026, "ids": {"tmdb": "222"}}


def _fix_policy(scope: str = "p1", original_key: str | None = "tmdb:111") -> dict[str, Any]:
    record: dict[str, Any] = {"saved_at": 1}
    if original_key:
        record.update(original_key=original_key, original=WRONG)
    node = {"PLEX": {"watchlist": {"adds": {"items": {"tmdb:222": RIGHT}}, "mappings": {"tmdb:222": record},
                                   "blocks": [original_key] if original_key else []}}}
    if scope == "shared":
        return {"version": 1, "providers": node}
    return {"version": 1, "providers": {}, "pairs": {scope: {"version": 1, "providers": node}}}


def _corrected(policy: dict[str, Any], items: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    from services.editor_origin import corrected_source, fix_index
    return corrected_source(items, policy, fix_index(policy), "p1", "PLEX", "default", "watchlist")


def test_plain_rows_match_same_ids_and_keep_movies_and_shows_apart(anime_index: Path) -> None:
    row = {"type": "movie", "title": "The Matrix", "ids": {"tmdb": "603"}}
    source = {"m": {"type": "movie", "title": "The Matrix", "ids": {"tmdb": "603", "imdb": "tt0133093"}},
              "s": {"type": "show", "title": "Show 603", "ids": {"tmdb": "603"}}}
    out = trace(CFG, row, [(PLEX, source, {})])
    assert out["mode"] == "ids"
    assert [(g["title"], g["via"], g["link"]["namespace"], g["link"]["id"]) for g in out["sources"]] == [
        ("The Matrix", "ids", "tmdb", "603")]


def test_plain_episode_rows_match_the_same_episode_only(anime_index: Path) -> None:
    row = _episode({"tmdb": "1399"}, "Game of Thrones", 1, 2)
    source = {"a": _episode({"tmdb": "1399"}, "Game of Thrones", 1, 2),
              "b": _episode({"tmdb": "1399"}, "Game of Thrones", 1, 3)}
    out = trace(CFG, row, [(PLEX, source, {})])
    assert [(g["season"], g["first_episode"], g["count"]) for g in out["sources"]] == [(1, 2, 1)]


def test_saved_fix_is_applied_to_sources_and_reported(anime_index: Path) -> None:
    items, tags = _corrected(_fix_policy(), {"tmdb:111": WRONG})
    assert "tmdb:111" not in items
    out = trace(CFG, {"type": "movie", "title": "Masters", "ids": {"tmdb": "222"}}, [(PLEX, items, tags)])
    group = out["sources"][0]
    assert group["year"] == 2026
    assert group["fix"]["scope"] == "pair"
    assert group["fix"]["original_key"] == "tmdb:111"
    assert group["fix"]["original"]["ids"] == {"tmdb": "111"}
    blocked = trace(CFG, {"type": "movie", "title": "Masters", "ids": {"tmdb": "111"}}, [(PLEX, items, tags)])
    assert blocked["sources"] == []


def test_shared_fix_and_manual_add_are_labelled(anime_index: Path) -> None:
    _, shared = _corrected(_fix_policy("shared"), {"tmdb:111": WRONG})
    assert shared["tmdb:222"]["scope"] == "shared"
    _, added = _corrected(_fix_policy("p1", None), {})
    assert added["tmdb:222"]["original_key"] is None and added["tmdb:222"]["original"] is None


def test_fix_for_another_pair_is_not_applied(anime_index: Path) -> None:
    from services.editor_origin import corrected_source, fix_index
    policy = _fix_policy("p2")
    items, tags = corrected_source({"tmdb:111": WRONG}, policy, fix_index(policy), "p1", "PLEX", "default", "watchlist")
    assert list(items) == ["tmdb:111"] and tags == {}


def test_row_fix_is_found_for_the_editor_row(anime_index: Path) -> None:
    from services.editor_origin import find_fix, fix_index
    policy = _fix_policy()
    fixes = fix_index(policy)
    assert find_fix(fixes, "p1", "PLEX", "default", "watchlist", "TMDB:222")["original_key"] == "tmdb:111"
    assert find_fix(fixes, "", "PLEX", "default", "watchlist", "tmdb:222") is None


def test_item_origin_applies_fixes_for_plain_providers(anime_index: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import api.editorAPI as editor_api
    from services.editor_origin import OriginRequest, item_origin

    cfg = {**CFG, "pairs": [{"id": "p1", "name": "Plex to Trakt", "source": "PLEX", "target": "TRAKT",
                             "mode": "one-way", "features": {"watchlist": {"enable": True}}}]}
    policy = _fix_policy("shared")
    policy["providers"]["TRAKT"] = {"watchlist": {"adds": {"items": {"tmdb:222": RIGHT}},
                                                  "mappings": {"tmdb:222": {"original_key": "tmdb:333", "original": WRONG}},
                                                  "blocks": ["tmdb:333"]}}
    monkeypatch.setattr(editor_api, "load_config", lambda: cfg)
    monkeypatch.setattr(editor_api, "_require_instance_scope", lambda *a, **k: None)
    monkeypatch.setattr(editor_api, "_load_current_state_features", lambda features: {})
    monkeypatch.setattr(editor_api, "_load_state_items", lambda *a, **k: {"tmdb:111": WRONG})
    monkeypatch.setattr(editor_api, "_load_policy", lambda: policy)
    monkeypatch.setattr("services.editor_origin.request_user", lambda request: None)

    payload = OriginRequest(provider="trakt", feature="watchlist", key="tmdb:222", original=RIGHT)
    out = item_origin(payload, None)
    assert out["mode"] == "ids"
    assert [(g["provider"], g["fix"]["original_key"]) for g in out["sources"]] == [("PLEX", "tmdb:111")]
    assert out["row_fix"]["original_key"] == "tmdb:333"


def test_rule_priority_follows_stored_order_not_item_id_order(anime_index: Path) -> None:
    first = _rule(match_provider="tmdb", match_id="31", target_id="100")
    _rule(match_provider="tvdb", match_id="32", target_id="200")
    row = {"type": "anime", "title": "One hundred", "ids": {"anilist": "100"}}
    source = {"show": {"type": "show", "title": "Show", "ids": {"tvdb": "32", "tmdb": "31"}},
              "ep": _episode({"tvdb": "32", "tmdb": "31"}, "Show", 1, 1)}
    out = trace(CFG, row, [(TRAKT, source, {})], provider="ANILIST")
    assert sorted((g["kind"], g["via"], g["rule_id"]) for g in out["sources"]) == [
        ("show", "custom", first["id"]), ("show", "custom", first["id"])]


def test_multi_hop_mapping_source_is_found(anime_index: Path) -> None:
    row = {"type": "anime", "title": "Seven hundred", "ids": {"anilist": "700"}}
    source = {"show": {"type": "show", "title": "Nine hundred", "ids": {"tmdb": "900"}},
              "mal": {"type": "anime", "title": "MAL only", "ids": {"mal": "300"}}}
    out = trace(CFG, row, [(TRAKT, source, {})], provider="ANILIST")
    found = {g["title"]: g for g in out["sources"]}
    assert set(found) == {"Nine hundred", "MAL only"}
    assert found["Nine hundred"]["via"] == "mapping"
    assert found["MAL only"]["link"] == {"namespace": "mal", "id": "300", "season": None, "kind": "show"}


def test_collision_warning_uses_the_id_that_produced_the_match(anime_index: Path) -> None:
    row = {"type": "anime", "title": "Eight hundred", "ids": {"anilist": "800"}}
    shown = {"x": {"type": "show", "title": "Mixed", "ids": {"tmdb": "999", "tvdb": "77"}}}
    assert trace(CFG, row, [(TRAKT, shown, {})], provider="ANILIST")["sources"] == []
    source = {"x": {"type": "anime", "title": "Mixed", "ids": {"tmdb": "999", "tvdb": "77"}}}
    out = trace(CFG, row, [(TRAKT, source, {})], provider="ANILIST")
    group = out["sources"][0]
    assert group["link"]["namespace"] == "tvdb" and group["link"]["id"] == "77"
    assert group["link"]["kind"] == "movie"
    assert group["kind_mismatch"] is True


def test_season_bound_rule_does_not_leak_into_other_seasons(anime_index: Path) -> None:
    rule = _rule(match_provider="tmdb", match_id="10", match_season=2, episode_from=1, episode_to=12,
                 episode_start_at=1, target_id="250")
    source = {"s1e1": _episode({"tmdb": "10"}, "Ten", 1, 1), "s2e1": _episode({"tmdb": "10"}, "Ten", 2, 1)}
    mapped = trace(CFG, {"type": "anime", "title": "One hundred", "ids": {"anilist": "100"}},
                   [(TRAKT, source, {})], provider="ANILIST")
    assert [(g["season"], g["via"]) for g in mapped["sources"]] == [(1, "mapping")]
    ruled = trace(CFG, {"type": "anime", "title": "Two fifty", "ids": {"anilist": "250"}},
                  [(TRAKT, source, {})], provider="ANILIST")
    assert [(g["season"], g["via"], g["rule_id"]) for g in ruled["sources"]] == [(2, "custom", rule["id"])]


def test_plain_match_normalizes_guid_ids_like_sync(anime_index: Path) -> None:
    row = {"type": "movie", "title": "The Matrix", "ids": {"tmdb": "603"}}
    source = {"p": {"type": "movie", "title": "The Matrix", "ids": {"guid": "com.plexapp.agents.themoviedb://603?lang=en"}}}
    out = trace(CFG, row, [(PLEX, source, {})])
    assert [(g["title"], g["via"], g["link"]["id"]) for g in out["sources"]] == [("The Matrix", "ids", "603")]


def test_kitsu_entry_matches_itself_without_identity_table(anime_index: Path) -> None:
    row = {"type": "show", "title": "Kitsu only", "ids": {"kitsu": "4242"}}
    source = {"k": {"type": "show", "title": "Kitsu only", "ids": {"kitsu": "4242"}}}
    out = trace(CFG, row, [(TRAKT, source, {})], provider="KITSU")
    assert out["anime"] == [{"namespace": "kitsu", "id": "4242", "via": "direct", "episodes": []}]
    assert [(g["via"], g["anime"]) for g in out["sources"]] == [("direct", {"namespace": "kitsu", "id": "4242"})]


def test_truncated_mapping_search_is_reported(anime_index: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    row = {"type": "anime", "title": "Dragon Ball Z", "ids": {"anilist": "813"}}
    assert trace(CFG, row, [], provider="ANILIST")["search_truncated"] is False
    monkeypatch.setattr("services.editor_origin.REACH_LIMIT", 1)
    assert trace(CFG, row, [], provider="ANILIST")["search_truncated"] is True


def test_kitsu_source_needs_the_writers_second_enrichment_pass(anime_index: Path) -> None:
    row = {"type": "show", "title": "Kitsu 1600", "ids": {"kitsu": "1600"}}
    source = {"s": {"type": "show", "title": "Ten ten", "ids": {"tmdb": "1010"}}}
    out = trace(CFG, row, [(TRAKT, source, {})], provider="KITSU")
    assert [(g["title"], g["anime"]) for g in out["sources"]] == [("Ten ten", {"namespace": "kitsu", "id": "1600"})]


def test_mal_episode_override_is_converted_to_the_mal_entry(anime_index: Path) -> None:
    rule = _rule(match_provider="tmdb", match_id="2000", match_season=1, episode_from=1, episode_to=12,
                 episode_start_at=1, target_id="1200")
    row = {"type": "show", "title": "MAL 1300", "ids": {"mal": "1300"}}
    source = {"e": _episode({"tmdb": "2000"}, "Two thousand", 1, 1)}
    out = trace(CFG, row, [(TRAKT, source, {})], provider="MYANIMELIST")
    assert [(g["via"], g["rule_id"], g["anime"]) for g in out["sources"]] == [
        ("custom", rule["id"], {"namespace": "mal", "id": "1300"})]


def test_overlapping_episode_rules_follow_the_writers_choice(anime_index: Path) -> None:
    _rule(match_provider="tmdb", match_id="31", match_season=1, episode_from=1, episode_to=12, episode_start_at=1,
          target_id="100")
    _rule(match_provider="tvdb", match_id="32", match_season=1, episode_from=1, episode_to=12, episode_start_at=1,
          target_id="200")
    source = {"e": _episode({"tmdb": "31", "tvdb": "32"}, "Both", 1, 1)}
    chosen = trace(CFG, {"type": "anime", "title": "One hundred", "ids": {"anilist": "100"}},
                   [(TRAKT, source, {})], provider="ANILIST")
    assert [g["via"] for g in chosen["sources"]] == ["custom"]
    other = trace(CFG, {"type": "anime", "title": "Two hundred", "ids": {"anilist": "200"}},
                  [(TRAKT, source, {})], provider="ANILIST")
    assert other["sources"] == []


def test_anilist_row_accepts_a_source_that_falls_back_to_its_mal_id(anime_index: Path) -> None:
    row = {"type": "anime", "title": "Forty one hundred", "ids": {"anilist": "4100", "mal": "4300"}}
    source = {"m": {"type": "show", "title": "MAL only", "ids": {"mal": "4300"}}}
    out = trace(CFG, row, [(TRAKT, source, {})], provider="ANILIST")
    assert [(g["title"], g["anime"]) for g in out["sources"]] == [("MAL only", {"namespace": "anilist", "id": "4100"})]


def test_anilist_whole_items_follow_the_pairs_mapping_gate(anime_index: Path) -> None:
    from cw_platform.anime_mapping.service import config_with_pair_feature_options
    _rule(match_provider="tmdb", match_id="30", target_id="200")
    source = {"x": {"type": "show", "title": "Thirty", "ids": {"anilist": "100", "tmdb": "30"}}}
    disabled = {**TRAKT, "cfg": {"anime_mapping": {"enabled": False}}}
    pair_off = {**TRAKT, "cfg": config_with_pair_feature_options(CFG, {"use_anime_mapping": False, "feature": "watchlist"})}
    for meta in (disabled, pair_off):
        hundred = trace(CFG, {"type": "anime", "title": "One hundred", "ids": {"anilist": "100"}},
                        [(meta, source, {})], provider="ANILIST")
        assert [g["via"] for g in hundred["sources"]] == ["direct"]
        two = trace(CFG, {"type": "anime", "title": "Two hundred", "ids": {"anilist": "200"}},
                    [(meta, source, {})], provider="ANILIST")
        assert two["sources"] == []
    mapped = trace(CFG, {"type": "anime", "title": "Two hundred", "ids": {"anilist": "200"}},
                   [(TRAKT, source, {})], provider="ANILIST")
    assert [g["via"] for g in mapped["sources"]] == ["custom"]


def test_converted_override_credits_the_rule_that_converts(anime_index: Path) -> None:
    _rule(match_provider="tmdb", match_id="2000", match_season=1, episode_from=1, episode_to=12, episode_start_at=1,
          target_namespace="anidb", target_id="9999")
    used = _rule(match_provider="tmdb", match_id="2000", match_season=1, episode_from=1, episode_to=12,
                 episode_start_at=1, target_id="1200")
    row = {"type": "show", "title": "MAL 1300", "ids": {"mal": "1300"}}
    source = {"e": _episode({"tmdb": "2000"}, "Two thousand", 1, 1)}
    out = trace(CFG, row, [(TRAKT, source, {})], provider="MYANIMELIST")
    assert [(g["via"], g["rule_id"]) for g in out["sources"]] == [("custom", used["id"])]


def test_anilist_ratings_use_the_ratings_writers_stricter_gate(anime_index: Path) -> None:
    from cw_platform.anime_mapping.service import config_with_pair_feature_options
    _rule(match_provider="tmdb", match_id="30", target_id="200")
    source = {"x": {"type": "show", "title": "Thirty", "ids": {"anilist": "100", "tmdb": "30"}}}
    unset = {**TRAKT, "cfg": config_with_pair_feature_options(CFG, {"feature": "ratings"})}
    row = {"type": "anime", "title": "One hundred", "ids": {"anilist": "100"}}
    ratings = trace(CFG, row, [(unset, source, {})], provider="ANILIST", feature="ratings")
    assert [g["via"] for g in ratings["sources"]] == ["direct"]
    on = {**TRAKT, "cfg": config_with_pair_feature_options(CFG, {"feature": "ratings", "use_anime_mapping": True})}
    ruled = trace(CFG, {"type": "anime", "title": "Two hundred", "ids": {"anilist": "200"}},
                  [(on, source, {})], provider="ANILIST", feature="ratings")
    assert [g["via"] for g in ruled["sources"]] == ["custom"]


def test_clicked_row_uses_the_same_pair_settings_as_its_sources(anime_index: Path) -> None:
    from cw_platform.anime_mapping.service import config_with_pair_feature_options
    _rule(match_provider="tmdb", match_id="30", target_id="200")
    row = {"type": "anime", "title": "One hundred", "ids": {"anilist": "100", "tmdb": "30"}}
    source = {"x": {"type": "show", "title": "Thirty", "ids": {"anilist": "100", "tmdb": "30"}}}
    off = {**TRAKT, "cfg": config_with_pair_feature_options(CFG, {"feature": "ratings"})}
    out = trace(CFG, row, [(off, source, {})], provider="ANILIST", feature="ratings")
    assert [g["via"] for g in out["sources"]] == ["direct"]


def test_conversion_credit_respects_episode_ranges(anime_index: Path) -> None:
    _rule(match_provider="tmdb", match_id="3000", match_season=1, episode_from=1, episode_to=12, episode_start_at=1,
          target_id="5100")
    used = _rule(match_provider="tmdb", match_id="3000", match_season=1, episode_from=1, episode_to=12,
                 episode_start_at=1, target_id="5200")
    row = {"type": "show", "title": "MAL 5300", "ids": {"mal": "5300"}}
    source = {"e": _episode({"tmdb": "3000"}, "Three thousand", 1, 10)}
    out = trace(CFG, row, [(TRAKT, source, {})], provider="MYANIMELIST")
    assert [(g["via"], g["rule_id"]) for g in out["sources"]] == [("custom", used["id"])]
