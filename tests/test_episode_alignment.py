# tests/test_episode_alignment.py
# CrossWatch - Episode numbering alignment proposals and combined save
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
import pytest

from cw_platform.id_map import canonical_key
from cw_platform.mapping_policy import effective_policy
from services.episode_alignment import align
from test_episode_groups_api import client

SHOW = {"tmdb": "1668"}
NAMES = {13: "Rachel's Sister", 14: "Chandler Can't Cry", 15: "Could Have Been", 16: "Unagi", 17: "Ross Dates a Student",
         18: "Joey's Fridge", 19: "Mac and C.H.E.E.S.E.", 20: "Elizabeth's Dad", 21: "Paul's the Man", 22: "The Ring", 23: "The Proposal"}


def row(number, tvdb, title, **ids):
    return dict(type="episode", title=title, series_title="Friends", show_ids=dict(SHOW), season=6, episode=number,
                ids={"tvdb": str(tvdb), **ids}, watched_at=f"2026-10-{number:02d}T12:00:00Z")


def index(rows):
    return {canonical_key(item): item for item in rows}


def library(parts=True):
    rows = [row(13, 303954, NAMES[13]), row(14, 303955, NAMES[14]),
            row(15, 303956, NAMES[15] + (" (1)" if parts else "")), row(16, 303957, NAMES[15] + (" (2)" if parts else " Again"))]
    rows += [row(number + 1, 303941 + number + 1, NAMES[number]) for number in range(16, 23)]
    return index(rows + [row(24, 303965, NAMES[23] + " (1)"), row(25, 303966, NAMES[23] + " (2)")])


def tracker(follows=True):
    tvdb = {13: 303954, 14: 303955, 15: 303956, 23: 303965, **{number: 303942 + number for number in range(16, 23)}}
    return index([row(number, tvdb[number], NAMES[number], tmdb=str(9000 + number if follows else 5)) for number in range(13, 24)])


def catalog(key, show, season):
    assert (key, show, season) == ("key", "1668", 6)
    return {number: dict(id=9000 + number, name=NAMES.get(number, f"Episode {number}"), runtime=45 if number in (15, 23) else 22)
            for number in range(1, 24)}


def key_of(number):
    return canonical_key(row(number, 0, ""))


def test_shifted_season_proposes_corrections_and_both_groups_with_metadata():
    result = align(library(), tracker(), SHOW, tmdb_key="key", fetch=catalog)
    assert result["metadata"] == "used"
    shifted = {(item["key"], item["episode"]) for item in result["corrections"]}
    assert shifted == {(key_of(number), number - 1) for number in range(17, 25)}
    double, finale = result["groups"]
    assert double["orphans"] == [key_of(16)] and double["suggested"] == "previous" and "double length" in double["reason"]
    assert double["options"]["previous"] == dict(source=[key_of(15), key_of(16)], target=[key_of(15)])
    assert double["options"]["next"] == dict(source=[key_of(16), key_of(17)], target=[key_of(16)])
    assert finale["orphans"] == [key_of(25)] and finale["suggested"] == "previous"
    assert finale["options"] == {"previous": dict(source=[key_of(24), key_of(25)], target=[key_of(23)])}
    assert double["name"] == "Friends S06E15" and double["split"] == "source"
    statuses = {item["source"]["episode"]: item["status"] for item in result["rows"] if item["source"]}
    assert statuses[13] == "same" and statuses[16] == "unmatched" and statuses[17] == "shifted"


def test_reversed_pair_finds_the_split_on_the_target():
    result = align(tracker(), library(), SHOW, tmdb_key="key", fetch=catalog)
    assert {(item["key"], item["episode"]) for item in result["corrections"]} == {(key_of(number), number + 1) for number in range(16, 24)}
    double, finale = result["groups"]
    assert double["split"] == "target" and double["suggested"] == "previous"
    assert double["options"]["previous"] == dict(source=[key_of(15)], target=[key_of(15), key_of(16)])
    assert finale["options"]["previous"] == dict(source=[key_of(23)], target=[key_of(24), key_of(25)])


@pytest.mark.parametrize("case", ["no_key", "failed", "other_numbering"])
def test_without_usable_metadata_corrections_remain_and_groups_become_a_question(case):
    fetch = (lambda *a: None) if case == "failed" else catalog
    result = align(library(parts=False), tracker(follows=case != "other_numbering"), SHOW,
                   tmdb_key="" if case == "no_key" else "key", fetch=fetch)
    assert result["metadata"] == {"no_key": "missing_key", "failed": "unavailable", "other_numbering": "used"}[case]
    assert len(result["corrections"]) == 8
    assert [(group["orphans"], group["suggested"]) for group in result["groups"]] == [([key_of(16)], None)]


def test_titles_suggest_the_group_without_metadata():
    result = align(library(), tracker(), SHOW)
    assert result["groups"][0]["suggested"] == "previous" and result["groups"][0]["reason"] == "The episode titles match."


def test_unwatched_destination_episodes_are_not_treated_as_split_parts():
    source = index([row(number, 303940 + number, NAMES[number]) for number in range(13, 20)])
    target = index([row(number, 303940 + number, NAMES[number], tmdb=str(9000 + number)) for number in range(13, 17)])
    result = align(source, target, SHOW, tmdb_key="key", fetch=catalog)
    assert not result["groups"] and not result["corrections"]


def test_missing_finale_watch_on_same_numbering_is_not_a_split():
    target = library()
    target.pop(key_of(25))
    result = align(library(), target, SHOW, tmdb_key="key", fetch=catalog)
    assert not result["groups"] and not result["corrections"]
    unmatched = [item["source"]["episode"] for item in result["rows"] if item["status"] == "unmatched"]
    assert unmatched == [25]


def test_catalog_is_only_trusted_when_every_comparable_episode_agrees():
    from services.episode_alignment import _follows_catalog

    entries = catalog("key", "1668", 6)
    rows = list(tracker().values())
    assert _follows_catalog(rows, entries, "1668")
    assert not _follows_catalog(rows + [row(24, 1, "Extra")], entries, "1668")
    assert not _follows_catalog([row(16, 1, "Could Have Been")], entries, "1668")
    assert not _follows_catalog([row(16, 1, "")], entries, "1668")


def specials(translate=False):
    name = lambda number: f"Aflevering naam {number}" if translate else NAMES[number]
    regular = [row(number, 303940 + number, NAMES[number]) for number in range(13, 20)]
    other = [row(number, 303940 + number, name(number)) for number in range(13, 20)]
    extras = [("Full Animatic", 1), ("Character Models 1", 2), ("Character Models 2", 3), ("Agent 12", 4)]
    mine = [{**row(5 + at, 800 + at, title), "season": 0} for at, (title, _) in enumerate(extras)]
    theirs = [{**row(5 + at, 800 + wrong if at else 800, title), "season": 0} for at, (title, wrong) in enumerate(extras)]
    return index(regular + mine), index(other + theirs)


def test_id_matches_between_different_titles_are_proposed_but_flagged():
    source, target = specials()
    result = align(source, target, SHOW)
    doubts = {(item["season"], item["episode"]): item["doubt"] for item in result["corrections"]}
    assert doubts and all(doubts.values()) and all(season == 0 for season, _ in doubts)
    assert not result["groups"]
    assert not any(item["doubt"] for item in result["rows"] if item["status"] == "same")
    assert sum(item["doubt"] for item in result["rows"]) == len(doubts)


def test_titles_in_another_language_do_not_flag_every_match():
    source, target = specials(translate=True)
    assert not any(item["doubt"] for item in align(source, target, SHOW)["corrections"])


def test_split_part_titles_are_not_flagged():
    result = align(library(), tracker(), SHOW, tmdb_key="key", fetch=catalog)
    assert not any(item["doubt"] for item in result["corrections"]) and len(result["groups"]) == 2


def test_doubtful_shifted_episode_never_enters_a_group():
    source = library()
    source[key_of(17)] = {**source[key_of(17)], "title": "Ross Dates a Student"}
    result = align(source, tracker(), SHOW, tmdb_key="key", fetch=catalog)
    assert [item["key"] for item in result["corrections"] if item["doubt"]] == [key_of(17)]
    members = {key for group in result["groups"] for option in group["options"].values() for key in option["source"]}
    assert key_of(17) not in members
    assert [group["orphans"] for group in result["groups"]] == [[key_of(25)]]


def test_same_number_match_with_a_different_title_stays_a_group_anchor():
    source = library()
    source[key_of(15)] = {**source[key_of(15)], "title": "Alternate localized title"}
    result = align(source, tracker(), SHOW, tmdb_key="key", fetch=catalog)
    assert not any(item["doubt"] for item in result["rows"])
    double = result["groups"][0]
    assert double["suggested"] == "previous" and double["options"]["previous"]["source"] == [key_of(15), key_of(16)]
    assert len(result["groups"]) == 2


def anthology():
    part = lambda show, season, number, tvdb: dict(type="episode", title=f"Chapter {number}", series_title="Monster",
        show_ids={"tmdb": show, "imdb": "tt13207736"}, season=season, episode=number, ids={"tvdb": str(tvdb)},
        watched_at="2026-10-01T12:00:00Z")
    source = index([part("299939", 1, number, 5000 + number) for number in range(1, 9)])
    earlier = [part("111", season, number, season * 100 + number) for season in (1, 2, 3) for number in range(1, 7)]
    latest = [part("111", 4, number, 5001 if number == 1 else 9000 + number) for number in range(1, 9)]
    return source, index(earlier + latest)


def test_seasons_kept_as_separate_shows_are_reported_as_a_layout_difference():
    source, target = anthology()
    calls = []
    result = align(source, target, {"tmdb": "299939", "imdb": "tt13207736"}, tmdb_key="key",
                   fetch=lambda key, show, season: calls.append(season) or {})
    assert result["diagnosis"] == "layout"
    assert result["layout"] == [dict(source_season=1, target_season=4, episodes=1)]
    assert [(item["episode"], item["doubt"]) for item in result["corrections"]] == [(1, True)] and not result["groups"]
    assert result["metadata"] == "used" and calls
    assert [item["status"] for item in result["rows"] if item["source"] and item["source"]["episode"] == 1] == ["moved"]


STORY = ["Bloodbath", "Strong Kitty", "Whack Job!", "Forty Whacks", "The Maid", "Inquest", "Verdict", "Carnival"]


def story():
    watched = lambda show, season, number, ids, title: dict(type="episode", title=title, series_title="Monster",
        show_ids={"tmdb": "299939", **show}, season=season, episode=number, ids=ids, watched_at="2026-10-01T12:00:00Z")
    source = index([watched({}, 1, number, {"tmdb": str(7000 + number), "tvdb": str(5000 + number)}, "") for number in range(1, 9)])
    target = index([watched({"simkl": "1446614"}, 4, 1, {"tvdb": "5001"}, "S04E01"),
                    watched({"simkl": "1446614"}, 4, 2, {"tvdb": "9999"}, "S04E02"),
                    *[watched({"simkl": "1446614"}, 1, number, {"tvdb": str(100 + number)}, f"S01E{number:02d}") for number in range(1, 4)]])
    listing = lambda titles, ids: [dict(episode=number, title=title, ids=ids(number), runtime=None) for number, title in enumerate(titles, 1)]
    layouts = dict(
        source=dict(title="Monster: The Lizzie Borden Story", counts={1: 8},
                    seasons={1: listing([""] * 8, lambda number: {"tmdb": str(7000 + number)})}),
        target=dict(title="Monster", counts={1: 3, 4: 8},
                    seasons={1: listing(["Episode One", "Please Don't Go", "Doin' a Dahmer"], lambda number: {}),
                             4: listing(STORY, lambda number: {})}),
        extra=[dict(title="Monster: The Lizzie Borden Story", counts={1: 8},
                    seasons={1: listing(STORY, lambda number: {"tmdb": str(7000 + number)})})])
    return source, target, layouts


def test_provider_layouts_line_up_a_season_kept_under_another_season():
    source, target, layouts = story()
    result = align(source, target, {"tmdb": "299939"}, layouts=layouts)
    assert result["diagnosis"] == "layout"
    assert result["layout"] == [dict(source_season=1, target_season=4, episodes=8)]
    assert [(item["season"], item["episode"], item["doubt"]) for item in result["corrections"]] == [(4, number, False) for number in range(1, 9)]
    assert not any("show_ids" in item for item in result["corrections"]) and not result["groups"]
    listed = [item for item in result["rows"] if item["source"] is None]
    assert [(item["target"]["season"], item["target"]["episode"]) for item in listed] == [(1, 1), (1, 2), (1, 3)]
    by_number = {item["source"]["episode"]: item for item in result["rows"] if item["source"]}
    assert by_number[1]["via"] == "id" and by_number[2]["via"] == "title" and by_number[8]["target"]["title"] == "Carnival"


def test_an_episode_with_a_differently_worded_title_is_placed_by_position():
    source, target, layouts = story()
    layouts["target"]["seasons"][4][4]["title"] = "The Maid Speaks"
    result = align(source, target, {"tmdb": "299939"}, layouts=layouts)
    by_number = {item["source"]["episode"]: item for item in result["rows"] if item["source"]}
    assert by_number[5]["via"] == "position" and by_number[5]["target"]["episode"] == 5
    assert len(result["corrections"]) == 8


def test_without_provider_layouts_only_the_id_match_is_found_and_left_unselected():
    source, target, _ = story()
    result = align(source, target, {"tmdb": "299939"})
    assert [(item["episode"], item["doubt"]) for item in result["corrections"]] == [(1, True)]
    assert result["corrections"][0]["reason"] == "Only one episode links these two seasons."


def test_a_correction_carries_the_other_show_when_the_show_ids_differ():
    source, target, layouts = story()
    for item in target.values():
        item["show_ids"] = {"simkl": "1446614", "imdb": "tt13207736"}
    for item in source.values():
        item["show_ids"] = {"tmdb": "299939", "imdb": "tt13207736"}
    target = index(list(target.values()))
    result = align(source, target, {"tmdb": "299939", "imdb": "tt13207736"}, layouts=layouts)
    assert len(result["corrections"]) == 8
    assert all(item["show_ids"] == {"simkl": "1446614", "imdb": "tt13207736"} for item in result["corrections"])


def test_generic_episode_titles_never_create_a_match():
    from services.episode_alignment import _title_key

    for title in ("Episode 5", "Chapter Seven", "S01E02", "Part 2", "", "12"):
        assert _title_key({"title": title}) == ""
    assert _title_key({"title": "Strong Kitty"}) == "strongkitty"


def test_a_missing_season_on_tmdb_is_not_reported_as_unreachable(monkeypatch):
    from types import SimpleNamespace
    from services import episode_alignment

    reply = lambda status: lambda *a, **k: SimpleNamespace(status_code=status, json=lambda: {})
    monkeypatch.setattr(episode_alignment.requests, "get", reply(404))
    assert episode_alignment.tmdb_season("key", "1", 4) == {}
    monkeypatch.setattr(episode_alignment.requests, "get", reply(500))
    assert episode_alignment.tmdb_season("key", "1", 4) is None


def test_a_long_run_of_unmatched_episodes_is_never_offered_as_one_combined_episode():
    source = index([row(number, 400 + number, NAMES.get(number, f"Extra {number}")) for number in range(13, 24)])
    shifted = {13: 413, 14: 419, 15: 420, 16: 421, 17: 422, 18: 423}
    target = index([row(number, tvdb, f"Other {number}") for number, tvdb in shifted.items()])
    result = align(source, target, SHOW)
    assert all(len(group["orphans"]) <= 3 for group in result["groups"])
    assert not any(len(option["source"]) > 4 for group in result["groups"] for option in group["options"].values())


def test_numbering_cases_report_their_diagnosis():
    assert align(library(), tracker(), SHOW)["diagnosis"] == "numbering"
    assert align(tracker(), tracker(), SHOW)["diagnosis"] == "aligned"


def test_combined_save_stores_groups_and_corrections_then_nothing_is_left(client, monkeypatch):
    client, api, cfg = client
    from cw_platform.local_db import state

    cfg["tmdb"] = {"api_key": ""}
    monkeypatch.setattr(state, "load_pair_state", lambda *a, **k: {"providers": {
        "SRC": {"history": {"baseline": {"items": library()}}}, "DST": {"history": {"baseline": {"items": tracker()}}}}})
    proposal = client.post("/api/editor/episode-alignment", json=dict(pair_id="p1", show_ids=SHOW)).json()
    assert proposal["metadata"] == "missing_key" and proposal["source"] == {"provider": "SRC", "instance": "default"}
    groups = [dict(name="Double", **proposal["groups"][0]["options"]["previous"]),
              dict(name="Finale", source=[key_of(24), key_of(25)], target=[key_of(23)])]
    grouped = {key for group in groups for key in group["source"]}
    chosen = [{name: item[name] for name in ("key", "season", "episode")} for item in proposal["corrections"]]
    body = dict(pair_id="p1", show_ids=SHOW, groups=groups, corrections=chosen)
    assert client.post("/api/editor/episode-alignment/apply", json=body).status_code == 409
    body["corrections"] = [item for item in chosen if item["key"] not in grouped]
    assert client.post("/api/editor/episode-alignment/apply", json=body).json() == dict(ok=True, groups=2, corrections=7)
    policy = api._load_policy()
    saved = policy["pairs"]["p1"]["episode_groups"]
    assert [[member["episode"] for member in group["source"]["episodes"]] for group in saved] == [[15, 16], [24, 25]]
    assert [group["target"]["episodes"][0]["episode"] for group in saved] == [15, 23]
    node = effective_policy(policy, "p1")["providers"]["SRC"]["history"]
    assert set(node["adds"]["items"]) == {key_of(number) for number in range(16, 23)}
    assert node["blocks"] == [key_of(23)]
    after = client.post("/api/editor/episode-alignment", json=dict(pair_id="p1", show_ids=SHOW)).json()
    assert not after["groups"] and not after["corrections"]
    assert all(item["status"] == "same" for item in after["rows"])


def test_layout_fixes_are_saved_as_corrections_and_then_nothing_is_left(client, monkeypatch):
    client, api, cfg = client
    from cw_platform.local_db import state
    from services import episode_alignment

    source, target, layouts = story()
    monkeypatch.setattr(state, "load_pair_state", lambda *a, **k: {"providers": {
        "SRC": {"history": {"baseline": {"items": source}}}, "DST": {"history": {"baseline": {"items": target}}}}})
    monkeypatch.setattr(episode_alignment, "_lookups", lambda *a: (layouts, [dict(label="SIMKL", ok=True, title="Monster", seasons=[[1, 3], [4, 8]])]))
    request = dict(pair_id="p1", show_ids={"tmdb": "299939"})
    proposal = client.post("/api/editor/episode-alignment", json=request).json()
    assert proposal["diagnosis"] == "layout" and proposal["sources"][0]["label"] == "SIMKL"
    chosen = [{name: item[name] for name in ("key", "season", "episode")} for item in proposal["corrections"]]
    assert client.post("/api/editor/episode-alignment/apply", json={**request, "corrections": chosen}).json() == dict(ok=True, groups=0, corrections=8)
    node = effective_policy(api._load_policy(), "p1")["providers"]["SRC"]["history"]
    assert sorted(item["season"] for item in node["adds"]["items"].values()) == [4] * 8
    after = client.post("/api/editor/episode-alignment", json=request).json()
    assert not after["corrections"] and after["diagnosis"] == "aligned"
    bad = dict(key=chosen[0]["key"], season=4, episode=1, show_ids={"nonsense": "1"})
    assert client.post("/api/editor/episode-alignment/apply", json={**request, "corrections": [bad]}).status_code in (400, 409)


def test_alignment_needs_an_accessible_history_pair(client):
    client, _, _ = client
    assert client.post("/api/editor/episode-alignment", json=dict(pair_id="missing", show_ids=SHOW)).status_code == 404


def test_a_catalogue_only_episode_is_never_a_group_anchor():
    target = tracker()
    unwatched = target.pop(key_of(15))
    layouts = dict(source=None, extra=[], target=dict(title="Friends", counts={6: 1}, seasons={6: [
        dict(episode=15, title=unwatched["title"], ids={"tvdb": unwatched["ids"]["tvdb"]}, runtime=None)]}))
    result = align(library(), target, SHOW, tmdb_key="key", fetch=catalog, layouts=layouts)
    members = {key for group in result["groups"] for option in group["options"].values() for key in (*option["source"], *option["target"])}
    assert key_of(15) not in members
    assert all(key in target or key in library() for key in members)


def test_a_title_used_more_than_once_is_never_matched_by_title():
    made = lambda number, tvdb: row(number, tvdb, "Finale")
    source = index([made(1, 1), made(5, 55), row(2, 2, "Opening")])
    target = index([made(1, 1), made(7, 77), row(2, 2, "Opening")])
    result = align(source, target, SHOW)
    assert not result["corrections"]
    assert {item["source"]["episode"]: item["status"] for item in result["rows"] if item["source"]}[5] == "unmatched"


def test_doubtful_matches_do_not_place_other_episodes_by_position():
    part = lambda season, number, tvdb, title: {**row(number, tvdb, title), "season": season}
    agreeing = [(number, f"Agreed {chr(64 + number)}") for number in range(1, 5)]
    source = index([part(2, number, 200 + number, title) for number, title in agreeing]
                   + [part(1, 1, 901, "Alpha"), part(1, 2, 902, "Beta"), part(1, 3, 903, "")])
    target = index([part(2, number, 200 + number, title) for number, title in agreeing]
                   + [part(4, 1, 901, "Gamma"), part(4, 2, 902, "Delta"), part(4, 3, 999, "")])
    result = align(source, target, SHOW)
    assert [(item["episode"], item["doubt"]) for item in result["corrections"]] == [(1, True), (2, True)]
    assert not any(item["via"] == "position" for item in result["rows"])


def test_a_watched_split_stays_suggested_when_the_catalogue_lists_a_later_unwatched_episode():
    made = lambda number, tvdb, title: {**row(number, tvdb, title), "season": 1}
    source = index([made(1, 11, "Opening"), made(2, 12, "Double (1)"), made(3, 13, "Double (2)")])
    target = index([made(1, 11, "Opening"), made(2, 13, "Double")])
    layouts = dict(source=None, extra=[], target=dict(title="Show", counts={1: 3}, seasons={1: [
        dict(episode=3, title="Next Adventure", ids={}, runtime=None)]}))
    result = align(source, target, SHOW, layouts=layouts)
    keys = sorted(source)
    assert len(result["groups"]) == 1 and result["groups"][0]["suggested"] == "next"
    assert result["groups"][0]["options"]["next"] == dict(source=[keys[1], keys[2]], target=[keys[1]])
    assert not any(item["source"] is None for item in result["rows"])
