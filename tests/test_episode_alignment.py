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


def test_alignment_needs_an_accessible_history_pair(client):
    client, _, _ = client
    assert client.post("/api/editor/episode-alignment", json=dict(pair_id="missing", show_ids=SHOW)).status_code == 404
