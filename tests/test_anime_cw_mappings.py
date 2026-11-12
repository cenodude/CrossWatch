# tests/test_anime_cw_mappings.py
# CrossWatch - CrossWatch anime mapping corrections on top of AniBridge
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from cw_platform.anime_mapping import cw_mappings, storage, updater
from cw_platform.anime_mapping.episodes import resolve_axis_coordinate_map

pytestmark = pytest.mark.real_cw_mappings

KAI_ENTRY = "dbz-kai-final-chapters-tmdb-seasons"

KAI_REAL: dict[str, Any] = {
    "anidb:10452:R": {"anilist:20635": {"1-61": "1-61"}, "mal:22777": {"1-61": "1-61"}, "tmdb_show:61709:s2": {"1-26": "1-26"}, "tvdb_show:88031:s2": {"1-61": "1-61"}},
    "anidb:6399:R": {"anilist:6033": {"1-97": "1-97"}, "mal:6033": {"1-97": "1-97"}, "tmdb_show:61709:s1": {"1-26": "1-26"}, "tvdb_show:88031:s1": {"1-97": "1-97"}},
    "anilist:20635": {"anidb:10452:R": {"1-61": "1-61"}, "anidb:10452:S": {"62-69": "1-8"}, "tmdb_show:61709:s0": {"1-8": "2-9"}, "tmdb_show:61709:s5": {"9-43": "1-35"}, "tmdb_show:61709:s6": {"44-69": "1-26"}, "tvdb_show:88031:s0": {"1-8": "1-8"}, "tvdb_show:88031:s2": {"9-69": "1-61"}},
    "mal:22777": {"anidb:10452:R": {"1-61": "1-61"}, "tmdb_show:61709:s5": {"1-35": "1-35"}, "tmdb_show:61709:s6": {"36-61": "1-26"}, "tvdb_show:88031:s2": {"1-61": "1-61"}},
    "anilist:6033": {"anidb:6399:R": {"1-97": "1-97"}, "mal:6033": {"1-97": "1-97"}, "tmdb_show:61709:s1": {"1-26": "1-26"}, "tmdb_show:61709:s2": {"27-52": "1-26"}, "tmdb_show:61709:s3": {"53-77": "1-25"}, "tmdb_show:61709:s4": {"78-97": "1-20"}, "tvdb_show:88031:s1": {"1-97": "1-97"}},
    "mal:6033": {"anidb:6399:R": {"1-97": "1-97"}, "anilist:6033": {"1-97": "1-97"}, "tvdb_show:88031:s1": {"1-97": "1-97"}},
    "tmdb_show:61709:s0": {"anidb:6399:S": {"1": "1"}, "anilist:11359": {"1": "1"}, "anilist:20635": {"2-9": "1-8"}, "mal:11359": {"1": "1"}, "tvdb_show:88031:s0": {"2-9": "1-8"}, "tvdb_show:88031:s1": {"1": "98"}},
    "tmdb_show:61709:s1": {"anidb:6399:R": {"1-26": "1-26"}, "anilist:6033": {"1-26": "1-26"}},
    "tmdb_show:61709:s2": {"anidb:10452:R": {"1-26": "1-26"}, "anilist:6033": {"1-26": "27-52"}},
    "tmdb_show:61709:s3": {"anilist:6033": {"1-25": "53-77"}},
    "tmdb_show:61709:s4": {"anilist:6033": {"1-20": "78-97"}},
    "tmdb_show:61709:s5": {"anilist:20635": {"1-35": "9-43"}, "mal:22777": {"1-35": "1-35"}},
    "tmdb_show:61709:s6": {"anilist:20635": {"1-26": "44-69"}, "mal:22777": {"1-26": "36-61"}},
    "tvdb_show:88031:s0": {"anilist:20635": {"1-8": "1-8"}, "tmdb_show:61709:s0": {"1-8": "2-9"}},
    "tvdb_show:88031:s1": {"anidb:6399:R": {"1-97": "1-97"}, "anidb:6399:S": {"98": "1"}, "anilist:11359": {"98": "1"}, "anilist:6033": {"1-97": "1-97"}, "mal:11359": {"98": "1"}, "mal:6033": {"1-97": "1-97"}, "tmdb_show:61709:s0": {"98": "1"}},
    "tvdb_show:88031:s2": {"anidb:10452:R": {"1-61": "1-61"}, "anilist:20635": {"1-61": "9-69"}, "mal:22777": {"1-61": "1-61"}},
}

BAD_KAI: dict[str, Any] = {
    "anidb:10452:R": {"tmdb_show:61709:s2": {"1-26": "1-26"}, "tvdb_show:88031:s2": {"1-61": "1-61"}},
    "tmdb_show:61709:s2": {"anidb:10452:R": {"1-26": "1-26"}, "anilist:6033": {"1-26": "27-52"}},
    "anidb:6399:R": {"tmdb_show:61709:s1": {"1-26": "1-26"}, "tvdb_show:88031:s1": {"1-97": "1-97"}},
}

FIXED_KAI: dict[str, Any] = {
    "anidb:10452:R": {
        "tmdb_show:61709:s5": {"1-35": "1-35"},
        "tmdb_show:61709:s6": {"36-61": "1-26"},
        "tvdb_show:88031:s2": {"1-61": "1-61"},
    },
    "anidb:6399:R": {
        "tmdb_show:61709:s1": {"1-26": "1-26"},
        "tmdb_show:61709:s2": {"27-52": "1-26"},
        "tmdb_show:61709:s3": {"53-77": "1-25"},
        "tmdb_show:61709:s4": {"78-97": "1-20"},
        "tvdb_show:88031:s1": {"1-97": "1-97"},
    },
}


def _payload(entries: list[dict[str, Any]], *, schema: int = 1, revision: str = "2000-01-01.1") -> bytes:
    return json.dumps({"schema_version": schema, "revision": revision, "entries": entries}).encode("utf-8")


def _entry(**extra: Any) -> dict[str, Any]:
    row = {"id": "e1", "remove": [{"source": "anidb:1:R", "target": "tmdb_show:2:s1"}]}
    row.update(extra)
    return row


def _build(mappings: dict[str, Any]) -> dict[str, Any]:
    pp = storage.paths("v3")
    pp["root"].mkdir(parents=True, exist_ok=True)
    pp["mappings"].write_bytes(json.dumps(mappings).encode("utf-8"))
    return storage.rebuild_sqlite_from_mappings(release_tag="v3")


def _edges(provider: str, ident: str) -> list[tuple[str, str, str, str, str]]:
    return sorted(
        (r["target_provider"], r["target_scope"], r["source_range"], r["target_range"], r["origin"])
        for r in storage.query_edges("v3", provider, ident)
    )


@pytest.mark.parametrize(
    "raw, message",
    [
        (_payload([_entry()], schema=2), "unsupported schema_version"),
        (_payload([_entry(remove=[{"source": "nope", "target": "tmdb_show:2:s1"}])]), "invalid descriptor"),
        (_payload([_entry(remove=[], add=[{"source": "anidb:1:R", "target": "tmdb_show:2:s1", "ranges": {"x": "1"}}])]), "invalid range"),
        (_payload([_entry(), _entry()]), "unique"),
        (_payload([_entry(remove=[], add=[])]), "nothing to change"),
        (b"{not json", "not valid JSON"),
        (_payload([_entry()], revision="latest"), "YYYY-MM-DD.N"),
        (_payload([_entry(evidence={"checked": "yesterday"})]), "evidence.checked"),
        (_payload([_entry(evidence={"checked": "2026-11-08", "tmdb": {"show": "x", "seasons": {"1": 12}}})]), "show id and seasons"),
        (_payload([_entry(evidence={"checked": "2026-11-08", "tmdb": {"show": "1", "seasons": {"one": 12}}})]), "episode counts"),
    ],
)
def test_invalid_files_are_rejected(raw: bytes, message: str) -> None:
    with pytest.raises(cw_mappings.CwMappingsError, match=message):
        cw_mappings.parse_payload(raw)


def test_bundled_file_is_valid() -> None:
    active = cw_mappings.load_active(None)
    assert active.source == "bundled"
    assert active.error == ""
    assert KAI_ENTRY in [e["id"] for e in active.entries]
    assert all(e["evidence"].get("tmdb") and e["evidence"]["checked"] for e in active.entries)


def test_valid_downloaded_copy_wins_and_bad_copy_falls_back(tmp_path: Path) -> None:
    cw_mappings.downloaded_path(tmp_path).write_bytes(_payload([_entry()], revision="2099-01-01.1"))
    active = cw_mappings.load_active(tmp_path)
    assert (active.source, active.revision) == ("downloaded", "2099-01-01.1")

    cw_mappings.downloaded_path(tmp_path).write_bytes(_payload([_entry()], revision="2000-01-01.1"))
    assert cw_mappings.load_active(tmp_path).source == "bundled"

    cw_mappings.downloaded_path(tmp_path).write_bytes(_payload([_entry()], schema=99))
    active = cw_mappings.load_active(tmp_path)
    assert active.source == "bundled"
    assert "downloaded copy rejected" in active.error


def test_rebuild_applies_the_correction_in_both_directions(config_base: Path) -> None:
    result = _build(BAD_KAI)

    assert ("tmdb", "s2", "1-26", "1-26", "anibridge") not in _edges("anidb", "10452")
    assert ("tmdb", "s5", "1-35", "1-35", "crosswatch") in _edges("anidb", "10452")
    assert ("tmdb", "s6", "36-61", "1-26", "crosswatch") in _edges("anidb", "10452")
    assert ("tvdb", "s2", "1-61", "1-61", "anibridge") in _edges("anidb", "10452")
    tmdb_s2 = sorted(
        (r["target_provider"], r["target_id"], r["source_range"], r["target_range"], r["origin"])
        for r in storage.query_edges("v3", "tmdb", "61709", scope="s2")
    )
    assert ("anidb", "10452", "1-26", "1-26", "anibridge") not in tmdb_s2
    assert ("anidb", "6399", "1-26", "27-52", "crosswatch") in tmdb_s2
    assert ("anilist", "6033", "1-26", "27-52", "anibridge") in tmdb_s2
    assert KAI_ENTRY in result["cw_applied"]
    assert KAI_ENTRY not in result["cw_redundant"]
    assert storage.read_state("v3")["cw_source"] == "bundled"


def test_entry_is_redundant_once_upstream_is_fixed(config_base: Path) -> None:
    result = _build(FIXED_KAI)

    assert KAI_ENTRY not in result["cw_applied"]
    assert KAI_ENTRY in result["cw_redundant"]
    assert all(row[4] == "anibridge" for row in _edges("anidb", "10452"))
    assert all(row[4] == "anibridge" for row in _edges("anidb", "6399"))


def test_kai_final_chapters_resolve_to_tmdb_season_five(config_base: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    real = KAI_REAL
    ids = {"anidb": "10452", "anilist": "20635", "mal": "22777"}

    empty = config_base / "empty_cw.json"
    empty.write_bytes(_payload([_entry(id="unrelated")]))
    monkeypatch.setattr(cw_mappings, "BUNDLED_PATH", empty)
    _build(real)
    before = resolve_axis_coordinate_map(ids, 6)

    monkeypatch.undo()
    _build(real)
    after = resolve_axis_coordinate_map(ids, 6)

    assert before[("tmdb", "61709")] == (2, 6)
    assert after[("tmdb", "61709")] == (5, 6)
    assert after[("tvdb", "88031")] == (2, 6)


def test_boot_check_rebuilds_when_the_corrections_change(config_base: Path) -> None:
    _build(BAD_KAI)
    cfg = {"anime_mapping": {"enabled": True, "release_tag": "v3"}}
    assert updater.boot_check(cfg=cfg)["status"] == "ready"

    root = storage.paths("v3")["root"]
    cw_mappings.downloaded_path(root).write_bytes(_payload([_entry(id="new-entry")], revision="2099-01-01.1"))
    res = updater.boot_check(cfg=cfg)

    assert res["status"] == "reindexed"
    assert "CW Anime DB updated" in res["message"]
    assert storage.read_state("v3")["cw_revision"] == "2099-01-01.1"
    assert updater.boot_check(cfg=cfg)["status"] == "ready"


SPLIT_COUR_REAL: dict[str, Any] = {
    "tmdb_show:117465:s1": {"anidb:16012:R": {"1-13": "1-13"}, "anilist:128893": {"1-13": "1-13"}, "mal:46569": {"1-13": "1-13"}, "tvdb_show:402474:s1": {"1-13": "1-13"}},
    "tmdb_show:117465:s2": {"anidb:18090:R": {"1-12": "1-12"}, "anilist:166613": {"1-12": "1-12"}, "mal:55825": {"1-12": "1-12"}, "tvdb_show:402474:s2": {"1-12": "1-12"}},
    "tmdb_show:240411:s1": {"anidb:18290:R": {"1-12": "1-12"}, "anilist:185660": {"13-24": "1-12"}, "mal:57334": {"1-12": "1-12"}, "mal:60543": {"13-24": "1-12"}, "tvdb_show:432832:s1": {"1-12": "1-12"}, "tvdb_show:432832:s2": {"13-24": "1-12"}},
    "tmdb_show:240411:s2": {"anidb:19060:R": {"1-12": "1-12"}, "anilist:185660": {"1-12": "1-12"}, "mal:60543": {"1-12": "1-12"}, "tvdb_show:432832:s2": {"1-12": "1-12"}},
    "tmdb_show:95479:s1": {"anidb:15275:R": {"1-24": "1-24"}, "anidb:17196:R": {"25-47": "1-23"}, "anidb:18363:R": {"48-59": "1-12"}, "anilist:113415": {"1-24": "1-24"}, "anilist:145064": {"25-47": "1-23"}, "anilist:172463": {"48-59": "1-12"}, "mal:40748": {"1-24": "1-24"}, "mal:51009": {"25-47": "1-23"}, "mal:57658": {"48-59": "1-12"}, "tvdb_show:377543:s1": {"1-24": "1-24"}, "tvdb_show:377543:s2": {"25-47": "1-23"}, "tvdb_show:377543:s3": {"48-59": "1-12"}},
    "tmdb_show:95479:s2": {"anidb:17196:R": {"1-23": "1-23"}, "anilist:145064": {"1-23": "1-23"}, "mal:51009": {"1-23": "1-23"}, "tvdb_show:377543:s2": {"1-23": "1-23"}},
    "tmdb_show:204832:s1": {"anidb:17478:R": {"1-12": "1-12"}, "anilist:166610": {"13-24": "1-12"}, "mal:52211": {"1-12": "1-12"}, "mal:55813": {"13-24": "1-12"}, "tvdb_show:421737:s1": {"1-12": "1-12"}, "tvdb_show:421737:s2": {"13-24": "1-12"}},
    "tmdb_show:204832:s2": {"anidb:18088:R": {"1-12": "1-12"}, "anilist:166610": {"1-12": "1-12"}, "mal:55813": {"1-12": "1-12"}, "tvdb_show:421737:s2": {"1-12": "1-12"}},
    "tmdb_show:194986:s1": {"anidb:17256:R": {"1-12": "1-12"}, "anidb:17979:R": {"13-24": "1-12"}, "anilist:146323": {"1-12": "1-12"}, "anilist:163542": {"13-24": "1-12"}, "mal:51252": {"1-12": "1-12"}, "mal:54947": {"13-24": "1-12"}, "tvdb_show:415821:s1": {"1-12": "1-12"}, "tvdb_show:415821:s2": {"13-24": "1-12"}},
    "tmdb_show:194986:s2": {"anidb:17979:R": {"1-12": "1-12"}, "anilist:163542": {"1-12": "1-12"}, "mal:54947": {"1-12": "1-12"}, "tvdb_show:415821:s2": {"1-12": "1-12"}},
}

SPLIT_COUR_CASES = [
    ("hells_paradise_s2e1", {"anidb": "18090", "anilist": "166613", "mal": "55825"}, 1, (2, 1), (1, 14)),
    ("hells_paradise_s2e12", {"anidb": "18090", "anilist": "166613", "mal": "55825"}, 12, (2, 12), (1, 25)),
    ("dan_da_dan_s2e1", {"anidb": "19060", "anilist": "185660", "mal": "60543"}, 1, (2, 1), (1, 13)),
    ("jujutsu_kaisen_s2e23", {"anidb": "17196", "anilist": "145064", "mal": "51009"}, 23, None, (1, 47)),
    ("mashle_s2e1", {"anidb": "18088", "anilist": "166610", "mal": "55813"}, 1, (2, 1), (1, 13)),
    ("spy_classroom_s2e12", {"anidb": "17979", "anilist": "163542", "mal": "54947"}, 12, None, (1, 24)),
]


def _tmdb_coords(ids: dict[str, str], episode: int) -> list[tuple[int, int]]:
    found = resolve_axis_coordinate_map(ids, episode)
    return sorted({coord for (provider, _), coord in found.items() if provider == "tmdb"})


@pytest.mark.parametrize("case, ids, episode, before, after", SPLIT_COUR_CASES, ids=[c[0] for c in SPLIT_COUR_CASES])
def test_split_cour_sequels_resolve_to_the_single_tmdb_season(
    config_base: Path, monkeypatch: pytest.MonkeyPatch, case: str, ids: dict[str, str], episode: int, before: Any, after: tuple[int, int]
) -> None:
    empty = config_base / "empty_cw.json"
    empty.write_bytes(_payload([_entry(id="unrelated")]))
    monkeypatch.setattr(cw_mappings, "BUNDLED_PATH", empty)
    _build(SPLIT_COUR_REAL)
    assert _tmdb_coords(ids, episode) == ([before] if before else [])

    monkeypatch.undo()
    _build(SPLIT_COUR_REAL)
    assert _tmdb_coords(ids, episode) == [after]


def test_hells_paradise_tmdb_second_cour_is_mapped(config_base: Path) -> None:
    from cw_platform.anime_mapping.episodes import resolve_absolute

    _build(SPLIT_COUR_REAL)
    found = resolve_absolute({"type": "episode", "season": 1, "episode": 14, "show_ids": {"tmdb": "117465"}})
    assert found is not None
    assert (found.namespace, found.target_id, found.absolute) == ("anidb", "18090", 1)


NEW_SEASON_REAL: dict[str, Any] = {
    "anidb:19952:R": {"anilist:209499": {"1-4": "1-4"}, "mal:63751": {"1-4": "1-4"}, "tvdb_show:475617:s1": {"1-4": "1-4"}},
    "mal:63751": {"anidb:19952:R": {"1-4": "1-4"}, "anilist:209499": {"1-4": "1-4"}, "tvdb_show:475617:s1": {"1-4": "1-4"}},
    "tvdb_show:475617:s1": {"anidb:19952:R": {"1-4": "1-4"}, "anilist:209499": {"1-4": "1-4"}, "mal:63751": {"1-4": "1-4"}},
    "mal:65009": {"anilist:216895": {"1-12": "1-12"}, "tvdb_show:465468:s1": {"1-12": "1-12"}},
    "anilist:216895": {"mal:65009": {"1-12": "1-12"}, "tvdb_show:465468:s1": {"1-12": "1-12"}},
}

NEW_SEASON_CASES = [
    ("tank_chair_e5", {"anidb": "19952", "anilist": "209499", "mal": "63751"}, 5, (1, 5)),
    ("welsh_and_shedar_e12", {"anilist": "216895", "mal": "65009"}, 12, (1, 12)),
]


@pytest.mark.parametrize("case, ids, episode, after", NEW_SEASON_CASES, ids=[c[0] for c in NEW_SEASON_CASES])
def test_new_season_anime_gets_its_tmdb_link(
    config_base: Path, monkeypatch: pytest.MonkeyPatch, case: str, ids: dict[str, str], episode: int, after: tuple[int, int]
) -> None:
    empty = config_base / "empty_cw.json"
    empty.write_bytes(_payload([_entry(id="unrelated")]))
    monkeypatch.setattr(cw_mappings, "BUNDLED_PATH", empty)
    _build(NEW_SEASON_REAL)
    assert _tmdb_coords(ids, episode) == []

    monkeypatch.undo()
    _build(NEW_SEASON_REAL)
    assert _tmdb_coords(ids, episode) == [after]
