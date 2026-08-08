# CrossWatch test scripts
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from cw_platform.anime_mapping import storage
from cw_platform.anime_mapping.service import AnimeMappingService

# Dragon Ball Z as AniBridge really stores it: the s0 scope holds the movies, the
# numbered seasons hold the series. First-wins used to return the movie (mal 894).
MAPPINGS: dict[str, Any] = {
    "tvdb_show:81472:s0": {"mal:894": {"1": "1"}, "anilist:894": {"1": "1"}, "anidb:397:R": {"1": "1"}},
    "tvdb_show:81472:s1": {"mal:813": {"1-39": "1-39"}, "anilist:813": {"1-39": "1-39"}, "anidb:1530:R": {"1-39": "1-39"}},
    "tvdb_show:81472:s2": {"mal:813": {"1-35": "40-74"}, "anilist:813": {"1-35": "40-74"}},
    # Attack on Titan: every season is its own native entry, so there is no single answer.
    "tvdb_show:267440:s1": {"mal:16498": {"1-25": "1-25"}},
    "tvdb_show:267440:s2": {"mal:25777": {"1-12": "1-12"}},
    "tvdb_show:267440:s3": {"mal:35760": {"1-12": "1-12"}},
    # An OVA-only entry: nothing but an s0 scope, and it is unambiguous.
    "tvdb_show:555:s0": {"mal:777": {"1": "1"}},
    # A plain single-entry show.
    "tvdb_show:600:s1": {"mal:600": {"1-12": "1-12"}},
}


@pytest.fixture()
def svc(config_base: Path) -> AnimeMappingService:
    paths = storage.paths("v3")
    paths["root"].mkdir(parents=True, exist_ok=True)
    paths["mappings"].write_text(json.dumps(MAPPINGS), encoding="utf-8")
    storage.rebuild_sqlite_from_mappings(release_tag="v3")
    return AnimeMappingService({"anime_mapping": {"enabled": True, "release_tag": "v3"}})


def _amap(res: dict[str, Any]) -> dict[str, Any]:
    return (res.get("detail") or {}).get("anime_mapping") or {}


def test_season_scopes_beat_the_specials_scope(svc: AnimeMappingService) -> None:
    """The DBZ regression: mal 894 is a movie under s0; 813 is the series."""
    got = svc.enrich_ids({"tvdb": "81472"}, media_type="show")
    assert got["ids"]["mal"] == "813"
    assert got["ids"]["anilist"] == "813"
    assert got["ids"]["anidb"] == "1530"
    assert "894" not in got["ids"].values()
    assert "397" not in got["ids"].values()


def test_multi_entry_franchise_is_refused_not_guessed(svc: AnimeMappingService) -> None:
    got = svc.enrich_ids({"tvdb": "267440"}, media_type="show")
    assert "mal" not in got["ids"]
    assert "mal" in _amap(got).get("ambiguous", [])


def test_refusal_leaves_the_source_ids_intact(svc: AnimeMappingService) -> None:
    got = svc.enrich_ids({"tvdb": "267440"}, media_type="show")
    assert got["ids"]["tvdb"] == "267440"


def test_specials_only_entry_still_resolves(svc: AnimeMappingService) -> None:
    """With no season scope at all, the s0 tier is the only tier and is usable."""
    got = svc.enrich_ids({"tvdb": "555"}, media_type="show")
    assert got["ids"]["mal"] == "777"
    assert "mal" not in _amap(got).get("ambiguous", [])


def test_unambiguous_show_is_unaffected(svc: AnimeMappingService) -> None:
    got = svc.enrich_ids({"tvdb": "600"}, media_type="show")
    assert got["ids"]["mal"] == "600"
    assert _amap(got).get("ambiguous") is None


def test_existing_source_ids_are_never_overwritten(svc: AnimeMappingService) -> None:
    got = svc.enrich_ids({"tvdb": "81472", "mal": "99999"}, media_type="show")
    assert got["ids"]["mal"] == "99999"


def test_detail_block_still_lists_every_candidate(svc: AnimeMappingService) -> None:
    """Refusing to pick must not hide the alternatives from the detail payload."""
    got = svc.enrich_ids({"tvdb": "267440"}, media_type="show")
    seen = {entry["id"] for entry in _amap(got).get("mal", [])}
    assert {"16498", "25777", "35760"} <= seen


def test_detail_entries_are_deduplicated(svc: AnimeMappingService) -> None:
    got = svc.enrich_ids({"tvdb": "81472"}, media_type="show")
    entries = _amap(got).get("mal", [])
    markers = [(e["id"], e["scope"], e["source_range"], e["target_range"]) for e in entries]
    assert len(markers) == len(set(markers))


def test_a_user_override_still_wins_over_a_refusal(svc: AnimeMappingService) -> None:
    from cw_platform.anime_mapping import overrides as ov

    ov.upsert_override(
        {
            "media_type": "show",
            "match_provider": "tvdb",
            "match_id": "267440",
            "target_namespace": "mal",
            "target_id": "16498",
        }
    )
    got = svc.enrich_ids({"tvdb": "267440"}, media_type="show")
    assert got["ids"]["mal"] == "16498"
