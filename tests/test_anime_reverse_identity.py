# CrossWatch test scripts
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from cw_platform.anime_mapping import storage
from cw_platform.anime_mapping.service import AnimeMappingService, enrich_index_for_pair

MAPPINGS: dict[str, Any] = {
    "mal:813": {"tmdb_show:12971": {}, "tvdb_show:81472": {}},
    "anidb:1530": {"mal:813": {}},
    "mal:21": {"tmdb_show:37854": {}},
}

IDENTITY_TSV = "\n".join(
    [
        "\t".join(["title", "anidb", "anilist", "kitsu", "myanimelist", "simkl", "themoviedb", "thetvdb"]),
        "\t".join(["Clean", "1530", "813", "720", "813", "41487", "12971", "81472"]),
        "\t".join(["KitsuOnly", "2369", "269", "244", "269", "", "30984", "74796"]),
        "\t".join(["DupA", "111", "", "", "555", "9001", "", ""]),
        "\t".join(["DupB", "222", "", "", "555", "9001", "", ""]),
    ]
)


@pytest.fixture()
def index(config_base: Path) -> Path:
    paths = storage.paths("v3")
    paths["root"].mkdir(parents=True, exist_ok=True)
    paths["mappings"].write_text(json.dumps(MAPPINGS), encoding="utf-8")
    paths["identity"].write_text(IDENTITY_TSV, encoding="utf-8")
    storage.rebuild_sqlite_from_mappings(release_tag="v3")
    return paths["db"]


def _svc() -> AnimeMappingService:
    return AnimeMappingService({"anime_mapping": {"enabled": True, "release_tag": "v3"}})


# --- reverse identity lookup --------------------------------------------------


def test_reverse_lookup_returns_native_namespaces(index: Path) -> None:
    assert storage.query_identity_natives("v3", "simkl", "41487") == {
        "anidb": "1530",
        "mal": "813",
        "anilist": "813",
    }


def test_reverse_lookup_by_kitsu(index: Path) -> None:
    assert storage.query_identity_natives("v3", "kitsu", "244") == {
        "anidb": "2369",
        "mal": "269",
        "anilist": "269",
    }


def test_reverse_lookup_drops_ambiguous_namespace(index: Path) -> None:
    found = storage.query_identity_natives("v3", "simkl", "9001")
    assert "anidb" not in found
    assert found.get("mal") == "555"


def test_reverse_lookup_rejects_unknown_namespace(index: Path) -> None:
    assert storage.query_identity_natives("v3", "tvdb", "81472") == {}
    assert storage.query_identity_natives("v3", "simkl", "") == {}
    assert storage.query_identity_natives("v3", "simkl", "404404") == {}


# --- SIMKL-only items reaching tmdb-based providers ---------------------------


def test_simkl_only_item_gains_aired_ids(index: Path) -> None:
    out = _svc().enrich_item({"type": "show", "title": "Clean", "ids": {"simkl": "41487"}})
    ids = out["ids"]
    assert ids["tmdb"] == "12971"
    assert ids["tvdb"] == "81472"
    assert ids["simkl"] == "41487"
    assert ids["mal"] == "813"


def test_simkl_only_item_reports_seeds_in_detail(index: Path) -> None:
    out = _svc().enrich_item({"type": "show", "ids": {"simkl": "41487"}})
    seeds = out["detail"]["anime_mapping"]["identity_seeds"]
    assert seeds == {"anidb": "1530", "mal": "813", "anilist": "813"}


def test_kitsu_only_item_is_seeded(index: Path) -> None:
    out = _svc().enrich_item({"type": "show", "ids": {"kitsu": "244"}})
    assert out["ids"]["mal"] == "269"


def test_seeding_is_skipped_when_a_native_id_is_present(index: Path) -> None:
    out = _svc().enrich_item({"type": "show", "ids": {"simkl": "41487", "mal": "813"}})
    assert "identity_seeds" not in out["detail"]["anime_mapping"]
    assert out["ids"]["tmdb"] == "12971"


def test_seeding_is_skipped_when_an_aired_id_is_present(index: Path) -> None:
    out = _svc().enrich_item({"type": "show", "ids": {"simkl": "41487", "tmdb": "12971"}})
    assert "identity_seeds" not in out["detail"]["anime_mapping"]


def test_unknown_simkl_id_is_left_untouched(index: Path) -> None:
    item = {"type": "show", "ids": {"simkl": "404404"}}
    out = _svc().enrich_item(item)
    assert out["ids"] == {"simkl": "404404"}
    assert "detail" not in out


def test_episodes_are_never_seeded_from_show_identity(index: Path) -> None:
    item = {
        "type": "episode",
        "season": 1,
        "episode": 5,
        "ids": {"simkl": "41487"},
        "show_ids": {"simkl": "41487"},
        "simkl_bucket": "anime",
    }
    out = _svc().enrich_item(item)
    assert out["ids"] == {"simkl": "41487"}
    assert "tmdb" not in out["ids"]
    assert "detail" not in out


def test_seasons_are_never_seeded_from_show_identity(index: Path) -> None:
    out = _svc().enrich_item({"type": "season", "season": 1, "ids": {"simkl": "41487"}})
    assert out["ids"] == {"simkl": "41487"}


def test_episode_keeps_simkl_native_markers(index: Path) -> None:
    from cw_platform.id_map import minimal

    item = {
        "type": "episode",
        "season": 1,
        "episode": 5,
        "ids": {"simkl": "41487"},
        "show_ids": {"simkl": "41487"},
        "simkl_bucket": "anime",
        "_simkl_episode_number": 5,
        "watched_at": "2026-01-01T00:00:00Z",
    }
    out = minimal(_svc().enrich_item(item))
    assert out["simkl_bucket"] == "anime"
    assert out["_simkl_episode_number"] == 5
    assert out["ids"] == {"simkl": "41487"}


def test_seeded_item_survives_minimal_and_rekeys(index: Path) -> None:
    from cw_platform.id_map import canonical_key, minimal

    out = minimal(_svc().enrich_item({"type": "show", "ids": {"simkl": "41487"}}))
    assert canonical_key(out) == canonical_key({"type": "show", "ids": {"tmdb": "12971"}})


# --- schema gate --------------------------------------------------------------


def test_index_ready_requires_current_schema(index: Path) -> None:
    assert storage.index_ready("v3") is True
    storage.write_state("v3", {"schema_version": storage.SCHEMA_VERSION - 1})
    assert storage.index_schema_ok("v3") is False
    assert storage.index_ready("v3") is False


def test_service_is_not_ready_on_stale_schema(index: Path) -> None:
    storage.write_state("v3", {"schema_version": 1})
    out = _svc().enrich_item({"type": "show", "ids": {"simkl": "41487"}})
    assert out["ids"] == {"simkl": "41487"}


# --- enrichment counters ------------------------------------------------------

_PAIR_CFG = {"anime_mapping": {"enabled": True, "release_tag": "v3"}}


def test_enrich_stats_count_seeds_and_dead_ends(index: Path) -> None:
    idx = {
        "simkl:41487": {"type": "show", "ids": {"simkl": "41487"}},
        "mal:813": {"type": "show", "ids": {"mal": "813"}},
        "simkl:404404": {"type": "show", "ids": {"simkl": "404404"}},
        "simkl:41487s01e01": {
            "type": "episode",
            "season": 1,
            "episode": 1,
            "ids": {"simkl": "41487"},
            "show_ids": {"simkl": "41487"},
        },
    }
    stats: dict[str, int] = {}
    out = enrich_index_for_pair(idx, _PAIR_CFG, "SIMKL", "PLEX", stats=stats)

    assert stats["items"] == 4
    assert stats["seeded"] == 1
    assert stats["dead_end"] == 1
    assert stats["failed"] == 0
    assert stats["enriched"] == 2
    assert stats["merged"] == 1
    assert "tmdb:12971" in out


def test_enrich_stats_are_absent_when_mapping_does_not_run(index: Path) -> None:
    stats: dict[str, int] = {}
    enrich_index_for_pair({"a": {"type": "show", "ids": {"simkl": "41487"}}}, _PAIR_CFG, "PLEX", "TRAKT", stats=stats)
    assert stats == {}


def test_enrich_stats_are_absent_on_stale_schema(index: Path) -> None:
    storage.write_state("v3", {"schema_version": 1})
    stats: dict[str, int] = {}
    enrich_index_for_pair({"a": {"type": "show", "ids": {"simkl": "41487"}}}, _PAIR_CFG, "SIMKL", "PLEX", stats=stats)
    assert stats == {}


def test_enrich_without_stats_still_works(index: Path) -> None:
    out = enrich_index_for_pair({"simkl:41487": {"type": "show", "ids": {"simkl": "41487"}}}, _PAIR_CFG, "SIMKL", "PLEX")
    assert "tmdb:12971" in out


# --- startup schema check -----------------------------------------------------

_BOOT_CFG = {"anime_mapping": {"enabled": True, "release_tag": "v3"}}


def test_boot_check_reports_ready(index: Path) -> None:
    from cw_platform.anime_mapping import boot_check

    res = boot_check(cfg=_BOOT_CFG)
    assert res["ok"] is True
    assert res["status"] == "ready"
    assert res["schema_version"] == storage.SCHEMA_VERSION
    assert res["edge_count"] > 0


def test_boot_check_reindexes_a_stale_schema(index: Path) -> None:
    from cw_platform.anime_mapping import boot_check

    storage.write_state("v3", {"schema_version": storage.SCHEMA_VERSION - 1})
    assert storage.index_ready("v3") is False

    res = boot_check(cfg=_BOOT_CFG)
    assert res["ok"] is True
    assert res["status"] == "reindexed"
    assert res["schema_version"] == storage.SCHEMA_VERSION
    assert storage.index_ready("v3") is True
    assert storage.query_identity_natives("v3", "simkl", "41487")["mal"] == "813"


def test_boot_check_rebuilds_a_missing_index(index: Path) -> None:
    from cw_platform.anime_mapping import boot_check

    storage.close_readers()
    storage.paths("v3")["db"].unlink()
    res = boot_check(cfg=_BOOT_CFG)
    assert res["status"] == "reindexed"
    assert storage.index_ready("v3") is True


def test_boot_check_does_not_repair_when_disabled(index: Path) -> None:
    from cw_platform.anime_mapping import boot_check

    storage.write_state("v3", {"schema_version": 1})
    res = boot_check(cfg={"anime_mapping": {"enabled": False, "release_tag": "v3"}})
    assert res["status"] == "disabled"
    assert res["ok"] is True
    assert storage.index_ready("v3") is False


def test_boot_check_reports_missing_dataset(config_base: Path) -> None:
    from cw_platform.anime_mapping import boot_check

    res = boot_check(cfg=_BOOT_CFG)
    assert res["status"] == "missing"
    assert res["ok"] is True


def test_boot_check_can_report_without_repairing(index: Path) -> None:
    from cw_platform.anime_mapping import boot_check

    storage.write_state("v3", {"schema_version": 1})
    res = boot_check(cfg=_BOOT_CFG, auto_repair=False)
    assert res["status"] == "stale"
    assert res["ok"] is False
    assert storage.index_ready("v3") is False
